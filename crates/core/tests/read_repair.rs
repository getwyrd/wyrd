//! M3.5 (issue #143, proposal 0005 slice 5): the **read path feeds the shared repair
//! queue** (`0005:174-176`). The scrub loop and the read path land repair obligations
//! on **one** durable queue ([`wyrd_core::repair`]); this is the read-path producer's
//! regression home (the enqueue seam lands in `core`, where the read path lives).
//!
//! BINDING leg 4 of the success criterion: a read that **excludes** a checksum-failing
//! fragment also **enqueues** its chunk for repair onto the same queue scrub feeds — a
//! corruption finding discovered reactively on read is never absorbed silently. Proven
//! in-process over the trait stores. The enqueue is keyed by the very
//! [`repair::repair_key`] the scrub loop also enqueues onto, so "the same queue" holds
//! by construction.
//!
//! Flippable demonstration: drop the enqueue loop in [`read::read_object`] and the
//! repair-queue assertions below fire while the bytes still read back — proving the
//! enqueue, not the read, is what these tests pin.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{encode, FragmentHeader, CORE_HEADER_LEN};
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeRecord, InodeState};
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, read, repair};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, FragmentId, Health, IntegrityFault, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the path is proven over the seams) ----

#[derive(Default)]
struct MemMeta {
    kv: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (k, v) in batch.puts {
            kv.insert(k, v);
        }
        for k in batch.deletes {
            kv.remove(&k);
        }
        Ok(CommitOutcome::Committed)
    }
}

/// A dumb in-memory chunk store holding the real stored fragment bytes; the default
/// `PlacementChunkStore::get_fragment_at` routes straight through by `FragmentId`.
#[derive(Default)]
struct MemChunks {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for MemChunks {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.frags.lock().unwrap().insert(id, fragment);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&id).cloned())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.frags.lock().unwrap().keys().copied().collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.frags.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

impl PlacementChunkStore for MemChunks {}

/// A chunk store that returns [`IntegrityFault`] for one specific fragment, simulating
/// a verifying backend that rejects a corrupt fragment with a typed fault instead of
/// returning raw corrupt bytes (e.g. `FsChunkStore`, or the gRPC client that maps
/// `DATA_LOSS` → [`IntegrityFault`]).  All other fragments are served normally from
/// the inner [`MemChunks`].
///
/// Used by issue #288 regressions: the read path must classify a typed `IntegrityFault`
/// the same way it classifies raw corrupt bytes — as a corruption finding that becomes
/// a durable repair obligation — never as an absent or transient-error shard.
struct IntegrityFaultingStore {
    inner: MemChunks,
    /// The one fragment whose fetch returns [`IntegrityFault`].
    fault_id: FragmentId,
}

#[async_trait]
impl ChunkStore for IntegrityFaultingStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        if id == self.fault_id {
            return Err(Box::new(IntegrityFault {
                id,
                detail: "checksum mismatch injected by test double (issue #288)".into(),
            }));
        }
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// The default `get_fragment_at` delegates to `get_fragment`, so a store whose
/// `get_fragment` returns `IntegrityFault` for a fragment also returns `IntegrityFault`
/// from `get_fragment_at` — exactly the shape `FsChunkStore` and the gRPC client emit.
impl PlacementChunkStore for IntegrityFaultingStore {}

// ---- helpers ----

/// Wrap a shard's bytes in a valid, self-describing v1 fragment for `chunk`.
fn fragment(chunk: ChunkId, payload: &[u8]) -> Bytes {
    Bytes::from(encode(
        &FragmentHeader::new_v1(chunk, payload.len() as u64),
        payload,
    ))
}

/// Commit a single-chunk inode into the metadata store, returning its id.
async fn commit_inode(meta: &MemMeta, inode: u64, chunk: ChunkRef, size: u64) {
    let record = InodeRecord {
        size,
        chunk_map: vec![chunk],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let outcome = meta
        .commit(WriteBatch::new().put(metadata::inode_key(inode), metadata::encode(&record)))
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

// ---- leg 4: an EC read excludes a corrupt fragment AND enqueues its chunk ----

#[tokio::test]
async fn ec_read_excludes_corrupt_fragment_and_enqueues_for_repair() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    // A real RS(2,1) chunk: 3 fragments, reconstructible from any k = 2.
    let (k, m) = (2u8, 1u8);
    let data = b"the read path catches bit rot too";
    let chunk_id: ChunkId = 0xF00D;
    let shards = erasure::encode(k as usize, m as usize, data).unwrap();
    assert_eq!(shards.len(), 3);

    // Store all three fragments; then inject a bit-flip into fragment index 0's
    // payload so its checksum fails — the read must exclude it and reconstruct from
    // the surviving two.
    for (index, shard) in shards.iter().enumerate() {
        chunks
            .put_fragment(
                FragmentId {
                    chunk: chunk_id,
                    index: index as u16,
                },
                encode_ec_fragment(chunk_id, index as u16, k, m, shard),
            )
            .await
            .unwrap();
    }
    let mut rotten = encode_ec_fragment(chunk_id, 0, k, m, &shards[0]).to_vec();
    rotten[CORE_HEADER_LEN as usize] ^= 0xff;
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            Bytes::from(rotten),
        )
        .await
        .unwrap();

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![0, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    // The read reconstructs byte-identical from the two surviving fragments...
    let got = read::read_object(&meta, &chunks, 1).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(data.as_slice()),
        "the corrupt fragment is read around; the object reconstructs"
    );

    // ...AND the chunk it excluded is now a durable repair obligation on the SAME
    // queue scrub feeds — keyed by the shared `repair::repair_key` (`0005:174-176`).
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "the read-time checksum failure enqueued its chunk for reconstruction"
    );
    assert_eq!(
        meta.get(&repair::repair_key(chunk_id)).await.unwrap(),
        Some(Bytes::from_static(b"read")),
        "the obligation records the read-path producer"
    );
}

// ---- leg 4 (unrecoverable case): a corrupt single fragment still enqueues ----

#[tokio::test]
async fn unrecoverable_read_still_enqueues_the_corrupt_chunk() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    // A `none`-scheme chunk has a single fragment; a corrupt one cannot be read
    // around, so the read fails — but the corruption is still a durable repair
    // obligation, never silently absorbed.
    let chunk_id: ChunkId = 0xBEEF;
    let payload = b"lonely fragment";
    let mut rotten = fragment(chunk_id, payload).to_vec();
    rotten[CORE_HEADER_LEN as usize] ^= 0xff;
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            Bytes::from(rotten),
        )
        .await
        .unwrap();

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::None,
            len: payload.len() as u64,
            placement: vec![0],
        },
        payload.len() as u64,
    )
    .await;

    let result = read::read_object(&meta, &chunks, 1).await;
    assert!(
        result.is_err(),
        "a corrupt single fragment cannot be read around: the read fails"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "even a failed read leaves the corrupt chunk enqueued for reconstruction"
    );
}

// ---- issue #198: the read path enforces the `chunk_id` recheck the shared verify ----
// ---- enforces — a misplaced-but-intact fragment is rejected on BOTH schemes -----------
//
// A misrouted / placement-confused fragment has a VALID self-describing checksum but a
// header naming a DIFFERENT chunk. Scrub (`repair::fragment_intact`) and reconstruction
// (`repair::intact_shard`) admit a fragment only if it decodes cleanly AND its header's
// `chunk_id` matches; the read path must apply the same gate (`0005:262-267`,
// `0005:174-176`). Flippable: drop the `decoded.header.chunk_id == chunk.id` guard from
// either read site in `read::read_chunk` and the matching assertion below fires.

/// `EcScheme::None`: a single stored fragment that decodes cleanly but whose header
/// names a DIFFERENT chunk must NOT be returned as the requested chunk's payload — the
/// read fails rather than handing back the foreign bytes.
#[tokio::test]
async fn none_read_rejects_a_misplaced_but_intact_fragment() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    let chunk_id: ChunkId = 0xC0FFEE;
    // A perfectly intact fragment — valid header + payload checksums — whose header
    // names a DIFFERENT chunk. Its payload is the SAME length as the chunk we read, so a
    // pre-fix admit would clear the inode size check and silently return foreign bytes.
    let foreign_chunk: ChunkId = 0xDEAD_BEEF;
    let foreign_payload = b"another chunk's bytes!";
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            fragment(foreign_chunk, foreign_payload),
        )
        .await
        .unwrap();

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::None,
            len: foreign_payload.len() as u64,
            placement: vec![0],
        },
        foreign_payload.len() as u64,
    )
    .await;

    // The foreign fragment is never admitted as this chunk: the read fails rather than
    // returning the misplaced fragment's bytes. (Pre-fix the bare `decode` accepts on
    // `Ok` and this returns `Ok(Some(foreign_payload))`.)
    let got = read::read_object(&meta, &chunks, 1).await;
    assert!(
        got.is_err(),
        "a misplaced-but-intact fragment (foreign chunk_id) must not be returned as the chunk; \
         got {got:?}"
    );
}

// ---- issue #288: read path must enqueue repair when a verifying store returns --------
// ---- IntegrityFault instead of raw corrupt bytes ------------------------------------
//
// Real backends (`FsChunkStore`, the gRPC client mapping `DATA_LOSS`) surface corruption
// as `Err(IntegrityFault)` from `get_fragment` / `get_fragment_at`, NOT as raw corrupt
// bytes.  Before the fix:
//   * `EcScheme::None`: `.await?` propagated the error before `corrupt.push` could run.
//   * `EcScheme::ReedSolomon`: `if let Ok(Some(fragment)) = fetched` silently dropped
//     all `Err` variants — the `IntegrityFault` was absorbed as if the shard were absent.
// In both cases the repair queue received no entry despite a corruption finding.
// Flippable: revert the fix in `read.rs` (restore the `?` / `if let`) and both
// assertions below fire while the bytes either fail or read back — proving the enqueue,
// not the read behaviour, is what these tests pin.

/// Issue #288 — `EcScheme::ReedSolomon`: when `get_fragment_at` returns `IntegrityFault`
/// for one shard the read path must (1) record the chunk as a durable repair obligation
/// and (2) read around the faulted shard and reconstruct from the surviving shards.
///
/// Before the fix: the `if let Ok(Some(fragment))` guard silently dropped the `Err`,
/// treating the faulted shard as absent — no `corrupt.push`, no repair entry (red).
/// After the fix: the `IntegrityFault` arm records the chunk and continues; the two
/// surviving shards reconstruct the object (green).
#[tokio::test]
async fn ec_read_enqueues_integrity_fault_shard_for_repair_and_reconstructs() {
    let meta = MemMeta::default();

    // RS(2,1): 3 fragments, any k = 2 suffice to reconstruct.
    let (k, m) = (2u8, 1u8);
    let data = b"repair queue must see integrity-fault shards too";
    let chunk_id: ChunkId = 0xDEAD_C0DE;
    let shards = erasure::encode(k as usize, m as usize, data).unwrap();
    assert_eq!(shards.len(), 3);

    // Store all three fragments as valid bytes in the inner store.
    let inner = MemChunks::default();
    for (index, shard) in shards.iter().enumerate() {
        inner
            .put_fragment(
                FragmentId {
                    chunk: chunk_id,
                    index: index as u16,
                },
                encode_ec_fragment(chunk_id, index as u16, k, m, shard),
            )
            .await
            .unwrap();
    }

    // Fragment index 0 returns `IntegrityFault` instead of bytes — the exact shape
    // `FsChunkStore` (on-disk checksum failure) and the gRPC client (DATA_LOSS) emit.
    // Indices 1 and 2 are valid; k = 2 of them reconstruct the object.
    let fault_id = FragmentId {
        chunk: chunk_id,
        index: 0,
    };
    let chunks = IntegrityFaultingStore { inner, fault_id };

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![0, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    // The read reconstructs from the two surviving fragments...
    let got = read::read_object(&meta, &chunks, 1).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(data.as_slice()),
        "the IntegrityFault shard is read around; the object reconstructs from the two survivors"
    );

    // ...AND the chunk is now a durable repair obligation on the shared queue — the
    // same queue scrub feeds (`0005:174-176`), keyed by `repair::repair_key`.
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "IntegrityFault from get_fragment_at must enqueue the chunk for reconstruction"
    );
    assert_eq!(
        meta.get(&repair::repair_key(chunk_id)).await.unwrap(),
        Some(Bytes::from_static(b"read")),
        "the repair obligation records the read-path producer"
    );
}

/// Issue #288 — `EcScheme::None`: when `get_fragment_at` returns `IntegrityFault` for
/// the single fragment the read path must enqueue the chunk for repair before surfacing
/// the error.
///
/// Before the fix: `.await?` propagated the `IntegrityFault` before `corrupt.push` ran
/// — no repair entry even though a corruption signal was received (red).
/// After the fix: the `IntegrityFault` arm pushes the chunk and returns the error; the
/// repair queue contains the chunk (green).
#[tokio::test]
async fn none_read_enqueues_integrity_fault_fragment_for_repair() {
    let meta = MemMeta::default();

    let chunk_id: ChunkId = 0x1234_5678;
    let payload = b"single-fragment chunk with integrity fault";
    let fault_id = FragmentId {
        chunk: chunk_id,
        index: 0,
    };

    // The inner store holds the (would-be) valid bytes; the faulting wrapper never
    // returns them — it always returns `IntegrityFault` for `fault_id`.
    let inner = MemChunks::default();
    inner
        .put_fragment(fault_id, fragment(chunk_id, payload))
        .await
        .unwrap();
    let chunks = IntegrityFaultingStore { inner, fault_id };

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::None,
            len: payload.len() as u64,
            placement: vec![0],
        },
        payload.len() as u64,
    )
    .await;

    // The read fails — a single-fragment `none`-scheme chunk has nothing to
    // reconstruct around...
    let result = read::read_object(&meta, &chunks, 1).await;
    assert!(
        result.is_err(),
        "an IntegrityFault on a single-fragment chunk cannot be read around: the read must fail"
    );

    // ...AND the chunk is a durable repair obligation on the shared queue, recorded
    // BEFORE the error was surfaced (`0005:174-176`).
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk_id],
        "IntegrityFault from get_fragment_at must enqueue the chunk even when the read fails"
    );
    assert_eq!(
        meta.get(&repair::repair_key(chunk_id)).await.unwrap(),
        Some(Bytes::from_static(b"read")),
        "the repair obligation records the read-path producer"
    );
}

/// `EcScheme::ReedSolomon`: a misplaced-but-intact fragment occupying a surviving index
/// is treated as **absent** — read around, never fed to the decoder. With only one
/// genuine survivor besides it (below `k`), the read fails on insufficient fragments
/// rather than feeding the foreign shard into the decoder (silent corrupt
/// reconstruction).
#[tokio::test]
async fn ec_read_treats_a_misplaced_but_intact_fragment_as_absent() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();

    // RS(2,1): 3 fragments, k = 2 needed to reconstruct. The payload spans BOTH data
    // shards (> one aligned shard), so a foreign shard at a data index corrupts the live
    // output — not padding that truncation discards — making "silent corrupt
    // reconstruction" the real pre-fix outcome.
    let (k, m) = (2u8, 1u8);
    let data: Vec<u8> = (0..200u32).map(|i| (i % 199) as u8).collect();
    let chunk_id: ChunkId = 0xF00D;
    let shards = erasure::encode(k as usize, m as usize, &data).unwrap();
    assert_eq!(shards.len(), 3);
    let shard_len = shards[0].len();

    // Index 0: the genuine data shard. Index 2: missing. Index 1: a misplaced-but-intact
    // fragment — a valid fragment of the SAME shard length whose header names a DIFFERENT
    // chunk (filled with 0xFF, a byte the data never contains). Only k - 1 = 1 genuine
    // survivor remains, so the misplaced fragment is the only thing that could lift the
    // read to k = 2 — if it is (wrongly) admitted.
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            encode_ec_fragment(chunk_id, 0, k, m, &shards[0]),
        )
        .await
        .unwrap();
    let foreign_chunk: ChunkId = 0x9999;
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 1,
            },
            fragment(foreign_chunk, &vec![0xFF; shard_len]),
        )
        .await
        .unwrap();

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![0, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    // The misplaced fragment is excluded (treated as absent), leaving only one genuine
    // survivor (< k), so the read fails — it is never fed to the decoder. (Pre-fix the
    // bare `decode` accepts it on `Ok`, the decoder runs on a foreign shard at index 1,
    // and the read returns silently corrupt bytes.)
    let got = read::read_object(&meta, &chunks, 1).await;
    assert!(
        got.is_err(),
        "a misplaced-but-intact shard must be read around, not fed to the decoder; got {got:?}"
    );
}

// ---- #530: the read path must NAME the failing fragment and its D-server ----

/// A store that fails one fragment with a **transient** (non-integrity) error — an
/// unreachable / timing-out / erroring D-server, the shape `chunkstore-grpc`'s
/// `TransportError` arrives in. All other fragments serve normally.
struct TransientlyFailingStore {
    inner: MemChunks,
    fail_id: FragmentId,
}

#[async_trait]
impl ChunkStore for TransientlyFailingStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }
    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        if id == self.fail_id {
            return Err("transport: the D-server is unreachable (connect refused)".into());
        }
        self.inner.get_fragment(id).await
    }
    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }
    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }
    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

impl PlacementChunkStore for TransientlyFailingStore {}

/// A `MakeWriter` collecting what the subscriber emits, so the test asserts on the record
/// the read path actually produced rather than assuming one exists.
#[derive(Clone, Default)]
struct Capture(std::sync::Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
    type Writer = Self;
    fn make_writer(&'w self) -> Self::Writer {
        self.clone()
    }
}

/// Install a permissive global `tracing` default **once**, so the read path's audit callsites
/// never latch `Interest::never` under the parallel test harness.
///
/// `tracing` caches each callsite's interest in a process-global table the first time it is
/// hit. A sibling test in this binary that reads a corrupt fragment with no subscriber
/// installed would hit `emit_fragment_fault`'s callsites first and latch them **disabled for
/// the whole process** — after which the tests below capture an empty buffer and fail, or
/// worse, would silently pass a weaker assertion. Registering an always-enabling default
/// before any callsite fires makes every first-registration agree.
///
/// This is the proven pattern from `crates/custodian/tests/scrub.rs` and
/// `crates/server/tests/custodian_day_one.rs` (`enable_metric_callsites`), which exist for
/// exactly this hazard.
fn enable_audit_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

fn capturing_dispatch(capture: Capture) -> tracing::Dispatch {
    use tracing_subscriber::layer::SubscriberExt;
    tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().json().with_writer(capture)),
    )
}

/// **A corrupt fragment must be reported with the D-server it was placed on, not merely the
/// chunk it belongs to.**
///
/// The placement here is deliberately **non-identity** (`[7, 1, 2]`): fragment index 0 lives
/// on D-server **7**. So an implementation that logs the fragment index in the `dserver` field
/// — the obvious way to get this wrong — reports `0` and fails. The read path resolves the
/// D-server through `placed_dserver`, and the log must report *that*.
///
/// Pre-fix the failure arm was `corrupt.push(chunk.id)` — the chunk id and nothing else, into
/// a `Vec<ChunkId>` — while `index` and `dserver` sat live in the enclosing scope. The
/// operator could learn that *something* was rotting and never *which disk* (#530). And it
/// went to no sink regardless (#527). The capture buffer is EMPTY — RED.
#[tokio::test]
async fn a_corrupt_fragment_is_reported_with_its_index_and_its_placed_dserver() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let chunks = MemChunks::default();
    let (k, m) = (2u8, 1u8);
    let data = b"which disk is rotting?";
    let chunk_id: ChunkId = 0xF00D;
    let shards = erasure::encode(k as usize, m as usize, data).unwrap();

    for (index, shard) in shards.iter().enumerate() {
        chunks
            .put_fragment(
                FragmentId {
                    chunk: chunk_id,
                    index: index as u16,
                },
                encode_ec_fragment(chunk_id, index as u16, k, m, shard),
            )
            .await
            .unwrap();
    }
    // Rot fragment index 0 — which the placement puts on D-server 7.
    let mut rotten = encode_ec_fragment(chunk_id, 0, k, m, &shards[0]).to_vec();
    rotten[CORE_HEADER_LEN as usize] ^= 0xff;
    chunks
        .put_fragment(
            FragmentId {
                chunk: chunk_id,
                index: 0,
            },
            Bytes::from(rotten),
        )
        .await
        .unwrap();

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![7, 1, 2],
        },
        data.len() as u64,
    )
    .await;

    let capture = Capture::default();
    let dispatch = capturing_dispatch(capture.clone());
    let got = {
        let _guard = tracing::dispatcher::set_default(&dispatch);
        read::read_object(&meta, &chunks, 1).await.unwrap()
    };

    // The fail-closed guarantee is untouched: the object still reconstructs from the k
    // survivors. This change adds a record; it does not alter control flow.
    assert_eq!(got.as_deref(), Some(data.as_slice()));

    let logged = capture.contents();
    assert!(
        logged.contains(r#""class":"corrupt""#),
        "the fault CLASS must be reported — corruption and a dead node need different \
         responses. pre-fix the buffer is EMPTY. got: {logged}"
    );
    assert!(
        logged.contains(r#""dserver":7"#),
        "the log must name the PLACED D-server (7), not the fragment index (0) — this is the \
         whole question a fault-injection experiment asks. got: {logged}"
    );
    assert!(
        logged.contains(r#""index":0"#),
        "the fragment index must be named. got: {logged}"
    );
    assert!(
        logged.contains(&format!("{chunk_id:032x}")),
        "the chunk must be named in the canonical hex form. got: {logged}"
    );
}

/// **A D-server that fails every RPC must not look healthy.**
///
/// This is the sharpest gap in the old code: the transient arm was literally `Err(_) => {}`.
/// A D-server that was up but timing out, refusing connections, or returning
/// `Rpc(Status::internal(..))` on every single read produced **no counter, no log, no trace** —
/// indistinguishable from a healthy node from every available vantage point. Reads quietly got
/// slower and leaned on parity, and nobody found out until a chunk fell below `k`.
///
/// The control flow is deliberately unchanged (the fragment is still read around, and it is
/// still NOT a corruption finding — reclassification is #431's question). Only the silence is.
///
/// Pre-fix: empty buffer, and `queued_repairs` empty — RED on the first assertion, and the
/// last assertion guards against over-correcting into a false corruption finding.
#[tokio::test]
async fn a_transient_dserver_failure_is_no_longer_silent() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let (k, m) = (2u8, 1u8);
    let data = b"a node that fails every rpc must not look healthy";
    let chunk_id: ChunkId = 0xBEEF;
    let shards = erasure::encode(k as usize, m as usize, data).unwrap();

    let inner = MemChunks::default();
    for (index, shard) in shards.iter().enumerate() {
        inner
            .put_fragment(
                FragmentId {
                    chunk: chunk_id,
                    index: index as u16,
                },
                encode_ec_fragment(chunk_id, index as u16, k, m, shard),
            )
            .await
            .unwrap();
    }
    // Fragment index 0 — placed on D-server 3 — comes from a node that refuses every read.
    //
    // Index 0, not index 2, and that matters: the read fires all n fetches and stops the
    // instant k of them verify (any-k-arrive-first, §6.2), CANCELLING the rest. A fault on the
    // last-polled fragment would never be observed at all — not a gap, the design working. The
    // node the read actually has to touch is the one whose silence was costing us.
    let chunks = TransientlyFailingStore {
        inner,
        fail_id: FragmentId {
            chunk: chunk_id,
            index: 0,
        },
    };

    commit_inode(
        &meta,
        1,
        ChunkRef {
            id: chunk_id,
            scheme: EcScheme::ReedSolomon { k, m },
            len: data.len() as u64,
            placement: vec![3, 4, 5],
        },
        data.len() as u64,
    )
    .await;

    let capture = Capture::default();
    let dispatch = capturing_dispatch(capture.clone());
    let got = {
        let _guard = tracing::dispatcher::set_default(&dispatch);
        read::read_object(&meta, &chunks, 1).await.unwrap()
    };
    assert_eq!(
        got.as_deref(),
        Some(data.as_slice()),
        "still reads around it"
    );

    let logged = capture.contents();
    assert!(
        logged.contains(r#""class":"transient""#) && logged.contains(r#""dserver":3"#),
        "the failing node must be NAMED and classed transient; pre-fix `Err(_) => {{}}` made it \
         invisible. got: {logged}"
    );
    assert!(
        logged.contains("unreachable"),
        "the transport's own classification must survive to the log — chunkstore-grpc computes \
         Unavailable/Timeout/Rpc/Connect and its one consumer used to bin it. got: {logged}"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "a transient fault is NOT a corruption finding — this must not enqueue a repair \
         obligation (that reclassification is #431's question, not this change's)"
    );
}
