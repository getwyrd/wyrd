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

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::{encode, FragmentHeader, CORE_HEADER_LEN};
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeRecord, InodeState};
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
                fragment(chunk_id, shard),
            )
            .await
            .unwrap();
    }
    let mut rotten = fragment(chunk_id, &shards[0]).to_vec();
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
                fragment(chunk_id, shard),
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
            fragment(chunk_id, &shards[0]),
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
