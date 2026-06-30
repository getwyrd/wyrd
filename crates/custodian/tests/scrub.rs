//! M3.5 (issue #143, proposal 0005 slice 5, `0005:528-530`): the **scrub custodian
//! loop**, driven through the real [`reconcile_step`] fenced control point.
//!
//! The BINDING legs of the success criterion, proven in-process over the trait
//! stores (Option A — no deployed custodian process exists yet, `0005:524-527`):
//!
//! 1. **Walk + verify** (`0005:262-263`): through `reconcile_step`, scrub walks each
//!    store (`list_fragments`) and verifies each **referenced** fragment's
//!    self-describing checksum against the committed chunk map; an unreferenced
//!    fragment (an orphan, GC's concern) is not a scrub finding.
//! 2. **Bit-flip detected, excluded, enqueued** (`0005:263-264`, the central DoD,
//!    flippable): an injected bit-flip in a referenced fragment is detected, the
//!    fragment is treated as lost, and its chunk is enqueued for reconstruction on
//!    the shared repair queue ([`wyrd_core::repair`]) — the same queue the read path
//!    feeds (`0005:174-176`). Scrub never deletes (that is GC / reconstruction).
//!    Flippable: negate `repair::fragment_intact` in `scrub::reconcile` (treat every
//!    fragment as intact) and the enqueue never happens — this assertion fires.
//! 3. **Durability-plane emission** (`0005:331-332`, ADR-0011/0012): scrub coverage
//!    and scrub-detected corruption are emitted on the `DurabilityTelemetry` seam as
//!    metric + audit events and read back in-process via `gather_prometheus`.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::{encode, FragmentHeader, CORE_HEADER_LEN};
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_core::repair;
use wyrd_custodian::{
    reconcile_step, Custodian, DurabilityTelemetry, ExporterConfig, FencedZone, Reconciled,
    ScrubContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the loop is proven over the seams) ----

/// A trivial in-memory metadata store.
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

/// One D server's fragment bytes — a deliberately dumb `ChunkStore` that holds the
/// **real** stored fragment bytes (so their checksums can be verified).
#[derive(Default)]
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for MemDServer {
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

// ---- helpers ----

const ROOT: InodeId = 0;

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A valid, self-describing v1 fragment for `chunk` (header + payload crc32c stamped
/// by the on-disk-format writer). This is what `fragment_intact` must accept.
fn valid_fragment(chunk: ChunkId) -> Bytes {
    let payload = b"scrubbable bytes";
    Bytes::from(encode(
        &FragmentHeader::new_v1(chunk, payload.len() as u64),
        payload,
    ))
}

/// The same fragment with a single **bit flipped in the payload** — its trailing
/// payload checksum no longer matches, so `decode` fails its crc32c gate: injected
/// bit rot.
fn corrupt_fragment(chunk: ChunkId) -> Bytes {
    let mut bytes = valid_fragment(chunk).to_vec();
    // Flip the first payload byte (just past the fixed core header).
    bytes[CORE_HEADER_LEN as usize] ^= 0xff;
    Bytes::from(bytes)
}

/// Commit an inode whose single `none`-scheme chunk's fragment at index 0 is placed
/// on `dserver` — a committed reference scrub must verify.
async fn commit_reference(
    meta: &MemMeta,
    inode: InodeId,
    name: &str,
    chunk: ChunkId,
    dserver: DServerId,
) {
    let record = InodeRecord {
        size: 16,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 16,
            placement: vec![dserver],
        }],
        state: InodeState::Committed,
        version: 1,
    };
    let outcome = metadata::create(meta, ROOT, name, inode, &record)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

/// Commit an inode whose single chunk map entry is exactly `chunk_ref` — used by the
/// empty/short/RS{6,3} placement cells below, which need schemes and placement shapes
/// `commit_reference` (`EcScheme::None`, one explicit dserver) cannot express.
async fn commit_chunk(meta: &MemMeta, inode: InodeId, name: &str, chunk_ref: ChunkRef) {
    let record = InodeRecord {
        size: 16,
        chunk_map: vec![chunk_ref],
        state: InodeState::Committed,
        version: 1,
    };
    let outcome = metadata::create(meta, ROOT, name, inode, &record)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-scrub").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

/// Install a permissive process-global default `tracing` subscriber once for this
/// test binary.
///
/// The durability metrics are emitted as `tracing::info!(monotonic_counter.* )`
/// callsites. `tracing` caches each callsite's *interest* in a process-global
/// table the first time it is hit. Most scrub tests run the pass WITHOUT
/// installing a subscriber, so that first hit would otherwise register the
/// callsite against the no-op default — and two such first-registrations racing
/// across test threads can latch the callsite as *disabled*, after which the one
/// test that DOES read the metric back (`gather_prometheus`) silently sees it
/// missing (flaky: corruption present, coverage absent). Registering against an
/// always-enabling default makes every first-registration agree, so the callsite
/// can never latch disabled. Per-test `.with_subscriber(...)` still overrides
/// this default to capture metrics into that test's own provider.
fn enable_metric_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

// ---- criterion 1: walk + verify referenced fragments through the control point ----

#[tokio::test]
async fn walks_and_verifies_referenced_fragments_through_reconcile_step() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // A referenced, intact fragment scrub must verify and pass.
    let chunk: ChunkId = 0xA1;
    d0.put_fragment(frag(chunk, 0), valid_fragment(chunk))
        .await
        .unwrap();
    commit_reference(&meta, 1, "intact", chunk, 0).await;

    // An UNREFERENCED fragment (an orphan: present, referenced by no committed chunk
    // map) — GC's concern, NOT a scrub finding even though it is also "valid" bytes
    // here. Scrub must skip it: it verifies only what the chunk map references.
    let orphan: ChunkId = 0xB2;
    d0.put_fragment(frag(orphan, 0), corrupt_fragment(orphan))
        .await
        .unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "all referenced fragments verified intact; the unreferenced orphan is not scrubbed"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "no repair obligation: the referenced fragment is intact, the corrupt one is unreferenced"
    );
}

// ---- criterion 2: bit-flip detected, excluded, enqueued (the central, flippable leg) ----

#[tokio::test]
async fn detects_a_bitflip_excludes_and_enqueues_for_reconstruction() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // A committed chunk map references (chunk, 0) on d0, but its stored bytes carry
    // an injected bit-flip — its payload checksum no longer verifies.
    let chunk: ChunkId = 0xC3;
    d0.put_fragment(frag(chunk, 0), corrupt_fragment(chunk))
        .await
        .unwrap();
    commit_reference(&meta, 1, "rotten", chunk, 0).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the bit-flip is detected and a repair obligation is produced"
    );

    // The chunk is enqueued for reconstruction on the SHARED repair queue — keyed by
    // the very `repair::repair_key` the read path also enqueues onto (`0005:174-176`).
    assert!(
        meta.get(&repair::repair_key(chunk))
            .await
            .unwrap()
            .is_some(),
        "the corrupt fragment's chunk is enqueued on the shared repair queue"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk],
        "exactly the rotten chunk is queued for reconstruction"
    );

    // Scrub only PRODUCES the obligation — it never deletes the fragment (reclaiming
    // displaced bytes is GC; rebuilding is reconstruction, slice 6, out of scope).
    assert!(
        d0.get_fragment(frag(chunk, 0)).await.unwrap().is_some(),
        "scrub does not delete the corrupt fragment; it only enqueues its chunk"
    );
}

// ---- criterion 2 (second half): an INTACT-but-misplaced fragment is also detected ----
//
// Verifying the checksum ALONE is not enough: scrub must verify each referenced
// fragment's self-describing header AGAINST THE COMMITTED CHUNK MAP (`0005:264-266`).
// A fragment whose payload checksum verifies cleanly but whose header names a
// *different* chunk than the map stores it under is corruption all the same — feeding
// its bytes to the referenced chunk's decoder would silently reconstruct another
// chunk's data. This pins the `decoded.header.chunk_id == chunk` half of
// `repair::fragment_intact` (the half the bit-flip case above does not reach).
//
// Flippable demonstration (recorded in build-notes): drop `&& header.chunk_id == chunk`
// from `repair::fragment_intact` and this fragment is absorbed silently — the enqueue
// never happens and the two assertions below fire.
#[tokio::test]
async fn detects_a_misplaced_intact_fragment_excludes_and_enqueues_for_reconstruction() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // The committed chunk map references (chunk, 0) on d0, but the bytes physically
    // stored there are a perfectly VALID v1 fragment for a DIFFERENT chunk: its
    // payload checksum verifies, yet its header names `foreign`, not `chunk`.
    let chunk: ChunkId = 0xC8;
    let foreign: ChunkId = 0x9999;
    d0.put_fragment(frag(chunk, 0), valid_fragment(foreign))
        .await
        .unwrap();
    commit_reference(&meta, 1, "misplaced", chunk, 0).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "an intact-but-misplaced fragment is a corruption finding, not silently absorbed"
    );

    // The REFERENCED chunk is enqueued for reconstruction — never the foreign header's
    // id, which the chunk map does not reference here.
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk],
        "exactly the referenced chunk is queued; the misplaced fragment is excluded"
    );
    assert!(
        meta.get(&repair::repair_key(foreign))
            .await
            .unwrap()
            .is_none(),
        "the foreign chunk the stray header names is not the obligation produced"
    );

    // Scrub only PRODUCES the obligation; it never deletes the misplaced fragment.
    assert!(
        d0.get_fragment(frag(chunk, 0)).await.unwrap().is_some(),
        "scrub does not delete the misplaced fragment; it only enqueues its chunk"
    );
}

// ---- criterion 3: scrub coverage + corruption on the durability seam, read back ----

#[tokio::test]
async fn emits_scrub_coverage_and_corruption_on_the_durability_seam() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // One intact and one corrupt referenced fragment, so both the coverage metric
    // (two fragments walked + verified) and the corruption metric (one finding) emit.
    let intact: ChunkId = 0xD4;
    d0.put_fragment(frag(intact, 0), valid_fragment(intact))
        .await
        .unwrap();
    commit_reference(&meta, 1, "intact", intact, 0).await;

    let rotten: ChunkId = 0xE5;
    d0.put_fragment(frag(rotten, 0), corrupt_fragment(rotten))
        .await
        .unwrap();
    commit_reference(&meta, 2, "rotten", rotten, 0).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    // Wire the backend-agnostic durability seam (ADR-0012) and run scrub under it.
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed);

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert!(
        exposed.contains("scrub_coverage"),
        "scrub coverage is exported on the durability seam; got:\n{exposed}"
    );
    assert!(
        exposed.contains("scrub_corruption_detected"),
        "the scrub-detected corruption rate is exported on the durability seam; got:\n{exposed}"
    );
}

// ---- issue #207: a VERIFYING backend (FsChunkStore) — corruption is enqueued AND the
//      pass continues; it never aborts at the first rotten fragment ----
//
// The earlier criteria run over `MemDServer`, which returns the raw (corrupt) bytes for
// scrub's own `fragment_intact` to check. A real on-disk / networked D server instead
// **verifies on read** and *rejects* a corrupt fragment with a
// `wyrd_traits::IntegrityFault` (over gRPC, a `DATA_LOSS` status) — it never hands back
// bytes that already failed the same check. Before the fix scrub propagated that `Err`
// with `?`, aborting the whole pass at the first rotten fragment: bit rot was never
// enqueued, the fragments after it were never scrubbed, and the corruption/coverage
// telemetry stayed all-green. These two tests pin the restored contract — corruption is
// classified, enqueued, and the pass continues — against the real `FsChunkStore` and a
// transient-fault fake, so the corrupt-vs-transient distinction is exercised both ways.

/// A D server whose `get_fragment` always fails with a **non-integrity** (transient)
/// error — the shape of an unreachable / timed-out backend (no `IntegrityFault` in the
/// chain). It lists one referenced fragment so scrub reaches the fetch.
struct TransientDServer {
    id: FragmentId,
}

#[async_trait]
impl ChunkStore for TransientDServer {
    async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> Result<()> {
        Ok(())
    }

    async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "d server unreachable",
        )))
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(vec![self.id])
    }

    async fn delete_fragment(&self, _id: FragmentId) -> Result<()> {
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Unhealthy)
    }
}

#[tokio::test]
async fn fschunkstore_corruption_is_enqueued_and_the_pass_continues() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let dir = tempfile::tempdir().unwrap();
    let store = FsChunkStore::open(dir.path()).unwrap();

    // Three referenced fragments on the on-disk D server: TWO bit-rotten, one intact.
    let rotten_a: ChunkId = 0xF1;
    let rotten_b: ChunkId = 0xF2;
    let intact: ChunkId = 0xF3;
    for chunk in [rotten_a, rotten_b, intact] {
        store
            .put_fragment(frag(chunk, 0), valid_fragment(chunk))
            .await
            .unwrap();
    }
    // Rot the two on disk *after* a valid write — flip a payload byte so the stored
    // bytes no longer verify. (We cannot `put` corrupt bytes: put verifies too.) This
    // is the bit-rot the repro describes; `get_fragment` will now reject each with an
    // `IntegrityFault` rather than return bytes.
    for chunk in [rotten_a, rotten_b] {
        std::fs::write(
            fragment_path(dir.path(), frag(chunk, 0)),
            corrupt_fragment(chunk),
        )
        .unwrap();
    }
    commit_reference(&meta, 1, "rotten-a", rotten_a, 0).await;
    commit_reference(&meta, 2, "rotten-b", rotten_b, 0).await;
    commit_reference(&meta, 3, "intact", intact, 0).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &store)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    // Run under the durability seam so the corruption metric (leg b) can be read back.
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());

    // PRE-FIX: `get_fragment` on the first rotten fragment returns `Err`, scrub
    // propagates it with `?` and ABORTS — `reconcile_step` returns `Err`, so this
    // `unwrap` panics (red). POST-FIX scrub classifies it as corruption, enqueues, and
    // continues over the rest of the D server.
    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the pass detects corruption and produces repair obligations, not an aborting Err"
    );

    // (a) BOTH rotten chunks are enqueued for reconstruction — and only those. That
    // BOTH are queued is the order-independent proof the pass CONTINUED past the first
    // rotten fragment (leg c): had it aborted at the first, the second could never be
    // enqueued.
    let mut queued = repair::queued_repairs(&meta).await.unwrap();
    queued.sort_unstable();
    assert_eq!(
        queued,
        vec![rotten_a, rotten_b],
        "both rotten chunks enqueued (the intact one is not) — the pass continued past the first"
    );

    // (b) the corruption + coverage metrics are emitted on the durability seam for the
    // verifying-backend (Err) path, so the plane no longer reports all-green while data
    // rots.
    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert!(
        exposed.contains("scrub_corruption_detected"),
        "corruption is emitted on the verifying-backend path; got:\n{exposed}"
    );
    assert!(
        exposed.contains("scrub_coverage"),
        "coverage is emitted across the pass; got:\n{exposed}"
    );

    // The intact fragment is still served as valid bytes; a corrupt fragment is never
    // returned as a valid payload — its get is an `Err`, never `Ok(Some(bytes))`.
    assert!(
        store.get_fragment(frag(intact, 0)).await.unwrap().is_some(),
        "the intact referenced fragment is untouched and still served"
    );
    assert!(
        store.get_fragment(frag(rotten_a, 0)).await.is_err(),
        "a corrupt fragment is rejected on read, never served as a valid payload"
    );
}

#[tokio::test]
async fn scrub_propagates_a_transient_get_fault_without_enqueuing() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let chunk: ChunkId = 0xAA;
    let store = TransientDServer { id: frag(chunk, 0) };
    commit_reference(&meta, 1, "transient", chunk, 0).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &store)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    // A transient fault carries no integrity signal, so it is NOT corruption: scrub must
    // not mint a false repair obligation for it. It propagates for the retry policy to
    // decide — the other half of the corrupt-vs-transient distinction at scrub's
    // decision point. (Were the two collapsed, this would either be swallowed as a
    // false repair or abort the pass the way real corruption used to.)
    let result = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0).await;
    assert!(
        result.is_err(),
        "a transient get fault propagates for retry; it is not classified as corruption"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "a transient fault never enqueues a repair obligation"
    );
}

// ---- issue #349 (mixed-era placement matrix): Scrub — empty/short placement, both
//      schemes, + an RS{6,3} cell, mirroring the existing full(None) coverage above
//      (`walks_and_verifies_referenced_fragments_through_reconcile_step`,
//      `detects_a_bitflip_excludes_and_enqueues_for_reconstruction`). Scrub resolves
//      its reference set through `crate::gc::referenced_fragments`
//      (`crates/custodian/src/scrub.rs:30`), which expands every committed `ChunkRef`
//      through the SAME `ChunkRef::placed_dserver` identity-placement fallback GC,
//      read, and reconstruction share (`core/src/metadata.rs:119-124`,
//      `crates/custodian/src/gc.rs:189-204`). Each case below commits a corrupt
//      fragment at an identity-fallback (or mixed explicit/fallback) location and
//      asserts scrub still finds it: a fragment scrub cannot resolve into its
//      reference set is a fragment scrub silently never scrubs while bit rot
//      accumulates underneath it (the silent-corruption invariant `0005:262-267`).
//
// Flippable (recorded in build-notes): revert `gc.rs:referenced_fragments`'s
// `chunk.placed_dserver(index)` expansion (`gc.rs:197-204`) back to raw
// `chunk.placement.iter().enumerate()` — an empty/short vector then yields too few
// (or zero) referenced fragments, the corrupt fragment at the fallback index is
// excluded from the reference set as "unreferenced", and the four assertions below
// fire (`Reconciled::Satisfied` / an empty repair queue instead of the corruption
// being caught).

/// Sub-case mirroring gc.rs's 4a: `EcScheme::None` + `placement: vec![]` — the single
/// fragment at index 0 resolves via identity fallback (D-server 0).
#[tokio::test]
async fn detects_corruption_on_an_empty_placement_none_chunk() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    let chunk: ChunkId = 0xF9_00;
    d0.put_fragment(frag(chunk, 0), corrupt_fragment(chunk))
        .await
        .unwrap();
    commit_chunk(
        &meta,
        1,
        "empty-none",
        ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 16,
            placement: Vec::new(), // pre-M3: empty placement, identity fallback applies
        },
    )
    .await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "empty-placement EcScheme::None: the identity-fallback fragment is referenced \
         and its bit-flip is detected"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk],
        "the identity-resolved chunk is enqueued for reconstruction"
    );
}

/// Sub-case mirroring gc.rs's 4b: `EcScheme::ReedSolomon{2,1}` + `placement: vec![]` —
/// the corrupt fragment sits at index 1 (D-server 1 via identity fallback), proving
/// the fallback at an index ABOVE zero, not just the trivial index-0 case above.
#[tokio::test]
async fn detects_corruption_on_an_empty_placement_rs_chunk_above_index_zero() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d1 = MemDServer::default();

    let chunk: ChunkId = 0xF9_01;
    d1.put_fragment(frag(chunk, 1), corrupt_fragment(chunk))
        .await
        .unwrap();
    commit_chunk(
        &meta,
        1,
        "empty-rs",
        ChunkRef {
            id: chunk,
            scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
            len: 16,
            placement: Vec::new(), // pre-M3: empty, identity fallback for all 3 indices
        },
    )
    .await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    // Fleet only contains D-server 1 — the one with the corrupt fragment.
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(1, &d1)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "empty-placement RS{{2,1}}: the identity-fallback fragment at index 1 is \
         referenced and its bit-flip is detected"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk],
        "the identity-resolved chunk is enqueued for reconstruction"
    );
}

/// Sub-case mirroring gc.rs's 4c: `EcScheme::ReedSolomon{2,1}` + `placement: vec![5]`
/// (short — only index 0 explicit, on D-server 5). The corrupt fragment sits at index
/// 2 (D-server 2 via identity fallback), proving the mixed explicit/fallback
/// resolution: `.get(2)` is absent, so `placed_dserver(2)` falls back to 2.
#[tokio::test]
async fn detects_corruption_at_a_short_placement_vectors_fallback_index() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d2 = MemDServer::default();

    let chunk: ChunkId = 0xF9_02;
    d2.put_fragment(frag(chunk, 2), corrupt_fragment(chunk))
        .await
        .unwrap();
    commit_chunk(
        &meta,
        1,
        "short-rs",
        ChunkRef {
            id: chunk,
            scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
            len: 16,
            // Short vector: index 0 -> D-server 5 (explicit), indices 1 and 2 fall back.
            placement: vec![5],
        },
    )
    .await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    // Fleet only contains D-server 2 — the one with the corrupt fallback-resolved
    // fragment.
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(2, &d2)];
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "short placement: the fallback-resolved fragment at index 2 is referenced and \
         its bit-flip is detected"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk],
        "the identity-resolved chunk is enqueued for reconstruction"
    );
}

/// The brief's required RS{6,3} cell: a FULL, explicit rs(6,3) placement (9 fragments
/// across 9 distinct D servers) — the scheme size gap left open by every other scrub
/// case above (RS{2,1}) and by the pre-existing full(None) coverage
/// (`detects_a_bitflip_excludes_and_enqueues_for_reconstruction`). One of the 9
/// referenced fragments is corrupt; scrub must walk, verify, and enqueue it.
#[tokio::test]
async fn detects_corruption_in_a_full_rs_6_3_placement() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..9).map(|_| MemDServer::default()).collect();

    let chunk: ChunkId = 0xF9_03;
    // Full, explicit placement: fragment i -> D-server i (9 distinct servers).
    let placement: Vec<DServerId> = (0..9).collect();
    for (index, server) in servers.iter().enumerate() {
        let bytes = if index == 4 {
            corrupt_fragment(chunk) // one corrupt referenced fragment, mid-vector
        } else {
            valid_fragment(chunk)
        };
        server
            .put_fragment(frag(chunk, index as u16), bytes)
            .await
            .unwrap();
    }
    commit_chunk(
        &meta,
        1,
        "full-rs-6-3",
        ChunkRef {
            id: chunk,
            scheme: EcScheme::ReedSolomon { k: 6, m: 3 },
            len: 16,
            placement: placement.clone(),
        },
    )
    .await;
    assert_eq!(
        placement.len(),
        9,
        "rs(6,3) -> a full, length-9 placement vector"
    );

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
        .iter()
        .enumerate()
        .map(|(id, s)| (id as DServerId, s as &dyn ChunkStore))
        .collect();
    let ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(&zone, &custodian, None, Some(&ctx), None, None, 0)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "rs(6,3): the one corrupt referenced fragment (index 4) is detected among the 9"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![chunk],
        "exactly the rs(6,3) chunk is queued for reconstruction"
    );
    // The other 8 fragments verified intact, never excluded as false positives.
    for (index, server) in servers.iter().enumerate() {
        if index != 4 {
            assert!(
                server
                    .get_fragment(frag(chunk, index as u16))
                    .await
                    .unwrap()
                    .is_some(),
                "intact fragment {index} is untouched (scrub never deletes)"
            );
        }
    }
}
