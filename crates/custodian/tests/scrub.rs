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

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-scrub").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

// ---- criterion 1: walk + verify referenced fragments through the control point ----

#[tokio::test]
async fn walks_and_verifies_referenced_fragments_through_reconcile_step() {
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
