//! M3.4 (issue #142, proposal 0005 slice 4, `0005:524-527`): the **GC custodian
//! loop**, driven through the real [`reconcile_step`] fenced control point.
//!
//! The BINDING legs of the success criterion, proven in-process over the trait
//! stores (Option A — no deployed custodian process exists yet, `0005:524-527`):
//!
//! 1. **Two-input reclaim** (`0005:288-291`): through `reconcile_step`, GC reclaims
//!    (a) the byte behind an **expired pending lease** and (b) an **orphaned**
//!    fragment (present in `list_fragments`, referenced by no committed chunk map),
//!    both via `ChunkStore::delete_fragment`.
//! 2. **Never reclaim a referenced fragment** (`0005:294-295`, Q3 `0005:394-397`):
//!    a fragment a committed chunk map references is never deleted — the
//!    silent-corruption invariant. Flippable: negate the reference check in
//!    `gc::reconcile` and this assertion fires.
//! 3. **Grace window honoured** (`0005:291-294`, Q3 `0005:397`): an orphan within
//!    its reader-safe window is not reclaimed and a reader holding the prior version
//!    still resolves it; it is reclaimed once the window elapses. Flippable: negate
//!    the window gate and the within-grace orphan is reclaimed early.
//! 4. **Durability-plane emission** (`0005:336-340`, ADR-0011/0012): GC actions are
//!    emitted on the `DurabilityTelemetry` seam as metric + audit events and read
//!    back in-process via `gather_prometheus`.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, PendingEntry,
};
use wyrd_custodian::{
    mark_orphaned, reconcile_step, Custodian, DurabilityTelemetry, ExporterConfig, FencedZone,
    GcContext, Reconciled,
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

/// One D server's fragment bytes — a deliberately dumb `ChunkStore`.
#[derive(Default)]
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

impl MemDServer {
    async fn put(&self, frag: FragmentId) {
        self.frags
            .lock()
            .unwrap()
            .insert(frag, Bytes::from_static(b"bytes"));
    }
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

/// Commit an inode whose single chunk's fragment at index 0 is placed on `dserver` —
/// a committed reference GC must never reclaim.
async fn commit_reference(
    meta: &MemMeta,
    inode: InodeId,
    name: &str,
    chunk: ChunkId,
    dserver: DServerId,
) {
    let record = InodeRecord {
        size: 5,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 5,
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
    let leader = Custodian::elect(coord, "zone-gc").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

// ---- criterion 1: two-input reclaim through the real control point ----

#[tokio::test]
async fn reclaims_expired_lease_byte_and_orphan_through_reconcile_step() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let d1 = MemDServer::default();

    // Input (a): an expired pending lease. Its fan-out byte landed on d0; the lease
    // expires at 100. Uncommitted, so referenced by no chunk map.
    let pending_chunk: ChunkId = 0xA1;
    d0.put(frag(pending_chunk, 0)).await;
    metadata::put_pending(
        &meta,
        pending_chunk,
        &PendingEntry {
            lease_expiry_millis: 100,
        },
    )
    .await
    .unwrap();

    // Input (b): an orphaned fragment on d1, stranded at t=0, referenced by nothing.
    let orphan_chunk: ChunkId = 0xB2;
    d1.put(frag(orphan_chunk, 0)).await;
    mark_orphaned(&meta, 1, frag(orphan_chunk, 0), 0)
        .await
        .unwrap();

    // A committed reference on d0 that GC must leave alone.
    let live_chunk: ChunkId = 0xC3;
    d0.put(frag(live_chunk, 0)).await;
    commit_reference(&meta, 1, "live", live_chunk, 0).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 2] = [(0, &d0), (1, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 50,
    };

    // now = 200: past the lease (100) and past the orphan window (0 + 50).
    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), 200)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed, "GC reclaimed fragment bytes");

    // (a) the expired-lease byte is gone; (b) the orphan byte is gone.
    assert!(
        d0.get_fragment(frag(pending_chunk, 0))
            .await
            .unwrap()
            .is_none(),
        "the byte behind the expired pending lease is reclaimed"
    );
    assert!(
        d1.get_fragment(frag(orphan_chunk, 0))
            .await
            .unwrap()
            .is_none(),
        "the orphaned fragment is reclaimed"
    );
    // The committed reference is untouched.
    assert!(
        d0.get_fragment(frag(live_chunk, 0))
            .await
            .unwrap()
            .is_some(),
        "a referenced fragment is never reclaimed"
    );
    // The pending-ledger entry was retired alongside its bytes.
    assert!(
        meta.get(&metadata::pending_key(pending_chunk))
            .await
            .unwrap()
            .is_none(),
        "the swept pending-ledger entry is removed"
    );
}

// ---- criterion 2: never reclaim a referenced fragment (the flippable invariant) ----

#[tokio::test]
async fn never_reclaims_a_referenced_fragment() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // A committed chunk map references fragment (chunk, 0) on d0.
    let chunk: ChunkId = 0xD4;
    d0.put(frag(chunk, 0)).await;
    commit_reference(&meta, 1, "obj", chunk, 0).await;

    // A STALE orphan grace record points at the very same fragment, long expired —
    // so the ONLY thing protecting the bytes is the reference check. Negating that
    // check in `gc::reconcile` deletes a referenced fragment and flips this red.
    mark_orphaned(&meta, 0, frag(chunk, 0), 0).await.unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 0,
    };

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), 1_000_000)
        .await
        .unwrap();

    assert_eq!(outcome, Reconciled::Satisfied, "nothing was reclaimable");
    assert!(
        d0.get_fragment(frag(chunk, 0)).await.unwrap().is_some(),
        "a fragment a committed chunk map references is NEVER passed to delete_fragment"
    );
}

// ---- criterion 3: grace window honoured (the flippable timing invariant) ----

#[tokio::test]
async fn honours_the_reader_safe_grace_window() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // An orphan stranded at t=100, with a reader-safe window of 50 → reclaimable at
    // t >= 150.
    let chunk: ChunkId = 0xE5;
    d0.put(frag(chunk, 0)).await;
    mark_orphaned(&meta, 0, frag(chunk, 0), 100).await.unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 50,
    };

    // WITHIN the window (now = 120 < 150): not reclaimed, and a reader holding the
    // prior version still resolves the fragment.
    let early = reconcile_step(&zone, &custodian, Some(&ctx), 120)
        .await
        .unwrap();
    assert_eq!(
        early,
        Reconciled::Satisfied,
        "nothing reclaimed within grace"
    );
    assert!(
        d0.get_fragment(frag(chunk, 0)).await.unwrap().is_some(),
        "an in-flight reader still resolves the fragment within the grace window"
    );

    // AFTER the window (now = 160 >= 150): reclaimed.
    let late = reconcile_step(&zone, &custodian, Some(&ctx), 160)
        .await
        .unwrap();
    assert_eq!(
        late,
        Reconciled::Changed,
        "reclaimed once the window elapsed"
    );
    assert!(
        d0.get_fragment(frag(chunk, 0)).await.unwrap().is_none(),
        "the orphan is reclaimed only after the reader-safe grace window"
    );
}

// ---- criterion 4: GC actions on the durability-plane seam, read back in-process ----

#[tokio::test]
async fn emits_gc_actions_on_the_durability_seam() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // One reclaim (an expired orphan) and one skip (a committed reference) so both
    // the reclaim and skip metrics are emitted.
    let orphan_chunk: ChunkId = 0xF6;
    d0.put(frag(orphan_chunk, 0)).await;
    mark_orphaned(&meta, 0, frag(orphan_chunk, 0), 0)
        .await
        .unwrap();

    let live_chunk: ChunkId = 0x17;
    d0.put(frag(live_chunk, 0)).await;
    commit_reference(&meta, 1, "live", live_chunk, 0).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 10,
    };

    // Wire the backend-agnostic durability seam (ADR-0012) and run GC under it.
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), 100)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed);

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert!(
        exposed.contains("gc_fragments_reclaimed"),
        "the GC reclaim metric is exported on the durability seam; got:\n{exposed}"
    );
    assert!(
        exposed.contains("gc_fragments_skipped"),
        "the GC skip metric is exported on the durability seam; got:\n{exposed}"
    );
}
