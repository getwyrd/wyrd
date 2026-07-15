//! M3.4 criterion 4 (issue #142, proposal 0005, `0005:336-340`, ADR-0011/0012):
//! **durability-plane emission** — GC actions are emitted on the
//! [`DurabilityTelemetry`] seam as metric + audit events and read back in-process
//! via `gather_prometheus`.
//!
//! This leg lives in its **own** test binary, deliberately apart from the other GC
//! criteria in `gc.rs`. The GC metrics are `tracing::info!(monotonic_counter.…)`
//! callsites ([`gc.rs`] `emit_reclaim`/`emit_skip`) bridged to OpenTelemetry, and
//! `tracing` caches per-callsite *interest* in **process-global** state. The other
//! GC tests exercise the same callsites under a no-op subscriber (they install
//! none); run in the same process, they race this test on callsite registration and
//! can cache a callsite as disabled, silently dropping a counter here (issue #214).
//! A separate test binary is a separate process, so its callsite cache is its own.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_custodian::{
    mark_orphaned, reconcile_step, Custodian, DurabilityTelemetry, ExpiredPendingPolicy,
    ExporterConfig, FencedZone, GcContext, Reconciled,
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

// ---- criterion 4: GC actions on the durability-plane seam, read back in-process ----

/// Install a permissive global `tracing` default **once** so the durability metric
/// callsites never latch `Interest::never` under the parallel test harness. `tracing`
/// caches each callsite's interest in a process-global table the first time it is hit;
/// a first hit racing a no-subscriber default can latch the callsite disabled, after
/// which the test that reads the metric back (`gather_prometheus`) silently sees it
/// missing (the flaky read-back the C4 gate caught, iteration-4). Registering against an
/// always-enabling default before any callsite fires makes every first-registration
/// agree; each test's own `.with_subscriber(...)` still routes its metrics into that
/// test's provider. Called at the top of every metric-touching test so whichever runs
/// first sets the default before any callsite fires (mirrors `scrub.rs:208`).
fn enable_metric_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

#[tokio::test]
async fn emits_gc_actions_on_the_durability_seam() {
    enable_metric_callsites();
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
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    // Wire the backend-agnostic durability seam (ADR-0012) and run GC under it.
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 100)
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
