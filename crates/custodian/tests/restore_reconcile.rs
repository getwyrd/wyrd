//! **Post-restore reconciliation** (#551) — the guard on the pass that makes a metadata
//! restore survivable.
//!
//! The hazard these tests pin is the one the FoundationDB backup drill (#546) surfaced: a
//! restore moves the **metadata** back to version *V* while the **fragments** stay at "now",
//! and `gc` — correctly — refuses to reclaim a fragment without *evidence* that a grace
//! deadline elapsed (an `orphan:` record or an expired `pending:` lease). Both records live
//! in the metadata the restore just rewound, so post-restore strays are unreferenced AND
//! evidence-free: GC keeps them forever and the space leaks.
//!
//! The pass supplies the missing evidence. Because that evidence is the front half of a
//! **deletion**, the tests that matter most here are the ones that prove what it does NOT
//! touch — a marked live fragment is silent corruption on a delay.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, PendingEntry,
};
use wyrd_custodian::{
    mark_orphaned, reconcile_after_restore, reconcile_step, Custodian, ExpiredPendingPolicy,
    FencedZone, GcContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    WriteBatch,
};

// ---- in-memory trait stores (the loop is proven over the seams, not a backend) ----

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

const NOW: u64 = 10_000;

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// Commit an inode holding one RS(k,m) chunk placed across `placement`.
async fn commit_chunk(
    meta: &MemMeta,
    inode: InodeId,
    chunk: ChunkId,
    k: u8,
    m: u8,
    placement: Vec<DServerId>,
) {
    let record = InodeRecord {
        size: 5,
        version: 1,
        state: InodeState::Committed,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::ReedSolomon { k, m },
            len: 5,
            placement,
        }],
        ..Default::default()
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(inode), metadata::encode(&record)))
        .await
        .unwrap();
}

/// Commit an inode holding one **single-fragment** (`EcScheme::None`) chunk on `dserver`.
/// No redundancy: the lone fragment IS the data, and `k` is 1.
async fn commit_single_chunk(meta: &MemMeta, inode: InodeId, chunk: ChunkId, dserver: DServerId) {
    let record = InodeRecord {
        size: 5,
        version: 1,
        state: InodeState::Committed,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 5,
            placement: vec![dserver],
        }],
        ..Default::default()
    };
    meta.commit(WriteBatch::new().put(metadata::inode_key(inode), metadata::encode(&record)))
        .await
        .unwrap();
}

/// The fragments of `chunk` as placed on `placement` (index i → placement[i]).
fn placed(chunk: ChunkId, placement: &[DServerId]) -> Vec<(DServerId, FragmentId)> {
    placement
        .iter()
        .enumerate()
        .map(|(i, &d)| (d, frag(chunk, i as u16)))
        .collect()
}

// ---- the leak this pass closes ----

/// THE BUG (#551). A file created after the restore point loses its chunk map, and its
/// `orphan:`/`pending:` records went with it — so GC's evidence rule keeps its fragments
/// FOREVER. The pass marks them, which is the only thing that lets GC ever reclaim them.
#[tokio::test]
async fn a_stranded_fragment_is_marked_so_gc_can_finally_reclaim_it() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // A fragment on disk that no committed chunk map references, and that carries no grace
    // record of any kind: exactly what a restore leaves behind.
    let stray = frag(7, 0);
    d0.put(stray).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.stranded_marked, 1,
        "an unreferenced, evidence-free fragment must be marked — without the orphan record \
         GC's conservative fallthrough keeps it forever and the space leaks: {report:?}"
    );
    // The evidence GC requires now exists, stamped at the pass's logical time.
    let key = metadata::orphan_key(0, stray);
    assert_eq!(
        meta.get(&key).await.unwrap().as_deref(),
        Some(NOW.to_string().as_bytes()),
        "the pass must write the orphan grace record GC reads"
    );
    // ...and the bytes are still THERE. Marking is not deleting.
    assert!(
        d0.get_fragment(stray).await.unwrap().is_some(),
        "the pass marks; it must never delete — GC reclaims later, after the grace window"
    );
}

/// The safety gate, and the reason this pass is not simply "mark everything unreferenced":
/// a fragment a COMMITTED chunk map points at is live data. Marking it would hand live bytes
/// to GC — silent corruption on a grace-window delay.
#[tokio::test]
async fn a_referenced_fragment_is_never_marked() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let d1 = MemDServer::default();

    // A live 1+1 chunk, both fragments placed and present.
    commit_chunk(&meta, 1, 42, 1, 1, vec![0, 1]).await;
    d0.put(frag(42, 0)).await;
    d1.put(frag(42, 1)).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.stranded_marked, 0,
        "live, referenced fragments must NOT be marked: {report:?}"
    );
    for (d, f) in placed(42, &[0, 1]) {
        assert!(
            meta.get(&metadata::orphan_key(d, f))
                .await
                .unwrap()
                .is_none(),
            "no orphan record may exist for a referenced fragment — that is GC's \
             never-reclaim-live-data invariant, breached one step earlier"
        );
    }
}

/// Idempotence, and specifically that re-running does not RESET a grace clock. A fragment
/// marked an hour ago is an hour closer to reclamation; re-stamping it with `now` would push
/// it back and quietly delay every reclamation the operator is waiting on.
#[tokio::test]
async fn re_running_does_not_reset_an_existing_grace_clock() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let stray = frag(9, 0);
    d0.put(stray).await;

    // Already marked, long ago.
    mark_orphaned(&meta, 0, stray, 1).await.unwrap();

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(report.already_marked, 1, "{report:?}");
    assert_eq!(report.stranded_marked, 0, "{report:?}");
    assert_eq!(
        meta.get(&metadata::orphan_key(0, stray))
            .await
            .unwrap()
            .as_deref(),
        Some(b"1".as_slice()),
        "the ORIGINAL timestamp must survive: re-stamping it with `now` would reset the grace \
         window and delay the reclamation this pass exists to enable"
    );
}

/// An in-flight write's fragments are not strays: the pending lease is already their grace,
/// and GC sweeps them when it expires. Marking them would give the same bytes two deadlines.
#[tokio::test]
async fn an_in_flight_pending_chunk_is_left_to_gc() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let inflight = frag(11, 0);
    d0.put(inflight).await;

    let entry = PendingEntry {
        lease_expiry_millis: NOW + 60_000,
    };
    meta.commit(WriteBatch::new().put(metadata::pending_key(11), metadata::encode(&entry)))
        .await
        .unwrap();

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(report.pending_skipped, 1, "{report:?}");
    assert_eq!(
        report.stranded_marked, 0,
        "a chunk holding a live pending lease belongs to GC's lease path, not to this pass: \
         {report:?}"
    );
}

// ---- the loss this pass reports ----

/// The mirror hazard: a file DELETED after the restore point comes back — its map
/// resurrected, its bytes already reclaimed. Below `k` surviving fragments it is unreadable
/// AND unreconstructible. Nothing detected this before; an operator met it as a failed read.
#[tokio::test]
async fn a_chunk_below_k_surviving_fragments_is_reported_dangling() {
    let meta = MemMeta::default();
    let (d0, d1, d2) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );

    // RS(2,1): k=2 of n=3. The restore resurrected the map; GC had already reclaimed two of
    // the three fragments, so only ONE survives — below k.
    commit_chunk(&meta, 1, 77, 2, 1, vec![0, 1, 2]).await;
    d0.put(frag(77, 0)).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1), (2, &d2)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.dangling,
        vec![77],
        "1 surviving fragment of RS(2,1) is below k=2: the file is back in the namespace, \
         unreadable, and cannot be rebuilt — it must be reported, not discovered: {report:?}"
    );
    assert!(
        report.under_replicated.is_empty(),
        "a chunk below k is LOST, not merely under-replicated — calling it under-replicated \
         would tell the repair loop to rebuild what cannot be rebuilt: {report:?}"
    );
}

/// ...and the distinction that keeps the report honest: still at or above `k` is
/// **survivable**. The reconstruction loop rebuilds it; it is not a casualty of the restore.
#[tokio::test]
async fn a_chunk_still_at_k_is_under_replicated_not_dangling() {
    let meta = MemMeta::default();
    let (d0, d1, d2) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );

    // RS(2,1) with TWO of three fragments surviving — exactly k. Rebuildable.
    commit_chunk(&meta, 1, 88, 2, 1, vec![0, 1, 2]).await;
    d0.put(frag(88, 0)).await;
    d1.put(frag(88, 1)).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1), (2, &d2)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert!(
        report.dangling.is_empty(),
        "k surviving fragments is RECOVERABLE — reporting it as lost would overstate the \
         restore's cost and could send an operator hunting for data that is still there: \
         {report:?}"
    );
    assert_eq!(report.under_replicated, vec![88], "{report:?}");
}

// ---- end to end: the leak, and the fix, through the REAL GC ----

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-restore").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

/// THE COUNTERFACTUAL, and the reason this issue exists. Without the pass, a post-restore
/// stray survives GC **forever** — not for a grace window, forever — because GC has no
/// evidence a deadline ever started. Run GC a year later and the bytes are still there.
///
/// If this test ever goes green-by-accident (i.e. GC starts reclaiming evidence-free
/// fragments on its own), the pass is no longer load-bearing — but so is GC's reader-safety
/// invariant, and that is a much bigger problem.
#[tokio::test]
async fn without_the_pass_a_stranded_fragment_leaks_forever() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let stray = frag(21, 0);
    d0.put(stray).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let gc = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let coord = MemCoordination::new();
    let (zone, leader) = elect(&coord).await;

    // A year later. No pass was run.
    let far_future = NOW + 365 * 24 * 60 * 60 * 1_000;
    reconcile_step(&zone, &leader, Some(&gc), None, None, None, far_future)
        .await
        .unwrap();

    assert!(
        d0.get_fragment(stray).await.unwrap().is_some(),
        "THE BUG (#551): GC reclaims only on EVIDENCE of an elapsed grace deadline (an \
         `orphan:` record or an expired `pending:` lease), and the restore erased both along \
         with the chunk map. So this fragment is unreferenced, evidence-free, and kept — a \
         year on, and forever. The space leaks with no mechanism to reclaim it."
    );
}

/// ...and the fix, end to end through the real fenced control point: run the pass, let the
/// grace window elapse, and GC — unchanged — reclaims the bytes it previously could not touch.
#[tokio::test]
async fn after_the_pass_gc_reclaims_the_stranded_fragment() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let stray = frag(21, 0);
    d0.put(stray).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let gc = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    // The operator runs the pass after the restore. It MARKS; it deletes nothing.
    let report = reconcile_after_restore(&gc, NOW).await.unwrap();
    assert_eq!(report.stranded_marked, 1, "{report:?}");
    assert!(
        d0.get_fragment(stray).await.unwrap().is_some(),
        "the pass itself must not delete — the grace window has not elapsed yet"
    );

    let coord = MemCoordination::new();
    let (zone, leader) = elect(&coord).await;

    // Still inside the grace window: GC must STILL decline. The reader-safety window is not
    // shortened by the pass — it is started by it.
    reconcile_step(&zone, &leader, Some(&gc), None, None, None, NOW + 500)
        .await
        .unwrap();
    assert!(
        d0.get_fragment(stray).await.unwrap().is_some(),
        "within the grace window GC must not reclaim: the pass supplies a DEADLINE, not a \
         licence to delete immediately"
    );

    // Past the grace window: the bytes GC could never touch are finally reclaimed.
    reconcile_step(&zone, &leader, Some(&gc), None, None, None, NOW + 2_000)
        .await
        .unwrap();
    assert!(
        d0.get_fragment(stray).await.unwrap().is_none(),
        "after the grace window the marked stray must be reclaimed — this is the leak (#551) \
         actually closing, through the UNCHANGED GC loop"
    );
}

/// Bounded batching must not lose the TAIL. Marks are committed every `MARK_BATCH` (1000) and
/// the remainder in a final commit — the classic off-by-one is dropping that remainder, which
/// would silently leave the last partial batch of fragments stranded forever, i.e. reintroduce
/// exactly the leak this pass closes, but only for large restores.
#[tokio::test]
async fn every_stray_is_marked_across_batch_boundaries_including_the_tail() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // 1001 strays: one full 1000-mark batch, plus a 1-fragment tail.
    let n: u16 = 1001;
    for i in 0..n {
        d0.put(frag(500, i)).await;
    }

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();
    assert_eq!(
        report.stranded_marked,
        usize::from(n),
        "every stray must be marked, across the batch boundary: {report:?}"
    );

    // The evidence must exist for ALL of them — the tail included.
    for i in 0..n {
        assert!(
            meta.get(&metadata::orphan_key(0, frag(500, i)))
                .await
                .unwrap()
                .is_some(),
            "fragment {i} has no orphan record — a dropped final batch leaves it stranded \
             forever, which is the very leak this pass exists to close"
        );
    }
}

// ---- the displaced-fragment trap: a repair that landed AFTER the restore point ----

/// THE DATA-LOSS BUG codex caught in review. A repair or rebalance after version V moves a
/// fragment: it writes the bytes to a NEW D server and repoints `placement[index]` at it
/// (`reconstruction.rs` / `rebalance.rs`: `new_placement[index] = target`). Restoring to V
/// rewinds the MAP to the old server — while the BYTES sit on the new one.
///
/// The naive `(dserver, fragment)` check then calls those bytes unreferenced and marks them.
/// GC deletes them. They were the ONLY surviving copy of a fragment the map still needs. That
/// is not a leak; it is destroying live data, and it is the one outcome this pass must never
/// produce.
#[tokio::test]
async fn the_only_copy_of_a_moved_fragment_is_never_marked() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let d1 = MemDServer::default();

    // The RESTORED map: RS(1,1), fragment 0 placed on d0, fragment 1 on d1.
    commit_chunk(&meta, 1, 55, 1, 1, vec![0, 1]).await;
    // The bytes, as they ACTUALLY are: a repair after the restore point moved fragment 0 from
    // d0 to d1 and repointed the map — then the restore rewound the map. d0 no longer has it.
    d1.put(frag(55, 0)).await; // moved here
    d1.put(frag(55, 1)).await; // always here

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.stranded_marked, 0,
        "the moved fragment is the ONLY copy of a fragment the map still needs — marking it \
         hands the last surviving bytes to GC: {report:?}"
    );
    assert_eq!(
        report.displaced_kept, 1,
        "it must be recognised as DISPLACED (bytes moved, map rewound) and kept: {report:?}"
    );
    assert!(
        meta.get(&metadata::orphan_key(1, frag(55, 0)))
            .await
            .unwrap()
            .is_none(),
        "no orphan record may exist for the last copy of a referenced fragment — GC would \
         reclaim it after the grace window and the data would be GONE"
    );

    // ...and the chunk must NOT be reported lost. The bytes are all there; only the map is stale.
    assert!(
        report.dangling.is_empty(),
        "every fragment's bytes exist (fragment 0 merely sits on d1 rather than d0) — reporting \
         this chunk as DANGLING would tell an operator their recoverable data is gone: {report:?}"
    );
    // It IS still degraded, though, and the report must say which kind. Fragment 1 sits at its
    // placement, and k=1, so the chunk still READS — the repair loop rebuilds fragment 0 back
    // onto d0 from it. That is under-replication, not an operator's problem.
    assert_eq!(
        report.under_replicated,
        vec![55],
        "fragment 0 is unreachable at its placement, so the chunk is degraded — silently calling \
         it healthy would hide a chunk running without redundancy: {report:?}"
    );
    assert!(
        report.misplaced.is_empty(),
        "k=1 fragment still reads at its placement, so this needs no human: {report:?}"
    );
}

/// The other half of the same rule: when the map's server DOES still hold the fragment, a copy
/// elsewhere is a genuine stale duplicate — the leftover of a completed move whose `orphan:`
/// record the restore erased. That one SHOULD be marked; refusing to would just leak.
#[tokio::test]
async fn a_stale_duplicate_is_marked_when_the_canonical_copy_survives() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let d1 = MemDServer::default();

    // The map places fragment 0 on d0 — and d0 HAS it.
    commit_chunk(&meta, 1, 66, 1, 1, vec![0, 1]).await;
    d0.put(frag(66, 0)).await; // canonical, present
    d1.put(frag(66, 1)).await;
    // A leftover copy of fragment 0 on d1, from a move that was later rolled back by the
    // restore. Its orphan record went with the metadata, so GC would keep it forever.
    d1.put(frag(66, 0)).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.stranded_marked, 1,
        "the canonical copy is alive on d0, so the copy on d1 is a stale duplicate and marking \
         it is exactly right — this is the leak the pass exists to close: {report:?}"
    );
    assert_eq!(report.displaced_kept, 0, "{report:?}");
    // The canonical copy is untouched.
    assert!(
        meta.get(&metadata::orphan_key(0, frag(66, 0)))
            .await
            .unwrap()
            .is_none(),
        "the canonical copy must never be marked"
    );
}

// ---- ...and the MIRROR of that trap: displaced bytes are not READABLE bytes ----

/// THE HOLLOW GREEN codex caught on the second pass — the exact mistake the fix above invites.
/// Having learned "don't call displaced bytes lost", the tempting next step is to count a
/// fragment as available wherever it sits. That is just as wrong, in the opposite direction.
///
/// NOTHING SCANS THE FLEET. The read path fetches `get_fragment_at(fragment_dserver(chunk, i))`
/// (`wyrd_core::read`), and reconstruction's `assess` walks `placement` and does
/// `stores.get(&dserver)` (`crate::reconstruction`) — a fragment found anywhere else is counted
/// MISSING by both. So a displaced fragment is unreadable AND unusable by the repair loop.
///
/// This is the sharpest case, and it is codex's own: a single-fragment chunk placed on d0, its
/// bytes sitting only on d1. Counting "present anywhere" makes it look PERFECT — `available == k
/// == n`, no dangling, not even under-replicated — so the command exits 0 and the restore script
/// records a clean run, while every read of that file fails.
#[tokio::test]
async fn a_single_fragment_chunk_whose_bytes_moved_is_reported_misplaced_not_healthy() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let d1 = MemDServer::default();

    // The RESTORED map places the lone fragment on d0...
    commit_single_chunk(&meta, 1, 99, 0).await;
    // ...but the bytes are on d1 (a rebalance moved them after the restore point).
    d1.put(frag(99, 0)).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.misplaced,
        vec![99],
        "the read path resolves this fragment strictly through the placement (d0), which does \
         NOT hold it — the chunk is UNREADABLE. Counting it available because the bytes exist \
         somewhere reports a broken chunk as healthy and exits 0 over a file that 404s: {report:?}"
    );
    assert!(
        report.dangling.is_empty(),
        "...and yet it is NOT lost: the bytes are one hop away on d1. Reporting it as DANGLING \
         would send an operator to a backup they do not need: {report:?}"
    );
    assert!(
        !report.is_clean(),
        "a chunk nobody can read must never let this command report a clean restore — the CLI \
         exits on `is_clean`, and a hollow green is the worst thing it can print: {report:?}"
    );
    // ...and the displaced bytes are still kept, never marked: they are the only copy.
    assert_eq!(report.displaced_kept, 1, "{report:?}");
    assert_eq!(report.stranded_marked, 0, "{report:?}");
}

/// The same lie, one step subtler: enough fragments exist across the fleet to clear `k`, but not
/// enough sit where the map looks. The repair loop cannot save this chunk — `assess` fetches by
/// placement, so it sees fewer than `k` survivors and cannot rebuild — which makes
/// "under-replicated" (i.e. *the repair loop will handle it*) an actively misleading verdict.
#[tokio::test]
async fn a_chunk_below_k_at_its_placement_is_misplaced_not_under_replicated() {
    let meta = MemMeta::default();
    let (d0, d1, d2) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );

    // RS(2,1): k=2 of n=3, placed 0→d0, 1→d1, 2→d2.
    commit_chunk(&meta, 1, 111, 2, 1, vec![0, 1, 2]).await;
    d1.put(frag(111, 1)).await; // canonical, present
    d2.put(frag(111, 0)).await; // DISPLACED: the map says d0, the bytes are on d2
                                // fragment 2 is genuinely gone.

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1), (2, &d2)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.misplaced,
        vec![111],
        "only ONE of the two fragments needed (k=2) sits at its placement — the other is on d2, \
         where neither a read nor `assess` will ever look. The chunk cannot be read and cannot be \
         rebuilt: {report:?}"
    );
    assert!(
        report.under_replicated.is_empty(),
        "calling this under-replicated says 'the repair loop rebuilds it' — and the repair loop \
         CANNOT: it fetches by placement, sees 1 survivor against k=2, and gives up. The operator \
         would wait for a repair that is never coming: {report:?}"
    );
    assert!(
        report.dangling.is_empty(),
        "and it is still not LOST — 2 fragments' bytes exist, which is k. The placement is what \
         is broken, not the data: {report:?}"
    );
}

/// The boundary that keeps the new state honest: a displaced fragment must NOT be escalated to
/// `misplaced` when the chunk is still readable without it. Here `k` fragments remain at their
/// placement, so reads succeed and the repair loop genuinely does rebuild the rest — from the
/// fragments it can actually fetch. Under-replicated, exactly as before.
#[tokio::test]
async fn a_displaced_fragment_is_only_under_replicated_while_k_survive_at_the_placement() {
    let meta = MemMeta::default();
    let (d0, d1, d2) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );

    // RS(2,1): k=2 of n=3. Fragments 1 and 2 sit at their placement — that is k, so the chunk
    // reads and rebuilds. Fragment 0's bytes were moved to d2 by a post-restore repair.
    commit_chunk(&meta, 1, 122, 2, 1, vec![0, 1, 2]).await;
    d1.put(frag(122, 1)).await;
    d2.put(frag(122, 2)).await;
    d2.put(frag(122, 0)).await; // displaced, and not needed to read

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0), (1, &d1), (2, &d2)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 1_000,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let report = reconcile_after_restore(&ctx, NOW).await.unwrap();

    assert_eq!(
        report.under_replicated,
        vec![122],
        "k=2 fragments are readable at their placement, so this chunk is fine and the repair loop \
         rebuilds fragment 0 from them — escalating it to MISPLACED would cry wolf over a chunk \
         that needs no operator at all: {report:?}"
    );
    assert!(report.misplaced.is_empty(), "{report:?}");
    assert!(report.dangling.is_empty(), "{report:?}");
    // The displaced copy is still the only copy of fragment 0, so it is still never marked.
    assert_eq!(report.displaced_kept, 1, "{report:?}");
    assert_eq!(report.stranded_marked, 0, "{report:?}");
}
