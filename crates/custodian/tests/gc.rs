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
//!    back in-process via `gather_prometheus`. This leg lives in its own test binary
//!    (`gc_telemetry.rs`) — it must not share this process's `tracing` callsite cache
//!    with the criteria above (issue #214).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, PendingEntry,
};
use wyrd_custodian::{mark_orphaned, reconcile_step, Custodian, FencedZone, GcContext, Reconciled};
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
    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 200)
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

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 1_000_000)
        .await
        .unwrap();

    assert_eq!(outcome, Reconciled::Satisfied, "nothing was reclaimable");
    assert!(
        d0.get_fragment(frag(chunk, 0)).await.unwrap().is_some(),
        "a fragment a committed chunk map references is NEVER passed to delete_fragment"
    );
}

// ---- criterion 4: identity-fallback placement protects committed fragments (issue #287) ----
//
// Three sub-cases:
//   4a: EcScheme::None  + placement: vec![]      — empty placement, index 0 falls back
//   4b: EcScheme::RS    + placement: vec![]      — empty placement, orphan at index > 0
//   4c: EcScheme::RS    + placement: vec![5]     — short vector, orphan at fallback index
//
// All three prove that GC's reference set equals the read path's resolved placement
// closure (`read.rs:fragment_dserver`, `read.rs:99-105`; `ChunkRef::placed_dserver`,
// `metadata.rs`), even for chunks whose `placement` vector is shorter than `n`.

/// **Regression for issue #287 — sub-case 4a: `EcScheme::None` + empty placement.**
///
/// A committed inode whose `ChunkRef` carries `placement: vec![]` (pre-M3 / mixed-era,
/// decoded with `#[serde(default)]`, `metadata.rs:93`) must have its identity-fallback
/// fragment (index 0 → D-server 0) included in GC's reference set.
///
/// Pre-fix: `referenced_fragments` iterated `placement.iter()` (empty) → reference
/// set was empty → stale orphan triggered `delete_fragment` → silent data loss.
/// Post-fix: `ChunkRef::placed_dserver` expands to `(0, FragmentId{index:0})` → GC
/// skips the fragment as referenced.
///
/// Flippable: revert `gc.rs:referenced_fragments` back to
/// `chunk.placement.iter().enumerate()` and this goes red.
#[tokio::test]
async fn identity_fallback_none_empty_placement_protects_index0() {
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // A committed inode: EcScheme::None, placement: vec![] (pre-M3 / mixed-era).
    // The read path resolves fragment 0 → D-server 0 via identity fallback
    // (`ChunkRef::placed_dserver`: `placement.get(0)` = None → `unwrap_or(0)`).
    let chunk: ChunkId = 0xF6_00;
    d0.put(frag(chunk, 0)).await;
    let record = InodeRecord {
        size: 5,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 5,
            placement: vec![], // pre-M3: empty placement, identity fallback applies
        }],
        state: InodeState::Committed,
        version: 1,
    };
    metadata::create(&meta, ROOT, "fallback-none-obj", 10, &record)
        .await
        .unwrap();

    // A stale orphan for the same fragment (D-server 0, index 0), grace expired.
    // Without the fix, the reference set is empty → orphan wins → delete_fragment.
    mark_orphaned(&meta, 0, frag(chunk, 0), 0).await.unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 0,
    };

    // now = 1_000_000: far past the orphan grace. Only protection is the identity-
    // fallback reference implied by the committed chunk map.
    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 1_000_000)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "4a: EcScheme::None + empty placement — committed fragment must not be reclaimed"
    );
    assert!(
        d0.get_fragment(frag(chunk, 0)).await.unwrap().is_some(),
        "4a: identity-fallback fragment of committed EcScheme::None chunk is NEVER deleted (issue #287)"
    );
}

/// **Regression for issue #287 — sub-case 4b: `EcScheme::ReedSolomon` + empty placement,
/// orphan at index > 0.**
///
/// An RS{k:2, m:1} chunk with `placement: vec![]` has 3 fragments (indices 0–2), all
/// resolved by identity fallback: index `i` → D-server `i`. GC must protect the
/// fragment at index 1 (D-server 1) even when a stale orphan record points at it.
///
/// This sub-case proves the scheme-aware fragment-count expansion (`fragment_count()`
/// returning `k + m = 3`) AND the fallback at an index beyond 0.
///
/// Flippable: revert `gc.rs:referenced_fragments` to `placement.iter().enumerate()`;
/// with `placement: vec![]` the loop yields nothing → (1, frag(chunk,1)) is not in the
/// reference set → the orphan triggers `delete_fragment` → this goes red.
#[tokio::test]
async fn identity_fallback_rs_empty_placement_protects_index_above_zero() {
    let meta = MemMeta::default();
    let d1 = MemDServer::default();

    // Committed inode: RS{k:2, m:1}, placement: vec![] (pre-M3).
    // Fragment 1 → D-server 1 via identity fallback.
    let chunk: ChunkId = 0xF6_01;
    d1.put(frag(chunk, 1)).await;
    let record = InodeRecord {
        size: 5,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
            len: 5,
            placement: vec![], // pre-M3: empty, identity fallback for all 3 indices
        }],
        state: InodeState::Committed,
        version: 1,
    };
    metadata::create(&meta, ROOT, "fallback-rs-obj", 20, &record)
        .await
        .unwrap();

    // Stale orphan at (D-server 1, index 1), grace expired.
    mark_orphaned(&meta, 1, frag(chunk, 1), 0).await.unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    // Fleet only contains D-server 1 — the one with the orphaned fragment.
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(1, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 0,
    };

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 1_000_000)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "4b: RS + empty placement — committed fragment at index > 0 must not be reclaimed"
    );
    assert!(
        d1.get_fragment(frag(chunk, 1)).await.unwrap().is_some(),
        "4b: identity-fallback RS fragment at index 1 is NEVER deleted (issue #287)"
    );
}

/// **Regression for issue #287 — sub-case 4c: `EcScheme::ReedSolomon` + short placement
/// vector, orphan at a fallback index.**
///
/// An RS{k:2, m:1} chunk with `placement: vec![5]` has index 0 explicit (D-server 5)
/// and indices 1–2 resolved by identity fallback (index 1 → D-server 1, index 2 →
/// D-server 2). GC must protect the fragment at index 2 (D-server 2) even when a stale
/// orphan record points at it and `placement[2]` does not exist.
///
/// This proves the mixed-explicit/fallback case: `.get(i).unwrap_or(i)` where some
/// indices are present and others are not.
///
/// Flippable: revert `gc.rs:referenced_fragments` to `placement.iter().enumerate()`;
/// with `placement: vec![5]` the loop yields only `(0, dserver=5)` → (2, frag(chunk,2))
/// is absent → the orphan triggers `delete_fragment` → this goes red.
#[tokio::test]
async fn short_placement_vector_fallback_protects_fallback_index() {
    let meta = MemMeta::default();
    let d2 = MemDServer::default();

    // Committed inode: RS{k:2, m:1}, placement: vec![5] (short — only index 0 explicit).
    // Index 2 → D-server 2 via identity fallback.
    let chunk: ChunkId = 0xF6_02;
    d2.put(frag(chunk, 2)).await;
    let record = InodeRecord {
        size: 5,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
            len: 5,
            // Short vector: index 0 → D-server 5 (explicit), indices 1 and 2 fall back.
            placement: vec![5],
        }],
        state: InodeState::Committed,
        version: 1,
    };
    metadata::create(&meta, ROOT, "short-placement-obj", 30, &record)
        .await
        .unwrap();

    // Stale orphan at (D-server 2, index 2), grace expired.
    mark_orphaned(&meta, 2, frag(chunk, 2), 0).await.unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    // Fleet only contains D-server 2 — the one with the orphaned fragment.
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(2, &d2)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 0,
    };

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 1_000_000)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "4c: RS + short placement — committed fragment at fallback index 2 must not be reclaimed"
    );
    assert!(
        d2.get_fragment(frag(chunk, 2)).await.unwrap().is_some(),
        "4c: short-placement RS fragment at index 2 is NEVER deleted (issue #287)"
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
    let early = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 120)
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
    let late = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 160)
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
