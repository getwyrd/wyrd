//! The **delete/overwrite → GC reclaim** contract (issue #364): the S3 DELETE and PUT-overwrite
//! paths do **not** reclaim superseded fragment bytes eagerly — an eager reclaim would tear a
//! concurrent streaming reader still holding the prior chunk map. Instead each writes an
//! **orphan grace record** for every superseded fragment — keyed by the D server the chunk map
//! **placed** it on — in the *same atomic commit* that unbinds/overwrites the object, and the
//! custodian **GC** loop (`crates/custodian/src/gc.rs`) reclaims the bytes once the reader-safe
//! grace window elapses. This is both the reader-safe discipline (proposal 0005, `0005:288-295`)
//! *and* a crash-proof backstop: the record is durable the instant the object becomes
//! unreferenced, so a crash never strands the bytes forever.
//!
//! These tests drive the real production paths end to end: `metadata::unlink` (the delete's
//! metadata commit), `metadata::commit_chunk_map_superseding` (the overwrite's), and
//! `reconcile_step` (the fenced GC control point).
//!
//! Pre-fix (the commit writes no orphan record): after it the fragment is unreferenced but
//! carries no orphan/pending deadline, so GC conservatively **keeps** it (`gc::reconcile`) —
//! the permanent leak. These tests then find the fragment still present → RED.
//! Post-fix: the orphan record is durable the instant the object is unbound/overwritten → GC
//! reclaims it after the grace window → the fragment is gone → GREEN.
//!
//! The chunk is placed on a **non-identity** D server (index 0 → D-server 1), so the tests also
//! prove the orphan record is keyed by the **placed** D server, not the fragment index: a
//! record keyed by the index would miss the fleet slot GC sweeps and never reclaim.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_custodian::{
    reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone, GcContext, Reconciled,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    WriteBatch,
};

const ROOT: InodeId = 0;

/// A trivial in-memory metadata store (the same seam GC and `unlink` run over).
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

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-gc-delete").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

#[tokio::test]
async fn delete_orphans_fragment_reclaimed_by_gc_from_the_placed_dserver() {
    let meta = MemMeta::default();
    let d1 = MemDServer::default();

    // A committed object under ROOT/"obj": one chunk whose single fragment (index 0) is
    // PLACED on D-server 1 (non-identity placement), so the fragment lives on d1.
    let placed_dserver: DServerId = 1;
    let chunk: ChunkId = 0xDE_1E;
    let frag = FragmentId { chunk, index: 0 };
    d1.put_fragment(frag, Bytes::from_static(b"object bytes"))
        .await
        .unwrap();
    let record = InodeRecord {
        size: 11,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 11,
            placement: vec![placed_dserver],
        }],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let created = metadata::create(&meta, ROOT, "obj", 1, &record)
        .await
        .unwrap();
    assert_eq!(created, CommitOutcome::Committed);

    // Delete's metadata commit at t=0 unbinds the object and orphans its fragment. The delete
    // path deliberately does NOT reclaim eagerly (a concurrent reader may still hold the chunk
    // map), so the fragment is still on d1 immediately after — reader-safe, not leaked.
    let unlinked = metadata::unlink(&meta, ROOT, "obj", 0)
        .await
        .unwrap()
        .expect("the bound name is removed");
    assert_eq!(unlinked.outcome, CommitOutcome::Committed);
    assert!(
        d1.get_fragment(frag).await.unwrap().is_some(),
        "the deleted object's fragment survives the reader-safe grace window (no eager reclaim)"
    );

    // The custodian GC now runs. With grace_window 50 and now=200 the orphan (stranded at
    // t=0) is past its window. The object is referenced by no committed chunk map, so the
    // ONLY thing that lets GC reclaim it is the orphan grace record `unlink` wrote — keyed by
    // the PLACED D-server (1), which is the fleet slot GC sweeps.
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(placed_dserver, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 50,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 200)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "GC must reclaim the deleted object's fragment via its orphan grace record"
    );
    assert!(
        d1.get_fragment(frag).await.unwrap().is_none(),
        "the fragment a DELETE orphaned is reclaimed by GC after the grace window (no leak)"
    );
}

/// A PUT that **overwrites** an existing key must not leak the prior object's fragments — the
/// same reclaim discipline as DELETE, on the more common verb (issue #364, iter-5 BLOCKING 1:
/// "PUT overwrite leaks the prior object's fragments permanently"). `commit_chunk_map_superseding`
/// (the function `Gateway::commit_written` routes an overwrite through) must orphan every prior
/// fragment in the *same atomic commit* that swaps the chunk map, so GC reclaims the superseded
/// bytes after the grace window while the **current** object's fragments — still referenced —
/// are NEVER touched.
///
/// Pre-fix (overwrite CAS-swaps the map but writes no orphan record): the prior fragment is
/// unreferenced yet un-deadlined, so GC keeps it forever → the leak → RED. Post-fix: the
/// overwrite's orphan record lets GC reclaim exactly the prior fragment → GREEN.
#[tokio::test]
async fn overwrite_orphans_prior_fragments_reclaimed_by_gc_but_keeps_the_current() {
    use wyrd_core::metadata::commit_chunk_map_superseding;

    let meta = MemMeta::default();
    let d1 = MemDServer::default();
    let placed_dserver: DServerId = 1;

    // Version 1: one chunk, its fragment placed on d1.
    let old_chunk: ChunkId = 0x0_1D_01;
    let old_frag = FragmentId {
        chunk: old_chunk,
        index: 0,
    };
    d1.put_fragment(old_frag, Bytes::from_static(b"old object bytes"))
        .await
        .unwrap();
    let v1 = InodeRecord {
        size: 16,
        chunk_map: vec![ChunkRef {
            id: old_chunk,
            scheme: EcScheme::None,
            len: 16,
            placement: vec![placed_dserver],
        }],
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    assert_eq!(
        metadata::create(&meta, ROOT, "obj", 1, &v1).await.unwrap(),
        CommitOutcome::Committed
    );

    // The overwrite (version 2): a fresh chunk id + fragment, placed on d1. This is the exact
    // production commit `Gateway::commit_written` runs for an overwrite. Orphan stamp t=0.
    let new_chunk: ChunkId = 0x0_2E_02;
    let new_frag = FragmentId {
        chunk: new_chunk,
        index: 0,
    };
    d1.put_fragment(new_frag, Bytes::from_static(b"new object bytes"))
        .await
        .unwrap();
    let new_map = vec![ChunkRef {
        id: new_chunk,
        scheme: EcScheme::None,
        len: 16,
        placement: vec![placed_dserver],
    }];
    let outcome = commit_chunk_map_superseding(
        &meta,
        1,
        &v1,
        new_map,
        16,
        0,
        &wyrd_core::metadata::ObjectMeta::default(),
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    // Immediately after the overwrite both fragments are on disk: the prior one is orphaned
    // (awaiting the grace window, reader-safe), the new one is the live object.
    assert!(d1.get_fragment(old_frag).await.unwrap().is_some());
    assert!(d1.get_fragment(new_frag).await.unwrap().is_some());

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(placed_dserver, &d1)];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: 50,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, 200)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "GC must reclaim the overwrite's superseded fragment via its orphan grace record"
    );

    // The prior fragment is reclaimed (no overwrite leak); the CURRENT object's fragment is
    // still referenced by the committed chunk map, so GC's safety gate NEVER reclaims it.
    assert!(
        d1.get_fragment(old_frag).await.unwrap().is_none(),
        "the overwritten object's prior fragment is reclaimed by GC (no permanent leak)"
    );
    assert!(
        d1.get_fragment(new_frag).await.unwrap().is_some(),
        "the current object's fragment is referenced and must NEVER be reclaimed"
    );
}
