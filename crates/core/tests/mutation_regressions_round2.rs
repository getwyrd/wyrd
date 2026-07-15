//! Round-2 regression tests for the wyrd-core mutants the nightly `cargo mutants`
//! sweep reported as surviving (issue #225): the write-plan accessor and the
//! lease-expiry arithmetic. Each test names the `file:line` mutant it kills.
//!
//! The lease-expiry value (`now + ttl`) is only observable when a write LOSES its
//! commit and so does not release its pending ledger entry; these tests stage that
//! loss deterministically by pre-occupying the target inode so `commit_create`
//! conflicts.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self, EcScheme, InodeId, PendingEntry};
use wyrd_core::placement::Topology;
use wyrd_core::write;
use wyrd_traits::{
    ChunkStore, CommitOutcome, FragmentId, Health, MetadataStore, PlacementChunkStore, Result,
    WriteBatch,
};

const ROOT: InodeId = 0;
const CHUNK: usize = 1 << 16;

// ---- in-memory trait stores (mirrors read_repair.rs) ----

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

/// Pre-occupy `inode_key(id)` so a subsequent `commit_create` for `id` conflicts.
async fn occupy_inode(meta: &MemMeta, id: InodeId) {
    meta.commit(WriteBatch::new().put(metadata::inode_key(id), Bytes::from_static(b"occupied")))
        .await
        .unwrap();
}

/// The single pending lease's expiry after a write left it behind.
async fn sole_pending_expiry(meta: &MemMeta) -> u64 {
    let pending = meta.scan(b"pending:").await.unwrap();
    assert_eq!(pending.len(), 1, "the losing write left exactly one lease");
    let entry: PendingEntry = metadata::decode(pending[0].1.as_ref()).unwrap();
    entry.lease_expiry_millis
}

// === write.rs:66 — WritePlan::chunk_ids returns the plan's minted ids ================

#[test]
fn write_plan_chunk_ids_are_the_minted_ids() {
    let mut next = 0x100u128;
    let plan = write::plan_write(b"one small chunk", CHUNK, EcScheme::None, || {
        next += 1;
        next
    })
    .unwrap();
    assert_eq!(
        plan.chunk_ids(),
        vec![0x101],
        "chunk_ids lists the plan's chunk ids, not an empty/default vec"
    );
}

// === write.rs:299 — write_new_object stamps the lease at `now + ttl` =================

#[tokio::test]
async fn write_new_object_lease_expiry_is_now_plus_ttl() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();
    occupy_inode(&meta, 1).await; // make the create lose so the lease persists

    let mut next = 0x42u128;
    let outcome = write::write_new_object(
        &meta,
        &chunks,
        ROOT,
        "obj",
        1,
        b"a losing write",
        CHUNK,
        EcScheme::ReedSolomon { k: 2, m: 1 },
        || 1_000, // now
        5_000,    // ttl
        || {
            next += 1;
            next
        },
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        CommitOutcome::Conflict,
        "the pre-occupied inode makes the create lose"
    );
    assert_eq!(
        sole_pending_expiry(&meta).await,
        1_000 + 5_000,
        "the lease expires at now + ttl, not now * ttl"
    );
}

// === write.rs:334 — write_new_object_placed stamps the lease at `now + ttl` ==========

#[tokio::test]
async fn write_new_object_placed_lease_expiry_is_now_plus_ttl() {
    let meta = MemMeta::default();
    let chunks = MemChunks::default();
    occupy_inode(&meta, 1).await;
    // RS(2,1) -> 3 fragments need 3 distinct failure domains.
    let mut topo = Topology::default();
    topo.register(0, "A").register(1, "B").register(2, "C");

    let mut next = 0x42u128;
    let outcome = write::write_new_object_placed(
        &meta,
        &chunks,
        ROOT,
        "obj",
        1,
        b"a losing placed write",
        CHUNK,
        EcScheme::ReedSolomon { k: 2, m: 1 },
        &topo,
        || 2_000, // now
        7_000,    // ttl
        || {
            next += 1;
            next
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Conflict);
    assert_eq!(
        sole_pending_expiry(&meta).await,
        2_000 + 7_000,
        "the placed write's lease also expires at now + ttl"
    );
}
