//! Regression for the gateway lease-expiry mutant the nightly cargo-mutants sweep
//! reported surviving (issue #225): `lib.rs:118` `+ -> *`/`-` in
//! `Gateway::put_object`, where the pending lease is stamped at `now + ttl`.
//!
//! The expiry is only observable when a PUT LOSES its commit (a winning PUT
//! releases the lease), so we pre-occupy the inode the create will allocate to
//! force a conflict, then read the leftover lease. The gateway reads the real wall
//! clock, so we pin the boundary by direction: `now + ttl` is in the FUTURE, while
//! `now - ttl` lands in the PAST — asserting the lease expires no earlier than the
//! instant the PUT started kills the mutant.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, PendingEntry};
use wyrd_server::Gateway;
use wyrd_traits::{
    ChunkStore, CommitOutcome, FragmentId, Health, MetadataStore, PlacementChunkStore, Result,
    WriteBatch,
};

#[derive(Clone, Default)]
struct MemMeta {
    kv: Arc<Mutex<HashMap<Vec<u8>, Bytes>>>,
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

#[derive(Clone, Default)]
struct MemChunks {
    frags: Arc<Mutex<HashMap<FragmentId, Bytes>>>,
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

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// `lib.rs:118` `+ -> -`/`*` — `put_object` stamps its pending lease at `now + ttl`,
/// a FUTURE instant. Force a losing PUT (pre-occupy the inode the create allocates)
/// and assert the leftover lease expires no earlier than the moment the PUT began;
/// `now - ttl` lands in the past and `now * ttl` overshoots wildly, so both fail.
#[tokio::test]
async fn losing_put_leaves_a_future_dated_lease() {
    let meta = MemMeta::default();
    let probe = meta.clone();
    let gw = Gateway::new(meta, MemChunks::default(), MemCoordination::new());

    // Inode 1 is the id the first create allocates; occupying it forces a conflict.
    probe
        .kv
        .lock()
        .unwrap()
        .insert(metadata::inode_key(1), Bytes::from_static(b"occupied"));

    let started = now_millis();
    let err = gw.put_object("obj", b"a losing put").await.unwrap_err();
    assert!(
        err.to_string().contains("concurrent writer"),
        "the pre-occupied inode makes the PUT lose: {err}"
    );

    let pending: Vec<Bytes> = probe
        .kv
        .lock()
        .unwrap()
        .iter()
        .filter(|(k, _)| k.starts_with(b"pending:"))
        .map(|(_, v)| v.clone())
        .collect();
    assert!(!pending.is_empty(), "the losing PUT left its lease behind");
    for value in pending {
        let entry: PendingEntry = metadata::decode(value.as_ref()).unwrap();
        let expiry = entry.lease_expiry_millis;
        // `now + ttl` lands just ahead of `started` (ttl is tens of seconds). `now - ttl`
        // lands in the past (fails the lower bound); `now * ttl` overshoots by millennia
        // (fails the upper bound). A 10-minute window comfortably brackets the real `+`.
        assert!(
            expiry >= started,
            "lease must expire in the future (now + ttl), not the past (now - ttl): {expiry} < {started}"
        );
        assert!(
            expiry <= started + 600_000,
            "lease must be ~now + ttl, not now * ttl: {expiry} >> {started}"
        );
    }
}
