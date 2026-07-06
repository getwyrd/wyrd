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

    let finished = now_millis();
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
        // The lease is stamped `internal_now + ttl` where the gateway's `ttl` is tens of
        // seconds. The two mutants this kills: `+ -> -` gives `internal_now - ttl` (the
        // PAST, ~30s BELOW `started`); `+ -> *` gives `internal_now * ttl` (millennia in
        // the future). The bounds bracket the real `+` while excluding both.
        //
        // DURABILITY (issue #364 carry-forward, iter-6 item 4 — quarantine the wall-clock
        // flake): `started`/`finished` are sampled from a DIFFERENT wall-clock read than the
        // gateway's internal one, so an NTP backward step between them could push a correct
        // `+ ttl` lease just under a tight `>= started` lower bound and flake the gate. The
        // lower bound is slackened by `SKEW_ALLOWANCE_MILLIS` (20s) to absorb a clock step of
        // up to ~50s while STILL rejecting the `- ttl` mutant: that mutant lands at least
        // `2*ttl` (~60s) below the correct lease, well beneath `started - 20s`. The upper
        // bound stays generous for the `* ttl` mutant.
        const SKEW_ALLOWANCE_MILLIS: u64 = 20_000;
        assert!(
            expiry + SKEW_ALLOWANCE_MILLIS >= started,
            "lease must expire in the future (now + ttl), not the past (now - ttl): \
             {expiry} < {started} - {SKEW_ALLOWANCE_MILLIS}"
        );
        assert!(
            expiry <= finished + 600_000,
            "lease must be ~now + ttl, not now * ttl: {expiry} >> {finished}"
        );
    }
}
