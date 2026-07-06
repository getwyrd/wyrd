//! Demonstrated-red: every shared `Coordination` clause is load-bearing.
//!
//! The shared suite runs against `coordination-mem`, where several clauses could
//! in principle pass trivially. Each clause below is shown to **catch** a
//! deliberately-violating `Coordination` — proof the property is non-vacuous, and
//! (for config) that its *monotonicity* assertion specifically bites rather than
//! tripping earlier on read-back.
//!
//! Each violating store is dev/test-scope only (`tests/`, never compiled into the
//! library, never shipped, never a real backend). Every `#[should_panic]` test
//! asserts the targeted clause goes RED against its violating store; this is the
//! codified "red" half of the flippable regression the real `coordination-etcd`
//! backend then turns green (the etcd store passes the identical clauses — proven
//! headlessly under the madsim etcd simulator in `crates/dst/tests/coordination.rs`
//! and against real etcd in `crates/coordination-etcd/tests/conformance.rs`).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use pollster::block_on;
use wyrd_coordination_conformance as conformance;
use wyrd_traits::{Coordination, FencingToken, Leadership, Lease, LockGuard, Result};

// A minimal correct in-memory store the violating stubs delegate to for the
// behaviour they DON'T break — so a violation trips its own targeted clause, not
// an unrelated one. (A trimmed cousin of `MemCoordination`, kept here so the
// conformance crate stays dependency-free of any concrete backend, ADR-0016.)
#[derive(Default)]
struct Core {
    regs: HashMap<String, Vec<(u64, Bytes)>>,
    leases: HashMap<u64, String>,
    locks: HashMap<String, FencingToken>,
    config: HashMap<String, Bytes>,
    config_rev: u64,
    next_lease: u64,
    next_token: FencingToken,
}

#[derive(Default)]
struct Good(Mutex<Core>);

#[async_trait]
impl Coordination for Good {
    async fn register(&self, key: &str, value: Bytes, _ttl: Duration) -> Result<Lease> {
        let mut c = self.0.lock().unwrap();
        c.next_lease += 1;
        let id = c.next_lease;
        c.leases.insert(id, key.to_owned());
        c.regs.entry(key.to_owned()).or_default().push((id, value));
        Ok(Lease { id })
    }
    async fn renew(&self, lease: Lease) -> Result<()> {
        if self.0.lock().unwrap().leases.contains_key(&lease.id) {
            Ok(())
        } else {
            Err("renew: unknown lease".into())
        }
    }
    async fn revoke(&self, lease: Lease) -> Result<()> {
        let mut c = self.0.lock().unwrap();
        if let Some(key) = c.leases.remove(&lease.id) {
            if let Some(v) = c.regs.get_mut(&key) {
                v.retain(|(id, _)| *id != lease.id);
            }
        }
        Ok(())
    }
    async fn discover(&self, key: &str) -> Result<Vec<Bytes>> {
        let c = self.0.lock().unwrap();
        Ok(c.regs
            .get(key)
            .map(|v| v.iter().map(|(_, val)| val.clone()).collect())
            .unwrap_or_default())
    }
    async fn elect_leader(&self, _key: &str) -> Result<Leadership> {
        let mut c = self.0.lock().unwrap();
        c.next_token += 1;
        Ok(Leadership {
            token: c.next_token,
        })
    }
    async fn lock(&self, key: &str) -> Result<Option<LockGuard>> {
        let mut c = self.0.lock().unwrap();
        if c.locks.contains_key(key) {
            return Ok(None);
        }
        c.next_token += 1;
        let token = c.next_token;
        c.locks.insert(key.to_owned(), token);
        Ok(Some(LockGuard { token }))
    }
    async fn unlock(&self, guard: LockGuard) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .locks
            .retain(|_, &mut t| t != guard.token);
        Ok(())
    }
    async fn set_config(&self, key: &str, value: Bytes) -> Result<()> {
        let mut c = self.0.lock().unwrap();
        c.config.insert(key.to_owned(), value);
        c.config_rev += 1;
        Ok(())
    }
    async fn get_config(&self, key: &str) -> Result<Option<Bytes>> {
        Ok(self.0.lock().unwrap().config.get(key).cloned())
    }
    async fn config_revision(&self) -> Result<u64> {
        Ok(self.0.lock().unwrap().config_rev)
    }
}

// The `Good` store passes the whole suite — the control that proves the harness
// is not red for an unrelated reason.
#[test]
fn a_correct_store_passes_every_clause() {
    block_on(conformance::run_all(|_tag| async { Good::default() }));
}

// ---- Violating stores, one per clause -------------------------------------

/// discover always empty → `contract_register_then_discover` fails.
#[derive(Default)]
struct NeverDiscovers(Good);
#[async_trait]
impl Coordination for NeverDiscovers {
    async fn register(&self, key: &str, value: Bytes, ttl: Duration) -> Result<Lease> {
        self.0.register(key, value, ttl).await
    }
    async fn renew(&self, l: Lease) -> Result<()> {
        self.0.renew(l).await
    }
    async fn revoke(&self, l: Lease) -> Result<()> {
        self.0.revoke(l).await
    }
    async fn discover(&self, _key: &str) -> Result<Vec<Bytes>> {
        Ok(Vec::new()) // the bug: registrations are never surfaced
    }
    async fn elect_leader(&self, k: &str) -> Result<Leadership> {
        self.0.elect_leader(k).await
    }
    async fn lock(&self, k: &str) -> Result<Option<LockGuard>> {
        self.0.lock(k).await
    }
    async fn unlock(&self, g: LockGuard) -> Result<()> {
        self.0.unlock(g).await
    }
    async fn set_config(&self, k: &str, v: Bytes) -> Result<()> {
        self.0.set_config(k, v).await
    }
    async fn get_config(&self, k: &str) -> Result<Option<Bytes>> {
        self.0.get_config(k).await
    }
    async fn config_revision(&self) -> Result<u64> {
        self.0.config_revision().await
    }
}

#[test]
#[should_panic(expected = "both members surface")]
fn register_then_discover_catches_a_store_that_never_discovers() {
    block_on(conformance::contract_register_then_discover(
        &NeverDiscovers::default(),
    ));
}

/// revoke is a no-op → `contract_renew_and_revoke` fails (still discoverable).
#[derive(Default)]
struct NeverRevokes(Good);
#[async_trait]
impl Coordination for NeverRevokes {
    async fn register(&self, key: &str, value: Bytes, ttl: Duration) -> Result<Lease> {
        self.0.register(key, value, ttl).await
    }
    async fn renew(&self, l: Lease) -> Result<()> {
        self.0.renew(l).await
    }
    async fn revoke(&self, _l: Lease) -> Result<()> {
        Ok(()) // the bug: never actually withdraws the registration
    }
    async fn discover(&self, k: &str) -> Result<Vec<Bytes>> {
        self.0.discover(k).await
    }
    async fn elect_leader(&self, k: &str) -> Result<Leadership> {
        self.0.elect_leader(k).await
    }
    async fn lock(&self, k: &str) -> Result<Option<LockGuard>> {
        self.0.lock(k).await
    }
    async fn unlock(&self, g: LockGuard) -> Result<()> {
        self.0.unlock(g).await
    }
    async fn set_config(&self, k: &str, v: Bytes) -> Result<()> {
        self.0.set_config(k, v).await
    }
    async fn get_config(&self, k: &str) -> Result<Option<Bytes>> {
        self.0.get_config(k).await
    }
    async fn config_revision(&self) -> Result<u64> {
        self.0.config_revision().await
    }
}

#[test]
#[should_panic(expected = "withdrawn immediately")]
fn renew_and_revoke_catches_a_store_that_never_revokes() {
    block_on(conformance::contract_renew_and_revoke(
        &NeverRevokes::default(),
    ));
}

/// lock always grants → `contract_locks_are_mutually_exclusive_and_fenced` fails.
#[derive(Default)]
struct NonExclusiveLock {
    next: Mutex<FencingToken>,
}
#[async_trait]
impl Coordination for NonExclusiveLock {
    async fn register(&self, _k: &str, _v: Bytes, _t: Duration) -> Result<Lease> {
        Ok(Lease { id: 1 })
    }
    async fn renew(&self, _l: Lease) -> Result<()> {
        Ok(())
    }
    async fn revoke(&self, _l: Lease) -> Result<()> {
        Ok(())
    }
    async fn discover(&self, _k: &str) -> Result<Vec<Bytes>> {
        Ok(Vec::new())
    }
    async fn elect_leader(&self, _k: &str) -> Result<Leadership> {
        Ok(Leadership { token: 1 })
    }
    async fn lock(&self, _key: &str) -> Result<Option<LockGuard>> {
        let mut n = self.next.lock().unwrap();
        *n += 1;
        Ok(Some(LockGuard { token: *n })) // the bug: never refuses a held key
    }
    async fn unlock(&self, _g: LockGuard) -> Result<()> {
        Ok(())
    }
    async fn set_config(&self, _k: &str, _v: Bytes) -> Result<()> {
        Ok(())
    }
    async fn get_config(&self, _k: &str) -> Result<Option<Bytes>> {
        Ok(None)
    }
    async fn config_revision(&self) -> Result<u64> {
        Ok(0)
    }
}

#[test]
#[should_panic(expected = "refuses contenders")]
fn locks_catch_a_non_exclusive_store() {
    block_on(
        conformance::contract_locks_are_mutually_exclusive_and_fenced(&NonExclusiveLock::default()),
    );
}

/// Every token is the same constant → `contract_fencing_tokens_rise...` fails.
#[derive(Default)]
struct ConstantToken;
#[async_trait]
impl Coordination for ConstantToken {
    async fn register(&self, _k: &str, _v: Bytes, _t: Duration) -> Result<Lease> {
        Ok(Lease { id: 1 })
    }
    async fn renew(&self, _l: Lease) -> Result<()> {
        Ok(())
    }
    async fn revoke(&self, _l: Lease) -> Result<()> {
        Ok(())
    }
    async fn discover(&self, _k: &str) -> Result<Vec<Bytes>> {
        Ok(Vec::new())
    }
    async fn elect_leader(&self, _k: &str) -> Result<Leadership> {
        Ok(Leadership { token: 7 }) // the bug: tokens never rise
    }
    async fn lock(&self, _k: &str) -> Result<Option<LockGuard>> {
        Ok(Some(LockGuard { token: 7 })) // the bug: tokens never rise
    }
    async fn unlock(&self, _g: LockGuard) -> Result<()> {
        Ok(())
    }
    async fn set_config(&self, _k: &str, _v: Bytes) -> Result<()> {
        Ok(())
    }
    async fn get_config(&self, _k: &str) -> Result<Option<Bytes>> {
        Ok(None)
    }
    async fn config_revision(&self) -> Result<u64> {
        Ok(0)
    }
}

#[test]
#[should_panic(expected = "globally monotonic")]
fn fencing_catches_a_constant_token_store() {
    block_on(conformance::contract_fencing_tokens_rise_across_locks_and_elections(&ConstantToken));
}

/// Config persists correctly (read-back PASSES) but the revision is frozen, so the
/// clause reaches `r1 > r0` and fails THERE — proof the monotonicity assertion is
/// the one that bites, not an earlier read-back check (the v2 gap).
#[derive(Default)]
struct FrozenConfigRevision(Good);
#[async_trait]
impl Coordination for FrozenConfigRevision {
    async fn register(&self, key: &str, value: Bytes, ttl: Duration) -> Result<Lease> {
        self.0.register(key, value, ttl).await
    }
    async fn renew(&self, l: Lease) -> Result<()> {
        self.0.renew(l).await
    }
    async fn revoke(&self, l: Lease) -> Result<()> {
        self.0.revoke(l).await
    }
    async fn discover(&self, k: &str) -> Result<Vec<Bytes>> {
        self.0.discover(k).await
    }
    async fn elect_leader(&self, k: &str) -> Result<Leadership> {
        self.0.elect_leader(k).await
    }
    async fn lock(&self, k: &str) -> Result<Option<LockGuard>> {
        self.0.lock(k).await
    }
    async fn unlock(&self, g: LockGuard) -> Result<()> {
        self.0.unlock(g).await
    }
    async fn set_config(&self, k: &str, v: Bytes) -> Result<()> {
        self.0.set_config(k, v).await // values persist correctly (read-back passes)
    }
    async fn get_config(&self, k: &str) -> Result<Option<Bytes>> {
        self.0.get_config(k).await
    }
    async fn config_revision(&self) -> Result<u64> {
        Ok(42) // the bug: the revision never advances
    }
}

#[test]
#[should_panic(expected = "strictly raises the revision")]
fn config_catches_a_frozen_revision_after_read_back_passes() {
    block_on(conformance::contract_config_is_revisioned(
        &FrozenConfigRevision::default(),
    ));
}

/// Config values persist correctly AND the revision strictly rises on a config
/// write (so it passes read-back and the `r1 > r0` / `r2 > r1` monotonicity
/// checks), but the revision is a cluster-GLOBAL write counter — it also advances
/// on an unrelated `register`. This models the exact etcd hazard the config clause
/// was tightened to catch (a header/global mvcc revision that leaks unrelated
/// traffic into the config watch), so it trips the config-ONLY-advancement
/// assertion, not an earlier one.
#[derive(Default)]
struct ConfigCountsEveryWrite {
    inner: Good,
    writes: Mutex<u64>,
}
#[async_trait]
impl Coordination for ConfigCountsEveryWrite {
    async fn register(&self, key: &str, value: Bytes, ttl: Duration) -> Result<Lease> {
        *self.writes.lock().unwrap() += 1; // the bug: a non-config write bumps the counter
        self.inner.register(key, value, ttl).await
    }
    async fn renew(&self, l: Lease) -> Result<()> {
        self.inner.renew(l).await
    }
    async fn revoke(&self, l: Lease) -> Result<()> {
        self.inner.revoke(l).await
    }
    async fn discover(&self, k: &str) -> Result<Vec<Bytes>> {
        self.inner.discover(k).await
    }
    async fn elect_leader(&self, k: &str) -> Result<Leadership> {
        self.inner.elect_leader(k).await
    }
    async fn lock(&self, k: &str) -> Result<Option<LockGuard>> {
        self.inner.lock(k).await
    }
    async fn unlock(&self, g: LockGuard) -> Result<()> {
        self.inner.unlock(g).await
    }
    async fn set_config(&self, k: &str, v: Bytes) -> Result<()> {
        *self.writes.lock().unwrap() += 1;
        self.inner.set_config(k, v).await // values persist correctly (read-back passes)
    }
    async fn get_config(&self, k: &str) -> Result<Option<Bytes>> {
        self.inner.get_config(k).await
    }
    async fn config_revision(&self) -> Result<u64> {
        Ok(*self.writes.lock().unwrap()) // the bug: a GLOBAL counter, not config-scoped
    }
}

#[test]
#[should_panic(expected = "must NOT advance the config revision")]
fn config_catches_a_global_write_counter_revision() {
    block_on(conformance::contract_config_is_revisioned(
        &ConfigCountsEveryWrite::default(),
    ));
}

/// Every register returns the same lease id → `contract_leases_are_distinct` fails.
#[derive(Default)]
struct DuplicateLeaseIds;
#[async_trait]
impl Coordination for DuplicateLeaseIds {
    async fn register(&self, _k: &str, _v: Bytes, _t: Duration) -> Result<Lease> {
        Ok(Lease { id: 1 }) // the bug: never distinct
    }
    async fn renew(&self, _l: Lease) -> Result<()> {
        Ok(())
    }
    async fn revoke(&self, _l: Lease) -> Result<()> {
        Ok(())
    }
    async fn discover(&self, _k: &str) -> Result<Vec<Bytes>> {
        Ok(Vec::new())
    }
    async fn elect_leader(&self, _k: &str) -> Result<Leadership> {
        Ok(Leadership { token: 1 })
    }
    async fn lock(&self, _k: &str) -> Result<Option<LockGuard>> {
        Ok(Some(LockGuard { token: 1 }))
    }
    async fn unlock(&self, _g: LockGuard) -> Result<()> {
        Ok(())
    }
    async fn set_config(&self, _k: &str, _v: Bytes) -> Result<()> {
        Ok(())
    }
    async fn get_config(&self, _k: &str) -> Result<Option<Bytes>> {
        Ok(None)
    }
    async fn config_revision(&self) -> Result<u64> {
        Ok(0)
    }
}

#[test]
#[should_panic(expected = "distinct lease id")]
fn leases_catch_a_duplicate_id_store() {
    block_on(conformance::contract_leases_are_distinct(
        &DuplicateLeaseIds,
    ));
}
