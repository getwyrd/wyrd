//! Demonstrated-red for the three read-consistency properties (#419).
//!
//! #419's forcing function (brief "Verification posture"): because
//! `contract_read_after_commit` / `contract_rename_race_yields_conflict` /
//! `contract_scan_is_consistent_cut` run against redb's atomic backend where
//! they may pass trivially, each must be shown to CATCH a deliberately-violating
//! `MetadataStore` — proof the property is load-bearing (non-vacuous) and that
//! it catches something the four pre-existing `contract_*` clauses
//! (`crates/metadata-conformance/src/lib.rs:24-111`) do not (non-redundant).
//!
//! Each violating store below is dev/test-scope only (`tests/`, never compiled
//! into the library, never shipped, never a real backend — the #419 brief's
//! "Violating-store test double" NEEDS-HUMAN note on placement). Each
//! `#[should_panic]` test asserts the new property's `assert_eq!` panics
//! (goes red) against its targeted violating store; the sibling
//! `*_passes_existing_sequential_contracts` test asserts the SAME violating
//! store still passes the four pre-existing sequential clauses unmodified —
//! together they demonstrate the new property adds discriminating power the
//! old suite lacked, not just a differently-shaped restatement of it.

#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use pollster::block_on;
use wyrd_metadata_conformance as conformance;
use wyrd_traits::{CommitOutcome, MetadataStore, Result, WriteBatch};

// ---- Violating store 1: a read cache that pins after the SECOND write -----
//
// Models ADR-0015's rejected Option B (nearest-replica, bounded staleness): a
// `get` is correct for a key's first and second successful write (so it is
// invisible to `contract_require_value_gates`, `:83-111`, which writes any one
// key at most twice — once to seed it, once through its `require`-gated
// commit — and re-reads it exactly twice), but from the key's THIRD write
// onward `get` keeps serving the second write's value forever — a fresh-TSO /
// read-your-writes violation that only a REPEATED-overwrite property can see.
// `contract_read_after_commit` writes the same key four times running and
// re-reads after every one, so it does.

#[derive(Default)]
struct StaleCacheStore {
    truth: Mutex<HashMap<Vec<u8>, Bytes>>,
    write_counts: Mutex<HashMap<Vec<u8>, u32>>,
    // Populated the instant a key's SECOND successful write lands, and never
    // touched again — the "warms up once, then pins" staleness bug.
    pinned_after_second_write: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for StaleCacheStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(v) = self.pinned_after_second_write.lock().unwrap().get(key) {
            return Ok(Some(v.clone()));
        }
        Ok(self.truth.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let truth = self.truth.lock().unwrap();
        Ok(truth
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut truth = self.truth.lock().unwrap();
        let ok = batch
            .preconditions
            .iter()
            .all(|pre| truth.get(&pre.key).cloned() == pre.expected);
        if !ok {
            return Ok(CommitOutcome::Conflict);
        }
        for k in &batch.deletes {
            truth.remove(k);
        }
        for (k, v) in batch.puts {
            truth.insert(k.clone(), v.clone());
            let mut counts = self.write_counts.lock().unwrap();
            let count = counts.entry(k.clone()).or_insert(0);
            *count += 1;
            if *count == 2 {
                self.pinned_after_second_write.lock().unwrap().insert(k, v);
            }
        }
        Ok(CommitOutcome::Committed)
    }
}

#[test]
#[should_panic(expected = "read-your-writes")]
fn stale_cache_store_fails_read_after_commit() {
    block_on(conformance::contract_read_after_commit(
        &StaleCacheStore::default(),
    ));
}

#[test]
fn stale_cache_store_passes_existing_sequential_contracts() {
    // Same violating store; the four PRE-EXISTING clauses never re-read a key
    // after a second overwrite of it, so they cannot observe the staleness.
    block_on(async {
        conformance::contract_commit_and_get(&StaleCacheStore::default()).await;
        conformance::contract_scan_by_prefix(&StaleCacheStore::default()).await;
        conformance::contract_require_absent_gates(&StaleCacheStore::default()).await;
        conformance::contract_require_value_gates(&StaleCacheStore::default()).await;
    });
}

// ---- Violating store 2: `require` bypassed for a key also being deleted ----
//
// A plausible real-world shortcut bug: "the key is being deleted this batch
// anyway, so skip re-checking its prior value." That is exactly wrong for the
// `rename` pattern (`require(old_key, current)` + `delete(old_key)`,
// `crates/core/src/metadata.rs:288,291`): it lets a stale racer's commit
// through, producing a DUPLICATED binding. Neither
// `contract_require_value_gates` nor `contract_require_absent_gates` ever
// combines a `require` precondition with a `delete` of that same key, so
// neither exercises this bug; `contract_rename_race_yields_conflict` does.

#[derive(Default)]
struct IgnoresRequireOnDeleteStore {
    truth: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for IgnoresRequireOnDeleteStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let truth = self.truth.lock().unwrap();
        Ok(truth
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut truth = self.truth.lock().unwrap();
        let delete_set: HashSet<&Vec<u8>> = batch.deletes.iter().collect();
        // BUG: a `require` precondition on a key that this same batch also
        // `delete`s is treated as trivially satisfied instead of checked.
        let ok = batch.preconditions.iter().all(|pre| {
            delete_set.contains(&pre.key) || truth.get(&pre.key).cloned() == pre.expected
        });
        if !ok {
            return Ok(CommitOutcome::Conflict);
        }
        for k in &batch.deletes {
            truth.remove(k);
        }
        for (k, v) in batch.puts {
            truth.insert(k, v);
        }
        Ok(CommitOutcome::Committed)
    }
}

#[test]
#[should_panic(expected = "a stale read-then-commit must lose")]
fn ignores_require_on_delete_store_fails_rename_race() {
    block_on(conformance::contract_rename_race_yields_conflict(
        &IgnoresRequireOnDeleteStore::default(),
    ));
}

#[test]
fn ignores_require_on_delete_store_passes_existing_sequential_contracts() {
    // The bug only fires when a `require`d key is ALSO `delete`d in the same
    // batch — a shape none of the four pre-existing clauses construct.
    block_on(async {
        conformance::contract_commit_and_get(&IgnoresRequireOnDeleteStore::default()).await;
        conformance::contract_scan_by_prefix(&IgnoresRequireOnDeleteStore::default()).await;
        conformance::contract_require_absent_gates(&IgnoresRequireOnDeleteStore::default()).await;
        conformance::contract_require_value_gates(&IgnoresRequireOnDeleteStore::default()).await;
    });
}

// ---- Violating store 3: a scan index that leaks deleted keys --------------
//
// `scan` here is backed by a listing index updated on `put` but never purged
// on `delete` — literally "a `scan` that returns a torn view" (#419 brief's
// own suggested double). A rename (delete old key, put new key under the same
// prefix) then makes BOTH positions appear in one `scan()` call.
// `contract_scan_by_prefix` never deletes anything before scanning, so it
// cannot observe the leak; `contract_scan_is_consistent_cut` re-scans after a
// delete+put and does.

#[derive(Default)]
struct LeakyScanIndexStore {
    truth: Mutex<HashMap<Vec<u8>, Bytes>>,
    ever_seen: Mutex<HashSet<Vec<u8>>>,
}

#[async_trait]
impl MetadataStore for LeakyScanIndexStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let truth = self.truth.lock().unwrap();
        let ever_seen = self.ever_seen.lock().unwrap();
        Ok(ever_seen
            .iter()
            .filter(|k| k.starts_with(prefix))
            .map(|k| {
                let v = truth
                    .get(k)
                    .cloned()
                    .unwrap_or_else(|| Bytes::from_static(b"<deleted-but-still-listed>"));
                (k.clone(), v)
            })
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut truth = self.truth.lock().unwrap();
        let ok = batch
            .preconditions
            .iter()
            .all(|pre| truth.get(&pre.key).cloned() == pre.expected);
        if !ok {
            return Ok(CommitOutcome::Conflict);
        }
        // BUG: deletes purge `truth` but never `ever_seen` — the scan index
        // leaks a deleted key forever.
        for k in &batch.deletes {
            truth.remove(k);
        }
        let mut ever_seen = self.ever_seen.lock().unwrap();
        for (k, v) in batch.puts {
            ever_seen.insert(k.clone());
            truth.insert(k, v);
        }
        Ok(CommitOutcome::Committed)
    }
}

#[test]
#[should_panic(expected = "exactly one scan position")]
fn leaky_scan_index_store_fails_scan_is_consistent_cut() {
    block_on(conformance::contract_scan_is_consistent_cut(
        &LeakyScanIndexStore::default(),
    ));
}

#[test]
fn leaky_scan_index_store_passes_existing_sequential_contracts() {
    // `contract_scan_by_prefix` never deletes a key before scanning, so the
    // leaked-index bug has no delete to leak and stays invisible to it.
    block_on(async {
        conformance::contract_commit_and_get(&LeakyScanIndexStore::default()).await;
        conformance::contract_scan_by_prefix(&LeakyScanIndexStore::default()).await;
        conformance::contract_require_absent_gates(&LeakyScanIndexStore::default()).await;
        conformance::contract_require_value_gates(&LeakyScanIndexStore::default()).await;
    });
}

// ---- Violating store 4: a lost race reported as `Conflict` on a BLIND batch --
//
// Models the mistake an optimistic backend is structurally invited to make
// (#437): its substrate reports ONE lost-race error for both batch shapes
// (FoundationDB's `1020 not_committed`), so the lazy routing — "lost race ⇒
// CommitOutcome::Conflict" — is right for a conditional batch and WRONG for a
// blind one, which asserted nothing about prior state and therefore cannot lose
// a precondition. Blind writers that `?` the commit and ignore the outcome
// (`core::repair::enqueue_repair`, the custodian's desired-state writes) then
// read a dropped write as success.
//
// The bug is race-only, which is the point: sequentially this store is a
// perfectly correct `MetadataStore` and passes every OTHER clause in the suite,
// including `contract_read_after_commit` (whose repeated blind overwrites would
// catch any cruder violator that conflicted blind writes unconditionally). Only a
// clause that drives two commits CONCURRENTLY can see it — which is why
// `contract_blind_batch_is_never_conflict` has a concurrent half.

/// A future that returns `Pending` exactly once, waking itself — a yield point,
/// so two commits driven by `futures_util::future::join` on a single-threaded
/// executor genuinely overlap (both enter `commit` before either finishes).
struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

#[derive(Default)]
struct RaceConflatingStore {
    truth: Mutex<HashMap<Vec<u8>, Bytes>>,
    /// Commits currently between entry and apply — the stand-in for "another
    /// writer touched my keys before my commit landed".
    in_flight: Mutex<usize>,
}

#[async_trait]
impl MetadataStore for RaceConflatingStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let truth = self.truth.lock().unwrap();
        Ok(truth
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        *self.in_flight.lock().unwrap() += 1;
        // Give a concurrent sibling the chance to enter `commit` too — without this
        // the executor would run each commit to completion and no race could form.
        YieldOnce(false).await;
        let raced = *self.in_flight.lock().unwrap() > 1;

        let mut truth = self.truth.lock().unwrap();
        let holds = batch
            .preconditions
            .iter()
            .all(|pre| truth.get(&pre.key).cloned() == pre.expected);
        if !holds {
            *self.in_flight.lock().unwrap() -= 1;
            return Ok(CommitOutcome::Conflict);
        }
        // THE BUG: the lost race is reported as `Conflict` whatever the batch's
        // shape. Correct for the conditional batch above; a swallowed write here.
        if raced {
            *self.in_flight.lock().unwrap() -= 1;
            return Ok(CommitOutcome::Conflict);
        }
        for k in &batch.deletes {
            truth.remove(k);
        }
        for (k, v) in batch.puts {
            truth.insert(k, v);
        }
        *self.in_flight.lock().unwrap() -= 1;
        Ok(CommitOutcome::Committed)
    }
}

#[test]
#[should_panic(expected = "must never conflict")]
fn race_conflating_store_fails_blind_batch_is_never_conflict() {
    block_on(conformance::contract_blind_batch_is_never_conflict(
        &RaceConflatingStore::default(),
    ));
}

#[test]
fn race_conflating_store_passes_every_other_contract() {
    // The whole rest of the suite — not just the four sequential clauses — because
    // this store's bug is invisible without concurrency, and no other clause drives
    // two commits at once. That is precisely the discriminating power
    // `contract_blind_batch_is_never_conflict` adds: without it, a backend could
    // swallow every raced blind write and stay green across all seven other clauses.
    block_on(async {
        conformance::contract_commit_and_get(&RaceConflatingStore::default()).await;
        conformance::contract_scan_by_prefix(&RaceConflatingStore::default()).await;
        conformance::contract_require_absent_gates(&RaceConflatingStore::default()).await;
        conformance::contract_require_value_gates(&RaceConflatingStore::default()).await;
        conformance::contract_read_after_commit(&RaceConflatingStore::default()).await;
        conformance::contract_rename_race_yields_conflict(&RaceConflatingStore::default()).await;
        conformance::contract_scan_is_consistent_cut(&RaceConflatingStore::default()).await;
    });
}

// ---- Violating store 5: a raced blind write reported as `Committed` and DROPPED ----
//
// The other half of "swallowing a blind write" (#437, and the review of the clause that
// caught it): `Conflict` is the loud way to swallow one, and this is the quiet way — the
// backend gives up on the raced batch, reports `Ok(Committed)`, and writes nothing. Every
// caller believes the write landed; none of them re-reads, because `Committed` is a claim
// that it already did.
//
// Like `RaceConflatingStore`, the bug is race-only: sequentially this is a perfectly
// correct store, so it passes every other clause in the suite. And it slips past the FIRST
// draft of `contract_blind_batch_is_never_conflict`, whose closing assertion was
// presence-conditional (`if let Some(value) = …`) — key absent ⇒ assertion skipped ⇒ green.
// The clause now requires the key to EXIST whenever either racer claimed `Committed`, which
// is what makes this store go red.

#[derive(Default)]
struct LyingCommitStore {
    truth: Mutex<HashMap<Vec<u8>, Bytes>>,
    in_flight: Mutex<usize>,
    /// Sticky: set the moment two commits are in flight at once. `in_flight` alone is not
    /// enough — the first racer decrements on its way out, so by the time the second
    /// resumes the counter reads 1 and it would not see the collision it was part of.
    collided: Mutex<bool>,
}

#[async_trait]
impl MetadataStore for LyingCommitStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let truth = self.truth.lock().unwrap();
        Ok(truth
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        *self.in_flight.lock().unwrap() += 1;
        YieldOnce(false).await;
        if *self.in_flight.lock().unwrap() > 1 {
            *self.collided.lock().unwrap() = true;
        }
        let raced = *self.collided.lock().unwrap();

        let mut truth = self.truth.lock().unwrap();
        let holds = batch
            .preconditions
            .iter()
            .all(|pre| truth.get(&pre.key).cloned() == pre.expected);
        if !holds {
            *self.in_flight.lock().unwrap() -= 1;
            return Ok(CommitOutcome::Conflict);
        }
        // THE BUG: the raced blind batch is abandoned — and reported as if it had landed.
        if raced && batch.preconditions.is_empty() {
            *self.in_flight.lock().unwrap() -= 1;
            return Ok(CommitOutcome::Committed);
        }
        for k in &batch.deletes {
            truth.remove(k);
        }
        for (k, v) in batch.puts {
            truth.insert(k, v);
        }
        *self.in_flight.lock().unwrap() -= 1;
        Ok(CommitOutcome::Committed)
    }
}

#[test]
#[should_panic(expected = "the key is absent")]
fn lying_commit_store_fails_blind_batch_is_never_conflict() {
    block_on(conformance::contract_blind_batch_is_never_conflict(
        &LyingCommitStore::default(),
    ));
}

#[test]
fn lying_commit_store_passes_every_other_contract() {
    // Race-only, so the whole rest of the suite is green against it — including
    // `contract_commit_and_get`, whose blind puts all land because nothing races them.
    block_on(async {
        conformance::contract_commit_and_get(&LyingCommitStore::default()).await;
        conformance::contract_scan_by_prefix(&LyingCommitStore::default()).await;
        conformance::contract_require_absent_gates(&LyingCommitStore::default()).await;
        conformance::contract_require_value_gates(&LyingCommitStore::default()).await;
        conformance::contract_read_after_commit(&LyingCommitStore::default()).await;
        conformance::contract_rename_race_yields_conflict(&LyingCommitStore::default()).await;
        conformance::contract_scan_is_consistent_cut(&LyingCommitStore::default()).await;
    });
}
