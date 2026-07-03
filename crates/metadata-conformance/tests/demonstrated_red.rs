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
