//! A **deterministic simulated-TiKV** `MetadataStore` — the *second* implementation
//! the DST tier drives, so the trait is pinned by two implementations (ADR-0006;
//! proposal 0015 §"Pinning the trait with the second implementation", accepted
//! `docs/design/proposals/accepted/0015-milestone-4-production-metadata-backend-revised.md:546-555`).
//!
//! This is **not** a real or containerized TiKV — putting one inside DST is
//! explicitly rejected (proposal 0015 lines 484-499, 600-603; ADR-0009 forbids a
//! real environment for correctness DST already covers). It is a small, in-memory,
//! seed-reproducible *model* that renders the one thing a redb store does not: a
//! commit that **awaits on network I/O mid-flight** (a 2PC/TSO round-trip), so
//! madsim can interleave a second writer *inside* a commit. The version
//! compare-and-set still yields exactly one winner because the decisive step — the
//! pessimistic prewrite lock-grab — is atomic, not spread across the await
//! (proposal 0015 lines 549-555).
//!
//! It lives under `tests/` (dev/test scope only, never shipped, never a real
//! backend) — the same discipline the shared conformance suite's violating stores
//! follow (`crates/metadata-conformance/tests/demonstrated_red.rs`). Because it is
//! test-scope and uses only *instance* state (never a `static`), it is outside the
//! ADR-0035 global-mutable-state gate (which scans `src/` only) and cannot leak
//! observations across seeds/threads.
//!
//! ## Fidelity is an open design point (issue #264 / proposal 0015 lines 798-801)
//!
//! How faithfully a simulated-TiKV must model 2PC/TSO interleavings — vs a
//! trait-level contract harness — to keep "exactly one wins" coverage honest is an
//! explicitly open M4 design point. This model proposes the **pessimistic-lock at an
//! atomic prewrite** level of fidelity and demonstrates it reaches the target
//! interleaving; the human ratifies that choice at sign-off.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_traits::{CommitOutcome, MetadataStore, Precondition, Result, WriteBatch};

/// How faithfully the model renders a commit's async shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fidelity {
    /// The faithful model: a commit spans network round-trips (a TSO/begin hop and a
    /// commit hop) with a real `.await` **inside** it, exactly as a TiKV 2PC does.
    /// The winner is decided at an atomic *prewrite* lock-grab, so exactly-one-winner
    /// survives the mid-commit interleaving the await boundary makes reachable.
    AwaitInsideCommit,
    /// The redb-shaped assumption the old `concurrency.rs` header encoded: a commit is
    /// one indivisible step, "no await inside". Still a correct store (exactly one
    /// winner), but structurally **unable** to reach a mid-commit interleaving — kept
    /// only to demonstrate that the await boundary is what makes that schedule
    /// reachable (the demonstrated-red twin).
    SynchronousRedbShaped,
}

/// Per-simulation observation of the interleavings actually exercised. Instance
/// state (never a `static`) so it lives inside madsim's simulated world and cannot
/// leak across seeds/threads (ADR-0035).
#[derive(Debug, Default, Clone, Copy)]
pub struct Observations {
    /// Commits that, at their atomic prewrite, found a key already **locked by an
    /// in-flight (not-yet-committed) commit** — i.e. observed another writer
    /// *mid-commit*, the schedule an indivisible (synchronous) commit can never
    /// produce.
    pub mid_commit_lock_conflicts: u64,
    /// High-water mark of commits simultaneously past prewrite and awaiting their
    /// commit hop — the depth of the in-flight window the await boundary opens.
    pub max_inflight: u64,
}

#[derive(Default)]
struct Inner {
    /// The committed key/value state.
    truth: BTreeMap<Vec<u8>, Bytes>,
    /// Keys currently locked by an in-flight commit (pessimistic prewrite locks).
    /// Only ever probed with `contains`/`insert`/`remove` — never iterated — so it
    /// introduces no ordering nondeterminism.
    locks: HashSet<Vec<u8>>,
    /// Number of commits currently past prewrite and awaiting their commit hop.
    inflight: u64,
    obs: Observations,
}

/// A deterministic simulated-TiKV `MetadataStore` (see module docs).
pub struct SimTikvMetadataStore {
    inner: Mutex<Inner>,
    fidelity: Fidelity,
}

impl SimTikvMetadataStore {
    /// A fresh, empty store with the faithful await-inside-commit fidelity.
    pub fn new() -> Self {
        Self::with_fidelity(Fidelity::AwaitInsideCommit)
    }

    /// A fresh, empty store at the given [`Fidelity`].
    pub fn with_fidelity(fidelity: Fidelity) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            fidelity,
        }
    }

    /// A snapshot of what this store observed during the run.
    pub fn observations(&self) -> Observations {
        self.inner.lock().unwrap().obs
    }
}

impl Default for SimTikvMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

/// One simulated network round-trip. A fixed, non-zero madsim timer, so every
/// writer yields the scheduler at the same virtual instant — the interleaving
/// point. `sleep` always returns `Pending` first, so this is a real await boundary
/// madsim can schedule across (unlike a redb write transaction).
async fn network_hop() {
    madsim::time::sleep(Duration::from_millis(1)).await;
}

/// Whether every precondition holds against the committed truth (`None` = require
/// absent, `Some(v)` = require exact value) — the byte-compare the trait specifies.
fn preconditions_hold(truth: &BTreeMap<Vec<u8>, Bytes>, preconditions: &[Precondition]) -> bool {
    preconditions
        .iter()
        .all(|pre| truth.get(&pre.key).cloned() == pre.expected)
}

/// The keys a commit touches — every precondition, put, and delete key — the set a
/// pessimistic transaction locks (TiKV's `get_for_update` over the read+write set).
fn write_set(batch: &WriteBatch) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    keys.extend(batch.preconditions.iter().map(|pre| pre.key.clone()));
    keys.extend(batch.puts.iter().map(|(k, _)| k.clone()));
    keys.extend(batch.deletes.iter().cloned());
    keys.sort();
    keys.dedup();
    keys
}

/// Apply a batch's mutations to the committed truth (deletes then puts).
fn apply(truth: &mut BTreeMap<Vec<u8>, Bytes>, batch: &WriteBatch) {
    for key in &batch.deletes {
        truth.remove(key);
    }
    for (key, value) in &batch.puts {
        truth.insert(key.clone(), value.clone());
    }
}

impl SimTikvMetadataStore {
    /// The redb-shaped model: check-and-apply in one indivisible step, no await
    /// inside — so no second writer can ever be observed mid-commit.
    fn commit_synchronous(&self, batch: &WriteBatch) -> CommitOutcome {
        let mut inner = self.inner.lock().unwrap();
        if !preconditions_hold(&inner.truth, &batch.preconditions) {
            return CommitOutcome::Conflict;
        }
        apply(&mut inner.truth, batch);
        CommitOutcome::Committed
    }

    /// The faithful model: begin (TSO) hop, an **atomic** prewrite that grabs the
    /// pessimistic locks and checks preconditions, the **mid-commit** await, then an
    /// atomic apply that releases the locks. The winner is decided at prewrite, so
    /// exactly one writer wins even though a commit spans two await boundaries.
    async fn commit_await_inside(&self, batch: &WriteBatch) -> CommitOutcome {
        let keys = write_set(batch);

        // Phase 1 — begin / TSO: a network hop. Every concurrent writer yields here.
        network_hop().await;

        // Phase 2 — prewrite: the ATOMIC decision point (one critical section, no
        // await). Grab pessimistic locks on the write set, then check preconditions.
        {
            let mut inner = self.inner.lock().unwrap();
            // Write-write conflict: a key is already locked by an in-flight commit,
            // so we are observing that writer *mid-commit* — past its prewrite, not
            // yet applied. This is precisely the schedule a synchronous, indivisible
            // commit can never produce.
            if keys.iter().any(|k| inner.locks.contains(k)) {
                inner.obs.mid_commit_lock_conflicts += 1;
                return CommitOutcome::Conflict;
            }
            if !preconditions_hold(&inner.truth, &batch.preconditions) {
                return CommitOutcome::Conflict;
            }
            for key in &keys {
                inner.locks.insert(key.clone());
            }
            inner.inflight += 1;
            inner.obs.max_inflight = inner.obs.max_inflight.max(inner.inflight);
        }

        // Phase 3 — the mid-commit await: the commit RPC round-trip. Another writer
        // runs here; this is the boundary the redb-shaped "no await inside" denied.
        network_hop().await;

        // Phase 4 — commit: apply and release the locks (atomic, no await).
        {
            let mut inner = self.inner.lock().unwrap();
            apply(&mut inner.truth, batch);
            for key in &keys {
                inner.locks.remove(key);
            }
            inner.inflight -= 1;
        }
        CommitOutcome::Committed
    }
}

#[async_trait]
impl MetadataStore for SimTikvMetadataStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.fidelity == Fidelity::AwaitInsideCommit {
            network_hop().await; // a snapshot read is a network round-trip too.
        }
        Ok(self.inner.lock().unwrap().truth.get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if self.fidelity == Fidelity::AwaitInsideCommit {
            network_hop().await;
        }
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .truth
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(match self.fidelity {
            Fidelity::SynchronousRedbShaped => self.commit_synchronous(&batch),
            Fidelity::AwaitInsideCommit => self.commit_await_inside(&batch).await,
        })
    }
}
