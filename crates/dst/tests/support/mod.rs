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
//!
//! ## The simulated-FoundationDB model (issue #468)
//!
//! [`SimFdbMetadataStore`] is the **second parametrization** of the skeleton above —
//! not a second framework (issue #468 item 4; #258/#447 landed this shape *to* be
//! parametrized). The recorded decision, condensed from the design proposal:
//!
//! * **Why a simulated FDB at all, when a contract harness already exists.** The
//!   harness leg is kept — `crates/metadata-fdb/tests/conformance.rs` drives the same
//!   `wyrd_metadata_conformance::run_all` clauses against a real `fdbserver`, so it is
//!   the equivalence anchor. But **none** of those clauses touches commit ambiguity,
//!   and none *could*: `crates/metadata-fdb/src/lib.rs:71` states that "a healthy
//!   `fdbserver` cannot be made to emit 1021 on demand". Commit ambiguity is the one
//!   genuinely new failure shape FDB introduces, and a real fault battery can only
//!   *sample* it. A seed-driven nemesis inside the simulator can *search* it.
//! * **Why not the real driver in-simulator.** `libfdb_c` spawns its own network
//!   thread and does real I/O (`crates/metadata-fdb/src/lib.rs:846-869`), which would
//!   violate seed determinism outright (ADR-0009, ADR-0035, and the rejection recorded
//!   at lines 6-8 above). So the FDB backend is *modelled*, never linked — and
//!   `crates/dst/tests/no_fdb_linkage.rs` keeps the `foundationdb` / `foundationdb-sys`
//!   packages out of the simulator's **feature-unified dependency graph**.
//! * **The ambiguity class has two members, and they are not equally bad.** Production
//!   maps **both** `1021 commit_unknown_result` and `1031 transaction_timed_out` to
//!   `classify::CommitClass::UnknownResult`, for *every* batch — the code check returns
//!   before the `conditional` check (`crates/metadata-fdb/src/lib.rs:212-215`, doc at
//!   `:191-204`). It carries the code on the error so a caller can tell them apart:
//!   "Where 1021 promises the transaction is out of flight, 1031 promises nothing"
//!   (`:165`, and `may_still_commit`, `:240-249`). [`SimCommitUnknownResult`] carries
//!   the same code and reproduces the same split: after a 1021 a re-read settles the
//!   outcome once and for all; after a 1031 the batch may still land **later**, so a
//!   re-read that observes nothing proves nothing.
//! * **The nemesis is not batch-shape-aware.** It strikes a **blind**
//!   (precondition-free) batch exactly as readily as a conditional one, because
//!   production classifies them identically. The `conditional` flag governs only
//!   `1020 not_committed` → `Conflict` (`:216-218`), never the undeterminable codes.
//!   The four-phase write protocol's pending-ledger put is a blind batch, so it too can
//!   come back ambiguous; `crates/dst/tests/commit_ambiguity.rs` drives that leg.
//! * **The fidelity claimed.** Optimistic conflict at commit (the `1020 not_committed`
//!   class) plus a seed-selected commit ambiguity over a plain `BTreeMap` rather than a
//!   versioned MVCC keyspace. MVCC fidelity buys nothing for this trait: the version
//!   compare-and-set is a *full-value* precondition and FDB stores keys/values
//!   byte-identically (`crates/metadata-fdb/src/lib.rs:873-875`). As with the
//!   simulated-TiKV choice above, **the human ratifies this at sign-off.**
//! * **No `Undeterminable` `CommitOutcome`.** The production driver models the
//!   ambiguous outcome as `Err(classify::CommitUnknownResult)`, never `Ok(Conflict)`
//!   (`crates/metadata-fdb/src/lib.rs:67-73`, `:150-153`). The model reproduces exactly
//!   that; adding a third `CommitOutcome` variant would make the simulation *less*
//!   faithful, not more.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_traits::{BoxError, CommitOutcome, MetadataStore, Precondition, Result, WriteBatch};

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
    async fn commit_await_inside(&self, batch: &WriteBatch) -> Result<CommitOutcome> {
        let keys = write_set(batch);
        // A batch WITH preconditions is a CAS: losing a lock race is the trait's
        // `Conflict` that the caller re-reads and retries. A precondition-FREE (blind)
        // batch has no precondition to have failed, so a lost race must NOT surface as
        // `Conflict` — the blind writers that use `?` and ignore the `CommitOutcome`
        // (core::write::intent -> metadata::put_pending / sweep_pending) would read it
        // as success and silently drop the write. It stays `Err`, mirroring the real
        // adapter's `conflict_or_err` (crates/metadata-tikv/src/lib.rs:382-414).
        let conditional = !batch.preconditions.is_empty();

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
                return if conditional {
                    Ok(CommitOutcome::Conflict)
                } else {
                    Err(BoxError::from(
                        "simulated-TiKV: blind (precondition-free) batch lost a \
                         write-write lock race — surfaced as Err, never a silent Conflict",
                    ))
                };
            }
            if !preconditions_hold(&inner.truth, &batch.preconditions) {
                // Only reachable with preconditions present, so this is a true CAS miss.
                return Ok(CommitOutcome::Conflict);
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
        Ok(CommitOutcome::Committed)
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
        match self.fidelity {
            Fidelity::SynchronousRedbShaped => Ok(self.commit_synchronous(&batch)),
            Fidelity::AwaitInsideCommit => self.commit_await_inside(&batch).await,
        }
    }
}

// ───────────────────── the simulated-FoundationDB model (#468) ─────────────────────

/// FDB error `1020 not_committed` — a lost read-write race. Mirrors
/// `crates/metadata-fdb/src/lib.rs:148`.
pub const SIM_NOT_COMMITTED: i32 = 1020;

/// FDB error `1021 commit_unknown_result` — the commit may or may not have landed, and
/// the transaction is **out of flight**, so a re-read settles it once and for all.
/// Mirrors `crates/metadata-fdb/src/lib.rs:153` and the `may_still_commit` = `false`
/// arm at `:247-249`.
pub const SIM_COMMIT_UNKNOWN_RESULT: i32 = 1021;

/// FDB error `1031 transaction_timed_out` — the commit may or may not have landed **and
/// may still land afterwards**: "Where 1021 promises the transaction is out of flight,
/// 1031 promises nothing" (`crates/metadata-fdb/src/lib.rs:165`). Mirrors `:172` and the
/// `may_still_commit` = `true` arm at `:247-249`.
pub const SIM_TRANSACTION_TIMED_OUT: i32 = 1031;

/// The simulated undeterminable commit outcome: the batch **may or may not** have been
/// applied and the client cannot tell.
///
/// A distinguishable error type carrying the FDB code, not a `String` — mirroring the
/// production driver's downcastable `classify::CommitUnknownResult`
/// (`crates/metadata-fdb/src/lib.rs:233-237`, "Errors a caller can tell apart" at
/// `:110-117`) — so a scenario settles an ambiguous commit by *type*, never by
/// string-matching a message, and can tell the two codes apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimCommitUnknownResult {
    /// The FDB error code that reported the undeterminable outcome: either
    /// [`SIM_COMMIT_UNKNOWN_RESULT`] or [`SIM_TRANSACTION_TIMED_OUT`].
    pub code: i32,
}

impl SimCommitUnknownResult {
    /// Whether the cluster may still apply this batch **after** the error was returned —
    /// the byte-for-byte rule of `classify::CommitUnknownResult::may_still_commit`
    /// (`crates/metadata-fdb/src/lib.rs:247-249`).
    ///
    /// `false` for 1021, whose guarantee is that the transaction is already out of
    /// flight. `true` for 1031, where the commit may have been sent and may land later:
    /// a re-read that observes nothing does **not** prove nothing will land.
    pub fn may_still_commit(self) -> bool {
        self.code == SIM_TRANSACTION_TIMED_OUT
    }
}

impl fmt::Display for SimCommitUnknownResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "simulated-FDB: commit returned an unknown result (error {}): the batch may \
             or may not have been applied; the caller must re-read to establish what \
             happened",
            self.code,
        )?;
        if self.may_still_commit() {
            write!(
                f,
                ". It timed out rather than reporting an unknown result, so it may still \
                 be applied AFTER this error: a re-read that observes nothing does not \
                 prove the batch will never land.",
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for SimCommitUnknownResult {}

/// One `u64` from a `ChaCha8Rng`, without pulling the `rand::Rng` trait into scope at
/// every call site (the same helper shape as `crates/dst/tests/network.rs:521-525`).
fn rng_u64(rng: &mut rand_chacha::ChaCha8Rng) -> u64 {
    use rand::Rng;
    rng.next_u64()
}

/// How faithfully the simulated-FDB model renders a commit's failure modes.
///
/// All three share the same *optimistic* commit shape — FDB takes no lock; the resolver
/// decides at commit time — which is what distinguishes this model from
/// [`SimTikvMetadataStore`]'s pessimistic prewrite lock-grab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FdbFidelity {
    /// The resolver rejects a conditional batch at commit time (the `1020 not_committed`
    /// class) and every commit outcome is *determinate*. The nemesis cannot be armed on
    /// this fidelity, so it is the mode the shared contract suite runs against: those
    /// clauses legitimately assume a determinate commit, and arming the nemesis there
    /// would be testing the suite, not the store.
    OptimisticConflictAtCommit,
    /// The above **plus** the commit-ambiguity nemesis (armed with
    /// [`SimFdbMetadataStore::arm_commit_ambiguity`]): on a commit the resolver accepted,
    /// the reply is lost. The model decides from the seed whether the mutation landed —
    /// and, for [`SIM_TRANSACTION_TIMED_OUT`], whether it is still in flight and lands
    /// *later* — then returns [`SimCommitUnknownResult`] **without telling the caller
    /// which**. When the batch does land it is applied **whole**: one atomic step.
    CommitUnknownResult,
    /// The **violating twin** of [`Self::CommitUnknownResult`] (cf. the violating stores
    /// in `crates/metadata-conformance/tests/demonstrated_red.rs`): an ambiguous commit
    /// that lands applies only the **first put** of its batch and drops the rest — a
    /// torn, non-atomic apply.
    ///
    /// For `core::metadata::commit_chunk_map_superseding`
    /// (`crates/core/src/metadata.rs:474-488`) that batch is `put(inode)` followed by one
    /// `orphan:<dserver>:<chunk>:<index>` record per fragment of the **superseded** chunk
    /// map, so a torn apply publishes the new inode while the old object's fragments are
    /// never orphaned: the custodian GC never sees them and they leak forever. This
    /// fidelity exists solely so the atomicity assertion in
    /// `crates/dst/tests/commit_ambiguity.rs` is shown to *catch* a torn apply, rather
    /// than resting green on the model's inability to produce one.
    TornApplyOnAmbiguity,
    /// The **violating twin** of [`Self::CommitUnknownResult`] for the *deferral* path: a
    /// `1031`-timed-out batch left in flight that lands **later** does so **without**
    /// re-checking its preconditions against current truth — it skips the resolver.
    ///
    /// FoundationDB re-runs a transaction's read-conflict set at commit time even for a
    /// commit that lands after a timeout (`crates/metadata-fdb/src/lib.rs:161-166`), so a
    /// deferred batch a later writer has since beaten **must** be rejected. This twin
    /// omits that re-check on the forced deferral ([`SimFdbMetadataStore::quiesce`]), so a
    /// stale batch clobbers the writer that already won — the demonstrated red that keeps
    /// the resolver re-check in [`FdbInner::settle_in_flight`] from resting on the model's
    /// good behaviour. Every other path (the commit-time apply, an opportunistic settle)
    /// is faithful; only the forced deferred landing is broken.
    DeferredResolverSkipped,
}

/// The armed commit-ambiguity nemesis: strike the next `budget` commits the resolver
/// **accepts**, raising `code`.
///
/// A budget, not a per-commit coin: the *schedule* (which writer reaches the resolver
/// first) and the *outcome* (whether the struck commit landed, and when) are the
/// seed-derived degrees of freedom, so every seed spends the budget and explores one
/// point of the ambiguity space. A per-commit coin would instead waste most seeds on no
/// fault at all. A scenario arms the nemesis at the point in the four-phase protocol whose
/// commits it wants ambiguous — the nemesis itself is deliberately **not** batch-shape
/// aware (see the module docs).
#[derive(Debug, Clone, Copy)]
struct Nemesis {
    code: i32,
    budget: u64,
}

/// Per-simulation observation of the ambiguity actually exercised. Instance state (never
/// a `static`), exactly as [`Observations`] — so it lives inside madsim's simulated world
/// and cannot leak across seeds/threads (ADR-0035).
///
/// A sweep that never armed the nemesis is *visibly* vacuous: `ambiguous_commits` stays 0.
#[derive(Debug, Default, Clone, Copy)]
pub struct FdbObservations {
    /// Commits struck by the nemesis — the resolver accepted them, then the reply was lost.
    pub ambiguous_commits: u64,
    /// Of those, the ones carrying **no** precondition (the four-phase protocol's
    /// pending-ledger put/delete). Production classifies these identically to conditional
    /// batches (`crates/metadata-fdb/src/lib.rs:212-215`); a model that exempted them
    /// would leave this counter at 0 forever.
    pub ambiguous_blind_commits: u64,
    /// Of those, the ones carrying preconditions (the version CAS).
    pub ambiguous_conditional_commits: u64,
    /// Struck commits whose mutation the seed decided **did** land at the moment of the
    /// error. This is the half of the ambiguity space an observer that "assumes not
    /// committed" gets wrong.
    pub ambiguous_commits_that_landed: u64,
    /// Struck `1031` commits the seed left **in flight**: not applied at the moment of the
    /// error, and still able to land afterwards. Always 0 for `1021`, whose guarantee is
    /// that the transaction is out of flight.
    pub commits_left_in_flight: u64,
    /// In-flight commits the cluster later applied — *after* the caller's settling re-read
    /// could have observed nothing. The half of the `1031` space `may_still_commit`
    /// exists to warn about.
    pub deferred_landings: u64,
    /// In-flight commits the resolver later **rejected**, because another writer moved a
    /// key in their read-conflict set first.
    pub deferred_rejections: u64,
    /// Conditional batches the resolver rejected outright (the `1020` class).
    pub resolver_conflicts: u64,
    /// Ambiguous commits applied **torn** — only ever non-zero on the violating
    /// [`FdbFidelity::TornApplyOnAmbiguity`].
    pub torn_applies: u64,
}

#[derive(Debug)]
struct FdbInner {
    /// The committed key/value state. A plain map, not a versioned MVCC keyspace — see
    /// the fidelity note in the module docs.
    truth: BTreeMap<Vec<u8>, Bytes>,
    fidelity: FdbFidelity,
    nemesis: Option<Nemesis>,
    /// Batches a `1031 transaction_timed_out` left **in flight**. FoundationDB's own guide:
    /// "if the commit has already been sent to the database, the transaction could get
    /// committed at a later point in time" (`crates/metadata-fdb/src/lib.rs:161-164`).
    /// Each is still subject to the resolver when it lands, so it can be rejected then.
    in_flight: Vec<WriteBatch>,
    /// Seed-derived, so a bug-finding seed replays the *same* fault decisions (ADR-0009).
    /// Never wall-clock, never thread scheduling.
    rng: rand_chacha::ChaCha8Rng,
    obs: FdbObservations,
}

impl FdbInner {
    /// A fair, seed-derived coin.
    fn coin(&mut self) -> bool {
        rng_u64(&mut self.rng).is_multiple_of(2)
    }

    /// Apply a batch the nemesis struck and the seed decided landed — **whole** on the
    /// faithful fidelity, **torn** on the violating one.
    fn apply_landed(&mut self, batch: &WriteBatch) {
        if self.fidelity == FdbFidelity::TornApplyOnAmbiguity {
            if let Some((key, value)) = batch.puts.first() {
                self.truth.insert(key.clone(), value.clone());
            }
            self.obs.torn_applies += 1;
        } else {
            apply(&mut self.truth, batch);
        }
        self.obs.ambiguous_commits_that_landed += 1;
    }

    /// Give every still-in-flight `1031` batch a chance to land, *now*, at whatever store
    /// operation happens to run. `force` resolves all of them (used by
    /// [`SimFdbMetadataStore::quiesce`]); otherwise each lands on a seed-derived coin.
    ///
    /// A landing batch goes through the **resolver** first: its preconditions are
    /// re-checked against the current truth, exactly as FDB re-checks a transaction's
    /// read-conflict set at commit time — so a deferred CAS that another writer has since
    /// beaten is rejected, and "exactly one writer wins" survives the deferral.
    ///
    /// Returns immediately, drawing **no** randomness, when nothing is in flight — so a
    /// nemesis-free run (the shared contract suite) is bit-for-bit unperturbed.
    fn settle_in_flight(&mut self, force: bool) {
        if self.in_flight.is_empty() {
            return;
        }
        // The violating twin skips the resolver's read-conflict re-check on the *forced*
        // deferral, so a stale batch lands where the faithful model rejects it — the red
        // that proves this re-check is load-bearing, not decorative.
        let skip_resolver = force && self.fidelity == FdbFidelity::DeferredResolverSkipped;
        for batch in std::mem::take(&mut self.in_flight) {
            if !force && !self.coin() {
                self.in_flight.push(batch);
                continue;
            }
            if skip_resolver || preconditions_hold(&self.truth, &batch.preconditions) {
                apply(&mut self.truth, &batch);
                self.obs.deferred_landings += 1;
            } else {
                self.obs.deferred_rejections += 1;
            }
        }
    }
}

/// A deterministic **simulated-FoundationDB** `MetadataStore` (see module docs).
///
/// Not a real or linked FoundationDB: `libfdb_c` owns a network thread and cannot live
/// inside the simulator (ADR-0009/ADR-0035). This is a small, in-memory, seed-reproducible
/// *model* of the one thing neither redb nor simulated-TiKV renders — a commit whose
/// outcome the client **cannot determine** (`crates/metadata-fdb/src/lib.rs:67-73`,
/// `:159-166`).
///
/// Instance state under a `Mutex`, never a `static` (ADR-0035), exactly as
/// [`SimTikvMetadataStore`].
#[derive(Debug)]
pub struct SimFdbMetadataStore {
    inner: Mutex<FdbInner>,
}

impl SimFdbMetadataStore {
    /// A fresh, empty store whose commits are **determinate**
    /// ([`FdbFidelity::OptimisticConflictAtCommit`]); the nemesis cannot be armed on it.
    pub fn new() -> Self {
        Self::with_fidelity(FdbFidelity::OptimisticConflictAtCommit)
    }

    /// A fresh, empty store at the given [`FdbFidelity`]. The nemesis starts **disarmed**
    /// on every fidelity; a scenario arms it once its fixture is in place, so ambiguity
    /// strikes the commits under test rather than the fixture build.
    ///
    /// Must be constructed **inside** a madsim runtime: the fault RNG is seeded from the
    /// run seed (`madsim::runtime::Handle::current().seed()`), the same seam
    /// `crates/dst/tests/network.rs:513-519` uses for its fault selection.
    pub fn with_fidelity(fidelity: FdbFidelity) -> Self {
        use rand::SeedableRng;
        Self {
            inner: Mutex::new(FdbInner {
                truth: BTreeMap::new(),
                fidelity,
                nemesis: None,
                in_flight: Vec::new(),
                rng: rand_chacha::ChaCha8Rng::seed_from_u64(
                    madsim::runtime::Handle::current().seed(),
                ),
                obs: FdbObservations::default(),
            }),
        }
    }

    /// Arm the commit-ambiguity nemesis with `code` for the next `budget` commits the
    /// resolver **accepts** — blind or conditional alike.
    ///
    /// `code` must be [`SIM_COMMIT_UNKNOWN_RESULT`] or [`SIM_TRANSACTION_TIMED_OUT`]: the
    /// exact pair production classifies as `CommitClass::UnknownResult`
    /// (`crates/metadata-fdb/src/lib.rs:212-215`).
    pub fn arm_commit_ambiguity(&self, code: i32, budget: u64) {
        assert!(
            code == SIM_COMMIT_UNKNOWN_RESULT || code == SIM_TRANSACTION_TIMED_OUT,
            "simulated-FDB: {code} is not an undeterminable commit code (1021 or 1031)"
        );
        let mut inner = self.inner.lock().unwrap();
        assert_ne!(
            inner.fidelity,
            FdbFidelity::OptimisticConflictAtCommit,
            "simulated-FDB: the ambiguity nemesis needs an ambiguity-capable fidelity — \
             build the store with FdbFidelity::CommitUnknownResult (or the violating \
             TornApplyOnAmbiguity twin)"
        );
        inner.nemesis = Some(Nemesis { code, budget });
    }

    /// A snapshot of what this store observed during the run.
    pub fn observations(&self) -> FdbObservations {
        self.inner.lock().unwrap().obs
    }

    /// How many `1031`-ambiguous commits are still in flight — able to land at any later
    /// point. Always 0 after a `1021`, whose guarantee is that the transaction is out of
    /// flight (`crates/metadata-fdb/src/lib.rs:242-245`).
    pub fn in_flight(&self) -> usize {
        self.inner.lock().unwrap().in_flight.len()
    }

    /// Force every still-in-flight commit through the resolver, so the store reaches its
    /// terminal state. The cluster gets there on its own eventually; a test needs to say
    /// *when*, to assert a terminal invariant without racing the deferral.
    pub fn quiesce(&self) {
        self.inner.lock().unwrap().settle_in_flight(true);
    }

    /// The optimistic commit: take a read version (a round-trip), send the commit RPC (a
    /// second round-trip), then let the resolver decide — atomically, no await.
    ///
    /// FDB takes **no lock**: a writer stages its mutations locally and the resolver
    /// rejects it at commit time if a key in its *read-conflict set* moved. That is the
    /// structural difference from `SimTikvMetadataStore::commit_await_inside`, whose
    /// winner is decided at a pessimistic prewrite lock-grab.
    async fn commit_optimistic(&self, batch: &WriteBatch) -> Result<CommitOutcome> {
        let conditional = !batch.preconditions.is_empty();

        // Phase 1 — `get_read_version`: a network round-trip. Every concurrent writer
        // yields here, and — unlike TiKV's prewrite — takes nothing with it.
        network_hop().await;

        // Phase 2 — the commit RPC. The client is now in flight and cannot observe the
        // resolver's verdict until the reply arrives. This is the window 1021/1031 live in.
        network_hop().await;

        // Phase 3 — resolver + apply: ONE atomic step, no await inside, so a batch is
        // all-or-nothing and no torn/hybrid state is ever observable.
        let mut inner = self.inner.lock().unwrap();
        inner.settle_in_flight(false);

        if !preconditions_hold(&inner.truth, &batch.preconditions) {
            // `1020 not_committed` (or the observed miss read inside the transaction):
            // the trait's `Conflict`, and — matching `classify_commit_error`'s
            // `conditional && code == NOT_COMMITTED` guard
            // (`crates/metadata-fdb/src/lib.rs:216-218`) — reachable only for a
            // conditional batch. An empty precondition list holds vacuously, exactly as
            // FDB's resolver cannot reject a write-only transaction whose read-conflict
            // set is empty (`:62-64`). The assert is defence-in-depth, structurally
            // unreachable, and stated plainly as such: a blind batch must NEVER escape as
            // `Conflict`, or the many callers that `?` the result and ignore the
            // `CommitOutcome` would read a dropped write as success
            // (`crates/traits/src/lib.rs:346-350`).
            assert!(
                conditional,
                "simulated-FDB: a precondition-free batch reached the Conflict arm — a \
                 blind write must never surface as Ok(Conflict)"
            );
            inner.obs.resolver_conflicts += 1;
            return Ok(CommitOutcome::Conflict);
        }

        // The resolver ACCEPTED. Now — and only now — the reply may be lost. Note what is
        // NOT tested here: the batch's shape. Production returns `UnknownResult` for
        // 1021/1031 for *every* batch, conditional or not — the code check returns before
        // the `conditional` check (`crates/metadata-fdb/src/lib.rs:212-215`) — so a blind
        // pending-ledger put is exactly as ambiguous as a version CAS.
        let strike = match &mut inner.nemesis {
            Some(nemesis) if nemesis.budget > 0 => {
                nemesis.budget -= 1;
                Some(nemesis.code)
            }
            _ => None,
        };

        let Some(code) = strike else {
            apply(&mut inner.truth, batch);
            return Ok(CommitOutcome::Committed);
        };

        inner.obs.ambiguous_commits += 1;
        if conditional {
            inner.obs.ambiguous_conditional_commits += 1;
        } else {
            inner.obs.ambiguous_blind_commits += 1;
        }

        match code {
            // 1021: the transaction is out of flight. Its outcome is fixed at this
            // instant — landed or not — and a re-read settles it once and for all.
            SIM_COMMIT_UNKNOWN_RESULT => {
                if inner.coin() {
                    inner.apply_landed(batch);
                }
            }
            // 1031: "promises nothing" (`crates/metadata-fdb/src/lib.rs:165`). Three
            // seed-selected fates: landed already, still in flight (may land after the
            // caller's re-read), or never sent at all.
            SIM_TRANSACTION_TIMED_OUT => match rng_u64(&mut inner.rng) % 3 {
                0 => inner.apply_landed(batch),
                1 => {
                    inner.in_flight.push(batch.clone());
                    inner.obs.commits_left_in_flight += 1;
                }
                _ => {}
            },
            other => unreachable!("simulated-FDB: unknown ambiguity code {other}"),
        }

        // Never `Ok(Conflict)`, never retried by the driver: a `WriteBatch` is not
        // guaranteed idempotent (`crates/metadata-fdb/src/lib.rs:67-69`).
        Err(BoxError::from(SimCommitUnknownResult { code }))
    }
}

impl Default for SimFdbMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetadataStore for SimFdbMetadataStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // A read version plus a `get`: a network round-trip.
        network_hop().await;
        let mut inner = self.inner.lock().unwrap();
        inner.settle_in_flight(false);
        Ok(inner.truth.get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        network_hop().await;
        let mut inner = self.inner.lock().unwrap();
        inner.settle_in_flight(false);
        Ok(inner
            .truth
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        self.commit_optimistic(&batch).await
    }
}
