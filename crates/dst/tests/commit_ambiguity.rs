//! **Commit ambiguity** — the one failure shape FoundationDB introduces that neither redb
//! nor the simulated-TiKV model can render, driven inside the deterministic simulator
//! (issue #468).
//!
//! An FDB commit can come back saying *"the batch may or may not have been applied and I
//! cannot tell you which"*. The production driver surfaces that as a distinguishable `Err`
//! carrying the FDB code — never `Ok(Conflict)`, never retried, because a `WriteBatch` is
//! not guaranteed idempotent (`crates/metadata-fdb/src/lib.rs:67-73`, `:150-153`). The
//! class has **two** members and they are not equally bad
//! (`crates/metadata-fdb/src/lib.rs:159-166`, `:240-249`):
//!
//! * **`1021 commit_unknown_result`** — the transaction is *out of flight*. A re-read
//!   settles the outcome once and for all.
//! * **`1031 transaction_timed_out`** — "promises nothing" (`:165`). The commit may have
//!   been sent and may land *after* the error, so a re-read that observes nothing does
//!   **not** prove nothing will land.
//!
//! Neither can be tested against a real cluster: *"A healthy `fdbserver` cannot be made to
//! emit 1021 on demand"* (`crates/metadata-fdb/src/lib.rs:71`). A real fault battery can
//! only *sample* the ambiguity space; a seed-driven nemesis inside madsim **searches** it —
//! which is exactly why the simulated-FDB model exists.
//!
//! ## Four legs, because the ambiguity class has four faces
//!
//! 1. **The version CAS under `1021`** (`ambiguous_cas_settles_over`). Four writers race
//!    the version-conditional commit with the nemesis armed. Every `Err` is settled by a
//!    re-read; then exactly one writer won, the version bumped exactly once, and the
//!    landed batch was applied *whole*.
//! 2. **The blind pending-ledger put under `1021`** (`ambiguous_pending_put_over`). The
//!    nemesis is **not** batch-shape aware — production classifies a precondition-free
//!    batch identically (`crates/metadata-fdb/src/lib.rs:212-215`: the code check returns
//!    *before* the `conditional` check) — so the four-phase protocol's Intent phase can
//!    come back ambiguous too. Assuming it landed leaves a written chunk with no ledger
//!    entry, and the custodian GC reclaims its fragments as unreferenced garbage.
//! 3. **The single-writer CAS under `1031`** (`timed_out_commit_over`). The batch may
//!    still land after the "settling" re-read, so an observer that treats 1031 like 1021
//!    is wrong.
//! 4. **The version CAS under `1031`, *under contention*** (`contended_cas_under_1031_over`
//!    and `deferred_1031_settles_against_current_truth`). Four writers race the CAS with
//!    the `1031` nemesis armed, so struck batches are left **in flight** and land — or are
//!    rejected — *later*, at the deferral, after a different writer may already have won.
//!    The property: even so, exactly one writer wins and the version bumps exactly once,
//!    because the deferral re-runs each batch through the resolver
//!    (`support/mod.rs` `settle_in_flight`) and rejects every stale one. The demonstrated
//!    red is the violating `FdbFidelity::DeferredResolverSkipped` twin, which omits that
//!    re-check and lets a stale batch clobber the writer that already won.
//!
//! Each leg ships its **demonstrated red** (cf.
//! `crates/metadata-conformance/tests/demonstrated_red.rs`): a violating observer, or the
//! violating `FdbFidelity::TornApplyOnAmbiguity` store, that the assertion catches. Nothing
//! here is asserted by a tautology.
//!
//! ## Why this file, and not `concurrency.rs`
//!
//! `concurrency.rs`'s `exactly_one_writer_wins_over` (`:75`) is the **shared** property body
//! driving redb and simulated-TiKV, and it `.unwrap()`s the commit result (`:106`). That
//! strictness is correct *for those stores* — an `Err` from their `commit` genuinely is a
//! bug — so it must not be weakened to accommodate an ambiguity-capable store. The ambiguity
//! scenario therefore gets its own property body here.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`, which sweeps 50 seeds); without it
//! this file compiles to nothing, so a normal `cargo test` neither builds nor runs it. The
//! structural companion guard, `crates/dst/tests/no_fdb_linkage.rs`, is deliberately *not*
//! madsim-gated.
#![cfg(madsim)]

use std::sync::Arc;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::{ChunkRef, EcScheme, InodeRecord};
use wyrd_core::{metadata, read, write};
use wyrd_traits::{BoxError, CommitOutcome, MetadataStore};

#[path = "support/mod.rs"]
mod support;
use support::{
    FdbFidelity, FdbObservations, SimCommitUnknownResult, SimFdbMetadataStore,
    SIM_COMMIT_UNKNOWN_RESULT, SIM_TRANSACTION_TIMED_OUT,
};

const CHUNK: usize = 4;
const LEASE_EXPIRY: u64 = 6_000;
const ORPHANED_AT: u64 = 0;
// Exercise the ambiguous commit over the default erasure-coded data path (n = 9 fragments
// per chunk), exactly as `concurrency.rs:52` does — so the superseding batch carries nine
// `orphan:` records alongside its inode put, and a torn apply is observable.
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

/// How many seeds the plain-`#[test]` sweeps below drive. `cargo xtask dst` sweeps its own
/// 50 seeds over the `#[madsim::test]` entry point; these explicit sweeps additionally need
/// to see **every** fate of an ambiguous commit (landed / not landed / still in flight),
/// which a single seed cannot show.
const AMBIGUITY_SWEEP_SEEDS: u64 = 64;

/// A unique, deterministic chunk-id generator starting just above `base`. Each writer gets
/// a disjoint id range, so an inode's `chunk_map` **identifies** the writer that wrote it —
/// which is what lets a settling re-read decide whether *this* writer's ambiguous commit
/// landed.
fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// The `Err` a simulated-FDB commit may legitimately return, by **type** — never by
/// string-matching a message. Any other `Err` (a blind batch losing a race, a fault) is a
/// bug and fails here.
fn expect_ambiguous(err: &BoxError) -> SimCommitUnknownResult {
    *err.downcast_ref::<SimCommitUnknownResult>()
        .unwrap_or_else(|| panic!("a simulated-FDB commit returned a non-ambiguous Err: {err}"))
}

/// How many `orphan:` records the superseding batch of an overwrite over `prior` stages —
/// one per fragment of the chunk map it supersedes
/// (`core::metadata::commit_chunk_map_superseding`, `crates/core/src/metadata.rs:477-488`).
fn orphan_records_for(prior: &InodeRecord) -> usize {
    prior
        .chunk_map
        .iter()
        .map(|chunk| usize::from(chunk.fragment_count()))
        .sum()
}

/// The atomicity invariant, checked against the **store's own** state rather than the
/// observer's verdict — so it is independent of the observer under test.
///
/// `commit_chunk_map_superseding` stages the inode put and every `orphan:` record of the
/// superseded chunk map in **one** `WriteBatch`. Observing the version bump without the
/// orphan records — or the records without the bump — is a torn, non-atomic apply: the
/// superseded object's fragments would never be reclaimed (the custodian GC reads the
/// orphan ledger), or would be reclaimed while still referenced. The violating
/// `FdbFidelity::TornApplyOnAmbiguity` store produces exactly the first, and this catches it.
async fn assert_batch_applied_whole(
    meta: &SimFdbMetadataStore,
    prior: &InodeRecord,
    settled: &InodeRecord,
) {
    let orphans = meta.scan(b"orphan:").await.unwrap().len();
    let expected = if settled.version > prior.version {
        orphan_records_for(prior)
    } else {
        0
    };
    assert_eq!(
        orphans, expected,
        "an ambiguous commit must apply its batch WHOLE: the inode moved {} -> {} but the \
         store holds {orphans} orphan records where the same batch staged {expected} — a \
         torn/hybrid state is observable",
        prior.version, settled.version,
    );
}

/// The settled inode's chunk map must be a **whole** writer's plan (or the untouched
/// prior's), never a hybrid of several. A merged map is a member of neither set.
fn assert_chunk_map_is_whole(settled: &InodeRecord, prior: &InodeRecord, plans: &[Vec<ChunkRef>]) {
    let whole = std::iter::once(&prior.chunk_map).chain(plans.iter());
    assert!(
        whole.into_iter().any(|map| *map == settled.chunk_map),
        "the settled inode's chunk map matches neither the prior nor any single writer's \
         plan — it is a torn/hybrid of several writers"
    );
}

// ─────────────────── leg 1: the version CAS under 1021 ───────────────────

/// How an observer treats a commit that returned `1021 commit_unknown_result`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Observer {
    /// The **correct** move: re-read the inode and let the store's own settled state decide
    /// whether the ambiguous commit landed.
    SettlingReRead,
    /// The **violating** observer (the demonstrated red): assume an ambiguous commit did not
    /// land — i.e. count it a loser without re-reading. Half the ambiguity space makes that
    /// assumption false.
    AssumeNotCommitted,
}

/// The ambiguity property body. Four writers stage independently, then race the
/// version-conditional CAS with the `1021` nemesis armed. Every `Err` is settled by
/// re-reading the inode (or, for the violating observer, *not*), and the invariant is
/// asserted over the settled set.
///
/// Note what is **not** done here: no commit result is `.unwrap()`ed, and no `Err` is turned
/// into a winner or a loser before the settling read. That is the property.
async fn ambiguous_cas_settles_over(meta: Arc<SimFdbMetadataStore>, observer: Observer) {
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    // An existing object at version 1. The nemesis is disarmed, so the fixture's own commits
    // are determinate — the ambiguity under test is the four-writer race, not the fixture.
    let v0 = write::plan_write(b"v0", CHUNK, RS, ids_from(1)).unwrap();
    write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &v0).await.unwrap();
    write::commit_create(&*meta, 0, "obj", 1, &v0)
        .await
        .unwrap();
    write::release(&*meta, &v0).await.unwrap();
    let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();
    assert_eq!(
        meta.scan(b"orphan:").await.unwrap().len(),
        0,
        "a create stages no orphan records; the fixture must start clean"
    );

    // Stage every writer's Intent + Data phase BEFORE arming, so the first commits the
    // resolver accepts after arming are the version-CAS batches this leg is about. The
    // nemesis itself is NOT batch-shape aware — it strikes blind batches just as readily,
    // which is what `ambiguous_pending_put_over` below drives.
    let mut plans = Vec::new();
    for i in 0..4u128 {
        let plan = write::plan_write(b"contended", CHUNK, RS, ids_from(0x1000 * (i + 1))).unwrap();
        write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
        write::write_fragments(&*chunks, &plan).await.unwrap();
        plans.push(plan);
    }
    let staged: Vec<Vec<ChunkRef>> = plans.iter().map(write::WritePlan::chunk_refs).collect();

    // Arm the 1021 nemesis with a budget that covers every writer: each commit the resolver
    // accepts becomes ambiguous, and the seed decides whether its mutation landed.
    meta.arm_commit_ambiguity(SIM_COMMIT_UNKNOWN_RESULT, 4);

    let mut handles = Vec::new();
    for plan in plans {
        let meta = Arc::clone(&meta);
        let prior = prior.clone();
        handles.push(madsim::task::spawn(async move {
            // NOT unwrapped: this commit may legitimately be ambiguous.
            let outcome = write::commit_overwrite(&*meta, 1, &prior, &plan, ORPHANED_AT).await;
            (outcome, plan.chunk_refs())
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // ── settle ─────────────────────────────────────────────────────────────────────────
    // 1021's guarantee is that the transaction is OUT OF FLIGHT: nothing can land after the
    // error, so ONE re-read settles every ambiguous commit for good.
    assert_eq!(
        meta.in_flight(),
        0,
        "a 1021 commit must leave nothing in flight (crates/metadata-fdb/src/lib.rs:242-245)"
    );
    let settled = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    let mut winners: u64 = 0;
    for (outcome, chunk_map) in &results {
        let won = match outcome {
            Ok(CommitOutcome::Committed) => true,
            Ok(CommitOutcome::Conflict) => false,
            Err(err) => {
                let ambiguous = expect_ambiguous(err);
                assert_eq!(ambiguous.code, SIM_COMMIT_UNKNOWN_RESULT);
                assert!(
                    !ambiguous.may_still_commit(),
                    "1021 promises the transaction is out of flight"
                );
                match observer {
                    // Correct: the store's settled state decides. Chunk ids are disjoint per
                    // writer, so an exact chunk-map match means *this* writer's commit landed.
                    Observer::SettlingReRead => settled.chunk_map == *chunk_map,
                    // Violating: assume it did not land.
                    Observer::AssumeNotCommitted => false,
                }
            }
        };
        if won {
            winners += 1;
        }
    }

    // ── the invariant ──────────────────────────────────────────────────────────────────
    // (ii) once the re-read settles it: exactly one writer won and the version bumped once.
    //
    // The load-bearing clause. An ambiguous commit that LANDED but was counted a loser (or
    // vice versa) shows up exactly here: the store bumped the version, the observer counted
    // nobody. This is the assertion the `AssumeNotCommitted` observer trips.
    assert_eq!(
        settled.version - prior.version,
        winners,
        "the settled winner count ({winners}) must equal the inode's version bump ({} -> {}) \
         — an ambiguous commit was counted without a settling re-read",
        prior.version,
        settled.version,
    );
    assert!(
        settled.version - prior.version <= 1,
        "at most one version-conditional commit may land: {} -> {}",
        prior.version,
        settled.version,
    );

    // (iii) no torn/hybrid state is observable. Two independent checks; the violating
    // `TornApplyOnAmbiguity` store trips the second.
    assert_chunk_map_is_whole(&settled, &prior, &staged);
    assert_batch_applied_whole(&meta, &prior, &settled).await;

    if winners == 1 {
        // …and the object reassembles to the winner's bytes, end to end.
        let bytes = read::read_path(&*meta, &*chunks, 0, "obj").await.unwrap();
        assert_eq!(bytes.as_deref(), Some(&b"contended"[..]));
    } else {
        assert_eq!(
            settled, prior,
            "with no winner the inode must be byte-identical to the prior — a commit that \
             did not land must leave nothing behind"
        );
    }
}

/// Drive the four-writer ambiguity race at one fixed seed and return what the store observed.
/// Mirrors `concurrency.rs:158`'s `run_pinned_race`.
fn run_cas_ambiguity(seed: u64, fidelity: FdbFidelity, observer: Observer) -> FdbObservations {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(fidelity));
        ambiguous_cas_settles_over(Arc::clone(&meta), observer).await;
        meta.observations()
    })
}

/// **The seed sweep `cargo xtask dst` drives** (`MADSIM_TEST_NUM=50`, `xtask/src/main.rs:1337`):
/// with the 1021 nemesis armed, no invariant is violated at any seed — an ambiguous commit is
/// never counted as a winner *or* a loser without a settling re-read; once settled, exactly one
/// writer won and the version bumped exactly once; the landed batch was applied whole.
///
/// The `ambiguous_conditional_commits >= 1` assertion keeps the sweep honest: a seed where the
/// nemesis never fired would prove nothing, and this fails loudly rather than passing vacuously.
#[madsim::test]
async fn commit_ambiguity_invariants_hold_under_the_dst_seed_sweep() {
    let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
        FdbFidelity::CommitUnknownResult,
    ));
    ambiguous_cas_settles_over(Arc::clone(&meta), Observer::SettlingReRead).await;

    let obs = meta.observations();
    assert!(
        obs.ambiguous_conditional_commits >= 1,
        "the 1021 nemesis must strike at least one version-CAS commit — a sweep that never \
         armed it is vacuous"
    );
}

/// The sweep is only meaningful if it actually explores **both halves** of the ambiguity
/// space: seeds where the ambiguous commit landed (so the settling re-read must count it a
/// winner) and seeds where it did not (so the re-read must count it a loser and let a later
/// writer win). This asserts both occur, and — because `run_cas_ambiguity` asserts the full
/// invariant at every seed — that the invariant holds across all of them.
#[test]
fn the_settling_re_read_covers_both_halves_of_the_ambiguity_space() {
    let mut landed = 0u64;
    let mut did_not_land = 0u64;
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        let obs = run_cas_ambiguity(
            seed,
            FdbFidelity::CommitUnknownResult,
            Observer::SettlingReRead,
        );
        assert!(
            obs.ambiguous_conditional_commits >= 1,
            "the nemesis must strike a CAS at seed {seed}"
        );
        assert_eq!(
            obs.commits_left_in_flight, 0,
            "a 1021 commit is out of flight by definition (seed {seed})"
        );
        if obs.ambiguous_commits_that_landed >= 1 {
            landed += 1;
        } else {
            did_not_land += 1;
        }
    }
    assert!(
        landed >= 1,
        "no seed in 0..{AMBIGUITY_SWEEP_SEEDS} produced an ambiguous commit that LANDED; the \
         settling re-read is never exercised on the half that matters"
    );
    assert!(
        did_not_land >= 1,
        "no seed in 0..{AMBIGUITY_SWEEP_SEEDS} produced an ambiguous commit that did NOT land; \
         the sweep only sees one half of the ambiguity space"
    );
}

/// **Demonstrated red (a): the settling re-read is load-bearing.**
///
/// The *only* thing that changes is the observer: instead of settling an `Err` by re-reading,
/// it **assumes the ambiguous commit did not land** — precisely the reasoning
/// `crates/metadata-fdb/src/lib.rs:67-73` forbids. On every seed where the struck commit
/// actually landed, the store bumped the inode's version while this observer counted zero
/// winners, and the version-bump assertion panics.
///
/// If the sweep contained no landed-ambiguous seed this test would *not* panic and
/// `#[should_panic]` would fail it — so it doubles as a vacuity guard on the nemesis itself.
#[test]
#[should_panic(expected = "must equal the inode's version bump")]
fn assuming_an_ambiguous_commit_did_not_land_fails_the_sweep() {
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        run_cas_ambiguity(
            seed,
            FdbFidelity::CommitUnknownResult,
            Observer::AssumeNotCommitted,
        );
    }
}

/// **Demonstrated red (b): "no torn/hybrid state is observable" is load-bearing.**
///
/// The observer is the correct one; only the *store* changes, to the violating
/// `FdbFidelity::TornApplyOnAmbiguity` (`support/mod.rs`) — an ambiguous commit that lands
/// applies only the first put of its batch. The inode moves to the winner's chunk map, the
/// version bumps once, and the winner is counted correctly, so every *other* assertion in the
/// body still passes; what breaks is atomicity: the nine `orphan:` records the same
/// `WriteBatch` staged never appear, so the superseded object's fragments would leak forever.
///
/// Without this test the atomicity clause would rest on the model's inability to tear —
/// exactly the vacuity `crates/metadata-conformance/tests/demonstrated_red.rs` exists to
/// forbid.
#[test]
#[should_panic(expected = "must apply its batch WHOLE")]
fn a_torn_apply_of_an_ambiguous_commit_is_caught() {
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        run_cas_ambiguity(
            seed,
            FdbFidelity::TornApplyOnAmbiguity,
            Observer::SettlingReRead,
        );
    }
}

// ────────────── leg 2: the blind pending-ledger put under 1021 ──────────────

/// How an observer treats an ambiguous **blind** (precondition-free) commit — the Intent
/// phase's `put_pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlindObserver {
    /// The **correct** move: re-read the ledger. If the put did not land, re-issue it — a
    /// blind `put_pending` is a plain overwrite, hence idempotent, so a retry is safe where
    /// a retry of the (non-idempotent) version CAS would not be.
    SettlingReRead,
    /// The **violating** observer: assume the ambiguous put landed and carry on to the Data
    /// phase.
    AssumeLanded,
}

/// The Intent phase of the four-phase write protocol under commit ambiguity.
///
/// The pending ledger is the *only* thing protecting a written-but-not-yet-committed chunk's
/// fragments from the custodian GC (`crates/core/src/metadata.rs:518-523`). A blind
/// `put_pending` that comes back ambiguous therefore cannot be assumed to have landed: if it
/// did not, the fragments this phase is about to write are unreferenced garbage the moment
/// they hit disk.
///
/// The plan is deliberately **one chunk** (`b"ab"` at `CHUNK = 4`), so `write::intent` issues
/// exactly one blind commit and the nemesis's single strike lands on it by construction.
async fn ambiguous_pending_put_over(meta: Arc<SimFdbMetadataStore>, observer: BlindObserver) {
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    let plan = write::plan_write(b"ab", CHUNK, RS, ids_from(1)).unwrap();
    assert_eq!(plan.chunk_ids().len(), 1, "one chunk, so one blind commit");

    meta.arm_commit_ambiguity(SIM_COMMIT_UNKNOWN_RESULT, 1);

    // Phase 1 — Intent. The blind pending-ledger put IS ambiguous: the model does not exempt
    // precondition-free batches, because production does not either (the code check returns
    // before the `conditional` check, `crates/metadata-fdb/src/lib.rs:212-215`).
    let err = write::intent(&*meta, &plan, LEASE_EXPIRY)
        .await
        .expect_err("the nemesis must strike the Intent phase's blind put");
    let ambiguous = expect_ambiguous(&err);
    assert_eq!(ambiguous.code, SIM_COMMIT_UNKNOWN_RESULT);
    assert_eq!(
        meta.observations().ambiguous_blind_commits,
        1,
        "a blind batch must be able to come back ambiguous — a model that gated the nemesis \
         on `conditional` would leave this at 0 and silently narrow the contract"
    );

    match observer {
        BlindObserver::SettlingReRead => {
            // Settle by re-read; re-issue the idempotent put if it did not land. The nemesis
            // budget is spent, so the retry is determinate.
            for id in plan.chunk_ids() {
                if meta
                    .get(&metadata::pending_key(id))
                    .await
                    .unwrap()
                    .is_none()
                {
                    write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
                }
            }
        }
        BlindObserver::AssumeLanded => {}
    }

    // Phase 2 — Data. The fragments are now on disk.
    write::write_fragments(&*chunks, &plan).await.unwrap();

    // The invariant: every chunk whose fragments are on disk but not yet in any committed
    // chunk map is protected by a pending-ledger entry. Without it the custodian GC reclaims
    // those fragments as unreferenced garbage before the commit publishes them — silent data
    // loss. This is what assuming an ambiguous blind commit landed costs.
    for id in plan.chunk_ids() {
        assert!(
            meta.get(&metadata::pending_key(id))
                .await
                .unwrap()
                .is_some(),
            "chunk {id}'s fragments are written but it has no pending-ledger entry: an \
             ambiguous blind commit was assumed to have landed"
        );
    }
}

/// Drive the Intent-phase ambiguity at one fixed seed.
fn run_blind_ambiguity(seed: u64, observer: BlindObserver) -> FdbObservations {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
            FdbFidelity::CommitUnknownResult,
        ));
        ambiguous_pending_put_over(Arc::clone(&meta), observer).await;
        meta.observations()
    })
}

/// The correct caller settles an ambiguous blind put by re-reading the ledger and re-issuing
/// the (idempotent) put. Swept so both halves — the struck put landed, and it did not — are
/// actually reached; otherwise the settling retry would be exercised by nothing.
#[test]
fn an_ambiguous_pending_ledger_put_is_settled_by_a_re_read() {
    let mut landed = 0u64;
    let mut did_not_land = 0u64;
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        let obs = run_blind_ambiguity(seed, BlindObserver::SettlingReRead);
        assert_eq!(
            obs.ambiguous_blind_commits, 1,
            "the nemesis must strike the blind Intent put at seed {seed}"
        );
        assert_eq!(
            obs.ambiguous_conditional_commits, 0,
            "no conditional batch is issued in this leg (seed {seed})"
        );
        if obs.ambiguous_commits_that_landed >= 1 {
            landed += 1;
        } else {
            did_not_land += 1;
        }
    }
    assert!(
        landed >= 1 && did_not_land >= 1,
        "the blind-ambiguity sweep must reach both halves; saw {landed} landed / \
         {did_not_land} not-landed"
    );
}

/// **Demonstrated red (c): a blind batch's ambiguity is load-bearing too.**
///
/// The observer assumes the ambiguous `put_pending` landed and proceeds to write fragments.
/// On every seed where it did *not* land, the chunk's fragments sit on disk with no
/// pending-ledger entry — invisible to the commit that has not happened yet, and reclaimable
/// by the custodian GC. The ledger assertion catches it.
///
/// This is also the guard on the model itself: re-introduce a `&& conditional` gate on the
/// nemesis and `write::intent` never errors, so this test stops panicking and
/// `#[should_panic]` fails it.
#[test]
#[should_panic(expected = "no pending-ledger entry")]
fn assuming_an_ambiguous_blind_put_landed_leaves_a_chunk_unprotected() {
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        run_blind_ambiguity(seed, BlindObserver::AssumeLanded);
    }
}

// ──────────────────── leg 3: the CAS under 1031 ────────────────────

/// The two undeterminable codes are not equally bad, and the model says so the way
/// production does — off the code, not off a message
/// (`crates/metadata-fdb/src/lib.rs:240-249`).
#[test]
fn the_two_undeterminable_codes_are_not_equally_bad() {
    assert!(
        !SimCommitUnknownResult {
            code: SIM_COMMIT_UNKNOWN_RESULT
        }
        .may_still_commit(),
        "1021 promises the transaction is out of flight"
    );
    assert!(
        SimCommitUnknownResult {
            code: SIM_TRANSACTION_TIMED_OUT
        }
        .may_still_commit(),
        "1031 promises nothing (crates/metadata-fdb/src/lib.rs:165)"
    );
}

/// How an observer treats a commit that returned `1031 transaction_timed_out`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutObserver {
    /// The **correct** move: accept that the outcome is not settleable. Re-read all you
    /// like; the batch may still land afterwards, so no read establishes a final answer.
    AcceptsIndeterminacy,
    /// The **violating** observer: treat 1031 exactly like 1021 — assume the first re-read
    /// after the error settles the outcome for good.
    TreatsReReadAsSettling,
}

/// A single writer's CAS struck by `1031`. Returns whether this seed exhibited the fate the
/// code exists to warn about: the batch was **not** visible at the settling re-read and
/// landed afterwards.
async fn timed_out_commit_over(meta: Arc<SimFdbMetadataStore>, observer: TimeoutObserver) -> bool {
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    let v0 = write::plan_write(b"v0", CHUNK, RS, ids_from(1)).unwrap();
    write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &v0).await.unwrap();
    write::commit_create(&*meta, 0, "obj", 1, &v0)
        .await
        .unwrap();
    write::release(&*meta, &v0).await.unwrap();
    let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    // Stage Intent + Data determinately, then arm: the CAS is the first accepted commit.
    let plan = write::plan_write(b"later", CHUNK, RS, ids_from(0x1000)).unwrap();
    write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &plan).await.unwrap();
    meta.arm_commit_ambiguity(SIM_TRANSACTION_TIMED_OUT, 1);

    let err = write::commit_overwrite(&*meta, 1, &prior, &plan, ORPHANED_AT)
        .await
        .expect_err("the nemesis must strike the CAS");
    let ambiguous = expect_ambiguous(&err);
    assert_eq!(ambiguous.code, SIM_TRANSACTION_TIMED_OUT);
    assert!(
        ambiguous.may_still_commit(),
        "1031 must be distinguishable from 1021 by the code the error carries"
    );

    // The "settling" re-read a 1021 caller is entitled to make…
    let first = read::read_inode(&*meta, 1).await.unwrap().unwrap();
    // …and further store traffic, during which the cluster may apply a commit it never told
    // us about (`crates/metadata-fdb/src/lib.rs:161-166`).
    let _ = read::read_inode(&*meta, 1).await.unwrap();
    let later = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    if observer == TimeoutObserver::TreatsReReadAsSettling {
        assert_eq!(
            first.version, later.version,
            "a re-read does not settle a timed-out commit: the batch was invisible at the \
             first re-read and landed afterwards ({} -> {}). 1031 promises nothing.",
            first.version, later.version,
        );
    }

    // Whatever the observer believed, the STORE's terminal state is sound: force every
    // in-flight batch through the resolver and at most one CAS ever landed, whole.
    meta.quiesce();
    let settled = read::read_inode(&*meta, 1).await.unwrap().unwrap();
    assert!(
        settled.version - prior.version <= 1,
        "a timed-out CAS may land at most once: {} -> {}",
        prior.version,
        settled.version,
    );
    assert_chunk_map_is_whole(&settled, &prior, &[plan.chunk_refs()]);
    assert_batch_applied_whole(&meta, &prior, &settled).await;

    first.version == prior.version && later.version > prior.version
}

/// Drive the 1031 scenario at one fixed seed; `true` iff the batch landed *after* the
/// settling re-read observed nothing.
fn run_timeout_ambiguity(seed: u64, observer: TimeoutObserver) -> bool {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
            FdbFidelity::CommitUnknownResult,
        ));
        timed_out_commit_over(Arc::clone(&meta), observer).await
    })
}

/// `1031 transaction_timed_out` withholds the one guarantee `1021` gives. The sweep must
/// actually reach the fate that makes the difference matter: a commit invisible at the
/// settling re-read that lands afterwards. If no seed reached it, the whole 1031 leg would be
/// the 1021 leg wearing a different code — this fails loudly instead.
#[test]
fn a_timed_out_commit_may_still_land_after_the_settling_re_read() {
    let landed_late = (0..AMBIGUITY_SWEEP_SEEDS)
        .filter(|&seed| run_timeout_ambiguity(seed, TimeoutObserver::AcceptsIndeterminacy))
        .count();
    assert!(
        landed_late >= 1,
        "no seed in 0..{AMBIGUITY_SWEEP_SEEDS} left a 1031 commit in flight past the settling \
         re-read; `may_still_commit` would be modelled by nothing"
    );
}

/// **Demonstrated red (d): 1031 is not 1021.**
///
/// The observer settles the timed-out commit with one re-read, exactly as it is entitled to
/// do after a 1021. On the seeds where the batch was still in flight, it lands afterwards and
/// the observer's "settled" answer was wrong. This is the assertion that would be missing if
/// `SimCommitUnknownResult` were the previous unit struct carrying no code — the ambiguity
/// space would be only the out-of-flight half of itself.
#[test]
#[should_panic(expected = "a re-read does not settle a timed-out commit")]
fn treating_a_timed_out_commit_like_1021_fails_the_sweep() {
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        run_timeout_ambiguity(seed, TimeoutObserver::TreatsReReadAsSettling);
    }
}

// ─────────── leg 4: the version CAS under 1031, under contention ───────────

/// How an observer treats a fleet of concurrent CAS commits that each returned
/// `1031 transaction_timed_out`. Unlike leg 1's `1021`, these are **not** out of flight:
/// the settling answer exists only once the deferred batches have run through the resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContendedObserver {
    /// The **correct** move: let every deferred batch run through the resolver (`quiesce`),
    /// then re-read and let the store's terminal state name the single winner.
    SettleThenReRead,
    /// The **violating** observer: assume every `1031` landed ("it timed out, it might have
    /// committed") and count each struck writer a winner. The deferral rejects all but one,
    /// so this over-counts and the version-bump assertion trips.
    AssumeEveryTimeoutLanded,
}

/// Four writers race the version-conditional CAS with the `1031` nemesis armed. A struck
/// batch may land now, be left **in flight**, or be dropped — the seed decides. The ones
/// left in flight resolve *later*, at the deferral, after a different writer may already
/// have won and bumped the version. The invariant, asserted over the terminal
/// (post-`quiesce`) state: exactly one writer won and the version bumped exactly once,
/// because `settle_in_flight` (`support/mod.rs`) re-runs every deferred batch through the
/// resolver and rejects each stale one — so "exactly one winner" survives a 1031 deferral
/// under contention.
async fn contended_cas_under_1031_over(
    meta: Arc<SimFdbMetadataStore>,
    observer: ContendedObserver,
) {
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    // An existing object at version 1, committed determinately (nemesis disarmed).
    let v0 = write::plan_write(b"v0", CHUNK, RS, ids_from(1)).unwrap();
    write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &v0).await.unwrap();
    write::commit_create(&*meta, 0, "obj", 1, &v0)
        .await
        .unwrap();
    write::release(&*meta, &v0).await.unwrap();
    let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    // Stage every writer's Intent + Data BEFORE arming, so the first commits the resolver
    // accepts after arming are the version-CAS batches this leg is about.
    let mut plans = Vec::new();
    for i in 0..4u128 {
        let plan = write::plan_write(b"contended", CHUNK, RS, ids_from(0x1000 * (i + 1))).unwrap();
        write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
        write::write_fragments(&*chunks, &plan).await.unwrap();
        plans.push(plan);
    }
    let staged: Vec<Vec<ChunkRef>> = plans.iter().map(write::WritePlan::chunk_refs).collect();

    // Arm the 1031 nemesis over the whole fleet: every accepted CAS becomes ambiguous, and
    // the seed decides — for each — whether it landed now, is still in flight, or was dropped.
    meta.arm_commit_ambiguity(SIM_TRANSACTION_TIMED_OUT, 4);

    let mut handles = Vec::new();
    for plan in plans {
        let meta = Arc::clone(&meta);
        let prior = prior.clone();
        handles.push(madsim::task::spawn(async move {
            let outcome = write::commit_overwrite(&*meta, 1, &prior, &plan, ORPHANED_AT).await;
            (outcome, plan.chunk_refs())
        }));
    }
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // ── the deferral ─────────────────────────────────────────────────────────────────────
    // 1031 "promises nothing": a struck batch may still be in flight. Force every one through
    // the resolver — the point of the leg — so the store reaches its terminal state.
    meta.quiesce();
    assert_eq!(
        meta.in_flight(),
        0,
        "quiesce must force every deferred 1031 batch to resolve"
    );
    let settled = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    let mut winners: u64 = 0;
    for (outcome, chunk_map) in &results {
        let won = match outcome {
            Ok(CommitOutcome::Committed) => true,
            Ok(CommitOutcome::Conflict) => false,
            Err(err) => {
                let ambiguous = expect_ambiguous(err);
                assert_eq!(ambiguous.code, SIM_TRANSACTION_TIMED_OUT);
                assert!(
                    ambiguous.may_still_commit(),
                    "1031 is not out of flight — only the deferral settles its outcome"
                );
                match observer {
                    // Correct: the store's terminal, resolver-checked state decides.
                    ContendedObserver::SettleThenReRead => settled.chunk_map == *chunk_map,
                    // Violating: assume every timed-out commit landed.
                    ContendedObserver::AssumeEveryTimeoutLanded => true,
                }
            }
        };
        if won {
            winners += 1;
        }
    }

    // ── the invariant ──────────────────────────────────────────────────────────────────
    assert_eq!(
        settled.version - prior.version,
        winners,
        "the settled winner count ({winners}) must equal the inode's version bump ({} -> {}) \
         — a 1031 commit was counted a winner before the deferral settled it",
        prior.version,
        settled.version,
    );
    assert!(
        settled.version - prior.version <= 1,
        "at most one CAS may win a 1031 deferral under contention: {} -> {}",
        prior.version,
        settled.version,
    );
    assert_chunk_map_is_whole(&settled, &prior, &staged);
    assert_batch_applied_whole(&meta, &prior, &settled).await;

    if winners == 1 {
        let bytes = read::read_path(&*meta, &*chunks, 0, "obj").await.unwrap();
        assert_eq!(bytes.as_deref(), Some(&b"contended"[..]));
    } else {
        assert_eq!(
            settled, prior,
            "with no winner the inode must be byte-identical to the prior"
        );
    }
}

/// Drive the four-writer 1031 contention race at one fixed seed and return what the store
/// observed.
fn run_contended_1031(seed: u64, observer: ContendedObserver) -> FdbObservations {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(
            FdbFidelity::CommitUnknownResult,
        ));
        contended_cas_under_1031_over(Arc::clone(&meta), observer).await;
        meta.observations()
    })
}

/// The multi-writer 1031 sweep. At every seed the invariant holds; across the sweep the
/// deferral branch is genuinely exercised — deferred batches **land** *and* are **rejected**
/// at the resolver after a later writer won — so "exactly one winner survives the deferral"
/// rests on executed code, not inspection. The counter assertions are the anti-vacuity
/// guard the brief's Risk note promises (`Observations` records how often it fired): a sweep
/// that never armed the deferral branch fails here rather than passing hollow.
#[test]
fn contention_under_1031_keeps_exactly_one_winner_through_the_deferral() {
    let mut deferred_landings = 0u64;
    let mut deferred_rejections = 0u64;
    let mut resolver_conflicts = 0u64;
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        let obs = run_contended_1031(seed, ContendedObserver::SettleThenReRead);
        assert!(
            obs.ambiguous_conditional_commits >= 1,
            "the 1031 nemesis must strike a CAS at seed {seed}"
        );
        deferred_landings += obs.deferred_landings;
        deferred_rejections += obs.deferred_rejections;
        resolver_conflicts += obs.resolver_conflicts;
    }
    assert!(
        deferred_landings >= 1,
        "no seed in 0..{AMBIGUITY_SWEEP_SEEDS} ever landed a deferred 1031 batch; the deferral \
         (`settle_in_flight` landing branch) is exercised by nothing"
    );
    assert!(
        deferred_rejections >= 1,
        "no seed in 0..{AMBIGUITY_SWEEP_SEEDS} ever REJECTED a stale deferred 1031 batch at the \
         resolver; 'exactly one winner survives the deferral' rests on inspection, not execution"
    );
    assert!(
        resolver_conflicts >= 1,
        "no seed produced an outright resolver conflict under contention; the 1031 fleet never \
         actually contended for the version CAS"
    );
}

/// **Demonstrated red (e): a 1031 commit cannot be counted a winner before the deferral
/// settles it.** The observer counts every struck (`1031`) writer a winner instead of
/// letting `quiesce` run the deferral. Under contention the deferral rejects all but one, so
/// the version bumped at most once while the observer counted several — the version-bump
/// assertion trips. The multi-writer analogue of leg 1's `AssumeNotCommitted`, against the
/// code that "promises nothing".
#[test]
#[should_panic(expected = "must equal the inode's version bump")]
fn counting_every_timed_out_commit_a_winner_fails_the_contended_sweep() {
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        run_contended_1031(seed, ContendedObserver::AssumeEveryTimeoutLanded);
    }
}

/// The **resolver re-check at the deferral, isolated** (`support/mod.rs` `settle_in_flight`).
///
/// One writer's CAS (A) is struck by `1031` and — on the seeds this scenario acts on — left
/// in flight. A second writer (B) then commits *determinately* and wins, bumping the inode
/// to its own whole chunk map. A's batch still carries the now-stale precondition on the
/// prior inode, and lands only at the forced deferral. The faithful model re-runs A through
/// the resolver, sees its precondition no longer holds, and **rejects** it, so B's win
/// survives. Returns `true` iff this seed reached that case (A in flight, B won cleanly).
async fn deferred_1031_settles_against_current_truth(meta: Arc<SimFdbMetadataStore>) -> bool {
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    let v0 = write::plan_write(b"v0", CHUNK, RS, ids_from(1)).unwrap();
    write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &v0).await.unwrap();
    write::commit_create(&*meta, 0, "obj", 1, &v0)
        .await
        .unwrap();
    write::release(&*meta, &v0).await.unwrap();
    let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    // A and B write disjoint chunk-id ranges, so the settled chunk map identifies the winner.
    let a = write::plan_write(b"writer-a", CHUNK, RS, ids_from(0x1000)).unwrap();
    let b = write::plan_write(b"writer-b", CHUNK, RS, ids_from(0x2000)).unwrap();
    write::intent(&*meta, &a, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &a).await.unwrap();
    write::intent(&*meta, &b, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &b).await.unwrap();

    // A's CAS is the only struck commit.
    meta.arm_commit_ambiguity(SIM_TRANSACTION_TIMED_OUT, 1);
    let a_err = write::commit_overwrite(&*meta, 1, &prior, &a, ORPHANED_AT)
        .await
        .expect_err("the 1031 nemesis must strike A's CAS");
    assert_eq!(expect_ambiguous(&a_err).code, SIM_TRANSACTION_TIMED_OUT);

    // Only the in-flight fate exercises the deferral; landed-now / dropped are leg 4a's.
    if meta.in_flight() == 0 {
        return false;
    }

    // B commits determinately (the nemesis budget is spent) and must win cleanly over the
    // prior for the case under test. If A was opportunistically settled during B's commit, B
    // may conflict — that seed is not the one this scenario needs.
    let _ = write::commit_overwrite(&*meta, 1, &prior, &b, ORPHANED_AT).await;
    let after_b = read::read_inode(&*meta, 1).await.unwrap().unwrap();
    if after_b.chunk_map != b.chunk_refs() {
        return false;
    }

    // Force A's still-in-flight batch through the deferral.
    meta.quiesce();
    let settled = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    // THE INVARIANT: A's stale batch is rejected at the resolver; B's win is untouched.
    assert_eq!(
        settled.chunk_map,
        b.chunk_refs(),
        "a 1031 batch that lands at the deferral must be re-checked against CURRENT truth and \
         REJECTED when a later writer already won — it clobbered the winner instead"
    );
    assert!(
        settled.version - prior.version <= 1,
        "the stale deferred batch bumped the version a second time: {} -> {}",
        prior.version,
        settled.version,
    );
    assert_batch_applied_whole(&meta, &prior, &settled).await;
    true
}

/// Drive the isolated deferral scenario at one fixed seed under `fidelity`; `true` iff the
/// seed reached the case (A deferred past B's determinate win).
fn run_deferred_resolver(seed: u64, fidelity: FdbFidelity) -> bool {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let meta = Arc::new(SimFdbMetadataStore::with_fidelity(fidelity));
        deferred_1031_settles_against_current_truth(Arc::clone(&meta)).await
    })
}

/// The faithful model rejects the stale deferred batch, and the sweep actually reaches the
/// case — a 1031 batch deferred past a determinate winner — so the guarantee is executed,
/// not assumed.
#[test]
fn a_stale_deferred_1031_batch_is_rejected_against_current_truth() {
    let reached = (0..AMBIGUITY_SWEEP_SEEDS)
        .filter(|&seed| run_deferred_resolver(seed, FdbFidelity::CommitUnknownResult))
        .count();
    assert!(
        reached >= 1,
        "no seed in 0..{AMBIGUITY_SWEEP_SEEDS} left a 1031 batch in flight past a determinate \
         winner; the deferral-rejection path is exercised by nothing"
    );
}

/// **Demonstrated red (f): the resolver re-check at the deferral is load-bearing.**
///
/// The only change is the violating `FdbFidelity::DeferredResolverSkipped` twin, which
/// applies a forced deferred batch **without** re-checking its preconditions. On the seeds
/// where A is deferred past B's win, A's stale batch clobbers B and the winner assertion
/// trips. Without this red, "the deferral rejects the stale batch" would rest on the
/// faithful model simply never misbehaving — exactly the vacuity
/// `crates/metadata-conformance/tests/demonstrated_red.rs` forbids.
#[test]
#[should_panic(expected = "clobbered the winner")]
fn a_deferred_1031_batch_that_skips_the_resolver_clobbers_the_winner() {
    for seed in 0..AMBIGUITY_SWEEP_SEEDS {
        run_deferred_resolver(seed, FdbFidelity::DeferredResolverSkipped);
    }
}
