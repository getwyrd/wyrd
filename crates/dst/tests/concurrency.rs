//! Commit-protocol concurrency tests on madsim's deterministic, single-threaded
//! scheduler (ADR-0009), driven through **both** `MetadataStore` implementations —
//! the deterministic redb backend and the deterministic **simulated-TiKV model**
//! (`support::SimTikvMetadataStore`) — so the identical exactly-one-winner property
//! pins the trait by two implementations (ADR-0006; proposal 0015 §"Pinning the
//! trait with the second implementation", accepted
//! `docs/design/proposals/accepted/0015-milestone-4-production-metadata-backend-revised.md:546-555`).
//!
//! madsim interleaves the spawned writer tasks at their `.await` boundaries from the
//! run seed. **What decides "which writer wins" — and how large the schedule space
//! is — differs by backend, and pinning the trait with a second implementation is
//! exactly what surfaces that:**
//!
//! * on **redb**, each `commit()` is one synchronous write transaction — indivisible,
//!   so the scheduler only chooses the *order* of whole commits and the version
//!   compare-and-set trivially admits exactly one;
//! * on **TiKV**, a `commit()` is a 2PC that **awaits on network I/O mid-flight**, so
//!   the scheduler can interleave a second writer *inside* a commit — a strictly
//!   larger schedule space than redb's. The simulated-TiKV model renders that await
//!   boundary explicitly; the version compare-and-set *still* yields exactly one
//!   winner because the decision is taken at an atomic prewrite lock-grab, not spread
//!   across the await (proposal 0015 lines 549-555).
//!
//! So determinism here rests on madsim's seed-reproducible scheduler, **not** on the
//! old redb-shaped claim that "each `commit()` is internally synchronous, no await
//! inside, so the only question is which writer commits first" — which is untrue of a
//! TiKV commit that awaits on network I/O mid-flight. The two seed-pinned tests below
//! (`sim_tikv_reaches_mid_commit_interleaving_and_one_wins` and its demonstrated-red
//! twin) prove the corrected reasoning: the mid-commit interleaving is genuinely
//! reached, and exactly one writer still wins across it.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`); without it this file
//! compiles to nothing, so a normal `cargo test` neither builds nor runs it.
#![cfg(madsim)]

use std::sync::Arc;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{CommitOutcome, MetadataStore};

#[path = "support/mod.rs"]
mod support;
use support::{Fidelity, Observations, SimTikvMetadataStore};

const CHUNK: usize = 4;
const LEASE_EXPIRY: u64 = 6_000;
// Exercise the concurrent commit over the default erasure-coded data path (n = 9
// fragments per chunk), not just M0 single-fragment replication (M1.6).
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

/// A committed, reproduces-forever seed (ADR-0009; proposal 0015 lines 539, 660):
/// the two seed-pinned tests below build the runtime with this fixed seed, so they
/// replay the *same* interleaving on every run, on every machine. Value spells
/// "M4.7" (`b"M4.7"` big-endian) — a mnemonic, not a magic constant.
const PINNED_INTERLEAVING_SEED: u64 = 0x4D34_2E37;

/// A unique, deterministic chunk-id generator starting just above `base`.
fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// The shared exactly-one-winner property, written against the `MetadataStore`
/// **trait** so redb and the simulated-TiKV model run the byte-identical race
/// ("shared, not forked" — proposal 0015 lines 548, 656). Four writers read the same
/// prior version, stage independently, then race to commit the version-conditional
/// CAS; exactly one must win and the version must bump exactly once, under whatever
/// interleaving the seed picks.
async fn exactly_one_writer_wins_over<M>(meta: Arc<M>)
where
    M: MetadataStore + 'static,
{
    let dir = tempfile::tempdir().expect("temp dir");
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    // An existing object at version 1.
    let v0 = write::plan_write(b"v0", CHUNK, RS, ids_from(1)).unwrap();
    write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &v0).await.unwrap();
    write::commit_create(&*meta, 0, "obj", 1, &v0)
        .await
        .unwrap();
    write::release(&*meta, &v0).await.unwrap();
    let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    // Four writers read the same prior, stage independently, then race to commit.
    // madsim schedules their interleaving from the seed.
    let mut handles = Vec::new();
    for i in 0..4u128 {
        let meta = Arc::clone(&meta);
        let chunks = Arc::clone(&chunks);
        let prior = prior.clone();
        handles.push(madsim::task::spawn(async move {
            let plan =
                write::plan_write(b"contended", CHUNK, RS, ids_from(0x1000 * (i + 1))).unwrap();
            write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
            write::write_fragments(&*chunks, &plan).await.unwrap();
            let outcome = write::commit_overwrite(&*meta, 1, &prior, &plan)
                .await
                .unwrap();
            if outcome == CommitOutcome::Committed {
                write::release(&*meta, &plan).await.unwrap();
            }
            outcome
        }));
    }

    let mut winners = 0;
    for handle in handles {
        if handle.await.unwrap() == CommitOutcome::Committed {
            winners += 1;
        }
    }

    assert_eq!(
        winners, 1,
        "exactly one concurrent writer must win the commit"
    );
    let after = read::read_inode(&*meta, 1).await.unwrap().unwrap();
    assert_eq!(
        after.version,
        prior.version + 1,
        "version bumped exactly once"
    );

    // The committed object is whole and readable (the winner's content).
    let bytes = read::read_path(&*meta, &*chunks, 0, "obj").await.unwrap();
    assert_eq!(bytes.as_deref(), Some(&b"contended"[..]));
}

/// Exactly-one-winner over the **deterministic redb backend** (the Tier-0 spine,
/// unchanged — proposal 0015 lines 489-499).
#[madsim::test]
async fn exactly_one_concurrent_writer_wins_redb() {
    let meta = Arc::new(RedbMetadataStore::in_memory().expect("redb"));
    exactly_one_writer_wins_over(meta).await;
}

/// Exactly-one-winner over the **second implementation** — the simulated-TiKV model,
/// whose commit awaits on (simulated) network I/O mid-flight. The identical property
/// body drives it, pinning the trait by two implementations (ADR-0006).
#[madsim::test]
async fn exactly_one_concurrent_writer_wins_sim_tikv() {
    let meta = Arc::new(SimTikvMetadataStore::new());
    exactly_one_writer_wins_over(meta).await;
}

/// Run the four-writer race over the simulated-TiKV model at the fixed
/// [`PINNED_INTERLEAVING_SEED`] and return what the store observed. The inner
/// assertions (exactly one winner, version bumped once) hold for **both**
/// fidelities — only the *reachability of the mid-commit interleaving* differs.
fn run_pinned_race(fidelity: Fidelity) -> Observations {
    let rt = madsim::runtime::Runtime::with_seed_and_config(
        PINNED_INTERLEAVING_SEED,
        madsim::Config::default(),
    );
    rt.block_on(async move {
        let meta = Arc::new(SimTikvMetadataStore::with_fidelity(fidelity));
        exactly_one_writer_wins_over(Arc::clone(&meta)).await;
        meta.observations()
    })
}

/// The corrected interleaving coverage (proposal 0015 lines 549-555): a
/// **committed, reproduces-forever seed** proving the two things the old redb-shaped
/// rationale conflated — (1) the mid-commit `.await` boundary is genuinely reached (a
/// writer's prewrite observes another writer *mid-commit*, a schedule redb's
/// indivisible commit cannot produce) and (2) the version CAS still yields exactly
/// one winner under that interleaving (asserted inside `run_pinned_race`).
#[test]
fn sim_tikv_reaches_mid_commit_interleaving_and_one_wins() {
    let obs = run_pinned_race(Fidelity::AwaitInsideCommit);
    assert!(
        obs.mid_commit_lock_conflicts >= 1,
        "the await-inside-commit model must reach a schedule where a writer's \
         prewrite observes another writer mid-commit (else the exactly-one-winner \
         coverage is vacuous); saw {} mid-commit observations",
        obs.mid_commit_lock_conflicts,
    );
}

/// The demonstrated-red twin (cf. `crates/metadata-conformance/tests/demonstrated_red.rs`):
/// the **synchronous, redb-shaped** commit — "no await inside" — is a correct store
/// (exactly one winner, so `run_pinned_race`'s inner assertions still pass) but, being
/// indivisible, can NEVER reach the mid-commit interleaving. Same seed, same race: the
/// observation count stays 0, so the `>= 1` assertion the await model satisfies panics
/// here. That is what makes the interleaving coverage load-bearing rather than a
/// tautology — and it is exactly the redb-shaped assumption the module header used to
/// state as the ground of determinism.
#[test]
#[should_panic(expected = "observes another writer mid-commit")]
fn synchronous_redb_shaped_commit_never_reaches_the_interleaving() {
    let obs = run_pinned_race(Fidelity::SynchronousRedbShaped);
    assert!(
        obs.mid_commit_lock_conflicts >= 1,
        "the await-inside-commit model must reach a schedule where a writer's \
         prewrite observes another writer mid-commit (else the exactly-one-winner \
         coverage is vacuous); saw {} mid-commit observations",
        obs.mid_commit_lock_conflicts,
    );
}
