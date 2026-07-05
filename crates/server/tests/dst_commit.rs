//! Tier 1 DST: seed-reproducible property tests for the commit invariants
//! (ADR-0009), driven by `wyrd_testkit::Sim` over the real in-memory redb +
//! filesystem backends. Single-threaded and deterministic — re-running a seed
//! reproduces the run exactly. This is the graduation proof that the commit is
//! atomic under fault injection.

use pollster::block_on;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::CommitOutcome;

const CHUNK: usize = 4;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const AFTER_LEASE: u64 = NOW + TTL + 1;
const SEEDS: u64 = 64;

/// The commit protocol is durability-scheme agnostic; run every property under
/// both the M0 single-fragment path and the rs(6,3) erasure-coded path (M1.6,
/// ADR-0008 mixed-era). The reads reconstruct, so the assertions are identical.
const SCHEMES: &[EcScheme] = &[EcScheme::None, EcScheme::ReedSolomon { k: 6, m: 3 }];

fn backends() -> (RedbMetadataStore, FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = RedbMetadataStore::in_memory().expect("redb");
    let chunks = FsChunkStore::open(dir.path()).expect("fs store");
    (meta, chunks, dir)
}

fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// A random payload of up to `max` bytes, drawn from the seeded RNG.
fn payload(sim: &mut Sim, max: usize) -> Vec<u8> {
    let len = (sim.gen::<u16>() as usize) % (max + 1);
    (0..len).map(|_| sim.gen::<u8>()).collect()
}

/// Write a new object end to end (happy path) and return its content.
async fn put_new(
    meta: &RedbMetadataStore,
    chunks: &FsChunkStore,
    name: &str,
    id: u64,
    data: &[u8],
    base: u128,
    scheme: EcScheme,
) {
    let plan = write::plan_write(data, CHUNK, scheme, ids_from(base)).unwrap();
    write::intent(meta, &plan, NOW + TTL).await.unwrap();
    write::write_fragments(chunks, &plan).await.unwrap();
    assert_eq!(
        write::commit_create(meta, 0, name, id, &plan)
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    write::release(meta, &plan).await.unwrap();
}

/// Invariant: under a concurrent overwrite, exactly one commit wins and the
/// loser sees the version conflict; the winner's content and a single version
/// bump persist.
fn exactly_one_wins(scheme: EcScheme, seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, _dir) = backends();
        put_new(
            &meta,
            &chunks,
            "obj",
            1,
            &payload(&mut sim, 32),
            0x10,
            scheme,
        )
        .await;
        let prior = read::read_inode(&meta, 1).await.unwrap().unwrap();

        let a = payload(&mut sim, 32);
        let b = payload(&mut sim, 32);
        let plan_a = write::plan_write(&a, CHUNK, scheme, ids_from(0x1_0000)).unwrap();
        let plan_b = write::plan_write(&b, CHUNK, scheme, ids_from(0x2_0000)).unwrap();
        for plan in [&plan_a, &plan_b] {
            write::intent(&meta, plan, NOW + TTL).await.unwrap();
            write::write_fragments(&chunks, plan).await.unwrap();
        }
        let out_a = write::commit_overwrite(&meta, 1, &prior, &plan_a, 0)
            .await
            .unwrap();
        let out_b = write::commit_overwrite(&meta, 1, &prior, &plan_b, 0)
            .await
            .unwrap();
        assert_eq!(out_a, CommitOutcome::Committed, "seed {seed}");
        assert_eq!(out_b, CommitOutcome::Conflict, "seed {seed}");

        let after = read::read_inode(&meta, 1).await.unwrap().unwrap();
        assert_eq!(after.version, prior.version + 1, "seed {seed}: one bump");
        assert_eq!(
            read::read_path(&meta, &chunks, 0, "obj")
                .await
                .unwrap()
                .as_deref(),
            Some(a.as_slice()),
            "seed {seed}: winner persisted"
        );
    });
}

/// Invariant: a reader sees the pre- or post-commit version whole, never a
/// hybrid — the captured v1 snapshot reassembles all of v1 while the live path
/// resolves all of v2.
fn never_a_hybrid(scheme: EcScheme, seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);
        let (meta, chunks, _dir) = backends();
        let v1 = payload(&mut sim, 48);
        put_new(&meta, &chunks, "obj", 1, &v1, 0x10, scheme).await;
        let snapshot = read::read_inode(&meta, 1).await.unwrap().unwrap();

        let v2 = payload(&mut sim, 48);
        let plan = write::plan_write(&v2, CHUNK, scheme, ids_from(0x9_0000)).unwrap();
        write::intent(&meta, &plan, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan).await.unwrap();
        write::commit_overwrite(&meta, 1, &snapshot, &plan, 0)
            .await
            .unwrap();

        assert_eq!(
            read::read_path(&meta, &chunks, 0, "obj")
                .await
                .unwrap()
                .as_deref(),
            Some(v2.as_slice()),
            "seed {seed}: live path is wholly v2"
        );
        assert_eq!(
            read::read_object_from(&chunks, &snapshot).await.unwrap(),
            v1,
            "seed {seed}: v1 snapshot is wholly v1"
        );
    });
}

/// Fault injection: a crash between phases 3 and 4 leaves the file fully visible
/// and a crash before commit leaves it invisible; the sweep reclaims only
/// ledger entries, never a committed object's fragments.
fn crash_is_atomic(scheme: EcScheme, seed: u64) {
    block_on(async {
        let mut sim = Sim::new(seed);

        // Crash between commit (3) and release (4).
        let (meta, chunks, _dir) = backends();
        let data = payload(&mut sim, 48);
        let plan = write::plan_write(&data, CHUNK, scheme, ids_from(0x10)).unwrap();
        write::intent(&meta, &plan, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan).await.unwrap();
        write::commit_create(&meta, 0, "obj", 1, &plan)
            .await
            .unwrap();
        // --- crash: no release ---
        write::sweep_expired_leases(&meta, AFTER_LEASE)
            .await
            .unwrap();
        assert_eq!(
            read::read_path(&meta, &chunks, 0, "obj")
                .await
                .unwrap()
                .as_deref(),
            Some(data.as_slice()),
            "seed {seed}: committed object intact after sweep — no committed fragment reclaimed"
        );

        // Crash before commit: nothing is visible; the sweep clears the leases.
        let (meta, chunks, _dir) = backends();
        let plan =
            write::plan_write(&payload(&mut sim, 48), CHUNK, scheme, ids_from(0x20)).unwrap();
        write::intent(&meta, &plan, NOW + TTL).await.unwrap();
        write::write_fragments(&chunks, &plan).await.unwrap();
        // --- crash: no commit ---
        assert!(
            read::read_path(&meta, &chunks, 0, "obj")
                .await
                .unwrap()
                .is_none(),
            "seed {seed}: uncommitted object is invisible"
        );
        let reclaimed = write::sweep_expired_leases(&meta, AFTER_LEASE)
            .await
            .unwrap();
        assert_eq!(
            reclaimed.len(),
            plan.chunks.len(),
            "seed {seed}: leases reclaimed"
        );
    });
}

#[test]
fn exactly_one_commit_wins_across_seeds() {
    for &scheme in SCHEMES {
        for seed in 0..SEEDS {
            exactly_one_wins(scheme, seed);
        }
    }
}

#[test]
fn reader_never_sees_a_hybrid_across_seeds() {
    for &scheme in SCHEMES {
        for seed in 0..SEEDS {
            never_a_hybrid(scheme, seed);
        }
    }
}

#[test]
fn crash_between_commit_and_release_is_atomic_across_seeds() {
    for &scheme in SCHEMES {
        for seed in 0..SEEDS {
            crash_is_atomic(scheme, seed);
        }
    }
}

/// A pinned seed kept as a permanent regression guard (the FoundationDB /
/// TigerBeetle pattern, ADR-0009): any seed that ever surfaces a bug is added
/// here so the exact scenario is replayed forever.
#[test]
fn commit_invariants_hold_at_pinned_regression_seed() {
    const REGRESSION_SEED: u64 = 0x00C0_FFEE;
    for &scheme in SCHEMES {
        exactly_one_wins(scheme, REGRESSION_SEED);
        never_a_hybrid(scheme, REGRESSION_SEED);
        crash_is_atomic(scheme, REGRESSION_SEED);
    }

    // Re-running the seed reproduces the run exactly (determinism).
    let once: Vec<u8> = {
        let mut sim = Sim::new(REGRESSION_SEED);
        (0..16).map(|_| sim.gen::<u8>()).collect()
    };
    let twice: Vec<u8> = {
        let mut sim = Sim::new(REGRESSION_SEED);
        (0..16).map(|_| sim.gen::<u8>()).collect()
    };
    assert_eq!(once, twice, "the same seed must reproduce the same run");
}
