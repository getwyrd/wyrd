//! **Determinism-gap coverage seed** (M4.6, #257; proposal 0015 §"Pinning the trait with
//! the second implementation"; ADR-0009; ADR-0015 commit-point contract; **ADR-0039
//! Option-B posture**).
//!
//! # Honest scope — read this before trusting the assertions below
//!
//! This seed is **pure redb coverage**. It carries **NO correctness weight for
//! `TikvMetadataStore::commit`**, and a production regression in the TiKV commit path
//! **cannot** turn it red. Do **not** grade it as evidence that the redb→TiKV metadata swap
//! upholds any contract. That evidence lives entirely **off-Check** (the live Tier-1
//! consistency scenario on a real ≥3-replica TiKV cluster —
//! `crates/metadata-tikv/tests/tier1_metadata_consistency.rs`, the `WYRD_TIER1` job).
//!
//! ## Why it cannot have TiKV teeth at Check (the Option-B reason, ratified)
//!
//! The binding oracle the brief's direction (a) wanted was the **real, unchanged
//! `TikvMetadataStore::commit` driven over a deterministic third-party sim**. That is
//! **unreachable at Check**, for two independent, checkable reasons:
//!
//! 1. **`madsim-tikv-client` does not exist.** `cargo search madsim-tikv-client` returns no
//!    release tracking `tikv-client = "0.4"` (confirmed at build). There is no third-party
//!    deterministic TiKV to alias in, the way `crates/chunkstore-grpc/Cargo.toml` aliases
//!    `madsim-tonic` for the gRPC path (proposal 0004).
//! 2. **The store is not injectable.** `TikvMetadataStore` holds a **concrete**
//!    `tikv_client::TransactionClient` (`crates/metadata-tikv/src/lib.rs:420-421`), built
//!    only by `connect(pd_endpoints)` → `TransactionClient::new(..).await`
//!    (`crates/metadata-tikv/src/lib.rs:435-436`) — it needs a **real cluster** and is not
//!    generic over a fake client. Making it injectable would require editing
//!    `crates/metadata-tikv/src`, which the slice's invariant forbids byte-for-byte
//!    (proposal 0015; ADR-0006).
//!
//! So the real TiKV **await-inside-`commit()` percolator window** — the interleaving between
//! the precondition re-read `get_for_update` (`crates/metadata-tikv/src/lib.rs:560`) and the
//! terminal `txn.commit().await` (`crates/metadata-tikv/src/lib.rs:597`) — is verified only
//! against a real cluster, off-Check. This seed runs over **redb**, whose `commit()` is
//! internally synchronous (one write transaction, no `await` inside), so it exhibits **no
//! interleaving that `concurrency.rs` does not already cover** and **no newly-reachable
//! interleaving** of any kind.
//!
//! # The determinism-gap fact — asserted where it is checkable (off-Check), conceded here
//!
//! The brief's Option-B line asks the seed to assert "the `concurrency.rs` synchronous-commit
//! rationale is unsound; here is a newly-reachable interleaving". That rationale
//! (`crates/dst/tests/concurrency.rs:3-4`, "each `commit()` is internally synchronous … no
//! await inside") is indeed **false for `TikvMetadataStore::commit`**, which awaits network
//! I/O across its precondition/commit window (`crates/metadata-tikv/src/lib.rs:540-600`). But
//! that unsoundness is a property **only redb-vs-TiKV divergence can exhibit** — it is not
//! observable in a redb-only test, and (per the two reasons above) not drivable at Check.
//! This seed therefore **explicitly concedes that assertion off-Check**: the newly-reachable
//! interleaving and the demonstration that the `concurrency.rs` rationale does not carry over
//! to TiKV are exercised by the live Tier-1 consistency scenario against a real cluster
//! (`crates/metadata-tikv/tests/tier1_metadata_consistency.rs`; `WYRD_TIER1` job). At Check,
//! the seed makes no such claim.
//!
//! # What it *does* add over `concurrency.rs` (honest, redb-only)
//!
//! `concurrency.rs::exactly_one_concurrent_writer_wins` counts *winners* only — it never
//! asserts how the losers are classified. This seed adds the narrow, honest redb assertion
//! that the loser of a concurrent overwrite CAS is classified **precisely as
//! `CommitOutcome::Conflict`** (a re-readable lost race), not a silent overwrite and not an
//! opaque `Err`. That is a real property of the trait contract redb implements — and the one
//! the TiKV backend must also implement behind the unchanged trait — but the *check here* is
//! against **redb**, and only redb.
//!
//! # Determinism / execution
//!
//! The redb assertion holds under **every** madsim seed the `cargo xtask dst` sweep runs (it
//! is a property of the CAS contract, not a lucky schedule). Executed at Check by
//! `cargo xtask ci` → `run_dst` under `--cfg madsim` (`xtask/src/main.rs`). Requires
//! `--cfg madsim`; without it this file compiles to nothing (matching `concurrency.rs`), so a
//! plain `cargo test` neither builds nor runs it.
#![cfg(madsim)]

use std::sync::Arc;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::CommitOutcome;

const CHUNK: usize = 4;
const LEASE_EXPIRY: u64 = 6_000;
// The default erasure-coded data path (n = 9 fragments per chunk), matching the sibling
// commit-protocol campaign in `concurrency.rs`.
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

/// A unique, deterministic chunk-id generator starting just above `base`.
fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// **redb-only coverage.** Two writers read the SAME committed prior, each crosses `.await`
/// boundaries, then both race to CAS the inode over a real [`RedbMetadataStore`]. The seed
/// asserts the trait CAS contract redb implements: exactly one wins, and the loser is
/// classified **precisely as [`CommitOutcome::Conflict`]** (the classification
/// `concurrency.rs` does not assert). This makes **no** claim about `TikvMetadataStore`: a
/// TiKV commit regression cannot flip it (see the module docs — Option B), and it exhibits no
/// newly-reachable interleaving. The TiKV await-inside-`commit()` window is verified
/// off-Check on a real cluster.
#[madsim::test]
async fn redb_overwrite_cas_classifies_the_stale_writer_as_conflict() {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = Arc::new(RedbMetadataStore::in_memory().expect("redb"));
    let chunks = Arc::new(FsChunkStore::open(dir.path()).expect("fs store"));

    // An existing committed object at version 1 — the shared prior both writers read.
    let v0 = write::plan_write(b"v0", CHUNK, RS, ids_from(1)).unwrap();
    write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
    write::write_fragments(&*chunks, &v0).await.unwrap();
    write::commit_create(&*meta, 0, "obj", 1, &v0, 0)
        .await
        .unwrap();
    write::release(&*meta, &v0).await.unwrap();
    let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();

    // Two writers read the SAME prior, stage independently, then race to commit. Each spawned
    // task crosses `.await` boundaries (intent / write_fragments) between reading `prior` and
    // committing. madsim interleaves them from the seed. (Over redb this is the same class of
    // interleaving `concurrency.rs` already schedules — NOT a newly-reachable one.)
    let mut handles = Vec::new();
    for i in 0..2u128 {
        let meta = Arc::clone(&meta);
        let chunks = Arc::clone(&chunks);
        let prior = prior.clone();
        handles.push(madsim::task::spawn(async move {
            let plan =
                write::plan_write(b"contended", CHUNK, RS, ids_from(0x1000 * (i + 1))).unwrap();
            write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
            write::write_fragments(&*chunks, &plan).await.unwrap();
            // The terminal commit-point CAS against the (possibly now-stale) `prior`.
            // `orphaned_at_millis = 0`: this race exercises the commit point, not the
            // overwrite-reclaim grace stamp (#364), so any value is fine here.
            let outcome = write::commit_overwrite(&*meta, 1, &prior, &plan, 0)
                .await
                .unwrap();
            if outcome == CommitOutcome::Committed {
                write::release(&*meta, &plan).await.unwrap();
            }
            outcome
        }));
    }

    let mut committed = 0;
    let mut conflicted = 0;
    for handle in handles {
        match handle.await.unwrap() {
            CommitOutcome::Committed => committed += 1,
            CommitOutcome::Conflict => conflicted += 1,
        }
    }

    // Exactly one wins; the writer holding the now-stale `prior` loses. Both facts are about
    // REDB's implementation of the trait CAS contract — not about TiKV.
    assert_eq!(
        committed, 1,
        "redb: exactly one concurrent CAS committer must win the commit point",
    );
    assert_eq!(
        conflicted, 1,
        "redb: the writer holding the now-stale `prior` must be classified as `Conflict` \
         (a re-readable lost race) — not a silent overwrite and not an opaque `Err`. This is \
         the classification `concurrency.rs` does not assert; it is checked here against redb \
         only",
    );

    // The version advanced by EXACTLY one — the winner's commit, and only the winner's.
    let after = read::read_inode(&*meta, 1).await.unwrap().unwrap();
    assert_eq!(
        after.version,
        prior.version + 1,
        "redb: the inode version must advance by exactly one across the concurrent commit",
    );

    // The committed object is whole and readable (the winner's content) — read-after-commit.
    let bytes = read::read_path(&*meta, &*chunks, 0, "obj").await.unwrap();
    assert_eq!(bytes.as_deref(), Some(&b"contended"[..]));
}
