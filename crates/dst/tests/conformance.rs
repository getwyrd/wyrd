//! The **shared** `MetadataStore` trait-contract suite, run inside the deterministic
//! simulator over **every** in-simulator implementation — the deterministic redb backend,
//! the deterministic simulated-TiKV model (`support::SimTikvMetadataStore`) and the
//! deterministic simulated-FDB model (`support::SimFdbMetadataStore`, issue #468) — so the
//! identical property suite pins the trait by more than one implementation (ADR-0006;
//! proposal 0015 §"Pinning the trait with the second implementation" and
//! §"Definition of done", accepted
//! `docs/design/proposals/accepted/0015-milestone-4-production-metadata-backend-revised.md:546-555,655-657`).
//!
//! The assertions are the *same* `wyrd_metadata_conformance::run_all` clauses redb
//! and TiKV already share out-of-simulator
//! (`crates/metadata-redb/tests/conformance.rs`,
//! `crates/metadata-tikv/tests/conformance.rs`); here they run under madsim's
//! seed-reproducible scheduler (ADR-0009), where the simulated-TiKV model's
//! await-inside-commit shape is exercised. "Shared, not forked" (proposal 0015 lines
//! 548, 656): the clauses redb passes are the clauses the second implementation
//! passes — neither backend gets its own copy.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`); without it this file compiles
//! to nothing, so a normal `cargo test` neither builds nor runs it.

#![forbid(unsafe_code)]
#![cfg(madsim)]

use wyrd_metadata_redb::RedbMetadataStore;

#[path = "support/mod.rs"]
mod support;
use support::{SimFdbMetadataStore, SimTikvMetadataStore};

/// The deterministic redb backend passes every shared contract clause under the
/// simulator (Tier-0 spine, unchanged — proposal 0015 lines 489-499).
#[madsim::test]
async fn redb_backend_passes_shared_contract() {
    wyrd_metadata_conformance::run_all(|_tag| async {
        RedbMetadataStore::in_memory().expect("redb in-memory store")
    })
    .await;
}

/// The second implementation — the deterministic simulated-TiKV model, with its
/// await-inside-commit shape — passes the **identical** shared contract clauses,
/// pinning the trait by two implementations (ADR-0006).
#[madsim::test]
async fn sim_tikv_backend_passes_shared_contract() {
    wyrd_metadata_conformance::run_all(|_tag| async { SimTikvMetadataStore::new() }).await;
}

/// The third implementation — the deterministic simulated-FDB model, whose commit is
/// *optimistic* (no lock; the resolver rejects at commit time) — passes the **identical**
/// shared contract clauses. Shared, not forked: the FFI backend Wyrd chose for production
/// (ADR-0042) is now held to the same in-simulator standard as the backends it replaces
/// (issue #468).
///
/// The commit-ambiguity nemesis is deliberately **off** here — [`SimFdbMetadataStore::new`]
/// builds the store at `FdbFidelity::OptimisticConflictAtCommit`, on which
/// `arm_commit_ambiguity` refuses to arm at all. The shared clauses legitimately assume a
/// determinate commit outcome, so arming it would be testing the suite rather than the
/// store. The ambiguity property gets its own body in `crates/dst/tests/commit_ambiguity.rs`.
#[madsim::test]
async fn sim_fdb_backend_passes_shared_contract() {
    wyrd_metadata_conformance::run_all(|_tag| async { SimFdbMetadataStore::new() }).await;
}
