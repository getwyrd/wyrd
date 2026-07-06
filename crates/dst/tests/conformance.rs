//! The **shared** `MetadataStore` trait-contract suite, run inside the deterministic
//! simulator over **both** implementations — the deterministic redb backend and the
//! deterministic simulated-TiKV model (`support::SimTikvMetadataStore`) — so the
//! identical property suite pins the trait by two implementations (ADR-0006;
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
#![cfg(madsim)]

use wyrd_metadata_redb::RedbMetadataStore;

#[path = "support/mod.rs"]
mod support;
use support::SimTikvMetadataStore;

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
