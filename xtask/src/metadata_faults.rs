//! Tier-1/Tier-2 **metadata-backend** fault-harness orchestration (M4.6, #257; proposal
//! 0015 §"DST and tests", PR-sequence item 6; ADR-0039 in-repo consistency scenario;
//! ADR-0015 single-zone contract). The **host-independent** run-routing decision for the
//! metadata-swap fault harness, as a pure function with no privileged I/O (no `docker`, no
//! `iptables`, no cluster).
//!
//! This is the metadata-tier sibling of `xtask/src/faults.rs`'s `jepsen_dispatch`: the
//! privileged legs that stand up a real ≥3-replica TiKV Raft group and partition it live are
//! **deferred / off-Check**, but the *routing decision* they turn on is pure and unit-tested
//! inside the unprivileged `cargo xtask ci` gate — the "deferred ≠ unbuilt" bar.
//!
//! The partition **fault-effect oracle** and the **quorum/consistency arithmetic** live in
//! [`wyrd_testkit`] (`partition_took_effect`, `heal_is_complete`, `partition_materialized`,
//! `consistency_passes`, …) so the **live scenario itself** can call them — they are wired
//! into `crates/metadata-tikv/tests/tier1_metadata_consistency.rs`, not dead code.

// ─── Run-routing dispatch (mirrors faults::jepsen_dispatch) ───────────────────────

/// The in-repo Tier-1 metadata consistency-over-the-swap scenario test (the `cargo test
/// --test <name>` target). The ADR-0039-sanctioned realization of the consistency leg: an
/// in-repo Rust scenario driving the production `metadata-tikv` path, NOT a literal public
/// Jepsen tool (deferred to #329).
pub const METADATA_TIER1_SCENARIO_TEST: &str = "tier1_metadata_consistency";

/// The Tier-2 single-machine metadata I/O scenario test.
pub const METADATA_TIER2_SCENARIO_TEST: &str = "tier2_metadata_io";

/// A deprecated external-harness env var. There is **no** external metadata fault harness;
/// this is representable only so the routing decision stays testable — selecting it is a
/// hard error downstream, never a shell-out (the same shape `jepsen_dispatch` keeps for its
/// removed legacy command).
pub const METADATA_TIER1_LEGACY_CMD_VAR: &str = "WYRD_TIER1_METADATA_CMD";

/// Where a metadata Tier run routes. Modelling the route as a value with BOTH alternatives
/// representable is the non-tautological bar (Success criterion §2): a Check-time unit test
/// binds to [`metadata_tier_dispatch`], and a regression that re-points the live route at
/// the (nonexistent) external command flips that test **red behaviourally** — not by a
/// compile error over a deleted module, and not by a constant the runner never reads.
#[derive(Debug, PartialEq, Eq)]
pub enum MetadataTierDispatch {
    /// Run the in-repo `cargo test --test <test> --features tikv` consistency scenario.
    InRepoScenario { test: &'static str },
    /// The removed external shell-out to `env_var` — representable but never re-selected for
    /// the default inputs; the runner turns it into a hard error.
    ExternalCommand { env_var: &'static str },
}

/// Decide where a metadata Tier run routes. Pure — decided solely from
/// `legacy_cmd_configured` (whether the deprecated [`METADATA_TIER1_LEGACY_CMD_VAR`] is
/// set) — so the dispatch test binds to it without a privileged environment. The default
/// (legacy var unset) is always the in-repo scenario (ADR-0039).
#[must_use]
pub fn metadata_tier_dispatch(legacy_cmd_configured: bool) -> MetadataTierDispatch {
    if legacy_cmd_configured {
        MetadataTierDispatch::ExternalCommand {
            env_var: METADATA_TIER1_LEGACY_CMD_VAR,
        }
    } else {
        MetadataTierDispatch::InRepoScenario {
            test: METADATA_TIER1_SCENARIO_TEST,
        }
    }
}

/// The `cargo test --features tikv --test <name> -- --ignored` argv that runs an in-repo
/// metadata scenario. The metadata scenarios drive the real `TikvMetadataStore` behind the
/// unchanged trait, so — unlike the custodian legs — the argv MUST carry `--features tikv`
/// (the backend is off by default so the whole-tree gate compiles the empty skeleton).
#[must_use]
pub fn metadata_scenario_args(test: &str) -> [&str; 10] {
    [
        "test",
        "-p",
        "wyrd-metadata-tikv",
        "--features",
        "tikv",
        "--test",
        test,
        "--",
        "--ignored",
        "--nocapture",
    ]
}
