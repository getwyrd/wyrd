//! Unit tests for the metadata-backend Tier fault-harness **run-routing** dispatch
//! (`xtask::metadata_faults` — the Check-time flippable coverage, M4.6, #257; proposal
//! 0015 §"DST and tests", PR-sequence item 6; ADR-0039; ADR-0015).
//!
//! **Why these tests exist.** The metadata Tier-1 consistency / Tier-2 legs are "deferred"
//! tiers: their real scenarios stand up a ≥3-replica TiKV Raft group and partition it live,
//! which needs a container runtime and privileged networking, so they are skipped in the
//! unprivileged `cargo xtask ci` gate. "Deferred" means the *green run* is off-Check; it
//! does NOT mean the harness is unbuilt. The *routing decision* the legs turn on is pure,
//! and this file exercises it at Check — mirroring the `jepsen_dispatch` precedent
//! (`xtask/src/faults.rs`). The partition **fault-effect oracle** + quorum arithmetic live
//! in `wyrd-testkit` (unit-tested there) and are wired into the live scenario itself.
//!
//! These tests run inside `cargo xtask ci`'s `cargo test --workspace` and go **RED** when
//! the dispatch is regressed. Each assertion uses an independent expectation, not the
//! literal the function returns.

use xtask::metadata_faults::{
    metadata_scenario_args, metadata_tier_dispatch, MetadataTierDispatch,
    METADATA_TIER1_LEGACY_CMD_VAR, METADATA_TIER1_SCENARIO_TEST,
};

#[test]
fn dispatch_routes_to_in_repo_scenario_not_external_command() {
    // The production Tier job opts in and does NOT set the deprecated external-command var,
    // so `legacy_cmd_configured` is false and the route MUST be the in-repo scenario
    // (ADR-0039), never a shell-out. Reverting the dispatch so the default input returns
    // `ExternalCommand` — reproducing an inert external-harness route — hits the panic arm.
    match metadata_tier_dispatch(false) {
        MetadataTierDispatch::InRepoScenario { test } => assert_eq!(
            test, METADATA_TIER1_SCENARIO_TEST,
            "the default metadata Tier-1 route must be the in-repo consistency scenario"
        ),
        MetadataTierDispatch::ExternalCommand { env_var } => panic!(
            "metadata Tier dispatch regressed to the (removed) external `{env_var}` \
             shell-out instead of the in-repo {METADATA_TIER1_SCENARIO_TEST} scenario"
        ),
    }
}

#[test]
fn legacy_var_is_the_only_path_to_the_removed_external_route() {
    // The external route is representable but is NEVER the default — only an explicitly-set
    // legacy var selects it (and the runner then hard-errors rather than shelling out).
    assert!(
        matches!(
            metadata_tier_dispatch(true),
            MetadataTierDispatch::ExternalCommand { env_var }
                if env_var == METADATA_TIER1_LEGACY_CMD_VAR
        ),
        "only the legacy WYRD_TIER1_METADATA_CMD var may reach the removed external route"
    );
}

#[test]
fn in_repo_scenario_argv_carries_the_tikv_feature_and_ignored_flag() {
    // The metadata scenarios drive the real TikvMetadataStore, so — unlike the custodian
    // legs — the argv MUST enable `--features tikv` (the backend is off by default). It runs
    // the #[ignore]d scenario in wyrd-metadata-tikv and never names the external command.
    let args = metadata_scenario_args(METADATA_TIER1_SCENARIO_TEST);
    assert_eq!(
        args[0], "test",
        "must be a `cargo test` invocation: {args:?}"
    );
    let flat = args.join(" ");
    assert!(
        flat.contains("wyrd-metadata-tikv"),
        "must target the metadata-tikv crate: {flat}"
    );
    assert!(
        flat.contains("--features tikv"),
        "must enable the tikv backend feature so the real store is built: {flat}"
    );
    assert!(
        flat.contains(METADATA_TIER1_SCENARIO_TEST) && flat.contains("--ignored"),
        "must run the #[ignore]d {METADATA_TIER1_SCENARIO_TEST} scenario: {flat}"
    );
    assert!(
        !flat.contains(METADATA_TIER1_LEGACY_CMD_VAR),
        "the in-repo route argv must not reference the external command var: {flat}"
    );
}
