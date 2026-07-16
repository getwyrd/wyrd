//! Unit tests for the metadata **nemesis** leg-enumeration / dispatch-routing / runner-argument
//! decisions (`xtask::nemesis` — the Check-time flippable coverage, #407, slice 4 of #329;
//! ADR-0041).
//!
//! **Why these tests exist.** The live nemesis legs stand up a ≥3-process FoundationDB cluster
//! and partition / skew / pause it, which needs a container runtime and privileged networking,
//! so they are skipped in the unprivileged `cargo xtask ci` gate. "Deferred" means the *green
//! run* is off-Check; it does NOT mean the harness is unbuilt. The *routing decisions* the legs
//! turn on are pure, and this file exercises them at Check — mirroring the
//! `tier1_jepsen_isolation_legs` / `metadata_tier_dispatch` precedents. Each assertion uses an
//! independent expectation, not the literal the function returns.
//!
//! These tests run inside `cargo xtask ci`'s `cargo test --workspace` and go **RED** when a leg
//! is dropped, when two legs collapse onto one scenario function, when the runner argv stops
//! building the real fdb-feature scenario invocation, or when the name-drift guard stops
//! rejecting a leg that ran zero tests.

use xtask::nemesis::{
    metadata_nemesis_legs, nemesis_leg_ran_exactly_one, nemesis_scenario_args, parse_tests_run,
    NemesisLegKind, METADATA_NEMESIS_SCENARIO_TEST,
};

#[test]
fn campaign_runs_all_three_fault_classes_not_a_subset() {
    let legs = metadata_nemesis_legs();
    assert!(
        legs.contains(&NemesisLegKind::NetworkPartition),
        "the nemesis campaign must include the live-node network-partition leg (#399 technique); \
         got legs={legs:?}"
    );
    assert!(
        legs.contains(&NemesisLegKind::ClockSkew),
        "the nemesis campaign must include the clock-skew leg — the fault class nothing \
         implemented before #407; got legs={legs:?}"
    );
    assert!(
        legs.contains(&NemesisLegKind::ProcessPause),
        "the nemesis campaign must include the process-pause leg; got legs={legs:?}"
    );
}

#[test]
fn each_leg_routes_to_its_own_scenario_function() {
    let legs = metadata_nemesis_legs();
    // Sharing one scenario function between two legs would silently collapse a fault class into
    // another — the dispatch must be injective over the campaign.
    let fns: std::collections::HashSet<&str> = legs.iter().map(|k| k.scenario_fn()).collect();
    assert_eq!(
        fns.len(),
        legs.len(),
        "each nemesis leg must route to its OWN scenario function, not share one: {legs:?} -> {fns:?}"
    );

    // The dispatch names each fault it injects — a routing swap flips these behaviourally, not
    // by a compile error over a renamed constant.
    assert!(
        NemesisLegKind::NetworkPartition
            .scenario_fn()
            .contains("network_partition"),
        "the partition leg's function must name the partition it injects: {}",
        NemesisLegKind::NetworkPartition.scenario_fn()
    );
    assert!(
        NemesisLegKind::ClockSkew
            .scenario_fn()
            .contains("clock_skew"),
        "the clock-skew leg's function must name the skew it injects: {}",
        NemesisLegKind::ClockSkew.scenario_fn()
    );
    assert!(
        NemesisLegKind::ProcessPause
            .scenario_fn()
            .contains("process_pause"),
        "the pause leg's function must name the pause it injects: {}",
        NemesisLegKind::ProcessPause.scenario_fn()
    );
}

#[test]
fn runner_argv_carries_the_fdb_feature_and_the_ignored_exact_leg() {
    // The nemesis drives the real FoundationDB metadata path, so — like the #442 battery legs —
    // the argv MUST enable `--features fdb` (the backend is off by default) and run the
    // #[ignore]d scenario function in wyrd-metadata-fdb.
    let exact = NemesisLegKind::ClockSkew.scenario_fn();
    let args = nemesis_scenario_args(METADATA_NEMESIS_SCENARIO_TEST, exact);
    assert_eq!(
        args[0], "test",
        "must be a `cargo test` invocation: {args:?}"
    );
    let flat = args.join(" ");
    assert!(
        flat.contains("wyrd-metadata-fdb"),
        "must target the metadata-fdb crate: {flat}"
    );
    assert!(
        flat.contains("--features fdb"),
        "must enable the fdb backend feature so the real store is built: {flat}"
    );
    assert!(
        flat.contains(METADATA_NEMESIS_SCENARIO_TEST),
        "must run the nemesis scenario binary: {flat}"
    );
    assert!(
        flat.contains("--ignored") && flat.contains("--exact") && flat.contains(exact),
        "must run exactly the #[ignore]d {exact} leg function: {flat}"
    );
}

#[test]
fn the_name_drift_guard_rejects_a_leg_that_ran_zero_tests() {
    // `cargo test --exact <fn>` for a name that matches nothing runs ZERO tests and exits 0 — a
    // silent green no-op. The runner reads the executed count off the output and refuses a leg
    // unless EXACTLY one test ran, so renaming a scenario function on one side of the dispatch
    // fails the leg loudly. These are the exact shapes `cargo test` prints.
    let zero =
        "\nrunning 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored; 1 filtered out\n";
    let one = "\nrunning 1 test\ntest nemesis_metadata_under_clock_skew ... ok\n\ntest result: ok. 1 passed; 0 failed\n";
    let two = "\nrunning 2 tests\n\ntest result: ok. 2 passed; 0 failed\n";

    assert_eq!(
        parse_tests_run(zero),
        Some(0),
        "a `--exact` filter that matched nothing ran 0 tests"
    );
    assert_eq!(parse_tests_run(one), Some(1));
    assert_eq!(parse_tests_run(two), Some(2));

    // A line whose tail merely BEGINS with "test" but is not cargo's exact "test"/"tests" token
    // (e.g. a scenario that logged "running 5 testbeds") must NOT be read as a test count. The
    // loose `tail.starts_with("test")` the parser used to carry would wrongly return Some(5) here;
    // the tightened `matches!(tail, "test" | "tests")` returns None.
    assert_eq!(
        parse_tests_run("running 5 testbeds before the suite\n"),
        None,
        "a `running N test…`-lookalike line must not be read as cargo's test count"
    );
    assert!(
        !nemesis_leg_ran_exactly_one("running 5 testbeds before the suite\n"),
        "a `running N test…`-lookalike line is not evidence the leg's single scenario function ran"
    );

    assert!(
        !nemesis_leg_ran_exactly_one(zero),
        "a leg that ran 0 tests (name drift) must be REJECTED, not read as a green no-op"
    );
    assert!(
        nemesis_leg_ran_exactly_one(one),
        "a leg that ran exactly its one #[exact] scenario function is accepted"
    );
    assert!(
        !nemesis_leg_ran_exactly_one(two),
        "a filter that matched more than one test is not the single leg we asked for"
    );
}
