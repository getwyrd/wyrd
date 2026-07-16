//! Tier-1 **metadata nemesis** orchestration (#407, slice 4 of #329; ADR-0041 "nemesis first,
//! then the checked artifact"). The **host-independent** leg-enumeration + dispatch-routing +
//! runner-argument decisions for the three-class nemesis over the real multi-node M4 metadata
//! cluster, as pure functions with no privileged I/O (no `docker`, no `iptables`, no cluster).
//!
//! This is the metadata-nemesis sibling of [`crate::faults`]'s `tier1_jepsen_isolation_legs`
//! (`xtask/src/faults.rs:245`) and [`crate::metadata_faults`]'s `metadata_tier_dispatch`: the
//! privileged legs that stand up the ≥3-process FoundationDB cluster and partition / skew /
//! pause it are **deferred / off-Check** (opt-in `WYRD_TIER1=1`), but the *routing decisions*
//! they turn on are pure and unit-tested inside the unprivileged `cargo xtask ci` gate — the
//! "deferred ≠ unbuilt" bar.
//!
//! The leg **lifecycle**, the typed materialization **evidence**, the per-leg **oracle
//! arithmetic** and the live-leg **impls** live in `wyrd-metadata-fault-conformance` (Design
//! §1, importable by both the battery and #408). This module deliberately keeps `xtask` at
//! **zero new dependencies** (`xtask/Cargo.toml:11-14`): it owns ONLY the leg-kind enum, the
//! dispatch routing to each leg's scenario function, the runner-argument building, and the pure
//! "the leg actually ran" guard the runner reads off `cargo test`'s output.
//!
//! The runnable entry point that consumes this module is `xtask fdb_faults::run_metadata_nemesis`
//! (the `cargo xtask metadata-nemesis` subcommand): it stands up `deploy/fdb-multi-replica`,
//! resolves the topology, and drives each leg. That is what makes the brief's sign-off open
//! question — one witnessed `WYRD_TIER1=1` run of the three legs — satisfiable.

// ─── Leg kinds (mirrors faults::IsolationNemesis) ──────────────────────────────────────────

/// Which fault class a metadata nemesis leg injects. Modeled as a value with all three
/// alternatives representable — mirroring [`crate::faults`]'s `IsolationNemesis` born-at-tier
/// pattern — so that dropping a fault class (collapsing the campaign) is catchable by a
/// Check-time unit test (`xtask/tests/nemesis_orchestration.rs`), not merely absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NemesisLegKind {
    /// Live-node **network partition** — an in-netns `iptables` DROP (the #399 technique) that
    /// keeps the target `running` while its peers lose it.
    NetworkPartition,
    /// **Clock skew** — a static per-leg `libfaketime` offset on ONE cluster node, the
    /// harness/client clock untouched.
    ClockSkew,
    /// **Process pause** — a freezer-cgroup `docker pause`/`unpause` of one cluster node.
    ProcessPause,
}

impl NemesisLegKind {
    /// The `#[ignore]`d scenario function (inside the fdb-feature Tier-1 nemesis binary) that
    /// injects this leg. Each leg routes to its **own** function so a regression that points two
    /// legs at one function silently collapses them — the orchestration test catches that with a
    /// distinct-function-names assertion.
    ///
    /// **Name-drift safety.** `cargo test --exact <fn>` for a name that matches nothing runs ZERO
    /// tests and exits 0 — a silent green no-op. So renaming the scenario function here without
    /// updating the fdb test (or vice versa) is NOT caught by exit status; the runner instead
    /// reads the executed-test count off the output ([`parse_tests_run`]) and fails a leg that
    /// ran 0 tests.
    #[must_use]
    pub fn scenario_fn(self) -> &'static str {
        match self {
            NemesisLegKind::NetworkPartition => "nemesis_metadata_under_network_partition",
            NemesisLegKind::ClockSkew => "nemesis_metadata_under_clock_skew",
            NemesisLegKind::ProcessPause => "nemesis_metadata_under_process_pause",
        }
    }

    /// A stable, human-readable slug for diagnostics (the runner names the failing leg by it).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            NemesisLegKind::NetworkPartition => "network-partition",
            NemesisLegKind::ClockSkew => "clock-skew",
            NemesisLegKind::ProcessPause => "process-pause",
        }
    }
}

/// Which nemesis legs the Tier-1 metadata campaign runs, and in what order, once
/// `run_metadata_nemesis` routes to the in-repo scenario. Pure — a plain `Vec` the orchestration
/// unit test inspects directly (not a downstream argv helper), so a regression that drops the
/// clock-skew leg (the fault class nothing implemented before #407) — or any leg — flips the test
/// red rather than resting red on non-existence.
#[must_use]
pub fn metadata_nemesis_legs() -> Vec<NemesisLegKind> {
    vec![
        NemesisLegKind::NetworkPartition,
        NemesisLegKind::ClockSkew,
        NemesisLegKind::ProcessPause,
    ]
}

/// The in-repo fdb-feature Tier-1 nemesis scenario test binary (the `cargo test --test <name>`
/// target). The legs wire into this feature-gated scenario under `crates/metadata-fdb/tests/`
/// (type-checked under the privileged `WYRD_FDB_TOOLCHAIN` opt-in), each `#[ignore]`d and
/// selected by [`NemesisLegKind::scenario_fn`].
pub const METADATA_NEMESIS_SCENARIO_TEST: &str = "tier1_metadata_nemesis";

/// The `cargo test --features fdb --test <name> -- --ignored --exact <fn>` argv that runs one
/// nemesis leg's scenario function inside the in-repo `test` binary. Downstream of
/// [`metadata_nemesis_legs`] (which leg) and [`NemesisLegKind::scenario_fn`] (which function).
///
/// The nemesis drives the real FoundationDB metadata path behind the unchanged trait, so — like
/// the #442 battery legs — the argv MUST carry `--features fdb` (the backend is off by default,
/// so the whole-tree gate compiles the empty skeleton) and target the `wyrd-metadata-fdb` crate.
#[must_use]
pub fn nemesis_scenario_args<'a>(test: &'a str, exact_fn: &'a str) -> [&'a str; 12] {
    [
        "test",
        "-p",
        "wyrd-metadata-fdb",
        "--features",
        "fdb",
        "--test",
        test,
        "--",
        "--ignored",
        "--exact",
        exact_fn,
        "--nocapture",
    ]
}

/// The number of tests `cargo test` reported it **ran**, parsed off its `running N test(s)` line,
/// or `None` when the line is absent. The runner uses this to close the `--exact` name-drift hole:
/// a leg that ran 0 tests (a scenario function renamed on one side of the dispatch but not the
/// other) is a silent green no-op under exit status alone, so the runner refuses a leg unless it
/// ran **exactly one** test.
///
/// `cargo test` prints `running 0 tests` / `running 1 test` / `running 2 tests`; the count is the
/// token after `running`.
#[must_use]
pub fn parse_tests_run(cargo_test_output: &str) -> Option<usize> {
    cargo_test_output.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("running ")?;
        let count = rest.split_whitespace().next()?;
        // Only accept the EXACT "running N test" / "running N tests" shape cargo prints (singular
        // for one test, plural otherwise), never an unrelated line that merely begins "running ".
        // `matches!(tail, "test" | "tests")` — not `starts_with("test")`, which would also swallow
        // "running 1 testbed …".
        let tail = rest[count.len()..].trim();
        matches!(tail, "test" | "tests")
            .then(|| count.parse::<usize>().ok())
            .flatten()
    })
}

/// Whether a leg's `cargo test` output proves the scenario function actually executed — exactly
/// one test ran. `false` when the `--exact` filter matched nothing (name drift) or matched more
/// than one. This is the guard `run_metadata_nemesis` gates each leg on, so a renamed scenario
/// function fails the leg loudly instead of reporting a green run nobody exercised.
#[must_use]
pub fn nemesis_leg_ran_exactly_one(cargo_test_output: &str) -> bool {
    parse_tests_run(cargo_test_output) == Some(1)
}
