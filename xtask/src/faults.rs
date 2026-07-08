//! **Tier-1 / Tier-2 custodian fault runners** — the *deferred-posture* legs of the M3
//! verification campaign (proposal 0005 §"DST and tests": Tier-1 disk-fault injection +
//! Jepsen, Tier-2 single-node kill-and-reconstruct, `0005:405-411`; the `xtask`
//! touch-point `0005:437-438`; PR-sequence slice 8 `0005:541-545`).
//!
//! These tiers exercise behaviour the deterministic Tier-0 campaign cannot: **real**
//! block-layer misbehaviour (device-mapper `dm-flakey` / `dm-error`), an in-repo
//! consistency harness asserting the ADR-0015 contract over the repair path under
//! partition and crash faults, and a **real node** with real NVMe/fsync. They need
//! root / privileged I/O / a container runtime, so they are **never** part of
//! `cargo xtask ci` (which stays unprivileged and container-free, ADR-0016) and never
//! run in the deterministic worktree (containerizing them would break seed determinism,
//! ADR-0009 / INTEGRATION §3).
//!
//! Each runner is **deferred by default**: without an explicit opt-in it prints what it
//! requires and exits cleanly, so it is INERT at Check and on a normal dev box. The
//! dedicated off-Check CI / Tier-2 job opts in (`WYRD_TIER1=1` / `WYRD_TIER2=1`);
//! there a missing tool is a hard failure. Only the gating decision ([`plan`]) is pure
//! and unit-tested.

use std::process::Command;

/// What a fault runner should do, decided purely from its inputs so the gating is
/// unit-testable without a privileged environment.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Plan {
    /// Not opted in: the tier is deferred / off-Check. Print `reason` and exit cleanly.
    Deferred(String),
    /// Opted in but the required tool is absent: a hard failure (the dedicated job
    /// promised the environment). Carries the error message.
    MissingTool(String),
    /// Opted in and the tool is present: run the scenario.
    Run,
}

/// Decide what a runner does. Opt-in is explicit (`opted_in`) because these tiers need
/// privileged / real environments that must never be assumed present. Mirrors the spirit
/// of the Tier-2 container gate ([`crate::run_integration`]): a job that opted in but
/// lacks its tool fails hard, rather than silently skipping the coverage it promised.
pub(crate) fn plan(tier: &str, tool: &str, opted_in: bool, tool_available: bool) -> Plan {
    if !opted_in {
        return Plan::Deferred(format!(
            "{tier} is a deferred (off-Check) tier; set its opt-in env var and provide the \
             harness command to run it. Skipping."
        ));
    }
    if !tool_available {
        return Plan::MissingTool(format!(
            "{tier} was opted in but its required tool `{tool}` is not available"
        ));
    }
    Plan::Run
}

/// Is `tool` runnable (a `--version` probe succeeds)? Used to gate the privileged run.
fn tool_available(tool: &str) -> bool {
    Command::new(tool)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn opted_in(var: &str) -> bool {
    std::env::var_os(var).is_some_and(|v| v == "1")
}

/// **Tier-1 disk-fault injection** (`0005:405-408`): drive scrub + the checksum-
/// verification path against **real** block-layer misbehaviour via device-mapper
/// `dm-error` — the real-hardware complement to DST's modelled bit rot.
/// Needs root + device-mapper (`dmsetup`). Opt in with `WYRD_TIER1=1`.
///
/// When opted in and `dmsetup` is present, dispatches to the in-repo Tier-1
/// scenario at `crates/custodian/tests/tier1_disk_faults.rs` via
/// `cargo test --ignored` — **replacing** the old `WYRD_TIER1_DISK_CMD`
/// external-command shell-out (pre-#195) with a real in-repo harness.
pub fn run_disk_faults() -> Result<(), String> {
    let p = plan(
        "Tier-1 disk-fault injection (dm-error)",
        "dmsetup",
        opted_in("WYRD_TIER1"),
        tool_available("dmsetup"),
    );
    match p {
        Plan::Deferred(reason) => {
            eprintln!("xtask: {reason}");
            Ok(())
        }
        Plan::MissingTool(msg) => Err(msg),
        Plan::Run => run_tier1_scenario(),
    }
}

/// Invoke the `#[ignore]`d Tier-1 disk-fault scenario at
/// `crates/custodian/tests/tier1_disk_faults.rs` via `cargo test --ignored`.
///
/// This replaces the old `WYRD_TIER1_DISK_CMD` external-command shell-out with
/// an in-repo `cargo test` invocation. The scenario drives the **production**
/// `FsChunkStore` / `reconcile_step` / `ScrubContext` / `ReconstructionContext`
/// APIs over a real `dm-error`-backed device (root required; opted in via
/// `WYRD_TIER1=1` in the Tier-1 CI job).
fn run_tier1_scenario() -> Result<(), String> {
    let args = [
        "test",
        "-p",
        "wyrd-custodian",
        "--test",
        "tier1_disk_faults",
        "--",
        "--ignored",
        "--nocapture",
    ];
    println!("\n$ cargo {}", args.join(" "));
    let status = Command::new("cargo")
        .args(args)
        .current_dir(crate::workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn cargo for Tier-1 scenario: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Tier-1 disk-fault scenario failed with {status}"))
    }
}

// ---- Tier-1 Jepsen consistency cluster constants ----

/// Docker Compose project name for the Tier-1 Jepsen consistency cluster —
/// distinct from the Tier-2 project (`wyrd-tier2`) to avoid namespace collision.
const JEPSEN_PROJECT: &str = "wyrd-tier1-jepsen";

/// Number of D-server containers for the Jepsen consistency cluster:
/// RS(6,3) needs 9 servers for the initial N=9 fragment placement, plus 1 spare
/// (domain J) that reconstruction places the rebuilt fragment onto after the victim
/// is killed. Matches [`crate::kill_reconstruct::KR_DSERVER_COUNT`].
const JEPSEN_DSERVER_COUNT: usize = 10; // N=9 + 1 spare

/// The D-server container image built by `crates/chunkstore-grpc/tests/docker-compose.yml`
/// (`image: wyrd-dserver:test`). The [`IsolationNemesis::NetworkPartition`] leg reuses this
/// image — which now ships `iptables` (`crates/chunkstore-grpc/tests/dserver/Dockerfile`)
/// — as a throwaway `--net container:<isolated>` sidecar that injects/heals the in-netns
/// packet drop, so the isolated D-server itself is never touched (stays `running`, keeps
/// its published-port mapping). Exported to the scenario as `WYRD_TIER1_DSERVER_IMAGE`.
const JEPSEN_DSERVER_IMAGE: &str = "wyrd-dserver:test";

/// The in-repo Tier-1 Jepsen consistency scenario test (the `cargo test --test <name>`
/// target). The post-#250 route for [`run_jepsen`].
const JEPSEN_SCENARIO_TEST: &str = "tier1_jepsen_consistency";

/// The deprecated pre-#250 external-harness env var. The external shell-out was removed in
/// #250; selecting it now is a hard error pointing at [`JEPSEN_SCENARIO_TEST`], never a
/// shell-out. Retained only so the routing decision stays representable and testable.
const JEPSEN_LEGACY_CMD_VAR: &str = "WYRD_TIER1_JEPSEN_CMD";

/// Where the Tier-1 Jepsen leg routes once opted in (`WYRD_TIER1=1`) and `docker` is
/// present — the **observable routing decision** [`run_jepsen`] consumes on its
/// `Plan::Run` path.
///
/// Modeling the route as a value with BOTH alternatives representable is the iter-6/7/8
/// fix (Success criterion §1): the prior attempts hid the choice inside a hardcoded match
/// arm and tested only the in-repo branch's argv, so reverting the dispatch to the
/// external shell-out left the test green. Here a Check-time unit test binds to
/// [`jepsen_dispatch`] and a regression that re-points the live route at the external
/// command flips that test **red behaviourally** (it panics on `ExternalCommand`), not by
/// a compile error over a deleted module and not by a constant the runner never reads.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum JepsenDispatch {
    /// Post-#250: run the in-repo `cargo test --test <test>` consistency scenario.
    InRepoScenario { test: &'static str },
    /// Pre-#250: the inert external shell-out to `env_var`. Removed in #250 — [`run_jepsen`]
    /// turns this into a hard error, never a shell-out — but kept representable so a
    /// regression re-pointing the live route at it is caught by the dispatch test.
    ExternalCommand { env_var: &'static str },
}

/// Decide where [`run_jepsen`] routes. Pure — decided solely from `legacy_cmd_configured`
/// (whether the deprecated [`JEPSEN_LEGACY_CMD_VAR`] is set) — so the dispatch test binds
/// to it without a privileged environment.
///
/// Post-#250 the route is the in-repo scenario. The legacy external command is honoured
/// only as a **removed** route (a hard error downstream), never re-selected for the
/// default inputs the production Tier-1 job runs with (`WYRD_TIER1=1`, the legacy var
/// unset). Reverting this so the default (`false`) input returns `ExternalCommand`
/// reproduces the pre-#250 shell-out behaviour and turns the dispatch test red.
pub(crate) fn jepsen_dispatch(legacy_cmd_configured: bool) -> JepsenDispatch {
    if legacy_cmd_configured {
        JepsenDispatch::ExternalCommand {
            env_var: JEPSEN_LEGACY_CMD_VAR,
        }
    } else {
        JepsenDispatch::InRepoScenario {
            test: JEPSEN_SCENARIO_TEST,
        }
    }
}

/// **Tier-1 isolation nemesis** (issue #399, ADR-0039's named additive upgrade): which
/// mechanism a Tier-1 Jepsen leg uses to make server 1 unreachable mid-repair.
///
/// Modeled as a value with BOTH alternatives representable — mirroring
/// [`JepsenDispatch`]'s born-at-tier pattern — so that dropping the network-partition
/// leg (collapsing the campaign back to freeze-only, the exact gap ADR-0039 names) is
/// catchable by a Check-time unit test ([`tier1_jepsen_isolation_legs_includes_network_partition_not_freeze_only`]),
/// not merely absent.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum IsolationNemesis {
    /// Jepsen's `:pause` — a freezer-cgroup process freeze (`docker pause`/`unpause`,
    /// `tier1_jepsen_consistency.rs:832-841`/`:901-909`). The isolated node's own clock
    /// STOPS. Cheaper; kept as the existing leg (#250) — Scope says do not delete it.
    ProcessFreeze,
    /// Jepsen's `:partition` — a network-level packet drop (an in-netns `iptables` DROP
    /// on the gRPC port) that keeps the container in the `running` state and PRESERVES
    /// its published-port mapping: the node stays LIVE on its own clock,
    /// network-unreachable, for the fault window, then is reachable again at the SAME
    /// endpoint on heal. The #399 upgrade this issue adds.
    NetworkPartition,
}

impl IsolationNemesis {
    /// The `#[ignore]`d scenario test function (inside the [`JEPSEN_SCENARIO_TEST`]
    /// binary) that injects this nemesis. Each nemesis routes to its OWN function so a
    /// regression that points both at the same function silently collapses one leg into
    /// the other — the [`tier1_jepsen_isolation_legs_includes_network_partition_not_freeze_only`]
    /// test catches that too (distinct-function-names assertion).
    pub(crate) fn scenario_fn(self) -> &'static str {
        match self {
            IsolationNemesis::ProcessFreeze => {
                "jepsen_consistency_over_repair_under_partition_and_crash"
            }
            IsolationNemesis::NetworkPartition => {
                "jepsen_consistency_over_repair_under_live_partition_and_crash"
            }
        }
    }
}

/// Which isolation nemeses the Tier-1 Jepsen leg runs, and in what order, once
/// [`run_jepsen`] routes to the in-repo scenario. Pure — a plain `Vec` the dispatch unit
/// test inspects directly (not a downstream argv helper), so a regression that drops
/// [`IsolationNemesis::NetworkPartition`] — collapsing the leg back to the freeze-only
/// nemesis ADR-0039 names as the gap this issue closes — flips the test red rather than
/// resting red on non-existence.
pub(crate) fn tier1_jepsen_isolation_legs() -> Vec<IsolationNemesis> {
    vec![
        IsolationNemesis::ProcessFreeze,
        IsolationNemesis::NetworkPartition,
    ]
}

/// The `cargo test --ignored` argv that runs one `exact_fn` scenario function inside the
/// in-repo `test` binary — the same `cargo test --test <name> -- --ignored --exact <fn>`
/// shape the sibling legs use. Downstream of the [`jepsen_dispatch`] decision (the
/// `InRepoScenario { test }` route supplies `test`) and of [`IsolationNemesis::scenario_fn`]
/// (supplies `exact_fn`, selecting which of the two isolation-nemesis functions runs).
fn jepsen_scenario_args<'a>(test: &'a str, exact_fn: &'a str) -> [&'a str; 10] {
    [
        "test",
        "-p",
        "wyrd-chunkstore-grpc",
        "--test",
        test,
        "--",
        "--ignored",
        "--exact",
        exact_fn,
        "--nocapture",
    ]
}

/// **Tier-1 Jepsen consistency** (`0005:408`): assert the ADR-0015 consistency
/// contract over the custodian repair path under **partitions and crashes**.
///
/// Stands up a real containerized RS(6,3) cluster via docker-compose, injects
/// a crash fault (`docker kill`, server 0) and a network partition (`docker pause`
/// / `unpause`, server 1) mid-repair, drives the **production**
/// `custodian::reconcile_step` → `reconstruction::reconcile` path, and asserts:
/// - commit-point-atomic repair (a crash/partition before commit leaves collectable
///   garbage, never a torn/hybrid chunk — `0005:385-389`);
/// - read-after-commit (a committed value remains readable — ADR-0015);
/// - exactly-once convergence (repair commits exactly once across the heal);
/// - no stale/torn reads after partition-and-heal.
///
/// Replaces the old `WYRD_TIER1_JEPSEN_CMD` external shell-out (pre-#250) and the
/// obsolete `lein` probe with a real in-repo harness mirroring the two merged
/// sibling legs (`run_disk_faults` / `run_tier1_scenario`, #195; `run_kill_reconstruct`,
/// #196). Opt in with `WYRD_TIER1=1`. Requires `docker` (compose plugin).
pub fn run_jepsen() -> Result<(), String> {
    let p = plan(
        "Tier-1 Jepsen consistency",
        "docker",
        opted_in("WYRD_TIER1"),
        tool_available("docker"),
    );
    match p {
        Plan::Deferred(reason) => {
            eprintln!("xtask: {reason}");
            return Ok(());
        }
        Plan::MissingTool(msg) => return Err(msg),
        Plan::Run => {}
    }

    // Route the run. The decision is a value (`jepsen_dispatch`) the dispatch test binds
    // to — re-pointing the live route at the removed external command flips that test red.
    match jepsen_dispatch(std::env::var_os(JEPSEN_LEGACY_CMD_VAR).is_some()) {
        JepsenDispatch::InRepoScenario { test } => {
            // Run each isolation-nemesis leg (`tier1_jepsen_isolation_legs`) against its
            // OWN freshly-stood-up cluster — server 0 is `docker kill`ed (permanently
            // dead) by each leg, so the legs cannot share one live cluster.
            for nemesis in tier1_jepsen_isolation_legs() {
                run_jepsen_scenario(test, nemesis)?;
            }
            Ok(())
        }
        JepsenDispatch::ExternalCommand { env_var } => Err(format!(
            "the external `{env_var}` Tier-1 Jepsen harness was removed in #250; the \
             in-repo `{JEPSEN_SCENARIO_TEST}` consistency scenario is the Tier-1 harness \
             now — unset `{env_var}` and re-run `cargo xtask jepsen`"
        )),
    }
}

/// Orchestrate a single Tier-1 Jepsen consistency leg's run for the given `nemesis`:
/// stand up a fresh [`JEPSEN_DSERVER_COUNT`]-server cluster, resolve endpoints, pass
/// the cluster info to the scenario test via env vars, then finalize (log capture
/// before teardown, unconditional teardown) — the same pattern as
/// [`run_kill_reconstruct`]. Called once per leg in [`tier1_jepsen_isolation_legs`]
/// order by [`run_jepsen`], each against its OWN cluster (server 0 is permanently
/// killed within a leg, so legs cannot share one live cluster).
fn run_jepsen_scenario(test: &str, nemesis: IsolationNemesis) -> Result<(), String> {
    let compose = crate::workspace_root()
        .join("crates/chunkstore-grpc/tests/docker-compose.yml")
        .to_string_lossy()
        .to_string();

    // 0-indexed server 0 → Docker Compose 1-indexed replica 1.
    let victim_container = format!("{JEPSEN_PROJECT}-dserver-1");
    // 0-indexed server 1 → Docker Compose 1-indexed replica 2.
    let partition_container = format!("{JEPSEN_PROJECT}-dserver-2");

    jepsen_compose_up(&compose)?;
    crate::finalize_panic_safe(
        || {
            let endpoints = jepsen_resolve_endpoints(&compose)?;
            println!(
                "\nxtask jepsen ({nemesis:?}): {JEPSEN_DSERVER_COUNT} D servers at \
                 {endpoints}; victim={victim_container}, isolated={partition_container}"
            );
            run_jepsen_test(
                &endpoints,
                &victim_container,
                &partition_container,
                test,
                nemesis,
            )
        },
        |result| {
            crate::finish_integration(
                result,
                || jepsen_compose_logs(&compose),
                || jepsen_compose_down(&compose),
            )
        },
    )?;
    println!("\nxtask jepsen ({nemesis:?}): Tier-1 consistency scenario passed");
    Ok(())
}

/// Run the (otherwise `#[ignore]`d) Tier-1 Jepsen consistency scenario function that
/// injects `nemesis`, with the resolved cluster endpoints and fault-injection targets
/// exported as env vars, so the test dials the live container cluster and knows which
/// containers to kill and isolate.
///
/// Runs the in-repo `test` scenario the [`jepsen_dispatch`] decision selected, filtered
/// to `nemesis`'s [`IsolationNemesis::scenario_fn`] — the
/// `cargo test --test <test> -- --ignored --exact <fn>` invocation built by
/// [`jepsen_scenario_args`].
fn run_jepsen_test(
    endpoints: &str,
    victim_container: &str,
    partition_container: &str,
    test: &str,
    nemesis: IsolationNemesis,
) -> Result<(), String> {
    let exact_fn = nemesis.scenario_fn();
    let args = jepsen_scenario_args(test, exact_fn);
    crate::print_step(&{
        let mut display = vec!["cargo"];
        display.extend_from_slice(&args);
        display
    });
    let status = Command::new("cargo")
        .args(args)
        .current_dir(crate::workspace_root())
        .env("WYRD_DSERVER_ENDPOINTS", endpoints)
        .env("WYRD_TIER1_VICTIM_CONTAINER", victim_container)
        .env("WYRD_TIER1_PARTITION_CONTAINER", partition_container)
        // The image the [`IsolationNemesis::NetworkPartition`] leg reuses as a
        // `--net container:<isolated>` iptables sidecar (unused by the `ProcessFreeze`
        // leg's function, harmless to export always) — it never disconnects the
        // container, so the published-port mapping survives the fault window.
        .env("WYRD_TIER1_DSERVER_IMAGE", JEPSEN_DSERVER_IMAGE)
        .status()
        .map_err(|e| format!("failed to spawn cargo for Tier-1 Jepsen ({exact_fn}): {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "Tier-1 Jepsen consistency scenario `{exact_fn}` failed with {status}"
        ))
    }
}

// ---- Jepsen-specific compose helpers ----
//
// Mirror the Tier-2 compose helpers in `crate` (`compose_up`, `compose_down`,
// `compose_logs`, `resolve_endpoints`) but use `JEPSEN_PROJECT` so the Tier-1
// cluster never collides with the Tier-2 cluster's "wyrd-tier2" namespace.

fn jepsen_compose_up(compose: &str) -> Result<(), String> {
    jepsen_docker_compose(
        compose,
        &[
            "up",
            "-d",
            "--build",
            "--scale",
            &format!("dserver={JEPSEN_DSERVER_COUNT}"),
        ],
    )
}

fn jepsen_compose_down(compose: &str) {
    let _ = jepsen_docker_compose(compose, &["down", "-v", "--remove-orphans"]);
}

/// Capture container logs before teardown (diagnostics on failure, mirrors #150).
/// Echoes logs to the job log and persists a copy to `target/tier1-logs/`.
fn jepsen_compose_logs(compose: &str) {
    let args = [
        "compose",
        "-p",
        JEPSEN_PROJECT,
        "-f",
        compose,
        "logs",
        "--no-color",
        "--timestamps",
    ];
    let mut display = vec!["docker"];
    display.extend_from_slice(&args);
    crate::print_step(&display);

    let out = match Command::new("docker")
        .args(args)
        .current_dir(crate::workspace_root())
        .output()
    {
        Ok(out) => out,
        Err(e) => {
            eprintln!("warning: failed to capture Jepsen container logs: {e}");
            return;
        }
    };
    print!("{}", String::from_utf8_lossy(&out.stdout));
    eprint!("{}", String::from_utf8_lossy(&out.stderr));

    let dir = crate::workspace_root().join("target").join("tier1-logs");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("warning: could not create {}: {e}", dir.display());
        return;
    }
    let path = dir.join("docker-compose.log");
    let mut captured = out.stdout;
    captured.extend_from_slice(&out.stderr);
    if let Err(e) = std::fs::write(&path, &captured) {
        eprintln!("warning: could not write {}: {e}", path.display());
    }
}

fn jepsen_resolve_endpoints(compose: &str) -> Result<String, String> {
    let mut endpoints = Vec::with_capacity(JEPSEN_DSERVER_COUNT);
    for index in 1..=JEPSEN_DSERVER_COUNT {
        let out = Command::new("docker")
            .args([
                "compose",
                "-p",
                JEPSEN_PROJECT,
                "-f",
                compose,
                "port",
                "--index",
                &index.to_string(),
                "dserver",
                "50051",
            ])
            .current_dir(crate::workspace_root())
            .output()
            .map_err(|e| format!("failed to spawn docker compose port: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "docker compose port (index {index}) failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let mapped = String::from_utf8_lossy(&out.stdout);
        let port = mapped
            .trim()
            .rsplit(':')
            .next()
            .filter(|p| !p.is_empty())
            .ok_or_else(|| format!("could not parse host port from `{}`", mapped.trim()))?;
        endpoints.push(format!("http://127.0.0.1:{port}"));
    }
    Ok(endpoints.join(","))
}

fn jepsen_docker_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", JEPSEN_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    crate::print_step(&display);
    let status = Command::new("docker")
        .args(&full)
        .current_dir(crate::workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`docker {}` failed with {status}", full.join(" ")))
    }
}

/// **Tier-2 single-node kill-and-reconstruct** (`0005:409-411`): stand up a cluster of
/// real, networked gRPC D servers under docker-compose, kill one of them, drive the
/// **production** custodian reconstruction path (`custodian::reconcile_step` →
/// `reconstruction::reconcile`) against the live cluster, and assert the durability
/// outcome — the killed node's affected chunks return to full redundancy in **distinct
/// failure domains**, and a crash mid-repair leaves **collectable garbage, never
/// corruption** (`0005:277`, `0005:385-389`).
///
/// Reuses the Tier-2 container plumbing from `run_integration` (`compose_up` /
/// `resolve_endpoints` / `finish_integration` / `finalize_panic_safe`) with a
/// [`crate::kill_reconstruct::KR_DSERVER_COUNT`]-server RS(6,3) cluster (nine for the
/// initial placement + one spare for the re-placed fragment). The live scenario test at
/// `crates/chunkstore-grpc/tests/tier2_kill_reconstruct.rs` drives the production path
/// and asserts the three-phase durability outcome.
///
/// Opt in with `WYRD_TIER2=1`. Requires `docker` (compose plugin) on the machine.
pub fn run_kill_reconstruct() -> Result<(), String> {
    let p = plan(
        "Tier-2 single-node kill-and-reconstruct",
        "docker",
        opted_in("WYRD_TIER2"),
        tool_available("docker"),
    );
    match p {
        Plan::Deferred(reason) => {
            eprintln!("xtask: {reason}");
            return Ok(());
        }
        Plan::MissingTool(msg) => return Err(msg),
        Plan::Run => {}
    }

    let compose = crate::workspace_root()
        .join("crates/chunkstore-grpc/tests/docker-compose.yml")
        .to_string_lossy()
        .to_string();

    let victim_index =
        crate::kill_reconstruct::select_victim_index(crate::kill_reconstruct::KR_DSERVER_COUNT);
    let victim_container = crate::kill_reconstruct::victim_container_name(victim_index);

    crate::compose_up(&compose, crate::kill_reconstruct::KR_DSERVER_COUNT)?;
    crate::finalize_panic_safe(
        || {
            let endpoints =
                crate::resolve_endpoints(&compose, crate::kill_reconstruct::KR_DSERVER_COUNT)?;
            println!(
                "\nxtask kill-reconstruct: {} D servers at {endpoints}; victim container: {victim_container}",
                crate::kill_reconstruct::KR_DSERVER_COUNT,
            );
            run_kill_reconstruct_test(&endpoints, &victim_container)
        },
        |result| {
            crate::finish_integration(
                result,
                || crate::compose_logs(&compose),
                || crate::compose_down(&compose),
            )
        },
    )?;
    println!("\nxtask kill-reconstruct: Tier-2 kill-and-reconstruct passed");
    Ok(())
}

/// Run the (otherwise `#[ignore]`d) Tier-2 kill-and-reconstruct scenario test with the
/// resolved endpoints and victim container exported, so it dials the live container
/// cluster and kills the right D server.
fn run_kill_reconstruct_test(endpoints: &str, victim_container: &str) -> Result<(), String> {
    crate::print_step(&[
        "cargo",
        "test",
        "-p",
        "wyrd-chunkstore-grpc",
        "--test",
        "tier2_kill_reconstruct",
        "--",
        "--ignored",
    ]);
    let status = Command::new("cargo")
        .args([
            "test",
            "-p",
            "wyrd-chunkstore-grpc",
            "--test",
            "tier2_kill_reconstruct",
            "--",
            "--ignored",
            "--nocapture",
        ])
        .current_dir(crate::workspace_root())
        .env("WYRD_DSERVER_ENDPOINTS", endpoints)
        .env("WYRD_TIER2_VICTIM_CONTAINER", victim_container)
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "Tier-2 kill-and-reconstruct test failed with {status}"
        ))
    }
}

// ---- Tier-1/Tier-2 metadata-backend fault runners (M4.6, #257) ----

/// Docker Compose project name for the ≥3-replica TiKV Raft-group cluster the metadata
/// Tier-1 consistency leg partitions — isolated from the single-node throwaway
/// (`wyrd-tikv-m41`) and the M4.5 small-multi-node stack.
const METADATA_TIER_PROJECT: &str = "wyrd-tier1-metadata";

/// The static bridge IP of the fallback isolation target (tikv-1 in
/// `deploy/tikv-multi-replica`) — used only when `WYRD_TIER1_ISOLATE=leader` is unset.
const METADATA_TIER1_ISOLATED_IP: &str = "172.30.57.12";
/// Every store's advertised address, for the scenario's readiness wait.
const METADATA_TIER1_STORE_ADDRS: &str = "172.30.57.11:20160,172.30.57.12:20160,172.30.57.13:20160";
/// Which container owns each store IP's network namespace (the iteration-13 netns cut:
/// the scenario applies the symmetric partition INSIDE the target's netns, because under a
/// shared netns a per-IP host cut provably never matches the node's own outbound traffic).
const METADATA_TIER1_NETNS_MAP: &str = "172.30.57.11=wyrd-tier1-metadata-tikv-0-1,\
     172.30.57.12=wyrd-tier1-metadata-tikv-1-1,172.30.57.13=wyrd-tier1-metadata-tikv-2-1";
/// The fault-agent image (an `iptables` entrypoint) the scenario runs inside the target's
/// netns; built by the Tier-1 runner from `deploy/tikv-multi-replica/iptables-agent/`.
const METADATA_IPTABLES_IMAGE: &str = "wyrd-iptables:local";

/// PD's client endpoint per tier: the Tier-1 ≥3-replica stack lives on a bridge network
/// (static container IPs, one netns per node — the partition premise), while the Tier-2
/// single node keeps host networking (no partition; real-I/O honesty only).
fn metadata_pd_endpoint(tier: &str) -> &'static str {
    if tier == "WYRD_TIER1" {
        "172.30.57.10:2379"
    } else {
        "127.0.0.1:2379"
    }
}

/// **Tier-1 metadata consistency-over-the-swap** (proposal 0015 §"DST and tests",
/// PR-sequence item 6; ADR-0039; ADR-0015): stand up a real ≥3-replica TiKV Raft group
/// (`deploy/tikv-multi-replica`), symmetrically isolate the **region LEADER** (resolved from
/// PD at runtime — a minority-follower cut is outcome-neutral against a linearizable store),
/// and drive the **production** `TikvMetadataStore` commit path (behind the unchanged trait)
/// through multi-key atomic create/rename/delete + ≥2 concurrent writers contending the
/// version-cell CAS + the independent ADR-0015 signals across the heal.
/// Deferred by default; opt in with `WYRD_TIER1=1`. Requires `docker`.
///
/// The run is ROUTED by the pure [`xtask::metadata_faults::metadata_tier_dispatch`] decision
/// — re-pointing it at the removed external command flips the dispatch unit test red.
pub fn run_metadata_tier1() -> Result<(), String> {
    let p = plan(
        "Tier-1 metadata consistency (≥3-replica TiKV)",
        "docker",
        opted_in("WYRD_TIER1"),
        tool_available("docker"),
    );
    match p {
        Plan::Deferred(reason) => {
            eprintln!("xtask: {reason}");
            return Ok(());
        }
        Plan::MissingTool(msg) => return Err(msg),
        Plan::Run => {}
    }

    use xtask::metadata_faults::{
        metadata_tier_dispatch, MetadataTierDispatch, METADATA_TIER1_LEGACY_CMD_VAR,
        METADATA_TIER1_SCENARIO_TEST,
    };
    let test =
        match metadata_tier_dispatch(std::env::var_os(METADATA_TIER1_LEGACY_CMD_VAR).is_some()) {
            MetadataTierDispatch::InRepoScenario { test } => test,
            MetadataTierDispatch::ExternalCommand { env_var } => {
                return Err(format!(
                    "there is no external `{env_var}` metadata harness; the in-repo \
                 `{METADATA_TIER1_SCENARIO_TEST}` scenario is the Tier-1 harness — unset \
                 `{env_var}` and re-run"
                ))
            }
        };
    run_metadata_scenario(test, "WYRD_TIER1")
}

/// **Tier-2 single-machine metadata I/O** (proposal 0015 §"DST and tests", PR-sequence item
/// 6 — the Tier-2 rung): drive the production `TikvMetadataStore` durable create/read/CAS/
/// delete cycle against a real single-node TiKV on one machine (real fsync / NVMe / OS).
/// Deferred by default; opt in with `WYRD_TIER2=1`. Requires `docker`.
pub fn run_metadata_tier2() -> Result<(), String> {
    let p = plan(
        "Tier-2 single-machine metadata I/O",
        "docker",
        opted_in("WYRD_TIER2"),
        tool_available("docker"),
    );
    match p {
        Plan::Deferred(reason) => {
            eprintln!("xtask: {reason}");
            return Ok(());
        }
        Plan::MissingTool(msg) => return Err(msg),
        Plan::Run => {}
    }
    run_metadata_scenario(
        xtask::metadata_faults::METADATA_TIER2_SCENARIO_TEST,
        "WYRD_TIER2",
    )
}

/// Bring up the metadata cluster, export the endpoints (and, for Tier-1, the symmetric
/// isolation target), run the `#[ignore]`d scenario with `--features tikv`, and tear the
/// cluster down on every path.
fn run_metadata_scenario(test: &str, tier: &str) -> Result<(), String> {
    // Tier-1 partitions a ≥3-replica group; Tier-2 is a single owned node.
    let compose_rel = if tier == "WYRD_TIER1" {
        "deploy/tikv-multi-replica/docker-compose.yml"
    } else {
        "deploy/tikv-single-node/docker-compose.yml"
    };
    let compose = crate::workspace_root()
        .join(compose_rel)
        .to_string_lossy()
        .to_string();

    // Tier-1 needs the fault-agent image (an `iptables` entrypoint) the scenario runs
    // inside the target node's netns; build it before the cluster so a broken agent fails
    // fast, not mid-partition.
    if tier == "WYRD_TIER1" {
        let agent_dir = crate::workspace_root()
            .join("deploy/tikv-multi-replica/iptables-agent")
            .to_string_lossy()
            .to_string();
        let build = ["build", "-t", METADATA_IPTABLES_IMAGE, agent_dir.as_str()];
        crate::print_step(&{
            let mut display = vec!["docker"];
            display.extend_from_slice(&build);
            display
        });
        let status = Command::new("docker")
            .args(build)
            .status()
            .map_err(|e| format!("failed to spawn docker build for the fault agent: {e}"))?;
        if !status.success() {
            return Err(format!("fault-agent image build failed with {status}"));
        }
    }

    metadata_compose(&compose, &["up", "-d"])?;
    let result = crate::finalize_panic_safe(
        || {
            // Wait for PD (and, for Tier-1, every store port) to accept connections before
            // dialing — the container's bootstrap lags `up -d`, so racing it fails spuriously
            // (the v8 codex advisory; mirrors `run_tikv_conformance`'s `wait_for_port` gate).
            // Inside the panic-safe closure so a readiness timeout still tears the stack down.
            wait_metadata_cluster_ready(tier)?;
            run_metadata_scenario_test(test, tier)
        },
        |result| {
            crate::finish_integration(
                result,
                || metadata_compose_logs(&compose),
                || {
                    let _ = metadata_compose(&compose, &["down", "-v", "--remove-orphans"]);
                },
            )
        },
    );
    result?;
    println!("\nxtask {tier} metadata: `{test}` scenario passed");
    Ok(())
}

/// Bounded readiness gate: wait for PD to accept clients, and — for Tier-1 — for every
/// store's advertised port too, so the scenario does not race the cluster's bootstrap after
/// `docker compose up -d` (the v8 codex advisory). Reuses the same `wait_for_port` bounded
/// poll `run_tikv_conformance` uses; a store that never comes up is a hard error (surfaced),
/// not a silent spurious failure mid-scenario.
fn wait_metadata_cluster_ready(tier: &str) -> Result<(), String> {
    crate::wait_for_port(metadata_pd_endpoint(tier))?;
    if tier == "WYRD_TIER1" {
        for addr in METADATA_TIER1_STORE_ADDRS.split(',') {
            crate::wait_for_port(addr.trim())?;
        }
    }
    Ok(())
}

/// Run the `#[ignore]`d metadata scenario test with `--features tikv` and the cluster
/// coordinates exported (the argv comes from the pure
/// [`xtask::metadata_faults::metadata_scenario_args`]).
fn run_metadata_scenario_test(test: &str, tier: &str) -> Result<(), String> {
    let args = xtask::metadata_faults::metadata_scenario_args(test);
    crate::print_step(&{
        let mut display = vec!["cargo"];
        display.extend_from_slice(&args);
        display
    });
    // Static endpoints (reduced bar until #365 lands L5 discovery — Deployment note). For
    // Tier-1 every node owns its netns on the bridge network, the scenario resolves and cuts
    // the region LEADER inside that netns (`WYRD_TIER1_NETNS_MAP`), and the fault-effect
    // oracle observes isolation from PD's side (peer view), not by probing the dropped port.
    let mut cmd = Command::new("cargo");
    cmd.args(args)
        .current_dir(crate::workspace_root())
        .env("WYRD_TIKV_PD_ENDPOINTS", metadata_pd_endpoint(tier));
    if tier == "WYRD_TIER1" {
        // Leader isolation (the iteration-12 fix): the scenario resolves the txn region's
        // LEADER from PD at runtime and cuts THAT store — a minority-follower cut never
        // changes a linearizable outcome (the adversary's "no teeth" refutation). The static
        // METADATA_TIER1_ISOLATED_IP stays exported as the fallback target for runs that
        // unset WYRD_TIER1_ISOLATE (e.g. manual smoke runs).
        cmd.env("WYRD_TIER1_ISOLATE", "leader")
            .env("WYRD_TIER1_ISOLATED_IP", METADATA_TIER1_ISOLATED_IP)
            .env("WYRD_TIER1_STORE_ADDRS", METADATA_TIER1_STORE_ADDRS)
            .env("WYRD_TIER1_NETNS_MAP", METADATA_TIER1_NETNS_MAP)
            .env("WYRD_TIER1_IPTABLES_IMAGE", METADATA_IPTABLES_IMAGE)
            .env("WYRD_TIER1_REPLICAS", "3")
            .env("WYRD_TIER1_ISOLATED", "1")
            // ≥2 concurrent writers contending the version-cell CAS across the fault window
            // (the no_lost_update teeth; tier1_metadata_consistency.rs).
            .env("WYRD_TIER1_CONTENDERS", "2");
    }
    let status = cmd
        .status()
        .map_err(|e| format!("failed to spawn cargo for metadata scenario: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("metadata `{test}` scenario failed with {status}"))
    }
}

fn metadata_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", METADATA_TIER_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    crate::print_step(&display);
    let status = Command::new("docker")
        .args(&full)
        .current_dir(crate::workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`docker {}` failed with {status}", full.join(" ")))
    }
}

fn metadata_compose_logs(compose: &str) {
    let _ = Command::new("docker")
        .args([
            "compose",
            "-p",
            METADATA_TIER_PROJECT,
            "-f",
            compose,
            "logs",
            "--no-color",
        ])
        .current_dir(crate::workspace_root())
        .status();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_when_not_opted_in() {
        // The default everywhere (incl. the C4-verify worktree): off-Check, skip cleanly.
        match plan("Tier-1 X", "dmsetup", false, false) {
            Plan::Deferred(_) => {}
            other => panic!("expected Deferred, got {other:?}"),
        }
        // Opt-in is what decides; tool availability is irrelevant when not opted in.
        assert!(matches!(
            plan("Tier-1 X", "dmsetup", false, true),
            Plan::Deferred(_)
        ));
    }

    #[test]
    fn opted_in_without_tool_fails_hard() {
        // The dedicated off-Check job promised the environment, so a missing tool is a
        // hard failure, never a silent skip.
        match plan("Tier-1 X", "dmsetup", true, false) {
            Plan::MissingTool(msg) => assert!(msg.contains("dmsetup")),
            other => panic!("expected MissingTool, got {other:?}"),
        }
    }

    #[test]
    fn opted_in_with_tool_runs() {
        assert_eq!(plan("Tier-2 X", "docker", true, true), Plan::Run);
    }

    /// The flippable routing regression for the Tier-1 Jepsen leg (Success criterion §1;
    /// the iter-6/7/8 fix). It binds to [`jepsen_dispatch`] — the value [`run_jepsen`]
    /// consumes on its `Plan::Run` path — NOT to a downstream argv helper (the iter-6/7/8
    /// tautology). So:
    ///
    /// - **green now**: the production inputs (`WYRD_TIER1=1`, the legacy var unset →
    ///   `legacy_cmd_configured == false`) route to the in-repo `tier1_jepsen_consistency`
    ///   scenario.
    /// - **red iff the dispatch regresses to the external shell-out**: reverting
    ///   [`jepsen_dispatch`] so the default (`false`) input returns
    ///   `ExternalCommand { env_var: "WYRD_TIER1_JEPSEN_CMD" }` — reproducing the inert
    ///   pre-#250 behaviour — makes the `false` arm below hit `ExternalCommand` and panic.
    ///   This is a **behavioural** flip, not a compile-seam over a deleted module (iter-7/8)
    ///   and not a test over a constant the runner never reads (iter-6).
    #[test]
    fn jepsen_dispatch_routes_to_in_repo_scenario_not_external_command() {
        // The production Tier-1 job opts in (WYRD_TIER1=1) and does NOT set the deprecated
        // WYRD_TIER1_JEPSEN_CMD, so `legacy_cmd_configured` is false and the route MUST be
        // the in-repo scenario, never the removed external shell-out.
        match jepsen_dispatch(false) {
            JepsenDispatch::InRepoScenario { test } => assert_eq!(
                test, "tier1_jepsen_consistency",
                "run_jepsen must route to the in-repo tier1_jepsen_consistency scenario"
            ),
            JepsenDispatch::ExternalCommand { env_var } => panic!(
                "run_jepsen regressed to the inert external `{env_var}` shell-out instead \
                 of routing to the in-repo tier1_jepsen_consistency scenario"
            ),
        }

        // The deprecated external command is representable but is NEVER the default route —
        // only an explicitly-set legacy var selects it (and `run_jepsen` then hard-errors
        // rather than shelling out).
        assert!(
            matches!(
                jepsen_dispatch(true),
                JepsenDispatch::ExternalCommand { env_var } if env_var == "WYRD_TIER1_JEPSEN_CMD"
            ),
            "the legacy WYRD_TIER1_JEPSEN_CMD var must be the only path to the (removed) \
             external route"
        );

        // The in-repo route assembles the sibling
        // `cargo test --test <name> -- --ignored --exact <fn>` shape against
        // wyrd-chunkstore-grpc, filtered to one isolation-nemesis scenario function.
        let args = jepsen_scenario_args(
            "tier1_jepsen_consistency",
            IsolationNemesis::ProcessFreeze.scenario_fn(),
        );
        assert_eq!(
            args[0], "test",
            "must be a `cargo test` invocation: {args:?}"
        );
        let flat = args.join(" ");
        assert!(
            flat.contains("tier1_jepsen_consistency")
                && flat.contains("--ignored")
                && flat.contains("wyrd-chunkstore-grpc"),
            "in-repo route must run the #[ignore]d tier1_jepsen_consistency scenario in \
             wyrd-chunkstore-grpc: {flat}"
        );
        assert!(
            !flat.contains("WYRD_TIER1_JEPSEN_CMD"),
            "in-repo route argv must not reference the external command var: {flat}"
        );
    }

    /// The Tier-1 Jepsen leg's isolation-nemesis routing (Success criterion, issue #399):
    /// binds directly to [`tier1_jepsen_isolation_legs`] (the value [`run_jepsen`]
    /// iterates on its `Plan::Run` path), mirroring
    /// [`jepsen_dispatch_routes_to_in_repo_scenario_not_external_command`]'s
    /// bind-to-the-decision-not-the-plumbing shape.
    ///
    /// - **green now**: BOTH the existing, cheaper `ProcessFreeze` leg (kept, not
    ///   deleted — Scope) and the new `NetworkPartition` leg (the #399 upgrade,
    ///   ADR-0039's named additive gap-closer) are present.
    /// - **red iff the leg collapses back to freeze-only**: dropping
    ///   `IsolationNemesis::NetworkPartition` from the returned legs — the exact gap
    ///   ADR-0039 names — flips this test red (the falsifiability demonstration: Do
    ///   proved this via a temporary negation, see build-notes.md).
    #[test]
    fn tier1_jepsen_isolation_legs_includes_network_partition_not_freeze_only() {
        let legs = tier1_jepsen_isolation_legs();
        assert!(
            legs.contains(&IsolationNemesis::NetworkPartition),
            "the Tier-1 Jepsen leg must include a network-level partition nemesis \
             (Jepsen's `:partition`) distinct from the process-freeze (`:pause`) \
             nemesis — ADR-0039's #399 upgrade; got legs={legs:?}"
        );
        assert!(
            legs.contains(&IsolationNemesis::ProcessFreeze),
            "the existing process-freeze (`docker pause`) leg must be KEPT as a \
             separate, cheaper nemesis, not deleted (Scope); got legs={legs:?}"
        );

        // Each nemesis must route to its OWN scenario function — sharing one would
        // silently collapse the new leg into the old one.
        let fns: std::collections::HashSet<&str> = legs.iter().map(|n| n.scenario_fn()).collect();
        assert_eq!(
            fns.len(),
            legs.len(),
            "each isolation nemesis must route to its own scenario function, not share \
             one with another leg: {legs:?} -> {fns:?}"
        );
        assert!(
            IsolationNemesis::NetworkPartition
                .scenario_fn()
                .contains("live_partition"),
            "the network-partition leg's scenario function name must name the live \
             partition it injects: {}",
            IsolationNemesis::NetworkPartition.scenario_fn()
        );
        assert_ne!(
            IsolationNemesis::NetworkPartition.scenario_fn(),
            IsolationNemesis::ProcessFreeze.scenario_fn(),
            "the network-partition leg must not route to the freeze leg's function"
        );
    }
}
