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

/// The `cargo test --ignored` argv that runs the in-repo `test` scenario — the same
/// `cargo test --test <name> -- --ignored` shape the sibling legs use. Downstream of the
/// [`jepsen_dispatch`] decision (the `InRepoScenario { test }` route supplies `test`).
fn jepsen_scenario_args(test: &str) -> [&str; 8] {
    [
        "test",
        "-p",
        "wyrd-chunkstore-grpc",
        "--test",
        test,
        "--",
        "--ignored",
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
        JepsenDispatch::InRepoScenario { test } => run_jepsen_scenario(test),
        JepsenDispatch::ExternalCommand { env_var } => Err(format!(
            "the external `{env_var}` Tier-1 Jepsen harness was removed in #250; the \
             in-repo `{JEPSEN_SCENARIO_TEST}` consistency scenario is the Tier-1 harness \
             now — unset `{env_var}` and re-run `cargo xtask jepsen`"
        )),
    }
}

/// Orchestrate the Tier-1 Jepsen consistency run:
/// stand up a [`JEPSEN_DSERVER_COUNT`]-server cluster, resolve endpoints, pass
/// the cluster info to the scenario test via env vars, then finalize (log capture
/// before teardown, unconditional teardown) — the same pattern as
/// [`run_kill_reconstruct`].
fn run_jepsen_scenario(test: &str) -> Result<(), String> {
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
                "\nxtask jepsen: {JEPSEN_DSERVER_COUNT} D servers at {endpoints}; \
                 victim={victim_container}, partitioned={partition_container}"
            );
            run_jepsen_test(&endpoints, &victim_container, &partition_container, test)
        },
        |result| {
            crate::finish_integration(
                result,
                || jepsen_compose_logs(&compose),
                || jepsen_compose_down(&compose),
            )
        },
    )?;
    println!("\nxtask jepsen: Tier-1 consistency scenario passed");
    Ok(())
}

/// Run the (otherwise `#[ignore]`d) Tier-1 Jepsen consistency scenario test with
/// the resolved cluster endpoints and fault-injection targets exported as env vars,
/// so the test dials the live container cluster and knows which containers to kill
/// and pause.
///
/// Runs the in-repo `test` scenario the [`jepsen_dispatch`] decision selected — the
/// `cargo test --test <test> -- --ignored` invocation built by [`jepsen_scenario_args`].
fn run_jepsen_test(
    endpoints: &str,
    victim_container: &str,
    partition_container: &str,
    test: &str,
) -> Result<(), String> {
    let args = jepsen_scenario_args(test);
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
        .status()
        .map_err(|e| format!("failed to spawn cargo for Tier-1 Jepsen: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "Tier-1 Jepsen consistency scenario failed with {status}"
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

        // The in-repo route assembles the sibling `cargo test --test <name> -- --ignored`
        // shape against wyrd-chunkstore-grpc.
        let args = jepsen_scenario_args("tier1_jepsen_consistency");
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
}
