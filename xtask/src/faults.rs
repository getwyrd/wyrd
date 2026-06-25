//! **Tier-1 / Tier-2 custodian fault runners** — the *deferred-posture* legs of the M3
//! verification campaign (proposal 0005 §"DST and tests": Tier-1 disk-fault injection +
//! Jepsen, Tier-2 single-node kill-and-reconstruct, `0005:405-411`; the `xtask`
//! touch-point `0005:437-438`; PR-sequence slice 8 `0005:541-545`).
//!
//! These tiers exercise behaviour the deterministic Tier-0 campaign cannot: **real**
//! block-layer misbehaviour (device-mapper `dm-flakey` / `dm-error`), a **Jepsen**
//! consistency harness over the repair path, and a **real node** with real NVMe/fsync.
//! They need root / privileged I/O / an external harness, so they are **never** part of
//! `cargo xtask ci` (which stays unprivileged and container-free, ADR-0016) and never run
//! in the deterministic worktree (containerizing them would break seed determinism,
//! ADR-0009 / INTEGRATION §3).
//!
//! Each runner is **deferred by default**: without an explicit opt-in it prints what it
//! requires and exits cleanly, so it is INERT at Check and on a normal dev box. The
//! dedicated off-Check CI / Tier-2 job opts in (`WYRD_TIER1=1` / `WYRD_TIER2=1`) and
//! wires the harness command; there a missing tool is a hard failure. Only the gating
//! decision ([`plan`]) is pure and unit-tested — the privileged scenario body runs solely
//! in the off-Check environment that supplies it.

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

/// Execute a runner's [`Plan`]: print-and-skip when deferred, error when the tool is
/// missing, or run the off-Check harness command from `cmd_var` when opted in. The
/// harness command is environment-supplied (the off-Check job sets it), so this body is
/// never exercised at Check.
fn execute(tier: &str, plan: Plan, cmd_var: &str) -> Result<(), String> {
    match plan {
        Plan::Deferred(reason) => {
            eprintln!("xtask: {reason}");
            Ok(())
        }
        Plan::MissingTool(msg) => Err(msg),
        Plan::Run => {
            let cmd = std::env::var(cmd_var).map_err(|_| {
                format!(
                    "{tier} is opted in but no harness command is configured; set {cmd_var} \
                     to the scenario command (the off-Check job supplies it)"
                )
            })?;
            run_shell(tier, &cmd)
        }
    }
}

/// Run an environment-supplied harness command through the shell from the workspace root.
fn run_shell(tier: &str, cmd: &str) -> Result<(), String> {
    println!("\n$ {cmd}");
    let status = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .current_dir(crate::workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn {tier} harness: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{tier} harness failed with {status}"))
    }
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

/// **Tier-1 Jepsen consistency** (`0005:408`): a Jepsen harness asserting consistency
/// over the repair path under partitions/crashes. Needs the Jepsen harness (`lein`).
/// Opt in with `WYRD_TIER1=1` and configure `WYRD_TIER1_JEPSEN_CMD`.
pub fn run_jepsen() -> Result<(), String> {
    let plan = plan(
        "Tier-1 Jepsen consistency",
        "lein",
        opted_in("WYRD_TIER1"),
        tool_available("lein"),
    );
    execute("Tier-1 Jepsen", plan, "WYRD_TIER1_JEPSEN_CMD")
}

/// **Tier-2 single-node kill-and-reconstruct** (`0005:409-411`): on a single real node,
/// kill a real D server and watch real reconstruction over real NVMe/fsync. Needs a real
/// node with the cluster tooling (`docker`). Opt in with `WYRD_TIER2=1` and configure
/// `WYRD_TIER2_CMD`.
pub fn run_kill_reconstruct() -> Result<(), String> {
    let plan = plan(
        "Tier-2 single-node kill-and-reconstruct",
        "docker",
        opted_in("WYRD_TIER2"),
        tool_available("docker"),
    );
    execute("Tier-2 kill-reconstruct", plan, "WYRD_TIER2_CMD")
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

    #[test]
    fn execute_deferred_is_ok_and_runs_nothing() {
        // A deferred plan exits cleanly without consulting the harness command.
        let outcome = execute(
            "Tier-1 X",
            Plan::Deferred("deferred".to_string()),
            "WYRD_NONEXISTENT_CMD",
        );
        assert!(outcome.is_ok());
    }

    #[test]
    fn execute_missing_tool_propagates_error() {
        let outcome = execute(
            "Tier-1 X",
            Plan::MissingTool("no dmsetup".to_string()),
            "WYRD_NONEXISTENT_CMD",
        );
        assert_eq!(outcome, Err("no dmsetup".to_string()));
    }
}
