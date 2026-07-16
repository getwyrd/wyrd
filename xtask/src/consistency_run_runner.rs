//! **The checked-consistency-run runner** (#408, slice 5 of #329; ADR-0041): the impure
//! bring-up / shell-out / JVM-invocation glue behind `WYRD_TIER1=1 cargo xtask
//! consistency-run`. Mirrors `fdb_faults::run_fdb_metadata_tier1`'s discipline (self-contained
//! bring-up of `deploy/fdb-multi-replica`, unconditional teardown, hard error when opted in
//! without its environment) and `fdb_faults::run_metadata_nemesis`'s shape (shell out to the
//! `#[ignore]`d scenario with the fault env exported).
//!
//! Everything **decision-shaped** here (leg selection, the vacuity gate, the elle-cli
//! invocation, the three-valued verdict parser, the report renderer, the preflight) is a call
//! into [`xtask::consistency_run`] — the pure, Check-tested core (Design §1). This module owns
//! only the I/O: `docker compose`, `java -jar`, and the `cargo test` shell-out to the live
//! scenario (`crates/server/tests/consistency_run_fdb.rs`, `fdb`-feature-gated).
//!
//! Off-Check by construction: opted in only via `WYRD_TIER1=1`, needs Docker + Java +
//! `$WYRD_ELLE_CLI_JAR`, and `cargo xtask ci` never calls this module.

use std::path::PathBuf;
use std::process::Command;

use xtask::consistency_run::{
    consistency_run_scenario_args, elle_invocation, elle_version_extraction, evaluate_summary,
    parse_checker_output, parse_elle_version, parse_run_summary, preflight, render_report,
    selected_leg, self_check_matches, wyrd_check_violations, CheckOutcome, CheckerIdentity,
    Environment, ReportInputs, RunSummary, SelfCheckExpectation, CONSISTENCY_RUN_SCENARIO_TEST,
    MODEL_DIRECTORY_SET, MODEL_REGISTER,
};

use crate::{print_step, workspace_root};

/// A distinct compose project from `fdb-tier1-metadata` / `metadata-nemesis`'s, so a checked
/// run never collides with a concurrently-running Tier-1 battery or nemesis campaign.
const PROJECT: &str = "wyrd-consistency-run";
const COMPOSE_FILE: &str = "deploy/fdb-multi-replica/docker-compose.yml";
/// Mirrors `fdb_faults::FDB_TIER1_NODES` — the same fixed 3-node topology
/// `deploy/fdb-multi-replica/docker-compose.yml` declares.
const NODES: [(&str, &str); 3] = [
    ("fdb0", "172.30.58.11"),
    ("fdb1", "172.30.58.12"),
    ("fdb2", "172.30.58.13"),
];
const FDB_PORT: &str = "4500";
const IPTABLES_IMAGE: &str = "wyrd-iptables:local";
const IPTABLES_AGENT_DIR: &str = "deploy/tikv-multi-replica/iptables-agent";
/// The partition/pause leg's target and survivor — an arbitrary coordinator is outcome-neutral
/// for the checked workload (unlike the #442 battery, this run does not need the master
/// specifically: it proves the register/directory history survives a real cluster fault, not a
/// commit-point re-check at the coordinator holding a particular role).
const TARGET_SERVICE: &str = "fdb0";
const SURVIVOR_SERVICE: &str = "fdb1";

fn output_dir() -> PathBuf {
    workspace_root().join("target/consistency-run")
}

fn fixtures_dir() -> PathBuf {
    workspace_root().join("xtask/tests/fixtures/consistency-run")
}

fn cluster_file_contents() -> String {
    let coordinators: Vec<String> = NODES
        .iter()
        .map(|(_, ip)| format!("{ip}:{FDB_PORT}"))
        .collect();
    format!("docker:docker@{}", coordinators.join(","))
}

fn compose(args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", PROJECT, "-f", COMPOSE_FILE];
    full.extend_from_slice(args);
    print_step(&{
        let mut display = vec!["docker"];
        display.extend_from_slice(&full);
        display
    });
    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker compose: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "docker compose {} failed with {status}",
            args.join(" ")
        ))
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .args(["version"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn java_available() -> bool {
    Command::new("java")
        .args(["-version"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn unzip_available() -> bool {
    Command::new("unzip")
        .args(["-v"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Read the checker's identity out of the jar the run actually invokes (Design §3/§6): its version
/// (and upstream revision) from the jar's own metadata, plus the SHA-256 of the file itself. Fails
/// loudly rather than defaulting to an "unknown" version — see [`parse_elle_version`].
fn checker_identity(jar: &str) -> Result<CheckerIdentity, String> {
    let argv = elle_version_extraction(jar);
    let out = Command::new("unzip")
        .args(&argv)
        .output()
        .map_err(|e| format!("failed to spawn unzip to read the elle-cli version: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "could not read `{}` out of the elle-cli jar ({}): {}",
            xtask::consistency_run::ELLE_VERSION_JAR_ENTRY,
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        ));
    }
    let (version, revision) = parse_elle_version(&String::from_utf8_lossy(&out.stdout))?;
    Ok(CheckerIdentity {
        version,
        revision,
        jar_sha256: sha256_of_file(jar)?,
    })
}

/// `configure new double ssd` once, then poll `status minimal` — mirrors
/// `fdb_faults::configure_database` (readiness read from the TEXT, never the exit status; see
/// that function's comment for why `status.success()` is not a health predicate).
fn configure_database() -> Result<(), String> {
    let _ = compose(&[
        "exec",
        "-T",
        "fdb0",
        "fdbcli",
        "--exec",
        "configure new double ssd",
    ]);
    let mut last_seen = String::new();
    for attempt in 1..=45 {
        let output = Command::new("docker")
            .args([
                "compose",
                "-p",
                PROJECT,
                "-f",
                COMPOSE_FILE,
                "exec",
                "-T",
                "fdb0",
                "fdbcli",
                "--timeout",
                "5",
                "--exec",
                "status minimal",
            ])
            .current_dir(workspace_root())
            .output()
            .map_err(|e| format!("failed to spawn docker: {e}"))?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        if xtask::fdb_doctor::cluster_status_is_healthy(&text) {
            return Ok(());
        }
        last_seen = text.trim().to_string();
        eprintln!(
            "xtask consistency-run: cluster not available yet (attempt {attempt}/45): {last_seen}"
        );
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    Err(format!(
        "the FoundationDB cluster did not report `database is available` within 90s; last \
         status was: {last_seen}"
    ))
}

fn write_cluster_file() -> Result<String, String> {
    let dir = workspace_root().join("target/consistency-run-fdb");
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let path = dir.join("fdb.cluster");
    std::fs::write(&path, cluster_file_contents())
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(path.to_string_lossy().to_string())
}

fn build_fault_agent() -> Result<(), String> {
    let dir = workspace_root()
        .join(IPTABLES_AGENT_DIR)
        .to_string_lossy()
        .to_string();
    let args = ["build", "-t", IPTABLES_IMAGE, dir.as_str()];
    print_step(&{
        let mut display = vec!["docker"];
        display.extend_from_slice(&args);
        display
    });
    let status = Command::new("docker")
        .args(args)
        .status()
        .map_err(|e| format!("failed to spawn docker build for the fault agent: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("fault-agent image build failed with {status}"))
    }
}

/// The stable compose container **name** of a service (never the ephemeral id — mirrors
/// `fdb_faults::container_name_of`).
fn container_name_of(service: &str) -> Result<String, String> {
    let out = Command::new("docker")
        .args([
            "compose",
            "-p",
            PROJECT,
            "-f",
            COMPOSE_FILE,
            "ps",
            "--format",
            "{{.Name}}",
            service,
        ])
        .current_dir(workspace_root())
        .output()
        .map_err(|e| format!("failed to spawn docker compose ps: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "`docker compose ps {service}` failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        ));
    }
    let name = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or_default()
        .to_string();
    if name.is_empty() {
        return Err(format!(
            "compose service `{service}` has no running container — the checked-run cluster did \
             not come up"
        ));
    }
    Ok(name)
}

/// The static IP `deploy/fdb-multi-replica/docker-compose.yml` gives a declared service.
fn ip_of(service: &str) -> Result<&'static str, String> {
    NODES
        .iter()
        .find(|(s, _)| *s == service)
        .map(|(_, ip)| *ip)
        .ok_or_else(|| format!("`{service}` is not a declared consistency-run node"))
}

/// Run the live scenario (`cargo test -p wyrd-server --features fdb --test consistency_run_fdb --
/// --ignored --nocapture`) with the fault + output env exported, mirroring
/// `fdb_faults::run_nemesis_leg`'s fully-resolved env.
fn run_scenario(cluster_file: &str, leg: &str) -> Result<(), String> {
    let target_ip = ip_of(TARGET_SERVICE)?;
    let target_addr = format!("{target_ip}:{FDB_PORT}");
    let target_container = container_name_of(TARGET_SERVICE)?;
    let survivor_container = container_name_of(SURVIVOR_SERVICE)?;
    let compose_file = workspace_root()
        .join(COMPOSE_FILE)
        .to_string_lossy()
        .to_string();
    let faketime_override = workspace_root()
        .join("deploy/fdb-multi-replica/docker-compose.faketime.yml")
        .to_string_lossy()
        .to_string();

    let args = consistency_run_scenario_args(CONSISTENCY_RUN_SCENARIO_TEST);
    print_step(
        &std::iter::once("cargo")
            .chain(args.iter().copied())
            .collect::<Vec<_>>(),
    );
    let status = Command::new("cargo")
        .args(args)
        .current_dir(workspace_root())
        .env("WYRD_FDB_CLUSTER_FILE", cluster_file)
        .env("WYRD_CONSISTENCY_NEMESIS", leg)
        .env("WYRD_CONSISTENCY_TARGET_ADDR", &target_addr)
        .env("WYRD_CONSISTENCY_TARGET_IP", target_ip)
        .env("WYRD_CONSISTENCY_TARGET_SERVICE", TARGET_SERVICE)
        .env("WYRD_CONSISTENCY_TARGET_CONTAINER", &target_container)
        .env("WYRD_CONSISTENCY_SURVIVOR_CONTAINER", &survivor_container)
        .env("WYRD_CONSISTENCY_IPTABLES_IMAGE", IPTABLES_IMAGE)
        .env("WYRD_CONSISTENCY_COMPOSE_FILE", &compose_file)
        .env("WYRD_CONSISTENCY_FAKETIME_OVERRIDE", &faketime_override)
        .env(
            "WYRD_CONSISTENCY_OUTPUT_DIR",
            output_dir().to_string_lossy().to_string(),
        )
        .status()
        .map_err(|e| format!("failed to spawn cargo test: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "the checked-consistency-run scenario failed with {status}"
        ))
    }
}

fn read_run_summary() -> Result<RunSummary, String> {
    let path = output_dir().join("run-summary.json");
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("could not read the run summary at {}: {e}", path.display()))?;
    parse_run_summary(&text)
}

/// Run one elle-cli invocation (Design §3) and parse its verdict token.
fn run_checker(jar: &str, model: &str, history_path: &str) -> Result<CheckOutcome, String> {
    let argv = elle_invocation(jar, model, history_path);
    print_step(
        &std::iter::once("java")
            .chain(argv.iter().map(String::as_str))
            .collect::<Vec<_>>(),
    );
    let output = Command::new("java")
        .args(&argv)
        .output()
        .map_err(|e| format!("failed to spawn java for elle-cli: {e}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    print!("{stdout}");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    Ok(parse_checker_output(&stdout, output.status.success()))
}

/// The two-tier fixtures self-check (Design §5): feed the SAME committed golden fixtures Check
/// pins through the real elle-cli and confirm — for **both** models, **both** polarities —
/// known-good comes back a pass and known-bad a genuine violation. Closes the v2 gap (self-check
/// covered only register fixtures). Recorded in the report.
fn fixtures_self_check(jar: &str) -> Result<String, String> {
    use SelfCheckExpectation::{Inconclusive, Pass, Violation};

    let dir = fixtures_dir();
    let cases = [
        (MODEL_REGISTER, "register-history-known-good.edn", Pass),
        (MODEL_REGISTER, "register-history-known-bad.edn", Violation),
        (
            MODEL_DIRECTORY_SET,
            "directory-history-known-good.edn",
            Pass,
        ),
        (
            MODEL_DIRECTORY_SET,
            "directory-history-known-bad.edn",
            Violation,
        ),
        // The degrade path (Design §2): an `:info` composed final read — what the scenario emits
        // when the post-heal sweep cannot resolve a member — MUST come back without a verdict. If
        // this checker build instead blessed it, every degraded run would silently report a pass,
        // so the run refuses to trust any verdict from a build that does.
        (
            MODEL_DIRECTORY_SET,
            "directory-history-indeterminate-final-read.edn",
            Inconclusive,
        ),
    ];
    let mut notes = Vec::new();
    for (model, file, expected) in cases {
        let path = dir.join(file);
        let outcome = run_checker(jar, model, &path.to_string_lossy())?;
        if !self_check_matches(expected, &outcome) {
            return Err(format!(
                "fixtures self-check FAILED: {file} (model {model}) expected {expected:?} but the \
                 real elle-cli returned {outcome:?} — the pinned parser/vocabulary disagrees with \
                 the checker",
            ));
        }
        notes.push(format!("{file} ({model}) -> {outcome:?}"));
    }
    Ok(format!(
        "fixtures self-check PASSED (both models, both polarities, plus the degraded composed \
         read): {}",
        notes.join("; ")
    ))
}

fn write_report(
    summary: &RunSummary,
    register_verdict: &CheckOutcome,
    directory_verdict: &CheckOutcome,
    checker: &CheckerIdentity,
    self_check_note: &str,
) -> Result<(), String> {
    let wyrd_violations = wyrd_check_violations(summary);
    let delete_pool_verdict = if wyrd_violations.is_empty() {
        "all #406 checks held".to_string()
    } else {
        format!("VIOLATED: {}", wyrd_violations.join(", "))
    };
    let report = render_report(&ReportInputs {
        workload: summary.workload.clone(),
        nemesis: ReportInputs::nemesis_field(&summary.nemesis),
        history_size: format!(
            "register (Elle-fed): {} ops ({:?}); directory (set): {} ops ({:?}, incl. ONE composed \
             post-heal full-set read over {} members — the sweep's per-member probes are that \
             read's raw material, not history ops); delete pool (Wyrd-checked, not serialized to \
             EDN): {} ops ({:?})",
            summary.register_ops,
            summary.register_outcomes,
            summary.directory_ops,
            summary.directory_outcomes,
            summary.member_id_map.len(),
            summary.delete_pool.ops,
            summary.delete_pool.outcomes,
        ),
        model: format!(
            "{MODEL_REGISTER} (register), {MODEL_DIRECTORY_SET} (directory); the delete pool is \
             judged by the #406 session/monotonicity checks, which no Elle model can represent"
        ),
        checker: format!("{}. {self_check_note}", checker.describe()),
        member_id_map: format!(
            "{}. {}",
            ReportInputs::member_id_map_field(&summary.member_id_map),
            ReportInputs::composed_final_read_field(summary),
        ),
        verdict: format!(
            "register: {register_verdict:?}; directory: {directory_verdict:?}; delete pool: \
             {delete_pool_verdict}"
        ),
    });
    let path = output_dir().join("report.md");
    std::fs::create_dir_all(output_dir())
        .map_err(|e| format!("{}: {e}", output_dir().display()))?;
    std::fs::write(&path, &report).map_err(|e| format!("{}: {e}", path.display()))?;
    println!(
        "xtask consistency-run: report written to {}",
        path.display()
    );
    Ok(())
}

fn sha256_of_file(path: &str) -> Result<String, String> {
    let out = Command::new("sha256sum")
        .arg(path)
        .output()
        .map_err(|e| format!("failed to spawn sha256sum: {e}"))?;
    if !out.status.success() {
        return Err(format!("sha256sum {path} failed: {}", out.status));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Ok(text.split_whitespace().next().unwrap_or("").to_string())
}

/// **The checked-consistency-run runner** (`WYRD_TIER1=1 cargo xtask consistency-run`): stand up
/// `deploy/fdb-multi-replica`, drive the live #406 workload under a #407 nemesis leg (partition
/// by default; `WYRD_CONSISTENCY_NEMESIS=clock-skew|process-pause` selectable), export the
/// Elle-EDN histories + run summary (carrying the leg's typed materialization evidence), gate on
/// non-vacuity (Design §4), obtain the elle-cli verdict (Design §3), run the fixtures self-check
/// (both models, both polarities), and render the report (Design §6). Tears the stack down
/// unconditionally, including on a panic or a partial bring-up — mirrors
/// `fdb_faults::run_metadata_nemesis` (bring-up inside the finalizer scope).
pub fn run_consistency_check() -> Result<(), String> {
    if std::env::var("WYRD_TIER1").as_deref() != Ok("1") {
        println!(
            "xtask consistency-run: DEFERRED — set WYRD_TIER1=1 to run the privileged #408 \
             checked consistency run (the #329 credibility artifact). It stands up a 3-process \
             FoundationDB cluster, drives the checked #406 workload under a #407 nemesis leg, \
             and needs Docker, Java, and $WYRD_ELLE_CLI_JAR (the elle-cli standalone jar)."
        );
        return Ok(());
    }

    let jar = std::env::var("WYRD_ELLE_CLI_JAR").unwrap_or_default();
    let jar_present = !jar.trim().is_empty() && std::path::Path::new(&jar).is_file();
    preflight(
        true,
        Environment {
            docker: docker_available(),
            java: java_available(),
            jar_present,
            unzip: unzip_available(),
        },
    )?;

    // Identify the checker BEFORE standing anything up: a run that cannot name the checker that
    // judged it cannot produce the report this issue exists to deliver, so learning that after a
    // 3-node cluster and a full nemesis window would be a waste of the whole run.
    let checker = checker_identity(&jar)?;
    println!("xtask consistency-run: checker = {}", checker.describe());

    let leg = selected_leg(std::env::var("WYRD_CONSISTENCY_NEMESIS").ok().as_deref())?;

    build_fault_agent()?;

    let result = crate::finalize_panic_safe(
        || {
            // Bring-up INSIDE the teardown scope: `docker compose up -d` can fail after
            // creating part of the stack (one service starts, another refuses), and a `?`
            // outside the finalizer would leak that partial cluster — contradicting the
            // unconditional-teardown contract above and contaminating the next opted-in
            // run (the same Codex P2 as #569, fixed the same way in
            // `fdb_faults::run_metadata_nemesis`). `down -v --remove-orphans` on a
            // never-created stack is a harmless no-op, so covering bring-up costs nothing.
            compose(&["up", "-d"])?;
            configure_database()?;
            let cluster_file = write_cluster_file()?;
            run_scenario(&cluster_file, leg.as_str())?;

            let summary = read_run_summary()?;
            evaluate_summary(&summary).map_err(|reason| {
                format!(
                    "checked consistency run INCONCLUSIVE: {} — refusing to report a verdict",
                    reason.message()
                )
            })?;

            let register_path = output_dir()
                .join("register-history.edn")
                .to_string_lossy()
                .to_string();
            let directory_path = output_dir()
                .join("directory-history.edn")
                .to_string_lossy()
                .to_string();
            let register_verdict = run_checker(&jar, MODEL_REGISTER, &register_path)?;
            let directory_verdict = run_checker(&jar, MODEL_DIRECTORY_SET, &directory_path)?;
            let self_check_note = fixtures_self_check(&jar)?;
            println!("xtask consistency-run: {self_check_note}");

            write_report(
                &summary,
                &register_verdict,
                &directory_verdict,
                &checker,
                &self_check_note,
            )?;

            // The Wyrd-checked delete pool's verdict is a run outcome, not a footnote: the #406
            // checks are the only judge of the delete traffic no Elle model can represent, so a
            // violation there is as real as Elle's `false` (Design §2).
            let wyrd_violations = wyrd_check_violations(&summary);
            if !wyrd_violations.is_empty() {
                return Err(format!(
                    "checked consistency run FAILED — the Wyrd-checked delete pool violated: {}. \
                     These checks are INV-1-sound (they skip indeterminate ops), so this is a real \
                     violation observed on the live cluster, not an artifact of the fault.",
                    wyrd_violations.join(", "),
                ));
            }

            match (&register_verdict, &directory_verdict) {
                (CheckOutcome::Pass, CheckOutcome::Pass) => Ok(()),
                (a, b) if a.is_violation() || b.is_violation() => Err(format!(
                    "checked consistency run FAILED — a real Elle violation. register: \
                     {register_verdict:?}, directory: {directory_verdict:?}"
                )),
                _ => Err(format!(
                    "checked consistency run INCONCLUSIVE — the checker did not return a definite \
                     verdict for both models. register: {register_verdict:?}, directory: \
                     {directory_verdict:?}"
                )),
            }
        },
        |result| {
            let _ = compose(&["down", "-v", "--remove-orphans"]);
            result
        },
    );

    result?;
    println!(
        "\nxtask consistency-run: PASS — the #406 checked register + directory workload, under \
         a #407 `{}` nemesis leg, obtained a non-vacuous Elle verdict (#329 DoD item 2)",
        leg.as_str()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cluster file names every declared coordinator — the same drift guard
    /// `fdb_faults::tests::the_cluster_file_names_every_declared_coordinator` pins for its own
    /// table, over THIS module's independent table.
    #[test]
    fn the_cluster_file_names_every_declared_coordinator() {
        let contents = cluster_file_contents();
        assert!(contents.starts_with("docker:docker@"));
        for (_, ip) in NODES {
            assert!(
                contents.contains(&format!("{ip}:{FDB_PORT}")),
                "the cluster file must name coordinator {ip}: {contents}"
            );
        }
    }
}
