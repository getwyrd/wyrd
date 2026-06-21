//! Project automation, in Rust rather than YAML, so the same checks run on a
//! laptop and in CI (ADR-0016, ADR-0009).
//!
//! Subcommands:
//! - `ci` — fmt, clippy (`-D warnings`), build, test, cargo-deny, conformance,
//!   and the madsim DST tier; the single gate CI calls.
//! - `conformance` — run the `chunk-format` reader against the committed
//!   conformance vectors.
//! - `dst` — run the madsim commit-protocol tests (`wyrd-dst`) under
//!   `--cfg madsim` across a sweep of seeds (ADR-0009).
//! - `integration` — the Tier-2 container tier (M2, proposal 0004): stand up a
//!   cluster of real, networked gRPC D servers under docker-compose and run the
//!   end-to-end write/read integration test against them. Not part of `ci` (it
//!   needs a container runtime); a heavier-runner / nightly job.
//! - `bench` — the tracked throughput benchmarks (EC micro-bench + the M2
//!   aggregate D-server throughput bench). Tracked, not gated.

#![forbid(unsafe_code)]

mod conformance;
mod vectors;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let task = std::env::args().nth(1);
    let result = match task.as_deref() {
        Some("ci") => run_ci(),
        Some("conformance") => run_conformance(),
        Some("gen-vectors") => run_gen_vectors(),
        Some("dst") => run_dst(),
        Some("integration") => run_integration(),
        Some("bench") => run_bench(),
        Some(other) => {
            eprintln!("xtask: unknown task `{other}`");
            print_usage();
            return ExitCode::FAILURE;
        }
        None => {
            print_usage();
            return ExitCode::FAILURE;
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("\nxtask: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn print_usage() {
    eprintln!("usage: cargo xtask <ci|conformance|gen-vectors|dst|integration|bench>");
}

/// Run the tracked throughput benchmarks. Deliberately **not** part of `run_ci`:
/// the numbers are tracked for regression visibility, not gated, because
/// CI-runner wall-clock is noisy. The bench targets' *compilation* is still
/// covered by `run_ci`'s `build --all-targets`.
///
/// - `erasure` — the EC coding micro-bench (M1.7, issue #99).
/// - `throughput` — the M2 aggregate write/read throughput across D-server counts
///   over real (in-process) tonic gRPC D servers (proposal 0004 § Benchmarks,
///   issue #117); makes the §10 Q6 throughput claim first measurable.
fn run_bench() -> Result<(), String> {
    cargo(&["bench", "-p", "wyrd-core", "--bench", "erasure"])?;
    cargo(&["bench", "-p", "wyrd-core", "--bench", "throughput"])
}

/// Default number of D-server containers the Tier-2 cluster stands up — 9, so an
/// `rs(6,3)` chunk's 9 fragments each land on a distinct networked D server.
const DSERVER_COUNT: usize = 9;
/// The compose project name, so the cluster is isolated and torn down cleanly.
const TIER2_PROJECT: &str = "wyrd-tier2";

/// Run the Tier-2 integration tier (M2, proposal 0004 PR step 7): bring up a
/// cluster of real, networked gRPC D servers under docker-compose, run the
/// end-to-end write/read integration test against them, then tear the cluster
/// down. This is the first container job in CI — a heavier-runner / nightly lane,
/// **not** part of `run_ci` (which must stay container-free).
///
/// If Docker is unavailable: a hard failure in CI (where the job is meant to run),
/// a warn-and-skip locally (so a laptop without Docker is not blocked) — mirroring
/// `cargo_deny_check`.
fn run_integration() -> Result<(), String> {
    let compose = workspace_root().join("crates/chunkstore-grpc/tests/docker-compose.yml");
    let compose = compose.to_string_lossy().to_string();

    if !docker_available() {
        if is_ci() {
            return Err("docker is not available but is required for the integration tier".into());
        }
        eprintln!(
            "warning: docker not available; skipping the Tier-2 container integration tier \
             locally. Install Docker (and the compose plugin) to run it."
        );
        return Ok(());
    }

    let count = std::env::var("WYRD_DSERVER_COUNT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 2)
        .unwrap_or(DSERVER_COUNT);

    // Bring the cluster up (building the image if needed), run the test, then
    // finalize: on failure capture container diagnostics BEFORE teardown, and
    // tear down unconditionally so a run never leaks containers (#150).
    compose_up(&compose, count)?;
    let result = (|| {
        let endpoints = resolve_endpoints(&compose, count)?;
        println!("\nxtask integration: {count} D servers at {endpoints}");
        run_integration_test(&endpoints)
    })();
    finish_integration(result, || compose_logs(&compose), || compose_down(&compose))?;
    println!("\nxtask integration: Tier-2 container integration passed");
    Ok(())
}

/// Finalize an integration run. On **failure**, capture container diagnostics
/// (`capture_logs`) **before** `teardown` destroys the cluster — the operability
/// invariant (#150): teardown must never precede log capture, or a failed
/// nightly run preserves nothing to diagnose it. `teardown` always runs
/// (best-effort) so a run never leaks containers, and `result` is propagated
/// unchanged. Generic over the two actions so the ordering is unit-testable
/// without a container runtime.
fn finish_integration<C, D>(
    result: Result<(), String>,
    capture_logs: C,
    teardown: D,
) -> Result<(), String>
where
    C: FnOnce(),
    D: FnOnce(),
{
    if result.is_err() {
        capture_logs();
    }
    teardown();
    result
}

/// Is a working Docker daemon reachable?
fn docker_available() -> bool {
    Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// `docker compose ... up -d --build --scale dserver=N`.
fn compose_up(compose: &str, count: usize) -> Result<(), String> {
    docker_compose(
        compose,
        &[
            "up",
            "-d",
            "--build",
            "--scale",
            &format!("dserver={count}"),
        ],
    )
}

/// `docker compose ... down -v --remove-orphans` (best-effort teardown).
fn compose_down(compose: &str) {
    let _ = docker_compose(compose, &["down", "-v", "--remove-orphans"]);
}

/// Capture `docker compose ... logs` before teardown (best-effort diagnostics,
/// #150). Echoes the logs to the job log for at-a-glance triage and persists a
/// copy under `target/tier2-logs/` for the workflow's `if: failure()` artifact
/// upload — so a failed nightly run's container diagnostics survive
/// `compose_down`. A capture failure only warns; it never masks the test result.
fn compose_logs(compose: &str) {
    let args = [
        "compose",
        "-p",
        TIER2_PROJECT,
        "-f",
        compose,
        "logs",
        "--no-color",
        "--timestamps",
    ];
    let mut display = vec!["docker"];
    display.extend_from_slice(&args);
    print_step(&display);

    let out = match Command::new("docker")
        .args(args)
        .current_dir(workspace_root())
        .output()
    {
        Ok(out) => out,
        Err(e) => {
            eprintln!("warning: failed to capture container logs: {e}");
            return;
        }
    };

    // Echo to the job log...
    print!("{}", String::from_utf8_lossy(&out.stdout));
    eprint!("{}", String::from_utf8_lossy(&out.stderr));

    // ...and persist a copy for the workflow's `if: failure()` artifact upload.
    let dir = workspace_root().join("target").join("tier2-logs");
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

/// Resolve each replica's published host port into a comma-separated endpoint
/// list (`http://127.0.0.1:PORT,…`) for `WYRD_DSERVER_ENDPOINTS`.
fn resolve_endpoints(compose: &str, count: usize) -> Result<String, String> {
    let mut endpoints = Vec::with_capacity(count);
    for index in 1..=count {
        let out = Command::new("docker")
            .args([
                "compose",
                "-p",
                TIER2_PROJECT,
                "-f",
                compose,
                "port",
                "--index",
                &index.to_string(),
                "dserver",
                "50051",
            ])
            .current_dir(workspace_root())
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

/// Run the (otherwise `#[ignore]`d) Tier-2 integration test with the resolved
/// endpoints exported, so it dials the live container cluster.
fn run_integration_test(endpoints: &str) -> Result<(), String> {
    print_step(&[
        "cargo",
        "test",
        "-p",
        "wyrd-chunkstore-grpc",
        "--test",
        "tier2_integration",
        "--",
        "--ignored",
    ]);
    let status = Command::new("cargo")
        .args([
            "test",
            "-p",
            "wyrd-chunkstore-grpc",
            "--test",
            "tier2_integration",
            "--",
            "--ignored",
            "--nocapture",
        ])
        .current_dir(workspace_root())
        .env("WYRD_DSERVER_ENDPOINTS", endpoints)
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Tier-2 integration test failed with {status}"))
    }
}

/// Run a `docker compose -p <project> -f <file> …` command from the workspace
/// root, echoing it first.
fn docker_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", TIER2_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    print_step(&display);

    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{}` failed with {status}", display.join(" ")))
    }
}

/// The full CI gate (ADR-0009). Each step runs in workspace order; the first
/// failure stops the run.
fn run_ci() -> Result<(), String> {
    // `wyrd-dst` only compiles under `--cfg madsim`; it is excluded from the
    // normal workspace commands and built solely by `run_dst` below.
    cargo(&["fmt", "--all", "--", "--check"])?;
    cargo(&[
        "clippy",
        "--workspace",
        "--exclude",
        "wyrd-dst",
        "--all-targets",
        "--",
        "-D",
        "warnings",
    ])?;
    cargo(&[
        "build",
        "--workspace",
        "--exclude",
        "wyrd-dst",
        "--all-targets",
    ])?;
    cargo(&["test", "--workspace", "--exclude", "wyrd-dst"])?;
    cargo_deny_check()?;
    run_conformance()?;
    run_dst()?;
    println!("\nxtask ci: all checks passed");
    Ok(())
}

/// Number of seeds the madsim DST tier sweeps per run.
const DST_SEEDS: &str = "50";

/// Run the madsim commit-protocol tests under `--cfg madsim` (ADR-0009). The
/// flag and seed count are set on this child process only, so the normal build
/// is untouched; this recompiles `wyrd-dst` and its deps under the simulator.
fn run_dst() -> Result<(), String> {
    print_step(&["cargo", "test", "-p", "wyrd-dst", "(--cfg madsim)"]);

    // Append `--cfg madsim` to any existing RUSTFLAGS rather than clobbering it.
    let rustflags = match std::env::var("RUSTFLAGS") {
        Ok(existing) if !existing.is_empty() => format!("{existing} --cfg madsim"),
        _ => "--cfg madsim".to_string(),
    };

    let status = Command::new("cargo")
        .args(["test", "-p", "wyrd-dst"])
        .current_dir(workspace_root())
        .env("RUSTFLAGS", rustflags)
        .env("MADSIM_TEST_NUM", DST_SEEDS)
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("madsim DST tests failed with {status}"))
    }
}

/// Run the committed conformance vectors against the reference reader.
fn run_conformance() -> Result<(), String> {
    conformance::run()
}

/// (Re)generate the committed conformance vectors deterministically. Run by a
/// maintainer when the vector set changes; CI runs `conformance` (read-only),
/// never this. The produced bytes must be byte-identical run to run.
fn run_gen_vectors() -> Result<(), String> {
    use std::fs;

    let valid_dir = vectors::valid_dir();
    let invalid_dir = vectors::invalid_dir();
    fs::create_dir_all(&valid_dir).map_err(|e| format!("{}: {e}", valid_dir.display()))?;
    fs::create_dir_all(&invalid_dir).map_err(|e| format!("{}: {e}", invalid_dir.display()))?;

    for v in vectors::valid_vectors() {
        let bytes = wyrd_chunk_format::encode(&v.header, &v.payload);
        // Build expected.json from a real decode, so it matches the reader exactly.
        let decoded = wyrd_chunk_format::decode(&bytes)
            .map_err(|e| format!("generated `{}` does not decode: {e}", v.name))?;
        let expected = vectors::ExpectedFragment::from_decoded(&decoded);
        let json = serde_json::to_string_pretty(&expected)
            .map_err(|e| format!("serialize {}: {e}", v.name))?;

        write(&valid_dir.join(format!("{}.fragment", v.name)), &bytes)?;
        write(
            &valid_dir.join(format!("{}.expected.json", v.name)),
            format!("{json}\n").as_bytes(),
        )?;
    }

    for v in vectors::invalid_vectors() {
        let reason = format!("error: {}\n{}\n", v.expected_variant, v.reason);
        write(&invalid_dir.join(format!("{}.fragment", v.name)), &v.bytes)?;
        write(
            &invalid_dir.join(format!("{}.reason.txt", v.name)),
            reason.as_bytes(),
        )?;
    }

    println!(
        "xtask gen-vectors: wrote vectors to {} and {}",
        valid_dir.display(),
        invalid_dir.display()
    );
    Ok(())
}

fn write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    std::fs::write(path, bytes).map_err(|e| format!("{}: {e}", path.display()))
}

/// Run `cargo deny check` — the machine-checked license + advisory wall
/// (ADR-0003 §2). If cargo-deny is not installed locally, warn and skip; in CI
/// (where `CI` is set and cargo-deny is installed) a missing binary is a hard
/// failure so the wall is always enforced on every PR.
fn cargo_deny_check() -> Result<(), String> {
    print_step(&["cargo", "deny", "check"]);
    let status = Command::new("cargo")
        .args(["deny", "check"])
        .current_dir(workspace_root())
        .status();

    match status {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => Err(format!("`cargo deny check` failed with {s}")),
        Err(_) if is_ci() => Err("cargo-deny is not installed but is required in CI".to_string()),
        Err(_) => {
            eprintln!(
                "warning: cargo-deny not installed; skipping the license/advisory \
                 wall locally. Install it with `cargo install cargo-deny --locked`."
            );
            Ok(())
        }
    }
}

/// Run a `cargo` subcommand from the workspace root, echoing it first.
fn cargo(args: &[&str]) -> Result<(), String> {
    let mut display = vec!["cargo"];
    display.extend_from_slice(args);
    print_step(&display);

    let status = Command::new("cargo")
        .args(args)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!("`{}` failed with {status}", display.join(" ")))
    }
}

fn print_step(parts: &[&str]) {
    println!("\n$ {}", parts.join(" "));
}

fn is_ci() -> bool {
    std::env::var_os("CI").is_some()
}

/// The workspace root, derived from this crate's manifest directory
/// (`<root>/xtask`).
pub(crate) fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    // The operability invariant (#150): on a FAILED integration run, container
    // diagnostics must be captured BEFORE the cluster is torn down — teardown
    // never precedes log capture, or a failed nightly run preserves nothing to
    // diagnose it. Drives `finish_integration` with order-recording closures so
    // the ordering is asserted without a container runtime.
    #[test]
    fn failure_captures_logs_before_teardown() {
        let order: RefCell<Vec<&str>> = RefCell::new(Vec::new());
        let result = finish_integration(
            Err("Tier-2 integration test failed".to_string()),
            || order.borrow_mut().push("capture_logs"),
            || order.borrow_mut().push("teardown"),
        );
        assert!(result.is_err(), "the failure must be propagated unchanged");
        assert_eq!(
            *order.borrow(),
            vec!["capture_logs", "teardown"],
            "diagnostics must be captured before teardown destroys them",
        );
    }

    // A passing run needs no diagnostics: only teardown runs, and Ok is
    // propagated. (Guards against a fix that always captures, which would noise
    // up every green nightly run.)
    #[test]
    fn success_tears_down_without_capturing_logs() {
        let order: RefCell<Vec<&str>> = RefCell::new(Vec::new());
        let result = finish_integration(
            Ok(()),
            || order.borrow_mut().push("capture_logs"),
            || order.borrow_mut().push("teardown"),
        );
        assert!(result.is_ok(), "a passing run must stay Ok");
        assert_eq!(
            *order.borrow(),
            vec!["teardown"],
            "a passing run captures no logs; only teardown runs",
        );
    }
}
