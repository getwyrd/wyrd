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

    // Bring the cluster up (building the image if needed), run the test, and tear
    // down unconditionally so a failed run never leaks containers.
    compose_up(&compose, count)?;
    let result = (|| {
        let endpoints = resolve_endpoints(&compose, count)?;
        println!("\nxtask integration: {count} D servers at {endpoints}");
        run_integration_test(&endpoints)
    })();
    compose_down(&compose);
    result?;
    println!("\nxtask integration: Tier-2 container integration passed");
    Ok(())
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
