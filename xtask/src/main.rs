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
    eprintln!("usage: cargo xtask <ci|conformance|gen-vectors|dst|bench>");
}

/// Run the EC throughput micro-benchmarks (M1.7, issue #99). Deliberately **not**
/// part of `run_ci`: the numbers are tracked for regression visibility, not
/// gated, because CI-runner wall-clock is noisy. The bench target's *compilation*
/// is still covered by `run_ci`'s `build --all-targets`.
fn run_bench() -> Result<(), String> {
    cargo(&["bench", "-p", "wyrd-core", "--bench", "erasure"])
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
