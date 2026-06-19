//! Project automation, in Rust rather than YAML, so the same checks run on a
//! laptop and in CI (ADR-0016, ADR-0009).
//!
//! Subcommands:
//! - `ci` — fmt, clippy (`-D warnings`), build, test, cargo-deny, and the
//!   conformance check; the single gate CI calls.
//! - `conformance` — run the `chunk-format` reader against the committed
//!   conformance vectors. A stub at M0.1 (no vectors yet); the real reader and
//!   vectors land in M0.2 (issue #65).

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let task = std::env::args().nth(1);
    let result = match task.as_deref() {
        Some("ci") => run_ci(),
        Some("conformance") => run_conformance(),
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
    eprintln!("usage: cargo xtask <ci|conformance>");
}

/// The full CI gate (ADR-0009). Each step runs in workspace order; the first
/// failure stops the run.
fn run_ci() -> Result<(), String> {
    cargo(&["fmt", "--all", "--", "--check"])?;
    cargo(&[
        "clippy",
        "--workspace",
        "--all-targets",
        "--",
        "-D",
        "warnings",
    ])?;
    cargo(&["build", "--workspace", "--all-targets"])?;
    cargo(&["test", "--workspace"])?;
    cargo_deny_check()?;
    run_conformance()?;
    println!("\nxtask ci: all checks passed");
    Ok(())
}

/// Run the format conformance vectors against the reference reader.
///
/// Stub at M0.1: there is no reader and there are no vectors yet, so this
/// reports an empty run and succeeds. M0.2 (issue #65) replaces this with a
/// walk of `docs/design/specs/conformance/{vectors,invalid}/v1/`.
fn run_conformance() -> Result<(), String> {
    let vectors = workspace_root().join("docs/design/specs/conformance/vectors/v1");
    if vectors.is_dir() {
        // Defensive: once #65 lands the vectors, this stub must not silently
        // pass them. Fail loudly so the placeholder is replaced.
        return Err(format!(
            "conformance vectors exist at {} but the reader is still the M0.1 stub; \
             implement the reader (issue #65)",
            vectors.display()
        ));
    }
    println!("xtask conformance: no vectors yet (M0.1 stub; reader lands in M0.2)");
    Ok(())
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
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}
