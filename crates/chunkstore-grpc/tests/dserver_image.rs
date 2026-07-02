//! D-server runtime image runs unprivileged (issue #286).
//!
//! The `d-server` role's container (`tests/dserver/Dockerfile`) had no `USER`
//! directive, so the runtime stage defaulted to uid 0 (`docker run --rm
//! --entrypoint id wyrd-dserver:test` printed `uid=0(root)`) — unnecessary
//! blast radius if the process/image is compromised. This test asserts the
//! *encoded* seam: the Dockerfile's runtime stage declares a non-root `USER`
//! before its `ENTRYPOINT`, so the image can never regress back to running
//! the role as root without this test catching it.
//!
//! What this test does NOT cover (deferred, off-Check — needs a Docker host,
//! see the brief's "Off-Check verification instructions"): actually building
//! the image and confirming the d-server process is non-root at runtime AND
//! can still write a fragment under `/data` as that user. `cargo xtask ci`
//! never invokes Docker (containers are outside the DST/gate substrate), so
//! that half is a maintainer/CI smoke check, not this test's job.

use std::fs;
use std::path::Path;

/// The runtime stage of the Dockerfile under test, relative to this crate's
/// `Cargo.toml` (`CARGO_MANIFEST_DIR`).
const DOCKERFILE_PATH: &str = "tests/dserver/Dockerfile";

/// Split the Dockerfile into its stages (`FROM ... AS name` / final `FROM`
/// starts a new stage), returning each stage's raw lines. Mirrors how Docker
/// itself scopes a multi-stage build: a `USER`/`ENTRYPOINT` in the *build*
/// stage says nothing about the image that's actually shipped and run.
fn stages(contents: &str) -> Vec<Vec<&str>> {
    let mut out: Vec<Vec<&str>> = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.to_uppercase().starts_with("FROM ") {
            out.push(Vec::new());
        }
        if let Some(stage) = out.last_mut() {
            stage.push(line);
        }
    }
    out
}

/// The final stage's directive lines, stripped of comments/blank lines,
/// preserving order (Dockerfile instructions are order-sensitive: `USER` must
/// precede `ENTRYPOINT` to actually apply to the process it starts).
fn directives(stage: &[&str]) -> Vec<(String, String)> {
    stage
        .iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            let mut parts = l.splitn(2, char::is_whitespace);
            let instr = parts.next()?.to_uppercase();
            let rest = parts.next().unwrap_or("").trim().to_string();
            Some((instr, rest))
        })
        .collect()
}

/// Reject uid/gid `0` in any of the forms `USER` accepts: a bare `0`, `0:0`,
/// `root`, or `root:root` — the point is "not root", not merely "some
/// argument was supplied".
fn is_root_user(arg: &str) -> bool {
    let user_part = arg.split(':').next().unwrap_or(arg);
    user_part == "0" || user_part.eq_ignore_ascii_case("root")
}

/// The runtime image must declare a non-root `USER` before its `ENTRYPOINT`
/// (BINDING per the brief's Success criterion): the process the container
/// starts must not default to uid 0.
#[test]
fn runtime_stage_sets_non_root_user_before_entrypoint() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(DOCKERFILE_PATH);
    let contents = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let stages = stages(&contents);
    let runtime_stage = stages
        .last()
        .expect("Dockerfile must contain at least one FROM stage");
    let directives = directives(runtime_stage);

    let entrypoint_idx = directives
        .iter()
        .position(|(instr, _)| instr == "ENTRYPOINT")
        .expect("runtime stage must set ENTRYPOINT");

    let user_before_entrypoint = directives[..entrypoint_idx]
        .iter()
        .rev()
        .find(|(instr, _)| instr == "USER");

    let (_, user_arg) = user_before_entrypoint.unwrap_or_else(|| {
        panic!(
            "runtime stage of {} must set a non-root USER before ENTRYPOINT \
             (the d-server role currently defaults to uid 0 / root)",
            DOCKERFILE_PATH
        )
    });

    assert!(
        !is_root_user(user_arg),
        "runtime stage USER directive (`USER {user_arg}`) must not be root/uid 0"
    );
}
