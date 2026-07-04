//! ADR-0010 structural guard + the M4.5 `deploy/` bring-up's structural validity
//! (proposal 0015 §"Deployment: TiKV/PD as a stateful, disk-affine,
//! orchestrator-agnostic tier", "Suggested PR sequence" item 5, issue #256).
//!
//! Two Check-time signals (the brief's "Test file"), both housed here:
//!
//! **(1) The flippable no-orchestrator-coupling regression.** ADR-0010: "no code
//! couples to orchestrator APIs"; peers are discovered through L5, never an
//! orchestrator API (architecture §7.2). This drives the SAME production scan
//! function (`xtask::deploy_guard::scan_dir`) that `cargo xtask ci`'s
//! `run_orchestrator_guard` (`xtask/src/main.rs`) runs over `crates/` on every CI
//! run — one guard, two call sites. `scan_dir_is_red_when_an_orchestrator_import_is_planted`
//! plants a REAL orchestrator import in a temp fixture and proves the guard catches
//! it (a demonstrated RED, not a guard resting red on non-existence);
//! `scan_dir_is_green_over_the_real_workspace_crates` proves the invariant holds
//! today (GREEN on the real tree).
//!
//! **(2) A `docker compose config` structural-validity check** over the new
//! `deploy/small-multi-node/docker-compose.yml` (the single-zone "Small multi-node
//! Production" stack, architecture §7.1): it must parse and declare the profile's
//! FOUR component roles — PD, TiKV, the 3-node etcd ensemble (L5 Coordination), and
//! D servers. This only *parses* the compose file (`docker compose config`), never
//! brings up a container, so it stays fast; it mirrors the project's existing
//! docker-availability convention (`docker_available` in `xtask/src/main.rs`): a
//! hard failure in CI (the `.github/workflows/ci.yml` `ubuntu-latest` runner always
//! has Docker), a warn-and-skip locally so a Docker-less laptop is never blocked —
//! exactly as `deploy/tikv-single-node/`'s own conformance test skips cleanly with
//! no TiKV configured.

use std::path::{Path, PathBuf};
use std::process::Command;

use xtask::deploy_guard::{scan_dir, scan_line, ORCHESTRATOR_NEEDLES};

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

// ─── (1) the flippable no-orchestrator-coupling regression ────────────────────

#[test]
fn scan_line_catches_a_kube_import() {
    assert_eq!(scan_line("use kube::Client;"), Some("kube::"));
    assert_eq!(
        scan_line("let c: k8s_openapi::api::core::v1::Pod;"),
        Some("k8s_openapi::")
    );
}

#[test]
fn scan_line_ignores_comments_and_prose() {
    // A doc/comment mentioning Kubernetes prose (ADR-0010's own language, and
    // `deploy/README.md`'s "Kubernetes is available, never required") must NOT be
    // flagged — only real Rust import syntax is a violation.
    assert!(scan_line("// Kubernetes is available, never required (ADR-0010).").is_none());
    assert!(scan_line("/// see kube::Client for an example").is_none());
    // The bare word "kubernetes" in a string literal matches no import needle.
    assert!(scan_line("let orchestrator_name = \"kubernetes\";").is_none());
}

#[test]
fn scan_dir_is_red_when_an_orchestrator_import_is_planted() {
    // Plant a REAL orchestrator import in a temp fixture tree and prove the SAME
    // production scan (`xtask::deploy_guard::scan_dir` — the function `cargo xtask
    // ci` runs over `crates/`) catches it: the "demonstrated red" the brief
    // requires, not a guard resting red on non-existence.
    let dir = std::env::temp_dir().join(format!(
        "wyrd-deploy-guard-fixture-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(dir.join("src")).expect("create fixture dir");
    std::fs::write(
        dir.join("src/lib.rs"),
        "use kube::Client;\n\nfn touches_kubernetes(_c: Client) {}\n",
    )
    .expect("write fixture file");

    let violations = scan_dir(&dir);
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        violations.len(),
        1,
        "planting `use kube::Client;` must be caught exactly once: {violations:?}"
    );
    assert!(
        violations[0].ends_with("kube::"),
        "the violation must name the matched needle: {violations:?}"
    );
}

#[test]
fn scan_dir_is_green_over_the_real_workspace_crates() {
    // The invariant itself: no workspace crate imports an orchestrator API today.
    let violations = scan_dir(&workspace_root().join("crates"));
    assert!(
        violations.is_empty(),
        "ADR-0010 violation(s) found under crates/: {violations:?}"
    );
}

#[test]
fn orchestrator_needles_are_non_empty_and_import_shaped() {
    // Sanity on the guard itself: the needle list is neither empty (a vacuous
    // guard) nor so broad that it would false-positive on bare Kubernetes prose —
    // every needle looks like real Rust import syntax.
    assert!(
        !ORCHESTRATOR_NEEDLES.is_empty(),
        "an empty needle list would make the guard vacuously green"
    );
    for needle in ORCHESTRATOR_NEEDLES {
        assert!(
            needle.contains("::") || needle.starts_with("use "),
            "needle {needle:?} should look like Rust import syntax, not bare prose"
        );
    }
}

// ─── (2) `docker compose config` structural validity of the new stack ─────────

/// Is a working `docker compose` CLI reachable? Mirrors `docker_available` in
/// `xtask/src/main.rs` (this test file cannot see that private fn, so it is
/// re-derived here rather than made `pub` for a single caller).
fn docker_compose_available() -> bool {
    Command::new("docker")
        .args(["compose", "version"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn is_ci() -> bool {
    std::env::var_os("CI").is_some()
}

#[test]
fn small_multi_node_compose_config_is_structurally_valid() {
    if !docker_compose_available() {
        assert!(
            !is_ci(),
            "docker compose is not available but is required in CI \
             (see .github/workflows/ci.yml's ubuntu-latest runner)"
        );
        eprintln!(
            "warning: docker compose not available; skipping the small-multi-node \
             compose-config validity check locally. Install Docker (and the compose \
             plugin) to run it."
        );
        return;
    }

    let compose = workspace_root().join("deploy/small-multi-node/docker-compose.yml");
    let output = Command::new("docker")
        .args(["compose", "-f"])
        .arg(&compose)
        .arg("config")
        .output()
        .expect("failed to spawn `docker compose config`");
    assert!(
        output.status.success(),
        "`docker compose -f {} config` must parse cleanly:\nstdout: {}\nstderr: {}",
        compose.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let merged = String::from_utf8_lossy(&output.stdout);
    // The profile's four component roles (proposal 0015 §"Deployment"; architecture
    // §7.1 "Small multi-node" row): the 3-node PD ensemble, TiKV-small, the 3-node
    // etcd ensemble (L5 Coordination), and the local-disk D servers.
    for service in [
        "pd0", "pd1", "pd2", "tikv:", "etcd0", "etcd1", "etcd2", "dserver0", "dserver1", "dserver2",
    ] {
        assert!(
            merged.contains(service),
            "merged compose config is missing the `{service}` service — all four \
             component roles (PD / TiKV / etcd / D server) must be declared:\n{merged}"
        );
    }
    // The images pinned for the three external components, and the D-server image
    // reused from the root dev stack (Scope: "reusing the wyrd-dserver:local image
    // and the wyrd d-server role").
    for image in [
        "pingcap/pd:",
        "pingcap/tikv:",
        "etcd:",
        "wyrd-dserver:local",
    ] {
        assert!(
            merged.contains(image),
            "merged compose config is missing the `{image}` image: {merged}"
        );
    }
}
