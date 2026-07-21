//! #616 repo-hygiene guards: the flippable regressions for the two scans
//! `cargo xtask ci` runs via `run_gitlink_guard` / `run_unsafe_forbid_guard`
//! (`xtask/src/main.rs`) — the SAME production functions
//! (`xtask::repo_guard::{scan_gitlinks, scan_crate_roots}`), driven here over
//! planted inputs to demonstrate RED (a guard catching a real violation, not
//! resting red on non-existence), and over the real tree to demonstrate GREEN
//! (the invariant holds today). Mirrors the deploy-guard convention
//! (`xtask/tests/deploy_no_orchestrator_coupling.rs`).
//!
//! The gitlink scan is pure text over NUL-delimited `git ls-files -s -z`
//! output, so the RED cases feed synthetic index listings — planting a real
//! gitlink in a fixture would itself commit the accident the guard exists to
//! prevent.

use std::path::{Path, PathBuf};

use xtask::repo_guard::{
    gitmodules_config_paths, scan_crate_roots, scan_gitlinks, UNSAFE_FORBID_ALLOWLIST,
};

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

// ─── (1) gitlink guard ────────────────────────────────────────────────────────

#[test]
fn scan_gitlinks_is_red_on_an_undeclared_gitlink() {
    // The exact accident from PRs #594/#595/#597/#600: a mode-160000 entry
    // with no .gitmodules declaration.
    let ls = "100644 aaaaaaaa 0\tREADME.md\x00160000 bbbbbbbb 0\tvendor/dep\0";
    let violations = scan_gitlinks(ls, &[]);
    assert_eq!(
        violations.len(),
        1,
        "one undeclared gitlink: {violations:?}"
    );
    assert!(
        violations[0].starts_with("vendor/dep:"),
        "the violation names the path: {violations:?}"
    );
}

#[test]
fn scan_gitlinks_is_red_on_any_tracked_agent_worktree_entry() {
    // A tracked path under .claude/worktrees/ is a violation regardless of
    // mode — agent worktrees are never repository content.
    let ls = "100644 aaaaaaaa 0\t.claude/worktrees/pr-fix/notes.md\0";
    let violations = scan_gitlinks(ls, &[]);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].contains(".claude/worktrees/"),
        "{violations:?}"
    );
}

#[test]
fn scan_gitlinks_is_green_for_a_declared_submodule() {
    // A REAL submodule — gitlink plus matching declared path — is legal; the
    // guard targets the undeclared accident, not submodules as such. The
    // declared list comes from `git config -z -f .gitmodules` output
    // (`<key>\n<value>\0` records), so git handles config quoting upstream —
    // a value that was quoted in .gitmodules ("vendor/my dep") arrives raw.
    let cfg = "submodule.dep.path\nvendor/dep\0submodule.spaced.path\nvendor/my dep\0";
    let declared = gitmodules_config_paths(cfg);
    assert_eq!(declared, ["vendor/dep", "vendor/my dep"]);
    let ls = "160000 bbbbbbbb 0\tvendor/dep\x00160000 cccccccc 0\tvendor/my dep\0";
    assert!(scan_gitlinks(ls, &declared).is_empty());
}

#[test]
fn scan_gitlinks_is_red_on_a_quoted_worktree_path() {
    // Regression for the C-quoting dodge: with the newline form of
    // `git ls-files -s`, a non-ASCII path is emitted C-quoted
    // (`"\".claude/worktrees/\\303\\251/x"`) under the default
    // `core.quotePath`, which a prefix check would miss. The guard consumes
    // the `-z` form, where the same path arrives raw and is caught.
    let ls = "100644 aaaaaaaa 0\t.claude/worktrees/\u{e9}/notes.md\0";
    let violations = scan_gitlinks(ls, &[]);
    assert_eq!(violations.len(), 1, "{violations:?}");
}

#[test]
fn scan_gitlinks_is_green_over_the_real_index() {
    // The invariant itself: the index has no stray gitlinks today. Uses the
    // same `git ls-files -s -z` input the CI-side guard reads.
    let output = std::process::Command::new("git")
        .args(["ls-files", "-s", "-z"])
        .current_dir(workspace_root())
        .output()
        .expect("failed to spawn `git ls-files -s -z`");
    assert!(output.status.success(), "git ls-files -s -z must succeed");
    let ls = String::from_utf8_lossy(&output.stdout);
    // Declared submodules via the SAME `git config -z` path the production
    // guard reads (empty when no .gitmodules exists), so this regression stays
    // green if a legitimate declared submodule is ever added.
    let declared = if workspace_root().join(".gitmodules").is_file() {
        let cfg = std::process::Command::new("git")
            .args([
                "config",
                "-z",
                "-f",
                ".gitmodules",
                "--get-regexp",
                r"^submodule\..*\.path$",
            ])
            .current_dir(workspace_root())
            .output()
            .expect("failed to spawn `git config`");
        gitmodules_config_paths(&String::from_utf8_lossy(&cfg.stdout))
    } else {
        Vec::new()
    };
    let violations = scan_gitlinks(&ls, &declared);
    assert!(
        violations.is_empty(),
        "stray tracked entries: {violations:?}"
    );
}

// ─── (2) forbid(unsafe_code) guard ────────────────────────────────────────────

/// Plant a fixture crate tree: `<tmp>/crates/<name>/{Cargo.toml, src/lib.rs}`.
fn plant_crate(root: &Path, name: &str, lib_rs: &str) {
    let dir = root.join(name);
    std::fs::create_dir_all(dir.join("src")).expect("create fixture crate dir");
    std::fs::write(dir.join("Cargo.toml"), "[package]\n").expect("write fixture manifest");
    std::fs::write(dir.join("src/lib.rs"), lib_rs).expect("write fixture lib.rs");
}

fn fixture_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "wyrd-repo-guard-fixture-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos()
    ))
}

#[test]
fn scan_crate_roots_is_red_when_the_attribute_is_missing() {
    // The exact drift the guard exists to stop: a new crate shipping without
    // the attribute (as gateway-core and gateway-s3 did before this PR).
    let dir = fixture_dir("missing");
    plant_crate(&dir, "new-backend", "pub fn f() {}\n");
    plant_crate(
        &dir,
        "compliant",
        "#![forbid(unsafe_code)]\npub fn f() {}\n",
    );

    let violations = scan_crate_roots(&dir);
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].contains("new-backend") && violations[0].contains("forbid(unsafe_code)"),
        "the violation names the crate and the required attribute: {violations:?}"
    );
}

#[test]
fn scan_crate_roots_is_red_on_an_uncovered_bin_root() {
    // src/bin/<name>.rs and src/bin/<name>/main.rs are each independent rustc
    // crate roots — a bin target must not evade the scan the lib root passes.
    let dir = fixture_dir("bin-roots");
    plant_crate(&dir, "tooling", "#![forbid(unsafe_code)]\npub fn f() {}\n");
    std::fs::create_dir_all(dir.join("tooling/src/bin/inner")).expect("create bin dirs");
    std::fs::write(dir.join("tooling/src/bin/flat.rs"), "fn main() {}\n").expect("write bin");
    std::fs::write(
        dir.join("tooling/src/bin/inner/main.rs"),
        "#![forbid(unsafe_code)]\nfn main() {}\n",
    )
    .expect("write bin main");

    let violations = scan_crate_roots(&dir);
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        violations.len(),
        1,
        "only the flat bin lacks it: {violations:?}"
    );
    assert!(violations[0].contains("flat.rs"), "{violations:?}");
}

#[test]
fn scan_crate_roots_covers_build_scripts_benches_and_examples() {
    // build.rs, benches/*.rs, and examples/*.rs (incl. the dir-with-main.rs
    // form) build with the workspace (`--all-targets`) and are independent
    // crate roots — none may evade the scan.
    let dir = fixture_dir("build-bench-example");
    plant_crate(&dir, "tooling", "#![forbid(unsafe_code)]\npub fn f() {}\n");
    std::fs::create_dir_all(dir.join("tooling/benches")).expect("create benches dir");
    std::fs::create_dir_all(dir.join("tooling/examples/demo")).expect("create example dirs");
    std::fs::write(dir.join("tooling/build.rs"), "fn main() {}\n").expect("write build.rs");
    std::fs::write(dir.join("tooling/benches/perf.rs"), "fn main() {}\n").expect("write bench");
    std::fs::write(dir.join("tooling/examples/flat.rs"), "fn main() {}\n").expect("write example");
    std::fs::write(
        dir.join("tooling/examples/demo/main.rs"),
        "#![forbid(unsafe_code)]\nfn main() {}\n",
    )
    .expect("write example main");

    let mut violations = scan_crate_roots(&dir);
    std::fs::remove_dir_all(&dir).ok();
    violations.sort();

    assert_eq!(
        violations.len(),
        3,
        "compliant demo/main.rs passes: {violations:?}"
    );
    assert!(violations[0].contains("perf.rs"), "{violations:?}");
    assert!(violations[1].contains("build.rs"), "{violations:?}");
    assert!(violations[2].contains("flat.rs"), "{violations:?}");
}

#[test]
fn scan_crate_roots_honors_the_deny_allowlist() {
    // The metadata-fdb shape: an allowlisted crate must carry its recorded
    // weaker attribute — and is red without it, so the exception cannot decay
    // into "no attribute at all".
    let dir = fixture_dir("allowlist");
    plant_crate(
        &dir,
        "metadata-fdb",
        "#![deny(unsafe_code)]\npub fn f() {}\n",
    );
    assert!(
        scan_crate_roots(&dir).is_empty(),
        "deny satisfies the exception"
    );
    std::fs::remove_dir_all(&dir).ok();

    let dir = fixture_dir("allowlist-decayed");
    plant_crate(&dir, "metadata-fdb", "pub fn f() {}\n");
    let violations = scan_crate_roots(&dir);
    std::fs::remove_dir_all(&dir).ok();
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].contains("deny(unsafe_code)"),
        "{violations:?}"
    );

    // The exception is keyed to the exact root path, not the crate: a NEW bin
    // root inside the exempt crate must still carry the full `forbid` — the
    // FFI carve-out covers src/lib.rs alone.
    let dir = fixture_dir("allowlist-bin");
    plant_crate(
        &dir,
        "metadata-fdb",
        "#![deny(unsafe_code)]\npub fn f() {}\n",
    );
    std::fs::create_dir_all(dir.join("metadata-fdb/src/bin")).expect("create bin dir");
    std::fs::write(
        dir.join("metadata-fdb/src/bin/fdbtool.rs"),
        "#![deny(unsafe_code)]\nfn main() {}\n",
    )
    .expect("write bin");
    let violations = scan_crate_roots(&dir);
    std::fs::remove_dir_all(&dir).ok();
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].contains("fdbtool.rs") && violations[0].contains("forbid(unsafe_code)"),
        "the bin root must not inherit the lib exception: {violations:?}"
    );
}

#[test]
fn scan_crate_roots_is_green_over_the_real_workspace_crates() {
    // The invariant itself: every crate root complies today (this PR added the
    // attribute to gateway-core and gateway-s3, closing the observed drift).
    let violations = scan_crate_roots(&workspace_root().join("crates"));
    assert!(
        violations.is_empty(),
        "non-compliant crate roots: {violations:?}"
    );
}

#[test]
fn unsafe_forbid_allowlist_is_narrow_and_reasoned() {
    // Sanity on the exception list itself: every entry keys an exact root
    // path (never a whole crate), names a concrete required attribute, and
    // carries a non-empty reason — an exemption is a reviewed, explained
    // one-liner scoped to one file, never a blank crate-wide pass.
    for (root, attr, reason) in UNSAFE_FORBID_ALLOWLIST {
        assert!(root.ends_with(".rs"), "path-keyed, not crate-keyed: {root}");
        assert!(attr.contains("unsafe_code") && !reason.is_empty());
    }
    assert!(
        UNSAFE_FORBID_ALLOWLIST.len() <= 1,
        "growing the unsafe-code exception list deserves explicit review"
    );
}
