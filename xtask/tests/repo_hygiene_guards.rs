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

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use xtask::repo_guard::{
    gitmodules_config_paths, scan_gitlinks, scan_roots, target_src_paths, unregistered_manifests,
    UNSAFE_FORBID_ALLOWLIST,
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
    let cfg = "submodule.dep.path\nvendor/dep\0submodule.dep.url\nhttps://example.com/dep\0\
               submodule.spaced.path\nvendor/my dep\0submodule.spaced.url\n../rel/dep\0";
    let declared = gitmodules_config_paths(cfg);
    assert_eq!(declared, ["vendor/dep", "vendor/my dep"]);
    let ls = "160000 bbbbbbbb 0\tvendor/dep\x00160000 cccccccc 0\tvendor/my dep\0";
    assert!(scan_gitlinks(ls, &declared).is_empty());
}

#[test]
fn scan_gitlinks_follows_gits_effective_value_for_duplicate_stanzas() {
    // `[submodule "dep"]` repeated with different paths: git resolves the
    // single-valued key to the LAST definition, and `git submodule status`
    // fatals on the shadowed first path ("no submodule mapping found in
    // .gitmodules for path 'vendor/a'" — verified live). The shadowed path
    // must therefore NOT legitimize a gitlink.
    let cfg = "submodule.dep.path\nvendor/a\0submodule.dep.url\nhttps://example.com/a\0\
               submodule.dep.path\nvendor/b\0submodule.dep.url\nhttps://example.com/b\0";
    let declared = gitmodules_config_paths(cfg);
    assert_eq!(declared, ["vendor/b"], "last definition wins: {declared:?}");
    let ls = "160000 aaaaaaaa 0\tvendor/a\x00160000 bbbbbbbb 0\tvendor/b\0";
    let violations = scan_gitlinks(ls, &declared);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].starts_with("vendor/a:"),
        "the shadowed path is the violation: {violations:?}"
    );
}

#[test]
fn scan_gitlinks_is_red_on_a_url_less_submodule_stanza() {
    // A path-only stanza is not a usable declaration: git refuses it at
    // consumption time ("fatal: No url found for submodule path ... in
    // .gitmodules" from clone --recurse-submodules / submodule update --init,
    // verified live), so the gitlink still breaks fresh clones.
    let cfg = "submodule.dep.path\nvendor/dep\0";
    let declared = gitmodules_config_paths(cfg);
    assert!(
        declared.is_empty(),
        "path without url is unusable: {declared:?}"
    );
    let ls = "160000 bbbbbbbb 0\tvendor/dep\0";
    let violations = scan_gitlinks(ls, &declared);
    assert_eq!(violations.len(), 1, "{violations:?}");
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
    // Declared submodules via the SAME index-snapshot path the production
    // guard reads (`--blob :.gitmodules`; empty when untracked), so this
    // regression stays green if a legitimate declared submodule is ever added.
    let declared = if xtask::repo_guard::index_has(&ls, ".gitmodules") {
        let cfg = std::process::Command::new("git")
            .args([
                "config",
                "-z",
                "--blob",
                ":.gitmodules",
                "--get-regexp",
                r"^submodule\..*\.(path|url)$",
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

/// Every `.rs` file under `dir`, sorted — the fixture stand-in for the target
/// list `cargo metadata` supplies in production.
fn planted_roots(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// Plant a fixture crate tree: `<tmp>/crates/<name>/{Cargo.toml, src/lib.rs}`.
fn plant_crate(root: &Path, name: &str, lib_rs: &str) {
    let dir = root.join(name);
    std::fs::create_dir_all(dir.join("src")).expect("create fixture crate dir");
    std::fs::write(dir.join("Cargo.toml"), "[package]\n").expect("write fixture manifest");
    std::fs::write(dir.join("src/lib.rs"), lib_rs).expect("write fixture lib.rs");
}

fn fixture_dir(tag: &str) -> PathBuf {
    // pid + per-process counter for uniqueness — deliberately no wall-clock
    // read (the rubric's clock rule: nothing here owns a clocked lifecycle).
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    std::env::temp_dir().join(format!(
        "wyrd-repo-guard-fixture-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
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

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
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
    std::fs::create_dir_all(dir.join("tooling/benches/suite")).expect("create bench dir-form");
    std::fs::write(dir.join("tooling/build.rs"), "fn main() {}\n").expect("write build.rs");
    std::fs::write(dir.join("tooling/benches/perf.rs"), "fn main() {}\n").expect("write bench");
    std::fs::write(dir.join("tooling/benches/suite/main.rs"), "fn main() {}\n")
        .expect("write dir-form bench");
    std::fs::write(dir.join("tooling/examples/flat.rs"), "fn main() {}\n").expect("write example");
    std::fs::write(
        dir.join("tooling/examples/demo/main.rs"),
        "#![forbid(unsafe_code)]\nfn main() {}\n",
    )
    .expect("write example main");

    let mut violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    std::fs::remove_dir_all(&dir).ok();
    violations.sort();

    assert_eq!(
        violations.len(),
        4,
        "compliant demo/main.rs passes: {violations:?}"
    );
    assert!(violations[0].contains("perf.rs"), "{violations:?}");
    assert!(
        violations[1].contains("suite") && violations[1].contains("main.rs"),
        "the dir-form bench root is scanned (cargo auto-discovers benches/*/main.rs): {violations:?}"
    );
    assert!(violations[2].contains("build.rs"), "{violations:?}");
    assert!(violations[3].contains("flat.rs"), "{violations:?}");
}

#[test]
fn scan_crate_roots_rejects_a_commented_out_attribute() {
    // A block-commented attribute keeps its line byte-identical, but rustc
    // applies no lint — the guard must see through comments, not match text.
    let dir = fixture_dir("commented");
    plant_crate(
        &dir,
        "sneaky",
        "/*\n#![forbid(unsafe_code)]\n*/\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "line-commented",
        "// #![forbid(unsafe_code)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "active-after-docs",
        "//! docs mention /* oddities */ freely\n#![forbid(unsafe_code)]\npub fn f() {}\n",
    );

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        violations.len(),
        2,
        "both commented forms are red; the active one passes: {violations:?}"
    );
    assert!(
        violations.iter().any(|v| v.contains("sneaky"))
            && violations.iter().any(|v| v.contains("line-commented")),
        "{violations:?}"
    );
}

#[test]
fn scan_crate_roots_honors_the_deny_allowlist() {
    // The metadata-fdb shape: an allowlisted crate must carry its recorded
    // weaker attribute — and is red without it, so the exception cannot decay
    // into "no attribute at all".
    let dir = fixture_dir("allowlist");
    plant_crate(
        &dir.join("crates"),
        "metadata-fdb",
        "#![deny(unsafe_code)]\npub fn f() {}\n",
    );
    assert!(
        scan_roots(&planted_roots(&dir), &dir)
            .expect("fixture tree is scannable")
            .is_empty(),
        "deny satisfies the exception"
    );
    std::fs::remove_dir_all(&dir).ok();

    let dir = fixture_dir("allowlist-decayed");
    plant_crate(&dir.join("crates"), "metadata-fdb", "pub fn f() {}\n");
    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
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
        &dir.join("crates"),
        "metadata-fdb",
        "#![deny(unsafe_code)]\npub fn f() {}\n",
    );
    std::fs::create_dir_all(dir.join("crates/metadata-fdb/src/bin")).expect("create bin dir");
    std::fs::write(
        dir.join("crates/metadata-fdb/src/bin/fdbtool.rs"),
        "#![deny(unsafe_code)]\nfn main() {}\n",
    )
    .expect("write bin");
    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
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
    let meta = std::process::Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(workspace_root())
        .output()
        .expect("failed to spawn cargo metadata");
    assert!(meta.status.success(), "cargo metadata must succeed");
    let roots =
        target_src_paths(&String::from_utf8_lossy(&meta.stdout)).expect("cargo metadata parses");
    let violations =
        scan_roots(&roots, &workspace_root()).expect("the real workspace is scannable");
    assert!(
        violations.is_empty(),
        "non-compliant crate roots: {violations:?}"
    );
}

#[test]
fn scan_crate_roots_requires_the_attribute_at_crate_level() {
    // An inner attribute inside a nested module scopes to THAT module only
    // (rustc applies crate-wide inner attributes only before the first item),
    // so a nested — or even cfg'd-out — occurrence must not satisfy the
    // guard; a multi-line inner attribute BEFORE it must not end the
    // preamble walk early.
    let dir = fixture_dir("nested-attr");
    plant_crate(
        &dir,
        "nested",
        "pub mod inner {\n    #![forbid(unsafe_code)]\n}\n",
    );
    plant_crate(
        &dir,
        "cfgd-out",
        "#[cfg(any())]\nmod ghost {\n    #![forbid(unsafe_code)]\n}\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "after-multiline-attr",
        "//! docs\n#![cfg_attr(\n    test,\n    allow(dead_code)\n)]\n#![forbid(unsafe_code)]\npub fn f() {}\n",
    );

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    assert_eq!(
        violations.len(),
        2,
        "nested and cfg'd-out are red; preamble-after-multiline is green: {violations:?}"
    );
    assert!(
        violations.iter().any(|v| v.contains("nested"))
            && violations.iter().any(|v| v.contains("cfgd-out")),
        "{violations:?}"
    );
}

#[test]
fn scan_crate_roots_accepts_comment_markers_inside_attribute_strings() {
    // `//` inside a string literal is content, not a comment: the canonical
    // preamble case is `#![doc(html_root_url = "https://docs.rs/foo")]`
    // before the unsafe attribute — the guard must not over-strip the URL,
    // unbalance the doc attribute, and swallow the forbid that follows.
    let dir = fixture_dir("attr-strings");
    plant_crate(
        &dir,
        "docurl",
        "#![doc(html_root_url = \"https://docs.rs/foo\")]\n#![forbid(unsafe_code)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "rawstr",
        "#![doc(html_root_url = r#\"https://docs.rs/bar\"#)]\n#![forbid(unsafe_code)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "starry",
        "#![doc = \"see /* not a comment */ and // neither\"]\n#![forbid(unsafe_code)]\npub fn f() {}\n",
    );

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    assert!(
        violations.is_empty(),
        "string contents must never unbalance the preamble walk: {violations:?}"
    );
}

#[test]
fn scan_crate_roots_ignores_brackets_and_attributes_inside_strings() {
    // Bracket characters inside a string literal must not unbalance the
    // preamble walk (`#![doc = "unmatched ["]` before the forbid was a FALSE
    // RED), and an attribute spelled inside a string must not count as
    // present — both follow from blanking string bodies.
    let dir = fixture_dir("string-brackets");
    plant_crate(
        &dir,
        "bracketed",
        "#![doc = \"unmatched [\"]\n#![forbid(unsafe_code)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "raw-bracketed",
        "#![doc = r#\"a ] and a [\"#]\n#![forbid(unsafe_code)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "spelled-in-string",
        "#![doc = \"#![forbid(unsafe_code)]\"]\npub fn f() {}\n",
    );

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        violations.len(),
        1,
        "the two bracketed crates are compliant; only the string-spelled one is red: {violations:?}"
    );
    assert!(
        violations[0].contains("spelled-in-string"),
        "{violations:?}"
    );
}

#[test]
fn scan_crate_roots_finds_packages_below_an_intermediate_directory() {
    // A workspace may group packages under an intermediate dir
    // (crates/storage/foo/Cargo.toml). A single-level listing walks past it,
    // leaving that crate unscanned while the gate stays green.
    let dir = fixture_dir("nested-pkg");
    plant_crate(&dir, "top", "#![forbid(unsafe_code)]\npub fn f() {}\n");
    std::fs::create_dir_all(dir.join("storage")).expect("create grouping dir");
    plant_crate(&dir.join("storage"), "foo", "pub fn f() {}\n");

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        violations.len(),
        1,
        "the nested package is scanned: {violations:?}"
    );
    assert!(
        violations[0].contains("storage") && violations[0].contains("foo"),
        "{violations:?}"
    );
}

#[test]
fn target_src_paths_follows_manifest_overrides_for_every_kind() {
    // The reason discovery is cargo's job, not a layout guess: a manifest may
    // point any target anywhere (`[lib] path`, `package.build`, custom
    // `[[bin]]`/`[[bench]]`/`[[example]]` paths), and cargo has already
    // resolved all of it. Every kind is in scope, `test` included.
    let metadata = r#"{"packages":[{"targets":[
        {"kind":["lib"],"src_path":"/w/crates/odd/weird/place.rs"},
        {"kind":["custom-build"],"src_path":"/w/crates/odd/build-script.rs"},
        {"kind":["bin"],"src_path":"/w/crates/odd/tools/cli.rs"},
        {"kind":["bench"],"src_path":"/w/crates/odd/perf/suite.rs"},
        {"kind":["example"],"src_path":"/w/crates/odd/demo/show.rs"},
        {"kind":["test"],"src_path":"/w/crates/odd/tests/it.rs"}
    ]}]}"#;
    let roots = target_src_paths(metadata).expect("metadata parses");
    let names: Vec<String> = roots
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(roots.len(), 6, "every target, whatever its kind: {roots:?}");
    for expected in [
        "place.rs",
        "build-script.rs",
        "cli.rs",
        "suite.rs",
        "show.rs",
        "it.rs",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "{expected} (a manifest-placed target) must be discovered: {roots:?}"
        );
    }
}

#[test]
fn target_src_paths_rejects_unusable_metadata() {
    assert!(target_src_paths("not json").is_err());
    assert!(
        target_src_paths(r#"{"no_packages":[]}"#).is_err(),
        "metadata without a packages array must fail closed"
    );
}

#[test]
fn scan_crate_roots_looks_past_a_leading_shebang() {
    // rustc skips a first-line shebang, so an attribute after it IS the
    // crate-level attribute; treating the shebang as the first item would
    // reject a compliant binary.
    let dir = fixture_dir("shebang");
    plant_crate(
        &dir,
        "scripted",
        "#!/usr/bin/env rust-script\n#![forbid(unsafe_code)]\nfn main() {}\n",
    );
    plant_crate(
        &dir,
        "scripted-missing",
        "#!/usr/bin/env rust-script\nfn main() {}\n",
    );

    let violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        violations.len(),
        1,
        "only the attribute-less one is red: {violations:?}"
    );
    assert!(violations[0].contains("scripted-missing"), "{violations:?}");
}

#[test]
fn a_crate_missing_from_workspace_members_is_reported() {
    // The workspace lists members explicitly, so a new crates/<name> nobody
    // wired up is absent from cargo metadata — and a metadata-driven scan
    // would pass over it exactly as it passes over a crate that does not
    // exist. It is also a defect in its own right: nothing builds or tests it.
    let dir = fixture_dir("unlisted");
    let crates = dir.join("crates");
    plant_crate(
        &crates,
        "listed",
        "#![forbid(unsafe_code)]\npub fn f() {}\n",
    );
    plant_crate(&crates, "forgotten", "pub fn f() {}\n");
    // Nested under an intermediate grouping directory — the shape a
    // single-level listing walks straight past.
    plant_crate(&crates.join("storage"), "buried", "pub fn f() {}\n");
    let metadata = format!(
        r#"{{"packages":[{{"manifest_path":"{}","targets":[]}}]}}"#,
        crates.join("listed/Cargo.toml").display()
    );

    let missing = unregistered_manifests(&metadata, &crates).expect("metadata parses");
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        missing.len(),
        2,
        "both unlisted crates, at any depth: {missing:?}"
    );
    assert!(
        missing.iter().any(|m| m.contains("forgotten")),
        "the top-level unlisted crate: {missing:?}"
    );
    assert!(
        missing
            .iter()
            .any(|m| m.contains("storage") && m.contains("buried")),
        "the nested unlisted crate must not escape the cross-check: {missing:?}"
    );
    assert!(
        missing.iter().all(|m| m.contains("workspace member")),
        "each violation says why it matters: {missing:?}"
    );
}

#[test]
fn every_crate_directory_is_a_workspace_member_today() {
    // The invariant over the real tree, and the reason the cross-check is
    // cheap to keep: it should always be empty here.
    let meta = std::process::Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(workspace_root())
        .output()
        .expect("failed to spawn cargo metadata");
    assert!(meta.status.success(), "cargo metadata must succeed");
    let missing = unregistered_manifests(
        &String::from_utf8_lossy(&meta.stdout),
        &workspace_root().join("crates"),
    )
    .expect("cargo metadata parses");
    assert!(
        missing.is_empty(),
        "unlisted crate directories: {missing:?}"
    );
}

#[test]
fn scan_crate_roots_accepts_any_forbid_list_containing_unsafe_code() {
    // `#![forbid(unsafe_code, unused_imports)]` forbids unsafe exactly as the
    // bare spelling does — rejecting it would fail a compliant crate. But a
    // list WITHOUT unsafe_code, and a weaker level, must still be red.
    let dir = fixture_dir("lint-lists");
    plant_crate(
        &dir,
        "listed-first",
        "#![forbid(unsafe_code, unused_imports)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "listed-later",
        "#![forbid(unused_imports, unsafe_code)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "trailing-comma",
        "#![forbid(unsafe_code,)]\npub fn f() {}\n",
    );
    plant_crate(
        &dir,
        "other-lints-only",
        "#![forbid(unused_imports)]\npub fn f() {}\n",
    );
    plant_crate(&dir, "too-weak", "#![deny(unsafe_code)]\npub fn f() {}\n");

    let mut violations = scan_roots(&planted_roots(&dir), &dir).expect("fixture tree is scannable");
    std::fs::remove_dir_all(&dir).ok();
    violations.sort();

    assert_eq!(
        violations.len(),
        2,
        "only the two non-compliant crates: {violations:?}"
    );
    assert!(
        violations.iter().any(|v| v.contains("other-lints-only")),
        "a forbid list without unsafe_code is not compliance: {violations:?}"
    );
    assert!(
        violations.iter().any(|v| v.contains("too-weak")),
        "deny does not satisfy a forbid requirement: {violations:?}"
    );
}

#[test]
fn scan_crate_roots_fails_closed_when_it_cannot_see_the_tree() {
    // A missing crates dir (e.g. after a workspace move) and a dir with no
    // crate roots must both be Err — a guard that cannot see the tree says
    // so; it never passes vacuously (the repo's own "resting red on
    // non-existence" rule, applied to the guard itself).
    let missing = fixture_dir("missing-tree");
    assert!(
        scan_roots(&planted_roots(&missing), &missing).is_err(),
        "missing dir must be Err"
    );

    let empty = fixture_dir("empty-tree");
    std::fs::create_dir_all(&empty).expect("create empty fixture dir");
    let out = scan_roots(&planted_roots(&empty), &empty);
    std::fs::remove_dir_all(&empty).ok();
    assert!(out.is_err(), "zero discovered roots must be Err: {out:?}");
}

#[test]
fn unsafe_forbid_allowlist_is_narrow_and_reasoned() {
    // Sanity on the exception list itself: every entry keys an exact root
    // path (never a whole crate), names a concrete required attribute, and
    // carries a non-empty reason — an exemption is a reviewed, explained
    // one-liner scoped to one file, never a blank crate-wide pass.
    for (root, level, reason) in UNSAFE_FORBID_ALLOWLIST {
        assert!(root.ends_with(".rs"), "path-keyed, not crate-keyed: {root}");
        assert!(
            *level == "deny" || *level == "forbid",
            "the exception records a lint LEVEL, not an attribute spelling: {level}"
        );
        assert!(!reason.is_empty(), "every exception states its reason");
    }
    assert!(
        UNSAFE_FORBID_ALLOWLIST.len() <= 1,
        "growing the unsafe-code exception list deserves explicit review"
    );
}
