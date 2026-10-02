//! #775 blackbox dependency-closure guard: the flippable regressions for the
//! third repo-hygiene invariant `cargo xtask ci` runs — nothing that ships in
//! the `wyrd-validate` binary reaches a `wyrd-*` crate (proposal 0017 §9).
//!
//! Three things are pinned here:
//!
//! 1. **the guard runs inside the gate** — `cargo xtask ci-dry-run --workspace
//!    DIR` runs the gate's guard phase for real (the same `run_ci_steps_in`
//!    `ci` calls, the same guard dispatch) over a planted cargo workspace and
//!    only prints the cargo steps after it. With a forbidden dependency it
//!    must stop at the blackbox guard before any cargo step; clean, all three
//!    guards must pass, in order, before the first cargo step;
//! 2. **the guard is flippable** — red on planted `cargo metadata` documents
//!    (a direct normal edge, an optional off-by-default declaration, a
//!    transitive edge) and on planted workspaces resolved by real cargo (an
//!    optional crate behind a feature, a renamed dependency); green on a dev
//!    edge and over the real workspace;
//! 3. **the guard fails closed** on a document it cannot fully see.
//!
//! Every case drives the real `xtask` binary, never a copy of the scan:
//! `blackbox-guard --metadata FILE` runs the production
//! `xtask::repo_guard::scan_blackbox_closure` over a planted document, and
//! `ci-dry-run --workspace DIR` runs the production guards, `cargo metadata`
//! calls included. Driving the binary rather than the lib also keeps this file
//! compiling against a tree WITHOUT the guard, where it fails by assertion
//! instead of failing to build. The inputs are planted so the red cases never
//! require committing the dependency the guard forbids.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{json, Value};

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

/// Run the real `xtask` binary with `args` from the workspace root.
fn xtask(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(args)
        .current_dir(workspace_root())
        .output()
        .expect("failed to spawn the xtask binary")
}

/// A per-test scratch path under cargo's test scratch dir (inside `target/`,
/// so the planted workspaces inherit the repo's pinned toolchain). `tag`
/// keeps concurrently running tests apart.
fn scratch(tag: &str) -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("blackbox-guard-{tag}-{}", std::process::id()))
}

/// What one `xtask` run said.
struct Verdict {
    passed: bool,
    /// One entry per reported blackbox violation line.
    violations: Vec<String>,
    /// Everything the binary printed, for assertions and their messages.
    output: String,
}

impl Verdict {
    fn of(out: Output) -> Self {
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let violations = output
            .lines()
            .filter(|l| l.contains("wyrd-validate must not link Wyrd crate"))
            .map(str::to_string)
            .collect();
        Verdict {
            passed: out.status.success(),
            violations,
            output,
        }
    }

    /// Where `needle` first appears in the output, if at all.
    fn position(&self, needle: &str) -> Option<usize> {
        self.output.find(needle)
    }
}

/// The line `ci-dry-run` prints in place of running a cargo step.
const DRY_RUN_CARGO: &str = "dry-run: would run `cargo";

/// The pass lines of the two #616 guards that run before the blackbox guard.
const GITLINK_PASSED: &str = "xtask gitlink-guard: no stray gitlinks";
const UNSAFE_PASSED: &str = "xtask unsafe-guard: every crate root forbids unsafe code";

/// The blackbox guard's own clean verdict.
const BLACKBOX_PASSED: &str = "wyrd-validate's normal dependency closure holds no wyrd-* crate";

/// Plant `doc` as a file and run the guard over it.
fn scan(tag: &str, doc: &str) -> Verdict {
    let file = scratch(&format!("{tag}-doc"));
    std::fs::write(&file, doc).expect("plant the metadata document");
    let out = xtask(&[
        "blackbox-guard",
        "--metadata",
        file.to_str().expect("utf-8 path"),
    ]);
    std::fs::remove_file(&file).ok();
    Verdict::of(out)
}

/// The guard must refuse `doc` with its own fail-closed error naming `reason`
/// — not pass it, and not report it as an ordinary violation.
fn assert_refused(tag: &str, doc: &str, reason: &str) {
    let v = scan(tag, doc);
    assert!(!v.passed, "{tag}: must not pass:\n{}", v.output);
    assert!(
        v.violations.is_empty() && v.output.contains(reason),
        "{tag}: must be refused with `{reason}`:\n{}",
        v.output
    );
}

// ─── planted cargo workspaces, run through the gate's guard phase ────────────

/// A real cargo workspace laid out like this one: `crates/core` (`wyrd-core`,
/// standing in for any Wyrd crate), `crates/middle` (`middle`, standing in for
/// a third-party crate that itself depends on `wyrd-core`), and
/// `crates/validate` (`wyrd-validate`, whose manifest ends with `deps`). Every
/// crate root forbids unsafe code and the directory is its own git repository
/// with an empty index, so the two #616 guards pass on it and only the
/// blackbox guard can fail. Path dependencies only, so locking it needs no
/// network. Removed on drop.
struct Workspace {
    dir: PathBuf,
}

impl Workspace {
    fn plant(tag: &str, deps: &str) -> Self {
        let dir = scratch(tag);
        std::fs::remove_dir_all(&dir).ok();
        let write = |rel: &str, body: &str| {
            let path = dir.join(rel);
            std::fs::create_dir_all(path.parent().expect("has a parent"))
                .expect("create the planted workspace");
            std::fs::write(&path, body).expect("write the planted workspace");
        };
        let manifest = |name: &str, extra: &str| {
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
                 publish = false\n\n{extra}"
            )
        };
        let forbid = "#![forbid(unsafe_code)]\n";
        write(
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/core\", \"crates/middle\", \"crates/validate\"]\n\
             resolver = \"2\"\n",
        );
        write("crates/core/Cargo.toml", &manifest("wyrd-core", ""));
        write("crates/core/src/lib.rs", forbid);
        write(
            "crates/middle/Cargo.toml",
            &manifest(
                "middle",
                "[dependencies]\nwyrd-core = { path = \"../core\" }\n",
            ),
        );
        write("crates/middle/src/lib.rs", forbid);
        write(
            "crates/validate/Cargo.toml",
            &manifest("wyrd-validate", deps),
        );
        write(
            "crates/validate/src/main.rs",
            &format!("{forbid}\nfn main() {{}}\n"),
        );
        run_in(&dir, "git", &["init", "--quiet"]);
        Workspace { dir }
    }

    /// Plant it with a `Cargo.lock`, as a committed workspace has.
    fn plant_locked(tag: &str, deps: &str) -> Self {
        let workspace = Self::plant(tag, deps);
        run_in(&workspace.dir, "cargo", &["generate-lockfile", "--offline"]);
        workspace
    }

    /// `cargo xtask ci-dry-run --workspace <this workspace>`.
    fn dry_run(&self) -> Verdict {
        Verdict::of(xtask(&[
            "ci-dry-run",
            "--workspace",
            self.dir.to_str().expect("utf-8 path"),
        ]))
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

/// Run a fixture-setup command in `dir`; it must succeed.
fn run_in(dir: &Path, program: &str, args: &[&str]) {
    let out = Command::new(program)
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn {program}: {e}"));
    assert!(
        out.status.success(),
        "`{program} {}` in the planted workspace: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
}

// ─── (1) the guard runs inside `cargo xtask ci` ──────────────────────────────

#[test]
fn the_gate_stops_at_the_blackbox_guard_on_a_forbidden_dependency() {
    // The gate's own guard phase, over a workspace whose validator depends on
    // `wyrd-core`. A guard missing from the gate, a dispatch that skips it, or
    // an error swallowed on the way back would each let this run go on to the
    // cargo steps and pass.
    let ws = Workspace::plant_locked(
        "gate-red",
        "[dependencies]\nwyrd-core = { path = \"../core\" }\n",
    );
    let v = ws.dry_run();
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    assert!(
        v.violations[0].contains("`wyrd-core`")
            && v.violations[0].contains("wyrd-validate -> wyrd-core"),
        "{}",
        v.output
    );
    // The two #616 guards ran on the same workspace and passed, so the stop is
    // the blackbox guard's.
    assert!(
        v.output.contains(GITLINK_PASSED) && v.output.contains(UNSAFE_PASSED),
        "{}",
        v.output
    );
    assert!(
        !v.output.contains(DRY_RUN_CARGO),
        "no cargo step may follow a failing guard:\n{}",
        v.output
    );
}

#[test]
fn the_gate_runs_every_repo_guard_before_any_cargo_step() {
    // A dev-dependency on a Wyrd crate is allowed (proposal 0017 §14's
    // fixtures are dev-only), so this workspace passes every guard. The two
    // #616 guards keep their places, and the blackbox guard runs third, before
    // the first cargo step.
    let ws = Workspace::plant_locked(
        "gate-green",
        "[dev-dependencies]\nwyrd-core = { path = \"../core\" }\n",
    );
    let v = ws.dry_run();
    assert!(v.passed, "{}", v.output);
    assert!(v.violations.is_empty(), "{}", v.output);
    let order = [
        "$ xtask gitlink-guard",
        GITLINK_PASSED,
        "$ xtask unsafe-guard",
        UNSAFE_PASSED,
        "$ xtask blackbox-guard",
        BLACKBOX_PASSED,
        "dry-run: would run `cargo fmt --all -- --check`",
    ];
    let positions: Vec<Option<usize>> = order.iter().map(|s| v.position(s)).collect();
    assert!(
        positions.iter().all(Option::is_some) && positions.windows(2).all(|w| w[0] < w[1]),
        "expected, in this order: {order:#?}\n{}",
        v.output
    );
}

#[test]
fn a_dry_run_with_no_argument_checks_this_workspace() {
    // The default is the workspace `cargo xtask ci` checks. Only the start is
    // asserted: the gitlink guard reads this checkout's git index, which a
    // copied tree may not have.
    let v = Verdict::of(xtask(&["ci-dry-run"]));
    assert!(
        v.output.starts_with(&format!(
            "xtask ci-dry-run: {}\n",
            workspace_root().display()
        )) && v.output.contains("$ xtask gitlink-guard"),
        "{}",
        v.output
    );
}

#[test]
fn unknown_arguments_are_a_usage_error() {
    let blackbox_usage = "usage: cargo xtask blackbox-guard [--metadata FILE]";
    let dry_run_usage = "usage: cargo xtask ci-dry-run [--workspace DIR]";
    let here = workspace_root();
    let here = here.to_str().expect("utf-8 path");
    for (args, usage) in [
        (
            vec!["blackbox-guard", "--bogus", "Cargo.toml"],
            blackbox_usage,
        ),
        (vec!["blackbox-guard", "--metadata"], blackbox_usage),
        (vec!["blackbox-guard", "--workspace", here], blackbox_usage),
        (vec!["ci-dry-run", "--bogus", here], dry_run_usage),
        (vec!["ci-dry-run", "--workspace"], dry_run_usage),
    ] {
        let v = Verdict::of(xtask(&args));
        assert!(
            !v.passed && v.output.contains(usage),
            "{args:?} must be refused with `{usage}`:\n{}",
            v.output
        );
    }
    let missing = scratch("no-such-workspace");
    let v = Verdict::of(xtask(&[
        "ci-dry-run",
        "--workspace",
        missing.to_str().expect("utf-8 path"),
    ]));
    assert!(
        !v.passed && v.output.contains("ci-dry-run: cannot open workspace"),
        "{}",
        v.output
    );
}

#[test]
fn the_real_workspace_has_no_violation() {
    // No argument: `cargo metadata --locked --all-features` over this
    // workspace, through the same guard dispatch the `ci` step uses.
    let v = Verdict::of(xtask(&["blackbox-guard"]));
    assert!(v.passed, "{}", v.output);
    assert!(v.output.contains(BLACKBOX_PASSED), "{}", v.output);
}

// ─── (2) the scan is flippable — planted workspaces, resolved by real cargo ──

#[test]
fn an_optional_crate_behind_an_off_by_default_feature_is_walked() {
    // `middle` is optional and its feature is off by default, so the graph
    // reaches `wyrd-core` only when every feature is resolved — the
    // `--all-features` the guard passes to cargo. The manifest names only
    // `middle`, so the manifest check cannot catch this one.
    let ws = Workspace::plant_locked(
        "real-optional",
        "[features]\nextra = [\"dep:middle\"]\n\n\
         [dependencies]\nmiddle = { path = \"../middle\", optional = true }\n",
    );
    let v = ws.dry_run();
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    assert!(
        v.violations[0].contains("wyrd-validate -> middle -> wyrd-core"),
        "{}",
        v.output
    );
}

#[test]
fn a_renamed_dependency_is_caught_by_its_package_name() {
    // `engine` is the extern name; the package is still `wyrd-core`.
    let ws = Workspace::plant_locked(
        "real-renamed",
        "[dependencies]\nengine = { package = \"wyrd-core\", path = \"../core\" }\n",
    );
    let v = ws.dry_run();
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    assert!(
        v.violations[0].contains("`wyrd-core`")
            && v.violations[0].contains("manifest")
            && v.violations[0].contains("wyrd-validate -> wyrd-core"),
        "{}",
        v.output
    );
}

#[test]
fn the_guard_never_writes_the_lock_file_it_audits() {
    // No `Cargo.lock`: `--locked` makes cargo refuse rather than create one,
    // so the guard fails with its own error and leaves the tree as it found
    // it — a guard that rewrote the lock file would mutate what it audits.
    let ws = Workspace::plant("real-unlocked", "");
    let v = ws.dry_run();
    assert!(!v.passed, "{}", v.output);
    assert!(
        v.output.contains("blackbox-guard: `cargo metadata"),
        "{}",
        v.output
    );
    assert!(
        !ws.dir.join("Cargo.lock").exists(),
        "the guard wrote a Cargo.lock:\n{}",
        v.output
    );
}

// ─── planted `cargo metadata` documents ──────────────────────────────────────

// Package ids are deliberately name-free: an id is opaque, so a scan that read
// a package's name out of its id would miss every violation below.
const VALIDATE: &str = "path+file:///ws/crates/validate#0.0.0";
const CORE: &str = "path+file:///ws/crates/core#0.0.0";
const MIDDLE: &str = "registry+https://github.com/rust-lang/crates.io-index#1.0.0";
const LEAF: &str = "registry+https://github.com/rust-lang/crates.io-index#2.0.0";

/// A package record (`packages[]`) as `cargo metadata` emits it.
fn package(id: &str, name: &str, dependencies: Vec<Value>) -> Value {
    json!({ "id": id, "name": name, "version": "0.0.0", "dependencies": dependencies })
}

/// A declared dependency (`packages[].dependencies[]`).
fn declared(name: &str, kind: Option<&str>, optional: bool) -> Value {
    json!({ "name": name, "kind": kind, "optional": optional, "rename": null, "target": null })
}

/// A resolve-graph edge to `pkg`, with one `dep_kinds` entry per kind. The
/// edge's `name` is the extern-crate name, which a rename changes — it is set
/// to junk so a scan that read names from edges would miss every violation.
fn edge(pkg: &str, kinds: &[Option<&str>]) -> Value {
    let kinds: Vec<Value> = kinds
        .iter()
        .map(|kind| json!({ "kind": kind, "target": null }))
        .collect();
    json!({ "name": "renamed", "pkg": pkg, "dep_kinds": kinds })
}

/// A resolve node with its outgoing edges.
fn node(id: &str, deps: Vec<Value>) -> Value {
    json!({ "id": id, "deps": deps, "features": [] })
}

fn document(packages: Vec<Value>, nodes: Vec<Value>) -> Value {
    json!({
        "packages": packages,
        "workspace_members": [VALIDATE, CORE],
        "resolve": { "nodes": nodes, "root": null },
        "version": 1
    })
}

/// `wyrd-validate` with one DIRECT dependency on `wyrd-core` of `kind`,
/// declared in the manifest and present in the resolve graph.
fn direct(kind: Option<&str>) -> Value {
    document(
        vec![
            package(
                VALIDATE,
                "wyrd-validate",
                vec![declared("wyrd-core", kind, false)],
            ),
            package(CORE, "wyrd-core", vec![]),
        ],
        vec![
            node(VALIDATE, vec![edge(CORE, &[kind])]),
            node(CORE, vec![]),
        ],
    )
}

/// `wyrd-validate -> middle -> leaf -> wyrd-core`, every edge normal. The
/// manifest names only `middle`, so only the graph walk can find `wyrd-core`.
fn transitive() -> Value {
    document(
        vec![
            package(
                VALIDATE,
                "wyrd-validate",
                vec![declared("middle", None, false)],
            ),
            package(MIDDLE, "middle", vec![]),
            package(LEAF, "leaf", vec![]),
            package(CORE, "wyrd-core", vec![]),
        ],
        vec![
            node(VALIDATE, vec![edge(MIDDLE, &[None])]),
            node(MIDDLE, vec![edge(LEAF, &[None])]),
            node(LEAF, vec![edge(CORE, &[None])]),
            node(CORE, vec![]),
        ],
    )
}

/// `wyrd-validate -> middle` (normal), then `middle -> wyrd-core` with the
/// given kinds.
fn via_middle(kinds: &[Option<&str>]) -> Value {
    document(
        vec![
            package(
                VALIDATE,
                "wyrd-validate",
                vec![declared("middle", None, false)],
            ),
            package(MIDDLE, "middle", vec![]),
            package(CORE, "wyrd-core", vec![]),
        ],
        vec![
            node(VALIDATE, vec![edge(MIDDLE, &[None])]),
            node(MIDDLE, vec![edge(CORE, kinds)]),
            node(CORE, vec![]),
        ],
    )
}

// ─── (2) the scan is flippable — planted documents ───────────────────────────

#[test]
fn a_normal_dependency_on_a_wyrd_crate_is_one_violation_naming_it() {
    let v = scan("direct-normal", &direct(None).to_string());
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    let violation = &v.violations[0];
    assert!(violation.contains("`wyrd-core`"), "{}", v.output);
    // Both checks fired, and the wording says which.
    assert!(
        violation.contains("declared as a normal dependency in its manifest")
            && violation.contains(
                "reached through the normal dependency graph (all features): \
                 wyrd-validate -> wyrd-core"
            ),
        "{}",
        v.output
    );
}

#[test]
fn a_dev_dependency_on_a_wyrd_crate_is_allowed() {
    // Dev-dependencies are deliberately unconstrained (proposal 0017 §14's
    // fixtures are dev-only).
    let v = scan("direct-dev", &direct(Some("dev")).to_string());
    assert!(v.passed, "{}", v.output);
    assert!(v.violations.is_empty(), "{}", v.output);
}

#[test]
fn a_build_dependency_on_a_wyrd_crate_is_not_a_normal_edge() {
    // A build-dependency links into the build script, not the shipped binary;
    // only normal (`kind: null`) edges are in scope.
    let v = scan("direct-build", &direct(Some("build")).to_string());
    assert!(v.passed, "{}", v.output);
}

#[test]
fn an_optional_off_by_default_wyrd_dependency_is_a_violation() {
    // The declaration is optional and its feature is off, so a default-feature
    // resolve graph has no edge to it: only the manifest check can see it.
    let doc = document(
        vec![
            package(
                VALIDATE,
                "wyrd-validate",
                vec![declared("wyrd-core", None, true)],
            ),
            package(CORE, "wyrd-core", vec![]),
        ],
        vec![node(VALIDATE, vec![]), node(CORE, vec![])],
    );
    let v = scan("optional", &doc.to_string());
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    assert!(
        v.violations[0].contains("`wyrd-core`")
            && v.violations[0].contains("declared as a normal optional dependency")
            && !v.violations[0].contains("graph"),
        "{}",
        v.output
    );
}

#[test]
fn a_transitive_wyrd_dependency_is_a_violation_naming_the_path() {
    let v = scan("transitive", &transitive().to_string());
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    assert!(
        v.violations[0].contains("wyrd-validate -> middle -> leaf -> wyrd-core")
            && !v.violations[0].contains("manifest"),
        "the violation names the path: {}",
        v.output
    );
}

#[test]
fn a_wyrd_crate_behind_a_third_party_dev_edge_is_not_followed() {
    // A dependency's own dev-dependency never links into our binary.
    let v = scan("deep-dev", &via_middle(&[Some("dev")]).to_string());
    assert!(v.passed, "{}", v.output);
}

#[test]
fn an_edge_that_is_both_dev_and_normal_is_followed() {
    // Cargo lists one kind per declaration; the edge links if ANY is normal,
    // wherever it sits in the list.
    let v = scan(
        "dev-and-normal",
        &via_middle(&[Some("dev"), None]).to_string(),
    );
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    assert!(
        v.violations[0].contains("wyrd-validate -> middle -> wyrd-core"),
        "{}",
        v.output
    );
}

#[test]
fn a_crate_first_met_on_a_dev_edge_is_still_walked_on_a_normal_path() {
    // `wyrd-validate` dev-depends on `wyrd-core` (allowed) AND reaches it
    // through `middle` (not allowed). The dev edge comes first; it must not
    // mark `wyrd-core` as already walked.
    let doc = document(
        vec![
            package(
                VALIDATE,
                "wyrd-validate",
                vec![
                    declared("wyrd-core", Some("dev"), false),
                    declared("middle", None, false),
                ],
            ),
            package(MIDDLE, "middle", vec![]),
            package(CORE, "wyrd-core", vec![]),
        ],
        vec![
            node(
                VALIDATE,
                vec![edge(CORE, &[Some("dev")]), edge(MIDDLE, &[None])],
            ),
            node(MIDDLE, vec![edge(CORE, &[None])]),
            node(CORE, vec![]),
        ],
    );
    let v = scan("dev-then-normal", &doc.to_string());
    assert!(!v.passed, "{}", v.output);
    assert_eq!(v.violations.len(), 1, "{}", v.output);
    assert!(
        v.violations[0].contains("wyrd-validate -> middle -> wyrd-core"),
        "{}",
        v.output
    );
}

// ─── (3) the scan fails closed ───────────────────────────────────────────────

#[test]
fn an_unparsable_document_is_refused() {
    assert_refused("unparsable", "{ not json", "cannot parse cargo metadata");
}

#[test]
fn a_document_without_a_resolve_graph_is_refused() {
    // What `cargo metadata --no-deps` produces: `resolve` is null.
    let mut doc = direct(None);
    doc["resolve"] = Value::Null;
    assert_refused("resolve-null", &doc.to_string(), "no `resolve.nodes` graph");
    doc.as_object_mut().expect("object").remove("resolve");
    assert_refused(
        "resolve-absent",
        &doc.to_string(),
        "no `resolve.nodes` graph",
    );
}

#[test]
fn a_missing_blackbox_package_is_refused() {
    // E.g. `crates/validate` dropped from `[workspace] members`: removing the
    // guard's subject must not make it pass.
    let doc = document(
        vec![package(CORE, "wyrd-core", vec![])],
        vec![node(CORE, vec![])],
    );
    assert_refused("no-subject", &doc.to_string(), "no `wyrd-validate` package");
}

#[test]
fn two_blackbox_packages_are_refused() {
    let mut doc = direct(None);
    doc["packages"]
        .as_array_mut()
        .expect("array")
        .push(package(LEAF, "wyrd-validate", vec![]));
    assert_refused(
        "two-subjects",
        &doc.to_string(),
        "more than one `wyrd-validate` package",
    );
}

#[test]
fn a_blackbox_package_in_no_resolve_node_is_refused() {
    let mut doc = direct(None);
    doc["resolve"]["nodes"]
        .as_array_mut()
        .expect("array")
        .remove(0);
    assert_refused(
        "subject-no-node",
        &doc.to_string(),
        "`wyrd-validate` appears in no resolve node",
    );
}

#[test]
fn a_reached_package_in_no_resolve_node_is_refused() {
    // `leaf` is reached, but its node — and so everything below it — is gone.
    let mut doc = transitive();
    doc["resolve"]["nodes"]
        .as_array_mut()
        .expect("array")
        .remove(2);
    assert_refused(
        "reached-no-node",
        &doc.to_string(),
        "is reached but appears in no resolve node",
    );
}

#[test]
fn a_reached_package_record_without_a_name_is_refused() {
    // Without the name the prefix check cannot run; reading a name out of the
    // opaque id instead is exactly the fallback that lets it miss.
    let mut doc = direct(None);
    doc["packages"][1]
        .as_object_mut()
        .expect("object")
        .remove("name");
    assert_refused("no-name", &doc.to_string(), "has no decodable `name`");
}

#[test]
fn a_reached_package_record_without_an_id_is_refused() {
    let mut doc = direct(None);
    doc["packages"][1]
        .as_object_mut()
        .expect("object")
        .remove("id");
    assert_refused("no-id", &doc.to_string(), "has no decodable `id`");
}

#[test]
fn an_edge_without_dep_kinds_is_refused() {
    // Cannot tell a normal edge from a dev edge, so it must not be skipped.
    let mut doc = direct(None);
    doc["resolve"]["nodes"][0]["deps"][0]
        .as_object_mut()
        .expect("object")
        .remove("dep_kinds");
    assert_refused("no-dep-kinds", &doc.to_string(), "has no `dep_kinds` list");
}

#[test]
fn an_edge_with_empty_dep_kinds_is_refused() {
    // Skipping such an edge hides everything below it. Deep: `middle ->
    // wyrd-core` is `[]`, and the manifest names only `middle`.
    assert_refused(
        "empty-kinds-deep",
        &via_middle(&[]).to_string(),
        "has an empty `dep_kinds` list",
    );
    // Direct: `wyrd-validate -> middle` is `[]`, hiding the whole closure.
    let mut doc = via_middle(&[None]);
    doc["resolve"]["nodes"][0]["deps"][0]["dep_kinds"] = json!([]);
    assert_refused(
        "empty-kinds-direct",
        &doc.to_string(),
        "has an empty `dep_kinds` list",
    );
}

#[test]
fn an_edge_of_unknown_kind_is_refused() {
    assert_refused(
        "unknown-edge-kind",
        &via_middle(&[Some("weird")]).to_string(),
        "has unknown dependency kind \"weird\"",
    );
    // Every entry is classified, not only up to the first normal one.
    assert_refused(
        "unknown-after-normal",
        &via_middle(&[None, Some("weird")]).to_string(),
        "has unknown dependency kind \"weird\"",
    );
    // An entry with no `kind` field at all.
    let mut doc = via_middle(&[None]);
    doc["resolve"]["nodes"][1]["deps"][0]["dep_kinds"] = json!([{ "target": null }]);
    assert_refused("edge-no-kind", &doc.to_string(), "has no `kind` field");
}

#[test]
fn a_declared_dependency_of_unknown_kind_is_refused() {
    let mut doc = direct(None);
    doc["packages"][0]["dependencies"][0]["kind"] = json!("weird");
    assert_refused(
        "unknown-declared-kind",
        &doc.to_string(),
        "declared dependency `wyrd-core` of `wyrd-validate` has unknown dependency kind",
    );
    doc["packages"][0]["dependencies"][0]
        .as_object_mut()
        .expect("object")
        .remove("kind");
    assert_refused(
        "declared-no-kind",
        &doc.to_string(),
        "declared dependency `wyrd-core` of `wyrd-validate` has no `kind` field",
    );
}

/// An edit that removes one record from a planted document.
type Breakage = fn(&mut Value);

#[test]
fn structurally_incomplete_documents_are_refused() {
    // Every other record the scan reads, missing: each is an error, never a
    // part of the closure silently left out.
    let cases: Vec<(&str, Breakage, &str)> = vec![
        (
            "no-packages",
            |d| {
                d.as_object_mut().expect("object").remove("packages");
            },
            "no `packages` array",
        ),
        (
            "no-declared-list",
            |d| {
                d["packages"][0]
                    .as_object_mut()
                    .expect("object")
                    .remove("dependencies");
            },
            "`wyrd-validate` has no `dependencies` list",
        ),
        (
            "declared-no-name",
            |d| {
                d["packages"][0]["dependencies"][0]
                    .as_object_mut()
                    .expect("object")
                    .remove("name");
            },
            "a declared dependency of `wyrd-validate` has no `name`",
        ),
        (
            "node-no-id",
            |d| {
                d["resolve"]["nodes"][1]
                    .as_object_mut()
                    .expect("object")
                    .remove("id");
            },
            "a resolve node has no decodable `id`",
        ),
        (
            "node-no-deps",
            |d| {
                d["resolve"]["nodes"][1]
                    .as_object_mut()
                    .expect("object")
                    .remove("deps");
            },
            "has no `deps` list",
        ),
        (
            "edge-no-pkg",
            |d| {
                d["resolve"]["nodes"][1]["deps"][0]
                    .as_object_mut()
                    .expect("object")
                    .remove("pkg");
            },
            "has no `pkg`",
        ),
        (
            "edge-to-no-record",
            |d| {
                d["packages"].as_array_mut().expect("array").remove(2);
            },
            "reaches a package with no record",
        ),
    ];
    for (tag, break_it, reason) in cases {
        let mut doc = via_middle(&[None]);
        break_it(&mut doc);
        assert_refused(tag, &doc.to_string(), reason);
    }
}
