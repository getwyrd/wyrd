//! **No `libfdb_c` in the simulator's dependency graph** — the mechanical guarantee that
//! backs the simulated-FDB model (issue #468).
//!
//! `crates/dst` is the deterministic-simulation tier (ADR-0009). The `foundationdb` crate
//! binds `libfdb_c` through `foundationdb-sys`, and `libfdb_c` **spawns its own network
//! thread** and does real, non-deterministic I/O — `ensure_network()` boots it behind a
//! process-wide `OnceLock` (`crates/metadata-fdb/src/lib.rs:846-869`). Linking it into a DST
//! build target would violate seed determinism outright (ADR-0035; the rejection is recorded
//! at `crates/dst/tests/support/mod.rs:6-8`). So the FDB backend is **modelled**
//! (`support::SimFdbMetadataStore`), never linked.
//!
//! ## Linkage is a graph property, not a manifest-text property
//!
//! The obvious guard — grep this crate's `Cargo.toml` for `foundationdb` — is wrong in both
//! directions, and this crate's own manifest proves it:
//!
//! * **It misses.** `crates/dst/Cargo.toml:56,66,68` already declares `tonic`, `etcd-client`
//!   and `tokio` in Cargo's **rename** form (`name = { package = "real-name", … }`). A line
//!   reading `fdb = { package = "foundationdb", … }` links `libfdb_c` into every DST test
//!   binary while never spelling the needle where a key-name scan would look. So does a
//!   **transitive** edge: a dependency that itself depends on `foundationdb`.
//! * **It over-reaches.** `foundationdb` is optional behind `wyrd-metadata-fdb`'s
//!   default-**off** `fdb` feature (`crates/metadata-fdb/Cargo.toml:11-22`), so a bare
//!   `wyrd-metadata-fdb` dependency would trip a name scan without linking `libfdb_c` at all.
//!
//! What actually decides whether `libfdb_c` is linked is the **feature-unified resolved
//! dependency graph** of the `wyrd-dst` package — the thing `cargo tree -e features` prints,
//! and the thing cargo itself builds from. So that is what is asserted here, with a
//! **planted red** carrying both blind spots (the rename form *and* a transitive edge) to
//! prove the scanner catches what a text scan cannot.
//!
//! ## The graph is resolved under a forced `--cfg madsim`, not the ambient one
//!
//! The `libfdb_c` risk lives in `[target.'cfg(madsim)'.dev-dependencies]`
//! (`crates/dst/Cargo.toml:55`) — the section this manifest puts its renamed madsim deps in
//! (`tonic`, `etcd-client`, `tokio`), and the section a real FDB dep would be added to in the
//! same house style. `cargo tree` resolves that section **only** when `--cfg madsim` is in the
//! effective `RUSTFLAGS`. That is true under `cargo xtask dst`/`run_dst()`, but **NOT** under
//! `run-verify.sh`'s bare `cargo test -p wyrd-dst --test no_fdb_linkage` (this file is
//! deliberately not `#![cfg(madsim)]`-gated, so it must also run there). Inheriting the ambient
//! cfg would leave the invariant blind to the real-risk section under exactly the bare
//! invocation it must also run in — the gap the iteration-2 carry-forward flagged. So the guard
//! **forces `RUSTFLAGS=--cfg madsim`** for the linkage scan (the madsim graph is a superset of
//! the bare one for the FDB question), and the planted red proves the point in both directions:
//! the `cfg(madsim)`-gated FDB node is caught under `--cfg madsim` and invisible without it, so
//! forcing the cfg is demonstrably load-bearing, not decorative.
//!
//! Three assertions, all cheap, all deterministic, all run by `cargo xtask ci` (via
//! `run_dst()`):
//!
//! 1. **The linkage invariant.** No `foundationdb` / `foundationdb-sys` package node in
//!    `wyrd-dst`'s feature-unified graph — with **two** demonstrated reds proving the scanner
//!    works rather than resting green on non-existence (the shape of
//!    `xtask/tests/deploy_no_orchestrator_coupling.rs:67`): a planted fixture carrying the
//!    rename form and a transitive edge, and the **real** `foundationdb 0.10` dependency,
//!    surfaced by turning on `wyrd-metadata-fdb`'s default-off `fdb` feature.
//! 2. **The policy invariant.** `crates/dst/Cargo.toml` names neither `wyrd-metadata-fdb`
//!    nor `foundationdb` — including in the rename and quoted-section forms. Strictly
//!    stronger than (1) as a *policy* (the FDB crate must not even be named here, so nobody
//!    can flip the feature on later) and strictly weaker as *evidence of linkage*, which is
//!    why (1) exists and is the one this file's title claims.
//! 3. **The model exists.** `crates/dst/tests/support/mod.rs` declares the simulated-FDB
//!    store with its ambiguity nemesis and its `MetadataStore` impl. A structural assertion,
//!    stated plainly: it pins the *seam*, not the behaviour. The behavioural red→green for
//!    the commit-ambiguity property lives in `crates/dst/tests/commit_ambiguity.rs`, which
//!    only executes under `--cfg madsim`.
//!
//! ## Why `cargo tree` and not a TOML parse
//!
//! `crates/dst` has no `toml`/`serde` dependency. Adding one would trip ADR-0003 §2's
//! three-test dependency audit and the `deny.toml` allowlist. `cargo` is already present —
//! it is running this test — and it is the *authority* on feature unification, which no
//! single-manifest parse can reconstruct anyway.
//!
//! Unlike its sibling `commit_ambiguity.rs`, this file is deliberately **not**
//! `#![cfg(madsim)]`-gated, so it also runs under a bare
//! `cargo test -p wyrd-dst --test no_fdb_linkage`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The packages that link `libfdb_c`: the `foundationdb` binding and the `-sys` crate that
/// actually declares the native library. Either one in the graph means the simulator's test
/// binaries link a real network thread.
const LINKS_LIBFDB_C: [&str; 2] = ["foundationdb", "foundationdb-sys"];

/// Dependency names `crates/dst/Cargo.toml` must never *name*, in any form. Broader than
/// [`LINKS_LIBFDB_C`] on purpose: `wyrd-metadata-fdb` links nothing by default, but naming it
/// here puts the `fdb` feature one flag away from the simulator.
const BANNED_MANIFEST_NAMES: [&str; 2] = ["wyrd-metadata-fdb", "foundationdb"];

/// This crate's directory (`<root>/crates/dst`).
fn crate_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

/// A unique temp directory, mirroring `xtask/tests/deploy_no_orchestrator_coupling.rs:72-79`.
fn temp_fixture_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wyrd-no-fdb-linkage-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create fixture dir");
    dir
}

// ─── (1) the linkage invariant: a feature-unified GRAPH property ──────────────────────

/// Which cfg the guard resolves the dependency graph under.
///
/// `cargo tree` decides whether `[target.'cfg(madsim)'.dev-dependencies]` is in the graph by
/// evaluating `cfg(madsim)` against the effective `RUSTFLAGS`. The guard **forces** the value
/// rather than inheriting the ambient one, so the linkage invariant holds identically whether
/// it runs under `cargo xtask dst` (`--cfg madsim` ambient) or under `run-verify.sh`'s bare
/// `cargo test` (no `--cfg madsim` ambient) — see the module doc, and the iteration-2
/// carry-forward this closes.
#[derive(Clone, Copy)]
enum Resolve {
    /// The simulator graph, `[target.'cfg(madsim)']` included. What the invariant scans, and
    /// where a real FDB dep would land in this manifest's house style (`Cargo.toml:55`).
    Madsim,
    /// The bare graph, `[target.'cfg(madsim)']` omitted. Used only in the planted red, to
    /// *prove* the section is cfg-gated — so [`Resolve::Madsim`] is demonstrably load-bearing.
    Bare,
}

impl Resolve {
    /// The `RUSTFLAGS` value that forces this resolution regardless of the ambient env. The
    /// only custom cfg this workspace's manifests gate a `[target.'cfg(...)']` section on is
    /// `madsim`, so a clean `--cfg madsim` (resp. empty) is the whole difference between the
    /// two graphs; `cargo tree` only resolves — it does not build — so no other ambient flag
    /// matters here.
    fn rustflags(self) -> &'static str {
        match self {
            Resolve::Madsim => "--cfg madsim",
            Resolve::Bare => "",
        }
    }
}

/// Print the feature-unified dependency graph of `package` rooted at `manifest`, resolved
/// under `resolve`'s forced cfg.
///
/// `--offline` so the guard never touches the network (every dependency is already resolved
/// — this test is running, so cargo built it). A failure to run `cargo tree` is a **loud
/// panic**, never a silent pass: a guard that cannot see the graph has not cleared it.
fn cargo_tree(
    manifest: &Path,
    package: &str,
    locked: bool,
    extra: &[&str],
    resolve: Resolve,
) -> String {
    let mut cmd = Command::new(env!("CARGO"));
    cmd.args(["tree", "--offline", "-e", "features", "--manifest-path"])
        .arg(manifest)
        .args(["-p", package])
        .args(extra);
    if locked {
        cmd.arg("--locked");
    }
    // Force the cfg the graph is resolved under. `CARGO_ENCODED_RUSTFLAGS` takes precedence
    // over `RUSTFLAGS` when set, so clear it — otherwise an ambient encoded value would
    // silently override the forced one and reintroduce the invocation asymmetry this closes.
    cmd.env("RUSTFLAGS", resolve.rustflags());
    cmd.env_remove("CARGO_ENCODED_RUSTFLAGS");
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("spawn `cargo tree` for {}: {e}", manifest.display()));
    assert!(
        out.status.success(),
        "`cargo tree` failed for {} ({}):\n{}",
        manifest.display(),
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).expect("cargo tree emitted non-UTF-8")
}

/// The set of **package** node names in a `cargo tree -e features` listing.
///
/// `cargo tree` prints a package node as `name vX.Y.Z [source]` and a feature node as
/// `name feature "f"`. Only package nodes are dependency edges on a crate, so only they are
/// collected — and because cargo prints the *resolved* package name, a `fdb = { package =
/// "foundationdb" }` rename appears here as `foundationdb`, which is the whole point.
fn package_nodes(tree: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for line in tree.lines() {
        let code = line.trim_start_matches([' ', '│', '├', '└', '─']);
        let mut fields = code.split_whitespace();
        let (Some(name), Some(version)) = (fields.next(), fields.next()) else {
            continue;
        };
        // A package node's second field is `vMAJOR…`; a feature node's is `feature`.
        if version.starts_with('v') && version[1..].starts_with(|c: char| c.is_ascii_digit()) {
            names.insert(name.to_string());
        }
    }
    names
}

/// Every `libfdb_c`-linking package in a graph listing.
fn libfdb_c_packages(tree: &str) -> Vec<String> {
    let nodes = package_nodes(tree);
    LINKS_LIBFDB_C
        .iter()
        .filter(|needle| nodes.contains(**needle))
        .map(|needle| (*needle).to_string())
        .collect()
}

#[test]
fn package_nodes_reads_package_lines_and_not_feature_lines() {
    let tree = "\
wyrd-dst v0.0.0 (/w/crates/dst)
[dev-dependencies]
├── foundationdb feature \"default\"
│   └── foundationdb v0.10.0
│       └── foundationdb-sys v0.10.0
└── async-trait v0.1.89 (proc-macro)
";
    let nodes = package_nodes(tree);
    assert!(nodes.contains("foundationdb"), "{nodes:?}");
    assert!(nodes.contains("foundationdb-sys"), "{nodes:?}");
    assert!(nodes.contains("async-trait"), "{nodes:?}");
    assert!(nodes.contains("wyrd-dst"), "{nodes:?}");
    // `[dev-dependencies]` is a section header, and `foundationdb feature "default"` is a
    // feature node — neither is a package.
    assert!(!nodes.contains("[dev-dependencies]"), "{nodes:?}");
    assert_eq!(nodes.len(), 4, "{nodes:?}");
}

/// The scanner, aimed at the **real** `foundationdb` dependency rather than a fixture: turn
/// on `wyrd-metadata-fdb`'s default-off `fdb` feature (`crates/metadata-fdb/Cargo.toml:22`)
/// and the workspace's real `foundationdb 0.10` pin (`Cargo.toml:108`) enters the graph,
/// dragging `foundationdb-sys` — the crate that declares `libfdb_c` — with it.
///
/// This is the demonstrated red the invariant below rests on: the same `libfdb_c_packages`
/// call, over a graph containing the genuine article, at the versions the workspace really
/// pins. `cargo tree` resolves; it does not build, so no `libfdb_c` need be installed.
///
/// It also pins the *reason* `crates/dst` may never name `wyrd-metadata-fdb`: the crate is
/// dependency-free by default (asserted first) and one feature flag from linking (asserted
/// second).
#[test]
fn the_graph_scanner_is_red_on_the_real_fdb_backend_with_its_feature_on() {
    let manifest = crate_dir()
        .parent()
        .expect("crates/")
        .join("metadata-fdb/Cargo.toml");

    // `foundationdb` is a plain (non-`[target.'cfg(...)']`) optional dep of `wyrd-metadata-fdb`
    // behind the `fdb` feature, so the cfg does not change what this resolves; force `Madsim`
    // for consistency with the invariant it backs.
    let default_tree = cargo_tree(&manifest, "wyrd-metadata-fdb", true, &[], Resolve::Madsim);
    assert!(
        libfdb_c_packages(&default_tree).is_empty(),
        "with `fdb` off, the backend links no libfdb_c — that is why `cargo xtask ci` is \
         green on a machine with no FoundationDB:\n{default_tree}"
    );

    let with_feature = cargo_tree(
        &manifest,
        "wyrd-metadata-fdb",
        true,
        &["--features", "fdb"],
        Resolve::Madsim,
    );
    assert_eq!(
        libfdb_c_packages(&with_feature),
        vec!["foundationdb".to_string(), "foundationdb-sys".to_string()],
        "with `fdb` on, the real dependency must be visible to the scanner; tree was:\n{with_feature}"
    );
}

#[test]
fn the_dst_dependency_graph_links_no_libfdb_c() {
    // The invariant itself, read off the graph cargo actually builds from — feature
    // unification, renames and transitive edges included. Resolved under a FORCED `--cfg
    // madsim` so `[target.'cfg(madsim)'.dev-dependencies]` (`Cargo.toml:55`, the section a real
    // FDB dep would enter in house style) is scanned even under `run-verify.sh`'s bare
    // `cargo test`, where the ambient cfg is off and that section would otherwise be omitted.
    let tree = cargo_tree(
        &crate_dir().join("Cargo.toml"),
        "wyrd-dst",
        true,
        &[],
        Resolve::Madsim,
    );
    let linked = libfdb_c_packages(&tree);
    assert!(
        linked.is_empty(),
        "wyrd-dst's feature-unified dependency graph contains {linked:?} — `libfdb_c` spawns \
         its own network thread and would break the simulator's seed determinism \
         (ADR-0009/ADR-0035). The FDB backend is MODELLED in tests/support/mod.rs, never linked."
    );
}

#[test]
fn the_graph_scanner_is_red_on_a_cfg_madsim_gated_rename_and_transitive_edge() {
    // Plant the FDB dependency where the real risk lives — the
    // `[target.'cfg(madsim)'.dev-dependencies]` section (`crates/dst/Cargo.toml:55`), in the
    // shapes a manifest-text scan is blind to — and prove the SAME scanner the invariant runs
    // catches it: a demonstrated red, not a guard resting green on non-existence
    // (`xtask/tests/deploy_no_orchestrator_coupling.rs:67-99`).
    //
    //   * `fdb = { package = "foundationdb", … }` — the RENAME form, which this very
    //     manifest already uses for `tonic` / `etcd-client` / `tokio`, under the cfg(madsim)
    //     section it uses them in.
    //   * `plausible-helper` -> `foundationdb` — a TRANSITIVE edge, named nowhere in the
    //     scanned manifest at all.
    //
    // Two resolutions, because the load-bearing behaviour is that forcing `--cfg madsim`
    // surfaces a cfg(madsim)-gated node the ambient bare invocation would omit (the iteration-2
    // carry-forward): caught under `Resolve::Madsim`, invisible under `Resolve::Bare`.
    //
    // The fixture's `foundationdb` is a local path crate, so the whole graph resolves offline
    // with no registry access; what is under test is the scanner, not cargo.
    let dir = temp_fixture_dir("planted-graph");
    let write = |rel: &str, body: &str| {
        let path = dir.join(rel);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("create fixture dir");
        std::fs::write(&path, body).expect("write fixture file");
    };

    write(
        "foundationdb/Cargo.toml",
        "[package]\nname = \"foundationdb\"\nversion = \"0.10.0\"\nedition = \"2021\"\n\
         [workspace]\n[dependencies]\nfoundationdb-sys = { path = \"../foundationdb-sys\" }\n",
    );
    write("foundationdb/src/lib.rs", "");
    write(
        "foundationdb-sys/Cargo.toml",
        "[package]\nname = \"foundationdb-sys\"\nversion = \"0.10.0\"\nedition = \"2021\"\n\
         [workspace]\n",
    );
    write("foundationdb-sys/src/lib.rs", "");
    write(
        "plausible-helper/Cargo.toml",
        "[package]\nname = \"plausible-helper\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\
         [workspace]\n[dependencies]\nfoundationdb = { path = \"../foundationdb\" }\n",
    );
    write("plausible-helper/src/lib.rs", "");
    write(
        "dst/Cargo.toml",
        "[package]\nname = \"wyrd-dst\"\nversion = \"0.0.0\"\nedition = \"2021\"\n[workspace]\n\
         [target.'cfg(madsim)'.dev-dependencies]\n\
         fdb = { package = \"foundationdb\", path = \"../foundationdb\" }\n\
         plausible-helper = { path = \"../plausible-helper\" }\n",
    );
    write("dst/src/lib.rs", "");

    let manifest = dir.join("dst/Cargo.toml");
    let madsim_tree = cargo_tree(&manifest, "wyrd-dst", false, &[], Resolve::Madsim);
    let caught = libfdb_c_packages(&madsim_tree);
    let bare_tree = cargo_tree(&manifest, "wyrd-dst", false, &[], Resolve::Bare);
    let missed = libfdb_c_packages(&bare_tree);
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        caught,
        vec!["foundationdb".to_string(), "foundationdb-sys".to_string()],
        "under a forced `--cfg madsim` the graph scanner must catch the cfg(madsim)-gated \
         rename form AND the transitive edge; tree was:\n{madsim_tree}"
    );
    assert!(
        missed.is_empty(),
        "control: without `--cfg madsim` the `[target.'cfg(madsim)']` section is omitted, so \
         the FDB node is invisible — this is exactly why the invariant forces the cfg rather \
         than inheriting the ambient one (iteration-2 carry-forward); bare tree was:\n{bare_tree}"
    );
}

// ─── (2) the policy invariant: the manifest names no FDB dependency ───────────────────

/// The banned dependency name a single manifest line declares, if any.
///
/// Four shapes, because Cargo has four and this manifest already uses three of them:
///
/// * `foundationdb = { version = "0.10" }`
/// * `wyrd-metadata-fdb.workspace = true`
/// * `[dependencies.foundationdb]` / `[dependencies."foundationdb"]`
/// * `fdb = { package = "foundationdb", … }` — the **rename**, whose real name only appears
///   as the value of a `package` key (`crates/dst/Cargo.toml:56,66,68` for `tonic`,
///   `etcd-client`, `tokio`).
///
/// A comment mentioning FoundationDB — as this crate's own docs do — is not a dependency, so
/// everything from a `#` on is stripped first.
fn scan_line(line: &str) -> Option<&'static str> {
    let code = line.split('#').next().unwrap_or("").trim();
    if code.is_empty() {
        return None;
    }

    // The rename form, anywhere on the line: `package = "<real-name>"`.
    if let Some(renamed) = renamed_package(code) {
        return Some(renamed);
    }

    // `[dependencies.foundationdb]` / `[target.'cfg(x)'.dev-dependencies."foundationdb"]`
    if let Some(section) = code.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        let last = section.rsplit('.').next().unwrap_or("").trim_matches('"');
        return BANNED_MANIFEST_NAMES.iter().copied().find(|n| last == *n);
    }

    // `name = …` / `name.workspace = true` — the key is the text before the first `=`, `.`
    // or whitespace.
    let key = code
        .split(['=', '.', ' ', '\t'])
        .next()
        .unwrap_or("")
        .trim_matches('"');
    BANNED_MANIFEST_NAMES.iter().copied().find(|n| key == *n)
}

/// The banned name a `package = "…"` rename key names, if any. Handles both the inline-table
/// form and the `[dependencies.fdb]` + `package = "foundationdb"` two-line form.
fn renamed_package(code: &str) -> Option<&'static str> {
    let mut rest = code;
    while let Some(at) = rest.find("package") {
        let before_is_boundary = rest[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !c.is_alphanumeric() && c != '_' && c != '-');
        let after = rest[at + "package".len()..].trim_start();
        if before_is_boundary {
            if let Some(value) = after.strip_prefix('=') {
                let value = value.trim_start();
                if let Some(quoted) = value.strip_prefix('"') {
                    let name = quoted.split('"').next().unwrap_or("");
                    if let Some(hit) = BANNED_MANIFEST_NAMES.iter().copied().find(|n| name == *n) {
                        return Some(hit);
                    }
                }
            }
        }
        rest = &rest[at + "package".len()..];
    }
    None
}

/// Every banned FDB dependency a Cargo manifest names. **One scanner, two call sites**: the
/// real `crates/dst/Cargo.toml` (which must be empty) and a planted temp fixture (which must
/// not be).
fn scan_manifest_at(manifest: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(manifest)
        .unwrap_or_else(|e| panic!("read {}: {e}", manifest.display()));
    text.lines()
        .filter_map(scan_line)
        .map(str::to_string)
        .collect()
}

#[test]
fn scan_line_catches_every_manifest_dependency_shape() {
    assert_eq!(
        scan_line("foundationdb = { version = \"0.10\" }"),
        Some("foundationdb")
    );
    assert_eq!(
        scan_line("wyrd-metadata-fdb.workspace = true"),
        Some("wyrd-metadata-fdb")
    );
    assert_eq!(
        scan_line("[target.'cfg(madsim)'.dev-dependencies.foundationdb]"),
        Some("foundationdb")
    );
    assert_eq!(
        scan_line("[dependencies.\"foundationdb\"]"),
        Some("foundationdb")
    );
    // The RENAME form — the one this manifest's own `tonic`/`etcd-client`/`tokio` lines use,
    // and the one a key-name scan is blind to.
    assert_eq!(
        scan_line(
            "fdb = { package = \"foundationdb\", version = \"0.10\", features = [\"fdb-7_3\"] }"
        ),
        Some("foundationdb")
    );
    assert_eq!(
        scan_line("package = \"wyrd-metadata-fdb\""),
        Some("wyrd-metadata-fdb")
    );
}

#[test]
fn scan_line_ignores_comments_and_unrelated_dependencies() {
    // This crate's own manifest and docs discuss FoundationDB in prose; prose is not a
    // dependency edge. Only a real declaration is a violation.
    assert!(scan_line("# never depend on foundationdb here (ADR-0035)").is_none());
    assert!(scan_line("wyrd-metadata-redb.workspace = true # not foundationdb").is_none());
    assert!(scan_line("[dev-dependencies]").is_none());
    // A crate whose name merely *contains* a needle is not the needle.
    assert!(scan_line("foundationdb-sys-shim.workspace = true").is_none());
    // The manifest's real rename lines must stay green.
    assert!(scan_line("tonic = { package = \"madsim-tonic\", version = \"0.6.0\" }").is_none());
    assert!(
        scan_line("etcd-client = { package = \"madsim-etcd-client\", version = \"0.6.0\" }")
            .is_none()
    );
    // A key whose name merely ends in `package`.
    assert!(scan_line("sub_package = \"foundationdb\"").is_none());
}

#[test]
fn scan_manifest_is_red_when_an_fdb_dependency_is_planted() {
    // Plant the FDB dependency in the shapes that actually occur, at the versions the
    // workspace really pins (`Cargo.toml:108`: `foundationdb = { version = "0.10",
    // default-features = false, features = ["fdb-7_3"] }`) — a fixture derived from the real
    // dependency, not an invented one.
    let dir = temp_fixture_dir("planted-manifest");
    let manifest = dir.join("Cargo.toml");
    std::fs::write(
        &manifest,
        "[package]\nname = \"wyrd-dst\"\n\n[dev-dependencies]\n\
         wyrd-metadata-redb.workspace = true\nwyrd-metadata-fdb.workspace = true\n\
         fdb = { package = \"foundationdb\", version = \"0.10\", features = [\"fdb-7_3\"] }\n\n\
         [dev-dependencies.\"foundationdb\"]\nworkspace = true\n",
    )
    .expect("write fixture manifest");

    let violations = scan_manifest_at(&manifest);
    std::fs::remove_dir_all(&dir).ok();

    assert_eq!(
        violations,
        vec![
            "wyrd-metadata-fdb".to_string(),
            "foundationdb".to_string(),
            "foundationdb".to_string(),
        ],
        "planting the FDB dependencies — plain, renamed and quoted-section — must be caught"
    );
}

#[test]
fn the_dst_manifest_declares_no_fdb_dependency() {
    let manifest = crate_dir().join("Cargo.toml");
    let violations = scan_manifest_at(&manifest);
    assert!(
        violations.is_empty(),
        "{} names FDB dependenc(ies) {violations:?}. Even `wyrd-metadata-fdb`, which links \
         nothing by default, is banned here: naming it puts `libfdb_c` one feature flag away \
         from the simulator (ADR-0009/ADR-0035).",
        manifest.display()
    );
}

// ─── (3) the model this slice ships actually exists ───────────────────────────────────

#[test]
fn the_dst_support_module_declares_the_simulated_fdb_store() {
    // The seam the purity guard protects is only worth protecting if the model exists: the
    // point of #468 is that the FFI backend gets an in-simulator story, not merely that it is
    // kept out of the simulator. Scanned as text because `support/mod.rs` is a madsim-only
    // module this (non-madsim) test binary cannot link.
    //
    // Each needle names a *load-bearing* part of the model, so an empty struct would not
    // satisfy this: the store, its `MetadataStore` impl, the ambiguity nemesis, and both
    // members of the undeterminable class.
    //
    // The model raises the SEAM error (`wyrd_traits::CommitUnknownResult`), not a private
    // look-alike of its own (#515). That is what makes the DST exercise the same caller
    // obligation the real backends impose: a scenario — or any generic helper — downcasts
    // to the one type production raises. It used to define its own `SimCommitUnknownResult`
    // with a `may_still_commit` method, so a seam-level downcast saw a simulated unknown
    // commit as a plain fault. The needles below pin the replacement, including that the
    // 1021-vs-1031 distinction is still DERIVED from the code rather than flattened away —
    // an `Err` that always said `may_still_commit: true` would model only half the class.
    let support = crate_dir().join("tests/support/mod.rs");
    let text = std::fs::read_to_string(&support)
        .unwrap_or_else(|e| panic!("read {}: {e}", support.display()));
    for needle in [
        "pub struct SimFdbMetadataStore",
        "impl MetadataStore for SimFdbMetadataStore",
        "pub fn arm_commit_ambiguity",
        "pub const SIM_COMMIT_UNKNOWN_RESULT: i32 = 1021",
        "pub const SIM_TRANSACTION_TIMED_OUT: i32 = 1031",
        "pub fn sim_commit_unknown_result(code: i32) -> CommitUnknownResult",
        "may_still_commit: code == SIM_TRANSACTION_TIMED_OUT",
    ] {
        assert!(
            text.contains(needle),
            "{} must declare `{needle}` — the simulated-FDB model whose commit-ambiguity \
             property `crates/dst/tests/commit_ambiguity.rs` drives under `--cfg madsim` \
             (issue #468)",
            support.display()
        );
    }
}
