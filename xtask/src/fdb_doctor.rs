//! FoundationDB environment doctor (issue #439): map each environment probe's
//! outcome to a **verdict + an actionable remediation**, so a developer or a CI job
//! with no `libfdb_c`, no cluster file, or a dead container gets a sentence telling
//! them what to install or run — never a raw linker error or a transaction timeout.
//!
//! Exposed as a **library** module (the same seam `deploy_guard` uses, `lib.rs:1-19`)
//! and kept **environment-free**: the probes' *results* are inputs, so every row, every
//! remediation string, and the conformance job's whole preflight decision are
//! unit-testable on a machine with no FoundationDB, no Docker and no `fdbcli`
//! (`xtask/tests/fdb_harness.rs`). `main.rs` owns the impure half — stat the library
//! paths, read the cluster file, spawn `fdbcli`, drive `docker compose` — and hands the
//! outcomes here.
//!
//! That split is what keeps the binding criterion container-free while still driving the
//! production decision logic. Two call sites share this module:
//!
//! * `run_fdb_doctor` (the `cargo xtask fdb-doctor` arm) renders [`diagnose`]'s [`Report`];
//! * `run_fdb_conformance` **is** a call to [`run_gated_conformance`] — its whole body is
//!   that one call, so the preflight cannot be bypassed without deleting the command.
//!   The stack (compose up → configure → test → down) is injected as a closure, exactly
//!   as `run_ci_steps` injects its `cargo` executor, so `fdb_harness.rs` drives the real
//!   gate and asserts the stack is **never entered** on a failed preflight.
//!
//! The three rows are the ones the issue names:
//!
//! 1. **client library** — is `libfdb_c` present on a path the linker will search? The
//!    `fdb` feature links it at *build* time (`crates/metadata-fdb/Cargo.toml`), so its
//!    absence is a linker error deep inside `cargo test`, not a runtime message.
//! 2. **cluster file** — is the file named by `WYRD_FDB_CLUSTER_FILE`
//!    (`crates/metadata-fdb/src/lib.rs:386`), or its `/etc/foundationdb/fdb.cluster`
//!    default (`:390`), readable and non-empty?
//! 3. **cluster health** — does `fdbcli --exec "status minimal"` report the database
//!    available? A cluster that is up but *unconfigured* reports `configuration
//!    missing` and every transaction blocks until its deadline.

use std::fmt::Write as _;

/// The FoundationDB version this repo is pinned to, in exactly one place per artifact:
/// the compose image (`deploy/fdb-single-node/docker-compose.yml:22`) and the crate's
/// `fdb-7_3` API feature (root `Cargo.toml:108`). A client library from a different
/// major/minor never connects, so the remediation names the version, not just the package.
pub const FDB_VERSION: &str = "7.3.77";

/// The server image the throwaway single-node cluster runs
/// (`deploy/fdb-single-node/docker-compose.yml:22`).
pub const FDB_IMAGE: &str = "foundationdb/foundationdb:7.3.77";

/// The upstream package that provides both [`CLIENT_LIBRARY_SONAME`] and `fdbcli`.
pub const CLIENT_PACKAGE: &str = "foundationdb-clients";

/// The shared object the `foundationdb` crate's `fdb` feature links at build time.
pub const CLIENT_LIBRARY_SONAME: &str = "libfdb_c.so";

/// The environment variable **`foundationdb-sys` itself** reads to locate the client
/// library: its `build.rs` emits `cargo:rustc-link-search=native=$FDB_CLIENT_LIB_PATH`
/// when it is set (`foundationdb-sys-0.10.0/build.rs:61-64`). A client installed under a
/// custom prefix with this variable set links **fine**, so the doctor must search it
/// first — otherwise a working build is reported as a missing client library, and
/// `run_fdb_conformance` skips (locally) or hard-fails (in CI) a job that would have
/// passed.
pub const CLIENT_LIB_PATH_ENV: &str = "FDB_CLIENT_LIB_PATH";

/// Where the upstream packages install [`CLIENT_LIBRARY_SONAME`]. Searched after
/// [`CLIENT_LIB_PATH_ENV`]; the dynamic-linker cache (`ldconfig -p`, parsed by
/// [`ldconfig_lists_client_library`]) is the last fallback.
pub const CLIENT_LIBRARY_CANDIDATES: &[&str] = &[
    "/usr/lib/libfdb_c.so",
    "/usr/lib64/libfdb_c.so",
    "/usr/lib/x86_64-linux-gnu/libfdb_c.so",
    "/usr/local/lib/libfdb_c.so",
    "/lib/libfdb_c.so",
];

/// The environment variable naming the cluster file. Mirrors
/// `wyrd_metadata_fdb::config::CLUSTER_FILE_ENV` (`crates/metadata-fdb/src/lib.rs:386`)
/// — duplicated as a literal rather than imported, because `xtask` must not depend on
/// `wyrd-metadata-fdb` (its `fdb` feature would drag `libfdb_c` into the gate's build
/// graph, and a new Cargo dependency is an ADR-0003 §2 decision). The duplication is
/// pinned by a drift assertion in `xtask/tests/fdb_harness.rs`.
pub const CLUSTER_FILE_ENV: &str = "WYRD_FDB_CLUSTER_FILE";

/// Where FoundationDB's own packages install the cluster file; used when
/// [`CLUSTER_FILE_ENV`] is unset or blank. Mirrors
/// `wyrd_metadata_fdb::config::DEFAULT_CLUSTER_FILE` (`crates/metadata-fdb/src/lib.rs:390`).
pub const DEFAULT_CLUSTER_FILE: &str = "/etc/foundationdb/fdb.cluster";

/// The throwaway single-node compose stack (#438), relative to the workspace root.
pub const COMPOSE_FILE: &str = "deploy/fdb-single-node/docker-compose.yml";

/// The one command that brings [`COMPOSE_FILE`]'s cluster up.
pub const CLUSTER_BRINGUP_COMMAND: &str =
    "docker compose -f deploy/fdb-single-node/docker-compose.yml up -d";

/// The health probe itself, quoted back to the operator so they can re-run it by hand.
/// Names the `-C <cluster file>` argument the real probe passes
/// (`xtask/src/main.rs`'s `probe_cluster_health` runs
/// `fdbcli -C <cluster file> --exec "status minimal"`), so the advisory command an
/// operator copies matches the one the doctor actually ran.
pub const HEALTH_COMMAND: &str = "fdbcli -C <cluster file> --exec \"status minimal\"";

/// The one blocker that is **not** a doctor row: `cargo xtask fdb-conformance` needs a
/// container runtime to stand the throwaway cluster up. The doctor reports on
/// FoundationDB, not on Docker (a developer who has `libfdb_c` and a remote cluster
/// needs no Docker at all), so this is a preflight blocker rather than a [`Probe`].
/// Text preserved from the pre-#439 check at `xtask/src/main.rs:296-301`.
pub const DOCKER_BLOCKER: &str = "docker is not available but is required for the \
     FoundationDB conformance job\n    fix: install Docker and the compose plugin \
     (https://docs.docker.com/engine/install/), then re-run `cargo xtask fdb-conformance`.";

/// What `status minimal` prints when the database is usable. Deliberately includes
/// `database is` so it cannot match `The database is unavailable` (which *contains* the
/// substring "available").
const HEALTHY_STATUS_NEEDLE: &str = "database is available";

/// The three environment probes the doctor reports on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    /// `libfdb_c` present on a path the linker will search.
    ClientLibrary,
    /// The cluster file readable and non-empty.
    ClusterFile,
    /// `fdbcli --exec "status minimal"` reports the database available.
    ClusterHealth,
}

impl Probe {
    /// The row's short name, as printed.
    pub fn label(self) -> &'static str {
        match self {
            Probe::ClientLibrary => "client library (libfdb_c)",
            Probe::ClusterFile => "cluster file",
            Probe::ClusterHealth => "cluster health",
        }
    }

    /// What to do when this probe fails. This is the whole point of the module: each
    /// string names the concrete package, variable, or command that fixes the row.
    pub fn remediation(self) -> String {
        match self {
            Probe::ClientLibrary => format!(
                "install the FoundationDB client package `{CLIENT_PACKAGE}` at version \
                 {FDB_VERSION} (matching the pinned server image `{FDB_IMAGE}`): it provides \
                 `{CLIENT_LIBRARY_SONAME}`, which the `fdb` feature links at BUILD time. \
                 A client under a custom prefix is found via `{CLIENT_LIB_PATH_ENV}`, the \
                 same variable `foundationdb-sys`' build script reads. \
                 Packages: https://github.com/apple/foundationdb/releases/tag/{FDB_VERSION}"
            ),
            Probe::ClusterFile => format!(
                "point `{CLUSTER_FILE_ENV}` at a readable cluster file, or install one at the \
                 `{DEFAULT_CLUSTER_FILE}` default. `cargo xtask fdb-conformance` writes a \
                 matching one under `target/fdb-single-node/fdb.cluster` once the cluster is up."
            ),
            Probe::ClusterHealth => format!(
                "bring the throwaway single-node cluster up with \
                 `{CLUSTER_BRINGUP_COMMAND}`, then re-check with `{HEALTH_COMMAND}`. A cluster \
                 that is running but never had `configure new single memory` applied reports \
                 `configuration missing` and blocks every transaction. (`fdbcli` ships in the \
                 `{CLIENT_PACKAGE}` package.)"
            ),
        }
    }
}

/// One probe's measured result. The *measurement* happens in `main.rs` (it stats files
/// and spawns `fdbcli`); this type is the pure boundary between that and the verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The probe passed; the string is the evidence (e.g. the path the library was found at).
    Ok(String),
    /// The probe failed; the string is what was observed (e.g. the `fdbcli` error).
    Failed(String),
}

impl Outcome {
    /// A passing outcome carrying its evidence.
    pub fn ok(detail: impl Into<String>) -> Self {
        Outcome::Ok(detail.into())
    }

    /// A failing outcome carrying what was observed.
    pub fn failed(detail: impl Into<String>) -> Self {
        Outcome::Failed(detail.into())
    }

    /// Did the probe pass?
    pub fn passed(&self) -> bool {
        matches!(self, Outcome::Ok(_))
    }

    /// The observed evidence, pass or fail.
    pub fn detail(&self) -> &str {
        match self {
            Outcome::Ok(detail) | Outcome::Failed(detail) => detail,
        }
    }
}

/// One diagnosed row: a probe, what was observed, and — when it failed — how to fix it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    /// Which environment property was probed.
    pub probe: Probe,
    /// What the probe observed.
    pub outcome: Outcome,
}

impl Row {
    /// Did this row pass?
    pub fn passed(&self) -> bool {
        self.outcome.passed()
    }

    /// The remediation text for a failing row (`None` when it passed — nothing to fix).
    pub fn remediation(&self) -> Option<String> {
        if self.passed() {
            None
        } else {
            Some(self.probe.remediation())
        }
    }
}

/// The doctor's verdict over a set of probes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// One row per probe, in the order they were supplied.
    pub rows: Vec<Row>,
}

impl Report {
    /// Is the environment ready? Vacuously true for an empty probe set — callers always
    /// supply the rows they mean to gate on ([`run_gated_conformance`]'s preflight supplies
    /// only [`Probe::ClientLibrary`], because the cluster file and the cluster itself are
    /// what the job goes on to create).
    pub fn is_ok(&self) -> bool {
        self.rows.iter().all(Row::passed)
    }

    /// The failing rows, in probe order.
    pub fn failures(&self) -> Vec<&Row> {
        self.rows.iter().filter(|row| !row.passed()).collect()
    }

    /// The full report, one row per line, with a `fix:` line under each failure.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for row in &self.rows {
            let mark = if row.passed() { "ok  " } else { "FAIL" };
            let _ = writeln!(
                out,
                "[{mark}] {}: {}",
                row.probe.label(),
                row.outcome.detail()
            );
            if let Some(fix) = row.remediation() {
                let _ = writeln!(out, "       fix: {fix}");
            }
        }
        out
    }

    /// `Ok(())` when every row passed; otherwise an error naming each failing row and its
    /// remediation — the message [`run_gated_conformance`] fails fast with, in place of a
    /// raw linker error.
    pub fn into_result(self) -> Result<(), String> {
        if self.is_ok() {
            return Ok(());
        }
        let mut out = String::from("the FoundationDB environment is not ready:\n");
        for row in self.failures() {
            let _ = writeln!(out, "  - {}: {}", row.probe.label(), row.outcome.detail());
            if let Some(fix) = row.remediation() {
                let _ = writeln!(out, "    fix: {fix}");
            }
        }
        Err(out.trim_end().to_string())
    }
}

/// Turn measured probe outcomes into a report. Pure: no IO, no environment reads.
pub fn diagnose(probes: Vec<(Probe, Outcome)>) -> Report {
    Report {
        rows: probes
            .into_iter()
            .map(|(probe, outcome)| Row { probe, outcome })
            .collect(),
    }
}

/// What the `cargo xtask fdb-conformance` preflight decided.
///
/// The CI-vs-local split is the repo's standing convention for a privileged job's
/// prerequisites (`xtask/src/main.rs`'s `docker_available` / `is_ci` pair): a missing
/// prerequisite is a **hard failure** on a runner that promised to have it, and a
/// **warn-and-skip** on a laptop, so `cargo xtask ci` is never blocked by an absent
/// FoundationDB.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Preflight {
    /// The environment is ready — run the stack.
    Proceed,
    /// Not ready, and this is CI: fail with the report + remediation.
    Fail(String),
    /// Not ready, and this is a developer machine: warn and skip.
    SkipLocally(String),
}

/// Decide whether `cargo xtask fdb-conformance` may enter its container stack.
///
/// Pure — `docker_available`, `client_library` and `is_ci` are *measured* by `main.rs`
/// and passed in, so every branch is unit-tested with no Docker and no `libfdb_c`.
///
/// Only the client-library row is probed: the cluster file and the cluster itself are
/// what this job goes on to **create** (`write_fdb_cluster_file`, `configure_fdb_database`),
/// so probing them here would report a red the job is about to fix. The client library,
/// by contrast, is linked at *build* time — without it `cargo test --features fdb` dies in
/// a linker error minutes in, after a container is already up.
pub fn preflight(docker_available: bool, client_library: &Outcome, is_ci: bool) -> Preflight {
    let blocker = if !docker_available {
        Some(DOCKER_BLOCKER.to_string())
    } else {
        diagnose(vec![(Probe::ClientLibrary, client_library.clone())])
            .into_result()
            .err()
    };

    match blocker {
        None => Preflight::Proceed,
        Some(report) if is_ci => Preflight::Fail(report),
        Some(report) => Preflight::SkipLocally(format!(
            "warning: skipping the FoundationDB conformance job locally.\n{report}\n\
             Run `cargo xtask fdb-doctor` for the full environment report."
        )),
    }
}

/// The production body of `cargo xtask fdb-conformance`: gate on [`preflight`], and only
/// then enter `stack` — the compose bring-up, `configure new single memory`, the five
/// `--features fdb` test legs, and the unconditional teardown.
///
/// `stack` is **injected** (the `run_ci_steps` executor pattern), and the environment
/// facts are parameters, so `xtask/tests/fdb_harness.rs` drives this exact function on a
/// plain worktree and asserts the stack is never entered on a failed preflight. That is
/// the whole coverage claim for the preflight, and it is the whole of `run_fdb_conformance`:
/// deleting the gate means deleting this function, not a block inside `main.rs`.
pub fn run_gated_conformance(
    docker_available: bool,
    is_ci: bool,
    client_library: Outcome,
    stack: &mut dyn FnMut() -> Result<(), String>,
) -> Result<(), String> {
    match preflight(docker_available, &client_library, is_ci) {
        Preflight::Fail(report) => Err(report),
        Preflight::SkipLocally(warning) => {
            eprintln!("{warning}");
            Ok(())
        }
        Preflight::Proceed => stack(),
    }
}

/// Resolve the cluster-file path from the environment value, falling back to
/// [`DEFAULT_CLUSTER_FILE`] when it is unset **or blank**. Mirrors the driver's own
/// resolution (`crates/metadata-fdb/src/lib.rs:384-390`) so the doctor reports on the
/// same file the driver would open.
pub fn cluster_file_path(env_value: Option<&str>) -> String {
    match env_value {
        Some(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => DEFAULT_CLUSTER_FILE.to_string(),
    }
}

/// Every path the client library may legitimately live at, in probe order: the directory
/// named by [`CLIENT_LIB_PATH_ENV`] first (because a build with that variable set links
/// against **that** copy, wherever it is), then the packages' standard prefixes.
///
/// Pure, so the `FDB_CLIENT_LIB_PATH` contract is unit-tested without touching the
/// process environment.
pub fn client_library_search_paths(client_lib_path: Option<&str>) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(dir) = client_lib_path {
        let dir = dir.trim();
        if !dir.is_empty() {
            // `/opt/fdb/lib/` and `/opt/fdb/lib` name the same directory; `/` must not
            // become the empty string.
            let dir = dir.trim_end_matches('/');
            paths.push(format!("{dir}/{CLIENT_LIBRARY_SONAME}"));
        }
    }
    paths.extend(CLIENT_LIBRARY_CANDIDATES.iter().map(|p| (*p).to_string()));
    paths
}

/// Does `ldconfig -p` output list the FDB client library? The last-resort probe for a
/// client installed outside [`client_library_search_paths`] but registered with the
/// dynamic linker.
pub fn ldconfig_lists_client_library(ldconfig_output: &str) -> bool {
    ldconfig_output
        .lines()
        .any(|line| line.contains(CLIENT_LIBRARY_SONAME))
}

/// Decide the client-library [`Outcome`] from **injected** effects: an environment
/// lookup, a path-existence check, and the `ldconfig -p` listing (lazily produced).
///
/// This is the whole client-library probe *decision* — including the read of
/// [`CLIENT_LIB_PATH_ENV`] a working custom-prefix build depends on. `main.rs`'s
/// `probe_client_library` is now only the wiring that supplies the real `std::env::var`,
/// `Path::exists`, and `ldconfig` effects; the branching logic lives here so
/// `xtask/tests/fdb_harness.rs` drives it on a plain worktree with no `libfdb_c`. Dropping
/// the env read (the iteration-1 false-negative: a `FDB_CLIENT_LIB_PATH` build reported as
/// missing) flips a Check test red, because a fake environment that sets the variable and
/// a fake filesystem in which *only* that path exists would no longer resolve.
///
/// Search order matches how a build resolves the library (`foundationdb-sys`'
/// `build.rs:61-64` turns `FDB_CLIENT_LIB_PATH` into a `rustc-link-search`): the
/// configured directory first, then the packages' standard prefixes
/// ([`client_library_search_paths`]), then the dynamic-linker cache.
pub fn probe_client_library(
    env: &dyn Fn(&str) -> Option<String>,
    path_exists: &dyn Fn(&str) -> bool,
    ldconfig: &dyn Fn() -> Option<String>,
) -> Outcome {
    let configured = env(CLIENT_LIB_PATH_ENV);
    let candidates = client_library_search_paths(configured.as_deref());
    for candidate in &candidates {
        if path_exists(candidate) {
            return Outcome::ok(format!("found at {candidate}"));
        }
    }
    if let Some(listing) = ldconfig() {
        if ldconfig_lists_client_library(&listing) {
            return Outcome::ok("found in the dynamic linker cache (ldconfig -p)");
        }
    }
    Outcome::failed(format!(
        "no {} at any of {} (set {} to the directory holding it), nor in the dynamic \
         linker cache",
        CLIENT_LIBRARY_SONAME,
        candidates.join(", "),
        CLIENT_LIB_PATH_ENV,
    ))
}

/// Does `fdbcli --exec "status minimal"` output say the database is available?
///
/// Note the needle: `The database is unavailable` contains the word "available", so a
/// naive `contains("available")` would report a dead cluster healthy. It does not
/// contain `database is available`. Matching on **text** rather than exit status is
/// deliberate: `fdbcli` 7.3.77 exits 0 against a dead coordinator.
pub fn cluster_status_is_healthy(status_output: &str) -> bool {
    status_output.to_lowercase().contains(HEALTHY_STATUS_NEEDLE)
}

/// The **one impure client-library adapter** — the real-effect wiring behind
/// [`probe_client_library`]. It supplies the three real effects and nothing else: the
/// `std::env::var` read of [`CLIENT_LIB_PATH_ENV`], a `std::path::Path::exists` check per
/// candidate, and the `ldconfig -p` listing. A total, logic-free pass-through to the pure
/// decision above.
///
/// It lives **in the lib**, not as a private `main.rs` closure, so
/// `xtask/tests/fdb_harness.rs` can drive it end-to-end against a real
/// `FDB_CLIENT_LIB_PATH` and a real temp file. That is what closes the defect class the
/// re-plan (iteration 4/5) names: the two effect-discarding mutations — dropping the env
/// read (`&|name| { let _ = std::env::var(name); None }`) or hard-coding existence
/// (`&|candidate| { let _ = candidate; true }`) — change the *observed resolved path*, so
/// a behavioural test flips RED. A substring assertion over the adapter body cannot: both
/// mutations keep the literal `std::env::var` / `Path::exists` tokens.
///
/// `main.rs`'s `run_fdb_conformance` and `run_fdb_doctor` call this; the other two probes
/// (cluster file, cluster health) stay impure in `main.rs` — they are exercised live only
/// by the nightly CI job, and are not the reintroducible-false-negative class.
pub fn probe_client_library_live() -> Outcome {
    probe_client_library(
        &|name| std::env::var(name).ok(),
        &|candidate| std::path::Path::new(candidate).exists(),
        &|| {
            std::process::Command::new("ldconfig")
                .arg("-p")
                .output()
                .ok()
                .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        },
    )
}
