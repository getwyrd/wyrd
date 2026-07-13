//! The FoundationDB dev/CI harness (issue #439, ADR-0042).
//!
//! The `fdb` backend is the *chosen production metadata backend*, yet before this slice
//! nothing invoked `cargo xtask fdb-conformance` (`xtask/src/main.rs`), no CI job ran it,
//! the merge gate never compiled the `fdb` feature, and a developer with no `libfdb_c`
//! got a raw linker error. This test is the Check-time evidence for the three artifacts
//! that close those gaps — and it runs on a **plain worktree**: no Docker, no `libfdb_c`,
//! default toolchain. That is deliberate. The whole point of the slice is that the merge
//! gate stays container-free (ADR-0016), so its regression test must be too.
//!
//! What is asserted, mirroring the two peer patterns the brief names:
//!
//! 1. **`xtask::fdb_doctor` is a pure, non-privileged module** mapping each probe outcome
//!    to a verdict + a remediation that names the concrete package / variable / command.
//! 2. **Demonstrated REDs**, not greens resting on non-existence
//!    (`xtask/tests/deploy_no_orchestrator_coupling.rs:67`'s planted-import pattern):
//!    a planted failing probe outcome flips the doctor's verdict; a planted comment-only
//!    workflow yields no executed commands; a planted ungated conformance body is caught.
//! 3. **The preflight call site** — `run_gated_conformance` *is* the body of
//!    `run_fdb_conformance`, and every branch of it is driven here with the container
//!    stack injected as a recording closure, so "the preflight never runs" is a red.
//! 4. **The workflow ↔ dispatch contract** — every `cargo xtask <sub>` the workflow
//!    **executes** (scraped from its `run:` scripts, never from prose) is a subcommand
//!    `xtask/src/main.rs` really dispatches; its pull-request path filter covers the
//!    backend *and* the server; and the `--features fdb` type-checks it runs on the PR
//!    leg are `xtask::feature_gated_checks(false, true)` verbatim.
//! 5. **`xtask::feature_gated_checks(tikv, fdb)`** yields the two `--features fdb`
//!    type-checks when `fdb` is on and not when it is off, *independently of `tikv`* —
//!    the coupling hazard that forced the two explicit parameters.
//!
//! The doctor's impure half is split by whether it can be driven headlessly. The
//! client-library adapter, `fdb_doctor::probe_client_library_live`, lives in the lib and is
//! driven END-TO-END here against a real `FDB_CLIENT_LIB_PATH` and a real temp file
//! (`the_live_client_library_adapter_reads_the_real_env_and_filesystem`), so the two
//! effect-discarding mutations the re-plan names flip Check red. The cluster-file and
//! cluster-health probes (which read `/etc/foundationdb` and spawn `fdbcli`) stay in
//! `xtask/src/main.rs` and are exercised for real by the nightly
//! `.github/workflows/fdb-conformance.yml` job. What is asserted here is the decision logic
//! those probes feed — the same functions production calls.

use std::path::{Path, PathBuf};

use xtask::fdb_doctor::{
    self, client_library_search_paths, cluster_file_path, cluster_status_is_healthy, diagnose,
    ldconfig_lists_client_library, probe_client_library, probe_client_library_live,
    run_gated_conformance, Outcome, Probe,
};

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The workflow file, relative to the workspace root.
const WORKFLOW: &str = ".github/workflows/fdb-conformance.yml";

/// Every probe passing — the environment a green conformance run needs.
fn healthy_probes() -> Vec<(Probe, Outcome)> {
    vec![
        (
            Probe::ClientLibrary,
            Outcome::ok("found at /usr/lib/libfdb_c.so"),
        ),
        (
            Probe::ClusterFile,
            Outcome::ok(
                "/etc/foundationdb/fdb.cluster names the coordinator docker:docker@127.0.0.1:4500",
            ),
        ),
        (
            Probe::ClusterHealth,
            Outcome::ok("`fdbcli --exec \"status minimal\"` reports available"),
        ),
    ]
}

// ─── (1) the doctor is pure, and each verdict carries an actionable remediation ────────

#[test]
fn doctor_is_green_when_every_probe_passes() {
    let report = diagnose(healthy_probes());
    assert!(
        report.is_ok(),
        "a fully-healthy environment must report ok: {}",
        report.render()
    );
    assert!(report.failures().is_empty());
    assert!(
        report.clone().into_result().is_ok(),
        "into_result must be Ok when every row passed"
    );
    // The rendered report still names every row, so `xtask fdb-doctor` is informative
    // when it passes, not silent.
    let rendered = report.render();
    for probe in [
        Probe::ClientLibrary,
        Probe::ClusterFile,
        Probe::ClusterHealth,
    ] {
        assert!(
            rendered.contains(probe.label()),
            "the green report omits the `{}` row: {rendered}",
            probe.label()
        );
    }
}

#[test]
fn a_missing_client_library_names_the_client_package() {
    let remediation = Probe::ClientLibrary.remediation();
    assert!(
        remediation.contains(fdb_doctor::CLIENT_PACKAGE),
        "the remediation must name the client package to install: {remediation}"
    );
    assert!(
        remediation.contains(fdb_doctor::CLIENT_LIBRARY_SONAME),
        "the remediation must name the library that is missing: {remediation}"
    );
    assert!(
        remediation.contains(fdb_doctor::FDB_VERSION),
        "a client of the wrong version never connects — the remediation must pin it: \
         {remediation}"
    );
}

#[test]
fn an_unreadable_cluster_file_names_the_env_var_and_its_default() {
    let remediation = Probe::ClusterFile.remediation();
    assert!(
        remediation.contains("WYRD_FDB_CLUSTER_FILE"),
        "the remediation must name the env var that overrides the cluster file: {remediation}"
    );
    assert!(
        remediation.contains("/etc/foundationdb/fdb.cluster"),
        "the remediation must name the default cluster-file path: {remediation}"
    );
}

#[test]
fn an_unhealthy_cluster_names_the_bring_up_command() {
    let remediation = Probe::ClusterHealth.remediation();
    assert!(
        remediation.contains("docker compose -f deploy/fdb-single-node/docker-compose.yml up -d"),
        "the remediation must name the exact command that brings the throwaway cluster up: \
         {remediation}"
    );
}

#[test]
fn cluster_file_path_falls_back_to_the_default_when_unset_or_blank() {
    // Mirrors the driver's own resolution (crates/metadata-fdb/src/lib.rs:384-390) so the
    // doctor reports on the SAME file `FdbMetadataStore` would open.
    assert_eq!(cluster_file_path(None), fdb_doctor::DEFAULT_CLUSTER_FILE);
    assert_eq!(
        cluster_file_path(Some("")),
        fdb_doctor::DEFAULT_CLUSTER_FILE
    );
    assert_eq!(
        cluster_file_path(Some("   ")),
        fdb_doctor::DEFAULT_CLUSTER_FILE
    );
    assert_eq!(
        cluster_file_path(Some("/tmp/fdb.cluster")),
        "/tmp/fdb.cluster"
    );
}

#[test]
fn an_unavailable_cluster_is_not_read_as_available() {
    // The trap this needle exists to avoid: "The database is unavailable" CONTAINS the
    // word "available", so a naive `contains("available")` reports a dead cluster healthy.
    // Matching text rather than exit status is also deliberate: fdbcli 7.3.77 exits 0
    // against a dead coordinator.
    assert!(cluster_status_is_healthy(
        "The database is available; 1 process across 1 machine."
    ));
    assert!(!cluster_status_is_healthy(
        "The database is unavailable; type `status` for more information."
    ));
    assert!(!cluster_status_is_healthy(
        "The coordinator(s) have no record of this database. \
         Either the coordinator addresses are incorrect or the database is not configured."
    ));
    assert!(!cluster_status_is_healthy("configuration missing"));
    assert!(!cluster_status_is_healthy(""));
}

#[test]
fn ldconfig_fallback_recognises_the_client_library() {
    assert!(ldconfig_lists_client_library(
        "\tlibfdb_c.so (libc6,x86-64) => /lib/libfdb_c.so\n\tlibc.so.6 => /lib/libc.so.6\n"
    ));
    assert!(!ldconfig_lists_client_library(
        "\tlibc.so.6 (libc6,x86-64) => /lib/libc.so.6\n"
    ));
    assert!(!ldconfig_lists_client_library(""));
}

#[test]
fn the_client_library_search_honours_fdb_client_lib_path() {
    // `foundationdb-sys`' own build.rs turns `FDB_CLIENT_LIB_PATH` into a
    // `rustc-link-search` (foundationdb-sys-0.10.0/build.rs:61-64). A client under a
    // custom prefix with that variable set therefore LINKS FINE — so the doctor must
    // search it, and search it FIRST. Miss it and a working build is reported as a
    // missing client library: `cargo xtask fdb-conformance` then exits 0 with all five
    // test legs silently skipped locally, and hard-fails in CI.
    let with_env = client_library_search_paths(Some("/opt/foundationdb/lib"));
    assert_eq!(
        with_env.first().map(String::as_str),
        Some("/opt/foundationdb/lib/libfdb_c.so"),
        "FDB_CLIENT_LIB_PATH must be the first path searched: {with_env:?}"
    );

    // A trailing slash names the same directory; `/` must not collapse to the empty path.
    assert_eq!(
        client_library_search_paths(Some("/opt/foundationdb/lib/")).first(),
        with_env.first(),
    );
    assert_eq!(
        client_library_search_paths(Some("/")).first().unwrap(),
        "/libfdb_c.so"
    );

    // Unset, blank, or whitespace-only → the packages' standard prefixes, unchanged.
    let bare = client_library_search_paths(None);
    assert_eq!(bare, client_library_search_paths(Some("   ")));
    assert_eq!(bare.len(), fdb_doctor::CLIENT_LIBRARY_CANDIDATES.len());
    assert_eq!(with_env.len(), bare.len() + 1);
    for candidate in fdb_doctor::CLIENT_LIBRARY_CANDIDATES {
        assert!(
            bare.iter().any(|p| p == candidate),
            "the standard prefix {candidate} is no longer searched: {bare:?}"
        );
    }
}

#[test]
fn probe_client_library_finds_a_custom_prefix_build_via_fdb_client_lib_path() {
    // The DECISION `main.rs`'s `probe_client_library` delegates to — driven with fake
    // effects so the impure `FDB_CLIENT_LIB_PATH` READ is Check-guarded, not just the pure
    // search-path list above. This is the iteration-1 defect made flippable: a client
    // installed under a custom prefix with `FDB_CLIENT_LIB_PATH` set LINKS FINE
    // (`foundationdb-sys` build.rs:61-64), so reporting it "missing" makes
    // `cargo xtask fdb-conformance` exit 0 with every test leg skipped locally, and
    // hard-fail in CI. Drop the env read in production (`let configured = None`) and this
    // goes red: the fake filesystem has the library at ONLY the configured path.
    let configured_dir = "/opt/foundationdb/lib";
    let expected = format!("{configured_dir}/{}", fdb_doctor::CLIENT_LIBRARY_SONAME);

    let env =
        |name: &str| (name == fdb_doctor::CLIENT_LIB_PATH_ENV).then(|| configured_dir.to_string());
    // Only the custom-prefix copy exists — no standard prefix, no ldconfig entry — so the
    // outcome can only be `Ok` if production actually consulted `FDB_CLIENT_LIB_PATH`.
    let exists = |candidate: &str| candidate == expected;
    let no_ldconfig = || None;

    let outcome = probe_client_library(&env, &exists, &no_ldconfig);
    assert!(
        outcome.passed(),
        "a build with FDB_CLIENT_LIB_PATH set and libfdb_c only under that prefix must \
         probe OK — production must read that variable: {outcome:?}"
    );
    assert!(
        outcome.detail().contains(&expected),
        "the OK outcome must name the path the library was found at: {outcome:?}"
    );

    // Non-vacuity: with NOTHING on any path and no ldconfig hit, the same decision fails,
    // and the failure names the variable the operator can set.
    let nowhere = probe_client_library(&env, &|_| false, &no_ldconfig);
    assert!(
        !nowhere.passed(),
        "no library anywhere must fail: {nowhere:?}"
    );
    assert!(
        nowhere.detail().contains(fdb_doctor::CLIENT_LIB_PATH_ENV),
        "the failure must name FDB_CLIENT_LIB_PATH as a fix: {nowhere:?}"
    );

    // The ldconfig fallback is still consulted when no path matches (a client registered
    // with the dynamic linker under some other prefix).
    let via_ldconfig = probe_client_library(&|_| None, &|_| false, &|| {
        Some(format!(
            "\t{} => /some/where/libfdb_c.so\n",
            fdb_doctor::CLIENT_LIBRARY_SONAME
        ))
    });
    assert!(
        via_ldconfig.passed(),
        "a client in the ldconfig cache must probe OK: {via_ldconfig:?}"
    );
}

// ─── (2) the demonstrated RED: a planted failing probe flips the verdict ───────────────

#[test]
fn doctor_is_red_when_a_failing_probe_outcome_is_planted() {
    // Plant a REAL failing outcome for each probe in turn and prove the SAME production
    // `diagnose` (the function `run_fdb_doctor` and the conformance preflight call) turns
    // it into a not-ok verdict carrying that probe's remediation. The "demonstrated red"
    // the brief requires — the row logic is load-bearing, not a guard resting red on
    // non-existence. Mirrors `scan_dir_is_red_when_an_orchestrator_import_is_planted`.
    for planted in [
        Probe::ClientLibrary,
        Probe::ClusterFile,
        Probe::ClusterHealth,
    ] {
        let probes = healthy_probes()
            .into_iter()
            .map(|(probe, outcome)| {
                if probe == planted {
                    (probe, Outcome::failed("planted failure"))
                } else {
                    (probe, outcome)
                }
            })
            .collect();

        let report = diagnose(probes);
        assert!(
            !report.is_ok(),
            "planting a failing `{}` probe must make the doctor report not-ok: {}",
            planted.label(),
            report.render()
        );

        let failures = report.failures();
        assert_eq!(
            failures.len(),
            1,
            "exactly the planted probe must fail: {}",
            report.render()
        );
        assert_eq!(failures[0].probe, planted);
        assert_eq!(
            failures[0].remediation(),
            Some(planted.remediation()),
            "a failing row must carry its remediation"
        );

        // The verdict a caller acts on: `Err`, naming the failing row AND its fix. This
        // is the message the conformance job fails fast with instead of a linker error.
        let err = report
            .into_result()
            .expect_err("a report with a failing row must be an Err");
        assert!(
            err.contains(planted.label()) && err.contains("planted failure"),
            "the error must name the failing row and what was observed: {err}"
        );
        assert!(
            err.contains(&planted.remediation()),
            "the error must carry the remediation: {err}"
        );
    }
}

#[test]
fn a_passing_row_carries_no_remediation() {
    // The converse of the planted red: were `remediation()` unconditional, the report
    // above would "pass" its assertions while telling a healthy operator to reinstall
    // FoundationDB. Nothing to fix ⇒ nothing printed.
    let report = diagnose(healthy_probes());
    for row in &report.rows {
        assert_eq!(
            row.remediation(),
            None,
            "a passing `{}` row must carry no remediation",
            row.probe.label()
        );
    }
}

// ─── (3) the preflight CALL SITE: the container stack is never entered unchecked ───────

/// Drive the production gate — the whole body of `run_fdb_conformance` — with the
/// container stack injected as a recording closure, and report `(result, stack entries)`.
fn gated_run(docker: bool, is_ci: bool, client_library: Outcome) -> (Result<(), String>, usize) {
    gated_run_with(docker, is_ci, client_library, Ok(()))
}

fn gated_run_with(
    docker: bool,
    is_ci: bool,
    client_library: Outcome,
    stack_result: Result<(), String>,
) -> (Result<(), String>, usize) {
    let mut entered = 0;
    let result = run_gated_conformance(docker, is_ci, client_library, &mut || {
        entered += 1;
        stack_result.clone()
    });
    (result, entered)
}

#[test]
fn a_missing_client_library_stops_the_job_before_a_container_is_started() {
    // THE regression this preflight exists to prevent: without it, `cargo test --features
    // fdb` dies in a raw linker error minutes in, with a container already up. The stack
    // closure counts entries, so deleting the preflight — letting the run proceed — flips
    // both assertions red.
    let missing = Outcome::failed("no libfdb_c.so on any searched path");

    // On CI the job HARD-FAILS with the remediation: a runner that promised libfdb_c and
    // has none is broken, not "skippable".
    let (result, entered) = gated_run(true, true, missing.clone());
    let err = result.expect_err("CI must fail hard when the client library is missing");
    assert_eq!(entered, 0, "the container stack must never be entered");
    assert!(
        err.contains(Probe::ClientLibrary.label()) && err.contains(fdb_doctor::CLIENT_PACKAGE),
        "the CI failure must name the failing row and the package that fixes it: {err}"
    );

    // Locally it warns and skips, so `cargo xtask ci` on a laptop is never blocked.
    let (result, entered) = gated_run(true, false, missing);
    assert!(
        result.is_ok(),
        "a laptop with no FoundationDB must not fail the job: {result:?}"
    );
    assert_eq!(entered, 0, "the container stack must never be entered");
}

#[test]
fn a_missing_docker_stops_the_job_before_a_container_is_started() {
    let healthy = Outcome::ok("found at /usr/lib/libfdb_c.so");

    let (result, entered) = gated_run(false, true, healthy.clone());
    let err = result.expect_err("CI must fail hard when docker is unavailable");
    assert_eq!(entered, 0, "the container stack must never be entered");
    assert!(
        err.contains("docker"),
        "the CI failure must name the missing container runtime: {err}"
    );

    let (result, entered) = gated_run(false, false, healthy);
    assert!(
        result.is_ok(),
        "a laptop with no Docker must skip: {result:?}"
    );
    assert_eq!(entered, 0, "the container stack must never be entered");
}

#[test]
fn a_ready_environment_enters_the_stack_exactly_once_and_propagates_its_result() {
    // Non-vacuity for the two tests above: the gate is not simply "never run the stack".
    let healthy = Outcome::ok("found at /usr/lib/libfdb_c.so");

    for is_ci in [true, false] {
        let (result, entered) = gated_run(true, is_ci, healthy.clone());
        assert!(result.is_ok(), "a ready environment must run: {result:?}");
        assert_eq!(entered, 1, "the stack must be entered exactly once");
    }

    // A failing conformance run is a failing job — the gate must not swallow it.
    let (result, entered) = gated_run_with(true, true, healthy, Err("conformance failed".into()));
    assert_eq!(result, Err("conformance failed".to_string()));
    assert_eq!(entered, 1);
}

/// The body of the top-level `fn <name>(` in `src`, up to the closing brace at column 0.
fn function_body(src: &str, signature: &str) -> String {
    let start = src
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` not found"));
    let mut body = String::new();
    for line in src[start..].lines().skip(1) {
        if line == "}" {
            return body;
        }
        body.push_str(line);
        body.push('\n');
    }
    panic!("`{signature}` has no closing brace at column 0");
}

/// Does `run_fdb_conformance`'s body delegate to the doctor's gate, rather than reaching
/// for the container stack itself?
fn conformance_body_is_gated(body: &str) -> bool {
    body.contains("fdb_doctor::run_gated_conformance") && !body.contains("fdb_compose(")
}

#[test]
fn the_conformance_command_delegates_to_the_gate_and_is_red_when_it_does_not() {
    // Demonstrated red first: a planted body that brings the stack up directly — the
    // pre-#439 shape — must be rejected by this guard, so the assertion below is not
    // resting on a scanner that always says yes.
    let planted = function_body(
        "fn run_fdb_conformance() -> Result<(), String> {\n    \
         let compose = workspace_root().join(fdb_doctor::COMPOSE_FILE);\n    \
         fdb_compose(&compose, &[\"up\", \"-d\"])?;\n    Ok(())\n}\n",
        "fn run_fdb_conformance()",
    );
    assert!(
        !conformance_body_is_gated(&planted),
        "the guard must reject a body that enters the container stack directly"
    );

    // …and the real command is gated: its body is the call to `run_gated_conformance`
    // whose branches are exercised above. Bypassing the preflight means deleting the
    // command, not deleting a block inside it.
    let body = function_body(
        &read("xtask/src/main.rs"),
        "fn run_fdb_conformance() -> Result<(), String> {",
    );
    assert!(
        conformance_body_is_gated(&body),
        "`run_fdb_conformance` must delegate to `fdb_doctor::run_gated_conformance` and \
         never touch `fdb_compose` itself: {body}"
    );
}

// ─── (5a) the live client-library adapter READS the real env and filesystem ────────────
//
// The pure decisions above (`probe_client_library`, `run_gated_conformance`) are driven by
// the unit tests with fake effects. The adapter that supplies the REAL effects is
// `fdb_doctor::probe_client_library_live` — moved into the lib (iteration 4/5) precisely so
// this integration test can drive it end-to-end. A *structural* substring check over its
// body (the previous approach) was not enough: two semantic-equivalent mutations
// (`&|name| { let _ = std::env::var(name); None }`, `&|candidate| { let _ = candidate; true }`)
// keep the `std::env::var` / `Path::exists` tokens present yet reintroduce iteration-1's
// false-negative. The behavioural test below binds on the OBSERVED RESOLVED PATH — controlled
// by a real `FDB_CLIENT_LIB_PATH` and a real temp file — so a discarded env read or a
// hard-coded `true` changes the outcome and turns Check RED.

#[test]
fn the_live_client_library_adapter_reads_the_real_env_and_filesystem() {
    // Two host-independent scenarios drive the PRODUCTION adapter
    // `fdb_doctor::probe_client_library_live` (what `run_fdb_conformance` / `run_fdb_doctor`
    // call). The configured directory is searched FIRST and short-circuits, and we control
    // whether the file under it exists, so neither assertion depends on whether the host has
    // a system `libfdb_c` (this host does; the plain verify worktree does not).
    let unique = format!(
        "wyrd-fdb-doctor-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after the epoch")
            .as_nanos()
    );
    let present_dir = std::env::temp_dir().join(format!("{unique}-present"));
    let absent_dir = std::env::temp_dir().join(format!("{unique}-absent"));
    std::fs::create_dir_all(&present_dir).expect("create the present tempdir");
    std::fs::create_dir_all(&absent_dir).expect("create the absent tempdir");
    let planted = present_dir.join(fdb_doctor::CLIENT_LIBRARY_SONAME);
    std::fs::write(&planted, b"not a real shared object").expect("plant a fake libfdb_c.so");
    let planted_str = planted.to_string_lossy().into_owned();
    let absent_str = absent_dir
        .join(fdb_doctor::CLIENT_LIBRARY_SONAME)
        .to_string_lossy()
        .into_owned();

    // (A) the env read is REAL. With `FDB_CLIENT_LIB_PATH` pointing at a dir that holds
    // `libfdb_c.so`, the adapter must resolve THAT copy — which is only possible if it
    // consulted the variable. Mutation `&|name| { …; None }` (env read discarded) resolves a
    // standard path or reports missing instead, so it no longer names the configured copy.
    std::env::set_var(fdb_doctor::CLIENT_LIB_PATH_ENV, &present_dir);
    let found = probe_client_library_live();

    // (B) the existence check is REAL. With `FDB_CLIENT_LIB_PATH` pointing at an EMPTY dir,
    // the adapter must NOT resolve a `libfdb_c.so` under it — there is none. Mutation
    // `&|candidate| { …; true }` (existence hard-coded) reports the configured file present
    // and resolves it, sending the preflight into a container stack with no client library.
    std::env::set_var(fdb_doctor::CLIENT_LIB_PATH_ENV, &absent_dir);
    let missing = probe_client_library_live();

    // Clean up before asserting, so a failed assertion never leaks env/temp state.
    std::env::remove_var(fdb_doctor::CLIENT_LIB_PATH_ENV);
    let _ = std::fs::remove_dir_all(&present_dir);
    let _ = std::fs::remove_dir_all(&absent_dir);

    assert!(
        found.passed() && found.detail().contains(&planted_str),
        "probe_client_library_live must READ FDB_CLIENT_LIB_PATH and resolve the libfdb_c.so \
         under it (dropping the env read reintroduces the iteration-1 false-negative — a \
         working custom-prefix build reported 'missing'): {found:?}"
    );
    assert!(
        !(missing.passed() && missing.detail().contains(&absent_str)),
        "probe_client_library_live must PROBE the filesystem: an EMPTY FDB_CLIENT_LIB_PATH dir \
         must not resolve a libfdb_c.so under it (hard-coding existence to `true` sends the \
         preflight into a container stack with no client library — a linker error minutes in): \
         {missing:?}"
    );
}

#[test]
fn the_conformance_preflight_is_handed_the_real_measured_probes() {
    // Binding property (Success criterion assertion 5): hardcoding the preflight call's
    // probe arguments to a passing `Outcome` — e.g.
    // `run_gated_conformance(true, false, Outcome::ok("x"), …)` — MUST turn this test red, so
    // a green preflight can never be faked past the real `docker_available()` /
    // `probe_client_library_live()` / `is_ci()` measurements.
    let body = function_body(
        &read("xtask/src/main.rs"),
        "fn run_fdb_conformance() -> Result<(), String> {",
    );
    assert!(
        body.contains("fdb_doctor::run_gated_conformance("),
        "run_fdb_conformance must delegate to the gate: {body}"
    );
    // Each effect argument is the REAL measurement, passed through untouched.
    for probe in [
        "docker_available()",
        "is_ci()",
        "probe_client_library_live()",
    ] {
        assert!(
            body.contains(probe),
            "the preflight must be handed the real `{probe}` — hardcoding it (e.g. a passing \
             Outcome) must fail here: {body}"
        );
    }
    // No hardcoded outcome may stand in for a measured probe.
    for hardcoded in [
        "Outcome::ok",
        "Outcome::Ok",
        "Outcome::failed",
        "Outcome::Failed",
    ] {
        assert!(
            !body.contains(hardcoded),
            "the preflight args must be measured, not a hardcoded `{hardcoded}`: {body}"
        );
    }
}

// ─── (4) the workflow exists, and every xtask command HEAD it runs is really dispatched ─

/// Every shell line a `run:` key actually executes, inline or block. Full-line comments
/// (a YAML `#` line, or a shell `#` line inside a `run: |` block) are excluded — a
/// workflow's prose is not a command it runs. There is deliberately **no** trailing-comment
/// surgery and **no** `windows()` scan here: each executed line is handed to
/// [`xtask_head_subcommand`], which binds on the command HEAD alone, so a `cargo xtask …`
/// buried mid-line (an argument, a `: …` no-op prefix, a trailing `# …` note) is never
/// mistaken for an execution. That is the whole re-plan (iteration 4): the three-iteration
/// recurrence was a scraper counting a *mention* as an *execution*.
///
/// Plain-text parsing, as `xtask/tests/readme_dev_section.rs` parses the README: `xtask`
/// has no YAML parser, and adding one would trigger the ADR-0003 §2 dependency audit for a
/// substring check.
fn run_script_lines(yaml: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut block_indent: Option<usize> = None;

    for raw in yaml.lines() {
        let trimmed = raw.trim_start();
        let indent = raw.len() - trimmed.len();

        if let Some(key_indent) = block_indent {
            if trimmed.is_empty() {
                continue;
            }
            if indent > key_indent {
                if !trimmed.starts_with('#') {
                    out.push(trimmed.to_string());
                }
                continue;
            }
            block_indent = None;
        }

        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // `run:` may be the step's list-item key (`- run: …`) or a plain mapping key.
        let (key_indent, key_line) = match trimmed.strip_prefix("- ") {
            Some(rest) => (indent + 2, rest),
            None => (indent, trimmed),
        };
        let Some(value) = key_line.strip_prefix("run:") else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() || value.starts_with('|') || value.starts_with('>') {
            block_indent = Some(key_indent);
        } else {
            out.push(value.to_string());
        }
    }
    out
}

/// The `cargo xtask <sub>` subcommand at the **HEAD** of one executed shell command, if
/// any — the first shell-word sequence of the line: `cargo`, then `xtask`, then the
/// subcommand. Binding on the head — never a `windows()` scan of the line — is exactly what
/// makes a *mention* uncountable, which is the whole point of the re-plan:
///
/// * `echo would run cargo xtask fdb-conformance` → head `echo` → `None`;
/// * `: cargo xtask fdb-conformance` (no-op builtin prefix) → head `:` → `None`;
/// * `echo x # cargo xtask fdb-conformance` (trailing comment) → head `echo` → `None`;
/// * `# cargo xtask fdb-conformance` (full-line comment) → head `#` → `None`.
///
/// The artifact is constrained to match (brief Scope (b)): every `cargo xtask` invocation
/// in the workflow is a bare, single-command `run:` step, so its head IS `cargo xtask
/// <sub>` — there is no shell composition (`&&`, `bash -c`, `run: |`) to reach past.
fn xtask_head_subcommand(command: &str) -> Option<String> {
    let mut tokens = command.split_whitespace();
    if tokens.next()? != "cargo" || tokens.next()? != "xtask" {
        return None;
    }
    let sub = tokens
        .next()?
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-');
    (!sub.is_empty()).then(|| sub.to_string())
}

/// The `cargo xtask` subcommands the given shell lines invoke **as command heads**, in
/// order, deduplicated.
fn xtask_head_subcommands(run_lines: &[String]) -> Vec<String> {
    let mut subs: Vec<String> = Vec::new();
    for line in run_lines {
        if let Some(sub) = xtask_head_subcommand(line) {
            if !subs.iter().any(|s| s == &sub) {
                subs.push(sub);
            }
        }
    }
    subs
}

/// The subcommands `xtask/src/main.rs` really dispatches, scraped from the `Some("<sub>")
/// =>` arms of its `match task.as_deref()` table — the compiled dispatch set, exactly as
/// `xtask/tests/readme_dev_section.rs:41` cross-checks a documented command against it. A
/// workflow head that is not in this set is a typo'd or deleted command, and fails.
fn dispatched_subcommands(main_rs: &str) -> Vec<String> {
    let mut subs = Vec::new();
    for line in main_rs.lines() {
        let Some(rest) = line.trim_start().strip_prefix("Some(\"") else {
            continue;
        };
        let Some(end) = rest.find("\")") else {
            continue;
        };
        if rest[end..].contains("=>") {
            subs.push(rest[..end].to_string());
        }
    }
    subs
}

/// The workflow's steps, one string each, comments stripped — so an `if:` in a step is
/// distinguishable from the word "if" in the prose above it.
fn steps(yaml: &str) -> Vec<String> {
    let mut steps: Vec<String> = Vec::new();
    let mut current: Option<String> = None;
    for raw in yaml.lines() {
        let trimmed = raw.trim_start();
        if trimmed.starts_with('#') {
            continue;
        }
        if trimmed.starts_with("- name:") || trimmed.starts_with("- uses:") {
            if let Some(step) = current.take() {
                steps.push(step);
            }
            current = Some(String::new());
        }
        if let Some(step) = current.as_mut() {
            step.push_str(raw);
            step.push('\n');
        }
    }
    steps.extend(current);
    steps
}

/// The one step whose script executes `needle`.
fn step_running(yaml: &str, needle: &str) -> String {
    let matching: Vec<String> = steps(yaml)
        .into_iter()
        .filter(|step| run_script_lines(step).iter().any(|l| l.contains(needle)))
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "expected exactly one step whose `run:` script executes `{needle}`, found \
         {}",
        matching.len()
    );
    matching.into_iter().next().expect("checked non-empty")
}

#[test]
fn a_bare_xtask_step_is_a_head_but_every_evasion_shape_is_not() {
    // The command-head predicate, unit-tested directly. A bare single-command `cargo xtask
    // <sub>` is the only shape that counts; the evasion shapes that reopened iterations 1–3
    // each have a non-`cargo` head and return None. This is the load-bearing predicate: if
    // any of these mentions were counted, replacing the real invoking step with it would
    // leave `the_fdb_conformance_workflow_runs_only_real_dispatched_subcommands` green while
    // the job executed nothing.
    assert_eq!(
        xtask_head_subcommand("cargo xtask fdb-conformance"),
        Some("fdb-conformance".to_string()),
        "a bare `cargo xtask <sub>` command head must be recognised"
    );
    assert_eq!(
        xtask_head_subcommand("cargo xtask ci"),
        Some("ci".to_string())
    );

    for evasion in [
        "echo would run cargo xtask fdb-conformance", // (iii) mention-as-argument
        ": cargo xtask fdb-conformance",              // (iv) no-op builtin prefix
        "echo DISABLED # cargo xtask fdb-conformance", // (ii) trailing inline comment
        "# cargo xtask fdb-conformance",              // (i) full-line comment
        "true && cargo xtask fdb-conformance",        // shell composition
    ] {
        assert_eq!(
            xtask_head_subcommand(evasion),
            None,
            "a non-head `cargo xtask …` mention must not count as an execution: {evasion:?}"
        );
    }
    // A real command that is not an xtask invocation (the PR-leg type-checks) is not a head.
    assert_eq!(
        xtask_head_subcommand("cargo check -p wyrd-server --features fdb --tests"),
        None
    );
}

#[test]
fn the_workflow_head_scrape_is_red_when_the_conformance_step_is_evaded() {
    // The demonstrated red the re-plan requires: enumerate every known evasion shape as an
    // explicit case, each replacing the REAL invoking step, and prove each removes
    // `fdb-conformance` from the executed-head set — so the consistency assertion in
    // `the_fdb_conformance_workflow_runs_only_real_dispatched_subcommands`
    // (`… .contains("fdb-conformance")`) turns RED. This is the exact class that reopened
    // iterations 1–3; a `windows()` scan over the line would have wrongly counted (ii)–(iv).
    fn workflow_with(conformance_step: &str) -> String {
        format!(
            "jobs:\n  fdb-conformance:\n    steps:\n      - name: Docker info\n        \
             run: docker info\n{conformance_step}"
        )
    }

    // Positive control: the real, bare single-command step IS counted as a head.
    let real = workflow_with(
        "      - name: cargo xtask fdb-conformance\n        run: cargo xtask fdb-conformance\n",
    );
    assert!(
        xtask_head_subcommands(&run_script_lines(&real)).contains(&"fdb-conformance".to_string()),
        "the real bare `run: cargo xtask fdb-conformance` step must be counted as a head"
    );

    let evasions = [
        // (i) full-line `#` comment inside a `run: |` block.
        "      - name: evaded\n        run: |\n          # cargo xtask fdb-conformance\n",
        // (ii) trailing inline `#` comment on a real command.
        "      - name: evaded\n        run: echo skip # cargo xtask fdb-conformance\n",
        // (iii) mention-as-argument.
        "      - name: evaded\n        run: echo would run cargo xtask fdb-conformance\n",
        // (iv) no-op builtin prefix.
        "      - name: evaded\n        run: : cargo xtask fdb-conformance\n",
    ];
    for (i, evasion) in evasions.iter().enumerate() {
        let yaml = workflow_with(evasion);
        let heads = xtask_head_subcommands(&run_script_lines(&yaml));
        assert!(
            !heads.contains(&"fdb-conformance".to_string()),
            "evasion shape {i} must NOT be counted as executing `cargo xtask fdb-conformance`: \
             {heads:?}"
        );
    }
}

#[test]
fn the_fdb_conformance_workflow_runs_only_real_dispatched_subcommands() {
    let workflow = read(WORKFLOW);
    let main_rs = read("xtask/src/main.rs");

    // The compiled dispatch set — the same `Some("<sub>") =>` cross-check
    // `readme_dev_section.rs:41` makes, so a bogus/typo'd head fails.
    let dispatched = dispatched_subcommands(&main_rs);
    assert!(
        dispatched.contains(&"ci".to_string())
            && dispatched.contains(&"fdb-conformance".to_string()),
        "the dispatch-table scrape found no `Some(\"…\") =>` arms — the cross-check would be \
         vacuous: {dispatched:?}"
    );

    // Every `cargo xtask` HEAD the workflow executes, cross-checked against that set.
    let heads = xtask_head_subcommands(&run_script_lines(&workflow));
    assert!(
        heads.contains(&"fdb-conformance".to_string()),
        "the workflow must actually RUN `cargo xtask fdb-conformance` as a command head — \
         that is the whole job (gap 1 of #439). Executed heads: {heads:?}"
    );
    for sub in &heads {
        assert!(
            dispatched.contains(sub),
            "{WORKFLOW} runs `cargo xtask {sub}`, but xtask/src/main.rs dispatches no such \
             subcommand (dispatched: {dispatched:?}) — a typo'd or deleted command"
        );
    }

    // The doctor is a real dispatched subcommand, and it is offered in the usage line so
    // `cargo xtask` discloses it.
    assert!(
        dispatched.contains(&"fdb-doctor".to_string()),
        "xtask/src/main.rs does not dispatch the `fdb-doctor` preflight: {dispatched:?}"
    );
    assert!(
        main_rs.contains("fdb-doctor|"),
        "`fdb-doctor` is missing from xtask's usage line"
    );

    // The conformance run is unconditional — it is the job. An `if:` here would silently
    // reduce the PR leg to a no-op.
    assert!(
        !step_running(&workflow, "cargo xtask fdb-conformance").contains("if:"),
        "the conformance step must run on every trigger, not behind an `if:`"
    );
}

#[test]
fn the_pull_request_leg_type_checks_every_fdb_feature_arm_it_filters_on() {
    // Gap 2 of #439, and the reason `crates/server/**` is in the path filter: the server's
    // `#[cfg(feature = "fdb")]` backend-selection arms are compiled by NO default-feature
    // build, and the conformance driver's legs build `-p wyrd-metadata-fdb` only. If the
    // only step that compiles them were the nightly whole-gate run, the filter would
    // promise a check it does not perform, and a type error would surface up to 24h after
    // merge.
    let workflow = read(WORKFLOW);
    let run_lines = run_script_lines(&workflow);

    for row in xtask::feature_gated_checks(false, true) {
        let command = format!("cargo {}", row.join(" "));
        assert!(
            run_lines.iter().any(|line| line.trim() == command),
            "{WORKFLOW} must run `{command}` — it is a row of \
             `xtask::feature_gated_checks(false, true)`, and nothing else in CI compiles it. \
             Executed: {run_lines:?}"
        );
        // …and unconditionally, so the pull-request leg really performs it.
        assert!(
            !step_running(&workflow, &command).contains("if:"),
            "`{command}` must run on every trigger (including pull requests), not behind \
             an `if:`"
        );
    }

    // The path filter that makes those checks reachable at all.
    for filtered in ["crates/metadata-fdb/**", "crates/server/**"] {
        assert!(
            workflow.contains(&format!("\"{filtered}\"")),
            "the pull-request path filter must include `{filtered}`"
        );
    }
}

#[test]
fn the_workflow_runs_on_pull_requests_and_nightly_and_is_not_a_merge_gate() {
    let workflow = read(WORKFLOW);

    // (a) A pull-request trigger — otherwise a PR that breaks `metadata-fdb` gets no FDB
    //     signal at all (gap 1 of #439).
    assert!(
        workflow.contains("pull_request:"),
        "the workflow must run on pull requests"
    );

    // (b) Nightly + opt-in, the tier1-jepsen.yml shape: post-merge regression visibility
    //     for what a path filter cannot catch.
    assert!(
        workflow.contains("schedule:") && workflow.contains("cron:"),
        "the workflow must run nightly (tier1-jepsen.yml's shape)"
    );
    assert!(
        workflow.contains("workflow_dispatch:"),
        "the workflow must be runnable on demand"
    );

    // (c) The integration-nightly.yml container-job shape: a docker check, a timeout so a
    //     hung run fails fast (#150), and a failure-artifact upload.
    assert!(
        run_script_lines(&workflow)
            .iter()
            .any(|line| line == "docker info"),
        "a container job must confirm the Docker daemon is reachable"
    );
    assert!(
        workflow.contains("timeout-minutes:"),
        "the job must be bounded, or a hung run drifts to GitHub's 6h default (#150)"
    );
    assert!(
        workflow.contains("if: failure()") && workflow.contains("upload-artifact"),
        "a failed run must upload its diagnostics"
    );

    // (d) NOT a required merge gate (ADR-0016): the unprivileged, container-free
    //     `cargo xtask ci` stays the one gating check. Recorded as prose in the workflow
    //     so the next editor knows the job is deliberately non-gating.
    assert!(
        workflow.contains("NOT a required merge-gate status check"),
        "the workflow must record that it is not a required merge gate (ADR-0016)"
    );
}

// ─── (5) the feature-gated type-checks, and their INDEPENDENT toolchain gates ──────────

/// Is `row` a `cargo check -p <pkg> --features <feature> …` invocation?
fn is_check_of(row: &[&str], pkg: &str, feature: &str) -> bool {
    row.first() == Some(&"check")
        && row.windows(2).any(|w| w == ["-p", pkg])
        && row.windows(2).any(|w| w == ["--features", feature])
}

#[test]
fn feature_gated_checks_type_check_the_fdb_surface_when_the_fdb_toolchain_is_present() {
    // `cargo xtask ci` builds/tests `--workspace` with DEFAULT features, and
    // `--all-targets` widens target kinds, not the feature set. So the whole
    // `#[cfg(feature = "fdb")]` `store` module of crates/metadata-fdb/src/lib.rs and every
    // `#[cfg(feature = "fdb")]` arm in crates/server/src/cli.rs are compiled by NO step of
    // the gate (gap 2 of #439). These two rows are what type-check them.
    let checks = xtask::feature_gated_checks(false, true);
    assert!(
        checks
            .iter()
            .any(|row| is_check_of(row, "wyrd-metadata-fdb", "fdb")),
        "the fdb toolchain must yield `cargo check -p wyrd-metadata-fdb --features fdb`: \
         {checks:?}"
    );
    assert!(
        checks
            .iter()
            .any(|row| is_check_of(row, "wyrd-server", "fdb,etcd")),
        "the fdb toolchain must yield `cargo check -p wyrd-server --features fdb,etcd` — the \
         server's backend-selection arms are `#[cfg(feature = \"fdb\")]`, AND the S3 gateway's \
         dispatch arm is `#[cfg(all(feature = \"fdb\", feature = \"etcd\"))]`, so a plain \
         `--features fdb` check cfg's out the very combination the canonical production stack \
         runs (`deploy/small-multi-node-fdb/` builds FEATURES=\"fdb,etcd\"): {checks:?}"
    );
    for row in &checks {
        assert!(
            row.contains(&"--tests"),
            "a type-check must cover the test targets too (that is where the live scenario \
             bodies live): {row:?}"
        );
    }
}

#[test]
fn the_fdb_and_tikv_toolchain_gates_are_independent() {
    // THE hazard that forced two explicit parameters. `run_ci_steps` used to gate the whole
    // `feature_gated_checks()` list on `tikv_toolchain_available()`, so appending the fdb
    // rows to the old zero-argument fn would have fired the FDB type-check only when
    // `WYRD_TIKV_TOOLCHAIN` happened to be set — a silent, wrong coupling between two
    // backends that need entirely different privileged toolchains (grpcio/protoc vs.
    // a system libfdb_c). `run_ci_steps` resolves each gate from its own env name
    // (`xtask::TIKV_TOOLCHAIN_ENV` / `xtask::FDB_TOOLCHAIN_ENV`); its unit test in
    // `xtask/src/main.rs` covers that call site.
    assert_ne!(
        xtask::TIKV_TOOLCHAIN_ENV,
        xtask::FDB_TOOLCHAIN_ENV,
        "the two toolchain gates must be separate environment variables"
    );

    let neither = xtask::feature_gated_checks(false, false);
    assert!(
        neither.is_empty(),
        "the default `cargo xtask ci` (no toolchain declared) must compile neither feature \
         tree — it stays offline and container-free: {neither:?}"
    );

    let tikv_only = xtask::feature_gated_checks(true, false);
    assert!(
        tikv_only
            .iter()
            .any(|row| is_check_of(row, "wyrd-metadata-tikv", "tikv")),
        "the tikv toolchain must still yield its own row: {tikv_only:?}"
    );
    assert!(
        tikv_only
            .iter()
            .any(|row| is_check_of(row, "wyrd-server", "tikv,etcd")),
        "the tikv toolchain must yield `cargo check -p wyrd-server --features tikv,etcd` — the \
         S3 gateway's dispatch arm is `#[cfg(all(feature = \"tikv\", feature = \"etcd\"))]`, so \
         a `--features tikv` check alone cfg's out the exact combination the RETAINED FALLBACK \
         stack runs (`deploy/small-multi-node/` builds FEATURES=\"tikv,etcd\" and its gateways \
         run `--metadata-backend tikv --coordination-backend etcd`). The anti-rot bar would \
         stay green while that combination rotted (#443): {tikv_only:?}"
    );
    assert!(
        !tikv_only.iter().any(|row| row.contains(&"fdb")),
        "the TIKV toolchain must not drag in the fdb feature tree (it has no libfdb_c): \
         {tikv_only:?}"
    );

    let fdb_only = xtask::feature_gated_checks(false, true);
    assert!(
        !fdb_only.iter().any(|row| row.contains(&"tikv")),
        "the FDB toolchain must not drag in the pre-1.0 tikv-client tree: {fdb_only:?}"
    );

    // Both declared → every row, no duplication.
    let both = xtask::feature_gated_checks(true, true);
    assert_eq!(
        both.len(),
        tikv_only.len() + fdb_only.len(),
        "the two gates must compose additively: {both:?}"
    );
}

// ─── drift guards: the doctor's duplicated literals, and the version pinned in 3 files ──

#[test]
fn the_doctors_cluster_file_literals_match_the_drivers_own() {
    // `xtask` must not depend on `wyrd-metadata-fdb` (its `fdb` feature would drag
    // `libfdb_c` into the gate's build graph, and a new Cargo dependency is an ADR-0003 §2
    // decision), so the doctor duplicates these two literals. Pin them: a rename in the
    // driver that leaves the doctor advising the old variable is worse than no doctor.
    let driver = read("crates/metadata-fdb/src/lib.rs");
    assert!(
        driver.contains(&format!(
            "CLUSTER_FILE_ENV: &str = \"{}\"",
            fdb_doctor::CLUSTER_FILE_ENV
        )),
        "crates/metadata-fdb/src/lib.rs no longer declares CLUSTER_FILE_ENV = {:?} — the \
         doctor's remediation would name a variable nothing reads",
        fdb_doctor::CLUSTER_FILE_ENV
    );
    assert!(
        driver.contains(&format!(
            "DEFAULT_CLUSTER_FILE: &str = \"{}\"",
            fdb_doctor::DEFAULT_CLUSTER_FILE
        )),
        "crates/metadata-fdb/src/lib.rs no longer defaults to {:?}",
        fdb_doctor::DEFAULT_CLUSTER_FILE
    );
}

#[test]
fn the_pinned_fdb_version_agrees_across_the_image_the_client_and_the_doctor() {
    // A client library whose version differs from the server's never connects. The pin
    // lives in three files; `deny.toml`'s audit-policy note explains why it must be bumped
    // in lockstep (libfdb_c is invisible to `cargo deny`/`cargo audit`). This assertion is
    // what makes a partial bump fail the gate.
    let compose = read(fdb_doctor::COMPOSE_FILE);
    assert!(
        compose.contains(fdb_doctor::FDB_IMAGE),
        "{} no longer runs the pinned image {}",
        fdb_doctor::COMPOSE_FILE,
        fdb_doctor::FDB_IMAGE
    );
    assert!(
        fdb_doctor::FDB_IMAGE.ends_with(fdb_doctor::FDB_VERSION),
        "the doctor's image pin and version pin disagree: {} vs {}",
        fdb_doctor::FDB_IMAGE,
        fdb_doctor::FDB_VERSION
    );

    let workflow = read(WORKFLOW);
    assert!(
        workflow.contains(&format!("FDB_VERSION: \"{}\"", fdb_doctor::FDB_VERSION)),
        "{WORKFLOW} installs a client package whose version is not the pinned {}",
        fdb_doctor::FDB_VERSION
    );

    // The bring-up command the doctor prints must name the compose file that exists.
    assert!(
        fdb_doctor::CLUSTER_BRINGUP_COMMAND.contains(fdb_doctor::COMPOSE_FILE),
        "the remediation names a different compose file than the doctor probes"
    );
    assert!(
        workspace_root().join(fdb_doctor::COMPOSE_FILE).is_file(),
        "the doctor's remediation names a compose file that does not exist: {}",
        fdb_doctor::COMPOSE_FILE
    );
}

#[test]
fn the_workflow_opts_the_fdb_typechecks_into_the_whole_gate() {
    // The nightly whole-gate leg proves `run_ci_steps`' env wiring end-to-end on a runner
    // that really has `libfdb_c`: the gate emits the two fdb rows because — and only
    // because — this variable is set, independently of WYRD_TIKV_TOOLCHAIN.
    let workflow = read(WORKFLOW);
    assert!(
        workflow.contains(&format!("{}: \"1\"", xtask::FDB_TOOLCHAIN_ENV)),
        "{WORKFLOW} must set {} so `cargo xtask ci` emits the fdb type-checks",
        xtask::FDB_TOOLCHAIN_ENV
    );
    // …and it must not SET the TiKV gate (naming it in the prose that explains the split
    // is exactly what we want): this runner has no `tikv-client` toolchain, and a gate
    // that opted both in would compile grpcio/protoc for nothing.
    assert!(
        !workflow.contains(&format!("{}: \"", xtask::TIKV_TOOLCHAIN_ENV)),
        "{WORKFLOW} must not set {} — this runner has no TiKV toolchain, and the two gates \
         are independent",
        xtask::TIKV_TOOLCHAIN_ENV
    );
}

#[test]
fn the_audit_policy_records_what_cargo_deny_cannot_see() {
    // Item 4 of #439: `cargo deny`/`cargo audit` traverse the Rust graph
    // (foundationdb-sys -> bindgen -> clang-sys -> libloading, allowlisted in deny.toml)
    // but CANNOT see `libfdb_c`, a system shared object with no lockfile entry. That split
    // is a written policy, and deny.toml is where the audit policy lives.
    let deny = read("deny.toml");
    assert!(
        deny.contains("libfdb_c"),
        "deny.toml's header must record that `libfdb_c` is invisible to cargo deny/audit"
    );
    assert!(
        deny.to_lowercase().contains("release notes"),
        "deny.toml's header must say how libfdb_c's advisory surface IS tracked \
         (upstream FoundationDB release notes)"
    );
}

/// #543: the shipped-artifact wall must carry NO exception for an off-by-default backend.
///
/// `cargo deny`'s `[advisories] ignore` entries are keyed by advisory ID alone — cargo-deny
/// 0.20 accepts only `id` and `reason`, with no crate- or feature-scoping — so an entry
/// applies to the WHOLE graph it is used with. A tikv-client exception parked in `deny.toml`
/// would therefore also suppress that advisory if a future DEFAULT dependency ever pulled an
/// affected version, silently holing the wall that guards what we ship.
///
/// So the tikv-client boundary lives in `deny-all-features.toml`, and this test is what stops
/// a well-meaning "let's consolidate the two configs" from quietly undoing that.
#[test]
fn the_shipped_artifact_wall_carries_no_off_by_default_backend_exception() {
    let deny = read("deny.toml");
    let all_features = read("deny-all-features.toml");

    // The six tikv-client advisories (#543). None may appear in the default wall.
    for advisory in [
        "RUSTSEC-2026-0104",
        "RUSTSEC-2026-0099",
        "RUSTSEC-2026-0098",
        "RUSTSEC-2024-0437",
        "RUSTSEC-2025-0134",
        // Only raised because both configs set `unsound = "all"`; cargo-deny's default
        // unsound scope (`workspace`) never looks at transitive crates like rand 0.7.3.
        "RUSTSEC-2026-0097",
    ] {
        assert!(
            !deny.contains(advisory),
            "{advisory} reaches us only through tikv-client, behind the off-by-default `tikv` \
             feature — it must NOT be ignored in deny.toml, which guards the DEFAULT (shipped) \
             graph. cargo-deny ignores are global to the graph they run against, so this entry \
             would also mask the advisory if a default dependency ever pulled an affected \
             version. It belongs in deny-all-features.toml (#543)."
        );
        assert!(
            all_features.contains(advisory),
            "{advisory} must be recorded in deny-all-features.toml — that is the config the \
             off-by-default backend trees are audited with (#543)"
        );
    }

    // The all-features graph is a SUPERSET of the default one, so every ignore the default
    // wall needs must also be present there, or `cargo xtask ci`'s second wall goes red.
    // cargo-deny has no config `include`, so this is enforced here rather than by the tool.
    assert!(
        all_features.contains("RUSTSEC-2025-0141"),
        "deny-all-features.toml must inherit deny.toml's bincode ignore: it audits a superset \
         of the default graph, so an ignore missing here fails the gate"
    );

    // BOTH walls must audit unsoundness across the WHOLE graph. cargo-deny's default scope
    // is `workspace` — our own crates only — which silently ignores every transitive
    // unsoundness advisory. That default is what hid RUSTSEC-2026-0190 (`anyhow` <= 1.0.102,
    // UB in `Error::downcast_mut()`) from the shipped-artifact wall, and RUSTSEC-2026-0097
    // (rand 0.7.3) from the TiKV one. Dropping this back to the default would re-blind both
    // walls without failing anything — hence a test.
    for (name, cfg) in [
        ("deny.toml", &deny),
        ("deny-all-features.toml", &all_features),
    ] {
        assert!(
            cfg.contains(r#"unsound = "all""#),
            "{name} must set `unsound = \"all\"`: cargo-deny's default scope is `workspace`, \
             which never raises a TRANSITIVE unsoundness advisory — and every crate we link is \
             transitive, so the wall would be blind to exactly the class of bug it exists for \
             (#543)"
        );
    }
}

/// #547: the ADR-0003 licence wall must cover the graphs a `--features fdb` / `--features tikv`
/// build actually LINKS — not just the shipped artifact.
///
/// ADR-0003 §2 judges *linked* crates. The default `cargo deny check` never sees `foundationdb`,
/// `libloading`, `ring` or the rest of the optional trees, so before this an AGPL/BSL dependency
/// could enter either one and pass CI — the licence wall is the one check whose whole purpose is
/// to be non-negotiable, and it had a blind spot the size of two backends.
///
/// This also pins the shape of the fix, because the tempting "simplification" reopens the hole:
/// a SECOND licence allowlist in `deny-all-features.toml` would drift out of step with the
/// ADR-0003 one, and a drifted licence wall is worse than a single wall.
#[test]
fn the_licence_wall_covers_the_optional_backend_graphs_from_one_allowlist() {
    let walls = xtask::dependency_wall_invocations();

    let licence_wall = walls.iter().find(|w| w.contains(&"licenses")).expect(
        "the dependency wall must include a LICENCE check over the off-by-default graphs —          without it a denied licence (AGPL/BSL/SSPL) can enter the fdb/tikv trees and pass CI          (#547)",
    );
    assert!(
        licence_wall.contains(&"--all-features"),
        "the licence wall must run over --all-features: the DEFAULT graph resolves none of the          optional backends, which is exactly the blind spot: {licence_wall:?}"
    );
    assert!(
        !licence_wall.contains(&"--config"),
        "the licence wall must use deny.toml's allowlist directly — a second config would mean a          second ADR-0003 allowlist, and two allowlists DRIFT. An allowlist applied to a wider          graph can only reject more, so it needs no quarantined copy (unlike an advisory ignore,          which suppresses): {licence_wall:?}"
    );
    assert!(
        !licence_wall.contains(&"advisories"),
        "advisories must NOT ride along on deny.toml over --all-features: the tikv-client          exceptions live in deny-all-features.toml precisely so they can never reach the shipped          wall (#543), and this invocation would fire them without those ignores: {licence_wall:?}"
    );

    // The single-source invariant, from the other side: no second allowlist may exist.
    let all_features = read("deny-all-features.toml");
    assert!(
        !all_features.contains("[licenses]"),
        "deny-all-features.toml must NOT define a licence policy: the ADR-0003 allowlist stays          SINGLE-SOURCE in deny.toml, which the --all-features licence wall reuses verbatim. Two          copies drift, and the drift is silent until a denied licence slips through the stale one"
    );
}
