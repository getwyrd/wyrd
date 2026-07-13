//! Project automation, in Rust rather than YAML, so the same checks run on a
//! laptop and in CI (ADR-0016, ADR-0009).
//!
//! Subcommands:
//! - `ci` — fmt, clippy, build, test, cargo-machete, cargo-deny, conformance,
//!   and the madsim DST tier; the single gate CI calls. The clippy/warnings
//!   levels come from `[workspace.lints]` in the root Cargo.toml (single source
//!   of truth), so no `-D warnings` flag is passed here.
//! - `conformance` — run the `chunk-format` reader against the committed
//!   conformance vectors.
//! - `dst` — run the madsim commit-protocol tests (`wyrd-dst`) under
//!   `--cfg madsim` across a sweep of seeds (ADR-0009).
//! - `integration` — the Tier-2 container tier (M2, proposal 0004): stand up a
//!   cluster of real, networked gRPC D servers under docker-compose and run the
//!   end-to-end write/read integration test against them. Not part of `ci` (it
//!   needs a container runtime); a heavier-runner / nightly job.
//! - `tikv-conformance` — the M4.1 backend-swap proof (proposal 0007): bring up the
//!   throwaway single-node TiKV under `deploy/` and drive the shared `MetadataStore`
//!   conformance suite against it. Not part of `ci` (it needs a container runtime);
//!   the conformance test itself skips cleanly when no TiKV endpoint is configured.
//! - `fdb-conformance` — the ADR-0042 production-metadata-backend proof (#438): bring up
//!   the throwaway single-node `fdbserver` under `deploy/`, create the database, and drive
//!   the shared `MetadataStore` conformance suite plus the driver-level contention
//!   properties against it (`cargo test -p wyrd-metadata-fdb --features fdb`). Not part of
//!   `ci` (it needs a container runtime AND a system `libfdb_c` to link the real client);
//!   all three cluster-file-gated test binaries skip cleanly when no cluster file is
//!   configured.
//! - `fdb-doctor` — the FoundationDB environment preflight (#439): probe the three
//!   things `fdb-conformance` needs (a loadable `libfdb_c`, a readable cluster file, a
//!   healthy cluster) and print a verdict plus an actionable remediation for each,
//!   instead of a raw linker error or a transaction timeout. The decision logic is
//!   `xtask::fdb_doctor`'s (environment facts in, verdict out); `run_fdb_conformance`
//!   *is* a call to `fdb_doctor::run_gated_conformance`, so the same client-library
//!   row fails the job fast.
//! - `etcd-conformance` — the L5 Coordination backend-swap proof (#365, proposal
//!   0015 §"Deployment prerequisite"): bring up the throwaway single-node etcd under
//!   `deploy/` and drive the shared `Coordination` conformance suite + cross-instance
//!   properties against it (`cargo test -p wyrd-coordination-etcd --features etcd`).
//!   Not part of `ci` (it needs a container runtime AND a system `protoc`); the test
//!   itself skips cleanly when no etcd endpoint is configured. The DETERMINISTIC proof
//!   of the same store is in the `dst` tier (madsim etcd simulator), which is IN `ci`.
//! - `disk-faults` / `jepsen` / `kill-reconstruct` — the deferred (off-Check)
//!   Tier-1 / Tier-2 custodian fault runners (M3, proposal 0005 `0005:405-411`,
//!   `0005:437-438`). Privileged / real-environment tiers, never part of `ci`;
//!   deferred by default, opted in by the dedicated off-Check job.
//! - `deploy-small-multi-node` — the M4.5 production-topology bring-up smoke check
//!   (proposal 0015 §"Deployment", PR sequence item 5, #256): bring up the
//!   `deploy/small-multi-node/` stack (TiKV-small + its PD ensemble + a 3-node etcd
//!   ensemble for L5 Coordination + local-disk D servers) and wait for every
//!   component to accept connections. Not part of `ci` (it needs a container
//!   runtime), exactly like `tikv-conformance` / `integration`. This is the
//!   pre-prerequisite bring-up (proposal 0015's "Deployment prerequisite" note): it
//!   proves the topology stands up on static endpoints, not yet "peers discovered
//!   through L5" (gated on #365 + the runnable gateway/custodian roles).
//! - `bench` — the tracked throughput benchmarks (EC micro-bench + the M2
//!   aggregate D-server throughput bench). Tracked, not gated.

#![forbid(unsafe_code)]

mod conformance;
mod faults;
mod fdb_faults;
mod kill_reconstruct;
mod vectors;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use xtask::fdb_doctor;

fn main() -> ExitCode {
    let task = std::env::args().nth(1);
    let result = match task.as_deref() {
        Some("ci") => run_ci(),
        Some("conformance") => run_conformance(),
        Some("gen-vectors") => run_gen_vectors(),
        Some("dst") => run_dst(),
        Some("statics") => run_statics(),
        Some("integration") => run_integration(),
        Some("tikv-conformance") => run_tikv_conformance(),
        Some("fdb-conformance") => run_fdb_conformance(),
        Some("fdb-doctor") => run_fdb_doctor(),
        Some("fdb-metadata-tier1") => fdb_faults::run_fdb_metadata_tier1(),
        Some("etcd-conformance") => run_etcd_conformance(),
        Some("deploy-small-multi-node") => run_deploy_small_multi_node(),
        Some("disk-faults") => faults::run_disk_faults(),
        Some("jepsen") => faults::run_jepsen(),
        Some("kill-reconstruct") => faults::run_kill_reconstruct(),
        Some("metadata-tier1") => faults::run_metadata_tier1(),
        Some("metadata-tier2") => faults::run_metadata_tier2(),
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
    eprintln!(
        "usage: cargo xtask \
         <ci|conformance|gen-vectors|dst|statics|integration|tikv-conformance|fdb-conformance|fdb-doctor|fdb-metadata-tier1|etcd-conformance|deploy-small-multi-node|disk-faults|jepsen|kill-reconstruct|metadata-tier1|metadata-tier2|bench>"
    );
}

/// Run the tracked throughput benchmarks. Deliberately **not** part of `run_ci`:
/// the numbers are tracked for regression visibility, not gated, because
/// CI-runner wall-clock is noisy. The bench targets' *compilation* is still
/// covered by `run_ci`'s `build --all-targets`.
///
/// - `erasure` — the EC coding micro-bench (M1.7, issue #99).
/// - `throughput` — the M2 aggregate write/read throughput across D-server counts
///   over real (in-process) tonic gRPC D servers (proposal 0004 § Benchmarks,
///   issue #117); makes the §10 Q6 throughput claim first measurable.
fn run_bench() -> Result<(), String> {
    cargo(&["bench", "-p", "wyrd-core", "--bench", "erasure"])?;
    cargo(&["bench", "-p", "wyrd-core", "--bench", "throughput"])
}

/// Default number of D-server containers the Tier-2 cluster stands up — 9, so an
/// `rs(6,3)` chunk's 9 fragments each land on a distinct networked D server.
const DSERVER_COUNT: usize = 9;
/// The compose project name, so the cluster is isolated and torn down cleanly.
const TIER2_PROJECT: &str = "wyrd-tier2";

/// Run the Tier-2 integration tier (M2, proposal 0004 PR step 7): bring up a
/// cluster of real, networked gRPC D servers under docker-compose, run the
/// end-to-end write/read integration test against them, then tear the cluster
/// down. This is the first container job in CI — a heavier-runner / nightly lane,
/// **not** part of `run_ci` (which must stay container-free).
///
/// If Docker is unavailable: a hard failure in CI (where the job is meant to run),
/// a warn-and-skip locally (so a laptop without Docker is not blocked) — mirroring
/// `cargo_deny_check`.
fn run_integration() -> Result<(), String> {
    let compose = workspace_root().join("crates/chunkstore-grpc/tests/docker-compose.yml");
    let compose = compose.to_string_lossy().to_string();

    if !docker_available() {
        if is_ci() {
            return Err("docker is not available but is required for the integration tier".into());
        }
        eprintln!(
            "warning: docker not available; skipping the Tier-2 container integration tier \
             locally. Install Docker (and the compose plugin) to run it."
        );
        return Ok(());
    }

    let (count, count_warning) = resolve_dserver_count(std::env::var("WYRD_DSERVER_COUNT").ok());
    if let Some(warning) = count_warning {
        eprintln!("warning: {warning}");
    }

    // Bring the cluster up (building the image if needed), run the test, then
    // finalize: on failure capture container diagnostics BEFORE teardown (#150),
    // and tear down unconditionally so a run never leaks containers — even if the
    // test *panics*. A `panic!` (vs an `Err` return) would otherwise unwind past
    // the finalization call, leaking the cluster and capturing no diagnostics;
    // running the body under `finalize_panic_safe` keeps capture+teardown on the
    // unwind path too, then resumes the panic (#154).
    compose_up(&compose, count)?;
    finalize_panic_safe(
        || {
            let endpoints = resolve_endpoints(&compose, count)?;
            println!("\nxtask integration: {count} D servers at {endpoints}");
            run_integration_test(&endpoints)
        },
        |result| finish_integration(result, || compose_logs(&compose), || compose_down(&compose)),
    )?;
    println!("\nxtask integration: Tier-2 container integration passed");
    Ok(())
}

/// The compose project name for the throwaway single-node TiKV, so it is isolated
/// and torn down cleanly.
const TIKV_PROJECT: &str = "wyrd-tikv-m41";
/// PD's client endpoint the host reaches (host networking, `deploy/tikv-single-node`).
const TIKV_PD_ENDPOINT: &str = "127.0.0.1:2379";

/// Run the M4.1 TiKV conformance job (proposal 0007 §"Suggested PR sequence" item
/// 1): bring up the throwaway single-node TiKV under `deploy/`, drive the **shared**
/// `MetadataStore` conformance suite against it (`cargo test -p wyrd-metadata-tikv
/// --features tikv`), then tear it down. **Not** part of `run_ci` — it needs a
/// container runtime, exactly like `run_integration`; without one it is a hard
/// failure in CI and a warn-and-skip locally (the conformance test ITSELF also skips
/// cleanly when `WYRD_TIKV_PD_ENDPOINTS` is unset, so `ci` stays green regardless).
fn run_tikv_conformance() -> Result<(), String> {
    let compose = workspace_root().join("deploy/tikv-single-node/docker-compose.yml");
    let compose = compose.to_string_lossy().to_string();

    if !docker_available() {
        if is_ci() {
            return Err(
                "docker is not available but is required for the TiKV conformance job".into(),
            );
        }
        eprintln!(
            "warning: docker not available; skipping the TiKV conformance job locally. \
             Install Docker (and the compose plugin) to run it."
        );
        return Ok(());
    }

    tikv_compose(&compose, &["up", "-d"])?;
    let result = wait_for_port(TIKV_PD_ENDPOINT).and_then(|()| run_tikv_conformance_test());
    // Always tear the stack down, even on failure — a run never leaks containers.
    let _ = tikv_compose(&compose, &["down", "-v", "--remove-orphans"]);
    result?;
    println!("\nxtask tikv-conformance: TiKV passed the shared MetadataStore conformance suite");
    Ok(())
}

/// Run the endpoint-gated TiKV integration tests with the `tikv` feature on and PD
/// exported. TiKV's store bootstrap can lag PD's port opening, so each test — which
/// dials the cluster — is retried a few times with backoff before giving up.
///
/// Four test binaries run: `conformance` (the shared trait-contract suite, M4.1),
/// `contention` (the write-conflict property tests, M4.2/#253), `scan` (the at-scale
/// native paged-scan completeness proof, M4.3/#254), and `deadline` (the operation
/// deadline's drop-safety, #517 — a cancelled operation must return its error, not abort
/// the process on a dropped active transaction). All must pass for the job to exercise the
/// commit conflict semantics, the scan completeness invariant AND the liveness guard.
fn run_tikv_conformance_test() -> Result<(), String> {
    for test in ["conformance", "contention", "scan", "deadline"] {
        run_tikv_test(test)?;
    }
    Ok(())
}

/// Run one endpoint-gated TiKV test binary (`--test <name>`) with retry/backoff.
fn run_tikv_test(test: &str) -> Result<(), String> {
    print_step(&[
        "cargo",
        "test",
        "-p",
        "wyrd-metadata-tikv",
        "--features",
        "tikv",
        "--test",
        test,
    ]);
    let mut last = String::new();
    for attempt in 1..=5 {
        let status = Command::new("cargo")
            .args([
                "test",
                "-p",
                "wyrd-metadata-tikv",
                "--features",
                "tikv",
                "--test",
                test,
                "--",
                "--nocapture",
            ])
            .current_dir(workspace_root())
            .env("WYRD_TIKV_PD_ENDPOINTS", TIKV_PD_ENDPOINT)
            .status()
            .map_err(|e| format!("failed to spawn cargo: {e}"))?;
        if status.success() {
            return Ok(());
        }
        last = format!("TiKV `{test}` test failed with {status}");
        eprintln!(
            "xtask tikv-conformance: `{test}` attempt {attempt}/5 failed; \
             TiKV may still be bootstrapping"
        );
        std::thread::sleep(std::time::Duration::from_secs(3 * attempt));
    }
    Err(last)
}

/// The compose project name for the throwaway single-node FoundationDB, so it is
/// isolated and torn down cleanly.
const FDB_PROJECT: &str = "wyrd-fdb-m4";
/// The address the single `fdbserver` advertises and the host client dials
/// (host networking, `deploy/fdb-single-node`).
const FDB_ENDPOINT: &str = "127.0.0.1:4500";
/// The cluster-file contents. Must be byte-identical to the compose file's
/// `FDB_CLUSTER_FILE_CONTENTS`: a client whose cluster file disagrees never connects.
const FDB_CLUSTER_FILE_CONTENTS: &str = "docker:docker@127.0.0.1:4500";

/// Run the FoundationDB conformance job (ADR-0042, issue #438): bring up the throwaway
/// single-node `fdbserver` under `deploy/`, create the database, drive the **shared**
/// `MetadataStore` conformance suite plus the driver-level contention properties against
/// it (`cargo test -p wyrd-metadata-fdb --features fdb`), then tear it down. **Not** part
/// of `run_ci` — it needs a container runtime AND a system `libfdb_c` to link the real
/// `foundationdb` client, exactly like `run_tikv_conformance` needs a container; without
/// one it is a hard failure in CI and a warn-and-skip locally (all three cluster-file-gated
/// test binaries THEMSELVES also skip cleanly when `WYRD_FDB_CLUSTER_FILE` is unset, so `ci`
/// stays green regardless).
///
/// The body is **only** the gate (#439). `fdb_doctor::run_gated_conformance` owns the
/// docker + client-library preflight and the CI-vs-local convention, and enters
/// [`fdb_conformance_stack`] only when the environment is ready — so a missing client
/// package is an actionable remediation, not a linker error minutes into a `cargo test`
/// with a container already up. Both halves are unit-tested with no Docker and no
/// `libfdb_c` (`xtask/tests/fdb_harness.rs`).
fn run_fdb_conformance() -> Result<(), String> {
    let compose = workspace_root().join(fdb_doctor::COMPOSE_FILE);
    let compose = compose.to_string_lossy().to_string();
    fdb_doctor::run_gated_conformance(
        docker_available(),
        is_ci(),
        fdb_doctor::probe_client_library_live(),
        &mut || fdb_conformance_stack(&compose),
    )
}

/// The privileged half of `fdb-conformance`, entered only past the preflight: bring the
/// compose stack up, create the database, write the host-side cluster file, drive the
/// five `--features fdb` test legs, and tear the stack down unconditionally.
fn fdb_conformance_stack(compose: &str) -> Result<(), String> {
    fdb_compose(compose, &["up", "-d"])?;
    let result = wait_for_port(FDB_ENDPOINT)
        .and_then(|()| configure_fdb_database(compose))
        .and_then(|()| write_fdb_cluster_file())
        .and_then(|cluster_file| run_fdb_conformance_test(&cluster_file));
    // Always tear the stack down, even on failure — a run never leaks containers.
    let _ = fdb_compose(compose, &["down", "-v", "--remove-orphans"]);
    result?;
    println!(
        "\nxtask fdb-conformance: FoundationDB passed the shared MetadataStore conformance \
         suite and the contention properties"
    );
    Ok(())
}

/// Run the FoundationDB environment doctor (#439): probe the three things
/// `run_fdb_conformance` needs, print the verdict + remediation for each row, and exit
/// non-zero if the environment is not ready.
///
/// Every probe below is the **impure** half — it stats files and spawns `fdbcli`. The
/// verdict and the remediation text are `xtask::fdb_doctor`'s, a module unit-tested with
/// no FoundationDB present (`xtask/tests/fdb_harness.rs`).
fn run_fdb_doctor() -> Result<(), String> {
    print_step(&[
        "xtask",
        "fdb-doctor",
        "(FoundationDB environment preflight)",
    ]);
    let cluster_file =
        fdb_doctor::cluster_file_path(std::env::var(fdb_doctor::CLUSTER_FILE_ENV).ok().as_deref());
    let report = fdb_doctor::diagnose(vec![
        (
            fdb_doctor::Probe::ClientLibrary,
            fdb_doctor::probe_client_library_live(),
        ),
        (
            fdb_doctor::Probe::ClusterFile,
            probe_cluster_file(&cluster_file),
        ),
        (
            fdb_doctor::Probe::ClusterHealth,
            probe_cluster_health(&cluster_file),
        ),
    ]);
    print!("{}", report.render());
    if report.is_ok() {
        println!("\nxtask fdb-doctor: the FoundationDB environment is ready");
    }
    report.into_result()
}

/// Probe: is the cluster file readable and non-empty? An empty file is the classic
/// half-installed state — the client accepts the path and then never connects.
fn probe_cluster_file(cluster_file: &str) -> fdb_doctor::Outcome {
    match std::fs::read_to_string(cluster_file) {
        Ok(contents) if contents.trim().is_empty() => {
            fdb_doctor::Outcome::failed(format!("{cluster_file} is empty"))
        }
        Ok(contents) => fdb_doctor::Outcome::ok(format!(
            "{cluster_file} names the coordinator {}",
            contents.trim()
        )),
        Err(e) => fdb_doctor::Outcome::failed(format!("{cluster_file}: {e}")),
    }
}

/// Probe: does `fdbcli --exec "status minimal"` report the database available? A missing
/// `fdbcli`, an unreachable coordinator, and a running-but-unconfigured cluster
/// (`configuration missing`) are all failures of the same row, each with its own detail.
///
/// The verdict is read out of the **text**, not the exit status: `fdbcli` 7.3.77 exits 0
/// against a dead coordinator (verified on 7.3.77), so `status.success()` is not a
/// health predicate.
fn probe_cluster_health(cluster_file: &str) -> fdb_doctor::Outcome {
    let output = Command::new("fdbcli")
        .args(["-C", cluster_file, "--exec", "status minimal"])
        .output();
    match output {
        Err(e) => fdb_doctor::Outcome::failed(format!("could not run fdbcli: {e}")),
        Ok(output) => {
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            text.push_str(&String::from_utf8_lossy(&output.stderr));
            if fdb_doctor::cluster_status_is_healthy(&text) {
                fdb_doctor::Outcome::ok(format!(
                    "`{}` reports available",
                    fdb_doctor::HEALTH_COMMAND
                ))
            } else {
                let detail = text.trim();
                let detail = if detail.is_empty() {
                    "fdbcli reported no status".to_string()
                } else {
                    detail.lines().take(2).collect::<Vec<_>>().join("; ")
                };
                fdb_doctor::Outcome::failed(detail)
            }
        }
    }
}

/// Create the database on a freshly-started `fdbserver`.
///
/// The image's entrypoint starts the server but deliberately leaves it unconfigured — a
/// fresh process reports `configuration missing` until `configure new` runs once. This is
/// executed *inside* the container (`docker compose exec`) so the job needs no host
/// `fdbcli`; only `libfdb_c`, which the `--features fdb` build already requires.
/// Re-running against an already-created database reports "Database already exists", which
/// is a benign no-op here, so the exit status is not treated as fatal on its own — the
/// readiness probe below is what actually gates the run.
fn configure_fdb_database(compose: &str) -> Result<(), String> {
    let _ = fdb_compose(
        compose,
        &[
            "exec",
            "-T",
            "fdb",
            "fdbcli",
            "--exec",
            "configure new single memory",
        ],
    );
    // The database is created asynchronously; poll `status minimal` until it reports
    // available rather than assuming the `configure` returned a usable cluster.
    for attempt in 1..=20 {
        let status = Command::new("docker")
            .args([
                "compose",
                "-p",
                FDB_PROJECT,
                "-f",
                compose,
                "exec",
                "-T",
                "fdb",
                "fdbcli",
                "--exec",
                "status minimal",
            ])
            .current_dir(workspace_root())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| format!("failed to spawn docker: {e}"))?;
        if status.success() {
            return Ok(());
        }
        eprintln!("xtask fdb-conformance: cluster not available yet (attempt {attempt}/20)");
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    Err("the FoundationDB cluster did not become available within 20s".into())
}

/// Write the host-side cluster file the `foundationdb` client dials, and return its path.
///
/// The container writes its own copy from the compose file's `FDB_CLUSTER_FILE_CONTENTS`;
/// the host needs a byte-identical one. It lands under `target/` (build output, already
/// git-ignored), never in the source tree.
fn write_fdb_cluster_file() -> Result<String, String> {
    let dir = workspace_root().join("target/fdb-single-node");
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let path = dir.join("fdb.cluster");
    std::fs::write(&path, FDB_CLUSTER_FILE_CONTENTS)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(path.to_string_lossy().to_string())
}

/// Run the FoundationDB test legs with the `fdb` feature on.
///
/// Five legs, all of which must pass:
///
/// * `--lib` — the driver's own unit tests. It needs no cluster, but it DOES need
///   `libfdb_c`, so it can only run here and not in `run_ci`. It carries the rules a live
///   server cannot exhibit: the commit **routing** rule (a precondition-free batch takes
///   the blind path), "`1021 commit_unknown_result` is never blind-retried", and "an
///   exhausted blind retry is `Err`, never `Conflict`"
///   (`crates/metadata-fdb/src/lib.rs`, `store::tests`).
/// * `conformance` — the shared trait-contract suite, all seven clauses through `run_all`.
/// * `contention` — the 1020 → `Conflict` classification and the blind-batch-`Err` rule.
/// * `scan` — the at-scale paged-scan completeness proof AND the `SCAN_CAP` fail-loud rule
///   (the shared suite's scan clause stores three keys, so it reaches neither).
/// * `timeout` — every operation terminates when the cluster is unreachable, and a timed-out
///   commit is an undeterminable outcome rather than a `Conflict`. It ignores the cluster
///   file entirely: it points its own at an unreachable coordinator, because the property is
///   about the *absence* of a cluster. Its wall-clock guard turns a dropped transaction
///   deadline into a failure instead of a hung job.
fn run_fdb_conformance_test(cluster_file: &str) -> Result<(), String> {
    run_fdb_leg(&["--lib"], cluster_file)?;
    for test in ["conformance", "contention", "scan", "timeout"] {
        run_fdb_leg(&["--test", test], cluster_file)?;
    }
    Ok(())
}

/// Run one cluster-file-gated FDB test leg (`--lib`, or `--test <name>`) **exactly once**.
///
/// Deliberately no retry/backoff, unlike the TiKV job (`run_tikv_test`): a failing test
/// binary is re-run there because TiKV's store bootstrap can lag PD's port opening, and
/// that inherited shape would launder a flaky *assertion* failure into a pass — the
/// contention and scan binaries are the sole witnesses for the commit-conflict and
/// scan-completeness invariants, so a green must mean they passed on the first run.
///
/// Nothing is lost. `configure_fdb_database` has already polled `status minimal` until the
/// cluster reports available, and the FDB client *blocks* on a settling cluster rather than
/// erroring (a transaction waits for a read version), so a not-yet-ready cluster shows up
/// as a slow first test, not as a failure to retry away.
fn run_fdb_leg(leg: &[&str], cluster_file: &str) -> Result<(), String> {
    let mut args = vec!["test", "-p", "wyrd-metadata-fdb", "--features", "fdb"];
    args.extend_from_slice(leg);

    let mut display = vec!["cargo"];
    display.extend_from_slice(&args);
    print_step(&display);

    let status = Command::new("cargo")
        .args(&args)
        .args(["--", "--nocapture"])
        .current_dir(workspace_root())
        .env("WYRD_FDB_CLUSTER_FILE", cluster_file)
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "FoundationDB `{}` test leg failed with {status}",
            leg.join(" ")
        ))
    }
}

/// Run a `docker compose -p <FDB_PROJECT> -f <file> …` command from the workspace root,
/// echoing it first.
fn fdb_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", FDB_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    print_step(&display);

    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{}` failed with {status}", display.join(" ")))
    }
}

/// The compose project name for the throwaway single-node etcd, isolated and torn
/// down cleanly.
const ETCD_PROJECT: &str = "wyrd-etcd-l5";
/// etcd's client endpoint the host reaches (`deploy/etcd-single-node`).
const ETCD_ENDPOINT: &str = "127.0.0.1:2379";

/// Run the L5 Coordination conformance job (#365, proposal 0015 §"Deployment
/// prerequisite"): bring up the throwaway single-node etcd under `deploy/`, drive
/// the **shared** `Coordination` conformance suite + the cross-instance properties
/// against it (`cargo test -p wyrd-coordination-etcd --features etcd`), then tear it
/// down. **Not** part of `run_ci` — it needs a container runtime AND a system
/// `protoc` to compile the real `etcd-client`, exactly like `run_tikv_conformance`
/// needs a container; without either it is a hard failure in CI and a warn-and-skip
/// locally (the conformance test ITSELF also skips cleanly when `WYRD_ETCD_ENDPOINTS`
/// is unset, so `ci` stays green regardless).
///
/// The DETERMINISTIC proof of the same store lives in the `dst` tier
/// (`crates/dst/tests/coordination.rs`, madsim etcd simulator), which needs neither a
/// container nor `protoc`; this job is the real-etcd home that pins fidelity.
fn run_etcd_conformance() -> Result<(), String> {
    let compose = workspace_root().join("deploy/etcd-single-node/docker-compose.yml");
    let compose = compose.to_string_lossy().to_string();

    // This job's ONLY purpose is to prove the real etcd store passes the shared
    // suite, so it must NEVER report success without actually having run it. If the
    // required tooling is missing we FAIL LOUD — locally as well as in CI — rather
    // than warn-and-return-`Ok` (a "false green": exit 0 having proved nothing).
    // The DETERMINISTIC, always-runnable proof of the same store lives in the `dst`
    // tier (madsim etcd simulator, in `ci`); this job is the real-etcd fidelity
    // backstop and is invoked deliberately, not from `ci`.
    if !docker_available() {
        return Err(
            "docker is not available but is required for the etcd conformance job; without it \
             this job proves nothing and must not report success. Install Docker (and the \
             compose plugin) to run it, or rely on the deterministic `cargo xtask dst` proof."
                .into(),
        );
    }
    // The real `etcd-client` regenerates its protobufs at build time; without a
    // system `protoc` the `--features etcd` build cannot even compile, so a run
    // would prove nothing.
    if !protoc_available() {
        return Err(
            "protoc is not available but is required to build `--features etcd` for the etcd \
             conformance job; without it this job proves nothing and must not report success. \
             Install the Protocol Buffers compiler (`protoc`), or rely on the deterministic \
             `cargo xtask dst` proof."
                .into(),
        );
    }

    etcd_compose(&compose, &["up", "-d"])?;
    let result = wait_for_port(ETCD_ENDPOINT).and_then(|()| run_etcd_conformance_test());
    // Always tear the stack down, even on failure — a run never leaks containers.
    let _ = etcd_compose(&compose, &["down", "-v", "--remove-orphans"]);
    result?;
    println!("\nxtask etcd-conformance: etcd passed the shared Coordination conformance suite");
    Ok(())
}

/// Run the endpoint-gated etcd conformance test with `--features etcd` and the
/// endpoint exported, with retry/backoff (etcd can lag its port opening).
///
/// COMPILE and RUN are separated deliberately. A build failure (e.g. the iter-5
/// E0599 missing-import in a file no `ci` gate compiles) is NOT a bootstrap flake:
/// retrying it 5× wastes minutes and — worse — reports it as "etcd may still be
/// bootstrapping", masquerading a hard defect as transient. So `--no-run` builds
/// first; a build failure fails LOUD and immediately, and only the actual test RUN
/// (which genuinely can race etcd's port coming up) is retried with backoff.
fn run_etcd_conformance_test() -> Result<(), String> {
    // 1. Build the test binary. A non-zero exit here is a compile error, never a
    //    bootstrap flake — surface it as such and do NOT retry.
    print_step(&[
        "cargo",
        "test",
        "-p",
        "wyrd-coordination-etcd",
        "--features",
        "etcd",
        "--test",
        "conformance",
        "--no-run",
    ]);
    let build = Command::new("cargo")
        .args([
            "test",
            "-p",
            "wyrd-coordination-etcd",
            "--features",
            "etcd",
            "--test",
            "conformance",
            "--no-run",
        ])
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;
    if !build.success() {
        return Err(format!(
            "the `--features etcd` conformance test failed to COMPILE ({build}); this is a build \
             error, not an etcd bootstrap flake — fix the code (it is not retried)."
        ));
    }

    // 2. Run the (already-built) test, retrying only the RUN — etcd can lag its
    //    port opening, which IS transient.
    print_step(&[
        "cargo",
        "test",
        "-p",
        "wyrd-coordination-etcd",
        "--features",
        "etcd",
        "--test",
        "conformance",
    ]);
    let mut last = String::new();
    for attempt in 1..=5 {
        let status = Command::new("cargo")
            .args([
                "test",
                "-p",
                "wyrd-coordination-etcd",
                "--features",
                "etcd",
                "--test",
                "conformance",
                "--",
                "--nocapture",
            ])
            .current_dir(workspace_root())
            .env("WYRD_ETCD_ENDPOINTS", format!("http://{ETCD_ENDPOINT}"))
            .status()
            .map_err(|e| format!("failed to spawn cargo: {e}"))?;
        if status.success() {
            return Ok(());
        }
        last = format!("etcd conformance test failed with {status}");
        eprintln!(
            "xtask etcd-conformance: run attempt {attempt}/5 failed; etcd may still be \
             bootstrapping (the test binary already built, so this is a run-time retry)"
        );
        std::thread::sleep(std::time::Duration::from_secs(3 * attempt));
    }
    Err(last)
}

/// Run a `docker compose -p <ETCD_PROJECT> -f <file> …` command from the workspace
/// root, echoing it first. Mirrors `tikv_compose`.
fn etcd_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", ETCD_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    print_step(&display);

    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{}` failed with {status}", display.join(" ")))
    }
}

/// Is a `protoc` binary reachable? (The real `etcd-client` build script needs it.)
fn protoc_available() -> bool {
    Command::new("protoc")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// The compose project name for the M4.5 "Small multi-node Production" stack, so it
/// is isolated and torn down cleanly (never collides with `TIKV_PROJECT`'s throwaway
/// single-node stack — the two are never meant to run at once).
const SMALL_MULTI_NODE_PROJECT: &str = "wyrd-small-multi-node-m45";
/// Every component endpoint the smoke check waits on (host-published ports; see
/// `deploy/small-multi-node/docker-compose.yml`): the 3-node etcd ensemble (L5
/// Coordination), the 3-node PD ensemble, the 3-node TiKV, the 9 local-disk D
/// servers, and the 3 S3 gateways. The 3 custodians publish no port (their Prometheus
/// registry is in-process read-back only), so their liveness is implied by the TiKV /
/// D servers they depend on rather than waited on directly.
const SMALL_MULTI_NODE_ENDPOINTS: &[&str] = &[
    // etcd ensemble (L5 Coordination)
    "127.0.0.1:12379",
    "127.0.0.1:22379",
    "127.0.0.1:32379",
    // PD ensemble (TiKV's coordinator)
    "127.0.0.1:23791",
    "127.0.0.1:23792",
    "127.0.0.1:23793",
    // TiKV (3-node store)
    "127.0.0.1:20160",
    "127.0.0.1:20161",
    "127.0.0.1:20162",
    // D servers (9, fd0..fd8)
    "127.0.0.1:50061",
    "127.0.0.1:50062",
    "127.0.0.1:50063",
    "127.0.0.1:50064",
    "127.0.0.1:50065",
    "127.0.0.1:50066",
    "127.0.0.1:50067",
    "127.0.0.1:50068",
    "127.0.0.1:50069",
    // S3 gateways (3)
    "127.0.0.1:8081",
    "127.0.0.1:8082",
    "127.0.0.1:8083",
];

/// Run the consolidated single-zone bring-up smoke check (proposal 0015
/// §"Deployment", PR sequence item 5, #256): bring up `deploy/small-multi-node/`
/// (3-node etcd for L5 Coordination + 3-node PD + 3-node TiKV + 9 local-disk D
/// servers + 3 custodians + 3 S3 gateways) and wait for every published component to
/// accept connections, then tear the stack down. **Not** part of `run_ci` — it needs
/// a container runtime, exactly like `run_tikv_conformance`.
///
/// This is a topology bring-up smoke check: it proves the whole stack stands up and
/// every published port accepts connections. The image is built with `--features
/// tikv,etcd`, so the D servers genuinely register through the etcd Coordination
/// backend (#449) and the custodians open the TiKV metadata backend. It does not
/// prove an end-to-end object write path: the `wyrd s3` gateway is still standalone
/// (#454) and nothing yet writes cluster metadata into TiKV for the custodian to
/// repair (#455).
fn run_deploy_small_multi_node() -> Result<(), String> {
    let compose = workspace_root().join("deploy/small-multi-node/docker-compose.yml");
    let compose = compose.to_string_lossy().to_string();

    if !docker_available() {
        if is_ci() {
            return Err(
                "docker is not available but is required for the small-multi-node bring-up".into(),
            );
        }
        eprintln!(
            "warning: docker not available; skipping the small-multi-node bring-up locally. \
             Install Docker (and the compose plugin) to run it."
        );
        return Ok(());
    }

    small_multi_node_compose(&compose, &["up", "-d"])?;
    let result = SMALL_MULTI_NODE_ENDPOINTS
        .iter()
        .try_for_each(|endpoint| wait_for_port(endpoint));
    // Always tear the stack down, even on failure — a run never leaks containers.
    let _ = small_multi_node_compose(&compose, &["down", "-v", "--remove-orphans"]);
    result?;
    println!(
        "\nxtask deploy-small-multi-node: the 3-node etcd ensemble + 3-node PD ensemble + \
         3-node TiKV + 9 D servers + 3 S3 gateways are all accepting connections \
         (custodians run without a published port)"
    );
    Ok(())
}

/// Run a `docker compose -p <SMALL_MULTI_NODE_PROJECT> -f <file> …` command from the
/// workspace root, echoing it first. Mirrors `tikv_compose`.
fn small_multi_node_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", SMALL_MULTI_NODE_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    print_step(&display);

    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{}` failed with {status}", display.join(" ")))
    }
}

/// Poll `host:port` until a TCP connection succeeds (PD is accepting clients), up to
/// ~30s. A bounded readiness gate so the test does not race the container's start.
fn wait_for_port(endpoint: &str) -> Result<(), String> {
    use std::net::{TcpStream, ToSocketAddrs};
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + Duration::from_secs(30);
    let addr = endpoint
        .to_socket_addrs()
        .map_err(|e| format!("could not resolve `{endpoint}`: {e}"))?
        .next()
        .ok_or_else(|| format!("`{endpoint}` resolved to no addresses"))?;
    loop {
        if TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "PD at `{endpoint}` did not accept connections within 30s"
            ));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Run a `docker compose -p <TIKV_PROJECT> -f <file> …` command from the workspace
/// root, echoing it first.
fn tikv_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", TIKV_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    print_step(&display);

    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{}` failed with {status}", display.join(" ")))
    }
}

/// Resolve the Tier-2 D-server count from a raw `WYRD_DSERVER_COUNT` value. An
/// integer >= 2 is honored (an `rs(6,3)` chunk needs >= 2 distinct servers to be
/// meaningfully spread); **any** other value — unset, unparsable, or `< 2` —
/// falls back to [`DSERVER_COUNT`]. A *rejected explicit* value returns a warning
/// string so the clamp is never silent (#154): a typo'd `WYRD_DSERVER_COUNT=1`
/// previously became 9 with no signal. Pure (input -> output) so it is unit-tested
/// without spawning a container.
fn resolve_dserver_count(raw: Option<String>) -> (usize, Option<String>) {
    match raw {
        None => (DSERVER_COUNT, None),
        Some(value) => match value.parse::<usize>() {
            Ok(n) if n >= 2 => (n, None),
            _ => (
                DSERVER_COUNT,
                Some(format!(
                    "WYRD_DSERVER_COUNT={value:?} is not a usable D-server count \
                     (need an integer >= 2); using the default {DSERVER_COUNT}"
                )),
            ),
        },
    }
}

/// Run `body`, then `finalize` it on **every** exit path — a normal return, an
/// `Err`, or a `panic!` unwind (#154). #150's [`finish_integration`] already
/// captures diagnostics before teardown, but it only runs if control *reaches*
/// it: a `panic!` in the test body unwinds straight past the call, leaking the
/// cluster and capturing nothing. `finalize_panic_safe` runs `body` under
/// `catch_unwind`, finalizes a panic as a failure (so `finalize` captures logs
/// **and** tears the cluster down), then resumes the unwind so the panic is not
/// swallowed. Generic over both actions so the panic-safety composes with
/// `finish_integration` and is unit-testable without a container runtime.
fn finalize_panic_safe<B, F>(body: B, finalize: F) -> Result<(), String>
where
    B: FnOnce() -> Result<(), String>,
    F: FnOnce(Result<(), String>) -> Result<(), String>,
{
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
    let (result, panic) = match outcome {
        Ok(result) => (result, None),
        Err(panic) => (
            Err("Tier-2 integration test panicked".to_string()),
            Some(panic),
        ),
    };
    let finished = finalize(result);
    if let Some(panic) = panic {
        std::panic::resume_unwind(panic);
    }
    finished
}

/// Finalize an integration run. On **failure**, capture container diagnostics
/// (`capture_logs`) **before** `teardown` destroys the cluster — the operability
/// invariant (#150): teardown must never precede log capture, or a failed
/// nightly run preserves nothing to diagnose it. `teardown` always runs
/// (best-effort) so a run never leaks containers, and `result` is propagated
/// unchanged. Generic over the two actions so the ordering is unit-testable
/// without a container runtime.
fn finish_integration<C, D>(
    result: Result<(), String>,
    capture_logs: C,
    teardown: D,
) -> Result<(), String>
where
    C: FnOnce(),
    D: FnOnce(),
{
    if result.is_err() {
        capture_logs();
    }
    teardown();
    result
}

/// Is a working Docker daemon reachable?
fn docker_available() -> bool {
    Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// `docker compose ... up -d --build --scale dserver=N`.
fn compose_up(compose: &str, count: usize) -> Result<(), String> {
    docker_compose(
        compose,
        &[
            "up",
            "-d",
            "--build",
            "--scale",
            &format!("dserver={count}"),
        ],
    )
}

/// `docker compose ... down -v --remove-orphans` (best-effort teardown).
fn compose_down(compose: &str) {
    let _ = docker_compose(compose, &["down", "-v", "--remove-orphans"]);
}

/// Capture `docker compose ... logs` before teardown (best-effort diagnostics,
/// #150). Echoes the logs to the job log for at-a-glance triage and persists a
/// copy under `target/tier2-logs/` for the workflow's `if: failure()` artifact
/// upload — so a failed nightly run's container diagnostics survive
/// `compose_down`. A capture failure only warns; it never masks the test result.
fn compose_logs(compose: &str) {
    let args = [
        "compose",
        "-p",
        TIER2_PROJECT,
        "-f",
        compose,
        "logs",
        "--no-color",
        "--timestamps",
    ];
    let mut display = vec!["docker"];
    display.extend_from_slice(&args);
    print_step(&display);

    let out = match Command::new("docker")
        .args(args)
        .current_dir(workspace_root())
        .output()
    {
        Ok(out) => out,
        Err(e) => {
            eprintln!("warning: failed to capture container logs: {e}");
            return;
        }
    };

    // Echo to the job log...
    print!("{}", String::from_utf8_lossy(&out.stdout));
    eprint!("{}", String::from_utf8_lossy(&out.stderr));

    // ...and persist a copy for the workflow's `if: failure()` artifact upload.
    let dir = workspace_root().join("target").join("tier2-logs");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("warning: could not create {}: {e}", dir.display());
        return;
    }
    let path = dir.join("docker-compose.log");
    let mut captured = out.stdout;
    captured.extend_from_slice(&out.stderr);
    if let Err(e) = std::fs::write(&path, &captured) {
        eprintln!("warning: could not write {}: {e}", path.display());
    }
}

/// Resolve each replica's published host port into a comma-separated endpoint
/// list (`http://127.0.0.1:PORT,…`) for `WYRD_DSERVER_ENDPOINTS`.
fn resolve_endpoints(compose: &str, count: usize) -> Result<String, String> {
    let mut endpoints = Vec::with_capacity(count);
    for index in 1..=count {
        let out = Command::new("docker")
            .args([
                "compose",
                "-p",
                TIER2_PROJECT,
                "-f",
                compose,
                "port",
                "--index",
                &index.to_string(),
                "dserver",
                "50051",
            ])
            .current_dir(workspace_root())
            .output()
            .map_err(|e| format!("failed to spawn docker compose port: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "docker compose port (index {index}) failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let mapped = String::from_utf8_lossy(&out.stdout);
        let port = mapped
            .trim()
            .rsplit(':')
            .next()
            .filter(|p| !p.is_empty())
            .ok_or_else(|| format!("could not parse host port from `{}`", mapped.trim()))?;
        endpoints.push(format!("http://127.0.0.1:{port}"));
    }
    Ok(endpoints.join(","))
}

/// Run the (otherwise `#[ignore]`d) Tier-2 integration test with the resolved
/// endpoints exported, so it dials the live container cluster.
fn run_integration_test(endpoints: &str) -> Result<(), String> {
    print_step(&[
        "cargo",
        "test",
        "-p",
        "wyrd-chunkstore-grpc",
        "--test",
        "tier2_integration",
        "--",
        "--ignored",
    ]);
    let status = Command::new("cargo")
        .args([
            "test",
            "-p",
            "wyrd-chunkstore-grpc",
            "--test",
            "tier2_integration",
            "--",
            "--ignored",
            "--nocapture",
        ])
        .current_dir(workspace_root())
        .env("WYRD_DSERVER_ENDPOINTS", endpoints)
        .status()
        .map_err(|e| format!("failed to spawn cargo: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("Tier-2 integration test failed with {status}"))
    }
}

/// Run a `docker compose -p <project> -f <file> …` command from the workspace
/// root, echoing it first.
fn docker_compose(compose: &str, args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", TIER2_PROJECT, "-f", compose];
    full.extend_from_slice(args);
    let mut display = vec!["docker"];
    display.extend_from_slice(&full);
    print_step(&display);

    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{}` failed with {status}", display.join(" ")))
    }
}

// ---- ADR-0035 DST-reachable global-mutable-state gate ----

/// Production crates the DST campaigns compile against — the set ADR-0035's rule covers.
/// `crates/dst` (the campaign substrate, incl. the sanctioned barrier) is scanned too.
/// NOTE (ADR-0035 §4 latent hazard): `crates/server` is deliberately ABSENT — it is not yet
/// DST-reachable. Its `Gateway` id allocators (`next_inode` / `next_chunk`, `AtomicU64`
/// *fields*, not statics, so not flagged below even if scanned) MUST move behind a seam
/// before any DST campaign exercises the server path; add `crates/server` here when it does.
const STATICS_SCAN_CRATES: &[&str] = &[
    "crates/core",
    "crates/traits",
    "crates/testkit",
    "crates/custodian",
    "crates/coordination-mem",
    // Now DST-reachable via `crates/dst/tests/coordination.rs`, which drives the
    // etcd store over the madsim etcd simulator (#365) — so it falls under the
    // ADR-0035 rule. It holds no process-global mutable state (only a `Mutex`
    // FIELD), so the scan passes.
    "crates/coordination-etcd",
    "crates/chunkstore-fs",
    "crates/chunkstore-grpc",
    "crates/chunk-format",
    "crates/metadata-redb",
    "crates/proto",
    "crates/dst",
];

/// Substrings that name process-global mutable state (ADR-0035 §1): a `static mut`, the
/// `lazy_static!` / `thread_local!` macros, or a `set_global_default` install. A bare
/// `Atomic*` / `Mutex` as a struct *field* is local, not global, so it is matched only when
/// it is the type of a `static` item (see [`statics_scan_line`]) — not flagged on its own.
const FORBIDDEN_NEEDLES: &[&str] = &[
    "static mut ",
    "lazy_static!",
    "thread_local!",
    "set_global_default",
];

/// Interior-mutable container types that turn a `static` item into shared mutable global
/// state (ADR-0035 §1). Matched only on a `static` declaration line.
const INTERIOR_MUT_TYPES: &[&str] = &["Mutex", "RwLock", "OnceLock", "OnceCell", "Atomic"];

/// Audited seed-safe occurrences permitted by ADR-0035 §4: `(path-fragment, label-fragment,
/// reason)`. A scan hit whose file path contains the fragment and whose label contains the
/// label-fragment is allowed. Keep this list short and every entry reasoned.
const STATICS_ALLOWLIST: &[(&str, &str, &str)] = &[(
    "crates/dst/src/lib.rs",
    "set_global_default",
    "the ADR-0035 determinism barrier itself — the one sanctioned global tracing default, \
     installed fail-loud exactly once before any campaign runs",
)];

/// Scan one source line for ADR-0035 §1 global-mutable-state patterns, returning a label
/// per hit. Comment lines are skipped so a doc/comment mention is not a false positive.
/// Pure (no IO) so it is unit-tested directly.
fn statics_scan_line(raw: &str) -> Vec<String> {
    let line = raw.trim_start();
    if line.starts_with("//") || line.starts_with('*') || line.starts_with("/*") {
        return Vec::new();
    }
    let mut hits = Vec::new();
    for needle in FORBIDDEN_NEEDLES {
        if raw.contains(needle) {
            hits.push((*needle).to_string());
        }
    }
    // A `static` ITEM (not a struct field) whose type is interior-mutable.
    let is_static_item = line.starts_with("static ")
        || line.starts_with("pub static ")
        || line.starts_with("pub(crate) static ");
    if is_static_item {
        for ty in INTERIOR_MUT_TYPES {
            if raw.contains(ty) {
                hits.push(format!("static holding {ty}"));
            }
        }
    }
    hits
}

fn statics_collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            statics_collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// ADR-0035: no DST-reachable shared mutable global state may influence a campaign outcome.
/// A lightweight grep-style gate (the ADR-0016 single-source style, alongside `cargo-deny`)
/// over the production crates the DST campaigns compile against — not a full reachability
/// analysis, which "reachable from `#[madsim::test]`" cannot be computed as. Paired with the
/// stated rule and review (ADR-0035 §4). Seed-safe exceptions live in [`STATICS_ALLOWLIST`].
fn run_statics() -> Result<(), String> {
    print_step(&["xtask", "statics", "(ADR-0035 DST global-state gate)"]);
    let root = workspace_root();
    let mut violations = Vec::new();
    for crate_dir in STATICS_SCAN_CRATES {
        let mut files = Vec::new();
        statics_collect_rs(&root.join(crate_dir).join("src"), &mut files);
        for file in files {
            let content = std::fs::read_to_string(&file)
                .map_err(|e| format!("statics: read {}: {e}", file.display()))?;
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            for (idx, raw) in content.lines().enumerate() {
                for label in statics_scan_line(raw) {
                    let allowed = STATICS_ALLOWLIST
                        .iter()
                        .any(|(p, n, _)| rel.contains(p) && label.contains(n));
                    if !allowed {
                        violations.push(format!("{rel}:{}: {label}", idx + 1));
                    }
                }
            }
        }
    }
    if violations.is_empty() {
        println!("xtask statics: no DST-reachable shared mutable global state (ADR-0035)");
        Ok(())
    } else {
        Err(format!(
            "ADR-0035 violation — DST-reachable shared mutable global state can defeat \
             seed-determinism:\n  {}\nInject the state through a testkit seam (Clock / \
             seed-derived RNG / fault traits), or — if audited seed-safe — add it to \
             STATICS_ALLOWLIST in xtask with a stated reason.",
            violations.join("\n  ")
        ))
    }
}

/// ADR-0010: no workspace crate may import an orchestrator/k8s API — the structural
/// guard the `deploy/` bring-up (M4.5, #256) depends on ("makes it hard for
/// orchestrator coupling to sneak into a component"). Scans every `.rs` file under
/// `crates/` via the shared `xtask::deploy_guard::scan_dir` (the SAME function
/// `xtask/tests/deploy_no_orchestrator_coupling.rs` drives over a planted fixture,
/// proving it load-bearing rather than resting red on non-existence).
fn run_orchestrator_guard() -> Result<(), String> {
    print_step(&[
        "xtask",
        "deploy-guard",
        "(ADR-0010 no-orchestrator-coupling gate)",
    ]);
    let violations = xtask::deploy_guard::scan_dir(&workspace_root().join("crates"));
    if violations.is_empty() {
        println!("xtask deploy-guard: no workspace crate imports an orchestrator API (ADR-0010)");
        Ok(())
    } else {
        Err(format!(
            "ADR-0010 violation — a crate imports an orchestrator/k8s API, breaking the \
             deployment-substrate pluggability invariant (\"Kubernetes is available, never \
             required\"):\n  {}\nMove the coupling behind a seam (peers are discovered \
             through L5, never an orchestrator API), or if the dependency is genuinely \
             needed, revisit ADR-0010 rather than working around it silently.",
            violations.join("\n  ")
        ))
    }
}

/// The ordered `cargo` steps of the CI gate, executed via the injected `exec`
/// (`run_ci` passes `cargo`; the unit test passes a recording closure so the real
/// wiring is exercised without spawning `cargo`).
///
/// `toolchain` is the injected **environment lookup** — `run_ci` passes
/// `std::env::var_os(..).is_some()`; the unit test passes a fixed set of declared
/// names. Reading the two feature gates *here*, by name
/// (`xtask::TIKV_TOOLCHAIN_ENV`, `xtask::FDB_TOOLCHAIN_ENV`), rather than accepting
/// two booleans from `run_ci`, is deliberate (#439): it leaves no call site in which
/// one backend's gate can be passed for the other's. The old shape — one
/// `tikv_toolchain` boolean gating the whole `feature_gated_checks()` list — would have
/// made the FDB typecheck fire only when `WYRD_TIKV_TOOLCHAIN` was also set, and no test
/// could have seen it, because the wrong wiring lived in `run_ci`'s untestable body.
///
/// Each gate is emitted **only** when its own toolchain is declared present, so the
/// default `cargo xtask ci` never compiles the pre-1.0 `tikv-client` tree
/// (`crates/metadata-tikv/Cargo.toml`) nor links `libfdb_c`
/// (`crates/metadata-fdb/Cargo.toml`), and stays green offline and container-free.
/// The row list itself lives in `xtask::feature_gated_checks` (the lib target) so
/// `xtask/tests/fdb_harness.rs` can assert its content directly.
fn run_ci_steps(
    toolchain: &mut dyn FnMut(&str) -> bool,
    exec: &mut dyn FnMut(&[&str]) -> Result<(), String>,
) -> Result<(), String> {
    // `wyrd-dst` only compiles under `--cfg madsim`; it is excluded from the
    // normal workspace commands and built solely by `run_dst`.
    exec(&["fmt", "--all", "--", "--check"])?;
    // Lint levels (incl. warnings-as-errors) come from `[workspace.lints]` in
    // the root Cargo.toml — the single source of truth — not a CLI flag here.
    exec(&[
        "clippy",
        "--workspace",
        "--exclude",
        "wyrd-dst",
        "--all-targets",
    ])?;
    exec(&[
        "build",
        "--workspace",
        "--exclude",
        "wyrd-dst",
        "--all-targets",
    ])?;
    exec(&["test", "--workspace", "--exclude", "wyrd-dst"])?;
    // Type-check the feature-gated backend bodies the default `--workspace` build skips
    // (`--all-targets` selects target KINDS, not features, so a `#[cfg(feature = "tikv")]`
    // / `#[cfg(feature = "fdb")]` body slips through). Each row is GATED on ITS OWN
    // toolchain (M4.6 #257; ADR-0042 #439): compiling the pre-1.0 `tikv-client` tree is
    // opt-in via WYRD_TIKV_TOOLCHAIN, linking `libfdb_c` is opt-in via
    // WYRD_FDB_TOOLCHAIN, and neither implies the other — so the default offline gate
    // stays container-free.
    for check in xtask::feature_gated_checks(
        toolchain(xtask::TIKV_TOOLCHAIN_ENV),
        toolchain(xtask::FDB_TOOLCHAIN_ENV),
    ) {
        exec(&check)?;
    }
    Ok(())
}

/// The full CI gate (ADR-0009). Each step runs in workspace order; the first
/// failure stops the run.
fn run_ci() -> Result<(), String> {
    run_ci_steps(&mut |name| std::env::var_os(name).is_some(), &mut |args| {
        cargo(args)
    })?;
    cargo_machete_check()?;
    cargo_deny_check()?;
    run_conformance()?;
    run_statics()?;
    run_orchestrator_guard()?;
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

/// Run the machine-checked dependency wall (ADR-0003 §2) — **both** graphs.
///
/// `cargo deny` audits the graph as resolved for the features it is invoked with, and the
/// backend features (`fdb`, `tikv`) are off by default. So one invocation is not enough:
///
/// 1. `cargo deny check` — the DEFAULT graph, i.e. the artifact we ship. Licenses,
///    advisories, bans, sources; zero tolerance, no backend exceptions (`deny.toml`).
/// 2. `cargo deny --all-features --config deny-all-features.toml check advisories` — the
///    OFF-BY-DEFAULT trees, which step 1 cannot see at all: neither `foundationdb` nor
///    `tikv-client` resolves into the default graph. Without this, a new advisory in the
///    abandoned `tikv-client` tree is caught by nothing (#543).
///
/// A separate config for step 2 on purpose: cargo-deny's `[advisories] ignore` entries are
/// keyed by advisory ID alone and apply to whatever graph they are used with, so parking the
/// tikv-client exceptions in `deny.toml` would let them suppress those same IDs if a future
/// DEFAULT dependency ever pulled an affected version — holing the shipped-artifact wall.
///
/// Both run HERE rather than in the workflow YAML, so `cargo xtask ci` remains the complete
/// local equivalent of the Rust gate (ADR-0009: CI is xtask-driven and runs the same checks
/// locally). A YAML-only audit would pass on a contributor's laptop and fail only in CI.
///
/// Step 2 costs seconds: cargo-deny resolves `Cargo.lock` and compiles nothing, so auditing
/// the optional trees needs none of their native toolchains (no `libfdb_c`, no cmake/protoc).
///
/// If cargo-deny is not installed locally, warn and skip; in CI (where `CI` is set) a missing
/// binary is a hard failure, so the wall is always enforced on every PR.
fn cargo_deny_check() -> Result<(), String> {
    let invocations: [&[&str]; 2] = [
        &["deny", "check"],
        &[
            "deny",
            "--all-features",
            "--config",
            "deny-all-features.toml",
            "check",
            "advisories",
        ],
    ];

    for args in invocations {
        let mut printed = vec!["cargo"];
        printed.extend_from_slice(args);
        print_step(&printed);

        let status = Command::new("cargo")
            .args(args)
            .current_dir(workspace_root())
            .status();

        match status {
            Ok(s) if s.success() => {}
            Ok(s) => return Err(format!("`cargo {}` failed with {s}", args.join(" "))),
            Err(_) if is_ci() => {
                return Err("cargo-deny is not installed but is required in CI".to_string());
            }
            Err(_) => {
                eprintln!(
                    "warning: cargo-deny not installed; skipping the license/advisory \
                     wall locally. Install it with `cargo install cargo-deny --locked`."
                );
                return Ok(());
            }
        }
    }

    Ok(())
}

/// Run `cargo machete` — flag dependencies declared in a `Cargo.toml` but never
/// referenced in source (ADR-0003 §2: keep the dependency/license surface small).
/// Mirrors `cargo_deny_check`: skip with a warning if cargo-machete is not
/// installed locally; in CI a missing binary is a hard failure so the check is
/// always enforced. A false positive (a dep used only behind a `cfg`) is silenced
/// per-crate with `[package.metadata.cargo-machete] ignored = ["crate-name"]`.
fn cargo_machete_check() -> Result<(), String> {
    // Probe the subcommand binary directly so "not installed" is cleanly
    // distinguishable from "found unused deps" (a non-zero exit of the real run).
    if Command::new("cargo-machete")
        .arg("--version")
        .output()
        .is_err()
    {
        if is_ci() {
            return Err("cargo-machete is not installed but is required in CI".to_string());
        }
        eprintln!(
            "warning: cargo-machete not installed; skipping the unused-dependency \
             check locally. Install it with `cargo install cargo-machete --locked`."
        );
        return Ok(());
    }
    // Invoke the binary DIRECTLY (`cargo-machete`), not `cargo machete`: some
    // cargo-machete builds don't strip the "machete" arg that the cargo
    // subcommand shim forwards, and then treat it as a path to scan ("No such
    // file or directory: machete"). With no args it scans the current dir.
    print_step(&["cargo-machete"]);
    let status = Command::new("cargo-machete")
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn cargo-machete: {e}"))?;
    match status.success() {
        true => Ok(()),
        false => Err(format!(
            "`cargo-machete` found unused dependencies (exit {status}). Remove them, \
             or mark intentional ones with `[package.metadata.cargo-machete] ignored`."
        )),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    // The operability invariant (#150): on a FAILED integration run, container
    // diagnostics must be captured BEFORE the cluster is torn down — teardown
    // never precedes log capture, or a failed nightly run preserves nothing to
    // diagnose it. Drives `finish_integration` with order-recording closures so
    // the ordering is asserted without a container runtime.
    #[test]
    fn failure_captures_logs_before_teardown() {
        let order: RefCell<Vec<&str>> = RefCell::new(Vec::new());
        let result = finish_integration(
            Err("Tier-2 integration test failed".to_string()),
            || order.borrow_mut().push("capture_logs"),
            || order.borrow_mut().push("teardown"),
        );
        assert!(result.is_err(), "the failure must be propagated unchanged");
        assert_eq!(
            *order.borrow(),
            vec!["capture_logs", "teardown"],
            "diagnostics must be captured before teardown destroys them",
        );
    }

    // Drive `run_ci`'s cargo wiring (via `run_ci_steps`, the sole cargo-step source
    // `run_ci` iterates) with a recording executor and a FAKE environment — no `cargo`
    // spawned, no process env touched — and return every argv it invokes.
    //
    // `declared` is the set of toolchain env vars that are "set". Because `run_ci_steps`
    // resolves both gates from this lookup ITSELF, reading `WYRD_TIKV_TOOLCHAIN` where it
    // should read `WYRD_FDB_TOOLCHAIN` (the #439 coupling hazard) is visible here.
    fn recorded_invocations(declared: &[&str]) -> Vec<String> {
        let mut calls = Vec::new();
        run_ci_steps(&mut |name| declared.contains(&name), &mut |args| {
            calls.push(args.join(" "));
            Ok(())
        })
        .expect("recording executor never errors");
        calls
    }

    // The feature list is matched EXACTLY (`--features <list> `, trailing space), not by
    // `contains`: a bare `contains("--features tikv")` also matches "--features tikv,etcd",
    // so it could not tell the two apart — and telling them apart is the whole point of the
    // `tikv,etcd` row (the gateway's dispatch arm is `cfg(all(tikv, etcd))`). A guard that
    // passes under both spellings guards nothing.
    fn is_feature_check(pkg: &'static str, feature: &'static str) -> impl Fn(&String) -> bool {
        move |c: &String| {
            c.starts_with("check ")
                && c.contains(&format!("-p {pkg}"))
                && c.contains(&format!("--features {feature} "))
                && c.contains("--tests")
        }
    }

    // M4.6 (#257): exercise `run_ci`'s REAL wiring with a recording executor and assert
    // the invocations it actually makes. This is not the iter-10 tautology (which
    // restated the `feature_gated_checks` constant and stayed green if the wiring loop
    // was deleted): here removing the step from the wiring OR making the gate
    // unconditional flips a case red.
    #[test]
    fn ci_type_checks_feature_gated_metadata_scenario() {
        // Toolchain present → run_ci INVOKES the metadata feature checks, so the
        // #[cfg(feature = "tikv")] Tier-1/Tier-2 scenario bodies (SymmetricPartition,
        // the PD oracle, the testkit-oracle wiring) are type-checked at Check.
        //
        // BOTH packages, not just the backend crate: the `Tikv` variant of
        // `MetadataBackend` and its selection arms live in `crates/server/src/cli.rs`
        // behind `#[cfg(feature = "tikv")]`, so a bar that checks only
        // `wyrd-metadata-tikv` lets the CLI wiring rot. #443 retains that variant, so
        // the anti-rot bar must compile it (this mirrors the `fdb` rows).
        //
        // And the SERVER row is `tikv,etcd`, not `tikv`: the S3 gateway's dispatch arm is
        // `#[cfg(all(feature = "tikv", feature = "etcd"))]`, so `--features tikv` alone
        // cfg's out the exact pairing the retained-fallback stack runs
        // (`deploy/small-multi-node/`, FEATURES="tikv,etcd"). The feature strings are
        // asserted EXACTLY here — `contains("--features tikv")` would also match
        // "tikv,etcd" and so could not tell the two apart, which is a guard that cannot
        // fail.
        let with_toolchain = recorded_invocations(&[xtask::TIKV_TOOLCHAIN_ENV]);
        for (pkg, features) in [("wyrd-metadata-tikv", "tikv"), ("wyrd-server", "tikv,etcd")] {
            let is_tikv_check = is_feature_check(pkg, features);
            assert!(
                with_toolchain.iter().any(&is_tikv_check),
                "run_ci must invoke `cargo check -p {pkg} --features {features} --tests` when \
                 the TiKV toolchain is present, so the feature-gated TiKV surface — including \
                 the cli.rs selection arms AND the tikv×etcd gateway dispatch arm (#443's \
                 retained fallback, the shape `deploy/small-multi-node/` actually runs) — is \
                 type-checked at Check: {with_toolchain:?}"
            );

            // Gate honesty: toolchain absent (a laptop / PDCA worktree) → run_ci must
            // NOT compile the pre-1.0 `tikv-client` tree — the container-free/offline
            // CI invariant. Making either step unconditional flips this red.
            let without_toolchain = recorded_invocations(&[]);
            assert!(
                !without_toolchain.iter().any(&is_tikv_check),
                "the default no-TiKV `cargo xtask ci` must not compile the tikv feature tree \
                 (WYRD_TIKV_TOOLCHAIN unset), pkg={pkg}: {without_toolchain:?}"
            );
        }
    }

    // ADR-0042 (#439): the same wiring assertion for the `fdb` rows, driven through the
    // env lookup `run_ci` really uses. This is the test that binds the coupling hazard the
    // brief names: with ONLY `WYRD_FDB_TOOLCHAIN` declared, both fdb rows must fire. Make
    // the fdb gate read `WYRD_TIKV_TOOLCHAIN` (or gate the loop on the tikv boolean, the
    // pre-#439 shape) and this goes red.
    #[test]
    fn ci_type_checks_the_fdb_feature_on_the_fdb_toolchain_alone() {
        let fdb_only = recorded_invocations(&[xtask::FDB_TOOLCHAIN_ENV]);
        // The server row is `fdb,etcd`, not `fdb` — same reason as the tikv row: the S3
        // gateway's dispatch arm is `#[cfg(all(feature = "fdb", feature = "etcd"))]`, and
        // that pairing is what the CANONICAL production stack runs
        // (`deploy/small-multi-node-fdb/`, FEATURES="fdb,etcd"). A plain `--features fdb`
        // check left it compiled by no CI job at all.
        for (pkg, features) in [("wyrd-metadata-fdb", "fdb"), ("wyrd-server", "fdb,etcd")] {
            let is_fdb_check = is_feature_check(pkg, features);
            assert!(
                fdb_only.iter().any(&is_fdb_check),
                "run_ci must invoke `cargo check -p {pkg} --features {features} --tests` when \
                 the FDB toolchain is declared, independently of WYRD_TIKV_TOOLCHAIN: \
                 {fdb_only:?}"
            );
        }
        assert!(
            !fdb_only.iter().any(|c| c.contains("--features tikv")),
            "the FDB toolchain must not drag in the pre-1.0 tikv-client tree: {fdb_only:?}"
        );

        // The converse: a TiKV-only runner must not link `libfdb_c`.
        let tikv_only = recorded_invocations(&[xtask::TIKV_TOOLCHAIN_ENV]);
        assert!(
            !tikv_only.iter().any(|c| c.contains("--features fdb")),
            "the TiKV toolchain must not compile the fdb feature tree (no libfdb_c there): \
             {tikv_only:?}"
        );

        // And the default gate compiles neither.
        let neither = recorded_invocations(&[]);
        assert!(
            !neither.iter().any(|c| c.contains("--features")),
            "the default `cargo xtask ci` must compile no feature-gated backend tree: \
             {neither:?}"
        );
    }

    // ADR-0035 statics gate: it must catch process-global mutable state but NOT a bare
    // atomic/mutex used as a struct field (local, seed-safe — e.g. the `Gateway` allocators
    // or `ManualClock`), and it must ignore comments so a doc mention is not a false hit.
    #[test]
    fn statics_gate_flags_globals_not_fields() {
        // Global mutable state — every forbidden shape is caught.
        assert!(!statics_scan_line("static mut COUNTER: u64 = 0;").is_empty());
        assert!(
            !statics_scan_line("thread_local! { static X: Cell<u8> = Cell::new(0); }").is_empty()
        );
        assert!(!statics_scan_line("    let _ = set_global_default(reg);").is_empty());
        assert!(
            !statics_scan_line("static REG: Mutex<Vec<u8>> = Mutex::new(Vec::new());").is_empty()
        );
        assert!(!statics_scan_line("static N: AtomicU64 = AtomicU64::new(0);").is_empty());

        // A struct FIELD atomic/mutex is local, not a global static — never flagged.
        assert!(statics_scan_line("    next_inode: AtomicU64,").is_empty());
        assert!(statics_scan_line("    kv: Mutex<HashMap<Vec<u8>, Bytes>>,").is_empty());
        // A plain immutable static (no interior mutability) is fine.
        assert!(statics_scan_line("static DST_SEEDS: &str = \"50\";").is_empty());
        // Comments are skipped — a doc mention of a forbidden token is not a violation.
        assert!(statics_scan_line("/// uses set_global_default in the barrier").is_empty());
        assert!(statics_scan_line("// static mut FOO: u8 = 0;").is_empty());
    }

    // The one sanctioned exception (the barrier) is allowlisted, and the allowlist is keyed
    // on path + label so it cannot accidentally permit the same token elsewhere.
    #[test]
    fn statics_allowlist_covers_only_the_barrier() {
        let label = "set_global_default";
        let allowed = |rel: &str| {
            STATICS_ALLOWLIST
                .iter()
                .any(|(p, n, _)| rel.contains(p) && label.contains(n))
        };
        assert!(
            allowed("crates/dst/src/lib.rs"),
            "the barrier is allowlisted"
        );
        assert!(
            !allowed("crates/custodian/src/telemetry.rs"),
            "the same token elsewhere is NOT allowlisted"
        );
    }

    // A passing run needs no diagnostics: only teardown runs, and Ok is
    // propagated. (Guards against a fix that always captures, which would noise
    // up every green nightly run.)
    #[test]
    fn success_tears_down_without_capturing_logs() {
        let order: RefCell<Vec<&str>> = RefCell::new(Vec::new());
        let result = finish_integration(
            Ok(()),
            || order.borrow_mut().push("capture_logs"),
            || order.borrow_mut().push("teardown"),
        );
        assert!(result.is_ok(), "a passing run must stay Ok");
        assert_eq!(
            *order.borrow(),
            vec!["teardown"],
            "a passing run captures no logs; only teardown runs",
        );
    }

    // #154 item 4 — `WYRD_DSERVER_COUNT` must not clamp a rejected value silently.

    #[test]
    fn dserver_count_unset_uses_default_silently() {
        assert_eq!(resolve_dserver_count(None), (DSERVER_COUNT, None));
    }

    #[test]
    fn dserver_count_accepts_a_valid_value_silently() {
        assert_eq!(resolve_dserver_count(Some("4".to_string())), (4, None));
        // The boundary (the smallest meaningful spread) is accepted.
        assert_eq!(resolve_dserver_count(Some("2".to_string())), (2, None));
    }

    #[test]
    fn dserver_count_rejects_unusable_values_with_a_warning() {
        // `0`/`1` (too few to spread), unparsable garbage, and empty all fall
        // back to the default — but each must surface a warning, not clamp silently.
        for bad in ["0", "1", "garbage", ""] {
            let (count, warning) = resolve_dserver_count(Some(bad.to_string()));
            assert_eq!(count, DSERVER_COUNT, "{bad:?} should fall back to default");
            let warning = warning.expect("a rejected value must warn, not clamp silently");
            assert!(
                warning.contains("WYRD_DSERVER_COUNT"),
                "warning should name the variable: {warning:?}"
            );
        }
    }

    // #154 item 2 — finalization must run even when the test body PANICS, and the
    // panic must still resume. Composed with #150's `finish_integration`, this
    // proves the two invariants hold together: a panicking run still captures
    // container diagnostics BEFORE teardown (#150) and never leaks the cluster
    // (#154). A `panic!` unwinding past finalization is exactly what would leak it.

    #[test]
    fn panic_finalizes_capture_then_teardown_then_resumes() {
        let order: RefCell<Vec<&str>> = RefCell::new(Vec::new());
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            finalize_panic_safe(
                || panic!("integration test panicked"),
                |result| {
                    finish_integration(
                        result,
                        || order.borrow_mut().push("capture_logs"),
                        || order.borrow_mut().push("teardown"),
                    )
                },
            )
        }));
        assert!(outcome.is_err(), "the panic must resume, not be swallowed");
        assert_eq!(
            *order.borrow(),
            vec!["capture_logs", "teardown"],
            "a panicking run must still capture diagnostics before teardown (#150 + #154)",
        );
    }

    #[test]
    fn clean_run_finalizes_without_capturing_or_panicking() {
        let order: RefCell<Vec<&str>> = RefCell::new(Vec::new());
        let result = finalize_panic_safe(
            || Ok(()),
            |result| {
                finish_integration(
                    result,
                    || order.borrow_mut().push("capture_logs"),
                    || order.borrow_mut().push("teardown"),
                )
            },
        );
        assert!(result.is_ok(), "a passing run stays Ok");
        assert_eq!(
            *order.borrow(),
            vec!["teardown"],
            "a passing run captures no logs; only teardown runs",
        );
    }

    #[test]
    fn err_return_finalizes_capture_then_teardown_and_propagates() {
        // A non-panic failure (`Err`) is finalized exactly like #150: capture
        // before teardown, error propagated unchanged.
        let order: RefCell<Vec<&str>> = RefCell::new(Vec::new());
        let result = finalize_panic_safe(
            || Err("boom".to_string()),
            |result| {
                finish_integration(
                    result,
                    || order.borrow_mut().push("capture_logs"),
                    || order.borrow_mut().push("teardown"),
                )
            },
        );
        assert_eq!(result, Err("boom".to_string()));
        assert_eq!(
            *order.borrow(),
            vec!["capture_logs", "teardown"],
            "an Err return captures before teardown, like #150",
        );
    }
}
