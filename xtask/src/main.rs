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
//! - `disk-faults` / `jepsen` / `kill-reconstruct` — the deferred (off-Check)
//!   Tier-1 / Tier-2 custodian fault runners (M3, proposal 0005 `0005:405-411`,
//!   `0005:437-438`). Privileged / real-environment tiers, never part of `ci`;
//!   deferred by default, opted in by the dedicated off-Check job.
//! - `bench` — the tracked throughput benchmarks (EC micro-bench + the M2
//!   aggregate D-server throughput bench). Tracked, not gated.

#![forbid(unsafe_code)]

mod conformance;
mod faults;
mod kill_reconstruct;
mod vectors;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

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
        Some("disk-faults") => faults::run_disk_faults(),
        Some("jepsen") => faults::run_jepsen(),
        Some("kill-reconstruct") => faults::run_kill_reconstruct(),
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
         <ci|conformance|gen-vectors|dst|statics|integration|tikv-conformance|disk-faults|jepsen|kill-reconstruct|bench>"
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
/// Two test binaries run: `conformance` (the shared trait-contract suite, M4.1) and
/// `contention` (the write-conflict property tests, M4.2/#253). Both must pass for
/// the job to exercise the atomic-commit conflict semantics this slice hardens.
fn run_tikv_conformance_test() -> Result<(), String> {
    for test in ["conformance", "contention"] {
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

/// The full CI gate (ADR-0009). Each step runs in workspace order; the first
/// failure stops the run.
fn run_ci() -> Result<(), String> {
    // `wyrd-dst` only compiles under `--cfg madsim`; it is excluded from the
    // normal workspace commands and built solely by `run_dst` below.
    cargo(&["fmt", "--all", "--", "--check"])?;
    // Lint levels (incl. warnings-as-errors) come from `[workspace.lints]` in
    // the root Cargo.toml — the single source of truth — not a CLI flag here.
    cargo(&[
        "clippy",
        "--workspace",
        "--exclude",
        "wyrd-dst",
        "--all-targets",
    ])?;
    cargo(&[
        "build",
        "--workspace",
        "--exclude",
        "wyrd-dst",
        "--all-targets",
    ])?;
    cargo(&["test", "--workspace", "--exclude", "wyrd-dst"])?;
    cargo_machete_check()?;
    cargo_deny_check()?;
    run_conformance()?;
    run_statics()?;
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
