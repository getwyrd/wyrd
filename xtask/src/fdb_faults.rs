//! **The FoundationDB Tier-1 fault + contention battery runner** (#442 — the go/no-go gate for
//! making FDB the production metadata backend, ADR-0042).
//!
//! The FDB peer of `faults::run_metadata_scenario` (TiKV's Tier-1 runner), and it exists
//! because the TiKV one cannot be pointed here: FDB needs two steps TiKV does not (a one-time
//! `configure new double ssd`, and a host-side cluster file), and its fault-effect oracle asks
//! `fdbcli status json` rather than PD.
//!
//! What it stands up: `deploy/fdb-multi-replica` — three `fdbserver` processes, each in its own
//! network namespace on a bridge with a static IP (`FDB_NETWORKING_MODE: container`), all three
//! coordinators. The netns topology is what makes an in-container `iptables` cut bite: under
//! host networking the processes would source their traffic from a shared loopback and a per-IP
//! cut would be a provable no-op.
//!
//! The three legs, all of which must pass for a "go":
//!
//! * `tier1_metadata_consistency` — the SHARED Tier-1 scenario (`wyrd-metadata-fault-conformance`, the same
//!   code TiKV is judged by) with the **master-role process symmetrically isolated** mid-flight.
//! * `tier1_contention` — rename races, the inode-allocator hot path, blind-batch storms.
//! * `tier1_kill_mid_commit` — `SIGKILL` the master mid-commit: the only way to induce a real
//!   `1021 commit_unknown_result`, and the only place a silently-retried non-idempotent batch
//!   would show up as a double-apply.
//!
//! Privileged and **opt-in** (`WYRD_TIER1=1`), like every other fault runner: opted-in but
//! tool-missing is a **hard error**, never a silent skip — a battery that quietly did not run
//! is worse than one that failed.

use std::process::Command;

use crate::{print_step, workspace_root};

/// The compose project — distinct from `fdb-conformance`'s, so the two never collide.
const FDB_TIER1_PROJECT: &str = "wyrd-fdb-tier1-metadata";
const FDB_TIER1_COMPOSE: &str = "deploy/fdb-multi-replica/docker-compose.yml";

/// The three processes' static IPs (`deploy/fdb-multi-replica/docker-compose.yml`) and the
/// compose service each belongs to. The cluster file and the netns map are both derived from
/// this ONE table, so a compose edit that moved an IP cannot leave them disagreeing.
const FDB_TIER1_NODES: [(&str, &str); 3] = [
    ("fdb0", "172.30.58.11"),
    ("fdb1", "172.30.58.12"),
    ("fdb2", "172.30.58.13"),
];
const FDB_TIER1_PORT: &str = "4500";

/// The fault-agent image (an `iptables` entrypoint), reused as-is from the TiKV Tier-1 leg.
const IPTABLES_IMAGE: &str = "wyrd-iptables:local";
const IPTABLES_AGENT_DIR: &str = "deploy/tikv-multi-replica/iptables-agent";

/// The scenario test binaries, in the order they run. Contention first: it is the cheap half
/// and needs no fault, so a broken *workload* fails fast before the expensive fault legs.
const FDB_TIER1_LEGS: [&str; 3] = [
    "tier1_contention",
    "tier1_metadata_consistency",
    "tier1_kill_mid_commit",
];

/// `docker:docker@172.30.58.11:4500,172.30.58.12:4500,172.30.58.13:4500`
fn cluster_file_contents() -> String {
    let coordinators: Vec<String> = FDB_TIER1_NODES
        .iter()
        .map(|(_, ip)| format!("{ip}:{FDB_TIER1_PORT}"))
        .collect();
    format!("docker:docker@{}", coordinators.join(","))
}

/// `172.30.58.11=<container>,172.30.58.12=<container>,…` — the ip→netns map the scenario cuts
/// inside, and the topology the legs resolve the master from.
fn netns_map() -> Result<String, String> {
    let mut pairs = Vec::new();
    for (service, ip) in FDB_TIER1_NODES {
        pairs.push(format!("{ip}={}", container_of(service)?));
    }
    Ok(pairs.join(","))
}

/// The container name compose gave a service. Resolved at runtime rather than assumed: compose
/// derives it from the project name and a replica index, and hard-coding that convention is how
/// a runner silently cuts nothing (the container it names does not exist, `docker run
/// --network=container:…` fails, and the fault is recorded as not materialized).
fn container_of(service: &str) -> Result<String, String> {
    let out = Command::new("docker")
        .args([
            "compose",
            "-p",
            FDB_TIER1_PROJECT,
            "-f",
            FDB_TIER1_COMPOSE,
            "ps",
            "-q",
            service,
        ])
        .current_dir(workspace_root())
        .output()
        .map_err(|e| format!("failed to spawn docker compose ps: {e}"))?;
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if id.is_empty() {
        return Err(format!(
            "compose service `{service}` has no running container — the FDB Tier-1 cluster did \
             not come up"
        ));
    }
    // `docker run --network=container:<id>` accepts the id, so the id is enough — and it is
    // stable against any naming convention change in compose.
    Ok(id)
}

fn compose(args: &[&str]) -> Result<(), String> {
    let mut full = vec!["compose", "-p", FDB_TIER1_PROJECT, "-f", FDB_TIER1_COMPOSE];
    full.extend_from_slice(args);
    print_step(&{
        let mut display = vec!["docker"];
        display.extend_from_slice(&full);
        display
    });
    let status = Command::new("docker")
        .args(&full)
        .current_dir(workspace_root())
        .status()
        .map_err(|e| format!("failed to spawn docker compose: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "docker compose {} failed with {status}",
            args.join(" ")
        ))
    }
}

/// Create the database once (a fresh FDB cluster reports `configuration missing` until told
/// what it is), then poll until it is genuinely available.
///
/// `double ssd`, not the conformance stack's `single memory`: the whole point of this tier is
/// that a process can be lost, which requires a redundancy mode that tolerates one.
fn configure_database() -> Result<(), String> {
    let _ = compose(&[
        "exec",
        "-T",
        "fdb0",
        "fdbcli",
        "--exec",
        "configure new double ssd",
    ]);
    let mut last_seen = String::new();
    for attempt in 1..=45 {
        // **Readiness is read out of the TEXT, never the exit status.** `fdbcli` 7.3.77 exits 0
        // against a dead coordinator — this repo established that and acts on it everywhere else
        // (`main.rs`'s `probe_cluster_health`, `fdb_doctor::cluster_status_is_healthy`), so
        // `status.success()` is not a health predicate. Trusting it here would let the runner
        // march into the fault legs against a cluster that is still `configuration missing` or
        // unavailable, and report *test* failures for what is really a SETUP failure — the most
        // expensive kind of misleading red there is, because it indicts the driver for the
        // harness's mistake. (Codex review of #535.)
        //
        // The predicate is `fdb_doctor`'s own, so this runner and the doctor cannot drift about
        // what "healthy" means — and it is the one that knows `The database is unavailable`
        // contains the substring "available".
        let output = Command::new("docker")
            .args([
                "compose",
                "-p",
                FDB_TIER1_PROJECT,
                "-f",
                FDB_TIER1_COMPOSE,
                "exec",
                "-T",
                "fdb0",
                "fdbcli",
                "--timeout",
                "5",
                "--exec",
                "status minimal",
            ])
            .current_dir(workspace_root())
            .output()
            .map_err(|e| format!("failed to spawn docker: {e}"))?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        if crate::fdb_doctor::cluster_status_is_healthy(&text) {
            return Ok(());
        }
        last_seen = text.trim().to_string();
        eprintln!(
            "xtask fdb-metadata-tier1: cluster not available yet (attempt {attempt}/45): {last_seen}"
        );
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
    // Fail as a SETUP error, naming what the cluster actually said — never let the battery run on.
    Err(format!(
        "the FoundationDB cluster did not report `database is available` within 90s; last \
         status was: {last_seen}"
    ))
}

/// The host-side cluster file the `foundationdb` client dials. Lands under `target/` (build
/// output, git-ignored), never in the source tree.
fn write_cluster_file() -> Result<String, String> {
    let dir = workspace_root().join("target/fdb-multi-replica");
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let path = dir.join("fdb.cluster");
    std::fs::write(&path, cluster_file_contents())
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(path.to_string_lossy().to_string())
}

/// Build the fault agent BEFORE the cluster, so a broken agent fails fast rather than
/// mid-partition (with a cut applied and no way to heal it).
fn build_fault_agent() -> Result<(), String> {
    let dir = workspace_root()
        .join(IPTABLES_AGENT_DIR)
        .to_string_lossy()
        .to_string();
    let args = ["build", "-t", IPTABLES_IMAGE, dir.as_str()];
    print_step(&{
        let mut display = vec!["docker"];
        display.extend_from_slice(&args);
        display
    });
    let status = Command::new("docker")
        .args(args)
        .status()
        .map_err(|e| format!("failed to spawn docker build for the fault agent: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("fault-agent image build failed with {status}"))
    }
}

/// Run one `#[ignore]`d scenario binary with the `fdb` feature on and the fault env exported.
fn run_leg(leg: &str, cluster_file: &str, netns_map: &str) -> Result<(), String> {
    print_step(&[
        "cargo",
        "test",
        "-p",
        "wyrd-metadata-fdb",
        "--features",
        "fdb",
        "--test",
        leg,
        "--",
        "--ignored",
        "--nocapture",
    ]);
    let status = Command::new("cargo")
        .args([
            "test",
            "-p",
            "wyrd-metadata-fdb",
            "--features",
            "fdb",
            "--test",
            leg,
            "--",
            "--ignored",
            "--nocapture",
        ])
        .current_dir(workspace_root())
        .env("WYRD_FDB_CLUSTER_FILE", cluster_file)
        .env("WYRD_TIER1_NETNS_MAP", netns_map)
        .env("WYRD_TIER1_FDB_PORT", FDB_TIER1_PORT)
        .env("WYRD_TIER1_IPTABLES_IMAGE", IPTABLES_IMAGE)
        .env("WYRD_TIER1_REPLICAS", FDB_TIER1_NODES.len().to_string())
        .env("WYRD_TIER1_ISOLATED", "1")
        .env("WYRD_TIER1_CONTENDERS", contenders())
        .status()
        .map_err(|e| format!("failed to spawn cargo test: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("FDB Tier-1 leg `{leg}` failed with {status}"))
    }
}

fn contenders() -> String {
    std::env::var("WYRD_TIER1_CONTENDERS").unwrap_or_else(|_| "4".to_string())
}

/// Bring up the 3-process FDB cluster, configure it, and drive the whole battery. Tears the
/// stack down unconditionally — including on a panic, so a failed leg never leaves a cut
/// cluster (or a container) behind.
pub fn run_fdb_metadata_tier1() -> Result<(), String> {
    if std::env::var("WYRD_TIER1").as_deref() != Ok("1") {
        println!(
            "xtask fdb-metadata-tier1: DEFERRED — set WYRD_TIER1=1 to run the privileged FDB \
             fault battery (#442's go/no-go gate). It stands up a 3-process FoundationDB \
             cluster, cuts and kills processes, and needs Docker."
        );
        return Ok(());
    }
    if !docker_available() {
        // Opted in but the tool is missing: a HARD error. A battery that quietly did not run is
        // worse than one that failed — it would be recorded as a "go" nobody earned.
        return Err(
            "WYRD_TIER1=1 but `docker` is not available — the FDB fault battery cannot run, and \
             skipping it silently would report a verdict that was never tested"
                .into(),
        );
    }

    build_fault_agent()?;
    compose(&["up", "-d"])?;

    let result = crate::finalize_panic_safe(
        || {
            configure_database()?;
            let cluster_file = write_cluster_file()?;
            let netns_map = netns_map()?;
            println!("xtask fdb-metadata-tier1: netns map = {netns_map}");
            for leg in FDB_TIER1_LEGS {
                run_leg(leg, &cluster_file, &netns_map)?;
            }
            Ok(())
        },
        |result| {
            // Unconditional teardown — a failed leg must never leave a CUT cluster (or a
            // container) behind, and the original error must survive the teardown.
            let _ = compose(&["down", "-v", "--remove-orphans"]);
            result
        },
    );

    result?;
    println!(
        "\nxtask fdb-metadata-tier1: FoundationDB passed the Tier-1 fault + contention battery \
         (#442) — consistency under a master isolation, the contention workloads, and a \
         mid-commit kill"
    );
    Ok(())
}

fn docker_available() -> bool {
    Command::new("docker")
        .args(["version"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// The compose override the clock-skew nemesis leg recreates the target node with (Design §3/§5).
const FDB_TIER1_FAKETIME_OVERRIDE: &str = "deploy/fdb-multi-replica/docker-compose.faketime.yml";
/// The node the skew leg recreates with a fake clock — MUST match the service the faketime
/// override targets (`docker-compose.faketime.yml`), so override/recreate/probe cannot disagree.
const FDB_TIER1_SKEW_SERVICE: &str = "fdb2";

/// A **surviving** service the skew leg queries `status json` in to confirm the cluster fully
/// recovered after each force-recreate (never the skewed node itself, which is mid-recreate) — the
/// first declared node other than [`FDB_TIER1_SKEW_SERVICE`].
fn fdb_tier1_skew_survivor_service() -> &'static str {
    FDB_TIER1_NODES
        .iter()
        .map(|(service, _)| *service)
        .find(|service| *service != FDB_TIER1_SKEW_SERVICE)
        .expect("the 3-node cluster always has a survivor other than the skew target")
}

/// The **stable compose container name** of a service (`docker compose ps --format '{{.Name}}'`),
/// as opposed to the ephemeral container **id** [`container_of`] returns (`ps -q`).
///
/// The distinction is load-bearing for the nemesis and is the fix for the two prior iterations'
/// live-skew defect: the clock-skew leg `--force-recreate`s its target node, which mints a **new
/// container id** but keeps the **same compose name** (`<project>-<service>-<index>`, deterministic
/// and reused on recreate). A campaign that resolved the skew target — or the whole netns map — as
/// an id ONCE, pre-campaign, would then `docker exec`/`docker pause` a container that no longer
/// exists after the recreate (id invalidated), and every later leg would fail to materialize. A
/// name survives the recreate, and `docker exec|pause|inspect <name>` and
/// `docker run --network=container:<name>` all accept it. We still ASK compose for the name rather
/// than hard-code the convention, so a compose naming change cannot silently point us at nothing.
fn container_name_of(service: &str) -> Result<String, String> {
    let out = Command::new("docker")
        .args([
            "compose",
            "-p",
            FDB_TIER1_PROJECT,
            "-f",
            FDB_TIER1_COMPOSE,
            "ps",
            "--format",
            "{{.Name}}",
            service,
        ])
        .current_dir(workspace_root())
        .output()
        .map_err(|e| format!("failed to spawn docker compose ps: {e}"))?;
    // Surface a genuine command failure with its stderr, rather than masking it as "the cluster did
    // not come up": a `docker compose ps` that FAILED (daemon hiccup, or a compose version without
    // Go-template `--format` support) also yields empty stdout, and every other shell-out in this
    // file reports the failure — so must this one.
    if !out.status.success() {
        return Err(format!(
            "`docker compose ps {service}` failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim(),
        ));
    }
    // Take the first non-empty line: with one replica `ps --format '{{.Name}}'` prints exactly the
    // container name; guard against a trailing blank line or a stray header.
    let name = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or_default()
        .to_string();
    if name.is_empty() {
        return Err(format!(
            "compose service `{service}` has no running container — the FDB Tier-1 cluster did \
             not come up"
        ));
    }
    Ok(name)
}

/// The nemesis's ip→container **name** map — the same shape as [`netns_map`] but resolved to stable
/// compose names, not ephemeral ids, so it stays valid after the clock-skew leg force-recreates its
/// node (see [`container_name_of`]). A separate builder from the #442 battery's [`netns_map`] so
/// the battery's proven id-based path is left untouched.
fn nemesis_netns_map() -> Result<String, String> {
    let mut pairs = Vec::new();
    for (service, ip) in FDB_TIER1_NODES {
        pairs.push(format!("{ip}={}", container_name_of(service)?));
    }
    Ok(pairs.join(","))
}

/// **The composable-nemesis runner** (#407): stand up `deploy/fdb-multi-replica`, then drive each
/// of the three nemesis legs (partition / clock-skew / process-pause) through
/// `wyrd-metadata-fault-conformance`'s `drive_leg`, which enforces the #442 gates
/// (materialized-or-inconclusive, complete-heal). This is the runnable entry point that makes the
/// brief's sign-off open question — one witnessed `WYRD_TIER1=1` run of the three legs —
/// satisfiable; #408 composes the *checked* workload under these same legs.
///
/// Tears the stack down unconditionally (including on a panic, and including a bring-up that
/// fails after creating part of the stack — `compose up` runs inside the finalizer's scope), so
/// neither a failed leg nor a half-created cluster is ever left behind — the panic-safe teardown
/// of [`run_fdb_metadata_tier1`], extended to cover bring-up.
pub fn run_metadata_nemesis() -> Result<(), String> {
    if std::env::var("WYRD_TIER1").as_deref() != Ok("1") {
        println!(
            "xtask metadata-nemesis: DEFERRED — set WYRD_TIER1=1 to run the privileged M4 metadata \
             nemesis (#407). It stands up a 3-process FoundationDB cluster and partitions / \
             skews / pauses it, and needs Docker (plus `libfaketime` for the skew leg)."
        );
        return Ok(());
    }
    if !docker_available() {
        // Opted in but the tool is missing: a HARD error — a nemesis that quietly did not run is
        // worse than one that failed (the same rule as the #442 battery).
        return Err(
            "WYRD_TIER1=1 but `docker` is not available — the metadata nemesis cannot run, and \
             skipping it silently would report a verdict that was never tested"
                .into(),
        );
    }

    build_fault_agent()?;

    let result = crate::finalize_panic_safe(
        || {
            // Bring-up INSIDE the teardown scope: `docker compose up -d` can fail after
            // creating part of the stack (one service starts, another refuses), and a `?`
            // outside the finalizer would leak that partial cluster — contradicting the
            // unconditional-teardown contract above and contaminating the next Tier-1 run
            // (Codex P2 on #569). `down -v --remove-orphans` on a never-created stack is a
            // harmless no-op, so covering bring-up costs nothing.
            compose(&["up", "-d"])?;
            configure_database()?;
            let cluster_file = write_cluster_file()?;
            // Resolve the topology by STABLE COMPOSE NAME, never ephemeral id: the clock-skew leg
            // force-recreates its node mid-campaign, which invalidates ids but keeps names, so a
            // name map (and skew target) stays valid for every later leg (the prior iterations'
            // live-skew defect).
            let netns_map = nemesis_netns_map()?;
            let skew_container = container_name_of(FDB_TIER1_SKEW_SERVICE)?;
            // A survivor (by stable name) the skew leg polls `status json` in to confirm the
            // cluster fully re-replicated after each force-recreate — so the leg measures skew, not
            // the restart (Design §3).
            let skew_survivor = container_name_of(fdb_tier1_skew_survivor_service())?;
            println!("xtask metadata-nemesis: netns map = {netns_map}");
            println!(
                "xtask metadata-nemesis: skew target = {FDB_TIER1_SKEW_SERVICE} ({skew_container}), \
                 recovery survivor = {skew_survivor}"
            );
            for leg in xtask::nemesis::metadata_nemesis_legs() {
                run_nemesis_leg(
                    leg,
                    &cluster_file,
                    &netns_map,
                    &skew_container,
                    &skew_survivor,
                )?;
            }
            Ok(())
        },
        |result| {
            let _ = compose(&["down", "-v", "--remove-orphans"]);
            result
        },
    );

    result?;
    println!(
        "\nxtask metadata-nemesis: FoundationDB survived the three-leg composable nemesis (#407) \
         — a live-node partition, a clock skew, and a process pause, each proven materialized and \
         healed"
    );
    Ok(())
}

/// Run one nemesis leg's `#[ignore]`d scenario function with the `fdb` feature on and the fault
/// env exported. Captures the output so the leg is failed unless `cargo test` reports it ran
/// **exactly one** test — closing the `--exact` name-drift hole (a renamed scenario function runs
/// 0 tests and exits 0, a silent green no-op).
fn run_nemesis_leg(
    leg: xtask::nemesis::NemesisLegKind,
    cluster_file: &str,
    netns_map: &str,
    skew_container: &str,
    skew_survivor: &str,
) -> Result<(), String> {
    let exact_fn = leg.scenario_fn();
    let args = xtask::nemesis::nemesis_scenario_args(
        xtask::nemesis::METADATA_NEMESIS_SCENARIO_TEST,
        exact_fn,
    );
    print_step(
        &std::iter::once("cargo")
            .chain(args.iter().copied())
            .collect::<Vec<_>>(),
    );

    let compose_file = workspace_root()
        .join(FDB_TIER1_COMPOSE)
        .to_string_lossy()
        .to_string();
    let faketime_override = workspace_root()
        .join(FDB_TIER1_FAKETIME_OVERRIDE)
        .to_string_lossy()
        .to_string();

    let output = Command::new("cargo")
        .args(args)
        .current_dir(workspace_root())
        .env("WYRD_FDB_CLUSTER_FILE", cluster_file)
        .env("WYRD_TIER1_NETNS_MAP", netns_map)
        .env("WYRD_TIER1_FDB_PORT", FDB_TIER1_PORT)
        .env("WYRD_TIER1_IPTABLES_IMAGE", IPTABLES_IMAGE)
        .env("WYRD_TIER1_COMPOSE_FILE", compose_file)
        .env("WYRD_TIER1_FAKETIME_OVERRIDE", faketime_override)
        .env("WYRD_TIER1_SKEW_SERVICE", FDB_TIER1_SKEW_SERVICE)
        .env("WYRD_TIER1_SKEW_CONTAINER", skew_container)
        .env("WYRD_TIER1_SKEW_SURVIVOR", skew_survivor)
        .output()
        .map_err(|e| format!("failed to spawn cargo test: {e}"))?;

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    print!("{combined}");

    if !output.status.success() {
        return Err(format!(
            "metadata nemesis leg `{}` ({exact_fn}) failed with {}",
            leg.as_str(),
            output.status,
        ));
    }
    if !xtask::nemesis::nemesis_leg_ran_exactly_one(&combined) {
        return Err(format!(
            "metadata nemesis leg `{}` reported {:?} tests run, not exactly one — the scenario \
             function `{exact_fn}` was probably renamed on one side of the dispatch (a `--exact` \
             filter that matches nothing exits 0, a silent green no-op)",
            leg.as_str(),
            xtask::nemesis::parse_tests_run(&combined),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cluster file and the netns map are derived from ONE table, so they cannot disagree
    /// about where a process lives — the failure mode where the client dials `.11` while the
    /// scenario cuts `.12` and the fault is silently a no-op.
    #[test]
    fn the_cluster_file_names_every_declared_coordinator() {
        let contents = cluster_file_contents();
        assert!(contents.starts_with("docker:docker@"));
        for (_, ip) in FDB_TIER1_NODES {
            assert!(
                contents.contains(&format!("{ip}:{FDB_TIER1_PORT}")),
                "the cluster file must name coordinator {ip}: {contents}",
            );
        }
    }

    /// **The readiness gate reads the STATUS TEXT, never `fdbcli`'s exit status** (codex review
    /// of #535).
    ///
    /// `fdbcli` 7.3.77 exits 0 against a dead coordinator — this repo established that and says so
    /// in `main.rs`'s `probe_cluster_health` and `fdb_doctor::cluster_status_is_healthy`. A runner
    /// that gated on `status.success()` would march into the fault legs against a cluster that is
    /// still `configuration missing`, and then report *test* failures for what is really a SETUP
    /// failure — indicting the driver for the harness's mistake.
    ///
    /// This pins the predicate itself, on the exact strings a real cluster emits: an unconfigured
    /// or unavailable cluster is NOT ready, and the "unavailable" case is the trap, because
    /// `The database is unavailable` contains the substring "available".
    #[test]
    fn the_readiness_gate_is_not_fooled_by_an_unhealthy_cluster() {
        use crate::fdb_doctor::cluster_status_is_healthy;

        assert!(
            cluster_status_is_healthy("The database is available."),
            "the only healthy answer",
        );
        assert!(
            !cluster_status_is_healthy("The database is unavailable; type `status` for more info."),
            "`unavailable` CONTAINS `available` — a naive substring check calls a dead cluster \
             healthy, which is precisely the bug this predicate exists to avoid",
        );
        assert!(
            !cluster_status_is_healthy(
                "The coordinator(s) have no record of this database. Either the coordinator \
                 addresses are incorrect or the database is not configured."
            ),
            "a fresh, unconfigured cluster is NOT ready — the battery would test nothing",
        );
        assert!(
            !cluster_status_is_healthy(""),
            "no output at all is not readiness",
        );
    }

    /// Every leg named here must exist as a test binary, or the runner would report a green
    /// battery while silently running two thirds of it.
    #[test]
    fn every_declared_leg_exists_as_a_test_binary() {
        for leg in FDB_TIER1_LEGS {
            let path = workspace_root().join(format!("crates/metadata-fdb/tests/{leg}.rs"));
            assert!(
                path.exists(),
                "FDB Tier-1 leg `{leg}` is declared by the runner but {} does not exist",
                path.display(),
            );
        }
    }
}
