//! **Tier-1 metadata consistency under a real FoundationDB cluster fault** — #442's fault
//! battery, the go/no-go gate for making FDB the production metadata backend (ADR-0042).
//!
//! What this proves that no in-process test can: the **production `FdbMetadataStore` commit
//! path**, behind the unchanged `MetadataStore` trait, upholds the ADR-0015 single-zone
//! contract across a real 3-process FDB cluster while the process holding the cluster's
//! **`master` role is symmetrically isolated** mid-scenario.
//!
//! FDB's own simulation pedigree is why it was chosen (ADR-0042) — but it validates *their*
//! code, not **our mapping layer**: precondition → read-conflict set, `1020 not_committed` →
//! `Conflict`, the unknown-result rules, the retry policy. This battery is a black-box check
//! of that mapping under real faults.
//!
//! # The scenario is SHARED with TiKV, deliberately
//!
//! The workload, the invariants and the signal arithmetic all live in `wyrd-metadata-fault-conformance`
//! and are the **identical code** #257's TiKV leg runs. Only two things are backend-shaped and
//! live here: *how a node is cut*, and *how its peers are asked whether the cut bit*. A
//! go/no-go verdict is only worth something if the two backends were held to the same
//! standard; a private FDB notion of "pass" would decide nothing.
//!
//! # The cut, and why it is the master
//!
//! Cutting an arbitrary node is **outcome-neutral**: FDB keeps quorum on the majority side, so
//! isolating a bystander cannot perturb a commit and every assertion would go green for free —
//! the "hollow flip" #257's iteration-12 review caught in the TiKV leg (a minority-follower cut
//! proved nothing). So the target is resolved at runtime from `status json`: the process
//! holding the **`master`** role (`wyrd_testkit::parse_fdb_process_with_role`). Isolating it
//! forces an FDB **recovery** while the contenders' commits are in flight.
//!
//! The isolation is applied **inside the target's own network namespace** (`docker run
//! --network container:<node> … iptables …`, the `iptables-agent` image), dropping traffic in
//! **both** directions (`-s` and `-d`, INPUT and OUTPUT) — so the node can neither send nor
//! receive, and a leaked HOST firewall rule is structurally impossible (worst-case residue dies
//! with the throwaway container). This is why `deploy/fdb-multi-replica` gives every process
//! its own netns and a static IP with `FDB_NETWORKING_MODE: container`: under host networking
//! the processes would source their traffic from a shared loopback and a per-IP cut would be a
//! provable no-op.
//!
//! # Invariant B: the fault must be confirmed from the PEERS' side
//!
//! `fault_materialized` is an independent signal and the verdict cannot pass without it. The
//! oracle runs `fdbcli --exec "status json"` **inside a surviving process's container** and
//! reads that survivor's own reachability view of the cut node
//! (`coordinators[].reachable` — `wyrd_testkit::fdb_peer_sees_target_live`). It never probes
//! the dropped port from the test, which would only prove *our* packets are dropped, not that
//! the cluster noticed.
//!
//! Runs only under the privileged Tier-1 job (`cargo xtask fdb-metadata-tier1`), which brings
//! up `deploy/fdb-multi-replica`, configures the database, writes the cluster file and exports
//! the env below. Absent that, it skips cleanly so `cargo xtask ci` stays green.

#![forbid(unsafe_code)]

#[cfg(feature = "fdb")]
use std::time::Duration;

#[cfg(feature = "fdb")]
mod support;

/// The FDB cluster file, or `None` when FDB is not configured (clean-skip gate).
fn cluster_file() -> Option<String> {
    match std::env::var("WYRD_FDB_CLUSTER_FILE") {
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => None,
    }
}

#[test]
#[ignore = "privileged Tier-1: needs a live 3-process FDB cluster (cargo xtask fdb-metadata-tier1)"]
fn tier1_metadata_consistency_under_an_fdb_cluster_fault() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!(
            "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the Tier-1 metadata \
             consistency scenario (clean skip; the gate stays green without an FDB)."
        );
        return;
    };
    run(cluster_file);
}

#[cfg(feature = "fdb")]
fn run(cluster_file: String) {
    use wyrd_metadata_fault_conformance::ClusterFault;

    // No explicit `foundationdb::boot()`: `FdbMetadataStore::open` boots the process-wide FDB
    // network itself (`ensure_network`), and selecting the API version twice PANICS the process.

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let prefix = format!("wyrd-tier1-fdb/{}/", std::process::id()).into_bytes();

        let fault = MasterIsolation::from_env();
        let fault_ref: Option<&dyn ClusterFault> = match &fault {
            Some(f) => Some(f),
            None => None,
        };

        wyrd_metadata_fault_conformance::run_consistency_under_fault(
            || {
                let cluster_file = cluster_file.clone();
                let prefix = prefix.clone();
                async move {
                    // A FRESH client per contender — the race must be between real clients,
                    // not between two handles inside one client's mutex.
                    wyrd_metadata_fdb::FdbMetadataStore::open(&cluster_file)
                        .expect("open the FoundationDB metadata store")
                        .with_prefix(prefix)
                }
            },
            fault_ref,
            contender_count(),
        )
        .await;

        drop(fault); // RAII safety net: warns loudly on any residual isolation rule.
    });
}

#[cfg(feature = "fdb")]
fn contender_count() -> usize {
    std::env::var("WYRD_TIER1_CONTENDERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2usize)
        .max(2)
}

/// A **symmetric, bidirectional, self-healing** isolation of the FDB process holding the
/// `master` role — the FDB half of the [`ClusterFault`] seam.
#[cfg(feature = "fdb")]
struct MasterIsolation {
    /// The isolated process's static IP (e.g. `172.30.58.11`).
    target_ip: String,
    /// Its full `<ip>:<port>` address, as `status json` names it.
    target_addr: String,
    /// The container owning the target's netns — where the `iptables` rules are applied.
    netns_container: String,
    /// A **surviving** container the oracle runs `fdbcli` in. Never the target: a cut node's
    /// own view of the cluster is exactly what we must not trust.
    survivor_container: String,
    total_replicas: usize,
    isolated: usize,
    applied: std::cell::RefCell<Vec<Vec<String>>>,
    removed: std::cell::RefCell<Vec<String>>,
    healed: std::cell::Cell<bool>,
}

#[cfg(feature = "fdb")]
impl MasterIsolation {
    /// Resolve the target from the LIVE cluster, or `None` when the runner configured no fault
    /// (a negative-control run: the scenario then records the fault as NOT materialized and the
    /// verdict fails honestly, rather than banking a green battery it never ran).
    ///
    /// The order matters, and it is what avoids a chicken-and-egg: the master is resolved by
    /// asking *any* process (`status json` is a cluster-wide report), and only then is the
    /// survivor chosen — as any process that is NOT the target. Designating a survivor up front
    /// would break the moment the master happened to land on it.
    fn from_env() -> Option<Self> {
        let all = support::processes()?;
        let target = support::resolve_role_holder(&all, "master", Duration::from_secs(90))
            .or_else(|| {
                eprintln!(
                    "wyrd-tier1-fdb: could not resolve the master process from status json — \
                     refusing to cut a guess; the fault will be recorded as NOT materialized."
                );
                None
            })?;
        let survivor = support::survivor(&all, &target).or_else(|| {
            eprintln!(
                "wyrd-tier1-fdb: no process other than the target — the oracle would be asking \
                 the isolated node about itself. Refusing to cut."
            );
            None
        })?;

        eprintln!(
            "wyrd-tier1-fdb: master at {} (container {}); oracle will ask {}",
            target.addr, target.container, survivor.container,
        );

        Some(Self {
            target_ip: target.ip.clone(),
            target_addr: target.addr.clone(),
            netns_container: target.container.clone(),
            survivor_container: survivor.container.clone(),
            total_replicas: env_usize("WYRD_TIER1_REPLICAS", all.len()),
            isolated: env_usize("WYRD_TIER1_ISOLATED", 1),
            applied: std::cell::RefCell::new(Vec::new()),
            removed: std::cell::RefCell::new(Vec::new()),
            healed: std::cell::Cell::new(false),
        })
    }

    /// The symmetric rule set: DROP on both chains, on both selectors — the node can neither
    /// send to nor receive from its peers.
    fn rules(&self) -> Vec<Vec<String>> {
        let mut rules = Vec::new();
        for chain in ["INPUT", "OUTPUT"] {
            for sel in ["-s", "-d"] {
                rules.push(vec![
                    chain.to_string(),
                    sel.to_string(),
                    self.target_ip.clone(),
                    "-j".to_string(),
                    "DROP".to_string(),
                ]);
            }
        }
        rules
    }

    /// Run `iptables <args>` INSIDE the target's network namespace.
    fn iptables(&self, args: &[String]) -> Result<(), String> {
        let image = std::env::var("WYRD_TIER1_IPTABLES_IMAGE")
            .unwrap_or_else(|_| "wyrd-iptables:local".to_string());
        let mut full = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--privileged".to_string(),
            format!("--network=container:{}", self.netns_container),
            image,
        ];
        full.extend(args.iter().cloned());
        run_cmd("docker", &full)
    }
}

#[cfg(feature = "fdb")]
impl wyrd_metadata_fault_conformance::ClusterFault for MasterIsolation {
    fn wait_cluster_ready(&self) {
        // Ready = a survivor reports the database available. Asking the CLUSTER (not a TCP
        // probe) is what makes this a readiness gate rather than a port check.
        //
        // `fdbcli` PRETTY-PRINTS `status json`, so the needle must be matched against the
        // whitespace-stripped body — `"available": true` in the raw text. The same compaction
        // every `wyrd_testkit` parser does, and for the same reason.
        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        loop {
            if let Some(status) = support::status_json(&self.survivor_container) {
                let compact: String = status.chars().filter(|c| !c.is_whitespace()).collect();
                if compact.contains("\"database_available\":true")
                    || compact.contains("\"available\":true")
                {
                    return;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the FDB cluster never became available within 90s",
            );
            std::thread::sleep(Duration::from_secs(2));
        }
    }

    fn topology(&self) -> (usize, usize) {
        (self.total_replicas, self.isolated)
    }

    fn peers_see_target_live(&self) -> bool {
        match support::status_json(&self.survivor_container) {
            Some(status) => wyrd_testkit::fdb_peer_sees_target_live(&status, &self.target_addr),
            None => false,
        }
    }

    fn apply(&self) -> Result<(), String> {
        for rule in self.rules() {
            let mut args = vec!["-I".to_string()];
            args.extend(rule.iter().cloned());
            self.iptables(&args)?;
            self.applied.borrow_mut().push(rule);
        }
        Ok(())
    }

    fn peers_still_see_target_live_after(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if !self.peers_see_target_live() {
                return false; // a survivor lost the node → the cut is real
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        true // still reachable after the whole window → a no-op cut, and the verdict must fail
    }

    fn heal(&self) -> Result<Vec<String>, String> {
        let mut healed = Vec::new();
        let mut first_err = None;
        for rule in self.applied.borrow().iter() {
            let id = rule.join(" ");
            let mut args = vec!["-D".to_string()];
            args.extend(rule.iter().cloned());
            match self.iptables(&args) {
                Ok(()) => {
                    self.removed.borrow_mut().push(id.clone());
                    healed.push(id);
                }
                Err(e) => {
                    let msg = format!("iptables -D {id} failed: {e}");
                    eprintln!("wyrd-tier1-fdb: HEAL FAILURE — {msg}");
                    first_err.get_or_insert(msg);
                }
            }
        }
        match first_err {
            // Partial heal: leave `healed == false` so `Drop` still retries the residue.
            Some(e) => Err(e),
            None => {
                self.healed.set(true);
                Ok(healed)
            }
        }
    }

    fn wait_peers_see_target_live(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.peers_see_target_live() {
                return true;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        false
    }

    fn applied_rules(&self) -> Vec<String> {
        self.applied.borrow().iter().map(|r| r.join(" ")).collect()
    }
}

#[cfg(feature = "fdb")]
impl Drop for MasterIsolation {
    fn drop(&mut self) {
        if self.healed.get() {
            return;
        }
        let removed = self.removed.borrow();
        for rule in self.applied.borrow().iter() {
            let id = rule.join(" ");
            if removed.contains(&id) {
                continue;
            }
            let mut args = vec!["-D".to_string()];
            args.extend(rule.iter().cloned());
            if let Err(e) = self.iptables(&args) {
                eprintln!(
                    "wyrd-tier1-fdb: WARNING — residual isolation rule NOT removed on unwind: \
                     iptables -D {id} ({e}); firewall state may have leaked.",
                );
            }
        }
    }
}

#[cfg(feature = "fdb")]
fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(feature = "fdb")]
fn run_cmd(program: &str, args: &[String]) -> Result<(), String> {
    let out = std::process::Command::new(program)
        .args(args)
        .output()
        .map_err(|e| format!("spawn {program}: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{program} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim(),
        ))
    }
}

#[cfg(not(feature = "fdb"))]
fn run(cluster_file: String) {
    let _ = cluster_file;
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE is set but the crate was built without \
         `--features fdb` — skipping. Run it via `cargo xtask fdb-metadata-tier1`."
    );
}
