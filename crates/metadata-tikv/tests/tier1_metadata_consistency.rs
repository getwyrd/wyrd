//! **Tier-1 metadata consistency-over-the-swap scenario** (M4.6, #257; proposal 0015
//! §"DST and tests", PR-sequence item 6; ADR-0039 in-repo consistency scenario; ADR-0015
//! single-zone contract).
//!
//! What this proves that no in-process test can: the **production `TikvMetadataStore` commit
//! path** (behind the unchanged `MetadataStore` trait) upholds the **ADR-0015 single-zone
//! contract** across a real ≥3-replica TiKV Raft group when the **region LEADER is
//! symmetrically isolated** mid-scenario. It carries the Tier-1 **integration** leg
//! (end-to-end multi-key atomic **create / rename / delete**, all-or-nothing) AND the Tier-1
//! **consistency** leg (read-after-commit, exactly-once convergence, and
//! no-lost-update-under-contention as **INDEPENDENT** signals — the v6 defect collapsed them —
//! asserted **across the heal**, gated by the Invariant-B fault-effect oracle).
//!
//! # Teeth (the iteration-12 amendment)
//!
//! The defect class this slice exists to guard — a missing/mis-ordered `get_for_update`
//! commit-point re-check (`crates/metadata-tikv/src/lib.rs:555-573`) — only fires under
//! **concurrent write-write contention**, and a symmetric cut of a **minority follower** of a
//! linearizable Raft group can never change a commit outcome (the iteration-12 adversary
//! refutation: with the old strictly-sequential single-writer flow, deleting the re-check left
//! every assertion green). Two changes give this leg teeth:
//!
//! * **Contention:** ≥2 concurrent writers ([`contender_count`], default 2, barrier-released
//!   together) race the SAME compare-and-swap on the version cell across the fault window;
//!   exactly one may win ([`wyrd_testkit::no_lost_update`]), a reported `Conflict` must not be
//!   visible afterwards, and a deliberately **stale** CAS probe must be rejected. Deleting or
//!   weakening the re-check admits a stale precondition → two winners and/or an admitted stale
//!   probe → the `no_lost_update` signal (and the marker reads) go red.
//! * **Leader isolation:** with `WYRD_TIER1_ISOLATE=leader` the partition target is resolved
//!   from PD at runtime — the **leader** of the (single) txn region
//!   ([`wyrd_testkit::parse_first_region_leader_store_id`] +
//!   [`wyrd_testkit::parse_store_ip`]) — forcing a leader election while the contenders'
//!   commits are in flight, instead of the outcome-neutral minority-follower cut.
//!
//! # Realization (ADR-0039)
//!
//! ADR-0039 rules that Wyrd's immutable single-write-per-key model does not fit a literal
//! public Jepsen/Elle artifact (deferred to #329); the sanctioned realization is this in-repo
//! Rust scenario driving the production path against a real containerized cluster, asserting
//! the contract directly.
//!
//! # Fault soundness (Invariant B) — the v7 must-fixes
//!
//! * **Truly bidirectional isolation (must-fix 1; the iteration-13 netns cut).** The v6/v7
//!   leg dropped `--dport <port>` on a *shared* `127.0.0.1` loopback — a receive-only
//!   blackout. The iteration-12 rework gave every node a "distinct loopback IP", but all
//!   nodes still shared the HOST netns, so a node's own outbound connections (PD heartbeats,
//!   Raft links) were sourced from `127.0.0.1` and a host-side per-IP cut missed them — a
//!   provable no-op the fault-effect oracle caught live (iteration-13 leg-1 evidence). Now
//!   every node owns its **own network namespace** on a bridge network with a static IP
//!   (`deploy/tikv-multi-replica`: pd = 172.30.57.10, tikv-0/1/2 = .11/.12/.13), and
//!   [`SymmetricPartition`] applies the `-s <ip>` / `-d <ip>` DROP rules **inside the
//!   target's netns** (`docker run --network container:<node>`, the `iptables-agent` image,
//!   `WYRD_TIER1_NETNS_MAP`) — every packet the node sends or receives traverses its own
//!   chains, so the cut is bidirectional by construction and worst-case rule residue dies
//!   with the container instead of leaking host state.
//! * **Peer-side fault-effect oracle keyed on the store's HEARTBEAT, not on `state_name` or a
//!   probe of the dropped port (must-fix 2; the iter-11 fix).** The oracle asks **PD** (the
//!   peers' coordinator) for the target store's `last_heartbeat`
//!   ([`wyrd_testkit::parse_store_last_heartbeat`] over `/pd/api/v1/stores`) and asks whether it
//!   is still **fresh** ([`wyrd_testkit::heartbeat_is_fresh`]). A partitioned voter stops
//!   heartbeating PD, so its heartbeat goes stale within a few store-heartbeat intervals
//!   (seconds) — unlike PD's administrative `state_name`, which stays `"Up"` through a short
//!   partition and only flips after `max-store-down-time` (~30min), so the iter-11 `state_name`
//!   oracle could NEVER observe the cut in a ~45s window and the leg could never pass. A one-way
//!   or probe-only cut leaves the heartbeat fresh, so [`wyrd_testkit::partition_took_effect`]
//!   returns `false` — a no-op fails the gate. The pure oracles
//!   [`wyrd_testkit::partition_took_effect`], [`wyrd_testkit::heartbeat_is_fresh`], and
//!   [`wyrd_testkit::heal_is_complete`] are **wired into this scenario** (not dead code), and
//!   the scenario calls the very `parse_store_last_heartbeat` / `heartbeat_is_fresh` the
//!   at-Check unit tests exercise, so a field-selection or threshold regression flips both.
//! * **Verified, non-lossy heal (must-fix 3).** [`SymmetricPartition::heal`] removes **every**
//!   rule it applied, **surfaces** each `iptables -D` failure (no silent `let _ =`), records the
//!   healed set, and the scenario waits until **PD sees the store's heartbeat fresh again**
//!   before accepting the heal ([`wyrd_testkit::heal_is_complete`]). The `Drop` guard is a
//!   panic-safety net that **warns loudly** on any residual rule rather than leaking host
//!   firewall state.
//!
//! # Gating
//!
//! `#[ignore]`d and **endpoint-gated**: with no `WYRD_TIKV_PD_ENDPOINTS` set (a laptop or a
//! PDCA worktree) it skips cleanly. The live bodies below (`SymmetricPartition`, its `Drop`
//! heal, the PD-side heartbeat fault-effect oracle, and the `partition_took_effect` /
//! `heartbeat_is_fresh` / `heal_is_complete` / `consistency_passes` wiring) sit behind
//! `#[cfg(feature = "tikv")]`,
//! so the default `cargo test --workspace` (tikv OFF) compiles only the skeleton — **not**
//! this code. The privileged Tier CI job type-checks it in the whole-tree gate via
//! `cargo xtask ci`'s dedicated `cargo check -p wyrd-metadata-tikv --features tikv --tests`
//! step (`xtask::feature_gated_checks`), which is **gated on `WYRD_TIKV_TOOLCHAIN`** so the
//! default offline `cargo xtask ci` on a laptop/worktree stays container-free and never
//! compiles the pre-1.0 `tikv-client` tree; when that toolchain gate is set a regression here
//! flips the gate red even though the scenario only *runs* off-Check. The live execution
//! happens only in the privileged
//! off-Check Tier job (`WYRD_TIER1=1`), which stands up the ≥3-replica
//! `deploy/tikv-multi-replica` cluster, exports the endpoints + isolation target, and runs
//! `cargo test --features tikv -- --ignored` (routed by
//! `xtask::metadata_faults::metadata_tier_dispatch`).

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): namespace uniqueness across runs against a
// live cluster, and PD liveness comparisons against PD's own wall-clock
// heartbeat stamps — both inherently real-time (#619).
// File scope (not per-site) is deliberate here: a test crate never
// ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

/// The PD endpoints, or `None` when TiKV is not configured (clean-skip gate).
fn pd_endpoints() -> Option<Vec<String>> {
    match std::env::var("WYRD_TIKV_PD_ENDPOINTS") {
        Ok(raw) if !raw.trim().is_empty() => Some(
            raw.split(',')
                .map(|e| e.trim().to_string())
                .filter(|e| !e.is_empty())
                .collect(),
        ),
        _ => None,
    }
}

/// Tier-1 integration + consistency-over-the-swap on the real ≥3-replica cluster. Skips
/// cleanly (and is `#[ignore]`d) unless the privileged Tier job configured the cluster.
#[test]
#[ignore = "privileged Tier-1: needs a live ≥3-replica TiKV cluster (WYRD_TIER1 job)"]
fn tier1_metadata_consistency_over_the_swap() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the Tier-1 metadata \
             consistency scenario (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run(endpoints);
}

#[cfg(feature = "tikv")]
fn run(endpoints: Vec<String>) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let ns = format!("wyrd-tier1-consistency/{}/", std::process::id()).into_bytes();

        // The SHARED scenario (`wyrd-metadata-fault-conformance`) drives the workload, the invariants and
        // the signal arithmetic — the identical code FoundationDB is judged by (#442). Only
        // the two backend-shaped parts stay here: how a node is cut (the leader, resolved from
        // PD, isolated inside its own netns) and how its PEERS are asked whether the cut bit
        // (PD's store-heartbeat freshness). That seam is `ClusterFault`, implemented below.
        //
        // Lifting this out of #257's TiKV-concrete test is what made an FDB verdict comparable
        // to a TiKV one rather than a parallel story with its own private notion of "pass".
        let partition = SymmetricPartition::from_env(&endpoints);
        let fault: Option<&dyn ClusterFault> = match &partition {
            Some(p) => Some(p),
            None => None,
        };

        wyrd_metadata_fault_conformance::run_consistency_under_fault(
            || connect(&endpoints, &ns),
            fault,
            contender_count(),
        )
        .await;

        drop(partition); // RAII safety net: warns loudly on any residual rule (no host leak).
    });
}

/// The TiKV half of the [`ClusterFault`] seam: cut the Raft **leader** (a minority-follower
/// cut cannot change a linearizable outcome), and confirm from **PD** — the peers'
/// coordinator — that the store's heartbeat went stale. A thin adapter; every method already
/// existed on [`SymmetricPartition`] before the lift.
#[cfg(feature = "tikv")]
impl ClusterFault for SymmetricPartition {
    fn wait_cluster_ready(&self) {
        self.wait_store_ready();
    }

    fn topology(&self) -> (usize, usize) {
        (self.total_replicas, self.isolated)
    }

    fn peers_see_target_live(&self) -> bool {
        self.pd_sees_target_live()
    }

    fn apply(&self) -> Result<(), String> {
        SymmetricPartition::apply(self)
    }

    fn peers_still_see_target_live_after(&self, timeout: Duration) -> bool {
        self.pd_still_sees_target_live_after(timeout)
    }

    fn heal(&self) -> Result<Vec<String>, String> {
        SymmetricPartition::heal(self)
    }

    fn wait_peers_see_target_live(&self, timeout: Duration) -> bool {
        self.wait_pd_sees_target_live(timeout)
    }

    fn applied_rules(&self) -> Vec<String> {
        SymmetricPartition::applied_rules(self)
    }
}

/// How many concurrent writers contend the version-cell CAS (`WYRD_TIER1_CONTENDERS`,
/// default 2, floor 2 — one writer cannot contend, and an uncontended leg has no teeth).
#[cfg(feature = "tikv")]
fn contender_count() -> usize {
    env_usize("WYRD_TIER1_CONTENDERS", 2).max(2)
}

/// The marker key contender `i` writes iff its CAS wins (`usize::MAX` = the stale-CAS
/// probe's marker). Plain like `dir/a` — the store's `with_namespace` prefixes it.
#[cfg(feature = "tikv")]
async fn connect(endpoints: &[String], namespace: &[u8]) -> wyrd_metadata_tikv::TikvMetadataStore {
    wyrd_metadata_tikv::TikvMetadataStore::connect(endpoints.to_vec())
        .await
        .expect("connect to TiKV")
        .with_namespace(namespace.to_vec())
}

#[cfg(feature = "tikv")]
use std::time::Duration;

#[cfg(feature = "tikv")]
use wyrd_metadata_fault_conformance::ClusterFault;

/// A **symmetric**, bidirectional, self-healing network partition of the targeted TiKV node
/// (Invariant B; the v7 must-fixes; the iteration-13 netns cut).
///
/// The isolation rules are applied **inside the target node's own network namespace**
/// (`docker run --network container:<node> … iptables …` via the runner-exported
/// `WYRD_TIER1_NETNS_MAP`; `deploy/tikv-multi-replica/iptables-agent/`), dropping traffic in
/// **both** directions (`-s <ip>` and `-d <ip>`) so the node can neither send to nor receive
/// from PD or its peers. Why the netns and not host rules: under the earlier host-networking
/// topology every node's outbound connections were sourced from `127.0.0.1`, so a host-side
/// per-IP cut missed the node's own heartbeats/Raft links entirely — a provable no-op the
/// fault-effect oracle caught in the iteration-13 leg-1 run. In the target's netns, every
/// packet it sends or receives traverses its own INPUT/OUTPUT chains, so the cut is
/// bidirectional by construction — and a leaked HOST firewall rule is structurally
/// impossible (the worst-case residue dies with the throwaway container). Fault effect and
/// heal are confirmed from **PD's** side by the target store's **heartbeat freshness** (the
/// transient-liveness signal), not by probing the dropped port and not via PD's slow
/// administrative `state_name` (the iter-11 defect).
#[cfg(feature = "tikv")]
struct SymmetricPartition {
    /// The isolated node's distinct static IP (e.g. `172.30.57.12`).
    target_ip: String,
    /// The container owning the target's netns (from `WYRD_TIER1_NETNS_MAP`), or `None` to
    /// fall back to host-side `iptables` (topologies with genuinely distinct host IPs).
    netns_container: Option<String>,
    /// The PD client endpoint the oracle queries (`host:port`, e.g. `172.30.57.10:2379`).
    pd_endpoint: String,
    /// Max heartbeat staleness below which PD is treated as still seeing the target live
    /// (`WYRD_TIER1_HEARTBEAT_STALE_SECS`, default a few store-heartbeat intervals).
    heartbeat_stale: Duration,
    /// Every store address (for the readiness wait) — `host:port` list.
    store_addrs: Vec<String>,
    total_replicas: usize,
    isolated: usize,
    /// The isolation rules actually applied (`iptables` arg vectors), for a complete heal.
    applied: std::cell::RefCell<Vec<Vec<String>>>,
    /// The rule identifiers already removed by a successful `iptables -D`, so `Drop`'s
    /// panic-safety net retries **only** the residue — never double-removing a rule that
    /// already came out, and never false-warning about a rule that was healed.
    removed: std::cell::RefCell<Vec<String>>,
    healed: std::cell::Cell<bool>,
}

#[cfg(feature = "tikv")]
impl SymmetricPartition {
    fn from_env(endpoints: &[String]) -> Option<Self> {
        let pd_endpoint = endpoints.first().cloned().unwrap_or_default();
        // Leader mode (the iteration-12 fix): resolve the txn region's LEADER from PD and cut
        // THAT store — a minority-follower cut never changes a linearizable outcome. Falls
        // back to the static WYRD_TIER1_ISOLATED_IP target when the mode is unset (and to no
        // partition at all — an honest not-materialized fault — when neither is configured,
        // e.g. the no-op negative-control leg).
        let target_ip = if std::env::var("WYRD_TIER1_ISOLATE")
            .map(|m| m.eq_ignore_ascii_case("leader"))
            .unwrap_or(false)
        {
            match resolve_leader_ip(&pd_endpoint, Duration::from_secs(60)) {
                Some(ip) => {
                    eprintln!("wyrd-tier1: resolved region leader at {ip} — isolating it");
                    ip
                }
                None => {
                    eprintln!(
                        "wyrd-tier1: could not resolve the region leader from PD — refusing \
                         to cut anything; the fault will be recorded as NOT materialized."
                    );
                    return None;
                }
            }
        } else {
            let ip = std::env::var("WYRD_TIER1_ISOLATED_IP").ok()?;
            if ip.trim().is_empty() {
                return None;
            }
            ip
        };
        // Which netns owns the target (the iteration-13 cut mechanism). When the runner
        // exported a map, an unmapped target is a configuration hole — refuse to cut
        // anything (recorded as fault NOT materialized) rather than fall back to host rules
        // that are a proven no-op on a shared-netns topology.
        let netns_container = match std::env::var("WYRD_TIER1_NETNS_MAP") {
            Ok(map) if !map.trim().is_empty() => {
                match wyrd_testkit::parse_netns_map(&map, &target_ip) {
                    Some(container) => Some(container),
                    None => {
                        eprintln!(
                            "wyrd-tier1: WYRD_TIER1_NETNS_MAP has no entry for {target_ip} — \
                             refusing to cut anything; the fault will be recorded as NOT \
                             materialized."
                        );
                        return None;
                    }
                }
            }
            _ => None,
        };
        let store_addrs = std::env::var("WYRD_TIER1_STORE_ADDRS")
            .ok()
            .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();
        let total_replicas = env_usize("WYRD_TIER1_REPLICAS", 3);
        let isolated = env_usize("WYRD_TIER1_ISOLATED", 1);
        let heartbeat_stale =
            Duration::from_secs(env_usize("WYRD_TIER1_HEARTBEAT_STALE_SECS", 20) as u64);
        Some(Self {
            target_ip,
            netns_container,
            pd_endpoint,
            heartbeat_stale,
            store_addrs,
            total_replicas,
            isolated,
            applied: std::cell::RefCell::new(Vec::new()),
            removed: std::cell::RefCell::new(Vec::new()),
            healed: std::cell::Cell::new(false),
        })
    }

    /// The bidirectional isolation rules for this node's IP, applied in the TARGET's netns:
    /// its outbound packets traverse its OUTPUT chain (source = its IP), its inbound packets
    /// its INPUT chain (destination = its IP), so dropping by **both** source-IP and
    /// destination-IP on **both** chains isolates the node fully.
    fn rules(&self) -> Vec<Vec<String>> {
        let ip = &self.target_ip;
        let mut rules = Vec::new();
        for chain in ["INPUT", "OUTPUT"] {
            for sel in ["-s", "-d"] {
                rules.push(vec![
                    chain.to_string(),
                    sel.to_string(),
                    ip.to_string(),
                    "-j".to_string(),
                    "DROP".to_string(),
                ]);
            }
        }
        rules
    }

    /// Stable identifiers for the applied rules (for `heal_is_complete`).
    fn applied_rules(&self) -> Vec<String> {
        self.applied.borrow().iter().map(|r| r.join(" ")).collect()
    }

    /// Wait until every store port accepts a connection before asserting (don't race cluster
    /// formation — the v6 defect).
    fn wait_store_ready(&self) {
        use std::net::TcpStream;
        use std::time::Instant;
        let deadline = Instant::now() + Duration::from_secs(60);
        for addr in &self.store_addrs {
            let Ok(sock) = addr.parse::<std::net::SocketAddr>() else {
                continue;
            };
            while TcpStream::connect_timeout(&sock, Duration::from_millis(500)).is_err() {
                if Instant::now() >= deadline {
                    panic!("store address {addr} never became ready within 60s");
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }

    /// Apply the bidirectional DROP rules; **surface** any `iptables` failure (no silent
    /// swallow) and record each successfully-applied rule for a complete heal.
    fn apply(&self) -> Result<(), String> {
        for rule in self.rules() {
            let mut args = vec!["-I".to_string()];
            args.extend(rule.iter().cloned());
            self.iptables(&args)
                .map_err(|e| format!("iptables -I {} failed: {e}", rule.join(" ")))?;
            self.applied.borrow_mut().push(rule);
        }
        Ok(())
    }

    /// Run one `iptables` invocation — inside the target's network namespace when the runner
    /// mapped one (`docker run --rm --privileged --network container:<node>` with the
    /// `iptables-agent` image), else on the host. The netns route is what makes the cut
    /// bidirectional for every packet the node sends or receives, and confines any
    /// worst-case rule residue to the throwaway container (Invariant B).
    fn iptables(&self, args: &[String]) -> Result<(), String> {
        match &self.netns_container {
            Some(container) => {
                let image = std::env::var("WYRD_TIER1_IPTABLES_IMAGE")
                    .unwrap_or_else(|_| "wyrd-iptables:local".to_string());
                let mut full = vec![
                    "run".to_string(),
                    "--rm".to_string(),
                    "--privileged".to_string(),
                    format!("--network=container:{container}"),
                    image,
                ];
                full.extend(args.iter().cloned());
                run_cmd("docker", &full)
            }
            None => run_cmd("iptables", args),
        }
    }

    /// Remove **every** applied rule; surface failures; return the healed-rule identifiers.
    ///
    /// Each rule that a successful `iptables -D` removes is recorded in [`Self::removed`], so a
    /// caller `panic` (e.g. the `heal().expect(..)` in [`run`]) leaves `Drop` able to retry
    /// **only** the rules that did NOT come out. Crucially, [`Self::healed`] is set to `true`
    /// **only when every rule was removed** — a partial heal returns `Err` with
    /// `healed == false`, so the panic-safety net in `Drop` still fires on the residue and no
    /// host firewall rule is leaked (the v8 codex advisory: the old code set `healed = true`
    /// unconditionally, so a panic after a partial heal skipped `Drop`).
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
                    eprintln!("wyrd-tier1: HEAL FAILURE — {msg}");
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

    // ── Peer-side (PD) fault-effect oracle: HEARTBEAT freshness, not `state_name` ──

    /// Whether PD's last recorded heartbeat for the target store is **fresh** right now (a
    /// single poll). Keyed on `last_heartbeat` age via [`wyrd_testkit::heartbeat_is_fresh`], NOT
    /// on PD's administrative `state_name` — the iter-11 fix (a short partition never flips
    /// `state_name`, so the leg could never pass). PD unreachable / store absent → not live.
    fn pd_sees_target_live(&self) -> bool {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i128)
            .unwrap_or(0);
        match pd_store_last_heartbeat(&self.pd_endpoint, &self.target_ip) {
            Some(last) => wyrd_testkit::heartbeat_is_fresh(last, now_nanos, self.heartbeat_stale),
            None => false,
        }
    }

    /// Poll until PD's heartbeat for the store goes **stale**, or `timeout` elapses. Returns
    /// whether PD **still** saw it live at the end — `true` means the partition was a **no-op**
    /// (peers kept receiving heartbeats), which fails `partition_took_effect`.
    fn pd_still_sees_target_live_after(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if !self.pd_sees_target_live() {
                return false; // peers stopped hearing the node → real isolation
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        true // heartbeat still fresh after the whole window → no-op cut
    }

    /// Poll until PD's heartbeat for the store is fresh again (heal confirmation) or `timeout`
    /// elapses.
    fn wait_pd_sees_target_live(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.pd_sees_target_live() {
                return true;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        false
    }
}

#[cfg(feature = "tikv")]
impl Drop for SymmetricPartition {
    fn drop(&mut self) {
        // Panic-safety net only: the happy path already healed every rule explicitly (and
        // verified it), so `healed == true` and there is nothing to retry.
        if self.healed.get() {
            return;
        }
        let removed = self.removed.borrow();
        for rule in self.applied.borrow().iter() {
            let id = rule.join(" ");
            // Skip rules a successful explicit `heal()` already removed — retrying them would
            // fail spuriously and emit a false "leaked" warning. Only the residue is retried.
            if removed.contains(&id) {
                continue;
            }
            let mut args = vec!["-D".to_string()];
            args.extend(rule.iter().cloned());
            if let Err(e) = self.iptables(&args) {
                // Loud — a leaked host firewall rule must never be silent (the v7 defect).
                eprintln!(
                    "wyrd-tier1: WARNING — residual isolation rule NOT removed on unwind: \
                     iptables -D {id} ({e}); host firewall state may have leaked.",
                );
            }
        }
    }
}

#[cfg(feature = "tikv")]
fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Run `<program> <args>`; map a non-zero exit or spawn failure to `Err` (surfaced, never
/// swallowed).
#[cfg(feature = "tikv")]
fn run_cmd(program: &str, args: &[String]) -> Result<(), String> {
    let status = std::process::Command::new(program)
        .args(args)
        .status()
        .map_err(|e| format!("spawn {program}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("exit {status}"))
    }
}

/// Query PD's `/pd/api/v1/stores` and return the `last_heartbeat` (nanoseconds since the Unix
/// epoch) PD last recorded for the store whose address contains `target_ip`, or `None` if
/// unavailable. The **transient-liveness** field — a partitioned voter stops heartbeating PD, so
/// this freezes and its age grows within seconds (unlike the administrative `state_name`, the
/// iter-11 defect).
///
/// A dependency-free raw HTTP/1.0 GET over `std::net` — this leg is off-Check and adds no crate
/// deps. It observes the cluster from **PD's** perspective (the peers' coordinator), the
/// peer-side signal Invariant B requires, not a probe of the isolated node's dropped port. The
/// parse is delegated to [`wyrd_testkit::parse_store_last_heartbeat`], the SAME function the
/// at-Check unit tests exercise, so the field selection is a single sourced, checkable oracle.
#[cfg(feature = "tikv")]
fn pd_store_last_heartbeat(pd_endpoint: &str, target_ip: &str) -> Option<i128> {
    let body = pd_http_get(pd_endpoint, "/pd/api/v1/stores")?;
    wyrd_testkit::parse_store_last_heartbeat(&body, target_ip)
}

/// A dependency-free raw HTTP/1.0 GET against PD (see [`pd_store_last_heartbeat`] for why:
/// off-Check leg, no crate deps; parses are delegated to the at-Check-tested `wyrd_testkit`
/// functions).
#[cfg(feature = "tikv")]
fn pd_http_get(pd_endpoint: &str, path: &str) -> Option<String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let addr: std::net::SocketAddr = pd_endpoint.parse().ok()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: {pd_endpoint}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// Resolve the IP of the store currently LEADING the (single) txn region, polling PD until
/// `timeout` (elections in a just-started cluster take a few seconds). `None` = no leader
/// resolvable — the caller must refuse to cut anything rather than cut a guess.
/// Leader selection and id→IP mapping are the at-Check-tested pure parsers
/// [`wyrd_testkit::parse_first_region_leader_store_id`] / [`wyrd_testkit::parse_store_ip`];
/// this function only moves bytes.
#[cfg(feature = "tikv")]
fn resolve_leader_ip(pd_endpoint: &str, timeout: Duration) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(regions) = pd_http_get(pd_endpoint, "/pd/api/v1/regions") {
            if let Some(leader_store) = wyrd_testkit::parse_first_region_leader_store_id(&regions) {
                if let Some(stores) = pd_http_get(pd_endpoint, "/pd/api/v1/stores") {
                    if let Some(ip) = wyrd_testkit::parse_store_ip(&stores, leader_store) {
                        return Some(ip);
                    }
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

#[cfg(not(feature = "tikv"))]
fn run(endpoints: Vec<String>) {
    let _ = endpoints;
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via the privileged Tier-1 job."
    );
}
