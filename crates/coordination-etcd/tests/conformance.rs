//! Drives the **shared** `Coordination` trait-contract suite
//! (`wyrd-coordination-conformance`) — the identical assertions
//! `coordination-mem` passes — against a real `EtcdCoordination`, plus the
//! cross-instance properties that only two networked instances can show (single
//! leader, mutual exclusion, cross-process discovery). Proposal 0015
//! §"Deployment prerequisite", #365 DoD: "etcd passes the shared, not forked,
//! conformance suite."
//!
//! The run is **endpoint-gated AND feature-gated**:
//! - With no `WYRD_ETCD_ENDPOINTS` set (a laptop or a PDCA worktree with no etcd)
//!   it **skips cleanly** so `cargo xtask ci` stays green.
//! - With the endpoint set but built WITHOUT `--features etcd` it **panics** —
//!   asking for a real-etcd run on a build that cannot serve one is an operator
//!   error (a misconfigured Tier-2 job), never a silent pass.
//!
//! `cargo xtask etcd-conformance` brings up the throwaway `deploy/etcd-single-node`
//! etcd, exports the endpoint, rebuilds with `--features etcd`, and runs it. The
//! DETERMINISTIC proof of the same store lives in `crates/dst/tests/coordination.rs`
//! (the madsim etcd simulator), which needs neither a container nor `protoc`.

#![forbid(unsafe_code)]

fn etcd_endpoints() -> Option<Vec<String>> {
    match std::env::var("WYRD_ETCD_ENDPOINTS") {
        Ok(raw) if !raw.trim().is_empty() => Some(
            raw.split(',')
                .map(|e| e.trim().to_string())
                .filter(|e| !e.is_empty())
                .collect(),
        ),
        _ => None,
    }
}

#[test]
fn trait_contract_against_etcd() {
    let Some(endpoints) = etcd_endpoints() else {
        eprintln!(
            "wyrd-coordination-etcd: WYRD_ETCD_ENDPOINTS not set — skipping the real-etcd \
             conformance run (clean skip; the gate stays green without an etcd)."
        );
        return;
    };
    run(endpoints);
}

#[cfg(feature = "etcd")]
fn run(endpoints: Vec<String>) {
    use wyrd_coordination_conformance as conformance;
    use wyrd_coordination_etcd::EtcdCoordination;
    // The single-leader clause below calls `b.elect_leader(...)` directly, an
    // inherent-looking call that resolves ONLY with the `Coordination` trait in
    // scope. Without this import the `--features etcd` build fails to compile
    // (E0599) — the exact iter-5 regression, which no `ci` gate catches because
    // `--features etcd` is off-CI. Keep it here so the real-etcd conformance builds.
    use wyrd_traits::Coordination;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async {
        // The whole shared contract via the single `run_all` runner, so etcd drives
        // the identical clause set mem does. `make_store(tag)` scopes each clause to
        // a fresh per-`tag` key namespace against the one cluster (the pid keeps
        // concurrent CI runs from colliding).
        conformance::run_all(|tag| {
            let endpoints = endpoints.clone();
            let ns = format!("wyrd-conformance/{}/{tag}/", std::process::id());
            async move {
                EtcdCoordination::connect(&endpoints)
                    .await
                    .expect("connect to etcd")
                    .with_namespace(ns)
            }
        })
        .await;

        // Cross-instance properties (only two networked instances can show these),
        // driven through the SAME shared `conformance::cross_instance_*` helpers the
        // madsim simulator drives — no fork (ADR-0006 "one contract, two
        // implementations"). Each property gets its own fresh namespace pair.
        use std::time::Duration;

        // A two-instance pair scoped to a fresh per-`tag` namespace against the one
        // cluster (the pid keeps concurrent CI runs from colliding).
        let pair = |tag: &str| {
            let endpoints = endpoints.clone();
            let ns = format!("wyrd-conformance/{}/xinst-{tag}/", std::process::id());
            let ns_b = ns.clone();
            async move {
                let a = EtcdCoordination::connect(&endpoints)
                    .await
                    .expect("connect A")
                    .with_namespace(ns);
                let b = EtcdCoordination::connect(&endpoints)
                    .await
                    .expect("connect B")
                    .with_namespace(ns_b);
                (a, b)
            }
        };

        // (b) SINGLE LEADER on REAL etcd: while A holds the term, B's concurrent
        // campaign must NOT resolve. This is the headline safety property and it must
        // be checked on a real cluster, not only the simulator — a bounded wait
        // (tokio's `timeout`) gives B ample time to WRONGLY win if the store allowed
        // a concurrent grant.
        let (a, b) = pair("lead").await;
        conformance::cross_instance_single_leader_is_exclusive(&a, "custodian", || async {
            tokio::time::timeout(Duration::from_secs(2), b.elect_leader("custodian"))
                .await
                .ok()
                .map(|r| r.unwrap())
        })
        .await;

        // Mutual exclusion across instances (a held lock refuses a peer; the peer
        // fences on re-acquire).
        let (a, b) = pair("lock").await;
        conformance::cross_instance_lock_is_mutually_exclusive(&a, &b).await;

        // Cross-process discovery: each instance discovers BOTH peers' registrations.
        let (a, b) = pair("disc").await;
        conformance::cross_instance_registration_is_discoverable(&a, &b).await;
    });

    eprintln!(
        "wyrd-coordination-etcd: real etcd passed the shared Coordination conformance suite \
         and the cross-instance properties (single leader, mutual exclusion, discovery)"
    );
}

#[cfg(not(feature = "etcd"))]
fn run(endpoints: Vec<String>) {
    let _ = endpoints;
    panic!(
        "wyrd-coordination-etcd: WYRD_ETCD_ENDPOINTS is set but the crate was built WITHOUT \
         `--features etcd`, so the etcd store was never compiled — a real-etcd run here would \
         prove nothing. Run it via `cargo xtask etcd-conformance` (which builds `--features etcd`)."
    );
}
