//! Deterministic-simulation proof of the etcd-backed `Coordination`
//! (`wyrd-coordination-etcd`, #365) — the crate's load-bearing correctness, run
//! on the **madsim etcd simulator** so it is actually compiled and exercised by
//! `cargo xtask dst` (part of `ci`), deterministically, with no `protoc` and no
//! live etcd.
//!
//! Under `--cfg madsim` the store's `etcd-client` dependency aliases to
//! `madsim-etcd-client`, whose in-simulator etcd faithfully models lease
//! tick-expiry, min-create-revision leader election, and the mvcc revision the
//! fencing tokens ride. So THIS file drives the SAME production store code the
//! real-etcd job (`crates/coordination-etcd/tests/conformance.rs`) drives — not a
//! copy — and proves the properties that justify the crate:
//!
//! - the shared trait-contract suite passes (single-instance clauses);
//! - only ONE of two instances leads AT A TIME (B's campaign stays PENDING while A
//!   holds the term — the split-brain guard), and leadership HANDS OFF when the
//!   holder drops (revoke-on-drop, not a TTL wait), the new term fencing the old;
//! - a lock is mutually exclusive AND fenced across two instances;
//! - peers register and are discovered ACROSS instances (the "L5 discovery" DoD);
//! - a registration lease EXPIRES deterministically once its TTL elapses;
//! - a CANCELLED campaign leaks no orphan — a third instance still wins;
//! - a leader whose lease LAPSES (partitioned past its TTL) re-campaigns for a
//!   fresh term rather than proclaiming on a dead key (the loss is detected by the
//!   keep-alive, not inferred from a transient RPC error).
//!
//! ## Demonstrated-red for the cross-instance clauses
//!
//! The cross-instance properties (single leader, mutual exclusion, cross-process
//! discovery) are written as helpers over `&impl Coordination`, so the SAME
//! assertion the etcd store passes is run against a deliberately-broken store — two
//! independent process-local `coordination-mem` instances, which share no state —
//! and shown to go RED (`#[should_panic]`). That pins the clauses as non-vacuous:
//! their green rests on real cross-process coordination, not on simulator fidelity
//! alone.
//!
//! madsim replays every seed the `dst` tier sweeps, so any ordering that could
//! break single-leader or fencing is a reproducible failure, not a flake.

#![forbid(unsafe_code)]
#![cfg(madsim)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use etcd_client::GetOptions;
use madsim::net::NetSim;
use madsim::runtime::Handle;
use madsim::time::{sleep, timeout};
use wyrd_coordination_conformance as conformance;
use wyrd_coordination_etcd::EtcdCoordination;
use wyrd_coordination_mem::MemCoordination;
use wyrd_traits::Coordination;

const ETCD_ADDR: &str = "10.1.0.1:2379";
const ETCD_IP: &str = "10.1.0.1";

/// Boot the in-simulator etcd on its own node and register its DNS name. Callers
/// then `sleep(1s)` so the server is listening before connecting (the
/// madsim-etcd-client harness convention).
fn boot_etcd(handle: &Handle) {
    let server = handle
        .create_node()
        .name("etcd")
        .ip(ETCD_IP.parse().unwrap())
        .build();
    NetSim::current().add_dns_record("etcd", ETCD_IP.parse().unwrap());
    server.spawn(async {
        etcd_client::SimServer::builder()
            .serve(ETCD_ADDR.parse().unwrap())
            .await
            .unwrap();
    });
}

/// Create a fresh client node and connect an `EtcdCoordination` scoped to `ns`.
async fn connect(handle: &Handle, name: &str, ip: &str, ns: &str) -> EtcdCoordination {
    let node = handle
        .create_node()
        .name(name.to_string())
        .ip(ip.parse().unwrap())
        .build();
    let ns = ns.to_string();
    node.spawn(async move {
        EtcdCoordination::connect(["etcd:2379"])
            .await
            .expect("connect to the simulated etcd")
            .with_namespace(ns)
    })
    .await
    .unwrap()
}

/// A raw admin `etcd_client::Client` on its own node — used by a test to inject a
/// deterministic fault (revoking a hold's lease directly), the connection-stable
/// analogue of pulling the plug on a leader without a flaky network partition.
async fn admin_client(handle: &Handle, ip: &str) -> etcd_client::Client {
    let node = handle
        .create_node()
        .name("admin")
        .ip(ip.parse().unwrap())
        .build();
    node.spawn(async {
        etcd_client::Client::connect(["etcd:2379"], None)
            .await
            .unwrap()
    })
    .await
    .unwrap()
}

// ---- Cross-instance contract helpers live in the SHARED suite ---------------
//
// The cross-instance clauses (single leader, mutual exclusion, cross-process
// discovery) are NOT defined here — they are the shared, non-forked
// `conformance::cross_instance_*` helpers (`wyrd-coordination-conformance`), the
// SAME assertions the real-etcd conformance
// (`crates/coordination-etcd/tests/conformance.rs`) drives. This file runs them
// GREEN against two networked etcd instances on the simulator, and RED against two
// process-local `coordination-mem` instances (the demonstrated-red below).
//
// The single-leader clause is runtime-agnostic, so each caller supplies its own
// bounded wait; this helper wraps madsim's `timeout` for the simulator runs.
async fn campaign_b_bounded(
    b: &impl Coordination,
    key: &'static str,
) -> Option<wyrd_traits::Leadership> {
    timeout(Duration::from_secs(2), b.elect_leader(key))
        .await
        .ok()
        .map(|r| r.unwrap())
}

// ---- The shared single-instance suite on the etcd simulator -----------------

/// The shared trait-contract suite (the identical clauses `coordination-mem`
/// passes) against the etcd store over the simulator — single-instance clauses,
/// a fresh namespace per clause.
#[madsim::test]
async fn shared_contract_suite_on_the_etcd_simulator() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let client = handle
        .create_node()
        .name("suite")
        .ip("10.1.0.9".parse().unwrap())
        .build();
    client
        .spawn(async {
            conformance::run_all(|tag| async move {
                EtcdCoordination::connect(["etcd:2379"])
                    .await
                    .expect("connect to the simulated etcd")
                    .with_namespace(format!("suite/{tag}/"))
            })
            .await;
        })
        .await
        .unwrap();
}

// ---- Cross-instance properties on the etcd simulator (GREEN) ----------------

/// Only one of two instances leads at a time; leadership hands off to the waiter
/// the instant the holder drops — proof of single-leader (B stays PENDING while A
/// leads: the split-brain guard) AND prompt revoke-on-drop (not a TTL wait), with
/// the new term fencing the old.
#[madsim::test]
async fn only_one_of_two_instances_leads_then_hands_off() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "lead/").await;
    let lead_a = a.elect_leader("custodian").await.unwrap();

    // B campaigns on its own node. `b_won` flips ONLY when B's campaign actually
    // resolves, so we can assert B is still PENDING while A leads — a store that
    // granted A and B concurrently (split-brain) would flip it before the drop and
    // fail the assertion below. This is the property the whole crate exists for.
    let b_won = Arc::new(AtomicBool::new(false));
    let b_won_task = Arc::clone(&b_won);
    let node_b = handle
        .create_node()
        .name("B")
        .ip("10.1.0.12".parse().unwrap())
        .build();
    let b_task = node_b.spawn(async move {
        let b = EtcdCoordination::connect(["etcd:2379"])
            .await
            .unwrap()
            .with_namespace("lead/");
        let lead = b.elect_leader("custodian").await.unwrap();
        b_won_task.store(true, Ordering::SeqCst);
        (b, lead)
    });

    // Give B ample simulated time to start and (wrongly) win if the store allowed
    // a concurrent grant.
    sleep(Duration::from_millis(500)).await;
    assert!(
        !b_won.load(Ordering::SeqCst),
        "split-brain: B won leadership while A still holds the term"
    );

    // A drops: its keep-alive revokes A's lease at once, so B wins immediately —
    // no waiting out A's HOLD_TTL.
    drop(a);
    let (_b, lead_b) = b_task.await.unwrap();
    assert!(
        b_won.load(Ordering::SeqCst),
        "B must win once A releases the term"
    );
    assert!(
        lead_b.token > lead_a.token,
        "B's term fences A's after handoff: {} !> {}",
        lead_b.token,
        lead_a.token
    );
}

/// The single-leader clause, driven against two etcd instances on the simulated
/// cluster (its RED counterpart runs the identical helper against two mem
/// instances below).
#[madsim::test]
async fn single_leader_is_exclusive_across_two_etcd_instances() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "excl/").await;
    let b = connect(&handle, "B", "10.1.0.12", "excl/").await;
    conformance::cross_instance_single_leader_is_exclusive(&a, "custodian", || {
        campaign_b_bounded(&b, "custodian")
    })
    .await;
}

/// A lock is mutually exclusive across two independent instances, and re-acquiring
/// after release fences the prior holder.
#[madsim::test]
async fn a_lock_is_mutually_exclusive_across_two_instances() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "lock/").await;
    let b = connect(&handle, "B", "10.1.0.12", "lock/").await;
    conformance::cross_instance_lock_is_mutually_exclusive(&a, &b).await;
}

/// Peers register and are discovered across instances — the "peers discovered
/// through L5" property #256 depends on.
#[madsim::test]
async fn peers_are_discovered_across_instances() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "disc/").await;
    let b = connect(&handle, "B", "10.1.0.12", "disc/").await;
    conformance::cross_instance_registration_is_discoverable(&a, &b).await;
}

// ---- Demonstrated-RED: a broken (process-local) store fails the clauses ------
//
// Two independent `coordination-mem` instances share no state, so they cannot
// provide cross-process coordination. Running the SAME helpers the etcd store
// passes against them must go RED — proof the clauses are non-vacuous.

#[madsim::test]
#[should_panic(expected = "split-brain")]
async fn process_local_store_fails_the_single_leader_clause() {
    let a = MemCoordination::new();
    let b = MemCoordination::new();
    conformance::cross_instance_single_leader_is_exclusive(&a, "custodian", || {
        campaign_b_bounded(&b, "custodian")
    })
    .await;
}

#[madsim::test]
#[should_panic(expected = "must refuse B across instances")]
async fn process_local_store_fails_the_mutual_exclusion_clause() {
    let a = MemCoordination::new();
    let b = MemCoordination::new();
    conformance::cross_instance_lock_is_mutually_exclusive(&a, &b).await;
}

#[madsim::test]
#[should_panic(expected = "must discover BOTH peers")]
async fn process_local_store_fails_the_cross_process_discovery_clause() {
    let a = MemCoordination::new();
    let b = MemCoordination::new();
    conformance::cross_instance_registration_is_discoverable(&a, &b).await;
}

// ---- Lease expiry, orphan-safety, and lapse-recovery -------------------------

/// A registration lease that is not kept alive EXPIRES once its TTL elapses —
/// deterministic on the simulator's ticked lease clock (the etcd analogue of
/// mem's ManualClock expiry).
#[madsim::test]
async fn a_registration_lease_expires_deterministically() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "exp/").await;
    a.register("svc", Bytes::from_static(b"n"), Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        a.discover("svc").await.unwrap(),
        vec![Bytes::from_static(b"n")]
    );

    // Past the TTL with no renewal: the registration lapses (etcd deletes the
    // lease's key), so a crashed member's registration disappears.
    sleep(Duration::from_secs(4)).await;
    assert!(
        a.discover("svc").await.unwrap().is_empty(),
        "the registration lapses once its lease expires"
    );
}

/// A cancelled campaign leaks NO orphan: when B's `elect_leader` future is dropped
/// by a timeout mid-campaign, B's speculative lease + candidate key are revoked at
/// once, so a THIRD instance still wins after A releases — it never deadlocks
/// behind an orphaned candidacy that a detached keep-alive renews forever.
#[madsim::test]
async fn a_cancelled_campaign_leaks_no_orphan() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "orph/").await;
    let lead_a = a.elect_leader("custodian").await.unwrap();

    // B campaigns but its future is cancelled by a timeout while A still leads.
    let b = connect(&handle, "B", "10.1.0.12", "orph/").await;
    let cancelled = timeout(Duration::from_millis(300), b.elect_leader("custodian")).await;
    assert!(
        cancelled.is_err(),
        "B's campaign must not resolve while A leads"
    );

    // A releases. A THIRD instance must win — proof B's cancelled campaign left no
    // orphan blocking the election.
    drop(a);
    let c = connect(&handle, "C", "10.1.0.13", "orph/").await;
    let lead_c = timeout(Duration::from_secs(30), c.elect_leader("custodian"))
        .await
        .expect("C must win, not deadlock behind B's cancelled candidacy")
        .unwrap();
    assert!(
        lead_c.token > lead_a.token,
        "C's term fences A's: {} !> {}",
        lead_c.token,
        lead_a.token
    );
    // Keep B alive until here so its Drop isn't what cleared the orphan.
    drop(b);
}

/// A leader whose lease genuinely LAPSES (here: revoked out from under it, the
/// deterministic analogue of a partition past its TTL) is detected by the
/// keep-alive — the authoritative `lost` signal — so a later `elect_leader` on the
/// same instance RE-CAMPAIGNS for a fresh term instead of proclaiming on a leader
/// key etcd has already deleted.
///
/// This is the distinction earlier iterations got wrong (rejection #5): loss is
/// concluded ONLY from the keep-alive, NEVER inferred from a transient proclaim
/// error. The test is load-bearing on exactly that: with the fix, A's keep-alive
/// records the loss and A re-campaigns and wins; WITHOUT it (loss ignored), A would
/// take the "still leading" path and proclaim on its deleted leader key, which the
/// store now propagates as a transient error — so `elect_leader` would return `Err`
/// and the `unwrap` below would panic. (Verified: reverting the `is_lost` check
/// turns this red with `ElectError("session expired")`.)
#[madsim::test]
async fn a_lapsed_leader_recampaigns_after_its_lease_is_lost() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "lapse/").await;
    let lead_a = a.elect_leader("custodian").await.unwrap();

    // Deterministically make A's leadership lease lapse: find the lease backing A's
    // leader key via an admin client and revoke it directly. This is a
    // connection-stable, seed-independent stand-in for "A was partitioned past its
    // TTL and etcd expired its lease" — without the flakiness of clogging A's own
    // client connection.
    let admin = admin_client(&handle, "10.1.0.20").await;
    let leader_keys = admin
        .kv_client()
        .get(
            // ns "lapse/" + election sub-prefix "elect/" + key "custodian"
            "lapse/elect/custodian",
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .unwrap();
    let lease_id = leader_keys
        .kvs()
        .first()
        .expect("A's leader key exists in etcd")
        .lease();
    assert_ne!(lease_id, 0, "A's leader key is lease-bound");
    admin.lease_client().revoke(lease_id).await.unwrap();

    // A's keep-alive observes the revoke on its next renewal (period = HOLD_TTL/3 =
    // 2s) and records the loss; 3s is comfortably past that, deterministically.
    sleep(Duration::from_secs(3)).await;

    // A re-elects: because the keep-alive reported the loss, A RE-CAMPAIGNS (it does
    // not proclaim on its now-deleted leader key) and re-earns a fenced term.
    let lead_a2 = a
        .elect_leader("custodian")
        .await
        .expect("A must re-campaign and win after its lease lapsed, not error on a dead key");
    assert!(
        lead_a2.token > lead_a.token,
        "A's recovered term fences its lapsed one: {} !> {}",
        lead_a2.token,
        lead_a.token
    );
}

/// A proclaim RPC error while the lease is STILL LIVE must be treated as TRANSIENT:
/// `elect_leader` surfaces the `Err` to the caller but RETAINS the hold and its
/// renewing lease — it must NOT infer leadership loss from a proclaim error and
/// silently re-campaign (the lease-leak / self-inflicted-churn bug earlier
/// iterations were rejected on; `store.rs`'s "still leading" re-proclaim path).
///
/// The fault is injected deterministically: A wins leadership, then an admin client
/// DELETES A's leader key out from under it WITHOUT revoking A's lease. So:
/// - A's keep-alive keeps renewing the lease → `is_lost()` stays `false` → the next
///   `elect_leader` takes the "still leading" re-proclaim path (not a fresh
///   campaign);
/// - but the proclaim targets a leader key etcd no longer has, so it ERRORS.
///
/// A correct store propagates that `Err` and leaves A's lease alive. A store that
/// (wrongly) treated the proclaim error as loss would either return `Ok` (a silent
/// re-campaign — caught by the `is_err()` assert) or drop the hold and stop renewing
/// (caught by the still-alive-lease assert past the TTL).
#[madsim::test]
async fn a_transient_proclaim_error_keeps_the_hold_and_its_lease() {
    let handle = Handle::current();
    boot_etcd(&handle);
    sleep(Duration::from_secs(1)).await;

    let a = connect(&handle, "A", "10.1.0.11", "trans/").await;
    a.elect_leader("custodian").await.unwrap();

    // Find A's leader key and the lease backing it via an admin client.
    let admin = admin_client(&handle, "10.1.0.20").await;
    let leader_keys = admin
        .kv_client()
        .get(
            "trans/elect/custodian",
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .unwrap();
    let kv = leader_keys
        .kvs()
        .first()
        .expect("A's leader key exists in etcd");
    let lease_id = kv.lease();
    assert_ne!(lease_id, 0, "A's leader key is lease-bound");
    let leader_key = kv.key().to_vec();

    // Delete ONLY the leader key; leave A's lease untouched so the keep-alive keeps
    // renewing it (this is what makes the coming proclaim error TRANSIENT — the
    // lease is still perfectly alive).
    admin.kv_client().delete(leader_key, None).await.unwrap();

    // The re-proclaim targets the now-deleted key and errors. The store MUST surface
    // that as `Err`, NOT swallow it into a silent re-campaign that returns `Ok`.
    assert!(
        a.elect_leader("custodian").await.is_err(),
        "a proclaim error on a still-live lease must surface as Err, not a silent re-campaign"
    );

    // ...and the hold's lease is RETAINED. Past the HOLD_TTL (6s) the lease is only
    // still alive if the keep-alive kept renewing it — i.e. the hold was NOT dropped.
    sleep(Duration::from_secs(8)).await;
    let ttl = admin
        .lease_client()
        .time_to_live(lease_id, None)
        .await
        .unwrap();
    assert!(
        ttl.ttl() > 0,
        "A's lease must stay alive (hold retained + still renewing) after the transient \
         proclaim error, ttl={}",
        ttl.ttl()
    );
}
