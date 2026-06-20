//! Conformance tests for the in-memory `Coordination`.
//!
//! The contract assertions are written against the `Coordination` *trait*
//! surface (helpers over `&impl Coordination`), so they lift to a shared suite
//! when a second backend (etcd) arrives. The in-memory methods never yield, so
//! `pollster::block_on` drives the async surface deterministically; lease expiry
//! is exercised with a `ManualClock`.

use std::time::Duration;

use bytes::Bytes;
use pollster::block_on;
use wyrd_coordination_mem::MemCoordination;
use wyrd_testkit::ManualClock;
use wyrd_traits::Coordination;

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_owned())
}

/// A lease long enough that nothing expires during a fast test.
const LONG: Duration = Duration::from_secs(3600);

// ---- Trait contract (generic over any Coordination) ------------------------

async fn contract_register_then_discover(c: &impl Coordination) {
    assert!(
        c.discover("svc/d").await.unwrap().is_empty(),
        "an unknown key discovers nobody"
    );

    c.register("svc/d", b("node-a"), LONG).await.unwrap();
    c.register("svc/d", b("node-b"), LONG).await.unwrap();

    let mut found = c.discover("svc/d").await.unwrap();
    found.sort();
    assert_eq!(
        found,
        vec![b("node-a"), b("node-b")],
        "both members surface"
    );

    // Registrations are isolated by key.
    assert!(c.discover("svc/other").await.unwrap().is_empty());
}

async fn contract_leases_are_unique_and_rising(c: &impl Coordination) {
    let first = c.register("k", b("v1"), LONG).await.unwrap();
    let second = c.register("k", b("v2"), LONG).await.unwrap();
    assert!(
        second.id > first.id,
        "each registration gets a distinct, rising lease id"
    );
}

async fn contract_election_is_always_granted_and_fenced(c: &impl Coordination) {
    let first = c.elect_leader("custodian").await.unwrap();
    let second = c.elect_leader("custodian").await.unwrap();
    assert!(
        second.token > first.token,
        "a lone process is always leader, and each term fences the last"
    );
}

async fn contract_locks_are_mutually_exclusive_and_fenced(c: &impl Coordination) {
    let first = c
        .lock("inode/7")
        .await
        .unwrap()
        .expect("a free lock is granted");
    assert!(
        c.lock("inode/7").await.unwrap().is_none(),
        "a held lock refuses contenders"
    );

    c.unlock(first).await.unwrap();
    let second = c
        .lock("inode/7")
        .await
        .unwrap()
        .expect("the lock is grantable again after release");
    assert!(
        second.token > first.token,
        "re-acquiring fences the prior holder"
    );

    // Distinct keys do not contend.
    assert!(c.lock("inode/8").await.unwrap().is_some());
}

async fn contract_fencing_tokens_rise_across_locks_and_elections(c: &impl Coordination) {
    // Leadership and locks draw from one monotonic source, so any later grant
    // fences any earlier one regardless of which kind it is.
    let a = c.lock("x").await.unwrap().unwrap().token;
    let b = c.elect_leader("y").await.unwrap().token;
    let d = c.lock("z").await.unwrap().unwrap().token;
    assert!(a < b && b < d, "tokens are globally monotonic: {a} {b} {d}");
}

#[test]
fn trait_contract() {
    block_on(async {
        contract_register_then_discover(&MemCoordination::new()).await;
        contract_leases_are_unique_and_rising(&MemCoordination::new()).await;
        contract_election_is_always_granted_and_fenced(&MemCoordination::new()).await;
        contract_locks_are_mutually_exclusive_and_fenced(&MemCoordination::new()).await;
        contract_fencing_tokens_rise_across_locks_and_elections(&MemCoordination::new()).await;
    });
}

// ---- Lease expiry / renewal / revocation (under a manual clock) -------------

#[test]
fn a_lease_expires_once_its_ttl_elapses() {
    block_on(async {
        let clock = ManualClock::new(0);
        let c = MemCoordination::with_clock(clock.clone());
        c.register("svc", b("n"), Duration::from_millis(1_000))
            .await
            .unwrap();
        assert_eq!(c.discover("svc").await.unwrap(), vec![b("n")]);

        clock.advance(1_001);
        assert!(
            c.discover("svc").await.unwrap().is_empty(),
            "the registration lapses once its lease expires"
        );
    });
}

#[test]
fn renewal_extends_a_lease_and_expired_leases_cannot_renew() {
    block_on(async {
        let clock = ManualClock::new(0);
        let c = MemCoordination::with_clock(clock.clone());
        let lease = c
            .register("svc", b("n"), Duration::from_millis(1_000))
            .await
            .unwrap();

        clock.advance(500);
        c.renew(lease).await.unwrap(); // expiry now at 1500

        clock.advance(600); // now 1100 < 1500
        assert_eq!(
            c.discover("svc").await.unwrap(),
            vec![b("n")],
            "renewed lease still live"
        );

        clock.advance(500); // now 1600 > 1500
        assert!(
            c.discover("svc").await.unwrap().is_empty(),
            "lapses after the renewal window"
        );
        assert!(
            c.renew(lease).await.is_err(),
            "an expired lease cannot be renewed"
        );
    });
}

#[test]
fn revoke_withdraws_a_registration_immediately() {
    block_on(async {
        let clock = ManualClock::new(0);
        let c = MemCoordination::with_clock(clock.clone());
        let lease = c.register("svc", b("n"), LONG).await.unwrap();
        assert_eq!(c.discover("svc").await.unwrap(), vec![b("n")]);

        c.revoke(lease).await.unwrap();
        assert!(
            c.discover("svc").await.unwrap().is_empty(),
            "revoked immediately"
        );
    });
}

// ---- Config: mutable, revisioned -------------------------------------------

#[test]
fn config_is_mutable_and_revisioned() {
    block_on(async {
        let c = MemCoordination::new();
        assert_eq!(c.config_revision().await.unwrap(), 0);
        assert_eq!(c.get_config("zone").await.unwrap(), None);

        c.set_config("zone", b("z-alpha")).await.unwrap();
        assert_eq!(
            c.get_config("zone").await.unwrap().as_deref(),
            Some(&b"z-alpha"[..])
        );
        assert_eq!(
            c.config_revision().await.unwrap(),
            1,
            "a write bumps the revision"
        );

        c.set_config("replication", b("1")).await.unwrap();
        assert_eq!(c.config_revision().await.unwrap(), 2);

        // Overwrites are visible and bump the revision (a watcher would re-read).
        c.set_config("zone", b("z-beta")).await.unwrap();
        assert_eq!(
            c.get_config("zone").await.unwrap().as_deref(),
            Some(&b"z-beta"[..])
        );
        assert_eq!(c.config_revision().await.unwrap(), 3);

        assert_eq!(c.get_config("missing").await.unwrap(), None);
    });
}
