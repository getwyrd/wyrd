//! Conformance tests for the in-memory `Coordination`.
//!
//! The contract assertions are written against the `Coordination` *trait*
//! surface (helpers over `&impl Coordination`), so they lift to a shared suite
//! when a second backend (etcd) arrives. The in-memory methods never yield, so
//! `pollster::block_on` drives the async surface deterministically.

use bytes::Bytes;
use pollster::block_on;
use wyrd_coordination_mem::MemCoordination;
use wyrd_traits::Coordination;

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_owned())
}

// ---- Trait contract (generic over any Coordination) ------------------------

async fn contract_register_then_discover(c: &impl Coordination) {
    assert!(
        c.discover("svc/d").await.unwrap().is_empty(),
        "an unknown key discovers nobody"
    );

    c.register("svc/d", b("node-a")).await.unwrap();
    c.register("svc/d", b("node-b")).await.unwrap();

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
    let first = c.register("k", b("v1")).await.unwrap();
    let second = c.register("k", b("v2")).await.unwrap();
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

async fn contract_locks_are_fenced(c: &impl Coordination) {
    let first = c.lock("inode/7").await.unwrap();
    let second = c.lock("inode/7").await.unwrap();
    assert!(
        second.token > first.token,
        "each lock grant carries a strictly rising fencing token"
    );
}

async fn contract_fencing_tokens_rise_across_locks_and_elections(c: &impl Coordination) {
    // Leadership and locks draw from one monotonic source, so any later grant
    // fences any earlier one regardless of which kind it is.
    let a = c.lock("x").await.unwrap().token;
    let b = c.elect_leader("y").await.unwrap().token;
    let d = c.lock("z").await.unwrap().token;
    assert!(a < b && b < d, "tokens are globally monotonic: {a} {b} {d}");
}

#[test]
fn trait_contract() {
    block_on(async {
        contract_register_then_discover(&MemCoordination::new()).await;
        contract_leases_are_unique_and_rising(&MemCoordination::new()).await;
        contract_election_is_always_granted_and_fenced(&MemCoordination::new()).await;
        contract_locks_are_fenced(&MemCoordination::new()).await;
        contract_fencing_tokens_rise_across_locks_and_elections(&MemCoordination::new()).await;
    });
}

// ---- Config: seeded at construction, read back -----------------------------

#[test]
fn config_reads_back_what_was_seeded() {
    block_on(async {
        let c = MemCoordination::with_config([
            ("zone".to_owned(), b("z-alpha")),
            ("replication".to_owned(), b("1")),
        ]);
        assert_eq!(
            c.get_config("zone").await.unwrap().as_deref(),
            Some(&b"z-alpha"[..])
        );
        assert_eq!(
            c.get_config("replication").await.unwrap().as_deref(),
            Some(&b"1"[..])
        );
        assert_eq!(c.get_config("missing").await.unwrap(), None);
    });
}

#[test]
fn empty_coordinator_has_no_config() {
    block_on(async {
        let c = MemCoordination::new();
        assert_eq!(c.get_config("anything").await.unwrap(), None);
    });
}
