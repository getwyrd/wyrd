//! The **shared** `Coordination` trait-contract suite.
//!
//! These assertions are written against the [`Coordination`] *trait* surface
//! (never a concrete backend, ADR-0016), so **one** suite pins the contract for
//! **every** implementation instead of each backend forking its own copy — the
//! discipline that "a trait's semantics are pinned by two implementations"
//! (ADR-0006; proposal 0015 §"Deployment prerequisite"). They were lifted out of
//! `crates/coordination-mem/tests/conformance.rs`, whose own header noted they
//! "lift to a shared suite when a second backend (etcd) arrives" — that arrival
//! is #365's `coordination-etcd`.
//!
//! Each function takes `&impl Coordination` and asserts one contract clause. A
//! backend's test target supplies a **fresh, empty store per function** (so the
//! functions never collide on keys) and drives them under whatever executor that
//! backend needs — `pollster::block_on` for the synchronous in-memory store, a
//! `tokio`/`madsim` runtime for the networked etcd store.
//!
//! ## What is deliberately NOT here
//!
//! Every clause here operates on a **single** `&impl Coordination`, because the
//! suite also runs against process-local `coordination-mem` (two mem instances
//! share no state). The **cross-process** guarantees that justify the etcd
//! backend — a single leader across processes, mutual exclusion across processes,
//! peers discovered across processes, and deterministic lease expiry — cannot be
//! stated against one single-process instance, so they live in the networked
//! backend's own two-instance tests (`crates/dst/tests/coordination.rs` under the
//! madsim etcd simulator, and `crates/coordination-etcd/tests/conformance.rs`
//! against a real etcd). This is not an etcd-only *fork* of a shared clause; it
//! is the set of properties that are only meaningful with two instances.
//!
//! Absolute values that are backend-specific (mem's config revision starts at 0
//! and rises by exactly 1; mem's lease ids rise) are asserted in mem's own
//! conformance, not here — the shared clauses assert only what BOTH backends
//! promise (distinct lease ids; a config revision that strictly *rises* on a
//! write).

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::Bytes;
use wyrd_traits::{Coordination, Leadership};

/// A lease long enough that nothing expires during a fast, single-instance test.
const LONG: Duration = Duration::from_secs(3600);

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_owned())
}

/// A key registers, is discovered, and is isolated from other keys.
pub async fn contract_register_then_discover(c: &impl Coordination) {
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

/// Two registrations get **distinct** lease ids. (The trait promises only an
/// opaque identifier, not a rising one — etcd lease ids are opaque `i64`s,
/// monotone only within a session; mem's rising ids satisfy distinctness a
/// fortiori. mem's own conformance additionally pins the rising property.)
pub async fn contract_leases_are_distinct(c: &impl Coordination) {
    let first = c.register("k", b("v1"), LONG).await.unwrap();
    let second = c.register("k", b("v2"), LONG).await.unwrap();
    assert_ne!(
        second.id, first.id,
        "each registration gets a distinct lease id"
    );
}

/// A registration can be **renewed** while live and **revoked** to withdraw it
/// immediately; a revoked lease can no longer be renewed. (This is the
/// registration-renewal path production wires — a `renew` defect would silently
/// lapse D-server registrations, so it must have contract coverage on both
/// backends.)
pub async fn contract_renew_and_revoke(c: &impl Coordination) {
    let lease = c.register("svc", b("n"), LONG).await.unwrap();
    assert_eq!(c.discover("svc").await.unwrap(), vec![b("n")]);

    // Renewing a live lease keeps the registration.
    c.renew(lease).await.unwrap();
    assert_eq!(
        c.discover("svc").await.unwrap(),
        vec![b("n")],
        "a renewed lease stays discoverable"
    );

    // Revoking withdraws it at once.
    c.revoke(lease).await.unwrap();
    assert!(
        c.discover("svc").await.unwrap().is_empty(),
        "a revoked registration is withdrawn immediately"
    );

    // A revoked lease can no longer be renewed.
    assert!(
        c.renew(lease).await.is_err(),
        "a revoked (or expired) lease cannot be renewed"
    );
}

/// Electing yields a leader, and re-electing opens a new fenced term with a
/// strictly higher token. (mem: a lone process is always leader; etcd: the first
/// campaign wins and a second call by the same holder re-proclaims a fresh term.)
pub async fn contract_election_is_granted_and_fenced(c: &impl Coordination) {
    let first = c.elect_leader("custodian").await.unwrap();
    let second = c.elect_leader("custodian").await.unwrap();
    assert!(
        second.token > first.token,
        "each election term fences the last: {} !> {}",
        second.token,
        first.token
    );
}

/// A lock is mutually exclusive (a held key refuses contenders), fenced (a later
/// acquisition's token is higher), and isolated by key.
pub async fn contract_locks_are_mutually_exclusive_and_fenced(c: &impl Coordination) {
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
        "re-acquiring fences the prior holder: {} !> {}",
        second.token,
        first.token
    );

    // Distinct keys do not contend.
    assert!(c.lock("inode/8").await.unwrap().is_some());
}

/// Leadership and locks draw fencing tokens from one monotonic source, so any
/// later grant fences any earlier one regardless of which kind it is.
pub async fn contract_fencing_tokens_rise_across_locks_and_elections(c: &impl Coordination) {
    let a = c.lock("x").await.unwrap().unwrap().token;
    let leader = c.elect_leader("y").await.unwrap().token;
    let d = c.lock("z").await.unwrap().unwrap().token;
    assert!(
        a < leader && leader < d,
        "tokens are globally monotonic: {a} {leader} {d}"
    );
}

/// Config is mutable, read-back is exact, overwrites are visible, and the config
/// revision **strictly rises** on every write. (This is binding criterion (a)'s
/// "config with a monotonic revision" — asserted RELATIVELY, `r1 > r0`, so it
/// holds for both mem's per-write counter and etcd's cluster-global mvcc
/// revision, without pinning either's absolute values.)
pub async fn contract_config_is_revisioned(c: &impl Coordination) {
    let r0 = c.config_revision().await.unwrap();
    assert_eq!(
        c.get_config("zone").await.unwrap(),
        None,
        "an unset config key reads as None"
    );

    c.set_config("zone", b("z-alpha")).await.unwrap();
    assert_eq!(
        c.get_config("zone").await.unwrap().as_deref(),
        Some(&b"z-alpha"[..]),
        "the written value reads back"
    );
    let r1 = c.config_revision().await.unwrap();
    assert!(
        r1 > r0,
        "a write strictly raises the revision: {r1} !> {r0}"
    );

    // Overwrites are visible and raise the revision again (a watcher re-reads).
    c.set_config("zone", b("z-beta")).await.unwrap();
    assert_eq!(
        c.get_config("zone").await.unwrap().as_deref(),
        Some(&b"z-beta"[..]),
        "an overwrite is visible"
    );
    let r2 = c.config_revision().await.unwrap();
    assert!(
        r2 > r1,
        "an overwrite strictly raises the revision again: {r2} !> {r1}"
    );

    // Config-ONLY advancement: an unrelated (non-config) coordination write must
    // NOT move the config revision. A config watcher polls this revision to decide
    // when to re-read config, so it must not wake on every registration / lock /
    // election in the cluster. This pins the revision to the config keyspace on
    // BOTH backends — mem's counter is config-scoped by construction; etcd must
    // report a config-scoped revision (max mod_revision over config keys), not its
    // global mvcc counter (which every write bumps). A store that returns a
    // cluster-global write counter fails HERE.
    let before_unrelated = c.config_revision().await.unwrap();
    c.register("unrelated-svc", b("member"), LONG)
        .await
        .unwrap();
    let after_unrelated = c.config_revision().await.unwrap();
    assert_eq!(
        after_unrelated, before_unrelated,
        "an unrelated (non-config) write must NOT advance the config revision: \
         {after_unrelated} != {before_unrelated}"
    );

    assert_eq!(c.get_config("missing").await.unwrap(), None);
}

// ---- Cross-instance clauses (two networked instances) -----------------------
//
// The single-process `contract_*` clauses above run against BOTH backends,
// including process-local `coordination-mem` (two mem instances share no state, so
// nothing cross-process can be stated there). The clauses BELOW are the properties
// that are only meaningful with two independent instances of a *networked* backend
// — a single leader across processes, mutual exclusion across processes, peers
// discovered across processes — the "L5 discovery" guarantees that justify the etcd
// backend (proposal 0015 §"Deployment prerequisite", #365, criterion (b)).
//
// They live HERE, in the one shared suite, rather than being re-stated in each
// backend's own test — so `coordination-etcd`'s real-etcd conformance
// (`cargo xtask etcd-conformance`) and its madsim simulator proof
// (`crates/dst/tests/coordination.rs`) drive the SAME assertion, never a fork
// (ADR-0006 "one contract, two implementations"; ADR-0016). Their demonstrated-RED
// counterpart runs the identical helpers against two `coordination-mem` instances,
// which share no state and so go RED — proof the clauses are non-vacuous.

/// **Single leader across instances (no split-brain).** While `a` holds leadership
/// of `key`, a concurrent campaign by a second instance MUST NOT resolve — a store
/// that granted two leaders at once (split-brain) would let the second campaign
/// complete. This is criterion (b)'s headline safety property and the custodian's
/// single-active guard (M3.3/#141) depends on it.
///
/// The suite is runtime-agnostic (it drives `coordination-mem` on `pollster`,
/// `coordination-etcd` on `tokio` / `madsim`), so the caller supplies the bounded
/// wait via `campaign_b_bounded`: it races the second instance's `elect_leader`
/// against the caller's own runtime timeout and yields `None` when it stayed pending
/// (the correct outcome) or `Some(_)` when it wrongly won concurrently. A
/// process-local pair returns `Some` (mem grants every lone process), so the SAME
/// helper goes RED against two mem instances — the demonstrated-red pin.
pub async fn cross_instance_single_leader_is_exclusive<A, W, WFut>(
    a: &A,
    key: &str,
    campaign_b_bounded: W,
) where
    A: Coordination,
    W: FnOnce() -> WFut,
    WFut: core::future::Future<Output = Option<Leadership>>,
{
    a.elect_leader(key).await.unwrap();
    let b_outcome = campaign_b_bounded().await;
    assert!(
        b_outcome.is_none(),
        "split-brain: B won leadership while A still holds the term"
    );
}

/// **Mutual exclusion across instances.** A lock held by `a` refuses `b`, and once
/// `a` releases, `b` acquires with a strictly higher (fencing) token.
pub async fn cross_instance_lock_is_mutually_exclusive(
    a: &impl Coordination,
    b: &impl Coordination,
) {
    let held = a
        .lock("inode/7")
        .await
        .unwrap()
        .expect("A acquires the free lock");
    assert!(
        b.lock("inode/7").await.unwrap().is_none(),
        "the lock A holds must refuse B across instances"
    );

    a.unlock(held).await.unwrap();
    let reb = b
        .lock("inode/7")
        .await
        .unwrap()
        .expect("B acquires after A releases");
    assert!(
        reb.token > held.token,
        "B fences A across instances: {} !> {}",
        reb.token,
        held.token
    );
}

/// **Cross-process discovery.** Each instance discovers BOTH peers' registrations —
/// the "peers discovered through L5" property #256 depends on.
pub async fn cross_instance_registration_is_discoverable(
    a: &impl Coordination,
    b: &impl Coordination,
) {
    a.register("svc/gateway", Bytes::from_static(b"A"), LONG)
        .await
        .unwrap();
    b.register("svc/gateway", Bytes::from_static(b"B"), LONG)
        .await
        .unwrap();
    let mut seen = b.discover("svc/gateway").await.unwrap();
    seen.sort();
    assert_eq!(
        seen,
        vec![Bytes::from_static(b"A"), Bytes::from_static(b"B")],
        "each instance must discover BOTH peers' registrations across instances"
    );
}

/// Drive **every** contract in this suite against a fresh store per clause.
///
/// A backend runs the whole contract by calling this ONE function, so there is no
/// per-driver list to drift out of sync: a new `contract_*` added here is picked
/// up by **both** backends automatically (the seam that kept the config +
/// renew/revoke clauses from being exercised on only one backend). `make_store(tag)`
/// yields a fresh, isolated store for each clause — mem hands back a new in-memory
/// coordinator, etcd a client scoped to a fresh per-`tag` key namespace against
/// the one shared cluster — the fresh-store-per-clause isolation every clause assumes.
pub async fn run_all<C, F, Fut>(mut make_store: F)
where
    C: Coordination,
    F: FnMut(&'static str) -> Fut,
    Fut: core::future::Future<Output = C>,
{
    contract_register_then_discover(&make_store("register_then_discover").await).await;
    contract_leases_are_distinct(&make_store("leases_distinct").await).await;
    contract_renew_and_revoke(&make_store("renew_and_revoke").await).await;
    contract_election_is_granted_and_fenced(&make_store("election").await).await;
    contract_locks_are_mutually_exclusive_and_fenced(&make_store("locks").await).await;
    contract_fencing_tokens_rise_across_locks_and_elections(&make_store("fencing").await).await;
    contract_config_is_revisioned(&make_store("config").await).await;
}
