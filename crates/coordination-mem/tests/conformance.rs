//! Conformance tests for the in-memory `Coordination`.
//!
//! The generic trait-contract assertions now live in the **shared**
//! `wyrd-coordination-conformance` crate (ADR-0006, #365): the in-memory backend
//! drives the identical suite the etcd backend drives, so the L5 contract is
//! pinned by both implementations rather than forked. The in-memory methods never
//! yield, so `pollster::block_on` drives the async surface deterministically;
//! lease expiry is exercised with a `ManualClock`.
//!
//! The **mem-specific** properties the shared suite deliberately does NOT pin
//! (deterministic lease expiry under a manual clock; the rising-lease-id and
//! config-starts-at-zero absolutes that etcd does not promise) stay here.

use std::time::Duration;

use bytes::Bytes;
use pollster::block_on;
use wyrd_coordination_conformance as conformance;
use wyrd_coordination_mem::MemCoordination;
use wyrd_testkit::ManualClock;
use wyrd_traits::Coordination;

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_owned())
}

/// A lease long enough that nothing expires during a fast test.
const LONG: Duration = Duration::from_secs(3600);

// ---- The shared, not forked, contract suite --------------------------------

#[test]
fn trait_contract() {
    // The whole shared contract via the single `run_all` runner, so mem and etcd
    // drive the identical clause set with no per-driver list to drift. Each clause
    // gets a fresh, empty coordinator — the same isolation the etcd target
    // provides per clause (a fresh key namespace).
    block_on(conformance::run_all(|_tag| async {
        MemCoordination::new()
    }));
}

// ---- Mem-specific: rising lease ids (the shared clause only pins distinctness) --

#[test]
fn lease_ids_rise_within_the_process() {
    block_on(async {
        let c = MemCoordination::new();
        let first = c.register("k", b("v1"), LONG).await.unwrap();
        let second = c.register("k", b("v2"), LONG).await.unwrap();
        assert!(
            second.id > first.id,
            "mem hands out strictly rising lease ids (etcd only promises distinctness)"
        );
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

/// `lib.rs:177` (`> -> >=`) AND `lib.rs:149` (`> -> >=`) — a lease lapses **at**
/// its expiry instant, not one millisecond later. The existing tests only step the
/// clock strictly past expiry, so flipping `>` to `>=` (treat `now == expiry` as
/// still live) survived. Pin the boundary `now == expiry`:
///   * `discover` must report the member as gone, and
///   * `renew` must refuse it as expired.
#[test]
fn a_lease_is_gone_at_its_exact_expiry_instant() {
    block_on(async {
        let clock = ManualClock::new(0);
        let c = MemCoordination::with_clock(clock.clone());
        let lease = c
            .register("svc", b("n"), Duration::from_millis(1_000))
            .await
            .unwrap(); // expiry = 0 + 1000 = 1000

        clock.set(1_000); // now == expiry exactly
        assert!(
            c.discover("svc").await.unwrap().is_empty(),
            "a registration is gone AT its expiry instant (`> now`), not one ms after"
        );
        assert!(
            c.renew(lease).await.is_err(),
            "a lease cannot be renewed AT its expiry instant"
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

// ---- Config: mem-specific absolute revision (shared suite pins only that it rises) --

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
