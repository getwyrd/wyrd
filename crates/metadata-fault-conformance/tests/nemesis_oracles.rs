//! Check-time coverage for the **composable nemesis** seam (#407): the three leg kinds, each
//! leg's **materialization oracle** — the pure decision logic over recorded observations that
//! refuses to report a fault that did not bite (the #442 rule) — AND the central [`drive_leg`]
//! rule itself (a fault that did not materialize FAILS, an incomplete heal FAILS, and the fault
//! is healed on every path including a panicking workload).
//!
//! The oracle functions here are the same `materialized()` the live `docker`/`fdbcli` impls
//! call, so a regression in the decision logic flips BOTH this test and the live leg. The
//! [`drive_leg`] tests use a `MockLeg` — no `docker` — so the rule that "an un-materialized fault
//! never passes silently" is GUARDED at Check, not left to a live run.
//!
//! Import-light on purpose: only the pure oracle arithmetic + enumeration + parse helpers + the
//! mock-driven lifecycle, never `docker` — so it runs inside the unprivileged `cargo xtask ci`
//! gate (the "deferred ≠ unbuilt" bar), while the live three-leg runs are opt-in (`WYRD_TIER1=1`),
//! off-Check.

use std::cell::Cell;
use std::time::Duration;

use wyrd_metadata_fault_conformance::nemesis::{
    clock_offset_secs, drive_leg, fdb_cluster_fully_recovered, inspected_status_is_paused,
    inspected_status_is_running, MaterializationEvidence, NemesisLeg, NemesisLegKind,
    PartitionEvidence, PauseEvidence, SkewEvidence,
};

#[test]
fn the_nemesis_exposes_exactly_the_three_fault_classes() {
    // The campaign must enumerate partition / clock-skew / process-pause — dropping any is a
    // fault class the nemesis silently stopped exercising (the born-at-tier bar; not merely
    // absent, but red).
    let all = NemesisLegKind::ALL;
    assert!(
        all.contains(&NemesisLegKind::Partition),
        "the nemesis must expose a network-partition leg: {all:?}"
    );
    assert!(
        all.contains(&NemesisLegKind::ClockSkew),
        "the nemesis must expose a clock-skew leg (the class nothing implemented before #407): {all:?}"
    );
    assert!(
        all.contains(&NemesisLegKind::ProcessPause),
        "the nemesis must expose a process-pause leg: {all:?}"
    );

    // Each leg carries a DISTINCT slug — collapsing two into one slug would silently merge two
    // fault classes.
    let slugs: std::collections::HashSet<&str> = all.iter().map(|k| k.as_str()).collect();
    assert_eq!(
        slugs.len(),
        all.len(),
        "each leg kind must carry its own slug, not share one: {all:?} -> {slugs:?}"
    );
}

#[test]
fn partition_oracle_needs_a_reachability_flip_while_the_target_stays_running() {
    // Materialized: the survivors saw the target before, lost it during, and the container
    // stayed `running` — a partition, not a crash.
    let bit = PartitionEvidence {
        peers_saw_target_before: true,
        peers_saw_target_during: false,
        target_running_during: true,
    };
    assert!(
        bit.materialized(),
        "a genuine reachability flip must materialize: {bit:?}"
    );
    assert_eq!(bit.kind(), NemesisLegKind::Partition);

    // A no-op cut: the survivors still see the target through the whole window.
    let noop = PartitionEvidence {
        peers_saw_target_during: true,
        ..bit
    };
    assert!(
        !noop.materialized(),
        "a cut the peers never noticed must be INCONCLUSIVE, not a silent pass: {noop:?}"
    );

    // A crash masquerading as a partition: reachability flipped but the container is NOT
    // running. The oracle must reject it — this leg claims a partition specifically.
    let crashed = PartitionEvidence {
        target_running_during: false,
        ..bit
    };
    assert!(
        !crashed.materialized(),
        "a crash (container not running) must not read as a partition: {crashed:?}"
    );

    // A broken oracle that always says "not live": the pre-fault sample fails, so no fault can
    // be manufactured.
    let broken = PartitionEvidence {
        peers_saw_target_before: false,
        peers_saw_target_during: false,
        target_running_during: true,
    };
    assert!(
        !broken.materialized(),
        "if the peers never saw the target live even BEFORE the cut, no fault is provable: {broken:?}"
    );
}

#[test]
fn pause_oracle_needs_serve_then_frozen_paused_not_a_single_probe() {
    // Served before, served NOTHING during, and the container inspected `paused`. The recovery
    // transition (serving again) is proven by `confirm_healed` / `heal_is_complete`, NOT by this
    // gate — so the workload can run WHILE the node is still paused.
    let bit = PauseEvidence {
        served_before: true,
        served_during: false,
        inspected_paused_during: true,
    };
    assert!(
        bit.materialized(),
        "serve→(frozen, paused) must materialize the pre-workload gate: {bit:?}"
    );
    assert_eq!(bit.kind(), NemesisLegKind::ProcessPause);

    // Absence of service WITHOUT a `paused` inspect state could be a partition or a crash — a
    // single "not serving" probe is not enough.
    let not_inspected_paused = PauseEvidence {
        inspected_paused_during: false,
        ..bit
    };
    assert!(
        !not_inspected_paused.materialized(),
        "absence of service without a `paused` inspect state could be a crash/partition, not a pause: \
         {not_inspected_paused:?}"
    );

    // Never served before the freeze: the "before" transition is unproven.
    let no_before = PauseEvidence {
        served_before: false,
        ..bit
    };
    assert!(
        !no_before.materialized(),
        "must observe service BEFORE the freeze: {no_before:?}"
    );

    // Still serving during the freeze ⇒ the pause was a no-op.
    let served_through = PauseEvidence {
        served_during: true,
        ..bit
    };
    assert!(
        !served_through.materialized(),
        "a target that kept serving through the freeze did not pause: {served_through:?}"
    );
}

#[test]
fn skew_oracle_clears_a_nonzero_floor_by_magnitude_forward_or_back() {
    // Forward skew clears the floor.
    let ahead = SkewEvidence {
        observed_offset_secs: 120,
        floor_secs: 60,
    };
    assert!(
        ahead.materialized(),
        "a +120s offset clears a 60s floor: {ahead:?}"
    );
    assert_eq!(ahead.kind(), NemesisLegKind::ClockSkew);

    // Backward skew of the same magnitude also clears it — the leg may skew either direction.
    let behind = SkewEvidence {
        observed_offset_secs: -120,
        ..ahead
    };
    assert!(
        behind.materialized(),
        "a -120s offset also clears a 60s floor by magnitude: {behind:?}"
    );

    // Below the floor ⇒ not materialized.
    let too_small = SkewEvidence {
        observed_offset_secs: 30,
        floor_secs: 60,
    };
    assert!(
        !too_small.materialized(),
        "an offset under the floor must be INCONCLUSIVE: {too_small:?}"
    );

    // A zero floor never materializes — no skew was asked for, so no skew is evidence.
    let no_floor = SkewEvidence {
        observed_offset_secs: 9999,
        floor_secs: 0,
    };
    assert!(
        !no_floor.materialized(),
        "a zero floor means no skew was requested; nothing can be evidence of it: {no_floor:?}"
    );
}

#[test]
fn parse_helpers_read_the_live_impls_observations() {
    // `docker inspect --format {{.State.Status}}` outputs (with trailing newline) drive the
    // pause oracle's running/paused observations.
    assert!(inspected_status_is_running("running\n"));
    assert!(!inspected_status_is_running("paused\n"));
    assert!(inspected_status_is_paused("paused\n"));
    assert!(!inspected_status_is_paused("running\n"));
    assert!(
        !inspected_status_is_paused("exited\n"),
        "a crashed container is not paused"
    );

    // The clock offset the skew oracle keys off: container epoch minus harness epoch.
    assert_eq!(clock_offset_secs(1_000_120, 1_000_000), 120);
    assert_eq!(clock_offset_secs(999_880, 1_000_000), -120);
}

#[test]
fn skew_recovery_gate_needs_available_healthy_and_fully_recovered_not_merely_available() {
    // The skew leg polls THIS oracle after each `--force-recreate` before opening the workload
    // window. Because the fdb services carry no `volumes:`, `--force-recreate` wipes the node, so a
    // cluster that reports `available` can still be re-replicating — the exact restart transient the
    // brief promises the workload window excludes. So `available` alone must NOT read as recovered.
    let fully_recovered = r#"{
      "client": { "database_status": { "available": true, "healthy": true } },
      "cluster": {
        "data": { "state": { "healthy": true, "name": "healthy" } },
        "recovery_state": { "name": "fully_recovered" }
      }
    }"#;
    assert!(
        fdb_cluster_fully_recovered(fully_recovered),
        "an available cluster whose data is healthy AND recovery is fully_recovered has re-stabilized"
    );

    // Available and recovered, but data still moving (re-replicating the wiped node's shards): NOT
    // recovered — opening the workload here would measure the restart, not the skew.
    let re_replicating = r#"{
      "client": { "database_status": { "available": true, "healthy": false } },
      "cluster": {
        "data": { "state": { "healthy": false, "name": "healthy_repartitioning" } },
        "recovery_state": { "name": "fully_recovered" }
      }
    }"#;
    assert!(
        !fdb_cluster_fully_recovered(re_replicating),
        "an `available` cluster whose data is still re-replicating has NOT re-stabilized: {re_replicating}"
    );

    // Available and data healthy, but the transaction subsystem is still recovering.
    let recovering = r#"{
      "client": { "database_status": { "available": true, "healthy": true } },
      "cluster": {
        "data": { "state": { "healthy": true, "name": "healthy" } },
        "recovery_state": { "name": "recruiting_transaction_servers" }
      }
    }"#;
    assert!(
        !fdb_cluster_fully_recovered(recovering),
        "a cluster whose recovery_state is not fully_recovered has not re-stabilized: {recovering}"
    );

    // Unavailable: nothing is recovered.
    let unavailable = r#"{
      "client": { "database_status": { "available": false, "healthy": false } },
      "cluster": { "recovery_state": { "name": "reading_coordinated_state" } }
    }"#;
    assert!(
        !fdb_cluster_fully_recovered(unavailable),
        "an unavailable cluster is not recovered: {unavailable}"
    );
}

// ─── The central `drive_leg` rule, guarded with a mock leg (no docker) ──────────────────────

/// A minimal [`MaterializationEvidence`] whose verdict is a plain bool — so a [`MockLeg`] can
/// drive the un-materialized path without any container.
#[derive(Debug)]
struct MockEvidence {
    materialized: bool,
}
impl MaterializationEvidence for MockEvidence {
    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::ProcessPause
    }
    fn materialized(&self) -> bool {
        self.materialized
    }
    fn diagnosis(&self) -> String {
        "mock".to_string()
    }
}

/// A programmable [`NemesisLeg`] with no I/O: it records how many times `apply`/`heal` ran, and
/// its `materialized` / `apply_ok` / `heal_complete` knobs let a test steer `drive_leg` down each
/// path. `applied_rules()` is always one rule, so `heal_is_complete` fails iff `heal_complete` is
/// false (heal returns nothing / the target never recovers).
struct MockLeg {
    materialized: bool,
    apply_ok: bool,
    heal_complete: bool,
    apply_count: Cell<usize>,
    heal_count: Cell<usize>,
}

impl MockLeg {
    fn new(materialized: bool, apply_ok: bool, heal_complete: bool) -> Self {
        Self {
            materialized,
            apply_ok,
            heal_complete,
            apply_count: Cell::new(0),
            heal_count: Cell::new(0),
        }
    }
}

impl NemesisLeg for MockLeg {
    type Evidence = MockEvidence;

    fn kind(&self) -> NemesisLegKind {
        NemesisLegKind::ProcessPause
    }
    fn plan(&self) -> Result<(), String> {
        Ok(())
    }
    fn apply(&self) -> Result<(), String> {
        self.apply_count.set(self.apply_count.get() + 1);
        if self.apply_ok {
            Ok(())
        } else {
            Err("mock apply failed".into())
        }
    }
    fn confirm_materialized(&self) -> Result<MockEvidence, String> {
        Ok(MockEvidence {
            materialized: self.materialized,
        })
    }
    fn heal(&self) -> Result<Vec<String>, String> {
        self.heal_count.set(self.heal_count.get() + 1);
        // A complete heal removes the one applied rule; an incomplete one removes nothing.
        Ok(if self.heal_complete {
            self.applied_rules()
        } else {
            Vec::new()
        })
    }
    fn applied_rules(&self) -> Vec<String> {
        vec!["mock-rule".to_string()]
    }
    fn confirm_healed(&self, _timeout: Duration) -> bool {
        self.heal_complete
    }
}

#[test]
fn drive_leg_runs_the_workload_and_returns_it_on_the_happy_path() {
    let leg = MockLeg::new(true, true, true);
    let ran = Cell::new(false);
    let result = drive_leg(&leg, || {
        ran.set(true);
        42
    });
    assert_eq!(
        result,
        Ok(42),
        "a materialized, cleanly-healed leg must return the workload's value"
    );
    assert!(
        ran.get(),
        "the workload must run under a materialized fault"
    );
    assert!(leg.heal_count.get() >= 1, "the fault must be healed");
}

#[test]
fn drive_leg_refuses_the_workload_when_the_fault_did_not_materialize() {
    // This is the #442 rule under test: an un-materialized fault is INCONCLUSIVE, never a silent
    // pass. Deleting the materialized-bail in `drive_leg` makes this test run the workload and
    // return Ok — red.
    let leg = MockLeg::new(false, true, true);
    let ran = Cell::new(false);
    let result = drive_leg(&leg, || {
        ran.set(true);
    });
    assert!(
        result.is_err(),
        "an un-materialized fault must FAIL as inconclusive, not pass: {result:?}"
    );
    assert!(
        result.unwrap_err().contains("did NOT materialize"),
        "the failure must name the un-materialized fault"
    );
    assert!(
        !ran.get(),
        "the workload must NOT run under a fault that did not bite"
    );
    assert!(
        leg.heal_count.get() >= 1,
        "even an un-materialized leg must heal (never leak fault state)"
    );
}

#[test]
fn drive_leg_fails_an_incomplete_heal() {
    // Deleting the `heal_is_complete` check in `drive_leg` makes this return Ok — red.
    let leg = MockLeg::new(true, true, false);
    let ran = Cell::new(false);
    let result = drive_leg(&leg, || {
        ran.set(true);
    });
    assert!(
        result.is_err(),
        "a leg that did not heal completely must FAIL: {result:?}"
    );
    assert!(
        result.unwrap_err().contains("did NOT heal completely"),
        "the failure must name the incomplete heal"
    );
    assert!(ran.get(), "the workload ran; only the heal was incomplete");
}

#[test]
fn drive_leg_heals_when_apply_fails_and_never_runs_the_workload() {
    // A partially-applied fault (e.g. one of four iptables rules landed) must be healed, and the
    // workload must not run. Deleting the heal-on-apply-failure makes heal_count == 0 — red.
    let leg = MockLeg::new(true, false, true);
    let ran = Cell::new(false);
    let result = drive_leg(&leg, || {
        ran.set(true);
    });
    assert!(
        result.is_err(),
        "an apply failure must fail the leg: {result:?}"
    );
    assert!(!ran.get(), "the workload must not run when apply failed");
    assert!(
        leg.heal_count.get() >= 1,
        "a failed apply must still heal any partial fault state"
    );
}

#[test]
fn drive_leg_surfaces_a_leaked_heal_on_the_un_materialized_early_path() {
    // The iteration-3 defect: the early exit paths (apply-failed / confirm-failed / un-materialized)
    // did `let _ = leg.heal()`, DROPPING a heal that itself failed/partial — a leaked cut/pause/skew
    // the primary error never named. This leg does not materialize AND heals into nothing (never
    // recovers), so `heal_is_complete` is false: `drive_leg` must fail with BOTH the inconclusive
    // verdict AND the leaked-fault reason. Reverting `heal_and_report` back to `let _ = leg.heal()`
    // on the early paths drops the leak clause — red.
    let leg = MockLeg::new(false, true, false);
    let ran = Cell::new(false);
    let err = drive_leg(&leg, || {
        ran.set(true);
    })
    .expect_err("an un-materialized leg with a leaked heal must FAIL");
    assert!(
        !ran.get(),
        "the workload must not run under a fault that did not bite"
    );
    assert!(
        err.contains("did NOT materialize"),
        "the failure must still name the inconclusive verdict: {err}"
    );
    assert!(
        err.contains("did NOT heal completely") || err.contains("heal leaked fault state"),
        "an early-path heal that leaked fault state must NOT be silently dropped: {err}"
    );
    assert!(
        leg.heal_count.get() >= 1,
        "the leg must at least attempt to heal"
    );
}

#[test]
fn drive_leg_surfaces_a_leaked_heal_when_apply_fails() {
    // Same defect class on the apply-failed early path: apply lands a partial fault, `heal`'s
    // `iptables -D` fails, and the leak must be reported alongside the apply-failure reason — not
    // dropped by `let _ = leg.heal()`.
    let leg = MockLeg::new(true, false, false);
    let err = drive_leg(&leg, || {})
        .expect_err("an apply failure whose heal also leaked must FAIL naming both");
    assert!(
        err.contains("apply failed"),
        "the failure must name the apply failure: {err}"
    );
    assert!(
        err.contains("did NOT heal completely") || err.contains("heal leaked fault state"),
        "the leaked heal on the apply-failed path must be surfaced: {err}"
    );
}

#[test]
fn drive_leg_heals_before_re_raising_a_panicking_workload() {
    // The checked #408 workload panics by design on a consistency violation. The fault must be
    // torn down BEFORE the panic propagates, or a red leaves a cut/paused/skewed cluster behind.
    // Deleting the `catch_unwind` makes the panic skip the heal (heal_count == 0) — red.
    let leg = MockLeg::new(true, true, true);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drive_leg(&leg, || panic!("workload blew up under the fault"))
    }));
    assert!(
        outcome.is_err(),
        "a panicking workload must re-raise the panic out of drive_leg"
    );
    assert!(
        leg.heal_count.get() >= 1,
        "the fault must be healed even when the workload panics"
    );

    // ...and when the workload panics, the ORIGINAL panic (not a leaked-fault escalation) must
    // propagate, because the heal here completed cleanly.
    let leg2 = MockLeg::new(true, true, true);
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drive_leg(&leg2, || panic!("workload blew up under the fault"))
    }))
    .expect_err("the panic must propagate");
    let msg = panic_message(payload.as_ref());
    assert!(
        msg.contains("workload blew up under the fault"),
        "a clean heal must re-raise the workload's OWN panic, not a leaked-fault escalation: {msg:?}"
    );
}

#[test]
fn drive_leg_surfaces_a_leaked_fault_even_when_the_workload_panics() {
    // The prior iteration's defect: `drive_leg` ran `resume_unwind` BEFORE checking the heal, so a
    // heal that FAILED while the workload also panicked was dropped silently — a leaked cut/pause/
    // skew hidden behind the panic. A leaked fault is the graver failure; it must be surfaced, not
    // swallowed. `MockLeg::new(_, _, heal_complete=false)` heals into nothing and never recovers,
    // so `heal_is_complete` is false — a leak. With the workload ALSO panicking, `drive_leg` must
    // still panic (the failure is not hidden), and the panic must NAME the leaked fault rather than
    // re-raise the workload's own message.
    let leg = MockLeg::new(true, true, false);
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drive_leg(&leg, || panic!("workload blew up under the fault"))
    }))
    .expect_err("a leaked fault under a panicking workload must still surface as a panic");
    assert!(
        leg.heal_count.get() >= 1,
        "the leg must at least attempt to heal even on the panic path"
    );
    let msg = panic_message(payload.as_ref());
    assert!(
        msg.contains("leaked fault state"),
        "the panic must name the LEAKED FAULT, not silently re-raise the workload panic: {msg:?}"
    );
}

/// Best-effort human-readable text of a caught panic payload (`&str` / `String`), for asserting
/// which panic propagated out of [`drive_leg`].
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
