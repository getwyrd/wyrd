//! M3.3 (issue #141, proposal 0005 slice 3, `0005:518-523`): the `custodian` crate
//! skeleton.
//!
//! The three BINDING legs of the slice's success criterion, proven in-process:
//!
//! 1. **Single active, fenced** (`0005:358-383`): a custodian is **elected** via
//!    `Coordination::elect_leader` and **fenced** — a deposed leader's coordination
//!    action is **rejected** by its stale fencing token.
//! 2. **Failure-domain-aware selector** (`0005:235-245`): it places a chunk's `n`
//!    fragments across `n` **distinct** domains where the topology offers ≥ `n`, and
//!    **refuses** (errors) when domains < `n`.
//! 3. **Durability-plane seam** (`0005:319-344`, ADR-0012): the OTel seam emits a
//!    **first custodian metric** via `tracing` + `tracing-opentelemetry`, exposing
//!    **both** a Prometheus registry **and** OTLP push, with no backend hardcoded —
//!    asserted in-process by reading the metric back off the Prometheus surface.

#![forbid(unsafe_code)]

use std::collections::HashSet;

use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_custodian::{
    reconcile_step, select_distinct_domains, Custodian, DurabilityTelemetry, ExporterConfig,
    FencedZone, Reconciled, SelectorError, Topology,
};

/// Leg 1 — single active custodian, fenced: the elected leader acts; a superseded
/// leader's coordination action is rejected by the monotonic fencing token.
/// Install a permissive global `tracing` default **once** so the durability metric
/// callsites never latch `Interest::never` under the parallel test harness. `tracing`
/// caches each callsite's interest in a process-global table the first time it is hit;
/// a first hit racing a no-subscriber default can latch the callsite disabled, after
/// which the test that reads the metric back (`gather_prometheus`) silently sees it
/// missing (the flaky read-back the C4 gate caught, iteration-4). Registering against an
/// always-enabling default before any callsite fires makes every first-registration
/// agree; each test's own `.with_subscriber(...)` still routes its metrics into that
/// test's provider. Called at the top of every metric-touching test so whichever runs
/// first sets the default before any callsite fires (mirrors `scrub.rs:208`).
fn enable_metric_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

#[tokio::test]
async fn elected_leader_is_fenced_and_deposed_leader_rejected() {
    enable_metric_callsites();
    let coord = MemCoordination::new();
    let zone_key = "zone-alpha";

    // First election: this custodian is the single active leader.
    let leader = Custodian::elect(&coord, zone_key).await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());

    // The active leader's reconciliation step is admitted (the bare fence path: no
    // maintenance inputs wired, so the control point reports a satisfied zone).
    assert_eq!(
        reconcile_step(&zone, &leader, None, None, None, None, 0)
            .await
            .unwrap(),
        Reconciled::Satisfied,
        "the active leader acts"
    );

    // A new election supersedes the first term — the fencing token rises.
    let usurper = Custodian::elect(&coord, zone_key).await.unwrap();
    assert!(
        usurper.term() > leader.term(),
        "a later leadership term carries a strictly greater fencing token"
    );
    zone.install(usurper.leadership());

    // The new leader acts; the DEPOSED leader is now fenced out.
    assert_eq!(
        reconcile_step(&zone, &usurper, None, None, None, None, 0)
            .await
            .unwrap(),
        Reconciled::Satisfied
    );
    let rejected = reconcile_step(&zone, &leader, None, None, None, None, 0).await;
    assert!(
        rejected.is_err(),
        "a deposed leader's coordination action must be rejected by its stale fencing token"
    );
}

/// Leg 2 — the failure-domain-aware selector: `n` distinct domains where the
/// topology allows, and a refusal when it does not.
#[test]
fn selector_places_distinct_domains_and_refuses_when_too_few() {
    // Twelve servers; servers 0 and 1 share domain "A", the rest are singletons, so
    // the identity placement `0..9` would collide in "A" while the topology still
    // offers ≥ 9 distinct domains to spread across.
    let mut topo = Topology::default();
    topo.register(0, "A").register(1, "A");
    for (id, label) in (2u64..).zip(["B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]) {
        topo.register(id, label);
    }

    let placement = select_distinct_domains(&topo, 9).unwrap();
    assert_eq!(placement.len(), 9);
    // The chosen ids must be 9 distinct servers (a precondition of distinct domains).
    let unique: HashSet<_> = placement.iter().copied().collect();
    assert_eq!(unique.len(), 9, "nine distinct D servers");
    // BINDING: and they span nine distinct domains.
    let domains: HashSet<_> = placement
        .iter()
        .map(|id| {
            if *id <= 1 {
                "shared-A".to_string()
            } else {
                format!("singleton-{id}")
            }
        })
        .collect();
    assert_eq!(
        domains.len(),
        9,
        "n fragments across n distinct failure domains"
    );

    // Refusal: a topology with only three distinct domains cannot host a 9-wide
    // distinct-domain placement.
    let mut narrow = Topology::default();
    narrow.register(0, "A").register(1, "B").register(2, "C");
    assert_eq!(
        select_distinct_domains(&narrow, 9).unwrap_err(),
        SelectorError::InsufficientDomains { have: 3, need: 9 },
        "the selector refuses when domains < n"
    );
}

/// Leg 3 — the durability-plane OTel seam: a first custodian metric emitted via
/// `tracing` + `tracing-opentelemetry`, dual-exported (Prometheus + OTLP, no backend
/// hardcoded) and read back in-process off the Prometheus surface.
#[tokio::test]
async fn exporter_emits_first_metric_over_dual_surface() {
    enable_metric_callsites();
    // The dual-export surface is BINDING (ADR-0012): both a Prometheus registry AND an
    // OTLP push exporter are wired, with no backend hardcoded. Constructing `Both`
    // proves the OTLP push surface is genuinely built (not a stub).
    let dual = DurabilityTelemetry::new(ExporterConfig::Both {
        otlp_endpoint: "http://127.0.0.1:4317".to_string(),
    })
    .expect("both Prometheus and OTLP export surfaces wire up");
    assert!(
        dual.gather_prometheus().is_some(),
        "the Prometheus surface is present alongside OTLP push"
    );

    // Emit the first custodian metric through `tracing` bridged to OpenTelemetry, then
    // read it back off the Prometheus surface in-process (the ILLUSTRATIVE assertion).
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    tracing::subscriber::with_default(subscriber, || {
        // The `monotonic_counter.` prefix makes `tracing-opentelemetry` route this
        // event to an OTel counter named `custodian_active`.
        tracing::info!(
            monotonic_counter.custodian_active = 1_u64,
            zone = "zone-alpha"
        );
    });
    telemetry.flush().unwrap();

    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert!(
        exposed.contains("custodian_active"),
        "the first custodian metric is exported on the Prometheus surface; got:\n{exposed}"
    );
}
