//! The Wyrd **custodian** (L4): the single-active, fenced maintenance plane that
//! M3 stands up (proposal 0005 §"Single active custodian, fenced",
//! §"Failure-domain-aware placement", §"The durability plane"; PR-sequence slice 3,
//! `0005:518-523`).
//!
//! M3.3 is the **skeleton** (`0005:519-523`): single-active leadership (fenced) over
//! the existing [`Coordination::elect_leader`] seam, the reconciliation control-loop
//! scaffold, the failure-domain-aware selector (which lives in `core` and is **shared
//! by the write fan-out**, re-exported here for custodian re-placement), and the
//! backend-agnostic OpenTelemetry seam wired over Prometheus + OTLP. The four loops'
//! *behaviour* — GC, scrub, reconstruction, rebalance — is later slices (4–7), out
//! of scope here.
//!
//! **Dependency rule (ADR-0010, `0005:421-422`):** this crate depends only on the
//! `traits` / `core` / `proto` seams (plus `tracing`/OpenTelemetry for the
//! durability plane) — **never** a concrete backend. The composing binary wires the
//! concretes.
//!
//! [`Coordination::elect_leader`]: wyrd_traits::Coordination::elect_leader

#![forbid(unsafe_code)]

pub mod desired_state;
pub mod gc;
pub mod leadership;
pub mod rebalance;
pub mod reconciliation;
pub mod reconstruction;
pub mod scrub;
pub mod telemetry;

pub use desired_state::{
    clear_lifecycle, draining_servers, reconciliation_status, set_lifecycle, DServerLifecycle,
    ReconciliationStatus,
};
pub use gc::{mark_orphaned, GcContext};
pub use leadership::{Custodian, FenceError, FencedZone};
pub use rebalance::RebalanceContext;
pub use reconciliation::{reconcile_step, ReconcileError, Reconciled};
pub use reconstruction::{repair_priority, ReconstructionContext};
pub use scrub::ScrubContext;
pub use telemetry::{DurabilityTelemetry, ExporterConfig, TelemetryError};

/// The failure-domain-aware **selector** the custodian places against — it lives in
/// `core` so it is the *same* selector the write fan-out uses (`0005:241-242`), and
/// is re-exported here for custodian re-placement. The distinctness invariant is the
/// custodian's to preserve on repair, exactly as the write preserves it on commit.
pub use wyrd_core::placement::{
    select_distinct_domains, select_distinct_domains_excluding, DServerTopology, FailureDomain,
    SelectorError, Topology,
};
