//! The **reconciliation control-loop scaffold** (proposal 0005 §"The four custodian
//! loops", `0005:255-260`; §"Declarative management hook", `0005:346-356`).
//!
//! All four maintenance loops (GC, scrub, reconstruction, rebalance) are continuous
//! reconciliation loops on the single active custodian: read authoritative state,
//! converge reality toward the recorded intent. M3.3 stood up the **scaffold** — the
//! shape of one reconciliation step, gated by the leadership fence — and M3.4 hangs
//! the first running loop, **GC**, off it (`0005:524-527`). Scrub / reconstruction /
//! rebalance (slices 5–7) remain deferred (`0005:79-83`).

use crate::gc::{self, GcContext};
use crate::leadership::{Custodian, FenceError, FencedZone};
use crate::rebalance::{self, RebalanceContext};
use crate::reconstruction::{self, ReconstructionContext};
use crate::scrub::{self, ScrubContext};

/// The observable outcome of a reconciliation step — "changed" vs "satisfied" are
/// distinct, observable moments (`0005:351-352`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconciled {
    /// Reality already matched the desired state; nothing was done.
    Satisfied,
    /// Reality diverged and the step converged it (a stand-in until the loops land).
    Changed,
    /// The loop ran over everything it could read and **refuses to certify the rest**: at
    /// least one committed object's chunk map could not be resolved
    /// ([`crate::gc::ReferenceSet::unresolvable`]), so the reference set the loop reasons
    /// over is incomplete.
    ///
    /// A third outcome rather than a flavour of the other two, because it is a different
    /// **claim**. `Satisfied` says every referenced fragment was checked and matched — over
    /// an incomplete set that is a clean bill for part of the store, wearing the name of one
    /// for all of it, which is the *Absent or unsupported entries* failure (`AGENTS.md:175-177`:
    /// never silent success, never a silent skip). `Changed` says the loop converged
    /// something, which a refusal did not.
    ///
    /// It is deliberately **not** an error: the fault is one object's, it is attributed on the
    /// durability seam, and every other object in the store is scrubbed, verified and repaired
    /// exactly as usual — the repo's containment shape for a record it cannot trust
    /// (`ReconciliationStatus::PendingMalformed`, `crates/custodian/src/desired_state.rs:88-101`).
    /// An `Err` here would end the step for every healthy object instead, and
    /// [`reconcile_step`] would stop before the loops that follow.
    Blocked,
}

impl Reconciled {
    /// The outcome of a step whose loops reported `self` and `other` — the **least certified**
    /// of the two, so a step never claims more than its weakest loop.
    ///
    /// `Blocked` outranks `Changed`, and `Changed` outranks `Satisfied`. A blocked loop beside
    /// a converging one is still a step that cannot certify the store: the enqueues and
    /// reclamations the other loop made are durable in the store either way, while the
    /// refusal is the only thing that tells the caller its picture has a hole in it.
    fn least_certified(self, other: Reconciled) -> Reconciled {
        match (self, other) {
            (Reconciled::Blocked, _) | (_, Reconciled::Blocked) => Reconciled::Blocked,
            (Reconciled::Changed, _) | (_, Reconciled::Changed) => Reconciled::Changed,
            (Reconciled::Satisfied, Reconciled::Satisfied) => Reconciled::Satisfied,
        }
    }
}

/// A reconciliation step was refused or could not complete: either the actor was
/// **fenced** (a superseded leadership term) or a store access underneath a loop
/// failed.
#[derive(Debug)]
pub enum ReconcileError {
    /// The custodian's leadership term is stale — the step is rejected by the fence.
    Fenced(FenceError),
    /// A metadata- or chunk-store access underneath a loop failed.
    Store(wyrd_traits::BoxError),
}

impl std::fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReconcileError::Fenced(e) => write!(f, "{e}"),
            ReconcileError::Store(e) => write!(f, "reconciliation store access: {e}"),
        }
    }
}

impl std::error::Error for ReconcileError {}

/// One reconciliation step on the single active custodian — **the** fenced control
/// point every maintenance loop runs through (the anti-#141 guard: when a custodian
/// runtime eventually drives it, it runs *this* code, never a parallel test-only
/// entry). It is **fenced**: the step is admitted only while `custodian` holds the
/// zone's current leadership term, so a superseded custodian's reconciliation is
/// rejected (`0005:362-367`).
///
/// The supplied maintenance inputs select which loops the step dispatches: `gc`
/// runs the **GC loop** ([`gc::reconcile`], `0005:288-295`), `scrub` runs the
/// **scrub loop** ([`scrub::reconcile`], `0005:262-267`), `reconstruction` runs the
/// **reconstruction loop** ([`reconstruction::reconcile`], `0005:269-286`), `rebalance`
/// runs the **rebalance loop** — drain/decommission evacuation ([`rebalance::reconcile`],
/// `0005:297-303`) — and all `None` exercises the fence alone (no maintenance inputs
/// wired). When several are supplied the step runs each independent loop and reports the
/// **least certified** of their outcomes ([`Reconciled::least_certified`]): [`Reconciled::Blocked`]
/// if any loop refused to certify, else [`Reconciled::Changed`] if any converged.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_step(
    zone: &FencedZone,
    custodian: &Custodian,
    gc: Option<&GcContext<'_>>,
    scrub: Option<&ScrubContext<'_>>,
    reconstruction: Option<&ReconstructionContext<'_>>,
    rebalance: Option<&RebalanceContext<'_>>,
    now_millis: u64,
) -> Result<Reconciled, ReconcileError> {
    zone.authorize(custodian.term())
        .map_err(ReconcileError::Fenced)?;

    let mut outcome = Reconciled::Satisfied;
    if let Some(ctx) = gc {
        outcome = outcome.least_certified(
            gc::reconcile(ctx, now_millis)
                .await
                .map_err(ReconcileError::Store)?,
        );
    }
    if let Some(ctx) = scrub {
        outcome = outcome.least_certified(
            scrub::reconcile(ctx, now_millis)
                .await
                .map_err(ReconcileError::Store)?,
        );
    }
    if let Some(ctx) = reconstruction {
        outcome = outcome.least_certified(
            reconstruction::reconcile(ctx, now_millis)
                .await
                .map_err(ReconcileError::Store)?,
        );
    }
    if let Some(ctx) = rebalance {
        outcome = outcome.least_certified(
            rebalance::reconcile(ctx, now_millis)
                .await
                .map_err(ReconcileError::Store)?,
        );
    }
    Ok(outcome)
}
