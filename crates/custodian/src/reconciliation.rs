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
/// wired). When several are supplied the step runs each independent loop and reports
/// [`Reconciled::Changed`] if **any** converged.
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
        if gc::reconcile(ctx, now_millis)
            .await
            .map_err(ReconcileError::Store)?
            == Reconciled::Changed
        {
            outcome = Reconciled::Changed;
        }
    }
    if let Some(ctx) = scrub {
        if scrub::reconcile(ctx, now_millis)
            .await
            .map_err(ReconcileError::Store)?
            == Reconciled::Changed
        {
            outcome = Reconciled::Changed;
        }
    }
    if let Some(ctx) = reconstruction {
        if reconstruction::reconcile(ctx, now_millis)
            .await
            .map_err(ReconcileError::Store)?
            == Reconciled::Changed
        {
            outcome = Reconciled::Changed;
        }
    }
    if let Some(ctx) = rebalance {
        if rebalance::reconcile(ctx, now_millis)
            .await
            .map_err(ReconcileError::Store)?
            == Reconciled::Changed
        {
            outcome = Reconciled::Changed;
        }
    }
    Ok(outcome)
}
