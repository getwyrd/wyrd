//! The **reconciliation control-loop scaffold** (proposal 0005 §"The four custodian
//! loops", `0005:255-260`; §"Declarative management hook", `0005:346-356`).
//!
//! All four maintenance loops (GC, scrub, reconstruction, rebalance) are continuous
//! reconciliation loops on the single active custodian: read authoritative state,
//! converge reality toward the recorded intent. M3.3 stands up the **scaffold** — the
//! shape of one reconciliation step, gated by the leadership fence — not the loops'
//! *behaviour* (GC/scrub/reconstruct/rebalance are 0005 slices 4–7, out of scope,
//! `0005:78-83`).

use crate::leadership::{Custodian, FenceError, FencedZone};

/// The observable outcome of a reconciliation step — "changed" vs "satisfied" are
/// distinct, observable moments (`0005:351-352`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconciled {
    /// Reality already matched the desired state; nothing was done.
    Satisfied,
    /// Reality diverged and the step converged it (a stand-in until the loops land).
    Changed,
}

/// One reconciliation step on the single active custodian. It is **fenced**: the
/// step is admitted only while `custodian` holds the zone's current leadership term,
/// so a superseded custodian's reconciliation is rejected (`0005:362-367`). The
/// per-loop convergence behaviour is deferred to slices 4–7; this scaffold proves
/// the fenced control point.
pub fn reconcile_step(zone: &FencedZone, custodian: &Custodian) -> Result<Reconciled, FenceError> {
    zone.authorize(custodian.term())?;
    // The loops' convergence behaviour lands in later slices; the skeleton reports a
    // satisfied zone so the control point is exercisable end-to-end.
    Ok(Reconciled::Satisfied)
}
