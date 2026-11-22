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
use crate::reaper::{self, Reaped, ReaperContext};
use crate::rebalance::{self, RebalanceContext};
use crate::reconstruction::{self, ReconstructionContext};
use crate::retirement;
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
    /// least one committed object's chunk map could not be read
    /// (`crate::gc::ReferenceSet::unresolvable`) — or one staged multipart record
    /// (`crate::gc::StagedSet::unresolvable`), or, for scrub, a staged chunk's part placement
    /// could not be used — so the set the loop reasoned over is incomplete.
    ///
    /// A third outcome rather than a flavour of the other two, because it is a different
    /// **claim**. `Satisfied` says every referenced fragment was checked and matched — over
    /// an incomplete set that is a clean bill for part of the store, wearing the name of one
    /// for all of it (the rubric's *Absent or unsupported entries*: never silent success,
    /// never a silent skip). `Changed` says the loop converged something, which a refusal
    /// did not.
    ///
    /// It is deliberately **not** an error: the fault is one object's, it is attributed on
    /// the durability seam, and every other object in the store is still protected, verified
    /// and reclaimed exactly as usual — the containment shape this repo already uses for a
    /// record it cannot trust (`ReconciliationStatus::PendingMalformed`,
    /// `crates/custodian/src/desired_state.rs`). An `Err` here would end the step for every
    /// healthy object instead, and [`reconcile_step`] would stop before the loops that
    /// follow it ever ran.
    Blocked,
    /// The loop read a **bounded window** of its input and stopped short of the end: nothing
    /// it read diverged, and the rest is unvisited. GC answers this when its walk of the
    /// `orphan:` ledger (`crate::gc::OrphanWindow`) did not reach the ledger's end this pass
    /// (PR #802 review), and when a reclaim intent it recorded lost to a mark that changed after
    /// the pass read it and nothing was reclaimed: the mark's new value is one the pass never
    /// read (#804). Likewise when its sweep of a mark with no fragment beneath it lost to a mark a
    /// fresh read then found still at the key, and nothing was reclaimed or swept (#800).
    ///
    /// Distinct from [`Reconciled::Satisfied`] because that outcome is a **certification** —
    /// "reality already matched the desired state" — and a caller that drives reconciliation
    /// until it is satisfied would stop on it. Over a partial walk that stop leaves eligible
    /// garbage unvisited in the windows the pass never read, so the pass must answer something
    /// a caller keeps driving on. Distinct from [`Reconciled::Blocked`] because nothing is
    /// wrong: the next pass resumes where this one stopped, and the whole ledger is read once
    /// per lap. Distinct from [`Reconciled::Changed`] because the loop converged nothing.
    Partial,
}

impl Reconciled {
    /// The outcome of a step whose loops reported `self` and `other` — the **least
    /// certified** of the two, so a step never claims more than its weakest loop did.
    ///
    /// `Blocked` outranks `Changed`, `Changed` outranks `Partial`, and `Partial` outranks
    /// `Satisfied`. A blocked loop beside a converging one is still a step that cannot
    /// certify the store: the enqueues and reclamations the other loop made are durable in
    /// the store either way, while the refusal is the only thing that tells the caller its
    /// picture has a hole in it. `Changed` and `Partial` both mean "drive again"; `Changed`
    /// wins the pair because it carries the evidence that something converged, and `Partial`
    /// exists only to deny a `Satisfied` that a partial walk could not honestly give.
    pub(crate) fn least_certified(self, other: Reconciled) -> Reconciled {
        match (self, other) {
            (Reconciled::Blocked, _) | (_, Reconciled::Blocked) => Reconciled::Blocked,
            (Reconciled::Changed, _) | (_, Reconciled::Changed) => Reconciled::Changed,
            (Reconciled::Partial, _) | (_, Reconciled::Partial) => Reconciled::Partial,
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
/// **reconstruction loop** ([`reconstruction::reconcile`], `0005:269-286`) and, after it, the
/// **retirement leg** ([`retirement`]: the teardown of multipart sessions that have left `Open`,
/// `0016:672-674`, then the `retire:` ledger's drain, `0016:668`), `reaper` runs the
/// **multipart reaper** ([`reaper`]: it fences abandoned and over-age `Open` sessions to
/// `Aborting`, `0016:1796-2277`) before that retirement leg, so a session it fences is drained
/// in the same step — and with no `reconstruction` input it runs the retirement leg itself,
/// **record-only** (no fleet: nothing is retired by chunk id), the shape the operator one-shot
/// takes (`wyrd custodian --reap-abandoned-uploads`). When the reaper's budget-profile check
/// refuses — before its pass, or at the check it makes again as the pass's last read, just before
/// the retirement leg — the step runs no retirement leg at all and is [`Reconciled::Blocked`]. A
/// reconstruction error does not cost the reaper and the retirement leg their turn: they run, and
/// the step then returns that error.
/// `rebalance`
/// runs the **rebalance loop** — drain/decommission evacuation ([`rebalance::reconcile`],
/// `0005:297-303`) — and all `None` exercises the fence alone (no maintenance inputs
/// wired). When several are supplied the step runs each independent loop and reports the
/// **least certified** of their outcomes ([`Reconciled::least_certified`], which ranks
/// [`Reconciled::Partial`] between `Changed` and `Satisfied`):
/// [`Reconciled::Blocked`] if any loop refused to certify, else [`Reconciled::Changed`] if
/// any converged.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_step(
    zone: &FencedZone,
    custodian: &Custodian,
    gc: Option<&GcContext<'_>>,
    scrub: Option<&ScrubContext<'_>>,
    reconstruction: Option<&ReconstructionContext<'_>>,
    rebalance: Option<&RebalanceContext<'_>>,
    reaper: Option<&ReaperContext<'_>>,
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
        let repaired = reconstruction::reconcile(ctx, now_millis)
            .await
            .map_err(ReconcileError::Store);
        // The retirement leg runs on the reconstruction input, never GC's: the deployed loop runs
        // a reconstruction pass every interval whatever the fleet's state, while a GC pass waits
        // for the whole fleet, and a GC-only pass reads no `retire:` range
        // (`tests/gc_reclaim_intent.rs`, leg D). After reconstruction, so a store fault in the
        // drain never costs a pass its repairs. The reaper runs between the two: after
        // reconstruction for the same reason, and before the drain so a session it fences is
        // walked and drained in this step rather than the next.
        //
        // Both run whatever reconstruction answered, and its error is returned once they have:
        // they read neither the repair queue nor anything reconstruction writes, so a fault there
        // — one that recurs every interval — must not keep every abandoned upload holding its
        // admission slot, and every fenced one its staged bytes.
        let retired = async {
            if reap(reaper, &mut outcome).await? {
                outcome = outcome.least_certified(
                    retirement::reconcile(ctx)
                        .await
                        .map_err(ReconcileError::Store)?,
                );
            }
            Ok(())
        }
        .await;
        outcome = outcome.least_certified(reconstruction_first(repaired, retired)?);
    } else if let Some(reaper) = reaper {
        // No fleet: the reaper, then the retirement leg record-only, from the reaper's own store,
        // clock and tombstone input (the operator one-shot).
        if reap(Some(reaper), &mut outcome).await? {
            outcome = outcome.least_certified(
                retirement::reconcile_record_only(
                    reaper.meta,
                    reaper.clock,
                    reaper.policy.tombstone(),
                )
                .await
                .map_err(ReconcileError::Store)?,
            );
        }
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

/// What a step's reconstruction pass answered, joined with whether the reaper and the retirement
/// leg after it failed. Reconstruction's own error comes first — it is the error the step
/// returned before those legs ran on every answer — then theirs. When both failed, the second is
/// named on the audit seam rather than lost.
fn reconstruction_first(
    repaired: Result<Reconciled, ReconcileError>,
    retired: Result<(), ReconcileError>,
) -> Result<Reconciled, ReconcileError> {
    match (repaired, retired) {
        (Ok(repaired), Ok(())) => Ok(repaired),
        (Err(fault), Ok(())) | (Ok(_), Err(fault)) => Err(fault),
        (Err(fault), Err(also)) => {
            tracing::warn!(
                target: "wyrd.custodian.reconcile",
                error = %also,
                "the reaper or the retirement leg also failed this step; the reconstruction \
                 error is the one returned",
            );
            Err(fault)
        }
    }
}

/// Run the multipart reaper when the step carries one, joining what it answers into `outcome`,
/// and answer whether the retirement leg may run after it.
///
/// It may not when the reaper's profile check refused ([`Reaped::Refused`]): the stored budget
/// profile is not the one this custodian is sized for — or it is unknown: the ledger does not
/// decode, or is absent while sessions exist — and the leg's walks build the staged references
/// that profile sizes, so the leg writes nothing that step, which is
/// [`Reconciled::Blocked`]. The reaper makes that check again as its pass's last read, so the leg
/// is never authorized by a ledger read before the reaper's pass ran — a first Create that installs
/// another profile while the pass runs refuses the step as a stored mismatch does (the reaper's
/// module doc, step 5, states what that last read cannot see).
/// The loops that read no multipart record — reconstruction before it, rebalance after — run as
/// they would. A session the reaper only *held* is one session's, not the store's: the leg still
/// runs, and drains every session the reaper never had to judge (a foreign-clocked one's teardown
/// included, `0016:1891-1909`). With no reaper the leg runs as it always has.
async fn reap(
    reaper: Option<&ReaperContext<'_>>,
    outcome: &mut Reconciled,
) -> Result<bool, ReconcileError> {
    let Some(reaper) = reaper else {
        return Ok(true);
    };
    match reaper::reconcile(reaper)
        .await
        .map_err(ReconcileError::Store)?
    {
        Reaped::Ran(reaped) => {
            *outcome = outcome.least_certified(reaped);
            Ok(true)
        }
        Reaped::Refused => {
            *outcome = outcome.least_certified(Reconciled::Blocked);
            Ok(false)
        }
    }
}
