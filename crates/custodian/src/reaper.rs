//! The **abandoned-upload reaper**, its `Open`-session half (proposal 0016 decision 6,
//! `0016:1796-2278`; #625, slice 625.1).
//!
//! A multipart session whose client went silent — never aborted, never completed — keeps its
//! staged parts protected on purpose (decision 2) and keeps its `mpuctl` admission slot, so
//! `MAX_SESSIONS` such sessions are a permanent `503 SlowDown`. 0016 makes the reaper a hard
//! ordering requirement for that reason (`0016:234-268`). This module **fences**: it moves an
//! abandoned `Open@E` session to `Aborting@E+1` with its `retire:bytes:` obligation, in the one
//! fence batch every writer of that edge commits ([`fence_batch`], #895). Everything after the
//! fence — the owned `sidx:` walk, the `retire:` drain, the `orphan:` marks and the terminal
//! delete with its one `mpuctl` decrement — is the retirement leg's ([`crate::retirement`],
//! #659), which runs after this pass in the same [`crate::reconcile_step`].
//!
//! # One pass (`0016:2167-2230`, the parts this slice owns, in that order)
//!
//! 1. **The profile check, first** ([`check_admission_profile`], `0016:2063-2072`). The stored
//!    `mpuctl` profile is compared with the custodian's own ([`ReaperPolicy::profile`]). A
//!    mismatch, or a ledger that will not decode, **refuses the pass** before it reads a session:
//!    an operator signal saying it refused before acting, no fence, no write, and a refusal
//!    (`Reaped::Refused`), on which the step runs no retirement leg either, so no multipart
//!    record is written that step. The fence is one batch that cannot be split, sized by the
//!    admitting profile (`MAX_INFLIGHT_PARTS` slot pins), and the drain builds the staged
//!    references that profile sizes, so a custodian sized for another profile acts on neither.
//!    **No ledger** means nothing was admitted when it was read (`0016:348`): no session existed
//!    then, so the pass judges none — a session listed after that read was created after it,
//!    under a profile this pass never checked, and the next pass judges it under its ledger. If
//!    the ledger is **still** absent at the pass's last read (step 5) and the listing holds a
//!    session — or could not be read — the step is refused as for a ledger that will not decode:
//!    every Create writes the ledger in the batch that installs its session, so the ledger is
//!    lost and the profile those sessions were admitted under is unknown. A refusal is the
//!    store's, not one session's, so no retirement leg runs that step either.
//! 2. `scan("mpu:")`, in pages, in id order, from where the last pass stopped
//!    ([`REAPER_CURSOR_KEY`]): at most [`SESSIONS_PER_PASS`] sessions a pass, and the next pass
//!    goes on after the last one judged, back to the head once a pass reaches the end. A stored
//!    position that is not an `mpu:` key a backend accepts is named and replaced by the head,
//!    never handed to the store as a key. A read of the listing the store cannot answer holds
//!    the rest of the listing and sends the next pass back to the head, never to the read that
//!    failed: a fault that lasts would otherwise be where every pass resumed, and no session
//!    listed before it would be judged again. That budget, not the ledger's `max_sessions`,
//!    bounds what one pass reads: a ledger an operator lowered below its live count
//!    (`0016:2082-2084`), or sessions created and deleted between two pages, list more sessions
//!    than `max_sessions`, and neither is damage. A pass whose budget runs out reads one entry
//!    further, judging none, so a listing that ends exactly at the budget is known to have
//!    ended. A listing longer than the budget is read in a **lap** of several passes, and the
//!    stored position carries whether the lap is still clean — nothing fenced, lost, held or
//!    left in doubt since its head — so the pass that ends a clean lap can answer
//!    [`Reconciled::Satisfied`] for all of it, and one that ends any other lap answers
//!    [`Reconciled::Partial`].
//! 3. Per session, **the clock guard first** (`0016:1957-1964`): a session whose `clock_source`
//!    is not the one this reaper reads ([`ReaperContext::clock_source`]) raises an operator
//!    signal and is **never judged** by `W_open` or `W_session`. The guard suppresses judgments,
//!    not the pass (`0016:1891-1909`, X71/X82): a foreign-clocked session already past `Open` is
//!    left to the retirement leg, which reads no session timestamp and walks, drains and deletes
//!    it as any other. A foreign-clocked **`Open`** session has no exit but an operator's
//!    (`0016:1986-2000`, #633), so it is **held** — named and counted, the pass
//!    [`Reconciled::Blocked`] — never reported as nothing to reap.
//! 4. Per owned `Open` session, the two arms (`0016:1910-1915`):
//!    * **(ii) the residency ceiling**: `now - created_at > W_session` fences, whatever the
//!      session's liveness, with **no** slot preconditions (`0016:1888-1890`) — the administrative
//!      bound, not a race;
//!    * **(i) idle abandonment**: the session's last progress instant
//!      `max(created_at, max psum committed_at, max slot reserved_at)` is older than `W_open`
//!      **and** no owned `slot:` or `sidx:` lease is unexpired. The ranges are read
//!      **source first** — `slot:` then `psum:` (normative, `0016:1810-1823`, X60) — so a part
//!      that commits between the two reads is seen in both or in `psum:` alone, never in neither.
//!      The fence then pins the slot evidence it judged (`0016:1859-1886`, X62):
//!      `require(slot:<id>:<k> == prior)` for every index seen present and `require_absent` for
//!      every index seen free, over the whole `[0, MAX_INFLIGHT_PARTS)` key space. A renewal or a
//!      release of a slot it read, or a reservation **still standing** when the fence lands,
//!      turns the fence into a `Conflict`, and the next pass judges again.
//!
//!    A fence of either arm that loses is classified from what the store then holds, by the one
//!    classifier the client Abort uses ([`lost`]): a session that moved, or a conflict no read
//!    explains (a pinned slot), is contention, judged again next pass; an obligation key already
//!    present beside the **unchanged** session is damage — no writer leaves one there — so the
//!    session is held and named, never reported as contention pass after pass.
//!
//!    A fence whose commit comes back with **an outcome the store could not report**
//!    ([`wyrd_traits::CommitUnknownResult`], [`ErrorClass::Indeterminate`]) is neither of those and
//!    is not a hold either: the batch may have landed — the retirement leg of this same step then
//!    tears the session down — or may yet land. So it is never named as a session left in place
//!    and never counted as fenced or lost: it has its own tally and its own operator signal
//!    (`reaper-fence-outcome-unknown`), the pass is [`Reconciled::Blocked`], and nothing re-sends
//!    the batch. The remedy the store's contract names is a re-read, and the next pass is one: it
//!    finds the session `Open` and judges it again, or past `Open` and leaves it to the leg. A
//!    commit that failed outright, with nothing in doubt, is a hold as before.
//!
//!    A stamp the pass would judge by — `created_at`, or the progress maximum — that is ahead of
//!    the reaper's clock by more than the clock-skew budget ([`DELTA_CLOCK_MILLIS`]) is held, not
//!    judged: it was written under a clock that disagrees with this one, and judging it would
//!    keep the session for as long as the stamp is ahead (`0016:1966-1975`, the #557 class).
//! 5. **The profile check, again**, as the pass's last read, so the retirement leg that runs
//!    after the pass is never authorized by a ledger read before the pass ran: a ledger installed
//!    or replaced while the pass ran, under another profile, refuses the step as the first check
//!    would have. So does a ledger that matched at the first read and is **gone** at the last: no
//!    writer deletes it, so it is lost, and the retirement leg must not run on the strength of a
//!    profile the store no longer holds. When the pass judged — its first read matched — a
//!    refusal at this read comes after it acted: what it fenced stays fenced, the signal says how
//!    many sessions that was instead of that it refused before acting, and the pass's tallies go
//!    out on the durability seam as any pass's do — as they also do when this read fails, which
//!    fails the step. A ledger installed under this profile while the pass ran — a first Create —
//!    leaves the pass [`Reconciled::Partial`], never `Satisfied`: the pass judged nothing under
//!    it, and the Create's session is the next pass's. What this read cannot see is a ledger
//!    installed after it: a first Create that lands between this read and the retirement leg's
//!    listing, its session then ended by its own client before the leg lists it, has that session
//!    torn down by the leg under a profile no check read. The window is narrower than one step
//!    and needs a client to create and end an upload inside it; what the leg does to that session
//!    is its ordinary teardown, in the leg's own bounded steps, and closing it would need the
//!    leg's batches to pin the ledger — #659's batches, which this slice does not change.
//!
//! The arms are **independent predicates**: a session can be idle long before it is over-age,
//! and the idle arm reaps it then (leg A of `crates/server/tests/multipart_reaper.rs`).
//!
//! A store fault reading one session's ranges, or the listing past some page, **holds** what it
//! could not read for the pass — counted, named, [`Reconciled::Blocked`] — and never fails the
//! step: the retirement leg after it reads no judgment of this pass, so a fault here must not cost
//! that leg its turn. Only a fault reading `mpuctl` itself fails the step, before any leg writes:
//! with the profile unknown, nothing may act on it.
//!
//! # Known gap: a completed slot cycle between the reads and the fence
//!
//! The idle fence does **not** see a request that reserves a slot index the pass read free,
//! stages, and commits its part, all after the reads and before the fence lands. The part commit
//! deletes its own slot (`wyrd_core::multipart::commit`, its `commit_batch`), so the index is
//! absent again and its `require_absent` holds; what the cycle leaves changed is the new part
//! number's `part:` and `psum:` keys, which the fence does not pin; and the session record, which
//! it does pin, a part commit only requires. The fence then lands, and an upload that has just
//! committed a part is aborted with it — the false positive decision 6's failure-mode table rules
//! out (`0016:2258`). `0016:1882-1883` counts a new reservation among what makes the fence
//! conflict; that holds only while the reservation stands.
//!
//! Point preconditions over the records 0016 defines cannot close it: the keys such a cycle can
//! leave changed are any part number's, up to `MAX_PART_NUMBER` of them, against
//! [`MAX_BATCH_OPS`] for the whole batch. Closing it needs a protocol change this slice does not
//! make — a witness every part commit rewrites, or the reaper occupying the free indices before
//! its last read — so it is stated here, and in the runtime view, rather than claimed covered.
//! It was accepted as a known gap of this slice; the follow-up that closes it must land before a
//! production path creates sessions (#668, #508). Its regression is the ignored DST row
//! `reaper_slot_cycle_between_the_reads_and_the_fence` (`crates/dst/tests/custodian.rs`).
//!
//! # The clock (ADR-0009)
//!
//! The abandonment judgment reads the **deployment wall clock** the gateway stamps
//! `created_at_millis`, `committed_at_millis` and `reserved_at_millis` from — `SystemTime::now()`,
//! which madsim virtualises — through [`ReaperContext::clock`], the same source the pass's
//! retirement leg stamps its marks from. One read per session for the judgment, taken **before**
//! that session's ranges are read: a stamp written after the read is then never older than `now`,
//! and a lease renewed after it is never read as lapsed, so a stale `now` errs toward keeping a
//! session. The stamp-ahead test of the progress maximum reads the same clock once more, after
//! the ranges, so a part that commits while they are read is never taken for a stamp from the
//! future.
//!
//! # Bounds (`0016:621-652`)
//!
//! A pass reads the ledger twice, the cursor once, the `mpu:` listing in pages of
//! `SESSION_PAGE` up to [`SESSIONS_PER_PASS`] sessions and at most one entry past them (to learn
//! whether the listing ended at the budget), and per owned `Open` session its `slot:`
//! range (at most `MAX_INFLIGHT_PARTS` records, by key space), its `psum:` range in pages (at most
//! `MAX_PARTS_PER_SESSION`), and, only when the session is otherwise idle, its owned `sidx:` range
//! in pages until the first live lease (at most `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`). A range
//! longer than its bound is damage: the session is held, never judged on a partial read. It
//! commits at most one fence per session — a fence that lost costs at most three more reads, to
//! classify it — and the cursor only when it moved. Every fence is measured against
//! [`MAX_BATCH_OPS`] and [`MAX_BATCH_BYTES`] before it is sent and is never sent past them; it
//! is one unsplittable batch (`0016:647-649`), and its largest shape fits both (the
//! `largest_fence_fits_both_batch_budgets` unit test). There is no batch splitter here: the
//! splittable work — marks and deletes — is the drain's (`0016:668`, `:672`).
//!
//! # Not here
//!
//! The `Completing` exits, the tombstone window and the `G_orphan`/`B_ops` calibration are
//! 625.3's; [`ReaperPolicy`] is where their windows go, and [`ReaperPolicy::tombstone`] is the
//! one input the retirement leg already reads from it. The deployment shapes beyond the fleet
//! loop and the one-shot are 625.2's.

use std::slice;

use wyrd_core::multipart::abort::{fence_batch, lost, Lost};
use wyrd_core::multipart::{
    decode_admission_record, decode_owned_entry, decode_part_summary, decode_session_record,
    decode_slot_record, parse_mpu_key, parse_psum_key, parse_slot_key, psum_range, sidx_range,
    slot_key, slot_range, AdmissionRecord, Budget, SessionRecord, SessionState, SlotIndex,
    UploadId, MAX_BATCH_BYTES, MAX_BATCH_OPS, MPUCTL_KEY, MPU_PREFIX,
};
use wyrd_testkit::Clock;
use wyrd_traits::{classify, CommitOutcome, ErrorClass, MetadataStore, Result, WriteBatch};

use crate::gc::{checked_page, object_name, DELTA_CLOCK_MILLIS};
use crate::reconciliation::Reconciled;
use crate::retirement::{Tombstone, LEDGER_KEY_MAX_LEN};

/// One day, in milliseconds.
const DAY_MILLIS: u64 = 24 * 60 * 60 * 1_000;

/// `W_open` — how long an `Open` session may go without progress, and with no live lease, before
/// the idle arm fences it (`0016:1910-1912`): **seven days**. Days-scale because the cost of a
/// false reap is a client's whole staged upload (`0016:2864`, FU-4), while a walked-away client's
/// slot is a capacity loss only; seven days is the common `AbortIncompleteMultipartUpload`
/// lifecycle setting. Bounded `≤ 7 days` (#625's scope), checked below.
pub const W_OPEN_MILLIS: u64 = 7 * DAY_MILLIS;

/// `W_session` — the administrative residency ceiling, measured from `created_at_millis`
/// (`0016:1913`, D-A): **thirty days**. Every `Open` session is fenced past it, live or not, so
/// it is the bound the drain bound (F6) and the residency promise (F14) are derived from.
/// Bounded `≥ 21 days` (#625's scope), checked below.
pub const W_SESSION_MILLIS: u64 = 30 * DAY_MILLIS;

const _: () = assert!(W_OPEN_MILLIS <= 7 * DAY_MILLIS);
const _: () = assert!(W_SESSION_MILLIS >= 21 * DAY_MILLIS);
// The idle arm is the earlier exit; a ceiling below it would make the idle arm dead code.
const _: () = assert!(W_OPEN_MILLIS < W_SESSION_MILLIS);

/// The most sessions one pass judges, every listed session counted whatever becomes of it — so a
/// listing of sessions that cannot be judged costs a pass exactly what a listing of judgeable
/// ones does, and never more. Above the shipped `MAX_SESSIONS`, so a store admitted under the
/// shipped profile is judged whole every pass and the cursor is never written; a longer listing
/// is judged across passes, every session once a lap. A listing of exactly this many is judged
/// whole too: when the budget runs out the pass reads one entry further, judging none, to learn
/// whether the listing ended there.
pub const SESSIONS_PER_PASS: usize = 64;

const _: () = assert!(SESSIONS_PER_PASS as u64 > wyrd_core::multipart::MAX_SESSIONS);

/// Where the reaper's listing resumes: the last `mpu:` key a pass judged when its
/// [`SESSIONS_PER_PASS`] ran out and more was listed after it — the exclusive cursor the next
/// pass reads after, under [`CLEAN_LAP_TAG`] while the lap is clean — or empty to start at the
/// head (as does an absent record). Outside `mpu:`, so the listing never reads it.
///
/// The deployed loop builds fresh contexts every pass and shared mutable globals are ruled out
/// (ADR-0035), so the position lives in the store, as the retirement walk's does
/// ([`crate::retirement::LEDGER_CURSOR_KEY`]). Whatever it holds, the listing only reads from
/// there, and no judgment, fence or drain reads it: a stale or damaged value costs at most a lap.
/// A pass writes it only when it moved — a listing judged whole from the head writes nothing.
pub const REAPER_CURSOR_KEY: &[u8] = b"custodian:reaper-cursor";

/// What a stored position is prefixed with while its lap is **clean**: every pass since the one
/// that started at the head fenced nothing, lost no fence, held nothing and left no fence's
/// outcome unknown. The pass that then reaches the end of the listing has, with those before it,
/// judged every listed session once and found nothing to do, so it may answer
/// [`Reconciled::Satisfied`]; without the tag — a lap in which something happened — the pass that
/// ends the lap answers [`Reconciled::Partial`], and the lap after it starts clean. So a listing
/// longer than one pass's budget still reaches `Satisfied` once a whole lap finds nothing to do,
/// and never while a session held, fenced, contended or left in doubt earlier in the lap is
/// unaccounted for.
///
/// A session created behind the walk during a lap is the next lap's to judge, as one created
/// behind a page already read is within a single pass.
///
/// It decides only that answer, and only this walk writes the record. A value carrying the tag
/// that no pass wrote could at worst make the pass ending that lap answer `Satisfied` once; it
/// fences, keeps and drains nothing, and the next lap starts at the head without it.
///
/// # What it does not bound
///
/// The tag carries no instant, so nothing bounds how long ago the lap it certifies began: the
/// pass that ends a clean lap answers `Satisfied` on judgments as old as the lap's first pass. A
/// session kept then and idle since is reported as nothing left until the next lap, which starts
/// at the head and fences it. Under the deployed loop a lap is a few intervals; under the
/// operator one-shot it is as long as the operator leaves between runs.
///
/// It needs a listing longer than [`SESSIONS_PER_PASS`] to be written at all, and no store
/// admitted under the shipped profile has one: that profile, the only one the profile check
/// accepts today ([`ReaperPolicy::profile`]), admits [`wyrd_core::multipart::MAX_SESSIONS`]
/// (46) sessions, so every listing is judged whole by one pass and the tag is never written.
/// Bounding a lap's age is deferred to the change that lets a deployment choose a larger
/// profile (deferred: #508).
const CLEAN_LAP_TAG: &[u8] = b"clean-lap:";

/// The most session records one page of the `mpu:` listing holds — the retirement leg's page,
/// 1.6 MB at the value ceiling.
const SESSION_PAGE: usize = 16;

/// The most `slot:`, `psum:` or `sidx:` records one page of a session's range holds. A summary is
/// tens of bytes, a slot record about a hundred and an owned entry a few hundred, so a page stays
/// far under a megabyte.
const RANGE_PAGE: usize = 256;

/// What the reaper judges against: its windows, the budget profile it was sized for, and the
/// tombstone input the retirement leg reads.
///
/// **Its shape is stable** — #633, 625.2 and 625.3 build one — and new windows arrive as new
/// fields: it is `#[non_exhaustive]`, so a caller outside this crate starts from
/// [`ReaperPolicy::default`] and sets what it changes. The server builds it from the default with
/// no window flags on the CLI, so a window added here needs no server change.
///
/// **The pass cadence** is the deployed loop's: one reaper pass per reconstruction pass, every
/// `--interval-secs` (30 s by default). Against days-scale windows that is far finer than any
/// judgment needs, and costs one `mpu:` listing (`≤ SESSIONS_PER_PASS` sessions) and a few
/// bounded ranges per `Open` session; a slower cadence would need a stored last-run instant,
/// which this slice does not add.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReaperPolicy {
    /// `W_open` ([`W_OPEN_MILLIS`]).
    pub w_open_millis: u64,
    /// `W_session` ([`W_SESSION_MILLIS`]).
    pub w_session_millis: u64,
    /// The budget profile this custodian is sized for — compared, whole, with the stored `mpuctl`
    /// profile before the pass acts and again after it ([`check_admission_profile`]). The shipped
    /// profile by default ([`Budget::shipped`]): no configuration builds another yet, so a store
    /// whose first Create installed a different profile is refused until the deployment that
    /// chose it gives the custodian the same one.
    pub profile: Budget,
    /// Whether a `Completed` session's tombstone window has passed — the input the retirement
    /// leg's terminal delete reads ([`Tombstone`]). [`Tombstone::Retain`] until `W_tombstone` is
    /// measured (625.3 flips it here).
    pub tombstone: Tombstone,
}

impl Default for ReaperPolicy {
    fn default() -> Self {
        Self {
            w_open_millis: W_OPEN_MILLIS,
            w_session_millis: W_SESSION_MILLIS,
            profile: Budget::shipped(),
            tombstone: Tombstone::Retain,
        }
    }
}

impl ReaperPolicy {
    /// The tombstone input the retirement leg reads for a `Completed` session — routed through
    /// the policy so the window that decides it lives beside the reaper's other windows.
    pub const fn tombstone(&self) -> Tombstone {
        self.tombstone
    }
}

/// What one reaper pass reads through ([`crate::reconcile_step`]'s `reaper` input).
pub struct ReaperContext<'a> {
    /// The authoritative metadata store.
    pub meta: &'a dyn MetadataStore,
    /// The clock the abandonment judgment reads (module doc, *The clock*). **One source with the
    /// pass's other legs**: the deployed loop hands the same `LoopClock` it puts in
    /// [`crate::ReconstructionContext::clock`], so the retirement leg's marks and this pass's
    /// judgment read one clock (ADR-0009).
    pub clock: &'a (dyn Clock + Sync),
    /// The `clock_source` spelling of [`Self::clock`] — a session stamped under any other is
    /// foreign and never judged ([`wyrd_core::multipart::WALL_CLOCK_SOURCE`] for the deployed
    /// custodian).
    pub clock_source: &'a str,
    /// The windows and the profile ([`ReaperPolicy`]).
    pub policy: &'a ReaperPolicy,
}

/// What [`check_admission_profile`] found in `mpuctl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionProfile {
    /// The stored profile equals the local one: the ledger as read.
    Matches(AdmissionRecord),
    /// No ledger: nothing was ever admitted on this store (`0016:348`, absent reads as
    /// `{ count: 0 }`), so there is no profile to disagree with — and no session admitted under
    /// one, as of the read.
    Absent,
    /// The stored profile is not the local one. The custodian is sized for a budget it is not
    /// getting, so it fails closed (`0016:2063-2072`).
    Differs {
        /// The profile `mpuctl` stores.
        stored: Budget,
    },
    /// `mpuctl` does not decode: the profile is unknown, so it is treated as differing.
    Unreadable {
        /// The decode fault.
        fault: String,
    },
}

/// **The one profile comparison** (`0016:348`, `:2063-2072`): read `mpuctl` and compare its
/// stored profile, whole, with `local`.
///
/// The reaper calls it before it reads any session and again after its last. #806's staged-set
/// preflight is the second caller to add, at the deferral in `crate::gc::staged_fragments_observing`
/// (the reference build that holds `W_ref` worth of chunk-refs in memory), so the two checks are
/// one comparison, never two that could drift.
///
/// What equal profiles do **not** prove is equal operation budgets: `B_ops`
/// ([`MAX_BATCH_OPS`]) is not part of the stored profile. Every batch this module sends is sized
/// from this build's own [`MAX_BATCH_OPS`] and [`MAX_BATCH_BYTES`] and measured against them
/// before it is sent.
pub async fn check_admission_profile(
    meta: &dyn MetadataStore,
    local: &Budget,
) -> Result<AdmissionProfile> {
    let Some(value) = meta.get(MPUCTL_KEY).await? else {
        return Ok(AdmissionProfile::Absent);
    };
    Ok(match decode_admission_record(&value) {
        Ok(ledger) if ledger.profile() == local => AdmissionProfile::Matches(ledger),
        Ok(ledger) => AdmissionProfile::Differs {
            stored: *ledger.profile(),
        },
        Err(fault) => AdmissionProfile::Unreadable {
            fault: fault.to_string(),
        },
    })
}

/// What one reaper pass came to, for [`crate::reconcile_step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reaped {
    /// The profile check refused (module doc, steps 1 and 5): the stored profile is not this
    /// custodian's, the ledger does not decode, it is absent beside a listed session or a
    /// listing that could not be read, or it matched at the pass's first read and is gone at its
    /// last. Refused at the first check, nothing past `mpuctl` was read
    /// and nothing was written; at the second, the pass's own fences and its listing position were
    /// committed under the profile the first check matched — its tallies are emitted, and the
    /// signal counts those fences — and under an absent ledger nothing was written at all. Either
    /// way the step reports [`Reconciled::Blocked`] and runs **no** retirement leg after it.
    Refused,
    /// The pass ran, and answered this. A session it held does not stop the retirement leg: a
    /// hold is one session's, the refusal above is the store's.
    Ran(Reconciled),
}

/// Which arm fenced a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arm {
    /// (i) idle abandonment, with the slot pins.
    Idle,
    /// (ii) the residency ceiling, without them.
    Ceiling,
}

impl Arm {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Ceiling => "residency-ceiling",
        }
    }
}

/// One pass's tallies.
#[derive(Debug, Default, PartialEq, Eq)]
struct Pass {
    /// Sessions fenced by each arm.
    fenced_idle: u64,
    fenced_ceiling: u64,
    /// Fences that lost a precondition: the evidence moved, judged again next pass.
    lost: u64,
    /// Fences whose commit came back with an outcome the store could not report: each may have
    /// landed, or may yet land. Not `fenced_*` — nothing says it landed — and not `held` — nothing
    /// says the session was left in place.
    unknown: u64,
    /// Sessions held: their records or ranges cannot be judged or read, their clock is not the
    /// reaper's while they are `Open`, or a fence's commit failed outright.
    held: u64,
    /// Sessions whose clock source this reaper does not own, in any state.
    foreign: u64,
    /// Owned `Open` sessions judged and left in place.
    kept: u64,
    /// Whether this pass ended a lap of the listing — head to end, read by this pass alone or by
    /// it and the passes before it — in which no **earlier** pass fenced, lost a fence, held or
    /// left a fence in doubt ([`CLEAN_LAP_TAG`]). Only then may it certify that nothing is left to
    /// judge; what this pass itself did is its other tallies.
    whole: bool,
}

impl Pass {
    /// The sessions this pass fenced, by either arm.
    const fn fenced(&self) -> u64 {
        self.fenced_idle + self.fenced_ceiling
    }

    /// [`Reconciled::Blocked`] when a session was held or a fence's outcome is unknown — either
    /// way the pass cannot say what the store holds for that session — else
    /// [`Reconciled::Changed`] when the pass fenced, else [`Reconciled::Partial`] when a fence lost
    /// to moved evidence or the pass did not end a clean lap of the listing (the next pass judges
    /// again, or goes on), else [`Reconciled::Satisfied`]: every listed session was judged once,
    /// head to end, and none was fenced, lost, held or left in doubt.
    fn outcome(&self) -> Reconciled {
        if self.held > 0 || self.unknown > 0 {
            Reconciled::Blocked
        } else if self.fenced() > 0 {
            Reconciled::Changed
        } else if self.lost > 0 || !self.whole {
            Reconciled::Partial
        } else {
            Reconciled::Satisfied
        }
    }
}

/// One reaper pass over `ctx.meta` (module doc), its outcome counts emitted on the durability
/// seam ([`pass`]). A store fault reading `mpuctl` is the one error: every later fault is a hold.
pub(crate) async fn reconcile(ctx: &ReaperContext<'_>) -> Result<Reaped> {
    Ok(match pass(ctx).await? {
        Some(pass) => Reaped::Ran(pass.outcome()),
        None => Reaped::Refused,
    })
}

/// The pass itself: its tallies, or `None` when a profile check refused it.
///
/// **The tallies go out ([`emit_pass`]) on every way out of a pass that judged**: when it
/// answers, when its last read of `mpuctl` refuses the step, and when that read fails. The fences
/// the walk committed stand whichever it is, and a fence no counter saw is an abort no metric
/// explains. A pass that found no ledger and was not refused emits them too. A pass refused
/// before it judged a session emits only the refusal: there is no pass to count.
async fn pass(ctx: &ReaperContext<'_>) -> Result<Option<Pass>> {
    let profile = &ctx.policy.profile;
    let mut pass = Pass::default();
    // Step 1.
    let first = check_admission_profile(ctx.meta, profile).await?;
    if let Some(detail) = refusal(&first, profile) {
        emit_profile_refused(None, &detail);
        return Ok(None);
    }
    if first == AdmissionProfile::Absent {
        // With no ledger the pass judges no session; it only looks whether one is listed.
        if !unadmitted(ctx, &mut pass).await? {
            return Ok(None);
        }
        emit_pass(&pass);
        return Ok(Some(pass));
    }
    walk(ctx, &mut pass).await;
    // Step 5: the ledger again, after the pass's last read. The tallies go out whatever that
    // read answered — the same profile, a refusal, or a store fault.
    let admitted = still_admitted(ctx, &pass).await;
    emit_pass(&pass);
    Ok(admitted?.then_some(pass))
}

/// Step 5 of a pass that judged under a matching ledger: whether the ledger still admits the
/// step. A refusal here comes after the walk, so its signal carries what the pass had fenced.
async fn still_admitted(ctx: &ReaperContext<'_>, pass: &Pass) -> Result<bool> {
    let profile = &ctx.policy.profile;
    let again = check_admission_profile(ctx.meta, profile).await?;
    let detail = match refusal(&again, profile) {
        Some(detail) => format!("at the pass's last read, {detail}"),
        // The first read matched and the pass judged under it. No writer deletes the ledger — a
        // Create and the terminal delete both put it — so one gone by the last read is lost, and
        // the profile the sessions still listed were admitted under is unknown again: refused, as
        // for a ledger absent from the start beside a session, so no retirement leg runs.
        None if again == AdmissionProfile::Absent => {
            "mpuctl matched at the start of the pass and is absent at its last read: no writer \
             deletes it, so the ledger is lost and the profile its sessions were admitted under \
             is unknown; nothing more is fenced or drained until it is restored"
                .to_owned()
        }
        None => return Ok(true),
    };
    emit_profile_refused(Some(pass), &detail);
    Ok(false)
}

/// A pass whose first read found **no ledger** (module doc, step 1): one look at the listing,
/// then the ledger again (step 5). `true` when the pass stands, `false` when it is refused —
/// before it judged or fenced anything, since under no ledger it judges no session.
async fn unadmitted(ctx: &ReaperContext<'_>, pass: &mut Pass) -> Result<bool> {
    let profile = &ctx.policy.profile;
    let probed = probe(ctx, None).await;
    let again = check_admission_profile(ctx.meta, profile).await?;
    if let Some(detail) = refusal(&again, profile) {
        emit_profile_refused(None, &format!("at the pass's last read, {detail}"));
        return Ok(false);
    }
    let detail = match (probed, again == AdmissionProfile::Absent) {
        // Nothing admitted, nothing listed, and still nothing admitted: the whole listing, read.
        (Probed::Empty, true) => {
            pass.whole = true;
            return Ok(true);
        }
        // A first Create installed the ledger while the pass ran, under this profile. Its session
        // was created after the first read and is the next pass's to judge, and this pass judged
        // nothing under that ledger, so it certifies nothing: `Partial`.
        (Probed::Empty | Probed::Listed(_), false) => return Ok(true),
        (Probed::Unreadable(fault), false) => {
            hold(pass, MPU_PREFIX, "listing-unreadable", &fault);
            return Ok(true);
        }
        // Still no ledger, beside a session or a listing that could not be read: the profile the
        // sessions were admitted under is unknown, which refuses the step as an undecodable
        // ledger does — every Create writes the ledger in the batch that installs its session.
        (Probed::Listed(key), true) => format!(
            "mpuctl is absent, yet mpu: lists {}: every Create writes mpuctl in the batch that \
             installs its session, so the ledger is lost and the profile its sessions were \
             admitted under is unknown; nothing is judged, fenced or drained until it is restored",
            object_name(&key)
        ),
        (Probed::Unreadable(fault), true) => format!(
            "mpuctl is absent and the mpu: listing could not be read ({fault}): whether a \
             session admitted under an unknown profile exists cannot be told, so nothing is \
             judged, fenced or drained"
        ),
    };
    emit_profile_refused(None, &detail);
    Ok(false)
}

/// Why `found` refuses the pass, for the operator signal — or `None` when it does not: the
/// stored profile matches, or there is no ledger (which [`unadmitted`] judges with the listing,
/// and [`still_admitted`] with what the pass's first read found).
fn refusal(found: &AdmissionProfile, profile: &Budget) -> Option<String> {
    match found {
        AdmissionProfile::Matches(_) | AdmissionProfile::Absent => None,
        AdmissionProfile::Differs { stored } => Some(format!(
            "the stored mpuctl profile {stored:?} differs from this custodian's {profile:?}"
        )),
        AdmissionProfile::Unreadable { fault } => {
            Some(format!("the stored mpuctl ledger does not decode: {fault}"))
        }
    }
}

/// What one read of the listing found ([`probe`]): from its head under an absent ledger, or from
/// where a pass's budget ran out.
enum Probed {
    /// Nothing: no session is listed there.
    Empty,
    /// The first session listed. Under an absent ledger it was created after the ledger was read
    /// — step 5 then finds the ledger its Create installed — or its ledger is lost; past a spent
    /// budget it is the next pass's to judge.
    Listed(Vec<u8>),
    /// The listing could not be read, so whether a session is listed there is unknown.
    Unreadable(String),
}

/// Whether the listing holds a session after `after` (from its head when `None`): one read of at
/// most one entry, which nothing judges. Step 1 asks it under an absent ledger, and [`walk`] when
/// its budget runs out on a full page.
async fn probe(ctx: &ReaperContext<'_>, after: Option<&[u8]>) -> Probed {
    match checked_page(ctx.meta, MPU_PREFIX, "reaper-sessions", after, 1).await {
        Ok((page, _)) => page
            .into_iter()
            .next()
            .map_or(Probed::Empty, |(key, _)| Probed::Listed(key)),
        Err(fault) => Probed::Unreadable(fault.to_string()),
    }
}

/// What [`REAPER_CURSOR_KEY`] holds, read as where the listing resumes ([`read_cursor`]).
#[derive(Debug, PartialEq, Eq)]
enum Resume<'a> {
    /// Absent or empty: the head, where a lap begins.
    Head,
    /// After `key`. `clean` is whether every earlier pass of the lap — from the head to `key` —
    /// fenced nothing, lost no fence, held nothing and left no fence in doubt ([`CLEAN_LAP_TAG`]).
    After { key: &'a [u8], clean: bool },
    /// Not a position ([`is_resumable_cursor`]): named, and read as the head.
    Unusable,
}

/// Read a stored cursor value.
fn read_cursor(stored: Option<&[u8]>) -> Resume<'_> {
    let Some(stored) = stored.filter(|stored| !stored.is_empty()) else {
        return Resume::Head;
    };
    let (key, clean) = match stored.strip_prefix(CLEAN_LAP_TAG) {
        Some(key) => (key, true),
        None => (stored, false),
    };
    if is_resumable_cursor(key) {
        Resume::After { key, clean }
    } else {
        Resume::Unusable
    }
}

/// The cursor value that resumes after `key` in a lap that is `clean` so far.
fn cursor_value(key: &[u8], clean: bool) -> Vec<u8> {
    if clean {
        [CLEAN_LAP_TAG, key].concat()
    } else {
        key.to_vec()
    }
}

/// Step 2: the listing, from [`REAPER_CURSOR_KEY`], at most [`SESSIONS_PER_PASS`] sessions, each
/// judged ([`visit`]); then the cursor, written only when it moved. Every fault in it is a hold.
///
/// **A read of the listing that fails sends the next pass back to the head** — a page, or the
/// one entry read past a spent budget. The rest of the listing is held and named, and no position
/// is kept at the fault: a pass that resumed there would read nothing, hold again and leave the
/// position where it was, so for as long as the fault lasted no session listed before it would be
/// judged again — one that went idle or over age behind it would keep its admission slot, the
/// *never reap* failure mode (`0016:2270`). From the head, every pass judges what the store does
/// return, a budget's worth a pass, and holds the fault each time it reaches it.
async fn walk(ctx: &ReaperContext<'_>, pass: &mut Pass) {
    let stored = match ctx.meta.get(REAPER_CURSOR_KEY).await {
        Ok(stored) => stored.map(|value| value.to_vec()),
        Err(fault) => {
            // Where to start is unknown: held, the listing judged by a later pass.
            hold(
                pass,
                REAPER_CURSOR_KEY,
                "cursor-unreadable",
                &fault.to_string(),
            );
            return;
        }
    };
    // Where this pass starts, and whether the lap it goes on with is clean so far. A pass that
    // starts at the head starts a lap, which is clean until something happens in it.
    let (start, lap_clean) = match read_cursor(stored.as_deref()) {
        Resume::Head => (None, true),
        Resume::After { key, clean } => (Some(key.to_vec()), clean),
        Resume::Unusable => {
            // Rewritten below: the head this pass starts from is not what is stored.
            emit_unusable_cursor(stored.as_deref().unwrap_or_default());
            (None, true)
        }
    };
    let mut position = start;
    let mut budget = SESSIONS_PER_PASS;
    let mut reached_end = false;
    let mut faulted = false;
    while budget > 0 {
        // Never more than the budget left, and `checked_page` refuses a longer page, so the
        // budget cannot run out inside a page.
        let limit = budget.min(SESSION_PAGE);
        let (page, next) = match checked_page(
            ctx.meta,
            MPU_PREFIX,
            "reaper-sessions",
            position.as_deref(),
            limit,
        )
        .await
        {
            Ok(page) => page,
            Err(fault) => {
                // The rest of the listing is unread: held, and the next pass starts at the head.
                let at = position.as_deref().unwrap_or(MPU_PREFIX);
                hold(pass, at, "listing-unreadable", &fault.to_string());
                faulted = true;
                break;
            }
        };
        for (key, value) in page {
            budget -= 1;
            if let Err(fault) = visit(ctx, pass, &key, &value).await {
                // One session's ranges unreadable: that session is held, the pass goes on.
                hold(pass, &key, "store-fault", &fault.to_string());
            }
            position = Some(key);
        }
        if next.is_none() {
            reached_end = true;
            break;
        }
    }
    // The budget ran out on a full page, and a full page says nothing of what follows it: a
    // listing that ends exactly there would otherwise be read as unfinished on every pass. One
    // more read, of at most one entry and judging none, tells the two apart. A fault reading it
    // is a listing fault as a page's is: whether more sessions are listed is unknown, so it is
    // held the same way and the next pass starts at the head. (A page fault above leaves budget
    // unspent, so the page that failed is not asked for again here.)
    if budget == 0 && !reached_end {
        match probe(ctx, position.as_deref()).await {
            Probed::Empty => reached_end = true,
            Probed::Listed(_) => {}
            Probed::Unreadable(fault) => {
                let at = position.as_deref().unwrap_or(MPU_PREFIX);
                hold(pass, at, "listing-unreadable", &fault);
                faulted = true;
            }
        }
    }
    // Whether the lap is still clean with this pass in it: what the next pass of the lap reads
    // back, so the pass that ends the lap can certify all of it.
    let clean =
        lap_clean && pass.held == 0 && pass.unknown == 0 && pass.lost == 0 && pass.fenced() == 0;
    // Where the next pass resumes — after the last session judged, or the head once the listing
    // reached the end or a page of it could not be read — put only when it moved. A blind put, as
    // the retirement walk's: whatever lands, the listing only reads from there.
    let resume = match position {
        Some(after) if !reached_end && !faulted => cursor_value(&after, clean),
        _ => Vec::new(),
    };
    let unchanged = match &stored {
        None => resume.is_empty(),
        Some(stored) => *stored == resume,
    };
    if !unchanged {
        if let Err(fault) = ctx
            .meta
            .commit(WriteBatch::new().put(REAPER_CURSOR_KEY, resume))
            .await
        {
            hold(
                pass,
                REAPER_CURSOR_KEY,
                "cursor-unwritable",
                &fault.to_string(),
            );
        }
    }
    // deferred: #508 — a lap's age is not bounded ([`CLEAN_LAP_TAG`], *What it does not bound*).
    pass.whole = reached_end && lap_clean;
}

/// Whether a stored cursor's key is a place the listing may resume from: a key under `mpu:` no
/// longer than the longest key a backend accepts ([`LEDGER_KEY_MAX_LEN`], FoundationDB's 10 KB).
/// It need not parse — a listed key that names no upload is held and can end a pass's budget, so
/// it is a position the cursor legitimately holds. Anything longer would be refused by the
/// backend as a key on every pass, before the cursor could move, and no session would be judged
/// again; so it is never handed to `scan_page` (the rule GC's `orphan:` cursor and the retirement
/// walk's resume point keep, `crate::gc::OrphanWindow::read`, `crate::retirement::walk_ledger`).
fn is_resumable_cursor(cursor: &[u8]) -> bool {
    cursor.starts_with(MPU_PREFIX) && cursor.len() <= LEDGER_KEY_MAX_LEN
}

/// Judge the session listed at `key` as `value` (module doc, steps 3 and 4). A store fault reading
/// its ranges is the caller's to hold.
async fn visit(ctx: &ReaperContext<'_>, pass: &mut Pass, key: &[u8], value: &[u8]) -> Result<()> {
    let upload = match parse_mpu_key(key) {
        Ok(upload) => upload,
        Err(fault) => {
            hold(pass, key, "key-names-no-upload", &fault.to_string());
            return Ok(());
        }
    };
    let record = match decode_session_record(value) {
        Ok(record) => record,
        Err(fault) => {
            hold(pass, key, "session-undecodable", &fault.to_string());
            return Ok(());
        }
    };
    let open = matches!(record.state(), SessionState::Open {});
    // The guard FIRST, from the session record (`0016:1957-1964`), before any of the session's
    // stamps or leases is read. It suppresses the judgments only: a foreign-clocked session past
    // `Open` is the retirement leg's, which reads no session timestamp (X71/X82).
    if record.clock_source() != ctx.clock_source {
        pass.foreign += 1;
        emit_foreign(key, record.clock_source(), ctx.clock_source);
        if open {
            // Neither window may judge it, so only an operator can end it (`0016:1986-2000`,
            // #633): held, so the pass never reports it as nothing to reap.
            hold(
                pass,
                key,
                "foreign-clock",
                &format!(
                    "an Open session stamped under clock_source {:?}, which this reaper does not \
                     read: neither window judges it, and only an operator abort ends it",
                    record.clock_source()
                ),
            );
        }
        return Ok(());
    }
    // deferred: #1017 — the `Completing` judgments (the `W_completing` rollback and the over-age
    // `Completing → Aborting` edge, `0016:2190-2199`). Every state past `Open` is the retirement
    // leg's from here: it walks, drains and deletes `Aborting` and `Completed` sessions itself.
    if !open {
        return Ok(());
    }
    let policy = ctx.policy;
    // One read of the reaper's clock per session, before its ranges (module doc, *The clock*).
    let now = ctx.clock.now_millis();
    if ahead(record.created_at_millis(), now) {
        stamp_ahead(
            pass,
            key,
            "created_at_millis",
            record.created_at_millis(),
            now,
        );
        return Ok(());
    }
    // Arm (ii): the administrative ceiling, regardless of liveness and with no slot pins
    // (`0016:1888-1890`). It reads no range.
    if now.saturating_sub(record.created_at_millis()) > policy.w_session_millis {
        return fence(ctx, pass, key, value, &upload, &record, Arm::Ceiling, &[]).await;
    }
    // Arm (i): idle abandonment.
    let slots = match read_slots(ctx, &upload).await? {
        Ok(slots) => slots,
        Err(detail) => {
            hold(pass, key, "slot-range-unjudgeable", &detail);
            return Ok(());
        }
    };
    let latest_psum = match latest_psum(ctx, &upload).await? {
        Ok(latest) => latest,
        Err(detail) => {
            hold(pass, key, "psum-range-unjudgeable", &detail);
            return Ok(());
        }
    };
    let progress = slots
        .iter()
        .map(|slot| slot.reserved_at_millis)
        .chain(latest_psum)
        .fold(record.created_at_millis(), u64::max);
    // Against a reading taken after the ranges: a part that committed while they were read is
    // stamped before it, never ahead of it but by the clocks' disagreement.
    let read_at = ctx.clock.now_millis();
    if ahead(progress, read_at) {
        stamp_ahead(pass, key, "the progress maximum", progress, read_at);
        return Ok(());
    }
    let slot_live = slots.iter().any(|slot| slot.lease_expiry_millis > now);
    if slot_live || now.saturating_sub(progress) <= policy.w_open_millis {
        pass.kept += 1;
        return Ok(());
    }
    // Idle by progress and by every slot lease; the owned `sidx:` leases are the last evidence.
    match owned_lease_live(ctx, &upload, now).await? {
        Ok(true) => {
            pass.kept += 1;
            Ok(())
        }
        Ok(false) => fence(ctx, pass, key, value, &upload, &record, Arm::Idle, &slots).await,
        Err(detail) => {
            hold(pass, key, "sidx-range-unjudgeable", &detail);
            Ok(())
        }
    }
}

/// Whether `stamp` is ahead of the reading `now` by more than the clock-skew budget every
/// time-dependent check shares ([`DELTA_CLOCK_MILLIS`]): a stamp no clock within that budget of
/// this one could have written by then.
const fn ahead(stamp: u64, now: u64) -> bool {
    stamp > now.saturating_add(DELTA_CLOCK_MILLIS)
}

/// Hold a session one of whose stamps is ahead of the reaper's clock ([`ahead`]).
fn stamp_ahead(pass: &mut Pass, key: &[u8], what: &str, stamp: u64, now: u64) {
    hold(
        pass,
        key,
        "stamp-ahead-of-clock",
        &format!(
            "{what} {stamp} is {} ms ahead of this reaper's clock ({now}), past the {} ms \
             clock-skew budget: the clock that wrote it is not this one, so the session is not \
             judged",
            stamp - now,
            DELTA_CLOCK_MILLIS
        ),
    );
}

/// One `slot:` record as the idle arm read it: its index, its exact bytes (the fence pins them),
/// and the two fields the judgment reads.
struct SlotSeen {
    index: u32,
    bytes: Vec<u8>,
    reserved_at_millis: u64,
    lease_expiry_millis: u64,
}

/// The session's `slot:` range, every record decoded — or why it cannot be judged. Read FIRST,
/// before `psum:` (`0016:1810-1823`). At most `MAX_INFLIGHT_PARTS` records exist by key space; a
/// record outside `[0, MAX_INFLIGHT_PARTS)`, or one that does not decode, leaves the range
/// unjudgeable, and the session is held rather than judged on part of it.
async fn read_slots(
    ctx: &ReaperContext<'_>,
    upload: &UploadId,
) -> Result<std::result::Result<Vec<SlotSeen>, String>> {
    let inflight = ctx.policy.profile.max_inflight_parts();
    let prefix = slot_range(upload);
    let mut slots = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = checked_page(
            ctx.meta,
            &prefix,
            "reaper-slots",
            after.as_deref(),
            RANGE_PAGE,
        )
        .await?;
        for (key, value) in &page {
            if slots.len() == inflight as usize {
                return Ok(Err(format!(
                    "more than {inflight} slot record(s) (MAX_INFLIGHT_PARTS)"
                )));
            }
            // The range is this session's by its prefix; the index is what the key space bounds.
            let index = match parse_slot_key(key) {
                Ok((_, index)) if index.get() < inflight => index.get(),
                Ok(_) => {
                    return Ok(Err(format!(
                        "{} is outside the session's slot key space",
                        object_name(key)
                    )))
                }
                Err(fault) => return Ok(Err(format!("{}: {fault}", object_name(key)))),
            };
            let slot = match decode_slot_record(value) {
                Ok(slot) => slot,
                Err(fault) => return Ok(Err(format!("{}: {fault}", object_name(key)))),
            };
            slots.push(SlotSeen {
                index,
                bytes: value.to_vec(),
                reserved_at_millis: slot.reserved_at_millis(),
                lease_expiry_millis: slot.lease_expiry_millis(),
            });
        }
        match (next, page.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(Ok(slots)),
        }
    }
}

/// The newest `committed_at_millis` of the session's `psum:` range (`None` when it is empty) —
/// or why it cannot be judged. Read SECOND, after `slot:`. Paged, never held whole; a range longer
/// than `MAX_PARTS_PER_SESSION` plus the in-flight overshoot, a key that does not name a part of
/// the session ([`parse_psum_key`]), or a summary that does not decode, leaves it unjudgeable: a
/// summary under a key that names no part is no part's progress.
async fn latest_psum(
    ctx: &ReaperContext<'_>,
    upload: &UploadId,
) -> Result<std::result::Result<Option<u64>, String>> {
    let profile = &ctx.policy.profile;
    let bound =
        u64::from(profile.max_parts_per_session()) + u64::from(profile.max_inflight_parts());
    let prefix = psum_range(upload);
    let mut latest: Option<u64> = None;
    let mut read: u64 = 0;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = checked_page(
            ctx.meta,
            &prefix,
            "reaper-psums",
            after.as_deref(),
            RANGE_PAGE,
        )
        .await?;
        for (key, value) in &page {
            read += 1;
            if read > bound {
                return Ok(Err(format!("more than {bound} part summaries")));
            }
            if let Err(fault) = parse_psum_key(key) {
                return Ok(Err(format!("{}: {fault}", object_name(key))));
            }
            match decode_part_summary(value) {
                Ok(summary) => {
                    latest = latest.max(Some(summary.committed_at_millis));
                }
                Err(fault) => return Ok(Err(format!("{}: {fault}", object_name(key)))),
            }
        }
        match (next, page.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(Ok(latest)),
        }
    }
}

/// Whether any owned `sidx:` entry of the session holds a lease unexpired at `now` — or why the
/// range cannot be judged. Stops at the first live lease. A range longer than
/// `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`, or an entry that does not decode under its key, leaves
/// it unjudgeable: an undecodable entry's lease is unknown, so it cannot be counted lapsed.
async fn owned_lease_live(
    ctx: &ReaperContext<'_>,
    upload: &UploadId,
    now: u64,
) -> Result<std::result::Result<bool, String>> {
    let profile = &ctx.policy.profile;
    let bound = u64::from(profile.max_inflight_parts()) * u64::from(profile.max_part_chunks());
    let prefix = sidx_range(upload);
    let mut read: u64 = 0;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = checked_page(
            ctx.meta,
            &prefix,
            "reaper-sidx",
            after.as_deref(),
            RANGE_PAGE,
        )
        .await?;
        for (key, value) in &page {
            read += 1;
            if read > bound {
                return Ok(Err(format!("more than {bound} owned entries")));
            }
            match decode_owned_entry(key, value) {
                Ok((_, _, entry)) if entry.lease_expiry_millis() > now => return Ok(Ok(true)),
                Ok(_) => {}
                Err(fault) => return Ok(Err(format!("{}: {fault}", object_name(key)))),
            }
        }
        match (next, page.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => return Ok(Ok(false)),
        }
    }
}

/// The idle arm's slot pins over the whole key space `[0, inflight)`: `require(slot == bytes
/// read)` for every index seen present, `require_absent` for every index seen free
/// (`0016:1859-1886`). A free index pinned absent does not see a reservation that has already
/// been released again by the time the fence lands (module doc, *Known gap*).
fn pin_slots(
    mut batch: WriteBatch,
    upload: &UploadId,
    inflight: u32,
    slots: &[SlotSeen],
) -> std::result::Result<WriteBatch, String> {
    for index in 0..inflight {
        let slot = SlotIndex::new(index).map_err(|fault| fault.to_string())?;
        let key = slot_key(upload, slot);
        batch = match slots.iter().find(|seen| seen.index == index) {
            Some(seen) => batch.require(key, seen.bytes.clone()),
            None => batch.require_absent(key),
        };
    }
    Ok(batch)
}

/// The operations and bytes `batch` spends against [`MAX_BATCH_OPS`] / [`MAX_BATCH_BYTES`]:
/// every precondition, put and delete is one operation, and every key and value it carries is
/// counted.
fn fence_cost(batch: &WriteBatch) -> (u64, u64) {
    let ops = batch.preconditions.len() + batch.puts.len() + batch.deletes.len();
    let bytes = batch
        .preconditions
        .iter()
        .map(|pre| pre.key.len() + pre.expected.as_ref().map_or(0, |value| value.len()))
        .chain(
            batch
                .puts
                .iter()
                .map(|(key, value)| key.len() + value.len()),
        )
        .chain(batch.deletes.iter().map(Vec::len))
        .sum::<usize>();
    (ops as u64, bytes as u64)
}

/// Whether a batch spending `ops` operations and `bytes` bytes is inside both budgets — each
/// limit itself included.
const fn within_budgets(ops: u64, bytes: u64) -> bool {
    ops <= MAX_BATCH_OPS as u64 && bytes <= MAX_BATCH_BYTES
}

/// Build the fence for `arm` and commit it once: the one teardown fence batch ([`fence_batch`],
/// #895) — `require(mpu:<id> == bytes read)`, `require_absent` on the obligation key, the session
/// put at `Aborting@E+1`, the `{session, all}` obligation put — and, for the idle arm, the slot
/// pins. A batch past [`MAX_BATCH_OPS`] or [`MAX_BATCH_BYTES`] is never sent.
#[allow(clippy::too_many_arguments)]
async fn fence(
    ctx: &ReaperContext<'_>,
    pass: &mut Pass,
    key: &[u8],
    read: &[u8],
    upload: &UploadId,
    record: &SessionRecord,
    arm: Arm,
    slots: &[SlotSeen],
) -> Result<()> {
    let Some(teardown) = record.open_teardown(upload) else {
        hold(
            pass,
            key,
            "epoch-exhausted",
            "the session is Open at the last epoch, so no fence can bump it",
        );
        return Ok(());
    };
    let batch = fence_batch(
        key,
        read,
        teardown.session(),
        slice::from_ref(teardown.obligation()),
    );
    let batch = match arm {
        Arm::Ceiling => batch,
        Arm::Idle => {
            match pin_slots(
                batch,
                upload,
                ctx.policy.profile.max_inflight_parts(),
                slots,
            ) {
                Ok(batch) => batch,
                Err(detail) => {
                    hold(pass, key, "slot-pin-unbuildable", &detail);
                    return Ok(());
                }
            }
        }
    };
    let (ops, bytes) = fence_cost(&batch);
    if !within_budgets(ops, bytes) {
        hold(
            pass,
            key,
            "fence-past-batch-budget",
            &format!(
                "the fence would carry {ops} operation(s) and {bytes} byte(s), past \
                 {MAX_BATCH_OPS} / {MAX_BATCH_BYTES}; it is one batch that cannot be split, so it \
                 is never sent"
            ),
        );
        return Ok(());
    }
    match ctx.meta.commit(batch).await {
        Ok(CommitOutcome::Committed) => {
            match arm {
                Arm::Idle => pass.fenced_idle += 1,
                Arm::Ceiling => pass.fenced_ceiling += 1,
            }
            emit_fenced(key, arm, teardown.session().epoch());
        }
        Ok(CommitOutcome::Conflict) => {
            // Classified from what the store holds, by the client Abort's own classifier
            // (module doc, step 4); a read fault here is the caller's to hold.
            let obligation_key = teardown.obligation().key();
            match lost(ctx.meta, key, read, &obligation_key).await? {
                // The session or a pinned slot moved after the reads: a renewal, a release, a
                // standing reservation, a client verb. The next pass re-derives the judgment from
                // what the store then holds (`0016:1882-1884`).
                Lost::Moved(_) | Lost::Unexplained => {
                    pass.lost += 1;
                    emit_lost(key, arm);
                }
                // The obligation key is taken beside the unchanged session: no writer leaves
                // that, and every later fence would lose the same way.
                Lost::Taken => hold(
                    pass,
                    key,
                    "obligation-key-taken",
                    &format!(
                        "{} is already present beside the unchanged Open session, which no \
                         writer leaves: every fence of this session would lose on it, so it is \
                         held rather than reported as contention",
                        object_name(&obligation_key)
                    ),
                ),
            }
        }
        // An outcome the store could not report is never read as a `Conflict`, as success, or as
        // a session left in place: the batch may have landed, or may yet. It is named and counted
        // as what it is, never re-sent, and the next pass reads the session and finds which
        // (module doc, step 4).
        Err(fault) if classify(fault.as_ref()) == ErrorClass::Indeterminate => {
            fence_unknown(pass, key, arm, &fault.to_string());
        }
        // A commit that failed with nothing in doubt: the batch did not land.
        Err(fault) => hold(pass, key, "fence-commit-failed", &fault.to_string()),
    }
    Ok(())
}

/// Name a fence whose commit came back with an outcome the store could not report, and count it
/// — as neither a fence nor a hold. The session may already be `Aborting`, in which case the
/// retirement leg of this step tears it down; it may be `Open` still, with the batch yet to land
/// or never to; so the signal says only what is known.
fn fence_unknown(pass: &mut Pass, key: &[u8], arm: Arm, detail: &str) {
    pass.unknown += 1;
    tracing::warn!(
        target: "wyrd.custodian.reaper.audit",
        action = "reaper-fence-outcome-unknown",
        key = %object_name(key),
        arm = arm.as_str(),
        detail = %detail,
        "the store could not report whether the multipart reaper's fence of this upload landed: \
         it may already be Aborting, or may yet become so, so it is counted neither as fenced nor \
         as left in place, and the fence is not sent again; the next pass reads which — operator \
         signal",
    );
}

/// Name a session the pass cannot judge, could not read or could not fence, and count it.
fn hold(pass: &mut Pass, key: &[u8], reason: &'static str, detail: &str) {
    pass.held += 1;
    tracing::warn!(monotonic_counter.reaper_sessions_held = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reaper.audit",
        action = "reaper-session-held",
        key = %object_name(key),
        hold = reason,
        detail = %detail,
        "the multipart reaper left a session in place it cannot judge or could not fence; the \
         pass goes on to the next — operator signal",
    );
}

/// A profile check refused: the step drains nothing. `stopped_after` tells the two refusals
/// apart for the operator. `None`: the pass was refused **before acting** — it judged no
/// session, fenced nothing and wrote nothing. `Some(pass)`: the pass had judged, and its last
/// read of `mpuctl` refused the step — the sessions it fenced by then were judged and sized by
/// the profile its first read matched, and they stay fenced (their teardown waits for a step the
/// check admits). The signal counts them, and beside them the fences whose outcome the store
/// could not report, which may be fenced too.
fn emit_profile_refused(stopped_after: Option<&Pass>, detail: &str) {
    tracing::warn!(monotonic_counter.reaper_profile_refusals = 1_u64);
    match stopped_after {
        None => tracing::warn!(
            target: "wyrd.custodian.reaper.audit",
            action = "reaper-profile-mismatch",
            refused = "before-acting",
            detail = %detail,
            "the multipart reaper refused before acting: the mpuctl budget profile is not the \
             one this custodian is sized for, or cannot be established, so it judged and fenced \
             nothing, and this step drains nothing — NEEDS-HUMAN operator signal",
        ),
        Some(pass) => {
            let (fenced, unknown) = (pass.fenced(), pass.unknown);
            tracing::warn!(
                target: "wyrd.custodian.reaper.audit",
                action = "reaper-profile-mismatch",
                refused = "after-fencing",
                fenced,
                unknown,
                detail = %detail,
                "the multipart reaper stopped after fencing {fenced} session(s), and after \
                 {unknown} fence(s) whose outcome the store could not report: at its last read \
                 the mpuctl budget profile is not the one this custodian is sized for, or cannot \
                 be established. What it fenced was judged and sized by the profile its first \
                 read matched, and stays fenced; this step drains nothing — NEEDS-HUMAN operator \
                 signal",
            );
        }
    }
}

/// The persisted listing position is not an `mpu:` key a backend accepts
/// ([`is_resumable_cursor`]): read from the head, and rewritten. A value past the key bound is
/// named by its length and its head, never logged whole.
fn emit_unusable_cursor(cursor: &[u8]) {
    let head = &cursor[..cursor.len().min(64)];
    tracing::warn!(
        target: "wyrd.custodian.reaper.audit",
        action = "unusable-reaper-cursor",
        cursor = %object_name(head),
        len = cursor.len(),
        "the multipart reaper could not resume from the persisted listing position (not an mpu: \
         key, or longer than any key a backend accepts); restarting from the head and rewriting \
         it — operator signal",
    );
}

/// A session whose clock source the reaper does not own (`0016:1986-2000`): never judged, always
/// named.
fn emit_foreign(key: &[u8], source: &str, ours: &str) {
    tracing::warn!(monotonic_counter.reaper_foreign_clock_sessions = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reaper.audit",
        action = "reaper-foreign-clock",
        key = %object_name(key),
        clock_source = %source,
        reaper_clock_source = %ours,
        "the multipart reaper will not judge a session stamped under a clock it does not read; \
         its teardown, once it leaves Open, still runs — operator signal",
    );
}

/// Name one fence that committed.
fn emit_fenced(key: &[u8], arm: Arm, epoch: u64) {
    tracing::info!(
        target: "wyrd.custodian.reaper",
        key = %object_name(key),
        arm = arm.as_str(),
        epoch,
        "the multipart reaper fenced an abandoned upload to Aborting",
    );
}

/// Name one fence that lost a precondition.
fn emit_lost(key: &[u8], arm: Arm) {
    tracing::info!(
        target: "wyrd.custodian.reaper",
        key = %object_name(key),
        arm = arm.as_str(),
        "the multipart reaper's fence lost to evidence that moved after its reads; the next pass \
         judges the session again",
    );
}

/// The pass's outcome counts on the durability seam.
fn emit_pass(pass: &Pass) {
    tracing::info!(monotonic_counter.reaper_passes = 1_u64);
    tracing::info!(monotonic_counter.reaper_sessions_fenced_idle = pass.fenced_idle);
    tracing::info!(monotonic_counter.reaper_sessions_fenced_ceiling = pass.fenced_ceiling);
    tracing::info!(monotonic_counter.reaper_fences_lost = pass.lost);
    tracing::info!(monotonic_counter.reaper_fences_unknown = pass.unknown);
    tracing::info!(monotonic_counter.reaper_sessions_kept = pass.kept);
    tracing::debug!(
        target: "wyrd.custodian.reaper",
        fenced_idle = pass.fenced_idle,
        fenced_ceiling = pass.fenced_ceiling,
        lost = pass.lost,
        unknown = pass.unknown,
        held = pass.held,
        foreign = pass.foreign,
        kept = pass.kept,
        whole = pass.whole,
        "multipart reaper pass",
    );
}

#[cfg(test)]
mod tests;
