//! The **retirement leg**: the custodian drains the `retire:` ledger, the obligations a
//! multipart transition installs for work too large for its own batch (proposal 0016 decision 4
//! rule 1, `0016:1081`; the batch inventory's *Retirement drain step* row, `0016:668`).
//!
//! An obligation is one small record, installed atomically with the transition that created the
//! work, and drained here in bounded batches, idempotently. The mode lives in the key, so it is
//! never a misread field (`0016:434-441`), and the two modes are opposite instructions:
//!
//! * **`retire:records:`** (#1061): records to delete whose bytes something else still protects
//!   — a publication's published parts (`{parts}`: each member's `part:` and `psum:` record,
//!   whose bytes the published object now names) and one rolled-back `Completing` attempt's
//!   segments (`{seg}`: the group's `seg:` records, whose chunks the session's own `part:`
//!   records still name), or both in one payload. **It deletes; it never writes an `orphan:`
//!   mark.**
//! * **`retire:bytes:`** (#1062, [`marks`]): bytes nothing will name once the obligation is
//!   drained — a session's teardown (`{session}`, `{session, all}`, `{session, parts}`), a
//!   Complete's unnamed parts (`{parts}`), a losing or re-uploaded part's chunks (`{chunks}`), a
//!   superseded generation (`{generation}`, flat or segmented). **It marks every position the
//!   obligation names, and only then deletes the records that named them** (decision 4 rule 2,
//!   `0016:1123`); the module [`marks`] says how, and what holds an obligation instead.
//!
//! # One leg, bounded per pass
//!
//! [`crate::reconcile_step`] runs the leg on the **reconstruction** input, after the
//! reconstruction loop, so the deployed custodian runs it every reconstruction pass
//! (`crates/server/src/custodian.rs`, `run_reconstruction_until`). One pass:
//!
//! * visits at most [`VISITS_PER_PASS`] obligations, read in pages of [`LEDGER_PAGE`]; every
//!   obligation read counts, whether it progresses, cannot progress, or is not this build's to
//!   drain;
//! * reads at most [`LEDGER_PAGES_PER_PASS`] ledger pages and [`READS_PER_PASS`] times in all —
//!   a records step reads at most [`READS_PER_STEP`] times, a marking step at most
//!   [`READS_PER_MARKING_STEP`];
//! * commits at most [`STEPS_PER_PASS`] drain steps, of either mode, plus one write of its resume
//!   point ([`COMMITS_PER_PASS`]).
//!
//! None of those depends on the ledger's size. The walk resumes after the last obligation the
//! previous pass visited and returns to the head once it reaches the end, so an obligation that
//! cannot progress delays the rest by at most a lap and never blocks them: every obligation
//! present for a whole lap is visited once per lap.
//!
//! # Where the walk resumes: [`LEDGER_CURSOR_KEY`]
//!
//! The deployed loop builds fresh contexts every pass and shared mutable globals are ruled out
//! (ADR-0035), so the position lives in the store, beside the ledger rather than in it, the way
//! GC keeps its `orphan:` walk position (`crate::gc`, `ORPHAN_CURSOR_KEY`). It is a position,
//! never evidence: whatever it holds, the walk only reads from there, so a stale or damaged value
//! costs at most a lap. It is **not** the obligation's own drain cursor, which `0016:612-619`
//! puts inside the obligation ([`wyrd_core::multipart::RetireCursor`]); it says only which
//! obligation the next pass looks at first.
//!
//! # One step
//!
//! Decode the obligation against its key, with its cursor; derive the next batch from the cursor;
//! commit `require(retire:… == prior)` + the step's work + the obligation put back with the
//! advanced cursor. The step that finishes the obligation deletes it instead, in the same batch.
//! A records step's work is its deletes; a marking step's is its marks, each under its own
//! precondition, and the deletes of records whose positions an earlier step marked ([`marks`]).
//! A step works in one component at a time, in the payload's order (`parts`, then `seg`), so
//! moving from one component to the next is always a commit boundary. Every batch stays within
//! [`MAX_BATCH_BYTES`] and [`MAX_BATCH_OPS`], its precondition counted. A precondition that fails
//! re-reads the obligation and plans again, within the pass's step budget.
//!
//! **A marking step stamps its marks with the loop's one clock, read for that step** (ADR-0009):
//! [`ReconstructionContext::clock`], the source the deployed loop's `now_millis` is also read
//! from, read after the step has its obligation in hand — never the pass's `now_millis`, which was
//! read before the pass's earlier legs ran. An obligation installed while those legs ran gets
//! marks no older than itself, so its grace runs in full from the drain (`0016:1098-1104`).
//!
//! * `{parts}`: the members from the cursor, found by one binary search over the set's runs
//!   ([`wyrd_core::multipart::PartNumberSet::members_from`]), never a scan from the first run;
//!   for each, `part:<id>:<n>` and `psum:<id>:<n>` are deleted. The cursor is the next member.
//! * `{seg}`: the group's `seg:` records from the cursor, page by page; the cursor is the next
//!   segment index. A `{seg}` payload names a nonce and an epoch, never an extent, so nothing but
//!   a read says where the range ends: the component is finished only when the store says no
//!   record follows the batch, and a read of the group's **whole** range, from its start, finds
//!   nothing below the cursor. A page the store cut short (`scan_page` may return fewer rows than
//!   asked, with `next` set) is never the end: one more one-row read past it finds the record the
//!   cursor moves to, or that there is none. A cursor past the last record with records still
//!   below it is held, never taken for the end.
//!
//! # What a pass certifies
//!
//! [`Reconciled::Satisfied`] says the ledger owes nothing, so a pass answers it only when it read
//! the **whole** ledger, from its head to its end, and found no obligation of either mode in it.
//! A pass that resumed part-way, stopped short of the end, or read an obligation it did not
//! finish — its step budget spent on preconditions another writer kept moving, say, or a marking
//! step waiting on GC or on its session's segments ([`marks`]) — answers [`Reconciled::Partial`]
//! when it changed and held nothing: the next pass goes on from there. A ledger longer than one
//! pass's visits therefore never certifies in one pass.
//!
//! # What holds one obligation, and only that one
//!
//! An obligation the step cannot act on is **held**: left byte-identical, named on the audit seam
//! with its [`Hold`] reason, and the pass answers [`Reconciled::Blocked`], while the walk goes on
//! to the next obligation. A value that does not decode against its key; an advanced value that
//! would cross [`MAX_VALUE_BYTES`], which the step measures and **never submits** (FoundationDB
//! refuses one for good, `2103 value_too_large`, so submitting it would end the pass every time
//! and starve every obligation after it); a `{seg}` cursor below which records remain; a key
//! under the group's range that is not one of its segments; and a step whose commit failed. A
//! store fault reading the ledger, or a segment page, still fails the pass, as in every leg.
//!
//! An obligation installed by this build fits: every installer is held to
//! [`wyrd_core::multipart::RETIRE_VALUE_CEILING`], which leaves the cursor's room under
//! [`MAX_VALUE_BYTES`]. The ceiling hold is the backstop for one installed without that room.
//!
//! A marking step adds its own holds ([`marks`]): a session whose owned `sidx:` range is not empty
//! ([`Hold::OwnedResidue`]), a record whose placement it cannot trust or cannot read
//! ([`Hold::UntrustedPlacement`], [`Hold::RecordUnreadable`]), a mark that is none of the three
//! shapes ([`Hold::UnreadableMark`]), and a `reclaiming` mark over a position a record the drain
//! keeps still names ([`Hold::ReclaimingRetained`], which needs a human).

mod marks;

use wyrd_core::metadata::{self, seg_key, seg_range_prefix, SegmentGroup, MAX_VALUE_BYTES};
use wyrd_core::multipart::{
    decode_retire_obligation, part_key, psum_key, PartNumber, PartScope, RetireComponent,
    RetireCursor, RetireMode, RetirePayload, RetireToken, UploadId, MAX_BATCH_BYTES, MAX_BATCH_OPS,
};
use wyrd_testkit::Clock;
use wyrd_traits::{CommitOutcome, MetadataStore, Result, WriteBatch};

use crate::gc::{checked_page, object_name};
use crate::reconciliation::Reconciled;
use crate::reconstruction::ReconstructionContext;

/// The prefix of the whole retirement ledger, both modes: the walk reads it in key order, so it
/// visits every obligation whatever its mode and decides by the key's own mode.
const RETIRE_PREFIX: &[u8] = b"retire:";

/// Where the retirement walk resumes: the last `retire:` key a pass visited, the exclusive cursor
/// the next pass reads after, or empty to start at the head of the ledger (as does an absent
/// record). Outside `retire:`, so the walk never reads its own bookmark (module doc).
pub const LEDGER_CURSOR_KEY: &[u8] = b"custodian:retire-cursor";

/// The most obligations one pass visits. Every obligation read counts as a visit, whatever
/// becomes of it, so a ledger of obligations that cannot progress costs a pass exactly what a
/// ledger of productive ones does.
pub const VISITS_PER_PASS: usize = 64;

/// The most obligations one page of the ledger holds. A page carries the obligations' values,
/// each up to [`MAX_VALUE_BYTES`], so this bounds what one read holds in memory: 1.6 MB.
pub const LEDGER_PAGE: usize = 16;

/// The most ledger pages one pass reads: the pages [`VISITS_PER_PASS`] fills, and one more for a
/// short page that does not end the ledger. A conforming store needs no more (`scan_page`'s
/// termination clause: a short page is the last); the cap makes the bound the walk's own.
pub const LEDGER_PAGES_PER_PASS: usize = VISITS_PER_PASS / LEDGER_PAGE + 1;

/// The most drain steps one pass plans. Each plans one batch and commits it at most once; a
/// precondition that failed and the re-plan after it spend another step.
pub const STEPS_PER_PASS: usize = 16;

/// The most store reads one **records** step makes: a re-read of the obligation after its
/// precondition failed, and for a `{seg}` component a page of the group's records from the
/// cursor, one row past that page when the store cut it short, and, at the end of the group, one
/// read of its whole range from the start.
pub const READS_PER_STEP: usize = 4;

/// The operations every step spends besides its work: the obligation's own precondition, and
/// its put (the advanced cursor) or its delete (the step that finishes it).
const STEP_FIXED_OPS: usize = 2;

/// The most entries one **marking** step visits: `part:` records it reads (one keyed read each,
/// or one page of a session's range) or `seg:` records it reads (one page), and so the most
/// entries whose records it deletes. One ledger page's worth, so a step holds at most sixteen
/// record values in memory, 1.6 MB at the value ceiling.
pub const ENTRIES_PER_MARKING_STEP: usize = 16;

/// The most fragment positions one **marking** step reads the `orphan:` mark of, and so the most
/// marks it writes. Derived from the operation budget (`0016:668`, `B` marks a batch): every mark
/// costs two operations, its own precondition and its put, and every entry whose records the step
/// deletes at most two (a `part:` and its `psum:`), beside the step's fixed two —
/// `(500 - 2 - 2 × 16) / 2` = 233. Its bytes are far inside [`MAX_BATCH_BYTES`]: a mark's key is
/// at most 73 bytes and its value at most 328 (`metadata::MAX_ORPHAN_EVENT_LEN`), so 233 of them,
/// precondition and put, come to under 200 KB.
pub const MARKS_PER_STEP: usize =
    (MAX_BATCH_OPS as usize - STEP_FIXED_OPS - 2 * ENTRIES_PER_MARKING_STEP) / 2;

/// The most store reads one **marking** step makes: a re-read of the obligation after its
/// precondition failed, one row of the session's owned `sidx:` range, the session token's
/// `retire:records:` obligation (segments before parts, [`marks`]), one read per entry
/// ([`ENTRIES_PER_MARKING_STEP`]) and one per position ([`MARKS_PER_STEP`]): 252.
pub const READS_PER_MARKING_STEP: usize = 3 + ENTRIES_PER_MARKING_STEP + MARKS_PER_STEP;

// A marking step is the one that reads most, so it is the per-step bound the pass is sized by.
const _: () = assert!(READS_PER_MARKING_STEP >= READS_PER_STEP);
// Its operations fit the batch budget: the fixed two, two per mark, two per deleted entry.
const _: () = assert!(
    STEP_FIXED_OPS + 2 * MARKS_PER_STEP + 2 * ENTRIES_PER_MARKING_STEP <= MAX_BATCH_OPS as usize
);

/// The most store reads one pass makes: the resume point, the ledger pages and every step's, each
/// at the marking step's bound — 4,038.
pub const READS_PER_PASS: usize =
    1 + LEDGER_PAGES_PER_PASS + READS_PER_MARKING_STEP * STEPS_PER_PASS;

/// The most commits one pass makes: one per step, and the resume point when it moved.
pub const COMMITS_PER_PASS: usize = STEPS_PER_PASS + 1;

/// The longest key a backend accepts — FoundationDB's 10 KB, the tightest in play
/// (`crates/traits/src/lib.rs`, *Operational envelope*). Every key the walk reads is one the store
/// already holds, so it is within this; a resume point past it is damage, and handed to a
/// backend as a cursor it would be refused on every pass.
const LEDGER_KEY_MAX_LEN: usize = 10_000;

/// Why an obligation is **held**: left byte-identical this pass, named on the audit seam, and the
/// pass answers [`Reconciled::Blocked`]. Holding one never stops the walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hold {
    /// The value does not decode against its key ([`decode_retire_obligation`]), or the key is
    /// not a `retire:` key the grammar spells. Acting on it would mean guessing what it owes.
    Undecodable,
    /// The value the step would put back, with its advanced cursor, is past [`MAX_VALUE_BYTES`].
    /// The store would refuse it for good, so it is never sent. An obligation installed within
    /// [`wyrd_core::multipart::RETIRE_VALUE_CEILING`] never meets this.
    ValuePastCeiling,
    /// A `{seg}` cursor past the group's last record, with records still below it. The payload
    /// names no extent, so only the whole range read from its start says the group is drained;
    /// here it says it is not, and the cursor was written by no drain.
    SegmentCursorPastRecords,
    /// A key under the group's `seg:` range that is not one of its segments' keys. Deleting it
    /// would act on a record nothing spells; skipping it would finish the group with it left.
    MalformedSegmentKey,
    /// The cursor the step would write is one decode refuses. No drain step derives one; held
    /// rather than written.
    CursorRefused,
    /// The step's commit failed: a store fault, or an outcome the store could not report. The
    /// next visit re-reads the obligation, which shows whether it landed.
    CommitFailed,
    /// A `session` component whose session still owns staged residue: its `sidx:` range is not
    /// empty. The owned walk that empties it is #1013's; until then the obligation is the only
    /// deleter in reach of those entries, so it stays.
    OwnedResidue,
    /// A record — or an obligation's own chunk list — whose placement is not exactly one D server
    /// per fragment of its scheme (the staged rule, `crate::gc::staged_placement`) or, for a
    /// committed generation's chunk, neither empty nor that length
    /// (`ChunkRef::checked_fragments`). The drain marks nothing from it and deletes nothing
    /// under it: its true positions are unknown. Its fragments stay — GC reclaims none it holds no
    /// mark for, and its staged class holds every fragment of an untrusted `part:` record's chunk
    /// whole; #1013 retires such a record by chunk id.
    UntrustedPlacement,
    /// A record the obligation names is absent, will not decode, or sits under a key that is not
    /// the record the obligation names: the positions it held are unknown, so nothing of it is
    /// marked or deleted.
    RecordUnreadable,
    /// An `orphan:` mark the drain met is none of the three shapes
    /// (`metadata::decode_orphan_mark`): it is kept byte-identical, as GC keeps it, and nothing
    /// after it is marked.
    UnreadableMark,
    /// A `reclaiming` mark over a position a record the drain keeps still names — a `part:` record
    /// it deletes only once the position carries its own mark. GC never finishes such a mark (the
    /// staged class outranks it) and the drain never replaces one, so nothing moves without a
    /// human; no in-tree path makes this state, so it means damage or a store edit.
    ReclaimingRetained,
}

impl Hold {
    /// The reason as the audit event spells it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Undecodable => "undecodable",
            Self::ValuePastCeiling => "value-past-ceiling",
            Self::SegmentCursorPastRecords => "segment-cursor-past-records",
            Self::MalformedSegmentKey => "malformed-segment-key",
            Self::CursorRefused => "cursor-refused",
            Self::CommitFailed => "commit-failed",
            Self::OwnedResidue => "owned-residue",
            Self::UntrustedPlacement => "untrusted-placement",
            Self::RecordUnreadable => "record-unreadable",
            Self::UnreadableMark => "unreadable-mark",
            Self::ReclaimingRetained => "reclaiming-under-retained-record",
        }
    }
}

/// One pass's budgets and what it did.
struct Pass {
    visits_left: usize,
    steps_left: usize,
    /// A step committed.
    changed: bool,
    /// An obligation was held.
    held: bool,
    /// An obligation was read: work was owed, so the pass certifies nothing, whatever became of
    /// it — drained a step, held, waiting, or left after its step budget was spent.
    owed: bool,
}

// deferred: #660 — two drainers of one obligation (the `require(retire:… == prior)` precondition
// is what serializes them; their interleavings are #660's). The one seeded Tier-0 row this leg
// carries is the marking step's exact-value re-stamp racing GC's swap to `reclaiming`
// (`crates/dst/tests/custodian.rs`, #1062).
/// One retirement pass over the store `ctx` names — the leg [`crate::reconcile_step`] runs after
/// reconstruction. Answers [`Reconciled::Blocked`] if it held any obligation, else
/// [`Reconciled::Changed`] if it committed a step, else [`Reconciled::Satisfied`] only if it read
/// the whole ledger from its head to its end and found no obligation in it, else
/// [`Reconciled::Partial`] (module doc, *What a pass certifies*). Every marking step reads its
/// stamp from `ctx.clock` ([`ReconstructionContext::clock`]), the loop's one time source.
pub(crate) async fn reconcile(ctx: &ReconstructionContext<'_>) -> Result<Reconciled> {
    let meta = ctx.meta;
    let stored = meta
        .get(LEDGER_CURSOR_KEY)
        .await?
        .map(|value| value.to_vec());
    let start = match stored.as_deref() {
        None | Some([]) => None,
        Some(key) if key.starts_with(RETIRE_PREFIX) && key.len() <= LEDGER_KEY_MAX_LEN => {
            Some(key.to_vec())
        }
        Some(key) => {
            emit_unusable_cursor(&object_name(key), key.len());
            None
        }
    };

    let mut pass = Pass {
        visits_left: VISITS_PER_PASS,
        steps_left: STEPS_PER_PASS,
        changed: false,
        held: false,
        owed: false,
    };
    let from_head = start.is_none();
    let mut position = start;
    let mut pages_left = LEDGER_PAGES_PER_PASS;
    let mut reached_end = false;
    'walk: while pass.visits_left > 0 && pass.steps_left > 0 && pages_left > 0 {
        pages_left -= 1;
        // Never more than the visits left, and `checked_page` refuses a longer page, so the visit
        // counter cannot run out inside a page.
        let limit = pass.visits_left.min(LEDGER_PAGE);
        let (page, next) = checked_page(
            meta,
            RETIRE_PREFIX,
            "retire-ledger",
            position.as_deref(),
            limit,
        )
        .await?;
        for (key, value) in page {
            if pass.steps_left == 0 {
                // The steps are spent: the walk stops before this obligation rather than passing
                // it by, so the next pass resumes here and no obligation is skipped for a lap.
                break 'walk;
            }
            pass.visits_left -= 1;
            visit(meta, ctx.clock, &mut pass, &key, value.to_vec()).await?;
            position = Some(key);
        }
        if next.is_none() {
            reached_end = true;
            break;
        }
    }

    // Persist where the next pass resumes — after the last obligation visited, or the head once
    // the walk reached the end — and only when it moved, so a small ledger read whole every pass
    // writes nothing. A blind put, as GC's: whatever lands, the walk only reads from there.
    let resume: Vec<u8> = if reached_end {
        Vec::new()
    } else {
        position.unwrap_or_default()
    };
    let unchanged = match &stored {
        None => resume.is_empty(),
        Some(stored) => *stored == resume,
    };
    if !unchanged {
        meta.commit(WriteBatch::new().put(LEDGER_CURSOR_KEY, resume))
            .await?;
    }

    // Certify only what this pass saw: the whole ledger, head to end, owing nothing. A pass that
    // resumed part-way did not read the obligations before its start this pass — earlier passes
    // may have left them half drained — and one that read an obligation it did not finish knows
    // work is left.
    let saw_whole_ledger = from_head && reached_end;
    Ok(if pass.held {
        Reconciled::Blocked
    } else if pass.changed {
        Reconciled::Changed
    } else if pass.owed || !saw_whole_ledger {
        Reconciled::Partial
    } else {
        Reconciled::Satisfied
    })
}

/// Visit one obligation read from the ledger as (`key`, `value`): drain it a step in the mode its
/// key names if the pass has a step left, or hold it if it cannot be. A marking step reads its
/// stamp from `clock` here, once its obligation's bytes are in hand — the ledger page's, or the
/// re-read's after a lost precondition — so the stamp is never older than the obligation it
/// evidences.
async fn visit(
    meta: &dyn MetadataStore,
    clock: &(dyn Clock + Sync),
    pass: &mut Pass,
    key: &[u8],
    value: Vec<u8>,
) -> Result<()> {
    let mut prior = value;
    loop {
        let (mode, token, payload) = match decode_retire_obligation(key, &prior) {
            Ok(decoded) => decoded,
            Err(fault) => {
                hold(pass, key, Hold::Undecodable, &fault.to_string());
                return Ok(());
            }
        };
        pass.owed = true;
        if pass.steps_left == 0 {
            // Only after a lost precondition (the walk visits nothing once the steps are spent):
            // the obligation is still here and still owed, so the pass answers `Partial` for it
            // and the next pass re-reads it.
            return Ok(());
        }
        pass.steps_left -= 1;
        let planned = match mode {
            RetireMode::Records => match plan(meta, key, &prior, &token, &payload).await? {
                Ok(step) => Planned::Step(step),
                Err((reason, detail)) => Planned::Held(Held::of(reason, detail)),
            },
            RetireMode::Bytes => {
                // The stamp this step's marks carry: the loop's one clock (ADR-0009), read now —
                // after this step read its obligation — never the pass's `now_millis`, which was
                // read before reconstruction ran and could predate an obligation installed since.
                let stamp_millis = clock.now_millis();
                marks::plan(meta, key, &prior, &token, &payload, stamp_millis).await?
            }
        };
        let step = match planned {
            Planned::Step(step) => step,
            Planned::Held(held) => {
                hold_at(pass, key, &held);
                return Ok(());
            }
            Planned::Wait(reason) => {
                // Owed, unchanged, and nothing at fault: the pass answers `Partial` for it.
                emit_wait(key, reason);
                return Ok(());
            }
        };
        let (deleted, marked) = (step.deletes.len(), step.marks.len());
        let mut batch = WriteBatch::new().require(key.to_vec(), prior.clone());
        for mark in step.marks {
            batch = match mark.prior {
                // Arm (b): the mark the step read, replaced only if it still holds those bytes.
                Some(read) => batch.require(mark.key.clone(), read),
                // Arm (c): written only if the position is still unmarked.
                None => batch.require_absent(mark.key.clone()),
            }
            .put(mark.key, mark.value);
        }
        batch.deletes.extend(step.deletes);
        batch = match step.value {
            Some(value) => batch.put(key.to_vec(), value),
            None => batch.delete(key.to_vec()),
        };
        match meta.commit(batch).await {
            Ok(CommitOutcome::Committed) => {
                pass.changed = true;
                emit_step(deleted, marked);
                return Ok(());
            }
            // Another writer moved the obligation, or a mark, between the read and the commit:
            // read the obligation again and plan from what is there now — every mark re-read, so
            // a mark GC moved to `reclaiming` meanwhile is met as one, never re-stamped — or stop
            // if it is gone (its drain finished).
            Ok(CommitOutcome::Conflict) => match meta.get(key).await? {
                Some(now) => prior = now.to_vec(),
                None => return Ok(()),
            },
            Err(fault) => {
                hold(pass, key, Hold::CommitFailed, &fault.to_string());
                return Ok(());
            }
        }
    }
}

/// What a step planned: a batch to commit, a hold, or — a marking step only — nothing to do this
/// step, with nothing at fault ([`marks`], the wait reasons).
enum Planned {
    Step(Step),
    Held(Held),
    Wait(&'static str),
}

/// One planned step: the marks it writes, the records it deletes, and the obligation's next value
/// — `None` when the step finishes its work and so deletes the obligation too. A records step
/// writes no mark.
struct Step {
    marks: Vec<Mark>,
    deletes: Vec<Vec<u8>>,
    value: Option<Vec<u8>>,
}

/// One `orphan:` mark a marking step writes: its key, the bytes it read there (`None` when the
/// position was unmarked), and the structured mark it puts.
struct Mark {
    key: Vec<u8>,
    prior: Option<Vec<u8>>,
    value: Vec<u8>,
}

/// What a records step can do instead of a [`Step`]: hold the obligation, with the reason and the
/// detail the audit event carries.
type HeldRecords = (Hold, String);

/// A hold, with what the audit event names: the reason, the detail, and — for a marking step —
/// the `orphan:` position and the record the hold is about.
struct Held {
    reason: Hold,
    detail: String,
    position: Option<Vec<u8>>,
    record: Option<Vec<u8>>,
}

impl Held {
    fn of(reason: Hold, detail: String) -> Self {
        Self {
            reason,
            detail,
            position: None,
            record: None,
        }
    }
}

/// Plan the next step of the `retire:records:` obligation at `key`, whose stored bytes are
/// `prior`: the component and entry its cursor names (the first component at its start when it
/// has none), and from there one batch.
async fn plan(
    meta: &dyn MetadataStore,
    key: &[u8],
    prior: &[u8],
    token: &RetireToken,
    payload: &RetirePayload,
) -> Result<std::result::Result<Step, HeldRecords>> {
    let budget = Budget::for_step(key, prior);
    let cursor = payload.cursor().copied();
    let component = match cursor {
        Some(cursor) => cursor.component(),
        None if payload.parts().is_some() => RetireComponent::Parts,
        None => RetireComponent::Seg,
    };
    let entry = cursor.map_or(0, |cursor| cursor.entry());
    let (deletes, next) = match component {
        RetireComponent::Parts => match parts_step(token, payload, entry, &budget) {
            Ok(planned) => planned,
            Err(held) => return Ok(Err(held)),
        },
        RetireComponent::Seg => match seg_step(meta, payload, entry, &budget).await? {
            Ok(planned) => planned,
            Err(held) => return Ok(Err(held)),
        },
        other => {
            // Decode admits only `parts` and `seg` under `retire:records:`, and a cursor only in
            // a component the payload carries; anything else is a value this step cannot act on.
            let detail = format!("a records-mode cursor in `{}`", other.name());
            return Ok(Err((Hold::CursorRefused, detail)));
        }
    };
    let value = match next {
        None => None,
        Some(cursor) => match advanced_value(payload, RetireMode::Records, cursor) {
            Ok(value) => Some(value),
            Err(held) => return Ok(Err(held)),
        },
    };
    let marks = Vec::new();
    Ok(Ok(Step {
        marks,
        deletes,
        value,
    }))
}

/// The obligation's value with its drain moved to `cursor`, as the step puts it back: checked as
/// decode checks a stored cursor ([`RetirePayload::advanced`]), and measured — a value past
/// [`MAX_VALUE_BYTES`] is held, never sent.
fn advanced_value(
    payload: &RetirePayload,
    mode: RetireMode,
    cursor: RetireCursor,
) -> std::result::Result<Vec<u8>, HeldRecords> {
    match payload.advanced(mode, cursor) {
        Ok(advanced) => {
            let value = metadata::encode(&advanced).to_vec();
            if value.len() > MAX_VALUE_BYTES {
                let detail = format!(
                    "the advanced value is {} bytes, past {MAX_VALUE_BYTES}; it was not sent",
                    value.len()
                );
                return Err((Hold::ValuePastCeiling, detail));
            }
            Ok(value)
        }
        Err(fault) => Err((Hold::CursorRefused, fault.to_string())),
    }
}

/// What one step's batch may carry besides its fixed operations (`0016:622-648`: `B` is the
/// smaller of the byte- and the operation-derived count). The fixed part is charged at its
/// largest: the obligation's precondition (its key and the exact bytes read) and its put (its
/// key and a value at the ceiling, which is the most the step ever sends).
struct Budget {
    ops: usize,
    bytes: usize,
}

impl Budget {
    fn for_step(key: &[u8], prior: &[u8]) -> Self {
        let fixed_bytes = 2 * key.len() + prior.len() + MAX_VALUE_BYTES;
        Self {
            ops: MAX_BATCH_OPS as usize - STEP_FIXED_OPS,
            bytes: (MAX_BATCH_BYTES as usize).saturating_sub(fixed_bytes),
        }
    }

    /// How many items fit, each costing `ops` operations and `bytes` bytes.
    fn items(&self, ops: usize, bytes: usize) -> usize {
        (self.ops / ops).min(self.bytes / bytes.max(1))
    }
}

/// The next records-step of a `{parts}` component: the members from `entry` (the set's first
/// member when the drain starts), as many as the budget holds at two deletes each. The cursor
/// after it is the next member, or, past the last, the start of `seg` if the payload carries one;
/// `None` when there is nothing after.
fn parts_step(
    token: &RetireToken,
    payload: &RetirePayload,
    entry: u32,
    budget: &Budget,
) -> std::result::Result<(Vec<Vec<u8>>, Option<RetireCursor>), HeldRecords> {
    let (Some(PartScope::Set(set)), RetireToken::Session { upload_id, .. }) =
        (payload.parts(), token)
    else {
        let detail = "a records-mode `parts` component that is not a part-number set".to_owned();
        return Err((Hold::CursorRefused, detail));
    };
    let probe = member_keys(upload_id, set.runs()[0].0);
    let per_member = probe.0.len() + probe.1.len();
    let take = budget.items(2, per_member).max(1);
    let mut members = set.members_from(entry);
    let mut deletes = Vec::with_capacity(2 * take);
    for member in members.by_ref().take(take) {
        let (part, psum) = member_keys(upload_id, member);
        deletes.push(part);
        deletes.push(psum);
    }
    let next = match members.next() {
        Some(member) => Some(RetireCursor::new(RetireComponent::Parts, member, 0)),
        None if payload.segments().is_some() => Some(RetireCursor::new(RetireComponent::Seg, 0, 0)),
        None => None,
    };
    Ok((deletes, next))
}

/// The `part:` and `psum:` keys of part `member` of `upload_id`. Every member of a decoded set is
/// a [`PartNumber`], so the conversion cannot fail.
fn member_keys(upload_id: &UploadId, member: u32) -> (Vec<u8>, Vec<u8>) {
    let number = PartNumber::new(member).expect("a part-number set holds only part numbers");
    (part_key(upload_id, number), psum_key(upload_id, number))
}

/// The next records-step of a `{seg}` component: the group's records from segment `entry`, as
/// many as the budget holds. When a record follows them the cursor is its index. When none does,
/// the group is finished only if a read of its whole range from the start finds nothing below
/// `entry`: a cursor past the last record with records below it is held, never a finish.
///
/// What follows the batch is the page's own extra row when it has one. A page with no extra row is
/// the end of the group only when the store says so (`next` is `None`): a store may return fewer
/// rows than asked (`scan_page`'s page bound, a backend's lowered scan cap), and then one more
/// one-row read past the page finds the record the cursor moves to, or that none follows. Taking
/// a short page for the end would delete the obligation with records left that nothing drains.
///
/// The page from the cursor holds at most `MAX_BATCH_OPS - 1` records, values included: 49.9 MB
/// at the value ceiling, inside the 51.2 MB one page of a staged read already holds
/// (`crate::gc`, `STAGED_PAGE`).
async fn seg_step(
    meta: &dyn MetadataStore,
    payload: &RetirePayload,
    entry: u32,
    budget: &Budget,
) -> Result<std::result::Result<(Vec<Vec<u8>>, Option<RetireCursor>), HeldRecords>> {
    let Some(group) = payload.segments() else {
        let detail = "a `seg` cursor on a payload with no segment group".to_owned();
        return Ok(Err((Hold::CursorRefused, detail)));
    };
    let prefix = seg_range_prefix(group);
    let at = |index: u32| seg_key(group, index).map_err(|fault| fault.to_string());
    let take = budget.items(1, at(0).map_or(0, |key| key.len())).max(1);
    let after = match entry.checked_sub(1) {
        None => None,
        Some(index) => match at(index) {
            Ok(key) => Some(key),
            Err(detail) => return Ok(Err((Hold::CursorRefused, detail))),
        },
    };
    let (page, more) =
        checked_page(meta, &prefix, "retire-seg", after.as_deref(), take + 1).await?;
    let mut records = Vec::with_capacity(page.len());
    for (key, _) in page {
        match segment_index(group, &key) {
            Some(index) => records.push((index, key)),
            None => return Ok(Err(stray(&key))),
        }
    }
    let following = if records.len() > take {
        records.pop().map(|(index, _)| index)
    } else if let (Some(_), Some((_, last))) = (&more, records.last()) {
        // The store cut the page short and says more may follow: read one row past it. An empty
        // read is terminal (`scan_page`), so `None` here is the store saying the group ends.
        let (ahead, _) = checked_page(meta, &prefix, "retire-seg", Some(last), 1).await?;
        match ahead.first() {
            None => None,
            Some((key, _)) => match segment_index(group, key) {
                Some(index) => Some(index),
                None => return Ok(Err(stray(key))),
            },
        }
    } else {
        None
    };
    let deletes = records.into_iter().map(|(_, key)| key).collect();
    if let Some(index) = following {
        let cursor = RetireCursor::new(RetireComponent::Seg, index, 0);
        return Ok(Ok((deletes, Some(cursor))));
    }
    if entry > 0 {
        // No record follows the batch. Whether the group is drained is a question about its
        // whole range, which only a read from its start answers.
        let (first, _) = checked_page(meta, &prefix, "retire-seg", None, 1).await?;
        let cursor_key = match at(entry) {
            Ok(key) => key,
            Err(detail) => return Ok(Err((Hold::CursorRefused, detail))),
        };
        if let Some((first, _)) = first.first() {
            if *first < cursor_key {
                let detail = format!(
                    "the cursor is at segment {entry}, past the group's last record, and {} is \
                     still below it",
                    object_name(first)
                );
                return Ok(Err((Hold::SegmentCursorPastRecords, detail)));
            }
        }
    }
    Ok(Ok((deletes, None)))
}

/// The segment index `key` names, if it is one of `group`'s segment keys spelled exactly as
/// [`seg_key`] spells it; `None` for anything else under the group's range.
fn segment_index(group: &SegmentGroup, key: &[u8]) -> Option<u32> {
    let (_, _, index) = metadata::parse_seg_key(key).ok()?;
    let canonical = seg_key(group, index).ok()?;
    (canonical == key).then_some(index)
}

/// The hold for `key`, found under a group's `seg:` range but not one of its segment keys.
fn stray(key: &[u8]) -> HeldRecords {
    let detail = format!("{} is not a segment key of the group", object_name(key));
    (Hold::MalformedSegmentKey, detail)
}

/// Hold the obligation at `key` for `reason`: nothing written, the pass marked blocked, the
/// obligation named on the audit seam.
fn hold(pass: &mut Pass, key: &[u8], reason: Hold, detail: &str) {
    hold_at(pass, key, &Held::of(reason, detail.to_owned()));
}

/// [`hold`], naming beside the obligation the `orphan:` position and the record the hold is about,
/// when it has them (empty when it has not).
fn hold_at(pass: &mut Pass, key: &[u8], held: &Held) {
    pass.held = true;
    let name = |key: &Option<Vec<u8>>| key.as_deref().map(object_name).unwrap_or_default();
    tracing::warn!(monotonic_counter.retirement_obligations_held = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.retirement.audit",
        action = "retire-obligation-held",
        key = %object_name(key),
        hold = held.reason.as_str(),
        detail = %held.detail,
        position = %name(&held.position),
        record = %name(&held.record),
        "the retirement drain held an obligation it cannot act on; it is left byte-identical and \
         the walk goes on to the next — operator signal",
    );
}

/// Count one committed step, the records it deleted and the marks it wrote on the durability seam.
fn emit_step(deleted: usize, marked: usize) {
    tracing::info!(monotonic_counter.retirement_steps_committed = 1_u64);
    tracing::info!(monotonic_counter.retirement_records_deleted = deleted as u64);
    tracing::info!(monotonic_counter.retirement_marks_written = marked as u64);
}

/// Count a marking step that found nothing to do yet, with nothing at fault: a `reclaiming` mark
/// GC is still finishing, or its session's segments still to be drained first ([`marks`]).
fn emit_wait(key: &[u8], reason: &'static str) {
    tracing::info!(monotonic_counter.retirement_steps_waiting = 1_u64);
    tracing::debug!(
        target: "wyrd.custodian.retirement",
        key = %object_name(key),
        wait = reason,
        "the retirement drain left an obligation unchanged this step; it is owed, and the next \
         pass returns to it",
    );
}

/// Name a resume point the walk cannot use: not a `retire:` key, or longer than any key a backend
/// accepts. The walk restarts from the head and rewrites it.
fn emit_unusable_cursor(cursor: &str, len: usize) {
    tracing::warn!(
        target: "wyrd.custodian.retirement.audit",
        action = "unusable-retire-cursor",
        cursor = %cursor,
        len,
        "the retirement drain could not resume from the persisted ledger position (not a \
         bounded retire: key); restarting from the head and rewriting it — operator signal",
    );
}
