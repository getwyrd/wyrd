//! The **session leg** of the retirement drain (#1013): for every multipart session that has left
//! `Open` for `Aborting` or `Completed`, the walk of its owned `sidx:` range, and then its
//! **terminal delete** — the one place a session leaves the store and releases its admission slot
//! (proposal 0016's *Owned-`sidx:` drain step* and *Terminal session delete* rows, `0016:672-674`;
//! decision 5's backstop, `0016:1654-1668`; the terminal-delete rule of §2, `0016:582-602`).
//!
//! # Which sessions, and how much per pass
//!
//! Every session listed under `mpu:`, read from the head in pages of [`SESSION_PAGE`] every pass.
//! Admission bounds that listing: `mpuctl` admits at most its stored `max_sessions`, which decode
//! holds to `SCAN_CAP / 2` (`0016:1470`), and GC reads the same listing every pass
//! (`crate::gc`, `staged_fragments`). What a pass changes is bounded by its step budget: at most
//! [`SESSION_STEPS_PER_PASS`] commits that mark, delete or end something — walk steps, slot
//! batches and terminal deletes — beside the walk's writes of its own two records alone, at most
//! three a session a pass (*The owned walk*, *What a step costs the pass*). A session
//! met once the steps are spent is left for the next pass, and the pass certifies nothing. A
//! session in `Open` or `Completing` is not this leg's. A session key that will not parse, or a
//! session record that will not decode, is held: its state is unknown.
//!
//! # The owned walk
//!
//! Nothing refills a session's `sidx:` range once it has left `Open` — every intent requires
//! `mpu == Open@E` (`wyrd_core::multipart::stage`) — so each step reads one page of
//! [`OWNED_PER_STEP`] entries, from the head of the range unless the walk has passed entries it
//! holds (*Past held entries*, below), and the entries left are the progress. An entry's
//! positions are, in this order:
//!
//! * **the positions it names**, a fixed order its bytes spell: for an entry that decodes and
//!   whose placement is exactly one D server per fragment of its scheme
//!   (`crate::gc::staged_placement`, the rule GC's staged class applies), every position of that
//!   placement; for an `Aborting` session's entry that decodes with a placement failing that rule,
//!   every position it names all the same (`(placement[i], (chunk, i))`), where a write the Abort
//!   left in flight may still land (`wyrd_core::multipart::stage`, *The write deadline*);
//! * **then, for an `Aborting` session's untrusted entry** — that one, or one whose value will not
//!   decode under a key naming its chunk (`parse_sidx_key`, the chunk GC's hold takes) — every
//!   other fragment bearing its chunk id that the fleet's listings find. This is retirement by
//!   chunk id (the parent module's *Untrusted records*), only on a pass that sees the whole fleet;
//!   otherwise, and for a `Completed` session, such an entry is held before anything of it is
//!   marked.
//!
//! **Where the walk stands inside an entry** is kept in the store, since one entry may name more
//! positions than a step reads (a placement is as long as its scheme, up to 510, or as its damage):
//! the bookmark [`owned_walk_key`] holds the key of one entry the last step left and how many of
//! the positions it names are marked. Each step walks the entry it names first, from that count,
//! and the rest of the page after it in range order; it moves to the first entry left, in that
//! order, that the step walked without a hold — whatever it covered of it, so an entry the step's
//! reads ran out on, or one whose listed fragments it could not finish, is walked first by the
//! next step, with the whole read budget — or, if there is none, the first held entry left with a
//! count above zero, so a hold keeps its place while nothing else moves but never takes it from an
//! entry the walk can still work on. A bookmark naming an entry outside the step's page is left as
//! it is unless an entry of the page takes it that way. One case is left out of that: a step that
//! marks and deletes nothing and moves the scan position on (*Past held entries*) has only read
//! again what earlier steps marked, and the entry its reads ran out on is at the head of the next
//! step's page, where it is read with the reads the held ones took here. Such a step does not
//! move the bookmark to that entry: a held entry's count stays, so held entries that one step
//! can read up to their holds do not move the bookmark pass after pass. (The entry the bookmark
//! names is walked first, so a step whose reads ran out on it has held nothing, and is not such a
//! step.)
//!
//! Only the walk writes the bookmark, in the batch of the step whose marks it counts, under a
//! precondition on the bytes it read there — and, where the batch puts it, on the session record
//! as read, so a step that commits after another drainer's terminal delete leaves none behind —
//! and the step that deletes the entry it names deletes or moves it; the terminal delete removes
//! it with the session. Absent, or a count of zero, means "at the first position". One that does
//! not name an entry of this session is no evidence: the step reads from the first position and
//! rewrites it.
//! Fragments the listings find are never counted in it — a listing is no fixed order (the parent
//! module's *Untrusted records*) — so every step that reaches them reads them all, and an entry
//! with more listed fragments than one step reads ([`super::LISTED_PER_CHUNK`]) is held
//! ([`Hold::ListingPastStep`]).
//!
//! **Past held entries.** A held entry stays in the range, so held entries at the head of a page
//! would be read again by every step, and a page of them would hide every entry after it. So a
//! step that marks and deletes nothing and holds something moves the walk's **scan position**
//! ([`owned_scan_key`]) past the held entries at the head of its page, as far as the first entry it
//! did not hold: the next step's page starts after it. Once nothing is left after the position —
//! a page that ends the range with every entry held, or an empty one — the walk is back at the
//! head, and the position is deleted. So every entry is read once a lap, an entry the walk does
//! not hold comes to the head of a page whatever is held before it, and the holds are named again
//! every lap. It is a position, never evidence, as the ledger walk's resume point is
//! ([`super::LEDGER_CURSOR_KEY`]): whatever it holds, the step only reads from there, what an
//! entry's deletion needs is unchanged, and one that names nothing of the session's range is named
//! to the operator and read as the head. It is written in a commit of its own, outside the step
//! budget — a held session would otherwise spend a step every pass, and enough of them would
//! starve every session listed after them — at most twice a session a pass (back to the head, then
//! past what is held there), a move past held entries only while the session record is the one
//! read, so it never outlives its session; the terminal delete removes it too.
//!
//! Each position goes through the drain's mark guard ([`super::marks`], *The mark guard*) with
//! this walk's event identity, `<upload-id>:<epoch>` (`0016:672`), and a stamp read from the loop's
//! one clock once the step has its page in hand (ADR-0009, [`ReconstructionContext::clock`]). A
//! `reclaiming` mark is never replaced, and here it is always a hold, never a wait: every position
//! an entry yields is one GC's staged class protects while the entry stands — its placement, or its
//! whole chunk — so GC never finishes such a mark first.
//!
//! **An entry is deleted only in a step after the one whose commit marked its last position**:
//! once every position it names already carries this walk's mark — read so in this step, or
//! counted by the bookmark — and, for one retired by chunk id, every fragment this step's listing
//! shows was read so in this step; under `require(sidx == the bytes read)`. So no batch both marks
//! a position and deletes the entry that names it, and between the two commits each position
//! carries both (decision 4 rule 2, `0016:1123`). A step reads the scan position, the bookmark,
//! one page — and the range's first page as well when nothing is left after the position, or one
//! row more to learn whether an entry follows a page it holds whole — and at most
//! [`super::MARKS_PER_STEP`] marks, lists the fleet at most once, and its batch carries two
//! operations per mark, two per deleted entry and three for the bookmark, within
//! [`MAX_BATCH_OPS`] and [`MAX_BATCH_BYTES`].
//!
//! **What a step costs the pass.** A step whose batch marks or deletes something is one of the
//! pass's [`SESSION_STEPS_PER_PASS`]. A step that does neither — everything it read is held, or
//! already carries this walk's mark — writes the bookmark at most, is not charged, and ends the
//! session's visit for the pass. Charged, a session whose holds move the bookmark every pass (two
//! entries each held deeper than one step reads take it in turn) would spend a step every pass,
//! and enough such sessions would starve every session listed after them. So a session costs a
//! pass at most one step that marks and deletes nothing: one bookmark write, beside the scan
//! position's two. A bookmark that was lost is caught up one step's reads a pass.
//!
//! The walk's marks name no retirement token, so GC reclaims under them once their grace has run.
//! A mark at a position whose write never landed has no fragment under it, and GC's sweep keeps
//! it (`event-may-await-write`): an abandoned write may still land there, with no bound on when.
//!
//! # The terminal delete
//!
//! Once a step **observes the session's `sidx:` range empty in this pass**, no session-scoped
//! obligation is left (one-row reads of `retire:bytes:s:<id>:` and `retire:records:s:<id>:`), and
//! — for a `Completed` session — the context's [`Tombstone`] says its window has passed: one batch
//! that requires the session record's exact bytes and the `mpuctl` ledger's, deletes the session
//! record, its surviving `slot:` records and the walk's bookmark and scan position if one is
//! left, deletes its `seggrp:<nonce>` marker when no `seg:`
//! record names the nonce (an adopted group's marker outlives the session, `0016:674`), and puts
//! the ledger one session lower (`AdmissionRecord::release_one`). The session-record precondition
//! makes it exactly-once: of two drainers that reach it, one commits, and the other's batch fails
//! whole and finds the record gone. More `slot:` records than one page go first, in batches of
//! their own under the same session precondition: a slot carries no bytes, and nothing reserves one
//! once the session has left `Open`.
//!
//! A ledger that is absent, will not decode or counts no session is held
//! ([`Hold::AdmissionLedger`]), and so is the session: the count is not this leg's to guess.

use std::collections::{BTreeSet, HashSet};

use wyrd_core::metadata::{
    self, encode_orphan_mark, orphan_key, seg_group_prefix, seggrp_key, ChunkRef, OrphanMark,
};
use wyrd_core::multipart::{
    decode_admission_record, decode_owned_entry, decode_session_record, parse_mpu_key,
    parse_sidx_key, retire_session_range, sidx_range, slot_range, RetireMode, SessionRecord,
    SessionState, UploadId, MAX_BATCH_BYTES, MAX_BATCH_OPS, MAX_INFLIGHT_PARTS, MPUCTL_KEY,
    MPU_PREFIX,
};
use wyrd_traits::{
    ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

use super::marks::{read_arm, Arm, MARK_MAX_BYTES};
use super::{
    listing_past_step, ByChunk, Held, Hold, Listed, Pass, Tombstone, LISTED_PER_CHUNK,
    MARKS_PER_STEP,
};
use crate::gc::{checked_page, object_name, staged_placement};
use crate::reconciliation::Reconciled;
use crate::reconstruction::ReconstructionContext;

/// The most session records one page of the listing holds: 1.6 MB at the value ceiling.
const SESSION_PAGE: usize = 16;

/// The most commits the session leg makes in one pass that mark, delete or end something — walk
/// steps, slot batches and terminal deletes, each committed at most once. A precondition that
/// failed and the re-plan after it spend another. A walk step that marks and deletes nothing, and
/// a write of the walk's scan position, are not among them (module doc, *What a step costs the
/// pass*).
const SESSION_STEPS_PER_PASS: usize = 16;

/// The most owned entries one walk step reads, and so the most it deletes.
const OWNED_PER_STEP: usize = 16;

/// The most `slot:` records a terminal delete carries. A session holding more — possible only
/// under a profile whose `max_inflight_parts` exceeds the shipped one — has the rest deleted
/// first, a page per batch.
const SLOT_PAGE: usize = MAX_INFLIGHT_PARTS as usize;

/// What every walk step's batch reserves: the bookmark's precondition, its put or delete, and the
/// session record's precondition a put goes under.
const BOOKMARK_OPS: usize = 3;

// A walk step's operations fit the batch budget: the bookmark's three, two per mark, two per
// deleted entry. A step that marks anything leaves the entry it marked, so it deletes at most
// the rest of its page; what is left of the budget bounds both as the step goes
// (`OwnedStep::run`, `OwnedStep::delete`).
const _: () =
    assert!(BOOKMARK_OPS + 2 * MARKS_PER_STEP + 2 * (OWNED_PER_STEP - 1) <= MAX_BATCH_OPS as usize);

/// The prefix of the owned walk's bookmarks, one per session the walk stopped inside an entry of
/// ([`owned_walk_key`]). Outside every namespace the walk reads.
pub const OWNED_WALK_PREFIX: &[u8] = b"custodian:owned-walk:";

/// The prefix of the owned walk's scan positions, one per session whose walk has passed entries it
/// holds ([`owned_scan_key`]). Outside every namespace the walk reads.
pub const OWNED_SCAN_PREFIX: &[u8] = b"custodian:owned-scan:";

/// A wait: the session's `sidx:` range is empty and a session-scoped obligation is still owed, so
/// the record stays until the ledger walk has drained it.
const OBLIGATIONS_FIRST: &str = "obligations-first";

/// The owned walk's name in a refused page read.
const WALK: &str = "owned-walk";

/// The audit action naming a bookmark that names no entry of its session's range.
const UNUSABLE_BOOKMARK: &str = "unusable-owned-walk-bookmark";

/// The audit action naming a scan position that names nothing of its session's range.
const UNUSABLE_POSITION: &str = "unusable-owned-walk-position";

/// A wait that no state should produce — a walk step that read and changed nothing and held
/// nothing. Counted rather than taken for progress.
const NO_PROGRESS: &str = "no-progress";

/// Where the owned walk of `upload_id`'s `sidx:` range stands inside one entry (module doc, *The
/// owned walk*): `custodian:owned-walk:<upload-id>`. Its value is the entry's key, then how many of
/// the positions that entry names are marked, as four big-endian bytes.
pub fn owned_walk_key(upload_id: &UploadId) -> Vec<u8> {
    let mut key = OWNED_WALK_PREFIX.to_vec();
    key.extend_from_slice(upload_id.to_string().as_bytes());
    key
}

/// Where the page of the owned walk's next step starts in `upload_id`'s `sidx:` range (module doc,
/// *The owned walk*): `custodian:owned-scan:<upload-id>`. Its value is the key of the last entry
/// the walk passed; absent, the page starts at the head of the range.
pub fn owned_scan_key(upload_id: &UploadId) -> Vec<u8> {
    let mut key = OWNED_SCAN_PREFIX.to_vec();
    key.extend_from_slice(upload_id.to_string().as_bytes());
    key
}

/// The bookmark's value for `marked` positions of the entry at `entry`.
fn encode_bookmark(entry: &[u8], marked: u32) -> Vec<u8> {
    let mut value = entry.to_vec();
    value.extend_from_slice(&marked.to_be_bytes());
    value
}

/// The entry key and count a bookmark `value` holds, if it holds a key at all.
fn decode_bookmark(value: &[u8]) -> Option<(&[u8], u32)> {
    let split = value.len().checked_sub(4).filter(|split| *split > 0)?;
    let (entry, marked) = value.split_at(split);
    Some((entry, u32::from_be_bytes(marked.try_into().ok()?)))
}

// deferred: #660 — seeded Tier-0 DST coverage of the session leg: the owned walk and its two
// records, the terminal delete, and two drainers of one session (the exact-bytes precondition on
// the session record is what serializes them). No DST scenario lands with #1013; the leg's seeded
// interleavings are #660's. The races that matter are forced by wrappers over a real backend in
// `tests/multipart_session_terminal.rs`: two drainers at the terminal delete (leg D, race), a
// ledger moved under it, and a walk commit overtaken by another drainer's terminal delete.
/// One session-leg pass over the sessions `ctx.meta` lists (module doc). Answers
/// [`Reconciled::Blocked`] if it held anything, else [`Reconciled::Changed`] if it committed,
/// else [`Reconciled::Satisfied`] only if it read the whole listing and no session owed anything
/// this pass, else [`Reconciled::Partial`].
pub(super) async fn reconcile(
    ctx: &ReconstructionContext<'_>,
    by_chunk: &ByChunk<'_>,
) -> Result<Reconciled> {
    let mut pass = Pass {
        visits_left: 0,
        steps_left: SESSION_STEPS_PER_PASS,
        changed: false,
        held: false,
        owed: false,
    };
    let mut after: Option<Vec<u8>> = None;
    let mut read_whole_listing = false;
    'listing: loop {
        let (page, next) = checked_page(
            ctx.meta,
            MPU_PREFIX,
            "retire-sessions",
            after.as_deref(),
            SESSION_PAGE,
        )
        .await?;
        for (key, value) in &page {
            if pass.steps_left == 0 {
                // Whatever the rest owes waits for the next pass, which reads from the head.
                break 'listing;
            }
            visit(ctx, by_chunk, &mut pass, key, value.to_vec()).await?;
        }
        match (next, page.into_iter().last()) {
            (Some(_), Some((last, _))) => after = Some(last),
            _ => {
                read_whole_listing = true;
                break;
            }
        }
    }
    Ok(if pass.held {
        Reconciled::Blocked
    } else if pass.changed {
        Reconciled::Changed
    } else if pass.owed || !read_whole_listing {
        Reconciled::Partial
    } else {
        Reconciled::Satisfied
    })
}

/// What the owned walk's step came to.
enum Walked {
    /// The range was observed empty: the terminal delete may be judged.
    Empty,
    /// A step committed, or lost a precondition: read the range again.
    Again,
    /// The visit ends here this pass: something was held, or the steps are spent.
    Stop,
}

/// What a terminal-delete attempt came to.
enum Ended {
    /// The session record moved, or a slot batch committed: judge the session again from these
    /// bytes.
    Again(Vec<u8>),
    /// Done for this pass: deleted, retained, waiting, or held.
    Stop,
}

/// Visit the session at `key`, whose record was listed as `value`: walk its owned range a step
/// at a time while it has steps, and once the range is empty judge its terminal delete.
async fn visit(
    ctx: &ReconstructionContext<'_>,
    by_chunk: &ByChunk<'_>,
    pass: &mut Pass,
    key: &[u8],
    value: Vec<u8>,
) -> Result<()> {
    let upload_id = match parse_mpu_key(key) {
        Ok(upload_id) => upload_id,
        Err(fault) => {
            let detail = format!("the key names no session: {fault}");
            hold(pass, key, &Held::of(Hold::RecordUnreadable, detail));
            return Ok(());
        }
    };
    let mut prior = value;
    loop {
        let session = match decode_session_record(&prior) {
            Ok(session) => session,
            Err(fault) => {
                let detail = format!("the session record will not decode: {fault}");
                hold(pass, key, &Held::of(Hold::Undecodable, detail));
                return Ok(());
            }
        };
        let aborting = match session.state() {
            SessionState::Aborting {} => true,
            SessionState::Completed { .. } => false,
            SessionState::Open {} | SessionState::Completing { .. } => return Ok(()),
        };
        if pass.steps_left == 0 {
            pass.owed = true;
            return Ok(());
        }
        let event = format!("{upload_id}:{}", session.epoch());
        let stored = (key, prior.as_slice());
        match walk_step(ctx, by_chunk, pass, stored, &upload_id, aborting, &event).await? {
            Walked::Empty => {}
            Walked::Again => continue,
            Walked::Stop => return Ok(()),
        }
        match terminal(ctx, pass, key, &upload_id, &session, &prior, aborting).await? {
            Ended::Again(now) => prior = now,
            Ended::Stop => return Ok(()),
        }
    }
}

/// How one owned entry was read: its trusted positions, a chunk to find by id, or a hold.
enum Read {
    /// Every position of a placement the staged rule accepts.
    Trusted(Vec<(DServerId, FragmentId)>),
    /// A record whose positions are found by `chunk` id — with the positions it names itself, if
    /// its value decodes — or, where retirement by chunk id is not this session's, held as `held`.
    ByChunk {
        chunk: ChunkId,
        named: Vec<(DServerId, FragmentId)>,
        held: Held,
    },
    /// Neither: the key names no chunk of this session.
    Held(Held),
}

/// Read the owned entry at `key` of `upload_id`'s range, stored as `value`, through the
/// namespace's one decode entry point ([`decode_owned_entry`]).
fn read_entry(upload_id: &UploadId, key: &[u8], value: &[u8]) -> Read {
    let record = Some(key.to_vec());
    match decode_owned_entry(key, value) {
        Ok((_, chunk, entry)) => {
            let staged = entry.staged();
            // The planned placement in the shape the staged rule is stated over; an owned entry
            // records no logical length, and the rule reads none.
            let planned = ChunkRef {
                id: chunk,
                scheme: staged.scheme(),
                len: 0,
                placement: staged.placement().to_vec(),
            };
            let placed = staged_placement(&planned).map(|fragments| {
                fragments
                    .map(|(index, dserver)| (dserver, FragmentId { chunk, index }))
                    .collect()
            });
            match placed {
                Ok(positions) => Read::Trusted(positions),
                Err(m) => {
                    // Every position the entry names, as a write to it would have been addressed.
                    let named = staged
                        .placement()
                        .iter()
                        .enumerate()
                        .filter_map(|(index, &dserver)| {
                            let index = u16::try_from(index).ok()?;
                            Some((dserver, FragmentId { chunk, index }))
                        })
                        .collect();
                    let detail = format!(
                        "chunk {chunk} names {} D server(s) for a scheme of {} fragment(s)",
                        m.actual, m.expected
                    );
                    let held = Held {
                        reason: Hold::UntrustedPlacement,
                        detail,
                        position: None,
                        record,
                    };
                    Read::ByChunk { chunk, named, held }
                }
            }
        }
        Err(fault) => match parse_sidx_key(key) {
            Ok((owner, _, chunk)) if owner == *upload_id => {
                let held = Held {
                    reason: Hold::RecordUnreadable,
                    detail: format!("the owned entry of chunk {chunk} will not decode: {fault}"),
                    position: None,
                    record,
                };
                Read::ByChunk {
                    chunk,
                    named: Vec::new(),
                    held,
                }
            }
            _ => Read::Held(Held {
                reason: Hold::RecordUnreadable,
                detail: format!(
                    "{fault}; the key names no chunk of this session, so its bytes cannot be \
                     found by chunk id — needs a human (operator repair)"
                ),
                position: None,
                record,
            }),
        },
    }
}

/// What a run of an entry's positions came to ([`OwnedStep::run`]).
struct Run {
    /// How many of them, from the first, the step covered: read as this walk's own mark, or queued
    /// to be marked in its batch.
    covered: usize,
    /// Whether every one covered already carried this walk's mark before this step.
    evidenced: bool,
    /// Whether the run stopped short: a hold on the entry, or the step's budget spent.
    stopped: bool,
}

/// One walk step's budget and what it has queued (module doc, *The owned walk*).
struct OwnedStep<'s> {
    meta: &'s dyn MetadataStore,
    event: &'s str,
    stamp: Vec<u8>,
    reads_left: usize,
    ops_left: usize,
    bytes_left: usize,
    /// The `orphan:` keys this step's batch marks.
    queued: HashSet<Vec<u8>>,
    batch: WriteBatch,
    marked: usize,
    deleted: usize,
    held: Vec<Held>,
    /// The step's reads, operations or bytes are spent: nothing after this is read.
    spent: bool,
}

impl OwnedStep<'_> {
    /// Apply the mark guard to each of `positions`, of the entry at `entry`, in order, until one
    /// stops it: queue a mark where one is owed, and say how many it covered.
    async fn run(&mut self, positions: &[(DServerId, FragmentId)], entry: &[u8]) -> Result<Run> {
        let mut run = Run {
            covered: 0,
            evidenced: true,
            stopped: false,
        };
        for &(dserver, frag) in positions {
            let mark_key = orphan_key(dserver, frag);
            if self.queued.contains(&mark_key) {
                // Another entry of this step marks it: covered once the batch commits, but not
                // evidence yet, so this entry waits for a later step to go.
                run.covered += 1;
                run.evidenced = false;
                continue;
            }
            let cost = 2 * mark_key.len() + MARK_MAX_BYTES + self.stamp.len();
            if self.reads_left == 0 || self.ops_left < 2 || self.bytes_left < cost {
                self.spent = true;
                run.stopped = true;
                return Ok(run);
            }
            self.reads_left -= 1;
            let prior = match read_arm(self.meta, &mark_key, self.event).await? {
                Arm::Own => {
                    run.covered += 1;
                    continue;
                }
                Arm::Absent => None,
                Arm::Other(read) => Some(read),
                Arm::Reclaiming => {
                    let detail = "a `reclaiming` mark over a position an owned entry still names: \
                                  GC never finishes it (the staged class outranks it) and the walk \
                                  never replaces it, so neither moves — needs a human (operator \
                                  repair)"
                        .to_owned();
                    self.held.push(Held {
                        reason: Hold::ReclaimingRetained,
                        detail,
                        position: Some(mark_key),
                        record: Some(entry.to_vec()),
                    });
                    run.stopped = true;
                    return Ok(run);
                }
                Arm::Unreadable(fault) => {
                    self.held.push(Held {
                        reason: Hold::UnreadableMark,
                        detail: format!("the mark is none of the three shapes: {fault}"),
                        position: Some(mark_key),
                        record: Some(entry.to_vec()),
                    });
                    run.stopped = true;
                    return Ok(run);
                }
            };
            self.ops_left -= 2;
            self.bytes_left -= cost;
            let batch = std::mem::take(&mut self.batch);
            self.batch = match prior {
                // Arm (b): replaced only if it still holds the bytes read.
                Some(read) => batch.require(mark_key.clone(), read),
                // Arm (c): written only if the position is still unmarked.
                None => batch.require_absent(mark_key.clone()),
            }
            .put(mark_key.clone(), self.stamp.clone());
            self.queued.insert(mark_key);
            self.marked += 1;
            run.covered += 1;
            run.evidenced = false;
        }
        Ok(run)
    }

    /// Queue the delete of the entry at `key`, under `require(sidx == value)`, if the budget holds
    /// it.
    fn delete(&mut self, key: &[u8], value: &[u8]) -> bool {
        let cost = 2 * key.len() + value.len();
        if self.ops_left < 2 || self.bytes_left < cost {
            return false;
        }
        self.ops_left -= 2;
        self.bytes_left -= cost;
        let batch = std::mem::take(&mut self.batch);
        self.batch = batch
            .require(key.to_vec(), value.to_vec())
            .delete(key.to_vec());
        self.deleted += 1;
        true
    }
}

/// One step of the owned walk over `upload_id`'s `sidx:` range (module doc, *The owned walk*):
/// read the page its scan position names and the bookmark, mark what is unmarked — the entry the
/// bookmark names first — delete what every position of already carries this walk's `event`, move
/// the bookmark, commit; and move the scan position past what the step could only hold.
/// `session` is the session's key and its record as read, `aborting` whether untrusted entries may
/// be retired by chunk id.
async fn walk_step(
    ctx: &ReconstructionContext<'_>,
    by_chunk: &ByChunk<'_>,
    pass: &mut Pass,
    session: (&[u8], &[u8]),
    upload_id: &UploadId,
    aborting: bool,
    event: &str,
) -> Result<Walked> {
    let meta = ctx.meta;
    let (session_key, record) = session;
    let range = sidx_range(upload_id);
    let scan_key = owned_scan_key(upload_id);
    let stored_scan = meta.get(&scan_key).await?.map(|value| value.to_vec());
    // A position, never evidence: one that names nothing of this range is named to the operator
    // and read as the head.
    let mut after = stored_scan.clone().filter(|at| at.starts_with(&range));
    if let (Some(stored), None) = (&stored_scan, &after) {
        emit_unusable(session_key, UNUSABLE_POSITION, stored);
    }
    let mut read = checked_page(meta, &range, WALK, after.as_deref(), OWNED_PER_STEP).await?;
    if after.is_some() && read.0.is_empty() {
        // Nothing is left after the position: the walk is back at the head of the range.
        after = None;
        read = checked_page(meta, &range, WALK, None, OWNED_PER_STEP).await?;
    }
    if stored_scan.is_some() && after.is_none() {
        // A position is stored and the step reads from the head: the store says so too.
        move_scan(meta, session, &scan_key, None).await?;
    }
    let (page, more) = read;
    if page.is_empty() {
        return Ok(Walked::Empty);
    }
    pass.owed = true;
    // The stamp this step's marks carry: the loop's one clock, read now that the step has its
    // page (ADR-0009).
    let stamp = match OrphanMark::structured(ctx.clock.now_millis(), event) {
        Ok(mark) => encode_orphan_mark(&mark).to_vec(),
        Err(fault) => {
            let detail = format!("`{event}` is not an orphan-mark event: {fault}");
            hold(pass, session_key, &Held::of(Hold::Undecodable, detail));
            return Ok(Walked::Stop);
        }
    };
    let bookmark_key = owned_walk_key(upload_id);
    let stored = meta.get(&bookmark_key).await?.map(|value| value.to_vec());
    let resume = stored
        .as_deref()
        .and_then(decode_bookmark)
        .filter(|(entry, _)| entry.starts_with(&range));
    if let (Some(stored), None) = (&stored, resume) {
        emit_unusable(session_key, UNUSABLE_BOOKMARK, stored);
    }
    // The page's keys, in range order, and whether the bookmark names an entry outside it — at or
    // before the scan position, or after the page's last key while more of the range follows.
    let keys: Vec<Vec<u8>> = page.iter().map(|(key, _)| key.clone()).collect();
    let elsewhere = resume.is_some_and(|(named, _)| {
        after.as_deref().is_some_and(|at| named <= at)
            || (more.is_some() && keys.last().is_some_and(|last| named > last.as_slice()))
    });

    let mut entries: Vec<(Vec<u8>, Vec<u8>, Read)> = page
        .into_iter()
        .map(|(key, value)| {
            let read = read_entry(upload_id, &key, &value);
            (key, value.to_vec(), read)
        })
        .collect();
    // The entry the bookmark names is walked first, with the step's whole budget: entries before
    // it in the range — held, or re-read from their first position — never spend the reads it
    // resumes with.
    if let Some((named, _)) = resume {
        if let Some(at) = entries
            .iter()
            .position(|(key, _, _)| key.as_slice() == named)
        {
            entries[..=at].rotate_right(1);
        }
    }
    // One listing for every entry this step retires by chunk id, taken when the walk first reaches
    // such an entry's listed fragments — which only an `Aborting` session's entry on a whole fleet
    // does (the gate below).
    let wanted: BTreeSet<ChunkId> = entries
        .iter()
        .filter_map(|(_, _, read)| match read {
            Read::ByChunk { chunk, .. } => Some(*chunk),
            _ => None,
        })
        .collect();
    let mut listing: Option<std::result::Result<Listed, Held>> = None;

    // The bookmark's precondition, its new value and the session record's precondition a put of
    // it goes under are reserved before any mark.
    let longest = entries.iter().map(|(key, _, _)| key.len()).max();
    let bookmark_bytes = 2 * bookmark_key.len()
        + stored.as_ref().map_or(0, Vec::len)
        + longest.unwrap_or_default()
        + 4
        + session_key.len()
        + record.len();
    let mut step = OwnedStep {
        meta,
        event,
        stamp,
        reads_left: MARKS_PER_STEP,
        ops_left: MAX_BATCH_OPS as usize - BOOKMARK_OPS,
        bytes_left: (MAX_BATCH_BYTES as usize).saturating_sub(bookmark_bytes),
        queued: HashSet::new(),
        batch: WriteBatch::new(),
        marked: 0,
        deleted: 0,
        held: Vec::new(),
        spent: false,
    };
    // Where the bookmark goes, as an entry this step does not delete and how many of the
    // positions it names are marked once the batch commits: the first, in the order walked, that
    // the step walked without a hold — whatever it covered, so the next step walks it first;
    // failing that, the first held one whose count is above zero, so a held entry keeps its place
    // while nothing else moves but never takes it from an entry the walk can still work on.
    let mut advanced: Option<(Vec<u8>, usize)> = None;
    let mut kept: Option<(Vec<u8>, usize)> = None;
    for (key, value, read) in entries {
        if step.spent {
            break;
        }
        // Where this entry resumes: the bookmark's count, if the bookmark names it.
        let named_len = match &read {
            Read::Trusted(positions) => positions.len(),
            Read::ByChunk { named, .. } => named.len(),
            Read::Held(_) => 0,
        };
        let from = match resume {
            Some((entry, marked)) if entry == key.as_slice() && marked as usize <= named_len => {
                marked as usize
            }
            _ => 0,
        };
        let (named, chunk) = match read {
            Read::Trusted(positions) => (positions, None),
            Read::ByChunk { chunk, named, held } => {
                // Not an `Aborting` session, or not a pass that sees the whole fleet: held before
                // anything of it is marked.
                let gate = if aborting {
                    by_chunk.whole(&key)
                } else {
                    Err(held)
                };
                if let Err(held) = gate {
                    step.held.push(held);
                    if from > 0 {
                        kept.get_or_insert((key, from));
                    }
                    continue;
                }
                (named, Some(chunk))
            }
            Read::Held(held) => {
                // Names no position, so the bookmark never counts any of it.
                step.held.push(held);
                continue;
            }
        };
        let held_before = step.held.len();
        let run = step.run(&named[from..], &key).await?;
        let covered = from + run.covered;
        let mut evidenced = run.evidenced;
        let mut walked = !run.stopped;
        if let (true, Some(chunk)) = (walked, chunk) {
            // Every position the entry names is covered: now every other fragment of its chunk
            // the listings find, read whole within this step.
            let found = match &listing {
                Some(found) => found,
                None => listing.insert(by_chunk.listed(&wanted, &key).await),
            };
            match found {
                Ok(listed) => {
                    let named: HashSet<&(DServerId, FragmentId)> = named.iter().collect();
                    let others: Vec<(DServerId, FragmentId)> = listed
                        .get(&chunk)
                        .into_iter()
                        .flatten()
                        .filter(|position| !named.contains(position))
                        .copied()
                        .collect();
                    if others.len() > LISTED_PER_CHUNK {
                        let held = listing_past_step(chunk, others.len(), Some(key.clone()));
                        step.held.push(held);
                        walked = false;
                    } else {
                        let run = step.run(&others, &key).await?;
                        walked = !run.stopped;
                        evidenced &= run.evidenced;
                    }
                }
                Err(partial) => {
                    step.held.push(Held {
                        record: Some(key.clone()),
                        ..partial.clone()
                    });
                    walked = false;
                }
            }
        }
        if walked && evidenced && step.delete(&key, &value) {
            continue;
        }
        if step.held.len() == held_before {
            advanced.get_or_insert((key, covered));
        } else if covered > 0 {
            kept.get_or_insert((key, covered));
        }
    }

    let OwnedStep {
        mut batch,
        marked,
        deleted,
        held,
        ..
    } = step;
    let any_held = !held.is_empty();
    for entry in &held {
        hold(pass, session_key, entry);
    }
    let idle = marked == 0 && deleted == 0;
    // Nothing of this page was marked or deleted, and something of it is held: the next step's
    // page starts past the held entries at its head (module doc, *Past held entries*). A page whose
    // first entry is not held stays.
    let is_held = |key: &&Vec<u8>| held.iter().any(|held| held.record.as_ref() == Some(*key));
    let passed = keys.iter().take_while(is_held).count();
    let passing = idle && passed > 0;

    // The bookmark moves to the entry chosen above, at the count of its named positions marked. One
    // naming an entry outside this page stays as it is unless an entry that was walked without a
    // hold takes it; otherwise it is absent when no entry left has a place to keep. A step that
    // moves on past held entries has only re-read the entry its reads ran out on, and the next
    // step reads that entry at the head of its page: it does not take the bookmark here, so a
    // held entry's count stays.
    let chosen = advanced
        .filter(|_| !passing)
        .or(if elsewhere { None } else { kept });
    let bookmark = match chosen {
        Some((key, covered)) => u32::try_from(covered)
            .ok()
            .map(|covered| encode_bookmark(&key, covered)),
        None if elsewhere => stored.clone(),
        None => None,
    };
    let moved = bookmark != stored;
    if moved {
        batch = match &stored {
            Some(read) => batch.require(bookmark_key.clone(), read.clone()),
            None => batch.require_absent(bookmark_key.clone()),
        };
        batch = match bookmark {
            // Put only while the session record is the one read: a step that commits after
            // another drainer's terminal delete leaves no bookmark behind a session that is gone.
            Some(value) => batch
                .require(session_key.to_vec(), record.to_vec())
                .put(bookmark_key, value),
            None => batch.delete(bookmark_key),
        };
    }

    if passing {
        let last = &keys[passed - 1];
        // With every entry of the page held, a full page says only that more may follow, so one
        // row past it says whether an entry does: if none, the walk is back at the head.
        let follows = passed < keys.len()
            || (more.is_some()
                && !checked_page(meta, &range, WALK, Some(last), 1)
                    .await?
                    .0
                    .is_empty());
        let to = follows.then(|| last.clone());
        if to != after {
            move_scan(meta, session, &scan_key, to).await?;
        }
    }
    if idle && !moved {
        // Nothing to commit: everything left is held, so the visit ends here and the session
        // stays. A step that holds nothing always moves something; one that did not is counted.
        if !any_held {
            tracing::info!(monotonic_counter.retirement_steps_waiting = 1_u64);
            emit_wait(session_key, NO_PROGRESS);
        }
        return Ok(Walked::Stop);
    }
    // A batch that marks or deletes something is one of the pass's steps. One that only moves the
    // bookmark is not: a session whose holds move it every pass would otherwise spend a step every
    // pass, and enough of them would starve every session listed after them (module doc, *What a
    // step costs the pass*).
    if !idle {
        pass.steps_left -= 1;
    }
    match meta.commit(batch).await {
        Ok(CommitOutcome::Committed) => {
            pass.changed = true;
            emit_walk_step(marked, deleted);
        }
        // A mark, an entry, the bookmark or the session record moved between the read and the
        // commit: read again next step, every mark re-read, so one GC moved to `reclaiming`
        // meanwhile is met as one.
        Ok(CommitOutcome::Conflict) => {}
        Err(fault) => {
            let detail = format!("the owned walk's commit failed: {fault}");
            hold(pass, session_key, &Held::of(Hold::CommitFailed, detail));
            return Ok(Walked::Stop);
        }
    }
    // A step that held something ends the visit: the next step would hold it again, and the
    // session cannot end while it stands. So does one that marked and deleted nothing: it is not
    // charged, so the visit takes no second one.
    Ok(if idle || any_held || pass.steps_left == 0 {
        Walked::Stop
    } else {
        Walked::Again
    })
}

/// Judge the terminal delete of `session`, stored at `key` as `prior`, whose owned range this pass
/// has just observed empty (module doc, *The terminal delete*), and commit it when it is due.
async fn terminal(
    ctx: &ReconstructionContext<'_>,
    pass: &mut Pass,
    key: &[u8],
    upload_id: &UploadId,
    session: &SessionRecord,
    prior: &[u8],
    aborting: bool,
) -> Result<Ended> {
    let meta = ctx.meta;
    if !aborting && ctx.tombstone == Tombstone::Retain {
        // A `Completed` session inside its tombstone window: its record answers an identical
        // Complete retry, and nothing is owed until the window passes.
        return Ok(Ended::Stop);
    }
    pass.owed = true;
    for mode in RetireMode::ALL {
        let range = retire_session_range(mode, upload_id);
        let (left, _) = checked_page(meta, &range, "retire-session-obligations", None, 1).await?;
        if !left.is_empty() {
            emit_wait(key, OBLIGATIONS_FIRST);
            return Ok(Ended::Stop);
        }
    }
    let (slots, more) = checked_page(
        meta,
        &slot_range(upload_id),
        "retire-slots",
        None,
        SLOT_PAGE,
    )
    .await?;
    let slot_count = slots.len();
    let mut batch = WriteBatch::new().require(key.to_vec(), prior.to_vec());
    for (slot, _) in slots {
        batch = batch.delete(slot);
    }
    if more.is_some() {
        // More slots than one terminal batch carries: these go first, on their own.
        return settle(meta, pass, key, batch, None).await;
    }
    let nonce = session.segment_nonce();
    let (adopted, _) =
        checked_page(meta, &seg_group_prefix(nonce), "retire-seg-group", None, 1).await?;
    let Some(ledger) = meta.get(MPUCTL_KEY).await? else {
        let fault = "the `mpuctl` admission ledger is absent beside a session to delete";
        return ledger_unusable(meta, pass, key, prior, fault.to_owned()).await;
    };
    let released = match decode_admission_record(&ledger).map(|read| read.release_one()) {
        Ok(Some(released)) => released,
        Ok(None) => {
            let fault = "the `mpuctl` admission ledger counts no session, beside a session to \
                         delete";
            return ledger_unusable(meta, pass, key, prior, fault.to_owned()).await;
        }
        Err(fault) => {
            let fault = format!("the `mpuctl` admission ledger will not decode: {fault}");
            return ledger_unusable(meta, pass, key, prior, fault).await;
        }
    };
    batch = batch
        .require(MPUCTL_KEY.to_vec(), ledger)
        .delete(key.to_vec())
        // The walk's bookmark goes with the entry it names, and its scan position when the walk
        // is back at the head; this removes one that damage, or another drainer, left.
        .delete(owned_walk_key(upload_id))
        .delete(owned_scan_key(upload_id));
    if adopted.is_empty() {
        // The group was never adopted: nothing else will delete its marker.
        batch = batch.delete(seggrp_key(nonce));
    }
    batch = batch.put(MPUCTL_KEY.to_vec(), metadata::encode(&released));
    settle(meta, pass, key, batch, Some(slot_count)).await
}

/// The ledger read beside the session at `key` cannot be released from (`fault`). If the session
/// record still holds `prior` that is damage, and the session is held for a human; if it moved or
/// is gone another drainer got there first — its terminal delete lowered the ledger — and the
/// session is judged again from what is there now.
async fn ledger_unusable(
    meta: &dyn MetadataStore,
    pass: &mut Pass,
    key: &[u8],
    prior: &[u8],
    fault: String,
) -> Result<Ended> {
    match meta.get(key).await? {
        Some(now) if now.as_ref() == prior => {
            let detail = format!("{fault}: the count it would release is unknown — needs a human");
            hold(pass, key, &Held::of(Hold::AdmissionLedger, detail));
            Ok(Ended::Stop)
        }
        Some(now) => Ok(Ended::Again(now.to_vec())),
        None => Ok(Ended::Stop),
    }
}

/// Commit `batch`, preconditioned on the session record at `key` — the terminal delete carrying
/// `terminal` slot deletes, or, when `None`, a slot batch before it — and say what follows.
async fn settle(
    meta: &dyn MetadataStore,
    pass: &mut Pass,
    key: &[u8],
    batch: WriteBatch,
    terminal: Option<usize>,
) -> Result<Ended> {
    pass.steps_left -= 1;
    match meta.commit(batch).await {
        Ok(CommitOutcome::Committed) => {
            pass.changed = true;
            if let Some(slots) = terminal {
                emit_terminal(key, slots);
                return Ok(Ended::Stop);
            }
            match meta.get(key).await? {
                Some(now) if pass.steps_left > 0 => Ok(Ended::Again(now.to_vec())),
                _ => Ok(Ended::Stop),
            }
        }
        // The session record or the ledger moved since it was read. A record gone is another
        // drainer's terminal delete, which released the slot: nothing is left to do. One still
        // present is judged again from its new bytes.
        Ok(CommitOutcome::Conflict) => match meta.get(key).await? {
            Some(now) if pass.steps_left > 0 => Ok(Ended::Again(now.to_vec())),
            _ => Ok(Ended::Stop),
        },
        Err(fault) => {
            let detail = format!("the session's commit failed: {fault}");
            hold(pass, key, &Held::of(Hold::CommitFailed, detail));
            Ok(Ended::Stop)
        }
    }
}

/// Hold `held` on the session at `key`: nothing written for it, the pass marked blocked, the
/// session named on the audit seam with the record and position the hold is about.
fn hold(pass: &mut Pass, key: &[u8], held: &Held) {
    pass.held = true;
    let name = |key: &Option<Vec<u8>>| key.as_deref().map(object_name).unwrap_or_default();
    tracing::warn!(monotonic_counter.retirement_sessions_held = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.retirement.audit",
        action = "retire-session-held",
        key = %object_name(key),
        hold = held.reason.as_str(),
        detail = %held.detail,
        position = %name(&held.position),
        record = %name(&held.record),
        "the retirement drain held a multipart session it cannot act on; what it names is left as \
         it is and the walk goes on to the next session — operator signal",
    );
}

/// Count one committed owned-walk step, the marks it wrote and the entries it deleted.
fn emit_walk_step(marked: usize, deleted: usize) {
    tracing::info!(monotonic_counter.retirement_owned_marks_written = marked as u64);
    tracing::info!(monotonic_counter.retirement_owned_entries_deleted = deleted as u64);
}

/// Count one terminal session delete, and name it.
fn emit_terminal(key: &[u8], slots: usize) {
    tracing::info!(monotonic_counter.retirement_sessions_deleted = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.retirement",
        key = %object_name(key),
        slots,
        "the retirement drain deleted a multipart session and released its admission slot",
    );
}

/// Name a record the owned walk of the session at `key` keeps for itself — its bookmark or its scan
/// position, as `action` says — whose `stored` value names nothing of the session's range: no
/// evidence and no position, so the walk reads from the head of the range and from every entry's
/// first position, and rewrites it.
fn emit_unusable(key: &[u8], action: &'static str, stored: &[u8]) {
    tracing::warn!(
        target: "wyrd.custodian.retirement.audit",
        action,
        key = %object_name(key),
        stored = %object_name(stored),
        "a record the owned walk keeps for itself names nothing of the session's range; it is read \
         as absent and rewritten — operator signal",
    );
}

/// Write where the page of the next owned-walk step of `session` (its key, and its record as read)
/// starts: after `to`, or at the head of the range when `None`, which deletes the position (module
/// doc, *Past held entries*). A commit of its own, outside the step budget. A position is put only
/// while the session record is the one read, so a session another drainer ended meanwhile leaves
/// none behind; a refusal is that drainer's, and nothing is owed for it. Never evidence: whatever
/// lands, the walk only reads from there.
async fn move_scan(
    meta: &dyn MetadataStore,
    session: (&[u8], &[u8]),
    scan_key: &[u8],
    to: Option<Vec<u8>>,
) -> Result<()> {
    let (session_key, record) = session;
    let batch = match to {
        Some(passed) => WriteBatch::new()
            .require(session_key.to_vec(), record.to_vec())
            .put(scan_key.to_vec(), passed),
        None => WriteBatch::new().delete(scan_key.to_vec()),
    };
    meta.commit(batch).await?;
    Ok(())
}

/// Name a session left unchanged this pass with nothing at fault.
fn emit_wait(key: &[u8], reason: &'static str) {
    tracing::debug!(
        target: "wyrd.custodian.retirement",
        key = %object_name(key),
        wait = reason,
        "the retirement drain left a multipart session in place this pass; the next pass returns \
         to it",
    );
}
