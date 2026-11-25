//! The **`retire:bytes:` mode** of the retirement drain (#1062): every fragment position an
//! obligation names is orphan-marked, and only then is the record that named it deleted (proposal
//! 0016 decision 4 rule 2, `0016:1123`: evidence precedes the lifting of protection). The
//! obligation goes with its last record or, for a row whose bytes no record names, with its last
//! mark — never with a position it names left unmarked.
//!
//! # The rows, and what one entry is
//!
//! An obligation has one component that carries its bytes, walked entry by entry in the order its
//! cursor counts them ([`RetireCursor`], whose `item` is the next position inside an entry):
//!
//! | Row | Entry | Its positions | Deleted once they are marked |
//! |---|---|---|---|
//! | `{parts: <set>}`, `{parts: all}` | a part number | its `part:` record's chunks, at their recorded placement | the `part:` and `psum:` records |
//! | `{chunks}` | a chunk of the list | its fragments | — |
//! | flat `{generation}` | a chunk of the list | its fragments | — |
//! | segmented `{generation}` | a segment index | its `seg:` record's chunks, at the placement read **now** (`0016:2432-2440`) | the `seg:` record |
//!
//! A `session` component (`{session}`, `{session, all}`, `{session, parts}`) owes the session's
//! owned `sidx:` residue. Walking that range is #1013's; this mode reads one row of it, and while
//! the range is not empty the obligation is **held** ([`Hold::OwnedResidue`]): it is the only
//! deleter in reach of that residue, so it never goes while the session still owns some.
//!
//! # The mark guard: the `reclaiming` check, then three arms
//!
//! Every position's mark is read through the one codec (`metadata::decode_orphan_mark`), and its
//! event identity is this obligation's token — the one GC's keyed protection looks up
//! (`crate::gc`, `retirement_draining`), so a mark this drain wrote protects its fragment until the
//! obligation is gone:
//!
//! * **(0)** a `reclaiming` mark, whatever event it carries, is **never replaced**
//!   (`metadata.rs:154-168`): GC deletes its key blind after destroying the fragment, so a value
//!   written over it would vanish with it. The step stops there, its cursor at or before the
//!   position. Where no record the drain keeps names the position, GC finishes the reclaim and
//!   deletes the key, and a later step takes arm (c) — the step **waits** ([`AWAITING_GC`]). Where
//!   a `part:` record the drain keeps names it, GC never moves (the staged class outranks the mark,
//!   `gc.rs:654-668`, and its sweep keeps the key, `gc.rs:887-893`): the step **holds**
//!   ([`Hold::ReclaimingRetained`]), audited as needing a human. It never deletes the record on
//!   the strength of that mark: GC would then resume the reclaim with no grace this retirement
//!   gave.
//! * **(a)** this obligation's own event ⇒ skipped, no mutation, its stamp intact.
//! * **(b)** another event, or a legacy mark (no identity) ⇒ re-stamped under
//!   `require(orphan:<pos> == prior)`, with this event and a fresh stamp.
//! * **(c)** absent ⇒ written under `require_absent(orphan:<pos>)`.
//!
//! A commit that loses a precondition re-reads the obligation and plans again, which re-reads
//! every mark: a value GC moved to `reclaiming` meanwhile is arm (0), never a retry of (b). A mark
//! that is none of the three shapes is kept and held on ([`Hold::UnreadableMark`]), as GC keeps it.
//!
//! # Marks before deletes, in two commits
//!
//! A record is deleted only in a step **after** the one whose commit marked its last position:
//! the step deletes an entry's records only when every one of its positions already carried this
//! obligation's mark when the step read it (arm (a)), or lies before the stored cursor, which no
//! step moves past a position it has not marked. So no batch both marks a position and deletes the
//! record naming it, and between the two commits the position carries both. A step marks as far
//! as its budget reaches ([`super::MARKS_PER_STEP`] positions, [`super::ENTRIES_PER_MARKING_STEP`]
//! entries); the cursor stops at the first entry it has not finished, with the count of that
//! entry's positions marked. Marks it wrote past that entry are not in the cursor: the next step
//! reads them again, finds them its own, and deletes the records they cover.
//!
//! Each mark carries the stamp the leg read for this step from the loop's clock
//! ([`super::visit`]), so grace for drained bytes starts at the drain (`0016:1098-1104`).
//!
//! # Placements are validated before marking
//!
//! A `part:` record or a `{chunks}` list carries a staged placement, held to exactly one D server
//! per fragment (`crate::gc::staged_placement`, the rule GC's staged class applies); a generation's
//! chunks are committed ones, empty (the identity fallback) or exactly that length
//! (`ChunkRef::checked_fragments`). An entry whose placement fails is never marked from and never
//! deleted ([`Hold::UntrustedPlacement`]); one whose record is absent or will not decode, neither
//! ([`Hold::RecordUnreadable`]). That includes the entry the stored cursor counts marks inside
//! when a range walk finds its record gone — its page empty, or its first record past it: the
//! record was the only list of the entry's remaining positions, so the walk never takes its
//! absence for the end of the component. The step stops there, and the obligation and its cursor
//! stay: the safe direction, since GC reclaims no fragment it holds no mark for, and its staged
//! class holds every fragment of an untrusted `part:` record's chunk whole. #1013 turns the hold
//! into retirement by chunk id.
//!
//! # Segments before parts
//!
//! For a session token `s:<id>:<E>` whose `retire:records:` obligation still owes `{seg}` (one
//! rolled-back `Completing` attempt's segments), this mode deletes no `part:` record under the same
//! token until that records obligation is gone; it may still write marks. The post-restore check
//! requires every surviving segment's chunks to be held by a surviving part
//! (`crate::restore`, `check_attempt`), so draining the two independently would make an honest
//! half-drained state read as damage. The check is one keyed read; while it holds, a step with
//! nothing else to do **waits** ([`SEGMENTS_FIRST`]).
//!
//! # Bounded
//!
//! A step reads one `sidx:` row, the records obligation, at most
//! [`super::ENTRIES_PER_MARKING_STEP`] records (one keyed read each for a part-number set, one page
//! for `all` or a segment range) and at most [`super::MARKS_PER_STEP`] marks, and its batch carries
//! at most two operations per mark and two per deleted entry beside its fixed two
//! ([`super::READS_PER_MARKING_STEP`]).

use wyrd_core::metadata::{
    self, decode_orphan_mark, encode_orphan_mark, orphan_key, seg_key, seg_range_prefix, ChunkRef,
    OrphanMark, SegmentGroup, SegmentRecord, MAX_SEGMENT_INDEX,
};
use wyrd_core::multipart::{
    decode_part_record, decode_retire_obligation, parse_part_key, part_key, part_range, psum_key,
    retire_key, sidx_range, PartNumber, PartNumberSet, PartScope, RetireComponent, RetireCursor,
    RetireMode, RetirePayload, RetireToken, RetiredMap, UploadId, MAX_PART_NUMBER,
};
use wyrd_traits::{DServerId, FragmentId, MetadataStore, Result};

use super::{
    advanced_value, segment_index, Budget, Held, Hold, Mark, Planned, Step,
    ENTRIES_PER_MARKING_STEP, MARKS_PER_STEP,
};
use crate::gc::{checked_page, object_name, staged_placement};

/// A wait: a `reclaiming` mark over a position no record the drain keeps names. GC finishes the
/// reclaim and deletes the key; the next step marks the position afresh.
pub(super) const AWAITING_GC: &str = "awaiting-gc";

/// A wait: the session token's `retire:records:` obligation still owes its segments, so no
/// `part:` record goes yet.
pub(super) const SEGMENTS_FIRST: &str = "segments-first";

/// A wait that no state should produce — a step that read and changed nothing with no cause to
/// name. Counted rather than taken for progress.
const NO_PROGRESS: &str = "no-progress";

/// The longest value a decodable `orphan:` mark has: the `reclaiming` shape with a twenty-digit
/// stamp and an event of [`metadata::MAX_ORPHAN_EVENT_LEN`] bytes —
/// `{"orphaned_at_millis":` (22) + 20 + `,"event":"` (10) + 256 + `","reclaiming":true}` (20) =
/// 328, the figure the codec's tests pin. What a re-stamp's precondition carries at most.
const MARK_MAX_BYTES: usize = 72 + metadata::MAX_ORPHAN_EVENT_LEN;

/// Plan the next step of the `retire:bytes:` obligation at `key`, whose stored bytes are `prior`,
/// stamping every mark it writes `stamp_millis` (module doc).
pub(super) async fn plan(
    meta: &dyn MetadataStore,
    key: &[u8],
    prior: &[u8],
    token: &RetireToken,
    payload: &RetirePayload,
    stamp_millis: u64,
) -> Result<Planned> {
    let event = token.to_string();
    let stamp = match OrphanMark::structured(stamp_millis, event.clone()) {
        Ok(mark) => encode_orphan_mark(&mark).to_vec(),
        Err(fault) => {
            let detail = format!("its token is not an orphan-mark event: {fault}");
            return Ok(Planned::Held(Held::of(Hold::Undecodable, detail)));
        }
    };
    if payload.session() {
        if let Some(held) = owned_residue(meta, token).await? {
            return Ok(Planned::Held(held));
        }
    }
    let work = if payload.parts().is_some() {
        Some(RetireComponent::Parts)
    } else if !payload.chunks().is_empty() {
        Some(RetireComponent::Chunks)
    } else if payload.generation().is_some() {
        Some(RetireComponent::Generation)
    } else {
        None
    };
    let cursor = payload.cursor().copied();
    if let Some(cursor) = cursor {
        if Some(cursor.component()) != work {
            let detail = format!(
                "a bytes-mode cursor in `{}`, which this build does not drain",
                cursor.component().name()
            );
            return Ok(Planned::Held(Held::of(Hold::CursorRefused, detail)));
        }
    }
    let Some(work) = work else {
        // `{session}` alone, its owned range empty: nothing else is owed.
        return Ok(Planned::Step(Step {
            marks: Vec::new(),
            deletes: Vec::new(),
            value: None,
        }));
    };
    let at = cursor.map(|cursor| (cursor.entry(), cursor.item()));
    let mut walk = Walk::new(meta, event, stamp, &Budget::for_step(key, prior));
    let tail = match (work, token) {
        (RetireComponent::Parts, RetireToken::Session { upload_id, .. }) => {
            walk.may_delete = segments_drained(meta, token).await?;
            match payload.parts() {
                Some(PartScope::Set(set)) => parts_set(&mut walk, upload_id, set, at).await?,
                _ => parts_all(&mut walk, upload_id, at).await?,
            }
        }
        (RetireComponent::Chunks, _) => list(&mut walk, payload.chunks(), Rule::Staged, at).await?,
        (RetireComponent::Generation, _) => match payload.generation().map(|g| g.map()) {
            Some(RetiredMap::Flat(chunks)) => list(&mut walk, chunks, Rule::Committed, at).await?,
            Some(RetiredMap::Segmented(group)) => segments(&mut walk, group, at).await?,
            None => Tail::Ended,
        },
        _ => {
            // Decode ties `parts` to a session token; anything else is no row of the table.
            let detail = "a `parts` component under a generation token".to_owned();
            return Ok(Planned::Held(Held::of(Hold::CursorRefused, detail)));
        }
    };
    Ok(walk.finish(payload, work, at, tail))
}

/// The hold for a session that still owns staged residue: the first key of its `sidx:` range, if
/// the range is not empty. One row is read; the walk that empties the range is #1013's.
async fn owned_residue(meta: &dyn MetadataStore, token: &RetireToken) -> Result<Option<Held>> {
    let RetireToken::Session { upload_id, .. } = token else {
        let detail = "a `session` component under a generation token".to_owned();
        return Ok(Some(Held::of(Hold::CursorRefused, detail)));
    };
    let (left, _) = checked_page(meta, &sidx_range(upload_id), "retire-sidx", None, 1).await?;
    Ok(left.first().map(|(first, _)| Held {
        reason: Hold::OwnedResidue,
        detail: "the session still owns staged residue under its `sidx:` range; the owned walk \
                 that empties it is #1013's, and this obligation stays until it has"
            .to_owned(),
        position: None,
        record: Some(first.clone()),
    }))
}

/// Whether the session token's `retire:records:` obligation owes no segments any more — absent, or
/// owing none. One that will not decode might owe them, so the drain waits on it (the records leg
/// holds it and names it).
async fn segments_drained(meta: &dyn MetadataStore, token: &RetireToken) -> Result<bool> {
    let key = retire_key(RetireMode::Records, token);
    Ok(match meta.get(&key).await? {
        None => true,
        Some(value) => match decode_retire_obligation(&key, &value) {
            Ok((_, _, owed)) => owed.segments().is_none(),
            Err(_) => false,
        },
    })
}

/// What a mark read at a position says the guard must do (module doc, *The mark guard*).
enum Arm {
    /// (c): no mark.
    Absent,
    /// (a): this obligation's own event, not `reclaiming`.
    Own,
    /// (b): another event's mark, or a legacy one: these exact bytes.
    Other(Vec<u8>),
    /// (0): a `reclaiming` mark, whatever event it carries.
    Reclaiming,
    /// None of the three shapes, with the fault.
    Unreadable(String),
}

async fn read_arm(meta: &dyn MetadataStore, key: &[u8], event: &str) -> Result<Arm> {
    let Some(stored) = meta.get(key).await? else {
        return Ok(Arm::Absent);
    };
    Ok(match decode_orphan_mark(&stored) {
        Err(fault) => Arm::Unreadable(fault.to_string()),
        // Arm (0) is checked first, so no event — this obligation's own included — gets past it.
        Ok(mark) if mark.is_reclaiming() => Arm::Reclaiming,
        Ok(mark) if mark.event() == Some(event) => Arm::Own,
        // A legacy mark names no event, so it is never this retirement's (`0016:1204-1209`).
        Ok(_) => Arm::Other(stored.to_vec()),
    })
}

/// One entry of the component the step walks.
struct Entry {
    /// The cursor's `entry` for it.
    index: u32,
    /// The record that names its positions, as the store spells its key; `None` for a chunk of
    /// the obligation's own list.
    record: Option<Vec<u8>>,
    /// What the step deletes once every position carries this obligation's mark.
    deletes: Vec<Vec<u8>>,
    positions: Vec<(DServerId, FragmentId)>,
    /// Whether a record the drain keeps until the entry is marked — a `part:` record, which GC's
    /// staged class protects — names these positions (module doc, arm (0)).
    retained: bool,
}

/// What lies past the entries a step met.
enum Tail {
    /// Nothing: the component ends with them.
    Ended,
    /// The next entry, which the step did not reach.
    At(u32),
}

/// Why a walk stopped inside what it read.
enum Stop {
    /// Reads, operations or bytes are spent.
    Budget,
    /// Arm (0) over a position GC will finish.
    AwaitingGc,
    /// A hold, at an entry the step reached.
    Held(Held),
}

/// One step's walk: what it has read, written and deleted, and where its cursor goes.
struct Walk<'s> {
    meta: &'s dyn MetadataStore,
    event: String,
    stamp: Vec<u8>,
    reads_left: usize,
    ops_left: usize,
    bytes_left: usize,
    marks: Vec<Mark>,
    deletes: Vec<Vec<u8>>,
    /// Whether records may be deleted this step (segments before parts).
    may_delete: bool,
    /// Every entry met so far is finished — its records deleted, or, for a row with none, every
    /// position marked — so the next entry's records may go too.
    finished_so_far: bool,
    /// The first entry the walk met, where a step with no stored cursor begins.
    first_met: Option<u32>,
    /// Where the cursor goes: the first unfinished entry, and how many of its positions are
    /// marked once this step commits. `None` while every entry met is finished.
    resume: Option<(u32, u32)>,
    stop: Option<Stop>,
}

impl<'s> Walk<'s> {
    fn new(meta: &'s dyn MetadataStore, event: String, stamp: Vec<u8>, budget: &Budget) -> Self {
        Self {
            meta,
            event,
            stamp,
            reads_left: MARKS_PER_STEP,
            ops_left: budget.ops,
            bytes_left: budget.bytes,
            marks: Vec::new(),
            deletes: Vec::new(),
            may_delete: true,
            finished_so_far: true,
            first_met: None,
            resume: None,
            stop: None,
        }
    }

    /// Walk one entry's positions from `from`, applying the guard to each, and delete its records
    /// if every position already carried this obligation's mark.
    async fn entry(&mut self, entry: &Entry, from: u32) -> Result<()> {
        self.first_met.get_or_insert(entry.index);
        let total = entry.positions.len();
        let Some(rest) = entry.positions.get(from as usize..) else {
            let detail = format!("the cursor names item {from} of an entry of {total} positions");
            let held = Held {
                reason: Hold::CursorRefused,
                detail,
                position: None,
                record: entry.record.clone(),
            };
            self.held_at(entry.index, from, held);
            return Ok(());
        };
        let mut covered = from as usize;
        // Every position covered carried this obligation's mark before this step: the ones before
        // the cursor, which no step passes unmarked, and each read below that was arm (a).
        let mut evidenced = true;
        for &(dserver, frag) in rest {
            let key = orphan_key(dserver, frag);
            // Reserve the worst case before the read: two operations and a re-stamp's bytes.
            let cost = 2 * key.len() + MARK_MAX_BYTES + self.stamp.len();
            if self.reads_left == 0 || self.ops_left < 2 || self.bytes_left < cost {
                self.stop = Some(Stop::Budget);
                break;
            }
            self.reads_left -= 1;
            match read_arm(self.meta, &key, &self.event).await? {
                Arm::Own => {}
                Arm::Absent => {
                    self.mark(key, None, cost);
                    evidenced = false;
                }
                Arm::Other(read) => {
                    self.mark(key, Some(read), cost);
                    evidenced = false;
                }
                Arm::Reclaiming if entry.retained => {
                    let detail = "a `reclaiming` mark over a position a record this drain keeps \
                                  still names: GC never finishes it (the staged class outranks \
                                  it) and the drain never replaces it, so neither moves — needs \
                                  a human (operator repair)"
                        .to_owned();
                    self.stop = Some(Stop::Held(Held {
                        reason: Hold::ReclaimingRetained,
                        detail,
                        position: Some(key),
                        record: entry.record.clone(),
                    }));
                    break;
                }
                Arm::Reclaiming => {
                    self.stop = Some(Stop::AwaitingGc);
                    break;
                }
                Arm::Unreadable(fault) => {
                    self.stop = Some(Stop::Held(Held {
                        reason: Hold::UnreadableMark,
                        detail: format!("the mark is none of the three shapes: {fault}"),
                        position: Some(key),
                        record: entry.record.clone(),
                    }));
                    break;
                }
            }
            covered += 1;
        }
        let marked = covered == total;
        let finished = marked && (entry.deletes.is_empty() || self.delete(entry, evidenced));
        if !finished {
            self.unfinished(entry.index, covered as u32);
        }
        Ok(())
    }

    /// Queue an arm (b) or (c) mark at `key`, spending its operations and bytes.
    fn mark(&mut self, key: Vec<u8>, prior: Option<Vec<u8>>, cost: usize) {
        self.ops_left -= 2;
        self.bytes_left -= cost;
        let value = self.stamp.clone();
        self.marks.push(Mark { key, prior, value });
    }

    /// Queue `entry`'s record deletes, if it may go now: every position already carried this
    /// obligation's mark (`evidenced`), every entry before it is finished, segments are drained,
    /// and the budget holds them.
    fn delete(&mut self, entry: &Entry, evidenced: bool) -> bool {
        let bytes: usize = entry.deletes.iter().map(Vec::len).sum();
        let may = evidenced
            && self.finished_so_far
            && self.may_delete
            && self.ops_left >= entry.deletes.len()
            && self.bytes_left >= bytes;
        if may {
            self.ops_left -= entry.deletes.len();
            self.bytes_left -= bytes;
            self.deletes.extend(entry.deletes.iter().cloned());
        }
        may
    }

    /// The entry at `index` is not finished, with `marked` of its positions marked: the cursor
    /// stops at the first such entry.
    fn unfinished(&mut self, index: u32, marked: u32) {
        self.finished_so_far = false;
        self.resume.get_or_insert((index, marked));
    }

    /// Stop at the entry at `index` for `held`, the cursor at or before it.
    fn held_at(&mut self, index: u32, from: u32, held: Held) {
        self.first_met.get_or_insert(index);
        self.unfinished(index, from);
        self.stop = Some(Stop::Held(held));
    }

    /// Stop at the stored cursor's own entry and item for the hold [`partial_entry_gone`] built:
    /// the cursor stays where it is, and what lies past it is that entry.
    fn held_at_cursor(&mut self, (entry, item, held): (u32, u32, Held)) -> Tail {
        self.held_at(entry, item, held);
        Tail::At(entry)
    }

    /// The step this walk adds up to: the obligation deleted when every entry is finished and the
    /// component has ended; otherwise its marks and deletes with the cursor moved — or, when it
    /// read and changed nothing, the hold or the wait that stopped it.
    fn finish(
        self,
        payload: &RetirePayload,
        work: RetireComponent,
        at: Option<(u32, u32)>,
        tail: Tail,
    ) -> Planned {
        let resume = match (self.resume, tail) {
            (Some(resume), _) => resume,
            (None, Tail::At(next)) => (next, 0),
            (None, Tail::Ended) => {
                // Every entry finished and none after them: the obligation goes, in the batch
                // with the marks that finished a row with no records.
                return Planned::Step(Step {
                    marks: self.marks,
                    deletes: self.deletes,
                    value: None,
                });
            }
        };
        let began = at.or(self.first_met.map(|first| (first, 0)));
        if self.marks.is_empty() && self.deletes.is_empty() && Some(resume) == began {
            return match self.stop {
                Some(Stop::Held(held)) => Planned::Held(held),
                Some(Stop::AwaitingGc) => Planned::Wait(AWAITING_GC),
                _ if !self.may_delete => Planned::Wait(SEGMENTS_FIRST),
                _ => Planned::Wait(NO_PROGRESS),
            };
        }
        let cursor = RetireCursor::new(work, resume.0, resume.1);
        match advanced_value(payload, RetireMode::Bytes, cursor) {
            Ok(value) => Planned::Step(Step {
                marks: self.marks,
                deletes: self.deletes,
                value: Some(value),
            }),
            Err((reason, detail)) => Planned::Held(Held::of(reason, detail)),
        }
    }
}

/// `{parts: <set>}`: the members from the cursor's, each read by its own key.
async fn parts_set(
    walk: &mut Walk<'_>,
    upload_id: &UploadId,
    set: &PartNumberSet,
    at: Option<(u32, u32)>,
) -> Result<Tail> {
    let first = at.map_or(set.runs()[0].0, |(entry, _)| entry);
    let mut members = set.members_from(first);
    let mut visited = 0;
    loop {
        let Some(n) = members.next() else {
            return Ok(Tail::Ended);
        };
        if walk.stop.is_some() || visited == ENTRIES_PER_MARKING_STEP {
            return Ok(Tail::At(n));
        }
        visited += 1;
        let from = from_for(at, n);
        let key = part_key(upload_id, part_number(n));
        let value = walk.meta.get(&key).await?;
        match load_part(upload_id, n, key, value.as_deref()) {
            Ok(entry) => walk.entry(&entry, from).await?,
            Err(held) => walk.held_at(n, from, held),
        }
    }
}

/// `{parts: all}`: the session's own part range from the cursor's part number, one page of it.
async fn parts_all(
    walk: &mut Walk<'_>,
    upload_id: &UploadId,
    at: Option<(u32, u32)>,
) -> Result<Tail> {
    let first = at.map_or(1, |(entry, _)| entry);
    let after = first
        .checked_sub(1)
        .filter(|n| *n >= 1)
        .map(|n| part_key(upload_id, part_number(n)));
    let range = part_range(upload_id);
    let (page, next) = checked_page(
        walk.meta,
        &range,
        "retire-parts",
        after.as_deref(),
        ENTRIES_PER_MARKING_STEP,
    )
    .await?;
    let mut last = None;
    for (key, value) in page {
        let expected = last.map_or(first, next_part);
        if walk.stop.is_some() {
            return Ok(Tail::At(expected));
        }
        let n = match parse_part_key(&key) {
            Ok((id, n)) if id == *upload_id => n.get(),
            _ => {
                let detail = format!("{} is not a part record of the session", object_name(&key));
                let held = Held {
                    reason: Hold::RecordUnreadable,
                    detail,
                    position: None,
                    record: Some(key),
                };
                walk.held_at(expected, from_for(at, expected), held);
                return Ok(Tail::At(expected));
            }
        };
        if last.is_none() && at.is_some_and(|(entry, _)| n != entry) {
            // The page begins after the part before the cursor's, so a first record past the
            // cursor's part says that part's record is gone.
            if let Some(held) =
                partial_entry_gone(at, |entry| part_key(upload_id, part_number(entry)))
            {
                return Ok(walk.held_at_cursor(held));
            }
        }
        match load_part(upload_id, n, key, Some(value.as_ref())) {
            Ok(entry) => walk.entry(&entry, from_for(at, n)).await?,
            Err(held) => walk.held_at(n, from_for(at, n), held),
        }
        last = Some(n);
    }
    if last.is_none() {
        // An empty page: no record is left from the cursor's part on, that part's included.
        if let Some(held) = partial_entry_gone(at, |entry| part_key(upload_id, part_number(entry)))
        {
            return Ok(walk.held_at_cursor(held));
        }
    }
    Ok(match (next, last) {
        (None, _) => Tail::Ended,
        (Some(_), Some(n)) if n < MAX_PART_NUMBER => Tail::At(next_part(n)),
        (Some(_), Some(_)) => Tail::Ended,
        (Some(_), None) => Tail::At(first),
    })
}

/// The hold for a range walk whose page shows the cursor's entry gone — the page is empty, or its
/// first record lies past that entry — while the stored cursor counts marks inside it. The record
/// was the only list of the entry's remaining positions, so this is never the end of the entry,
/// nor of the component: the obligation would go with positions it names left unmarked, whose
/// fragments GC would then keep for good (it reclaims none it holds no mark for), and with no
/// audit naming them. The step holds at the cursor instead, naming the record `key_of(entry)`
/// spells. `None` when the cursor counts no marks (or there is none): the cursor says no step got
/// inside the entry, so the walk reads on from the first record it finds.
fn partial_entry_gone(
    at: Option<(u32, u32)>,
    key_of: impl FnOnce(u32) -> Vec<u8>,
) -> Option<(u32, u32, Held)> {
    let (entry, item) = at.filter(|&(_, item)| item > 0)?;
    let held = Held {
        reason: Hold::RecordUnreadable,
        detail: format!(
            "the cursor counts {item} marked position(s) of entry {entry}, whose record is gone; \
             nothing is left to name the rest of its positions"
        ),
        position: None,
        record: Some(key_of(entry)),
    };
    Some((entry, item, held))
}

/// The part number after `n` that a cursor can name.
fn next_part(n: u32) -> u32 {
    n.saturating_add(1).min(MAX_PART_NUMBER)
}

/// Part `n` of a decoded set or of the session's own range: a part number by construction.
fn part_number(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part-number set and a parsed part key hold only part numbers")
}

/// The position inside entry `n` the step begins at: the cursor's item for the cursor's entry,
/// the first position for any other.
fn from_for(at: Option<(u32, u32)>, n: u32) -> u32 {
    match at {
        Some((entry, item)) if entry == n => item,
        _ => 0,
    }
}

/// Part `n`'s record at `key`, read as `value`, as an entry: its chunks' positions at the
/// placement it records, each held to the staged rule, and its `part:` and `psum:` records to
/// delete. Absent, undecodable or untrusted, it is a hold.
fn load_part(
    upload_id: &UploadId,
    n: u32,
    key: Vec<u8>,
    value: Option<&[u8]>,
) -> std::result::Result<Entry, Held> {
    let unreadable = |detail: String, key: &Vec<u8>| Held {
        reason: Hold::RecordUnreadable,
        detail,
        position: None,
        record: Some(key.clone()),
    };
    let Some(value) = value else {
        return Err(unreadable(
            format!("part {n} is owed and its record is absent"),
            &key,
        ));
    };
    let record = match decode_part_record(value) {
        Ok(record) => record,
        Err(fault) => return Err(unreadable(fault.to_string(), &key)),
    };
    let mut positions = Vec::new();
    for chunk in record.chunks() {
        match staged_placement(chunk) {
            Ok(fragments) => positions.extend(fragments.map(|(index, dserver)| {
                let frag = FragmentId {
                    chunk: chunk.id,
                    index,
                };
                (dserver, frag)
            })),
            Err(m) => {
                return Err(untrusted(chunk, m.expected, m.actual, Some(key)));
            }
        }
    }
    let psum = psum_key(upload_id, part_number(n));
    Ok(Entry {
        index: n,
        record: Some(key.clone()),
        deletes: vec![key, psum],
        positions,
        retained: true,
    })
}

/// The hold for `chunk`, whose placement names `actual` D servers for a scheme of `expected`
/// fragments, found in `record` (none: the obligation's own list).
fn untrusted(chunk: &ChunkRef, expected: u16, actual: usize, record: Option<Vec<u8>>) -> Held {
    Held {
        reason: Hold::UntrustedPlacement,
        detail: format!(
            "chunk {} names {actual} D server(s) for a scheme of {expected} fragment(s); nothing \
             is marked from it and nothing naming it is deleted",
            chunk.id
        ),
        position: None,
        record,
    }
}

/// Which placement rule a list row's chunks are held to.
#[derive(Clone, Copy)]
enum Rule {
    /// A staged chunk (`{chunks}`): exactly one D server per fragment.
    Staged,
    /// A committed generation's chunk: empty (the identity fallback) or exactly that length.
    Committed,
}

impl Rule {
    fn positions(
        self,
        chunk: &ChunkRef,
    ) -> std::result::Result<Vec<(DServerId, FragmentId)>, metadata::MalformedPlacement> {
        let at = |(index, dserver): (u16, DServerId)| {
            let frag = FragmentId {
                chunk: chunk.id,
                index,
            };
            (dserver, frag)
        };
        Ok(match self {
            Self::Staged => staged_placement(chunk)?.map(at).collect(),
            Self::Committed => chunk.checked_fragments()?.map(at).collect(),
        })
    }
}

/// `{chunks}` and a flat `{generation}`: the obligation's own chunk list from the cursor's index.
/// No record names these positions, so an entry is finished once marked, and the obligation goes
/// with the batch that marks the last.
async fn list(
    walk: &mut Walk<'_>,
    chunks: &[ChunkRef],
    rule: Rule,
    at: Option<(u32, u32)>,
) -> Result<Tail> {
    let first = at.map_or(0, |(entry, _)| entry);
    for (index, chunk) in chunks.iter().enumerate().skip(first as usize) {
        // A list is one value, at most `MAX_VALUE_BYTES`, so its index fits.
        let index = u32::try_from(index).expect("a chunk list's index fits a cursor");
        if walk.stop.is_some() {
            return Ok(Tail::At(index));
        }
        let from = from_for(at, index);
        match rule.positions(chunk) {
            Ok(positions) => {
                let entry = Entry {
                    index,
                    record: None,
                    deletes: Vec::new(),
                    positions,
                    retained: false,
                };
                walk.entry(&entry, from).await?;
            }
            Err(m) => {
                let held = untrusted(chunk, m.expected, m.actual, None);
                walk.held_at(index, from, held);
            }
        }
    }
    Ok(Tail::Ended)
}

/// A segmented `{generation}`: the group's `seg:` records, one page read from the start of the
/// range — the records before the cursor are gone, so the first one is the cursor's, and one below
/// it is a hold. Each record's placement is the one it holds now (`0016:2432-2440`).
async fn segments(
    walk: &mut Walk<'_>,
    group: &SegmentGroup,
    at: Option<(u32, u32)>,
) -> Result<Tail> {
    let prefix = seg_range_prefix(group);
    let (page, next) = checked_page(
        walk.meta,
        &prefix,
        "retire-generation",
        None,
        ENTRIES_PER_MARKING_STEP,
    )
    .await?;
    let start = at.map_or(0, |(entry, _)| entry);
    let mut last: Option<u32> = None;
    for (key, value) in page {
        let expected = last.map_or(start, |index| index.saturating_add(1));
        if walk.stop.is_some() {
            return Ok(Tail::At(expected));
        }
        let Some(index) = segment_index(group, &key) else {
            let detail = format!("{} is not a segment key of the group", object_name(&key));
            let held = Held {
                reason: Hold::MalformedSegmentKey,
                detail,
                position: None,
                record: Some(key),
            };
            walk.held_at(expected, from_for(at, expected), held);
            return Ok(Tail::At(expected));
        };
        if let (None, Some((entry, item))) = (last, at) {
            if index < entry {
                let detail = format!(
                    "the cursor is at segment {entry} and segment {index} is still below it"
                );
                let held = Held {
                    reason: Hold::SegmentCursorPastRecords,
                    detail,
                    position: None,
                    record: Some(key),
                };
                return Ok(walk.held_at_cursor((entry, item, held)));
            }
            if index > entry {
                // The records before the cursor's are gone, so a first record past it says the
                // cursor's own segment record is gone too.
                if let Some(held) = partial_entry_gone(at, segment_key_of(group, &prefix)) {
                    return Ok(walk.held_at_cursor(held));
                }
            }
        }
        let from = from_for(at, index);
        let segment = match metadata::decode::<SegmentRecord>(&value) {
            Ok(segment) => segment,
            Err(fault) => {
                let held = Held {
                    reason: Hold::RecordUnreadable,
                    detail: fault.to_string(),
                    position: None,
                    record: Some(key),
                };
                walk.held_at(index, from, held);
                return Ok(Tail::At(index));
            }
        };
        let mut positions = Vec::new();
        for chunk in segment.chunks() {
            match Rule::Committed.positions(chunk) {
                Ok(found) => positions.extend(found),
                Err(m) => {
                    let held = untrusted(chunk, m.expected, m.actual, Some(key.clone()));
                    walk.held_at(index, from, held);
                    return Ok(Tail::At(index));
                }
            }
        }
        let entry = Entry {
            index,
            record: Some(key.clone()),
            deletes: vec![key],
            positions,
            retained: false,
        };
        walk.entry(&entry, from).await?;
        last = Some(index);
    }
    if last.is_none() {
        // An empty page: no record of the group is left, the cursor's segment's included.
        if let Some(held) = partial_entry_gone(at, segment_key_of(group, &prefix)) {
            return Ok(walk.held_at_cursor(held));
        }
    }
    Ok(match (next, last) {
        (None, _) => Tail::Ended,
        // Unreachable from a segment Complete writes: a step reads at most `MARKS_PER_STEP` marks,
        // and a page of sixteen such segments names thousands of positions, so a step ends inside
        // the page, its cursor on the first segment it did not finish. Reached only by a group of
        // segments small enough to finish sixteen in one step.
        (Some(_), Some(index)) if index < MAX_SEGMENT_INDEX => Tail::At(index + 1),
        (Some(_), Some(_)) => Tail::Ended,
        (Some(_), None) => Tail::At(start),
    })
}

/// How a hold names segment `index`'s record: its key, or, for an index no key spells (which no
/// cursor decode admits carries), the group's range.
fn segment_key_of<'g>(
    group: &'g SegmentGroup,
    prefix: &'g [u8],
) -> impl FnOnce(u32) -> Vec<u8> + 'g {
    move |index| seg_key(group, index).unwrap_or_else(|_| prefix.to_vec())
}
