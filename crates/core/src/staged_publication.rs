//! **Staged publication** of an inode's chunk map (proposal 0016 decision 7(b)–(d),
//! `0016:2342-2401`): the committer that publishes a chunk list as a flat root, or — past
//! [`MAX_MAP_CHUNKS`] — writes it as `seg:<nonce>:<epoch>:<index>` records in bounded
//! batches, verifies every one of them durable, and then flips the inode root in **one**
//! batch.
//!
//! A segmented map exists because a large map cannot fit one metadata value, and for the
//! same reason it cannot be published in one transaction (`0016:2494`). So publication has
//! two phases: the **segment phase** (rows of the batch inventory's *Segment write*,
//! `0016:662`) and the **flip** (*Root flip*, `0016:663`). The flip is the publication
//! instant: the root that names the segments commits only after every one of them was read
//! back durable and byte-identical to the plan.
//!
//! # What the caller supplies, and what it owns
//!
//! The committer knows nothing about multipart sessions. The caller (#658's Complete, its
//! only producer) passes a **fence record** — any key — and, for each batch, a
//! **contribution**: a [`WriteBatch`] of its own preconditions and mutations committed in
//! the same transaction as the committer's rows. For #658 a segment batch's contribution is
//! `require(mpu == Completing@E, segments_written = first_index)` plus the cursor move to
//! `written` (`0016:613-620`), and the flip's is the session's move to `Completed` plus its
//! `retire:` obligations, including `retire:bytes:{generation}` for a superseded
//! generation, which this module never installs. [`planned_segments`] gives the caller the
//! segment count its flip contribution must require the cursor at.
//!
//! # The fence rules, checked before anything is read or written
//!
//! * every contribution **pins** the fence record with exactly one precondition, at a value
//!   ([`PublicationError::Unfenced`]);
//! * no segment batch deletes it ([`PublicationError::FenceDeleted`]);
//! * **the fence is a function of the cursor.** The value a segment batch requires depends
//!   only on its `first_index`, and the value it leaves only on its `written`; so is the
//!   value the flip requires: the one the phase leaves at `segment_count`
//!   ([`PublicationError::FenceOffCursor`]). The committer reads that value at every cursor
//!   position `0..=segment_count` off the hook's one-segment batches, and checks every batch
//!   it commits against it;
//! * the flip **moves** the fence ([`PublicationError::FenceNotMoved`]) — otherwise a
//!   rollback keyed on the value the flip required still holds after the flip and deletes
//!   the segments the live root names;
//! * the fence never returns to a value it already left ([`PublicationError::FenceCycled`]):
//!   `A → B → A` re-arms a rollback keyed on `A`. This is checked over **every** cursor
//!   position, not only the ones this call's split stops at, because a resume, or a run with
//!   another budget, splits elsewhere. So the cycle verdict depends only on the hook, the
//!   plan and the flip contribution — never on `resume_from` or the budgets — and a hook
//!   that is a function of the cursor passes the per-batch check on every split: a
//!   publication whose first attempt passed is never refused on a recovery for a history it
//!   did not have, and a history that would cycle on some split is refused before its first
//!   write. Every committed batch moves the fence from the value at its first cursor
//!   position to the value at its last, so across attempts the fence only ever walks
//!   forward along those values;
//! * no contribution puts, deletes or preconditions a key this committer **owns** — the
//!   target inode, the target dirent, or any key in the group's `seg:<nonce>:<epoch>:`
//!   range ([`PublicationError::OwnedKey`]). A contribution that deleted a segment in the
//!   very batch that publishes the root naming it would make the verify step check the
//!   wrong state.
//!
//! The hook must be a **pure function of its [`SegmentBatchInfo`]**, and the same on every
//! attempt at one epoch: it is asked more often than the phase commits (once per cursor
//! position, and once per candidate batch size while the split is measured).
//!
//! **These checks are necessary, not sufficient.** The guarantee that no committed root
//! names a missing `seg:` record (C-1, `docs/principles.md` §5) also rests on two caller
//! obligations:
//!
//! 1. **A rollback deletes a group's `seg:` records only after it has committed a move of
//!    the fence record off every value a publisher batch requires**, and never authorizes
//!    a delete from a fence value it merely read. The flip requires the fence at the
//!    publication value and moves it, so the flip and that fence move cannot both land.
//!    Reads protect nothing here: [`MetadataStore::scan_page`] gives no snapshot isolation
//!    and only one batch is atomic. The target's rollback is
//!    [`SessionRecord::completing_teardown`](crate::multipart::SessionRecord::completing_teardown)
//!    (`Completing@E → Aborting@E+1` with its `retire:records:{seg}` obligation).
//! 2. **Contributions never touch a committer-owned key.** Enforced: such a contribution is
//!    refused (above).
//!
//! # Errors
//!
//! A refusal is a [`PublicationError`] inside the returned error. A store fault, a lost
//! precondition ([`CommitOutcome::Conflict`]) and a `CommitUnknownResult` are returned as the
//! store gave them, never reinterpreted: the retry loop around a lost flip (`R_publish`,
//! `0016:926-941`) and the re-read after an unknown result are the caller's.
//!
//! **Clock.** This module reads no clock. The root's `modified` is the caller's stamp,
//! ordered after a superseded prior's by [`overwrite_modified`] (#768), which reads none
//! either (ADR-0009).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bytes::Bytes;
use wyrd_traits::{CommitOutcome, MetadataStore, Result, WriteBatch};

use crate::metadata::{
    dirent_key, encode, inode_key, overwrite_modified, parse_seg_key, seg_key, seg_range_prefix,
    ChunkMap, ChunkMapError, ChunkRef, DirentRecord, InodeId, InodeRecord, InodeState, ObjectMeta,
    SegmentGroup, SegmentRecord, SegmentRef, SegmentedMap, UnstampedOverwrite, MAX_ROOT_SEGMENTS,
    MAX_ROOT_VALUE_BYTES, MAX_VALUE_BYTES, SEGMENT_PAGE_LIMIT,
};
use crate::multipart::{MAX_BATCH_BYTES, MAX_BATCH_OPS, MAX_MAP_CHUNKS, MAX_SEG_CHUNKS};

/// The root the flip installs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootTarget<'a> {
    /// The name is unbound: create inode `id` at version 1 and the dirent naming it, both
    /// under `require_absent`, as [`crate::metadata::create_leased`] does.
    Fresh {
        /// The freshly minted inode id.
        id: InodeId,
    },
    /// The name is bound to inode `id`, whose committed record the caller re-read as
    /// `prior`. The flip requires the dirent and `prior`'s exact bytes, and writes version
    /// `prior.version + 1`, as
    /// [`crate::metadata::commit_chunk_map_superseding_leased`] does. Retiring the
    /// superseded generation is the caller's flip contribution (`0016:663`).
    Supersede {
        /// The inode the dirent names.
        id: InodeId,
        /// Its committed record, as the caller read it.
        prior: &'a InodeRecord,
    },
}

impl RootTarget<'_> {
    fn id(&self) -> InodeId {
        match *self {
            Self::Fresh { id } | Self::Supersede { id, .. } => id,
        }
    }
}

/// The cursor positions one segment-write batch moves between: what the caller's fence
/// contribution for that batch must require and what it leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentBatchInfo {
    /// The first segment index the batch writes, which is also how many segments are
    /// durable before it: the cursor value the batch requires.
    pub first_index: u32,
    /// How many segments are durable once the batch commits: the cursor value it leaves.
    pub written: u32,
}

/// One publication: everything [`publish`] needs.
pub struct Publication<'a> {
    /// The directory inode the target's name lives in.
    pub parent: InodeId,
    /// The target's name.
    pub name: &'a str,
    /// Fresh key or supersede.
    pub root: RootTarget<'a>,
    /// The segment group `(nonce, epoch)` the `seg:` records are keyed by. Unused by a flat
    /// publication, whose root names no group.
    pub group: SegmentGroup,
    /// The ordered chunk list to publish.
    pub chunks: &'a [ChunkRef],
    /// The object metadata to stamp (ADR-0047).
    pub meta: &'a ObjectMeta,
    /// The key of the record every batch is fenced on (the caller's; see the module docs).
    pub fence_key: &'a [u8],
    /// The caller's contribution to the segment-write batch described by its argument.
    pub segment_contribution: &'a (dyn Fn(SegmentBatchInfo) -> WriteBatch + Sync),
    /// How many leading segments the caller believes are already durable (a recovered
    /// cursor). A hint for where to start writing, never evidence: the whole range is read
    /// and checked before anything is written and again before the flip.
    pub resume_from: u32,
    /// The byte envelope of one batch. `None` is [`MAX_BATCH_BYTES`]; a larger value is
    /// clamped to it, a smaller one honoured.
    pub byte_budget: Option<u64>,
    /// The operation envelope of one batch. `None` is [`MAX_BATCH_OPS`]; a larger value is
    /// clamped to it, a smaller one honoured.
    pub ops_budget: Option<u32>,
    /// The caller's contribution to the flip: its preconditions and mutations, committed
    /// atomically with the root.
    pub flip_contribution: WriteBatch,
}

/// Which batch a refusal is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The segment-write batch starting at this segment index.
    Segments {
        /// Its first segment index.
        first_index: u32,
    },
    /// The root flip.
    Flip,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Segments { first_index } => {
                write!(f, "the segment batch starting at segment {first_index}")
            }
            Self::Flip => f.write_str("the root flip"),
        }
    }
}

/// A publication [`publish`] refused. Every variant down to [`Self::Unstamped`] is decided
/// from the inputs alone, before the store is read or written; the rest are decided by the
/// range read, before the batch that would have depended on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationError {
    /// The plan needs more segments than one root may name ([`MAX_ROOT_SEGMENTS`]).
    TooManySegments {
        /// Segments the plan needs.
        segments: usize,
    },
    /// One chunk reference encodes too large for any segment value.
    ChunkTooLarge {
        /// Its position in the chunk list.
        position: usize,
        /// Its encoded size.
        bytes: usize,
    },
    /// The chunk list does not form a representable map.
    Shape(ChunkMapError),
    /// `resume_from` is past the plan.
    ResumePastPlan {
        /// The cursor the caller passed.
        resume_from: u32,
        /// Segments the plan has.
        segments: u32,
    },
    /// A contribution does not pin the fence record with exactly one precondition, at a
    /// value.
    Unfenced {
        /// The batch.
        phase: Phase,
    },
    /// A segment-batch contribution deletes the fence record, which every later batch pins.
    FenceDeleted {
        /// The batch.
        phase: Phase,
    },
    /// A contribution's fence values are not the ones the cursor positions it moves between
    /// hold: the hook is not a function of the cursor, or the flip requires a value the
    /// segment phase does not leave.
    FenceOffCursor {
        /// The batch.
        phase: Phase,
    },
    /// The flip contribution pins the fence record without moving it.
    FenceNotMoved,
    /// The fence returns to a value it already left.
    FenceCycled {
        /// The batch that returns to it.
        phase: Phase,
    },
    /// A contribution puts, deletes or preconditions a key the committer owns.
    OwnedKey {
        /// The batch.
        phase: Phase,
        /// The key, lossily rendered.
        key: String,
    },
    /// A contribution mutates one key twice, whose outcome no backend defines.
    DuplicateMutation {
        /// The batch.
        phase: Phase,
        /// The key, lossily rendered.
        key: String,
    },
    /// A put whose value is over [`MAX_VALUE_BYTES`].
    ValueOverCeiling {
        /// The batch.
        phase: Phase,
        /// The key, lossily rendered.
        key: String,
        /// The value's size.
        bytes: usize,
    },
    /// A batch over the byte envelope (for a segment batch: one row plus its contribution).
    BatchOverBytes {
        /// The batch.
        phase: Phase,
        /// Its size.
        bytes: u64,
        /// The envelope.
        budget: u64,
    },
    /// A batch over the operation envelope.
    BatchOverOps {
        /// The batch.
        phase: Phase,
        /// Its operations.
        ops: u64,
        /// The envelope.
        budget: u64,
    },
    /// The prior's version is `u64::MAX`; no next version exists.
    VersionOverflow {
        /// The inode.
        inode: InodeId,
    },
    /// An overwrite with no `modified` over a stamped prior (#768).
    Unstamped(UnstampedOverwrite),
    /// A key under the group's range that is not one of its segment keys.
    ForeignRow {
        /// The key, lossily rendered.
        key: String,
    },
    /// A durable segment record past the end of the plan, which the resolver would refuse.
    StrandedRow {
        /// Its index.
        index: u32,
    },
    /// A segment record that must be durable is absent.
    SegmentNotDurable {
        /// Its index.
        index: u32,
    },
    /// A durable segment record whose bytes differ from the plan's.
    SegmentMismatch {
        /// Its index.
        index: u32,
    },
}

impl fmt::Display for PublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManySegments { segments } => write!(
                f,
                "the map needs {segments} segments, over the {MAX_ROOT_SEGMENTS} one root may name"
            ),
            Self::ChunkTooLarge { position, bytes } => write!(
                f,
                "chunk {position} encodes to {bytes} bytes, too large for a {SEGMENT_VALUE_BUDGET}-byte segment value"
            ),
            Self::Shape(err) => write!(f, "the chunk list is not a publishable map: {err}"),
            Self::ResumePastPlan {
                resume_from,
                segments,
            } => write!(
                f,
                "resume_from {resume_from} is past the plan's {segments} segments"
            ),
            Self::Unfenced { phase } => write!(
                f,
                "{phase}: the contribution does not pin the fence record at exactly one value"
            ),
            Self::FenceDeleted { phase } => {
                write!(f, "{phase}: the contribution deletes the fence record")
            }
            Self::FenceOffCursor { phase } => write!(
                f,
                "{phase}: the contribution's fence values are not the ones its cursor positions hold"
            ),
            Self::FenceNotMoved => {
                f.write_str("the flip contribution pins the fence record without moving it")
            }
            Self::FenceCycled { phase } => write!(
                f,
                "{phase}: the fence record returns to a value it already left"
            ),
            Self::OwnedKey { phase, key } => write!(
                f,
                "{phase}: the contribution touches {key:?}, which the committer owns"
            ),
            Self::DuplicateMutation { phase, key } => {
                write!(f, "{phase}: the contribution mutates {key:?} twice")
            }
            Self::ValueOverCeiling { phase, key, bytes } => write!(
                f,
                "{phase}: {key:?} is {bytes} bytes, over the {MAX_VALUE_BYTES}-byte value ceiling"
            ),
            Self::BatchOverBytes {
                phase,
                bytes,
                budget,
            } => write!(
                f,
                "{phase} is {bytes} bytes, over the {budget}-byte batch envelope"
            ),
            Self::BatchOverOps { phase, ops, budget } => write!(
                f,
                "{phase} is {ops} operations, over the {budget}-operation batch envelope"
            ),
            Self::VersionOverflow { inode } => {
                write!(f, "inode {inode} is at the last version; no next one exists")
            }
            Self::Unstamped(err) => err.fmt(f),
            Self::ForeignRow { key } => {
                write!(f, "{key:?} is in the group's range but is not one of its segment keys")
            }
            Self::StrandedRow { index } => write!(
                f,
                "segment {index} is durable past the end of the plan: a root over it would not resolve"
            ),
            Self::SegmentNotDurable { index } => write!(f, "segment {index} is not durable"),
            Self::SegmentMismatch { index } => {
                write!(f, "segment {index} is durable with bytes the plan does not write")
            }
        }
    }
}

impl std::error::Error for PublicationError {}

/// The bytes one segment value may encode to: `V/2` (`0016:1467`), the one-value rule
/// [`MAX_SEG_CHUNKS`] is derived from, so the other half is margin, never needed.
const SEGMENT_VALUE_BUDGET: usize = MAX_ROOT_VALUE_BYTES;

/// A segment record's encoding around its chunk array at the widest `u64` renderings:
/// `{"chunks":[` `],"byte_offset":` 20 digits `,"byte_len":` 20 digits `}`.
const SEGMENT_ENVELOPE_BYTES: usize = 80;

/// How many `seg:` records [`publish`] writes for `chunks`: 0 for a flat map (at most
/// [`MAX_MAP_CHUNKS`] chunks). It is the cursor value the last segment batch leaves, so it is
/// what the caller's flip contribution must require.
///
/// Usually `⌈chunks / MAX_SEG_CHUNKS⌉` (`0016:2345-2347`), but not always: a segment is also
/// closed early once its encoding would pass the `V/2` value budget, so chunk refs wider than
/// the sizing scheme's [`crate::multipart::MAX_CHUNKREF_BYTES`] (more fragments, or wider
/// placement ids) give more segments. Callers ask this function; they never re-derive it.
pub fn planned_segments(chunks: &[ChunkRef]) -> std::result::Result<u32, PublicationError> {
    if chunks.len() <= MAX_MAP_CHUNKS as usize {
        return Ok(0);
    }
    // At most `MAX_ROOT_SEGMENTS` spans, so the count fits.
    Ok(split_segments(chunks)?.len() as u32)
}

/// Publish `publication`. Returns `Committed` when the root flipped, and `Conflict` when a
/// segment batch or the flip lost a precondition (nothing after it was committed).
///
/// The order is the protocol:
///
/// 1. every refusal decidable from the inputs ([`PublicationError`] down to
///    [`PublicationError::Unstamped`]) — so such a refusal leaves zero commits;
/// 2. read the group's whole range (at most [`MAX_ROOT_SEGMENTS`] records, paged): a row
///    below `resume_from` must be durable and identical, no row may differ from the plan,
///    and none may lie past it;
/// 3. the segment batches, in order, each creating its rows under `require_absent` or —
///    for a row the read found durable and identical — pinning it with `require` rather
///    than rewriting it, so a recovery at the same epoch never double-writes
///    (`0016:2392-2401`);
/// 4. read the whole range again and require **every** planned record durable and
///    identical — never only `0..resume_from`;
/// 5. the flip: the root CAS, the dirent pin and the caller's whole flip contribution in
///    one batch.
///
/// A flat publication (at most [`MAX_MAP_CHUNKS`] chunks) is step 5 alone.
pub async fn publish(
    store: &impl MetadataStore,
    publication: &Publication<'_>,
) -> Result<CommitOutcome> {
    let prepared = prepare(publication)?;
    if !prepared.rows.is_empty() {
        let count = prepared.rows.len();
        let resume = publication.resume_from as usize;
        let durable = read_group(store, &publication.group, count).await?;
        let witnessed = check_durable(&prepared.rows, &durable, resume)?;
        for batch in &prepared.batches {
            let assembled = assemble_segments(&prepared.rows, batch, &witnessed);
            if store.commit(assembled).await? == CommitOutcome::Conflict {
                return Ok(CommitOutcome::Conflict);
            }
        }
        let durable = read_group(store, &publication.group, count).await?;
        check_durable(&prepared.rows, &durable, count)?;
    }
    store.commit(prepared.flip).await
}

/// One planned segment record.
struct Row {
    key: Vec<u8>,
    value: Bytes,
}

/// One planned segment-write batch: rows `start..end` plus the caller's contribution.
struct SegmentBatch {
    start: usize,
    end: usize,
    contribution: WriteBatch,
}

/// Everything [`publish`] commits, assembled and checked before any I/O.
struct Prepared {
    rows: Vec<Row>,
    batches: Vec<SegmentBatch>,
    flip: WriteBatch,
}

/// Every refusal decidable from the inputs, and the batches they were decided on.
fn prepare(p: &Publication<'_>) -> std::result::Result<Prepared, PublicationError> {
    let budget_bytes = p
        .byte_budget
        .unwrap_or(MAX_BATCH_BYTES)
        .min(MAX_BATCH_BYTES);
    let budget_ops = u64::from(p.ops_budget.unwrap_or(MAX_BATCH_OPS).min(MAX_BATCH_OPS));
    let (chunk_map, size, rows) = plan(&p.group, p.chunks)?;
    let segments = rows.len() as u32;
    if p.resume_from > segments {
        return Err(PublicationError::ResumePastPlan {
            resume_from: p.resume_from,
            segments,
        });
    }
    let owned = Owned {
        inode: inode_key(p.root.id()),
        dirent: dirent_key(p.parent, p.name),
        segments: seg_range_prefix(&p.group),
    };
    let track = fence_track(p, segments, &owned)?;
    check_track(&track)?;
    let batches = split_batches(p, &rows, budget_bytes, budget_ops)?;
    for batch in &batches {
        let phase = Phase::Segments {
            first_index: batch.start as u32,
        };
        let (pin, left) = segment_step(&batch.contribution, phase, &owned, p.fence_key)?;
        if *pin != track[batch.start] || *left != track[batch.end] {
            return Err(PublicationError::FenceOffCursor { phase });
        }
    }
    check_flip_fence(&p.flip_contribution, &owned, p.fence_key, &track)?;
    let root = root_record(p, chunk_map, size)?;
    let flip = flip_batch(p, &root, budget_bytes, budget_ops)?;
    Ok(Prepared {
        rows,
        batches,
        flip,
    })
}

/// The root's chunk map, the object's size, and the segment records (none for a flat map).
fn plan(
    group: &SegmentGroup,
    chunks: &[ChunkRef],
) -> std::result::Result<(ChunkMap, u64, Vec<Row>), PublicationError> {
    if chunks.len() <= MAX_MAP_CHUNKS as usize {
        let size = chunks
            .iter()
            .try_fold(0u64, |total, chunk| total.checked_add(chunk.len))
            .ok_or(PublicationError::Shape(
                ChunkMapError::SegmentLengthOverflow {
                    chunks: chunks.len(),
                },
            ))?;
        return Ok((ChunkMap::Flat(chunks.to_vec()), size, Vec::new()));
    }
    let spans = split_segments(chunks)?;
    let mut refs = Vec::with_capacity(spans.len());
    let mut rows = Vec::with_capacity(spans.len());
    let mut offset = 0u64;
    for (index, span) in spans.into_iter().enumerate() {
        // At most `MAX_ROOT_SEGMENTS` spans, so the index fits.
        let index = index as u32;
        let record = SegmentRecord::new(span.to_vec(), offset).map_err(PublicationError::Shape)?;
        refs.push(SegmentRef {
            index,
            byte_offset: offset,
            byte_len: record.byte_len(),
        });
        rows.push(Row {
            key: seg_key(group, index).map_err(PublicationError::Shape)?,
            value: encode(&record),
        });
        offset = offset
            .checked_add(record.byte_len())
            .ok_or(PublicationError::Shape(
                ChunkMapError::SegmentSpanOverflow { index },
            ))?;
    }
    let map = SegmentedMap::new(group.clone(), refs).map_err(PublicationError::Shape)?;
    // The size is the table's own span, so the root meets its decode invariant by
    // construction.
    let size = map.span();
    Ok((ChunkMap::Segmented(map), size, rows))
}

/// Split the chunk list into segments by **encoded bytes** against [`SEGMENT_VALUE_BUDGET`]
/// and by count against [`MAX_SEG_CHUNKS`], whichever binds first: a segment closes at
/// `MAX_SEG_CHUNKS` refs, or sooner when wider refs would overrun the value. So the plan has
/// `⌈chunks / MAX_SEG_CHUNKS⌉` segments only while every ref is within the sizing scheme's
/// width; [`planned_segments`] is the count either way.
fn split_segments(chunks: &[ChunkRef]) -> std::result::Result<Vec<&[ChunkRef]>, PublicationError> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    let mut bytes = SEGMENT_ENVELOPE_BYTES;
    for (position, chunk) in chunks.iter().enumerate() {
        let encoded = encode(chunk).len();
        // A separating comma per chunk (one too many, so the estimate is an upper bound).
        let cost = encoded + 1;
        if SEGMENT_ENVELOPE_BYTES + cost > SEGMENT_VALUE_BUDGET {
            return Err(PublicationError::ChunkTooLarge {
                position,
                bytes: encoded,
            });
        }
        let held = position - start;
        if held > 0 && (held == MAX_SEG_CHUNKS as usize || bytes + cost > SEGMENT_VALUE_BUDGET) {
            spans.push(&chunks[start..position]);
            start = position;
            bytes = SEGMENT_ENVELOPE_BYTES;
        }
        bytes += cost;
    }
    if start < chunks.len() {
        spans.push(&chunks[start..]);
    }
    if spans.len() > MAX_ROOT_SEGMENTS {
        return Err(PublicationError::TooManySegments {
            segments: spans.len(),
        });
    }
    Ok(spans)
}

/// What one row costs a batch in its **unwitnessed** shape — `require_absent(key)` plus
/// `put(key, value)` — which is the larger of its two shapes (a witnessed row is
/// `require(key, value)` alone), so the split never depends on what the store held.
fn row_bytes(row: &Row) -> u64 {
    (2 * row.key.len() + row.value.len()) as u64
}

/// Operations one row costs a batch: its precondition and, unwitnessed, its put.
const ROW_OPS: u64 = 2;

/// Split rows `resume_from..` into batches, each as many rows as fit the envelope **with
/// that batch's own contribution** — measured in encoded bytes and in operations, never by
/// a fixed row count. A batch that cannot fit even one row beside its contribution is
/// refused.
fn split_batches(
    p: &Publication<'_>,
    rows: &[Row],
    budget_bytes: u64,
    budget_ops: u64,
) -> std::result::Result<Vec<SegmentBatch>, PublicationError> {
    let mut batches = Vec::new();
    let mut start = p.resume_from as usize;
    while start < rows.len() {
        // Rows `start..end` costing `rows_bytes`, measured with the contribution for exactly
        // that batch: its total bytes, its total operations, and the contribution itself.
        let measure = |end: usize, rows_bytes: u64| {
            let contribution = (p.segment_contribution)(SegmentBatchInfo {
                first_index: start as u32,
                written: end as u32,
            });
            let bytes = rows_bytes + batch_bytes(&contribution);
            let ops = ROW_OPS * (end - start) as u64 + batch_ops(&contribution);
            (bytes, ops, contribution)
        };
        let phase = Phase::Segments {
            first_index: start as u32,
        };
        let mut rows_bytes = row_bytes(&rows[start]);
        let (bytes, ops, contribution) = measure(start + 1, rows_bytes);
        if bytes > budget_bytes {
            return Err(PublicationError::BatchOverBytes {
                phase,
                bytes,
                budget: budget_bytes,
            });
        }
        if ops > budget_ops {
            return Err(PublicationError::BatchOverOps {
                phase,
                ops,
                budget: budget_ops,
            });
        }
        let (mut end, mut contribution) = (start + 1, contribution);
        for next in start + 2..=rows.len() {
            let candidate = rows_bytes + row_bytes(&rows[next - 1]);
            let (bytes, ops, grown) = measure(next, candidate);
            if bytes > budget_bytes || ops > budget_ops {
                break;
            }
            (rows_bytes, end, contribution) = (candidate, next, grown);
        }
        batches.push(SegmentBatch {
            start,
            end,
            contribution,
        });
        start = end;
    }
    Ok(batches)
}

/// The bytes a batch carries: every precondition's key and expected value, every put's key
/// and value, every delete's key — the whole transaction, not just the caller's idea of its
/// payload.
fn batch_bytes(batch: &WriteBatch) -> u64 {
    let preconditions: usize = batch
        .preconditions
        .iter()
        .map(|p| p.key.len() + p.expected.as_ref().map_or(0, Bytes::len))
        .sum();
    let puts: usize = batch.puts.iter().map(|(k, v)| k.len() + v.len()).sum();
    let deletes: usize = batch.deletes.iter().map(Vec::len).sum();
    (preconditions + puts + deletes) as u64
}

/// The operations a batch carries: every precondition read and every mutation, each a
/// sequential round trip inside the transaction (`0016:630-648`).
fn batch_ops(batch: &WriteBatch) -> u64 {
    (batch.preconditions.len() + batch.puts.len() + batch.deletes.len()) as u64
}

/// The keys the committer writes itself.
struct Owned {
    inode: Vec<u8>,
    dirent: Vec<u8>,
    segments: Vec<u8>,
}

impl Owned {
    fn owns(&self, key: &[u8]) -> bool {
        key == self.inode.as_slice()
            || key == self.dirent.as_slice()
            || key.starts_with(&self.segments)
    }
}

/// The rules every contribution meets: no owned key in any of its three lists, no key
/// mutated twice, no value over the ceiling. Returns the one value it pins the fence at.
fn check_contribution<'a>(
    contribution: &'a WriteBatch,
    phase: Phase,
    owned: &Owned,
    fence_key: &[u8],
) -> std::result::Result<&'a Bytes, PublicationError> {
    let read = contribution.preconditions.iter().map(|p| p.key.as_slice());
    let mut mutated = contribution
        .puts
        .iter()
        .map(|(k, _)| k.as_slice())
        .chain(contribution.deletes.iter().map(Vec::as_slice));
    if let Some(key) = read.chain(mutated.clone()).find(|k| owned.owns(k)) {
        return Err(PublicationError::OwnedKey {
            phase,
            key: rendered(key),
        });
    }
    let mut seen = BTreeSet::new();
    if let Some(key) = mutated.find(|k| !seen.insert(*k)) {
        return Err(PublicationError::DuplicateMutation {
            phase,
            key: rendered(key),
        });
    }
    if let Some((key, value)) = contribution
        .puts
        .iter()
        .find(|(_, v)| v.len() > MAX_VALUE_BYTES)
    {
        return Err(PublicationError::ValueOverCeiling {
            phase,
            key: rendered(key),
            bytes: value.len(),
        });
    }
    let mut pins = contribution
        .preconditions
        .iter()
        .filter(|p| p.key == fence_key);
    match (pins.next().and_then(|p| p.expected.as_ref()), pins.next()) {
        (Some(pin), None) => Ok(pin),
        _ => Err(PublicationError::Unfenced { phase }),
    }
}

/// A segment contribution checked, and the fence values it moves between: the one it
/// requires, and the one it leaves (its put, or the required value when it writes none).
fn segment_step<'a>(
    contribution: &'a WriteBatch,
    phase: Phase,
    owned: &Owned,
    fence_key: &[u8],
) -> std::result::Result<(&'a Bytes, &'a Bytes), PublicationError> {
    let pin = check_contribution(contribution, phase, owned, fence_key)?;
    if contribution.deletes.iter().any(|k| k == fence_key) {
        return Err(PublicationError::FenceDeleted { phase });
    }
    // One put at most: `check_contribution` refused two.
    let left = fence_put(contribution, fence_key).unwrap_or(pin);
    Ok((pin, left))
}

/// The fence value at every cursor position `0..=segments` — `track[k]` is what the batch
/// starting at segment `k` requires, `track[k + 1]` what the batch ending there leaves —
/// read off the hook's one-segment batches, each checked like a committed one. Empty for a
/// flat publication. One batch leaving a value the next does not require is a hook that is
/// not a function of the cursor.
fn fence_track(
    p: &Publication<'_>,
    segments: u32,
    owned: &Owned,
) -> std::result::Result<Vec<Bytes>, PublicationError> {
    let mut track: Vec<Bytes> = Vec::with_capacity(segments as usize + 1);
    for first_index in 0..segments {
        let phase = Phase::Segments { first_index };
        let probe = (p.segment_contribution)(SegmentBatchInfo {
            first_index,
            written: first_index + 1,
        });
        let (pin, left) = segment_step(&probe, phase, owned, p.fence_key)?;
        match track.last() {
            None => track.push(pin.clone()),
            Some(held) if held != pin => return Err(PublicationError::FenceOffCursor { phase }),
            Some(_) => {}
        }
        track.push(left.clone());
    }
    Ok(track)
}

/// Refuse a track that returns to a value it already left. A repeat of the value just held
/// is the same state (a batch that leaves the fence where it was); any other repeat is a
/// cycle, and is one on some split, so it is refused whatever split this call uses.
fn check_track(track: &[Bytes]) -> std::result::Result<(), PublicationError> {
    let mut left: BTreeSet<&Bytes> = BTreeSet::new();
    for (first_index, pair) in track.windows(2).enumerate() {
        if pair[1] == pair[0] {
            continue;
        }
        left.insert(&pair[0]);
        if left.contains(&pair[1]) {
            return Err(PublicationError::FenceCycled {
                phase: Phase::Segments {
                    first_index: first_index as u32,
                },
            });
        }
    }
    Ok(())
}

/// The flip contribution checked, and its fence rules: it requires the value the segment
/// phase leaves (any value, for a flat publication), and moves the fence — deletes it, or
/// puts a value the fence never held at any cursor position.
fn check_flip_fence(
    flip: &WriteBatch,
    owned: &Owned,
    fence_key: &[u8],
    track: &[Bytes],
) -> std::result::Result<(), PublicationError> {
    let pin = check_contribution(flip, Phase::Flip, owned, fence_key)?;
    if track.last().is_some_and(|end| end != pin) {
        return Err(PublicationError::FenceOffCursor { phase: Phase::Flip });
    }
    if flip.deletes.iter().any(|k| k == fence_key) {
        return Ok(());
    }
    match fence_put(flip, fence_key) {
        Some(value) if value != pin && track.contains(value) => {
            Err(PublicationError::FenceCycled { phase: Phase::Flip })
        }
        Some(value) if value != pin => Ok(()),
        _ => Err(PublicationError::FenceNotMoved),
    }
}

/// The value a batch writes to the fence record, if it writes one.
fn fence_put<'a>(batch: &'a WriteBatch, fence_key: &[u8]) -> Option<&'a Bytes> {
    batch
        .puts
        .iter()
        .find(|(k, _)| k == fence_key)
        .map(|(_, v)| v)
}

/// The root the flip installs: version 1 or `prior.version + 1`, the caller's object
/// metadata, and `modified` ordered after the prior's by [`overwrite_modified`].
fn root_record(
    p: &Publication<'_>,
    chunk_map: ChunkMap,
    size: u64,
) -> std::result::Result<InodeRecord, PublicationError> {
    let (version, prior_modified) = match p.root {
        RootTarget::Fresh { .. } => (1, None),
        RootTarget::Supersede { id, prior } => (
            prior
                .version
                .checked_add(1)
                .ok_or(PublicationError::VersionOverflow { inode: id })?,
            prior.modified,
        ),
    };
    let modified = overwrite_modified(p.root.id(), prior_modified, p.meta.modified)
        .map_err(PublicationError::Unstamped)?;
    Ok(InodeRecord {
        size,
        chunk_map,
        state: InodeState::Committed,
        version,
        etag: p.meta.etag.clone(),
        content_type: p.meta.content_type.clone(),
        modified,
    })
}

/// The flip: the dirent and inode pinned on both branches (`0016:663`), the root put, and
/// the caller's whole contribution — one batch, checked against both envelopes.
fn flip_batch(
    p: &Publication<'_>,
    root: &InodeRecord,
    budget_bytes: u64,
    budget_ops: u64,
) -> std::result::Result<WriteBatch, PublicationError> {
    let id = p.root.id();
    let inode = inode_key(id);
    let dirent = dirent_key(p.parent, p.name);
    let encoded = encode(root);
    if encoded.len() > MAX_VALUE_BYTES {
        return Err(PublicationError::ValueOverCeiling {
            phase: Phase::Flip,
            key: rendered(&inode),
            bytes: encoded.len(),
        });
    }
    let mut flip = match p.root {
        RootTarget::Fresh { .. } => WriteBatch::new()
            .require_absent(inode.clone())
            .require_absent(dirent.clone())
            .put(inode, encoded)
            .put(dirent, encode(&DirentRecord { inode: id })),
        RootTarget::Supersede { prior, .. } => WriteBatch::new()
            .require(dirent, encode(&DirentRecord { inode: id }))
            .require(inode.clone(), encode(prior))
            .put(inode, encoded),
    };
    merge(&mut flip, &p.flip_contribution);
    let bytes = batch_bytes(&flip);
    if bytes > budget_bytes {
        return Err(PublicationError::BatchOverBytes {
            phase: Phase::Flip,
            bytes,
            budget: budget_bytes,
        });
    }
    let ops = batch_ops(&flip);
    if ops > budget_ops {
        return Err(PublicationError::BatchOverOps {
            phase: Phase::Flip,
            ops,
            budget: budget_ops,
        });
    }
    Ok(flip)
}

/// Append a contribution's preconditions and mutations to `batch`.
fn merge(batch: &mut WriteBatch, contribution: &WriteBatch) {
    batch
        .preconditions
        .extend(contribution.preconditions.iter().cloned());
    batch.puts.extend(contribution.puts.iter().cloned());
    batch.deletes.extend(contribution.deletes.iter().cloned());
}

/// One segment batch as it commits: each row created under `require_absent`, or pinned
/// with `require` when the range read found it durable and identical, plus the caller's
/// contribution.
fn assemble_segments(
    rows: &[Row],
    batch: &SegmentBatch,
    witnessed: &BTreeSet<usize>,
) -> WriteBatch {
    let mut assembled = WriteBatch::new();
    for (index, row) in rows.iter().enumerate().take(batch.end).skip(batch.start) {
        assembled = if witnessed.contains(&index) {
            assembled.require(row.key.clone(), row.value.clone())
        } else {
            assembled
                .require_absent(row.key.clone())
                .put(row.key.clone(), row.value.clone())
        };
    }
    merge(&mut assembled, &batch.contribution);
    assembled
}

/// Read the group's whole `seg:<nonce>:<epoch>:` range in [`SEGMENT_PAGE_LIMIT`] pages,
/// keyed by parsed index. Refuses a key that is not one of the group's segment keys and a
/// row at or past `planned`, so at most `planned` rows are kept.
async fn read_group(
    store: &impl MetadataStore,
    group: &SegmentGroup,
    planned: usize,
) -> Result<BTreeMap<u32, Bytes>> {
    let prefix = seg_range_prefix(group);
    let mut rows = BTreeMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = store
            .scan_page(&prefix, after.as_deref(), SEGMENT_PAGE_LIMIT)
            .await?;
        for (key, value) in page {
            // A key under the group's own prefix that parses names that nonce and epoch:
            // the prefix spells both and ends in `:`.
            let Ok((_, _, index)) = parse_seg_key(&key) else {
                return Err(PublicationError::ForeignRow {
                    key: rendered(&key),
                }
                .into());
            };
            if index as usize >= planned {
                return Err(PublicationError::StrandedRow { index }.into());
            }
            rows.insert(index, value);
        }
        match next {
            Some(cursor) => after = Some(cursor),
            None => return Ok(rows),
        }
    }
}

/// Check the durable range against the plan: every row below `required` must be durable
/// and identical, and no durable row may differ from the plan. Returns the rows found
/// durable and identical.
fn check_durable(
    rows: &[Row],
    durable: &BTreeMap<u32, Bytes>,
    required: usize,
) -> std::result::Result<BTreeSet<usize>, PublicationError> {
    let mut witnessed = BTreeSet::new();
    for (index, row) in rows.iter().enumerate() {
        match durable.get(&(index as u32)) {
            Some(value) if *value == row.value => {
                witnessed.insert(index);
            }
            Some(_) => {
                return Err(PublicationError::SegmentMismatch {
                    index: index as u32,
                })
            }
            None if index < required => {
                return Err(PublicationError::SegmentNotDurable {
                    index: index as u32,
                })
            }
            None => {}
        }
    }
    Ok(witnessed)
}

/// A key for an error message, cut to 64 bytes.
fn rendered(key: &[u8]) -> String {
    String::from_utf8_lossy(&key[..key.len().min(64)]).into_owned()
}
