//! `CompleteMultipartUpload` (#658): its **fence phase** (#1007, slice 1 of 5) and the
//! **publication** composed onto it, flat (#1008, slice 2) and segmented (#1009, slice 3), with
//! the recovery from a commit whose outcome the store could not report (#1010, slice 4).
//! [`fence_complete`] is the fence, the validation of the named parts against the part set the
//! fence froze, and the release of a rejected request's fence; it publishes nothing, and hands a
//! valid request's fence back as a [`HeldFence`]. [`complete_upload`] is the verb: that phase,
//! then the publication of what the fence holds: one flat root, or past
//! [`MAX_MAP_CHUNKS`](super::MAX_MAP_CHUNKS) chunks the attempt's `seg:` records and then the
//! root that names them (the *Segment write* and *Root flip* rows, `0016:662-663`; module doc
//! *Publication*). *Unknown outcomes* below is how both phases settle a commit whose outcome the
//! store could not report.
//!
//! [`fence_complete`] reads the session and answers every state but `Open` as decision 3's table
//! does (`0016:976`, [`super::complete_answer`]). An `Open@E` session is fenced to
//! `Completing@E+1` (proposal 0016's *Complete fence* row, `0016:661`), and the request is
//! validated against what the fence froze. A rejection releases the fence again (the *Fence
//! release* row, `0016:664`) before it is answered:
//!
//! | Found | Answer | Written |
//! |---|---|---|
//! | `Open@E`, a valid list | [`FenceOutcome::Held`] | the fence: `require(mpu:<id> == bytes read)`; put the session at `Completing@E+1`, with `fenced_at_millis`, `segments_written: 0`, `publish_target {parent, name, epoch: E+1}` and `attempts + 1` |
//! | `Open@E`, a list rejected against the frozen set or the bucket | [`Refusal::InvalidPart`] or [`Refusal::NoSuchBucket`] | the fence, then its release: `require(mpu:<id> == the fence's bytes)`; put the session at `Open@E+2` with `attempts` as read |
//! | `Open@E`, a list already invalid before the fence | the same refusals | nothing |
//! | `Open@E`, `attempts` at [`MAX_COMPLETE_ATTEMPTS`] | [`Refusal::CompleteAttemptsExhausted`] | nothing |
//! | `Completing` (another publisher's fence) | [`Refusal::OperationAborted`] | nothing |
//! | `Completed`, the same list | [`CompleteOutcome::AlreadyCompleted`], the **recorded** publication | nothing |
//! | `Completed` (another list), `Aborting`, no record, or a session for another target | [`Refusal::NoSuchUpload`] | nothing |
//!
//! # The budget, and what a rejection costs (`0016:994-1000`)
//!
//! `attempts` bounds how many `seg:` epochs one session can mint (`0016:661`, `:1479`). A fence
//! that is released before it wrote a segment minted none, so the release puts `attempts` back
//! to the value read before the fence. A client error therefore never spends the budget, and an
//! `Open` session can always be fenced again: three typos do not leave a correct fourth request
//! answering [`Refusal::CompleteAttemptsExhausted`], which is the wedge #636's patch had. A
//! fence whose segment batch landed did mint one, and its release keeps the attempt spent
//! (*Publication* below), so [`MAX_COMPLETE_ATTEMPTS`] bounds every fence that wrote a segment
//! (X58, `0016:2577`).
//!
//! The list is checked before the fence too, against the part set and bucket as they are then,
//! so a request that is already invalid is answered without fencing (no commit, and no in-flight
//! `UploadPart` loses the session bytes it is fenced on). That check is only an optimization.
//! The check that decides is the one against the set the fence froze: a part can be replaced
//! between the two reads, and that request is still released with its attempt given back.
//!
//! What the fence freezes is the session's `psum:` range. A part summary is written in the same
//! batch as the `part:` record it summarizes (`0016:660`) and carries what validation needs,
//! the part's digest, plus its chunk count and length, so the next slices can size an assembly
//! before they read a chunk list. The `part:` records themselves are not read here: their chunk
//! lists can total tens of megabytes for one session, and only publication needs them.
//!
//! # Ownership, and the two errors
//!
//! A fence is this Complete's only once a fence commit of its own answered `Committed`. From
//! then on, every answer this function returns finds it down, released by this Complete or moved
//! off by another writer, and a valid request hands it over in the [`HeldFence`]; only an error
//! may leave it standing (**C-1**, `docs/principles.md` §5). A fence commit whose outcome is
//! unknown never makes it this Complete's, whatever a read then finds (*Unknown outcomes*):
//!
//! * [`CompleteError::InDoubt`]: a fence, release, segment or flip commit of this Complete came
//!   back with an unknown outcome ([`CommitUnknownResult`]) that no read settled: a read that
//!   failed, a fence found at its own bytes, a release, or a batch that may still land when the
//!   Complete had to stop (*Unknown outcomes*).
//! * [`CompleteError::Stopped`]: nothing is in doubt, and the error says what became of this
//!   Complete's fence ([`FenceState`]). The release is the one way this slice takes its own
//!   fence down; when its rounds run out ([`RELEASE_ROUNDS`]) or the store refuses it for good,
//!   the fence stands and the reaper's `W_completing` rollback is what clears it (#625).
//!
//! A Complete that meets **another** publisher's `Completing` fence leaves it untouched: it
//! answers [`Refusal::OperationAborted`] and writes nothing (`0016:953-958`).
//!
//! A store fault while validating behind the fence is not a rejection, so it is never answered as
//! one: it ends in an error, unless that release finds the session moved to a state with an answer
//! of its own. The Complete tries to take its fence down, because a fence that wrote no segment
//! minted nothing the cap bounds, and left standing it would cost the client a wait and an attempt
//! for a fault of the store's. It tries with **one** release round, the same exact-bytes check and
//! the same record as a rejection's release (a second commit only for a session found at a later
//! segment cursor of the same fence, *The release once a segment landed*):
//!
//! | The release | The error |
//! |---|---|
//! | lands | [`FenceState::Released`]: the session is `Open@E+2` with `attempts` as read before the fence, unless a segment batch of the attempt landed (*The release once a segment landed*) |
//! | fails for good, or answers `Conflict` with the session still the bytes it required | [`FenceState::Stands`] |
//! | comes back with an unknown outcome | [`CompleteError::InDoubt`], about the release |
//! | answers `Conflict` and the session moved | the answer for its new state, as a rejection's release gets; an `Open` session has no rejection to answer, and a torn record no answer at all, so those are [`FenceState::Gone`] |
//! | answers `Conflict` and the session cannot be read | [`FenceState::Unknown`] |
//!
//! Every error in that table keeps the fault that stopped validation: a stopped error as its cause,
//! the in-doubt error in [`InDoubt::fault_behind_fence`], beside the unknown outcome that is its
//! source. An answer has nowhere to carry it, so a session moved to a state with an answer drops
//! the fault: the store's state is what the client is told about. A completer that crashes behind
//! its fence, or whose one release does not land, still costs the wait 0016 accepts for that case,
//! up to `W_completing` (`0016:956-962`).
//!
//! # Publication (#1008, #1009)
//!
//! [`complete_upload`] fences as [`fence_complete`] does. Behind the held fence it reads each
//! named part's `part:` record, in the list's order, and requires it to agree with the summary
//! the fence froze, which carries the digest the list names ([`Stop::PartDisagrees`]). Those
//! chunk lists, concatenated in that order, are the object. It then reads the target: the dirent
//! the session names and, when one is bound, the inode it names. Publication goes through
//! [`crate::staged_publication`], whose own rows pin the dirent and the inode on both branches
//! (both `require_absent` for a free name, both exact bytes for a bound one) and write version
//! 1, or the re-read prior's version + 1.
//!
//! Up to [`MAX_MAP_CHUNKS`](super::MAX_MAP_CHUNKS) chunks the publication is the flip alone, one
//! flat root. Past it the committer first writes the chunk list as the attempt's `seg:`
//! records, under the group `(segment_nonce, E+1)` ([`SessionRecord::attempt_segment_group`]),
//! in batches sized to the transaction envelope, and reads every one back before the flip
//! publishes a root that names them (decision 7(b), `0016:2342-2356`). Each segment batch
//! carries this contribution, which moves the session's segment cursor in the batch that writes
//! the records it counts (`0016:613-620`, `:662`):
//!
//! | Precondition | Mutation |
//! |---|---|
//! | `require(mpu:<id> == Completing@E+1 at segments_written = first)` | the session at `segments_written = written` |
//!
//! `first` and `written` are the batch's cursor positions ([`SegmentBatchInfo`]). The count the
//! flip requires the cursor at is the committer's ([`staged_publication::planned_segments`]),
//! never re-derived from the chunk count: wide chunk refs close a segment early. A later round
//! of the same attempt resumes from the cursor the session holds (`resume_from`): every record
//! below it is read back and pinned, never written again (`0016:2392-2401`). The flip's
//! contribution, for an `Open@E` session fenced at `Completing@E+1`:
//!
//! | Precondition | Mutation |
//! |---|---|
//! | `require(mpu:<id> == Completing@E+1 at the cursor the segment phase leaves)`, 0 for a flat map | the session at `Completed@E+2`, recording the inode, the version, the multipart ETag, `completed_at_millis` and the list's `complete_fingerprint` |
//! | `require(bucket:<name> == the marker bytes validated behind the fence)` (ADR-0046 §4) | none |
//! | `require_absent(retire:records:s:<id>:<E+1>)` | `{parts}` naming the **published** parts only (`0016:910-925`) |
//! | `require_absent(retire:bytes:s:<id>:<E+1>)`, when a frozen part is unnamed | `{parts}` naming the unnamed parts: their bytes, then their records |
//! | `require_absent(retire:bytes:g:<inode>:<version>)`, when the flip supersedes a prior that names a chunk | `{generation}` naming that prior by its chunk list, or by its segment group |
//!
//! A zero-byte prior names no chunk, so superseding it installs no obligation: one naming an
//! empty list would not decode. The root's `etag` is the multipart ETag, its content type the
//! session's, and its `modified` the publication stamp ordered after the prior's
//! ([`metadata::overwrite_modified`], which the committer applies).
//!
//! A round that answers `Conflict` (its flip lost, or one of its segment batches did) is judged
//! from what the store then holds, in this order:
//!
//! | Found | Then |
//! |---|---|
//! | the session is not this fence at any cursor | the answer for its new state, as a release that finds it moved gets; an `Open` session answers [`Refusal::OperationAborted`] |
//! | the bucket marker absent, or not the bytes validated | release, then [`Refusal::NoSuchBucket`] |
//! | [`R_PUBLISH`] rounds sent | release, then [`Refusal::OperationAborted`]; the session is `Open` and the client may retry |
//! | otherwise | read the target again and send the next round from the cursor found, the version recomputed from the prior read now (`0016:926-941`) |
//!
//! Those releases are a rejection's, so a release that cannot land ends in the error that says
//! the fence still stands. A round the store refuses for good (a definite error, neither
//! `Conflict` nor an unknown outcome) is followed by no other round: the request tries one
//! release, as for a fault behind the fence (the table under *Ownership*), and ends
//! [`CompleteError::Stopped`] with that error as its cause and the fence's state. So does a
//! fault reading the parts or the target, a part or target record that is damaged, and a
//! publication the committer refuses before writing ([`Stop::Publication`]). A round whose
//! outcome is unknown is never read as `Conflict`: the session is read again first (*Unknown
//! outcomes*). The committer does not say which of a round's commits that was, so an error about
//! it names the flip only for a round with no segment batch to send ([`DoubtfulCommit::Flip`]),
//! and the round otherwise ([`DoubtfulCommit::Publication`]).
//!
//! # The release once a segment landed
//!
//! A segment batch moves the cursor in the batch that writes its records, so a cursor above 0 is
//! the proof that this attempt wrote `seg:` records, and a cursor of 0 that it wrote none. Every
//! release requires the session at the cursor it holds, read after the round that moved it, and
//! from a cursor above 0 it is the whole *Fence release* row (`0016:664`):
//!
//! | Precondition | Mutation |
//! |---|---|
//! | `require(mpu:<id> == Completing@E+1 at that cursor)` | the session at `Open@E+2`, `attempts` as the fence left them |
//! | `require_absent(retire:records:s:<id>:<E+1>)` | `{seg: (segment_nonce, E+1)}`, which deletes those records and never orphan-marks a fragment |
//!
//! So every `seg:` record a fence wrote is named by the root the flip published or by the
//! obligation the batch that releases the fence installs, and the attempt stays charged: it
//! minted a segment epoch and a rollback obligation, which is what [`MAX_COMPLETE_ATTEMPTS`]
//! bounds (X58, `0016:2577`). A release from cursor 0 is the one the fence phase sends, with
//! `attempts` given back and no obligation, so a fence that wrote no segment never spends the
//! budget.
//!
//! A release that answers `Conflict` and finds the session at a later cursor of this same fence
//! (a segment batch landed that this Complete had not read back, because the read after a failed
//! round failed too) is sent again from that cursor. That spends no release round: only this
//! Complete's segment batches move the cursor, and only forward, so it happens at most once per
//! batch that landed.
//!
//! # Unknown outcomes (#1010)
//!
//! A commit that answers [`CommitUnknownResult`] is never read as `Conflict`: the Complete reads
//! the session again and answers only from a state it found and can attribute to itself
//! (`0016:942-964`). It keeps the two kinds of doubt an Abort keeps (`abort.rs:259-273`). An
//! outcome that said `may_still_commit` is **held**, the first one kept, until a read finds the
//! session off the bytes a batch of this Complete that may still land requires; one that did not
//! is **unsettled** until the next read that succeeds. A session record that does not decode
//! says nothing of what landed before it, so a read that finds one settles neither kind. While
//! either stands, a read that fails and every other stop ends the request in
//! [`CompleteError::InDoubt`], about the first held outcome, else the unsettled one: never an
//! answer, and never [`CompleteError::Stopped`].
//!
//! **The initial fence.** A fence's bytes are fixed by the session read and the request's
//! stamp, and a `Completing` record names no request (`multipart.rs:2276-2285`), so a rival
//! Complete that read the same `Open@E` with the same stamp builds a byte-identical fence.
//! Finding those bytes proves nothing about who owns them:
//!
//! | Read after a fence whose outcome is in doubt | Then |
//! |---|---|
//! | the fence's own bytes | [`CompleteError::InDoubt`] about the fence, and nothing more written: it landed, or a rival's did |
//! | `Open@E` unchanged, the outcome out of flight | the fence did not land and never will: it is sent again, within [`FENCE_ROUNDS`] |
//! | `Open@E` unchanged, the outcome held | the fence is sent again, without the check before it; a `Committed` settles the held one, whose required bytes are then gone |
//! | `Open` with more attempts spent than `Open@E` | [`Refusal::OperationAborted`], and nothing written: a fence came down and kept its attempt, as the reaper's rollback does, and it may have been this one |
//! | any other state | answered from it, as after a `Conflict`; an `Open` session with no attempt spent since is fenced again |
//!
//! So a fence this Complete did place, but cannot prove, is left to the reaper's `W_completing`
//! rollback (#625), and its attempt stays charged. Once a read finds the session `Open` with an
//! attempt spent since, that rollback may have come, and this call does not fence again: its stamp
//! may be the one the reaper found past `W_completing`, and a fence stamped with it would be past
//! the window from the start (F16, `0016:1005-1009`). The client's retry is the fresh Complete,
//! with a fresh stamp, as after a rollback behind the fence (below). An `Open` session with no
//! attempt spent since was fenced and released only by Completes that owned their fences and gave
//! the attempt back. A fence of this call has no owner, so no such release took it down, and the
//! request fences again.
//!
//! **Behind the fence.** The fence is this Complete's, so what a read finds there is
//! attributable:
//!
//! | Read after a publication round whose outcome is in doubt | Then |
//! |---|---|
//! | the `Completed` record a flip of this Complete put | [`CompleteOutcome::Published`], with that flip's publication |
//! | this fence, at a segment cursor | the next round, from that cursor (`resume_from`): no `seg:` record is written twice, and the version is recomputed from the target read again |
//! | any other state | answered from it, as after a round that lost |
//!
//! An `Open` session there is the reaper's rollback (`0016:951-953`), answered
//! [`Refusal::OperationAborted`] with nothing of this Complete able to land: the fresh Complete is
//! the client's retry. This call does not fence again, because its fence stamp is a reading taken
//! before the fence the reaper just found past `W_completing`, and a second fence stamped with it
//! would be past the window from the start (F16, `0016:1005-1009`).
//!
//! An unknown round counts against [`R_PUBLISH`]. The committer does not say which of a round's
//! batches the outcome is about, but every batch before it answered `Committed`, so the one in
//! doubt required the session where they left it: the read after it finds the session there, or
//! past it if it landed. Those bytes are what a held outcome is kept against. Every later batch
//! of the attempt requires them as well, so the first of those to land settles it. While a held
//! outcome stands, the Complete sends no release: a round that would release (its bucket marker
//! gone, [`R_PUBLISH`] rounds spent, a fault) ends the request in doubt instead, and the fence is
//! left to the batch that may still land, or to the reaper.
//!
//! # Runtime-free (ADR-0009), and one clock per session
//!
//! Core reads no clock, draws no randomness and never sleeps here. The fence stamp
//! `fenced_at_millis` arrives in the [`CompleteRequest`], with the `clock_source` that names its
//! clock. That stamp belongs to the session's residency lifecycle: the reaper measures
//! `W_completing` from it (`0016:1005-1009`) under the clock the session record names, so a
//! request whose clock source is not the session's is stopped before anything is written
//! ([`Stop::ForeignClock`], `0016:1966-1976`). A lost round, and a fence a read showed did not
//! land, is retried at once, with no backoff; a retried fence carries the request's one stamp,
//! for the same session lifecycle, and is never sent once a read found that the reaper may have
//! rolled a fence with that stamp back (*Unknown outcomes*).
//!
//! The publication stamp ([`PublishRequest::completed_at_millis`]) is read by the caller on that
//! same clock, the session's, which the fence has just checked is the request's: the
//! `Completed` session's `completed_at_millis` is a session-scoped stamp, so it must be
//! (`0016:1966-1976`). It is one reading, and it stamps both records the flip publishes: that
//! field and the root's ADR-0047 `modified`. The session's `clock_source` is the publishing
//! gateway's wall clock (`0016:1955-1958`), the clock `modified` is read on for any other
//! publication, so the flip mixes no clocks. The inode a free name is published under
//! ([`PublishRequest::fresh_inode`]) is the caller's too.
//!
//! Production calls nothing here yet: the S3 verb that will is #670, once #1011 completes
//! publication.

use std::collections::BTreeMap;
use std::fmt;

use bytes::Bytes;
use wyrd_traits::{BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, WriteBatch};

use super::abort::ABORT_ROUNDS;
use super::{
    complete_answer, complete_fingerprint, decode_part_record, decode_part_summary,
    decode_session_record, mpu_key, multipart_etag, parse_psum_key, part_key, psum_range,
    CompleteAnswer, CompleteOutcome, Completion, Digest, InvalidPart, MultipartEtag, PartNumber,
    PartNumberSet, PartSummary, Publication, RecordError, Refusal, RetireObligation, RetiredMap,
    SessionRecord, SessionState, UploadId, MAX_COMPLETE_ATTEMPTS, R_PUBLISH,
};
use crate::metadata::{
    self, ChunkMap, ChunkRef, DirentRecord, InodeId, InodeRecord, ObjectMeta, SegmentGroup,
};
use crate::staged_publication::{self, PublicationError, RootTarget, SegmentBatchInfo};

/// How many fence commits one [`fence_complete`] makes at most before it stops with no fence
/// placed: [`ABORT_ROUNDS`].
///
/// A fence round is spent by a commit that answered `Conflict` while the session is still `Open`:
/// unchanged (on TiKV, a lock another writer of `mpu:<id>` held while nothing moved,
/// `abort.rs:249-251`), or at a later epoch after another request's fence was released. Any other
/// state found is answered at once. It is the budget an Abort spends against the same key and the
/// same writers, and it bounds the work one Complete does, not its correctness: one that spends
/// it ends with [`Stop::FenceRoundsSpent`], never with a wrong answer.
pub const FENCE_ROUNDS: u32 = ABORT_ROUNDS;

/// How many release commits one [`fence_complete`] makes at most before it stops with its fence
/// standing: [`ABORT_ROUNDS`].
///
/// A release round is spent only by a commit that answered `Conflict` while the session is still
/// exactly the bytes the release required, a conflict nothing a read can see explains (on TiKV,
/// a lock another writer held): a session moved off the fence ends the release and is answered
/// from its new state, and one found at a later segment cursor of the same fence is released
/// again from there. Bounded so a Complete terminates with no reaper running, at the same budget
/// an Abort spends against the same key. A Complete that spends it ends with
/// [`Stop::ReleaseRoundsSpent`] and its fence standing.
pub const RELEASE_ROUNDS: u32 = ABORT_ROUNDS;

/// How many `psum:` records one page of the frozen-set read asks for. At the value ceiling a
/// page is at most `128 ×` [`crate::metadata::MAX_VALUE_BYTES`] (12.8 MB), the bound
/// [`crate::metadata::SEGMENT_PAGE_LIMIT`] keeps for a `seg:` page; a well-formed summary is
/// tens of bytes, so [`super::MAX_PARTS_PER_SESSION`] parts take 79 pages.
const PART_PAGE_LIMIT: usize = metadata::SEGMENT_PAGE_LIMIT;

/// The `(part number, digest)` list a Complete names, in the client's order. Never empty.
///
/// Order is **not** checked here. A list out of order, or naming a part twice, is a well-formed
/// request that the protocol answers ([`InvalidPart::OutOfOrder`]); an empty list is not a request
/// at all (S3 refuses it as malformed XML before it reaches a store), so it has no value of this
/// type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedParts(Vec<(PartNumber, Digest)>);

impl NamedParts {
    /// The list, refusing an empty one ([`RecordError::NoPartsNamed`]).
    pub fn new(parts: Vec<(PartNumber, Digest)>) -> Result<Self, RecordError> {
        if parts.is_empty() {
            return Err(RecordError::NoPartsNamed);
        }
        Ok(Self(parts))
    }

    /// The pairs, in the order the client named them.
    pub fn as_slice(&self) -> &[(PartNumber, Digest)] {
        &self.0
    }
}

/// What one `CompleteMultipartUpload` asks for. Every field is the caller's: core composes no
/// name and reads no clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompleteRequest<'a> {
    /// The bucket whose `bucket:<name>` marker must exist for the request to be valid
    /// (ADR-0046 §4, `0016:1035`). The marker's bytes are handed on in the [`HeldFence`], for the
    /// publication's precondition.
    pub bucket: &'a str,
    /// The dirent parent the request names. It must be the session's own target
    /// ([`SessionRecord::parent`]), spelled as Create was given it ([`super::create::CreateRequest`]).
    pub parent: InodeId,
    /// The dirent name the request names. It must be the session's own ([`SessionRecord::object`]).
    pub object: &'a str,
    /// The parts to assemble.
    pub parts: &'a NamedParts,
    /// The fence stamp, in milliseconds of the clock [`Self::clock_source`] names.
    pub fenced_at_millis: u64,
    /// The clock that read [`Self::fenced_at_millis`]. It must be the session's own
    /// ([`SessionRecord::clock_source`]).
    pub clock_source: &'a str,
}

/// What [`fence_complete`] returns when nothing is in doubt and nothing stopped it.
#[derive(Debug)]
pub enum FenceOutcome {
    /// The request is valid against the part set its fence froze and the bucket, and this call
    /// holds that fence. Nothing is published.
    Held(Box<HeldFence>),
    /// The answer, and no fence of this call stands: decision 3's answer for the state found, or
    /// a rejection whose fence this call released. Never [`CompleteOutcome::Published`]: nothing
    /// here publishes.
    Answered(CompleteOutcome),
}

/// **A fence this Complete owns**: the session at `Completing@E+1` under a fence commit of its
/// own that answered `Committed`, with the request validated against the part set that fence
/// froze. Only the fence phase makes one ([`fence_complete`]); it is the value publication
/// consumes ([`complete_upload`], #1008).
///
/// It is neither `Clone` nor constructible elsewhere: one fence, one holder. Dropping it leaves
/// the fence standing until the reaper's `W_completing` rollback (#625).
#[derive(Debug)]
pub struct HeldFence {
    upload_id: UploadId,
    placed: Placed,
    attempts_before: u32,
    named: NamedParts,
    frozen: BTreeMap<PartNumber, PartSummary>,
    bucket_marker: Bytes,
}

impl HeldFence {
    /// The upload the fence is on.
    pub const fn upload_id(&self) -> &UploadId {
        &self.upload_id
    }

    /// The fence epoch `E+1`: the session's epoch and its `publish_target.epoch`.
    pub const fn epoch(&self) -> u64 {
        self.placed.session.epoch()
    }

    /// The session as the fence left it, at `Completing@E+1` with no segment written.
    pub const fn session(&self) -> &SessionRecord {
        &self.placed.session
    }

    /// The exact bytes the fence put at `mpu:<id>`: what every later batch of this attempt
    /// requires there until a segment batch of it moves the segment cursor.
    pub const fn session_bytes(&self) -> &Bytes {
        &self.placed.bytes
    }

    /// `attempts` as read before the fence: the value a release of a fence that wrote no
    /// segment puts back.
    pub const fn attempts_before(&self) -> u32 {
        self.attempts_before
    }

    /// The validated list: every part present in [`Self::frozen_parts`] with the named digest, in
    /// strictly ascending order.
    pub fn named_parts(&self) -> &[(PartNumber, Digest)] {
        self.named.as_slice()
    }

    /// The frozen part set: every `psum:<id>:` record the fence froze, by part number, named or
    /// not.
    pub const fn frozen_parts(&self) -> &BTreeMap<PartNumber, PartSummary> {
        &self.frozen
    }

    /// The `bucket:<name>` marker bytes read behind the fence, for the publication's ADR-0046 §4
    /// precondition.
    pub const fn bucket_marker(&self) -> &Bytes {
        &self.bucket_marker
    }
}

/// A Complete fence and every record that takes it down, minted together before the fence is
/// sent, so no fence is placed that could not be released (`0016:661`, `:664`).
#[derive(Debug)]
struct Placed {
    /// The session as the fence puts it: `Completing@E+1`, no segment written.
    session: SessionRecord,
    /// Its encoding: what the fence puts.
    bytes: Bytes,
    /// The segment group the attempt writes under ([`SessionRecord::attempt_segment_group`]): the
    /// session's nonce at `E+1`.
    group: SegmentGroup,
    /// The release while no segment batch of the attempt landed: `Open@E+2`, `attempts` as read
    /// before the fence.
    restored: Bytes,
    /// The release once one did: `Open@E+2`, `attempts` as the fence left them.
    charged: Bytes,
}

impl Placed {
    /// The fence of `opened` stamped `fenced_at_millis`, and its releases. `None` when the session
    /// cannot be fenced and released: not `Open`, or too near the epoch or attempts ceiling.
    fn mint(opened: &SessionRecord, fenced_at_millis: u64) -> Option<Self> {
        let session = opened.complete_fence(fenced_at_millis)?;
        let restored = session.fence_released(opened.attempts())?;
        let charged = session.fence_released(session.attempts())?;
        Some(Self {
            bytes: metadata::encode(&session),
            group: session.attempt_segment_group()?,
            restored: metadata::encode(&restored),
            charged: metadata::encode(&charged),
            session,
        })
    }

    /// The fence epoch, `E+1`.
    const fn epoch(&self) -> u64 {
        self.session.epoch()
    }

    /// The session's bytes at segment cursor `written`: the fence's record with only
    /// `segments_written` moved. `None` past the cursor's decode bound, which no plan the
    /// committer accepts reaches ([`crate::metadata::MAX_ROOT_SEGMENTS`] segments at most).
    fn at(&self, written: u32) -> Option<Bytes> {
        self.session
            .with_segments_written(written)
            .map(|session| metadata::encode(&session))
    }

    /// The segment cursor `found` holds, when it is this fence at some cursor: byte for byte the
    /// record [`Self::at`] mints there. `None` for any other value.
    fn cursor_of(&self, found: &[u8]) -> Option<u32> {
        let session = decode_session_record(found).ok()?;
        let SessionState::Completing {
            segments_written, ..
        } = session.state()
        else {
            return None;
        };
        let cursor = *segments_written;
        (self.at(cursor)?.as_ref() == found).then_some(cursor)
    }
}

/// A Complete that ended with neither an answer nor a held fence.
///
/// # Error class
///
/// [`wyrd_traits::classify`] reads [`Self::InDoubt`] as
/// [`ErrorClass::Indeterminate`](wyrd_traits::ErrorClass::Indeterminate): its
/// [`source`](std::error::Error::source) is the store's own [`CommitUnknownResult`].
/// [`Self::Stopped`] keeps its cause's class: a [`Stop::Store`] wrapping a
/// [`TransientFault`](wyrd_traits::TransientFault) is transient, because nothing is in doubt.
#[derive(Debug)]
pub enum CompleteError {
    /// A fence, release, segment or flip commit of this Complete may have landed, or may still
    /// land. Never "nothing was written".
    InDoubt(Box<InDoubt>),
    /// The Complete stopped, and no commit of it is in doubt.
    Stopped(Box<Stopped>),
}

/// What a [`CompleteError::InDoubt`] reports.
#[derive(Debug)]
pub struct InDoubt {
    /// The upload whose session is in doubt.
    pub upload_id: UploadId,
    /// Which commit's outcome the store could not report: the one [`Self::outcome`] is about.
    pub commit: DoubtfulCommit,
    /// Whether a commit of this Complete may still be applied **after** it returned
    /// ([`CommitUnknownResult::may_still_commit`]). Once one said so, only a read that found the
    /// session off the bytes it requires clears it, so later outcomes, failed reads and other
    /// stops leave it `true`.
    pub may_still_commit: bool,
    /// The store's report of the unknown outcome that keeps the doubt: the first that said
    /// `may_still_commit`, or else the latest one no successful read has settled (module doc,
    /// *Unknown outcomes*). It is the error's [`source`](std::error::Error::source).
    pub outcome: CommitUnknownResult,
    /// The fault behind the fence that made this Complete try the release, when that is why it
    /// tried: a release of a rejected request, or a fence commit, has none. Kept so an operator
    /// still sees what stopped validation, and shown in the error's text. It does not change the
    /// error's class: the commit in doubt does that.
    pub fault_behind_fence: Option<Stop>,
}

/// The commit a [`CompleteError::InDoubt`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoubtfulCommit {
    /// The fence `Open@E → Completing@epoch`: the session is still `Open@E`, or fenced at those
    /// bytes by this Complete or by a rival whose fence is byte-identical, which a `Completing`
    /// record cannot tell apart (module doc, *Unknown outcomes*).
    Fence {
        /// The fence epoch, `E+1`.
        epoch: u64,
    },
    /// The release `Completing@epoch → Open@epoch+1` of a fence this Complete owns: the fence
    /// still stands, or is released.
    Release {
        /// The epoch of the fence being released.
        epoch: u64,
    },
    /// The root flip `Completing@epoch → Completed@epoch+1` of a fence this Complete owns, one
    /// batch with the root and its obligations, in a round with no segment batch to send: either
    /// the object is published and the session `Completed`, or neither is and the fence still
    /// stands.
    Flip {
        /// The epoch of the fence the flip requires.
        epoch: u64,
    },
    /// A commit of a publication round of a fence this Complete owns that had segment batches to
    /// send (`0016:662-663`): one of them, or the root flip after them. The committer returns the
    /// store's unknown outcome as it gave it, without saying which batch it was. Either the
    /// object is published and the session `Completed`, or the fence still stands, at the segment
    /// cursor the last landed batch moved it to.
    Publication {
        /// The epoch of the fence every batch of the round requires.
        epoch: u64,
    },
}

/// What a [`CompleteError::Stopped`] reports.
#[derive(Debug)]
pub struct Stopped {
    /// The upload the Complete was for.
    pub upload_id: UploadId,
    /// What became of this Complete's own fence.
    pub fence: FenceState,
    /// What stopped it.
    pub cause: Stop,
}

/// What became of a stopped Complete's own fence ([`Stopped::fence`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceState {
    /// No fence commit of this Complete landed.
    NotPlaced,
    /// This Complete's fence landed at `Completing@epoch` (a commit of it answered `Committed`),
    /// and no read since found the session moved off it: it still stands.
    Stands {
        /// The fence epoch, `E+1`.
        epoch: u64,
    },
    /// This Complete's fence landed at `Completing@epoch`, and a release commit of its own then
    /// answered `Committed`: the fence is down.
    Released {
        /// The fence epoch, `E+1`.
        epoch: u64,
    },
    /// This Complete's fence landed at `Completing@epoch`, and a later read found the session
    /// moved off it by another writer: it no longer stands, and this Complete did not release it.
    Gone {
        /// The fence epoch, `E+1`.
        epoch: u64,
    },
    /// This Complete's fence landed at `Completing@epoch`, a release of it answered `Conflict`, and
    /// the read that would say why failed. The release did not apply, but the conflict may mean
    /// another writer moved the session, so whether the fence stands is not known. It is safe to
    /// not know: every later batch of an attempt requires the fence's exact bytes.
    Unknown {
        /// The fence epoch, `E+1`.
        epoch: u64,
    },
}

/// What stopped a Complete ([`Stopped::cause`]).
#[derive(Debug)]
pub enum Stop {
    /// All [`FENCE_ROUNDS`] fence commits answered `Conflict` and the session was still `Open`.
    FenceRoundsSpent {
        /// Fence commits made.
        rounds: u32,
    },
    /// All [`RELEASE_ROUNDS`] release rounds were spent: each a release commit that answered
    /// `Conflict` with the session still exactly the bytes it required.
    ReleaseRoundsSpent {
        /// Release commits that answered so.
        rounds: u32,
    },
    /// The session is `Open` at an epoch too high for a fence and its release both to be
    /// written (`0016:704-708`).
    EpochExhausted,
    /// The request's clock is not the one the session's timestamps are read under, so its stamp
    /// may not be written into the session (`0016:1966-1976`).
    ForeignClock {
        /// The session's `clock_source`.
        session: String,
        /// The request's `clock_source`.
        request: String,
    },
    /// A stored `mpu:` or `psum:` value is not a valid record (ADR-0045), or a key under the
    /// session's `psum:` range does not parse.
    Record(RecordError),
    /// A store operation failed, or a page of the `psum:` range broke the paging contract (a key
    /// at or below the last one read, or outside the range asked for). After a held fence, this
    /// is also a publication round the store refused for good: a definite error that is neither
    /// `Conflict` nor an unknown outcome.
    Store(BoxError),
    /// A named part's `part:` record is absent, or disagrees with the `psum:` summary the fence
    /// froze, which carries the digest the list names. The commit writes the pair in one batch
    /// and no part commits behind a fence, so this is a damaged record, never a request to answer.
    PartDisagrees {
        /// The part.
        part_number: PartNumber,
    },
    /// The publish target's dirent, or the inode it names, is absent where it must be present or
    /// is not a record that decodes.
    Target(BoxError),
    /// The staged-publication committer refused the publication (a value over the ceiling, a
    /// batch over its envelope, a version with no successor, more segments than one root may
    /// name), before writing anything or, for a `seg:` range it read back, before the batch that
    /// would have depended on it.
    // deferred: #1011 — the value ceiling a flip's records are sized against.
    Publication(PublicationError),
}

impl fmt::Display for CompleteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InDoubt(doubt) => write!(
                f,
                "multipart complete: upload {} is in doubt: {} {}; re-read the session to learn \
                 whether it landed{}",
                doubt.upload_id,
                doubt.commit,
                if doubt.may_still_commit {
                    "may still be applied"
                } else {
                    "may have been applied"
                },
                match &doubt.fault_behind_fence {
                    Some(fault) =>
                        format!("; that release followed a fault behind the fence: {fault}"),
                    None => String::new(),
                },
            ),
            Self::Stopped(stopped) => write!(
                f,
                "multipart complete: upload {}: {}; {}; no commit of this complete is in doubt",
                stopped.upload_id, stopped.cause, stopped.fence,
            ),
        }
    }
}

impl fmt::Display for DoubtfulCommit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fence { epoch } => write!(f, "its fence at Completing@{epoch}"),
            Self::Release { epoch } => write!(f, "the release of its fence at Completing@{epoch}"),
            Self::Flip { epoch } => write!(f, "its root flip from Completing@{epoch}"),
            Self::Publication { epoch } => write!(
                f,
                "a segment batch or the root flip of its publication from Completing@{epoch}"
            ),
        }
    }
}

impl fmt::Display for FenceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPlaced => f.write_str("no fence of this complete landed"),
            Self::Stands { epoch } => write!(
                f,
                "this complete's fence at Completing@{epoch} still stands"
            ),
            Self::Released { epoch } => write!(
                f,
                "this complete's fence at Completing@{epoch} was taken down: its release landed"
            ),
            Self::Gone { epoch } => write!(
                f,
                "this complete's fence at Completing@{epoch} no longer stands: another writer \
                 moved the session off it"
            ),
            Self::Unknown { epoch } => write!(
                f,
                "this complete's fence at Completing@{epoch} may or may not stand: a release \
                 answered Conflict and the session could not be read again"
            ),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FenceRoundsSpent { rounds } => write!(
                f,
                "all {rounds} fence rounds were spent and the session is still Open"
            ),
            Self::ReleaseRoundsSpent { rounds } => write!(
                f,
                "all {rounds} release rounds were spent and the session is still at the fence"
            ),
            Self::EpochExhausted => {
                f.write_str("the session is Open at an epoch too high for a fence and its release")
            }
            Self::ForeignClock { session, request } => write!(
                f,
                "the request's clock source {request:?} is not the session's {session:?}, so the \
                 fence may not be stamped"
            ),
            Self::Record(torn) => write!(f, "a stored record is invalid: {torn}"),
            Self::Store(fault) => write!(f, "store fault: {fault}"),
            Self::PartDisagrees { part_number } => write!(
                f,
                "part {part_number}'s part: record is absent or disagrees with the summary the \
                 fence froze"
            ),
            Self::Target(torn) => write!(f, "the publish target is not readable: {torn}"),
            Self::Publication(refused) => {
                write!(f, "the committer refused the publication: {refused}")
            }
        }
    }
}

impl std::error::Error for CompleteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InDoubt(doubt) => Some(&doubt.outcome),
            Self::Stopped(stopped) => match &stopped.cause {
                Stop::Record(torn) => Some(torn),
                Stop::Store(fault) | Stop::Target(fault) => Some(fault.as_ref()),
                Stop::Publication(refused) => Some(refused),
                Stop::FenceRoundsSpent { .. }
                | Stop::ReleaseRoundsSpent { .. }
                | Stop::EpochExhausted
                | Stop::ForeignClock { .. }
                | Stop::PartDisagrees { .. } => None,
            },
        }
    }
}

// deferred: #660. Seeded Tier-0 DST coverage of this fence racing an Abort, a rival Complete and
// the reaper's rollback, and of the segment batches and the flip racing a `PutObject`, a bucket
// delete and the reaper, is #660's (out of #1007's to #1010's scope). Meanwhile the integration
// test `crates/core/tests/multipart_complete.rs` lands a rival fence, a teardown and a rollback
// at the fence's and the release's commit points, an overwrite, a bucket delete, a refused flip
// and applied, held and unread unknown flip outcomes at the flip's, lost flips, a bucket delete,
// a refused flip and an unknown segment-batch outcome behind landed segment batches, and two
// Completes racing byte-identical fences, through a store wrapper.

/// **`CompleteMultipartUpload`'s fence phase**: fence the session `upload_id` names
/// (`0016:661`), validate `request` against the part set the fence froze, and either hand the
/// fence back held ([`FenceOutcome::Held`]) or release it and answer
/// ([`FenceOutcome::Answered`]), in the landed vocabulary ([`CompleteOutcome`], [`Refusal`]).
/// [`complete_upload`] is the verb that composes publication onto this phase (#1008); no
/// production path calls either yet (#670 will).
///
/// # Before the fence
///
/// Read `mpu:<id>`. A failed read is an error, never an answer. A session for another target
/// than `request`'s answers [`Refusal::NoSuchUpload`]; every state but `Open` is answered from
/// [`complete_answer`] with the request's [`complete_fingerprint`]; a list not strictly ascending
/// answers [`InvalidPart::OutOfOrder`] at the first part out of place. Then, on `Open@E`: a
/// request whose clock source is not the session's stops ([`Stop::ForeignClock`]); a session at
/// [`MAX_COMPLETE_ATTEMPTS`] answers [`Refusal::CompleteAttemptsExhausted`]; a session too near
/// the epoch ceiling stops ([`Stop::EpochExhausted`]); and the list is checked against the bucket
/// and the part set as they are now (the module doc's optimization). None of that writes anything.
///
/// # The fence
///
/// One commit: `require(mpu:<id> == the bytes read)` and the session put at `Completing@E+1`
/// (`SessionRecord::complete_fence`). A `Conflict` reads the session again and answers from what
/// is there, so a rival Complete that fenced first answers [`Refusal::OperationAborted`] and an
/// Abort that did answers [`Refusal::NoSuchUpload`]; a session still `Open` spends one of
/// [`FENCE_ROUNDS`]. A [`CommitUnknownResult`] spends a round too, and reads the session again
/// (the module doc, *Unknown outcomes*): the fence's own bytes there are
/// [`CompleteError::InDoubt`], never a fence this call owns; `Open@E` unchanged sends the fence
/// again, skipping the check before it while the outcome may still land; and an `Open` session
/// with an attempt spent since answers [`Refusal::OperationAborted`], since the reaper may have
/// rolled this fence back. While an outcome is in doubt, every stop of this phase is
/// [`CompleteError::InDoubt`].
///
/// # Behind the fence
///
/// Read the bucket marker and the `psum:<id>:` range in bounded pages, and require every named
/// part to be present with the named digest. A valid request returns the [`HeldFence`]; a
/// rejected one is released: `require(mpu:<id> == the fence's bytes)` and the session put at
/// `Open@E+2` with `attempts` as read before the fence (`SessionRecord::fence_released`). Then
/// it is answered with the rejection. A release that answers `Conflict` reads the session again:
/// still the fence's bytes, it spends one of [`RELEASE_ROUNDS`]; moved, it is answered from the
/// new state (the rejection, if that state is `Open` again). A release the store refuses for good
/// or that spends its rounds is [`CompleteError::Stopped`] with [`FenceState::Stands`]; one whose
/// outcome is unknown is [`CompleteError::InDoubt`]; a `Conflict` whose read then fails is
/// [`CompleteError::Stopped`] with [`FenceState::Unknown`].
///
/// A store fault during that validation is never answered as a rejection. It tries one release,
/// and ends in an error unless that release finds the session moved to a state with an answer of
/// its own (the module doc's table).
///
/// # Termination
///
/// At most [`FENCE_ROUNDS`] fence commits and [`RELEASE_ROUNDS`] release commits, each followed
/// by at most one read of `mpu:<id>`; one validation before each fence commit, one more when the
/// fence rounds are spent, and one behind the fence, each one read of the bucket marker and a walk
/// of the `psum:` range that ends because every page must name part numbers strictly above the
/// last one seen, and there are [`super::MAX_PART_NUMBER`] of them. Each store call is bounded by
/// the store's own contract (every operation terminates, `crates/traits/src/lib.rs`, "Operational
/// envelope").
///
/// **Not cancel-safe once a commit is sent.** Dropping the future after that can leave a fence
/// standing that no answer or error names; the reaper's `W_completing` rollback is then what
/// takes it down (#625). Bound a Complete through the store's own deadlines, never by dropping
/// the future: a commit the store abandons at its deadline comes back as a
/// [`CommitUnknownResult`], which this function settles by a read or reports.
pub async fn fence_complete(
    store: &impl MetadataStore,
    upload_id: &UploadId,
    request: &CompleteRequest<'_>,
) -> Result<FenceOutcome, CompleteError> {
    let session_key = mpu_key(upload_id);
    let bucket_key = metadata::bucket_key(request.bucket);
    let named = request.parts.as_slice();
    // `Err` for a list that is not strictly ascending: no recorded fingerprint can match it, and
    // an `Open` session refuses it at the part it names.
    let fingerprint = complete_fingerprint(named);
    let not_placed = |cause| stopped(upload_id, FenceState::NotPlaced, cause);
    // Every stop is the in-doubt error while a fence of this call is in doubt.
    let stop = |doubt: &Doubt, cause| doubt.end(upload_id, not_placed(cause));
    let mut doubt = Doubt::default();
    let mut rounds = 0u32;
    let mut found = store
        .get(&session_key)
        .await
        .map_err(|fault| not_placed(Stop::Store(fault)))?;
    let (opened, placed) = loop {
        // `found` is always the result of a read that succeeded, and never the bytes of a fence of
        // this call whose outcome is in doubt: those end the request below.
        let Some(read) = found else {
            return Ok(refused(Refusal::NoSuchUpload));
        };
        // A record that does not decode says nothing of what a fence of this call did, so it
        // settles no doubt: the stop is the in-doubt error while one stands.
        let opened =
            decode_session_record(&read).map_err(|torn| stop(&doubt, Stop::Record(torn)))?;
        doubt.settle(&read);
        // A fence of this call held in doubt outlives that only when `read` is the very bytes it
        // required, which passed every check below in the round that sent it. So none of them
        // answers while it may still land, the optimization aside, which skips itself.
        let answer = answer_without_fence(&opened, request, &fingerprint)
            .map_err(|torn| stop(&doubt, Stop::Record(torn)))?;
        if let Some(answer) = answer {
            return Ok(answered(answer));
        }
        if opened.clock_source() != request.clock_source {
            return Err(stop(
                &doubt,
                Stop::ForeignClock {
                    session: opened.clock_source().to_owned(),
                    request: request.clock_source.to_owned(),
                },
            ));
        }
        if opened.attempts() >= MAX_COMPLETE_ATTEMPTS {
            return Ok(refused(Refusal::CompleteAttemptsExhausted));
        }
        // The releases are minted with the fence, so no fence is placed that could not be taken
        // down again.
        let Some(placed) = Placed::mint(&opened, request.fenced_at_millis) else {
            return Err(stop(&doubt, Stop::EpochExhausted));
        };
        // The optimization: a request already invalid is answered without a fence. Never while a
        // fence of this call may still land: that answer would leave the session to a fence no
        // answer names. The fence below settles the doubt instead, and the validation behind it
        // decides.
        if !doubt.holds() {
            match validate(store, upload_id, &bucket_key, named).await {
                Ok(Validation::Valid { .. }) => {}
                Ok(Validation::Rejected(refusal)) => return Ok(refused(refusal)),
                Err(cause) => return Err(stop(&doubt, cause)),
            }
        }
        if rounds == FENCE_ROUNDS {
            return Err(stop(&doubt, Stop::FenceRoundsSpent { rounds }));
        }
        rounds += 1;
        let batch = WriteBatch::new()
            .require(session_key.clone(), read.clone())
            .put(session_key.clone(), placed.bytes.clone());
        let reread = match store.commit(batch).await {
            Ok(CommitOutcome::Committed) => break (opened, placed),
            // Answered from what the store holds now: moved, from its new state; still the
            // bytes read, the round is spent and the fence tried again.
            Ok(CommitOutcome::Conflict) => store.get(&session_key).await,
            // Never read as `Conflict`: the read below says whether it can be settled.
            Err(fault) => {
                let Some(unknown) = fault.downcast_ref::<CommitUnknownResult>() else {
                    return Err(stop(&doubt, Stop::Store(fault)));
                };
                let commit = DoubtfulCommit::Fence {
                    epoch: placed.epoch(),
                };
                doubt.observe(commit, unknown, Some(read));
                store.get(&session_key).await
            }
        };
        found = reread.map_err(|fault| stop(&doubt, Stop::Store(fault)))?;
        if let Some(doubtful) = doubt.in_doubt(upload_id) {
            // A fence of this call in doubt, and the session at the bytes it puts: it landed, or a
            // rival's byte-identical fence did (a `Completing` record names no request,
            // `multipart.rs:2276-2285`). Never ownership, so nothing more is written.
            if found.as_ref() == Some(&placed.bytes) {
                return Err(doubtful);
            }
            // `Open` again, with an attempt spent since the session this fence was built from:
            // a fence was taken down and its attempt kept, which the reaper's rollback does, and
            // this fence may be the one it rolled back. Its stamp may then be one the reaper found
            // past `W_completing`, so the request never fences with it again.
            if open_and_charged_since(found.as_ref(), opened.attempts()) {
                return Ok(refused(Refusal::OperationAborted));
            }
        }
    };

    // The fence is this Complete's: its commit answered `Committed`. A fence of this call held in
    // doubt required the bytes this one replaced, which never come back, so it can no longer land.
    let why = match validate(store, upload_id, &bucket_key, named).await {
        Ok(Validation::Valid { frozen, marker }) => {
            return Ok(FenceOutcome::Held(Box::new(HeldFence {
                upload_id: upload_id.clone(),
                attempts_before: opened.attempts(),
                placed,
                named: request.parts.clone(),
                frozen,
                bucket_marker: marker,
            })))
        }
        Ok(Validation::Rejected(rejection)) => ReleaseFor::Rejection(rejection),
        // Never answered as a rejection; see the module doc.
        Err(cause) => ReleaseFor::Fault(cause),
    };
    // No segment batch was sent, so the release is from cursor 0: `attempts` given back.
    let mut own = OwnFence::new(upload_id, &session_key, &placed);
    release(store, &mut own, why, fingerprint.as_ref().ok())
        .await
        .map(FenceOutcome::Answered)
}

/// Whether `found`, a read of the session, is `Open` with more attempts spent than `attempts`.
///
/// Only a fence spends an attempt, and every fence moves the epoch forward, so such a session is
/// past every state a fence built on a session with `attempts` spent requires. Of the ways a fence
/// comes down, only one that keeps its attempt leaves an `Open` session with more of them spent:
/// the reaper's `W_completing` rollback (#625), or a release once a segment batch landed (module
/// doc, *The release once a segment landed*). A release that gives the attempt back does not.
fn open_and_charged_since(found: Option<&Bytes>, attempts: u32) -> bool {
    let Some(session) = found.and_then(|found| decode_session_record(found).ok()) else {
        return false;
    };
    matches!(session.state(), SessionState::Open {}) && session.attempts() > attempts
}

/// Decision 3's answer for a session a Complete found (`0016:976`), or `None` when it may fence:
/// a session for another target is no upload this request can complete, and an `Open` one still
/// refuses a list [`complete_fingerprint`] refused. The state answers first, so a list out of order
/// against a `Completing` session is still `OperationAborted`.
fn answer_without_fence(
    session: &SessionRecord,
    request: &CompleteRequest<'_>,
    fingerprint: &Result<Digest, RecordError>,
) -> Result<Option<CompleteOutcome>, RecordError> {
    if session.parent() != request.parent || session.object() != request.object {
        return Ok(Some(CompleteOutcome::Refused(Refusal::NoSuchUpload)));
    }
    Ok(
        match complete_answer(Some(session.state()), fingerprint.as_ref().ok()) {
            CompleteAnswer::Fences => match fingerprint {
                Ok(_) => None,
                Err(invalid) => Some(CompleteOutcome::Refused(order_refusal(invalid)?)),
            },
            CompleteAnswer::AlreadyCompleted(publication) => {
                Some(CompleteOutcome::AlreadyCompleted(publication))
            }
            CompleteAnswer::Refused(refusal) => Some(CompleteOutcome::Refused(refusal)),
        },
    )
}

/// The protocol's answer to the ordering error [`complete_fingerprint`] returned (`0016:707`, the
/// mapping [`InvalidPart::OutOfOrder`] documents), at the part it names. Any other error is handed
/// back: [`NamedParts`] cannot hold the one other list the fingerprint refuses, an empty one.
fn order_refusal(invalid: &RecordError) -> Result<Refusal, RecordError> {
    let (RecordError::DuplicatePart { part_number }
    | RecordError::PartsOutOfOrder { part_number, .. }) = invalid
    else {
        return Err(invalid.clone());
    };
    Ok(Refusal::InvalidPart {
        part_number: PartNumber::new(*part_number)?,
        reason: InvalidPart::OutOfOrder,
    })
}

/// What a validation found.
enum Validation {
    /// Every named part is present with the named digest, and the bucket marker is present.
    Valid {
        /// The part set read.
        frozen: BTreeMap<PartNumber, PartSummary>,
        /// The bucket marker bytes read.
        marker: Bytes,
    },
    /// The request is rejected with this refusal.
    Rejected(Refusal),
}

/// Validate `named` against the bucket and the session's part set as the store holds them now
/// (`0016:994-1000`): the bucket marker first, which is one read; then every `psum:` record of
/// the session; then each named part, in order, which must be present with the named digest.
/// The first part that is not answers [`Refusal::InvalidPart`].
async fn validate(
    store: &impl MetadataStore,
    upload_id: &UploadId,
    bucket_key: &[u8],
    named: &[(PartNumber, Digest)],
) -> Result<Validation, Stop> {
    let Some(marker) = store.get(bucket_key).await.map_err(Stop::Store)? else {
        return Ok(Validation::Rejected(Refusal::NoSuchBucket));
    };
    let frozen = read_part_set(store, upload_id).await?;
    for (part_number, digest) in named {
        let reason = match frozen.get(part_number) {
            None => InvalidPart::Absent,
            Some(summary) if summary.digest != *digest => InvalidPart::DigestMismatch,
            Some(_) => continue,
        };
        return Ok(Validation::Rejected(Refusal::InvalidPart {
            part_number: *part_number,
            reason,
        }));
    }
    Ok(Validation::Valid { frozen, marker })
}

/// Every `psum:<id>:` record, by part number, read in pages of [`PART_PAGE_LIMIT`] with
/// [`MetadataStore::scan_page`]. Each key must parse back to this upload, and each value must
/// decode ([`Stop::Record`]). Each part number must be strictly above the last one read, as the
/// paging contract's order and exclusive cursor make it (`crates/traits/src/lib.rs`, `scan_page`
/// clauses 1 and 2); one that is not is a store fault, and it is also what makes the walk end:
/// there are [`super::MAX_PART_NUMBER`] part numbers.
async fn read_part_set(
    store: &impl MetadataStore,
    upload_id: &UploadId,
) -> Result<BTreeMap<PartNumber, PartSummary>, Stop> {
    let prefix = psum_range(upload_id);
    let mut parts: BTreeMap<PartNumber, PartSummary> = BTreeMap::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (page, next) = store
            .scan_page(&prefix, after.as_deref(), PART_PAGE_LIMIT)
            .await
            .map_err(Stop::Store)?;
        // An empty page is terminal (clause 3).
        let Some((last, _)) = page.last() else {
            return Ok(parts);
        };
        let last = last.clone();
        for (key, value) in page {
            let (owner, part_number) = parse_psum_key(&key).map_err(Stop::Record)?;
            let ascending = parts
                .last_key_value()
                .is_none_or(|(previous, _)| part_number > *previous);
            if owner != *upload_id || !ascending {
                return Err(Stop::Store(BoxError::from(format!(
                    "scan_page over {} answered {} out of order or outside the range",
                    String::from_utf8_lossy(&prefix),
                    String::from_utf8_lossy(&key),
                ))));
            }
            parts.insert(
                part_number,
                decode_part_summary(&value).map_err(Stop::Record)?,
            );
        }
        if next.is_none() {
            return Ok(parts);
        }
        after = Some(last);
    }
}

/// The fence this Complete owns, the segment cursor its session is at as far as this Complete
/// knows, and the release that takes it down from there (module doc, *The release once a
/// segment landed*).
struct OwnFence<'a> {
    upload_id: &'a UploadId,
    session_key: &'a [u8],
    placed: &'a Placed,
    /// The segment cursor: 0 until a read finds a segment batch of this attempt landed.
    cursor: u32,
    /// The session's bytes at that cursor: what a release requires.
    bytes: Bytes,
}

impl<'a> OwnFence<'a> {
    /// The fence `placed`, at cursor 0: its bytes as the fence put them.
    fn new(upload_id: &'a UploadId, session_key: &'a [u8], placed: &'a Placed) -> Self {
        Self {
            upload_id,
            session_key,
            placed,
            cursor: 0,
            bytes: placed.bytes.clone(),
        }
    }

    /// The fence epoch, `E+1`.
    const fn epoch(&self) -> u64 {
        self.placed.epoch()
    }

    /// Whether `found`, a read of the session, is still this fence: at the cursor known, or at a
    /// later one, which this then follows. Never an earlier one: only this Complete's segment
    /// batches move the cursor, and only forward. `false` for anything else, which another writer
    /// put there.
    fn follow(&mut self, found: Option<&Bytes>) -> bool {
        let Some(found) = found else {
            return false;
        };
        match self.placed.cursor_of(found) {
            Some(cursor) if cursor >= self.cursor => {
                self.cursor = cursor;
                self.bytes = found.clone();
                true
            }
            _ => false,
        }
    }

    /// The release from where the session is (`0016:664`): from cursor 0, the session at
    /// `Open@E+2` with `attempts` given back; from a later cursor, with `attempts` as the fence
    /// left them and the attempt's `retire:records:{seg}` obligation, under `require_absent`.
    fn release_batch(&self) -> WriteBatch {
        let batch = WriteBatch::new().require(self.session_key.to_vec(), self.bytes.clone());
        if self.cursor == 0 {
            return batch.put(self.session_key.to_vec(), self.placed.restored.clone());
        }
        let segments =
            RetireObligation::attempt_segments(self.upload_id, self.placed.group.clone());
        batch
            .require_absent(segments.key())
            .put(self.session_key.to_vec(), self.placed.charged.clone())
            .put(segments.key(), metadata::encode(segments.payload()))
    }
}

/// Why a Complete is taking its own fence down.
enum ReleaseFor {
    /// The request is answered with this refusal once its fence is down: a list rejected behind
    /// the fence, or a publication whose rounds lost for good ([`Refusal::NoSuchBucket`],
    /// [`Refusal::OperationAborted`]). The release is what lets it be answered.
    Rejection(Refusal),
    /// The request cannot go on, for a reason that is no answer: validation or publication behind
    /// the fence could not finish (a store fault, a damaged record, a round the store refused for
    /// good). The release is best effort, and the error the request ends in keeps this cause; only
    /// a session moved to a state with an answer of its own ends it without one ([`moved`]).
    Fault(Stop),
}

impl ReleaseFor {
    /// How many release commits may answer `Conflict` with the session unchanged.
    const fn rounds(&self) -> u32 {
        match self {
            Self::Rejection(_) => RELEASE_ROUNDS,
            Self::Fault(_) => 1,
        }
    }

    /// The cause a stopped error reports when the release itself went wrong with `failure`. A fault
    /// keeps its own: it is what stopped the request, and the release was only an attempt to clean
    /// up after it.
    fn cause(self, failure: Stop) -> Stop {
        match self {
            Self::Rejection(_) => failure,
            Self::Fault(cause) => cause,
        }
    }

    /// The fault an in-doubt release followed: an in-doubt error's cause is the release's unknown
    /// outcome, so this is reported beside it.
    fn into_fault(self) -> Option<Stop> {
        match self {
            Self::Rejection(_) => None,
            Self::Fault(cause) => Some(cause),
        }
    }

    /// How the request ends once the session is `Open` again, its fence down as `fence` says.
    fn end(self, own: &OwnFence<'_>, fence: FenceState) -> Result<CompleteOutcome, CompleteError> {
        match self {
            Self::Rejection(refusal) => Ok(CompleteOutcome::Refused(refusal)),
            Self::Fault(cause) => Err(stopped(own.upload_id, fence, cause)),
        }
    }
}

/// Release `own` (`0016:664`, `:994-1000`) from the cursor it is at, with at most
/// [`ReleaseFor::rounds`] commits answering `Conflict` on an unchanged session, and end the
/// request as `why` says. A `Conflict` that finds the session at a later cursor of this fence
/// sends the release again from there without spending a round; that happens at most once per
/// segment batch that landed, since the cursor only moves forward.
async fn release(
    store: &impl MetadataStore,
    own: &mut OwnFence<'_>,
    why: ReleaseFor,
    fingerprint: Option<&Digest>,
) -> Result<CompleteOutcome, CompleteError> {
    let epoch = own.epoch();
    let upload_id = own.upload_id;
    let stands = |cause| stopped(upload_id, FenceState::Stands { epoch }, cause);
    let mut rounds = 0u32;
    loop {
        let cursor = own.cursor;
        match store.commit(own.release_batch()).await {
            Ok(CommitOutcome::Committed) => return why.end(own, FenceState::Released { epoch }),
            Ok(CommitOutcome::Conflict) => {
                let now = match store.get(own.session_key).await {
                    Ok(now) => now,
                    // The conflict may mean another writer moved the session; no read says so.
                    Err(fault) => {
                        let cause = why.cause(Stop::Store(fault));
                        return Err(stopped(own.upload_id, FenceState::Unknown { epoch }, cause));
                    }
                };
                if !own.follow(now.as_ref()) {
                    return moved(own, now, why, fingerprint);
                }
                // A segment batch of this fence landed that this Complete had not read back: the
                // release goes again from the cursor the session holds.
                if own.cursor > cursor {
                    continue;
                }
                // Nothing a read can see explains the conflict (on TiKV, a lock): spend the round.
                rounds += 1;
                if rounds == why.rounds() {
                    return Err(stands(why.cause(Stop::ReleaseRoundsSpent { rounds })));
                }
            }
            Err(fault) => {
                return Err(match fault.downcast_ref::<CommitUnknownResult>() {
                    Some(unknown) => in_doubt(
                        own.upload_id,
                        DoubtfulCommit::Release { epoch },
                        unknown,
                        why.into_fault(),
                    ),
                    // A definite failure of the release: the fence stands.
                    None => stands(why.cause(Stop::Store(fault))),
                });
            }
        }
    }
}

/// End a request whose fence another writer moved the session off before this Complete released
/// it, from the state it moved to (`0016:976`). Only the reaper's rollback (`Open`) and a teardown
/// fence (`Aborting`) move a `Completing` session (`0016:664-666`); an `Open` session is not fenced
/// by this Complete any more, so the request ends as `why` says. A fault's error ends here too, for
/// `Open` and for a torn record; every other state answers, and that answer drops the fault.
fn moved(
    own: &OwnFence<'_>,
    found: Option<Bytes>,
    why: ReleaseFor,
    fingerprint: Option<&Digest>,
) -> Result<CompleteOutcome, CompleteError> {
    let epoch = own.epoch();
    let Some(bytes) = found else {
        return Ok(CompleteOutcome::Refused(Refusal::NoSuchUpload));
    };
    let session = match decode_session_record(&bytes) {
        Ok(session) => session,
        Err(torn) => {
            let cause = why.cause(Stop::Record(torn));
            return Err(stopped(own.upload_id, FenceState::Gone { epoch }, cause));
        }
    };
    match complete_answer(Some(session.state()), fingerprint) {
        CompleteAnswer::Fences => why.end(own, FenceState::Gone { epoch }),
        CompleteAnswer::AlreadyCompleted(publication) => {
            Ok(CompleteOutcome::AlreadyCompleted(publication))
        }
        CompleteAnswer::Refused(refusal) => Ok(CompleteOutcome::Refused(refusal)),
    }
}

/// What publication needs beyond the fence's [`CompleteRequest`]. Every field is the caller's:
/// core mints no inode id and reads no clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishRequest {
    /// The inode a flip that finds the target name free creates, minted by the caller as for any
    /// new object (`write::commit_create` takes its id the same way). A flip that finds the name
    /// bound supersedes the inode the dirent names and does not use this one. A flip that lost
    /// wrote nothing, so every flip of one request may offer it again.
    pub fresh_inode: InodeId,
    /// The publication stamp, in milliseconds of the clock [`CompleteRequest::clock_source`]
    /// names: the `Completed` session's `completed_at_millis`, and the root's `modified` before
    /// [`metadata::overwrite_modified`] orders it after a prior's (module doc, *Runtime-free*).
    pub completed_at_millis: u64,
}

/// **`CompleteMultipartUpload`** (#658): fence the session as [`fence_complete`] does, then
/// publish the named parts (`0016:662-663`): one flat root, or past
/// [`MAX_MAP_CHUNKS`](super::MAX_MAP_CHUNKS) chunks the attempt's `seg:` records and then the
/// root that names them, in a flip that also moves the session to `Completed` and installs its
/// `retire:` obligations, all in one batch. Answers in the landed vocabulary
/// ([`CompleteOutcome`], [`Refusal`]); the module doc's *Publication* states the segment batches
/// and the flip, their preconditions, and what a lost or refused round does.
///
/// * The fence phase's answers and errors are this verb's too.
/// * A flip that lands answers [`CompleteOutcome::Published`]. A later identical request answers
///   [`CompleteOutcome::AlreadyCompleted`] with that publication, from the `Completed` record.
/// * A publication that loses for good answers [`Refusal::NoSuchBucket`] (the bucket marker is
///   gone) or [`Refusal::OperationAborted`] (another writer took the fence, or [`R_PUBLISH`]
///   rounds lost), with the fence released and the session `Open`, unless another writer moved
///   it. A fence whose segment batch landed stays charged, and its release installs the
///   attempt's `retire:records:{seg}` obligation (module doc, *The release once a segment
///   landed*).
/// * A round whose outcome the store could not report is settled by reading the session
///   (module doc, *Unknown outcomes*): a flip of this Complete found landed answers
///   [`CompleteOutcome::Published`] with that flip's publication, and this fence found at a
///   cursor is resumed from there. What no read settles is [`CompleteError::InDoubt`], and while
///   a batch may still land nothing is released.
///
/// # Termination
///
/// The fence phase's bound, then one read of each named part's record, then at most
/// [`R_PUBLISH`] rounds, a round whose outcome is unknown among them. Each round follows one
/// read of the target (its dirent and, when bound, the inode it names); it is the committer's
/// segment batches, at most one per segment and at most
/// [`crate::metadata::MAX_ROOT_SEGMENTS`] of them, its two reads of the attempt's bounded `seg:`
/// range, and one flip. A round that loses or whose outcome is unknown is followed by one read
/// of the session and one of the bucket marker, and a round the store refuses for good after a
/// segment batch may have landed by one read of the session. Then at most [`RELEASE_ROUNDS`]
/// release commits that find the session unchanged, plus at most one per segment batch that
/// landed, each followed by at most one read of the session. Each store call is bounded by the
/// store's own contract.
///
/// **Not cancel-safe once a commit is sent**, as [`fence_complete`] is not: a fence, a segment
/// batch or a flip left unanswered by a dropped future is the reaper's to settle.
pub async fn complete_upload(
    store: &impl MetadataStore,
    upload_id: &UploadId,
    request: &CompleteRequest<'_>,
    publish: &PublishRequest,
) -> Result<CompleteOutcome, CompleteError> {
    match fence_complete(store, upload_id, request).await? {
        FenceOutcome::Answered(outcome) => Ok(outcome),
        FenceOutcome::Held(fence) => publish_held(store, &fence, request.bucket, publish).await,
    }
}

/// Publish what `fence` holds (module doc, *Publication*).
async fn publish_held(
    store: &impl MetadataStore,
    fence: &HeldFence,
    bucket: &str,
    publish: &PublishRequest,
) -> Result<CompleteOutcome, CompleteError> {
    let session_key = mpu_key(&fence.upload_id);
    let bucket_key = metadata::bucket_key(bucket);
    let placed = &fence.placed;
    let mut own = OwnFence::new(&fence.upload_id, &session_key, placed);
    // The list was validated behind the fence, so neither digest can refuse it.
    let named = fence.named_parts();
    let identity = complete_fingerprint(named)
        .and_then(|fingerprint| Ok((fingerprint, multipart_etag(named)?)));
    let (fingerprint, etag) = match identity {
        Ok(identity) => identity,
        Err(torn) => return give_up(store, &mut own, Stop::Record(torn), None).await,
    };
    let lost = |refusal| ReleaseFor::Rejection(refusal);
    let plan = match FlipPlan::read(store, fence).await {
        Ok(plan) => plan,
        Err(cause) => return give_up(store, &mut own, cause, Some(&fingerprint)).await,
    };
    let parent = placed.session.parent();
    let name = placed.session.object();
    let mut target = match read_target(store, parent, name).await {
        Ok(target) => target,
        Err(cause) => return give_up(store, &mut own, cause, Some(&fingerprint)).await,
    };
    let meta = ObjectMeta {
        etag: Some(etag.to_string()),
        content_type: placed.session.content_type().map(str::to_owned),
        modified: Some(publish.completed_at_millis),
    };
    // Each segment batch's contribution: the session at the cursor the batch starts from, moved
    // to the count it leaves durable (`0016:613-620`, `:662`). A cursor past the decode bound has
    // no record, so its batch pins nothing and the committer refuses it before writing
    // ([`PublicationError::Unfenced`]); no plan the committer accepts reaches one.
    let step = |info: SegmentBatchInfo| match (placed.at(info.first_index), placed.at(info.written))
    {
        (Some(from), Some(to)) => WriteBatch::new()
            .require(session_key.clone(), from)
            .put(session_key.clone(), to),
        _ => WriteBatch::new(),
    };
    let mut doubt = Doubt::default();
    // Every flip of this Complete whose outcome the store could not report: the session record it
    // puts, and the publication that record names. Each names this fence's epoch and the version
    // that flip offered, and only this Complete flips from this fence, so a read that finds one of
    // them has found that flip landed.
    let mut unknown_flips: Vec<(Bytes, Publication)> = Vec::new();
    let mut rounds = 0u32;
    loop {
        rounds += 1;
        let flip = match plan.flip(fence, &bucket_key, &target, publish, (etag, fingerprint)) {
            Ok(flip) => flip,
            Err(cause) => {
                let why = ReleaseFor::Fault(cause);
                return end_publication(store, &mut own, &doubt, why, &fingerprint).await;
            }
        };
        // A round that resumes below the plan's count sends segment batches before its flip.
        let writes_segments = own.cursor < plan.segments;
        let publication = staged_publication::Publication {
            parent,
            name,
            root: flip.root,
            group: placed.group.clone(),
            chunks: &plan.chunks,
            meta: &meta,
            fence_key: &session_key,
            segment_contribution: &step,
            resume_from: own.cursor,
            byte_budget: None,
            ops_budget: None,
            flip_contribution: flip.contribution,
        };
        match staged_publication::publish(store, &publication).await {
            Ok(CommitOutcome::Committed) => {
                return Ok(CompleteOutcome::Published(flip.publication))
            }
            Ok(CommitOutcome::Conflict) => {}
            Err(failure) => {
                // Never read as `Conflict`: the read below settles it, or the request ends in
                // doubt. Which batch of the round it was is not known, so neither are the bytes it
                // requires: that read pins them (module doc, *Unknown outcomes*).
                if let Some(unknown) = failure.downcast_ref::<CommitUnknownResult>() {
                    let epoch = own.epoch();
                    let commit = if writes_segments {
                        DoubtfulCommit::Publication { epoch }
                    } else {
                        DoubtfulCommit::Flip { epoch }
                    };
                    doubt.observe(commit, unknown, None);
                    unknown_flips.push((flip.completed, flip.publication));
                } else {
                    // Refused before anything was written, or a round the store refused for good:
                    // no other round, and one release unless a batch of this Complete may still
                    // land.
                    let cause = match failure.downcast_ref::<PublicationError>() {
                        Some(refused) => Stop::Publication(refused.clone()),
                        None => Stop::Store(failure),
                    };
                    if let Some(doubtful) = doubt.in_doubt(&fence.upload_id) {
                        return Err(doubtful);
                    }
                    // A segment batch of the round may have landed first: the release goes from
                    // the cursor the session holds. A read that fails here leaves that to the
                    // release.
                    if writes_segments {
                        if let Ok(session) = store.get(&session_key).await {
                            if !own.follow(session.as_ref()) {
                                let why = ReleaseFor::Fault(cause);
                                return moved(&own, session, why, Some(&fingerprint));
                            }
                        }
                    }
                    return give_up(store, &mut own, cause, Some(&fingerprint)).await;
                }
            }
        }
        // The round lost, or its outcome is unknown: judged from what the store holds now, which
        // also says how far the segment phase got. A read that fails leaves an unknown outcome
        // unsettled, so that ends in doubt.
        let session = match store.get(&session_key).await {
            Ok(session) => session,
            Err(failure) => {
                let why = ReleaseFor::Fault(Stop::Store(failure));
                return end_publication(store, &mut own, &doubt, why, &fingerprint).await;
            }
        };
        if let Some((_, landed)) = unknown_flips
            .iter()
            .find(|(completed, _)| session.as_ref() == Some(completed))
        {
            return Ok(CompleteOutcome::Published(landed.clone()));
        }
        // Off this fence, nothing of this Complete can land: every batch of it requires the
        // session at this fence's epoch, which never comes back. For a lost round, `moved` errs
        // only on a record that does not decode, which says nothing of what landed before it, so
        // that stop settles no doubt: it is the in-doubt error while one stands.
        if !own.follow(session.as_ref()) {
            let why = lost(Refusal::OperationAborted);
            return moved(&own, session, why, Some(&fingerprint))
                .map_err(|stop| doubt.end(&fence.upload_id, stop));
        }
        doubt.settle(&own.bytes);
        let marker = match store.get(&bucket_key).await {
            Ok(marker) => marker,
            Err(failure) => {
                let why = ReleaseFor::Fault(Stop::Store(failure));
                return end_publication(store, &mut own, &doubt, why, &fingerprint).await;
            }
        };
        if marker.as_ref() != Some(&fence.bucket_marker) {
            let why = lost(Refusal::NoSuchBucket);
            return end_publication(store, &mut own, &doubt, why, &fingerprint).await;
        }
        if rounds == R_PUBLISH {
            let why = lost(Refusal::OperationAborted);
            return end_publication(store, &mut own, &doubt, why, &fingerprint).await;
        }
        target = match read_target(store, parent, name).await {
            Ok(target) => target,
            Err(cause) => {
                let why = ReleaseFor::Fault(cause);
                return end_publication(store, &mut own, &doubt, why, &fingerprint).await;
            }
        };
    }
}

/// End a publication that cannot go on with `cause`: one release, as for a fault behind the fence
/// ([`ReleaseFor::Fault`]), so the error keeps `cause` and says what became of the fence.
async fn give_up(
    store: &impl MetadataStore,
    own: &mut OwnFence<'_>,
    cause: Stop,
    fingerprint: Option<&Digest>,
) -> Result<CompleteOutcome, CompleteError> {
    release(store, own, ReleaseFor::Fault(cause), fingerprint).await
}

/// End a publication round that cannot go on, as `why` says: the in-doubt error while `doubt`
/// keeps one, with nothing more sent, so no release races a batch of this Complete that may still
/// land (module doc, *Unknown outcomes*); else the release `why` asks for.
async fn end_publication(
    store: &impl MetadataStore,
    own: &mut OwnFence<'_>,
    doubt: &Doubt,
    why: ReleaseFor,
    fingerprint: &Digest,
) -> Result<CompleteOutcome, CompleteError> {
    if let Some(doubtful) = doubt.in_doubt(own.upload_id) {
        return Err(doubtful);
    }
    release(store, own, why, Some(fingerprint)).await
}

/// What every round of one publication shares: the object's chunk list, the segment count the
/// committer plans for it, the session the flip requires, and the two part-scoped obligations,
/// all fixed by the frozen set and the named list.
struct FlipPlan {
    chunks: Vec<ChunkRef>,
    /// How many `seg:` records the committer writes for `chunks`
    /// ([`staged_publication::planned_segments`]), 0 for a flat map: the cursor the segment phase
    /// leaves the session at.
    segments: u32,
    /// The session at that cursor: what the flip requires.
    ready: Bytes,
    /// `retire:records:` for the published parts.
    published: RetireObligation,
    /// `retire:bytes:` for the frozen parts left unnamed, if any.
    unnamed: Option<RetireObligation>,
}

/// One flip: the root it publishes, the contribution that rides with it, the session record that
/// contribution puts, and the answer it earns.
struct Flip<'a> {
    root: RootTarget<'a>,
    contribution: WriteBatch,
    /// The `Completed` record the flip puts at `mpu:<id>`: what a read finds once it landed.
    completed: Bytes,
    publication: Publication,
}

impl FlipPlan {
    /// Read each named part's `part:` record, in the list's order, and require it to agree with
    /// the summary the fence froze ([`Stop::PartDisagrees`]): its chunk count, length, digest and
    /// commit stamp. Validation behind the fence checked that summary's digest against the list,
    /// so the record carries the digest the list names. Their chunk lists, concatenated in that
    /// order, are the object. The segment count is the committer's, never re-derived from the
    /// chunk count: wide chunk refs close a segment early.
    async fn read(store: &impl MetadataStore, fence: &HeldFence) -> Result<Self, Stop> {
        let upload_id = &fence.upload_id;
        let named = fence.named_parts();
        let mut chunks = Vec::new();
        for (part_number, _) in named {
            let value = store
                .get(&part_key(upload_id, *part_number))
                .await
                .map_err(Stop::Store)?;
            let record = value
                .as_deref()
                .map(decode_part_record)
                .transpose()
                .map_err(Stop::Record)?;
            let summary = fence.frozen.get(part_number);
            match record {
                Some(record) if Some(&record.summary()) == summary => {
                    chunks.extend_from_slice(record.chunks());
                }
                _ => {
                    return Err(Stop::PartDisagrees {
                        part_number: *part_number,
                    })
                }
            }
        }
        let segments = staged_publication::planned_segments(&chunks).map_err(Stop::Publication)?;
        let ready = fence.placed.at(segments).ok_or(Stop::Record(
            RecordError::SegmentCursorUnaddressable {
                segments_written: segments,
            },
        ))?;
        let epoch = fence.epoch();
        let published =
            PartNumberSet::from_numbers(named.iter().map(|(part_number, _)| *part_number))
                .ok_or(Stop::Record(RecordError::NoPartsNamed))?;
        let unnamed = fence
            .frozen
            .keys()
            .filter(|part_number| named.binary_search_by_key(part_number, |(n, _)| n).is_err())
            .copied();
        Ok(Self {
            chunks,
            segments,
            ready,
            published: RetireObligation::published_parts(upload_id, epoch, published),
            unnamed: PartNumberSet::from_numbers(unnamed)
                .map(|parts| RetireObligation::unnamed_parts(upload_id, epoch, parts)),
        })
    }

    /// The flip onto `target`: version 1 under [`PublishRequest::fresh_inode`] for a free name, or
    /// the prior's version + 1 for a bound one, recomputed from the prior read for this flip
    /// (`0016:926-941`); the session required at the cursor the segment phase leaves and put at
    /// `Completed` recording that publication; the bucket marker validated behind the fence
    /// pinned; and each obligation under `require_absent`.
    fn flip<'a>(
        &self,
        fence: &HeldFence,
        bucket_key: &[u8],
        target: &'a Target,
        publish: &PublishRequest,
        identity: (MultipartEtag, Digest),
    ) -> Result<Flip<'a>, Stop> {
        let (etag, fingerprint) = identity;
        let (root, inode, version, superseded) = match target {
            Target::Free => {
                let id = publish.fresh_inode;
                (RootTarget::Fresh { id }, id, 1, None)
            }
            Target::Bound { id, prior } => {
                let version = prior.version.checked_add(1).ok_or(Stop::Publication(
                    PublicationError::VersionOverflow { inode: *id },
                ))?;
                let superseded = superseded(*id, prior).map_err(Stop::Record)?;
                (
                    RootTarget::Supersede { id: *id, prior },
                    *id,
                    version,
                    superseded,
                )
            }
        };
        let completion = Completion {
            inode,
            version,
            etag,
            completed_at_millis: publish.completed_at_millis,
            complete_fingerprint: fingerprint,
        };
        let publication = Publication::of(&completion);
        let completed = fence
            .placed
            .session
            .completed(completion)
            .ok_or(Stop::EpochExhausted)?;
        let session_key = mpu_key(&fence.upload_id);
        let obligations = [
            Some(&self.published),
            self.unnamed.as_ref(),
            superseded.as_ref(),
        ];
        let mut contribution = WriteBatch::new()
            .require(session_key.clone(), self.ready.clone())
            // ADR-0046 §4: the bucket is the one validated behind the fence, byte for byte.
            .require(bucket_key.to_vec(), fence.bucket_marker.clone());
        for obligation in obligations.iter().flatten() {
            contribution = contribution.require_absent(obligation.key());
        }
        let completed = metadata::encode(&completed);
        contribution = contribution.put(session_key, completed.clone());
        for obligation in obligations.iter().flatten() {
            contribution =
                contribution.put(obligation.key(), metadata::encode(obligation.payload()));
        }
        Ok(Flip {
            root,
            contribution,
            completed,
            publication,
        })
    }
}

/// What the publish target's name is bound to, read for one flip.
enum Target {
    /// No dirent: the flip creates the inode and the dirent.
    Free,
    /// The dirent names inode `id`, whose record is `prior`.
    Bound { id: InodeId, prior: InodeRecord },
}

/// Read the dirent `parent`/`name` and, when it is bound, the inode it names.
async fn read_target(
    store: &impl MetadataStore,
    parent: InodeId,
    name: &str,
) -> Result<Target, Stop> {
    let dirent = store
        .get(&metadata::dirent_key(parent, name))
        .await
        .map_err(Stop::Store)?;
    let Some(dirent) = dirent else {
        return Ok(Target::Free);
    };
    let DirentRecord { inode: id } = metadata::decode(&dirent).map_err(Stop::Target)?;
    let inode = store
        .get(&metadata::inode_key(id))
        .await
        .map_err(Stop::Store)?;
    let Some(inode) = inode else {
        return Err(Stop::Target(BoxError::from(format!(
            "the dirent names inode {id}, which has no record"
        ))));
    };
    let prior = metadata::decode(&inode).map_err(Stop::Target)?;
    Ok(Target::Bound { id, prior })
}

/// The `{generation}` obligation superseding `prior` (inode `id`) owes: none for a prior that names
/// no chunk, since an obligation naming an empty list would not decode (`0016:663`, `:910-925`).
fn superseded(id: InodeId, prior: &InodeRecord) -> Result<Option<RetireObligation>, RecordError> {
    let map = match &prior.chunk_map {
        ChunkMap::Flat(chunks) if chunks.is_empty() => return Ok(None),
        ChunkMap::Flat(chunks) => RetiredMap::Flat(chunks.clone()),
        ChunkMap::Segmented(map) => RetiredMap::Segmented(map.group().clone()),
    };
    RetireObligation::superseded_generation(id, prior.version, map).map(Some)
}

/// One commit of this Complete whose outcome the store could not report.
#[derive(Debug)]
struct Unknown {
    /// The commit it is about.
    commit: DoubtfulCommit,
    /// What the store said.
    outcome: CommitUnknownResult,
}

/// What a Complete knows about its own commits whose outcome the store could not report: the two
/// kinds of doubt an Abort keeps (`abort.rs:259-273`, `:394-451`), each with the commit it is about
/// (module doc, *Unknown outcomes*).
#[derive(Debug, Default)]
struct Doubt {
    /// **Held**: the first outcome that said `may_still_commit`, kept and never replaced by a later
    /// one, and the session bytes a batch of this Complete that may still land requires at
    /// `mpu:<id>`. `None` until the read after it pins them, when the caller cannot name them (a
    /// publication round). Only a read that finds the session no longer those bytes clears it:
    /// every batch requires exact bytes, and neither the epoch nor the segment cursor ever goes
    /// back.
    held: Option<(Unknown, Option<Bytes>)>,
    /// **Unsettled**: the latest out-of-flight outcome no successful read has followed yet.
    unsettled: Option<Unknown>,
}

impl Doubt {
    /// A commit about `commit` answered `outcome`. `required` is the session bytes its batch
    /// requires, or `None` when the read that follows pins them ([`Self::settle`]).
    fn observe(
        &mut self,
        commit: DoubtfulCommit,
        outcome: &CommitUnknownResult,
        required: Option<Bytes>,
    ) {
        let unknown = Unknown {
            commit,
            outcome: outcome.clone(),
        };
        if !outcome.may_still_commit {
            self.unsettled = Some(unknown);
        } else if let Some((_, pinned)) = &mut self.held {
            // A later batch that may still land: it was sent after a read found the session at the
            // held bytes, so it requires those or later ones, and the read after it pins which.
            // The held outcome stays the first one; the bytes it is kept against move on.
            *pinned = required;
        } else {
            self.held = Some((unknown, required));
        }
    }

    /// Whether a commit of this Complete may still land.
    const fn holds(&self) -> bool {
        self.held.is_some()
    }

    /// A read of `mpu:<id>` that succeeded and found `found`, a record that decodes, and not a
    /// commit of this Complete that it can answer from: no out-of-flight commit of it is unsettled
    /// any more, a held one whose bytes are not pinned yet is kept against `found`, and one whose
    /// bytes are gone can never land. A record that does not decode is never passed here: it
    /// settles nothing (module doc, *Unknown outcomes*). Neither is a read that found no record,
    /// which is answered without a look at the doubt.
    fn settle(&mut self, found: &Bytes) {
        self.unsettled = None;
        let Some((_, pinned)) = &mut self.held else {
            return;
        };
        let kept = match pinned {
            Some(required) => found == required,
            None => {
                *pinned = Some(found.clone());
                true
            }
        };
        if !kept {
            self.held = None;
        }
    }

    /// The in-doubt error while a commit of this Complete may still land or was never settled:
    /// about the held outcome, else the unsettled one. `None` when nothing is in doubt.
    fn in_doubt(&self, upload_id: &UploadId) -> Option<CompleteError> {
        let unknown = self
            .held
            .as_ref()
            .map(|(unknown, _)| unknown)
            .or(self.unsettled.as_ref())?;
        Some(in_doubt(upload_id, unknown.commit, &unknown.outcome, None))
    }

    /// The error a Complete stopped with `otherwise` ends in: the in-doubt error while something
    /// is in doubt, else `otherwise`.
    fn end(&self, upload_id: &UploadId, otherwise: CompleteError) -> CompleteError {
        self.in_doubt(upload_id).unwrap_or(otherwise)
    }
}

fn answered(outcome: CompleteOutcome) -> FenceOutcome {
    FenceOutcome::Answered(outcome)
}

fn refused(refusal: Refusal) -> FenceOutcome {
    answered(CompleteOutcome::Refused(refusal))
}

fn stopped(upload_id: &UploadId, fence: FenceState, cause: Stop) -> CompleteError {
    CompleteError::Stopped(Box::new(Stopped {
        upload_id: upload_id.clone(),
        fence,
        cause,
    }))
}

fn in_doubt(
    upload_id: &UploadId,
    commit: DoubtfulCommit,
    unknown: &CommitUnknownResult,
    fault_behind_fence: Option<Stop>,
) -> CompleteError {
    CompleteError::InDoubt(Box::new(InDoubt {
        upload_id: upload_id.clone(),
        commit,
        may_still_commit: unknown.may_still_commit,
        outcome: unknown.clone(),
        fault_behind_fence,
    }))
}
