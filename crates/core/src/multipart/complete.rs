//! `CompleteMultipartUpload` (#658): its **fence phase** (#1007, slice 1 of 5) and the **flat
//! publication** composed onto it (#1008, slice 2). [`fence_complete`] is the fence, the
//! validation of the named parts against the part set the fence froze, and the release of a
//! rejected request's fence; it publishes nothing, and hands a valid request's fence back as a
//! [`HeldFence`]. [`complete_upload`] is the verb: that phase, then the publication of what the
//! fence holds as one flat root (the *Root flip* row, `0016:663`; module doc *Publication*).
//! Segmented publication is #1009's, and settling an unknown outcome #1010's.
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
//! `attempts` bounds how many `seg:` epochs one session can mint (`0016:661`, `:1477`). A fence
//! that is released before it wrote a segment minted none, so the release puts `attempts` back
//! to the value read before the fence. A client error therefore never spends the budget, and an
//! `Open` session can always be fenced again: three typos do not leave a correct fourth request
//! answering [`Refusal::CompleteAttemptsExhausted`], which is the wedge #636's patch had.
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
//! may leave it standing (**C-1**, `docs/principles.md` §5):
//!
//! * [`CompleteError::InDoubt`]: a fence, release or flip commit of this Complete came back with
//!   an unknown outcome ([`CommitUnknownResult`]). It is returned at once, with nothing sent after
//!   it; settling such an outcome by reading the session again is #1010's.
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
//! for a fault of the store's. It tries with **one** release commit, the same exact-bytes check and
//! the same record as a rejection's release:
//!
//! | The release | The error |
//! |---|---|
//! | lands | [`FenceState::Released`]: the session is `Open@E+2` with `attempts` as read before the fence |
//! | fails for good, or answers `Conflict` with the session still the fence's bytes | [`FenceState::Stands`] |
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
//! # Publication (#1008)
//!
//! [`complete_upload`] fences as [`fence_complete`] does, with one more check before and behind
//! the fence: the named parts' chunks, counted from their summaries, must fit one flat map
//! ([`MAX_MAP_CHUNKS`]). A list past it stops with [`Stop::SegmentedUnsupported`]: with no commit
//! when the check before the fence sees it, and after one release when only the frozen set does.
//! #1009 replaces that refusal with segmented publication.
//!
//! Behind the held fence it reads each named part's `part:` record, in the list's order, and
//! requires it to agree with the summary the fence froze, which carries the digest the list
//! names ([`Stop::PartDisagrees`]). Those chunk lists, concatenated in that order, are the
//! object. It then reads the target: the dirent the session names and, when one is bound, the
//! inode it names. The flip is one batch through [`crate::staged_publication`], whose own rows
//! pin the dirent and the inode on both branches (both `require_absent` for a free name, both
//! exact bytes for a bound one) and write version 1, or the re-read prior's version + 1. The
//! flip's contribution, for an `Open@E` session fenced at `Completing@E+1`:
//!
//! | Precondition | Mutation |
//! |---|---|
//! | `require(mpu:<id> == the fence's bytes)` | the session at `Completed@E+2`, recording the inode, the version, the multipart ETag, `completed_at_millis` and the list's `complete_fingerprint` |
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
//! A flip that answers `Conflict` is judged from what the store then holds, in this order:
//!
//! | Found | Then |
//! |---|---|
//! | the session moved off the fence | the answer for its new state, as a release that finds it moved gets; an `Open` session answers [`Refusal::OperationAborted`] |
//! | the bucket marker absent, or not the bytes validated | release, then [`Refusal::NoSuchBucket`] |
//! | [`R_PUBLISH`] flips sent | release, then [`Refusal::OperationAborted`]; the session is `Open` and the client may retry |
//! | otherwise | read the target again and flip again, the version recomputed from the prior read now (`0016:926-941`) |
//!
//! Those releases are a rejection's, so a release that cannot land ends in the error that says
//! the fence still stands. A flip the store refuses for good (a definite error, neither `Conflict`
//! nor an unknown outcome) is followed by no other flip: the request tries one release, as for a
//! fault behind the fence (the table under *Ownership*), and ends [`CompleteError::Stopped`]
//! with that error as its cause and the fence's state. So does a fault reading the parts or the
//! target, a part or target record that is damaged, and a flip the committer refuses before
//! writing ([`Stop::Publication`]). A flip whose outcome is unknown ends
//! [`CompleteError::InDoubt`] at once, with nothing sent after it: it may have published, so no
//! release may follow it.
//!
//! # Runtime-free (ADR-0009), and one clock per session
//!
//! Core reads no clock, draws no randomness and never sleeps here. The fence stamp
//! `fenced_at_millis` arrives in the [`CompleteRequest`], with the `clock_source` that names its
//! clock. That stamp belongs to the session's residency lifecycle: the reaper measures
//! `W_completing` from it (`0016:1005-1009`) under the clock the session record names, so a
//! request whose clock source is not the session's is stopped before anything is written
//! ([`Stop::ForeignClock`], `0016:1966-1976`). A lost round is retried at once, with no backoff.
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
//! Production calls nothing here yet: the S3 verb that will is #670, once #1009–#1011 complete
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
    SessionRecord, UploadId, MAX_COMPLETE_ATTEMPTS, MAX_MAP_CHUNKS, R_PUBLISH,
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
/// exactly the fence's bytes, a conflict nothing a read can see explains (on TiKV, a lock another
/// writer held): a session moved off the fence ends the release and is answered from its new
/// state. Bounded so a Complete terminates with no
/// reaper running, at the same budget an Abort spends against the same key. A Complete that
/// spends it ends with [`Stop::ReleaseRoundsSpent`] and its fence standing.
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
    session: SessionRecord,
    bytes: Bytes,
    /// The bytes this fence's release puts, minted with the fence: `Open@E+2`, `attempts` as read
    /// before the fence.
    released: Bytes,
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
        self.session.epoch()
    }

    /// The session as the fence left it, at `Completing@E+1`.
    pub const fn session(&self) -> &SessionRecord {
        &self.session
    }

    /// The exact bytes the fence put at `mpu:<id>`: what every later batch of this attempt
    /// requires there.
    pub const fn session_bytes(&self) -> &Bytes {
        &self.bytes
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
    /// A fence, release or flip commit of this Complete may have landed, or may still land. Never
    /// "nothing was written".
    InDoubt(Box<InDoubt>),
    /// The Complete stopped, and no commit of it is in doubt.
    Stopped(Box<Stopped>),
}

/// What a [`CompleteError::InDoubt`] reports.
#[derive(Debug)]
pub struct InDoubt {
    /// The upload whose session is in doubt.
    pub upload_id: UploadId,
    /// Which commit's outcome the store could not report.
    pub commit: DoubtfulCommit,
    /// Whether that commit may still be applied **after** this Complete returned
    /// ([`CommitUnknownResult::may_still_commit`]).
    pub may_still_commit: bool,
    /// The store's report of the unknown outcome. It is the error's
    /// [`source`](std::error::Error::source).
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
    /// The fence `Open@E → Completing@epoch`: the session is still `Open@E`, or fenced by this
    /// Complete.
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
    /// batch with the root and its obligations: either the object is published and the session
    /// `Completed`, or neither is and the fence still stands.
    Flip {
        /// The epoch of the fence the flip requires.
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
    /// All [`RELEASE_ROUNDS`] release commits answered `Conflict` with the session still exactly
    /// the fence's bytes.
    ReleaseRoundsSpent {
        /// Release commits made.
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
    /// is also a flip the store refused for good: a definite error that is neither `Conflict`
    /// nor an unknown outcome.
    Store(BoxError),
    /// The named parts total `chunks` chunks by their summaries, past the `limit` one flat map
    /// holds ([`MAX_MAP_CHUNKS`]), and segmented publication is not supported yet.
    // deferred: #1009 — segmented publication; this stop is deleted with it.
    SegmentedUnsupported {
        /// The chunks the named parts' summaries total.
        chunks: u64,
        /// [`MAX_MAP_CHUNKS`].
        limit: u64,
    },
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
    /// The staged-publication committer refused the flip before writing anything (a value over
    /// the ceiling, a batch over its envelope, a version with no successor).
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
            Self::SegmentedUnsupported { chunks, limit } => write!(
                f,
                "the named parts total {chunks} chunks, past the {limit} one flat map holds, and \
                 segmented publication is not supported yet"
            ),
            Self::PartDisagrees { part_number } => write!(
                f,
                "part {part_number}'s part: record is absent or disagrees with the summary the \
                 fence froze"
            ),
            Self::Target(torn) => write!(f, "the publish target is not readable: {torn}"),
            Self::Publication(refused) => write!(f, "the committer refused the flip: {refused}"),
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
                | Stop::SegmentedUnsupported { .. }
                | Stop::PartDisagrees { .. } => None,
            },
        }
    }
}

// deferred: #660. Seeded Tier-0 DST coverage of this fence racing an Abort, a rival Complete and
// the reaper's rollback, and of the flip racing a `PutObject`, a bucket delete and the reaper, is
// #660's (out of #1007's and #1008's scope). Meanwhile the integration test
// `crates/core/tests/multipart_complete.rs` lands a rival fence, a teardown and a rollback at the
// fence's and the release's commit points, and an overwrite, a bucket delete, a refused flip and
// an unknown flip outcome at the flip's, through a store wrapper.

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
/// [`FENCE_ROUNDS`]. A [`CommitUnknownResult`] is [`CompleteError::InDoubt`] at once.
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
/// [`CommitUnknownResult`], which this function reports.
pub async fn fence_complete(
    store: &impl MetadataStore,
    upload_id: &UploadId,
    request: &CompleteRequest<'_>,
) -> Result<FenceOutcome, CompleteError> {
    fence(store, upload_id, request, Admit::Any).await
}

/// Which valid lists a fence is held for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admit {
    /// Every valid list: the fence phase on its own ([`fence_complete`]).
    Any,
    /// A valid list whose named parts fit one flat map, by their summaries: what
    /// [`complete_upload`] can publish.
    // deferred: #1009 — segmented publication; this arm is deleted with it.
    Flat,
}

impl Admit {
    /// The stop for a valid `named` list this fence is not held for, judged on the part set
    /// `frozen` read with it. Every part a valid list names is in that set.
    fn refuses(
        self,
        frozen: &BTreeMap<PartNumber, PartSummary>,
        named: &[(PartNumber, Digest)],
    ) -> Option<Stop> {
        match self {
            Self::Any => None,
            Self::Flat => {
                let chunks: u64 = named
                    .iter()
                    .filter_map(|(part_number, _)| frozen.get(part_number))
                    .map(|summary| u64::from(summary.chunks))
                    .sum();
                let limit = u64::from(MAX_MAP_CHUNKS);
                (chunks > limit).then_some(Stop::SegmentedUnsupported { chunks, limit })
            }
        }
    }
}

/// [`fence_complete`], holding the fence only for a list `admit` admits. A valid list it does not
/// admit stops with its [`Admit::refuses`] cause: before the fence, with nothing written; behind
/// it, after one release, as a fault behind the fence does (the module doc's table).
async fn fence(
    store: &impl MetadataStore,
    upload_id: &UploadId,
    request: &CompleteRequest<'_>,
    admit: Admit,
) -> Result<FenceOutcome, CompleteError> {
    let session_key = mpu_key(upload_id);
    let bucket_key = metadata::bucket_key(request.bucket);
    let named = request.parts.as_slice();
    // `Err` for a list that is not strictly ascending: no recorded fingerprint can match it, and
    // an `Open` session refuses it at the part it names.
    let fingerprint = complete_fingerprint(named);
    let not_placed = |cause| stopped(upload_id, FenceState::NotPlaced, cause);
    let mut rounds = 0u32;
    let mut found = store
        .get(&session_key)
        .await
        .map_err(|fault| not_placed(Stop::Store(fault)))?;
    let (opened, fenced, released) = loop {
        // `found` is always the result of a read that succeeded.
        let Some(read) = found else {
            return Ok(refused(Refusal::NoSuchUpload));
        };
        let opened = decode_session_record(&read).map_err(|torn| not_placed(Stop::Record(torn)))?;
        let answer = answer_without_fence(&opened, request, &fingerprint)
            .map_err(|torn| not_placed(Stop::Record(torn)))?;
        if let Some(answer) = answer {
            return Ok(answered(answer));
        }
        if opened.clock_source() != request.clock_source {
            return Err(not_placed(Stop::ForeignClock {
                session: opened.clock_source().to_owned(),
                request: request.clock_source.to_owned(),
            }));
        }
        if opened.attempts() >= MAX_COMPLETE_ATTEMPTS {
            return Ok(refused(Refusal::CompleteAttemptsExhausted));
        }
        // The release is minted with the fence, so no fence is placed that could not be taken
        // down again.
        let Some((fenced, released)) =
            opened
                .complete_fence(request.fenced_at_millis)
                .and_then(|fenced| {
                    let released = fenced.fence_released(opened.attempts())?;
                    Some((fenced, released))
                })
        else {
            return Err(not_placed(Stop::EpochExhausted));
        };
        // The optimization: a request already invalid is answered without a fence, and one this
        // fence would not be held for is stopped without one.
        match validate(store, upload_id, &bucket_key, named).await {
            Ok(Validation::Valid { frozen, .. }) => {
                if let Some(cause) = admit.refuses(&frozen, named) {
                    return Err(not_placed(cause));
                }
            }
            Ok(Validation::Rejected(refusal)) => return Ok(refused(refusal)),
            Err(cause) => return Err(not_placed(cause)),
        }
        if rounds == FENCE_ROUNDS {
            return Err(not_placed(Stop::FenceRoundsSpent { rounds }));
        }
        rounds += 1;
        let batch = WriteBatch::new()
            .require(session_key.clone(), read)
            .put(session_key.clone(), metadata::encode(&fenced));
        match store.commit(batch).await {
            Ok(CommitOutcome::Committed) => break (opened, fenced, released),
            // Answered from what the store holds now: moved, from its new state; still the
            // bytes read, the round is spent and the fence tried again.
            Ok(CommitOutcome::Conflict) => {
                found = store
                    .get(&session_key)
                    .await
                    .map_err(|fault| not_placed(Stop::Store(fault)))?;
            }
            Err(fault) => {
                return Err(match fault.downcast_ref::<CommitUnknownResult>() {
                    Some(unknown) => in_doubt(
                        upload_id,
                        DoubtfulCommit::Fence {
                            epoch: fenced.epoch(),
                        },
                        unknown,
                        None,
                    ),
                    None => not_placed(Stop::Store(fault)),
                });
            }
        }
    };

    // The fence is this Complete's: its commit answered `Committed`.
    let own = OwnFence {
        upload_id,
        session_key: &session_key,
        epoch: fenced.epoch(),
        bytes: metadata::encode(&fenced),
        released: metadata::encode(&released),
    };
    let why = match validate(store, upload_id, &bucket_key, named).await {
        Ok(Validation::Valid { frozen, marker }) => match admit.refuses(&frozen, named) {
            None => {
                return Ok(FenceOutcome::Held(Box::new(HeldFence {
                    upload_id: upload_id.clone(),
                    attempts_before: opened.attempts(),
                    session: fenced,
                    bytes: own.bytes,
                    released: own.released,
                    named: request.parts.clone(),
                    frozen,
                    bucket_marker: marker,
                })))
            }
            Some(cause) => ReleaseFor::Fault(cause),
        },
        Ok(Validation::Rejected(rejection)) => ReleaseFor::Rejection(rejection),
        // Never answered as a rejection; see the module doc.
        Err(cause) => ReleaseFor::Fault(cause),
    };
    release(store, &own, why, fingerprint.as_ref().ok())
        .await
        .map(FenceOutcome::Answered)
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

/// The fence this Complete owns, and the release that takes it down.
struct OwnFence<'a> {
    upload_id: &'a UploadId,
    session_key: &'a [u8],
    /// The fence epoch, `E+1`.
    epoch: u64,
    /// The bytes the fence put: `Completing@E+1`.
    bytes: Bytes,
    /// The bytes the release puts: `Open@E+2`, `attempts` as read before the fence.
    released: Bytes,
}

/// Why a Complete is taking its own fence down.
enum ReleaseFor {
    /// The request is answered with this refusal once its fence is down: a list rejected behind
    /// the fence, or a publication whose flip lost for good ([`Refusal::NoSuchBucket`],
    /// [`Refusal::OperationAborted`]). The release is what lets it be answered.
    Rejection(Refusal),
    /// The request cannot go on, for a reason that is no answer: validation or publication behind
    /// the fence could not finish (a store fault, a damaged record, a flip the store refused for
    /// good), or the list is one this fence is not held for ([`Admit::refuses`]). The release is
    /// best effort, and the error the request ends in keeps this cause; only a session moved to a
    /// state with an answer of its own ends it without one ([`moved`]).
    Fault(Stop),
}

impl ReleaseFor {
    /// How many commits the release may make.
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

/// Release `own` (`0016:664`, `:994-1000`) in at most [`ReleaseFor::rounds`] commits, and end the
/// request as `why` says.
async fn release(
    store: &impl MetadataStore,
    own: &OwnFence<'_>,
    why: ReleaseFor,
    fingerprint: Option<&Digest>,
) -> Result<CompleteOutcome, CompleteError> {
    let epoch = own.epoch;
    let stands = |cause| stopped(own.upload_id, FenceState::Stands { epoch }, cause);
    let mut rounds = 0u32;
    loop {
        rounds += 1;
        let batch = WriteBatch::new()
            .require(own.session_key.to_vec(), own.bytes.clone())
            .put(own.session_key.to_vec(), own.released.clone());
        match store.commit(batch).await {
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
                if now.as_ref() != Some(&own.bytes) {
                    return moved(own, now, why, fingerprint);
                }
                // Nothing a read can see explains the conflict (on TiKV, a lock): spend the round.
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
    let epoch = own.epoch;
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

/// **`CompleteMultipartUpload`** (#658, flat objects only until #1009): fence the session as
/// [`fence_complete`] does, then publish the named parts as one flat root (`0016:663`) in a flip
/// that also moves the session to `Completed` and installs its `retire:` obligations, all in one
/// batch. Answers in the landed vocabulary ([`CompleteOutcome`], [`Refusal`]); the module doc's
/// *Publication* states the flip, its preconditions, and what a lost or refused flip does.
///
/// * The fence phase's answers and errors are this verb's too, plus one stop: a list whose named
///   parts total more than [`MAX_MAP_CHUNKS`] chunks ([`Stop::SegmentedUnsupported`]), with no
///   commit when that shows before the fence and after one release when only the frozen set
///   shows it.
/// * A flip that lands answers [`CompleteOutcome::Published`]. A later identical request answers
///   [`CompleteOutcome::AlreadyCompleted`] with that publication, from the `Completed` record.
/// * A flip that loses for good answers [`Refusal::NoSuchBucket`] (the bucket marker is gone) or
///   [`Refusal::OperationAborted`] (another writer took the fence, or [`R_PUBLISH`] flips lost),
///   with the fence released and the session `Open`, unless another writer moved it.
///
/// # Termination
///
/// The fence phase's bound, then one read of each named part's record, then at most
/// [`R_PUBLISH`] flips. Each flip follows one read of the target (its dirent and, when bound, the
/// inode it names), and a flip that loses is followed by one read of the session and one of the
/// bucket marker. Then at most [`RELEASE_ROUNDS`] release commits, each followed by at most one
/// read of the session. Each store call is bounded by the store's own contract.
///
/// **Not cancel-safe once a commit is sent**, as [`fence_complete`] is not: a fence or a flip left
/// unanswered by a dropped future is the reaper's or #1010's to settle.
pub async fn complete_upload(
    store: &impl MetadataStore,
    upload_id: &UploadId,
    request: &CompleteRequest<'_>,
    publish: &PublishRequest,
) -> Result<CompleteOutcome, CompleteError> {
    match fence(store, upload_id, request, Admit::Flat).await? {
        FenceOutcome::Answered(outcome) => Ok(outcome),
        FenceOutcome::Held(fence) => publish_flat(store, &fence, request.bucket, publish).await,
    }
}

/// Publish what `fence` holds as one flat root (module doc, *Publication*).
async fn publish_flat(
    store: &impl MetadataStore,
    fence: &HeldFence,
    bucket: &str,
    publish: &PublishRequest,
) -> Result<CompleteOutcome, CompleteError> {
    let session_key = mpu_key(&fence.upload_id);
    let bucket_key = metadata::bucket_key(bucket);
    let own = OwnFence {
        upload_id: &fence.upload_id,
        session_key: &session_key,
        epoch: fence.epoch(),
        bytes: fence.bytes.clone(),
        released: fence.released.clone(),
    };
    // The list was validated behind the fence, so neither digest can refuse it.
    let named = fence.named_parts();
    let identity = complete_fingerprint(named)
        .and_then(|fingerprint| Ok((fingerprint, multipart_etag(named)?)));
    let (fingerprint, etag) = match identity {
        Ok(identity) => identity,
        Err(torn) => return give_up(store, &own, Stop::Record(torn), None).await,
    };
    let lost = |refusal| ReleaseFor::Rejection(refusal);
    let plan = match FlipPlan::read(store, fence).await {
        Ok(plan) => plan,
        Err(cause) => return give_up(store, &own, cause, Some(&fingerprint)).await,
    };
    let parent = fence.session.parent();
    let name = fence.session.object();
    let mut target = match read_target(store, parent, name).await {
        Ok(target) => target,
        Err(cause) => return give_up(store, &own, cause, Some(&fingerprint)).await,
    };
    let meta = ObjectMeta {
        etag: Some(etag.to_string()),
        content_type: fence.session.content_type().map(str::to_owned),
        modified: Some(publish.completed_at_millis),
    };
    let group = SegmentGroup::from_nonce(fence.session.segment_nonce().clone(), fence.epoch());
    let mut flips = 0u32;
    loop {
        flips += 1;
        let flip = match plan.flip(fence, &bucket_key, &target, publish, (etag, fingerprint)) {
            Ok(flip) => flip,
            Err(cause) => return give_up(store, &own, cause, Some(&fingerprint)).await,
        };
        let publication = staged_publication::Publication {
            parent,
            name,
            root: flip.root,
            group: group.clone(),
            chunks: &plan.chunks,
            meta: &meta,
            fence_key: &session_key,
            segment_contribution: &no_segments,
            resume_from: 0,
            byte_budget: None,
            ops_budget: None,
            flip_contribution: flip.contribution,
        };
        match staged_publication::publish(store, &publication).await {
            Ok(CommitOutcome::Committed) => {
                return Ok(CompleteOutcome::Published(flip.publication))
            }
            Ok(CommitOutcome::Conflict) => {}
            Err(fault) => {
                if let Some(unknown) = fault.downcast_ref::<CommitUnknownResult>() {
                    let commit = DoubtfulCommit::Flip { epoch: own.epoch };
                    return Err(in_doubt(&fence.upload_id, commit, unknown, None));
                }
                // Refused before anything was written, or a flip the store refused for good: no
                // other flip, one release.
                let cause = match fault.downcast_ref::<PublicationError>() {
                    Some(refused) => Stop::Publication(refused.clone()),
                    None => Stop::Store(fault),
                };
                return give_up(store, &own, cause, Some(&fingerprint)).await;
            }
        }
        // The flip lost: judged from what the store holds now.
        let session = match store.get(&session_key).await {
            Ok(session) => session,
            Err(fault) => {
                return give_up(store, &own, Stop::Store(fault), Some(&fingerprint)).await
            }
        };
        if session.as_ref() != Some(&own.bytes) {
            return moved(
                &own,
                session,
                lost(Refusal::OperationAborted),
                Some(&fingerprint),
            );
        }
        let marker = match store.get(&bucket_key).await {
            Ok(marker) => marker,
            Err(fault) => {
                return give_up(store, &own, Stop::Store(fault), Some(&fingerprint)).await
            }
        };
        if marker.as_ref() != Some(&fence.bucket_marker) {
            return release(store, &own, lost(Refusal::NoSuchBucket), Some(&fingerprint)).await;
        }
        if flips == R_PUBLISH {
            let why = lost(Refusal::OperationAborted);
            return release(store, &own, why, Some(&fingerprint)).await;
        }
        target = match read_target(store, parent, name).await {
            Ok(target) => target,
            Err(cause) => return give_up(store, &own, cause, Some(&fingerprint)).await,
        };
    }
}

/// End a publication that cannot go on with `cause`: one release, as for a fault behind the fence
/// ([`ReleaseFor::Fault`]), so the error keeps `cause` and says what became of the fence.
async fn give_up(
    store: &impl MetadataStore,
    own: &OwnFence<'_>,
    cause: Stop,
    fingerprint: Option<&Digest>,
) -> Result<CompleteOutcome, CompleteError> {
    release(store, own, ReleaseFor::Fault(cause), fingerprint).await
}

/// The segment-batch contribution of a flat publication. The committer asks for one only for a
/// map past [`MAX_MAP_CHUNKS`] chunks, and [`complete_upload`] never hands it one: the fence is
/// held only for a list whose summaries total at most that ([`Admit::Flat`]), and
/// [`FlipPlan::read`] requires each record to agree with its summary. Were it ever asked, this
/// contribution pins no fence, so the committer would refuse the publication before writing.
fn no_segments(_: SegmentBatchInfo) -> WriteBatch {
    WriteBatch::new()
}

/// What every flip of one publication shares: the object's chunk list and the two part-scoped
/// obligations, both fixed by the frozen set and the named list.
struct FlipPlan {
    chunks: Vec<ChunkRef>,
    /// `retire:records:` for the published parts.
    published: RetireObligation,
    /// `retire:bytes:` for the frozen parts left unnamed, if any.
    unnamed: Option<RetireObligation>,
}

/// One flip: the root it publishes, the contribution that rides with it, and the answer it earns.
struct Flip<'a> {
    root: RootTarget<'a>,
    contribution: WriteBatch,
    publication: Publication,
}

impl FlipPlan {
    /// Read each named part's `part:` record, in the list's order, and require it to agree with
    /// the summary the fence froze ([`Stop::PartDisagrees`]): its chunk count, length, digest and
    /// commit stamp. Validation behind the fence checked that summary's digest against the list,
    /// so the record carries the digest the list names. Their chunk lists, concatenated in that
    /// order, are the object.
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
            published: RetireObligation::published_parts(upload_id, epoch, published),
            unnamed: PartNumberSet::from_numbers(unnamed)
                .map(|parts| RetireObligation::unnamed_parts(upload_id, epoch, parts)),
        })
    }

    /// The flip onto `target`: version 1 under [`PublishRequest::fresh_inode`] for a free name, or
    /// the prior's version + 1 for a bound one, recomputed from the prior read for this flip
    /// (`0016:926-941`); the session at `Completed` recording that publication; the bucket marker
    /// validated behind the fence pinned; and each obligation under `require_absent`.
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
            .require(session_key.clone(), fence.bytes.clone())
            // ADR-0046 §4: the bucket is the one validated behind the fence, byte for byte.
            .require(bucket_key.to_vec(), fence.bucket_marker.clone());
        for obligation in obligations.iter().flatten() {
            contribution = contribution.require_absent(obligation.key());
        }
        contribution = contribution.put(session_key, metadata::encode(&completed));
        for obligation in obligations.iter().flatten() {
            contribution =
                contribution.put(obligation.key(), metadata::encode(obligation.payload()));
        }
        Ok(Flip {
            root,
            contribution,
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
