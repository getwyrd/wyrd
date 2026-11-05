//! `UploadPart`'s **commit half** (#1027, the first slice of the part commit; staging is
//! [`super::stage`]): turn a staged attempt into a committed part, or into a compensated stop, and
//! compose the whole verb, reserve → stage → commit ([`upload_part`]). Proposal 0016's *Part commit*
//! and *Live-session compensation* rows (`0016:660`, `:673`), the post-staging refusal rule
//! (`0016:1420-1431`) and the quiesce rule (`0016:1720-1733`).
//!
//! | Batch | Preconditions | Mutations |
//! |---|---|---|
//! | part commit | `require(mpu:<id> == Open@E)`, `require_absent(part:<id>:<n>)` or `require(part:<id>:<n> == prior)`, `require(slot:<id>:<k> == the bytes this attempt last wrote)`, and for a re-upload over a non-empty prior `require_absent(retire:bytes:s:<id>:<E>:<n>:<attempt>)` | put `part:`, put its `psum:` summary, delete the slot, delete every owned `sidx:` entry, and for that re-upload put the `{chunks}` obligation naming the **prior** part's chunks |
//! | compensation | `require(mpu:<id> == Open@E)`, `require(slot:<id>:<k> == the bytes this attempt last wrote)`, and for a non-empty part `require_absent` of its obligation key | delete the slot, delete every owned `sidx:` entry, and for a non-empty part put the `{chunks}` obligation naming **this attempt's own** chunks |
//!
//! An obligation names at least one chunk (`RetireObligation` refuses an empty list), so an empty
//! part, or a re-upload over an empty prior, installs none, and its slot release and owned-entry
//! deletes ride in the same batch all the same.
//!
//! # Quiesce, then commit
//!
//! A part past a ceiling is refused first. Otherwise the attempt's last renewal
//! ([`StagedAttempt::renew`]) runs, with a lease covering [`DISPATCH_MARGIN_MILLIS`]; no renewal is
//! sent after it, so the only writer of the slot until the commit lands is the commit itself
//! (`0016:1720-1733`). Then two reads: the bounded `psum:<id>:` range, for the cumulative ceiling,
//! and the part key. Then one batch.
//!
//! The renewal can adopt a session it finds at other `Open` bytes: that is how a rollback before it
//! reaches the commit, which then runs under the live epoch. But it does not judge the epoch, so
//! every arm that would go on to send a batch compares the epochs here: an adopted record at a lower
//! epoch, or under other bytes at the same epoch, is damage ([`RecordError::SessionEpochNotRisen`]),
//! and no batch is sent under it. An arm that sends nothing keeps what the renewal said, its doubt
//! included.
//!
//! # What the lease check does and does not promise
//!
//! A renewal guarantees its cover only at the moment it returns, and the reads spend from it. So the
//! clock is read before the commit, and the commit is sent only with [`DISPATCH_MARGIN_MILLIS`] left
//! on the lease. The attempt never renews again to buy time: that would rewrite the slot the commit
//! requires.
//!
//! That is a promise about dispatch, and only about dispatch. The margin is `E_tx`'s five seconds,
//! not a deadline on `commit`: the store promises termination, not five seconds, and a commit
//! abandoned at a deadline may still apply (`crates/traits/src/lib.rs`, "Operational envelope"). So
//! no check here can stop a sent batch applying after its lease ran out. Safety rests on the
//! `require`s instead, since the store enforces bytes, never expiry (`0016:1851-1852`): the commit
//! requires `mpu == Open@E` and its own slot's bytes, and the idle fence requires the same session
//! bytes and pins that slot (`0016:1859-1863`), so whichever lands first makes the other fail. A
//! lapsed lease is therefore never evidence about whether a commit landed.
//!
//! # The stops
//!
//! The reaper does not walk an `Open` session (`0016:1600-1620`), so a stop that left an attempt's
//! slot held under a live session would keep it until `W_session`, and repeated stops would fill
//! `MAX_INFLIGHT_PARTS` (`0016:1420-1431`). So a settled stop before any commit is sent compensates:
//!
//! | The stop | Action |
//! |---|---|
//! | the quiesce renewal stopped by `CoverShort`, `RenewalRoundsSpent`, `LeaseLapsed` or `InvalidRequest` | compensate; the answer keeps the renewal's error |
//! | the lease short before the commit | compensate; [`Stop::LeaseShort`] |
//! | over the per-part or the cumulative ceiling | compensate; [`Refusal::EntityTooLarge`] |
//! | the quiesce renewal in doubt | keep the doubt; no compensation |
//! | `SlotLost`, and staging's `EntryLost` | no compensation: the attempt cannot show the records are its own |
//! | the session has left `Open` | answer the refusal; the residue is the reaper's |
//! | a store fault or a damaged record | no compensation; the residue stays, as after a crash (`0016:1778`) |
//! | a sent commit answered `Conflict` | stop, the slot still held; no compensation, no second commit |
//! | a sent commit answered unknown | stop in doubt, since it may have landed; no compensation, no second commit |
//! | a compensation answered `Conflict` or unknown | stop, claiming no release; no further batch |
//!
//! An owned entry lost after staging has no stop of its own: the renewal requires that entry,
//! conflicts on every round and ends `RenewalRoundsSpent`, which compensates like any other.
//!
//! A lapsed lease does not block a compensation: renewal refuses to revive a lapsed lease
//! (`0016:1845`), but the compensation revives nothing and lands only if the session and the slot
//! are unchanged (`0016:1651-1653`). The caller gets the original stop's typed error, in its own
//! error class, and [`CommitError::slot_released`] says whether the slot was released.
//!
//! # Every batch requires the slot bytes this attempt last wrote
//!
//! Both builders take the slot bytes from the attempt's own state ([`StagedAttempt::slot_bytes`],
//! which its last renewal updated), never from a read of the key. So a slot index another attempt
//! has since reserved is never required or deleted by this one.
//!
//! # The cumulative ceiling is a read, not a fence
//!
//! The `psum:` read is unfenced on purpose: concurrent commits of different part numbers may both
//! read the same headroom and both commit, and the overshoot that leaves is bounded by the in-flight
//! cap, `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`, and charged in `U_REF` (`0016:1441-1451`). No counter
//! or session-wide precondition joins the batch (`0016:1686-1742`).
//!
//! # One clock (ADR-0009)
//!
//! The quiesce renewal, the lease check before the commit and the part's `committed_at_millis` stamp
//! all read the caller's [`StageClock`], the clock that owns the attempt's lease lifecycle and that
//! staging ran on ([`super::stage`]'s "One clock"). Nothing else here reads a clock, and the
//! compensation reads none.
//!
//! No production path calls this module yet: the S3 `UploadPart` verb is #669, and the part records
//! it writes are consumed by Complete (#658). The tests drive every branch over a real store with
//! each interleaving forced by a scripted wrapper
//! (`crates/core/tests/multipart_upload_part_commit.rs`).

// deferred: #660 — seeded Tier-0 DST coverage of the commit and compensation races (a commit
// against a rival, an Abort, a Complete's rollback, the reaper), as for staging (`stage.rs`).

use std::fmt;

use bytes::Bytes;
use futures_util::Stream;
use wyrd_traits::{
    BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, PlacementChunkStore, WriteBatch,
};

use super::stage::{
    stage_part, RenewOutcome, StageClock, StageError, StageOutcome, StageRequest, StageRuntime,
    StagedAttempt, Stop as StageStop,
};
use super::{
    decode_part_record, decode_part_summary, decode_session_record, mpu_key, parse_psum_key,
    part_key, psum_key, psum_range, PartRecord, RecordError, Refusal, RetireObligation, UploadId,
    UploadPartOutcome, E_TX_MILLIS, MAX_PART_CHUNKS, MAX_STAGED_CHUNKS,
};
use crate::metadata;

/// The lease a part commit must have left at the moment it is sent, and so the cover the quiesce
/// renewal asks for: `E_tx`'s five seconds, the number 0016 chose for how much lease a commit should
/// start with (`0016:1726-1728`). A **dispatch margin**, not a deadline: it says nothing about when
/// a sent batch applies (the module doc's *What the lease check does and does not promise*). A
/// request staged with a lease TTL not above it cannot be committed: the renewal refuses a cover it
/// could not give.
pub const DISPATCH_MARGIN_MILLIS: u64 = E_TX_MILLIS;

/// Which of the attempt's batches a [`CommitError::InDoubt`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoubtfulBatch {
    /// The part commit: the part may be committed, or may still be.
    Commit,
    /// The live-session compensation: the obligation may be installed and the slot released, or
    /// may still be.
    Compensation,
}

/// A part commit that ended with neither an outcome nor a refusal.
///
/// # Error class
///
/// [`wyrd_traits::classify`] reads [`Self::InDoubt`] as
/// [`ErrorClass::Indeterminate`](wyrd_traits::ErrorClass::Indeterminate): its
/// [`source`](std::error::Error::source) is the store's own [`CommitUnknownResult`].
/// [`Self::Stopped`] keeps its cause's class, and [`Self::Renewal`] its renewal error's, whether or
/// not the slot was then released.
#[derive(Debug)]
pub enum CommitError {
    /// The quiesce renewal ([`StagedAttempt::renew`]) did not cover the commit, so no commit was
    /// sent: its error, its doubt about its own rounds kept.
    Renewal {
        /// What stopped the renewal.
        error: StageError,
        /// Whether the attempt's live-session compensation then landed. When `false`, what it
        /// staged stays in the store.
        slot_released: bool,
    },
    /// A batch of this attempt may have landed, or may still land, and the attempt sent nothing
    /// more. Never "nothing was written". Later slices settle it by re-reading (#1029, #1030).
    InDoubt(Box<InDoubt>),
    /// The commit stopped, and no batch of it is in doubt.
    Stopped {
        /// The session.
        upload_id: UploadId,
        /// What stopped it.
        cause: Stop,
        /// Whether the attempt's live-session compensation then landed. When `false`, what it
        /// staged stays in the store.
        slot_released: bool,
    },
}

impl CommitError {
    /// Whether this error's stop was followed by a compensation that landed: the attempt's slot is
    /// released, its owned entries are deleted, and its chunks are owed to a `retire:bytes:`
    /// obligation (none for an empty part). Never `true` for [`Self::InDoubt`].
    pub const fn slot_released(&self) -> bool {
        match self {
            Self::Renewal { slot_released, .. } | Self::Stopped { slot_released, .. } => {
                *slot_released
            }
            Self::InDoubt(_) => false,
        }
    }
}

/// What a [`CommitError::InDoubt`] reports.
#[derive(Debug)]
pub struct InDoubt {
    /// The session.
    pub upload_id: UploadId,
    /// Which batch is in doubt.
    pub batch: DoubtfulBatch,
    /// Whether that batch may still be applied **after** the call returned, as the store reported
    /// it ([`CommitUnknownResult::may_still_commit`]). With `false` it may still have landed.
    pub may_still_commit: bool,
    /// The store's report. It is the error's [`source`](std::error::Error::source).
    pub outcome: CommitUnknownResult,
}

/// What stopped a part commit ([`CommitError::Stopped`]).
#[derive(Debug)]
pub enum Stop {
    /// A sent part commit was answered `Conflict`. No read has classified it, so nothing more is
    /// sent, and the attempt's slot and owned entries stay as they are.
    // deferred: #1028 — the classification of a commit's `Conflict` and its answer.
    CommitConflict,
    /// The compensation was answered `Conflict`, so it released nothing.
    // deferred: #1028 — the epoch chase of a compensation that lost to a rollback.
    CompensationConflict,
    /// The attempt's lease had less than [`DISPATCH_MARGIN_MILLIS`] left when the commit came to be
    /// sent, so it was not.
    LeaseShort {
        /// The lease the quiesce renewal wrote.
        lease_expiry_millis: u64,
        /// The reading it was judged at.
        now_millis: u64,
    },
    /// A stored record is not valid (ADR-0045), or a value this attempt would write is not one its
    /// decoder accepts.
    Record(RecordError),
    /// A metadata store operation failed.
    Store(BoxError),
}

/// What a stop left in the store, for an error's message.
const fn residue(slot_released: bool) -> &'static str {
    if slot_released {
        "the attempt's slot is released and its chunks are retired"
    } else {
        "this attempt released nothing, and what it staged stays in the store"
    }
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Renewal {
                error,
                slot_released,
            } => write!(
                f,
                "multipart part commit: the quiesce renewal did not cover the commit, so none was \
                 sent: {error}; {}",
                residue(*slot_released)
            ),
            Self::InDoubt(doubt) => write!(
                f,
                "multipart part commit: upload {} is in doubt: this attempt's {} {}",
                doubt.upload_id,
                match doubt.batch {
                    DoubtfulBatch::Commit => "part commit",
                    DoubtfulBatch::Compensation => "compensation",
                },
                if doubt.may_still_commit {
                    "may still be applied"
                } else {
                    "may have been applied"
                },
            ),
            Self::Stopped {
                upload_id,
                cause,
                slot_released,
            } => write!(
                f,
                "multipart part commit: upload {upload_id}: {cause}; no batch of this part attempt \
                 is in doubt; {}",
                residue(*slot_released)
            ),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitConflict => f.write_str(
                "the part commit was answered Conflict, and this attempt's slot is still held",
            ),
            Self::CompensationConflict => {
                f.write_str("the compensation was answered Conflict, so it released nothing")
            }
            Self::LeaseShort {
                lease_expiry_millis,
                now_millis,
            } => write!(
                f,
                "the lease expiring at {lease_expiry_millis} had less than \
                 {DISPATCH_MARGIN_MILLIS} ms left at {now_millis}, so no commit was sent"
            ),
            Self::Record(err) => write!(f, "invalid record: {err}"),
            Self::Store(err) => write!(f, "store fault: {err}"),
        }
    }
}

impl std::error::Error for CommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Renewal { error, .. } => Some(error),
            Self::InDoubt(doubt) => Some(&doubt.outcome),
            Self::Stopped { cause, .. } => match cause {
                Stop::Record(err) => Some(err),
                Stop::Store(err) => Some(err.as_ref()),
                Stop::CommitConflict | Stop::CompensationConflict | Stop::LeaseShort { .. } => None,
            },
        }
    }
}

/// What the composed [`upload_part`] ended with, when it ended with neither an outcome nor a
/// refusal: staging's error, or the commit's. Each keeps its own class ([`StageError`],
/// [`CommitError`]).
#[derive(Debug)]
pub enum UploadPartError {
    /// Staging stopped ([`stage_part`]); no commit was attempted.
    Stage(StageError),
    /// The commit stopped ([`commit_part`]).
    Commit(CommitError),
}

impl fmt::Display for UploadPartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stage(err) => write!(f, "{err}"),
            Self::Commit(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for UploadPartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stage(err) => Some(err),
            Self::Commit(err) => Some(err),
        }
    }
}

/// The `mpu:` bytes a batch of this attempt requires, with the epoch they carry, decoded once.
#[derive(Debug, Clone)]
struct Session {
    bytes: Bytes,
    epoch: u64,
}

impl Session {
    fn of(bytes: &Bytes) -> Result<Self, RecordError> {
        Ok(Self {
            bytes: bytes.clone(),
            epoch: decode_session_record(bytes)?.epoch(),
        })
    }
}

/// A commit stopped with nothing in doubt and nothing released.
fn stopped(upload_id: &UploadId, cause: Stop) -> CommitError {
    CommitError::Stopped {
        upload_id: upload_id.clone(),
        cause,
        slot_released: false,
    }
}

/// A sent batch came back with an error: an unknown outcome keeps the doubt, as the store reported
/// it, and anything else is a definite failure of that batch.
fn failed(upload_id: &UploadId, batch: DoubtfulBatch, fault: BoxError) -> CommitError {
    match fault.downcast_ref::<CommitUnknownResult>() {
        Some(unknown) => CommitError::InDoubt(Box::new(InDoubt {
            upload_id: upload_id.clone(),
            batch,
            may_still_commit: unknown.may_still_commit,
            outcome: unknown.clone(),
        })),
        None => stopped(upload_id, Stop::Store(fault)),
    }
}

/// The refusal of an attempt past the per-part ceiling, if it is. `stage_part` cannot enforce it:
/// the bound it stops at is the caller's, and only a body past that bound is marked
/// ([`StagedAttempt::chunk_bound_exceeded`]), so this looks at both. A truncated body had at least
/// one more chunk than it holds, and its [`StagedAttempt::len`] and [`StagedAttempt::digest`]
/// describe only the prefix.
fn past_part_ceiling(attempt: &StagedAttempt) -> Option<Refusal> {
    let held = attempt.chunks().len() as u64;
    let limit = u64::from(MAX_PART_CHUNKS);
    if attempt.chunk_bound_exceeded() {
        return Some(Refusal::EntityTooLarge {
            chunks: held.saturating_add(1),
            limit: held.min(limit),
        });
    }
    (held > limit).then_some(Refusal::EntityTooLarge {
        chunks: held,
        limit,
    })
}

/// The session the batches after the quiesce renewal require: what the renewal left, which is
/// `staged` unless it adopted a session it found at other `Open` bytes. An adoption the epoch does
/// not explain is damage.
fn renewed_session(attempt: &StagedAttempt, staged: &Session) -> Result<Session, RecordError> {
    // deferred: #1026 — a non-`Open` record at an epoch that has not risen is answered
    // `NoSuchUpload` inside the renewal, before this can judge it.
    let session = Session::of(attempt.session_bytes())?;
    if session.bytes != staged.bytes && session.epoch <= staged.epoch {
        return Err(RecordError::SessionEpochNotRisen {
            found: session.epoch,
            required: staged.epoch,
        });
    }
    Ok(session)
}

/// Whether a stopped quiesce renewal leaves the attempt to compensate (the module doc's *The
/// stops*): only a stop with nothing in doubt, whose records the attempt can still show are its own,
/// under a store that is answering.
const fn compensates_after(error: &StageError) -> bool {
    matches!(
        error,
        StageError::Stopped {
            cause: StageStop::CoverShort { .. }
                | StageStop::RenewalRoundsSpent { .. }
                | StageStop::LeaseLapsed { .. }
                | StageStop::InvalidRequest { .. },
            ..
        }
    )
}

/// The chunks the session's committed parts hold, this part number's own prior left out (a re-upload
/// replaces it): the sum over the bounded `psum:<id>:` range of summaries, never the chunk lists
/// (`0016:352`).
async fn staged_elsewhere(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
) -> Result<u64, Stop> {
    let mut total = 0u64;
    for (key, value) in store
        .scan(&psum_range(attempt.upload_id()))
        .await
        .map_err(Stop::Store)?
    {
        let (_, part_number) = parse_psum_key(&key).map_err(Stop::Record)?;
        if part_number == attempt.part_number() {
            continue;
        }
        let summary = decode_part_summary(&value).map_err(Stop::Record)?;
        total = total.saturating_add(u64::from(summary.chunks));
    }
    Ok(total)
}

/// The obligation an attempt's batch under `session` installs for `chunks`, none when there are
/// none.
fn obligation_for(
    attempt: &StagedAttempt,
    session: &Session,
    chunks: &[metadata::ChunkRef],
) -> Result<Option<RetireObligation>, RecordError> {
    if chunks.is_empty() {
        return Ok(None);
    }
    RetireObligation::part_chunks(
        attempt.upload_id(),
        session.epoch,
        attempt.part_number(),
        attempt.attempt_id(),
        chunks,
    )
    .map(Some)
}

/// The part commit's batch (`0016:660`).
fn commit_batch(
    attempt: &StagedAttempt,
    session: &Bytes,
    required: Option<&Bytes>,
    record: &PartRecord,
    retire: Option<&RetireObligation>,
) -> WriteBatch {
    let upload_id = attempt.upload_id();
    let part = part_key(upload_id, attempt.part_number());
    let mut batch = WriteBatch::new().require(mpu_key(upload_id), session.clone());
    batch = match required {
        Some(prior) => batch.require(part.clone(), prior.clone()),
        None => batch.require_absent(part.clone()),
    };
    // The bytes this attempt last wrote, from its own state: never re-read from the key.
    batch = batch.require(attempt.slot_key().to_vec(), attempt.slot_bytes().clone());
    if let Some(obligation) = retire {
        batch = batch.require_absent(obligation.key());
    }
    batch = batch
        .put(part, metadata::encode(record))
        .put(
            psum_key(upload_id, attempt.part_number()),
            metadata::encode(&record.summary()),
        )
        .delete(attempt.slot_key().to_vec());
    for owned in attempt.owned() {
        batch = batch.delete(owned.key().to_vec());
    }
    if let Some(obligation) = retire {
        batch = batch.put(obligation.key(), metadata::encode(obligation.payload()));
    }
    batch
}

/// The live-session compensation's batch (`0016:673`), under `session`.
fn compensation_batch(
    attempt: &StagedAttempt,
    session: &Bytes,
    obligation: Option<&RetireObligation>,
) -> WriteBatch {
    let mut batch = WriteBatch::new()
        .require(mpu_key(attempt.upload_id()), session.clone())
        // The bytes this attempt last wrote, from its own state: never re-read from the key.
        .require(attempt.slot_key().to_vec(), attempt.slot_bytes().clone());
    if let Some(obligation) = obligation {
        batch = batch.require_absent(obligation.key());
    }
    batch = batch.delete(attempt.slot_key().to_vec());
    for owned in attempt.owned() {
        batch = batch.delete(owned.key().to_vec());
    }
    if let Some(obligation) = obligation {
        batch = batch.put(obligation.key(), metadata::encode(obligation.payload()));
    }
    batch
}

/// **The live-session compensation** (`0016:673`): one batch that releases the slot, deletes the
/// owned entries and, for a non-empty part, installs one `{chunks}` obligation naming the attempt's
/// own chunks. Called only before any commit of the attempt is sent. The clock plays no part.
async fn compensate(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
) -> Result<(), CommitError> {
    let upload_id = attempt.upload_id();
    let obligation = obligation_for(attempt, session, attempt.chunks())
        .map_err(|err| stopped(upload_id, Stop::Record(err)))?;
    let batch = compensation_batch(attempt, &session.bytes, obligation.as_ref());
    match store.commit(batch).await {
        Ok(CommitOutcome::Committed) => Ok(()),
        // deferred: #1028, #1030 — the classification of a compensation's `Conflict`, and the
        // settling of its unknown outcome.
        Ok(CommitOutcome::Conflict) => Err(stopped(upload_id, Stop::CompensationConflict)),
        Err(fault) => Err(failed(upload_id, DoubtfulBatch::Compensation, fault)),
    }
}

/// Refuse the part with `refusal`, once what it staged is compensated under `session`.
async fn refuse(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
    refusal: Refusal,
) -> Result<UploadPartOutcome, CommitError> {
    compensate(store, attempt, session).await?;
    Ok(UploadPartOutcome::Refused(refusal))
}

/// **The commit round**: read the clock, send the commit only with [`DISPATCH_MARGIN_MILLIS`] left
/// on the lease, and answer what the store says. Once it has sent the batch it sends nothing more:
/// `Committed` is answered only when the store answered this batch as landed, `Conflict` stops with
/// the slot held, and an unknown outcome stops in doubt.
async fn commit_round(
    store: &impl MetadataStore,
    clock: &impl StageClock,
    attempt: &StagedAttempt,
    session: &Session,
    lease_expiry_millis: u64,
    required: Option<&Bytes>,
    prior: Option<&PartRecord>,
) -> Result<UploadPartOutcome, CommitError> {
    let upload_id = attempt.upload_id();
    let damage = |err| stopped(upload_id, Stop::Record(err));
    // One reading, for the check and for the stamp: the clock that owns the attempt's leases.
    let now_millis = clock.now_millis();
    if lease_expiry_millis.saturating_sub(now_millis) < DISPATCH_MARGIN_MILLIS {
        compensate(store, attempt, session).await?;
        return Err(CommitError::Stopped {
            upload_id: upload_id.clone(),
            cause: Stop::LeaseShort {
                lease_expiry_millis,
                now_millis,
            },
            slot_released: true,
        });
    }
    let record = PartRecord::commit(
        attempt.chunks().to_vec(),
        *attempt.digest(),
        now_millis,
        session.epoch,
    )
    .map_err(damage)?;
    // The prior part's chunks, owed by a re-upload (none for an empty prior).
    let retire = obligation_for(attempt, session, prior.map_or(&[], |prior| prior.chunks()))
        .map_err(damage)?;
    let batch = commit_batch(attempt, &session.bytes, required, &record, retire.as_ref());
    match store.commit(batch).await {
        Ok(CommitOutcome::Committed) => Ok(UploadPartOutcome::Committed {
            part_number: attempt.part_number(),
            digest: *attempt.digest(),
        }),
        // deferred: #1028, #1029 — the classification of a commit's `Conflict`, and the settling
        // of its unknown outcome.
        Ok(CommitOutcome::Conflict) => Err(stopped(upload_id, Stop::CommitConflict)),
        Err(fault) => Err(failed(upload_id, DoubtfulBatch::Commit, fault)),
    }
}

/// **Commit a staged part** (`0016:660`, `:1720-1733`), answering in the landed vocabulary
/// ([`UploadPartOutcome`], [`Refusal`]). The module doc states the batches and the stops.
///
/// * A part past [`MAX_PART_CHUNKS`], however it was staged, is refused
///   [`Refusal::EntityTooLarge`] and compensated. So is a body truncated at the request's bound,
///   whose `chunks` is then one past what it holds, since staging did not read the rest.
/// * Otherwise the quiesce renewal runs ([`DISPATCH_MARGIN_MILLIS`]); a session it finds no longer
///   `Open` is [`Refusal::NoSuchUpload`], with nothing more written.
/// * A part that would take the session's committed chunks past [`MAX_STAGED_CHUNKS`], counted over
///   the `psum:` summaries of every other part number, is refused [`Refusal::EntityTooLarge`]
///   (`chunks` the total it would reach, `limit` the ceiling) and compensated.
/// * Otherwise one commit batch, sent only with [`DISPATCH_MARGIN_MILLIS`] left on the lease.
///
/// `clock` must be the clock the attempt was staged on.
///
/// **Not cancel-safe once a batch is sent**: dropping the future can leave a batch that lands with
/// no answer naming it. The slot and every owned entry it could leave are still in reach of the
/// reaper's per-session ranges.
pub async fn commit_part(
    store: &impl MetadataStore,
    clock: &impl StageClock,
    mut attempt: StagedAttempt,
) -> Result<UploadPartOutcome, CommitError> {
    let upload_id = attempt.upload_id().clone();
    let stop = |cause| stopped(&upload_id, cause);
    let damage = |err| stopped(&upload_id, Stop::Record(err));
    let staged = Session::of(attempt.session_bytes()).map_err(damage)?;
    if let Some(refusal) = past_part_ceiling(&attempt) {
        return refuse(store, &attempt, &staged, refusal).await;
    }
    let renewed = attempt.renew(store, clock, DISPATCH_MARGIN_MILLIS).await;
    let (session, lease_expiry_millis) = match renewed {
        Ok(RenewOutcome::Covered {
            lease_expiry_millis,
            ..
        }) => (
            renewed_session(&attempt, &staged).map_err(damage)?,
            lease_expiry_millis,
        ),
        Ok(RenewOutcome::Refused(refusal)) => return Ok(UploadPartOutcome::Refused(refusal)),
        Err(error) if compensates_after(&error) => {
            let session = renewed_session(&attempt, &staged).map_err(damage)?;
            compensate(store, &attempt, &session).await?;
            return Err(CommitError::Renewal {
                error,
                slot_released: true,
            });
        }
        // Nothing more is sent, so nothing is judged: a renewal in doubt keeps its doubt.
        Err(error) => {
            return Err(CommitError::Renewal {
                error,
                slot_released: false,
            })
        }
    };
    // The commit's reads, after the last renewal: no renewal is sent from here on.
    let total = staged_elsewhere(store, &attempt)
        .await
        .map_err(stop)?
        .saturating_add(attempt.chunks().len() as u64);
    if total > u64::from(MAX_STAGED_CHUNKS) {
        let refusal = Refusal::EntityTooLarge {
            chunks: total,
            limit: u64::from(MAX_STAGED_CHUNKS),
        };
        return refuse(store, &attempt, &session, refusal).await;
    }
    let required = store
        .get(&part_key(&upload_id, attempt.part_number()))
        .await
        .map_err(|fault| stop(Stop::Store(fault)))?;
    let prior = required
        .as_deref()
        .map(decode_part_record)
        .transpose()
        .map_err(damage)?;
    commit_round(
        store,
        clock,
        &attempt,
        &session,
        lease_expiry_millis,
        required.as_ref(),
        prior.as_ref(),
    )
    .await
}

/// **`UploadPart`, whole**: reserve a slot, stage the body ([`stage_part`]), then commit it
/// ([`commit_part`]), on one runtime whose clock owns the attempt's leases. A staging refusal is
/// answered as it is; a staging error is [`UploadPartError::Stage`] and no commit is attempted.
///
/// No production path calls this yet (the S3 verb is #669).
pub async fn upload_part<B>(
    store: &impl MetadataStore,
    chunk_store: &impl PlacementChunkStore,
    runtime: &mut impl StageRuntime,
    request: &StageRequest<'_>,
    body: B,
) -> Result<UploadPartOutcome, UploadPartError>
where
    B: Stream<Item = wyrd_traits::Result<Bytes>> + Unpin,
{
    match stage_part(store, chunk_store, runtime, request, body).await {
        Ok(StageOutcome::Staged(attempt)) => commit_part(store, &*runtime, *attempt)
            .await
            .map_err(UploadPartError::Commit),
        Ok(StageOutcome::Refused(refusal)) => Ok(UploadPartOutcome::Refused(refusal)),
        Err(err) => Err(UploadPartError::Stage(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multipart::{AttemptId, PartNumber};

    fn stopped_by(cause: StageStop) -> StageError {
        StageError::Stopped {
            upload_id: UploadId::new("0".repeat(32)).unwrap(),
            cause,
        }
    }

    /// The table in the module doc, read row by row. `EntryLost` has its own row here because
    /// `StagedAttempt::renew` cannot produce it (only a chunk's intent does, `stage.rs`), so no
    /// store history can drive that row through the renewal; the crate's test file drives the
    /// others, and `EntryLost` through the composed `upload_part`.
    #[test]
    fn a_stopped_renewal_compensates_only_on_the_rows_that_say_so() {
        let key = || "k".to_owned();
        for (cause, compensates) in [
            (
                StageStop::CoverShort {
                    rounds: 1,
                    cover_millis: 1,
                    lease_expiry_millis: 1,
                    checked_at_millis: 1,
                },
                true,
            ),
            (StageStop::RenewalRoundsSpent { rounds: 1 }, true),
            (
                StageStop::LeaseLapsed {
                    key: key(),
                    lease_expiry_millis: 1,
                    now_millis: 1,
                },
                true,
            ),
            (StageStop::InvalidRequest { reason: "r" }, true),
            (StageStop::SlotLost { key: key() }, false),
            (StageStop::EntryLost { key: key() }, false),
            (StageStop::Store("fault".into()), false),
        ] {
            let error = stopped_by(cause);
            assert_eq!(compensates_after(&error), compensates, "{error}");
        }
    }

    /// `RetireObligation` keeps refusing an empty list, on the writer's route as on decode's
    /// (`crates/core/tests/multipart_retire_obligation.rs`), which is why a batch with nothing to
    /// retire installs no obligation.
    #[test]
    fn the_per_part_obligation_refuses_an_empty_list() {
        let upload_id = UploadId::new("0".repeat(32)).unwrap();
        let attempt_id = AttemptId::new("1".repeat(32)).unwrap();
        let part_number = PartNumber::new(1).unwrap();
        assert!(matches!(
            RetireObligation::part_chunks(&upload_id, 0, part_number, &attempt_id, &[]),
            Err(RecordError::RetireObligationOwesNothing {
                component: "chunks"
            })
        ));
    }
}
