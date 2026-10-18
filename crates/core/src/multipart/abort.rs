//! `AbortMultipartUpload`'s **teardown fence**, and the one builder of the fence batch that every
//! writer of the `Open → Aborting` edge commits (#895, slice 3b of #636).
//!
//! [`abort_session`] moves an `Open@E` session to `Aborting@E+1` and installs its retirement
//! obligation in the **same** batch (proposal 0016's *Abort / reap fence* row, `0016:665`), and
//! answers every other state as decision 3's table does (`0016:977`, [`super::abort_answer`]):
//!
//! | State read | Answer | Written |
//! |---|---|---|
//! | `Open@E` | [`AbortOutcome::Fenced`] | the [`fence_batch`]: `require(mpu:<id> == bytes read)`, `require_absent(retire:bytes:s:<id>:<E>)`; put the session at `Aborting@E+1`, put its `{session, all}` obligation ([`SessionRecord::open_teardown`]) |
//! | `Aborting` | [`AbortOutcome::AlreadyAborting`] | nothing |
//! | `Completing` | [`Refusal::OperationAborted`] | nothing |
//! | `Completed`, or no record | [`Refusal::NoSuchUpload`] | nothing |
//!
//! A client Abort carries no slot preconditions (`0016:665`). The fence never touches `mpuctl`: a
//! fenced session still holds its staged bytes and its `mpu:` record, so it stays counted until
//! the terminal delete, the one place the count goes down (#659).
//!
//! # One batch builder, two writers
//!
//! [`fence_batch`] is the only place the teardown fence's [`WriteBatch`] is built. The client
//! Abort here and the post-restore session fence (`crates/custodian/src/restore.rs`,
//! `fence_session`) both commit what it builds, so the two writers of the `Open → Aborting` edge
//! cannot drift apart in their preconditions or their puts.
//!
//! Production calls no client Abort yet: the S3 verb that will is #668, under 0016's rule that
//! the reaper and operator abort ship in the same release (`0016:236-268`). The restore fence is
//! the one production caller of [`fence_batch`].
//!
//! # Runtime-free (ADR-0009)
//!
//! Core reads no clock, draws no randomness and never sleeps here. The fence stamps no time: the
//! `Aborting` record carries the session's own `created_at_millis` and `clock_source` unchanged,
//! and a round that lost is retried at once, with no backoff to wait out.

use std::fmt;
use std::slice;

use bytes::Bytes;
use wyrd_traits::{BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, WriteBatch};

use super::{
    abort_answer, decode_session_record, mpu_key, AbortAnswer, AbortOutcome, RecordError, Refusal,
    RetireObligation, SessionRecord, UploadId, MAX_COMPLETE_ATTEMPTS,
};
use crate::metadata;

/// How many fence commits one [`abort_session`] makes at most: [`MAX_COMPLETE_ATTEMPTS`] `+ 1`.
///
/// A session goes back to `Open` only when a Complete fence is released, and it spends at most
/// [`MAX_COMPLETE_ATTEMPTS`] Complete fences, so a session that keeps moving to a later `Open@E'`
/// under the Abort can cost it at most that many rounds. The one more is the round that fences.
/// A round is also spent by a conflict no re-read explains (on TiKV, a lock another writer held,
/// with nothing moved) and by a commit whose outcome the store could not report; those share
/// the same budget. It is sized from the shipped value of that knob, and it bounds the work one
/// Abort does, not its correctness: an Abort that spends it ends with an error that says whether
/// a fence of it may still land, never with a wrong answer.
pub const ABORT_ROUNDS: u32 = MAX_COMPLETE_ATTEMPTS + 1;

/// **The teardown fence's batch** (`0016:665`), the one builder of it: given the session's
/// `mpu:` key, the exact bytes read there, the session as the fence leaves it and the obligations
/// the fence installs, it
///
/// 1. `require`s `session_key` to hold exactly `read`;
/// 2. `require_absent`s every obligation's key (`0016:369-373`), in the order given;
/// 3. puts `session` at `session_key`;
/// 4. puts every obligation's payload under its key, in the order given.
///
/// It takes a list because the restore fence also fences `Completing` sessions, whose teardown
/// installs two obligations ([`SessionRecord::completing_teardown`]); the client Abort passes the
/// one [`SessionRecord::open_teardown`] mints.
///
/// It writes only obligations that [`super::decode_retire_obligation`] accepts at the key they go
/// under. That is a property of the type: a [`RetireObligation`] is minted only by the teardown
/// rows, whose key and payload come from one constructor and are pinned to decode against each
/// other, so a caller cannot hand this builder a payload under a key it chose.
pub fn fence_batch(
    session_key: &[u8],
    read: &[u8],
    session: &SessionRecord,
    obligations: &[RetireObligation],
) -> WriteBatch {
    let keys: Vec<Vec<u8>> = obligations.iter().map(RetireObligation::key).collect();
    let mut batch = WriteBatch::new().require(session_key.to_vec(), read.to_vec());
    for key in &keys {
        batch = batch.require_absent(key.clone());
    }
    batch = batch.put(session_key.to_vec(), metadata::encode(session));
    for (obligation, key) in obligations.iter().zip(keys) {
        batch = batch.put(key, metadata::encode(obligation.payload()));
    }
    batch
}

/// An Abort that ended with neither an answer nor a refusal.
///
/// # Error class
///
/// [`wyrd_traits::classify`] reads [`Self::InDoubt`] as
/// [`ErrorClass::Indeterminate`](wyrd_traits::ErrorClass::Indeterminate), whatever stopped the
/// Abort: its [`source`](std::error::Error::source) is the store's own [`CommitUnknownResult`].
/// [`Self::Stopped`] keeps its cause's class: a [`Stop::Store`] wrapping a
/// [`TransientFault`](wyrd_traits::TransientFault) is transient, because nothing is in doubt.
#[derive(Debug)]
pub enum AbortError {
    /// A fence batch of this Abort may have landed, or may still land, and the Abort stopped
    /// before a read could say which. Never "nothing was written".
    InDoubt(Box<InDoubt>),
    /// The Abort stopped, and no fence batch of it is in doubt: none landed, and none can land
    /// later.
    Stopped {
        /// The upload the Abort was for.
        upload_id: UploadId,
        /// What stopped it.
        cause: Stop,
    },
}

/// What an [`AbortError::InDoubt`] reports.
#[derive(Debug)]
pub struct InDoubt {
    /// The upload whose fence is in doubt.
    pub upload_id: UploadId,
    /// Whether a fence batch of this Abort may still be applied **after** it returned
    /// ([`CommitUnknownResult::may_still_commit`]). Once a fence of this Abort said so, only a read
    /// that found the session moved clears it, so later outcomes, failed reads and store faults
    /// leave it `true`.
    pub may_still_commit: bool,
    /// The store's report of the unknown outcome that keeps the doubt: the first that said
    /// `may_still_commit`, or else the latest one no successful read has settled. It is the
    /// error's [`source`](std::error::Error::source).
    pub outcome: CommitUnknownResult,
    /// What stopped the Abort before it could settle the doubt.
    pub cause: Stop,
}

/// What stopped an Abort ([`AbortError::Stopped`], [`InDoubt::cause`]).
#[derive(Debug)]
pub enum Stop {
    /// All [`ABORT_ROUNDS`] fence commits were made and the session was still `Open`.
    RoundsSpent {
        /// Fence commits made.
        rounds: u32,
    },
    /// The fence's obligation key is already present while the session is still exactly the
    /// bytes the fence read: two reads of the session, one on each side of the key's read, found
    /// it unchanged. A token is minted once (`0016:358-373`), so this is damage a retry cannot
    /// settle.
    ObligationKeyTaken {
        /// The `retire:` key, as UTF-8 (lossy).
        key: String,
    },
    /// The session is `Open` at the last epoch, so no fence can bump it (`0016:704-708`).
    EpochExhausted,
    /// The stored `mpu:` value is not a valid session record (ADR-0045).
    Record(RecordError),
    /// A store operation failed.
    Store(BoxError),
}

impl fmt::Display for AbortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InDoubt(doubt) => write!(
                f,
                "multipart abort: upload {} is in doubt: a fence batch of this abort {}; re-read \
                 the session to learn whether it is Aborting ({})",
                doubt.upload_id,
                if doubt.may_still_commit {
                    "may still be applied"
                } else {
                    "may have been applied"
                },
                doubt.cause,
            ),
            Self::Stopped { upload_id, cause } => write!(
                f,
                "multipart abort: upload {upload_id}: {cause}; no fence batch of this abort is in \
                 doubt (none landed, and none can land later)"
            ),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RoundsSpent { rounds } => write!(
                f,
                "all {rounds} fence rounds were spent and the session is still Open"
            ),
            Self::ObligationKeyTaken { key } => write!(
                f,
                "the obligation key {key} is already taken while the session is unchanged; this \
                 is damage a retry cannot settle"
            ),
            Self::EpochExhausted => {
                f.write_str("the session is Open at the last epoch, so no fence can bump it")
            }
            Self::Record(torn) => write!(f, "the stored session record is invalid: {torn}"),
            Self::Store(fault) => write!(f, "store fault: {fault}"),
        }
    }
}

impl std::error::Error for AbortError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // The unknown outcome, never the stop cause: `classify` stops at the first seam type
            // on the chain, and a doubtful Abort must read `Indeterminate` even when a transient
            // fault is what stopped it.
            Self::InDoubt(doubt) => Some(&doubt.outcome),
            Self::Stopped { cause, .. } => match cause {
                Stop::Record(torn) => Some(torn),
                Stop::Store(fault) => Some(fault.as_ref()),
                Stop::RoundsSpent { .. }
                | Stop::ObligationKeyTaken { .. }
                | Stop::EpochExhausted => None,
            },
        }
    }
}

/// **`AbortMultipartUpload`**: fence the session `upload_id` names (`0016:665`), answering in the
/// landed vocabulary ([`AbortOutcome`], [`Refusal`]) per decision 3's table (`0016:977`). No
/// production path calls this yet (#668 will).
///
/// # The fence
///
/// Read `mpu:<id>`. An `Open@E` session is fenced by one commit of [`fence_batch`]: the session
/// at `Aborting@E+1` and its `retire:bytes:s:<id>:<E>` obligation `{session, all}`, under
/// `require` on the exact bytes read and `require_absent` on the obligation key. `{session, all}`
/// rather than a part list, because a list can outgrow the value ceiling for a sparse session and
/// `all` is sound: no part commits outside `Open` (`0016:712-717`). Every other state is
/// answered from the table and nothing is written.
///
/// # A commit that did not say `Committed`
///
/// The Abort runs at most [`ABORT_ROUNDS`] fence commits, and after each one reads the session
/// again and answers from what it finds, the last round's read included:
///
/// * **`Conflict`, session moved**: answer from the new state. A rival Abort that won leaves
///   `Aborting`, which answers [`AbortOutcome::AlreadyAborting`]; two racing Aborts never get a
///   `409`. A session at a later `Open@E'` is fenced again, at `E'`.
/// * **`Conflict`, session unchanged**: read the obligation key. Absent, the conflict is one no
///   read explains (TiKV answers `Conflict` when its locking read meets another writer's lock,
///   with nothing moved), so the Abort spends a round and tries again. Present, it reads the
///   session once more before naming anything: a rival fence that landed between the two reads
///   moved the session in the same batch that wrote the key, so a moved session is answered from
///   its new state. Only a session **still** exactly the bytes this round read makes the key
///   damage, [`Stop::ObligationKeyTaken`].
/// * **[`CommitUnknownResult`]**: never read as `Conflict`. The next read answers it: a session
///   found exactly as a fence of this Abort leaves it is [`AbortOutcome::Fenced`].
///
/// # Two kinds of doubt, both kept
///
/// * **May still land** (`may_still_commit: true`): kept while the session is unchanged, across
///   later out-of-flight unknowns, failed reads, store faults and unexplained conflicts. Only a
///   read that finds the session moved clears it: every fence requires the exact bytes it read,
///   and the epoch only rises, so a fence of bytes that are gone can never land.
/// * **Out of flight, outcome not read** (`may_still_commit: false`): the batch cannot land
///   later, but may already have landed. One successful read of `mpu:<id>` settles it.
///
/// When the rounds run out or a fault ends the Abort, it returns [`AbortError::InDoubt`] with the
/// first outcome that said `may_still_commit` if a fence of it may still land; else, if one of
/// its unknown outcomes was never settled by a read, [`AbortError::InDoubt`] with that outcome;
/// only otherwise [`AbortError::Stopped`]. There is no settling read before it returns: a fence
/// that lands after the Abort returned cannot be found by a look taken before, so the Abort
/// returns the doubt instead.
///
/// # Termination
///
/// At most [`ABORT_ROUNDS`] commits, each followed by at most three reads, after one first read.
/// Each store call is bounded by the store's own contract (every operation terminates,
/// `crates/traits/src/lib.rs`, "Operational envelope").
///
/// **Not cancel-safe once a commit is sent.** Dropping the future after that can leave a fence
/// that lands with no answer naming it. The fence still installs its obligation in the same
/// batch, so the session stays in reach of the drain, but the caller is told nothing. Bound an
/// Abort through the store's own deadlines, never by dropping the future: a commit the store
/// abandons at its deadline comes back as a [`CommitUnknownResult`], which this function reports.
pub async fn abort_session(
    store: &impl MetadataStore,
    upload_id: &UploadId,
) -> Result<AbortOutcome, AbortError> {
    let session_key = mpu_key(upload_id);
    let mut doubt = Doubt::default();
    let mut rounds = 0u32;
    let mut found = match store.get(&session_key).await {
        Ok(found) => found,
        Err(fault) => return Err(doubt.end(upload_id, Stop::Store(fault))),
    };
    loop {
        // `found` is always the result of a read that succeeded.
        if doubt.landed(found.as_ref()) {
            return Ok(AbortOutcome::Fenced);
        }
        doubt.settle(found.as_ref());
        let Some(read) = found else {
            return Ok(AbortOutcome::Refused(Refusal::NoSuchUpload));
        };
        let record = match decode_session_record(&read) {
            Ok(record) => record,
            Err(torn) => return Err(doubt.end(upload_id, Stop::Record(torn))),
        };
        match abort_answer(Some(record.state())) {
            AbortAnswer::Fences => {}
            AbortAnswer::AlreadyAborting => return Ok(AbortOutcome::AlreadyAborting),
            AbortAnswer::Refused(refusal) => return Ok(AbortOutcome::Refused(refusal)),
        }
        let Some(teardown) = record.open_teardown(upload_id) else {
            return Err(doubt.end(upload_id, Stop::EpochExhausted));
        };
        if rounds == ABORT_ROUNDS {
            return Err(doubt.end(upload_id, Stop::RoundsSpent { rounds }));
        }
        rounds += 1;
        let obligation = teardown.obligation();
        let obligation_key = obligation.key();
        let batch = fence_batch(
            &session_key,
            &read,
            teardown.session(),
            slice::from_ref(obligation),
        );
        found = match store.commit(batch).await {
            Ok(CommitOutcome::Committed) => return Ok(AbortOutcome::Fenced),
            Ok(CommitOutcome::Conflict) => {
                match lost(store, &session_key, &read, &obligation_key).await {
                    Ok(Lost::Moved(now)) => now,
                    // Spend the round; the session is still `read`.
                    Ok(Lost::Unexplained) => Some(read),
                    Ok(Lost::Taken) => {
                        let key = String::from_utf8_lossy(&obligation_key).into_owned();
                        return Err(doubt.end(upload_id, Stop::ObligationKeyTaken { key }));
                    }
                    Err(fault) => return Err(doubt.end(upload_id, Stop::Store(fault))),
                }
            }
            Err(fault) => {
                let Some(unknown) = fault.downcast_ref::<CommitUnknownResult>() else {
                    // A definite failure of this batch; an earlier batch's doubt still stands.
                    return Err(doubt.end(upload_id, Stop::Store(fault)));
                };
                doubt.observe(unknown, &read, metadata::encode(teardown.session()));
                match store.get(&session_key).await {
                    Ok(found) => found,
                    Err(fault) => return Err(doubt.end(upload_id, Stop::Store(fault))),
                }
            }
        };
    }
}

/// What a fence commit that answered `Conflict` lost to ([`lost`]).
enum Lost {
    /// The session is no longer the bytes the round read: this is what a read found there.
    Moved(Option<Bytes>),
    /// The session is unchanged and the obligation key absent: nothing a read can see explains
    /// the conflict.
    Unexplained,
    /// The obligation key is present, and the session was unchanged on both sides of that read.
    Taken,
}

/// Classify a fence commit that answered `Conflict`, from what the store holds, never by
/// inference: the session first; if unchanged, the obligation key; if that is present, the
/// session again, since every fence puts its session and its obligation in one batch and each
/// read is its own transaction, so a rival fence can land between the first two reads.
async fn lost(
    store: &impl MetadataStore,
    session_key: &[u8],
    read: &Bytes,
    obligation_key: &[u8],
) -> Result<Lost, BoxError> {
    let now = store.get(session_key).await?;
    if now.as_ref() != Some(read) {
        return Ok(Lost::Moved(now));
    }
    if store.get(obligation_key).await?.is_none() {
        return Ok(Lost::Unexplained);
    }
    let now = store.get(session_key).await?;
    if now.as_ref() != Some(read) {
        return Ok(Lost::Moved(now));
    }
    Ok(Lost::Taken)
}

/// What this Abort knows about its own fence commits whose outcome the store could not report.
#[derive(Debug, Default)]
struct Doubt {
    /// The session record each such fence puts: `Aborting@E+1` for the `Open@E` it required.
    /// Finding one of these at `mpu:<id>` is finding that fence landed.
    puts: Vec<Bytes>,
    /// **Held**: the first outcome that said `may_still_commit`, with the session bytes its batch
    /// requires. Kept, never replaced by a later outcome; only a read that finds the session no
    /// longer those bytes clears it.
    held: Option<(CommitUnknownResult, Bytes)>,
    /// **Unsettled**: the latest out-of-flight outcome no successful read has followed yet.
    unsettled: Option<CommitUnknownResult>,
}

impl Doubt {
    fn observe(&mut self, outcome: &CommitUnknownResult, required: &Bytes, puts: Bytes) {
        self.puts.push(puts);
        if !outcome.may_still_commit {
            self.unsettled = Some(outcome.clone());
        } else if self.held.is_none() {
            self.held = Some((outcome.clone(), required.clone()));
        }
    }

    /// Whether a read that found `found` found a fence of this Abort landed.
    fn landed(&self, found: Option<&Bytes>) -> bool {
        found.is_some_and(|found| self.puts.contains(found))
    }

    /// A successful read of `mpu:<id>` that found `found`, and not a fence of this Abort: no
    /// out-of-flight fence of it landed, and a held fence whose required bytes are gone can never
    /// land.
    fn settle(&mut self, found: Option<&Bytes>) {
        self.unsettled = None;
        if self
            .held
            .as_ref()
            .is_some_and(|(_, required)| found != Some(required))
        {
            self.held = None;
        }
    }

    /// The error an Abort stopped by `cause` ends with: in doubt while a fence of it may still
    /// land or was never settled, else stopped.
    fn end(self, upload_id: &UploadId, cause: Stop) -> AbortError {
        let upload_id = upload_id.clone();
        match self.held.map(|(outcome, _)| outcome).or(self.unsettled) {
            Some(outcome) => AbortError::InDoubt(Box::new(InDoubt {
                upload_id,
                may_still_commit: outcome.may_still_commit,
                outcome,
                cause,
            })),
            None => AbortError::Stopped { upload_id, cause },
        }
    }
}
