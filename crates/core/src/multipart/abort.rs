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
//! # One batch builder, four writers
//!
//! [`fence_batch`] is the only place the teardown fence's [`WriteBatch`] is built. The client
//! Abort here, the operator abort here ([`operator_abort`], FU-6), the post-restore session fence
//! (`crates/custodian/src/restore.rs`, `fence_session`) and the custodian's multipart reaper
//! (`crates/custodian/src/reaper.rs`, #625) all commit what it builds, so the writers of the
//! `Open → Aborting` and `Completing → Aborting` edges cannot drift apart in their preconditions
//! or their puts. A fence that loses is classified by one function too, [`lost`], which the two
//! Aborts and the reaper share; the two Aborts also share one round loop.
//!
//! # The operator abort
//!
//! [`operator_abort`] is the exit 0016 names for a session no clock will judge (`0016:255-262`):
//! it fences `Open@E` as the client Abort does and `Completing@E` as the restore fence does
//! ([`SessionRecord::completing_teardown`], `0016:665`), each in one batch, and refuses a
//! `Completed` or absent session. `wyrd mpu abort` (`crates/server/src/cli.rs`) is its caller.
//!
//! Production calls no client Abort yet: the S3 verb that will is #668, under 0016's rule that
//! the reaper and operator abort ship in the same release (`0016:236-268`). The restore fence, the
//! reaper and the operator abort are the production callers of [`fence_batch`].
//!
//! # Runtime-free (ADR-0009)
//!
//! Core reads no clock, draws no randomness and never sleeps here. The fence stamps no time: the
//! `Aborting` record carries the session's own `created_at_millis` and `clock_source` unchanged,
//! and a round that lost is retried at once, with no backoff to wait out.

use std::fmt;

use bytes::Bytes;
use wyrd_traits::{BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, WriteBatch};

use super::{
    abort_answer, decode_session_record, mpu_key, AbortAnswer, AbortOutcome, RecordError, Refusal,
    RetireObligation, SessionRecord, SessionState, UploadId, MAX_COMPLETE_ATTEMPTS,
};
use crate::metadata;

/// How many fence commits one [`abort_session`] makes at most: [`MAX_COMPLETE_ATTEMPTS`] `+ 1`.
///
/// A session goes back to `Open` only when a Complete fence is released, and it can spend at
/// most [`MAX_COMPLETE_ATTEMPTS`] charged Complete fences, so that many rounds cover a session
/// moved to a later `Open@E'` by each of them. The one more is the round that fences. A Complete
/// that fenced and was then rejected, or stopped by a store fault, behind its fence gives its
/// attempt back when it releases that fence (#1007), so such Completes can move the session under
/// the Abort more often than that; each such move spends a round too. A list already invalid
/// before the fence commits nothing and moves nothing. A round is also spent by a conflict no
/// re-read explains (on TiKV, a lock another writer held, with nothing moved) and by a commit whose
/// outcome the store could not report; those share the same budget. It is sized from the shipped
/// value of that knob, and it bounds the work one Abort does, not its correctness: an Abort that
/// spends it ends with an error that says whether a fence of it may still land, never with a wrong
/// answer.
///
/// It is sized for the `Open` door. On the operator abort's `Completing` door ([`operator_abort`])
/// every segment batch of a completer still running also moves the session, so aborting a session
/// whose completer is live can spend the budget and end [`Stop::RoundsSpent`]: that fails closed,
/// with no fence landed that the error does not name, and a re-run starts over.
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
    /// All [`ABORT_ROUNDS`] fence commits were made and the session was still in a state the
    /// Abort fences (`Open`; for [`operator_abort`], `Open` or `Completing`).
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
    /// The session is at the last epoch in a state the Abort fences, so no fence can bump it
    /// (`0016:704-708`).
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
                "all {rounds} fence rounds were spent and the session is still not fenced"
            ),
            Self::ObligationKeyTaken { key } => write!(
                f,
                "the obligation key {key} is already taken while the session is unchanged; this \
                 is damage a retry cannot settle"
            ),
            Self::EpochExhausted => {
                f.write_str("the session is at the last epoch, so no fence can bump it")
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
    fence_rounds(
        store,
        upload_id,
        |_| AbortOutcome::Refused(Refusal::NoSuchUpload),
        |_, _| AbortOutcome::Fenced,
        |record| {
            match abort_answer(Some(record.state())) {
                AbortAnswer::Fences => {}
                AbortAnswer::AlreadyAborting => {
                    return Ok(Round::Answer(AbortOutcome::AlreadyAborting))
                }
                AbortAnswer::Refused(refusal) => {
                    return Ok(Round::Answer(AbortOutcome::Refused(refusal)))
                }
            }
            let teardown = record
                .open_teardown(upload_id)
                .ok_or(Stop::EpochExhausted)?;
            Ok(Round::Fence {
                session: Box::new(teardown.session().clone()),
                obligations: vec![teardown.obligation().clone()],
            })
        },
    )
    .await
}

/// What an operator abort ([`operator_abort`]) answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorAbortOutcome {
    /// A fence commit of this abort the store acknowledged: the session is at `Aborting@E+1`, with
    /// its teardown obligations durable beside it.
    Fenced {
        /// The epoch the fence put the session at, `E+1`.
        epoch: u64,
    },
    /// The session is at `Aborting@epoch` exactly as a fence of this abort leaves it, found by a
    /// read after a fence commit of it whose outcome the store could not report. A rival fence
    /// of the same bytes (the restore fence, the reaper, another operator abort) puts the same
    /// record and the same obligations, so this does not say whose fence landed.
    FencedUnattributed {
        /// The epoch read, `E+1`.
        epoch: u64,
    },
    /// The session was already `Aborting` at `epoch`: nothing was written.
    AlreadyAborting {
        /// The epoch read.
        epoch: u64,
    },
    /// The session is `Completed` at `epoch`, a terminal state: abort does not apply to it.
    /// Nothing was written.
    Completed {
        /// The epoch read.
        epoch: u64,
    },
    /// No `mpu:<id>` record: there is no session to abort. Nothing was written.
    NoSuchUpload,
    /// No `mpu:<id>` record, read while a fence commit of this abort whose outcome the store
    /// could not report may have landed unseen: the drain may have torn the fenced session down
    /// since. Not "nothing was written".
    AbsentAfterDoubt,
}

/// **The operator abort** (FU-6, `0016:255-262`, `:572`): fence the session `upload_id` names to
/// `Aborting`, from `Open` **or** `Completing`, for the operator to run where no other actor will
/// move the session — a foreign-clocked session the reaper declines to judge (`0016:1984-1998`).
///
/// | State read | Answer | Written |
/// |---|---|---|
/// | `Open@E` | [`OperatorAbortOutcome::Fenced`] | the [`fence_batch`] of [`SessionRecord::open_teardown`]: the session at `Aborting@E+1` and its `retire:bytes:s:<id>:<E>` `{session, all}` (`0016:664`) |
/// | `Completing@E` | [`OperatorAbortOutcome::Fenced`] | the [`fence_batch`] of [`SessionRecord::completing_teardown`], the restore fence's one shape (`0016:665`): the session at `Aborting@E+1`, `retire:bytes:s:<id>:<E>` `{session, all}` and `retire:records:s:<id>:<E>` `{seg: <nonce, E>}` |
/// | `Aborting@E` | [`OperatorAbortOutcome::AlreadyAborting`] | nothing |
/// | `Completed@E` | [`OperatorAbortOutcome::Completed`] | nothing |
/// | no record | [`OperatorAbortOutcome::NoSuchUpload`] | nothing |
///
/// After a fence commit whose outcome the store could not report, the read that follows answers
/// it without claiming more than it saw: the session found exactly as that fence leaves it is
/// [`OperatorAbortOutcome::FencedUnattributed`], since a rival fence of the same bytes leaves it
/// the same; no record, while that fence is unsettled, is
/// [`OperatorAbortOutcome::AbsentAfterDoubt`], since the drain deletes the record of a fenced
/// session. Only a commit the store acknowledged is [`OperatorAbortOutcome::Fenced`].
///
/// Each fence is **one** batch: the state change and every obligation it installs commit
/// together or not at all, under `require` on the exact bytes read and `require_absent` on every
/// obligation key, so no interleaving leaves a fenced session whose segments or staged bytes have
/// no reclaimer (X57). The `Completing` door's `{seg}` obligation names the fence epoch's range
/// `seg:<nonce>:<E>:` only, never the whole group (F18).
///
/// Unlike the client Abort ([`abort_session`]), a `Completing` session is fenced here rather than
/// refused: the operator is the exit 0016 names for a session no clock will judge, and the
/// restore fence's batch is what a `Completing` teardown is.
///
/// The abort tears nothing down. Deleting records, writing `orphan:` marks and releasing the
/// admission slot stay the retirement drain's, which reads no clock (#659).
///
/// Rounds, doubt and termination are [`abort_session`]'s, from the same loop: at most
/// [`ABORT_ROUNDS`] fence commits, a lost fence classified by [`lost`] on every obligation key it
/// required absent, and an unknown outcome never read as a conflict. Not cancel-safe once a commit
/// is sent, for the reason given there.
pub async fn operator_abort(
    store: &impl MetadataStore,
    upload_id: &UploadId,
) -> Result<OperatorAbortOutcome, AbortError> {
    fence_rounds(
        store,
        upload_id,
        |in_doubt| {
            if in_doubt {
                OperatorAbortOutcome::AbsentAfterDoubt
            } else {
                OperatorAbortOutcome::NoSuchUpload
            }
        },
        |session, acknowledged| {
            let epoch = session.epoch();
            if acknowledged {
                OperatorAbortOutcome::Fenced { epoch }
            } else {
                OperatorAbortOutcome::FencedUnattributed { epoch }
            }
        },
        |record| {
            let (session, obligations) = match record.state() {
                SessionState::Open {} => {
                    let teardown = record
                        .open_teardown(upload_id)
                        .ok_or(Stop::EpochExhausted)?;
                    (
                        teardown.session().clone(),
                        vec![teardown.obligation().clone()],
                    )
                }
                SessionState::Completing { .. } => {
                    let teardown = record
                        .completing_teardown(upload_id)
                        .ok_or(Stop::EpochExhausted)?;
                    (
                        teardown.session().clone(),
                        vec![teardown.bytes().clone(), teardown.records().clone()],
                    )
                }
                SessionState::Aborting {} => {
                    let epoch = record.epoch();
                    return Ok(Round::Answer(OperatorAbortOutcome::AlreadyAborting {
                        epoch,
                    }));
                }
                SessionState::Completed { .. } => {
                    let epoch = record.epoch();
                    return Ok(Round::Answer(OperatorAbortOutcome::Completed { epoch }));
                }
            };
            Ok(Round::Fence {
                session: Box::new(session),
                obligations,
            })
        },
    )
    .await
}

/// What one round of [`fence_rounds`] does with the session it read.
enum Round<A> {
    /// Answer with this, writing nothing.
    Answer(A),
    /// Commit the [`fence_batch`] putting `session` and installing `obligations`. Boxed: the
    /// record is far larger than any answer.
    Fence {
        session: Box<SessionRecord>,
        obligations: Vec<RetireObligation>,
    },
}

/// The fence loop [`abort_session`] and [`operator_abort`] share: read the session, ask `plan`
/// what this round does with it, commit the fence it names, and answer from what the store then
/// holds. `absent` answers a read that found no record, told whether a fence of this loop may
/// have landed unseen before that read ([`Doubt::may_have_landed`]); `fenced` answers a fence of this loop that
/// landed, given the session it put and whether the store acknowledged it (`false`: a read found
/// the session as it puts it, after an unknown outcome). The rules are [`abort_session`]'s,
/// documented there.
async fn fence_rounds<A>(
    store: &impl MetadataStore,
    upload_id: &UploadId,
    absent: impl Fn(bool) -> A,
    fenced: impl Fn(&SessionRecord, bool) -> A,
    plan: impl Fn(&SessionRecord) -> Result<Round<A>, Stop>,
) -> Result<A, AbortError> {
    let session_key = mpu_key(upload_id);
    let mut doubt = Doubt::default();
    let mut rounds = 0u32;
    let mut found = match store.get(&session_key).await {
        Ok(found) => found,
        Err(fault) => return Err(doubt.end(upload_id, Stop::Store(fault))),
    };
    loop {
        // `found` is always the result of a read that succeeded.
        if let Some(session) = doubt.landed(found.as_ref()) {
            return Ok(fenced(session, false));
        }
        let in_doubt = doubt.may_have_landed();
        doubt.settle(found.as_ref());
        let Some(read) = found else {
            return Ok(absent(in_doubt));
        };
        let record = match decode_session_record(&read) {
            Ok(record) => record,
            Err(torn) => return Err(doubt.end(upload_id, Stop::Record(torn))),
        };
        let (session, obligations) = match plan(&record) {
            Ok(Round::Answer(answer)) => return Ok(answer),
            Ok(Round::Fence {
                session,
                obligations,
            }) => (*session, obligations),
            Err(cause) => return Err(doubt.end(upload_id, cause)),
        };
        if rounds == ABORT_ROUNDS {
            return Err(doubt.end(upload_id, Stop::RoundsSpent { rounds }));
        }
        rounds += 1;
        let batch = fence_batch(&session_key, &read, &session, &obligations);
        found = match store.commit(batch).await {
            Ok(CommitOutcome::Committed) => return Ok(fenced(&session, true)),
            Ok(CommitOutcome::Conflict) => {
                match lost_any(store, &session_key, &read, &obligations).await {
                    Ok((Lost::Moved(now), _)) => now,
                    // Spend the round; the session is still `read`.
                    Ok((Lost::Unexplained, _)) => Some(read),
                    Ok((Lost::Taken, obligation_key)) => {
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
                doubt.observe(unknown, &read, session);
                match store.get(&session_key).await {
                    Ok(found) => found,
                    Err(fault) => return Err(doubt.end(upload_id, Stop::Store(fault))),
                }
            }
        };
    }
}

/// [`lost`] over every obligation key a fence required absent, in the order it required them:
/// the first key that says [`Lost::Moved`] or [`Lost::Taken`] decides, with the key it was read
/// at; [`Lost::Unexplained`] only when every key does.
async fn lost_any(
    store: &impl MetadataStore,
    session_key: &[u8],
    read: &[u8],
    obligations: &[RetireObligation],
) -> Result<(Lost, Vec<u8>), BoxError> {
    let mut last = Vec::new();
    for obligation in obligations {
        let key = obligation.key();
        match lost(store, session_key, read, &key).await? {
            Lost::Unexplained => last = key,
            decided => return Ok((decided, key)),
        }
    }
    Ok((Lost::Unexplained, last))
}

/// What a [`fence_batch`] commit that answered `Conflict` lost to ([`lost`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lost {
    /// The session is no longer the bytes the round read: this is what a read found there.
    Moved(Option<Bytes>),
    /// The session is unchanged and the obligation key absent: nothing a read can see explains
    /// the conflict.
    Unexplained,
    /// The obligation key is present, and the session was unchanged on both sides of that read.
    Taken,
}

/// Classify a [`fence_batch`] commit that answered `Conflict`, from what the store holds, never
/// by inference: the session first; if unchanged, the obligation key; if that is present, the
/// session again, since every fence puts its session and its obligation in one batch and each
/// read is its own transaction, so a rival fence can land between the first two reads.
///
/// The one classifier for the one fence: the client Abort spends a round on what it finds, and
/// the custodian's reaper (#625) holds a session whose obligation key is [`Lost::Taken`] rather
/// than counting it as contention.
pub async fn lost(
    store: &(impl MetadataStore + ?Sized),
    session_key: &[u8],
    read: &[u8],
    obligation_key: &[u8],
) -> Result<Lost, BoxError> {
    let now = store.get(session_key).await?;
    if now.as_deref() != Some(read) {
        return Ok(Lost::Moved(now));
    }
    if store.get(obligation_key).await?.is_none() {
        return Ok(Lost::Unexplained);
    }
    let now = store.get(session_key).await?;
    if now.as_deref() != Some(read) {
        return Ok(Lost::Moved(now));
    }
    Ok(Lost::Taken)
}

/// What this Abort knows about its own fence commits whose outcome the store could not report.
#[derive(Debug, Default)]
struct Doubt {
    /// The session record each such fence puts, encoded and as built: `Aborting@E+1` for the
    /// `Open@E` or `Completing@E` it required. Finding one of these at `mpu:<id>` is finding that
    /// fence landed.
    puts: Vec<(Bytes, SessionRecord)>,
    /// **Held**: the first outcome that said `may_still_commit`, with the session bytes its batch
    /// requires. Kept, never replaced by a later outcome; only a read that finds the session no
    /// longer those bytes clears it.
    held: Option<(CommitUnknownResult, Bytes)>,
    /// **Unsettled**: the latest out-of-flight outcome no successful read has followed yet.
    unsettled: Option<CommitUnknownResult>,
}

impl Doubt {
    fn observe(&mut self, outcome: &CommitUnknownResult, required: &Bytes, puts: SessionRecord) {
        self.puts.push((metadata::encode(&puts), puts));
        if !outcome.may_still_commit {
            self.unsettled = Some(outcome.clone());
        } else if self.held.is_none() {
            self.held = Some((outcome.clone(), required.clone()));
        }
    }

    /// The session a fence of this Abort put, when a read that found `found` found that fence
    /// landed.
    fn landed(&self, found: Option<&Bytes>) -> Option<&SessionRecord> {
        let found = found?;
        self.puts
            .iter()
            .find(|(put, _)| put == found)
            .map(|(_, session)| session)
    }

    /// Whether a fence of this Abort may have landed unseen: one may still land (`held`), or one
    /// out of flight has no successful read after it (`unsettled`). Asked before
    /// [`Self::settle`], a read that then finds no record cannot say that fence never landed: a
    /// fenced session's record is deleted by its drain.
    fn may_have_landed(&self) -> bool {
        self.held.is_some() || self.unsettled.is_some()
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
