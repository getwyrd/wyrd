//! `CreateMultipartUpload`'s **admission and session creation**: the multipart protocol's first
//! store round trip (#894, slice 3a of #636).
//!
//! [`create_session`] admits a session through the serialized fleet ledger `mpuctl` and installs
//! it, in one atomic batch per attempt (proposal 0016's *Create session* row, `0016:657`). It has
//! the shape of [`crate::write::commit_create`]: the store as `&impl MetadataStore`, and one
//! [`WriteBatch`] whose preconditions carry the whole decision.
//!
//! | Ledger | Preconditions and mutations |
//! |---|---|
//! | present | `require(mpuctl == prior bytes)`, admitted only while `prior.count < prior.max_sessions` (the **stored** limit) and only when the caller's [`Budget`] equals `prior.profile` ([`Refusal::ProfileSkew`] otherwise) |
//! | absent | `require_absent(mpuctl)`, and a bootstrap ledger `{count: 1, max_sessions, profile}` from the caller's [`Budget`] |
//! | either | `require_absent(mpu:<id>)`, `require_absent(seggrp:<nonce>)`, `require(bucket:<name> == the marker bytes read)`; put the session, put the `seggrp:` marker, put the ledger at `count + 1` |
//!
//! So every `mpu:` session and `seggrp:` marker a create installs is counted by `mpuctl` in the
//! same batch, and every session it installs is one its caller is told about: by
//! [`CreateOutcome::Created`] for that very session, or by a [`CreateError::InDoubt`] that names
//! its identity (**C-1**, `docs/principles.md` §5). The reaper (#625) and the terminal delete
//! (#659) can then reach every admitted session; nothing here is outside their scan.
//!
//! Production calls nothing in this module yet: the S3 verb that will is #668, which also carries
//! 0016's rule that the reaper and operator abort ship in the same release (`0016:236-268`).
//!
//! # Runtime-free (ADR-0009)
//!
//! Core reads no clock, draws no randomness and never sleeps here. The session's
//! `created_at_millis` and the `clock_source` that names its clock arrive in the
//! [`CreateRequest`]: that pair is the one clock of the session's residency lifecycle, the one
//! the reaper's `W_session` ceiling and its clock guard read (`0016:1957-1990`), so this function
//! adds no clock read of its own. Identities, backoff jitter and the backoff wait come from the
//! caller's [`CreateRuntime`].

use std::fmt;
use std::future::Future;

use bytes::Bytes;
use wyrd_traits::{BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, WriteBatch};

use super::{
    admission_backoff_millis, decode_admission_record, decode_session_record, mpu_key,
    AdmissionRecord, Backpressure, Budget, CreateOutcome, RecordError, Refusal, SessionRecord,
    UploadId, MAX_UPLOAD_ID_ATTEMPTS, MPUCTL_KEY,
};
use crate::metadata::{self, InodeId, SegmentNonce, MAX_VALUE_BYTES, SEGGRP_MARKER};

/// What one `CreateMultipartUpload` asks for. Every field is the caller's: core composes no
/// name and reads no clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateRequest<'a> {
    /// The bucket whose `bucket:<name>` marker must exist, and must not change, for the session
    /// to be admitted (ADR-0046 §4).
    pub bucket: &'a str,
    /// The dirent parent the session will publish under (`publish_target.parent`, `0016:350`).
    /// At Alpha every object dirent lives under the root inode with the flat name
    /// `<bucket>/<key>` (ADR-0046 decision 3); the composition root spells `parent` and
    /// [`Self::object`] for a session exactly as it does for a `PutObject` to the same key, and
    /// names the same bucket in [`Self::bucket`].
    pub parent: InodeId,
    /// The dirent name the session will publish (`publish_target.name`, `0016:350`).
    pub object: &'a str,
    /// The client's declared content type, if any. `None` is stored by omitting the field.
    pub content_type: Option<&'a str>,
    /// When the session was created, in milliseconds of the clock [`Self::clock_source`] names.
    pub created_at_millis: u64,
    /// The clock that stamped [`Self::created_at_millis`]; the reaper judges a session's age
    /// only under a clock it recognises (`0016:1957-1990`).
    pub clock_source: &'a str,
}

/// The caller's runtime, which core does not have (ADR-0009): the randomness a session's identity
/// and the backoff jitter are drawn from, and the wait between admission attempts.
pub trait CreateRuntime {
    /// A fresh session identity: an upload id and a segment-group nonce, each a 128-bit random
    /// token (`UploadId`, `SegmentNonce`: 32 lowercase hex characters) from the caller's
    /// randomness. A create calls it once, and again only after a collision.
    fn mint(&mut self) -> (UploadId, SegmentNonce);

    /// A uniformly random `u64`, placed into the backoff window by [`admission_backoff_millis`].
    fn jitter(&mut self) -> u64;

    /// Wait `millis` milliseconds before the create's next admission attempt.
    fn wait(&mut self, millis: u64) -> impl Future<Output = ()> + Send;
}

/// A create that ended with neither an admission nor a protocol refusal.
///
/// # Error class
///
/// [`wyrd_traits::classify`] reads [`Self::InDoubt`] as
/// [`ErrorClass::Indeterminate`](wyrd_traits::ErrorClass::Indeterminate), whatever stopped the
/// create: its [`source`](std::error::Error::source) is the [`CommitUnknownResult`] that left a
/// batch in doubt, so a generic retry policy never re-runs it under a fresh identity. Every other
/// variant keeps its cause's class: a [`Self::Store`] wrapping a
/// [`TransientFault`](wyrd_traits::TransientFault) is transient, because nothing is in doubt.
#[derive(Debug)]
pub enum CreateError {
    /// Every one of [`MAX_UPLOAD_ID_ATTEMPTS`] identities collided with a record already in the
    /// store (an `mpu:<id>` or a `seggrp:<nonce>`). Nothing was written.
    IdentityCollisions {
        /// Identities minted and discarded.
        attempts: u32,
    },
    /// A batch of this create may have landed, or may still land, and the create stopped before
    /// it could settle which. It names the identity, so the session that batch would install is
    /// one the caller has been told about.
    ///
    /// The upload id is a **report**, not a usable session: it says a batch under that id may
    /// land, not that one did. Re-read it; never retry the create.
    InDoubt(Box<InDoubt>),
    /// The stored `mpuctl` ledger is not a valid admission record. No batch of this create is in
    /// doubt.
    Ledger(RecordError),
    /// A store operation failed. No batch of this create is in doubt.
    Store(BoxError),
}

/// What a [`CreateError::InDoubt`] reports: the identity whose batch is in doubt, whether it may
/// still land, the unknown outcome that put it in doubt, and what stopped the create.
#[derive(Debug)]
pub struct InDoubt {
    /// The upload id the doubtful batch would install `mpu:<id>` under.
    pub upload_id: UploadId,
    /// The segment-group nonce that batch would reserve under `seggrp:<nonce>`.
    pub segment_nonce: SegmentNonce,
    /// Whether that batch may still be applied **after** the create returned
    /// ([`CommitUnknownResult::may_still_commit`]). Once any commit of the create under this
    /// identity said so, only a read that finds R (the batch's own session record, unchanged)
    /// clears it, so this stays `true` whatever outcomes came after, an
    /// [`Unsettled::SessionMoved`] ending included.
    pub may_still_commit: bool,
    /// The store's own report of the unknown outcome that keeps the doubt: the first that said
    /// `may_still_commit`, or else the latest that did not. It is the error's
    /// [`source`](std::error::Error::source).
    pub outcome: CommitUnknownResult,
    /// What stopped the create before it could settle the doubt.
    pub cause: Unsettled,
}

/// What stopped a create whose batch is still in doubt ([`InDoubt::cause`]).
#[derive(Debug)]
pub enum Unsettled {
    /// The answer the create would have given had no batch of its own been in doubt.
    Refused(Refusal),
    /// Another record holds this identity: an `mpu:<id>` that is not the session this create's
    /// batch puts, or a `seggrp:<nonce>` with no session of this create's beside it. A batch of
    /// this create may still land, so the create may not mint a fresh identity: that batch would
    /// then install a session no answer named.
    IdentityTaken,
    /// The `mpu:<id>` record is this create's own session, but no longer R: a batch of this
    /// create landed, and the session has since moved on (a teardown fence took it to
    /// `Aborting`, say). Every field no transition changes is R's, so it is not a collision, and
    /// a fresh identity would leave it admitted, counted and unnamed. Nor is it a fresh
    /// session to answer `Created` for. So the create reports the identity instead.
    SessionMoved,
    /// The stored `mpuctl` ledger is not a valid admission record.
    Ledger(RecordError),
    /// A store operation failed.
    Store(BoxError),
}

impl fmt::Display for CreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentityCollisions { attempts } => write!(
                f,
                "multipart create: all {attempts} minted session identities collided with \
                 records already in the store; nothing was written"
            ),
            Self::InDoubt(doubt) => write!(
                f,
                "multipart create: upload {} (segment group {}) is in doubt: its batch {}; \
                 re-read it, do not retry ({})",
                doubt.upload_id,
                doubt.segment_nonce,
                if doubt.may_still_commit {
                    "may still be applied"
                } else {
                    "may have been applied"
                },
                doubt.cause,
            ),
            Self::Ledger(torn) => write!(f, "multipart create: the mpuctl ledger is torn: {torn}"),
            Self::Store(fault) => write!(f, "multipart create: store fault: {fault}"),
        }
    }
}

impl fmt::Display for Unsettled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(refusal) => write!(f, "stopped at the refusal {refusal:?}"),
            Self::IdentityTaken => f.write_str("another record holds its upload id or nonce"),
            Self::SessionMoved => f.write_str("its own session landed and has since moved on"),
            Self::Ledger(torn) => write!(f, "the mpuctl ledger is torn: {torn}"),
            Self::Store(fault) => write!(f, "store fault: {fault}"),
        }
    }
}

impl std::error::Error for CreateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IdentityCollisions { .. } => None,
            // The unknown outcome, never the stop cause: `classify` stops at the first seam type
            // on the chain, and a doubtful create must read `Indeterminate` even when a transient
            // fault is what stopped it.
            Self::InDoubt(doubt) => Some(&doubt.outcome),
            Self::Ledger(torn) => Some(torn),
            Self::Store(fault) => Some(fault.as_ref()),
        }
    }
}

/// **`CreateMultipartUpload`**: admit one session through `mpuctl` and install it
/// (`0016:657`), answering in the landed vocabulary ([`CreateOutcome`], [`Refusal`],
/// [`Backpressure`]).
///
/// `budget` is the caller's own profile. The ledger stores the profile the fleet admits under;
/// a caller whose profile differs is refused [`Refusal::ProfileSkew`] and writes nothing
/// (`0016:348`). No production path calls this yet (#668 will).
///
/// # The batch
///
/// One per attempt, built from a fresh read of `mpu:<id>`, `seggrp:<nonce>`, `mpuctl` and the
/// bucket marker: see the module table. A **full** ledger (at its stored `max_sessions`) is
/// [`Backpressure::SessionCap`], an absent bucket marker is [`Refusal::NoSuchBucket`], and so is
/// a marker that changed after this create first read it: an immutable marker that changed
/// belongs to a bucket deleted and created again (ADR-0046 decision 2). Each refusal writes
/// nothing. The session record is built **once** per identity, clock stamp included, so every
/// batch under an identity puts exactly the same bytes, R.
///
/// Before any read, the session's record is measured in the widest state it can later reach
/// (`SessionRecord::widest_reachable_bytes`), and a record that would cross [`MAX_VALUE_BYTES`]
/// there is refused [`Refusal::SessionTooLarge`]: admitted at `Open`, its fences could never be
/// written and its admission slot would never come back.
///
/// On every read the create judges the identity's own keys first, then the bucket marker, then
/// the ledger, and only then treats a commit it lost as contention.
///
/// # Two budgets, never shared
///
/// * **Contention**: [`Budget::admission_cas_attempts`] of `budget`, the profile's own
///   `max_sessions`: 46 at the shipped profile, and linearly more under a larger one. A commit
///   spends one when it loses the `mpuctl` CAS, when it comes back `Conflict` with every
///   precondition still holding on the re-read, or when it comes back with an outcome the store
///   could not report. After its `k`-th (counting from 0) the create waits
///   [`admission_backoff_millis`]`(k, jitter)` before it reads again, so a create that spends the
///   whole budget has waited at most `max_sessions ×` [`super::ADMISSION_BACKOFF_CAP_MILLIS`]
///   (2.3 s at the shipped profile). When the budget is spent the answer is
///   [`Backpressure::AdmissionContention`]: the create lost or could not settle its commit as
///   many times as the profile admits sessions. Each loss is the ledger moving, an outcome the
///   store could not report, or (on TiKV, whose locking read answers `Conflict` when it meets
///   another writer's lock, with nothing moved) a lock another writer held. So on TiKV, lock
///   contention alone can spend the budget on a ledger with room.
/// * **Collision**: [`MAX_UPLOAD_ID_ATTEMPTS`] identities. An identity whose `mpu:<id>` or
///   `seggrp:<nonce>` is present when this create reads it, and is not this create's own batch
///   (below), is a collision: the create mints a fresh one, whatever the record found contains.
///   When every identity collided it ends with [`CreateError::IdentityCollisions`].
///
/// A collision never spends the contention budget, and a lost CAS never spends the collision
/// budget.
///
/// # An unknown commit outcome (`0016:2010-2028`)
///
/// A [`CommitUnknownResult`] is never read as `Conflict` and never answered by a new identity
/// while its batch may still land. It spends one unit of contention, and the re-drive uses the
/// same upload id and nonce with every precondition rebuilt from a fresh read, the `mpuctl` CAS
/// included: replaying the identical batch could never commit once another create or a
/// teardown moved the ledger. `require_absent(mpu:<id>)` and `require_absent(seggrp:<nonce>)`
/// are what make the re-drive safe, so at most one of this create's batches lands, however the
/// ledger moves.
///
/// An outcome with `may_still_commit == true` **holds** the create: no later out-of-flight
/// outcome, failed read or store fault clears it, and only a read that finds R does. An outcome
/// with `may_still_commit == false` leaves it **unsettled** until the next successful read of
/// `mpu:<id>`.
///
/// So the create **settles before it gives up**. Before any answer other than `Created` while
/// held or unsettled, it reads `mpu:<id>` once more, since its own batch may be what filled the
/// ledger or took the nonce. If that read finds R the answer is `Created`; if it finds this
/// create's own session moved on (below) the answer is [`CreateError::InDoubt`]
/// ([`Unsettled::SessionMoved`]); if it finds anything else and nothing is held, the answer
/// stands. Otherwise, a held create or a failed settling read, it ends with
/// [`CreateError::InDoubt`], naming the upload id and nonce. While held, an identity taken by
/// another record ends the same way ([`Unsettled::IdentityTaken`]), never in a fresh identity:
/// the held batch could still land and install a session no answer named.
///
/// # Which session is this create's
///
/// The create answers `Created` for a session it **finds** only when both hold: it read that
/// `mpu:<id>` and `seggrp:<nonce>` absent before its first commit under the identity, and,
/// after a commit of its own under the identity returned an unknown outcome, the record found is
/// **exactly R**. Neither alone is enough. Content alone would adopt a byte-identical record
/// that was there before this create began: a session record carries no per-invocation token,
/// so two invocations given the same identity and request write identical bytes. History alone
/// would adopt a different create's session that took the same upload id with another nonce or
/// object.
///
/// A record found there that is **not** R can still be this create's own session: its batch
/// landed, and before the create read it back something moved the session on (a teardown fence
/// took it from `Open@0` to `Aborting@1`, say). While a batch of this create is held or
/// unsettled, a record whose every field no transition changes is R's (the same nonce, publish
/// target, content type, clock stamp and clock source; only `epoch`, `attempts` and `state`
/// differ) is that session. It is never a collision, because a fresh identity would leave it
/// admitted, counted and named by no answer. It is never `Created` either, because it is no
/// longer the `Open` session a create answers for. The create ends with
/// [`CreateError::InDoubt`] naming the identity ([`Unsettled::SessionMoved`]), with
/// `may_still_commit` set as for any other in-doubt ending. A record under the upload id with a
/// different nonce or request stays a collision.
///
/// That leaves one case outside the guarantee: two **live** invocations handed the same upload
/// id, the same nonce **and** the same request. Both read the identity absent, one batch lands,
/// and the other's re-read finds bytes it could itself have written, so both may answer
/// `Created`. Upload ids and nonces are 128-bit random tokens minted from the caller's
/// randomness ([`CreateRuntime::mint`]), so the chance is negligible in production. It is a
/// stated bound, not a tested guarantee.
///
/// # Termination
///
/// Every commit that neither admits nor meets a collision spends one unit of the contention
/// budget at the next read, and every collision spends one identity, so a create makes at most
/// `max_sessions + MAX_UPLOAD_ID_ATTEMPTS` admission commits, with at most two rounds of four
/// reads between two of them and one more read to settle a doubt before it returns. Each store
/// call is bounded by the store's own contract (every operation terminates,
/// `crates/traits/src/lib.rs`, "Operational envelope"), and each wait by
/// [`super::ADMISSION_BACKOFF_CAP_MILLIS`].
///
/// **Not cancel-safe once a commit is sent.** Dropping the future before its first commit is
/// sent writes nothing. Dropping it after (a request timeout wrapped around the create, say) can
/// leave a batch that lands with no `Created` and no [`CreateError::InDoubt`] naming it: an
/// admitted, counted session no client will ever name, which is the case `0016:2015-2018` rules
/// out. So bound a create through the store's own deadlines, never by dropping the future: every
/// store operation terminates, and a commit the store abandons at its deadline comes back as a
/// [`CommitUnknownResult`] (`crates/traits/src/lib.rs:1334-1345`), which this function settles
/// or reports as [`CreateError::InDoubt`]. Wiring the caller's deadline is #668's.
pub async fn create_session(
    store: &impl MetadataStore,
    budget: &Budget,
    request: &CreateRequest<'_>,
    runtime: &mut impl CreateRuntime,
) -> Result<CreateOutcome, CreateError> {
    let mut identity = Identity::mint(runtime, request);
    // Size headroom, before any read or commit. The nonce is fixed-width and the upload id is
    // only in the key, so every identity this create may re-mint measures the same.
    let widest = identity.record.widest_reachable_bytes();
    if widest > MAX_VALUE_BYTES {
        return Ok(CreateOutcome::Refused(Refusal::SessionTooLarge {
            bytes: widest as u64,
            limit: MAX_VALUE_BYTES as u64,
        }));
    }
    let contention_budget = budget.admission_cas_attempts();
    let bucket_key = metadata::bucket_key(request.bucket);
    // The marker bytes this create first read: a later read must find exactly these.
    let mut marker: Option<Bytes> = None;
    let mut collisions = 0u32;
    let mut losses = 0u32;
    // The previous commit did not admit: lost the CAS, or came back unknown.
    let mut lost = false;
    let mut doubt = Doubt::default();
    loop {
        let view = match View::read(store, &identity, &bucket_key).await {
            Ok(view) => view,
            Err(fault) => return finish(store, &identity, doubt, Ending::Store(fault)).await,
        };

        // The identity's own keys first. After an unknown outcome of this create's own, R
        // itself is its batch.
        if doubt.unknown {
            let found = identity.judge(view.session.as_ref());
            if found == Found::Exact {
                return Ok(identity.created());
            }
            // Our own session, moved on since our batch landed (a fence, say): never a
            // collision, since a fresh identity would leave it counted and unnamed.
            if let (Found::Moved, Some(outcome)) = (found, doubt.standing()) {
                return Err(identity.in_doubt(outcome.clone(), Unsettled::SessionMoved));
            }
            // `mpu:<id>` read, and neither R nor our session: no out-of-flight batch of ours
            // landed.
            doubt.unsettled = None;
        }
        if view.session.is_some() || view.group.is_some() {
            if let Some(held) = &doubt.held {
                // Another record holds the identity while our batch may still land. Settle once,
                // then report the doubt: a fresh identity would leave that batch unnamed.
                let cause = match read_own(store, &identity).await {
                    Ok(Found::Exact) => return Ok(identity.created()),
                    Ok(Found::Moved) => Unsettled::SessionMoved,
                    Ok(Found::Other) | Err(_) => Unsettled::IdentityTaken,
                };
                return Err(identity.in_doubt(held.clone(), cause));
            }
            // A collision: never adopted, whatever the record holds. Re-mint.
            collisions += 1;
            if collisions >= MAX_UPLOAD_ID_ATTEMPTS {
                return Err(CreateError::IdentityCollisions {
                    attempts: collisions,
                });
            }
            identity = Identity::mint(runtime, request);
            doubt = Doubt::default();
            lost = false;
            continue;
        }

        // The bucket: present, and the same marker this create first read.
        let current = match (view.marker, &marker) {
            (Some(current), Some(first)) if current != *first => None,
            (current, _) => current,
        };
        let Some(current) = current else {
            return finish(store, &identity, doubt, Refusal::NoSuchBucket.into()).await;
        };
        let current = marker.get_or_insert(current).clone();

        // The ledger, against its stored limit and its stored profile.
        let prior = match view.ledger {
            None => None,
            Some(bytes) => match decode_admission_record(&bytes) {
                Ok(record) => Some((bytes, record)),
                Err(torn) => return finish(store, &identity, doubt, Ending::Ledger(torn)).await,
            },
        };
        let next = match &prior {
            None => AdmissionRecord::bootstrap(*budget),
            Some((_, record)) if record.profile() != budget => {
                let skew = Refusal::ProfileSkew {
                    local: *budget,
                    ledger: *record.profile(),
                };
                return finish(store, &identity, doubt, skew.into()).await;
            }
            Some((_, record)) => match record.admit_one() {
                Some(next) => next,
                None => {
                    let full = Backpressure::SessionCap {
                        count: record.count(),
                        max_sessions: record.max_sessions(),
                    };
                    return finish(store, &identity, doubt, slow_down(full)).await;
                }
            },
        };

        // The previous commit lost (only the ledger moved, or nothing did) or came back unknown:
        // spend contention budget, back off, and read again before the next attempt.
        if std::mem::take(&mut lost) {
            losses += 1;
            if losses >= contention_budget {
                let contention = Backpressure::AdmissionContention { attempts: losses };
                return finish(store, &identity, doubt, slow_down(contention)).await;
            }
            let delay = admission_backoff_millis(losses - 1, runtime.jitter());
            runtime.wait(delay).await;
            continue;
        }

        let prior = prior.as_ref().map(|(bytes, _)| bytes);
        let batch = admission_batch(&identity, prior, &next, &bucket_key, &current);
        match store.commit(batch).await {
            Ok(CommitOutcome::Committed) => return Ok(identity.created()),
            Ok(CommitOutcome::Conflict) => lost = true,
            Err(fault) => match fault.downcast_ref::<CommitUnknownResult>() {
                Some(unknown) => {
                    doubt.observe(unknown);
                    lost = true;
                }
                // A definite failure of this batch; an earlier batch's doubt still stands.
                None => return finish(store, &identity, doubt, Ending::Store(fault)).await,
            },
        }
    }
}

/// One minted identity and the session record it would install.
struct Identity {
    upload_id: UploadId,
    nonce: SegmentNonce,
    session_key: Vec<u8>,
    group_key: Vec<u8>,
    record: SessionRecord,
    /// `record`, encoded: R, the exact bytes every batch under this identity puts.
    session: Bytes,
}

impl Identity {
    fn mint(runtime: &mut impl CreateRuntime, request: &CreateRequest<'_>) -> Self {
        let (upload_id, nonce) = runtime.mint();
        let record = SessionRecord::open(
            request.parent,
            request.object.to_owned(),
            request.content_type.map(str::to_owned),
            request.created_at_millis,
            request.clock_source.to_owned(),
            nonce.clone(),
        );
        Self {
            session_key: mpu_key(&upload_id),
            group_key: metadata::seggrp_key(&nonce),
            session: metadata::encode(&record),
            record,
            upload_id,
            nonce,
        }
    }

    /// What a read of this identity's `mpu:<id>` found.
    fn judge(&self, found: Option<&Bytes>) -> Found {
        match found {
            Some(bytes) if *bytes == self.session => Found::Exact,
            Some(bytes) => match decode_session_record(bytes) {
                Ok(record) if self.record.same_session(&record) => Found::Moved,
                _ => Found::Other,
            },
            None => Found::Other,
        }
    }

    fn created(&self) -> CreateOutcome {
        CreateOutcome::Created {
            upload_id: self.upload_id.clone(),
        }
    }

    fn in_doubt(&self, outcome: CommitUnknownResult, cause: Unsettled) -> CreateError {
        CreateError::InDoubt(Box::new(InDoubt {
            upload_id: self.upload_id.clone(),
            segment_nonce: self.nonce.clone(),
            may_still_commit: outcome.may_still_commit,
            outcome,
            cause,
        }))
    }
}

/// What this create knows about its own commits under the current identity.
#[derive(Debug, Default)]
struct Doubt {
    /// A commit under this identity returned an unknown outcome. With the absence read every
    /// identity gets before its first commit, this is what lets R found under the identity be
    /// answered as this create's own.
    unknown: bool,
    /// **Held**: the first unknown outcome under this identity that said `may_still_commit`.
    /// Only a read that finds R clears it, and that read answers `Created`.
    held: Option<CommitUnknownResult>,
    /// **Unsettled**: the latest out-of-flight unknown outcome that no successful read of
    /// `mpu:<id>` has followed yet.
    unsettled: Option<CommitUnknownResult>,
}

impl Doubt {
    fn observe(&mut self, outcome: &CommitUnknownResult) {
        self.unknown = true;
        if !outcome.may_still_commit {
            self.unsettled = Some(outcome.clone());
        } else if self.held.is_none() {
            // Kept, never replaced by a later outcome.
            self.held = Some(outcome.clone());
        }
    }

    /// The outcome that keeps the create in doubt, if any: held first, else unsettled.
    fn standing(&self) -> Option<&CommitUnknownResult> {
        self.held.as_ref().or(self.unsettled.as_ref())
    }
}

/// An ending other than `Created` or a collision, before the settling rule decides whether it
/// stands.
enum Ending {
    Refused(Refusal),
    Ledger(RecordError),
    Store(BoxError),
}

impl From<Refusal> for Ending {
    fn from(refusal: Refusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<Ending> for Unsettled {
    fn from(ending: Ending) -> Self {
        match ending {
            Ending::Refused(refusal) => Self::Refused(refusal),
            Ending::Ledger(torn) => Self::Ledger(torn),
            Ending::Store(fault) => Self::Store(fault),
        }
    }
}

fn slow_down(pressure: Backpressure) -> Ending {
    Ending::Refused(Refusal::SlowDown { pressure })
}

/// What a read of an identity's `mpu:<id>` found ([`Identity::judge`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Found {
    /// Exactly R.
    Exact,
    /// Not R, but this identity's own session moved on (`SessionRecord::same_session`): the
    /// same nonce and request, at a later epoch or state.
    Moved,
    /// Nothing, or a record that is not this identity's session: another nonce or request, or
    /// bytes that do not decode as a session.
    Other,
}

/// The settling read: one read of this identity's `mpu:<id>`.
async fn read_own(store: &impl MetadataStore, identity: &Identity) -> Result<Found, BoxError> {
    Ok(identity.judge(store.get(&identity.session_key).await?.as_ref()))
}

/// End a create that did not admit, by the settling rule. While held or unsettled it first
/// reads its own `mpu:<id>` once more and answers `Created` if that is R: its batch may be what
/// it just met (a ledger it filled). A read that finds this create's own session moved on ends in
/// [`Unsettled::SessionMoved`]. A read that finds anything else settles an out-of-flight
/// outcome; a failed one settles nothing. If the doubt still stands, the answer is
/// [`CreateError::InDoubt`] naming the identity; otherwise `ending` stands.
async fn finish(
    store: &impl MetadataStore,
    identity: &Identity,
    mut doubt: Doubt,
    ending: Ending,
) -> Result<CreateOutcome, CreateError> {
    if let Some(outcome) = doubt.standing() {
        match read_own(store, identity).await {
            Ok(Found::Exact) => return Ok(identity.created()),
            Ok(Found::Moved) => {
                return Err(identity.in_doubt(outcome.clone(), Unsettled::SessionMoved))
            }
            Ok(Found::Other) => doubt.unsettled = None,
            Err(_) => {}
        }
    }
    if let Some(outcome) = doubt.standing() {
        return Err(identity.in_doubt(outcome.clone(), ending.into()));
    }
    match ending {
        Ending::Refused(refusal) => Ok(CreateOutcome::Refused(refusal)),
        Ending::Ledger(torn) => Err(CreateError::Ledger(torn)),
        Ending::Store(fault) => Err(CreateError::Store(fault)),
    }
}

/// The four records one attempt decides on, each read fresh.
struct View {
    session: Option<Bytes>,
    group: Option<Bytes>,
    ledger: Option<Bytes>,
    marker: Option<Bytes>,
}

impl View {
    /// The identity's own two keys first: the absence read every identity gets before its first
    /// commit, and the read that settles an unknown outcome. Then the ledger and the bucket
    /// marker the batch is built from.
    async fn read(
        store: &impl MetadataStore,
        identity: &Identity,
        bucket_key: &[u8],
    ) -> Result<Self, BoxError> {
        Ok(Self {
            session: store.get(&identity.session_key).await?,
            group: store.get(&identity.group_key).await?,
            ledger: store.get(MPUCTL_KEY).await?,
            marker: store.get(bucket_key).await?,
        })
    }
}

/// The Create batch (`0016:657`): the ledger CAS (or its bootstrap), the identity's two
/// `require_absent`s and the bucket precondition; then the session, its `seggrp:` marker and the
/// ledger at `count + 1`, all in one commit.
fn admission_batch(
    identity: &Identity,
    prior: Option<&Bytes>,
    next: &AdmissionRecord,
    bucket_key: &[u8],
    marker: &Bytes,
) -> WriteBatch {
    let ledger = match prior {
        Some(prior) => WriteBatch::new().require(MPUCTL_KEY, prior.clone()),
        None => WriteBatch::new().require_absent(MPUCTL_KEY),
    };
    require_bucket(ledger, bucket_key, marker)
        .require_absent(identity.session_key.clone())
        .require_absent(identity.group_key.clone())
        .put(identity.session_key.clone(), identity.session.clone())
        .put(
            identity.group_key.clone(),
            Bytes::from_static(SEGGRP_MARKER),
        )
        .put(MPUCTL_KEY, metadata::encode(next))
}

/// The bucket-existence precondition of ADR-0046 §4 (`require` on the exact marker bytes read,
/// `docs/design/adr/0046-bucket-model-real-namespace.md:93-95`), kept in this one place.
fn require_bucket(batch: WriteBatch, bucket_key: &[u8], marker: &Bytes) -> WriteBatch {
    // deferred: #511. ADR-0046 §4 also requires `require_absent` on the bucket's deletion fence
    // (decision 5, `0046:104-121`). No fence key exists yet: decision 5 only sketches one and
    // leaves the choice to #511, and nothing writes one, so nothing can put a bucket into the
    // deleting state today. #511 adds the fence's `require_absent` here.
    batch.require(bucket_key.to_vec(), marker.clone())
}
