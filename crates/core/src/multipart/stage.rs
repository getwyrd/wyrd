//! `UploadPart`'s **staging half** (#657; this module is #932, the first of #926's two slices):
//! reserve one in-flight slot, stream the part body into chunks, register each chunk as an owned
//! `sidx:` entry **before** any of its fragments is written, and write the fragments under an
//! authorization deadline (proposal 0016 decision 5, `0016:1676-1847`, and its *Part slot
//! reserve* and *Part intent* rows, `0016:658-659`). What [`stage_part`] hands back, a
//! [`StagedAttempt`], is everything the part commit (#927) builds its batch, or its
//! compensation, from.
//!
//! | Batch | Preconditions | Puts |
//! |---|---|---|
//! | reserve | `require(mpu:<id> == Open@E)` (the bytes read), `require_absent(slot:<id>:<k>)` | the slot record |
//! | intent, one per chunk | `require(mpu:<id> == Open@E)`, `require_absent(desired:dserver:<S>)` for every `S` in the placement, `require(slot:<id>:<k> == prior)`, `require_absent(sidx:<id>:<n>:<chunk>)` | the owned entry, and the slot with its lease moved forward |
//!
//! Nothing here writes `part:` or `psum:`, releases a slot or deletes an owned entry: the part
//! commit and its compensation are #927. Nothing here renews a lease between those batches
//! either: in-flight renewal is #926's second slice (#933), so until it lands a request that
//! outlives a lease stops at its next intent ([`Stop::LeaseLapsed`]). No production path calls
//! this module yet (the S3 verb is #669).
//!
//! # Order, and the drain fence
//!
//! A chunk's fragments are written only once its owned entry has been **observed durable** — a
//! `Committed` answer to its intent, or a re-read finding the entry's exact bytes. So no staged
//! byte exists that no `sidx:` record names (`0016:1530-1531`). The entry records the placement
//! the fragments go to, planned against the topology **minus every server with a drain record**
//! ([`Topology::excluding`]), and the intent `require_absent`s the drain record of each server it
//! names. An intent that loses to a drain recorded after it planned re-plans; it never refuses
//! the part for it (`0016:659`). Every intent also requires the session at the `Open@E` bytes the
//! attempt was fenced on, so no owned entry is created once the session has left `Open`.
//!
//! # The write deadline
//!
//! Every fragment write of a chunk carries the deadline `t_auth + W_write`, `t_auth` being the
//! clock reading taken before the chunk's intent was committed, however late the write is
//! dispatched (`0016:1553-1578`). The chunk store refuses a write that has not begun publishing
//! by then (#638; `ChunkStore::put_fragment`'s contract). Staging waits for a chunk's writes no
//! longer than that same instant, fail-closed (`0016:1555-1557`), and counts each write by what
//! it came to:
//!
//! | The write | What staging counts | What staging does |
//! |---|---|---|
//! | answered `Ok` | written, whatever the clock reads when the answer is seen: `Ok` means published strictly before the deadline | goes on, once every write of the chunk is |
//! | refused [`WriteEffect::NotApplied`], or not sent because the deadline had passed first | not written, and nothing of it landed | stops: [`Stop::WriteDeadlineElapsed`] |
//! | answered [`WriteEffect::Unknown`], failed some other way, or still pending at the deadline | neither written nor not | stops: [`Stop::WriteEffectUnknown`] |
//!
//! A write still pending at the deadline is **abandoned**, not retracted. No store may begin
//! publishing it any more, but one already inside its publishing step may still finish
//! (`crates/traits/src/lib.rs`, `ChunkStore::put_fragment`), so its effect is unknown, never
//! "nothing landed". Whatever lands sits on a position the chunk's durable owned entry already
//! names (position coverage, `0016:1549-1552`), so it is evidenced while that position's mark
//! survives. What this module does **not** establish is a bound on *when* such a straggler can
//! land: no store bounds its publishing step, and that is exactly the case
//! [`WriteEffect::Unknown`] reports. An abandoned write and a store's `Unknown` answer are the
//! same fact about the bytes, so they get the same error class
//! ([`ErrorClass::Indeterminate`](wyrd_traits::ErrorClass::Indeterminate)), however the timing
//! fell.
//!
//! # The body
//!
//! Each body read is raced against the caller's [`StageRuntime::wait`] for its idle deadline:
//! [`StageRequest::body_idle_millis`] after the read began. A read the body has not answered
//! while the clock is short of that deadline stops staging ([`Stop::BodyIdle`]): one still
//! pending when the timer fires, and one whose answer (a piece, or the body's end) is only seen
//! at or past the deadline, which is as late as no answer. Nothing is written after it. An empty
//! piece (HTTP/2 permits empty `DATA` frames) does not end the read, so it never moves that
//! deadline. A read is polled across the wait, never restarted, so while staging goes on no piece
//! is dropped or asked for twice. This is the opposite of a fragment write's rule above: a write's
//! `Ok` proves by itself that it came in time, and a late body answer proves nothing.
//!
//! # Leases
//!
//! The reserve leases the slot a TTL past its reading. Each intent leases its new owned entry and
//! moves the slot's lease, in the same batch, to one instant: a TTL past its own reading, or one
//! millisecond past the slot lease it replaces if that is later. So every lease an intent writes is
//! later than the slot lease its batch replaces, and no lease moves backwards. Slot values can
//! still repeat: the retry rounds of one intent may put the same slot value (a value depends only
//! on the round's reading and the slot lease it replaces), and every round of a reserve puts the
//! same bytes on purpose. A slot value an intent puts differs from every slot value that landed
//! before it, which is how a slot read recognises one of the intent's own rounds; it does not tell
//! one round from another, and the owned entry's bytes do. An intent moves a lease forward, so it
//! must never be the batch that revives a lapsed one: each of its rounds — the first and every
//! retry — takes a fresh clock reading, after its drain read, and checks the slot's lease and every
//! owned lease at it before it sends anything. A lapsed lease **refuses, writes nothing** and stops
//! staging (`0016:1843-1847`, the rule `write::lease_write_chunk` applies to an ordinary streaming
//! write), keeping the doubt of any batch already sent.
//!
//! # A commit that did not say `Committed`
//!
//! The store's report is never guessed at. After `Conflict` or a [`CommitUnknownResult`], the
//! batch's own record is read back — the slot for a reserve, the owned entry for an intent — and
//! only its exact bytes, or this attempt's id in the slot, prove it landed. Absence settles an
//! outcome only when the store said it can no longer land (`may_still_commit: false`). With
//! `may_still_commit: true` the doubt is **kept**: every later batch of the attempt requires the
//! slot (or, for a reserve, the slot's absence) at the bytes the doubtful batch also requires, so
//! at most one of them lands, and a loop that runs out of rounds returns [`StageError::InDoubt`]
//! carrying the doubt, as `abort::abort_session` does. A read that finds the slot no longer this
//! attempt's clears it: every batch requires the slot. So does a read that finds the session
//! moved, since every batch requires the session bytes and an epoch only rises — but only after
//! one more read of the doubtful batch's own record.
//!
//! That is because a doubtful batch can land between two of the reads, so neither is judged
//! alone. A batch that lands after its own record was read and before the session moved has
//! landed, though the record read missed it: the reserve and the intent each read their record
//! again after seeing the session move, and adopt the batch if it is there (for a reserve,
//! probing elsewhere would give the attempt two slots). Likewise an intent whose entry read
//! finds nothing but whose slot read finds the slot value one of its own batches put reads the
//! entry again and adopts that batch.
//!
//! # Bounds
//!
//! | Loop or wait | At most |
//! |---|---|
//! | reserve, probing free indices | `max_inflight_parts` commits; then `SlowDown` |
//! | reserve, retrying the index a doubtful batch may still install | [`RESERVE_AMBIGUOUS_ROUNDS`] commits; then `SlowDown` |
//! | intent, per chunk (re-plans included) | [`INTENT_ROUNDS`] commits |
//! | chunks, per part | `max_part_chunks`; a longer body stops staging ([`StagedAttempt::chunk_bound_exceeded`]) |
//! | a body read | `body_idle_millis` from the instant it began |
//! | a chunk's fragment writes | their deadline |
//!
//! # One clock, and no runtime (ADR-0009)
//!
//! Every time this module reads comes from the caller's [`StageRuntime::now_millis`]: the
//! reservation stamp, every lease, every lapse check, every write deadline and every body-idle
//! deadline. That clock owns the staging lease lifecycle, so it must be the one the reaper and GC
//! judge those leases by (the deployment wall clock in production; madsim's under DST), and the
//! deadline timer ([`StageRuntime::wait`]) must count the same time. Chunk ids come from the
//! caller too. The module spawns nothing.
//!
//! The tests drive these batches over a real store with every interleaving forced by a scripted
//! wrapper (`crates/core/tests/multipart_upload_part_stage.rs`). Seeded DST coverage of the races
//! (an intent against a drain request or an Abort) is deferred: #660, #665.

use std::collections::BTreeSet;
use std::fmt;
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::task::Poll;

use bytes::Bytes;
use futures_util::future::{select, Either};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, Stream, StreamExt};
use wyrd_traits::{
    classify, write_deadline_outcome, BoxError, ChunkId, CommitOutcome, CommitUnknownResult,
    DServerId, ErrorClass, FragmentId, MetadataStore, PlacementChunkStore, WriteBatch,
    WriteDeadlineExpired, WriteEffect,
};

use super::{
    decode_session_record, decode_slot_record, mpu_key, sidx_key, slot_key, slot_range,
    upload_part_answer, AttemptId, Backpressure, Digest, DigestHasher, OwnedEntry, PartNumber,
    RecordError, Refusal, ReserveOutcome, SlotIndex, SlotRecord, StagedPlacement, UploadId,
    UploadPartAnswer, MAX_SLOT_INDEX,
};
use crate::erasure::ErasureError;
use crate::metadata::{self, ChunkRef, EcScheme};
use crate::placement::{select_distinct_domains, SelectorError, Topology};
use crate::write::encode_chunk;

/// The most intent commits one chunk makes: its first, and three more for a drain recorded after
/// it planned, a conflict no read explains (on TiKV, a lock another writer held), or an outcome
/// the store could not report. Each round re-reads the drain records and plans again.
pub const INTENT_ROUNDS: u32 = 4;

/// The most reserve commits made at one index once a batch there came back
/// `CommitUnknownResult { may_still_commit: true }`, that batch included. The reserve retries
/// that index and no other (`0016:1707-1716`): a reserve that probed elsewhere could end up
/// holding two slots. When the rounds run out it answers `SlowDown`; a slot the doubtful batch
/// installs afterwards is residue, counted against the cap until the reaper reclaims it with the
/// session (`0016:1716-1717`, F11a).
pub const RESERVE_AMBIGUOUS_ROUNDS: u32 = 3;

/// What one part attempt asks for. Every field is the caller's: core reads no clock and mints no
/// id here.
#[derive(Debug, Clone, Copy)]
pub struct StageRequest<'a> {
    /// The session the part is staged under.
    pub upload_id: &'a UploadId,
    /// The part number.
    pub part_number: PartNumber,
    /// This attempt's 128-bit id, from the caller's randomness. The slot it reserves carries it,
    /// which is how an ambiguous reserve recognises its own slot (`0016:1699-1702`).
    pub attempt_id: &'a AttemptId,
    /// The size of the session's slot key space, `[0, max_inflight_parts)`
    /// ([`super::MAX_INFLIGHT_PARTS`] at the shipped profile). At least 1, and no wider than the
    /// `slot:` key grammar ([`super::MAX_SLOT_INDEX`]` + 1`).
    pub max_inflight_parts: u32,
    /// How long a lease this attempt writes runs past the reading it is written at. At least 2,
    /// so a lease written at one reading is still live at the next.
    pub lease_ttl_millis: u64,
    /// Bytes per chunk; the last chunk carries the remainder. At least 1.
    pub chunk_size: usize,
    /// The erasure scheme every chunk is written under.
    pub scheme: EcScheme,
    /// The fleet a chunk is placed on. Servers with a drain record are left out at every plan.
    pub topology: &'a Topology,
    /// `W_write`: every fragment write carries the deadline `t_auth + write_window_millis`,
    /// where `t_auth` is the clock reading taken before its chunk's intent was committed
    /// (`0016:1553-1578`). The custodian's `W_WRITE_MILLIS` in production; core cannot name it
    /// (ADR-0010). At least 1.
    pub write_window_millis: u64,
    /// The most chunks one part may register ([`super::MAX_PART_CHUNKS`] at the shipped
    /// profile). A body longer than that stops staging at the bound.
    pub max_part_chunks: u32,
    /// How long one body read may stay pending, from the instant it began. A read the body has
    /// not answered with a non-empty piece by then stops staging ([`Stop::BodyIdle`]). Each
    /// read gets the whole bound, so a slow body that keeps sending is not cut off; an empty
    /// piece does not count as sending. At least 1.
    pub body_idle_millis: u64,
}

impl StageRequest<'_> {
    /// The one check of a request, shared by [`reserve_slot`] and [`stage_part`] so that
    /// neither entry point writes what the other refuses: a field no staging can honour is
    /// [`Stop::InvalidRequest`], before anything is read or written.
    fn check(&self) -> Result<(), Stop> {
        let refusals = [
            (
                self.max_inflight_parts == 0,
                "max_inflight_parts is 0, so no slot could ever be reserved",
            ),
            (
                self.max_inflight_parts > MAX_SLOT_INDEX + 1,
                "max_inflight_parts is wider than the slot: key grammar can address",
            ),
            (
                self.lease_ttl_millis < 2,
                "lease_ttl_millis is below 2, so a lease would lapse at the next reading",
            ),
            (self.chunk_size == 0, "chunk_size is 0"),
            (
                self.write_window_millis == 0,
                "write_window_millis is 0, so every fragment write would be too late",
            ),
            (
                self.body_idle_millis == 0,
                "body_idle_millis is 0, so no body read could ever be waited for",
            ),
        ];
        match refusals.into_iter().find(|(refused, _)| *refused) {
            Some((_, reason)) => Err(Stop::InvalidRequest { reason }),
            None => Ok(()),
        }
    }
}

/// The caller's runtime, which core does not have (ADR-0009): the clock that owns an attempt's
/// leases and deadlines (the module doc's "One clock"), its timer, and the chunk ids.
pub trait StageRuntime {
    /// The current time in milliseconds.
    fn now_millis(&self) -> u64;

    /// A fresh chunk id for the next chunk.
    fn next_chunk_id(&mut self) -> ChunkId;

    /// Resolve once `millis` milliseconds of that same clock have passed: the timer for the
    /// deadline of a pending body read or of a chunk's fragment writes. It is raced against
    /// them and dropped when they answer first. Waking early is allowed: staging reads the
    /// clock again and waits for what is left. It takes `&self`, unlike `CreateRuntime::wait`,
    /// so the clock can be read while a wait is pending.
    fn wait(&self, millis: u64) -> impl Future<Output = ()> + Send;
}

/// What [`stage_part`] answers.
#[derive(Debug)]
pub enum StageOutcome {
    /// The body is staged: every chunk is registered and its fragments are written.
    Staged(Box<StagedAttempt>),
    /// A typed refusal: the session is not `Open` ([`Refusal::NoSuchUpload`]), or every slot
    /// index is taken ([`Backpressure::InflightParts`]). A refusal after the reservation leaves
    /// the slot and any owned entries in place, for the reaper to reclaim with the session.
    Refused(Refusal),
}

/// One staged chunk's owned `sidx:` entry, as the store holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedChunk {
    key: Vec<u8>,
    bytes: Bytes,
    entry: OwnedEntry,
}

impl OwnedChunk {
    fn new(key: Vec<u8>, entry: OwnedEntry) -> Self {
        Self {
            bytes: metadata::encode(&entry.to_pending()),
            key,
            entry,
        }
    }

    /// The entry's `sidx:<id>:<n>:<chunk>` key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// The entry's current bytes: what the next batch of this attempt requires there.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// The entry, its placement included.
    pub fn entry(&self) -> &OwnedEntry {
        &self.entry
    }
}

/// What every batch of an attempt requires: the session it was fenced on, its slot, and its
/// owned entries, each at the bytes the store holds.
#[derive(Debug, Clone)]
struct Footprint {
    upload_id: UploadId,
    session_key: Vec<u8>,
    session: Bytes,
    slot_key: Vec<u8>,
    slot: Bytes,
    record: SlotRecord,
    owned: Vec<OwnedChunk>,
}

/// A staged part attempt, handed to the part commit: everything it builds its batch, or its
/// compensation, from, without re-reading the body or re-deriving anything (`0016:660`, `:673`).
#[derive(Debug, Clone)]
pub struct StagedAttempt {
    footprint: Footprint,
    part_number: PartNumber,
    attempt_id: AttemptId,
    index: SlotIndex,
    chunks: Vec<ChunkRef>,
    len: u64,
    digest: Digest,
    chunk_bound_exceeded: bool,
}

impl StagedAttempt {
    /// The session.
    pub fn upload_id(&self) -> &UploadId {
        &self.footprint.upload_id
    }

    /// The part number.
    pub const fn part_number(&self) -> PartNumber {
        self.part_number
    }

    /// The attempt id the slot carries.
    pub fn attempt_id(&self) -> &AttemptId {
        &self.attempt_id
    }

    /// The `mpu:<id>` bytes every batch of the attempt required: the session at `Open@E`.
    pub fn session_bytes(&self) -> &Bytes {
        &self.footprint.session
    }

    /// The slot index the attempt holds.
    pub const fn slot_index(&self) -> SlotIndex {
        self.index
    }

    /// The slot's `slot:<id>:<k>` key.
    pub fn slot_key(&self) -> &[u8] {
        &self.footprint.slot_key
    }

    /// The slot's current bytes: what the commit requires there.
    pub fn slot_bytes(&self) -> &Bytes {
        &self.footprint.slot
    }

    /// The slot's current record.
    pub fn slot(&self) -> &SlotRecord {
        &self.footprint.record
    }

    /// Every owned entry, in chunk order.
    pub fn owned(&self) -> &[OwnedChunk] {
        &self.footprint.owned
    }

    /// One complete [`ChunkRef`] per chunk, in chunk order — `id`, `scheme`, `len` and
    /// `placement` — which is exactly the list a part record stores (`0016:351`). Every chunk but
    /// the last is `chunk_size` long; the last carries the remainder.
    pub fn chunks(&self) -> &[ChunkRef] {
        &self.chunks
    }

    /// The bytes staged. The whole body, unless [`Self::chunk_bound_exceeded`].
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether no byte was staged (an empty body).
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The SHA-256 of the bytes staged (ADR-0047). The body's digest, unless
    /// [`Self::chunk_bound_exceeded`].
    pub const fn digest(&self) -> &Digest {
        &self.digest
    }

    /// Whether the body was longer than `max_part_chunks` chunks. Staging then stopped at the
    /// bound, without reading the rest of the body: `max_part_chunks` entries are registered and
    /// written, and [`Self::len`] and [`Self::digest`] cover those chunks only. The commit must
    /// refuse such a part and compensate what is staged.
    pub const fn chunk_bound_exceeded(&self) -> bool {
        self.chunk_bound_exceeded
    }
}

/// Staging that ended with neither a staged attempt nor a refusal.
///
/// # Error class
///
/// [`wyrd_traits::classify`] reads [`Self::InDoubt`] as [`ErrorClass::Indeterminate`]: its
/// [`source`](std::error::Error::source) is the store's own [`CommitUnknownResult`].
/// [`Self::Stopped`] keeps its cause's class. For [`Stop::WriteEffectUnknown`] that is the class
/// of its most indeterminate account ([`Stop::WriteEffectUnknown`]'s `abandoned` and `reports`):
/// `Indeterminate` whenever a write was abandoned at its deadline or a chunk store answered
/// [`WriteEffect::Unknown`], since those are the same fact about the bytes, and otherwise the
/// class of the store faults it carries, `Transient` only if every one of them is. So the class
/// follows what is known about the bytes, never which write answered first or whether the store
/// answered before the deadline.
#[derive(Debug)]
pub enum StageError {
    /// A batch of this attempt may have landed, or may still land, and staging stopped before a
    /// read could say which. Never "nothing was written".
    InDoubt(Box<InDoubt>),
    /// Staging stopped, and no batch of it is in doubt. What it reserved and registered before
    /// it stopped stays in the store for the reaper.
    Stopped {
        /// The session.
        upload_id: UploadId,
        /// What stopped it.
        cause: Stop,
    },
}

/// What a [`StageError::InDoubt`] reports.
#[derive(Debug)]
pub struct InDoubt {
    /// The session.
    pub upload_id: UploadId,
    /// Whether a batch of this attempt may still be applied **after** staging returned
    /// ([`CommitUnknownResult::may_still_commit`]).
    pub may_still_commit: bool,
    /// The report that keeps the doubt: the first of the store's that said `may_still_commit`,
    /// or else the latest no read has settled. It is the error's
    /// [`source`](std::error::Error::source).
    pub outcome: CommitUnknownResult,
    /// What stopped staging before it could settle the doubt.
    pub cause: Stop,
}

/// What stopped staging ([`StageError::Stopped`], [`InDoubt::cause`]).
#[derive(Debug)]
pub enum Stop {
    /// The request cannot be staged as given. Nothing was read or written.
    InvalidRequest {
        /// Which field, and why.
        reason: &'static str,
    },
    /// The topology minus the draining servers offers fewer failure domains than a chunk has
    /// fragments.
    Placement(SelectorError),
    /// A chunk's intent made all [`INTENT_ROUNDS`] commits without landing.
    IntentRoundsSpent {
        /// The chunk.
        chunk: ChunkId,
        /// Commits made.
        rounds: u32,
    },
    /// A lease of this attempt had lapsed when an intent came to move the slot's lease forward.
    /// It is not revived (`0016:1843-1847`), and nothing was written.
    LeaseLapsed {
        /// The record whose lease lapsed, as UTF-8 (lossy).
        key: String,
        /// When its lease expired.
        lease_expiry_millis: u64,
        /// The reading it was judged at.
        now_millis: u64,
    },
    /// The attempt's slot no longer holds the bytes its last batch put, while the session is
    /// unchanged: something other than this attempt wrote or deleted it.
    SlotLost {
        /// The slot key, as UTF-8 (lossy).
        key: String,
    },
    /// A chunk's intent landed (the slot holds the value it put), but the chunk's owned entry no
    /// longer holds the bytes it put: something other than this attempt removed or rewrote it.
    /// No fragment of the chunk was written.
    EntryLost {
        /// The `sidx:` key, as UTF-8 (lossy).
        key: String,
    },
    /// A chunk's fragment writes did not all land, and **nothing** of the ones that did not
    /// landed: the store refused each as too late before publishing it
    /// ([`WriteEffect::NotApplied`]), or the deadline had passed before staging sent any. Every
    /// other write of the chunk was answered `Ok`, at the placement its owned entry records.
    WriteDeadlineElapsed {
        /// The chunk.
        chunk: ChunkId,
        /// The deadline its writes carried.
        deadline_millis: u64,
        /// Writes the store refused as too late; 0 when none was sent.
        refused: usize,
    },
    /// A chunk's fragment writes did not all land, and the **effect** of at least one is
    /// unknown: it was still pending at the deadline and staging stopped waiting for it (a
    /// store may still finish publishing it), the store answered [`WriteEffect::Unknown`] for
    /// it, or it failed in a way that does not say what it left. None of them is counted
    /// written, and none is reported as nothing written. Anything they left sits on a position
    /// the chunk's durable owned entry names.
    WriteEffectUnknown {
        /// The chunk.
        chunk: ChunkId,
        /// The deadline its writes carried.
        deadline_millis: u64,
        /// One account per write still pending at the deadline, which staging stopped waiting
        /// for, in fragment order: the seam's [`WriteEffect::Unknown`], because a store already
        /// inside its publishing step may still finish it. Staging writes these, not the store,
        /// and each one's `detail` says so.
        abandoned: Vec<WriteDeadlineExpired>,
        /// The chunk store's own answer for each write it answered without a verified landing,
        /// in the order they came.
        reports: Vec<BoxError>,
    },
    /// A body read was still pending [`StageRequest::body_idle_millis`] after it began: the
    /// client stopped sending (empty pieces do not count). What was staged stays for the
    /// reaper.
    BodyIdle {
        /// The bound that passed.
        idle_millis: u64,
    },
    /// A fresh chunk's `sidx:` key already holds bytes no batch of this attempt put.
    ChunkIdTaken {
        /// The key, as UTF-8 (lossy).
        key: String,
    },
    /// A stored record is not valid (ADR-0045), or a value this attempt would write is not one
    /// its decoder accepts.
    Record(RecordError),
    /// A chunk could not be erasure-coded.
    Encode(ErasureError),
    /// The part body failed.
    Body(BoxError),
    /// A metadata store operation failed.
    Store(BoxError),
}

impl StageError {
    fn stopped(upload_id: &UploadId, cause: Stop) -> Self {
        Self::Stopped {
            upload_id: upload_id.clone(),
            cause,
        }
    }
}

impl fmt::Display for StageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InDoubt(doubt) => write!(
                f,
                "multipart staging: upload {} is in doubt: a batch of this part attempt {} ({})",
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
                "multipart staging: upload {upload_id}: {cause}; no batch of this part attempt is \
                 in doubt"
            ),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { reason } => write!(f, "invalid request: {reason}"),
            Self::Placement(err) => write!(f, "{err}"),
            Self::IntentRoundsSpent { chunk, rounds } => {
                write!(f, "chunk {chunk}'s intent spent all {rounds} rounds")
            }
            Self::LeaseLapsed {
                key,
                lease_expiry_millis,
                now_millis,
            } => write!(
                f,
                "the lease on {key} lapsed at {lease_expiry_millis} (now {now_millis}); a lapsed \
                 lease is not revived"
            ),
            Self::SlotLost { key } => write!(f, "the slot {key} is no longer this attempt's"),
            Self::EntryLost { key } => write!(
                f,
                "the owned entry {key} was removed or rewritten after its intent landed"
            ),
            Self::WriteDeadlineElapsed {
                chunk,
                deadline_millis,
                refused: 0,
            } => write!(
                f,
                "chunk {chunk}'s write deadline {deadline_millis} had passed before its fragment \
                 writes were sent; none was sent"
            ),
            Self::WriteDeadlineElapsed {
                chunk,
                deadline_millis,
                refused,
            } => write!(
                f,
                "the chunk store refused {refused} of chunk {chunk}'s fragment writes as past \
                 their deadline {deadline_millis}; nothing of them landed"
            ),
            Self::WriteEffectUnknown {
                chunk,
                deadline_millis,
                abandoned,
                reports,
            } => {
                write!(
                    f,
                    "the effect of chunk {chunk}'s fragment writes is unknown: {} still pending \
                     at their deadline {deadline_millis} were abandoned, and {} were answered \
                     without a verified landing",
                    abandoned.len(),
                    reports.len()
                )?;
                if let Some(report) = most_indeterminate(reports, &[]) {
                    write!(f, " (the store: {report})")?;
                }
                f.write_str(
                    "; they are not counted as written, and any bytes they left sit at positions \
                     the chunk's owned entry names",
                )
            }
            Self::BodyIdle { idle_millis } => {
                write!(f, "the part body sent nothing for {idle_millis} ms")
            }
            Self::ChunkIdTaken { key } => {
                write!(f, "{key} already holds an entry this attempt did not write")
            }
            Self::Record(err) => write!(f, "invalid record: {err}"),
            Self::Encode(err) => write!(f, "erasure coding failed: {err}"),
            Self::Body(err) => write!(f, "the part body failed: {err}"),
            Self::Store(err) => write!(f, "store fault: {err}"),
        }
    }
}

impl std::error::Error for StageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // The unknown outcome, never the stop cause: `classify` stops at the first seam type
            // on the chain, and a doubtful attempt must read `Indeterminate`.
            Self::InDoubt(doubt) => Some(&doubt.outcome),
            Self::Stopped { cause, .. } => match cause {
                Stop::Placement(err) => Some(err),
                Stop::Record(err) => Some(err),
                Stop::Encode(err) => Some(err),
                Stop::Body(err) | Stop::Store(err) => Some(err.as_ref()),
                Stop::WriteEffectUnknown {
                    abandoned, reports, ..
                } => most_indeterminate(reports, abandoned),
                Stop::InvalidRequest { .. }
                | Stop::IntentRoundsSpent { .. }
                | Stop::LeaseLapsed { .. }
                | Stop::SlotLost { .. }
                | Stop::EntryLost { .. }
                | Stop::WriteDeadlineElapsed { .. }
                | Stop::BodyIdle { .. }
                | Stop::ChunkIdTaken { .. } => None,
            },
        }
    }
}

/// The account a [`Stop::WriteEffectUnknown`] is classed by: of the store's `reports` and the
/// `abandoned` writes, the one whose [`classify`] class says least is settled —
/// `Indeterminate`, then `Integrity`, then `Terminal`, and `Transient` last, so the stop reads
/// as worth retrying only if every account does. Ties go to the earliest, the store's own
/// reports first. So the class depends on what the writes came to, never on the order they
/// answered in.
fn most_indeterminate<'e>(
    reports: &'e [BoxError],
    abandoned: &'e [WriteDeadlineExpired],
) -> Option<&'e (dyn std::error::Error + 'static)> {
    let unsettled = |class| match class {
        ErrorClass::Indeterminate => 0,
        ErrorClass::Integrity => 1,
        ErrorClass::Terminal => 2,
        ErrorClass::Transient => 3,
    };
    reports
        .iter()
        .map(|report| report.as_ref() as &(dyn std::error::Error + 'static))
        .chain(
            abandoned
                .iter()
                .map(|write| write as &(dyn std::error::Error + 'static)),
        )
        .min_by_key(|account| unsettled(classify(*account)))
}

/// Staging's account of fragment write `id`, which the store had not answered when the clock
/// reached its `deadline_millis` and which staging stopped waiting for: the seam's
/// [`WriteEffect::Unknown`], since no store may begin publishing it any more but one already
/// inside its publishing step may still finish it (`ChunkStore::put_fragment`). So it is
/// classed `Indeterminate`, as the store's own `Unknown` answer is.
fn abandoned_write(id: FragmentId, deadline_millis: u64, now_millis: u64) -> WriteDeadlineExpired {
    WriteDeadlineExpired {
        id,
        effect: WriteEffect::Unknown,
        detail: format!(
            "the writer, not the store: no answer had come by the authorization deadline \
             {deadline_millis} ms (epoch), and the writer stopped waiting at its clock reading \
             {now_millis} ms. A store that began publishing it before the deadline may still \
             finish"
        ),
    }
}

/// Why a step stopped: a refusal the caller answers, or an error.
enum Halt {
    Refused(Refusal),
    Failed(StageError),
}

impl From<StageError> for Halt {
    fn from(err: StageError) -> Self {
        Self::Failed(err)
    }
}

/// What one loop knows about its own commits whose outcome the store could not report.
#[derive(Debug, Default)]
struct Doubt {
    /// The first outcome that said `may_still_commit`. Only a read that finds the session moved
    /// or the slot gone from under it clears it: the doubtful batch required both.
    held: Option<CommitUnknownResult>,
    /// The latest out-of-flight outcome no read has settled yet.
    unsettled: Option<CommitUnknownResult>,
}

impl Doubt {
    fn observe(&mut self, outcome: &CommitUnknownResult) {
        if outcome.may_still_commit {
            self.held.get_or_insert_with(|| outcome.clone());
        } else {
            self.unsettled = Some(outcome.clone());
        }
    }

    /// A read of the batch's own record that did not find it: an out-of-flight batch did not
    /// land, and never will.
    fn settle(&mut self) {
        self.unsettled = None;
    }

    /// The bytes every doubtful batch required are gone, and never come back: none can land.
    fn release(&mut self) {
        self.held = None;
        self.unsettled = None;
    }

    fn end(&self, upload_id: &UploadId, cause: Stop) -> StageError {
        match self.held.as_ref().or(self.unsettled.as_ref()) {
            Some(outcome) => StageError::InDoubt(Box::new(InDoubt {
                upload_id: upload_id.clone(),
                may_still_commit: outcome.may_still_commit,
                outcome: outcome.clone(),
                cause,
            })),
            None => StageError::stopped(upload_id, cause),
        }
    }
}

/// What a read of `mpu:<id>` found, for a verb that needs the session `Open`.
enum Session {
    Open(Bytes),
    Refused(Refusal),
}

fn judge_session(found: Option<Bytes>) -> Result<Session, RecordError> {
    let Some(bytes) = found else {
        return Ok(Session::Refused(Refusal::NoSuchUpload));
    };
    let record = decode_session_record(&bytes)?;
    Ok(match upload_part_answer(Some(record.state())) {
        UploadPartAnswer::Accepted => Session::Open(bytes),
        UploadPartAnswer::Refused(refusal) => Session::Refused(refusal),
    })
}

async fn read_session(store: &impl MetadataStore, upload_id: &UploadId) -> Result<Session, Stop> {
    let found = store.get(&mpu_key(upload_id)).await.map_err(Stop::Store)?;
    judge_session(found).map_err(Stop::Record)
}

/// Every D server with a drain record, whatever its value: `require_absent` tests presence.
async fn draining(store: &impl MetadataStore) -> Result<BTreeSet<DServerId>, BoxError> {
    Ok(store
        .scan(metadata::DESIRED_DSERVER_PREFIX)
        .await?
        .into_iter()
        .filter_map(|(key, _)| metadata::parse_desired_dserver_key(&key))
        .collect())
}

/// A chunk's placement: `fragments` servers on distinct failure domains, none draining — the
/// selector `write::WritePlan::place` runs, over the topology minus `draining`.
fn plan(
    topology: &Topology,
    draining: &BTreeSet<DServerId>,
    fragments: u16,
) -> Result<Vec<DServerId>, SelectorError> {
    select_distinct_domains(&topology.excluding(draining), fragments)
}

fn slow_down(max_inflight_parts: u32) -> Refusal {
    Refusal::SlowDown {
        pressure: Backpressure::InflightParts { max_inflight_parts },
    }
}

fn lossy(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

/// What [`bounded`] makes of an answer it sees once the clock has reached the deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Late {
    /// Taken: the answer speaks for its own timing. A fragment write answered `Ok` was published
    /// strictly before its deadline, checked by the store at both ends of its publishing step,
    /// whenever staging gets to see the answer; any other answer is the store's own account.
    Taken,
    /// Refused, as if it had never come: the deadline is the bound itself. A body read answered
    /// at or past its idle deadline, with a piece or with the body's end, is a body that sent
    /// nothing for the whole bound.
    Refused,
}

/// Await `work` until the clock reaches `deadline`, on the caller's timer: `Some` with its
/// output if it answered first, `None` once the deadline has passed with it still pending. On
/// `None`, `work` has been dropped: never awaited further, never restarted. An answer is taken
/// as it comes: the one `work` has ready when the timer fires too. A timer that fired early is
/// waited out again for what is left. An answer seen with the clock at or past `deadline` —
/// however the race fell, and whichever branch saw it — is judged by `late`.
async fn bounded<F: Future>(
    runtime: &impl StageRuntime,
    work: F,
    deadline: u64,
    late: Late,
) -> Option<F::Output> {
    let mut work = pin!(work);
    loop {
        let now = runtime.now_millis();
        if now >= deadline {
            return None;
        }
        let output = match select(work.as_mut(), pin!(runtime.wait(deadline - now))).await {
            Either::Left((output, _)) => output,
            Either::Right(((), _)) => match work.as_mut().now_or_never() {
                Some(output) => output,
                None => continue,
            },
        };
        return match late {
            Late::Taken => Some(output),
            Late::Refused => (runtime.now_millis() < deadline).then_some(output),
        };
    }
}

/// The body's next **non-empty** piece, its failure, or its end. Empty pieces are read and
/// skipped inside this one read, so they never end it; after each, the read yields once, so a
/// body that always has an empty piece ready cannot keep the caller from polling its timer.
async fn next_piece<B>(body: &mut B) -> Option<wyrd_traits::Result<Bytes>>
where
    B: Stream<Item = wyrd_traits::Result<Bytes>> + Unpin,
{
    loop {
        match body.next().await {
            Some(Ok(piece)) if piece.is_empty() => yield_once().await,
            other => return other,
        }
    }
}

/// Return `Pending` once, waking the task straight away.
async fn yield_once() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// A reservation that landed.
struct Reservation {
    session: Bytes,
    index: SlotIndex,
    slot: Bytes,
    record: SlotRecord,
}

enum Reserve {
    Reserved(Reservation),
    Refused(Refusal),
}

/// **Reserve one in-flight slot** for a part attempt (`0016:349`, `:658`, `:1676-1719`),
/// answering in [`ReserveOutcome`]. [`stage_part`] reserves through the same code; this entry
/// point reserves and stops.
///
/// The request is checked as [`stage_part`] checks it, by the same code: a request it cannot
/// honour (a `max_inflight_parts` of 0 or wider than the `slot:` key grammar, a
/// `lease_ttl_millis` below 2, or a field only staging reads that [`stage_part`] would refuse)
/// is [`Stop::InvalidRequest`] with nothing read or written.
///
/// Read `mpu:<id>`: unless it is `Open` the answer is [`Refusal::NoSuchUpload`] and nothing is
/// written. Read the bounded `slot:<id>:` range, take the lowest free index `k` below
/// `max_inflight_parts`, and commit `require(mpu == Open@E)` + `require_absent(slot:<id>:<k>)` +
/// the slot record (`part_number`, `attempt_id`, `reserved_at_millis = now_millis`,
/// `lease_expiry_millis = now_millis + lease_ttl_millis`). The `require_absent` authorizes; the
/// range read only chooses, so concurrent reservers cannot overshoot the cap.
///
/// * **Lost index**: probe the next free one, each probe against a different index, at most
///   `max_inflight_parts` probes. No free index left is
///   `SlowDown { InflightParts }`: live attempts and crashed ones' residue alike count.
/// * **Unknown outcome**: re-read the slot. This attempt's id there means the reserve landed: it
///   adopts the slot. With `may_still_commit: false`, anything else means it did not land. With
///   `may_still_commit: true`, absence proves nothing, so the reserve retries the **same** index
///   with the same attempt id, never a different one, and answers `SlowDown` after
///   [`RESERVE_AMBIGUOUS_ROUNDS`] commits there or once another attempt holds that index. A
///   slot the doubtful batch installs after that is residue (`0016:1716-1717`), so no attempt
///   ever holds two. A slot that does not decode is a [`Stop::Record`] error, never read as
///   another attempt's.
/// * **Session moved**: answered from the new state — a session no longer `Open` is
///   [`Refusal::NoSuchUpload`] on every path, and a later `Open` epoch is reserved under. If a
///   batch may still be in flight at the index, that index is read once more first: the batch
///   may have landed before the session moved, and the reserve then keeps that slot.
pub async fn reserve_slot(
    store: &impl MetadataStore,
    request: &StageRequest<'_>,
    now_millis: u64,
) -> Result<ReserveOutcome, StageError> {
    request
        .check()
        .map_err(|cause| StageError::stopped(request.upload_id, cause))?;
    let session = match read_session(store, request.upload_id).await {
        Ok(Session::Open(bytes)) => bytes,
        Ok(Session::Refused(refusal)) => return Ok(ReserveOutcome::Refused(refusal)),
        Err(cause) => return Err(StageError::stopped(request.upload_id, cause)),
    };
    Ok(match reserve(store, request, session, now_millis).await? {
        Reserve::Reserved(reservation) => ReserveOutcome::Reserved {
            index: reservation.index,
            attempt_id: request.attempt_id.clone(),
        },
        Reserve::Refused(refusal) => ReserveOutcome::Refused(refusal),
    })
}

/// The reserve loop, from the `Open` session bytes already read.
async fn reserve(
    store: &impl MetadataStore,
    request: &StageRequest<'_>,
    mut session: Bytes,
    now_millis: u64,
) -> Result<Reserve, StageError> {
    let upload_id = request.upload_id;
    let session_key = mpu_key(upload_id);
    let full = || Ok(Reserve::Refused(slow_down(request.max_inflight_parts)));
    let record = SlotRecord::reserve(
        request.part_number,
        request.attempt_id.clone(),
        now_millis,
        now_millis.saturating_add(request.lease_ttl_millis),
    )
    .map_err(|err| StageError::stopped(upload_id, Stop::Record(err)))?;
    // One stamp per reserve: every batch of it puts these same bytes, so whichever lands is the
    // same observable slot.
    let bytes = metadata::encode(&record);
    let mut doubt = Doubt::default();
    // Indices this reserve lost, or could not explain losing: never probed again.
    let mut lost: BTreeSet<u32> = BTreeSet::new();
    let mut probes = 0u32;
    // The index a batch of this reserve may still install, and the commits made there.
    let mut pinned: Option<SlotIndex> = None;
    let mut at_pin = 0u32;
    loop {
        let index = match pinned {
            Some(_) if at_pin == RESERVE_AMBIGUOUS_ROUNDS => return full(),
            Some(index) => index,
            None => {
                if probes == request.max_inflight_parts {
                    return full();
                }
                let taken: BTreeSet<Vec<u8>> = match store.scan(&slot_range(upload_id)).await {
                    Ok(found) => found.into_iter().map(|(key, _)| key).collect(),
                    Err(fault) => return Err(doubt.end(upload_id, Stop::Store(fault))),
                };
                let free = (0..request.max_inflight_parts)
                    .filter(|k| !lost.contains(k))
                    .filter_map(|k| SlotIndex::new(k).ok())
                    .find(|index| !taken.contains(&slot_key(upload_id, *index)));
                let Some(index) = free else {
                    return full();
                };
                probes += 1;
                index
            }
        };
        let key = slot_key(upload_id, index);
        let batch = WriteBatch::new()
            .require(session_key.clone(), session.clone())
            .require_absent(key.clone())
            .put(key.clone(), bytes.clone());
        match store.commit(batch).await {
            Ok(CommitOutcome::Committed) => {
                return Ok(Reserve::Reserved(Reservation {
                    session,
                    index,
                    slot: bytes,
                    record,
                }))
            }
            Ok(CommitOutcome::Conflict) => {}
            Err(fault) => match fault.downcast_ref::<CommitUnknownResult>() {
                Some(unknown) => {
                    doubt.observe(unknown);
                    if unknown.may_still_commit {
                        pinned = Some(index);
                    }
                }
                // A definite failure of this batch; an earlier batch's doubt still stands.
                None => return Err(doubt.end(upload_id, Stop::Store(fault))),
            },
        }
        if pinned.is_some() {
            at_pin += 1;
        }
        // Settle from what the store holds: the slot first, since only it can show this reserve
        // landed.
        let found = match read_own_slot(store, request, &key).await {
            Ok(found) => found,
            Err(cause) => return Err(doubt.end(upload_id, cause)),
        };
        if let Found::Ours(slot, record) = found {
            return Ok(Reserve::Reserved(Reservation {
                session,
                index,
                slot,
                record,
            }));
        }
        doubt.settle();
        let now = match store.get(&session_key).await {
            Ok(now) => now,
            Err(fault) => return Err(doubt.end(upload_id, Stop::Store(fault))),
        };
        if now.as_ref() != Some(&session) {
            let moved = match judge_session(now) {
                Ok(moved) => moved,
                Err(err) => return Err(doubt.end(upload_id, Stop::Record(err))),
            };
            if pinned.is_some() {
                // Every batch of this reserve required the session bytes that are gone, and an
                // epoch only rises, so none can land from here on. But one may have landed after
                // the slot read above and before the session moved: read the index again before
                // letting it go, or the next probe could give this attempt a second slot.
                match read_own_slot(store, request, &key).await {
                    Ok(Found::Ours(slot, record)) => {
                        return Ok(match moved {
                            Session::Open(session) => Reserve::Reserved(Reservation {
                                session,
                                index,
                                slot,
                                record,
                            }),
                            // The slot it installed is residue now, for the reaper.
                            Session::Refused(refusal) => Reserve::Refused(refusal),
                        });
                    }
                    Ok(Found::Absent | Found::Other) => {}
                    Err(cause) => return Err(doubt.end(upload_id, cause)),
                }
            }
            // Read after the session moved: no batch of this reserve landed, and none can.
            doubt.release();
            pinned = None;
            at_pin = 0;
            match moved {
                Session::Open(bytes) => session = bytes,
                Session::Refused(refusal) => return Ok(Reserve::Refused(refusal)),
            }
            continue;
        }
        match (found, pinned) {
            // Another attempt holds the index a batch of this reserve may still install. Probing
            // elsewhere could leave this attempt two slots once that attempt releases it
            // (`0016:1714-1716`).
            (Found::Other, Some(_)) => return full(),
            // Not landed and no longer in flight: taken by another attempt, or a loss no read
            // explains (on TiKV, a lock another writer held). Probe the next index.
            (_, None) => {
                lost.insert(index.get());
            }
            // May still land: retry the same index.
            (_, Some(_)) => {}
        }
    }
}

/// What a read of one slot index found, for the reserve that may have installed it.
enum Found {
    /// This attempt's slot: its bytes and record.
    Ours(Bytes, SlotRecord),
    /// Another attempt's slot.
    Other,
    /// Nothing.
    Absent,
}

/// Read `key` and judge it against `request`'s attempt. A stored slot that does not decode is an
/// error (ADR-0045), never "another attempt's".
async fn read_own_slot(
    store: &impl MetadataStore,
    request: &StageRequest<'_>,
    key: &[u8],
) -> Result<Found, Stop> {
    let Some(bytes) = store.get(key).await.map_err(Stop::Store)? else {
        return Ok(Found::Absent);
    };
    let record = decode_slot_record(&bytes).map_err(Stop::Record)?;
    Ok(
        if record.attempt_id() == request.attempt_id && record.part_number() == request.part_number
        {
            Found::Ours(bytes, record)
        } else {
            Found::Other
        },
    )
}

/// **Stage one part** (`0016:658-659`, `:1676-1847`): reserve a slot, then stream `body` into
/// `chunk_size` chunks, registering each chunk's owned `sidx:` entry before writing its
/// fragments. The module doc states the batches, the order, the deadlines and the bounds.
///
/// A request that cannot be honoured (a zero `chunk_size`, `write_window_millis` or
/// `body_idle_millis`, a `lease_ttl_millis` below 2, or a `max_inflight_parts` of 0 or wider than
/// the `slot:` key grammar) is [`Stop::InvalidRequest`] with nothing read or written; the check
/// is the one [`reserve_slot`] runs. A placement that cannot be met even before staging (too few
/// non-draining failure domains for the scheme) is [`Stop::Placement`] with nothing written; an
/// `Open` session is never refused [`Refusal::NoSuchUpload`] for it. A session that leaves `Open`
/// while the part stages is answered [`Refusal::NoSuchUpload`], and what was staged stays for the
/// reaper (F11a).
///
/// # Termination
///
/// Every loop is bounded (the module doc's table). Each metadata store call is bounded by the
/// store's own contract ("every operation terminates"). The wait for a chunk's fragment writes is
/// bounded by their deadline, and the wait for each body read by `body_idle_millis` from the
/// instant it began, both on the caller's timer; a body answer seen at or past its idle deadline
/// is refused as late.
///
/// **Not cancel-safe once a commit is sent**: dropping the future can leave a batch that lands
/// with no answer naming it. The slot and every owned entry it could leave are still in reach of
/// the reaper's per-session ranges.
pub async fn stage_part<B>(
    store: &impl MetadataStore,
    chunk_store: &impl PlacementChunkStore,
    runtime: &mut impl StageRuntime,
    request: &StageRequest<'_>,
    body: B,
) -> Result<StageOutcome, StageError>
where
    B: Stream<Item = wyrd_traits::Result<Bytes>> + Unpin,
{
    let stop = |cause| StageError::stopped(request.upload_id, cause);
    request.check().map_err(stop)?;
    let session = match read_session(store, request.upload_id).await.map_err(stop)? {
        Session::Open(bytes) => bytes,
        Session::Refused(refusal) => return Ok(StageOutcome::Refused(refusal)),
    };
    // Can a chunk be placed at all? Asked before anything is written, so a fleet with too few
    // non-draining domains for the scheme leaves no slot behind.
    let fragments = ChunkRef {
        id: 0,
        scheme: request.scheme,
        len: 0,
        placement: Vec::new(),
    }
    .fragment_count();
    let drains = draining(store)
        .await
        .map_err(|fault| stop(Stop::Store(fault)))?;
    plan(request.topology, &drains, fragments).map_err(|err| stop(Stop::Placement(err)))?;
    let reservation = match reserve(store, request, session, runtime.now_millis()).await? {
        Reserve::Reserved(reservation) => reservation,
        Reserve::Refused(refusal) => return Ok(StageOutcome::Refused(refusal)),
    };
    let stager = Stager::new(request, reservation);
    match stager.run(store, chunk_store, runtime, body).await {
        Ok(attempt) => Ok(StageOutcome::Staged(Box::new(attempt))),
        Err(Halt::Refused(refusal)) => Ok(StageOutcome::Refused(refusal)),
        Err(Halt::Failed(err)) => Err(err),
    }
}

/// An intent batch: what it puts, and the reading its deadline is measured from.
struct Intent {
    owned: OwnedChunk,
    slot: Bytes,
    record: SlotRecord,
    placement: Vec<DServerId>,
    t_auth: u64,
}

impl Footprint {
    fn stop(&self, cause: Stop) -> StageError {
        StageError::stopped(&self.upload_id, cause)
    }

    /// The slot lease a batch written at `now_millis` stamps: a TTL from now, and always later
    /// than the slot lease it replaces, so a lease never moves backwards and the value differs
    /// from every slot value that landed before the batch. It is not unique per round: retry
    /// rounds at one reading, replacing the same slot lease, stamp the same value.
    fn next_expiry(&self, now_millis: u64, lease_ttl_millis: u64) -> u64 {
        now_millis
            .saturating_add(lease_ttl_millis)
            .max(self.record.lease_expiry_millis().saturating_add(1))
    }

    /// Refuse, never revive: a lease of this attempt that lapsed at or before `now_millis` is
    /// dead authority (`0016:1843-1847`; `<=` is the reaper's own boundary, as in
    /// `metadata::renew_pending`). Every intent round runs this at its own fresh reading before
    /// it is sent, since it moves the slot's lease forward.
    fn check_live(&self, now_millis: u64) -> Result<(), Stop> {
        let slot = (&self.slot_key, self.record.lease_expiry_millis());
        let owned = self
            .owned
            .iter()
            .map(|owned| (&owned.key, owned.entry.lease_expiry_millis()));
        for (key, lease_expiry_millis) in std::iter::once(slot).chain(owned) {
            if lease_expiry_millis <= now_millis {
                return Err(Stop::LeaseLapsed {
                    key: lossy(key),
                    lease_expiry_millis,
                    now_millis,
                });
            }
        }
        Ok(())
    }

    /// After a batch of this attempt did not land: re-read the session, and judge the slot read
    /// as `found`. A session that moved is returned, for the caller to settle any doubtful batch
    /// by one more read of that batch's own record before it calls [`Self::moved_on`]: such a
    /// batch may have landed before the session moved. A slot that is not the bytes this attempt
    /// last put, under an unchanged session, is [`Stop::SlotLost`]. Anything else (`None`) is a
    /// loss no read explains, and the caller retries.
    async fn reconcile(
        &mut self,
        store: &impl MetadataStore,
        found: Option<Bytes>,
        doubt: &mut Doubt,
    ) -> Result<Option<Session>, Halt> {
        let now = store
            .get(&self.session_key)
            .await
            .map_err(|fault| doubt.end(&self.upload_id, Stop::Store(fault)))?;
        if now.as_ref() != Some(&self.session) {
            return judge_session(now)
                .map(Some)
                .map_err(|err| doubt.end(&self.upload_id, Stop::Record(err)).into());
        }
        if found.as_ref() != Some(&self.slot) {
            // Nothing is in doubt: every doubtful batch required the slot bytes that are gone.
            return Err(self
                .stop(Stop::SlotLost {
                    key: lossy(&self.slot_key),
                })
                .into());
        }
        Ok(None)
    }

    /// The session moved, and the caller's read after the move settled every doubtful batch: it
    /// adopted one that had landed, or found none had, and none can land now (each required the
    /// session bytes that are gone, and an epoch only rises). A later `Open` epoch is adopted, so
    /// the next round is fenced on it; a session that left `Open` is the refusal.
    fn moved_on(&mut self, moved: Session, doubt: &mut Doubt) -> Result<(), Halt> {
        doubt.release();
        match moved {
            Session::Open(bytes) => {
                self.session = bytes;
                Ok(())
            }
            Session::Refused(refusal) => Err(Halt::Refused(refusal)),
        }
    }
}

/// One attempt, while it stages.
struct Stager<'r> {
    request: &'r StageRequest<'r>,
    footprint: Footprint,
    index: SlotIndex,
    chunks: Vec<ChunkRef>,
}

impl<'r> Stager<'r> {
    fn new(request: &'r StageRequest<'r>, reservation: Reservation) -> Self {
        Self {
            footprint: Footprint {
                upload_id: request.upload_id.clone(),
                session_key: mpu_key(request.upload_id),
                session: reservation.session,
                slot_key: slot_key(request.upload_id, reservation.index),
                slot: reservation.slot,
                record: reservation.record,
                owned: Vec::new(),
            },
            request,
            index: reservation.index,
            chunks: Vec::new(),
        }
    }

    fn stop(&self, cause: Stop) -> Halt {
        self.footprint.stop(cause).into()
    }

    async fn run<B>(
        mut self,
        store: &impl MetadataStore,
        chunk_store: &impl PlacementChunkStore,
        runtime: &mut impl StageRuntime,
        mut body: B,
    ) -> Result<StagedAttempt, Halt>
    where
        B: Stream<Item = wyrd_traits::Result<Bytes>> + Unpin,
    {
        let chunk_size = self.request.chunk_size;
        let idle_millis = self.request.body_idle_millis;
        let mut buf: Vec<u8> = Vec::with_capacity(chunk_size);
        let mut digest = DigestHasher::new();
        let mut len = 0u64;
        let mut exceeded = false;
        'body: loop {
            // One read, bounded from the instant it began, on the clock that owns the leases.
            // Empty pieces are skipped inside it, so they never move this deadline, and an answer
            // seen at or past it is late, piece or end alike.
            let idle_at = runtime.now_millis().saturating_add(idle_millis);
            let read = bounded(&*runtime, next_piece(&mut body), idle_at, Late::Refused).await;
            let Some(next) = read else {
                return Err(self.stop(Stop::BodyIdle { idle_millis }));
            };
            let Some(piece) = next else {
                break;
            };
            let piece = piece.map_err(|fault| self.stop(Stop::Body(fault)))?;
            let mut rest: &[u8] = &piece;
            while !rest.is_empty() {
                if buf.is_empty() && self.chunks.len() == self.request.max_part_chunks as usize {
                    exceeded = true;
                    break 'body;
                }
                let (head, tail) = rest.split_at((chunk_size - buf.len()).min(rest.len()));
                buf.extend_from_slice(head);
                digest.update(head);
                len += head.len() as u64;
                rest = tail;
                if buf.len() == chunk_size {
                    self.stage_chunk(store, chunk_store, runtime, &buf).await?;
                    buf.clear();
                }
            }
        }
        if !buf.is_empty() {
            self.stage_chunk(store, chunk_store, runtime, &buf).await?;
        }
        Ok(StagedAttempt {
            footprint: self.footprint,
            part_number: self.request.part_number,
            attempt_id: self.request.attempt_id.clone(),
            index: self.index,
            chunks: self.chunks,
            len,
            digest: digest.finish(),
            chunk_bound_exceeded: exceeded,
        })
    }

    /// Register one chunk, then write its fragments.
    async fn stage_chunk(
        &mut self,
        store: &impl MetadataStore,
        chunk_store: &impl PlacementChunkStore,
        runtime: &mut impl StageRuntime,
        piece: &[u8],
    ) -> Result<(), Halt> {
        let scheme = self.request.scheme;
        let id = runtime.next_chunk_id();
        let fragments =
            encode_chunk(scheme, id, piece).map_err(|err| self.stop(Stop::Encode(err)))?;
        let landed = self
            .intent(store, &*runtime, id, fragments.len() as u16)
            .await?;
        // Measured from the reading taken before the intent was committed, however late the
        // writes are dispatched: the deadline can only be early, never late (`0016:1553-1578`).
        let deadline = landed
            .t_auth
            .saturating_add(self.request.write_window_millis);
        self.write_fragments(
            chunk_store,
            &*runtime,
            id,
            fragments,
            &landed.placement,
            deadline,
        )
        .await?;
        self.chunks.push(ChunkRef {
            id,
            scheme,
            len: piece.len() as u64,
            placement: landed.placement,
        });
        Ok(())
    }

    /// Write chunk `id`'s fragments to `placement`, each carrying `deadline`, and wait for them
    /// no longer than that instant (the module doc's "The write deadline").
    async fn write_fragments(
        &self,
        chunk_store: &impl PlacementChunkStore,
        runtime: &impl StageRuntime,
        id: ChunkId,
        fragments: Vec<(u16, Bytes)>,
        placement: &[DServerId],
        deadline: u64,
    ) -> Result<(), Halt> {
        if runtime.now_millis() >= deadline {
            return Err(self.stop(Stop::WriteDeadlineElapsed {
                chunk: id,
                deadline_millis: deadline,
                refused: 0,
            }));
        }
        let mut unanswered: BTreeSet<u16> = BTreeSet::new();
        let mut pending: FuturesUnordered<_> = fragments
            .into_iter()
            .zip(placement)
            .map(|((index, fragment), dserver)| {
                unanswered.insert(index);
                let fragment_id = FragmentId { chunk: id, index };
                let put =
                    chunk_store.put_fragment_at(*dserver, fragment_id, fragment, Some(deadline));
                async move { (index, put.await) }
            })
            .collect();
        // Kept outside the raced future, so the answers that came before the deadline survive it.
        let mut answers: Vec<(u16, wyrd_traits::Result<()>)> = Vec::new();
        let every = async {
            while let Some(answer) = pending.next().await {
                answers.push(answer);
            }
        };
        // An `Ok` is taken whenever it is seen: the store checked it against the deadline itself.
        bounded(runtime, every, deadline, Late::Taken).await;
        // Still pending at the deadline: abandoned here, never re-sent.
        drop(pending);
        let stopped_at = runtime.now_millis();
        let mut refused = 0;
        let mut reports = Vec::new();
        for (index, answer) in answers {
            unanswered.remove(&index);
            let Err(fault) = answer else { continue };
            let effect = write_deadline_outcome(fault.as_ref()).map(|expired| expired.effect);
            if effect == Some(WriteEffect::NotApplied) {
                refused += 1;
            } else {
                reports.push(fault);
            }
        }
        let abandoned: Vec<WriteDeadlineExpired> = unanswered
            .into_iter()
            .map(|index| abandoned_write(FragmentId { chunk: id, index }, deadline, stopped_at))
            .collect();
        if !abandoned.is_empty() || !reports.is_empty() {
            return Err(self.stop(Stop::WriteEffectUnknown {
                chunk: id,
                deadline_millis: deadline,
                abandoned,
                reports,
            }));
        }
        if refused > 0 {
            return Err(self.stop(Stop::WriteDeadlineElapsed {
                chunk: id,
                deadline_millis: deadline,
                refused,
            }));
        }
        Ok(())
    }

    /// Register chunk `id`'s owned entry (`0016:659`), returning the batch that landed. No
    /// fragment of the chunk may be written before this returns.
    async fn intent(
        &mut self,
        store: &impl MetadataStore,
        runtime: &impl StageRuntime,
        id: ChunkId,
        fragments: u16,
    ) -> Result<Intent, Halt> {
        let request = self.request;
        let upload_id = request.upload_id;
        let key = sidx_key(upload_id, request.part_number, id);
        let mut doubt = Doubt::default();
        let mut sent: Vec<Intent> = Vec::new();
        let mut rounds = 0u32;
        loop {
            if rounds == INTENT_ROUNDS {
                let spent = Stop::IntentRoundsSpent { chunk: id, rounds };
                return Err(doubt.end(upload_id, spent).into());
            }
            rounds += 1;
            // Plan against the topology minus every server whose drain is recorded, read afresh
            // each round: a round lost to a drain recorded since the last plan re-plans here.
            let drains = draining(store)
                .await
                .map_err(|fault| doubt.end(upload_id, Stop::Store(fault)))?;
            let placement = plan(request.topology, &drains, fragments)
                .map_err(|err| doubt.end(upload_id, Stop::Placement(err)))?;
            let staged = StagedPlacement::new(request.scheme, placement.clone())
                .map_err(|err| doubt.end(upload_id, Stop::Record(err)))?;
            let t_auth = runtime.now_millis();
            let footprint = &self.footprint;
            // This batch moves the slot's lease forward, so it must not be the one that revives
            // a lapsed lease: every lease of the attempt is checked at this round's own reading,
            // taken after the drain read, since that read or an earlier round can carry the
            // clock past one. A batch already sent keeps its doubt.
            footprint
                .check_live(t_auth)
                .map_err(|cause| doubt.end(upload_id, cause))?;
            // One lease for the batch: the new owned entry's and the slot's, so neither is
            // earlier than the slot lease this batch replaces.
            let expiry = footprint.next_expiry(t_auth, request.lease_ttl_millis);
            let entry = OwnedEntry::new(upload_id.clone(), expiry, staged);
            let record = footprint
                .record
                .with_lease(expiry)
                .map_err(|err| doubt.end(upload_id, Stop::Record(err)))?;
            let intent = Intent {
                owned: OwnedChunk::new(key.clone(), entry),
                slot: metadata::encode(&record),
                record,
                placement,
                t_auth,
            };
            // The session fence: no owned entry once the session has left `Open@E`.
            let mut batch =
                WriteBatch::new().require(footprint.session_key.clone(), footprint.session.clone());
            // The drain fence: one `require_absent` per server the entry names.
            let named: BTreeSet<DServerId> = intent.placement.iter().copied().collect();
            for server in named {
                batch = batch.require_absent(metadata::desired_dserver_key(server));
            }
            let batch = batch
                .require(footprint.slot_key.clone(), footprint.slot.clone())
                .require_absent(key.clone())
                .put(key.clone(), intent.owned.bytes.clone())
                .put(footprint.slot_key.clone(), intent.slot.clone());
            match store.commit(batch).await {
                Ok(CommitOutcome::Committed) => return Ok(self.adopt(intent)),
                Ok(CommitOutcome::Conflict) => {}
                Err(fault) => match fault.downcast_ref::<CommitUnknownResult>() {
                    Some(unknown) => {
                        doubt.observe(unknown);
                        sent.push(intent);
                    }
                    None => return Err(doubt.end(upload_id, Stop::Store(fault)).into()),
                },
            }
            // Settle from the owned entry's exact bytes: only a batch of this attempt puts them.
            let found = store
                .get(&key)
                .await
                .map_err(|fault| doubt.end(upload_id, Stop::Store(fault)))?;
            if let Some(found) = found {
                if let Some(at) = sent.iter().position(|intent| intent.owned.bytes == found) {
                    return Ok(self.adopt(sent.swap_remove(at)));
                }
                let taken = Stop::ChunkIdTaken { key: lossy(&key) };
                return Err(doubt.end(upload_id, taken).into());
            }
            doubt.settle();
            let slot = store
                .get(&self.footprint.slot_key)
                .await
                .map_err(|fault| doubt.end(upload_id, Stop::Store(fault)))?;
            if sent
                .iter()
                .any(|intent| slot.as_ref() == Some(&intent.slot))
            {
                // Only a batch of this attempt puts that slot value: one landed after the entry
                // read above. Settle it from the entry, read again; two rounds can put the same
                // slot value with different placements, and only the entry tells them apart.
                let again = store
                    .get(&key)
                    .await
                    .map_err(|fault| doubt.end(upload_id, Stop::Store(fault)))?;
                if let Some(at) = sent
                    .iter()
                    .position(|intent| again.as_ref() == Some(&intent.owned.bytes))
                {
                    return Ok(self.adopt(sent.swap_remove(at)));
                }
                // Nothing is in doubt: every other batch of this loop required the slot value
                // the landed one replaced, so none can land now.
                let lost = Stop::EntryLost { key: lossy(&key) };
                return Err(self.footprint.stop(lost).into());
            }
            let Some(moved) = self.footprint.reconcile(store, slot, &mut doubt).await? else {
                continue;
            };
            if !sent.is_empty() {
                // A round whose outcome was unknown may have landed after the entry read and
                // before the session moved: read the entry again before letting the doubt go.
                let again = store
                    .get(&key)
                    .await
                    .map_err(|fault| doubt.end(upload_id, Stop::Store(fault)))?;
                if let Some(at) = sent
                    .iter()
                    .position(|intent| again.as_ref() == Some(&intent.owned.bytes))
                {
                    // Under a session no longer `Open` this is the refusal, and no fragment of
                    // the chunk is written.
                    self.footprint.moved_on(moved, &mut doubt)?;
                    return Ok(self.adopt(sent.swap_remove(at)));
                }
            }
            self.footprint.moved_on(moved, &mut doubt)?;
        }
    }

    /// An intent batch landed: its entry and its slot are now what the store holds.
    fn adopt(&mut self, intent: Intent) -> Intent {
        self.footprint.slot = intent.slot.clone();
        self.footprint.record = intent.record.clone();
        self.footprint.owned.push(intent.owned.clone());
        intent
    }
}
