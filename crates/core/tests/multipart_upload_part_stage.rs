//! Issue #932 (the first of #926's two slices of #657) — `UploadPart`'s **staging half**,
//! `wyrd_core::multipart::stage::{stage_part, reserve_slot}`, driven over a real store
//! (`RedbMetadataStore::in_memory()`), a placement-aware chunk-store fake (the shape of `Fleet`
//! in `domain_placement.rs`) that enforces the write deadline as the trait requires, a store
//! wrapper that logs every batch and can act on any of them, and a runtime whose clock and timer
//! the test moves by hand (proposal 0016 decision 5, `0016:1676-1847`; the *Part slot reserve*
//! and *Part intent* rows, `0016:658-659`). Every session here is made by Create (#894).
//!
//! * **(A)** staging, and the order that makes it safe; the hand-off's chunk references.
//! * **(A′)** the write deadline: exact, enforced at the fake's publication point, waited for no
//!   longer than itself; an unverified or abandoned write never counted either way, and classed
//!   `Indeterminate` however the timing fell or whichever write answered first; an `Ok` counted
//!   even when it is first seen as the deadline's timer fires.
//! * **(A″)** the body-idle bound: per read, on the test's clock and timer; empty pieces are not
//!   progress, and a piece or end seen past the bound is late.
//! * **(B)** the drain fence: a drain recorded just before an intent commits re-plans (B1); a
//!   drain recorded before staging is planned around (B2); four of twelve domains draining (B3).
//! * **(C)** slots: the cap, `stage_part`'s answer to a full range, a session leaving `Open`
//!   under the reserve, an adopted ambiguous reserve, the same-index rule, a held reserve that
//!   lands after its attempt refused, one that lands just before the session moves, and a store
//!   read or commit that fails while a reserve is in doubt.
//! * **(D)** leases: written forward, checked by every intent round at its own reading, never
//!   revived.
//! * **(E)** unknown intent outcomes, settled by a re-read, including a held intent that lands
//!   between two of the reads, and a store read or commit that fails while an intent is in doubt;
//!   **(E′)** the intent's own session fence against a real Abort.
//! * **(F)** every loop ends after exactly its stated number of commits.
//! * **(G)** the per-part chunk bound. **(H)** a request that cannot be honoured, refused alike
//!   by `stage_part` and `reserve_slot`.
//!
//! 657.1b (#933) appends its renewal legs, over the same harness:
//!
//! * **(R1)** every renewal batch rewrites the slot with every owned lease, each required at its
//!   prior bytes under the session fence, in exactly the operations the clamp counts.
//! * **(R2)** renewal runs for the whole request: before the first chunk, while a body read is
//!   withheld (D-i), while a fragment write is held (D-ii), at exactly half a TTL, before an intent
//!   and never during one; and a renewal that crosses expiry defeats a stand-in reaper fence.
//! * **(R3)** renewal never holds staging past an armed deadline, and a write answered in time is
//!   taken however long the renewal beside it takes; the body-idle bound still holds.
//! * **(R4)** a lapsed lease is refused, never revived, by every round at its own reading; and a
//!   renewal that stops staging beside a chunk's writes leaves their effect unknown, a refusal
//!   included, unless every write had answered.
//! * **(R5)** unknown renewal outcomes are settled by a re-read. **(R6)** the renewal loop is
//!   bounded. **(R7)** the commit's quiesce renewal, `StagedAttempt::renew`, and its cover.
//!   **(R8)** a chunk bound whose renewal would pass `MAX_BATCH_OPS` is refused.
//!
//! The wrappers keep their state in struct fields, never in a `static` (ADR-0035).

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::io::Write as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, Stream};
use sha2::{Digest as _, Sha256};
use tokio::sync::Semaphore;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, SegmentNonce};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::stage::{
    reserve_slot, stage_part, InDoubt, RenewOutcome, StageClock, StageError, StageOutcome,
    StageRequest, StageRuntime, StagedAttempt, Stop, ABANDONED_COMMIT, INTENT_ROUNDS,
    RENEWAL_ROUNDS, RESERVE_AMBIGUOUS_ROUNDS,
};
use wyrd_core::multipart::{
    decode_owned_entry, decode_session_record, decode_slot_record, mpu_key, parse_sidx_key,
    parse_slot_key, sidx_key, sidx_range, slot_key, slot_range, AbortOutcome, AttemptId,
    Backpressure, Budget, CreateOutcome, Digest, OwnedEntry, PartNumber, Refusal, ReserveOutcome,
    SessionState, SlotIndex, SlotRecord, UploadId, MAX_BATCH_OPS, MAX_INFLIGHT_PARTS,
    MAX_PART_CHUNKS, MAX_SLOT_INDEX, PART_PREFIX, PSUM_PREFIX, SIDX_PREFIX, SLOT_PREFIX,
};
use wyrd_core::placement::{select_distinct_domains, SelectorError, Topology};
use wyrd_core::read::read_chunk_verified;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::{Clock, ManualClock};
use wyrd_traits::{
    classify, write_deadline_outcome, BoxError, ChunkId, ChunkStore, CommitOutcome,
    CommitUnknownResult, DServerId, ErrorClass, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result as StoreResult, ScanPage, TransientFault, WriteBatch,
    WriteDeadlineExpired, WriteEffect,
};

// ─────────────────────────────── fixtures ───────────────────────────────

const BUCKET: &str = "photos";
/// The root inode every Alpha object dirent lives under, as `<bucket>/<key>` (ADR-0046).
const ROOT: InodeId = 0;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
/// No leg may hang the suite: a leg runs in milliseconds, so one still running after this has
/// hung (the repo's 30 s watchdog, `chunkstore-grpc/tests/write_deadline.rs`).
const LEG_TIMEOUT: Duration = Duration::from_secs(30);
/// How long past [`LEG_TIMEOUT`] the watchdog thread waits for a leg that never yields, which no
/// async timeout can interrupt, before it ends the test process.
const WATCHDOG_GRACE: Duration = Duration::from_secs(5);
/// How long a driver waits for the staging future to reach a point it is waiting for.
const WAIT_FOR: Duration = Duration::from_secs(10);
/// More commits than any staging may make. A loop past it never stops committing.
const RUNAWAY_COMMITS: usize = 400;
/// The test clock's first reading.
const T0: u64 = 1_000_000;
/// The lease TTL every request here asks for.
const TTL: u64 = 10_000;
/// `W_write`.
const W: u64 = 3_000;
/// The body-idle bound every request here asks for: far longer than any leg moves the clock
/// while a read is pending, except the legs that test it.
const IDLE: u64 = 100 * TTL;
/// The body-idle bound the (A″) legs ask for: inside the lease, so a leg that runs the clock to
/// the bound stops on the bound, never on a lapsed lease.
const BOUND: u64 = 5_000;
const _: () = assert!(BOUND < TTL);
/// Bytes per chunk.
const CHUNK: usize = 64;
/// Six fragments per chunk.
const RS42: EcScheme = EcScheme::ReedSolomon { k: 4, m: 2 };
const FRAGMENTS: u16 = 6;
/// Twelve D servers, each its own failure domain.
const SERVERS: u64 = 12;
const PART: u32 = 7;

/// Run `leg` under [`LEG_TIMEOUT`]. A leg stuck in a loop that never yields cannot be timed out
/// from its own task, so a watchdog thread ends the whole test process (exit 101) if the leg has
/// neither finished nor panicked [`WATCHDOG_GRACE`] after the timeout.
async fn within<T>(leg: impl Future<Output = T>) -> T {
    let (disarm, armed) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        // A finished or panicking leg drops `disarm`, which ends the wait early.
        if armed.recv_timeout(LEG_TIMEOUT + WATCHDOG_GRACE) == Err(RecvTimeoutError::Timeout) {
            // Straight to stderr: the test harness captures `eprintln!` from this thread, and
            // the process ends before that capture is printed.
            let _ = writeln!(
                std::io::stderr(),
                "a leg ran past {LEG_TIMEOUT:?} without yielding; ending the test process"
            );
            std::process::exit(101);
        }
    });
    let output = tokio::time::timeout(LEG_TIMEOUT, leg)
        .await
        .expect("the leg settled in time");
    drop(disarm);
    output
}

/// Wait until `ready` holds, letting the staging future run in between; panics after
/// [`WAIT_FOR`]. A schedule whose point never comes fails here, not at the leg timeout.
async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    while !ready() {
        assert!(start.elapsed() < WAIT_FOR, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// A profile admitting 8 sessions. Create stores it; staging never reads it.
fn profile() -> Budget {
    let json = r#"{"w_ref":16,"max_part_chunks":1,"max_parts_per_session":1,"max_inflight_parts":1,"max_staged_chunks":1}"#;
    metadata::decode(json.as_bytes()).expect("a legal profile")
}

fn identity(n: u128) -> (UploadId, SegmentNonce) {
    (
        UploadId::new(format!("{n:032x}")).unwrap(),
        SegmentNonce::new(format!("{:032x}", n ^ 0x5a5a_5a5a)).unwrap(),
    )
}

fn attempt(n: u128) -> AttemptId {
    AttemptId::new(format!("{:032x}", 0xa77e_0000_u128 + n)).unwrap()
}

fn part() -> PartNumber {
    PartNumber::new(PART).unwrap()
}

/// The runtime Create needs: one fixed identity, no jitter, no wait.
struct Fixed(Option<(UploadId, SegmentNonce)>);

impl CreateRuntime for Fixed {
    fn mint(&mut self) -> (UploadId, SegmentNonce) {
        self.0.take().expect("one identity per create")
    }

    fn jitter(&mut self) -> u64 {
        0
    }

    fn wait(&mut self, _millis: u64) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

/// Twelve servers `0..12` in domains `d00..d11`, equally loaded: the selector takes domains in
/// label order, so an RS(4,2) chunk lands on servers `0..6`.
fn topology() -> Topology {
    let mut topology = Topology::default();
    for id in 0..SERVERS {
        topology.register(id, format!("d{id:02}"));
    }
    topology
}

/// [`topology`], with every server loaded but `preferred`: the selector's first pick.
fn topology_preferring(preferred: DServerId) -> Topology {
    let mut topology = topology();
    for id in 0..SERVERS {
        topology.set_utilization(id, if id == preferred { 0 } else { 10 });
    }
    topology
}

/// The request every leg starts from.
fn request<'a>(
    topology: &'a Topology,
    upload: &'a UploadId,
    attempt: &'a AttemptId,
) -> StageRequest<'a> {
    StageRequest {
        upload_id: upload,
        part_number: part(),
        attempt_id: attempt,
        max_inflight_parts: MAX_INFLIGHT_PARTS,
        lease_ttl_millis: TTL,
        chunk_size: CHUNK,
        scheme: RS42,
        topology,
        write_window_millis: W,
        // The shipped bound: a wider one would make a renewal batch past `MAX_BATCH_OPS`, which
        // staging refuses (R8).
        max_part_chunks: MAX_PART_CHUNKS,
        body_idle_millis: IDLE,
    }
}

/// `len` deterministic body bytes.
fn body_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 + 7) as u8).collect()
}

/// `body` cut into pieces of `sizes`, which must cover it exactly.
fn pieces(body: &[u8], sizes: &[usize]) -> Vec<Bytes> {
    assert_eq!(
        sizes.iter().sum::<usize>(),
        body.len(),
        "the pieces cover the body"
    );
    let mut at = 0;
    sizes
        .iter()
        .map(|size| {
            let piece = Bytes::copy_from_slice(&body[at..at + size]);
            at += size;
            piece
        })
        .collect()
}

fn sha256(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}

type Body = Pin<Box<dyn Stream<Item = StoreResult<Bytes>> + Send>>;

fn stream_of(pieces: Vec<Bytes>) -> Body {
    Box::pin(stream::iter(pieces.into_iter().map(Ok)))
}

/// A body the test feeds piece by piece, each logged as read ([`Event::Read`]) the moment the
/// body yields it. It ends when the sender is dropped.
fn channel(log: &Log) -> (tokio::sync::mpsc::UnboundedSender<Bytes>, Body) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let log = log.clone();
    let body = stream::unfold((rx, 0usize), move |(mut rx, index)| {
        let log = log.clone();
        async move {
            let piece = rx.recv().await?;
            log.push(Event::Read { index });
            Some((Ok(piece), (rx, index + 1)))
        }
    });
    (tx, Box::pin(body))
}

/// Run `staging`, then set `ended`: a driver can tell that staging returned while something it
/// sent is still held.
async fn flagging<T>(ended: Arc<AtomicBool>, staging: impl Future<Output = T>) -> T {
    let output = staging.await;
    ended.store(true, Ordering::SeqCst);
    output
}

fn unknown(may_still_commit: bool) -> BoxError {
    Box::new(CommitUnknownResult {
        backend: "scripted",
        code: None,
        detail: "scripted unknown outcome".into(),
        may_still_commit,
    })
}

/// The fault a scripted read answers with: a transient one, as a network blip would be, so a
/// leg can show the class of an in-doubt error is still the commit's, not the read's.
fn read_fault(key: &[u8]) -> BoxError {
    Box::new(TransientFault::new(format!(
        "scripted read fault on {}",
        String::from_utf8_lossy(key)
    )))
}

// ─────────────────────────────── the log ───────────────────────────────

/// Which batch of staging a commit is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    /// `require_absent(slot:..)`.
    Reserve,
    /// `require_absent(sidx:..)`.
    Intent,
    /// No `require_absent`, and a put of the slot or an owned entry: a renewal, which moves the
    /// slot's lease with every owned one. Told apart by what it puts, not by whether it puts the
    /// slot, so a renewal that left the slot out is still one.
    Renewal,
    Other,
}

fn kind_of(batch: &WriteBatch) -> Kind {
    let absent = |prefix: &[u8]| {
        batch
            .preconditions
            .iter()
            .any(|p| p.expected.is_none() && p.key.starts_with(prefix))
    };
    if absent(SIDX_PREFIX) {
        Kind::Intent
    } else if absent(SLOT_PREFIX) {
        Kind::Reserve
    } else if batch
        .puts
        .iter()
        .any(|(key, _)| key.starts_with(SLOT_PREFIX) || key.starts_with(SIDX_PREFIX))
    {
        Kind::Renewal
    } else {
        Kind::Other
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    Committed,
    Conflict,
    Unknown { may_still_commit: bool },
    Failed,
}

fn answer_of(result: &StoreResult<CommitOutcome>) -> Answer {
    match result {
        Ok(CommitOutcome::Committed) => Answer::Committed,
        Ok(CommitOutcome::Conflict) => Answer::Conflict,
        Err(fault) => match fault.downcast_ref::<CommitUnknownResult>() {
            Some(unknown) => Answer::Unknown {
                may_still_commit: unknown.may_still_commit,
            },
            None => Answer::Failed,
        },
    }
}

/// Where the chunk-store fake holds a put until the test releases it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldAt {
    /// On arrival, before the publication-point check: the put has not begun publishing.
    Arrival,
    /// Inside the publishing step: past the check, before the bytes are stored.
    Publishing,
    /// After the store answered, before the answer reaches the caller: a reply in transit.
    Answer,
}

#[derive(Debug, Clone)]
enum Event {
    /// A batch through the wrapper: what it was, the clock when it arrived, and the answer.
    Commit {
        kind: Kind,
        batch: WriteBatch,
        clock: u64,
        answer: Answer,
    },
    /// A channel body yielded its `index`-th piece (0-based).
    Read { index: usize },
    /// A fragment put the chunk store was asked for, logged on arrival, before any hold.
    Put {
        dserver: DServerId,
        id: FragmentId,
        deadline: Option<u64>,
        clock: u64,
    },
    /// A put reached the hold point the test set for it, and waits there.
    Held { id: FragmentId, at: HoldAt },
    /// The fake refused a put at its publication point: the deadline had passed.
    Refused { id: FragmentId },
    /// The fake stored a fragment: it is readable from now on.
    Stored { id: FragmentId },
    /// The fake stored a fragment but its clock had reached the deadline once the publishing
    /// step returned: it answered `WriteEffect::Unknown`.
    Unverified { id: FragmentId },
    /// The fake answered a put `Ok`: published strictly before its deadline.
    Acked { id: FragmentId },
    /// The fake answered a put with the fault scripted for it ([`Fleet::fail`]), storing nothing.
    Faulted { id: FragmentId },
    /// The wrapper answered a read of `key` (a `get`'s key, or a `scan`'s prefix) with a fault.
    ReadFault { key: Vec<u8> },
    /// A commit of `kind` reached its [`Act::Gated`] gate, and waits there.
    Gated { kind: Kind },
    /// A read reached its [`Then::Gate`], and waits there.
    ReadGated,
    /// The answer to the put of `id` reached its caller, past any transit hold: pushed while
    /// staging polls that put, so once a driver sees it, staging has taken the answer.
    Delivered { id: FragmentId },
}

/// One ordered log shared by the store wrapper, the chunk-store fake and the bodies.
#[derive(Debug, Clone, Default)]
struct Log(Arc<Mutex<Vec<Event>>>);

struct Commit {
    batch: WriteBatch,
    clock: u64,
    answer: Answer,
}

struct Put {
    dserver: DServerId,
    id: FragmentId,
    deadline: Option<u64>,
    clock: u64,
}

impl Log {
    fn push(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }

    fn events(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }

    fn has(&self, wanted: impl Fn(&Event) -> bool) -> bool {
        self.events().iter().any(wanted)
    }

    fn commits(&self, of: Kind) -> Vec<Commit> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                Event::Commit {
                    kind,
                    batch,
                    clock,
                    answer,
                } if kind == of => Some(Commit {
                    batch,
                    clock,
                    answer,
                }),
                _ => None,
            })
            .collect()
    }

    fn puts(&self) -> Vec<Put> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                Event::Put {
                    dserver,
                    id,
                    deadline,
                    clock,
                } => Some(Put {
                    dserver,
                    id,
                    deadline,
                    clock,
                }),
                _ => None,
            })
            .collect()
    }

    fn put_count(&self, id: FragmentId) -> usize {
        self.puts().iter().filter(|put| put.id == id).count()
    }

    /// Whether some body has yielded its `index`-th piece.
    fn read(&self, index: usize) -> bool {
        self.has(|e| matches!(e, Event::Read { index: i } if *i == index))
    }

    /// The intent commits for `chunk`.
    fn intents_of(&self, chunk: ChunkId) -> Vec<Commit> {
        self.commits(Kind::Intent)
            .into_iter()
            .filter(|commit| intent_chunk(&commit.batch) == chunk)
            .collect()
    }

    /// The commits of `of` the store answered `Committed`.
    fn landed(&self, of: Kind) -> usize {
        self.commits(of)
            .iter()
            .filter(|commit| commit.answer == Answer::Committed)
            .count()
    }

    /// The commits of `of` that have reached an [`Act::Gated`] gate.
    fn gated(&self, of: Kind) -> usize {
        self.events()
            .iter()
            .filter(|event| matches!(event, Event::Gated { kind } if *kind == of))
            .count()
    }

    /// The reads that have reached a [`Then::Gate`].
    fn reads_gated(&self) -> usize {
        self.events()
            .iter()
            .filter(|event| matches!(event, Event::ReadGated))
            .count()
    }

    /// Whether the fake has answered the put of `id` `Ok`.
    fn acked(&self, id: FragmentId) -> bool {
        self.has(|e| matches!(e, Event::Acked { id: answered } if *answered == id))
    }

    /// Whether the answer to the put of `id` has reached staging ([`Event::Delivered`]).
    fn delivered(&self, id: FragmentId) -> bool {
        self.has(|e| matches!(e, Event::Delivered { id: answered } if *answered == id))
    }
}

/// The owned entry an intent batch puts: its key and value.
fn intent_put(batch: &WriteBatch) -> (Vec<u8>, Bytes) {
    batch
        .puts
        .iter()
        .find(|(key, _)| key.starts_with(SIDX_PREFIX))
        .cloned()
        .expect("an intent puts its owned entry")
}

fn intent_chunk(batch: &WriteBatch) -> ChunkId {
    parse_sidx_key(&intent_put(batch).0).unwrap().2
}

fn intent_entry(batch: &WriteBatch) -> OwnedEntry {
    let (key, value) = intent_put(batch);
    decode_owned_entry(&key, &value).unwrap().2
}

/// The slot value a batch requires.
fn slot_required(batch: &WriteBatch) -> Bytes {
    batch
        .preconditions
        .iter()
        .find(|p| p.key.starts_with(SLOT_PREFIX))
        .and_then(|p| p.expected.clone())
        .expect("the batch requires its slot's bytes")
}

/// Every value a batch puts under `slot:` or `sidx:` is one its decoder accepts back unchanged.
fn assert_values_decode(batch: &WriteBatch) {
    for (key, value) in &batch.puts {
        if key.starts_with(SLOT_PREFIX) {
            let record =
                decode_slot_record(value).expect("every slot value staging writes decodes");
            assert_eq!(
                metadata::encode(&record),
                *value,
                "a slot value re-encodes byte for byte"
            );
        } else if key.starts_with(SIDX_PREFIX) {
            decode_owned_entry(key, value).expect("every owned entry decodes against its key");
        }
    }
}

// ──────────────────────── the scripted store wrapper ────────────────────────

/// A batch to commit on the inner store, built from the batch under test.
type Hook = Arc<dyn Fn(&WriteBatch) -> WriteBatch + Send + Sync>;

/// Something the wrapper does on the inner store beside a commit under test.
#[derive(Clone)]
enum Side {
    /// Commit `hook(batch)`, which must land.
    Batch(Hook),
    /// Run a real Abort (#895) of the session, which must fence it.
    Abort(UploadId),
}

/// What the wrapper does with one commit of a [`Kind`].
#[derive(Clone)]
enum Act {
    /// Delegate unchanged.
    Pass,
    /// Answer `Conflict` without applying anything, as TiKV does when a locking read meets
    /// another writer's lock.
    Conflict,
    /// As `Conflict`, and advance the clock by the given milliseconds before answering: a round
    /// lost slowly.
    ConflictThenAdvance(u64),
    /// Apply the batch or not, then answer `CommitUnknownResult { may_still_commit }`.
    Unknown {
        applied: bool,
        may_still_commit: bool,
    },
    /// Keep the batch unapplied, advance the clock by `advance`, and answer
    /// `CommitUnknownResult { may_still_commit: true }`. It lands when the test releases it.
    Hold { advance: u64 },
    /// As `Hold { advance: 0 }`, after doing `side` on the inner store.
    HoldThen(Side),
    /// Do `side` on the inner store first, then delegate.
    First(Side),
    /// Delegate, then advance the clock: a dispatch delayed after the commit returned.
    PassThenAdvance(u64),
    /// Apply the oldest held batch, then delegate.
    ReleaseHeldFirst,
    /// Answer a plain store fault without applying anything: a definite failure of this batch,
    /// not an unknown outcome.
    Fail,
    /// A slow commit: log [`Event::Gated`] on arrival, wait for a permit on the gate, then do the
    /// boxed act (never itself `Gated`). A commit its caller stops waiting for at the gate is kept
    /// unapplied in the held queue, as a store may still apply a commit its client gave up on: the
    /// test lands it with [`Scripted::release_held`].
    Gated(Arc<Semaphore>, Box<Act>),
}

/// What the wrapper does right after a read returns, before handing its answer back: the
/// interleaving "between this read and the next one".
#[derive(Clone)]
enum Then {
    /// Advance the clock.
    Advance(u64),
    /// Apply the oldest held batch, which must land.
    ReleaseHeld,
    /// Commit this batch on the inner store, which must land.
    Commit(WriteBatch),
    /// Run a real Abort of the session, which must fence it.
    Abort(UploadId),
    /// Answer this read with a store fault instead of what it found ([`read_fault`]), after
    /// any action listed before it.
    Fault,
    /// A slow read: log [`Event::ReadGated`], then wait for a permit on the gate before the
    /// answer is handed back.
    Gate(Arc<Semaphore>),
}

/// A gated commit's batch while it waits at its gate: dropped there, by a caller that stopped
/// waiting, it goes to the held queue unapplied.
struct HeldIfDropped<'a> {
    state: &'a Mutex<ScriptState>,
    batch: Option<WriteBatch>,
}

impl Drop for HeldIfDropped<'_> {
    fn drop(&mut self) {
        if let Some(batch) = self.batch.take() {
            self.state.lock().unwrap().held.push_back(batch);
        }
    }
}

/// Fire `then` after the read of `key` (a `get`'s key, or a `scan`'s prefix) that follows `skip`
/// earlier reads of it. Hooks on one key fire in the order they were added, each counting its
/// `skip` from the read after the previous one fired.
struct ReadHook {
    key: Vec<u8>,
    skip: usize,
    then: Vec<Then>,
}

#[derive(Default)]
struct ScriptState {
    plans: HashMap<Kind, (VecDeque<Act>, Option<Act>)>,
    held: VecDeque<WriteBatch>,
    made: usize,
    reads: Vec<ReadHook>,
}

struct Scripted {
    inner: RedbMetadataStore,
    clock: ManualClock,
    log: Log,
    state: Mutex<ScriptState>,
}

async fn fence(inner: &RedbMetadataStore, upload: &UploadId) {
    let fenced = abort_session(inner, upload).await.expect("the abort ran");
    assert_eq!(fenced, AbortOutcome::Fenced, "the abort fenced the session");
}

impl Scripted {
    /// `kind`'s commits follow `plan`, then `then` (`Pass` when unset).
    fn script(&self, kind: Kind, plan: Vec<Act>, then: Option<Act>) {
        self.state
            .lock()
            .unwrap()
            .plans
            .insert(kind, (plan.into(), then));
    }

    /// Apply the oldest held batch now; what it met.
    async fn release_held(&self) -> CommitOutcome {
        let held = self
            .state
            .lock()
            .unwrap()
            .held
            .pop_front()
            .expect("a batch is held");
        self.inner.commit(held).await.unwrap()
    }

    fn hold(&self, batch: WriteBatch) {
        self.state.lock().unwrap().held.push_back(batch);
    }

    /// How many batches are held unapplied.
    fn held_count(&self) -> usize {
        self.state.lock().unwrap().held.len()
    }

    async fn side(&self, side: &Side, batch: &WriteBatch) {
        match side {
            Side::Batch(hook) => {
                assert_eq!(
                    self.inner.commit(hook(batch)).await.unwrap(),
                    CommitOutcome::Committed
                );
            }
            Side::Abort(upload) => fence(&self.inner, upload).await,
        }
    }

    /// After the read of `key` that follows `skip` earlier ones, do `then`.
    fn after_read(&self, key: &[u8], skip: usize, then: Vec<Then>) {
        self.state.lock().unwrap().reads.push(ReadHook {
            key: key.to_vec(),
            skip,
            then,
        });
    }

    /// A read of `key` has returned: fire the hook it is due for, if any. Whether the read is
    /// to answer a fault instead of what it found.
    async fn on_read(&self, key: &[u8]) -> bool {
        let due = {
            let mut state = self.state.lock().unwrap();
            match state.reads.iter().position(|hook| hook.key == key) {
                Some(at) if state.reads[at].skip > 0 => {
                    state.reads[at].skip -= 1;
                    Vec::new()
                }
                Some(at) => state.reads.remove(at).then,
                None => Vec::new(),
            }
        };
        let mut fault = false;
        for then in due {
            match then {
                Then::Fault => {
                    self.log.push(Event::ReadFault { key: key.to_vec() });
                    fault = true;
                }
                Then::Advance(millis) => self.clock.advance(millis),
                Then::ReleaseHeld => {
                    assert_eq!(self.release_held().await, CommitOutcome::Committed);
                }
                Then::Commit(batch) => {
                    assert_eq!(
                        self.inner.commit(batch).await.unwrap(),
                        CommitOutcome::Committed
                    );
                }
                Then::Abort(upload) => fence(&self.inner, &upload).await,
                Then::Gate(gate) => {
                    self.log.push(Event::ReadGated);
                    gate.acquire().await.expect("the gate stays open").forget();
                }
            }
        }
        fault
    }
}

#[async_trait]
impl MetadataStore for Scripted {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let found = self.inner.get(key).await;
        if self.on_read(key).await {
            return Err(read_fault(key));
        }
        found
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        let found = self.inner.scan(prefix).await;
        if self.on_read(prefix).await {
            return Err(read_fault(prefix));
        }
        found
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<ScanPage> {
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        let kind = kind_of(&batch);
        assert_values_decode(&batch);
        let clock = self.clock.now_millis();
        let act = {
            let mut state = self.state.lock().unwrap();
            state.made += 1;
            assert!(
                state.made <= RUNAWAY_COMMITS,
                "staging never stopped committing ({} commits)",
                state.made
            );
            match state.plans.get_mut(&kind) {
                Some((plan, then)) => plan.pop_front().or_else(|| then.clone()),
                None => None,
            }
            .unwrap_or(Act::Pass)
        };
        let act = match act {
            Act::Gated(gate, then) => {
                self.log.push(Event::Gated { kind });
                let mut waiting = HeldIfDropped {
                    state: &self.state,
                    batch: Some(batch.clone()),
                };
                gate.acquire().await.expect("the gate stays open").forget();
                // Past the gate: the act below decides what the batch comes to.
                waiting.batch = None;
                *then
            }
            act => act,
        };
        let result = match act {
            Act::Pass => self.inner.commit(batch.clone()).await,
            Act::Conflict => {
                // A real store yields here; so must this one, or no timeout could fire.
                tokio::task::yield_now().await;
                Ok(CommitOutcome::Conflict)
            }
            Act::ConflictThenAdvance(advance) => {
                tokio::task::yield_now().await;
                self.clock.advance(advance);
                Ok(CommitOutcome::Conflict)
            }
            Act::Unknown {
                applied,
                may_still_commit,
            } => {
                if applied {
                    let landed = self.inner.commit(batch.clone()).await?;
                    assert_eq!(landed, CommitOutcome::Committed, "the scripted batch lands");
                }
                Err(unknown(may_still_commit))
            }
            Act::Hold { advance } => {
                self.hold(batch.clone());
                self.clock.advance(advance);
                Err(unknown(true))
            }
            Act::HoldThen(side) => {
                self.hold(batch.clone());
                self.side(&side, &batch).await;
                Err(unknown(true))
            }
            Act::First(side) => {
                self.side(&side, &batch).await;
                self.inner.commit(batch.clone()).await
            }
            Act::PassThenAdvance(advance) => {
                let result = self.inner.commit(batch.clone()).await;
                self.clock.advance(advance);
                result
            }
            Act::ReleaseHeldFirst => {
                assert_eq!(self.release_held().await, CommitOutcome::Committed);
                self.inner.commit(batch.clone()).await
            }
            Act::Fail => {
                tokio::task::yield_now().await;
                Err("scripted commit fault: the batch was not applied".into())
            }
            Act::Gated(..) => panic!("a gated act's own act is never gated"),
        };
        self.log.push(Event::Commit {
            kind,
            batch,
            clock,
            answer: answer_of(&result),
        });
        result
    }
}

// ──────────────────────── the chunk-store fake ────────────────────────

/// In-process D servers addressed by stable id (`domain_placement.rs`'s `Fleet`), logging every
/// put with its deadline, enforcing that deadline as `ChunkStore::put_fragment` requires, and
/// able to hold a put at any of three points until the test releases it, or to fail one.
///
/// A put runs in its own task, as it would on a remote D server: a caller that stops waiting
/// drops only its wait, and the D server goes on with the put. Its deadline is judged twice, as
/// the trait defines (`crates/traits/src/lib.rs`, `ChunkStore::put_fragment`):
///
/// 1. at the **publication point**, the instant before the fragment is stored, with
///    [`WriteDeadlineExpired::if_elapsed`]: a late put is refused and nothing of it is stored;
/// 2. right after the publishing step returns, with
///    [`WriteDeadlineExpired::if_publication_unverified`]: a put whose step straddled the
///    deadline stays stored and is answered [`WriteEffect::Unknown`].
#[derive(Clone)]
struct Fleet(Arc<FleetState>);

struct FleetState {
    servers: Mutex<BTreeMap<DServerId, HashMap<FragmentId, Bytes>>>,
    clock: ManualClock,
    log: Log,
    holds: Mutex<Vec<(FragmentId, HoldAt, Arc<Semaphore>)>>,
    /// Puts answered with a fault once past their arrival hold, storing nothing.
    failing: Mutex<HashMap<FragmentId, Failure>>,
}

/// How a put the fake fails is answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// A [`TransientFault`]: the D server unreachable.
    Transient,
    /// A plain error no seam type names, which `classify` reads `Terminal`.
    Plain,
}

impl Fleet {
    fn new(clock: &ManualClock, log: &Log) -> Self {
        Self(Arc::new(FleetState {
            servers: Mutex::new((0..SERVERS).map(|id| (id, HashMap::new())).collect()),
            clock: clock.clone(),
            log: log.clone(),
            holds: Mutex::new(Vec::new()),
            failing: Mutex::new(HashMap::new()),
        }))
    }

    /// Hold the put of `id` at `at` until a permit is added to the returned gate.
    fn hold(&self, id: FragmentId, at: HoldAt) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        self.0
            .holds
            .lock()
            .unwrap()
            .push((id, at, Arc::clone(&gate)));
        gate
    }

    /// Answer the put of `id` with `failure` once it is past its arrival hold, if any.
    fn fail(&self, id: FragmentId, failure: Failure) {
        self.0.failing.lock().unwrap().insert(id, failure);
    }

    /// The server that stores fragment `id`, if any.
    fn stored_on(&self, id: FragmentId) -> Option<DServerId> {
        self.0
            .servers
            .lock()
            .unwrap()
            .iter()
            .find(|(_, held)| held.contains_key(&id))
            .map(|(server, _)| *server)
    }

    /// Whether every put of `held`'s chunk but `held` itself has been answered: `Ok`, or the
    /// transient fault scripted for it.
    fn answered_the_rest(&self, held: FragmentId) -> bool {
        let events = self.0.log.events();
        (0..FRAGMENTS)
            .filter(|index| *index != held.index)
            .all(|index| {
                let id = FragmentId {
                    chunk: held.chunk,
                    index,
                };
                events.iter().any(|e| {
                    matches!(e, Event::Acked { id: answered } | Event::Faulted { id: answered }
                        if *answered == id)
                })
            })
    }

    /// Whether the put of `id` is waiting at `at`, and every other put of its chunk is answered,
    /// so a clock the test moves now reaches that one put only.
    fn holds_only(&self, id: FragmentId, at: HoldAt) -> bool {
        self.held(id, at) && self.answered_the_rest(id)
    }

    fn held(&self, id: FragmentId, at: HoldAt) -> bool {
        self.0
            .log
            .has(|e| matches!(e, Event::Held { id: i, at: a } if *i == id && *a == at))
    }
}

impl FleetState {
    async fn gate(&self, id: FragmentId, at: HoldAt) {
        let gate = self
            .holds
            .lock()
            .unwrap()
            .iter()
            .find(|(held, point, _)| *held == id && *point == at)
            .map(|(_, _, gate)| Arc::clone(gate));
        if let Some(gate) = gate {
            self.log.push(Event::Held { id, at });
            gate.acquire().await.expect("the gate stays open").forget();
        }
    }

    async fn apply(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> StoreResult<()> {
        self.gate(id, HoldAt::Arrival).await;
        let failure = self.failing.lock().unwrap().get(&id).copied();
        if let Some(failure) = failure {
            self.log.push(Event::Faulted { id });
            return Err(match failure {
                Failure::Transient => Box::new(TransientFault::new(format!(
                    "scripted: the D server holding fragment {id:?} is unreachable"
                ))),
                Failure::Plain => format!("scripted: fragment {id:?}'s disk failed").into(),
            });
        }
        // The publication point.
        if let Some(deadline) = deadline_millis {
            let now = self.clock.now_millis();
            if let Some(refused) = WriteDeadlineExpired::if_elapsed(id, deadline, now) {
                self.log.push(Event::Refused { id });
                return Err(Box::new(refused));
            }
        }
        // The publishing step.
        self.gate(id, HoldAt::Publishing).await;
        self.servers
            .lock()
            .unwrap()
            .get_mut(&dserver)
            .ok_or_else(|| format!("Fleet: unknown D server {dserver}"))?
            .insert(id, fragment);
        self.log.push(Event::Stored { id });
        if let Some(deadline) = deadline_millis {
            let now = self.clock.now_millis();
            if let Some(unverified) =
                WriteDeadlineExpired::if_publication_unverified(id, deadline, now)
            {
                self.log.push(Event::Unverified { id });
                return Err(Box::new(unverified));
            }
        }
        self.log.push(Event::Acked { id });
        Ok(())
    }
}

#[async_trait]
impl ChunkStore for Fleet {
    async fn put_fragment(
        &self,
        _id: FragmentId,
        _fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> StoreResult<()> {
        Err("Fleet: a staged write must address a D server (use put_fragment_at)".into())
    }

    async fn get_fragment(&self, _id: FragmentId) -> StoreResult<Option<Bytes>> {
        Ok(None)
    }

    async fn list_fragments(&self) -> StoreResult<Vec<FragmentId>> {
        Ok(self
            .0
            .servers
            .lock()
            .unwrap()
            .values()
            .flat_map(|held| held.keys().copied().collect::<Vec<_>>())
            .collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> StoreResult<()> {
        for held in self.0.servers.lock().unwrap().values_mut() {
            held.remove(&id);
        }
        Ok(())
    }

    async fn health(&self) -> StoreResult<Health> {
        Ok(Health::Healthy)
    }
}

#[async_trait]
impl PlacementChunkStore for Fleet {
    async fn get_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
    ) -> StoreResult<Option<Bytes>> {
        Ok(self
            .0
            .servers
            .lock()
            .unwrap()
            .get(&dserver)
            .and_then(|held| held.get(&id).cloned()))
    }

    async fn put_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> StoreResult<()> {
        let state = Arc::clone(&self.0);
        state.log.push(Event::Put {
            dserver,
            id,
            deadline: deadline_millis,
            clock: state.clock.now_millis(),
        });
        let task = Arc::clone(&state);
        let put =
            tokio::spawn(async move { task.apply(dserver, id, fragment, deadline_millis).await });
        let answer = put.await.expect("the put task ran to an answer");
        // The reply in transit: the store has answered, the caller has not heard yet.
        state.gate(id, HoldAt::Answer).await;
        state.log.push(Event::Delivered { id });
        answer
    }
}

// ─────────────────────────────── the runtime ───────────────────────────────

/// What the test's timer does in the instant a wait fires, before staging sees it fire.
type OnFire = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

/// The staging runtime: the manual clock, chunk ids `1, 2, 3, …`, and a timer the test fires by
/// hand ([`Runtime::timer`]'s permits). Every wait staging asks for is logged with its length
/// ([`Runtime::waits`]), though only a permit ends it. The first wait to fire after the test
/// sets [`Runtime::on_fire`] runs it, inside its own firing: so a test can make something
/// happen exactly as the timer fires, with no poll of staging in between.
struct Runtime {
    clock: ManualClock,
    next_chunk: ChunkId,
    timer: Arc<Semaphore>,
    waits: Arc<Mutex<Vec<u64>>>,
    on_fire: OnFire,
}

impl StageClock for Runtime {
    fn now_millis(&self) -> u64 {
        self.clock.now_millis()
    }
}

impl StageRuntime for Runtime {
    fn next_chunk_id(&mut self) -> ChunkId {
        self.next_chunk += 1;
        self.next_chunk
    }

    fn wait(&self, millis: u64) -> impl Future<Output = ()> + Send {
        self.waits.lock().unwrap().push(millis);
        let timer = Arc::clone(&self.timer);
        let on_fire = Arc::clone(&self.on_fire);
        async move {
            timer
                .acquire()
                .await
                .expect("the timer stays open")
                .forget();
            let fired = on_fire.lock().unwrap().take();
            if let Some(fired) = fired {
                fired();
            }
        }
    }
}

// ─────────────────────────────── the rig ───────────────────────────────

struct Rig {
    clock: ManualClock,
    log: Log,
    store: Scripted,
    fleet: Fleet,
    runtime: Runtime,
    topology: Topology,
    upload: UploadId,
    /// The session bytes Create stored: `Open@0`.
    session: Bytes,
    attempt: AttemptId,
}

impl Rig {
    async fn new() -> Self {
        let clock = ManualClock::new(T0);
        let log = Log::default();
        let inner = RedbMetadataStore::in_memory().unwrap();
        inner
            .commit(WriteBatch::new().put(metadata::bucket_key(BUCKET), MARKER))
            .await
            .unwrap();
        let object = format!("{BUCKET}/object");
        let create = CreateRequest {
            bucket: BUCKET,
            parent: ROOT,
            object: &object,
            content_type: None,
            created_at_millis: T0,
            clock_source: "wall",
        };
        let outcome = create_session(&inner, &profile(), &create, &mut Fixed(Some(identity(1))))
            .await
            .expect("the create ran");
        let CreateOutcome::Created { upload_id } = outcome else {
            panic!("expected Created, got {outcome:?}");
        };
        let session = inner.get(&mpu_key(&upload_id)).await.unwrap().unwrap();
        Self {
            store: Scripted {
                inner,
                clock: clock.clone(),
                log: log.clone(),
                state: Mutex::new(ScriptState::default()),
            },
            fleet: Fleet::new(&clock, &log),
            runtime: Runtime {
                clock: clock.clone(),
                next_chunk: 0,
                timer: Arc::new(Semaphore::new(0)),
                waits: Arc::new(Mutex::new(Vec::new())),
                on_fire: Arc::new(Mutex::new(None)),
            },
            topology: topology(),
            upload: upload_id,
            session,
            attempt: attempt(0),
            clock,
            log,
        }
    }

    /// Stage `body` under `request`, with nothing running beside it.
    async fn stage(
        &mut self,
        request: impl FnOnce(&mut StageRequest<'_>),
        body: Body,
    ) -> Result<StageOutcome, StageError> {
        let mut req = self::request(&self.topology, &self.upload, &self.attempt);
        request(&mut req);
        stage_part(&self.store, &self.fleet, &mut self.runtime, &req, body).await
    }

    fn waits(&self) -> Vec<u64> {
        self.runtime.waits.lock().unwrap().clone()
    }

    /// The session's state as the store holds it.
    async fn state(&self) -> SessionState {
        let bytes = self
            .store
            .inner
            .get(&mpu_key(&self.upload))
            .await
            .unwrap()
            .expect("the session is stored");
        decode_session_record(&bytes).unwrap().state().clone()
    }
}

fn staged(outcome: Result<StageOutcome, StageError>) -> Box<StagedAttempt> {
    match outcome {
        Ok(StageOutcome::Staged(attempt)) => attempt,
        other => panic!("expected Staged, got {other:?}"),
    }
}

/// The cause of a staging that stopped with nothing in doubt.
fn stopped(outcome: Result<StageOutcome, StageError>) -> Stop {
    match outcome {
        Err(StageError::Stopped { cause, .. }) => cause,
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// The doubt a reserve or a staging ended in, once it is shown to read `Indeterminate`.
fn in_doubt<T: std::fmt::Debug>(outcome: Result<T, StageError>) -> Box<InDoubt> {
    match outcome {
        Err(err @ StageError::InDoubt(_)) => {
            assert_eq!(classify(&err), ErrorClass::Indeterminate, "{err}");
            let StageError::InDoubt(doubt) = err else {
                unreachable!()
            };
            doubt
        }
        other => panic!("expected InDoubt, got {other:?}"),
    }
}

/// Every slot of `upload`, by index.
async fn slots(store: &impl MetadataStore, upload: &UploadId) -> Vec<(SlotIndex, SlotRecord)> {
    let mut found: Vec<_> = store
        .scan(&slot_range(upload))
        .await
        .unwrap()
        .into_iter()
        .map(|(key, value)| {
            let (owner, index) = parse_slot_key(&key).unwrap();
            assert_eq!(&owner, upload);
            (index, decode_slot_record(&value).unwrap())
        })
        .collect();
    found.sort_by_key(|(index, _)| *index);
    found
}

/// The slots `attempt` holds.
async fn slots_of(store: &impl MetadataStore, upload: &UploadId, attempt: &AttemptId) -> usize {
    slots(store, upload)
        .await
        .iter()
        .filter(|(_, slot)| slot.attempt_id() == attempt)
        .count()
}

/// Every owned entry of `upload`, by chunk id.
async fn owned(store: &impl MetadataStore, upload: &UploadId) -> BTreeMap<ChunkId, OwnedEntry> {
    store
        .scan(&sidx_range(upload))
        .await
        .unwrap()
        .into_iter()
        .map(|(key, value)| {
            let (part_number, chunk, entry) = decode_owned_entry(&key, &value).unwrap();
            assert_eq!(part_number, part());
            (chunk, entry)
        })
        .collect()
}

async fn count(store: &impl MetadataStore, prefix: &[u8]) -> usize {
    store.scan(prefix).await.unwrap().len()
}

async fn snapshot(store: &impl MetadataStore) -> Vec<(Vec<u8>, Bytes)> {
    let mut all = store.scan(b"").await.unwrap();
    all.sort();
    all
}

/// **(A)** Every chunk's intent batches all precede the first put of that chunk's fragments
/// (`0016:1788`: assert the order, not just the end state).
fn assert_intents_precede_puts(log: &Log) {
    let events = log.events();
    let mut chunks = BTreeSet::new();
    for event in &events {
        if let Event::Put { id, .. } = event {
            chunks.insert(id.chunk);
        }
    }
    for chunk in chunks {
        let first_put = events
            .iter()
            .position(|e| matches!(e, Event::Put { id, .. } if id.chunk == chunk))
            .unwrap();
        let intents: Vec<usize> = events
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                matches!(e, Event::Commit { kind: Kind::Intent, batch, .. }
                    if intent_chunk(batch) == chunk)
            })
            .map(|(at, _)| at)
            .collect();
        assert!(!intents.is_empty(), "chunk {chunk} has an intent");
        assert!(
            intents.iter().all(|at| *at < first_put),
            "chunk {chunk}: every intent commit precedes its first fragment put"
        );
    }
}

/// Every fragment the fleet was asked to write, and every fragment it stores, is named, now, by
/// a durable owned entry whose placement puts that fragment on the server it went to.
async fn assert_fragments_evidenced(rig: &Rig) {
    let entries = owned(&rig.store.inner, &rig.upload).await;
    let named = |id: FragmentId, dserver: DServerId| {
        let entry = entries
            .get(&id.chunk)
            .unwrap_or_else(|| panic!("fragment {id:?} has no durable owned entry"));
        assert_eq!(
            entry.staged().placement()[usize::from(id.index)],
            dserver,
            "fragment {id:?} is where its entry says"
        );
    };
    for put in rig.log.puts() {
        named(put.id, put.dserver);
    }
    for (dserver, held) in rig.fleet.0.servers.lock().unwrap().iter() {
        for id in held.keys() {
            named(*id, *dserver);
        }
    }
}

/// Nothing staged names `server`: no owned entry, no fragment put, no hand-off chunk.
async fn assert_nothing_names(rig: &Rig, attempt: &StagedAttempt, server: DServerId) {
    for (chunk, entry) in owned(&rig.store.inner, &rig.upload).await {
        assert!(
            !entry.staged().placement().contains(&server),
            "chunk {chunk}'s entry names draining server {server}"
        );
    }
    assert!(
        rig.log.puts().iter().all(|put| put.dserver != server),
        "no fragment is written to draining server {server}"
    );
    assert!(
        attempt
            .chunks()
            .iter()
            .all(|c| !c.placement.contains(&server)),
        "no hand-off chunk names draining server {server}"
    );
}

/// `open`'s session at `Open@epoch`, in the codec's canonical spelling.
fn open_at(open: &Bytes, epoch: u64) -> Bytes {
    let record = decode_session_record(open).unwrap();
    let json = |text: &str| String::from_utf8(metadata::encode(&text).to_vec()).unwrap();
    let bytes = format!(
        "{{\"parent\":{},\"object\":{},\"created_at_millis\":{},\"clock_source\":{},\
         \"segment_nonce\":\"{}\",\"epoch\":{epoch},\"attempts\":0,\"state\":{{\"kind\":\"Open\"}}}}",
        record.parent(),
        json(record.object()),
        record.created_at_millis(),
        json(record.clock_source()),
        record.segment_nonce(),
    );
    let decoded = decode_session_record(bytes.as_bytes()).expect("a canonical session");
    assert!(matches!(decoded.state(), SessionState::Open {}));
    assert_eq!(metadata::encode(&decoded), bytes.as_bytes());
    Bytes::from(bytes)
}

/// A side action that moves `upload`'s session to `to` (a Complete fence and its release).
fn move_session(upload: &UploadId, to: Bytes) -> Side {
    let key = mpu_key(upload);
    Side::Batch(Arc::new(move |_| {
        WriteBatch::new().put(key.clone(), to.clone())
    }))
}

/// A canonical slot value for attempt `n` of part `part_number`, as residue or a live part would
/// leave it.
fn other_slot(n: u128, part_number: u32) -> Bytes {
    let bytes = format!(
        "{{\"part_number\":{part_number},\"attempt_id\":\"{}\",\"reserved_at_millis\":{T0},\
         \"lease_expiry_millis\":{}}}",
        attempt(n),
        T0 + TTL
    );
    let decoded = decode_slot_record(bytes.as_bytes()).expect("a canonical slot");
    assert_eq!(metadata::encode(&decoded), bytes.as_bytes());
    Bytes::from(bytes)
}

// ═══════════════════════ (A) staging and its order ═══════════════════════

/// **(A)** A body of three full chunks and a short fourth, fed as uneven pieces (10 bytes, then
/// 100 spanning the first boundary, then 37, then 65 spanning the third; no boundary aligned to
/// the chunk size), stages exactly four chunks. Read back through the fragments they equal the
/// body; the length and digest are the body's. Mid-stage the store holds an owned entry and no
/// `pending:` entry. After: one slot (this attempt's), one owned entry per chunk at the
/// placement its fragments went to, nothing under `part:` or `psum:`; every intent precedes its
/// chunk's first fragment put; the hand-off carries one complete `ChunkRef` per chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stages_uneven_pieces_in_order_and_hands_off_complete_chunk_refs() {
    within(async {
        let mut rig = Rig::new().await;
        let body = body_bytes(3 * CHUNK + 20);
        let body_pieces = pieces(&body, &[10, 100, 37, 65]);
        let first = FragmentId { chunk: 1, index: 0 };
        let gate = rig.fleet.hold(first, HoldAt::Arrival);
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let staging = stage_part(
            &rig.store,
            &rig.fleet,
            &mut rig.runtime,
            &req,
            stream_of(body_pieces),
        );
        let (fleet, inner) = (rig.fleet.clone(), &rig.store.inner);
        let driver = async {
            until("chunk 1's first fragment put", || {
                fleet.held(first, HoldAt::Arrival)
            })
            .await;
            assert!(
                !inner.scan(SIDX_PREFIX).await.unwrap().is_empty(),
                "while staging, scan(\"sidx:\") is non-empty"
            );
            assert!(
                inner.scan(b"pending:").await.unwrap().is_empty(),
                "while staging, scan(\"pending:\") is empty"
            );
            gate.add_permits(1);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let attempt = staged(outcome);
        let inner = &rig.store.inner;

        assert_eq!(attempt.chunks().len(), body.len().div_ceil(CHUNK));
        let mut read_back = Vec::new();
        for chunk in attempt.chunks() {
            read_back
                .extend_from_slice(&read_chunk_verified(inner, &rig.fleet, chunk).await.unwrap());
        }
        assert_eq!(
            read_back, body,
            "the chunks read back through their fragments are the body"
        );
        assert_eq!(attempt.len(), body.len() as u64);
        assert_eq!(
            *attempt.digest(),
            sha256(&body),
            "the digest is the body's SHA-256"
        );

        let held = slots(inner, &rig.upload).await;
        assert_eq!(held.len(), 1, "one slot");
        assert_eq!(held[0].1.attempt_id(), &rig.attempt);
        assert_eq!(held[0].1.part_number(), part());
        let entries = owned(inner, &rig.upload).await;
        assert_eq!(
            entries.len(),
            attempt.chunks().len(),
            "one owned entry per chunk"
        );
        for (chunk, entry) in &entries {
            let written: BTreeMap<u16, DServerId> = rig
                .log
                .puts()
                .iter()
                .filter(|put| put.id.chunk == *chunk)
                .map(|put| (put.id.index, put.dserver))
                .collect();
            assert_eq!(written.len(), usize::from(FRAGMENTS));
            for (index, dserver) in written {
                assert_eq!(entry.staged().placement()[usize::from(index)], dserver);
            }
        }
        assert_eq!(count(inner, PART_PREFIX).await, 0, "nothing under part:");
        assert_eq!(count(inner, PSUM_PREFIX).await, 0, "nothing under psum:");
        assert!(inner.scan(b"pending:").await.unwrap().is_empty());
        assert_intents_precede_puts(&rig.log);
        assert_fragments_evidenced(&rig).await;

        // The hand-off: complete chunk references, in chunk order.
        let chunks = attempt.chunks();
        assert_eq!(chunks.iter().map(|c| c.len).sum::<u64>(), body.len() as u64);
        let (last, full) = chunks.split_last().unwrap();
        assert!(
            full.iter().all(|c| c.len == CHUNK as u64),
            "every chunk but the last is full"
        );
        assert_eq!(last.len, 20, "the last chunk carries the short remainder");
        assert_eq!(attempt.owned().len(), chunks.len());
        for (chunk, held) in chunks.iter().zip(attempt.owned()) {
            let (_, id, entry) = decode_owned_entry(held.key(), held.bytes()).unwrap();
            assert_eq!(&entry, held.entry());
            assert_eq!(chunk.id, id, "the ChunkRef's id is its owned entry's chunk");
            assert_eq!(chunk.scheme, entry.staged().scheme());
            assert_eq!(chunk.placement, entry.staged().placement());
            assert_eq!(
                entries[&id], entry,
                "the hand-off's entry is the stored one"
            );
            assert_eq!(
                inner.get(held.key()).await.unwrap().as_ref(),
                Some(held.bytes())
            );
        }
        let ids: Vec<ChunkId> = chunks.iter().map(|c| c.id).collect();
        assert!(ids.windows(2).all(|w| w[0] < w[1]), "chunk order");
        assert_eq!(attempt.part_number(), part());
        assert_eq!(attempt.attempt_id(), &rig.attempt);
        assert_eq!(attempt.upload_id(), &rig.upload);
        assert_eq!(attempt.session_bytes(), &rig.session);
        assert_eq!(attempt.slot_index(), held[0].0);
        assert_eq!(
            attempt.slot_key(),
            slot_key(&rig.upload, held[0].0).as_slice()
        );
        assert_eq!(attempt.slot(), &held[0].1);
        assert_eq!(
            inner.get(attempt.slot_key()).await.unwrap().as_ref(),
            Some(attempt.slot_bytes())
        );
        assert!(!attempt.chunk_bound_exceeded());
        assert!(!attempt.is_empty());
    })
    .await;
}

/// **(A)** An empty body stages no chunk: the attempt holds its slot and nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_an_empty_body_stages_no_chunk() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = staged(rig.stage(|_| {}, stream_of(Vec::new())).await);
        assert!(attempt.is_empty());
        assert!(attempt.chunks().is_empty());
        assert!(attempt.owned().is_empty());
        assert_eq!(*attempt.digest(), sha256(&[]));
        assert_eq!(slots(&rig.store.inner, &rig.upload).await.len(), 1);
        assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
        assert!(rig.log.commits(Kind::Intent).is_empty());
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

// ═══════════════════════ (A′) the write deadline ═══════════════════════

/// **(A′)** Every staged fragment put carries `t_auth + W`, where `t_auth` is the clock reading
/// when its chunk's intent was committed. The wrapper advances the clock right after each intent
/// commit returns, so the puts are dispatched later — and the deadline is still `t_auth + W`:
/// never `t_dispatch + W`, never `None`, never unbounded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_every_fragment_deadline_is_t_auth_plus_w() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store
            .script(Kind::Intent, vec![], Some(Act::PassThenAdvance(777)));
        let body = body_bytes(3 * CHUNK);
        staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK])))
                .await,
        );
        let intents = rig.log.commits(Kind::Intent);
        assert_eq!(intents.len(), 3);
        let puts = rig.log.puts();
        assert_eq!(puts.len(), 3 * usize::from(FRAGMENTS));
        for intent in intents {
            assert_eq!(intent.answer, Answer::Committed);
            let chunk = intent_chunk(&intent.batch);
            let t_auth = intent.clock;
            for put in puts.iter().filter(|put| put.id.chunk == chunk) {
                assert_eq!(put.clock, t_auth + 777, "the dispatch was delayed");
                assert_eq!(put.deadline, Some(t_auth + W), "deadline = t_auth + W");
                assert_ne!(put.deadline, Some(put.clock + W), "never t_dispatch + W");
                assert_ne!(put.deadline, None);
                assert_ne!(put.deadline, Some(u64::MAX));
            }
        }
    })
    .await;
}

/// The effect-unknown stop a staging ended with: its chunk, its deadline, the writes it
/// abandoned and the store's reports.
fn effect_unknown(cause: &Stop) -> (ChunkId, u64, &[WriteDeadlineExpired], &[BoxError]) {
    match cause {
        Stop::WriteEffectUnknown {
            chunk,
            deadline_millis,
            abandoned,
            reports,
            ..
        } => (*chunk, *deadline_millis, abandoned, reports),
        other => panic!("expected WriteEffectUnknown, got {other:?}"),
    }
}

/// What stopped the renewal that stopped staging beside the writes an effect-unknown stop
/// accounts for; `None` when staging waited for them to their end or their deadline (657.1b).
fn stopped_by_renewal(cause: &Stop) -> Option<&Stop> {
    match cause {
        Stop::WriteEffectUnknown { renewal, .. } => renewal.as_deref(),
        other => panic!("expected WriteEffectUnknown, got {other:?}"),
    }
}

/// Staging's account of the one write it abandoned: `held`, its effect unknown, in staging's own
/// words, never dressed up as the store's.
fn assert_abandoned_only(abandoned: &[WriteDeadlineExpired], held: FragmentId) {
    assert_eq!(abandoned.len(), 1, "one write abandoned");
    assert_eq!(abandoned[0].id, held);
    assert_eq!(abandoned[0].effect, WriteEffect::Unknown);
    assert_eq!(abandoned[0].error_class(), ErrorClass::Indeterminate);
    assert!(
        abandoned[0].detail.contains("the writer, not the store"),
        "{}",
        abandoned[0].detail
    );
}

/// Hold chunk 1's first fragment put at `at`, stage two chunks, and let `driver` move the clock
/// and the timer. Returns the outcome and whether staging had ended when the driver finished.
async fn stage_with_held_put<D, F>(
    rig: &mut Rig,
    at: HoldAt,
    driver: D,
) -> (Result<StageOutcome, StageError>, Arc<Semaphore>)
where
    D: FnOnce(Fleet, ManualClock, Arc<Semaphore>, Arc<Semaphore>, Arc<AtomicBool>) -> F,
    F: Future<Output = ()>,
{
    let held = FragmentId { chunk: 1, index: 0 };
    let gate = rig.fleet.hold(held, at);
    let body = body_bytes(2 * CHUNK);
    let ended = Arc::new(AtomicBool::new(false));
    let driver = driver(
        rig.fleet.clone(),
        rig.clock.clone(),
        Arc::clone(&rig.runtime.timer),
        Arc::clone(&gate),
        Arc::clone(&ended),
    );
    let req = request(&rig.topology, &rig.upload, &rig.attempt);
    let staging = flagging(
        ended,
        stage_part(
            &rig.store,
            &rig.fleet,
            &mut rig.runtime,
            &req,
            stream_of(pieces(&body, &[2 * CHUNK])),
        ),
    );
    let (outcome, ()) = tokio::join!(staging, driver);
    (outcome, gate)
}

/// **(A′)** The wait for a chunk's fragment writes is bounded by their deadline, fail-closed
/// (`0016:1555-1557`): a put the fake holds once the clock has reached `t_auth + W` is abandoned
/// at the deadline, without waiting for it, and staging stops with a typed error naming the chunk
/// and the deadline — an effect it cannot know, never "nothing landed". The put is not re-sent
/// and no later chunk is started. A staging that awaited the writes without their deadline would
/// still be waiting; this leg fails waiting for it to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_a_write_pending_past_its_deadline_is_abandoned() {
    within(async {
        let mut rig = Rig::new().await;
        let held = FragmentId { chunk: 1, index: 0 };
        let (outcome, gate) = stage_with_held_put(
            &mut rig,
            HoldAt::Arrival,
            |fleet, clock, timer, _, ended| async move {
                until("chunk 1's held put", || {
                    fleet.holds_only(held, HoldAt::Arrival)
                })
                .await;
                clock.advance(W);
                timer.add_permits(1);
                until("staging to stop at the deadline", || {
                    ended.load(Ordering::SeqCst)
                })
                .await;
            },
        )
        .await;
        assert_eq!(rig.clock.now_millis(), T0 + W, "it stopped at the deadline");
        assert_eq!(
            gate.available_permits(),
            0,
            "the held put was never released"
        );
        let err = outcome.expect_err("staging stopped");
        let text = err.to_string();
        assert!(text.contains("is unknown"), "{text}");
        assert_eq!(
            classify(&err),
            ErrorClass::Indeterminate,
            "an abandoned write's effect is unknown, and its class says so"
        );
        let cause = stopped(Err(err));
        let (chunk, deadline, abandoned, reports) = effect_unknown(&cause);
        assert_eq!((chunk, deadline), (1, T0 + W));
        assert_abandoned_only(abandoned, held);
        assert!(reports.is_empty(), "the other five answered Ok");
        assert_eq!(
            rig.log.put_count(held),
            1,
            "the abandoned put is not re-sent"
        );
        assert_eq!(
            rig.log.commits(Kind::Intent).len(),
            1,
            "no later chunk was started"
        );
        // The body read waited on the timer, then the writes waited for exactly their window. The
        // read's wait runs to the renewal instant, half a TTL on, which comes before its idle bound
        // (#933).
        assert_eq!(rig.waits(), vec![HALF, W]);
        assert_fragments_evidenced(&rig).await;
        assert_eq!(owned(&rig.store.inner, &rig.upload).await.len(), 1);
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            1
        );
    })
    .await;
}

/// **(A′)** A held put released after its deadline is refused and **not published** (v3
/// sign-off). The fake holds a put on arrival, the clock passes its deadline, staging stops, and
/// only then is the put released. The fake's publication-point check refuses it with
/// `WriteEffect::NotApplied`, and no fragment of that put is readable afterwards. A fake without
/// that check would store it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_a_held_put_released_after_expiry_is_refused_and_not_published() {
    within(async {
        let mut rig = Rig::new().await;
        let held = FragmentId { chunk: 1, index: 0 };
        let (outcome, gate) = stage_with_held_put(
            &mut rig,
            HoldAt::Arrival,
            |fleet, clock, timer, _, ended| async move {
                until("chunk 1's held put", || {
                    fleet.holds_only(held, HoldAt::Arrival)
                })
                .await;
                clock.advance(W + 1);
                timer.add_permits(1);
                until("staging to stop", || ended.load(Ordering::SeqCst)).await;
            },
        )
        .await;
        let cause = stopped(outcome);
        assert_abandoned_only(effect_unknown(&cause).2, held);
        gate.add_permits(1);
        let log = rig.log.clone();
        until("the fake to answer the late put", || {
            log.has(|e| matches!(e, Event::Refused { id } | Event::Stored { id } if *id == held))
        })
        .await;
        assert_eq!(
            rig.fleet.stored_on(held),
            None,
            "nothing of it was published"
        );
        assert!(
            log.has(|e| matches!(e, Event::Refused { id } if *id == held)),
            "the fake refused it at its publication point"
        );
        let dserver = rig
            .log
            .puts()
            .iter()
            .find(|p| p.id == held)
            .unwrap()
            .dserver;
        assert_eq!(
            rig.fleet.get_fragment_at(dserver, held).await.unwrap(),
            None,
            "no fragment of that put is readable"
        );
        assert!(!log.has(|e| matches!(e, Event::Stored { id } if *id == held)));
    })
    .await;
}

/// **(A′)** A put the store refuses as too late *before* staging stops waiting is reported as
/// what it is: nothing of it landed. The fake holds a put on arrival; the clock passes the
/// deadline and the put is released before the timer fires, so the fake refuses it
/// (`WriteEffect::NotApplied`) and every write of the chunk has answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_a_put_refused_as_late_is_reported_as_nothing_landed() {
    within(async {
        let mut rig = Rig::new().await;
        let held = FragmentId { chunk: 1, index: 0 };
        let (outcome, _) = stage_with_held_put(
            &mut rig,
            HoldAt::Arrival,
            |fleet, clock, _, gate, _| async move {
                until("chunk 1's held put", || {
                    fleet.holds_only(held, HoldAt::Arrival)
                })
                .await;
                clock.advance(W);
                gate.add_permits(1);
            },
        )
        .await;
        let err = outcome.expect_err("staging stopped");
        assert_eq!(classify(&err), ErrorClass::Terminal);
        assert!(err.to_string().contains("nothing of them landed"), "{err}");
        let cause = stopped(Err(err));
        assert!(
            matches!(
                cause,
                Stop::WriteDeadlineElapsed { chunk: 1, deadline_millis, refused: 1 }
                    if deadline_millis == T0 + W
            ),
            "{cause:?}"
        );
        assert_eq!(rig.fleet.stored_on(held), None);
        // The read's wait runs to the renewal instant, before its idle bound (#933).
        assert_eq!(rig.waits(), vec![HALF, W], "the timer never fired");
    })
    .await;
}

/// **(A′)** A deadline that has passed before the writes are dispatched sends none of them: the
/// intent commit is slow enough that the clock reaches `t_auth + W` before it returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_a_deadline_passed_before_dispatch_sends_nothing() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store
            .script(Kind::Intent, vec![Act::PassThenAdvance(W)], None);
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(
                cause,
                Stop::WriteDeadlineElapsed { chunk: 1, deadline_millis, refused: 0 }
                    if deadline_millis == T0 + W
            ),
            "{cause:?}"
        );
        assert!(rig.log.puts().is_empty(), "no fragment write was sent");
        assert_eq!(owned(&rig.store.inner, &rig.upload).await.len(), 1);
    })
    .await;
}

/// **(A′)** A publication that straddles the deadline (added 2026-10-22). The fake passes its
/// publication-point check inside the window, then holds **inside** its publishing step while
/// the clock passes the deadline (`crates/traits/src/lib.rs`, `WriteEffect::Unknown`).
///
/// * Run 1: the test lets the fake finish; the fragment is stored and the fake answers
///   `WriteEffect::Unknown` (`WriteDeadlineExpired::if_publication_unverified`).
/// * Run 2: the timer fires while the fake is still inside the step; staging abandons the put and
///   returns, and only then does the fake finish and publish.
///
/// In both: a typed error saying the write's effect is unknown — never success, never the clean
/// "nothing landed" refusal — and classed `Indeterminate` in both, since the bytes are in the same
/// state whichever way the timing fell (review finding: run 2 read `Terminal`); no staged
/// attempt; the fragment that did land is on a position the chunk's durable owned entry names;
/// the owned entry and the slot are still there, residue for the reaper.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_a_publication_straddling_the_deadline_is_never_counted() {
    for run in [1, 2] {
        within(async {
            let mut rig = Rig::new().await;
            let held = FragmentId { chunk: 1, index: 0 };
            let (outcome, gate) = stage_with_held_put(
                &mut rig,
                HoldAt::Publishing,
                |fleet, clock, timer, gate, ended| async move {
                    until("chunk 1's put inside its publishing step", || {
                        fleet.holds_only(held, HoldAt::Publishing)
                    })
                    .await;
                    clock.advance(W);
                    if run == 1 {
                        gate.add_permits(1);
                    } else {
                        timer.add_permits(1);
                        until("staging to stop at the deadline", || {
                            ended.load(Ordering::SeqCst)
                        })
                        .await;
                    }
                },
            )
            .await;
            let err = match outcome {
                Err(err @ StageError::Stopped { .. }) => err,
                other => panic!("run {run}: expected Stopped, got {other:?}"),
            };
            assert!(err.to_string().contains("is unknown"), "run {run}: {err}");
            assert_eq!(
                classify(&err),
                ErrorClass::Indeterminate,
                "run {run}: the class follows the bytes, not the timing"
            );
            let cause = stopped(Err(err));
            let (chunk, deadline, abandoned, reports) = effect_unknown(&cause);
            assert_eq!((chunk, deadline), (1, T0 + W), "run {run}");
            if run == 1 {
                assert!(abandoned.is_empty());
                assert_eq!(reports.len(), 1);
                let report = write_deadline_outcome(reports[0].as_ref()).expect("the store's");
                assert_eq!(report.effect, WriteEffect::Unknown);
                assert_eq!(report.id, held);
                assert!(rig
                    .log
                    .has(|e| matches!(e, Event::Unverified { id } if *id == held)));
            } else {
                assert_abandoned_only(abandoned, held);
                assert!(reports.is_empty());
                assert_eq!(gate.available_permits(), 0, "still inside its step");
                gate.add_permits(1);
                let log = rig.log.clone();
                until("the fake to finish publishing", || {
                    log.has(|e| matches!(e, Event::Stored { id } if *id == held))
                })
                .await;
            }
            let landed = rig.fleet.stored_on(held).expect("the fragment landed");
            let entries = owned(&rig.store.inner, &rig.upload).await;
            assert_eq!(
                entries[&1].staged().placement()[0],
                landed,
                "run {run}: it landed on a position the durable owned entry names"
            );
            assert_fragments_evidenced(&rig).await;
            assert_eq!(entries.len(), 1, "run {run}: the owned entry stays");
            assert_eq!(
                slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
                1,
                "run {run}: the slot stays"
            );
            assert_eq!(rig.log.commits(Kind::Intent).len(), 1, "no later chunk");
        })
        .await;
    }
}

/// **(A′)** The class of a write stop does not depend on which write answered first (review
/// finding): the most indeterminate account decides, `Transient` only if every one is. In both
/// runs chunk 1's second fragment write is answered with a transient fault, first.
///
/// * Run 1: the first write is held inside its publishing step across the deadline and answered
///   `WriteEffect::Unknown`, later. The stop is classed `Indeterminate`, so a caller is never
///   told to retry a write that may have landed.
/// * Run 2: the first write is held on arrival and then failed with a plain fault, later. The
///   stop is classed `Terminal`: one write said retrying cannot help, and a transient fault that
///   answered earlier does not outrank it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_a_write_stop_is_classed_by_its_most_indeterminate_answer() {
    for unknown_later in [true, false] {
        within(async {
            let mut rig = Rig::new().await;
            let held = FragmentId { chunk: 1, index: 0 };
            let failing = FragmentId { chunk: 1, index: 1 };
            rig.fleet.fail(failing, Failure::Transient);
            let at = if unknown_later {
                HoldAt::Publishing
            } else {
                rig.fleet.fail(held, Failure::Plain);
                HoldAt::Arrival
            };
            let (outcome, _) =
                stage_with_held_put(&mut rig, at, |fleet, clock, _, gate, _| async move {
                    until("chunk 1's held put, the rest answered", || {
                        fleet.holds_only(held, at)
                    })
                    .await;
                    if unknown_later {
                        clock.advance(W);
                    }
                    gate.add_permits(1);
                })
                .await;
            let err = match outcome {
                Err(err @ StageError::Stopped { .. }) => err,
                other => panic!("expected Stopped, got {other:?}"),
            };
            let class = if unknown_later {
                ErrorClass::Indeterminate
            } else {
                ErrorClass::Terminal
            };
            assert_eq!(classify(&err), class, "{err}");
            let said = if unknown_later {
                "NOT acknowledged"
            } else {
                "disk failed"
            };
            assert!(err.to_string().contains(said), "{err}");
            let cause = stopped(Err(err));
            let (chunk, _, abandoned, reports) = effect_unknown(&cause);
            assert_eq!(chunk, 1);
            assert!(abandoned.is_empty(), "every write answered");
            assert_eq!(reports.len(), 2);
            assert_eq!(
                classify(reports[0].as_ref()),
                ErrorClass::Transient,
                "the transient fault answered first"
            );
            match write_deadline_outcome(reports[1].as_ref()) {
                Some(verdict) => {
                    assert!(unknown_later);
                    assert_eq!((verdict.id, verdict.effect), (held, WriteEffect::Unknown));
                }
                None => {
                    assert!(!unknown_later);
                    assert_eq!(classify(reports[1].as_ref()), ErrorClass::Terminal);
                }
            }
            assert_eq!(rig.fleet.stored_on(failing), None);
            assert_fragments_evidenced(&rig).await;
        })
        .await;
    }
}

/// **(A′)** An `Ok` the store gave in time is counted written even when staging first sees it in
/// the instant its deadline's timer fires: `Ok` means published strictly before the deadline,
/// whenever it is seen. The fake publishes chunk 1's first fragment in time and answers `Ok`, but
/// holds the reply in transit; the clock reaches the deadline, and the timer's own firing releases
/// the reply, so the wait sees the timer fire with the answer ready behind it. The part stages. A
/// wait that dropped an answer ready when its timer fired would abandon a write that landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prime_an_ok_ready_as_the_deadline_timer_fires_is_counted_written() {
    within(async {
        let mut rig = Rig::new().await;
        let held = FragmentId { chunk: 1, index: 0 };
        let on_fire = Arc::clone(&rig.runtime.on_fire);
        let fired = Arc::clone(&on_fire);
        let (outcome, _) = stage_with_held_put(
            &mut rig,
            HoldAt::Answer,
            |fleet, clock, timer, gate, _| async move {
                until("chunk 1's Ok in transit, the rest answered", || {
                    fleet.holds_only(held, HoldAt::Answer)
                })
                .await;
                clock.advance(W);
                *on_fire.lock().unwrap() = Some(Box::new(move || gate.add_permits(1)));
                timer.add_permits(1);
            },
        )
        .await;
        let attempt = staged(outcome);
        assert!(fired.lock().unwrap().is_none(), "the timer fired");
        assert!(rig
            .log
            .has(|e| matches!(e, Event::Acked { id } if *id == held)));
        assert_eq!(attempt.chunks().len(), 2);
        assert_eq!(
            rig.clock.now_millis(),
            T0 + W,
            "chunk 1's deadline had come"
        );
        // The read's wait runs to the renewal instant, before its idle bound (#933).
        assert_eq!(
            rig.waits()[..2],
            [HALF, W],
            "chunk 1's writes waited for their window"
        );
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

// ═══════════════════════ (A″) the body-idle bound ═══════════════════════

/// **(A″)** A body read left unanswered for the bound stops staging, on the test's clock and
/// timer, and nothing is written after it. The body sends 10 bytes and then nothing. A timer that
/// fires a millisecond early is waited out for what is left; at the bound, staging stops with
/// `BodyIdle`. The slot stays for the reaper. A staging that awaited the body without the bound
/// would still be waiting; this leg fails waiting for it to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_double_prime_a_stalled_body_read_stops_at_its_idle_bound() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body) = channel(&rig.log);
        tx.send(Bytes::from(body_bytes(10))).unwrap();
        let (clock, timer, waits) = (
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
        req.body_idle_millis = BOUND;
        let ended = Arc::new(AtomicBool::new(false));
        let staging = flagging(
            Arc::clone(&ended),
            stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body),
        );
        let armed = |n: usize| {
            let waits = Arc::clone(&waits);
            move || waits.lock().unwrap().len() == n
        };
        let driver = async move {
            until("the second read to wait", armed(2)).await;
            clock.advance(BOUND - 1);
            timer.add_permits(1);
            until("the early wake to wait out the rest", armed(3)).await;
            clock.advance(1);
            timer.add_permits(1);
            until("staging to give up the stalled body", || {
                ended.load(Ordering::SeqCst)
            })
            .await;
            // The body never ended: the client is still connected, sending nothing.
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let err = outcome.expect_err("staging stopped");
        assert!(
            err.to_string()
                .contains(&format!("the part body sent nothing for {BOUND} ms")),
            "{err}"
        );
        let cause = stopped(Err(err));
        assert!(
            matches!(cause, Stop::BodyIdle { idle_millis } if idle_millis == BOUND),
            "{cause:?}"
        );
        assert_eq!(rig.clock.now_millis(), T0 + BOUND);
        assert_eq!(rig.waits(), vec![BOUND, BOUND, 1]);
        assert!(
            rig.log.commits(Kind::Intent).is_empty(),
            "nothing written after it"
        );
        assert!(rig.log.puts().is_empty());
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            1,
            "the slot stays for the reaper"
        );
    })
    .await;
}

/// **(A″)** A body answer seen at or past the read's idle deadline is late, a piece or the
/// body's end alike (review finding). The body sends 10 bytes, so a partial chunk is buffered,
/// and the next read is left pending; the clock reaches the bound (the lease still live), and
/// only then does the body answer — it ends, or it sends a late piece and then ends — while the
/// timer fires too. Staging stops with `BodyIdle` at the bound and writes nothing: the buffered
/// bytes are never staged. A wait that took whatever answer it saw, without judging the clock,
/// would stage them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_double_prime_a_body_answer_seen_past_its_idle_deadline_is_late() {
    for late_piece in [false, true] {
        within(async {
            let mut rig = Rig::new().await;
            let (tx, body) = channel(&rig.log);
            tx.send(Bytes::from(body_bytes(10))).unwrap();
            let (clock, timer, waits) = (
                rig.clock.clone(),
                Arc::clone(&rig.runtime.timer),
                Arc::clone(&rig.runtime.waits),
            );
            let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
            req.body_idle_millis = BOUND;
            let ended = Arc::new(AtomicBool::new(false));
            let staging = flagging(
                Arc::clone(&ended),
                stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body),
            );
            let driver = async move {
                until("the second read to wait", || {
                    waits.lock().unwrap().len() == 2
                })
                .await;
                clock.advance(BOUND);
                if late_piece {
                    tx.send(Bytes::from(body_bytes(20))).unwrap();
                }
                // The body ends: a staging that took the late piece still has an end to stage on.
                drop(tx);
                timer.add_permits(1);
                until("staging to stop", || ended.load(Ordering::SeqCst)).await;
            };
            let (outcome, ()) = tokio::join!(staging, driver);
            let cause = stopped(outcome);
            assert!(
                matches!(cause, Stop::BodyIdle { idle_millis } if idle_millis == BOUND),
                "late piece {late_piece}: {cause:?}"
            );
            assert_eq!(rig.clock.now_millis(), T0 + BOUND);
            assert!(
                rig.log.commits(Kind::Intent).is_empty(),
                "late piece {late_piece}: the buffered bytes were never staged"
            );
            assert!(rig.log.puts().is_empty());
            assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
            assert_eq!(
                slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
                1,
                "the slot stays for the reaper"
            );
        })
        .await;
    }
}

/// **(A″)** The bound is per read, not per body: a body that sends a piece inside every window is
/// never cut off. With a bound `B`, the second piece comes at `T0 + B - 1` and the end at
/// `T0 + 2B - 2`, the timer firing just before each; the part stages. A bound measured from the
/// start of the body would stop it at the second wake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_double_prime_the_idle_bound_is_per_read_not_per_body() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body_stream) = channel(&rig.log);
        let body = body_bytes(20);
        tx.send(Bytes::copy_from_slice(&body[..10])).unwrap();
        let (log, clock, timer, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
        req.body_idle_millis = BOUND;
        // A lease long enough that no renewal falls due inside the leg, so its waits are the idle
        // bound's alone (#933; the bound beside renewals is R3's
        // `r3_the_body_idle_bound_holds_while_renewals_keep_the_leases`).
        req.lease_ttl_millis = 4 * TTL;
        let staging = stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream);
        let piece = Bytes::copy_from_slice(&body[10..]);
        let armed = |n: usize| {
            let waits = Arc::clone(&waits);
            move || waits.lock().unwrap().len() == n
        };
        let driver = async move {
            until("the second read to wait", armed(2)).await;
            clock.advance(BOUND - 1);
            timer.add_permits(1);
            until("the wake to wait again", armed(3)).await;
            tx.send(piece).unwrap();
            until("the second piece read", || log.read(1)).await;
            until("the third read to wait", armed(4)).await;
            clock.advance(BOUND - 1);
            timer.add_permits(1);
            until("the wake to wait again", armed(5)).await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let attempt = staged(outcome);
        assert_eq!(attempt.len(), 20);
        assert_eq!(*attempt.digest(), sha256(&body));
        assert_eq!(
            rig.clock.now_millis(),
            T0 + 2 * (BOUND - 1),
            "the body took longer than one bound"
        );
        assert_eq!(rig.waits()[..5], [BOUND, BOUND, 1, BOUND, 1]);
    })
    .await;
}

/// **(A″)** Empty pieces do not count as progress (v3 sign-off). A body that sends only empty
/// pieces, each just inside the bound after the last, is stopped at the bound measured from the
/// last **non-empty** piece, or from the start of the body if there was none. HTTP/2 permits empty
/// `DATA` frames, so a production body can do this. A staging whose empty piece re-armed the
/// deadline would wait past the bound; this leg fails waiting for it to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_double_prime_empty_pieces_do_not_count_as_progress() {
    // The first non-empty piece's offset from T0, if any.
    for first in [None, Some(500)] {
        within(async {
            let mut rig = Rig::new().await;
            let (tx, body) = channel(&rig.log);
            let (log, clock, timer, waits) = (
                rig.log.clone(),
                rig.clock.clone(),
                Arc::clone(&rig.runtime.timer),
                Arc::clone(&rig.runtime.waits),
            );
            let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
            req.body_idle_millis = BOUND;
            let ended = Arc::new(AtomicBool::new(false));
            let staging = flagging(
                Arc::clone(&ended),
                stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body),
            );
            let driver = async move {
                let armed = |n: usize| {
                    let waits = Arc::clone(&waits);
                    move || waits.lock().unwrap().len() == n
                };
                until("the first read to wait", armed(1)).await;
                let mut read = 0;
                if let Some(offset) = first {
                    clock.advance(offset);
                    tx.send(Bytes::from(body_bytes(10))).unwrap();
                    until("the non-empty piece read", || log.read(0)).await;
                    read = 1;
                    until("the next read to wait", armed(2)).await;
                }
                for _ in 0..3 {
                    clock.advance(BOUND - 1);
                    tx.send(Bytes::new()).unwrap();
                    until("the empty piece read", || log.read(read)).await;
                    read += 1;
                    clock.advance(1);
                    // `add_permits` hands the permit straight to a queued wait, so the permit
                    // count says nothing about whether staging has run: watch for it to stop, or
                    // to ask for another wait.
                    let asked = waits.lock().unwrap().len();
                    timer.add_permits(1);
                    let stopped = || ended.load(Ordering::SeqCst);
                    let waits = Arc::clone(&waits);
                    until("staging to stop, or to wait again", || {
                        stopped() || waits.lock().unwrap().len() > asked
                    })
                    .await;
                    if stopped() {
                        break;
                    }
                }
                drop(tx);
            };
            let (outcome, ()) = tokio::join!(staging, driver);
            let cause = stopped(outcome);
            assert!(
                matches!(cause, Stop::BodyIdle { idle_millis } if idle_millis == BOUND),
                "{first:?}: {cause:?}"
            );
            let from = T0 + first.unwrap_or(0);
            assert_eq!(
                rig.clock.now_millis(),
                from + BOUND,
                "{first:?}: stopped at the bound from the last non-empty piece"
            );
            assert!(rig.log.commits(Kind::Intent).is_empty());
            assert!(rig.log.puts().is_empty());
        })
        .await;
    }
}

/// **(A″)** A body that always has an empty piece ready never keeps staging from its timer: the
/// read yields after each one, and staging stops at the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_double_prime_an_endless_run_of_empty_pieces_stops_at_the_bound() {
    within(async {
        let mut rig = Rig::new().await;
        let body: Body = Box::pin(stream::repeat_with(|| Ok(Bytes::new())));
        let (clock, timer, waits) = (
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
        req.body_idle_millis = BOUND;
        let staging = stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body);
        let driver = async move {
            until("the read to wait", || waits.lock().unwrap().len() == 1).await;
            clock.advance(BOUND);
            timer.add_permits(1);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let cause = stopped(outcome);
        assert!(matches!(cause, Stop::BodyIdle { .. }), "{cause:?}");
        assert_eq!(rig.waits(), vec![BOUND]);
    })
    .await;
}

// ═══════════════════════ (B) the drain fence ═══════════════════════

/// The first server the owned entry an intent batch puts is placed on.
fn first_server(batch: &WriteBatch) -> DServerId {
    intent_entry(batch).staged().placement()[0]
}

/// **B1.** The wrapper records `desired:dserver:<S>`, for a server `S` the plan selected, just
/// before the first intent commits. That intent fails on its own drain fence and re-plans;
/// staging completes, and no owned entry, fragment or hand-off chunk names `S`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b1_a_drain_recorded_just_before_the_intent_commits_makes_it_replan() {
    within(async {
        let mut rig = Rig::new().await;
        let drained = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&drained);
        let drain: Hook = Arc::new(move |batch| {
            let server = first_server(batch);
            *seen.lock().unwrap() = Some(server);
            WriteBatch::new().put(metadata::desired_dserver_key(server), b"draining".to_vec())
        });
        rig.store
            .script(Kind::Intent, vec![Act::First(Side::Batch(drain))], None);
        let body = body_bytes(3 * CHUNK);
        let attempt = staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK])))
                .await,
        );
        let server = drained.lock().unwrap().expect("the hook ran");

        let intents = rig.log.commits(Kind::Intent);
        assert_eq!(
            intents[0].answer,
            Answer::Conflict,
            "the drain fence failed the intent"
        );
        assert_eq!(first_server(&intents[0].batch), server);
        // The fence is one `require_absent` per server the entry names, in the custodian's bytes.
        let fenced: BTreeSet<Vec<u8>> = intents[0]
            .batch
            .preconditions
            .iter()
            .filter(|p| p.expected.is_none() && p.key.starts_with(b"desired:dserver:"))
            .map(|p| p.key.clone())
            .collect();
        let named: BTreeSet<Vec<u8>> = intent_entry(&intents[0].batch)
            .staged()
            .placement()
            .iter()
            .map(|s| format!("desired:dserver:{s}").into_bytes())
            .collect();
        assert_eq!(fenced, named);
        assert_eq!(intents[1].answer, Answer::Committed, "the re-plan landed");
        assert_nothing_names(&rig, &attempt, server).await;
        assert_eq!(attempt.chunks().len(), 3);
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

/// **B2.** `desired:dserver:<S>` is recorded before staging and `S` is the selector's first
/// pick (lowest utilization) on the full topology. Staging completes, nothing names `S`, and no
/// intent conflicts: the plan left `S` out, rather than learning of it from the fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b2_a_drain_recorded_before_staging_is_planned_around() {
    within(async {
        let mut rig = Rig::new().await;
        let preferred = 9;
        rig.topology = topology_preferring(preferred);
        assert_eq!(
            select_distinct_domains(&rig.topology, FRAGMENTS).unwrap()[0],
            preferred,
            "the fixture binds: S is the full topology's first pick"
        );
        let drain = WriteBatch::new().put(
            metadata::desired_dserver_key(preferred),
            b"draining".to_vec(),
        );
        rig.store.inner.commit(drain).await.unwrap();
        let body = body_bytes(3 * CHUNK);
        let attempt = staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK])))
                .await,
        );
        assert_nothing_names(&rig, &attempt, preferred).await;
        assert!(
            rig.log
                .commits(Kind::Intent)
                .iter()
                .all(|c| c.answer == Answer::Committed),
            "no intent conflicted"
        );
        assert_eq!(rig.log.commits(Kind::Intent).len(), 3);
    })
    .await;
}

/// **B3.** Four of twelve single-server domains draining and RS(4,2), which fits in the other
/// eight: the part stages, and nothing names a draining server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b3_four_of_twelve_draining_still_stages() {
    within(async {
        let mut rig = Rig::new().await;
        for server in 0..4 {
            let drain =
                WriteBatch::new().put(metadata::desired_dserver_key(server), b"draining".to_vec());
            rig.store.inner.commit(drain).await.unwrap();
        }
        let body = body_bytes(2 * CHUNK + 5);
        let attempt = staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[2 * CHUNK + 5])))
                .await,
        );
        for server in 0..4 {
            assert_nothing_names(&rig, &attempt, server).await;
        }
        assert_eq!(attempt.chunks().len(), 3);
    })
    .await;
}

/// **B3.** Too few non-draining domains for the scheme (RS(6,3) needs nine, eight remain): a
/// typed placement error, never `NoSuchUpload` for an `Open` session, and nothing written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b3_too_few_domains_is_a_typed_placement_error_writing_nothing() {
    within(async {
        let mut rig = Rig::new().await;
        for server in 0..4 {
            let drain =
                WriteBatch::new().put(metadata::desired_dserver_key(server), b"draining".to_vec());
            rig.store.inner.commit(drain).await.unwrap();
        }
        let before = snapshot(&rig.store.inner).await;
        let rs63 = EcScheme::ReedSolomon { k: 6, m: 3 };
        let outcome = rig
            .stage(
                |r| r.scheme = rs63,
                stream_of(vec![Bytes::from(body_bytes(CHUNK))]),
            )
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(
                cause,
                Stop::Placement(SelectorError::InsufficientDomains { have: 8, need: 9 })
            ),
            "{cause:?}"
        );
        assert_eq!(snapshot(&rig.store.inner).await, before, "nothing written");
        assert!(rig.log.events().is_empty(), "no commit, no put");
        assert!(matches!(rig.state().await, SessionState::Open {}));
    })
    .await;
}

/// **B3.** Drains recorded after the slot is reserved, leaving too few domains: the intent's own
/// plan refuses with the same typed placement error, not `NoSuchUpload`, and no owned entry is
/// written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b3_drains_after_the_reserve_leave_a_typed_placement_error() {
    within(async {
        let mut rig = Rig::new().await;
        let drains: Hook = Arc::new(|_| {
            (0..7).fold(WriteBatch::new(), |batch, server| {
                batch.put(metadata::desired_dserver_key(server), b"draining".to_vec())
            })
        });
        rig.store
            .script(Kind::Reserve, vec![Act::First(Side::Batch(drains))], None);
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(
                cause,
                Stop::Placement(SelectorError::InsufficientDomains { have: 5, need: 6 })
            ),
            "{cause:?}"
        );
        assert!(
            rig.log.commits(Kind::Intent).is_empty(),
            "no intent was sent"
        );
        assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

/// **(B)** The drain key keeps the custodian's exact bytes, `desired:dserver:<id>`.
#[test]
fn b_the_drain_key_keeps_its_bytes() {
    assert_eq!(
        metadata::desired_dserver_key(42),
        b"desired:dserver:42".to_vec()
    );
    assert_eq!(metadata::DESIRED_DSERVER_PREFIX, b"desired:dserver:");
    assert_eq!(
        metadata::parse_desired_dserver_key(b"desired:dserver:42"),
        Some(42)
    );
    assert_eq!(
        metadata::parse_desired_dserver_key(b"desired:dserver:x"),
        None
    );
    assert_eq!(metadata::parse_desired_dserver_key(b"orphan:42"), None);
}

// ═══════════════════════ (C) slots ═══════════════════════

/// A request for `attempt` on the rig's session.
fn reserve_request<'a>(rig: &'a Rig, attempt: &'a AttemptId) -> StageRequest<'a> {
    request(&rig.topology, &rig.upload, attempt)
}

fn inflight_full() -> Refusal {
    Refusal::SlowDown {
        pressure: Backpressure::InflightParts {
            max_inflight_parts: MAX_INFLIGHT_PARTS,
        },
    }
}

/// Reserve every slot index of the rig's session for attempts `1..=MAX_INFLIGHT_PARTS`.
async fn fill_every_slot(rig: &Rig) {
    for n in 1..=u128::from(MAX_INFLIGHT_PARTS) {
        let id = attempt(n);
        let outcome = reserve_slot(&rig.store, &reserve_request(rig, &id), T0)
            .await
            .unwrap();
        assert!(matches!(outcome, ReserveOutcome::Reserved { .. }));
    }
}

/// **(C)** `MAX_INFLIGHT_PARTS` reserves succeed on one session, at indices `0..16`; the next is
/// `SlowDown { InflightParts }`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_the_cap_is_the_key_space() {
    within(async {
        let rig = Rig::new().await;
        for n in 0..u128::from(MAX_INFLIGHT_PARTS) {
            let id = attempt(n);
            let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &id), T0)
                .await
                .unwrap();
            assert_eq!(
                outcome,
                ReserveOutcome::Reserved {
                    index: SlotIndex::new(n as u32).unwrap(),
                    attempt_id: id
                }
            );
        }
        let id = attempt(99);
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &id), T0)
            .await
            .unwrap();
        assert_eq!(outcome, ReserveOutcome::Refused(inflight_full()));
        assert_eq!(
            slots(&rig.store.inner, &rig.upload).await.len(),
            MAX_INFLIGHT_PARTS as usize
        );
    })
    .await;
}

/// **(C)** The same answer from `stage_part` (v3 sign-off): with every slot index held, staging
/// answers `Refused(SlowDown { InflightParts })`, never `Refused(NoSuchUpload)`, and writes
/// nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_stage_part_with_every_slot_held_answers_slow_down() {
    within(async {
        let mut rig = Rig::new().await;
        fill_every_slot(&rig).await;
        let before = snapshot(&rig.store.inner).await;
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        match outcome {
            Ok(StageOutcome::Refused(refusal)) => assert_eq!(refusal, inflight_full()),
            other => panic!("expected Refused(SlowDown), got {other:?}"),
        }
        assert_eq!(snapshot(&rig.store.inner).await, before, "nothing written");
        assert!(rig.log.commits(Kind::Intent).is_empty());
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

/// **(C)** A reserve against a session that is not `Open` — fenced by Abort, or absent — answers
/// `NoSuchUpload` and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_non_open_session_is_no_such_upload_writing_nothing() {
    within(async {
        let mut rig = Rig::new().await;
        fence(&rig.store.inner, &rig.upload).await;
        let before = snapshot(&rig.store.inner).await;
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
            .await
            .unwrap();
        assert_eq!(outcome, ReserveOutcome::Refused(Refusal::NoSuchUpload));
        let staging = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(10))]))
            .await;
        assert!(
            matches!(staging, Ok(StageOutcome::Refused(Refusal::NoSuchUpload))),
            "{staging:?}"
        );
        let (absent, _) = identity(77);
        let mut req = reserve_request(&rig, &rig.attempt);
        req.upload_id = &absent;
        let outcome = reserve_slot(&rig.store, &req, T0).await.unwrap();
        assert_eq!(outcome, ReserveOutcome::Refused(Refusal::NoSuchUpload));
        assert_eq!(snapshot(&rig.store.inner).await, before, "nothing written");
        assert!(rig.log.events().is_empty(), "no commit");
    })
    .await;
}

/// **(C)** A session that leaves `Open` between the reserve's session read and its commit (an
/// Abort in between) answers `NoSuchUpload`, not `SlowDown`, from `reserve_slot` and from
/// `stage_part`. The reserve's one commit conflicts and no slot of the attempt exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_session_leaving_open_under_the_reserve_is_no_such_upload() {
    for through_stage_part in [false, true] {
        within(async {
            let mut rig = Rig::new().await;
            let abort = Act::First(Side::Abort(rig.upload.clone()));
            rig.store.script(Kind::Reserve, vec![abort], None);
            let refusal = if through_stage_part {
                match rig
                    .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
                    .await
                {
                    Ok(StageOutcome::Refused(refusal)) => refusal,
                    other => panic!("expected Refused, got {other:?}"),
                }
            } else {
                match reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
                    .await
                    .unwrap()
                {
                    ReserveOutcome::Refused(refusal) => refusal,
                    other => panic!("expected Refused, got {other:?}"),
                }
            };
            assert_eq!(
                refusal,
                Refusal::NoSuchUpload,
                "stage_part {through_stage_part}"
            );
            let reserves = rig.log.commits(Kind::Reserve);
            assert_eq!(reserves.len(), 1);
            assert_eq!(reserves[0].answer, Answer::Conflict);
            assert_eq!(
                slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
                0
            );
            assert!(rig.log.commits(Kind::Intent).is_empty());
        })
        .await;
    }
}

/// **(C)** The same move on the pinned path, after an ambiguous outcome: the reserve's batch is
/// held (`may_still_commit: true`), and right after its slot read an Abort fences the session.
/// Whether the held batch stays out or lands just before the Abort, the reserve answers
/// `NoSuchUpload`, never `SlowDown`; a slot the held batch installed is residue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_the_pinned_reserve_answers_no_such_upload_once_the_session_left_open() {
    for lands in [false, true] {
        within(async {
            let rig = Rig::new().await;
            rig.store
                .script(Kind::Reserve, vec![Act::Hold { advance: 0 }], None);
            let index0 = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
            let mut then = Vec::new();
            if lands {
                then.push(Then::ReleaseHeld);
            }
            then.push(Then::Abort(rig.upload.clone()));
            rig.store.after_read(&index0, 0, then);
            let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
                .await
                .unwrap();
            assert_eq!(
                outcome,
                ReserveOutcome::Refused(Refusal::NoSuchUpload),
                "lands {lands}"
            );
            assert_eq!(rig.log.commits(Kind::Reserve).len(), 1, "no second probe");
            assert_eq!(
                slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
                usize::from(lands),
                "lands {lands}"
            );
        })
        .await;
    }
}

/// **(C)** A reserve whose batch lands but answers `CommitUnknownResult` — either
/// `may_still_commit` — adopts its own slot on re-read, with one commit. With a second live
/// attempt beside it and no residue, the held slots equal the live attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_landed_unknown_reserve_adopts_its_own_slot() {
    for may_still_commit in [false, true] {
        within(async {
            let rig = Rig::new().await;
            rig.store.script(
                Kind::Reserve,
                vec![Act::Unknown {
                    applied: true,
                    may_still_commit,
                }],
                None,
            );
            let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
                .await
                .unwrap();
            assert_eq!(
                outcome,
                ReserveOutcome::Reserved {
                    index: SlotIndex::new(0).unwrap(),
                    attempt_id: rig.attempt.clone()
                },
                "may_still_commit {may_still_commit}"
            );
            assert_eq!(
                rig.log.commits(Kind::Reserve).len(),
                1,
                "adopted, not re-reserved"
            );
            let other = attempt(5);
            let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &other), T0)
                .await
                .unwrap();
            assert!(matches!(outcome, ReserveOutcome::Reserved { .. }));
            assert_eq!(
                slots(&rig.store.inner, &rig.upload).await.len(),
                2,
                "two live attempts"
            );
            assert_eq!(
                slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
                1
            );
        })
        .await;
    }
}

/// **(C) The same-index rule.** Attempt X holds index 0. This reserve takes index 1 and its batch
/// is held (`may_still_commit: true`, absent on re-read); X releases index 0 before the retry.
/// The retry goes to index **1** again with the same attempt id, and once the held batch is
/// released this attempt holds exactly one slot. A reserve that took the lowest free index after
/// the ambiguous outcome would take 0 here and end with two.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_an_ambiguous_reserve_retries_the_same_index() {
    within(async {
        let rig = Rig::new().await;
        let x = attempt(1);
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &x), T0)
            .await
            .unwrap();
        assert!(matches!(outcome, ReserveOutcome::Reserved { index, .. } if index.get() == 0));
        let key = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        let x_slot = rig.store.inner.get(&key).await.unwrap().unwrap();
        let release: Hook = Arc::new(move |_| {
            WriteBatch::new()
                .require(key.clone(), x_slot.clone())
                .delete(key.clone())
        });
        rig.store.script(
            Kind::Reserve,
            vec![Act::HoldThen(Side::Batch(release))],
            None,
        );
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReserveOutcome::Reserved {
                index: SlotIndex::new(1).unwrap(),
                attempt_id: rig.attempt.clone()
            },
            "the retry pinned index 1, though index 0 was free"
        );
        assert_eq!(
            rig.log.commits(Kind::Reserve).len(),
            3,
            "X's, the held one, the retry"
        );
        assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            1,
            "exactly one slot of this attempt"
        );
    })
    .await;
}

/// **(C)** After [`RESERVE_AMBIGUOUS_ROUNDS`] ambiguous rounds the reserve refuses `SlowDown`,
/// after exactly that many commits. Then the held batch lands after the refusal, with
/// `MAX_INFLIGHT_PARTS − 1` other slots held: exactly one slot of the refused attempt exists, no
/// live attempt owns it, and it counts toward the cap — the next reserve is refused too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_held_reserve_landing_after_its_refusal_is_one_counted_slot() {
    within(async {
        let rig = Rig::new().await;
        let x = attempt(1);
        reserve_slot(&rig.store, &reserve_request(&rig, &x), T0)
            .await
            .unwrap();
        let mut plan = vec![Act::Hold { advance: 0 }];
        plan.extend((1..RESERVE_AMBIGUOUS_ROUNDS).map(|_| Act::Unknown {
            applied: false,
            may_still_commit: true,
        }));
        rig.store.script(Kind::Reserve, plan, None);
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReserveOutcome::Refused(inflight_full()),
            "refused after its ambiguous rounds"
        );
        let ours: Vec<_> = rig.log.commits(Kind::Reserve).into_iter().skip(1).collect();
        assert_eq!(
            ours.len(),
            RESERVE_AMBIGUOUS_ROUNDS as usize,
            "exactly the bound"
        );
        let pinned = slot_key(&rig.upload, SlotIndex::new(1).unwrap());
        assert!(
            ours.iter()
                .all(|c| c.batch.puts.iter().all(|(key, _)| *key == pinned)),
            "every round went to the one index"
        );
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            0
        );

        // Fill every other index, then the held batch lands.
        for index in 2..MAX_INFLIGHT_PARTS {
            let key = slot_key(&rig.upload, SlotIndex::new(index).unwrap());
            let put = WriteBatch::new().put(key, other_slot(u128::from(index), PART + 1));
            rig.store.inner.commit(put).await.unwrap();
        }
        assert_eq!(
            slots(&rig.store.inner, &rig.upload).await.len(),
            MAX_INFLIGHT_PARTS as usize - 1
        );
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            1,
            "exactly one slot of the refused attempt, never two"
        );
        assert_eq!(
            slots(&rig.store.inner, &rig.upload).await.len(),
            MAX_INFLIGHT_PARTS as usize
        );
        let next = attempt(50);
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &next), T0)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ReserveOutcome::Refused(inflight_full()),
            "the residue counts toward the cap"
        );
    })
    .await;
}

/// **(C)** A reserve whose held index is then taken by another attempt refuses rather than
/// probing elsewhere: a second slot for this attempt could land once that attempt released it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_held_index_taken_by_another_refuses() {
    within(async {
        let rig = Rig::new().await;
        let key = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        // Another attempt at the same part: only the attempt id tells it from this one's slot.
        let squat: Hook =
            Arc::new(move |_| WriteBatch::new().put(key.clone(), other_slot(9, PART)));
        rig.store
            .script(Kind::Reserve, vec![Act::HoldThen(Side::Batch(squat))], None);
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
            .await
            .unwrap();
        assert_eq!(outcome, ReserveOutcome::Refused(inflight_full()));
        assert_eq!(
            rig.log.commits(Kind::Reserve).len(),
            1,
            "no probe of another index"
        );
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            0
        );
    })
    .await;
}

/// **(C)** A session moved to a later `Open` epoch under the reserve (a Complete fence and its
/// release): the reserve is answered from the new state and reserves under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_session_moved_to_a_later_open_epoch_is_reserved_under() {
    within(async {
        let rig = Rig::new().await;
        let later = open_at(&rig.session, 2);
        let side = move_session(&rig.upload, later.clone());
        rig.store
            .script(Kind::Reserve, vec![Act::First(side)], None);
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
            .await
            .unwrap();
        assert!(
            matches!(outcome, ReserveOutcome::Reserved { .. }),
            "{outcome:?}"
        );
        let reserves = rig.log.commits(Kind::Reserve);
        assert_eq!(reserves.len(), 2);
        assert_eq!(reserves[0].answer, Answer::Conflict);
        let session = mpu_key(&rig.upload);
        assert!(reserves[1]
            .batch
            .preconditions
            .iter()
            .any(|p| p.key == session && p.expected.as_ref() == Some(&later)));
    })
    .await;
}

/// **(C)** A pinned index is not dropped on a newer session epoch alone (iteration-1 finding). A
/// held reserve at index 0 lands **after** its slot read found the index empty and **before** the
/// session moves to a later `Open` epoch, so the session read that follows finds the session
/// moved. The reserve reads the index once more before letting it go, finds its own slot, and
/// keeps it under the new session: one reserve commit, one slot of this attempt, and the intent
/// fenced on the new session bytes lands first time. A reserve that unpinned on seeing the
/// session move would probe index 1 and hold two slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_held_reserve_landing_before_the_session_moves_keeps_its_one_slot() {
    within(async {
        let mut rig = Rig::new().await;
        let later = open_at(&rig.session, 2);
        rig.store
            .script(Kind::Reserve, vec![Act::Hold { advance: 0 }], None);
        let index0 = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        let move_session = WriteBatch::new().put(mpu_key(&rig.upload), later.clone());
        rig.store.after_read(
            &index0,
            0,
            vec![Then::ReleaseHeld, Then::Commit(move_session)],
        );
        let body = body_bytes(CHUNK);
        let attempt = staged(rig.stage(|_| {}, stream_of(vec![Bytes::from(body)])).await);
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            1,
            "exactly one slot of this attempt"
        );
        assert_eq!(
            attempt.slot_index().get(),
            0,
            "the index its held batch took"
        );
        assert_eq!(rig.log.commits(Kind::Reserve).len(), 1, "no second probe");
        assert_eq!(attempt.session_bytes(), &later, "fenced on the new session");
        let intents = rig.log.commits(Kind::Intent);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].answer, Answer::Committed);
    })
    .await;
}

/// **(C)** The index a held reserve may still install is found holding bytes that are not a
/// slot record: a typed record error (ADR-0045) that keeps the held batch's doubt, never read as
/// "another attempt's slot", never adopted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_an_undecodable_slot_at_the_reserved_index_is_a_typed_error() {
    within(async {
        let rig = Rig::new().await;
        let key = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        let garbage: Hook =
            Arc::new(move |_| WriteBatch::new().put(key.clone(), b"not a slot".to_vec()));
        rig.store.script(
            Kind::Reserve,
            vec![Act::HoldThen(Side::Batch(garbage))],
            None,
        );
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0).await;
        let Err(StageError::InDoubt(doubt)) = outcome else {
            panic!("expected InDoubt, got {outcome:?}");
        };
        assert!(doubt.may_still_commit, "the held batch's doubt is kept");
        assert!(matches!(doubt.cause, Stop::Record(_)), "{:?}", doubt.cause);
        assert_eq!(rig.log.commits(Kind::Reserve).len(), 1);
    })
    .await;
}

/// One schedule of a read-fault leg: what the commits of one kind do, which read fails (its key
/// and how many earlier reads of that key pass), how many commits of that kind are made, and
/// what the held batch meets when the test releases it afterwards.
struct FaultSchedule {
    name: &'static str,
    plan: Vec<Act>,
    fails: Option<(Vec<u8>, usize)>,
    commits: usize,
    released: CommitOutcome,
}

/// Check the end state of a read-fault schedule: the doubt is the held batch's, the scripted read
/// fault fired, and the held batch, released afterwards, meets what the schedule says — landing
/// shows the doubt was real.
fn assert_fault_doubt(schedule: &FaultSchedule, doubt: &InDoubt, log: &Log) {
    let name = schedule.name;
    assert!(doubt.may_still_commit, "{name}: the held batch's doubt");
    assert!(
        doubt.outcome.may_still_commit,
        "{name}: the report kept is the held batch's, not a later out-of-flight one"
    );
    assert!(
        matches!(doubt.cause, Stop::Store(_)),
        "{name}: {:?}",
        doubt.cause
    );
    if let Some((key, _)) = &schedule.fails {
        assert!(
            log.has(|e| matches!(e, Event::ReadFault { key: k } if k == key)),
            "{name}: the scripted read fault fired"
        );
    }
}

/// **(C)** A store read (or commit) that fails while a reserve batch is in doubt keeps the doubt
/// (review finding): the reserve answers `InDoubt`, classed `Indeterminate`, carrying the held
/// batch's `may_still_commit: true` — never a plain stop that says nothing is in doubt, though
/// the fault itself is transient. Each schedule holds the reserve's first batch at index 0, then
/// fails a different step: the slot read after a same-index retry answered an out-of-flight
/// unknown (both doubts are live, and the held one must win); the session read after the held
/// round's slot read; the second read of the pinned index after the session moved; the retry's
/// own commit. Releasing the held batch afterwards lands it (or, after the session moved,
/// conflicts), so the doubt was real.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_reserve_read_that_fails_while_a_batch_is_in_doubt_keeps_the_doubt() {
    for at in 0..4 {
        within(async {
            let rig = Rig::new().await;
            let index0 = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
            let session = mpu_key(&rig.upload);
            let held = Act::Hold { advance: 0 };
            let out_of_flight = Act::Unknown {
                applied: false,
                may_still_commit: false,
            };
            let moved = move_session(&rig.upload, open_at(&rig.session, 2));
            let schedule = [
                FaultSchedule {
                    name: "the retry's slot read, after an out-of-flight unknown",
                    plan: vec![held.clone(), out_of_flight],
                    fails: Some((index0.clone(), 1)),
                    commits: 2,
                    released: CommitOutcome::Committed,
                },
                FaultSchedule {
                    name: "the session read after the held round's slot read",
                    plan: vec![held.clone()],
                    fails: Some((session, 1)),
                    commits: 1,
                    released: CommitOutcome::Committed,
                },
                FaultSchedule {
                    name: "the pinned index's second read, after the session moved",
                    plan: vec![Act::HoldThen(moved)],
                    fails: Some((index0, 1)),
                    commits: 1,
                    released: CommitOutcome::Conflict,
                },
                FaultSchedule {
                    name: "the same-index retry's commit",
                    plan: vec![held, Act::Fail],
                    fails: None,
                    commits: 2,
                    released: CommitOutcome::Committed,
                },
            ]
            .into_iter()
            .nth(at)
            .unwrap();
            rig.store.script(Kind::Reserve, schedule.plan.clone(), None);
            if let Some((key, skip)) = &schedule.fails {
                rig.store.after_read(key, *skip, vec![Then::Fault]);
            }
            let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0).await;
            let doubt = in_doubt(outcome);
            assert_fault_doubt(&schedule, &doubt, &rig.log);
            let name = schedule.name;
            assert_eq!(
                rig.log.commits(Kind::Reserve).len(),
                schedule.commits,
                "{name}"
            );
            assert_eq!(
                slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
                0,
                "{name}: nothing landed yet"
            );
            assert_eq!(rig.store.release_held().await, schedule.released, "{name}");
            assert_eq!(
                slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
                usize::from(schedule.released == CommitOutcome::Committed),
                "{name}: one slot at most"
            );
        })
        .await;
    }
}

// ═══════════════════════ (D) leases ═══════════════════════

/// The lease a batch puts on `key`.
fn lease_put(batch: &WriteBatch, key: &[u8]) -> u64 {
    let (_, value) = batch
        .puts
        .iter()
        .find(|(k, _)| k == key)
        .expect("the batch puts the key");
    if key.starts_with(SLOT_PREFIX) {
        decode_slot_record(value).unwrap().lease_expiry_millis()
    } else {
        decode_owned_entry(key, value)
            .unwrap()
            .2
            .lease_expiry_millis()
    }
}

/// **(D)** The reserve writes the slot lease at `now + TTL`. Each intent writes its owned entry's
/// lease and rewrites the slot with its lease moved forward, in the same batch, both at its own
/// reading plus a TTL — or a millisecond past the slot lease it replaces, when two intents share
/// a reading. No lease staging writes is earlier than the slot lease it replaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_the_reserve_and_every_intent_write_their_leases_forward() {
    within(async {
        let mut rig = Rig::new().await;
        // Chunks 1 and 2 are registered at T0, chunk 3 at T0 + 100.
        rig.store.script(
            Kind::Intent,
            vec![Act::Pass, Act::PassThenAdvance(100)],
            None,
        );
        let body = body_bytes(3 * CHUNK);
        let attempt = staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK])))
                .await,
        );
        let reserve = &rig.log.commits(Kind::Reserve)[0];
        let slot = attempt.slot_key().to_vec();
        assert_eq!(lease_put(&reserve.batch, &slot), T0 + TTL, "now + TTL");
        let intents = rig.log.commits(Kind::Intent);
        let expected = [T0 + TTL + 1, T0 + TTL + 2, T0 + 100 + TTL];
        let mut replaced = T0 + TTL;
        for (intent, lease) in intents.iter().zip(expected) {
            assert_eq!(intent.answer, Answer::Committed);
            let entry = intent_put(&intent.batch).0;
            assert_eq!(
                decode_slot_record(&slot_required(&intent.batch))
                    .unwrap()
                    .lease_expiry_millis(),
                replaced,
                "the slot lease it replaces"
            );
            assert_eq!(
                lease_put(&intent.batch, &slot),
                lease,
                "the slot's new lease"
            );
            assert_eq!(lease_put(&intent.batch, &entry), lease, "the owned lease");
            assert!(lease > replaced, "never earlier than the lease it replaces");
            assert!(
                lease >= intent.clock + TTL,
                "at least a TTL past its reading"
            );
            replaced = lease;
        }
        assert_eq!(attempt.slot().lease_expiry_millis(), T0 + 100 + TTL);
        let leases: Vec<u64> = attempt
            .owned()
            .iter()
            .map(|held| held.entry().lease_expiry_millis())
            .collect();
        assert_eq!(leases, expected);
    })
    .await;
}

/// The cause of a staging that stopped on a lapsed lease: the key, its expiry, the reading.
fn lapsed(cause: &Stop) -> (Vec<u8>, u64, u64) {
    match cause {
        Stop::LeaseLapsed {
            key,
            lease_expiry_millis,
            now_millis,
        } => (key.clone().into_bytes(), *lease_expiry_millis, *now_millis),
        other => panic!("expected LeaseLapsed, got {other:?}"),
    }
}

/// **(D)** The slot's lease lapses while the first intent reads the drain records. The intent
/// checks every lease at its own reading, taken after that read, and refuses: no intent is sent,
/// so the lapsed slot lease is never moved forward, and no fragment is written. An intent that
/// stamped a fresh lease without the check would revive the slot and stage the part.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_an_intent_refuses_a_slot_lease_that_lapsed_during_its_drain_read() {
    within(async {
        let mut rig = Rig::new().await;
        // The first drain read is staging's pre-check; the second is the first intent's.
        rig.store.after_read(
            metadata::DESIRED_DSERVER_PREFIX,
            1,
            vec![Then::Advance(TTL)],
        );
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let err = outcome.expect_err("staging stopped");
        let text = err.to_string();
        let reserved = rig.log.commits(Kind::Reserve).remove(0).batch.puts[0].clone();
        let slot = String::from_utf8(reserved.0.clone()).unwrap();
        for part in [
            rig.upload.as_str(),
            slot.as_str(),
            "is not revived",
            "no batch of this part attempt is in doubt",
        ] {
            assert!(text.contains(part), "{part:?} in {text:?}");
        }
        let cause = stopped(Err(err));
        assert_eq!(lapsed(&cause), (reserved.0.clone(), T0 + TTL, T0 + TTL));
        assert!(
            rig.log.commits(Kind::Intent).is_empty(),
            "no intent was sent"
        );
        assert!(rig.log.puts().is_empty());
        assert_eq!(
            rig.store.inner.get(&reserved.0).await.unwrap(),
            Some(reserved.1),
            "the slot is exactly as reserved"
        );
    })
    .await;
}

/// **(D)** Chunk 1's owned lease lapses while chunk 3's intent reads the drain records; the slot's
/// lease, moved forward by chunk 2's intent, has not. Chunk 3's intent checks chunk 1's lease too
/// and refuses before it is sent. An intent that checked only the slot's lease would land chunk 3
/// beside a dead entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_an_intent_refuses_an_owned_lease_that_lapsed_during_its_drain_read() {
    within(async {
        let mut rig = Rig::new().await;
        // Chunk 1 at T0 (leases T0 + TTL + 1), chunk 2 at T0 + 1000.
        rig.store
            .script(Kind::Intent, vec![], Some(Act::PassThenAdvance(1_000)));
        // Pre-check, chunk 1's intent, chunk 2's, then chunk 3's: the clock reaches chunk 1's
        // lease during that last read.
        rig.store.after_read(
            metadata::DESIRED_DSERVER_PREFIX,
            3,
            vec![Then::Advance(TTL + 1 - 2_000)],
        );
        let body = body_bytes(3 * CHUNK);
        let outcome = rig
            .stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK])))
            .await;
        let cause = stopped(outcome);
        let chunk1 = sidx_key(&rig.upload, part(), 1);
        assert_eq!(lapsed(&cause), (chunk1, T0 + TTL + 1, T0 + TTL + 1));
        assert!(
            rig.log.intents_of(3).is_empty(),
            "chunk 3's intent was never sent"
        );
        assert!(rig.log.puts().iter().all(|put| put.id.chunk != 3));
        assert_eq!(
            owned(&rig.store.inner, &rig.upload).await[&1].lease_expiry_millis(),
            T0 + TTL + 1,
            "chunk 1's lease was not revived"
        );
    })
    .await;
}

/// **(D)** An intent round is lost — answered `Conflict` unapplied, or held — slowly enough that
/// the slot's lease lapses before the retry. The retry checks the leases at its own fresh reading
/// and refuses before it is sent: one intent commit. Lost to a conflict, nothing is in doubt; held,
/// the error keeps the held batch's `may_still_commit` doubt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_an_intent_retry_refuses_a_lease_that_lapsed_between_rounds() {
    for held in [false, true] {
        within(async {
            let mut rig = Rig::new().await;
            let lost = if held {
                Act::Hold { advance: TTL }
            } else {
                Act::ConflictThenAdvance(TTL)
            };
            rig.store.script(Kind::Intent, vec![lost], None);
            let outcome = rig
                .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
                .await;
            let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
            let cause = match (held, outcome) {
                (false, outcome) => stopped(outcome),
                (true, Err(StageError::InDoubt(doubt))) => {
                    assert!(doubt.may_still_commit, "the held batch's doubt is kept");
                    doubt.cause
                }
                (true, other) => panic!("expected InDoubt, got {other:?}"),
            };
            assert_eq!(lapsed(&cause), (slot, T0 + TTL, T0 + TTL), "held {held}");
            assert_eq!(rig.log.commits(Kind::Intent).len(), 1, "no retry was sent");
            assert!(rig.log.puts().is_empty());
        })
        .await;
    }
}

// ═══════════════════════ (E) unknown outcomes ═══════════════════════

/// **(E)** An intent whose batch lands but answers `CommitUnknownResult` (either
/// `may_still_commit`) is recognised by re-reading the owned entry's exact bytes: one commit, no
/// second entry for the chunk, and its fragments go to the placement that landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_a_landed_unknown_intent_is_adopted_from_its_exact_bytes() {
    for may_still_commit in [false, true] {
        within(async {
            let mut rig = Rig::new().await;
            rig.store.script(
                Kind::Intent,
                vec![Act::Unknown {
                    applied: true,
                    may_still_commit,
                }],
                None,
            );
            let body = body_bytes(3 * CHUNK);
            let attempt = staged(
                rig.stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK])))
                    .await,
            );
            assert_eq!(rig.log.intents_of(1).len(), 1, "adopted, not re-sent");
            let entries = owned(&rig.store.inner, &rig.upload).await;
            assert_eq!(entries.len(), 3, "one owned entry per chunk");
            let landed = intent_entry(&rig.log.intents_of(1)[0].batch);
            assert_eq!(entries[&1], landed);
            assert_eq!(attempt.chunks()[0].placement, landed.staged().placement());
            assert_fragments_evidenced(&rig).await;
        })
        .await;
    }
}

/// **(E)** An intent answered `CommitUnknownResult { may_still_commit: false }` and not applied is
/// retried; staging ends with exactly one owned entry per chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_an_out_of_flight_unknown_intent_that_did_not_land_is_retried() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(
            Kind::Intent,
            vec![Act::Unknown {
                applied: false,
                may_still_commit: false,
            }],
            None,
        );
        let body = body_bytes(3 * CHUNK);
        staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK])))
                .await,
        );
        assert_eq!(rig.log.intents_of(1).len(), 2, "retried once");
        assert_eq!(owned(&rig.store.inner, &rig.upload).await.len(), 3);
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

/// **(E) (i)** An intent is held (`may_still_commit: true`) and the clock moves on; the held
/// batch is released just before the attempt's next batch, which therefore conflicts. The
/// re-read finds the held batch's exact entry: it is adopted — its placement, and its own
/// `t_auth` for the deadline — and no fragment was written before that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_i_a_held_intent_released_before_the_next_batch_is_adopted() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(
            Kind::Intent,
            vec![Act::Hold { advance: 500 }, Act::ReleaseHeldFirst],
            None,
        );
        let body = body_bytes(2 * CHUNK);
        staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[2 * CHUNK])))
                .await,
        );
        let intents = rig.log.intents_of(1);
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[1].answer, Answer::Conflict);
        let held = intent_put(&intents[0].batch);
        assert_eq!(
            rig.store.inner.get(&held.0).await.unwrap(),
            Some(held.1),
            "the held batch's entry is the durable one"
        );
        for put in rig.log.puts().iter().filter(|p| p.id.chunk == 1) {
            assert_eq!(
                put.deadline,
                Some(intents[0].clock + W),
                "the held batch's t_auth"
            );
        }
        assert_intents_precede_puts(&rig.log);
        assert_fragments_evidenced(&rig).await;
        assert_eq!(owned(&rig.store.inner, &rig.upload).await.len(), 2);
    })
    .await;
}

/// **(E) (ii)** Chunk 2's intent is held, and every later batch is answered `Conflict` unapplied
/// until the rounds run out: no fragment of chunk 2 is written on the strength of an absent read,
/// and staging returns an in-doubt error that keeps `may_still_commit`. The held batch then lands,
/// and every fragment written is named by a durable owned entry with its placement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_ii_a_held_intent_that_never_settles_ends_in_doubt() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(
            Kind::Intent,
            vec![Act::Pass, Act::Hold { advance: 0 }],
            Some(Act::Conflict),
        );
        let body = body_bytes(3 * CHUNK);
        let outcome = rig.stage(|_| {}, stream_of(pieces(&body, &[3 * CHUNK]))).await;
        let err = match outcome {
            Err(err @ StageError::InDoubt(_)) => err,
            other => panic!("expected InDoubt, got {other:?}"),
        };
        assert_eq!(classify(&err), ErrorClass::Indeterminate, "{err}");
        let text = err.to_string();
        let spent = format!("chunk 2's intent spent all {INTENT_ROUNDS} rounds");
        for part in [
            rig.upload.as_str(),
            "is in doubt",
            "may still be applied",
            spent.as_str(),
        ] {
            assert!(text.contains(part), "{part:?} in {text:?}");
        }
        let StageError::InDoubt(doubt) = err else {
            unreachable!()
        };
        assert!(doubt.may_still_commit, "the doubt is kept");
        assert!(
            matches!(doubt.cause, Stop::IntentRoundsSpent { chunk: 2, rounds } if rounds == INTENT_ROUNDS),
            "{:?}",
            doubt.cause
        );
        assert_eq!(rig.log.intents_of(2).len(), INTENT_ROUNDS as usize);
        // Every later batch required the slot at the bytes the held batch required, so at most one
        // of them could land.
        let required: BTreeSet<Bytes> = rig
            .log
            .intents_of(2)
            .iter()
            .map(|intent| slot_required(&intent.batch))
            .collect();
        assert_eq!(required.len(), 1, "one required slot value across every round");
        assert!(
            rig.log.puts().iter().all(|p| p.id.chunk != 2),
            "no fragment of chunk 2 was written"
        );
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
        assert_fragments_evidenced(&rig).await;
        assert_eq!(owned(&rig.store.inner, &rig.upload).await.len(), 2);
    })
    .await;
}

/// **(E)** An intent answered `CommitUnknownResult { may_still_commit: false }`, not applied, and
/// then answered `Conflict` until its rounds run out: the read after the unknown outcome settled
/// it (the batch is out of flight and its entry absent), so the error says nothing is in doubt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_an_out_of_flight_unknown_a_read_settled_is_not_reported_in_doubt() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(
            Kind::Intent,
            vec![Act::Unknown {
                applied: false,
                may_still_commit: false,
            }],
            Some(Act::Conflict),
        );
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(cause, Stop::IntentRoundsSpent { chunk: 1, rounds } if rounds == INTENT_ROUNDS),
            "{cause:?}"
        );
        assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
    })
    .await;
}

/// **(E)** An intent held (`may_still_commit: true`) while the session moves on to a later `Open`
/// epoch: the held batch required the old session bytes, so it can never land, and once the
/// rounds run out the error says nothing is in doubt. Released afterwards, the held batch indeed
/// conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_a_held_intent_whose_session_moved_on_is_no_longer_in_doubt() {
    within(async {
        let mut rig = Rig::new().await;
        let later = open_at(&rig.session, 2);
        let side = move_session(&rig.upload, later);
        rig.store
            .script(Kind::Intent, vec![Act::HoldThen(side)], Some(Act::Conflict));
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(cause, Stop::IntentRoundsSpent { chunk: 1, rounds } if rounds == INTENT_ROUNDS),
            "{cause:?}"
        );
        assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
        assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

/// **(E)** A session moved to a later `Open` epoch under an intent: the intent is answered from
/// the new state and lands under it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_an_intent_under_a_session_moved_to_a_later_open_epoch_lands_under_it() {
    within(async {
        let mut rig = Rig::new().await;
        let later = open_at(&rig.session, 2);
        let side = move_session(&rig.upload, later.clone());
        rig.store.script(Kind::Intent, vec![Act::First(side)], None);
        let body = body_bytes(2 * CHUNK);
        let attempt = staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[2 * CHUNK])))
                .await,
        );
        assert_eq!(attempt.session_bytes(), &later);
        let intents = rig.log.intents_of(1);
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].answer, Answer::Conflict);
        assert_eq!(intents[1].answer, Answer::Committed);
    })
    .await;
}

/// **(E)** A fresh chunk's `sidx:` key already holding bytes no batch of this attempt put is a
/// typed error, never adopted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_a_chunk_id_already_taken_is_a_typed_error() {
    within(async {
        let mut rig = Rig::new().await;
        let key = sidx_key(&rig.upload, part(), 1);
        rig.store
            .inner
            .commit(WriteBatch::new().put(key.clone(), b"someone else".to_vec()))
            .await
            .unwrap();
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(&cause, Stop::ChunkIdTaken { key: named } if named.as_bytes() == key.as_slice()),
            "{cause:?}"
        );
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

/// **(E)** The slot is deleted from under a live attempt (the session unchanged) just before its
/// first intent commits: the intent conflicts and staging stops with a typed error naming the
/// slot, writing no owned entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_a_slot_lost_under_an_unchanged_session_is_a_typed_error() {
    within(async {
        let mut rig = Rig::new().await;
        let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        let named = slot.clone();
        let delete: Hook = Arc::new(move |_| WriteBatch::new().delete(slot.clone()));
        rig.store
            .script(Kind::Intent, vec![Act::First(Side::Batch(delete))], None);
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(&cause, Stop::SlotLost { key } if key.as_bytes() == named.as_slice()),
            "{cause:?}"
        );
        assert_eq!(rig.log.commits(Kind::Intent).len(), 1);
        assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

/// **(E)** Chunk 1's intent is held, and lands **after** its entry read found nothing and
/// **before** its slot read (iteration-1 finding). The slot then holds the value only that batch
/// puts, so the intent reads its entry again and adopts the batch: no retry, no `SlotLost`, its
/// fragments go to the placement that landed under its own `t_auth`, and chunk 2's intent requires
/// the slot that batch put.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_a_held_intent_landing_between_its_entry_and_slot_reads_is_adopted() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store
            .script(Kind::Intent, vec![Act::Hold { advance: 0 }], None);
        let entry = sidx_key(&rig.upload, part(), 1);
        rig.store.after_read(&entry, 0, vec![Then::ReleaseHeld]);
        let body = body_bytes(2 * CHUNK);
        let attempt = staged(
            rig.stage(|_| {}, stream_of(pieces(&body, &[2 * CHUNK])))
                .await,
        );
        let intents = rig.log.intents_of(1);
        assert_eq!(intents.len(), 1, "adopted, not re-sent");
        let held = intent_put(&intents[0].batch);
        assert_eq!(rig.store.inner.get(&held.0).await.unwrap(), Some(held.1));
        assert_eq!(
            attempt.chunks()[0].placement,
            intent_entry(&intents[0].batch).staged().placement()
        );
        for put in rig.log.puts().iter().filter(|p| p.id.chunk == 1) {
            assert_eq!(
                put.deadline,
                Some(intents[0].clock + W),
                "the held batch's t_auth"
            );
        }
        let held_slot = intents[0]
            .batch
            .puts
            .iter()
            .find(|(key, _)| key.starts_with(SLOT_PREFIX))
            .unwrap()
            .1
            .clone();
        assert_eq!(
            slot_required(&rig.log.intents_of(2)[0].batch),
            held_slot,
            "chunk 2 is fenced on the slot the adopted batch put"
        );
        assert_intents_precede_puts(&rig.log);
        assert_fragments_evidenced(&rig).await;
        assert_eq!(owned(&rig.store.inner, &rig.upload).await.len(), 2);
    })
    .await;
}

/// **(E)** Chunk 1's intent loses its first rounds and its **last** round is held. That batch
/// lands after its slot read and just before the session moves to a later `Open` epoch, so the
/// session read finds the session moved. The intent reads its entry again before letting the
/// doubt go, adopts the batch that landed, and writes the chunk under the new session — never
/// "rounds spent, nothing in doubt" for a chunk whose entry is durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_a_held_intent_landing_before_the_session_moves_is_adopted() {
    within(async {
        let mut rig = Rig::new().await;
        let later = open_at(&rig.session, 2);
        let mut plan = vec![Act::Conflict; INTENT_ROUNDS as usize - 1];
        plan.push(Act::Hold { advance: 0 });
        rig.store.script(Kind::Intent, plan, None);
        let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        let move_session = WriteBatch::new().put(mpu_key(&rig.upload), later.clone());
        // Each lost round reads the slot once; fire after the last round's slot read.
        rig.store.after_read(
            &slot,
            INTENT_ROUNDS as usize - 1,
            vec![Then::ReleaseHeld, Then::Commit(move_session)],
        );
        let attempt = staged(
            rig.stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
                .await,
        );
        let intents = rig.log.intents_of(1);
        assert_eq!(
            intents.len(),
            INTENT_ROUNDS as usize,
            "no round past the bound"
        );
        let landed = intent_put(&intents[INTENT_ROUNDS as usize - 1].batch);
        assert_eq!(
            rig.store.inner.get(&landed.0).await.unwrap(),
            Some(landed.1)
        );
        assert_eq!(attempt.session_bytes(), &later, "fenced on the new session");
        assert_eq!(attempt.chunks().len(), 1);
        assert_intents_precede_puts(&rig.log);
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

/// **(E)** As in `e_a_held_intent_landing_between_its_entry_and_slot_reads_is_adopted`, but chunk
/// 1's entry is deleted right after its held intent lands, before the intent reads it again. The
/// slot proves the batch landed and the entry is gone: a typed error naming the entry, nothing in
/// doubt, and no fragment of the chunk written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_an_entry_removed_after_its_intent_landed_is_a_typed_error() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store
            .script(Kind::Intent, vec![Act::Hold { advance: 0 }], None);
        let entry = sidx_key(&rig.upload, part(), 1);
        let remove = WriteBatch::new().delete(entry.clone());
        rig.store
            .after_read(&entry, 0, vec![Then::ReleaseHeld, Then::Commit(remove)]);
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let err = match outcome {
            Err(err @ StageError::Stopped { .. }) => err,
            other => panic!("expected Stopped, got {other:?}"),
        };
        let text = err.to_string();
        let named = String::from_utf8(entry.clone()).unwrap();
        for part in [
            named.as_str(),
            "removed or rewritten after its intent landed",
            "no batch of this part attempt is in doubt",
        ] {
            assert!(text.contains(part), "{part:?} in {text:?}");
        }
        let cause = stopped(Err(err));
        assert!(
            matches!(&cause, Stop::EntryLost { key } if key.as_bytes() == entry.as_slice()),
            "{cause:?}"
        );
        assert_eq!(rig.log.commits(Kind::Intent).len(), 1);
        assert!(
            rig.log.puts().is_empty(),
            "no fragment without a durable entry"
        );
    })
    .await;
}

/// **(E)** A store read (or commit) that fails while an intent batch is in doubt keeps the doubt
/// (review finding): staging answers `InDoubt`, classed `Indeterminate`, carrying the held batch's
/// `may_still_commit: true`, and writes no fragment of the chunk. Each schedule holds chunk 1's
/// first intent, then fails a different step. The first is the finding's own: the retry answers
/// `CommitUnknownResult { may_still_commit: false }` unapplied and the entry read after it fails,
/// so both doubts are live and the held one must win. Then: the held round's slot read; its
/// session read; the next round's drain read; the entry's second read after the session moved;
/// the retry's own commit. Releasing the held batch afterwards lands it (or, after the session
/// moved, conflicts), so the doubt was real.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_an_intent_read_that_fails_while_a_batch_is_in_doubt_keeps_the_doubt() {
    for at in 0..6 {
        within(async {
            let mut rig = Rig::new().await;
            let entry = sidx_key(&rig.upload, part(), 1);
            let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
            let session = mpu_key(&rig.upload);
            let drains = metadata::DESIRED_DSERVER_PREFIX.to_vec();
            let held = Act::Hold { advance: 0 };
            let out_of_flight = Act::Unknown {
                applied: false,
                may_still_commit: false,
            };
            let moved = move_session(&rig.upload, open_at(&rig.session, 2));
            let schedule = [
                FaultSchedule {
                    name: "the retry's entry read, after an out-of-flight unknown",
                    plan: vec![held.clone(), out_of_flight],
                    fails: Some((entry.clone(), 1)),
                    commits: 2,
                    released: CommitOutcome::Committed,
                },
                FaultSchedule {
                    name: "the held round's slot read",
                    plan: vec![held.clone()],
                    fails: Some((slot, 0)),
                    commits: 1,
                    released: CommitOutcome::Committed,
                },
                FaultSchedule {
                    // The first session read is staging's own, before the reserve.
                    name: "the held round's session read",
                    plan: vec![held.clone()],
                    fails: Some((session, 1)),
                    commits: 1,
                    released: CommitOutcome::Committed,
                },
                FaultSchedule {
                    // Staging's pre-check, the held round's, then the next round's.
                    name: "the next round's drain read",
                    plan: vec![held.clone()],
                    fails: Some((drains, 2)),
                    commits: 1,
                    released: CommitOutcome::Committed,
                },
                FaultSchedule {
                    name: "the entry's second read, after the session moved",
                    plan: vec![Act::HoldThen(moved)],
                    fails: Some((entry.clone(), 1)),
                    commits: 1,
                    released: CommitOutcome::Conflict,
                },
                FaultSchedule {
                    name: "the retry's commit",
                    plan: vec![held, Act::Fail],
                    fails: None,
                    commits: 2,
                    released: CommitOutcome::Committed,
                },
            ]
            .into_iter()
            .nth(at)
            .unwrap();
            rig.store.script(Kind::Intent, schedule.plan.clone(), None);
            if let Some((key, skip)) = &schedule.fails {
                rig.store.after_read(key, *skip, vec![Then::Fault]);
            }
            let outcome = rig
                .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
                .await;
            let doubt = in_doubt(outcome);
            assert_fault_doubt(&schedule, &doubt, &rig.log);
            let name = schedule.name;
            assert_eq!(rig.log.intents_of(1).len(), schedule.commits, "{name}");
            assert!(rig.log.puts().is_empty(), "{name}: no fragment written");
            assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
            assert_eq!(rig.store.release_held().await, schedule.released, "{name}");
            assert_eq!(
                rig.store.inner.get(&entry).await.unwrap().is_some(),
                schedule.released == CommitOutcome::Committed,
                "{name}: the held batch's entry"
            );
            assert_fragments_evidenced(&rig).await;
        })
        .await;
    }
}

// ═══════════════════════ (E′) the intent's own session fence ═══════════════════════

/// **E′1.** The reserve succeeds; the wrapper runs a real Abort (#895) immediately before chunk
/// `n`'s intent commits, for `n` 1 and 3. The Abort rewrites only the session and its obligation
/// (`crates/core/src/multipart/abort.rs`), so the intent's slot precondition still holds and only
/// its own `require(mpu == Open@E)` can stop it. The intent conflicts, staging answers
/// `NoSuchUpload`, and no owned entry or fragment of chunk `n` exists; earlier chunks' entries
/// stay, as residue for the reaper.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_prime_1_an_abort_just_before_an_intent_commits_stops_it() {
    for chunk in [1u128, 3] {
        within(async {
            let mut rig = Rig::new().await;
            let mut plan = vec![Act::Pass; chunk as usize - 1];
            plan.push(Act::First(Side::Abort(rig.upload.clone())));
            rig.store.script(Kind::Intent, plan, None);
            let body = body_bytes(4 * CHUNK);
            let outcome = rig
                .stage(|_| {}, stream_of(pieces(&body, &[4 * CHUNK])))
                .await;
            assert!(
                matches!(outcome, Ok(StageOutcome::Refused(Refusal::NoSuchUpload))),
                "chunk {chunk}: {outcome:?}"
            );
            assert!(matches!(rig.state().await, SessionState::Aborting {}));
            let intents = rig.log.intents_of(chunk);
            assert_eq!(intents.len(), 1, "chunk {chunk}: one intent, no retry");
            assert_eq!(intents[0].answer, Answer::Conflict);
            let entries = owned(&rig.store.inner, &rig.upload).await;
            assert!(
                !entries.contains_key(&chunk),
                "chunk {chunk}: no owned entry"
            );
            assert_eq!(
                entries.keys().copied().collect::<Vec<_>>(),
                (1..chunk).collect::<Vec<_>>(),
                "earlier chunks' entries stay"
            );
            assert!(
                rig.log.puts().iter().all(|p| p.id.chunk != chunk),
                "chunk {chunk}: no fragment"
            );
            assert!((0..FRAGMENTS)
                .all(|index| rig.fleet.stored_on(FragmentId { chunk, index }).is_none()));
            assert_fragments_evidenced(&rig).await;
        })
        .await;
    }
}

/// **E′2.** An intent held (`may_still_commit: true`) and not applied; the wrapper then runs a
/// real Abort. Staging re-reads, finds the session no longer `Open`, and stops with
/// `NoSuchUpload`: that batch required the `Open` bytes, so it can no longer land and nothing is
/// in doubt. Released to the inner store afterwards, the held batch conflicts, and no owned entry
/// for the chunk exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_prime_2_a_held_intent_then_an_abort_stops_with_nothing_in_doubt() {
    within(async {
        let mut rig = Rig::new().await;
        let abort = Act::HoldThen(Side::Abort(rig.upload.clone()));
        rig.store.script(Kind::Intent, vec![abort], None);
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        assert!(
            matches!(outcome, Ok(StageOutcome::Refused(Refusal::NoSuchUpload))),
            "{outcome:?}"
        );
        assert_eq!(rig.log.commits(Kind::Intent).len(), 1);
        assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
        assert!(owned(&rig.store.inner, &rig.upload).await.is_empty());
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

// ═══════════════════════ (F) every loop is bounded ═══════════════════════

/// **(F)** Every reserve answered `Conflict` unapplied: the reserve probes each index once —
/// exactly `MAX_INFLIGHT_PARTS` commits — and refuses `SlowDown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_the_reserve_probe_is_bounded() {
    within(async {
        let rig = Rig::new().await;
        rig.store.script(Kind::Reserve, vec![], Some(Act::Conflict));
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
            .await
            .unwrap();
        assert_eq!(outcome, ReserveOutcome::Refused(inflight_full()));
        let reserves = rig.log.commits(Kind::Reserve);
        assert_eq!(reserves.len(), MAX_INFLIGHT_PARTS as usize);
        let probed: BTreeSet<Vec<u8>> =
            reserves.iter().map(|c| c.batch.puts[0].0.clone()).collect();
        assert_eq!(
            probed.len(),
            MAX_INFLIGHT_PARTS as usize,
            "each probe a different index"
        );
    })
    .await;
}

/// **(F)** A session that moves to a later `Open` epoch under every reserve commit: the probe
/// count alone bounds the loop, at exactly `MAX_INFLIGHT_PARTS` commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_the_reserve_probe_is_bounded_while_the_session_keeps_moving() {
    within(async {
        let rig = Rig::new().await;
        let epoch = Arc::new(AtomicU64::new(0));
        let (open, key) = (rig.session.clone(), mpu_key(&rig.upload));
        let moving: Hook = Arc::new(move |_| {
            let next = epoch.fetch_add(2, Ordering::SeqCst) + 2;
            WriteBatch::new().put(key.clone(), open_at(&open, next))
        });
        rig.store
            .script(Kind::Reserve, vec![], Some(Act::First(Side::Batch(moving))));
        let outcome = reserve_slot(&rig.store, &reserve_request(&rig, &rig.attempt), T0)
            .await
            .unwrap();
        assert_eq!(outcome, ReserveOutcome::Refused(inflight_full()));
        assert_eq!(
            rig.log.commits(Kind::Reserve).len(),
            MAX_INFLIGHT_PARTS as usize
        );
    })
    .await;
}

/// **(F)** Every intent answered `Conflict` unapplied: exactly [`INTENT_ROUNDS`] commits, then a
/// typed error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_the_intent_loop_is_bounded() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(Kind::Intent, vec![], Some(Act::Conflict));
        let outcome = rig
            .stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await;
        let cause = stopped(outcome);
        assert!(
            matches!(cause, Stop::IntentRoundsSpent { chunk: 1, rounds } if rounds == INTENT_ROUNDS),
            "{cause:?}"
        );
        assert_eq!(rig.log.commits(Kind::Intent).len(), INTENT_ROUNDS as usize);
        assert!(rig.log.puts().is_empty());
    })
    .await;
}

// ═══════════════════════ (G) the per-part chunk bound ═══════════════════════

/// **(G)** A body longer than the per-part bound (3 here, a parameter) never registers more than
/// 3 owned entries. Staging stops and reports the overrun, handing the staged attempt back
/// intact for the commit to refuse and compensate: its chunks, entries, length and digest cover
/// exactly the staged bytes. A body of exactly 3 chunks is not an overrun.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_the_per_part_chunk_bound_stops_staging_and_reports_it() {
    within(async {
        let mut rig = Rig::new().await;
        let body = body_bytes(5 * CHUNK);
        let outcome = rig
            .stage(
                |r| r.max_part_chunks = 3,
                stream_of(pieces(&body, &[CHUNK + 1, 4 * CHUNK - 1])),
            )
            .await;
        let attempt = staged(outcome);
        assert!(attempt.chunk_bound_exceeded(), "the overrun is reported");
        assert_eq!(attempt.chunks().len(), 3);
        assert_eq!(attempt.owned().len(), 3);
        assert_eq!(
            owned(&rig.store.inner, &rig.upload).await.len(),
            3,
            "never more than 3"
        );
        assert_eq!(attempt.len(), 3 * CHUNK as u64);
        assert_eq!(*attempt.digest(), sha256(&body[..3 * CHUNK]));
        assert_eq!(rig.log.commits(Kind::Intent).len(), 3);
        assert_fragments_evidenced(&rig).await;

        let mut rig = Rig::new().await;
        let body = body_bytes(3 * CHUNK);
        let attempt = staged(
            rig.stage(
                |r| r.max_part_chunks = 3,
                stream_of(pieces(&body, &[3 * CHUNK])),
            )
            .await,
        );
        assert!(
            !attempt.chunk_bound_exceeded(),
            "exactly the bound is not an overrun"
        );
        assert_eq!(attempt.chunks().len(), 3);
        assert_eq!(*attempt.digest(), sha256(&body));
    })
    .await;
}

// ═══════════════════════ (H) request validation ═══════════════════════

/// **(H)** A zero chunk size, a lease TTL below 2, a zero write window, a zero body-idle bound,
/// or a slot key space that is empty or wider than the `slot:` grammar is refused with a typed
/// error before anything is read or written — by `stage_part` and by `reserve_slot` alike, which
/// share one check (review finding: `reserve_slot` committed a slot leased one millisecond past
/// its stamp, and answered `SlowDown` forever for an empty key space). A TTL of 2 and a key space
/// exactly as wide as the grammar are valid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h_an_invalid_request_writes_nothing() {
    within(async {
        let mut rig = Rig::new().await;
        let before = snapshot(&rig.store.inner).await;
        let invalid: [fn(&mut StageRequest<'_>); 7] = [
            |r| r.chunk_size = 0,
            |r| r.lease_ttl_millis = 0,
            |r| r.lease_ttl_millis = 1,
            |r| r.write_window_millis = 0,
            |r| r.body_idle_millis = 0,
            |r| r.max_inflight_parts = 0,
            |r| r.max_inflight_parts = MAX_SLOT_INDEX + 2,
        ];
        for edit in invalid {
            let outcome = rig
                .stage(edit, stream_of(vec![Bytes::from_static(b"x")]))
                .await;
            let cause = stopped(outcome);
            assert!(matches!(cause, Stop::InvalidRequest { .. }), "{cause:?}");

            let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
            edit(&mut req);
            match reserve_slot(&rig.store, &req, T0).await {
                Err(StageError::Stopped {
                    cause: Stop::InvalidRequest { .. },
                    ..
                }) => {}
                other => panic!("reserve_slot: expected InvalidRequest, got {other:?}"),
            }
        }
        assert_eq!(snapshot(&rig.store.inner).await, before, "nothing written");
        assert!(rig.log.events().is_empty(), "no commit, no put");
        let staged_two = rig
            .stage(|r| r.lease_ttl_millis = 2, stream_of(Vec::new()))
            .await;
        assert!(
            matches!(staged_two, Ok(StageOutcome::Staged(_))),
            "a TTL of 2 is valid"
        );
        let other = attempt(3);
        let mut req = request(&rig.topology, &rig.upload, &other);
        req.lease_ttl_millis = 2;
        req.max_inflight_parts = MAX_SLOT_INDEX + 1;
        let outcome = reserve_slot(&rig.store, &req, T0).await.unwrap();
        assert!(
            matches!(outcome, ReserveOutcome::Reserved { .. }),
            "a TTL of 2 and the grammar's whole key space are valid: {outcome:?}"
        );
    })
    .await;
}

/// The rig's chunk references point at real fragments: a check that the fake stores what it is
/// given at the server it is given.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_fleet_fake_resolves_fragments_by_placement() {
    within(async {
        let mut rig = Rig::new().await;
        let body = body_bytes(CHUNK);
        let attempt = staged(
            rig.stage(|_| {}, stream_of(vec![Bytes::from(body.clone())]))
                .await,
        );
        let chunk: &ChunkRef = &attempt.chunks()[0];
        let moved = ChunkRef {
            placement: chunk.placement.iter().map(|s| (s + 1) % SERVERS).collect(),
            ..chunk.clone()
        };
        assert!(
            read_chunk_verified(&rig.store.inner, &rig.fleet, &moved)
                .await
                .is_err(),
            "a wrong placement finds nothing"
        );
        assert_eq!(
            read_chunk_verified(&rig.store.inner, &rig.fleet, chunk)
                .await
                .unwrap(),
            body
        );
    })
    .await;
}

// ═══════════════════════ 657.1b (#933): renewal ═══════════════════════

/// Half the lease TTL: renewal falls due this long after the last renewal, or the reservation.
const HALF: u64 = TTL / 2;
/// A write window wider than half a TTL, so the renewal due at `T0 + HALF` falls inside the
/// window of writes authorized at `T0`.
const WINDOW: u64 = HALF + 1_000;
/// The cover the quiesce legs ask for: the 5,000 ms transaction envelope the part commit (#927)
/// passes (`crates/core/src/multipart.rs`, `E_TX_MILLIS`).
const COVER: u64 = 5_000;
const _: () = assert!(WINDOW < TTL && COVER < TTL);

/// A body whose `i`-th piece advances the clock by its `u64` the moment it is read, logged as read
/// ([`Event::Read`]): a client that sends slowly, with no timer fired.
fn ticking(clock: &ManualClock, log: &Log, pieces: Vec<(u64, Bytes)>) -> Body {
    let (clock, log) = (clock.clone(), log.clone());
    let pieces: VecDeque<(u64, Bytes)> = pieces.into();
    Box::pin(stream::unfold(
        (pieces, 0usize),
        move |(mut pieces, index)| {
            let (clock, log) = (clock.clone(), log.clone());
            async move {
                let (advance, piece) = pieces.pop_front()?;
                clock.advance(advance);
                log.push(Event::Read { index });
                Some((Ok(piece), (pieces, index + 1)))
            }
        },
    ))
}

/// The body most renewal legs stage: two chunks, the second piece read at `T0 + HALF + 1`, so a
/// renewal falls due as the third read begins.
fn renewal_body(clock: &ManualClock, log: &Log) -> Body {
    let body = body_bytes(2 * CHUNK);
    let sized = pieces(&body, &[10, 10, 2 * CHUNK - 20]);
    ticking(
        clock,
        log,
        [0, HALF + 1, 0].into_iter().zip(sized).collect(),
    )
}

/// The slot value a batch puts.
fn slot_value_put(batch: &WriteBatch) -> Bytes {
    batch
        .puts
        .iter()
        .find(|(key, _)| key.starts_with(SLOT_PREFIX))
        .map(|(_, value)| value.clone())
        .expect("the batch puts the slot")
}

/// The slot record a batch puts.
fn slot_put(batch: &WriteBatch) -> SlotRecord {
    decode_slot_record(&slot_value_put(batch)).unwrap()
}

/// Staging's `n`-th wait has been asked for.
fn waits_asked(waits: &Arc<Mutex<Vec<u64>>>, n: usize) -> impl FnMut() -> bool {
    let waits = Arc::clone(waits);
    move || waits.lock().unwrap().len() >= n
}

/// **(R1)** The renewal commits in the log, each checked to be one (`0016:349`, `:659`): it
/// requires the session (the `Open@E` fence) and requires nothing absent; it rewrites exactly the
/// records it requires but the session — the attempt's slot, always, and every owned entry beside
/// it; every lease it puts is one instant, strictly later than the slot lease it replaces; and it
/// carries `3 + 2N` operations for `N` owned entries, the count the clamp holds to `B_ops` (R8).
fn renewals(rig: &Rig) -> Vec<Commit> {
    let session = mpu_key(&rig.upload);
    let renewals = rig.log.commits(Kind::Renewal);
    for renewal in &renewals {
        let batch = &renewal.batch;
        assert!(
            batch.preconditions.iter().all(|p| p.expected.is_some()),
            "a renewal requires nothing absent"
        );
        assert!(batch.deletes.is_empty(), "a renewal deletes nothing");
        let mut required: BTreeSet<&[u8]> = batch
            .preconditions
            .iter()
            .map(|p| p.key.as_slice())
            .collect();
        assert!(required.remove(session.as_slice()), "the session fence");
        let put: BTreeSet<&[u8]> = batch.puts.iter().map(|(key, _)| key.as_slice()).collect();
        assert_eq!(
            required, put,
            "a renewal rewrites exactly the slot and the owned entries it requires"
        );
        assert_eq!(
            put.iter()
                .filter(|key| key.starts_with(SLOT_PREFIX))
                .count(),
            1,
            "every renewal rewrites the attempt's slot"
        );
        let owned = put
            .iter()
            .filter(|key| key.starts_with(SIDX_PREFIX))
            .count();
        let ops = batch.preconditions.len() + batch.puts.len() + batch.deletes.len();
        assert_eq!(ops, 3 + 2 * owned, "3 + 2N operations");
        let replaced = decode_slot_record(&slot_required(batch))
            .unwrap()
            .lease_expiry_millis();
        let leases: BTreeSet<u64> = put.iter().map(|key| lease_put(batch, key)).collect();
        assert_eq!(leases.len(), 1, "one instant for every lease it moves");
        assert!(
            leases.iter().all(|lease| *lease > replaced),
            "strictly later than the slot lease it replaces"
        );
    }
    renewals
}

/// What a reaper reading the store at `now` finds: every lease of `upload`'s slots and owned
/// entries still live (`0016:1824-1837`).
async fn assert_live(store: &impl MetadataStore, upload: &UploadId, now: u64) {
    for (index, slot) in slots(store, upload).await {
        assert!(
            slot.lease_expiry_millis() > now,
            "slot {} lapsed at {} (now {now})",
            index.get(),
            slot.lease_expiry_millis()
        );
    }
    for (chunk, entry) in owned(store, upload).await {
        assert!(
            entry.lease_expiry_millis() > now,
            "chunk {chunk}'s owned lease lapsed at {} (now {now})",
            entry.lease_expiry_millis()
        );
    }
}

/// The index of the `n`-th (0-based) commit of `of` in the log's events.
fn commit_at(events: &[Event], of: Kind, n: usize) -> usize {
    events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e, Event::Commit { kind, .. } if *kind == of))
        .map(|(at, _)| at)
        .nth(n)
        .unwrap_or_else(|| panic!("a {of:?} commit #{n}"))
}

/// `open`'s session fenced to `Aborting@1`, the session put a reaper's idle fence makes
/// (`0016:664`).
fn aborting(open: &Bytes) -> Bytes {
    let text = String::from_utf8(open_at(open, 1).to_vec())
        .unwrap()
        .replace(r#"{"kind":"Open"}"#, r#"{"kind":"Aborting"}"#);
    let decoded = decode_session_record(text.as_bytes()).expect("a canonical session");
    assert!(matches!(decoded.state(), SessionState::Aborting {}));
    assert_eq!(metadata::encode(&decoded), text.as_bytes());
    Bytes::from(text)
}

// ─────────────── (R1) the renewal batch ───────────────

/// **(R1)** The renewal batch (`0016:349`, `:659`). Chunks 1 and 2 are staged at `T0`; the next
/// piece arrives at `T0 + HALF + 1`, past the renewal instant, and one renewal runs before chunk
/// 3's intent. It requires the session at its `Open@E` bytes, the slot at the bytes chunk 2's
/// intent put and each owned entry at the bytes its intent put; it rewrites exactly those, every
/// lease moved to its reading plus a TTL, later than the slot lease it replaces, the placements
/// untouched; it carries 7 operations (`3 + 2 × 2`); and chunk 3's intent is fenced on the slot it
/// put. A renewal that left out the slot or an owned entry fails here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r1_a_renewal_rewrites_the_slot_with_every_owned_lease() {
    within(async {
        let mut rig = Rig::new().await;
        let body = body_bytes(2 * CHUNK + 10);
        let sized = pieces(&body, &[2 * CHUNK, 10]);
        let body_stream = ticking(
            &rig.clock,
            &rig.log,
            [0, HALF + 1].into_iter().zip(sized).collect(),
        );
        let attempt = staged(rig.stage(|_| {}, body_stream).await);
        let renewals = renewals(&rig);
        assert_eq!(renewals.len(), 1);
        let renewal = &renewals[0];
        assert_eq!(renewal.answer, Answer::Committed);
        assert_eq!(renewal.clock, T0 + HALF + 1);
        let intents = rig.log.commits(Kind::Intent);
        assert_eq!(intents.len(), 3);
        // What the store held when the renewal was sent.
        let mut prior: BTreeMap<Vec<u8>, Bytes> = BTreeMap::new();
        prior.insert(mpu_key(&rig.upload), rig.session.clone());
        prior.insert(
            attempt.slot_key().to_vec(),
            slot_value_put(&intents[1].batch),
        );
        for intent in &intents[..2] {
            let (key, value) = intent_put(&intent.batch);
            prior.insert(key, value);
        }
        let required: BTreeMap<Vec<u8>, Bytes> = renewal
            .batch
            .preconditions
            .iter()
            .map(|p| (p.key.clone(), p.expected.clone().unwrap()))
            .collect();
        assert_eq!(required, prior, "every record required at its prior bytes");
        assert_eq!(
            renewal.batch.preconditions.len() + renewal.batch.puts.len(),
            3 + 2 * 2
        );
        let lease = T0 + HALF + 1 + TTL;
        for (key, _) in &renewal.batch.puts {
            assert_eq!(
                lease_put(&renewal.batch, key),
                lease,
                "the reading plus a TTL"
            );
        }
        for intent in &intents[..2] {
            let (key, _) = intent_put(&intent.batch);
            let before = intent_entry(&intent.batch);
            let (_, value) = renewal.batch.puts.iter().find(|(k, _)| *k == key).unwrap();
            let after = decode_owned_entry(&key, value).unwrap().2;
            assert_eq!(after.owner(), before.owner());
            assert_eq!(after.staged(), before.staged(), "only the lease moved");
        }
        assert_eq!(
            slot_required(&intents[2].batch),
            slot_value_put(&renewal.batch),
            "chunk 3's intent is fenced on the renewed slot"
        );
        let leases: Vec<u64> = attempt
            .owned()
            .iter()
            .map(|held| held.entry().lease_expiry_millis())
            .collect();
        assert_eq!(leases[..2], [lease, lease]);
        assert_eq!(*attempt.digest(), sha256(&body));
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

// ─────────────── (R2) renewal runs for the whole request ───────────────

/// **(R2) Before the first chunk.** The body arrives in four small pieces, the clock moving past
/// half a TTL before each of the last three, with no timer fired, so the first chunk boundary
/// falls well after the reserved slot lease (`T0 + TTL`) would have lapsed. A renewal runs as each
/// next read begins and before the intent: the first before the third piece is read, the slot lease
/// moved forward before the first intent, which is fenced on the last renewal's slot. At every
/// instant the clock reaches, a reaper reading the store finds every lease live. A writer that
/// renewed only at chunk boundaries finds the slot lapsed at the first one (`0016:1824-1837`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_renewal_runs_while_the_body_arrives_before_the_first_chunk() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body_stream) = channel(&rig.log);
        let body = body_bytes(CHUNK);
        let sized = pieces(&body, &[10, 10, 10, CHUNK - 30]);
        // The fixture binds: the first chunk boundary falls after the reserved lease expired.
        const { assert!(3 * (HALF + 1) > TTL) };
        tx.send(sized[0].clone()).unwrap();
        let (log, clock, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.waits),
        );
        let (inner, upload) = (&rig.store.inner, rig.upload.clone());
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let staging = stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream);
        let driver = async move {
            until("the second read to wait", waits_asked(&waits, 2)).await;
            for (n, piece) in sized[1..].iter().enumerate() {
                clock.advance(HALF + 1);
                assert_live(inner, &upload, clock.now_millis()).await;
                tx.send(piece.clone()).unwrap();
                until("the piece read", || log.read(n + 1)).await;
                until("a renewal after it", || log.landed(Kind::Renewal) == n + 1).await;
            }
            until("the first intent", || log.landed(Kind::Intent) == 1).await;
            assert_live(inner, &upload, clock.now_millis()).await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let attempt = staged(outcome);
        assert_eq!(attempt.len(), CHUNK as u64);
        assert_eq!(*attempt.digest(), sha256(&body));
        let renewals = renewals(&rig);
        assert_eq!(renewals.len(), 3);
        for (n, renewal) in renewals.iter().enumerate() {
            let reading = T0 + (n as u64 + 1) * (HALF + 1);
            assert_eq!(renewal.answer, Answer::Committed);
            assert_eq!(renewal.clock, reading);
            assert_eq!(
                slot_put(&renewal.batch).lease_expiry_millis(),
                reading + TTL,
                "the slot lease moved forward from the renewal's own reading"
            );
        }
        let events = rig.log.events();
        let third_read = events
            .iter()
            .position(|e| matches!(e, Event::Read { index: 2 }))
            .unwrap();
        assert!(
            commit_at(&events, Kind::Renewal, 0) < third_read,
            "renewed while the body was still arriving"
        );
        assert!(
            commit_at(&events, Kind::Renewal, 2) < commit_at(&events, Kind::Intent, 0),
            "renewed before the first intent"
        );
        assert_eq!(
            slot_required(&rig.log.commits(Kind::Intent)[0].batch),
            slot_value_put(&renewals[2].batch)
        );
    })
    .await;
}

/// **(R2) D-i, the body withheld.** After the slot is reserved and the first piece read, the test
/// withholds the next piece, moves the clock past half a TTL and fires the timer: a renewal
/// rewriting the slot commits **while the read is still pending**. Then the piece is delivered and
/// staging finishes normally, each piece read exactly once. A staging that renewed only between
/// delivered pieces would still be waiting here for the renewal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_d_i_a_renewal_commits_while_a_body_read_is_pending() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body_stream) = channel(&rig.log);
        let body = body_bytes(2 * CHUNK);
        tx.send(Bytes::copy_from_slice(&body[..10])).unwrap();
        let (log, clock, timer, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let (inner, upload) = (&rig.store.inner, rig.upload.clone());
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let staging = stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream);
        let rest = Bytes::copy_from_slice(&body[10..]);
        let driver = async move {
            until("the second read to wait", waits_asked(&waits, 2)).await;
            clock.advance(HALF + 1);
            assert_live(inner, &upload, clock.now_millis()).await;
            timer.add_permits(1);
            until("a renewal while the read is pending", || {
                log.landed(Kind::Renewal) == 1
            })
            .await;
            assert!(!log.read(1), "the piece is still withheld");
            assert!(log.commits(Kind::Intent).is_empty());
            assert_live(inner, &upload, clock.now_millis()).await;
            tx.send(rest).unwrap();
            until("the withheld piece read", || log.read(1)).await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let attempt = staged(outcome);
        assert_eq!(attempt.len(), 2 * CHUNK as u64);
        assert_eq!(*attempt.digest(), sha256(&body), "no piece dropped");
        for index in [0, 1] {
            let reads = rig
                .log
                .events()
                .iter()
                .filter(|e| matches!(e, Event::Read { index: i } if *i == index))
                .count();
            assert_eq!(reads, 1, "piece {index} read once");
        }
        let renewals = renewals(&rig);
        assert_eq!(renewals.len(), 1);
        assert_eq!(renewals[0].clock, T0 + HALF + 1);
        assert_eq!(
            slot_put(&renewals[0].batch).lease_expiry_millis(),
            T0 + HALF + 1 + TTL
        );
        let events = rig.log.events();
        let withheld = events
            .iter()
            .position(|e| matches!(e, Event::Read { index: 1 }))
            .unwrap();
        assert!(commit_at(&events, Kind::Renewal, 0) < withheld);
    })
    .await;
}

/// **(R2) D-ii, a fragment write held.** Chunk 1 is staged at `T0`; chunk 2's bytes arrive at
/// `T0 + 4,000`, so its writes' deadline (`T0 + 7,000`) falls after the renewal instant. The fake
/// holds one put of chunk 2, once chunk 1's and chunk 2's owned entries are durable. The test moves
/// the clock past the renewal instant and fires the timer: a renewal rewriting the slot and chunk
/// 1's and chunk 2's owned leases commits **while the put is still pending**. Then the put is
/// released, inside its deadline, staging finishes normally, and that put was sent once. A staging
/// that did not race the timer against a pending write would wait for the put here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_d_ii_a_renewal_commits_while_a_fragment_write_is_pending() {
    within(async {
        let mut rig = Rig::new().await;
        let held = FragmentId { chunk: 2, index: 0 };
        let gate = rig.fleet.hold(held, HoldAt::Arrival);
        let (tx, body_stream) = channel(&rig.log);
        let body = body_bytes(2 * CHUNK);
        tx.send(Bytes::copy_from_slice(&body[..CHUNK])).unwrap();
        let (log, clock, timer, waits, fleet) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
            rig.fleet.clone(),
        );
        let (inner, upload) = (&rig.store.inner, rig.upload.clone());
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let staging = stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream);
        let second = Bytes::copy_from_slice(&body[CHUNK..]);
        let driver = async move {
            // The first read, chunk 1's writes, and the read for chunk 2's bytes.
            until("the read after chunk 1 to wait", waits_asked(&waits, 3)).await;
            clock.advance(4_000);
            tx.send(second).unwrap();
            until("chunk 2's held put, the rest answered", || {
                fleet.holds_only(held, HoldAt::Arrival)
            })
            .await;
            clock.advance(1_001);
            assert_live(inner, &upload, clock.now_millis()).await;
            timer.add_permits(1);
            until("a renewal while the put is pending", || {
                log.landed(Kind::Renewal) == 1
            })
            .await;
            assert!(!log.acked(held), "the put is still pending");
            assert!(fleet.held(held, HoldAt::Arrival));
            assert_live(inner, &upload, clock.now_millis()).await;
            gate.add_permits(1);
            until("the released put answered", || log.acked(held)).await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let attempt = staged(outcome);
        assert_eq!(attempt.chunks().len(), 2);
        assert_eq!(rig.log.put_count(held), 1, "the held put was not re-sent");
        let renewals = renewals(&rig);
        assert_eq!(renewals.len(), 1);
        let renewal = &renewals[0];
        assert_eq!(renewal.clock, T0 + 5_001);
        for chunk in [1, 2] {
            assert_eq!(
                lease_put(&renewal.batch, &sidx_key(&rig.upload, part(), chunk)),
                T0 + 5_001 + TTL,
                "the renewal moves chunk {chunk}'s owned lease"
            );
        }
        let events = rig.log.events();
        let answered = events
            .iter()
            .position(|e| matches!(e, Event::Acked { id } if *id == held))
            .unwrap();
        assert!(commit_at(&events, Kind::Renewal, 0) < answered);
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

/// **(R2)** A renewal falls due at exactly half a TTL after the last one, and not a millisecond
/// earlier. With a body read pending, the timer fires a millisecond early: staging only waits out
/// the rest, renewing nothing. At `T0 + HALF` it fires again and a renewal commits at that reading,
/// the read's idle deadline armed beside it; the next wait for the renewal instant is half a TTL
/// long, and at `T0 + TTL` the next renewal commits at that reading.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_a_renewal_falls_due_at_exactly_half_a_ttl_after_the_last() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body_stream) = channel(&rig.log);
        let body = body_bytes(CHUNK);
        tx.send(Bytes::copy_from_slice(&body[..10])).unwrap();
        let (log, clock, timer, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let staging = stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream);
        let rest = Bytes::copy_from_slice(&body[10..]);
        let asked = Arc::clone(&waits);
        let driver = async move {
            until("the second read to wait", waits_asked(&waits, 2)).await;
            clock.advance(HALF - 1);
            timer.add_permits(1);
            until(
                "the early wake to wait out the rest",
                waits_asked(&waits, 3),
            )
            .await;
            assert_eq!(log.commits(Kind::Renewal).len(), 0, "not due yet");
            clock.advance(1);
            timer.add_permits(1);
            until("a renewal at the due instant", || {
                log.landed(Kind::Renewal) == 1
            })
            .await;
            // The idle deadline's timer beside the renewal, then the wait for the next instant.
            until("the next wait", waits_asked(&waits, 5)).await;
            clock.advance(HALF);
            timer.add_permits(1);
            until("the next renewal", || log.landed(Kind::Renewal) == 2).await;
            tx.send(rest).unwrap();
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        staged(outcome);
        assert_eq!(asked.lock().unwrap()[1..5], [HALF, 1, IDLE - HALF, HALF]);
        let renewals = renewals(&rig);
        assert_eq!(renewals.len(), 2);
        for (renewal, reading) in renewals.iter().zip([T0 + HALF, T0 + TTL]) {
            assert_eq!(renewal.clock, reading);
            assert_eq!(
                slot_put(&renewal.batch).lease_expiry_millis(),
                reading + TTL
            );
        }
    })
    .await;
}

/// **(R2)** A renewal due as an intent starts is sent before that intent. The body's only piece,
/// one whole chunk, arrives at exactly `T0 + HALF`, as renewal falls due, with no timer fired: the
/// renewal is the very next commit, at that reading, and the intent is fenced on the slot it put —
/// never deferred until the chunk's writes are pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_a_renewal_due_as_an_intent_starts_is_sent_before_it() {
    within(async {
        let mut rig = Rig::new().await;
        let piece = Bytes::from(body_bytes(CHUNK));
        let body_stream = ticking(&rig.clock, &rig.log, vec![(HALF, piece)]);
        staged(rig.stage(|_| {}, body_stream).await);
        let events = rig.log.events();
        let renewal = commit_at(&events, Kind::Renewal, 0);
        let intent = commit_at(&events, Kind::Intent, 0);
        assert!(renewal < intent, "renewed before the intent");
        assert!(
            events[renewal + 1..intent]
                .iter()
                .all(|e| !matches!(e, Event::Commit { .. })),
            "the intent is the next commit"
        );
        let renewals = renewals(&rig);
        assert_eq!(renewals[0].clock, T0 + HALF);
        assert_eq!(
            slot_put(&renewals[0].batch).lease_expiry_millis(),
            T0 + HALF + TTL
        );
        assert_eq!(
            slot_required(&rig.log.commits(Kind::Intent)[0].batch),
            slot_value_put(&renewals[0].batch)
        );
    })
    .await;
}

/// **(R2)** Never while the attempt's own intent batch is in flight. Chunk 1's intent is held
/// (`may_still_commit: true`) while the clock passes the renewal instant; its next round releases
/// the held batch first, conflicts, and adopts it from the entry's bytes. No renewal is sent
/// between those two intent commits: the first is sent once the intent has settled, beside the
/// chunk's writes, fenced on the slot the adopted intent put and renewing its entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_no_renewal_is_sent_while_an_intent_is_in_flight() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(
            Kind::Intent,
            vec![Act::Hold { advance: HALF + 1 }, Act::ReleaseHeldFirst],
            None,
        );
        // Wide enough that the held intent's own `t_auth` still authorizes the writes.
        let attempt = staged(
            rig.stage(
                |r| r.write_window_millis = 2 * TTL,
                stream_of(vec![Bytes::from(body_bytes(CHUNK))]),
            )
            .await,
        );
        let events = rig.log.events();
        let (first, second) = (
            commit_at(&events, Kind::Intent, 0),
            commit_at(&events, Kind::Intent, 1),
        );
        let renewal = commit_at(&events, Kind::Renewal, 0);
        assert!(
            second < renewal,
            "no renewal between the intent's rounds: {first}, {second}, {renewal}"
        );
        let intents = rig.log.intents_of(1);
        assert_eq!(intents[1].answer, Answer::Conflict);
        let renewals = renewals(&rig);
        assert_eq!(
            slot_required(&renewals[0].batch),
            slot_value_put(&intents[0].batch),
            "fenced on the slot the adopted intent put"
        );
        let (entry, value) = intent_put(&intents[0].batch);
        assert!(renewals[0]
            .batch
            .preconditions
            .iter()
            .any(|p| p.key == entry && p.expected.as_ref() == Some(&value)));
        assert_eq!(attempt.chunks().len(), 1);
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

/// **(R2) A renewal that crosses expiry** (`0016:1843-1874`). Chunk 1 is staged at `T0`, so the
/// slot's lease is `T0 + TTL + 1`. With the next read pending, the renewal falls due at
/// `T0 + HALF + 1`: it checks every lease live and sends, and the wrapper holds its commit. The
/// clock moves past the slot's previous expiry; no write deadline or idle bound passes. Standing
/// in for the reaper, the test reads the slot's bytes `S_old` and finds the lease lapsed. It
/// releases the renewal, which lands — the store checks bytes, not expiry — and commits a stand-in
/// idle fence, `require(mpu == Open@E)` + `require(slot == S_old)` + the session at `Aborting`: it
/// conflicts, because the renewal rewrote the slot. The session stays `Open`, and staging goes on
/// under the new leases and finishes normally. A renewal batch without the slot rewrite lets the
/// fence commit; a renewal refused once its commit returned late stops staging; either fails here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r2_a_renewal_that_crosses_expiry_defeats_the_reapers_fence() {
    within(async {
        let mut rig = Rig::new().await;
        let gate = Arc::new(Semaphore::new(0));
        rig.store.script(
            Kind::Renewal,
            vec![Act::Gated(Arc::clone(&gate), Box::new(Act::Pass))],
            None,
        );
        let (tx, body_stream) = channel(&rig.log);
        let body = body_bytes(2 * CHUNK);
        tx.send(Bytes::copy_from_slice(&body[..CHUNK])).unwrap();
        let (log, clock, timer, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let (inner, session) = (&rig.store.inner, rig.session.clone());
        let (slot, session_key) = (
            slot_key(&rig.upload, SlotIndex::new(0).unwrap()),
            mpu_key(&rig.upload),
        );
        let s_old = Arc::new(Mutex::new(None));
        let seen = Arc::clone(&s_old);
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let staging = stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream);
        let second = Bytes::copy_from_slice(&body[CHUNK..]);
        let driver = async move {
            until("the read after chunk 1 to wait", waits_asked(&waits, 3)).await;
            clock.advance(HALF + 1);
            timer.add_permits(1);
            until("the renewal at its gate", || log.gated(Kind::Renewal) == 1).await;
            clock.set(T0 + TTL + 2);
            // The reaper's read: the slot's bytes, and a lease lapsed at its clock.
            let old = inner.get(&slot).await.unwrap().expect("the slot");
            let lease = decode_slot_record(&old).unwrap().lease_expiry_millis();
            assert_eq!(lease, T0 + TTL + 1);
            assert!(lease <= clock.now_millis(), "the reaper reads it lapsed");
            *seen.lock().unwrap() = Some(old.clone());
            gate.add_permits(1);
            until("the renewal to land", || log.landed(Kind::Renewal) >= 1).await;
            let fence = WriteBatch::new()
                .require(session_key.clone(), session.clone())
                .require(slot.clone(), old)
                .put(session_key.clone(), aborting(&session));
            assert_eq!(
                inner.commit(fence).await.unwrap(),
                CommitOutcome::Conflict,
                "the fence pinned on S_old cannot land over the renewed request"
            );
            tx.send(second).unwrap();
            until("chunk 2's intent", || log.landed(Kind::Intent) == 2).await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let attempt = staged(outcome);
        assert_eq!(attempt.chunks().len(), 2);
        assert!(
            matches!(rig.state().await, SessionState::Open {}),
            "the session is still Open"
        );
        let renewals = renewals(&rig);
        let first = &renewals[0];
        assert_eq!(first.answer, Answer::Committed);
        assert_eq!(first.clock, T0 + HALF + 1);
        let old = s_old.lock().unwrap().clone().unwrap();
        assert_eq!(slot_required(&first.batch), old, "it was sent over S_old");
        assert_eq!(
            slot_put(&first.batch).lease_expiry_millis(),
            T0 + HALF + 1 + TTL
        );
        assert_live(&rig.store.inner, &rig.upload, rig.clock.now_millis()).await;
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

// ─────────────── (R3) renewal never holds staging past a deadline ───────────────

/// **(R3) The reviewers' schedule** (v2 sign-off). Chunk 1's writes carry `T0 + WINDOW`, past the
/// renewal instant. The fake holds one of them, and the renewal that falls due at `T0 + HALF` is
/// sent and held at the wrapper's gate. The test moves the clock to the write deadline and fires
/// the timer: staging ends there, before the renewal is released, with an in-doubt error that keeps
/// the renewal's `may_still_commit` doubt — staging's own report ([`ABANDONED_COMMIT`]) — and a cause
/// naming the abandoned write. Released afterwards, the renewal lands: the doubt was real. A
/// staging that awaited the renewal before watching the deadline would still be waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_the_write_deadline_cuts_off_a_renewal_still_committing() {
    within(async {
        let mut rig = Rig::new().await;
        let held = FragmentId { chunk: 1, index: 0 };
        let put = rig.fleet.hold(held, HoldAt::Arrival);
        let renewal_gate = Arc::new(Semaphore::new(0));
        rig.store.script(
            Kind::Renewal,
            vec![Act::Gated(Arc::clone(&renewal_gate), Box::new(Act::Pass))],
            None,
        );
        let body = body_bytes(2 * CHUNK);
        let (log, clock, timer, fleet) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            rig.fleet.clone(),
        );
        let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
        req.write_window_millis = WINDOW;
        let ended = Arc::new(AtomicBool::new(false));
        let staging = flagging(
            Arc::clone(&ended),
            stage_part(
                &rig.store,
                &rig.fleet,
                &mut rig.runtime,
                &req,
                stream_of(pieces(&body, &[2 * CHUNK])),
            ),
        );
        let driver = async move {
            until("chunk 1's held put", || {
                fleet.holds_only(held, HoldAt::Arrival)
            })
            .await;
            clock.advance(HALF);
            timer.add_permits(1);
            until("the renewal at its gate", || log.gated(Kind::Renewal) == 1).await;
            clock.advance(WINDOW - HALF);
            timer.add_permits(1);
            until(
                "staging to end at the deadline, its renewal still held",
                || ended.load(Ordering::SeqCst),
            )
            .await;
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        assert_eq!(
            rig.clock.now_millis(),
            T0 + WINDOW,
            "it ended at the deadline"
        );
        let err = outcome.expect_err("staging stopped");
        assert!(err.to_string().contains("may still be applied"), "{err}");
        let doubt = in_doubt::<StageOutcome>(Err(err));
        assert!(
            doubt.may_still_commit,
            "the abandoned renewal may still land"
        );
        assert!(doubt.outcome.may_still_commit);
        assert_eq!(
            doubt.outcome.backend, ABANDONED_COMMIT,
            "staging's own report"
        );
        let (chunk, deadline, abandoned, reports) = effect_unknown(&doubt.cause);
        assert_eq!((chunk, deadline), (1, T0 + WINDOW));
        assert_abandoned_only(abandoned, held);
        assert!(reports.is_empty());
        assert!(
            stopped_by_renewal(&doubt.cause).is_none(),
            "the deadline stopped staging, not the renewal"
        );
        assert_eq!(rig.log.gated(Kind::Renewal), 1, "one renewal was sent");
        assert!(
            rig.log.commits(Kind::Renewal).is_empty(),
            "and never answered"
        );
        assert_eq!(renewal_gate.available_permits(), 0);
        assert_eq!(
            put.available_permits(),
            0,
            "the held put was never released"
        );
        assert_eq!(rig.log.put_count(held), 1, "the held write was not re-sent");
        assert_eq!(rig.log.commits(Kind::Intent).len(), 1);
        let reserved = rig.log.commits(Kind::Reserve)[0].batch.puts[0].0.clone();
        let before = rig.store.inner.get(&reserved).await.unwrap();
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
        assert_ne!(
            rig.store.inner.get(&reserved).await.unwrap(),
            before,
            "the abandoned renewal could still land, and did"
        );
    })
    .await;
}

/// **(R3) The adversary's schedule, and a write published in time** (v3 sign-off). As in the
/// reviewers' schedule, chunk 1's put is held and the renewal's commit waits at its gate. Now the
/// test lets the fake publish the put while the renewal waits, and waits until the fake has stored
/// it and answered `Ok` — at a clock reading before the deadline, which the fake enforces at its
/// publication point — and its reply, held in transit, is collected by staging while the renewal
/// still waits. Once staging has taken the reply, the test moves the clock past the deadline and
/// fires the timer, which no wait is armed for any more, and then releases the renewal. The write
/// was answered in time and is taken: the part stages, the put sent once, the renewal held at the
/// gate landed. A staging that awaited the renewal before polling the writes never collects the
/// reply while the renewal waits; one that still judged the deadline once the writes had answered
/// abandons the renewal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_a_write_answered_while_a_renewal_commits_is_taken() {
    within(async {
        let mut rig = Rig::new().await;
        let held = FragmentId { chunk: 1, index: 0 };
        let arrival = rig.fleet.hold(held, HoldAt::Arrival);
        let reply = rig.fleet.hold(held, HoldAt::Answer);
        let renewal_gate = Arc::new(Semaphore::new(0));
        rig.store.script(
            Kind::Renewal,
            vec![Act::Gated(Arc::clone(&renewal_gate), Box::new(Act::Pass))],
            None,
        );
        let body = body_bytes(2 * CHUNK);
        let (log, clock, timer, fleet) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            rig.fleet.clone(),
        );
        let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
        req.write_window_millis = WINDOW;
        let staging = stage_part(
            &rig.store,
            &rig.fleet,
            &mut rig.runtime,
            &req,
            stream_of(pieces(&body, &[2 * CHUNK])),
        );
        let driver = async move {
            until("chunk 1's held put", || {
                fleet.holds_only(held, HoldAt::Arrival)
            })
            .await;
            clock.advance(HALF);
            timer.add_permits(1);
            until("the renewal at its gate", || log.gated(Kind::Renewal) == 1).await;
            arrival.add_permits(1);
            // The fake publishes and answers `Ok`, and staging, polling the writes beside the
            // renewal, collects the reply up to its transit hold.
            until("the put published, its reply collected", || {
                fleet.held(held, HoldAt::Answer)
            })
            .await;
            assert!(log.acked(held), "the fake stored it and answered Ok");
            assert!(
                clock.now_millis() < T0 + WINDOW,
                "published before its deadline"
            );
            assert!(!log.has(|e| matches!(e, Event::Refused { id } if *id == held)));
            assert!(
                log.commits(Kind::Renewal).is_empty(),
                "the renewal still waits"
            );
            reply.add_permits(1);
            until(
                "staging to take the reply, the renewal still waiting",
                || log.delivered(held),
            )
            .await;
            clock.set(T0 + WINDOW + 1);
            timer.add_permits(1);
            assert_eq!(
                timer.available_permits(),
                1,
                "nothing waits on the answered write's deadline any more"
            );
            renewal_gate.add_permits(1);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let attempt = staged(outcome);
        assert_eq!(attempt.chunks().len(), 2);
        assert_eq!(
            rig.log.put_count(held),
            1,
            "the answered put was not re-sent"
        );
        let renewals = renewals(&rig);
        assert_eq!(renewals[0].answer, Answer::Committed);
        assert_eq!(renewals[0].clock, T0 + HALF, "the renewal held at the gate");
        assert_eq!(rig.store.held_count(), 0, "it was not abandoned");
        assert_fragments_evidenced(&rig).await;
    })
    .await;
}

/// **(R3) The put answers in time, then the renewal outlives the put's deadline** (added
/// 2026-10-22). The fake holds chunk 1's last pending put and the wrapper holds the renewal's
/// commit. The fake publishes the put and answers `Ok` before its deadline — the test waits until it
/// has stored the fragment, and until staging has taken the answer — and only then does the test
/// move the clock past that deadline and fire the timer, the renewal still held: no wait is armed
/// for that deadline any more. Then the wrapper releases the renewal: run 1 as `Committed`, run 2
/// as `CommitUnknownResult { may_still_commit: false }`, applied, which the renewal settles from
/// its slot's bytes (R5). Staging succeeds, not in doubt, the held renewal's leases adopted —
/// chunk 2's intent is fenced on the slot it put — and the put not re-sent. A write deadline that
/// still constrained the renewal once its writes had answered would abandon it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_a_renewal_outliving_an_answered_writes_deadline_is_awaited() {
    for unknown in [false, true] {
        within(async {
            let mut rig = Rig::new().await;
            let held = FragmentId { chunk: 1, index: 0 };
            let put = rig.fleet.hold(held, HoldAt::Arrival);
            let renewal_gate = Arc::new(Semaphore::new(0));
            let released = if unknown {
                Act::Unknown {
                    applied: true,
                    may_still_commit: false,
                }
            } else {
                Act::Pass
            };
            rig.store.script(
                Kind::Renewal,
                vec![Act::Gated(Arc::clone(&renewal_gate), Box::new(released))],
                None,
            );
            let body = body_bytes(2 * CHUNK);
            let (log, clock, timer, fleet) = (
                rig.log.clone(),
                rig.clock.clone(),
                Arc::clone(&rig.runtime.timer),
                rig.fleet.clone(),
            );
            let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
            req.write_window_millis = WINDOW;
            let staging = stage_part(
                &rig.store,
                &rig.fleet,
                &mut rig.runtime,
                &req,
                stream_of(pieces(&body, &[2 * CHUNK])),
            );
            let driver = async move {
                until("chunk 1's last pending put", || {
                    fleet.holds_only(held, HoldAt::Arrival)
                })
                .await;
                clock.advance(HALF);
                timer.add_permits(1);
                until("the renewal at its gate", || log.gated(Kind::Renewal) == 1).await;
                put.add_permits(1);
                until("the fake to store the put and answer Ok", || {
                    log.acked(held)
                })
                .await;
                assert!(clock.now_millis() < T0 + WINDOW, "answered in time");
                until("staging to take the answer, the renewal still held", || {
                    log.delivered(held)
                })
                .await;
                clock.set(T0 + WINDOW + 1);
                timer.add_permits(1);
                assert_eq!(
                    timer.available_permits(),
                    1,
                    "nothing waits on the answered write's deadline any more"
                );
                renewal_gate.add_permits(1);
            };
            let (outcome, ()) = tokio::join!(staging, driver);
            let attempt = staged(outcome);
            assert_eq!(attempt.chunks().len(), 2, "unknown {unknown}");
            assert_eq!(rig.log.put_count(held), 1, "the put was not re-sent");
            let renewals = renewals(&rig);
            assert_eq!(renewals.len(), 1, "settled by the re-read, not re-sent");
            assert_eq!(renewals[0].clock, T0 + HALF, "the renewal held at the gate");
            assert_eq!(rig.store.held_count(), 0, "it was not abandoned");
            let answer = if unknown {
                Answer::Unknown {
                    may_still_commit: false,
                }
            } else {
                Answer::Committed
            };
            assert_eq!(renewals[0].answer, answer);
            assert_eq!(
                slot_required(&rig.log.intents_of(2)[0].batch),
                slot_value_put(&renewals[0].batch),
                "the renewal's leases adopted"
            );
            assert_fragments_evidenced(&rig).await;
        })
        .await;
    }
}

/// **(R3)** A renewal cut off by the deadline between two of its own commits keeps exactly the
/// doubt of the commit still in flight, no more and no less. Chunk 1's put is held, the renewal's
/// first round is lost, and the write deadline passes while the renewal is
///
/// * reading its slot after a `Conflict`: nothing is in doubt;
/// * reading its slot after `CommitUnknownResult { may_still_commit: false }`, unapplied: the
///   store's own report, unsettled, which can no longer land;
/// * reading its slot after a held round (`may_still_commit: true`): the store's own report;
/// * awaiting its second commit, after a `Conflict`: staging's own report, which may still land.
///
/// The cause is the write's in every run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_a_renewal_cut_off_between_its_commits_keeps_exactly_its_doubt() {
    for run in 0..4 {
        within(async {
            let mut rig = Rig::new().await;
            let held = FragmentId { chunk: 1, index: 0 };
            let _put = rig.fleet.hold(held, HoldAt::Arrival);
            let gate = Arc::new(Semaphore::new(0));
            let first = match run {
                0 | 3 => Act::Conflict,
                1 => Act::Unknown {
                    applied: false,
                    may_still_commit: false,
                },
                _ => Act::Hold { advance: 0 },
            };
            let mut plan = vec![first];
            if run == 3 {
                plan.push(Act::Gated(Arc::clone(&gate), Box::new(Act::Pass)));
            } else {
                // No slot read precedes the renewal's in a clean run.
                let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
                rig.store
                    .after_read(&slot, 0, vec![Then::Gate(Arc::clone(&gate))]);
            }
            rig.store.script(Kind::Renewal, plan, None);
            let body = body_bytes(2 * CHUNK);
            let (log, clock, timer, fleet) = (
                rig.log.clone(),
                rig.clock.clone(),
                Arc::clone(&rig.runtime.timer),
                rig.fleet.clone(),
            );
            let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
            req.write_window_millis = WINDOW;
            let ended = Arc::new(AtomicBool::new(false));
            let staging = flagging(
                Arc::clone(&ended),
                stage_part(
                    &rig.store,
                    &rig.fleet,
                    &mut rig.runtime,
                    &req,
                    stream_of(pieces(&body, &[2 * CHUNK])),
                ),
            );
            let driver = async move {
                until("chunk 1's held put", || {
                    fleet.holds_only(held, HoldAt::Arrival)
                })
                .await;
                clock.advance(HALF);
                timer.add_permits(1);
                until("the renewal between its commits", || {
                    log.reads_gated() == 1 || log.gated(Kind::Renewal) == 1
                })
                .await;
                clock.advance(WINDOW - HALF);
                timer.add_permits(1);
                until("staging to end at the deadline", || {
                    ended.load(Ordering::SeqCst)
                })
                .await;
            };
            let (outcome, ()) = tokio::join!(staging, driver);
            assert_eq!(gate.available_permits(), 0, "run {run}: never let through");
            let cause = match (run, outcome) {
                (0, outcome) => stopped(outcome),
                (_, outcome) => {
                    let doubt = in_doubt(outcome);
                    let (may_still_commit, backend) = match run {
                        1 => (false, "scripted"),
                        2 => (true, "scripted"),
                        _ => (true, ABANDONED_COMMIT),
                    };
                    assert_eq!(doubt.may_still_commit, may_still_commit, "run {run}");
                    assert_eq!(doubt.outcome.backend, backend, "run {run}");
                    doubt.cause
                }
            };
            let (chunk, deadline, abandoned, _) = effect_unknown(&cause);
            assert_eq!((chunk, deadline), (1, T0 + WINDOW), "run {run}");
            assert_abandoned_only(abandoned, held);
            assert!(stopped_by_renewal(&cause).is_none(), "run {run}");
            assert_eq!(
                rig.log.commits(Kind::Renewal).len(),
                1,
                "run {run}: one answered"
            );
        })
        .await;
    }
}

/// **(R3) The body-idle bound still holds under renewal.** The body-idle bound is two TTLs; after
/// its first piece the body sends nothing but one empty piece. Renewals at `T0 + HALF`, `+ TTL` and
/// `+ 3 × HALF` keep every lease live while the read is pending, and the empty piece does not move
/// the bound: at `T0 + 2 × TTL` staging stops with `BodyIdle`, sending no renewal at that instant
/// and writing nothing more. A renewal that deferred the idle bound would keep a silent client's
/// slot for ever; this leg fails waiting for staging to stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r3_the_body_idle_bound_holds_while_renewals_keep_the_leases() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body_stream) = channel(&rig.log);
        tx.send(Bytes::from(body_bytes(10))).unwrap();
        let (log, clock, timer, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let (inner, upload) = (&rig.store.inner, rig.upload.clone());
        let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
        req.body_idle_millis = 2 * TTL;
        let ended = Arc::new(AtomicBool::new(false));
        let staging = flagging(
            Arc::clone(&ended),
            stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream),
        );
        let driver = async move {
            until("the second read to wait", waits_asked(&waits, 2)).await;
            for n in 1..=3 {
                if n == 3 {
                    // An empty piece halfway through: read, and not progress.
                    clock.advance(2_000);
                    tx.send(Bytes::new()).unwrap();
                    until("the empty piece read", || log.read(1)).await;
                    clock.advance(HALF - 2_000);
                } else {
                    clock.advance(HALF);
                }
                assert_live(inner, &upload, clock.now_millis()).await;
                timer.add_permits(1);
                until("a renewal while the read is pending", || {
                    log.landed(Kind::Renewal) == n
                })
                .await;
                until("the next wait", waits_asked(&waits, 2 + n)).await;
            }
            clock.advance(HALF);
            timer.add_permits(1);
            until("staging to give up the silent body", || {
                ended.load(Ordering::SeqCst)
            })
            .await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let cause = stopped(outcome);
        assert!(
            matches!(cause, Stop::BodyIdle { idle_millis } if idle_millis == 2 * TTL),
            "{cause:?}"
        );
        assert_eq!(rig.clock.now_millis(), T0 + 2 * TTL, "stopped at the bound");
        let renewals = renewals(&rig);
        assert_eq!(renewals.len(), 3, "none at the bound itself");
        assert!(rig.log.commits(Kind::Intent).is_empty(), "nothing staged");
        assert!(rig.log.puts().is_empty());
        assert_eq!(
            slots_of(&rig.store.inner, &rig.upload, &rig.attempt).await,
            1,
            "the slot stays for the reaper"
        );
    })
    .await;
}

// ─────────────── (R4) a lapsed lease is refused, never revived ───────────────

/// **(R4)** The slot's lease lapsed before the next renewal: with a body read pending, the clock
/// moves past it and the timer fires. The renewal refuses, writes nothing, and staging stops with a
/// typed error naming the slot and the rule; the slot is exactly as reserved. A renewal that moved
/// the lapsed lease forward would stage the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_a_renewal_refuses_a_lapsed_slot_lease() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body_stream) = channel(&rig.log);
        tx.send(Bytes::from(body_bytes(10))).unwrap();
        let (log, clock, timer, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let ended = Arc::new(AtomicBool::new(false));
        let staging = flagging(
            Arc::clone(&ended),
            stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream),
        );
        let driver = async move {
            until("the second read to wait", waits_asked(&waits, 2)).await;
            clock.advance(TTL + 1);
            timer.add_permits(1);
            // End the body either way, so a staging that went on stages it and fails the leg.
            until("staging to stop, or to renew", || {
                ended.load(Ordering::SeqCst) || !log.commits(Kind::Renewal).is_empty()
            })
            .await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let reserved = rig.log.commits(Kind::Reserve).remove(0).batch.puts[0].clone();
        let err = outcome.expect_err("staging stopped");
        let text = err.to_string();
        let slot = String::from_utf8(reserved.0.clone()).unwrap();
        for part in [
            rig.upload.as_str(),
            slot.as_str(),
            "is not revived",
            "no batch of this part attempt is in doubt",
        ] {
            assert!(text.contains(part), "{part:?} in {text:?}");
        }
        let cause = stopped(Err(err));
        assert_eq!(lapsed(&cause), (reserved.0.clone(), T0 + TTL, T0 + TTL + 1));
        assert!(rig.log.commits(Kind::Renewal).is_empty(), "nothing written");
        assert_eq!(
            rig.store.inner.get(&reserved.0).await.unwrap(),
            Some(reserved.1),
            "the slot is exactly as reserved"
        );
    })
    .await;
}

/// **(R4)** An owned entry's lease lapsed, the slot's has not, before the next renewal. Chunk 1 is
/// registered at `T0` and chunk 2 at `T0 + 1,000`, so the slot's lease outlives chunk 1's. With the
/// next read pending the clock reaches chunk 1's lease and the timer fires: the renewal refuses,
/// naming that entry, and writes nothing; chunk 1's lease is not revived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_a_renewal_refuses_a_lapsed_owned_lease() {
    within(async {
        let mut rig = Rig::new().await;
        let (tx, body_stream) = channel(&rig.log);
        let body = body_bytes(2 * CHUNK);
        tx.send(Bytes::copy_from_slice(&body[..CHUNK])).unwrap();
        let (log, clock, timer, waits) = (
            rig.log.clone(),
            rig.clock.clone(),
            Arc::clone(&rig.runtime.timer),
            Arc::clone(&rig.runtime.waits),
        );
        let req = request(&rig.topology, &rig.upload, &rig.attempt);
        let ended = Arc::new(AtomicBool::new(false));
        let staging = flagging(
            Arc::clone(&ended),
            stage_part(&rig.store, &rig.fleet, &mut rig.runtime, &req, body_stream),
        );
        let second = Bytes::copy_from_slice(&body[CHUNK..]);
        let driver = async move {
            until("the read after chunk 1 to wait", waits_asked(&waits, 3)).await;
            clock.advance(1_000);
            tx.send(second).unwrap();
            // Chunk 2's writes, then the next read.
            until("the read after chunk 2 to wait", waits_asked(&waits, 5)).await;
            clock.set(T0 + TTL + 1);
            timer.add_permits(1);
            until("staging to stop, or to renew", || {
                ended.load(Ordering::SeqCst) || !log.commits(Kind::Renewal).is_empty()
            })
            .await;
            drop(tx);
        };
        let (outcome, ()) = tokio::join!(staging, driver);
        let cause = stopped(outcome);
        let chunk1 = sidx_key(&rig.upload, part(), 1);
        assert_eq!(lapsed(&cause), (chunk1, T0 + TTL + 1, T0 + TTL + 1));
        assert!(rig.log.commits(Kind::Renewal).is_empty(), "nothing written");
        let entries = owned(&rig.store.inner, &rig.upload).await;
        assert_eq!(
            entries[&1].lease_expiry_millis(),
            T0 + TTL + 1,
            "chunk 1's lease was not revived"
        );
        assert_eq!(entries[&2].lease_expiry_millis(), T0 + 1_000 + TTL);
    })
    .await;
}

/// **(R4)** Every renewal round re-reads the clock and re-checks every lease before it sends
/// (iteration-1 finding). The renewal's first round is lost — answered `Conflict` unapplied, or
/// held — slowly enough that the slot's lease lapses before the retry. The retry checks at its own
/// fresh reading and refuses: one renewal commit, the slot not revived. Held, the error keeps the
/// held round's `may_still_commit` doubt, and releasing that round lands it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_a_renewal_retry_refuses_a_lease_that_lapsed_between_rounds() {
    for held in [false, true] {
        within(async {
            let mut rig = Rig::new().await;
            // Renewal falls due at T0 + HALF + 1; the lost round moves the clock past T0 + TTL.
            let lost = if held {
                Act::Hold { advance: HALF }
            } else {
                Act::ConflictThenAdvance(HALF)
            };
            rig.store.script(Kind::Renewal, vec![lost], None);
            let body_stream = renewal_body(&rig.clock, &rig.log);
            let outcome = rig.stage(|_| {}, body_stream).await;
            let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
            let cause = match (held, outcome) {
                (false, outcome) => stopped(outcome),
                (true, outcome) => {
                    let doubt = in_doubt(outcome);
                    assert!(doubt.may_still_commit, "the held round's doubt is kept");
                    doubt.cause
                }
            };
            assert_eq!(
                lapsed(&cause),
                (slot.clone(), T0 + TTL, T0 + TTL + 1),
                "held {held}"
            );
            let renewals = rig.log.commits(Kind::Renewal);
            assert_eq!(renewals.len(), 1, "no retry was sent");
            assert_eq!(renewals[0].clock, T0 + HALF + 1);
            assert!(rig.log.commits(Kind::Intent).is_empty());
            let reserved = rig.log.commits(Kind::Reserve).remove(0).batch.puts[0].clone();
            assert_eq!(
                rig.store.inner.get(&slot).await.unwrap(),
                Some(reserved.1),
                "the slot was not revived"
            );
            if held {
                assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
            }
        })
        .await;
    }
}

/// **(R4)** A renewal after the session left `Open` (a real Abort fenced it) refuses and
/// resurrects nothing: its batch conflicts on the session fence, nothing is written, the session
/// stays `Aborting`, and staging answers `NoSuchUpload`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_a_renewal_after_the_session_left_open_resurrects_nothing() {
    within(async {
        let mut rig = Rig::new().await;
        let abort = Act::First(Side::Abort(rig.upload.clone()));
        rig.store.script(Kind::Renewal, vec![abort], None);
        let body_stream = renewal_body(&rig.clock, &rig.log);
        let outcome = rig.stage(|_| {}, body_stream).await;
        assert!(
            matches!(outcome, Ok(StageOutcome::Refused(Refusal::NoSuchUpload))),
            "{outcome:?}"
        );
        let answers: Vec<Answer> = rig
            .log
            .commits(Kind::Renewal)
            .iter()
            .map(|c| c.answer)
            .collect();
        assert_eq!(
            answers,
            vec![Answer::Conflict],
            "one renewal, and it wrote nothing"
        );
        assert!(matches!(rig.state().await, SessionState::Aborting {}));
        let reserved = rig.log.commits(Kind::Reserve).remove(0).batch.puts[0].clone();
        assert_eq!(
            rig.store.inner.get(&reserved.0).await.unwrap(),
            Some(reserved.1)
        );
        assert!(rig.log.commits(Kind::Intent).is_empty());
    })
    .await;
}

/// **(R4)** The slot is deleted from under a live attempt, the session unchanged, just before a
/// renewal commits: the renewal conflicts and staging stops with a typed error naming the slot,
/// nothing in doubt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_a_slot_lost_under_a_renewal_is_a_typed_error() {
    within(async {
        let mut rig = Rig::new().await;
        let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        let named = slot.clone();
        let delete: Hook = Arc::new(move |_| WriteBatch::new().delete(slot.clone()));
        rig.store
            .script(Kind::Renewal, vec![Act::First(Side::Batch(delete))], None);
        let body_stream = renewal_body(&rig.clock, &rig.log);
        let cause = stopped(rig.stage(|_| {}, body_stream).await);
        assert!(
            matches!(&cause, Stop::SlotLost { key } if key.as_bytes() == named.as_slice()),
            "{cause:?}"
        );
        assert_eq!(rig.log.commits(Kind::Renewal).len(), 1);
        assert!(rig.log.commits(Kind::Intent).is_empty());
    })
    .await;
}

/// How a renewal running beside chunk 1's writes stops staging, in the legs below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenewalStop {
    /// The wrapper answers its commit with a plain store fault, applying nothing.
    Fails,
    /// The clock is moved past the slot's lease before the renewal's first round.
    Lapsed,
    /// A real Abort fences the session just before the renewal commits.
    Refused,
    /// Its first round is held unapplied (it may still land) while the clock moves past the
    /// slot's lease, and its retry refuses the lapsed lease.
    HeldThenLapsed,
}

/// The cause staging reports for chunk 1's writes when a renewal beside them stopped it: the
/// writes' account, never the renewal's own stop, whatever the renewal came to.
fn assert_stopped_by(cause: &Stop, how: RenewalStop, slot: &[u8]) {
    let renewal = stopped_by_renewal(cause).expect("a renewal stopped staging");
    match how {
        RenewalStop::Fails => {
            assert!(matches!(renewal, Stop::Store(_)), "{how:?}: {renewal:?}");
        }
        RenewalStop::Lapsed | RenewalStop::HeldThenLapsed => {
            assert_eq!(
                lapsed(renewal),
                (slot.to_vec(), T0 + TTL + 1, T0 + TTL + 1),
                "{how:?}"
            );
        }
        RenewalStop::Refused => assert!(
            matches!(
                renewal,
                Stop::SessionLeftOpen {
                    refusal: Refusal::NoSuchUpload
                }
            ),
            "{how:?}: {renewal:?}"
        ),
    }
}

/// **(R4) A renewal that stops staging beside a pending write** (iteration-1 finding). Chunk 1's
/// put is held at its arrival, and its deadline (`T0 + TTL + 1,000`) is still ahead when the
/// renewal falls due and stops staging: it fails, it refuses a lapsed lease, it finds the session
/// gone from `Open` (a real Abort), or its first round is held and its retry refuses a lapsed
/// lease. Staging stops at once, before the put's deadline, and the stop is the write's account:
/// `WriteEffectUnknown`, classed `Indeterminate`, the held put abandoned in staging's own words,
/// and the renewal's cause carried in it. The held round keeps its `may_still_commit` doubt.
/// Staging never answers a determinate error here, nor `Refused(NoSuchUpload)`. Released after
/// staging returned, the put is published, so the doubt was real. A staging that let the
/// renewal's own error or refusal through, dropping the write unaccounted, fails every run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_a_renewal_stopping_beside_a_pending_write_leaves_its_effect_unknown() {
    for how in [
        RenewalStop::Fails,
        RenewalStop::Lapsed,
        RenewalStop::Refused,
        RenewalStop::HeldThenLapsed,
    ] {
        within(async {
            let mut rig = Rig::new().await;
            let held = FragmentId { chunk: 1, index: 0 };
            let put = rig.fleet.hold(held, HoldAt::Arrival);
            let plan = match how {
                RenewalStop::Fails => vec![Act::Fail],
                RenewalStop::Lapsed => Vec::new(),
                RenewalStop::Refused => vec![Act::First(Side::Abort(rig.upload.clone()))],
                // The renewal falls due at `T0 + HALF`; the held round leaves the clock at
                // `T0 + TTL + 1`, past the slot lease chunk 1's intent wrote.
                RenewalStop::HeldThenLapsed => vec![Act::Hold { advance: HALF + 1 }],
            };
            rig.store.script(Kind::Renewal, plan, None);
            let body = body_bytes(2 * CHUNK);
            let (log, clock, timer, fleet) = (
                rig.log.clone(),
                rig.clock.clone(),
                Arc::clone(&rig.runtime.timer),
                rig.fleet.clone(),
            );
            let deadline = T0 + TTL + 1_000;
            let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
            req.write_window_millis = TTL + 1_000;
            let ended = Arc::new(AtomicBool::new(false));
            let staging = flagging(
                Arc::clone(&ended),
                stage_part(
                    &rig.store,
                    &rig.fleet,
                    &mut rig.runtime,
                    &req,
                    stream_of(pieces(&body, &[2 * CHUNK])),
                ),
            );
            let driver = async move {
                until("chunk 1's held put", || {
                    fleet.holds_only(held, HoldAt::Arrival)
                })
                .await;
                if how == RenewalStop::Lapsed {
                    clock.set(T0 + TTL + 1);
                } else {
                    clock.advance(HALF);
                }
                timer.add_permits(1);
                until("staging to stop, the put still held", || {
                    ended.load(Ordering::SeqCst)
                })
                .await;
                assert!(!log.acked(held), "{how:?}: the put is still pending");
                assert!(
                    clock.now_millis() < deadline,
                    "{how:?}: stopped before the put's deadline"
                );
                put.add_permits(1);
                until("the abandoned put to publish", || {
                    log.has(|e| matches!(e, Event::Stored { id } if *id == held))
                })
                .await;
            };
            let (outcome, ()) = tokio::join!(staging, driver);
            let err = outcome.expect_err("never a refusal, never staged");
            assert_eq!(classify(&err), ErrorClass::Indeterminate, "{how:?}: {err}");
            let text = err.to_string();
            assert!(
                text.contains("a renewal beside them stopped staging"),
                "{how:?}: {text}"
            );
            let cause = if how == RenewalStop::HeldThenLapsed {
                let doubt = in_doubt::<StageOutcome>(Err(err));
                assert!(doubt.may_still_commit, "the held round's doubt is kept");
                assert_eq!(doubt.outcome.backend, "scripted");
                doubt.cause
            } else {
                stopped(Err(err))
            };
            let (chunk, deadline_millis, abandoned, reports) = effect_unknown(&cause);
            assert_eq!((chunk, deadline_millis), (1, deadline), "{how:?}");
            assert_abandoned_only(abandoned, held);
            assert!(
                abandoned[0]
                    .detail
                    .contains("a renewal of the attempt's leases stopped staging"),
                "{}",
                abandoned[0].detail
            );
            assert!(reports.is_empty(), "{how:?}");
            let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
            assert_stopped_by(&cause, how, &slot);
            assert_eq!(rig.log.put_count(held), 1, "{how:?}: never re-sent");
            assert_eq!(
                rig.log.commits(Kind::Intent).len(),
                1,
                "{how:?}: nothing staged after it"
            );
            if how == RenewalStop::Refused {
                assert!(matches!(rig.state().await, SessionState::Aborting {}));
            }
        })
        .await;
    }
}

/// **(R4) A renewal that stops staging after the writes answered** (iteration-1 finding). Chunk
/// 1's put is held inside its publishing step, and the renewal due at `T0 + HALF` waits at the
/// wrapper's gate. The put then answers while the renewal waits: `Ok`, or `WriteEffect::Unknown`,
/// the clock moved to the put's deadline first so its publication straddles it. Staging takes
/// the answer. Then the renewal is let through and fails (a plain store fault) or finds the
/// session gone (a real Abort). An `Unknown` answer already taken is kept: the stop is
/// `WriteEffectUnknown` carrying that report, nothing abandoned, the renewal's cause in it,
/// classed `Indeterminate`. With every write answered `Ok`, nothing is unsettled and the renewal's
/// own answer stands: its store fault, `Terminal`, or `Refused(NoSuchUpload)`. A staging that
/// dropped the taken report when the renewal failed answers those two runs determinately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r4_a_renewal_stopping_after_the_writes_answered_keeps_their_account() {
    for (answers_unknown, how) in [
        (true, RenewalStop::Fails),
        (true, RenewalStop::Refused),
        (false, RenewalStop::Fails),
        (false, RenewalStop::Refused),
    ] {
        within(async {
            let mut rig = Rig::new().await;
            let held = FragmentId { chunk: 1, index: 0 };
            let publishing = rig.fleet.hold(held, HoldAt::Publishing);
            let renewal_gate = Arc::new(Semaphore::new(0));
            let then = match how {
                RenewalStop::Refused => Act::First(Side::Abort(rig.upload.clone())),
                _ => Act::Fail,
            };
            rig.store.script(
                Kind::Renewal,
                vec![Act::Gated(Arc::clone(&renewal_gate), Box::new(then))],
                None,
            );
            let body = body_bytes(2 * CHUNK);
            let (log, clock, timer, fleet) = (
                rig.log.clone(),
                rig.clock.clone(),
                Arc::clone(&rig.runtime.timer),
                rig.fleet.clone(),
            );
            let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
            req.write_window_millis = WINDOW;
            let staging = stage_part(
                &rig.store,
                &rig.fleet,
                &mut rig.runtime,
                &req,
                stream_of(pieces(&body, &[2 * CHUNK])),
            );
            let driver = async move {
                until("chunk 1's put inside its publishing step", || {
                    fleet.holds_only(held, HoldAt::Publishing)
                })
                .await;
                clock.advance(HALF);
                timer.add_permits(1);
                until("the renewal at its gate", || log.gated(Kind::Renewal) == 1).await;
                if answers_unknown {
                    clock.set(T0 + WINDOW);
                }
                publishing.add_permits(1);
                until(
                    "staging to take the put's answer, the renewal still held",
                    || log.delivered(held),
                )
                .await;
                assert_eq!(
                    log.has(|e| matches!(e, Event::Unverified { id } if *id == held)),
                    answers_unknown
                );
                assert!(log.commits(Kind::Renewal).is_empty(), "the renewal waits");
                renewal_gate.add_permits(1);
            };
            let (outcome, ()) = tokio::join!(staging, driver);
            let run = format!("unknown {answers_unknown}, {how:?}");
            assert_eq!(rig.log.put_count(held), 1, "{run}: never re-sent");
            assert_eq!(rig.log.commits(Kind::Renewal).len(), 1, "{run}");
            if !answers_unknown {
                match how {
                    RenewalStop::Refused => assert!(
                        matches!(outcome, Ok(StageOutcome::Refused(Refusal::NoSuchUpload))),
                        "{run}: {outcome:?}"
                    ),
                    _ => {
                        let err = outcome.expect_err("the renewal's fault");
                        assert_eq!(classify(&err), ErrorClass::Terminal, "{run}: {err}");
                        assert!(matches!(stopped(Err(err)), Stop::Store(_)), "{run}");
                    }
                }
                return;
            }
            let err = outcome.expect_err("never a refusal, never staged");
            assert_eq!(classify(&err), ErrorClass::Indeterminate, "{run}: {err}");
            let cause = stopped(Err(err));
            let (chunk, deadline, abandoned, reports) = effect_unknown(&cause);
            assert_eq!((chunk, deadline), (1, T0 + WINDOW), "{run}");
            assert!(abandoned.is_empty(), "{run}: every write had answered");
            assert_eq!(reports.len(), 1, "{run}");
            let report = write_deadline_outcome(reports[0].as_ref()).expect("the store's report");
            assert_eq!((report.id, report.effect), (held, WriteEffect::Unknown));
            let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
            assert_stopped_by(&cause, how, &slot);
        })
        .await;
    }
}

// ─────────────── (R5) unknown renewal outcomes ───────────────

/// **(R5)** A renewal answered `CommitUnknownResult` is settled by re-reading its own slot's exact
/// bytes: one that landed is adopted (one commit, and the next batch requires its slot), and one
/// that did not land, with `may_still_commit: false`, is sent again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_an_unknown_renewal_is_settled_by_its_own_slot_bytes() {
    for (applied, may_still_commit, commits) in
        [(true, false, 1), (true, true, 1), (false, false, 2)]
    {
        within(async {
            let mut rig = Rig::new().await;
            rig.store.script(
                Kind::Renewal,
                vec![Act::Unknown {
                    applied,
                    may_still_commit,
                }],
                None,
            );
            let body_stream = renewal_body(&rig.clock, &rig.log);
            staged(rig.stage(|_| {}, body_stream).await);
            let renewals = renewals(&rig);
            let case = format!("applied {applied}, may {may_still_commit}");
            assert_eq!(renewals.len(), commits, "{case}");
            let landed = renewals.last().unwrap();
            let intent = &rig.log.commits(Kind::Intent)[0];
            assert_eq!(
                slot_required(&intent.batch),
                slot_value_put(&landed.batch),
                "{case}: the next batch requires the renewed slot"
            );
            assert_eq!(intent.answer, Answer::Committed);
        })
        .await;
    }
}

/// **(R5)** A held renewal that lands before the session is seen to move is adopted. The
/// renewal's first rounds conflict and its last is held; that round lands after its slot read and
/// just before the session moves to a later `Open` epoch. The renewal reads its slot again before
/// letting the doubt go, adopts that round, and staging goes on under the new session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_a_held_renewal_landing_before_the_session_moves_is_adopted() {
    within(async {
        let mut rig = Rig::new().await;
        let later = open_at(&rig.session, 2);
        let mut plan = vec![Act::Conflict; RENEWAL_ROUNDS as usize - 1];
        plan.push(Act::Hold { advance: 0 });
        rig.store.script(Kind::Renewal, plan, None);
        let slot = slot_key(&rig.upload, SlotIndex::new(0).unwrap());
        let move_session = WriteBatch::new().put(mpu_key(&rig.upload), later.clone());
        // Each lost round reads the slot once; fire after the last round's slot read.
        rig.store.after_read(
            &slot,
            RENEWAL_ROUNDS as usize - 1,
            vec![Then::ReleaseHeld, Then::Commit(move_session)],
        );
        let body_stream = renewal_body(&rig.clock, &rig.log);
        let attempt = staged(rig.stage(|_| {}, body_stream).await);
        let renewals = renewals(&rig);
        assert_eq!(
            renewals.len(),
            RENEWAL_ROUNDS as usize,
            "no round past the bound"
        );
        let landed = &renewals[RENEWAL_ROUNDS as usize - 1];
        let intent = &rig.log.commits(Kind::Intent)[0];
        assert_eq!(intent.answer, Answer::Committed);
        assert_eq!(
            slot_required(&intent.batch),
            slot_value_put(&landed.batch),
            "the next batch requires the adopted round's slot"
        );
        assert_eq!(attempt.session_bytes(), &later, "fenced on the new session");
    })
    .await;
}

/// **(R5)** A held renewal that never settles ends in doubt. The renewal's first round is held
/// (`may_still_commit: true`) and every later one is answered `Conflict` unapplied until the rounds
/// run out: an in-doubt error carrying `may_still_commit`, every round fenced on the same slot
/// bytes, so at most one can land; released afterwards, the held round lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r5_a_held_renewal_that_never_settles_ends_in_doubt() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(
            Kind::Renewal,
            vec![Act::Hold { advance: 0 }],
            Some(Act::Conflict),
        );
        let body_stream = renewal_body(&rig.clock, &rig.log);
        let doubt = in_doubt(rig.stage(|_| {}, body_stream).await);
        assert!(doubt.may_still_commit, "the doubt is kept");
        assert_eq!(doubt.outcome.backend, "scripted");
        assert!(
            matches!(doubt.cause, Stop::RenewalRoundsSpent { rounds } if rounds == RENEWAL_ROUNDS),
            "{:?}",
            doubt.cause
        );
        let renewals = rig.log.commits(Kind::Renewal);
        assert_eq!(renewals.len(), RENEWAL_ROUNDS as usize);
        let required: BTreeSet<Bytes> = renewals
            .iter()
            .map(|renewal| slot_required(&renewal.batch))
            .collect();
        assert_eq!(
            required.len(),
            1,
            "one required slot value across every round"
        );
        assert!(rig.log.commits(Kind::Intent).is_empty());
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
    })
    .await;
}

// ─────────────── (R6) the renewal loop is bounded ───────────────

/// **(R6)** Every renewal answered `Conflict` unapplied: exactly [`RENEWAL_ROUNDS`] commits, then
/// a typed error with nothing in doubt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r6_the_renewal_loop_is_bounded() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(Kind::Renewal, vec![], Some(Act::Conflict));
        let body_stream = renewal_body(&rig.clock, &rig.log);
        let cause = stopped(rig.stage(|_| {}, body_stream).await);
        assert!(
            matches!(cause, Stop::RenewalRoundsSpent { rounds } if rounds == RENEWAL_ROUNDS),
            "{cause:?}"
        );
        assert_eq!(
            rig.log.commits(Kind::Renewal).len(),
            RENEWAL_ROUNDS as usize
        );
        assert!(rig.log.commits(Kind::Intent).is_empty());
    })
    .await;
}

// ─────────────── (R7) the quiesce renewal ───────────────

/// The cause of a quiesce renewal that stopped with nothing in doubt.
fn renew_stopped(outcome: Result<RenewOutcome, StageError>) -> Stop {
    match outcome {
        Err(StageError::Stopped { cause, .. }) => cause,
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// Stage one chunk at `T0`: the slot and its owned entry leased to `T0 + TTL + 1`.
async fn staged_one_chunk(rig: &mut Rig) -> Box<StagedAttempt> {
    let attempt = staged(
        rig.stage(|_| {}, stream_of(vec![Bytes::from(body_bytes(CHUNK))]))
            .await,
    );
    assert_eq!(attempt.slot().lease_expiry_millis(), T0 + TTL + 1);
    attempt
}

/// Every lease of `attempt` is `lease`, and the store holds exactly its slot and entries.
async fn assert_attempt_leases(rig: &Rig, attempt: &StagedAttempt, lease: u64) {
    assert_eq!(attempt.slot().lease_expiry_millis(), lease);
    let inner = &rig.store.inner;
    assert_eq!(
        inner.get(attempt.slot_key()).await.unwrap().as_ref(),
        Some(attempt.slot_bytes())
    );
    for held in attempt.owned() {
        assert_eq!(held.entry().lease_expiry_millis(), lease);
        assert_eq!(
            inner.get(held.key()).await.unwrap().as_ref(),
            Some(held.bytes())
        );
    }
}

/// **(R7)** The quiesce renewal ([`StagedAttempt::renew`]) takes the caller's clock and reads it
/// every round. Its first round is lost while the clock moves on 1,000 ms; the retry stamps the
/// leases from its own reading, `T0 + 1,000 + TTL`, and its cover is judged at a reading after
/// that commit returned. A renewal handed one fixed reading would stamp `T0 + TTL + 2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_the_quiesce_renewal_reads_the_callers_clock_every_round() {
    within(async {
        let mut rig = Rig::new().await;
        let mut attempt = staged_one_chunk(&mut rig).await;
        rig.store
            .script(Kind::Renewal, vec![Act::ConflictThenAdvance(1_000)], None);
        let outcome = attempt
            .renew(&rig.store, &rig.runtime, COVER)
            .await
            .unwrap();
        let lease = T0 + 1_000 + TTL;
        assert_eq!(
            outcome,
            RenewOutcome::Covered {
                checked_at_millis: T0 + 1_000,
                lease_expiry_millis: lease
            }
        );
        let renewals = renewals(&rig);
        assert_eq!(renewals.len(), 2);
        assert_eq!(
            (renewals[0].clock, renewals[1].clock),
            (T0, T0 + 1_000),
            "a reading per round"
        );
        assert_attempt_leases(&rig, &attempt, lease).await;
    })
    .await;
}

/// **(R7)** The quiesce renewal refuses a lapsed lease rather than reviving it: its first round is
/// lost while the clock passes every lease; the retry, at its own reading, refuses with a typed
/// error, nothing more sent and nothing revived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_the_quiesce_renewal_refuses_a_lapsed_lease() {
    within(async {
        let mut rig = Rig::new().await;
        let mut attempt = staged_one_chunk(&mut rig).await;
        let unchanged = snapshot(&rig.store.inner).await;
        rig.store
            .script(Kind::Renewal, vec![Act::ConflictThenAdvance(2 * TTL)], None);
        let cause = renew_stopped(attempt.renew(&rig.store, &rig.runtime, COVER).await);
        let (key, lease, now) = lapsed(&cause);
        assert_eq!(key, attempt.slot_key());
        assert_eq!(lease, T0 + TTL + 1);
        assert_eq!(now, T0 + 2 * TTL);
        assert_eq!(rig.log.commits(Kind::Renewal).len(), 1, "no retry was sent");
        assert_eq!(
            snapshot(&rig.store.inner).await,
            unchanged,
            "nothing revived"
        );
    })
    .await;
}

/// **(R7)** The quiesce renewal never moves a lease backwards, and refuses once the session has
/// left `Open`. With the caller's clock read behind the leases, the lease moves one millisecond on,
/// never back; after an Abort it answers `NoSuchUpload` and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_the_quiesce_renewal_never_moves_a_lease_backwards_or_past_a_fence() {
    within(async {
        let mut rig = Rig::new().await;
        let mut attempt = staged_one_chunk(&mut rig).await;
        let before = attempt.slot().lease_expiry_millis();
        rig.clock.set(T0 - HALF);
        let outcome = attempt
            .renew(&rig.store, &rig.runtime, COVER)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RenewOutcome::Covered {
                checked_at_millis: T0 - HALF,
                lease_expiry_millis: before + 1
            },
            "one millisecond on, never back"
        );
        assert_attempt_leases(&rig, &attempt, before + 1).await;

        fence(&rig.store.inner, &rig.upload).await;
        let unchanged = snapshot(&rig.store.inner).await;
        rig.clock.set(T0 + 1);
        let outcome = attempt
            .renew(&rig.store, &rig.runtime, COVER)
            .await
            .unwrap();
        assert_eq!(outcome, RenewOutcome::Refused(Refusal::NoSuchUpload));
        assert_eq!(
            snapshot(&rig.store.inner).await,
            unchanged,
            "nothing resurrected"
        );
        assert_eq!(rig.log.commits(Kind::Renewal).len(), 2);
    })
    .await;
}

/// **(R7) Coverage at hand-off** (added 2026-10-22; `0016:1726-1728`). The quiesce renewal
/// succeeds only when, at a clock reading taken after its last commit returned, every lease it
/// wrote expires at least `COVER` later.
///
/// * (i) Its first commit returns `TTL − COVER + 1` late: that lease covers one millisecond too
///   little at the reading after it, so `renew` runs another round, whose commit is prompt, and
///   answers with a reading and an expiry at least `COVER` apart.
/// * (ii) Every commit returns that late: a typed error after exactly [`RENEWAL_ROUNDS`] commits,
///   the leases moved forward and no lease lapsed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_the_quiesce_renewal_covers_its_hand_off_or_refuses() {
    for every in [false, true] {
        within(async {
            let mut rig = Rig::new().await;
            let mut attempt = staged_one_chunk(&mut rig).await;
            // Past the staged leases, so each round's lease is its own reading plus a TTL.
            rig.clock.set(T0 + 100);
            let late = Act::PassThenAdvance(TTL - COVER + 1);
            let (plan, then) = if every {
                (vec![], Some(late))
            } else {
                (vec![late], None)
            };
            rig.store.script(Kind::Renewal, plan, then);
            let outcome = attempt.renew(&rig.store, &rig.runtime, COVER).await;
            let renewals = renewals(&rig);
            assert!(renewals.iter().all(|r| r.answer == Answer::Committed));
            let first = slot_put(&renewals[0].batch).lease_expiry_millis();
            assert_eq!(first, T0 + 100 + TTL);
            assert_eq!(
                first - (T0 + 100 + TTL - COVER + 1),
                COVER - 1,
                "the first round covers one millisecond too little"
            );
            if !every {
                assert_eq!(renewals.len(), 2, "it renewed again");
                let checked = T0 + 100 + TTL - COVER + 1;
                let lease = checked + TTL;
                assert_eq!(
                    outcome.unwrap(),
                    RenewOutcome::Covered {
                        checked_at_millis: checked,
                        lease_expiry_millis: lease
                    }
                );
                assert!(lease - checked >= COVER);
                assert_eq!(renewals[1].clock, checked, "read after the late commit");
                assert_attempt_leases(&rig, &attempt, lease).await;
            } else {
                assert_eq!(renewals.len(), RENEWAL_ROUNDS as usize, "exactly the bound");
                let cause = renew_stopped(outcome);
                let now = rig.clock.now_millis();
                assert!(
                    matches!(cause, Stop::CoverShort {
                        rounds, cover_millis, lease_expiry_millis, checked_at_millis
                    } if rounds == RENEWAL_ROUNDS
                        && cover_millis == COVER
                        && checked_at_millis == now
                        && lease_expiry_millis == now + COVER - 1),
                    "{cause:?}"
                );
                assert_live(&rig.store.inner, &rig.upload, now).await;
            }
        })
        .await;
    }
}

/// **(R7)** A cover not below the lease TTL is a typed input error, with nothing read or written:
/// no lease could cover it. One millisecond below the TTL is accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r7_a_cover_not_below_the_ttl_is_refused_writing_nothing() {
    within(async {
        let mut rig = Rig::new().await;
        let mut attempt = staged_one_chunk(&mut rig).await;
        assert_eq!(attempt.lease_ttl_millis(), TTL);
        let unchanged = snapshot(&rig.store.inner).await;
        let events = rig.log.events().len();
        for cover in [TTL, TTL + 1] {
            let cause = renew_stopped(attempt.renew(&rig.store, &rig.runtime, cover).await);
            assert!(matches!(cause, Stop::InvalidRequest { .. }), "{cause:?}");
        }
        assert_eq!(
            snapshot(&rig.store.inner).await,
            unchanged,
            "nothing written"
        );
        assert_eq!(rig.log.events().len(), events, "no commit sent");
        let outcome = attempt.renew(&rig.store, &rig.runtime, TTL - 1).await;
        assert!(
            matches!(outcome, Ok(RenewOutcome::Covered { .. })),
            "{outcome:?}"
        );
    })
    .await;
}

// ─────────────── (R8) every renewal batch fits `B_ops` ───────────────

/// **(R8)** Staging refuses, before reading or writing anything, a request whose chunk bound would
/// build a renewal batch past `MAX_BATCH_OPS`. A renewal of `N` owned entries carries `3 + 2N`
/// operations (counted on a real batch by `renewals`), so the widest bound that fits is
/// `(MAX_BATCH_OPS − 3) / 2`: one more is refused by `stage_part` and `reserve_slot` alike, and the
/// bound itself stages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r8_a_chunk_bound_past_a_renewal_batch_is_refused() {
    within(async {
        let mut rig = Rig::new().await;
        let fits = (MAX_BATCH_OPS - 3) / 2;
        assert!(3 + 2 * fits <= MAX_BATCH_OPS && 3 + 2 * (fits + 1) > MAX_BATCH_OPS);
        let before = snapshot(&rig.store.inner).await;
        let past = move |r: &mut StageRequest<'_>| r.max_part_chunks = fits + 1;
        let cause = stopped(
            rig.stage(past, stream_of(vec![Bytes::from_static(b"x")]))
                .await,
        );
        assert!(
            matches!(cause, Stop::InvalidRequest { reason } if reason.contains("MAX_BATCH_OPS")),
            "{cause:?}"
        );
        let mut req = request(&rig.topology, &rig.upload, &rig.attempt);
        past(&mut req);
        assert!(
            matches!(
                reserve_slot(&rig.store, &req, T0).await,
                Err(StageError::Stopped {
                    cause: Stop::InvalidRequest { .. },
                    ..
                })
            ),
            "reserve_slot refuses it too"
        );
        assert_eq!(snapshot(&rig.store.inner).await, before, "nothing written");
        assert!(rig.log.events().is_empty(), "no commit, no put");
        let attempt = staged(
            rig.stage(
                move |r| r.max_part_chunks = fits,
                stream_of(vec![Bytes::from(body_bytes(CHUNK))]),
            )
            .await,
        );
        assert_eq!(
            attempt.chunks().len(),
            1,
            "the widest bound that fits stages"
        );
    })
    .await;
}
