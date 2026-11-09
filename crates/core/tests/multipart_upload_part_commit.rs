//! Issue #1027 (657.2a, the first of the part commit's four slices) — `UploadPart`'s **commit
//! half**, `wyrd_core::multipart::commit::{commit_part, upload_part}`, for every case where no
//! commit outcome needs classifying, driven over a real store (`RedbMetadataStore::in_memory()`), a
//! placement-aware chunk-store double, and a store wrapper that logs every batch and read and can
//! answer a batch `Conflict` or `CommitUnknownResult` (applied or not, either flag), hold one and
//! land it later, or run an action or move the clock beside a batch or a read (proposal 0016's
//! *Part commit* and *Live-session compensation* rows, `0016:660`, `:673`; `0016:1420-1431`,
//! `:1720-1733`). Every session is made by Create (#894), every part staged by `stage_part`
//! (#932/#933), and every Abort is a real one (#895).
//!
//! * **(A)** parts committed out of order, the `part:`/`psum:` pair agreeing, the attempt's staging
//!   records gone, and the last renewal before the commit's reads.
//! * **(D)** re-upload, empty parts, the per-part ceiling and the cumulative ceiling.
//! * **(F)** a commit is sent only with the dispatch margin left.
//! * **(G)** a settled stop before any commit compensates, and the others do not.
//! * **(H2)** a damaged session a renewal adopted is never compensated under.
//! * **(H3)** the slot guard names the submitter.
//! * **(H4)** a batch answered unknown is the last one sent: a compensation's stops in doubt, and a
//!   commit's is settled from reads alone (its leg reworked by #1029, below).
//!
//! Issue #1028 (657.2b, the second slice) appends what a `Conflict` means for a commit and for a
//! compensation (`0016:1584-1653`, `:673`, `:674`), every leg with nothing in doubt:
//!
//! * **(B)** a lost commit is classified from a re-read, never compensated blind: a fence, a
//!   rollback, a same-number rival, a repair, and a conflict no read explains.
//! * **(T)** a terminal delete is not damage, and a slot behind a missing session is.
//! * **(R)** damage is a record stop, never a rival or a fence.
//! * **(O)** a taken obligation key, under a re-upload with chunks and an empty one.
//! * **(L)** the loop bounds, the epoch chase under every charged Complete fence, and the lease
//!   between rounds.
//!
//! For those the wrapper can also run **an action between two reads** ([`Step::AfterRead`]): a
//! step beside a batch arms it, and it runs right after the next read of its key, so a delete can
//! land between a re-read's slot read and its session read.
//!
//! Issue #1029 (657.2c, the third slice) appends what a commit's `CommitUnknownResult` means
//! (`0016:1630-1651`, `:1780`, `:1792`): no further batch after it, neither the commit again nor a
//! compensation, then reads that answer `Committed` only on the attempt's own-commit proof and
//! otherwise keep the doubt.
//!
//! * **(B)** settling: a landed commit is `Committed`; an unlanded re-upload, a held commit, a
//!   same-number rival and a changed prior are not; and the read-gap leg.
//! * **(X)** late staging rebuilds the slot of a commit that landed, and **(X2)** a same-number
//!   empty rival then rebuilds an empty prior: no compensation, and no second commit.
//! * **(C)** the regressions: an empty part's bytes, a torn read, a landed commit then a rival, one
//!   batch then reads, a conflict then a held round, read faults, a slot rewritten or gone.
//! * **(F)** a commit delayed past its lease; **(H)** a held commit and a rollback; **(R)** damage
//!   under a held commit.
//!
//! For those the wrapper can also land a held batch, or start failing reads, from a step, and a
//! leg can run steps at any gap between a re-read's four reads ([`Rig::at_gaps`]).
//!
//! The wrapper is handed each batch's submitter apart from the batch, by the [`As`] view each
//! attempt's store calls go through. On **every** batch of every leg it checks that a batch naming a
//! `slot:` key is either that submitter's reservation or requires exactly the bytes the submitter
//! last landed there; that a part commit is sent with the dispatch margin left on the lease the
//! submitter last wrote; that no submitter sends more part commits or compensations than
//! [`MOST_ROUNDS`]; and that every value a batch puts decodes back unchanged.
//!
//! The wrapper keeps its state in struct fields, never in a `static` (ADR-0035).

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, Stream};
use sha2::{Digest as _, Sha256};
use tokio::sync::Semaphore;
use wyrd_core::metadata::{self, EcScheme, InodeId, SegmentNonce};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::commit::{
    commit_part, upload_part, CommitError, DoubtfulBatch, InDoubt, Stop, UploadPartError,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::stage::{
    reserve_slot, stage_part, StageClock, StageError, StageOutcome, StageRequest, StageRuntime,
    StagedAttempt, Stop as StageStop,
};
use wyrd_core::multipart::{
    decode_owned_entry, decode_part_record, decode_part_summary, decode_retire_obligation,
    decode_session_record, decode_slot_record, mpu_key, parse_sidx_key, part_key, psum_key,
    psum_range, retire_key, sidx_range, slot_key, slot_range, AbortOutcome, AttemptId, Budget,
    CreateOutcome, Digest, PartNumber, PartRecord, PartSummary, RecordError, Refusal,
    ReserveOutcome, RetireMode, RetireToken, SessionState, SlotIndex, UploadId, UploadPartOutcome,
    MAX_INFLIGHT_PARTS, MAX_PART_CHUNKS, MAX_STAGED_CHUNKS, PART_PREFIX, PSUM_PREFIX,
    RETIRE_BYTES_PREFIX, SIDX_PREFIX, SLOT_PREFIX,
};
use wyrd_core::placement::Topology;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::{Clock, ManualClock};
use wyrd_traits::{
    classify, BoxError, ChunkId, ChunkStore, CommitOutcome, CommitUnknownResult, DServerId,
    ErrorClass, FragmentId, Health, MetadataStore, PlacementChunkStore, Result as StoreResult,
    ScanPage, WriteBatch, WriteDeadlineExpired,
};

// ─────────────────────────────── fixtures ───────────────────────────────

const BUCKET: &str = "photos";
/// The root inode every Alpha object dirent lives under, as `<bucket>/<key>` (ADR-0046).
const ROOT: InodeId = 0;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
/// No leg may hang the suite. The longest leg stages a few hundred chunks.
const LEG_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a driver waits for a commit to reach a point it is waiting for.
const WAIT_FOR: Duration = Duration::from_secs(10);
/// The test clock's first reading. A leg that does not move it stamps everything the same
/// millisecond.
const T0: u64 = 1_000_000;
/// The lease TTL every attempt is staged with: above the dispatch margin, which it must exceed.
const TTL: u64 = 10_000;
/// The lease a part commit must have left when it is sent, written here rather than imported so
/// that changing the production value fails this file.
const MARGIN: u64 = 5_000;
const _: () = assert!(MARGIN < TTL);
/// The most part commits, and the most compensations, one attempt may send (`COMMIT_ROUNDS`,
/// `COMPENSATION_ROUNDS`), written here rather than imported. The wrapper fails the leg at the first
/// batch past it, so a loop that never reaches its bound fails there, not at [`LEG_TIMEOUT`].
const MOST_ROUNDS: u32 = 4;
/// `W_write`.
const W: u64 = 3_000;
/// The body-idle bound: never reached, since no leg moves the clock while staging.
const IDLE: u64 = 100 * TTL;
/// Bytes per chunk.
const CHUNK: usize = 64;
/// Six fragments per chunk.
const RS42: EcScheme = EcScheme::ReedSolomon { k: 4, m: 2 };
/// Twelve D servers, each its own failure domain.
const SERVERS: u64 = 12;

/// Run `leg` under [`LEG_TIMEOUT`].
async fn within<T>(leg: impl Future<Output = T>) -> T {
    tokio::time::timeout(LEG_TIMEOUT, leg)
        .await
        .expect("the leg settled in time")
}

/// Wait until `ready` holds, letting the other futures of the leg run in between.
async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    while !ready() {
        assert!(start.elapsed() < WAIT_FOR, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// A profile admitting 8 sessions. Create stores it; the commit never reads it.
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

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
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

fn topology() -> Topology {
    let mut topology = Topology::default();
    for id in 0..SERVERS {
        topology.register(id, format!("d{id:02}"));
    }
    topology
}

/// `len` deterministic body bytes, distinct per `seed`.
fn body(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn sha256(bytes: &[u8]) -> Digest {
    Digest::from_bytes(Sha256::digest(bytes).into())
}

type Body = Pin<Box<dyn Stream<Item = StoreResult<Bytes>> + Send>>;

fn stream_of(body: &[u8]) -> Body {
    let pieces = if body.is_empty() {
        Vec::new()
    } else {
        vec![Ok(Bytes::copy_from_slice(body))]
    };
    Box::pin(stream::iter(pieces))
}

fn unknown(may_still_commit: bool) -> BoxError {
    Box::new(CommitUnknownResult {
        backend: "scripted",
        code: None,
        detail: "scripted unknown outcome".into(),
        may_still_commit,
    })
}

// ─────────────────────────────── the log ───────────────────────────────

/// Which batch a commit is, told apart by what it writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    /// `require_absent(slot:..)` and a slot put.
    Reserve,
    /// `require_absent(sidx:..)`.
    Intent,
    /// Puts the slot and owned entries, no `require_absent`: a renewal.
    Renewal,
    /// Puts a `part:` record.
    Commit,
    /// Deletes a slot and puts no `part:`: the live-session compensation.
    Compensation,
    Other,
}

fn kind_of(batch: &WriteBatch) -> Kind {
    let absent = |prefix: &[u8]| {
        batch
            .preconditions
            .iter()
            .any(|p| p.expected.is_none() && p.key.starts_with(prefix))
    };
    let puts = |prefix: &[u8]| batch.puts.iter().any(|(key, _)| key.starts_with(prefix));
    let deletes = |prefix: &[u8]| batch.deletes.iter().any(|key| key.starts_with(prefix));
    if puts(PART_PREFIX) {
        Kind::Commit
    } else if deletes(SLOT_PREFIX) {
        Kind::Compensation
    } else if absent(SIDX_PREFIX) {
        Kind::Intent
    } else if absent(SLOT_PREFIX) {
        Kind::Reserve
    } else if puts(SLOT_PREFIX) || puts(SIDX_PREFIX) {
        Kind::Renewal
    } else {
        Kind::Other
    }
}

#[derive(Debug, Clone)]
enum Event {
    /// A batch through the wrapper, logged when it is answered.
    Commit { kind: Kind, batch: WriteBatch },
    /// A read through the wrapper: a `get`'s key or a `scan`'s prefix.
    Read { key: Vec<u8> },
    /// A batch of `kind` reached its gate.
    Gated { kind: Kind },
}

#[derive(Debug, Clone, Default)]
struct Log(Arc<Mutex<Vec<Event>>>);

impl Log {
    fn push(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }

    fn events(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    /// Every batch of `of`, in order.
    fn commits(&self, of: Kind) -> Vec<WriteBatch> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                Event::Commit { kind, batch } if kind == of => Some(batch),
                _ => None,
            })
            .collect()
    }

    /// The keys read through the wrapper after the `nth` batch of `of` was answered (from 0), up to
    /// the next batch: that batch's re-read, in order.
    fn reads_after(&self, of: Kind, nth: usize) -> Vec<Vec<u8>> {
        let events = self.events();
        let at = events
            .iter()
            .enumerate()
            .filter(|(_, event)| matches!(event, Event::Commit { kind, .. } if *kind == of))
            .nth(nth)
            .map(|(at, _)| at)
            .expect("that batch was sent");
        events[at + 1..]
            .iter()
            .take_while(|event| !matches!(event, Event::Commit { .. }))
            .filter_map(|event| match event {
                Event::Read { key } => Some(key.clone()),
                _ => None,
            })
            .collect()
    }

    fn gated(&self, of: Kind) -> usize {
        self.events()
            .iter()
            .filter(|event| matches!(event, Event::Gated { kind } if *kind == of))
            .count()
    }
}

// ──────────────────────── the scripted store wrapper ────────────────────────

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
/// Something the wrapper runs on the inner store beside a batch or a read.
type Action = Arc<dyn Fn() -> BoxFuture + Send + Sync>;

/// An action that runs `future` the one time it is called.
fn once(future: impl Future<Output = ()> + Send + 'static) -> Action {
    let slot: Mutex<Option<BoxFuture>> = Mutex::new(Some(Box::pin(future)));
    Arc::new(move || slot.lock().unwrap().take().expect("an action runs once"))
}

/// One thing the wrapper does beside a batch or a read.
#[derive(Clone)]
enum Step {
    /// Run an action on the inner store.
    Run(Action),
    /// Move the test clock forward.
    Advance(u64),
    /// **An action between two reads**: run these steps right after the next `get` of this key
    /// through the wrapper has read the inner store, before that read is answered and so before
    /// the next read is made. Armed when the step runs, so a step beside a batch arms it for the
    /// re-read that follows that batch.
    AfterRead(Vec<u8>, Vec<Step>),
}

/// How the wrapper answers one batch.
#[derive(Clone)]
enum How {
    /// Delegate unchanged.
    Pass,
    /// Answer `Conflict`, applying nothing (TiKV's lock conflict).
    Conflict,
    /// Apply the batch or not, then answer `CommitUnknownResult { may_still_commit }`.
    Unknown {
        applied: bool,
        may_still_commit: bool,
    },
    /// Keep the batch unapplied, answer `CommitUnknownResult { may_still_commit: true }`, and land
    /// it when the test releases it.
    Hold,
    /// Wait for a permit on the gate, then delegate.
    Gate(Arc<Semaphore>),
    /// Fail definitely, applying nothing.
    Fail,
}

#[derive(Clone)]
struct Act {
    before: Vec<Step>,
    how: How,
    after: Vec<Step>,
}

impl Act {
    fn new(how: How) -> Self {
        Self {
            before: Vec::new(),
            how,
            after: Vec::new(),
        }
    }

    fn pass() -> Self {
        Self::new(How::Pass)
    }

    fn conflict() -> Self {
        Self::new(How::Conflict)
    }

    fn unknown(applied: bool, may_still_commit: bool) -> Self {
        Self::new(How::Unknown {
            applied,
            may_still_commit,
        })
    }

    fn hold() -> Self {
        Self::new(How::Hold)
    }

    fn fail() -> Self {
        Self::new(How::Fail)
    }

    fn before(mut self, step: Step) -> Self {
        self.before.push(step);
        self
    }

    fn after(mut self, step: Step) -> Self {
        self.after.push(step);
        self
    }
}

#[derive(Default)]
struct ScriptState {
    plans: HashMap<Kind, (VecDeque<Act>, Option<Act>)>,
    held: VecDeque<(AttemptId, WriteBatch)>,
    /// How far each read through the wrapper moves the test clock.
    read_tick: u64,
    /// Steps to run after the next `scan`: the commit's cumulative read.
    scan_steps: Vec<Step>,
    /// Keys, or scan prefixes, whose reads answer a store fault.
    read_faults: Vec<Vec<u8>>,
    /// Steps armed by [`Step::AfterRead`], each to run once after the next `get` of its key.
    read_hooks: Vec<(Vec<u8>, Vec<Step>)>,
    /// The slot bytes each attempt holds from its own last landed write, per slot key; gone once
    /// that attempt's own batch released the slot. Recorded only from the attempt's own batches as
    /// they landed, so another attempt's write to the key never changes it.
    slots: HashMap<(Vec<u8>, AttemptId), Bytes>,
    /// Every batch that landed through the wrapper, held ones released included, with its
    /// submitter.
    landed: Vec<(AttemptId, WriteBatch)>,
    /// How many part commits and how many compensations each submitter has sent.
    sent: HashMap<(AttemptId, Kind), u32>,
}

struct Scripted {
    inner: Arc<RedbMetadataStore>,
    clock: ManualClock,
    log: Log,
    state: Mutex<ScriptState>,
}

/// Every value a batch puts decodes back unchanged: a slot, an owned entry, a part, a summary, a
/// retirement obligation (decoded against its own key, so an empty `{chunks}` is refused here).
fn assert_values_decode(batch: &WriteBatch) {
    for (key, value) in &batch.puts {
        if key.starts_with(SLOT_PREFIX) {
            let record = decode_slot_record(value).expect("every slot value decodes");
            assert_eq!(metadata::encode(&record), *value);
        } else if key.starts_with(SIDX_PREFIX) {
            decode_owned_entry(key, value).expect("every owned entry decodes against its key");
        } else if key.starts_with(PART_PREFIX) {
            let record = decode_part_record(value).expect("every part record decodes");
            assert_eq!(metadata::encode(&record), *value, "a part re-encodes");
        } else if key.starts_with(PSUM_PREFIX) {
            let summary = decode_part_summary(value).expect("every summary decodes");
            assert_eq!(metadata::encode(&summary), *value);
        } else if key.starts_with(RETIRE_BYTES_PREFIX) {
            let (mode, _, _) = decode_retire_obligation(key, value)
                .expect("every obligation decodes against its own key");
            assert_eq!(mode, RetireMode::Bytes);
        }
    }
}

impl Scripted {
    /// `kind`'s batches follow `plan`, then `then` (`Pass` when unset).
    fn script(&self, kind: Kind, plan: Vec<Act>, then: Option<Act>) {
        self.state
            .lock()
            .unwrap()
            .plans
            .insert(kind, (plan.into(), then));
    }

    /// Every read through the wrapper from now on moves the clock by `millis`.
    fn tick_reads(&self, millis: u64) {
        self.state.lock().unwrap().read_tick = millis;
    }

    /// After the next `scan`, run `steps`.
    fn after_scan(&self, steps: Vec<Step>) {
        self.state.lock().unwrap().scan_steps = steps;
    }

    /// Reads of `key` (a `get`'s key or a `scan`'s prefix) answer a store fault.
    fn fail_reads(&self, key: Vec<u8>) {
        self.state.lock().unwrap().read_faults.push(key);
    }

    fn read_faults(&self, key: &[u8]) -> bool {
        self.state
            .lock()
            .unwrap()
            .read_faults
            .iter()
            .any(|fault| fault == key)
    }

    /// Land the oldest held batch now; what it met.
    async fn release_held(&self) -> CommitOutcome {
        let (submitter, held) = self
            .state
            .lock()
            .unwrap()
            .held
            .pop_front()
            .expect("a batch is held");
        let outcome = self.inner.commit(held.clone()).await.unwrap();
        if outcome == CommitOutcome::Committed {
            self.landed(&submitter, &held);
        }
        outcome
    }

    /// Record a batch of `submitter` that landed: the slot bytes it now holds, or that it holds
    /// none any more.
    fn landed(&self, submitter: &AttemptId, batch: &WriteBatch) {
        let mut state = self.state.lock().unwrap();
        for (key, value) in &batch.puts {
            if key.starts_with(SLOT_PREFIX) {
                state
                    .slots
                    .insert((key.clone(), submitter.clone()), value.clone());
            }
        }
        for key in &batch.deletes {
            if key.starts_with(SLOT_PREFIX) {
                state.slots.remove(&(key.clone(), submitter.clone()));
            }
        }
        state.landed.push((submitter.clone(), batch.clone()));
    }

    /// **The slot guard**, on every batch: a batch naming a `slot:` key is checked against
    /// `submitter`, the attempt that sent it, which the wrapper knows apart from the batch and from
    /// whoever occupies the slot now. Two shapes, chosen by what the submitter holds from its own
    /// last landed write there:
    ///
    /// * holding nothing, it can only be **a reservation**: the slot required absent, and the value
    ///   it puts naming the submitter's `attempt_id`;
    /// * holding bytes, it must require **exactly those bytes**.
    fn check_slot_guard(&self, submitter: &AttemptId, batch: &WriteBatch) -> Result<(), String> {
        let keys: BTreeSet<&Vec<u8>> = batch
            .preconditions
            .iter()
            .map(|p| &p.key)
            .chain(batch.puts.iter().map(|(key, _)| key))
            .chain(batch.deletes.iter())
            .filter(|key| key.starts_with(SLOT_PREFIX))
            .collect();
        let state = self.state.lock().unwrap();
        for key in keys {
            let name = String::from_utf8_lossy(key);
            let required = batch
                .preconditions
                .iter()
                .find(|p| p.key == *key)
                .ok_or_else(|| format!("{submitter}'s batch names {name} without requiring it"))?;
            match state.slots.get(&(key.clone(), submitter.clone())) {
                Some(held) => {
                    if required.expected.as_ref() != Some(held) {
                        return Err(format!(
                            "{submitter}'s batch requires {name} at bytes it did not last write"
                        ));
                    }
                }
                None => {
                    if required.expected.is_some() {
                        return Err(format!(
                            "{submitter} holds nothing at {name}, yet its batch requires bytes there"
                        ));
                    }
                    let put = batch
                        .puts
                        .iter()
                        .find(|(put, _)| put == key)
                        .ok_or_else(|| {
                            format!("{submitter}'s reservation of {name} puts nothing")
                        })?;
                    let named = decode_slot_record(&put.1).unwrap().attempt_id().clone();
                    if named != *submitter {
                        return Err(format!(
                            "{submitter}'s reservation of {name} puts a value naming {named}"
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// **The dispatch margin**, on every part commit of every leg: the lease `submitter` last wrote
    /// has at least [`MARGIN`] left at the moment the batch is sent.
    fn check_margin(&self, submitter: &AttemptId, batch: &WriteBatch) {
        let now = self.clock.now_millis();
        let state = self.state.lock().unwrap();
        let held = batch
            .deletes
            .iter()
            .filter(|key| key.starts_with(SLOT_PREFIX))
            .map(|key| {
                state
                    .slots
                    .get(&(key.clone(), submitter.clone()))
                    .expect("a part commit names the slot its submitter holds")
            })
            .next()
            .expect("a part commit releases a slot");
        let lease = decode_slot_record(held).unwrap().lease_expiry_millis();
        assert!(
            lease.saturating_sub(now) >= MARGIN,
            "a part commit was sent with {} ms left on its lease",
            lease.saturating_sub(now)
        );
    }

    /// **The loop bounds**, on every part commit and compensation of every leg: `submitter` sends
    /// at most [`MOST_ROUNDS`] of each.
    fn check_rounds(&self, submitter: &AttemptId, kind: Kind) {
        let mut state = self.state.lock().unwrap();
        let sent = state.sent.entry((submitter.clone(), kind)).or_default();
        *sent += 1;
        assert!(
            *sent <= MOST_ROUNDS,
            "{submitter} sent {sent} batches of {kind:?}, past the bound of {MOST_ROUNDS}"
        );
    }

    /// How many landed batches of `submitter` deleted `key`.
    fn releases(&self, submitter: &AttemptId, key: &[u8]) -> usize {
        self.state
            .lock()
            .unwrap()
            .landed
            .iter()
            .filter(|(by, batch)| by == submitter && batch.deletes.iter().any(|d| d == key))
            .count()
    }

    async fn run(&self, steps: &[Step]) {
        for step in steps {
            match step {
                Step::Run(action) => action().await,
                Step::Advance(millis) => self.clock.advance(*millis),
                Step::AfterRead(key, then) => self
                    .state
                    .lock()
                    .unwrap()
                    .read_hooks
                    .push((key.clone(), then.clone())),
            }
        }
    }

    fn on_read(&self, key: &[u8]) {
        self.log.push(Event::Read { key: key.to_vec() });
        self.clock.advance(self.state.lock().unwrap().read_tick);
    }

    /// The steps armed for the read of `key` just made, if any; disarmed as they are taken.
    fn take_read_hook(&self, key: &[u8]) -> Option<Vec<Step>> {
        let mut state = self.state.lock().unwrap();
        let at = state
            .read_hooks
            .iter()
            .position(|(armed, _)| armed == key)?;
        Some(state.read_hooks.remove(at).1)
    }

    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        if self.read_faults(key) {
            return Err("scripted read fault".into());
        }
        let found = self.inner.get(key).await;
        self.on_read(key);
        if let Some(then) = self.take_read_hook(key) {
            self.run(&then).await;
        }
        found
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        if self.read_faults(prefix) {
            return Err("scripted read fault".into());
        }
        let found = self.inner.scan(prefix).await;
        self.on_read(prefix);
        let steps = std::mem::take(&mut self.state.lock().unwrap().scan_steps);
        self.run(&steps).await;
        found
    }

    async fn commit(&self, submitter: &AttemptId, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        let kind = kind_of(&batch);
        assert_values_decode(&batch);
        self.check_slot_guard(submitter, &batch)
            .expect("the slot guard");
        if kind == Kind::Commit {
            self.check_margin(submitter, &batch);
        }
        if matches!(kind, Kind::Commit | Kind::Compensation) {
            self.check_rounds(submitter, kind);
        }
        let act = {
            let mut state = self.state.lock().unwrap();
            match state.plans.get_mut(&kind) {
                Some((plan, then)) => plan.pop_front().or_else(|| then.clone()),
                None => None,
            }
            .unwrap_or_else(Act::pass)
        };
        self.run(&act.before).await;
        let result = match act.how {
            How::Pass => self.delegate(submitter, &batch).await,
            How::Conflict => {
                tokio::task::yield_now().await;
                Ok(CommitOutcome::Conflict)
            }
            How::Unknown {
                applied,
                may_still_commit,
            } => {
                if applied {
                    let landed = self.delegate(submitter, &batch).await?;
                    assert_eq!(landed, CommitOutcome::Committed, "the scripted batch lands");
                }
                Err(unknown(may_still_commit))
            }
            How::Hold => {
                self.state
                    .lock()
                    .unwrap()
                    .held
                    .push_back((submitter.clone(), batch.clone()));
                Err(unknown(true))
            }
            How::Fail => Err("scripted store fault".into()),
            How::Gate(gate) => {
                self.log.push(Event::Gated { kind });
                gate.acquire().await.expect("the gate stays open").forget();
                self.delegate(submitter, &batch).await
            }
        };
        self.run(&act.after).await;
        self.log.push(Event::Commit { kind, batch });
        result
    }

    async fn delegate(
        &self,
        submitter: &AttemptId,
        batch: &WriteBatch,
    ) -> StoreResult<CommitOutcome> {
        let result = self.inner.commit(batch.clone()).await;
        if matches!(result, Ok(CommitOutcome::Committed)) {
            self.landed(submitter, batch);
        }
        result
    }
}

/// The wrapper, as seen by one attempt: every batch through it carries that attempt as its
/// submitter, apart from the batch's own bytes.
struct As {
    store: Arc<Scripted>,
    attempt: AttemptId,
}

#[async_trait]
impl MetadataStore for As {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.store.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        self.store.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<ScanPage> {
        self.store.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        self.store.commit(&self.attempt, batch).await
    }
}

// ──────────────────────── the chunk-store double ────────────────────────

/// In-process D servers addressed by stable id, enforcing a put's deadline at its publication
/// point as `ChunkStore::put_fragment` requires.
#[derive(Clone)]
struct Fleet(Arc<FleetState>);

struct FleetState {
    servers: Mutex<HashMap<DServerId, HashMap<FragmentId, Bytes>>>,
    clock: ManualClock,
}

impl Fleet {
    fn new(clock: &ManualClock) -> Self {
        Self(Arc::new(FleetState {
            servers: Mutex::new((0..SERVERS).map(|id| (id, HashMap::new())).collect()),
            clock: clock.clone(),
        }))
    }

    /// Every chunk id a fragment of which is stored anywhere.
    fn chunks(&self) -> BTreeSet<ChunkId> {
        self.0
            .servers
            .lock()
            .unwrap()
            .values()
            .flat_map(|held| held.keys().map(|id| id.chunk).collect::<Vec<_>>())
            .collect()
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
        Err("Fleet: a staged write must address a D server".into())
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
        if let Some(deadline) = deadline_millis {
            if let Some(refused) =
                WriteDeadlineExpired::if_elapsed(id, deadline, self.0.clock.now_millis())
            {
                return Err(Box::new(refused));
            }
        }
        self.0
            .servers
            .lock()
            .unwrap()
            .get_mut(&dserver)
            .ok_or_else(|| format!("Fleet: unknown D server {dserver}"))?
            .insert(id, fragment);
        Ok(())
    }
}

// ─────────────────────────────── the runtime ───────────────────────────────

/// The test clock as a [`StageClock`], for a rival's commit on the inner store.
struct At(ManualClock);

impl StageClock for At {
    fn now_millis(&self) -> u64 {
        self.0.now_millis()
    }
}

/// The clock every attempt is staged and committed on, and a timer that never fires: no leg moves
/// the clock while staging, so no renewal falls due and no staging bound is reached.
struct Runtime {
    clock: ManualClock,
    next_chunk: Arc<AtomicU64>,
}

impl StageClock for Runtime {
    fn now_millis(&self) -> u64 {
        self.clock.now_millis()
    }
}

impl StageRuntime for Runtime {
    fn next_chunk_id(&mut self) -> ChunkId {
        ChunkId::from(self.next_chunk.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn wait(&self, _millis: u64) -> impl Future<Output = ()> + Send {
        std::future::pending()
    }
}

// ─────────────────────────────── the rig ───────────────────────────────

/// The request every attempt is staged with, at the shipped per-part bound.
fn request<'a>(
    upload: &'a UploadId,
    topology: &'a Topology,
    attempt: &'a AttemptId,
    n: u32,
) -> StageRequest<'a> {
    StageRequest {
        upload_id: upload,
        part_number: part(n),
        attempt_id: attempt,
        max_inflight_parts: MAX_INFLIGHT_PARTS,
        lease_ttl_millis: TTL,
        chunk_size: CHUNK,
        scheme: RS42,
        topology,
        write_window_millis: W,
        max_part_chunks: MAX_PART_CHUNKS,
        body_idle_millis: IDLE,
    }
}

struct Rig {
    clock: ManualClock,
    log: Log,
    inner: Arc<RedbMetadataStore>,
    store: Arc<Scripted>,
    fleet: Fleet,
    runtime: Runtime,
    topology: Topology,
    upload: UploadId,
    next_attempt: u128,
}

impl Rig {
    async fn new() -> Self {
        let clock = ManualClock::new(T0);
        let log = Log::default();
        let inner = Arc::new(RedbMetadataStore::in_memory().unwrap());
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
        let outcome = create_session(&*inner, &profile(), &create, &mut Fixed(Some(identity(1))))
            .await
            .expect("the create ran");
        let CreateOutcome::Created { upload_id } = outcome else {
            panic!("expected Created, got {outcome:?}");
        };
        Self {
            store: Arc::new(Scripted {
                inner: Arc::clone(&inner),
                clock: clock.clone(),
                log: log.clone(),
                state: Mutex::new(ScriptState::default()),
            }),
            fleet: Fleet::new(&clock),
            runtime: Runtime {
                clock: clock.clone(),
                next_chunk: Arc::new(AtomicU64::new(0)),
            },
            topology: topology(),
            upload: upload_id,
            next_attempt: 0,
            inner,
            clock,
            log,
        }
    }

    fn attempt_id(&mut self) -> AttemptId {
        self.next_attempt += 1;
        AttemptId::new(format!("{:032x}", 0xa77e_0000_u128 + self.next_attempt)).unwrap()
    }

    /// The wrapper, as `attempt` sees it.
    fn view(&self, attempt: &AttemptId) -> As {
        As {
            store: Arc::clone(&self.store),
            attempt: attempt.clone(),
        }
    }

    /// Stage `bytes` as part `n`.
    async fn stage(&mut self, n: u32, bytes: &[u8]) -> StagedAttempt {
        self.stage_with(n, bytes, |_| {}).await
    }

    async fn stage_with(
        &mut self,
        n: u32,
        bytes: &[u8],
        tweak: impl FnOnce(&mut StageRequest<'_>),
    ) -> StagedAttempt {
        let attempt = self.attempt_id();
        let view = self.view(&attempt);
        let mut request = request(&self.upload, &self.topology, &attempt, n);
        tweak(&mut request);
        let outcome = stage_part(
            &view,
            &self.fleet,
            &mut self.runtime,
            &request,
            stream_of(bytes),
        )
        .await;
        match outcome {
            Ok(StageOutcome::Staged(attempt)) => *attempt,
            other => panic!("expected Staged, got {other:?}"),
        }
    }

    /// Commit `attempt` through the wrapper.
    async fn commit(&self, attempt: StagedAttempt) -> Result<UploadPartOutcome, CommitError> {
        let view = self.view(attempt.attempt_id());
        commit_part(&view, &self.runtime, attempt).await
    }

    /// Stage and commit part `n`: a part some earlier attempt committed. The log starts afresh.
    async fn committed(&mut self, n: u32, bytes: &[u8]) -> PartRecord {
        let attempt = self.stage(n, bytes).await;
        let outcome = self.commit(attempt).await.expect("the prior commit ran");
        assert!(matches!(outcome, UploadPartOutcome::Committed { .. }));
        self.log.clear();
        self.part(n).await.expect("the prior part is committed").0
    }

    /// The whole `UploadPart` through the wrapper.
    async fn upload(
        &mut self,
        n: u32,
        bytes: &[u8],
        max_part_chunks: u32,
    ) -> Result<UploadPartOutcome, UploadPartError> {
        let attempt = self.attempt_id();
        let view = self.view(&attempt);
        let mut request = request(&self.upload, &self.topology, &attempt, n);
        request.max_part_chunks = max_part_chunks;
        upload_part(
            &view,
            &self.fleet,
            &mut self.runtime,
            &request,
            stream_of(bytes),
        )
        .await
    }

    /// A Complete that fenced the session and rolled it back: `Open@E` → `Open@E+2`.
    fn rollback(&self) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        Step::Run(once(async move {
            roll_back(&inner, &upload).await;
        }))
    }

    /// A real Abort (#895) of the session, which must fence it.
    fn abort(&self) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        Step::Run(once(async move {
            let fenced = abort_session(&*inner, &upload)
                .await
                .expect("the abort ran");
            assert_eq!(fenced, AbortOutcome::Fenced);
        }))
    }

    /// Run the steps now.
    async fn run(&self, steps: Vec<Step>) {
        self.store.run(&steps).await;
    }

    /// A rival attempt's commit of `attempt` (staged on this rig) on the inner store, which must
    /// land: the wrapper neither logs nor scripts it.
    fn rival(&self, attempt: StagedAttempt) -> Step {
        let inner = Arc::clone(&self.inner);
        let clock = At(self.clock.clone());
        Step::Run(once(async move {
            let outcome = commit_part(&*inner, &clock, attempt)
                .await
                .expect("the rival's commit ran");
            assert!(
                matches!(outcome, UploadPartOutcome::Committed { .. }),
                "the rival committed: {outcome:?}"
            );
        }))
    }

    /// Deletes `keys` on the inner store, in one batch.
    fn erased(&self, keys: Vec<Vec<u8>>) -> Step {
        let inner = Arc::clone(&self.inner);
        Step::Run(once(async move {
            let mut batch = WriteBatch::new();
            for key in keys {
                batch = batch.delete(key);
            }
            assert_eq!(inner.commit(batch).await.unwrap(), CommitOutcome::Committed);
        }))
    }

    /// **The terminal session delete** (`0016:674`), as this file needs it: the session record,
    /// every surviving `slot:` record and every owned `sidx:` entry of the session, removed in one
    /// batch on the inner store.
    fn terminal_delete(&self) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        Step::Run(once(async move {
            let mut batch = WriteBatch::new().delete(mpu_key(&upload));
            for range in [slot_range(&upload), sidx_range(&upload)] {
                for (key, _) in inner.scan(&range).await.unwrap() {
                    batch = batch.delete(key);
                }
            }
            assert_eq!(inner.commit(batch).await.unwrap(), CommitOutcome::Committed);
        }))
    }

    /// Writes `bytes` at `key` on the inner store: damage, or another writer's value.
    fn plant(&self, key: Vec<u8>, bytes: Bytes) -> Step {
        let inner = Arc::clone(&self.inner);
        Step::Run(once(async move {
            let batch = WriteBatch::new().put(key, bytes);
            assert_eq!(inner.commit(batch).await.unwrap(), CommitOutcome::Committed);
        }))
    }

    /// Deletes every `sidx:` entry of the session on the inner store.
    fn erase_owned(&self) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        Step::Run(once(async move {
            let mut batch = WriteBatch::new();
            for (key, _) in inner.scan(&sidx_range(&upload)).await.unwrap() {
                batch = batch.delete(key);
            }
            assert_eq!(inner.commit(batch).await.unwrap(), CommitOutcome::Committed);
        }))
    }

    /// Asserts that `attempt` compensated once under `epoch`: one batch with the session and the
    /// slot required, one obligation for its own chunks under that epoch (none for an empty part),
    /// no slot, no owned entry.
    async fn assert_compensated(&self, attempt: &StagedAttempt, epoch: u64) {
        let batches = self.log.commits(Kind::Compensation);
        assert_eq!(batches.len(), 1, "one compensation batch");
        let batch = &batches[0];
        assert_eq!(required_epoch(batch, &self.upload), epoch);
        assert!(
            batch
                .preconditions
                .iter()
                .any(|p| p.key == attempt.slot_key() && p.expected.is_some()),
            "the slot is required"
        );
        assert_eq!(
            batch.deletes.len(),
            1 + attempt.chunks().len(),
            "the slot and one owned entry per chunk"
        );
        let expected = if attempt.chunks().is_empty() {
            Vec::new()
        } else {
            vec![(token_of(attempt, epoch), chunk_ids(attempt.chunks()))]
        };
        assert_eq!(self.part_obligations().await, expected);
        for key in batch.puts.iter().map(|(key, _)| key) {
            assert!(
                batch
                    .preconditions
                    .iter()
                    .any(|p| p.key == *key && p.expected.is_none()),
                "the obligation key is required absent"
            );
        }
        assert_eq!(self.slots_of(attempt).await, 0, "no slot of the attempt");
        assert_eq!(self.owned_of(attempt).await, 0, "no owned entry");
        assert_eq!(self.releases(attempt), 1, "released once");
        self.assert_every_fragment_named().await;
    }

    /// Asserts that nothing of `attempt` was sent after staging but renewals: no commit, no
    /// compensation, no obligation, and the slot still held.
    async fn assert_untouched(&self, attempt: &StagedAttempt, at: &str) {
        assert_eq!(self.log.commits(Kind::Commit).len(), 0, "{at}: no commit");
        assert_eq!(
            self.log.commits(Kind::Compensation).len(),
            0,
            "{at}: no compensation"
        );
        assert!(
            self.part_obligations().await.is_empty(),
            "{at}: no obligation"
        );
        assert_eq!(self.releases(attempt), 0, "{at}: nothing released");
    }

    async fn session(&self) -> Bytes {
        self.inner
            .get(&mpu_key(&self.upload))
            .await
            .unwrap()
            .expect("the session is stored")
    }

    /// Part `n`'s record and summary, each decoded, if committed; they must agree.
    async fn part(&self, n: u32) -> Option<(PartRecord, PartSummary)> {
        let record = self
            .inner
            .get(&part_key(&self.upload, part(n)))
            .await
            .unwrap();
        let summary = self
            .inner
            .get(&psum_key(&self.upload, part(n)))
            .await
            .unwrap();
        match (record, summary) {
            (None, None) => None,
            (Some(record), Some(summary)) => {
                let record = decode_part_record(&record).unwrap();
                let summary = decode_part_summary(&summary).unwrap();
                assert_eq!(
                    summary.chunks as usize,
                    record.chunks().len(),
                    "chunk count"
                );
                assert_eq!(summary.len, record.len(), "length");
                assert_eq!(summary.digest, *record.digest(), "digest");
                assert_eq!(summary.committed_at_millis, record.committed_at_millis());
                Some((record, summary))
            }
            (record, summary) => panic!("part and summary disagree: {record:?} / {summary:?}"),
        }
    }

    /// Plants a `psum:` summary of `chunks` chunks under part number 9000.
    async fn plant_summary(&self, chunks: u32) {
        let planted = PartSummary {
            chunks,
            len: 0,
            digest: sha256(b""),
            committed_at_millis: T0,
        };
        self.inner
            .commit(WriteBatch::new().put(
                psum_key(&self.upload, part(9000)),
                metadata::encode(&planted),
            ))
            .await
            .unwrap();
    }

    /// Every `retire:bytes:` obligation: its token and the chunk ids it names.
    async fn obligations(&self) -> Vec<(RetireToken, Vec<ChunkId>)> {
        self.inner
            .scan(RETIRE_BYTES_PREFIX)
            .await
            .unwrap()
            .into_iter()
            .map(|(key, value)| {
                let (_, token, payload) = decode_retire_obligation(&key, &value).unwrap();
                (token, payload.chunks().iter().map(|c| c.id).collect())
            })
            .collect()
    }

    /// The `retire:bytes:` obligations a part attempt installed (session-wide ones excluded).
    async fn part_obligations(&self) -> Vec<(RetireToken, Vec<ChunkId>)> {
        self.obligations()
            .await
            .into_iter()
            .filter(|(token, _)| matches!(token, RetireToken::Session { part: Some(_), .. }))
            .collect()
    }

    /// How many slots the session holds, whoever holds them.
    async fn slots(&self) -> usize {
        self.inner
            .scan(&slot_range(&self.upload))
            .await
            .unwrap()
            .len()
    }

    /// How many slots `attempt` holds.
    async fn slots_of(&self, attempt: &StagedAttempt) -> usize {
        self.inner
            .scan(&slot_range(&self.upload))
            .await
            .unwrap()
            .iter()
            .filter(|(_, value)| {
                decode_slot_record(value).unwrap().attempt_id() == attempt.attempt_id()
            })
            .count()
    }

    /// How many of `attempt`'s owned entries remain.
    async fn owned_of(&self, attempt: &StagedAttempt) -> usize {
        let mut held = 0;
        for owned in attempt.owned() {
            if self.inner.get(owned.key()).await.unwrap().is_some() {
                held += 1;
            }
        }
        held
    }

    /// How many landed batches released `attempt`'s slot.
    fn releases(&self, attempt: &StagedAttempt) -> usize {
        self.store
            .releases(attempt.attempt_id(), attempt.slot_key())
    }

    /// **C-1, whole-store**: every chunk a fragment of which the fleet stores is named by an owned
    /// `sidx:` entry, a `part:` record or a `retire:bytes:` obligation.
    async fn assert_every_fragment_named(&self) {
        let mut named = BTreeSet::new();
        for (key, _) in self.inner.scan(&sidx_range(&self.upload)).await.unwrap() {
            named.insert(parse_sidx_key(&key).unwrap().2);
        }
        for (_, value) in self.inner.scan(PART_PREFIX).await.unwrap() {
            for chunk in decode_part_record(&value).unwrap().chunks() {
                named.insert(chunk.id);
            }
        }
        for (_, chunks) in self.obligations().await {
            named.extend(chunks);
        }
        let stored = self.fleet.chunks();
        let unnamed: Vec<_> = stored.difference(&named).collect();
        assert!(
            unnamed.is_empty(),
            "stored chunks no record names: {unnamed:?}"
        );
    }
}

/// `Open@E` → `Open@E+2` (a fence to `Completing@E+1` and its rollback, decision 3), committed
/// under `require` on the bytes read. The record's other fields are kept, so it is the same session
/// at a later epoch.
async fn roll_back(inner: &RedbMetadataStore, upload: &UploadId) {
    let key = mpu_key(upload);
    let bytes = inner.get(&key).await.unwrap().expect("the session exists");
    let record = decode_session_record(&bytes).unwrap();
    assert!(
        matches!(record.state(), SessionState::Open {}),
        "rolled back from Open"
    );
    let text = std::str::from_utf8(&bytes).unwrap();
    let from = format!(
        "\"epoch\":{},\"attempts\":{},",
        record.epoch(),
        record.attempts()
    );
    let to = format!(
        "\"epoch\":{},\"attempts\":{},",
        record.epoch() + 2,
        record.attempts() + 1
    );
    assert_eq!(text.matches(&from).count(), 1);
    let next = Bytes::from(text.replace(&from, &to));
    let rolled = decode_session_record(&next).expect("the rolled-back session decodes");
    assert_eq!(rolled.epoch(), record.epoch() + 2);
    assert!(matches!(rolled.state(), SessionState::Open {}));
    let batch = WriteBatch::new().require(key.clone(), bytes).put(key, next);
    assert_eq!(inner.commit(batch).await.unwrap(), CommitOutcome::Committed);
}

/// A record that decodes as `Open` at `bytes`'s own epoch and is not `bytes`: a value no legal
/// history writes, since a session returns to `Open` only at a higher epoch.
fn same_epoch_other_bytes(bytes: &Bytes) -> Bytes {
    let record = decode_session_record(bytes).unwrap();
    let text = std::str::from_utf8(bytes).unwrap();
    let from = format!("\"attempts\":{},", record.attempts());
    let to = format!("\"attempts\":{},", record.attempts() + 1);
    assert_eq!(text.matches(&from).count(), 1);
    let other = Bytes::from(text.replace(&from, &to));
    let decoded = decode_session_record(&other).expect("the planted session decodes");
    assert_eq!(decoded.epoch(), record.epoch());
    assert!(matches!(decoded.state(), SessionState::Open {}));
    other
}

fn committed_as(outcome: &Result<UploadPartOutcome, CommitError>, attempt: &StagedAttempt) {
    assert_eq!(
        outcome.as_ref().ok(),
        Some(&UploadPartOutcome::Committed {
            part_number: attempt.part_number(),
            digest: *attempt.digest(),
        }),
        "{outcome:?}"
    );
}

fn refused(outcome: &Result<UploadPartOutcome, CommitError>, refusal: Refusal) {
    assert_eq!(
        outcome.as_ref().ok(),
        Some(&UploadPartOutcome::Refused(refusal)),
        "{outcome:?}"
    );
}

/// The doubt a commit ended in, once it is shown to read `Indeterminate`. Its `Display` names the
/// upload id, the batch in doubt, whether it may still be applied and what ended the reads, and it
/// claims no release. The flag is never above the store's own report: a read can show a batch
/// unable to land (#1029), never the reverse.
fn in_doubt(outcome: Result<UploadPartOutcome, CommitError>) -> Box<InDoubt> {
    match outcome {
        Err(err @ CommitError::InDoubt(_)) => {
            assert_eq!(classify(&err), ErrorClass::Indeterminate, "{err}");
            assert!(!err.slot_released(), "{err}");
            let text = err.to_string();
            let CommitError::InDoubt(doubt) = err else {
                unreachable!()
            };
            let batch = match doubt.batch {
                DoubtfulBatch::Commit => "part commit",
                DoubtfulBatch::Compensation => "compensation",
            };
            let may = if doubt.may_still_commit {
                "may still be applied"
            } else {
                "may have been applied"
            };
            for wanted in [
                doubt.upload_id.to_string(),
                "is in doubt".to_owned(),
                batch.to_owned(),
                may.to_owned(),
                "nothing more was sent".to_owned(),
                doubt.cause.to_string(),
            ] {
                assert!(text.contains(&wanted), "{text:?} names {wanted:?}");
            }
            assert!(
                !doubt.may_still_commit || doubt.outcome.may_still_commit,
                "the flag is never above the store's report"
            );
            doubt
        }
        other => panic!("expected InDoubt, got {other:?}"),
    }
}

/// What a stop's message says it left in the store.
fn residue(released: bool) -> &'static str {
    if released {
        "the attempt's slot is released and its chunks are retired"
    } else {
        "this attempt released nothing, and what it staged stays in the store"
    }
}

/// The cause of a commit that stopped with nothing in doubt. Its `Display` names the upload id, the
/// cause, that nothing is in doubt, and what it left; it is never classed `Indeterminate`.
fn stopped(outcome: Result<UploadPartOutcome, CommitError>, released: bool) -> Stop {
    match outcome {
        Err(err @ CommitError::Stopped { .. }) => {
            assert_ne!(classify(&err), ErrorClass::Indeterminate, "{err}");
            assert_eq!(err.slot_released(), released, "{err}");
            let text = err.to_string();
            let CommitError::Stopped {
                upload_id, cause, ..
            } = err
            else {
                unreachable!()
            };
            for wanted in [
                upload_id.to_string(),
                cause.to_string(),
                "no batch of this part attempt is in doubt".to_owned(),
                residue(released).to_owned(),
            ] {
                assert!(text.contains(&wanted), "{text:?} names {wanted:?}");
            }
            cause
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// What stopped a quiesce renewal that returned an error. Its `Display` names the renewal's error
/// and what it left, and its class is the renewal error's.
fn renewal_stopped(outcome: Result<UploadPartOutcome, CommitError>, released: bool) -> StageError {
    match outcome {
        Err(err @ CommitError::Renewal { .. }) => {
            assert_eq!(err.slot_released(), released, "{err}");
            let text = err.to_string();
            assert!(text.contains(residue(released)), "{text:?}");
            let class = classify(&err);
            let CommitError::Renewal { error, .. } = err else {
                unreachable!()
            };
            assert!(text.contains(&error.to_string()), "{text:?}");
            assert_eq!(class, classify(&error), "the renewal error's class");
            error
        }
        other => panic!("expected a renewal error, got {other:?}"),
    }
}

/// The part record `attempt`'s commit puts: its chunks, under its digest.
fn is_own(record: &PartRecord, attempt: &StagedAttempt) -> bool {
    record.chunks() == attempt.chunks() && record.digest() == attempt.digest()
}

fn chunk_ids(chunks: &[metadata::ChunkRef]) -> Vec<ChunkId> {
    chunks.iter().map(|chunk| chunk.id).collect()
}

/// What the quiesce renewal will leave on `attempt`'s lease if the clock does not move first: a
/// renewal stamps one millisecond later than the lease it replaces when a TTL from now is no later
/// than that, and every intent while staging did the same.
fn left_after_renewal(rig: &Rig, attempt: &StagedAttempt) -> u64 {
    attempt.slot().lease_expiry_millis() + 1 - rig.clock.now_millis()
}

/// The token of `attempt`'s per-part obligation under `epoch`.
fn token_of(attempt: &StagedAttempt, epoch: u64) -> RetireToken {
    RetireToken::Session {
        upload_id: attempt.upload_id().clone(),
        epoch,
        part: Some((attempt.part_number(), attempt.attempt_id().clone())),
    }
}

/// The epoch a batch requires the session at.
fn required_epoch(batch: &WriteBatch, upload: &UploadId) -> u64 {
    let session = batch
        .preconditions
        .iter()
        .find(|p| p.key == mpu_key(upload))
        .and_then(|p| p.expected.clone())
        .expect("the batch requires the session");
    decode_session_record(&session).unwrap().epoch()
}

// ─────────────── (A) the commit ───────────────

/// **(A)** Parts 3, 1 and 2, in that order, through the composed `upload_part` (reserve → stage →
/// commit), part 3 three chunks long and part 1 two: each answers `Committed` with its number and
/// the body's digest; its `part:` and `psum:` records decode and agree; the attempt holds no slot
/// and no owned entry afterwards. The log shows each attempt's last renewal before the commit's
/// reads, and no renewal between those reads and the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parts_staged_out_of_order_each_commit_with_records_that_agree() {
    within(async {
        let mut rig = Rig::new().await;
        for (n, len, seed) in [(3u32, 3 * CHUNK - 5, 3u8), (1, 2 * CHUNK, 1), (2, 1, 2)] {
            rig.log.clear();
            let bytes = body(len, seed);
            let outcome = rig
                .upload(n, &bytes, MAX_PART_CHUNKS)
                .await
                .expect("the upload ran");
            assert_eq!(
                outcome,
                UploadPartOutcome::Committed {
                    part_number: part(n),
                    digest: sha256(&bytes),
                }
            );
            let (record, summary) = rig.part(n).await.expect("the part is committed");
            assert_eq!(record.len(), len as u64);
            assert_eq!(*record.digest(), sha256(&bytes));
            assert_eq!(record.chunks().len(), len.div_ceil(CHUNK));
            assert_eq!(summary.chunks as usize, len.div_ceil(CHUNK));
            assert_eq!(record.session_epoch(), 0);
            assert_eq!(record.committed_at_millis(), T0);
            // The attempt's staging records are gone.
            assert_eq!(rig.slots().await, 0, "no slot");
            assert!(
                rig.inner
                    .scan(&sidx_range(&rig.upload))
                    .await
                    .unwrap()
                    .is_empty(),
                "no owned entry"
            );
            // Last renewal → the commit's reads → the commit, no renewal in between.
            let events = rig.log.events();
            let is_commit = |e: &Event, of| matches!(e, Event::Commit { kind, .. } if *kind == of);
            let commit_at = events
                .iter()
                .position(|e| is_commit(e, Kind::Commit))
                .expect("a commit was sent");
            let renewal_at = events[..commit_at]
                .iter()
                .rposition(|e| is_commit(e, Kind::Renewal))
                .expect("a renewal preceded the commit");
            let reads: Vec<&Vec<u8>> = events[renewal_at..commit_at]
                .iter()
                .filter_map(|e| match e {
                    Event::Read { key } => Some(key),
                    _ => None,
                })
                .collect();
            assert_eq!(reads.len(), 2, "the summaries and the part key: {reads:?}");
            assert!(reads[0].starts_with(PSUM_PREFIX));
            assert_eq!(*reads[1], part_key(&rig.upload, part(n)));
            assert!(
                !events[renewal_at + 1..commit_at]
                    .iter()
                    .any(|e| is_commit(e, Kind::Renewal)),
                "no renewal between the commit's reads and the commit"
            );
            // One batch: the part, its summary, the slot release and every owned delete, under the
            // session, the slot, and the part key required absent.
            let commits = rig.log.commits(Kind::Commit);
            assert_eq!(commits.len(), 1);
            let commit = &commits[0];
            assert_eq!(commit.puts.len(), 2);
            assert_eq!(
                commit.deletes.len(),
                1 + record.chunks().len(),
                "the slot and one owned entry per chunk"
            );
            assert_eq!(required_epoch(commit, &rig.upload), 0);
            assert!(commit
                .preconditions
                .iter()
                .any(|p| p.key == part_key(&rig.upload, part(n)) && p.expected.is_none()));
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        }
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(A)** A rollback **before** the quiesce renewal is adopted by that renewal (`stage.rs`, its
/// session re-read), so the part commits under the live epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_a_rollback_before_the_quiesce_renewal_commits_under_the_live_epoch() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 221)).await;
        rig.store.script(
            Kind::Renewal,
            vec![Act::conflict().before(rig.rollback())],
            None,
        );
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        let commits = rig.log.commits(Kind::Commit);
        assert_eq!(commits.len(), 1);
        assert_eq!(
            required_epoch(&commits[0], &rig.upload),
            2,
            "the live epoch"
        );
        assert_eq!(rig.part(1).await.unwrap().0.session_epoch(), 2);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 0);
        rig.assert_every_fragment_named().await;
    })
    .await;
}

// ─────────────── (D) re-upload and ceilings ───────────────

/// **(D)** Re-uploading a committed part replaces it under `require(part == prior)` and puts the
/// prior's chunks in one `retire:bytes:` obligation, under `require_absent` of its key, all in the
/// commit's one batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_reupload_retires_the_prior_chunks_in_the_same_batch() {
    within(async {
        let mut rig = Rig::new().await;
        let prior = rig.committed(1, &body(2 * CHUNK, 71)).await;
        let prior_bytes = rig
            .inner
            .get(&part_key(&rig.upload, part(1)))
            .await
            .unwrap()
            .unwrap();
        let attempt = rig.stage(1, &body(CHUNK + 9, 72)).await;
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        let commits = rig.log.commits(Kind::Commit);
        assert_eq!(commits.len(), 1);
        let batch = &commits[0];
        assert!(
            batch
                .preconditions
                .iter()
                .any(|p| p.key == part_key(&rig.upload, part(1))
                    && p.expected.as_ref() == Some(&prior_bytes)),
            "require(part == prior)"
        );
        let retire: Vec<&Vec<u8>> = batch
            .puts
            .iter()
            .map(|(key, _)| key)
            .filter(|key| key.starts_with(RETIRE_BYTES_PREFIX))
            .collect();
        assert_eq!(retire.len(), 1, "one obligation, in the same batch");
        assert!(
            batch
                .preconditions
                .iter()
                .any(|p| p.key == *retire[0] && p.expected.is_none()),
            "require_absent of its key"
        );
        assert!(is_own(&rig.part(1).await.unwrap().0, &attempt));
        assert_eq!(
            rig.part_obligations().await,
            vec![(token_of(&attempt, 0), chunk_ids(prior.chunks()))]
        );
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(D)** A re-upload under a session that has moved up: the prior was committed at epoch 0, a
/// Complete rolls the session back to `Open@2` before the quiesce renewal, which adopts it. The
/// obligation owing the prior's chunks is keyed under the live epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_reupload_under_a_newer_epoch_owes_the_prior_under_that_epoch() {
    within(async {
        let mut rig = Rig::new().await;
        let prior = rig.committed(1, &body(CHUNK, 247)).await;
        let attempt = rig.stage(1, &body(2 * CHUNK, 248)).await;
        rig.store.script(
            Kind::Renewal,
            vec![Act::conflict().before(rig.rollback())],
            None,
        );
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        let (record, _) = rig.part(1).await.unwrap();
        assert!(is_own(&record, &attempt));
        assert_eq!(record.session_epoch(), 2);
        assert_eq!(
            rig.part_obligations().await,
            vec![(token_of(&attempt, 2), chunk_ids(prior.chunks()))],
            "the prior's chunks, owed under the live epoch"
        );
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(D)** An empty re-upload over an empty prior writes no `retire:` key. An empty re-upload over
/// a non-empty prior writes one, for the prior's chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_an_empty_reupload_writes_an_obligation_only_for_a_non_empty_prior() {
    within(async {
        let mut rig = Rig::new().await;
        rig.committed(1, &[]).await;
        let attempt = rig.stage(1, &[]).await;
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        assert!(rig.obligations().await.is_empty(), "no retire: key");
        assert_eq!(rig.slots_of(&attempt).await, 0);

        let mut rig = Rig::new().await;
        let prior = rig.committed(1, &body(2 * CHUNK, 41)).await;
        let attempt = rig.stage(1, &[]).await;
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        assert_eq!(
            rig.part_obligations().await,
            vec![(token_of(&attempt, 0), chunk_ids(prior.chunks()))],
            "one obligation, for the prior's chunks"
        );
        assert!(rig.part(1).await.unwrap().0.is_empty());
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// Asserts that `attempt`, staged on `rig` and refused past a ceiling, was compensated in one batch
/// with no `part:` record left behind.
async fn assert_refused_and_compensated(rig: &Rig, attempt: &StagedAttempt) {
    assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "no commit");
    rig.assert_compensated(attempt, 0).await;
    assert!(rig.part(attempt.part_number().get()).await.is_none());
}

/// Asserts the composed `UploadPart`'s answer, and that it left one compensation, no `part:` record
/// and no slot behind.
async fn assert_upload_refused(
    rig: &Rig,
    outcome: Result<UploadPartOutcome, UploadPartError>,
    refusal: Refusal,
) {
    assert_eq!(
        outcome.expect("the upload ran"),
        UploadPartOutcome::Refused(refusal)
    );
    assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "no commit");
    assert_eq!(rig.log.commits(Kind::Compensation).len(), 1, "one batch");
    assert!(rig.part(1).await.is_none());
    assert_eq!(rig.slots().await, 0);
    assert_eq!(rig.part_obligations().await.len(), 1);
    rig.assert_every_fragment_named().await;
}

/// **(D) The per-part ceiling, first witness: a body truncated at its bound.** Staging stopped at
/// the request's bound and marked it. Through `commit_part` and through `upload_part`: refused and
/// compensated in one batch, at a low bound, at the shipped one, and at one past the shipped one,
/// where the ceiling reported is still the shipped one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_body_truncated_at_its_bound_is_refused_and_compensated() {
    for (bound, limit) in [
        (2u32, 2u64),
        (MAX_PART_CHUNKS, u64::from(MAX_PART_CHUNKS)),
        (MAX_PART_CHUNKS + 2, u64::from(MAX_PART_CHUNKS)),
    ] {
        within(async {
            let refusal = Refusal::EntityTooLarge {
                chunks: u64::from(bound) + 1,
                limit,
            };
            let bytes = body((bound as usize + 1) * CHUNK, 81);
            let mut rig = Rig::new().await;
            let attempt = rig
                .stage_with(1, &bytes, |r| r.max_part_chunks = bound)
                .await;
            assert!(attempt.chunk_bound_exceeded());
            assert_eq!(attempt.chunks().len(), bound as usize);
            let outcome = rig.commit(attempt.clone()).await;
            refused(&outcome, refusal.clone());
            assert_refused_and_compensated(&rig, &attempt).await;

            let mut rig = Rig::new().await;
            let outcome = rig.upload(1, &bytes, bound).await;
            assert_upload_refused(&rig, outcome, refusal).await;
        })
        .await;
    }
}

/// **(D) The per-part ceiling, second witness: a whole body past it.** `stage_part` cannot enforce
/// `MAX_PART_CHUNKS`: the bound it stops at is the caller's, and a whole body within that bound is
/// not marked. A body of `MAX_PART_CHUNKS + 1` chunks staged with the limit set to
/// `MAX_PART_CHUNKS + 1` is refused all the same, through `commit_part` and through `upload_part`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_whole_body_past_the_per_part_ceiling_is_refused_and_compensated() {
    within(async {
        let over = MAX_PART_CHUNKS + 1;
        let refusal = Refusal::EntityTooLarge {
            chunks: u64::from(over),
            limit: u64::from(MAX_PART_CHUNKS),
        };
        let bytes = body(over as usize * CHUNK, 83);
        let mut rig = Rig::new().await;
        let attempt = rig
            .stage_with(1, &bytes, |r| r.max_part_chunks = over)
            .await;
        assert!(!attempt.chunk_bound_exceeded(), "the whole body is staged");
        assert_eq!(attempt.chunks().len(), over as usize);
        let outcome = rig.commit(attempt.clone()).await;
        refused(&outcome, refusal.clone());
        assert_refused_and_compensated(&rig, &attempt).await;

        let mut rig = Rig::new().await;
        let outcome = rig.upload(1, &bytes, over).await;
        assert_upload_refused(&rig, outcome, refusal).await;
    })
    .await;
}

/// **(D)** A whole body of exactly `MAX_PART_CHUNKS` chunks commits, through both routes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_whole_body_of_exactly_the_per_part_ceiling_commits() {
    within(async {
        let bytes = body(MAX_PART_CHUNKS as usize * CHUNK, 84);
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &bytes).await;
        assert!(!attempt.chunk_bound_exceeded());
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        let (record, summary) = rig.part(1).await.unwrap();
        assert_eq!(record.chunks().len(), MAX_PART_CHUNKS as usize);
        assert_eq!(summary.chunks, MAX_PART_CHUNKS);
        assert_eq!(*record.digest(), sha256(&bytes));

        let mut rig = Rig::new().await;
        let outcome = rig
            .upload(1, &bytes, MAX_PART_CHUNKS)
            .await
            .expect("the upload ran");
        assert_eq!(
            outcome,
            UploadPartOutcome::Committed {
                part_number: part(1),
                digest: sha256(&bytes),
            }
        );
    })
    .await;
}

/// **(D) The cumulative ceiling is a read-time check, and 0016 allows the race it leaves**
/// (`0016:1441-1451`). With `psum:` planted one chunk below `MAX_STAGED_CHUNKS`, two attempts on
/// parts 1 and 2, one chunk each, both read that one chunk of headroom: the wrapper holds the first
/// commit until the second has committed. Both commit, to `MAX_STAGED_CHUNKS + 1`. A third commit
/// after them is refused `EntityTooLarge` and compensated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_the_cumulative_ceiling_refuses_and_its_overshoot_is_the_race_0016_allows() {
    within(async {
        let mut rig = Rig::new().await;
        rig.plant_summary(MAX_STAGED_CHUNKS - 1).await;
        let first = rig.stage(1, &body(CHUNK, 91)).await;
        let second = rig.stage(2, &body(CHUNK, 92)).await;
        let gate = Arc::new(Semaphore::new(0));
        rig.store.script(
            Kind::Commit,
            vec![Act::new(How::Gate(Arc::clone(&gate)))],
            None,
        );
        let (a, b) = tokio::join!(rig.commit(first.clone()), async {
            until("the first commit is held", || {
                rig.log.gated(Kind::Commit) == 1
            })
            .await;
            let outcome = rig.commit(second.clone()).await;
            gate.add_permits(1);
            outcome
        });
        committed_as(&a, &first);
        committed_as(&b, &second);
        let mut total = 0u64;
        for (_, value) in rig.inner.scan(PSUM_PREFIX).await.unwrap() {
            total += u64::from(decode_part_summary(&value).unwrap().chunks);
        }
        assert_eq!(total, u64::from(MAX_STAGED_CHUNKS) + 1, "the overshoot");

        rig.log.clear();
        let third = rig.stage(3, &body(CHUNK, 93)).await;
        let outcome = rig.commit(third.clone()).await;
        refused(
            &outcome,
            Refusal::EntityTooLarge {
                chunks: u64::from(MAX_STAGED_CHUNKS) + 2,
                limit: u64::from(MAX_STAGED_CHUNKS),
            },
        );
        assert_refused_and_compensated(&rig, &third).await;
    })
    .await;
}

/// **(D) The ceiling leaves out the part's own prior** (a re-upload replaces it). With `psum:`
/// planted at `MAX_STAGED_CHUNKS - 1` and a committed one-chunk part 1, the session is at the
/// ceiling exactly, and a one-chunk re-upload of part 1 keeps it there: `Committed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_the_cumulative_ceiling_leaves_out_the_parts_own_prior() {
    within(async {
        let mut rig = Rig::new().await;
        rig.plant_summary(MAX_STAGED_CHUNKS - 1).await;
        let prior = rig.committed(1, &body(CHUNK, 207)).await;
        assert_eq!(prior.chunks().len(), 1);
        let attempt = rig.stage(1, &body(CHUNK, 208)).await;
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        assert!(is_own(&rig.part(1).await.unwrap().0, &attempt));
        assert_eq!(
            rig.part_obligations().await,
            vec![(token_of(&attempt, 0), chunk_ids(prior.chunks()))]
        );
    })
    .await;
}

// ─────────────── (F) the dispatch margin ───────────────

/// **(F) Boundary.** With exactly the margin left when the commit comes to be sent it is sent; with
/// one millisecond less it is not, and the attempt compensates and returns the typed stop, for a
/// part with chunks and for an empty one. The wrapper fails any leg that sends with less.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_a_commit_is_sent_with_exactly_the_margin_and_not_with_a_millisecond_less() {
    for bytes in [body(CHUNK, 121), Vec::new()] {
        within(async {
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &bytes).await;
            let spent = left_after_renewal(&rig, &attempt) - MARGIN;
            rig.store.after_scan(vec![Step::Advance(spent)]);
            let outcome = rig.commit(attempt.clone()).await;
            committed_as(&outcome, &attempt);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
            let (record, _) = rig.part(1).await.unwrap();
            assert_eq!(
                record.committed_at_millis(),
                T0 + spent,
                "stamped at the reading the lease was judged at"
            );

            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &bytes).await;
            let spent = left_after_renewal(&rig, &attempt) - MARGIN + 1;
            rig.store.after_scan(vec![Step::Advance(spent)]);
            let expiry = attempt.slot().lease_expiry_millis() + 1;
            let now = T0 + spent;
            let cause = stopped(rig.commit(attempt.clone()).await, true);
            assert!(
                matches!(
                    cause,
                    Stop::LeaseShort { lease_expiry_millis, now_millis }
                        if lease_expiry_millis == expiry && now_millis == now
                ),
                "{cause:?}"
            );
            assert_eq!(
                cause.to_string(),
                format!(
                    "the lease expiring at {expiry} had less than 5000 ms left at {now}, so no \
                     further commit was sent"
                )
            );
            assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "not sent");
            rig.assert_compensated(&attempt, 0).await;
            assert!(rig.part(1).await.is_none());
        })
        .await;
    }
}

/// **(F) Used up by the reads.** The summary scan and the part read take the lease below the mark,
/// so no commit is sent and the attempt compensates. A lease the reads use up altogether does not
/// block the compensation: it revives nothing, and lands because the session and slot are unchanged
/// (`0016:1651-1653`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_a_lease_used_up_by_the_reads_sends_no_commit_and_compensates() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 123)).await;
        rig.store
            .tick_reads((left_after_renewal(&rig, &attempt) - MARGIN) / 2 + 1);
        let cause = stopped(rig.commit(attempt.clone()).await, true);
        assert!(matches!(cause, Stop::LeaseShort { .. }), "{cause:?}");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 0);
        rig.assert_compensated(&attempt, 0).await;

        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 124)).await;
        let lapse = left_after_renewal(&rig, &attempt) + 1;
        rig.store.after_scan(vec![Step::Advance(lapse)]);
        let cause = stopped(rig.commit(attempt.clone()).await, true);
        assert!(matches!(cause, Stop::LeaseShort { .. }), "{cause:?}");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 0);
        rig.assert_compensated(&attempt, 0).await;
    })
    .await;
}

// ─────────────── (G) a settled stop compensates ───────────────

/// Script the quiesce renewal of `attempt` on `rig` to stop with `stop`.
fn stop_the_renewal(rig: &Rig, attempt: &StagedAttempt, stop: &str) {
    match stop {
        "cover-short" => rig.store.script(
            Kind::Renewal,
            vec![],
            // Below the margin at each round's judging, after any bump a renewal's stamp gets from
            // the lease it replaces.
            Some(Act::pass().after(Step::Advance(TTL - MARGIN + 10))),
        ),
        "rounds" => rig
            .store
            .script(Kind::Renewal, vec![], Some(Act::conflict())),
        "lapsed" => rig.clock.set(attempt.slot().lease_expiry_millis()),
        // "invalid": staged with a TTL no cover fits under.
        _ => {}
    }
}

/// **(G)** The quiesce renewal stopping with `CoverShort`, `RenewalRoundsSpent`, `LeaseLapsed` or
/// `InvalidRequest` sends the live-session compensation: one obligation for the attempt's own chunks
/// (none for an empty part), no slot, no owned entry. The answer is the renewal's typed error, in
/// its class, reporting that the slot was released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_a_settled_renewal_stop_compensates_and_keeps_its_error() {
    for bytes in [body(CHUNK + 1, 131), Vec::new()] {
        for stop in ["cover-short", "rounds", "lapsed", "invalid"] {
            within(async {
                let at = format!("{stop}, {} bytes", bytes.len());
                let mut rig = Rig::new().await;
                let ttl = if stop == "invalid" { MARGIN } else { TTL };
                let attempt = rig
                    .stage_with(1, &bytes, |r| r.lease_ttl_millis = ttl)
                    .await;
                stop_the_renewal(&rig, &attempt, stop);
                let error = renewal_stopped(rig.commit(attempt.clone()).await, true);
                let StageError::Stopped { cause, .. } = &error else {
                    panic!("{at}: expected a settled stop, got {error:?}");
                };
                let matched = match stop {
                    "cover-short" => matches!(cause, StageStop::CoverShort { .. }),
                    "rounds" => matches!(cause, StageStop::RenewalRoundsSpent { .. }),
                    "lapsed" => matches!(cause, StageStop::LeaseLapsed { .. }),
                    _ => matches!(cause, StageStop::InvalidRequest { .. }),
                };
                assert!(matched, "{at}: {cause:?}");
                assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "{at}: no commit");
                rig.assert_compensated(&attempt, 0).await;
            })
            .await;
        }
    }
}

/// **(G) The cap witness.** `MAX_INFLIGHT_PARTS` + 1 attempts on one session each end in a settled
/// stop (a stopped renewal of each kind that can be scripted, and a short lease, in turn). The held
/// slot count is 0 afterwards, and the next reserve succeeds instead of answering `SlowDown`. An
/// attempt past the cap would have failed to stage at all had the earlier ones kept their slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_settled_stops_never_fill_the_slot_cap() {
    within(async {
        let mut rig = Rig::new().await;
        for at in 0..=MAX_INFLIGHT_PARTS {
            let attempt = rig.stage(at + 1, &body(CHUNK, 132)).await;
            match at % 4 {
                0 => {
                    stop_the_renewal(&rig, &attempt, "rounds");
                    renewal_stopped(rig.commit(attempt).await, true);
                }
                1 => {
                    stop_the_renewal(&rig, &attempt, "cover-short");
                    renewal_stopped(rig.commit(attempt).await, true);
                }
                2 => {
                    stop_the_renewal(&rig, &attempt, "lapsed");
                    renewal_stopped(rig.commit(attempt).await, true);
                }
                _ => {
                    let spent = left_after_renewal(&rig, &attempt) - MARGIN + 1;
                    rig.store.after_scan(vec![Step::Advance(spent)]);
                    stopped(rig.commit(attempt).await, true);
                }
            }
            rig.store.script(Kind::Renewal, vec![], None);
        }
        assert_eq!(rig.slots().await, 0, "no slot is held");
        let next = rig.attempt_id();
        let view = rig.view(&next);
        let request = request(&rig.upload, &rig.topology, &next, 100);
        let reserved = reserve_slot(&view, &request, rig.clock.now_millis())
            .await
            .unwrap();
        assert!(
            matches!(reserved, ReserveOutcome::Reserved { .. }),
            "{reserved:?}"
        );
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(G)** A compensation after a rollback the renewal adopted runs under the live epoch, on each
/// route to one past the renewal: its obligation is keyed under that epoch, and the batch requires
/// the session at it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_a_compensation_after_an_adopted_rollback_runs_under_the_live_epoch() {
    for route in ["renewal", "lease", "ceiling"] {
        within(async {
            let (rig, attempt) = compensating(route, 144).await;
            let then = (route == "renewal").then(Act::conflict);
            rig.store.script(
                Kind::Renewal,
                vec![Act::conflict().before(rig.rollback())],
                then,
            );
            let outcome = rig.commit(attempt.clone()).await;
            assert!(!matches!(outcome, Ok(UploadPartOutcome::Committed { .. })));
            assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "{route}");
            rig.assert_compensated(&attempt, 2).await;
        })
        .await;
    }
}

/// **(G) Not compensated: the renewal's stops.** A renewal in doubt, `SlotLost`, `Record` and
/// `Store`: the log shows no compensation batch, and the answer claims no release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_a_renewal_stop_that_cannot_show_its_records_are_its_own_does_not_compensate() {
    within(async {
        // A renewal in doubt.
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 133)).await;
        rig.store.script(
            Kind::Renewal,
            vec![Act::unknown(false, true)],
            Some(Act::conflict()),
        );
        let error = renewal_stopped(rig.commit(attempt.clone()).await, false);
        assert!(matches!(error, StageError::InDoubt(_)), "{error:?}");
        assert_eq!(classify(&error), ErrorClass::Indeterminate);
        rig.assert_untouched(&attempt, "in doubt").await;
        assert_eq!(rig.slots_of(&attempt).await, 1);

        // `SlotLost`.
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 134)).await;
        let gone = WriteBatch::new().delete(attempt.slot_key().to_vec());
        rig.inner.commit(gone).await.unwrap();
        let error = renewal_stopped(rig.commit(attempt.clone()).await, false);
        assert!(
            matches!(
                &error,
                StageError::Stopped {
                    cause: StageStop::SlotLost { .. },
                    ..
                }
            ),
            "{error:?}"
        );
        rig.assert_untouched(&attempt, "slot lost").await;

        // `Record`: a damaged session.
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 135)).await;
        let damage = rig.plant(mpu_key(&rig.upload), Bytes::from_static(b"torn"));
        rig.store
            .script(Kind::Renewal, vec![Act::conflict().before(damage)], None);
        let error = renewal_stopped(rig.commit(attempt.clone()).await, false);
        assert!(
            matches!(
                &error,
                StageError::Stopped {
                    cause: StageStop::Record(_),
                    ..
                }
            ),
            "{error:?}"
        );
        rig.assert_untouched(&attempt, "record").await;

        // `Store`.
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 136)).await;
        rig.store.script(Kind::Renewal, vec![Act::fail()], None);
        let error = renewal_stopped(rig.commit(attempt.clone()).await, false);
        assert!(
            matches!(
                &error,
                StageError::Stopped {
                    cause: StageStop::Store(_),
                    ..
                }
            ),
            "{error:?}"
        );
        rig.assert_untouched(&attempt, "store").await;
    })
    .await;
}

/// **(G) Not compensated: the commit's own `Store` and `Record` stops.** A fault on either read, a
/// damaged summary, a damaged part key, and a definite failure of the commit: no compensation, and
/// the residue stays as after a crash (`0016:1778`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_a_store_fault_or_a_damaged_record_at_the_commit_does_not_compensate() {
    within(async {
        // A fault on each of the commit's two reads.
        for read in ["summaries", "part"] {
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 137)).await;
            rig.store.fail_reads(if read == "part" {
                part_key(&rig.upload, part(1))
            } else {
                psum_range(&rig.upload)
            });
            let cause = stopped(rig.commit(attempt.clone()).await, false);
            assert!(matches!(cause, Stop::Store(_)), "{read}: {cause:?}");
            rig.assert_untouched(&attempt, read).await;
            assert_eq!(rig.slots_of(&attempt).await, 1);
        }

        // A definite failure of the commit itself.
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 138)).await;
        rig.store.script(Kind::Commit, vec![Act::fail()], None);
        let cause = stopped(rig.commit(attempt.clone()).await, false);
        assert!(matches!(cause, Stop::Store(_)), "{cause:?}");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "one commit");
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 1);

        // A damaged summary, and a damaged part key.
        for name in ["summary", "part"] {
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 139)).await;
            let key = if name == "part" {
                part_key(&rig.upload, part(1))
            } else {
                psum_key(&rig.upload, part(9))
            };
            rig.run(vec![rig.plant(key, Bytes::from_static(b"torn"))])
                .await;
            let cause = stopped(rig.commit(attempt.clone()).await, false);
            assert!(matches!(cause, Stop::Record(_)), "{name}: {cause:?}");
            rig.assert_untouched(&attempt, name).await;
            assert_eq!(rig.slots_of(&attempt).await, 1);
        }
    })
    .await;
}

/// **(G)** The session has left `Open`: a real Abort lands between staging and the quiesce. The
/// answer is the refusal, nothing more is written, and the residue is the reaper's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_a_session_that_left_open_answers_its_refusal_and_leaves_the_residue() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 141)).await;
        rig.run(vec![rig.abort()]).await;
        let outcome = rig.commit(attempt.clone()).await;
        refused(&outcome, Refusal::NoSuchUpload);
        rig.assert_untouched(&attempt, "fenced").await;
        assert_eq!(rig.slots_of(&attempt).await, 1, "the slot stays");
        assert_eq!(rig.owned_of(&attempt).await, 1, "the owned entry stays");
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(G) `Stop::EntryLost` is a staging stop.** Its one construction is intent settlement, before a
/// `StagedAttempt` exists. Through the composed `upload_part`: the wrapper applies a chunk's intent,
/// answers it unknown, and deletes that `sidx:` entry before the re-read. The answer is the stage
/// error with `EntryLost`, and the log shows no commit and no compensation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_entry_lost_is_a_stage_error_with_no_commit_and_no_compensation() {
    within(async {
        let mut rig = Rig::new().await;
        let erase = rig.erase_owned();
        rig.store.script(
            Kind::Intent,
            vec![Act::unknown(true, true).after(erase)],
            None,
        );
        let err = rig
            .upload(1, &body(CHUNK, 142), MAX_PART_CHUNKS)
            .await
            .expect_err("staging stopped");
        let UploadPartError::Stage(StageError::Stopped {
            cause: StageStop::EntryLost { .. },
            ..
        }) = err
        else {
            panic!("expected EntryLost, got {err:?}");
        };
        assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "no commit");
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "no release");
        assert_eq!(rig.slots().await, 1, "the slot stays");
        assert!(rig.part_obligations().await.is_empty());
    })
    .await;
}

/// **(G) An owned entry lost after staging is not `EntryLost`.** Delete one between staging and the
/// quiesce, with the session and slot unchanged and the clock live: the renewal requires that entry,
/// conflicts every round, finds the session and slot as they were, and ends `RenewalRoundsSpent`.
/// That compensates like any other settled stop, and the obligation names all the attempt's chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g_an_owned_entry_lost_after_staging_ends_rounds_spent_and_compensates() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(3 * CHUNK, 143)).await;
        assert_eq!(attempt.chunks().len(), 3);
        let gone = WriteBatch::new().delete(attempt.owned()[1].key().to_vec());
        rig.inner.commit(gone).await.unwrap();
        let error = renewal_stopped(rig.commit(attempt.clone()).await, true);
        assert!(
            matches!(
                &error,
                StageError::Stopped {
                    cause: StageStop::RenewalRoundsSpent { .. },
                    ..
                }
            ),
            "{error:?}"
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 0);
        rig.assert_compensated(&attempt, 0).await;
    })
    .await;
}

// ─────────────── (H2) a damaged session the renewal adopted ───────────────

/// A rig with a session rolled back to `Open@2` and part 1 staged under it, and the two damaged
/// records a renewal could adopt: under other bytes at the same epoch, and at a lower one.
async fn staged_at_epoch_2(damage: &str) -> (Rig, StagedAttempt, Step, u64) {
    let mut rig = Rig::new().await;
    let original = rig.session().await;
    rig.run(vec![rig.rollback()]).await;
    let live = rig.session().await;
    let attempt = rig.stage(1, &body(CHUNK, 151)).await;
    assert_eq!(
        decode_session_record(attempt.session_bytes())
            .unwrap()
            .epoch(),
        2
    );
    let (planted, found) = match damage {
        "same epoch" => (same_epoch_other_bytes(&live), 2),
        _ => (original, 0),
    };
    let plant = rig.plant(mpu_key(&rig.upload), planted);
    (rig, attempt, plant, found)
}

/// **(H2)** A damaged session adopted by the renewal is never committed or compensated under. On a
/// renewal that ends covered, ends `RenewalRoundsSpent` and ends `CoverShort`, with an `Open` record
/// planted under other bytes at the same epoch, and at a lower epoch (staged at 2, planted at 0): the
/// answer is `Stop::Record(SessionEpochNotRisen)`, with no commit and no compensation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_a_damaged_session_the_renewal_adopted_is_never_compensated_under() {
    for damage in ["same epoch", "lower epoch"] {
        for ends in ["covered", "rounds", "cover-short"] {
            within(async {
                let at = format!("{damage}, {ends}");
                let (rig, attempt, plant, found_epoch) = staged_at_epoch_2(damage).await;
                let then = match ends {
                    "covered" => None,
                    "rounds" => Some(Act::conflict()),
                    _ => Some(Act::pass().after(Step::Advance(TTL - MARGIN + 10))),
                };
                rig.store
                    .script(Kind::Renewal, vec![Act::conflict().before(plant)], then);
                let cause = stopped(rig.commit(attempt.clone()).await, false);
                assert!(
                    matches!(
                        cause,
                        Stop::Record(RecordError::SessionEpochNotRisen { found, required: 2 })
                            if found == found_epoch
                    ),
                    "{at}: {cause:?}"
                );
                rig.assert_untouched(&attempt, &at).await;
                assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot stays");
            })
            .await;
        }
    }
}

/// **(H2)** A renewal `StageError::InDoubt` in the same history keeps its doubt: the check applies
/// only to an arm that would go on to send a batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_a_renewal_in_doubt_over_an_adopted_session_keeps_its_doubt() {
    for damage in ["same epoch", "lower epoch"] {
        within(async {
            let (rig, attempt, plant, _) = staged_at_epoch_2(damage).await;
            rig.store.script(
                Kind::Renewal,
                vec![Act::conflict().before(plant), Act::unknown(false, true)],
                Some(Act::conflict()),
            );
            let error = renewal_stopped(rig.commit(attempt.clone()).await, false);
            let StageError::InDoubt(doubt) = &error else {
                panic!("{damage}: expected the renewal's doubt, got {error:?}");
            };
            assert!(doubt.may_still_commit, "{damage}");
            assert_eq!(classify(&error), ErrorClass::Indeterminate, "{damage}");
            rig.assert_untouched(&attempt, damage).await;
        })
        .await;
    }
}

// ─────────────── (H3) the slot guard names the submitter ───────────────

/// **(H3)** The guard rejects attempt A's batch carrying attempt B's bytes, after A released the
/// index and B reserved it, for the reservation shape and for the later-batch shape. Every other leg
/// of this file runs the same guard on every batch it sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h3_the_slot_guard_rejects_a_batch_carrying_another_attempts_bytes() {
    within(async {
        let mut rig = Rig::new().await;
        let a = rig.stage(1, &body(CHUNK, 158)).await;
        committed_as(&rig.commit(a.clone()).await, &a);
        let b = rig.stage(2, &body(CHUNK, 159)).await;
        assert_eq!(
            a.slot_key(),
            b.slot_key(),
            "B reserved the index A released"
        );
        assert_ne!(a.slot_bytes(), b.slot_bytes());
        let key = || b.slot_key().to_vec();
        let (a_id, b_id) = (a.attempt_id(), b.attempt_id());

        // A's batch requiring B's bytes: it holds nothing, so it can only be a reservation.
        let carrying = WriteBatch::new()
            .require(key(), b.slot_bytes().clone())
            .delete(key());
        let err = rig
            .store
            .check_slot_guard(a_id, &carrying)
            .expect_err("A carries B's bytes");
        assert!(err.contains("holds nothing"), "{err}");

        // A's reservation putting a value that names B.
        let naming = WriteBatch::new()
            .require_absent(key())
            .put(key(), b.slot_bytes().clone());
        let err = rig
            .store
            .check_slot_guard(a_id, &naming)
            .expect_err("A's reservation names B");
        assert!(err.contains("naming"), "{err}");

        // B's batch requiring bytes it did not last write.
        let stale = WriteBatch::new()
            .require(key(), a.slot_bytes().clone())
            .delete(key());
        let err = rig
            .store
            .check_slot_guard(b_id, &stale)
            .expect_err("B requires A's old bytes");
        assert!(err.contains("did not last write"), "{err}");

        // A slot deleted under no requirement at all.
        let bare = WriteBatch::new().delete(key());
        rig.store
            .check_slot_guard(b_id, &bare)
            .expect_err("no requirement on the slot");

        // The two shapes that conform.
        let own = WriteBatch::new()
            .require(key(), b.slot_bytes().clone())
            .delete(key());
        rig.store
            .check_slot_guard(b_id, &own)
            .expect("B's batch, requiring the bytes it holds");
        let reserve = WriteBatch::new()
            .require_absent(key())
            .put(key(), a.slot_bytes().clone());
        rig.store
            .check_slot_guard(a_id, &reserve)
            .expect("A's reservation, naming A");
    })
    .await;
}

// ─────────────── (H4) a batch answered unknown is the last one sent ───────────────

/// **(H4) One batch, then reads** (reworked by #1029). A commit answered `CommitUnknownResult`, for
/// each `(applied, may_still_commit)` pair: exactly one commit batch and no compensation, whatever
/// the flag says. Applied, the re-read finds the slot gone under the unchanged session:
/// `Committed`, the slot released once. Not applied, it finds the slot held, which proves nothing:
/// the in-doubt error keeping the flag as reported, its cause `OutcomeUnknown`, and the slot still
/// held. Every read after the batch is a lap of the four reads, at most two laps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_commit_answered_unknown_is_in_doubt_with_no_further_batch() {
    for (applied, may_still_commit) in [(true, false), (true, true), (false, false), (false, true)]
    {
        within(async {
            let at = format!("applied {applied}, may_still_commit {may_still_commit}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 162)).await;
            rig.store.script(
                Kind::Commit,
                vec![Act::unknown(applied, may_still_commit)],
                None,
            );
            let outcome = rig.commit(attempt.clone()).await;
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: no retry");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.part(1).await.is_some(), applied, "{at}");
            // A not-applied round that may still land reads a second lap; nothing else does.
            let laps = if !applied && may_still_commit { 2 } else { 1 };
            assert_eq!(
                rig.log.reads_after(Kind::Commit, 0),
                laps_of(&rig, &attempt, laps),
                "{at}: only reads after the batch"
            );
            if applied {
                committed_as(&outcome, &attempt);
                assert_eq!(rig.releases(&attempt), 1, "{at}: released once");
                assert_eq!(rig.slots_of(&attempt).await, 0, "{at}");
            } else {
                let doubt = in_doubt(outcome);
                assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
                assert_eq!(doubt.may_still_commit, may_still_commit, "{at}");
                assert!(
                    matches!(doubt.cause, Stop::OutcomeUnknown),
                    "{at}: {:?}",
                    doubt.cause
                );
                assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot still held");
                assert_eq!(rig.releases(&attempt), 0, "{at}");
            }
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **(H4)** A commit held and landed after its in-doubt answer: nothing the attempt did stands in
/// its way, the slot is released once, and the part stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_held_commit_that_lands_later_is_released_once() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 163)).await;
        rig.store.script(Kind::Commit, vec![Act::hold()], None);
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        assert!(doubt.may_still_commit);
        assert!(rig.part(1).await.is_none(), "not landed yet");
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
        assert!(is_own(&rig.part(1).await.unwrap().0, &attempt));
        assert_eq!(rig.releases(&attempt), 1);
        assert_eq!(rig.slots_of(&attempt).await, 0);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// The three routes to a compensation: a settled renewal stop, a short lease, and a refusal past a
/// ceiling. Stages part 1 on a fresh rig, with the route's stop scripted.
async fn compensating(route: &str, seed: u8) -> (Rig, StagedAttempt) {
    let mut rig = Rig::new().await;
    if route == "ceiling" {
        rig.plant_summary(MAX_STAGED_CHUNKS).await;
    }
    let attempt = rig.stage(1, &body(CHUNK, seed)).await;
    match route {
        "renewal" => stop_the_renewal(&rig, &attempt, "rounds"),
        "lease" => {
            let spent = left_after_renewal(&rig, &attempt) - MARGIN + 1;
            rig.store.after_scan(vec![Step::Advance(spent)]);
        }
        _ => {}
    }
    (rig, attempt)
}

/// **(H4)** A compensation answered `CommitUnknownResult` returns `CommitError::InDoubt` keeping the
/// flag as reported, with no further batch, even for `may_still_commit: false`, on each route. A
/// compensation held and landed later releases the slot once and installs its obligation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_compensation_answered_unknown_is_in_doubt_with_no_further_batch() {
    for route in ["renewal", "lease", "ceiling"] {
        for (applied, may_still_commit) in [(true, false), (false, false), (false, true)] {
            within(async {
                let at = format!("{route}, applied {applied}, may {may_still_commit}");
                let (rig, attempt) = compensating(route, 165).await;
                rig.store.script(
                    Kind::Compensation,
                    vec![Act::unknown(applied, may_still_commit)],
                    None,
                );
                let doubt = in_doubt(rig.commit(attempt.clone()).await);
                assert_eq!(doubt.batch, DoubtfulBatch::Compensation, "{at}");
                assert_eq!(doubt.may_still_commit, may_still_commit, "{at}");
                assert_eq!(rig.log.commits(Kind::Compensation).len(), 1, "{at}");
                assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "{at}");
                assert_eq!(rig.slots_of(&attempt).await, usize::from(!applied), "{at}");
            })
            .await;
        }
        within(async {
            let (rig, attempt) = compensating(route, 166).await;
            rig.store
                .script(Kind::Compensation, vec![Act::hold()], None);
            in_doubt(rig.commit(attempt.clone()).await);
            assert_eq!(rig.slots_of(&attempt).await, 1, "{route}: not landed yet");
            assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
            assert_eq!(rig.releases(&attempt), 1, "{route}");
            assert_eq!(rig.slots_of(&attempt).await, 0, "{route}");
            assert_eq!(rig.part_obligations().await.len(), 1, "{route}");
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **(H4)** An in-doubt error through the composed `upload_part`, read through
/// `wyrd_traits::classify`, is `Indeterminate`, and it says what the commit's says: for a commit, a
/// compensation and a quiesce renewal. A staging error and a staging refusal come through the
/// composition as themselves, with no commit sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_composed_error_keeps_its_class_and_its_message() {
    within(async {
        let mut rig = Rig::new().await;
        rig.store.script(Kind::Commit, vec![Act::hold()], None);
        let err = rig
            .upload(1, &body(CHUNK, 191), MAX_PART_CHUNKS)
            .await
            .expect_err("the commit is in doubt");
        assert_eq!(classify(&err), ErrorClass::Indeterminate, "{err}");
        let text = err.to_string();
        let UploadPartError::Commit(inner) = err else {
            panic!("expected the commit's error, got {err:?}");
        };
        assert_eq!(text, inner.to_string());
        assert!(text.contains("is in doubt"), "{text}");
        assert!(matches!(inner, CommitError::InDoubt(_)));

        // A compensation in doubt, past the cumulative ceiling.
        let mut rig = Rig::new().await;
        rig.plant_summary(MAX_STAGED_CHUNKS).await;
        rig.store
            .script(Kind::Compensation, vec![Act::unknown(false, false)], None);
        let err = rig
            .upload(1, &body(CHUNK, 192), MAX_PART_CHUNKS)
            .await
            .expect_err("the compensation is in doubt");
        assert_eq!(classify(&err), ErrorClass::Indeterminate, "{err}");

        // A quiesce renewal in doubt.
        let mut rig = Rig::new().await;
        rig.store.script(
            Kind::Renewal,
            vec![Act::unknown(false, true)],
            Some(Act::conflict()),
        );
        let err = rig
            .upload(1, &body(CHUNK, 193), MAX_PART_CHUNKS)
            .await
            .expect_err("the renewal is in doubt");
        assert_eq!(classify(&err), ErrorClass::Indeterminate, "{err}");

        // A commit stopped with nothing in doubt is not: every round answered `Conflict` with
        // nothing a read explains, and then compensated.
        let mut rig = Rig::new().await;
        rig.store
            .script(Kind::Commit, vec![], Some(Act::conflict()));
        let err = rig
            .upload(1, &body(CHUNK, 194), MAX_PART_CHUNKS)
            .await
            .expect_err("the commit rounds were spent");
        assert_eq!(classify(&err), ErrorClass::Terminal, "{err}");

        // A staging error.
        let mut rig = Rig::new().await;
        rig.store.script(Kind::Reserve, vec![Act::fail()], None);
        let err = rig
            .upload(1, &body(CHUNK, 195), MAX_PART_CHUNKS)
            .await
            .expect_err("staging stopped");
        assert_eq!(classify(&err), ErrorClass::Terminal, "{err}");
        let text = err.to_string();
        let UploadPartError::Stage(inner) = err else {
            panic!("expected staging's error, got {err:?}");
        };
        assert_eq!(text, inner.to_string());
        assert!(text.contains("multipart staging"), "{text}");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 0);

        // A staging refusal.
        let mut rig = Rig::new().await;
        rig.run(vec![rig.abort()]).await;
        let outcome = rig
            .upload(1, &body(CHUNK, 196), MAX_PART_CHUNKS)
            .await
            .expect("the upload ran");
        assert_eq!(outcome, UploadPartOutcome::Refused(Refusal::NoSuchUpload));
        assert_eq!(rig.log.commits(Kind::Commit).len(), 0);
    })
    .await;
}

// ─────────────── #1028: helpers ───────────────

// deferred: #660 — seeded Tier-0 DST coverage of the races below (a commit against a rival, an
// Abort, a Complete's rollback, a terminal delete), as `commit.rs` records; here each interleaving
// is forced by the scripted wrapper over a real store.

/// `Step::AfterRead`, spelled for a key borrowed from an attempt.
fn after_read(key: &[u8], then: Vec<Step>) -> Step {
    Step::AfterRead(key.to_vec(), then)
}

/// Whether two batches are the same bytes: the same preconditions, puts and deletes, in order.
fn same_batch(a: &WriteBatch, b: &WriteBatch) -> bool {
    a.preconditions == b.preconditions && a.puts == b.puts && a.deletes == b.deletes
}

/// The epoch each compensation batch so far required the session at, in order.
fn compensation_epochs(rig: &Rig) -> Vec<u64> {
    rig.log
        .commits(Kind::Compensation)
        .iter()
        .map(|batch| required_epoch(batch, &rig.upload))
        .collect()
}

/// The key `attempt`'s per-part obligation goes under at `epoch`.
fn obligation_key(attempt: &StagedAttempt, epoch: u64) -> Vec<u8> {
    retire_key(RetireMode::Bytes, &token_of(attempt, epoch))
}

/// `attempt`'s slot record with another attempt's id in it: what its index holds once it was
/// released and reserved again.
fn foreign_slot(attempt: &StagedAttempt) -> Bytes {
    let text = std::str::from_utf8(attempt.slot_bytes()).unwrap();
    let own = attempt.attempt_id().to_string();
    let other = AttemptId::new("f".repeat(32)).unwrap();
    assert_ne!(other, *attempt.attempt_id());
    assert_eq!(text.matches(&own).count(), 1);
    let foreign = Bytes::from(text.replace(&own, &other.to_string()));
    let record = decode_slot_record(&foreign).expect("the foreign slot decodes");
    assert_eq!(*record.attempt_id(), other);
    foreign
}

/// `attempt`'s slot record under its own id with a later lease: bytes it never wrote, since no
/// renewal of a leg that does not move the clock stamps a lease this far on.
fn rewritten_slot(attempt: &StagedAttempt) -> Bytes {
    let record = decode_slot_record(attempt.slot_bytes()).unwrap();
    let text = std::str::from_utf8(attempt.slot_bytes()).unwrap();
    let from = format!("\"lease_expiry_millis\":{}", record.lease_expiry_millis());
    let to = format!(
        "\"lease_expiry_millis\":{}",
        record.lease_expiry_millis() + 1_000
    );
    assert_eq!(text.matches(&from).count(), 1);
    let rewritten = Bytes::from(text.replace(&from, &to));
    let decoded = decode_slot_record(&rewritten).expect("the rewritten slot decodes");
    assert_eq!(decoded.attempt_id(), attempt.attempt_id());
    rewritten
}

/// The session at `bytes` fenced to `Aborting` as a real fence writes it (`open_teardown`), but at
/// `epoch` in place of the next one. With `epoch` not above the record's own, no legal history
/// writes it: every fence bumps the epoch (`0016:704-708`).
fn aborting_at(bytes: &Bytes, upload: &UploadId, epoch: u64) -> Bytes {
    let record = decode_session_record(bytes).unwrap();
    let fenced = metadata::encode(
        record
            .open_teardown(upload)
            .expect("fenced from Open")
            .session(),
    );
    let text = std::str::from_utf8(&fenced).unwrap();
    let from = format!("\"epoch\":{},", record.epoch() + 1);
    let to = format!("\"epoch\":{epoch},");
    assert_eq!(text.matches(&from).count(), 1);
    let other = Bytes::from(text.replace(&from, &to));
    let decoded = decode_session_record(&other).expect("the planted session decodes");
    assert_eq!(decoded.epoch(), epoch);
    assert!(matches!(decoded.state(), SessionState::Aborting {}));
    other
}

/// A repair of part `n` in the shape of the custodian's staged re-place (`0016:671`): under
/// `require` on the session and the part bytes read, the record put back with chunk `chunk`'s first
/// fragment placed on a server that chunk does not use, every other byte kept.
fn repair(rig: &Rig, n: u32, chunk: usize) -> Step {
    let inner = Arc::clone(&rig.inner);
    let upload = rig.upload.clone();
    Step::Run(once(async move {
        let session_key = mpu_key(&upload);
        let session = inner.get(&session_key).await.unwrap().unwrap();
        let key = part_key(&upload, part(n));
        let stored = inner.get(&key).await.unwrap().expect("the part exists");
        let record = decode_part_record(&stored).unwrap();
        let mut chunks = record.chunks().to_vec();
        let to = (0..SERVERS)
            .find(|server| !chunks[chunk].placement.contains(server))
            .expect("a server the chunk does not use");
        chunks[chunk].placement[0] = to;
        let list = metadata::encode(&record.chunks());
        let at = stored
            .windows(list.len())
            .position(|window| window == &list[..])
            .expect("the stored bytes spell the chunk list canonically");
        let mut next = Vec::with_capacity(stored.len());
        next.extend_from_slice(&stored[..at]);
        next.extend_from_slice(&metadata::encode(&chunks));
        next.extend_from_slice(&stored[at + list.len()..]);
        let read_back = decode_part_record(&next).expect("the repaired record decodes");
        assert_eq!(read_back.chunks(), chunks.as_slice());
        let batch = WriteBatch::new()
            .require(session_key, session)
            .require(key.clone(), stored)
            .put(key, next);
        assert_eq!(inner.commit(batch).await.unwrap(), CommitOutcome::Committed);
    }))
}

/// A rig with part 1 staged as `bytes` by the attempt under test, and a rival attempt for the same
/// part number, staged after it, whose commit lands on the inner store just before the attempt's
/// own commit is sent. Returns the rival as staged.
async fn losing_to_a_rival(bytes: &[u8], seed: u8) -> (Rig, StagedAttempt, StagedAttempt) {
    let mut rig = Rig::new().await;
    let attempt = rig.stage(1, bytes).await;
    let rival = rig.stage(1, &body(CHUNK, seed)).await;
    rig.store.script(
        Kind::Commit,
        vec![Act::pass().before(rig.rival(rival.clone()))],
        None,
    );
    (rig, attempt, rival)
}

/// `cause` is `Stop::SlotLost`, naming `attempt`'s slot key.
fn assert_slot_lost(cause: &Stop, attempt: &StagedAttempt, at: &str) {
    let expected = String::from_utf8_lossy(attempt.slot_key()).into_owned();
    assert!(
        matches!(cause, Stop::SlotLost { key } if *key == expected),
        "{at}: {cause:?}"
    );
    assert!(cause.to_string().contains(&expected), "{at}: {cause}");
}

/// `cause` is `Stop::Record(SessionRecordMissing)` for the epoch the batch required.
fn assert_session_record_missing(cause: &Stop, required: u64, at: &str) {
    assert!(
        matches!(
            cause,
            Stop::Record(RecordError::SessionRecordMissing { required: found })
                if *found == required
        ),
        "{at}: {cause:?}"
    );
}

/// Asserts that `attempt` ended compensated by the last of its compensation batches, under
/// `epoch`, after any number that lost: one obligation for its own chunks under that epoch (none for
/// an empty part), its slot released once, no owned entry left.
async fn assert_finally_compensated(rig: &Rig, attempt: &StagedAttempt, epoch: u64) {
    let batches = rig.log.commits(Kind::Compensation);
    let last = batches.last().expect("a compensation was sent");
    assert_eq!(required_epoch(last, &rig.upload), epoch);
    let expected = if attempt.chunks().is_empty() {
        Vec::new()
    } else {
        vec![(token_of(attempt, epoch), chunk_ids(attempt.chunks()))]
    };
    assert_eq!(rig.part_obligations().await, expected);
    assert_eq!(rig.slots_of(attempt).await, 0, "no slot of the attempt");
    assert_eq!(rig.owned_of(attempt).await, 0, "no owned entry");
    assert_eq!(rig.releases(attempt), 1, "released once");
    rig.assert_every_fragment_named().await;
}

// ─────────────── (B) a lost commit is classified (#1028) ───────────────

/// **(B)** The session fenced to `Aborting` by a real Abort between the last renewal and the
/// commit: `NoSuchUpload`, and nothing more written. No compensation: the slot and the owned entries
/// are the reaper's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_fenced_session_answers_no_such_upload_and_writes_nothing() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK + 1, 9)).await;
        rig.store
            .script(Kind::Commit, vec![Act::pass().before(rig.abort())], None);
        refused(&rig.commit(attempt.clone()).await, Refusal::NoSuchUpload);
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "no retry");
        assert_eq!(
            rig.log.commits(Kind::Compensation).len(),
            0,
            "no compensation"
        );
        assert_eq!(rig.slots_of(&attempt).await, 1, "the slot is the reaper's");
        assert_eq!(rig.owned_of(&attempt).await, attempt.owned().len());
        assert_eq!(rig.releases(&attempt), 0);
        assert!(rig.part(1).await.is_none());
        assert!(rig.part_obligations().await.is_empty());
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(B)** The session rolled back to `Open@E'` (`E' = E + 2`: a Complete fenced and rolled back)
/// between the last renewal and the commit: the attempt compensates under `require(mpu ==
/// Open@E')`, one obligation for its own chunks under `E'`, its slot released and its owned entries
/// deleted in one batch, and answers exactly `OperationAborted`, never `Committed` and never
/// `NoSuchUpload`. For a part with chunks and for an empty one, which writes no obligation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_rolled_back_session_is_compensated_under_its_newer_epoch() {
    for bytes in [body(2 * CHUNK, 4), Vec::new()] {
        within(async {
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &bytes).await;
            rig.store
                .script(Kind::Commit, vec![Act::pass().before(rig.rollback())], None);
            refused(
                &rig.commit(attempt.clone()).await,
                Refusal::OperationAborted,
            );
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "no retry");
            let session = decode_session_record(&rig.session().await).unwrap();
            assert_eq!(session.epoch(), 2);
            assert!(
                matches!(session.state(), SessionState::Open {}),
                "still Open"
            );
            rig.assert_compensated(&attempt, 2).await;
            assert!(rig.part(1).await.is_none());
        })
        .await;
    }
}

/// **(B)** A same-part rival committed between the loser's last renewal and its commit: the loser
/// commits one obligation for its **own** chunks (decoded by `decode_retire_obligation`), releases
/// its slot and deletes its owned entries in one batch, and answers exactly `OperationAborted`. The
/// rival's part stands. An empty loser writes no obligation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_same_part_loser_compensates_its_own_chunks() {
    for bytes in [body(CHUNK, 1), Vec::new()] {
        within(async {
            let (rig, loser, rival) = losing_to_a_rival(&bytes, 2).await;
            refused(&rig.commit(loser.clone()).await, Refusal::OperationAborted);
            let (record, _) = rig.part(1).await.unwrap();
            assert!(is_own(&record, &rival), "the rival's part stands");
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "no retry");
            rig.assert_compensated(&loser, 0).await;
            let puts = rig.log.commits(Kind::Compensation)[0].puts.len();
            assert_eq!(
                puts,
                usize::from(!loser.chunks().is_empty()),
                "the obligation"
            );
        })
        .await;
    }
}

/// **(B)** A repair rewrote the prior part record a re-upload required, with nothing in doubt: the
/// re-upload compensates and answers `OperationAborted`, with no retry against the new record. The
/// repaired prior stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_repair_of_the_prior_is_compensated_with_no_retry() {
    within(async {
        let mut rig = Rig::new().await;
        let prior = rig.committed(1, &body(2 * CHUNK, 5)).await;
        let attempt = rig.stage(1, &body(CHUNK, 6)).await;
        rig.store.script(
            Kind::Commit,
            vec![Act::pass().before(repair(&rig, 1, 0))],
            None,
        );
        refused(
            &rig.commit(attempt.clone()).await,
            Refusal::OperationAborted,
        );
        assert_eq!(
            rig.log.commits(Kind::Commit).len(),
            1,
            "no retry against the new record"
        );
        let (record, _) = rig.part(1).await.unwrap();
        assert_eq!(record.digest(), prior.digest(), "the prior stands");
        assert_eq!(chunk_ids(record.chunks()), chunk_ids(prior.chunks()));
        assert_ne!(record, prior, "repaired");
        rig.assert_compensated(&attempt, 0).await;
    })
    .await;
}

/// **(B)** A conflict no read explains (the part as required, the session `Open@E`, the slot held):
/// the same batch is sent again, and lands. Between the two, the re-read is the part key, the
/// session, the attempt's own slot and the session again, in that order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_conflict_no_read_explains_sends_the_same_batch_again() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 7)).await;
        rig.store.script(Kind::Commit, vec![Act::conflict()], None);
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        let commits = rig.log.commits(Kind::Commit);
        assert_eq!(commits.len(), 2);
        assert!(same_batch(&commits[0], &commits[1]), "the same batch");
        let session = mpu_key(&rig.upload);
        assert_eq!(
            rig.log.reads_after(Kind::Commit, 0),
            vec![
                part_key(&rig.upload, part(1)),
                session.clone(),
                attempt.slot_key().to_vec(),
                session,
            ]
        );
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert!(is_own(&rig.part(1).await.unwrap().0, &attempt));
        assert_eq!(rig.releases(&attempt), 1);
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(B)** A compensation answered `Conflict` with nothing a read explains (no record at its
/// obligation key, the slot held, the session unchanged) sends the same batch again, on each route
/// to a compensation, and the route's own answer stands once it lands. Its re-read is the obligation
/// key, the slot and the session, in that order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_compensation_conflict_no_read_explains_sends_the_same_batch_again() {
    for route in ["renewal", "lease", "ceiling"] {
        within(async {
            let (rig, attempt) = compensating(route, 164).await;
            rig.store
                .script(Kind::Compensation, vec![Act::conflict()], None);
            let outcome = rig.commit(attempt.clone()).await;
            match route {
                "renewal" => {
                    renewal_stopped(outcome, true);
                }
                "lease" => {
                    let cause = stopped(outcome, true);
                    assert!(matches!(cause, Stop::LeaseShort { .. }), "{cause:?}");
                }
                _ => refused(
                    &outcome,
                    Refusal::EntityTooLarge {
                        chunks: u64::from(MAX_STAGED_CHUNKS) + 1,
                        limit: u64::from(MAX_STAGED_CHUNKS),
                    },
                ),
            }
            let batches = rig.log.commits(Kind::Compensation);
            assert_eq!(batches.len(), 2, "{route}");
            assert!(same_batch(&batches[0], &batches[1]), "{route}");
            assert_eq!(
                rig.log.reads_after(Kind::Compensation, 0),
                vec![
                    obligation_key(&attempt, 0),
                    attempt.slot_key().to_vec(),
                    mpu_key(&rig.upload),
                ],
                "{route}"
            );
            assert_finally_compensated(&rig, &attempt, 0).await;
        })
        .await;
    }
}

// ─────────────── (T) a terminal delete is not damage (#1028) ───────────────

/// **(T) At the commit's re-read.** A real Abort fences the session, so the commit answers
/// `Conflict`; the re-read finds the part as required, the session `Aborting` and the slot held;
/// **then** the terminal delete lands, removing the session, the slots and the owned entries in one
/// batch, and the last session read finds nothing. The slot key read once more after that is
/// absent: `NoSuchUpload`, never `SessionRecordMissing`. The same with the delete landing before the
/// commit, so that every read finds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_a_terminal_delete_between_the_commits_reads_answers_no_such_upload() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 11)).await;
        let slot = attempt.slot_key().to_vec();
        let delete = after_read(&slot, vec![rig.terminal_delete()]);
        rig.store.script(
            Kind::Commit,
            vec![Act::pass().before(rig.abort()).after(delete)],
            None,
        );
        refused(&rig.commit(attempt.clone()).await, Refusal::NoSuchUpload);
        let session = mpu_key(&rig.upload);
        assert_eq!(
            rig.log.reads_after(Kind::Commit, 0),
            vec![
                part_key(&rig.upload, part(1)),
                session.clone(),
                slot.clone(),
                session.clone(),
                slot,
            ],
            "the four reads, then the slot key once more"
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert!(rig.inner.get(&session).await.unwrap().is_none(), "deleted");
        assert_eq!(rig.slots().await, 0);

        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 12)).await;
        rig.store.script(
            Kind::Commit,
            vec![Act::pass().before(rig.terminal_delete())],
            None,
        );
        refused(&rig.commit(attempt.clone()).await, Refusal::NoSuchUpload);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
    })
    .await;
}

/// **(T) At the compensation's re-read.** A loser to a same-number rival compensates; a real Abort
/// fences the session first, so the compensation answers `Conflict`; its re-read finds no record at
/// the obligation key and the slot held; **then** the terminal delete lands, and its session read
/// finds nothing. The slot key read once more after that is absent: `NoSuchUpload`, with no further
/// write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_a_terminal_delete_between_the_compensations_reads_answers_no_such_upload() {
    within(async {
        let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 13), 14).await;
        let slot = loser.slot_key().to_vec();
        let delete = after_read(&slot, vec![rig.terminal_delete()]);
        rig.store.script(
            Kind::Compensation,
            vec![Act::pass().before(rig.abort()).after(delete)],
            None,
        );
        refused(&rig.commit(loser.clone()).await, Refusal::NoSuchUpload);
        assert_eq!(
            rig.log.reads_after(Kind::Compensation, 0),
            vec![
                obligation_key(&loser, 0),
                slot.clone(),
                mpu_key(&rig.upload),
                slot,
            ],
            "its three reads, then the slot key once more"
        );
        assert_eq!(
            rig.log.commits(Kind::Compensation).len(),
            1,
            "no further write"
        );
        assert!(rig.part_obligations().await.is_empty());
        assert_eq!(rig.slots().await, 0);
    })
    .await;
}

/// **(T) The slot still held behind a missing session.** Only the session record is erased, which
/// no terminal delete does (it takes the slots in the same batch): the slot key read after the
/// missing session still holds this attempt's slot, so the answer is
/// `Stop::Record(SessionRecordMissing)`, with no compensation and no release claimed, at the
/// commit's re-read and at the compensation's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_a_missing_session_with_the_slot_still_held_is_damage() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 15)).await;
        let gone = rig.erased(vec![mpu_key(&rig.upload)]);
        rig.store
            .script(Kind::Commit, vec![Act::pass().before(gone)], None);
        let cause = stopped(rig.commit(attempt.clone()).await, false);
        assert_session_record_missing(&cause, 0, "commit");
        assert!(cause.to_string().contains("terminal delete"), "{cause}");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 1, "the slot stays");
        assert_eq!(rig.releases(&attempt), 0);

        let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 16), 17).await;
        let gone = rig.erased(vec![mpu_key(&rig.upload)]);
        rig.store
            .script(Kind::Compensation, vec![Act::pass().before(gone)], None);
        let cause = stopped(rig.commit(loser.clone()).await, false);
        assert_session_record_missing(&cause, 0, "compensation");
        assert_eq!(
            rig.log.commits(Kind::Compensation).len(),
            1,
            "no further write"
        );
        assert!(rig.part_obligations().await.is_empty());
        assert_eq!(rig.slots_of(&loser).await, 1, "the slot stays");
        assert_eq!(rig.releases(&loser), 0);
    })
    .await;
}

/// **(T) Another attempt's slot behind a missing session.** Right after the re-read's slot read
/// finds this attempt's slot held, the session record is erased and the slot key given another
/// attempt's record. The slot key read after the missing session holds that record: damage,
/// `Stop::Record(SessionRecordMissing)`, never a terminal delete, at the commit's re-read and at
/// the compensation's. Nothing is compensated or released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn t_another_attempts_slot_behind_a_missing_session_is_damage() {
    for phase in ["commit", "compensation"] {
        within(async {
            let (rig, attempt) = if phase == "commit" {
                let mut rig = Rig::new().await;
                let attempt = rig.stage(1, &body(CHUNK, 18)).await;
                (rig, attempt)
            } else {
                let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 19), 20).await;
                (rig, loser)
            };
            let slot = attempt.slot_key().to_vec();
            let swap = after_read(
                &slot,
                vec![
                    rig.erased(vec![mpu_key(&rig.upload)]),
                    rig.plant(slot.clone(), foreign_slot(&attempt)),
                ],
            );
            let kind = if phase == "commit" {
                Kind::Commit
            } else {
                Kind::Compensation
            };
            rig.store
                .script(kind, vec![Act::conflict().after(swap)], None);
            let cause = stopped(rig.commit(attempt.clone()).await, false);
            assert_session_record_missing(&cause, 0, phase);
            let sent = usize::from(phase == "compensation");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), sent, "{phase}");
            assert!(rig.part_obligations().await.is_empty(), "{phase}");
            assert_eq!(rig.releases(&attempt), 0, "{phase}");
            assert_eq!(
                rig.inner.get(&slot).await.unwrap(),
                Some(foreign_slot(&attempt)),
                "{phase}: the other attempt's slot is left as it is"
            );
        })
        .await;
    }
}

// ─────────────── (R) damage is a record stop (#1028) ───────────────

/// **(R) Bytes at the part key that do not decode**, planted before the commit over no prior, a
/// non-empty prior and an empty one, and under a session that moved (a rollback, a fence): a record
/// stop, never a rival, with no compensation, no retry and no release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_a_damaged_part_record_is_a_record_stop_not_a_rival() {
    for (prior, moved) in [
        (None, "none"),
        (Some(body(CHUNK, 21)), "none"),
        (Some(Vec::new()), "none"),
        (None, "rollback"),
        (None, "abort"),
    ] {
        within(async {
            let at = format!(
                "{:?} prior bytes, moved {moved}",
                prior.as_ref().map(Vec::len)
            );
            let mut rig = Rig::new().await;
            if let Some(bytes) = &prior {
                rig.committed(1, bytes).await;
            }
            let attempt = rig.stage(1, &body(CHUNK, 22)).await;
            let mut act = Act::pass()
                .before(rig.plant(part_key(&rig.upload, part(1)), Bytes::from_static(b"torn")));
            act = match moved {
                "rollback" => act.before(rig.rollback()),
                "abort" => act.before(rig.abort()),
                _ => act,
            };
            rig.store.script(Kind::Commit, vec![act], None);
            let cause = stopped(rig.commit(attempt.clone()).await, false);
            assert!(matches!(cause, Stop::Record(_)), "{at}: {cause:?}");
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: no retry");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot stays");
            assert_eq!(rig.owned_of(&attempt).await, 1, "{at}: the entry stays");
            assert_eq!(rig.releases(&attempt), 0, "{at}");
        })
        .await;
    }
}

/// **(R) A prior part record gone while the session is still `Open@E`**: `PartRecordMissing`, never
/// `OperationAborted`, with no compensation, over a non-empty prior and an empty one. Under a session
/// that moved, that session's row answers instead: a rollback compensates, a fence refuses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_a_prior_part_record_gone_under_the_same_session_is_damage() {
    for prior in [body(CHUNK, 23), Vec::new()] {
        for moved in ["none", "rollback", "abort"] {
            within(async {
                let at = format!("{} prior bytes, moved {moved}", prior.len());
                let mut rig = Rig::new().await;
                rig.committed(1, &prior).await;
                let attempt = rig.stage(1, &body(CHUNK, 24)).await;
                let gone = rig.erased(vec![
                    part_key(&rig.upload, part(1)),
                    psum_key(&rig.upload, part(1)),
                ]);
                let mut act = Act::pass().before(gone);
                act = match moved {
                    "rollback" => act.before(rig.rollback()),
                    "abort" => act.before(rig.abort()),
                    _ => act,
                };
                rig.store.script(Kind::Commit, vec![act], None);
                let outcome = rig.commit(attempt.clone()).await;
                match moved {
                    "none" => {
                        let cause = stopped(outcome, false);
                        assert!(
                            matches!(
                                cause,
                                Stop::Record(RecordError::PartRecordMissing { part_number: 1 })
                            ),
                            "{at}: {cause:?}"
                        );
                        assert!(
                            cause.to_string().contains("part 1's record is gone"),
                            "{at}: {cause}"
                        );
                        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
                        assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot stays");
                    }
                    "rollback" => {
                        refused(&outcome, Refusal::OperationAborted);
                        assert_eq!(compensation_epochs(&rig), vec![2], "{at}");
                        assert_eq!(rig.slots_of(&attempt).await, 0, "{at}");
                        assert_eq!(rig.releases(&attempt), 1, "{at}");
                    }
                    _ => {
                        refused(&outcome, Refusal::NoSuchUpload);
                        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
                    }
                }
                assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: no retry");
            })
            .await;
        }
    }
}

/// **(R) A damaged session at the last read.** The session is staged at epoch 2 (one rollback), and
/// planted before the batch, at the commit's re-read and at the compensation's: bytes that do not
/// decode, and records under other bytes at an epoch that has not risen, `Open` and `Aborting`, at
/// the same epoch and a lower one. Each is a record stop (`SessionEpochNotRisen` for a record),
/// never a rollback and never a fence, with no compensation and no release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_a_damaged_session_at_the_last_read_is_a_record_stop() {
    for phase in ["commit", "compensation"] {
        for kind in [
            "torn",
            "Open, same",
            "Open, lower",
            "Aborting, same",
            "Aborting, lower",
        ] {
            within(async {
                let at = format!("{kind} at the {phase}'s re-read");
                let mut rig = Rig::new().await;
                let older = rig.session().await;
                rig.run(vec![rig.rollback()]).await;
                let session = rig.session().await;
                let attempt = rig.stage(1, &body(CHUNK, 25)).await;
                let (planted, found) = match kind {
                    "torn" => (Bytes::from_static(b"torn"), None),
                    "Open, same" => (same_epoch_other_bytes(&session), Some(2)),
                    "Open, lower" => (older, Some(0)),
                    "Aborting, same" => (aborting_at(&session, &rig.upload, 2), Some(2)),
                    _ => (aborting_at(&session, &rig.upload, 1), Some(1)),
                };
                let plant = rig.plant(mpu_key(&rig.upload), planted);
                if phase == "commit" {
                    rig.store
                        .script(Kind::Commit, vec![Act::pass().before(plant)], None);
                } else {
                    let rival = rig.stage(1, &body(CHUNK, 26)).await;
                    rig.store.script(
                        Kind::Commit,
                        vec![Act::pass().before(rig.rival(rival))],
                        None,
                    );
                    rig.store
                        .script(Kind::Compensation, vec![Act::pass().before(plant)], None);
                }
                let cause = stopped(rig.commit(attempt.clone()).await, false);
                match found {
                    Some(found) => assert!(
                        matches!(
                            cause,
                            Stop::Record(RecordError::SessionEpochNotRisen { found: f, required: 2 })
                                if f == found
                        ),
                        "{at}: {cause:?}"
                    ),
                    None => assert!(matches!(cause, Stop::Record(_)), "{at}: {cause:?}"),
                }
                let sent = usize::from(phase == "compensation");
                assert_eq!(rig.log.commits(Kind::Compensation).len(), sent, "{at}");
                assert!(rig.part_obligations().await.is_empty(), "{at}");
                assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot stays");
                assert_eq!(rig.releases(&attempt), 0, "{at}");
            })
            .await;
        }
    }
}

/// **(R) Damage at the round's first session read is passed over.** The session is torn just before
/// the commit and restored right after the re-read's first session read: the last session read
/// finds it as the commit required, so the conflict is one no read explains, and the same batch is
/// sent again and lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_damage_at_the_first_session_read_is_passed_over() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 27)).await;
        let session = rig.session().await;
        let key = mpu_key(&rig.upload);
        let tear = rig.plant(key.clone(), Bytes::from_static(b"torn"));
        let restore = after_read(&key, vec![rig.plant(key.clone(), session)]);
        rig.store.script(
            Kind::Commit,
            vec![Act::conflict().before(tear).after(restore)],
            None,
        );
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        assert_eq!(rig.log.commits(Kind::Commit).len(), 2);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
    })
    .await;
}

/// **(R) A slot lost or rewritten at a re-read**: deleted, given another attempt's record, or
/// rewritten under this attempt's id, just before the commit and just before the compensation,
/// under a live session and under one a real Abort fenced just before that.
///
/// * Live: each is `SlotLost` naming the key.
/// * Fenced: only the slot rewritten under this attempt's id is `SlotLost` (only this attempt
///   writes its own id, and it has quiesced). A slot deleted or holding another attempt's record
///   is answered by the fence, `NoSuchUpload`, the residue the reaper's. So these legs tell
///   "another attempt's" from "this attempt's" by the answer.
/// * Bytes there that do not decode are `Record` under either session, never another attempt's.
///
/// In every leg: no compensation at the commit's re-read and no further batch after either
/// re-read, the slot key left as the step put it, and no release claimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_a_slot_lost_or_rewritten_at_a_re_read_stops_without_a_release() {
    for session in ["live", "fenced"] {
        for phase in ["commit", "compensation"] {
            for how in ["gone", "foreign", "rewritten", "torn"] {
                within(async {
                    let at = format!("slot {how} at the {phase}'s re-read, session {session}");
                    let (rig, attempt) = if phase == "commit" {
                        let mut rig = Rig::new().await;
                        let attempt = rig.stage(1, &body(CHUNK, 28)).await;
                        (rig, attempt)
                    } else {
                        let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 29), 30).await;
                        (rig, loser)
                    };
                    let key = attempt.slot_key().to_vec();
                    let (step, left) = match how {
                        "gone" => (rig.erased(vec![key.clone()]), None),
                        "foreign" => {
                            let foreign = foreign_slot(&attempt);
                            (rig.plant(key.clone(), foreign.clone()), Some(foreign))
                        }
                        "rewritten" => {
                            let rewritten = rewritten_slot(&attempt);
                            (rig.plant(key.clone(), rewritten.clone()), Some(rewritten))
                        }
                        _ => {
                            let torn = Bytes::from_static(b"torn");
                            (rig.plant(key.clone(), torn.clone()), Some(torn))
                        }
                    };
                    let kind = if phase == "commit" {
                        Kind::Commit
                    } else {
                        Kind::Compensation
                    };
                    let mut act = Act::conflict();
                    if session == "fenced" {
                        act = act.before(rig.abort());
                    }
                    rig.store.script(kind, vec![act.before(step)], None);
                    let outcome = rig.commit(attempt.clone()).await;
                    match (session, how) {
                        (_, "torn") => {
                            let cause = stopped(outcome, false);
                            assert!(matches!(cause, Stop::Record(_)), "{at}: {cause:?}");
                        }
                        ("fenced", "gone" | "foreign") => refused(&outcome, Refusal::NoSuchUpload),
                        _ => assert_slot_lost(&stopped(outcome, false), &attempt, &at),
                    }
                    assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
                    let sent = usize::from(phase == "compensation");
                    assert_eq!(rig.log.commits(Kind::Compensation).len(), sent, "{at}");
                    assert!(rig.part_obligations().await.is_empty(), "{at}");
                    assert_eq!(rig.releases(&attempt), 0, "{at}");
                    assert_eq!(
                        rig.inner.get(&key).await.unwrap(),
                        left,
                        "{at}: the slot key"
                    );
                    if session == "fenced" {
                        let fenced = decode_session_record(&rig.session().await).unwrap();
                        assert!(
                            matches!(fenced.state(), SessionState::Aborting {}),
                            "{at}: fenced"
                        );
                    }
                })
                .await;
            }
        }
    }
}

// ─────────────── (O) a taken obligation key (#1028) ───────────────

/// **(O)** Other bytes planted at the obligation's key, then a re-upload over a non-empty prior,
/// one with chunks and an empty one. Its commit requires that key absent, so every round answers
/// `Conflict` with nothing the round's four reads explain. When the rounds end, spent or stopped by
/// the lease before a later round, the attempt reads that key before it compensates, finds the
/// bytes, and stops `ObligationKeyTaken` with no compensation sent. The empty re-upload is the one
/// that needs that read: its compensation installs no obligation, so it would land past the bytes
/// and report the rounds spent. Those bytes and the prior are kept, and nothing is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o_a_taken_obligation_key_keeps_those_bytes_and_the_prior_part() {
    for bytes in [body(CHUNK, 32), Vec::new()] {
        for end in ["rounds", "lease"] {
            within(async {
                let at = format!("{} bytes, rounds ended by the {end}", bytes.len());
                let mut rig = Rig::new().await;
                let prior = rig.committed(1, &body(CHUNK, 31)).await;
                let attempt = rig.stage(1, &bytes).await;
                let key = obligation_key(&attempt, 0);
                let planted = Bytes::from_static(b"other");
                rig.run(vec![rig.plant(key.clone(), planted.clone())]).await;
                // Ended by the lease: every read moves the clock an eighth of the lease's spare
                // over the margin, so the reads after the second round take it below the margin.
                let rounds = if end == "lease" {
                    let spare = left_after_renewal(&rig, &attempt) - MARGIN;
                    rig.store.tick_reads(spare / 8);
                    2
                } else {
                    4
                };
                let cause = stopped(rig.commit(attempt.clone()).await, false);
                let expected = String::from_utf8_lossy(&key).into_owned();
                assert!(
                    matches!(&cause, Stop::ObligationKeyTaken { key } if *key == expected),
                    "{at}: {cause:?}"
                );
                assert!(cause.to_string().contains(&expected), "{at}: {cause}");
                assert_eq!(rig.inner.get(&key).await.unwrap(), Some(planted), "{at}");
                let (record, _) = rig.part(1).await.unwrap();
                assert_eq!(record, prior, "{at}: the prior part stands");
                assert_eq!(rig.log.commits(Kind::Commit).len(), rounds, "{at}");
                let session = mpu_key(&rig.upload);
                assert_eq!(
                    rig.log.reads_after(Kind::Commit, rounds - 1),
                    vec![
                        part_key(&rig.upload, part(1)),
                        session.clone(),
                        attempt.slot_key().to_vec(),
                        session,
                        key.clone(),
                    ],
                    "{at}: the round's four reads, then the obligation key"
                );
                assert_eq!(
                    rig.log.commits(Kind::Compensation).len(),
                    0,
                    "{at}: no compensation"
                );
                assert_eq!(
                    rig.slots_of(&attempt).await,
                    1,
                    "{at}: nothing was released"
                );
                assert_eq!(rig.owned_of(&attempt).await, attempt.owned().len(), "{at}");
                assert_eq!(rig.releases(&attempt), 0, "{at}");
            })
            .await;
        }
    }
}

// ─────────────── (L) bounds and the lease (#1028) ───────────────

/// **(L) Every loop ends after exactly its bound**, counted against numbers written here, never the
/// imported constants, with the wrapper answering every batch `Conflict` unapplied: the commit
/// rounds, whose compensation then lands; the commit rounds and then the compensation rounds; the
/// compensation rounds of a loser to a rival; and the epoch chase, every compensation losing to one
/// more rollback, each under a newer epoch than the last.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l_every_loop_ends_after_exactly_its_bound() {
    within(async {
        // The commit rounds, spent with nothing in doubt: compensate.
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 41)).await;
        rig.store
            .script(Kind::Commit, vec![], Some(Act::conflict()));
        let cause = stopped(rig.commit(attempt.clone()).await, true);
        assert!(
            matches!(cause, Stop::CommitRoundsSpent { rounds: 4 }),
            "{cause:?}"
        );
        assert_eq!(
            cause.to_string(),
            "all 4 part commits were answered Conflict with nothing a read could see to explain it"
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 4);
        rig.assert_compensated(&attempt, 0).await;

        // The commit rounds, then the compensation rounds.
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 42)).await;
        rig.store
            .script(Kind::Commit, vec![], Some(Act::conflict()));
        rig.store
            .script(Kind::Compensation, vec![], Some(Act::conflict()));
        let cause = stopped(rig.commit(attempt.clone()).await, false);
        assert!(
            matches!(cause, Stop::CompensationRoundsSpent { rounds: 4 }),
            "{cause:?}"
        );
        assert_eq!(
            cause.to_string(),
            "all 4 compensations were sent without one landing"
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 4);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 4);
        assert_eq!(rig.slots_of(&attempt).await, 1, "nothing was released");
        assert!(rig.part_obligations().await.is_empty());

        // The compensation rounds of a loser to a rival.
        let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 43), 44).await;
        rig.store
            .script(Kind::Compensation, vec![], Some(Act::conflict()));
        let cause = stopped(rig.commit(loser.clone()).await, false);
        assert!(
            matches!(cause, Stop::CompensationRoundsSpent { rounds: 4 }),
            "{cause:?}"
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 4);
        assert_eq!(rig.slots_of(&loser).await, 1, "nothing was released");

        // The epoch chase: every compensation loses to one more rollback.
        let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 45), 46).await;
        let chase = (0..4).map(|_| Act::pass().before(rig.rollback())).collect();
        rig.store.script(Kind::Compensation, chase, None);
        let cause = stopped(rig.commit(loser.clone()).await, false);
        assert!(
            matches!(cause, Stop::CompensationRoundsSpent { rounds: 4 }),
            "{cause:?}"
        );
        assert_eq!(compensation_epochs(&rig), vec![0, 2, 4, 6]);
        assert_eq!(rig.slots_of(&loser).await, 1, "nothing was released");
        assert!(rig.part_obligations().await.is_empty());
    })
    .await;
}

/// **(L) The epoch chase under every charged Complete fence.** A same-part rival beats the commit,
/// which spends no rollback; then three rollbacks, one per charged Complete fence the shipped
/// `MAX_COMPLETE_ATTEMPTS` allows (each spends an attempt, so the session ends at three), each beat
/// one compensation, and the fourth compensation lands: exactly four, the last under `Open@6`. Only
/// this run needs the whole budget. In a second run a rollback beats the commit, and two more beat
/// compensations: the third compensation lands, under `Open@6`.
///
/// Three is the most rollbacks **charged** fences can make, not the most a session can see: a
/// Complete rejected behind its fence gives its attempt back when it releases it (#1007), so
/// rejected Completes can roll the session back any number of times. `COMPENSATION_ROUNDS` is a
/// budget for the work, not for the answer, and a fourth rollback against a compensation spends
/// it (`l_every_loop_ends_after_exactly_its_bound`'s chase leg).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l_the_epoch_chase_lands_within_its_bound_after_one_rollback_per_charged_complete() {
    within(async {
        let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 47), 48).await;
        let chase = (0..3).map(|_| Act::pass().before(rig.rollback())).collect();
        rig.store.script(Kind::Compensation, chase, None);
        refused(&rig.commit(loser.clone()).await, Refusal::OperationAborted);
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
        assert_eq!(compensation_epochs(&rig), vec![0, 2, 4, 6], "exactly 4");
        let session = decode_session_record(&rig.session().await).unwrap();
        assert_eq!((session.epoch(), session.attempts()), (6, 3));
        assert!(matches!(session.state(), SessionState::Open {}), "live");
        assert_finally_compensated(&rig, &loser, 6).await;

        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 49)).await;
        rig.store
            .script(Kind::Commit, vec![Act::pass().before(rig.rollback())], None);
        let chase = (0..2).map(|_| Act::pass().before(rig.rollback())).collect();
        rig.store.script(Kind::Compensation, chase, None);
        refused(
            &rig.commit(attempt.clone()).await,
            Refusal::OperationAborted,
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
        assert_eq!(compensation_epochs(&rig), vec![2, 4, 6], "exactly 3");
        assert_finally_compensated(&rig, &attempt, 6).await;
    })
    .await;
}

/// **(L) During a compensation**, on each route to one: a real Abort fences the session before the
/// compensation, which answers `Conflict`, and its re-read finds the session fenced:
/// `NoSuchUpload`, with no further write. The slot and the owned entries are the reaper's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l_a_compensation_that_finds_the_session_fenced_answers_no_such_upload() {
    for route in ["renewal", "lease", "ceiling", "rival", "rounds"] {
        within(async {
            let (rig, attempt) = match route {
                "rival" => {
                    let (rig, loser, _) = losing_to_a_rival(&body(CHUNK, 51), 52).await;
                    (rig, loser)
                }
                "rounds" => {
                    let mut rig = Rig::new().await;
                    let attempt = rig.stage(1, &body(CHUNK, 53)).await;
                    rig.store
                        .script(Kind::Commit, vec![], Some(Act::conflict()));
                    (rig, attempt)
                }
                _ => compensating(route, 54).await,
            };
            rig.store.script(
                Kind::Compensation,
                vec![Act::pass().before(rig.abort())],
                None,
            );
            refused(&rig.commit(attempt.clone()).await, Refusal::NoSuchUpload);
            assert_eq!(
                rig.log.commits(Kind::Compensation).len(),
                1,
                "{route}: no further write"
            );
            assert_eq!(rig.slots_of(&attempt).await, 1, "{route}: the reaper's");
            assert_eq!(rig.owned_of(&attempt).await, 1, "{route}");
            assert!(rig.part_obligations().await.is_empty(), "{route}");
            assert_eq!(rig.releases(&attempt), 0, "{route}");
        })
        .await;
    }
}

/// **(L) The lease between rounds.** `Conflict` rounds separated by slow reads (every read moves the
/// clock an eighth of the lease's spare over the margin): the first two rounds are sent with the
/// margin left, and the four reads after the second take the lease below it, so no third commit is
/// sent and the attempt compensates. The answer is `LeaseShort`, never `Committed`; the wrapper
/// fails any round sent with less.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l_a_lease_used_up_between_rounds_stops_them_and_compensates() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 55)).await;
        let spare = left_after_renewal(&rig, &attempt) - MARGIN;
        rig.store.tick_reads(spare / 8);
        rig.store
            .script(Kind::Commit, vec![], Some(Act::conflict()));
        let cause = stopped(rig.commit(attempt.clone()).await, true);
        assert!(matches!(cause, Stop::LeaseShort { .. }), "{cause:?}");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 2, "no third commit");
        rig.assert_compensated(&attempt, 0).await;
        assert!(rig.part(1).await.is_none());
    })
    .await;
}

// ─────────────── #1029: helpers ───────────────

// deferred: #660 — seeded Tier-0 DST coverage of a commit in doubt against a rival, a rollback, an
// Abort, the idle fence and late staging, as `commit.rs` records; here each interleaving is forced
// by the scripted wrapper over a real store.

/// The session's `slot:` records as one read found them, for a test-local idle fence to pin.
type SeenSlots = Arc<Mutex<Vec<(Vec<u8>, Bytes)>>>;

/// What a step read from the inner store, key by key.
type Seen = Arc<Mutex<Vec<Option<Bytes>>>>;

/// The most laps of reads a commit answered unknown makes (`SETTLE_LAPS`), written here rather
/// than imported, as [`MOST_ROUNDS`] is.
const MOST_LAPS: usize = 2;

impl Scripted {
    /// How many batches the wrapper holds unapplied.
    fn held_count(&self) -> usize {
        self.state.lock().unwrap().held.len()
    }

    /// How many batches of `kind` sent by `submitter` landed, held ones released included.
    fn landed_of(&self, submitter: &AttemptId, kind: Kind) -> usize {
        self.state
            .lock()
            .unwrap()
            .landed
            .iter()
            .filter(|(by, batch)| by == submitter && kind_of(batch) == kind)
            .count()
    }
}

impl Act {
    /// Runs every one of `steps` after the batch, in order.
    fn after_all(mut self, steps: Vec<Step>) -> Self {
        self.after.extend(steps);
        self
    }
}

impl Rig {
    /// Lands the oldest held batch on the inner store, which must meet `expected`. The step holds
    /// the wrapper weakly, so a script never keeps its own wrapper alive.
    fn land_held(&self, expected: CommitOutcome) -> Step {
        let store = Arc::downgrade(&self.store);
        Step::Run(once(async move {
            let store = store.upgrade().expect("the wrapper outlives its script");
            assert_eq!(store.release_held().await, expected, "the held batch");
        }))
    }

    /// From now on, every read of `key` through the wrapper answers a store fault.
    fn fail_reads_from_now(&self, key: Vec<u8>) -> Step {
        let store = Arc::downgrade(&self.store);
        Step::Run(once(async move {
            store
                .upgrade()
                .expect("the wrapper outlives its script")
                .fail_reads(key);
        }))
    }

    /// Steps to run after a batch so that each `(gap, steps)` runs at that gap of the first lap
    /// of reads that follows it: gap 0 before the part read, 1 after it, 2 after the first session
    /// read, 3 after the slot read and 4 after the last session read. One chain of
    /// [`Step::AfterRead`] hooks, each armed by the one before, so no two hooks wait on one key at
    /// once; the chain ends at the last gap that has steps.
    fn at_gaps(&self, attempt: &StagedAttempt, gaps: Vec<(usize, Vec<Step>)>) -> Vec<Step> {
        let reads = lap(self, attempt);
        let mut at = vec![Vec::new(); reads.len() + 1];
        for (gap, steps) in gaps {
            at[gap].extend(steps);
        }
        let last = at.iter().rposition(|steps| !steps.is_empty()).unwrap_or(0);
        let mut chain = std::mem::take(&mut at[last]);
        for gap in (1..=last).rev() {
            let mut steps = std::mem::take(&mut at[gap - 1]);
            steps.push(Step::AfterRead(reads[gap - 1].clone(), chain));
            chain = steps;
        }
        chain
    }

    /// Every `sidx:` entry and `slot:` record of the session deleted on the inner store, its
    /// record kept: what the reaper's teardown (#659) of a fenced session takes.
    fn teardown(&self) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        Step::Run(once(async move {
            let mut batch = WriteBatch::new();
            for range in [sidx_range(&upload), slot_range(&upload)] {
                for (key, _) in inner.scan(&range).await.unwrap() {
                    batch = batch.delete(key);
                }
            }
            assert_eq!(inner.commit(batch).await.unwrap(), CommitOutcome::Committed);
        }))
    }

    /// `keys` as the inner store holds them now, read into `into`.
    fn read_into(&self, keys: Vec<Vec<u8>>, into: Seen) -> Step {
        let inner = Arc::clone(&self.inner);
        Step::Run(once(async move {
            let mut found = Vec::with_capacity(keys.len());
            for key in &keys {
                found.push(inner.get(key).await.unwrap());
            }
            *into.lock().unwrap() = found;
        }))
    }

    /// The session's slot range as it stands now, read into `into`: the liveness evidence an idle
    /// fence pins (`0016:1859-1863`).
    fn read_slots(&self, into: SeenSlots) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        Step::Run(once(async move {
            let found = inner.scan(&slot_range(&upload)).await.unwrap();
            *into.lock().unwrap() = found;
        }))
    }

    /// A test-local batch in the idle fence's shape (`0016:1859-1863`; the reaper's own is #625):
    /// the session required at the bytes read now, each slot index `seen` held required at those
    /// bytes and every other index required absent, and the session moved to `Aborting` with its
    /// `{session, all}` obligation, as a fence writes it. It must answer `expected`.
    fn idle_fence(&self, seen: SeenSlots, expected: CommitOutcome) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        Step::Run(once(async move {
            let key = mpu_key(&upload);
            let session = inner.get(&key).await.unwrap().expect("the session exists");
            let record = decode_session_record(&session).unwrap();
            let fenced = record.open_teardown(&upload).expect("fenced from Open");
            let seen = seen.lock().unwrap().clone();
            let mut batch = WriteBatch::new().require(key.clone(), session);
            for index in 0..MAX_INFLIGHT_PARTS {
                let slot = slot_key(&upload, SlotIndex::new(index).unwrap());
                batch = match seen.iter().find(|(held, _)| *held == slot) {
                    Some((_, bytes)) => batch.require(slot, bytes.clone()),
                    None => batch.require_absent(slot),
                };
            }
            let obligation = fenced.obligation();
            batch = batch
                .require_absent(obligation.key())
                .put(key, metadata::encode(fenced.session()))
                .put(obligation.key(), metadata::encode(obligation.payload()));
            assert_eq!(
                inner.commit(batch).await.unwrap(),
                expected,
                "the idle fence"
            );
        }))
    }

    /// A third attempt reserving a slot on the inner store, which must be at `attempt`'s freed
    /// index; the bytes it put are read into `into`.
    fn reserve_freed(
        &self,
        attempt: &StagedAttempt,
        third: &AttemptId,
        into: Arc<Mutex<Option<Bytes>>>,
    ) -> Step {
        let inner = Arc::clone(&self.inner);
        let upload = self.upload.clone();
        let topology = self.topology.clone();
        let third = third.clone();
        let freed = attempt.slot_index();
        Step::Run(once(async move {
            let request = request(&upload, &topology, &third, 4);
            let reserved = reserve_slot(&*inner, &request, T0).await.unwrap();
            assert_eq!(
                reserved,
                ReserveOutcome::Reserved {
                    index: freed,
                    attempt_id: third.clone()
                },
                "the third attempt holds the freed index"
            );
            let held = inner.get(&slot_key(&upload, freed)).await.unwrap();
            *into.lock().unwrap() = held;
        }))
    }

    async fn state_of(&self) -> SessionState {
        decode_session_record(&self.session().await)
            .unwrap()
            .state()
            .clone()
    }

    /// **C-1, the owed-chunk form**: no `part:` record names a chunk that a `retire:bytes:`
    /// obligation owes, so nothing published can be reclaimed under it.
    async fn assert_no_owed_chunk_is_published(&self, at: &str) {
        let owed: BTreeSet<ChunkId> = self
            .obligations()
            .await
            .into_iter()
            .flat_map(|(_, chunks)| chunks)
            .collect();
        for (key, value) in self.inner.scan(PART_PREFIX).await.unwrap() {
            for chunk in decode_part_record(&value).unwrap().chunks() {
                assert!(
                    !owed.contains(&chunk.id),
                    "{at}: {} names chunk {:?}, which an obligation owes",
                    String::from_utf8_lossy(&key),
                    chunk.id
                );
            }
        }
    }
}

/// The stored bytes of part `n`.
async fn part_bytes(rig: &Rig, n: u32) -> Bytes {
    rig.inner
        .get(&part_key(&rig.upload, part(n)))
        .await
        .unwrap()
        .expect("the part is committed")
}

/// The value `batch` puts at `key`.
fn put_at(batch: &WriteBatch, key: &[u8]) -> Option<Bytes> {
    batch
        .puts
        .iter()
        .find(|(put, _)| put == key)
        .map(|(_, value)| value.clone())
}

/// A step in the shape of the custodian's staged re-place adoption
/// (`crates/custodian/src/reconstruction/staged.rs`, `0016:671`): under `require` on the session
/// and the part bytes read, part `n`'s record put back with chunk `chunk`'s first fragment placed on
/// `to`, every other byte kept. A fragment moved to another server and, on a later pass, back writes
/// the prior's exact bytes a second time.
fn repoint(rig: &Rig, n: u32, chunk: usize, to: DServerId) -> Step {
    let inner = Arc::clone(&rig.inner);
    let upload = rig.upload.clone();
    Step::Run(once(async move {
        let session_key = mpu_key(&upload);
        let session = inner.get(&session_key).await.unwrap().unwrap();
        let key = part_key(&upload, part(n));
        let stored = inner.get(&key).await.unwrap().expect("the part exists");
        let record = decode_part_record(&stored).unwrap();
        let mut chunks = record.chunks().to_vec();
        chunks[chunk].placement[0] = to;
        let list = metadata::encode(&record.chunks());
        let at = stored
            .windows(list.len())
            .position(|window| window == &list[..])
            .expect("the stored bytes spell the chunk list canonically");
        let mut next = Vec::with_capacity(stored.len());
        next.extend_from_slice(&stored[..at]);
        next.extend_from_slice(&metadata::encode(&chunks));
        next.extend_from_slice(&stored[at + list.len()..]);
        let read_back = decode_part_record(&next).expect("the re-placed record decodes");
        assert_eq!(read_back.chunks(), chunks.as_slice());
        assert_eq!(read_back.digest(), record.digest());
        let batch = WriteBatch::new()
            .require(session_key, session)
            .require(key.clone(), stored)
            .put(key, next);
        assert_eq!(
            inner.commit(batch).await.unwrap(),
            CommitOutcome::Committed,
            "the re-place"
        );
    }))
}

/// How many kinds of damage [`session_damage`] builds.
const SESSION_DAMAGE_KINDS: usize = 5;

/// The kinds of damage a session read can find, each `(label, bytes, the record error)`: bytes
/// that do not decode; an `Open` record under other bytes at the same epoch, and `older`, the
/// session's own `Open` record from before its rollback, at a lower one; and an `Aborting` record at
/// the same epoch and at a lower one, the shape of a fence whose epoch has not risen. No legal
/// history writes any of them. `session` must be at a higher epoch than `older`.
fn session_damage(
    rig: &Rig,
    session: &Bytes,
    older: &Bytes,
) -> Vec<(&'static str, Bytes, Option<RecordError>)> {
    let epoch = decode_session_record(session).unwrap().epoch();
    let lower = decode_session_record(older).unwrap().epoch();
    assert!(lower < epoch, "a lower epoch exists");
    let not_risen = |found| {
        Some(RecordError::SessionEpochNotRisen {
            found,
            required: epoch,
        })
    };
    let kinds = vec![
        ("torn", Bytes::from_static(b"not a session record"), None),
        (
            "Open at the same epoch",
            same_epoch_other_bytes(session),
            not_risen(epoch),
        ),
        ("Open at a lower epoch", older.clone(), not_risen(lower)),
        (
            "Aborting at the same epoch",
            aborting_at(session, &rig.upload, epoch),
            not_risen(epoch),
        ),
        (
            "Aborting at a lower epoch",
            aborting_at(session, &rig.upload, epoch - 1),
            not_risen(epoch - 1),
        ),
    ];
    assert_eq!(kinds.len(), SESSION_DAMAGE_KINDS);
    kinds
}

/// `cause` is the record error `expected`, or, with none named, any record error.
fn assert_record_stop(cause: &Stop, expected: Option<&RecordError>, at: &str) {
    match (cause, expected) {
        (Stop::Record(found), Some(expected)) => assert_eq!(found, expected, "{at}"),
        (Stop::Record(_), None) => {}
        _ => panic!("{at}: expected a record stop, got {cause:?}"),
    }
}

/// `cause` is the store fault the wrapper answers with `message`.
fn assert_store_fault(cause: &Stop, message: &str, at: &str) {
    assert!(
        matches!(cause, Stop::Store(fault) if fault.to_string() == message),
        "{at}: {cause:?}"
    );
}

/// `cause` says no read showed the commit landed: the slot still held at the end of the reads.
fn assert_outcome_unknown(cause: &Stop, at: &str) {
    assert!(matches!(cause, Stop::OutcomeUnknown), "{at}: {cause:?}");
}

/// `cause` says the session moved, or was deleted with its slots, while the commit was in doubt.
fn assert_session_moved(cause: &Stop, at: &str) {
    assert!(matches!(cause, Stop::SessionMoved), "{at}: {cause:?}");
}

/// The slot bytes `attempt`'s first logged part commit required: what the quiesce renewal left,
/// which the staged attempt's own copy predates.
fn required_slot(rig: &Rig, attempt: &StagedAttempt) -> Bytes {
    rig.log.commits(Kind::Commit)[0]
        .preconditions
        .iter()
        .find(|p| p.key == attempt.slot_key())
        .and_then(|p| p.expected.clone())
        .expect("the commit requires its slot")
}

/// The four reads of one lap, in order: the part key, the session, the attempt's slot, the
/// session again.
fn lap(rig: &Rig, attempt: &StagedAttempt) -> Vec<Vec<u8>> {
    let session = mpu_key(&rig.upload);
    vec![
        part_key(&rig.upload, attempt.part_number()),
        session.clone(),
        attempt.slot_key().to_vec(),
        session,
    ]
}

/// `n` laps of [`lap`], one after the other.
fn laps_of(rig: &Rig, attempt: &StagedAttempt, n: usize) -> Vec<Vec<u8>> {
    assert!(n <= MOST_LAPS, "at most {MOST_LAPS} laps");
    (0..n).flat_map(|_| lap(rig, attempt)).collect()
}

/// Scripts the reserve, the first intent and the quiesce renewal of the next attempt `[hold,
/// pass]`: each first round is held unapplied, a batch the store may still apply, and the next
/// round of the same batch lands. Staging and the renewal settle each held round by re-reading and
/// sending the next, so the attempt stages and quiesces normally, with three held batches the test
/// can land later, in order.
fn hold_each_staging_round_once(rig: &Rig) {
    for kind in [Kind::Reserve, Kind::Intent, Kind::Renewal] {
        rig.store.script(kind, vec![Act::hold(), Act::pass()], None);
    }
}

/// No `retire:` obligation owes any of `attempt`'s own chunks.
async fn assert_own_chunks_not_owed(rig: &Rig, attempt: &StagedAttempt, at: &str) {
    let own = chunk_ids(attempt.chunks());
    for (token, owed) in rig.obligations().await {
        assert!(
            owed.iter().all(|chunk| !own.contains(chunk)),
            "{at}: {token:?} owes this attempt's own chunks"
        );
    }
}

// ─────────────── (B) settling a commit in doubt (#1029) ───────────────

/// **(B) A commit that lands but answers unknown**, with either flag: the first lap finds the slot
/// gone while the session read after it is still `Open@E`, so the answer is `Committed`, with no
/// further batch and no compensation, and the slot released exactly once, by the commit itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_landed_commit_answered_unknown_is_committed() {
    for may_still_commit in [false, true] {
        within(async {
            let at = format!("may_still_commit {may_still_commit}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 201)).await;
            rig.store.script(
                Kind::Commit,
                vec![Act::unknown(true, may_still_commit)],
                None,
            );
            committed_as(&rig.commit(attempt.clone()).await, &attempt);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: no retry");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(
                rig.log.reads_after(Kind::Commit, 0),
                laps_of(&rig, &attempt, 1),
                "{at}: one lap"
            );
            assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
            assert_eq!(rig.releases(&attempt), 1, "{at}: released once");
            assert_eq!(rig.slots_of(&attempt).await, 0, "{at}");
            assert!(rig.part_obligations().await.is_empty(), "{at}");
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **(B) A re-upload that did not land** (`may_still_commit: false`, nothing applied): the lap
/// finds the **prior** part, the slot held and the session unchanged. That is never `Committed`:
/// the part's bytes are the prior's, and a held slot proves nothing. The in-doubt error keeping
/// `may_still_commit: false`, its cause `OutcomeUnknown`, one commit batch, no compensation, the
/// slot still held and the prior standing. The availability cost of sending nothing more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_an_unlanded_reupload_is_not_committed_on_the_prior_part() {
    within(async {
        let mut rig = Rig::new().await;
        let prior = rig.committed(1, &body(2 * CHUNK, 202)).await;
        let attempt = rig.stage(1, &body(CHUNK, 203)).await;
        rig.store
            .script(Kind::Commit, vec![Act::unknown(false, false)], None);
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        assert_eq!(doubt.batch, DoubtfulBatch::Commit);
        assert!(!doubt.may_still_commit, "as the store reported");
        assert_outcome_unknown(&doubt.cause, "re-upload");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "one commit batch");
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 1, "the slot still held");
        assert_eq!(rig.releases(&attempt), 0);
        assert_eq!(rig.part(1).await.unwrap().0, prior, "the prior stands");
        assert!(rig.part_obligations().await.is_empty());
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(B) A held commit** (`may_still_commit: true`, kept unapplied by the wrapper), landed later.
/// No `Committed` on a lap that finds the slot held. (i) Released before the re-read: the first lap
/// finds the slot gone, `Committed`. (ii) Released after the attempt returned: both laps find the
/// slot held, so the in-doubt error keeping `may_still_commit: true`; the held batch then lands. In
/// both runs there is one commit batch, it releases the slot once, and the attempt's own part is
/// durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_held_commit_is_committed_only_once_a_lap_finds_its_slot_gone() {
    for run in ["i", "ii"] {
        within(async {
            let at = format!("run {run}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK + 3, 204)).await;
            let held = if run == "i" {
                Act::hold().after(rig.land_held(CommitOutcome::Committed))
            } else {
                Act::hold()
            };
            rig.store.script(Kind::Commit, vec![held], None);
            let outcome = rig.commit(attempt.clone()).await;
            if run == "i" {
                committed_as(&outcome, &attempt);
            } else {
                let doubt = in_doubt(outcome);
                assert!(doubt.may_still_commit, "{at}: the doubt is kept");
                assert_outcome_unknown(&doubt.cause, &at);
                assert_eq!(
                    rig.log.reads_after(Kind::Commit, 0),
                    laps_of(&rig, &attempt, MOST_LAPS),
                    "{at}: every lap found the slot held"
                );
                assert!(rig.part(1).await.is_none(), "{at}: not landed yet");
                assert_eq!(rig.slots_of(&attempt).await, 1, "{at}");
                assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
            }
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: one batch");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
            assert_eq!(rig.releases(&attempt), 1, "{at}: released once");
            assert_eq!(rig.slots_of(&attempt).await, 0, "{at}");
            assert_eq!(rig.owned_of(&attempt).await, 0, "{at}");
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **(B) A same-number rival, this attempt's commit not applied**, the commit having required the
/// part key absent, once per flag (the wrapper holds the `true` one): the rival's record lands
/// right after the batch. Never `Committed`: the part read finds the rival's record where the
/// commit required none, so the commit can never land, and the slot read finds the slot held. The
/// in-doubt error with `may_still_commit: false`, no compensation, the slot still held. In the held
/// run the held batch, released later, answers `Conflict` and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_same_number_rival_over_an_unapplied_commit_keeps_the_doubt() {
    for may_still_commit in [false, true] {
        within(async {
            let at = format!("may_still_commit {may_still_commit}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 205)).await;
            let rival = rig.stage(1, &body(2 * CHUNK, 206)).await;
            let lost = if may_still_commit {
                Act::hold()
            } else {
                Act::unknown(false, false)
            };
            rig.store.script(
                Kind::Commit,
                vec![lost.after(rig.rival(rival.clone()))],
                None,
            );
            let doubt = in_doubt(rig.commit(attempt.clone()).await);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            assert!(!doubt.may_still_commit, "{at}: the rival's record ended it");
            assert_outcome_unknown(&doubt.cause, &at);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot still held");
            if may_still_commit {
                assert_eq!(
                    rig.store.release_held().await,
                    CommitOutcome::Conflict,
                    "{at}: the held batch writes nothing"
                );
            }
            assert_eq!(rig.releases(&attempt), 0, "{at}");
            assert!(is_own(&rig.part(1).await.unwrap().0, &rival), "{at}");
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **(B) A changed non-empty prior settles nothing.** A re-upload over a non-empty prior is held,
/// and a same-number rival replaces the prior. The rival's record does not show the held commit can
/// never land, since a repair can write the prior's exact bytes back (the next leg): the in-doubt
/// error keeping `may_still_commit: true`, never `OperationAborted`, no compensation. The held
/// batch, released over the rival's record, conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_held_reupload_over_a_non_empty_prior_that_loses_to_a_rival_keeps_the_doubt() {
    within(async {
        let mut rig = Rig::new().await;
        let prior = rig.committed(1, &body(2 * CHUNK, 207)).await;
        assert!(!prior.is_empty());
        let attempt = rig.stage(1, &body(CHUNK, 208)).await;
        let rival = rig.stage(1, &body(CHUNK, 209)).await;
        rig.store.script(
            Kind::Commit,
            vec![Act::hold().after(rig.rival(rival.clone()))],
            None,
        );
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        assert_eq!(doubt.batch, DoubtfulBatch::Commit);
        assert!(doubt.may_still_commit, "the doubt is kept");
        assert_outcome_unknown(&doubt.cause, "rival over a non-empty prior");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 1, "covered by the doubt");
        assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
        assert_eq!(rig.releases(&attempt), 0, "nothing of this attempt landed");
        assert!(is_own(&rig.part(1).await.unwrap().0, &rival));
    })
    .await;
}

/// **(B) A held re-upload while a repair moves the prior away and back** (staged reconstruction,
/// `0016:671`). A re-upload over a non-empty prior is held. A batch in the re-place adoption's exact
/// shape moves one of the prior's fragments to another server, at each gap of the first lap in
/// turn, so the part key no longer holds what the held commit requires; a later re-place moves it
/// back, at the end of the first lap or after the attempt returned, so the prior's exact bytes
/// return. Every run answers the in-doubt error keeping `may_still_commit: true`, never
/// `OperationAborted`, with no compensation and one commit batch. The held batch, released once the
/// prior is back, lands: the attempt's own part durable, the prior's chunks owed, the slot released
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_held_reupload_survives_a_repair_that_moves_the_prior_away_and_back() {
    for back_after_return in [false, true] {
        for away_at in 0..=4usize {
            within(async {
                let at =
                    format!("moved away at gap {away_at}, back after return {back_after_return}");
                let mut rig = Rig::new().await;
                let prior = rig.committed(1, &body(2 * CHUNK, 210)).await;
                let placement = &prior.chunks()[0].placement;
                let first = placement[0];
                let spare = (0..SERVERS)
                    .find(|id| !placement.contains(id))
                    .expect("a server outside the chunk's placement");
                let attempt = rig.stage(1, &body(CHUNK, 211)).await;
                let mut gaps = vec![(away_at, vec![repoint(&rig, 1, 0, spare)])];
                if !back_after_return {
                    gaps.push((4, vec![repoint(&rig, 1, 0, first)]));
                }
                let held = Act::hold().after_all(rig.at_gaps(&attempt, gaps));
                rig.store.script(Kind::Commit, vec![held], None);
                let doubt = in_doubt(rig.commit(attempt.clone()).await);
                assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
                assert!(doubt.may_still_commit, "{at}: the doubt is kept");
                assert_outcome_unknown(&doubt.cause, &at);
                assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
                assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
                assert_eq!(
                    rig.slots_of(&attempt).await,
                    1,
                    "{at}: covered by the doubt"
                );
                if back_after_return {
                    rig.run(vec![repoint(&rig, 1, 0, first)]).await;
                }
                assert_eq!(
                    rig.part(1).await.unwrap().0,
                    prior,
                    "{at}: the prior's exact record is back"
                );
                assert_eq!(
                    rig.store.release_held().await,
                    CommitOutcome::Committed,
                    "{at}: the held commit lands"
                );
                assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
                assert_eq!(
                    rig.part_obligations().await,
                    vec![(token_of(&attempt, 0), chunk_ids(prior.chunks()))],
                    "{at}: the prior's chunks are owed"
                );
                assert_eq!(rig.releases(&attempt), 1, "{at}: released once");
                assert_eq!(rig.slots_of(&attempt).await, 0, "{at}");
                assert_eq!(rig.owned_of(&attempt).await, 0, "{at}");
                rig.assert_every_fragment_named().await;
            })
            .await;
        }
    }
}

/// **(B) A rival over an empty prior settles nothing, because those bytes can come back.** Over an
/// empty prior a commit is held and a **non-empty** rival lands. That record does not settle the
/// held commit: the in-doubt error keeping `may_still_commit: true`. The history is real: an empty
/// rival in the same millisecond then writes the prior's exact bytes again, and the held commit's
/// `require(part == prior)` holds once more, so it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_rival_over_an_empty_prior_settles_nothing_because_those_bytes_can_come_back() {
    within(async {
        let mut rig = Rig::new().await;
        let prior = rig.committed(1, &[]).await;
        assert!(prior.is_empty());
        let empty = part_bytes(&rig, 1).await;
        let attempt = rig.stage(1, &body(CHUNK, 212)).await;
        let rival = rig.stage(1, &body(CHUNK, 213)).await;
        let restorer = rig.stage(1, &[]).await;
        rig.store.script(
            Kind::Commit,
            vec![Act::hold().after(rig.rival(rival.clone()))],
            None,
        );
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        assert_eq!(doubt.batch, DoubtfulBatch::Commit);
        assert!(doubt.may_still_commit, "the doubt is kept");
        assert_outcome_unknown(&doubt.cause, "rival over an empty prior");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 1, "covered by the doubt");
        assert!(is_own(&rig.part(1).await.unwrap().0, &rival));
        rig.run(vec![rig.rival(restorer)]).await;
        assert_eq!(part_bytes(&rig, 1).await, empty, "the prior's bytes again");
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
        assert!(is_own(&rig.part(1).await.unwrap().0, &attempt));
        assert_eq!(rig.releases(&attempt), 1);
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// What the wrapper does at a read gap while a commit is held (the read-gap leg below).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum History {
    /// The held commit lands, then a same-number rival replaces its part.
    LandedThenRival,
    /// The held commit lands, then a Complete fences and rolls the session back.
    LandedThenRollback,
    /// A same-number rival lands, where the commit required the part key absent; the held commit
    /// does not.
    NotLandedRival,
    /// The session is rolled back; the held commit does not land.
    NotLandedRollback,
}

/// **(B) The read-gap leg**, one test per history. The commit of a non-empty part is held. At
/// **each** gap of the first lap (0 before the part read, then after it, after the first session
/// read, after the slot read, and after the last session read) the wrapper runs `history`. One
/// commit batch and no compensation in every run.
///
/// * Landed, then a rival: `Committed`, the slot released once. A landing the first lap's slot
///   read missed (gaps 3 and 4) is found by the second lap.
/// * Landed, then a rollback: the in-doubt error (`SessionMoved`), the attempt's own part durable.
/// * Not landed, then a rival, and not landed, then a rollback: the in-doubt error saying the
///   commit cannot land, the slot still held, and the held batch, released later, answers
///   `Conflict`.
///
/// The reads after the batch are pinned exactly: one lap, or two where the first lap found the
/// slot held under the unchanged session while the commit could still land.
async fn read_gap(history: History) {
    for gap in 0..=4usize {
        within(async {
            let at = format!("{history:?} at gap {gap}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK + 2, 214)).await;
            let rival = rig.stage(1, &body(CHUNK, 215)).await;
            let steps = match history {
                History::LandedThenRival => vec![
                    rig.land_held(CommitOutcome::Committed),
                    rig.rival(rival.clone()),
                ],
                History::LandedThenRollback => {
                    vec![rig.land_held(CommitOutcome::Committed), rig.rollback()]
                }
                History::NotLandedRival => vec![rig.rival(rival.clone())],
                History::NotLandedRollback => vec![rig.rollback()],
            };
            let held = Act::hold().after_all(rig.at_gaps(&attempt, vec![(gap, steps)]));
            rig.store.script(Kind::Commit, vec![held], None);
            let outcome = rig.commit(attempt.clone()).await;
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: one batch");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            let laps = match (history, gap) {
                (History::LandedThenRival, 3..=4)
                | (History::LandedThenRollback | History::NotLandedRollback, 4)
                | (History::NotLandedRival, 1..=4) => 2,
                _ => 1,
            };
            assert_eq!(
                rig.log.reads_after(Kind::Commit, 0),
                laps_of(&rig, &attempt, laps),
                "{at}: the laps read"
            );
            match history {
                History::LandedThenRival => {
                    committed_as(&outcome, &attempt);
                    assert_eq!(rig.releases(&attempt), 1, "{at}");
                    assert_eq!(rig.slots_of(&attempt).await, 0, "{at}");
                    assert!(is_own(&rig.part(1).await.unwrap().0, &rival), "{at}");
                }
                History::LandedThenRollback => {
                    let doubt = in_doubt(outcome);
                    assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
                    assert!(!doubt.may_still_commit, "{at}: the session moved");
                    assert_session_moved(&doubt.cause, &at);
                    assert_eq!(rig.releases(&attempt), 1, "{at}");
                    assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
                }
                History::NotLandedRival | History::NotLandedRollback => {
                    let doubt = in_doubt(outcome);
                    assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
                    assert!(!doubt.may_still_commit, "{at}: it cannot land any more");
                    if history == History::NotLandedRival {
                        assert_outcome_unknown(&doubt.cause, &at);
                    } else {
                        assert_session_moved(&doubt.cause, &at);
                    }
                    assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot still held");
                    assert_eq!(
                        rig.store.release_held().await,
                        CommitOutcome::Conflict,
                        "{at}: the held batch writes nothing"
                    );
                    assert_eq!(rig.releases(&attempt), 0, "{at}");
                }
            }
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **(B) The read-gap leg, landed then a rival** ([`read_gap`]): `Committed` at every gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_read_gap_landed_then_a_rival_is_committed() {
    read_gap(History::LandedThenRival).await;
}

/// **(B) The read-gap leg, landed then a rollback** ([`read_gap`]): the in-doubt error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_read_gap_landed_then_a_rollback_is_in_doubt() {
    read_gap(History::LandedThenRollback).await;
}

/// **(B) The read-gap leg, not landed then a rival** ([`read_gap`]): the in-doubt error, the slot
/// still held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_read_gap_not_landed_then_a_rival_is_in_doubt() {
    read_gap(History::NotLandedRival).await;
}

/// **(B) The read-gap leg, not landed then a rollback** ([`read_gap`]): the in-doubt error, the
/// slot still held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_read_gap_not_landed_then_a_rollback_is_in_doubt() {
    read_gap(History::NotLandedRollback).await;
}

// ─────────────── (X) late staging rebuilds the slot (#1029) ───────────────

/// **(X) The cross-phase leg** (iteration 2's rejection). The clock fixed; the reserve, the intent
/// and the quiesce renewal each scripted `[hold, pass]`; one chunk staged; the commit applies but
/// answers unknown, once per flag. Before the re-read the three held batches are released, in that
/// order, and each must answer `Committed`: the commit deleted the slot and the owned entry, so the
/// reserve finds the slot key absent again, and the intent and the renewal find exactly the bytes
/// before them. The slot then holds exactly the bytes the commit required and the owned entry is
/// back, so the history is the real one. A slot held at those bytes proves nothing: the answer is
/// the in-doubt error, with no compensation and no second commit, no `retire:` obligation owing the
/// attempt's own chunks, and its own part durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn x_late_staging_that_rebuilds_a_landed_commits_slot_never_compensates() {
    for may_still_commit in [false, true] {
        within(async {
            let at = format!("may_still_commit {may_still_commit}");
            let mut rig = Rig::new().await;
            hold_each_staging_round_once(&rig);
            let attempt = rig.stage(1, &body(CHUNK, 240)).await;
            assert_eq!(attempt.chunks().len(), 1, "{at}: one chunk");
            assert_eq!(
                rig.store.held_count(),
                2,
                "{at}: the reserve and the intent"
            );
            let owned = attempt.owned()[0].key().to_vec();
            let seen: Seen = Arc::default();
            // The reserve, the intent and the quiesce renewal, oldest first.
            let mut act = Act::unknown(true, may_still_commit);
            for _ in 0..3 {
                act = act.after(rig.land_held(CommitOutcome::Committed));
            }
            act = act
                .after(rig.read_into(vec![attempt.slot_key().to_vec(), owned], Arc::clone(&seen)));
            rig.store.script(Kind::Commit, vec![act], None);
            let outcome = rig.commit(attempt.clone()).await;
            let seen = seen.lock().unwrap().clone();
            assert_eq!(
                seen[0],
                Some(required_slot(&rig, &attempt)),
                "{at}: the slot holds exactly the bytes the commit required"
            );
            assert!(seen[1].is_some(), "{at}: the owned entry is back");
            assert_eq!(rig.store.held_count(), 0, "{at}");
            assert_eq!(
                rig.log.commits(Kind::Compensation).len(),
                0,
                "{at}: no compensation"
            );
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: one commit");
            assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
            assert_own_chunks_not_owed(&rig, &attempt, &at).await;
            rig.assert_no_owed_chunk_is_published(&at).await;
            let doubt = in_doubt(outcome);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            assert!(
                !doubt.may_still_commit,
                "{at}: its own record took the part key it required absent"
            );
            assert_outcome_unknown(&doubt.cause, &at);
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// How the replay leg runs ([`replay`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replay {
    /// A's commit applies but answers unknown with this flag, and the replay runs at the gap
    /// before A's re-read.
    Applied(bool),
    /// A's commit is held, and the replay runs after A returned, the held commit last.
    Held,
}

/// **(X2) The replay leg** (the plan review's finding), one test per run. The clock fixed and the
/// session `Open@E`. An empty part 1, P, commits first. A is a non-empty re-upload of part 1,
/// staged with (X)'s `[hold, pass]` reserve, intent and renewal. B is a same-number **empty**
/// attempt in the same millisecond and epoch, so the part B writes is byte for byte P.
///
/// * **(a) Applied, before the re-read**, once per flag: A's commit applies but answers unknown.
///   At the gap before A's re-read, A's three held staging batches land in order, then B commits
///   over A's part, installing a `retire:` obligation keyed by B's attempt that owes A's chunks.
///   Each of those four answers `Committed`, the part key then holds P's exact bytes and A's slot
///   the exact bytes A's commit required: every precondition of A's commit holds again. A's
///   answer is the in-doubt error.
/// * **(b) Held, then released after return**: A's commit is held. After A returns, A's held
///   staging batches are released in order, then B commits, then A's held commit is released.
///
/// In both runs: exactly one commit batch from A, at most one of A's commit batches lands, and at
/// the end no `part:` record names a chunk that a `retire:` obligation owes. The answer is judged
/// last, so a run that sends the commit again fails on the history it leaves.
async fn replay(runs: &[Replay]) {
    for &run in runs {
        within(async {
            let at = format!("{run:?}");
            let mut rig = Rig::new().await;
            rig.committed(1, &[]).await;
            let p = part_bytes(&rig, 1).await;
            // B is staged first, so the `[hold, pass]` script is A's alone.
            let b = rig.stage(1, &[]).await;
            hold_each_staging_round_once(&rig);
            let a = rig.stage(1, &body(CHUNK, 241)).await;
            let seen: Seen = Arc::default();
            let outcome = match run {
                Replay::Applied(flag) => {
                    let mut act = Act::unknown(true, flag);
                    for _ in 0..3 {
                        act = act.after(rig.land_held(CommitOutcome::Committed));
                    }
                    act = act.after(rig.rival(b.clone())).after(rig.read_into(
                        vec![part_key(&rig.upload, part(1)), a.slot_key().to_vec()],
                        Arc::clone(&seen),
                    ));
                    rig.store.script(Kind::Commit, vec![act], None);
                    let outcome = rig.commit(a.clone()).await;
                    let seen = seen.lock().unwrap().clone();
                    assert_eq!(seen[0], Some(p.clone()), "{at}: P's exact bytes");
                    assert_eq!(
                        seen[1],
                        Some(required_slot(&rig, &a)),
                        "{at}: A's slot at the bytes A's commit required"
                    );
                    outcome
                }
                Replay::Held => {
                    rig.store.script(Kind::Commit, vec![Act::hold()], None);
                    let outcome = rig.commit(a.clone()).await;
                    assert_eq!(
                        rig.store.held_count(),
                        4,
                        "{at}: A's reserve, intent, renewal and commit"
                    );
                    for _ in 0..3 {
                        rig.store.release_held().await;
                    }
                    rig.run(vec![rig.rival(b.clone())]).await;
                    rig.store.release_held().await;
                    outcome
                }
            };
            assert!(
                rig.store.landed_of(a.attempt_id(), Kind::Commit) <= 1,
                "{at}: at most one of A's commit batches lands"
            );
            rig.assert_no_owed_chunk_is_published(&at).await;
            assert_eq!(
                rig.log.commits(Kind::Commit).len(),
                1,
                "{at}: exactly one commit batch from A"
            );
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            let doubt = in_doubt(outcome);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            if let Replay::Applied(_) = run {
                assert_eq!(
                    rig.part_obligations().await,
                    vec![(token_of(&b, 0), chunk_ids(a.chunks()))],
                    "{at}: B's obligation owes A's chunks"
                );
                assert_eq!(part_bytes(&rig, 1).await, p, "{at}: P's bytes stand");
            }
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **(X2) run (a)** ([`replay`]): A's commit applied but answered unknown, once per flag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn x2_a_replay_after_an_applied_commit_never_lands_it_twice() {
    replay(&[Replay::Applied(false), Replay::Applied(true)]).await;
}

/// **(X2) run (b)** ([`replay`]): A's commit held, the replay after A returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn x2_b_replay_after_a_held_commit_never_lands_it_twice() {
    replay(&[Replay::Held]).await;
}

// ─────────────── (C) the regressions (#1029) ───────────────

/// **C1, an empty part's bytes prove nothing.** The clock fixed, an empty part 1 is committed. An
/// empty re-upload in the same millisecond and epoch is answered unknown, **not applied**, once per
/// flag (the `true` one held): its batch requires and puts the prior's very bytes, and the answer
/// is never `Committed` on them. The in-doubt error, the slot still held. Then a first upload
/// against an empty same-number rival landing in the same millisecond, the commit having required
/// the part key absent, not applied: the in-doubt error. On return every slot still held belongs to
/// an attempt that is live or returned the in-doubt error (`0016:1779`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c1_an_empty_part_is_never_committed_on_its_bytes() {
    for may_still_commit in [false, true] {
        let lost = || {
            if may_still_commit {
                Act::hold()
            } else {
                Act::unknown(false, false)
            }
        };
        within(async {
            let at = format!("re-upload, may_still_commit {may_still_commit}");
            let mut rig = Rig::new().await;
            rig.committed(1, &[]).await;
            let prior = part_bytes(&rig, 1).await;
            let attempt = rig.stage(1, &[]).await;
            rig.store.script(Kind::Commit, vec![lost()], None);
            let outcome = rig.commit(attempt.clone()).await;
            let key = part_key(&rig.upload, part(1));
            let first = &rig.log.commits(Kind::Commit)[0];
            assert_eq!(put_at(first, &key), Some(prior.clone()), "{at}: same bytes");
            let doubt = in_doubt(outcome);
            assert_eq!(doubt.may_still_commit, may_still_commit, "{at}");
            assert_outcome_unknown(&doubt.cause, &at);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.slots().await, 1, "{at}: one slot");
            assert_eq!(
                rig.slots_of(&attempt).await,
                1,
                "{at}: the attempt in doubt"
            );
            assert_eq!(part_bytes(&rig, 1).await, prior, "{at}");
            assert!(rig.obligations().await.is_empty(), "{at}");
        })
        .await;

        within(async {
            let at = format!("empty rival, may_still_commit {may_still_commit}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &[]).await;
            let rival = rig.stage(1, &[]).await;
            rig.store
                .script(Kind::Commit, vec![lost().after(rig.rival(rival))], None);
            let outcome = rig.commit(attempt.clone()).await;
            let first = &rig.log.commits(Kind::Commit)[0];
            assert_eq!(
                put_at(first, &part_key(&rig.upload, part(1))),
                Some(part_bytes(&rig, 1).await),
                "{at}: the rival's bytes are the ones this attempt would put"
            );
            let doubt = in_doubt(outcome);
            assert!(!doubt.may_still_commit, "{at}: the rival's record ended it");
            assert_outcome_unknown(&doubt.cause, &at);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.slots().await, 1, "{at}: one slot");
            assert_eq!(
                rig.slots_of(&attempt).await,
                1,
                "{at}: the attempt in doubt"
            );
            if rig.store.held_count() > 0 {
                assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
            }
            assert_eq!(rig.releases(&attempt), 0, "{at}");
            assert!(
                rig.obligations().await.is_empty(),
                "{at}: an empty part owes nothing"
            );
        })
        .await;
    }
}

/// **C2, a torn read.** A commit held and never applied. At each gap of the first lap the wrapper
/// runs a real Abort and then deletes the session's `sidx:` entries and `slot:` records (the
/// reaper's teardown, #659). No part exists, so the answer is never `Committed`: `NoSuchUpload` or
/// the in-doubt error, which says the commit cannot land. The held batch, released afterwards,
/// conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c2_a_torn_read_never_answers_committed() {
    for gap in 0..=4usize {
        within(async {
            let at = format!("gap {gap}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 216)).await;
            let steps = rig.at_gaps(&attempt, vec![(gap, vec![rig.abort(), rig.teardown()])]);
            rig.store
                .script(Kind::Commit, vec![Act::hold().after_all(steps)], None);
            let outcome = rig.commit(attempt.clone()).await;
            if matches!(outcome, Err(CommitError::InDoubt(_))) {
                let doubt = in_doubt(outcome);
                assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
                assert!(!doubt.may_still_commit, "{at}: the Abort ended its bytes");
            } else {
                refused(&outcome, Refusal::NoSuchUpload);
            }
            assert!(
                matches!(rig.state_of().await, SessionState::Aborting {}),
                "{at}: the abort ran"
            );
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
            assert!(rig.part(1).await.is_none(), "{at}: no part exists");
        })
        .await;
    }
}

/// **C3, a landed commit, then a rival before the re-read.** The commit lands but answers
/// `CommitUnknownResult { may_still_commit: false }`; a same-number rival then replaces the part.
/// The answer is `Committed`: the slot is gone while the session is still `Open@E`, which only this
/// attempt's commit can do. No compensation.
///
/// **C3′**: as C3, and then a third attempt reserves the index the commit freed, all before the
/// re-read, so the slot key holds another `attempt_id`: still `Committed`, and the third attempt's
/// slot is byte-for-byte unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c3_a_landed_commit_then_a_rival_is_committed() {
    for reuse in [false, true] {
        within(async {
            let at = if reuse { "C3′" } else { "C3" };
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 217)).await;
            let rival = rig.stage(1, &body(CHUNK, 218)).await;
            let third = rig.attempt_id();
            let held: Arc<Mutex<Option<Bytes>>> = Arc::default();
            let mut act = Act::unknown(true, false).after(rig.rival(rival.clone()));
            if reuse {
                act = act.after(rig.reserve_freed(&attempt, &third, Arc::clone(&held)));
            }
            rig.store.script(Kind::Commit, vec![act], None);
            committed_as(&rig.commit(attempt.clone()).await, &attempt);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert!(
                is_own(&rig.part(1).await.unwrap().0, &rival),
                "{at}: the rival replaced it"
            );
            assert_eq!(rig.releases(&attempt), 1, "{at}");
            if reuse {
                let third = held.lock().unwrap().clone();
                assert!(third.is_some(), "{at}: the third reserved");
                assert_eq!(
                    rig.inner.get(attempt.slot_key()).await.unwrap(),
                    third,
                    "{at}: the third attempt's slot is unchanged"
                );
            }
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **A commit out of flight, never applied**, for a part with chunks and an empty one: every commit
/// round would answer `CommitUnknownResult { may_still_commit: false }` and apply nothing, but only
/// the first is sent. One lap of reads finds the slot held, which proves nothing, and the commit
/// can no longer land: the in-doubt error saying so, no compensation and no obligation, the slot
/// still held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_commit_out_of_flight_is_never_sent_again_or_compensated() {
    for bytes in [body(CHUNK, 219), Vec::new()] {
        within(async {
            let at = format!("{} bytes", bytes.len());
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &bytes).await;
            rig.store
                .script(Kind::Commit, vec![], Some(Act::unknown(false, false)));
            let doubt = in_doubt(rig.commit(attempt.clone()).await);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            assert!(!doubt.may_still_commit, "{at}: as the store reported");
            assert_outcome_unknown(&doubt.cause, &at);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: one batch");
            assert_eq!(
                rig.log.reads_after(Kind::Commit, 0),
                laps_of(&rig, &attempt, 1),
                "{at}: one lap"
            );
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert!(rig.part_obligations().await.is_empty(), "{at}");
            assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot still held");
            assert_eq!(rig.releases(&attempt), 0, "{at}");
            assert!(rig.part(1).await.is_none(), "{at}");
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
}

/// **A conflict, then a held round.** Round 1 answers a `Conflict` that no read explains, so the
/// same batch goes again (#1028); round 2 is held, and the first read after it faults. The in-doubt
/// error keeping `may_still_commit: true`, the fault as its cause, and no third batch. The held
/// round then lands once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_conflict_then_a_held_round_sends_no_third_batch() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 220)).await;
        let fault = rig.fail_reads_from_now(part_key(&rig.upload, part(1)));
        rig.store.script(
            Kind::Commit,
            vec![Act::conflict(), Act::hold().after(fault)],
            None,
        );
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        let commits = rig.log.commits(Kind::Commit);
        assert_eq!(commits.len(), 2, "no third batch");
        assert!(same_batch(&commits[0], &commits[1]), "the same batch again");
        assert_eq!(doubt.batch, DoubtfulBatch::Commit);
        assert!(doubt.may_still_commit, "the held round may still land");
        assert_store_fault(&doubt.cause, "scripted read fault", "round 2");
        assert!(
            rig.log.reads_after(Kind::Commit, 1).is_empty(),
            "the faulted read logs nothing"
        );
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 1, "the slot still held");
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
        assert!(is_own(&rig.part(1).await.unwrap().0, &attempt));
        assert_eq!(rig.releases(&attempt), 1, "released once");
    })
    .await;
}

/// **Read faults keep the doubt.** The commit is held, and the wrapper fails one read of the first
/// lap: the part read, the first session read, the slot read or the last session read; or, with
/// the session record erased right after the slot read, the slot key read once more behind the
/// missing record. Every run answers the in-doubt error keeping `may_still_commit: true`, its cause
/// the read fault, never `Stopped`. No second batch, no compensation, the slot still held, and the
/// reads made are exactly those before the faulted one. The held batch then lands (once the session
/// record is put back, in the last run), so the doubt was real, and that landing is the one release.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_read_fault_at_each_read_under_a_held_commit_keeps_the_doubt() {
    // Each run: the read that faults, the gap of the lap at which its fault is armed, and how many
    // reads of the lap answer before it.
    let runs = [
        ("the part read", 0usize, 0usize),
        ("the first session read", 1, 1),
        ("the slot read", 2, 2),
        ("the last session read", 3, 3),
        ("the slot read behind a missing session", 3, 4),
    ];
    for (seed, (at, gap, answered)) in (242u8..).zip(runs) {
        within(async {
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, seed)).await;
            let session = rig.session().await;
            let session_key = mpu_key(&rig.upload);
            let reads = lap(&rig, &attempt);
            let behind_missing = answered == reads.len();
            let fault = if behind_missing {
                vec![
                    rig.erased(vec![session_key.clone()]),
                    rig.fail_reads_from_now(attempt.slot_key().to_vec()),
                ]
            } else {
                vec![rig.fail_reads_from_now(reads[answered].clone())]
            };
            let steps = rig.at_gaps(&attempt, vec![(gap, fault)]);
            rig.store
                .script(Kind::Commit, vec![Act::hold().after_all(steps)], None);
            let outcome = rig.commit(attempt.clone()).await;
            assert!(
                matches!(outcome, Err(CommitError::InDoubt(_))),
                "{at}: {outcome:?}"
            );
            let doubt = in_doubt(outcome);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            assert!(doubt.may_still_commit, "{at}: it may still land");
            assert_store_fault(&doubt.cause, "scripted read fault", at);
            assert_eq!(
                rig.log.reads_after(Kind::Commit, 0),
                reads[..answered].to_vec(),
                "{at}: the reads before the fault"
            );
            assert_eq!(
                rig.log.commits(Kind::Commit).len(),
                1,
                "{at}: no second batch"
            );
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot held");
            assert!(rig.part(1).await.is_none(), "{at}");
            if behind_missing {
                rig.run(vec![rig.plant(session_key, session)]).await;
            }
            assert_eq!(
                rig.store.release_held().await,
                CommitOutcome::Committed,
                "{at}: the held batch could still land"
            );
            assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
            assert_eq!(rig.releases(&attempt), 1, "{at}");
        })
        .await;
    }
}

/// Which read of a held commit's first lap shows it can no longer land, before the next read
/// faults ([`read_fault_after_the_end`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    /// A rollback lands right after the part read, so the first session read finds the session at
    /// a higher epoch; the slot read then faults.
    SessionMoved,
    /// The commit required the part key absent, a same-number rival lands before the part read,
    /// which finds its record; the first session read then faults.
    RivalWhereNoneWasRequired,
}

/// **A read fault keeps what the reads before it showed**, one test per [`Ended`]. The commit is
/// held, a read of the first lap finds that it can no longer land, and the next read faults. The
/// in-doubt error with `may_still_commit: false` and the read fault as its cause, never `Stopped`,
/// after exactly the reads before the fault. No second batch, no compensation, the slot still
/// held. The held batch, released afterwards, answers `Conflict` and writes nothing, so the `false`
/// was true.
async fn read_fault_after_the_end(ended: Ended, seed: u8) {
    within(async {
        let at = format!("{ended:?}");
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, seed)).await;
        let session = rig.session().await;
        let reads = lap(&rig, &attempt);
        // The gap of the first lap the change and the fault are armed at, the change, and how many
        // reads of the lap answer before the faulted one.
        let (gap, change, answered, rival) = match ended {
            Ended::SessionMoved => (1usize, rig.rollback(), 2usize, None),
            Ended::RivalWhereNoneWasRequired => {
                let rival = rig.stage(1, &body(2 * CHUNK, seed + 1)).await;
                (0, rig.rival(rival.clone()), 1, Some(rival))
            }
        };
        let fault = rig.fail_reads_from_now(reads[answered].clone());
        let steps = rig.at_gaps(&attempt, vec![(gap, vec![change, fault])]);
        rig.store
            .script(Kind::Commit, vec![Act::hold().after_all(steps)], None);
        let outcome = rig.commit(attempt.clone()).await;
        assert!(
            matches!(outcome, Err(CommitError::InDoubt(_))),
            "{at}: {outcome:?}"
        );
        let doubt = in_doubt(outcome);
        assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
        assert!(
            !doubt.may_still_commit,
            "{at}: a read before the fault showed it cannot land"
        );
        assert_store_fault(&doubt.cause, "scripted read fault", &at);
        assert_eq!(
            rig.log.reads_after(Kind::Commit, 0),
            reads[..answered].to_vec(),
            "{at}: the reads before the fault"
        );
        assert_eq!(
            rig.log.commits(Kind::Commit).len(),
            1,
            "{at}: no second batch"
        );
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
        assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot held");
        assert_eq!(
            rig.store.release_held().await,
            CommitOutcome::Conflict,
            "{at}: the held batch cannot land"
        );
        assert_eq!(rig.releases(&attempt), 0, "{at}");
        match &rival {
            Some(rival) => assert!(is_own(&rig.part(1).await.unwrap().0, rival), "{at}"),
            None => {
                assert!(rig.part(1).await.is_none(), "{at}");
                let moved = decode_session_record(&rig.session().await).unwrap();
                let was = decode_session_record(&session).unwrap();
                assert_eq!(moved.epoch(), was.epoch() + 2, "{at}: rolled back");
            }
        }
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **A read fault after the session moved** ([`read_fault_after_the_end`]): the first session read
/// finds the session at a higher epoch, the slot read faults, `may_still_commit: false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_read_fault_after_the_session_moved_keeps_the_commit_unable_to_land() {
    read_fault_after_the_end(Ended::SessionMoved, 250).await;
}

/// **A read fault after a rival where none was required** ([`read_fault_after_the_end`]): the part
/// read finds the rival's record, the first session read faults, `may_still_commit: false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_read_fault_after_a_rival_where_none_was_required_keeps_the_commit_unable_to_land() {
    read_fault_after_the_end(Ended::RivalWhereNoneWasRequired, 252).await;
}

/// **A slot rewritten under the attempt's own id during a held commit.** Not the bytes it last
/// wrote and not another attempt's: the in-doubt error with the cause `SlotLost`, keeping the flag,
/// never `Committed` and never a compensation. The rewritten bytes are left as they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_slot_rewritten_under_the_attempts_own_id_during_a_held_commit_is_slot_lost() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 221)).await;
        let key = attempt.slot_key().to_vec();
        let rewritten = rewritten_slot(&attempt);
        rig.store.script(
            Kind::Commit,
            vec![Act::hold().after(rig.plant(key.clone(), rewritten.clone()))],
            None,
        );
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        assert_eq!(doubt.batch, DoubtfulBatch::Commit);
        assert!(doubt.may_still_commit);
        assert_slot_lost(&doubt.cause, &attempt, "held");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "no retry");
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert!(rig.part(1).await.is_none());
        assert_eq!(rig.inner.get(&key).await.unwrap(), Some(rewritten));
        assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
    })
    .await;
}

/// **A slot gone under a moved session with a commit in doubt.** The session rolled back, or fenced
/// by a real Abort, and the slot deleted, right after a commit answered unknown (held, or out of
/// flight and not applied): a teardown removes slots too, so no read says whose batch removed it.
/// The in-doubt error, `SessionMoved`, saying the commit cannot land; never `Committed` and never a
/// compensation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_slot_gone_under_a_moved_session_with_a_commit_in_doubt_is_in_doubt() {
    for moved in ["rollback", "abort"] {
        for held in [false, true] {
            within(async {
                let at = format!("{moved}, held {held}");
                let mut rig = Rig::new().await;
                let attempt = rig.stage(1, &body(CHUNK, 222)).await;
                let lost = if held {
                    Act::hold()
                } else {
                    Act::unknown(false, false)
                };
                let step = if moved == "rollback" {
                    rig.rollback()
                } else {
                    rig.abort()
                };
                let gone = rig.erased(vec![attempt.slot_key().to_vec()]);
                rig.store
                    .script(Kind::Commit, vec![lost.after(step).after(gone)], None);
                let doubt = in_doubt(rig.commit(attempt.clone()).await);
                assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
                assert!(!doubt.may_still_commit, "{at}: the session moved");
                assert_session_moved(&doubt.cause, &at);
                assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
                assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
                assert!(rig.part(1).await.is_none(), "{at}");
            })
            .await;
        }
    }
}

// ─────────────── (F) the lease, delayed after dispatch (#1029) ───────────────

/// **(F) Delayed after dispatch.** A commit is sent with the margin left, and the wrapper holds it
/// and moves the clock past the lease's expiry, so the batch passed the dispatch check and has not
/// applied when the lease runs out. (i) It lands before the re-read: `Committed`. (ii) It lands
/// after the attempt returned: the in-doubt error keeping the flag, then the part durable and the
/// slot released once. (iii) A test-local batch in the idle fence's shape (`0016:1859-1863`) lands
/// first and the held batch then answers `Conflict`: no part, and `NoSuchUpload` or in doubt. (iv)
/// The held batch lands, then that fence, built from slot bytes read before the landing: the fence
/// answers `Conflict`, the session stays `Open@E`, and the part is durable. In all four one commit
/// batch and no compensation: an expired lease is never evidence about a commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_a_commit_delayed_past_its_lease_is_settled_by_reads_never_by_the_clock() {
    for run in ["i", "ii", "iii", "iv"] {
        within(async {
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK + 4, 224)).await;
            let before = rig.session().await;
            let seen: SeenSlots = Arc::default();
            let past = Step::Advance(left_after_renewal(&rig, &attempt) + 1);
            let held = match run {
                "i" => Act::hold()
                    .after(past)
                    .after(rig.land_held(CommitOutcome::Committed)),
                "ii" => Act::hold().after(past),
                "iii" => Act::hold()
                    .after(past)
                    .after(rig.read_slots(Arc::clone(&seen)))
                    .after(rig.idle_fence(Arc::clone(&seen), CommitOutcome::Committed)),
                _ => Act::hold()
                    .after(past)
                    .after(rig.read_slots(Arc::clone(&seen)))
                    .after(rig.land_held(CommitOutcome::Committed))
                    .after(rig.idle_fence(Arc::clone(&seen), CommitOutcome::Conflict)),
            };
            rig.store.script(Kind::Commit, vec![held], None);
            let outcome = rig.commit(attempt.clone()).await;
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "run {run}");
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "run {run}");
            match run {
                "i" | "iv" => {
                    committed_as(&outcome, &attempt);
                    assert_eq!(rig.session().await, before, "run {run}: still Open@E");
                }
                "ii" => {
                    let doubt = in_doubt(outcome);
                    assert!(doubt.may_still_commit, "the doubt is kept");
                    assert_outcome_unknown(&doubt.cause, "run ii");
                    assert!(rig.part(1).await.is_none(), "not yet");
                    assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
                }
                _ => {
                    if matches!(outcome, Err(CommitError::InDoubt(_))) {
                        in_doubt(outcome);
                    } else {
                        refused(&outcome, Refusal::NoSuchUpload);
                    }
                    assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
                    assert!(rig.part(1).await.is_none(), "no part record");
                    assert_eq!(rig.releases(&attempt), 0);
                    return;
                }
            }
            assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "run {run}");
            assert_eq!(rig.releases(&attempt), 1, "run {run}: released once");
            assert_eq!(rig.slots_of(&attempt).await, 0, "run {run}");
            assert_eq!(rig.owned_of(&attempt).await, 0, "run {run}");
        })
        .await;
    }
}

// ─────────────── (H) a held commit and a rollback (#1029) ───────────────

/// **(H) A held commit and a rollback, both right after the slot read**, in either order, one test
/// per order. The slot read found the slot held, and the last session read finds the session
/// moved, so no later read can prove the commit. **The commit lands first:** the slot is gone under
/// a moved session, which proves nothing about whose batch removed it: the in-doubt error
/// (`SessionMoved`), no compensation, the slot released once by the commit, and the attempt's own
/// part durable. **The rollback lands first:** the held batch, released right after it, answers
/// `Conflict`; the in-doubt error, no compensation, and the slot still held.
async fn held_commit_and_rollback(commit_first: bool) {
    within(async {
        let at = format!("commit first {commit_first}");
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 225)).await;
        let pair = if commit_first {
            vec![rig.land_held(CommitOutcome::Committed), rig.rollback()]
        } else {
            vec![rig.rollback(), rig.land_held(CommitOutcome::Conflict)]
        };
        let steps = rig.at_gaps(&attempt, vec![(3, pair)]);
        rig.store
            .script(Kind::Commit, vec![Act::hold().after_all(steps)], None);
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
        assert!(!doubt.may_still_commit, "{at}: the session moved");
        assert_session_moved(&doubt.cause, &at);
        assert_eq!(
            rig.log.reads_after(Kind::Commit, 0),
            laps_of(&rig, &attempt, 1),
            "{at}: one lap"
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
        assert_eq!(
            rig.log.commits(Kind::Compensation).len(),
            0,
            "{at}: no compensation"
        );
        if commit_first {
            assert_eq!(rig.releases(&attempt), 1, "{at}");
            assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
        } else {
            assert_eq!(rig.releases(&attempt), 0, "{at}");
            assert_eq!(rig.slots_of(&attempt).await, 1, "{at}: the slot still held");
            assert!(rig.part(1).await.is_none(), "{at}");
        }
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(H), the commit lands first** ([`held_commit_and_rollback`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h_a_held_commit_landing_before_a_rollback_is_in_doubt() {
    held_commit_and_rollback(true).await;
}

/// **(H), the rollback lands first** ([`held_commit_and_rollback`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h_a_rollback_landing_before_a_held_commit_is_in_doubt_and_never_compensated() {
    held_commit_and_rollback(false).await;
}

// ─────────────── (R) damage under a held commit (#1029) ───────────────

/// **(R) Damage under a held commit keeps the doubt.** The commit is held, then a record is damaged
/// and stays damaged: bytes at the part key that do not decode; bytes at the slot key that do not
/// decode; the session record erased with this attempt's slot left there, or with another
/// attempt's record put at the slot key; and the session damaged at each gap before the last
/// session read, with each kind of damage (torn, an `Open` record at the same and at a lower epoch,
/// an `Aborting` record at the same and at a lower epoch). Each is the in-doubt error keeping
/// `may_still_commit`, its cause `Record`, with no second batch and no compensation; repairing the
/// record lets the held batch land, the one release. Damage after the held commit landed is not
/// reached: the own-commit proof answers first, `Committed`. A prior part record gone under
/// `Open@E` is damage too, and putting it back lets the held re-upload land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_damage_under_a_held_commit_keeps_the_doubt() {
    // Each case: what it is, the gap the damage lands at, the damage, the record error, the repair,
    // and whether the repair also puts back the slot bytes the held commit requires. Four cases,
    // then each kind of session damage at each of gaps 0 to 3.
    let missing = Some(RecordError::SessionRecordMissing { required: 2 });
    for case in 0..(4 + SESSION_DAMAGE_KINDS * 4) {
        within(async {
            let mut rig = Rig::new().await;
            // One rollback first, so that a lower epoch exists for the damage to carry.
            let older = rig.session().await;
            rig.run(vec![rig.rollback()]).await;
            let attempt = rig.stage(1, &body(CHUNK, 227)).await;
            let session = rig.session().await;
            let session_key = mpu_key(&rig.upload);
            let slot = attempt.slot_key().to_vec();
            let part_at = part_key(&rig.upload, part(1));
            let restore_session = || rig.plant(session_key.clone(), session.clone());
            let (at, gap, damage, expected, mut repair, restore_slot) = match case {
                0 => (
                    "torn part".to_owned(),
                    0,
                    vec![rig.plant(part_at.clone(), Bytes::from_static(b"torn"))],
                    None,
                    vec![rig.erased(vec![part_at.clone()])],
                    false,
                ),
                1 => (
                    "torn slot".to_owned(),
                    0,
                    vec![rig.plant(slot.clone(), Bytes::from_static(b"torn"))],
                    None,
                    vec![],
                    true,
                ),
                2 => (
                    "session erased, own slot".to_owned(),
                    0,
                    vec![rig.erased(vec![session_key.clone()])],
                    missing.clone(),
                    vec![restore_session()],
                    false,
                ),
                3 => (
                    "session erased, another attempt's slot".to_owned(),
                    0,
                    vec![
                        rig.erased(vec![session_key.clone()]),
                        rig.plant(slot.clone(), foreign_slot(&attempt)),
                    ],
                    missing.clone(),
                    vec![restore_session()],
                    true,
                ),
                _ => {
                    let (gap, kind) = (
                        (case - 4) / SESSION_DAMAGE_KINDS,
                        (case - 4) % SESSION_DAMAGE_KINDS,
                    );
                    let (label, bytes, expected) =
                        session_damage(&rig, &session, &older).swap_remove(kind);
                    (
                        format!("session {label} at gap {gap}"),
                        gap,
                        vec![rig.plant(session_key.clone(), bytes)],
                        expected,
                        vec![restore_session()],
                        false,
                    )
                }
            };
            let steps = rig.at_gaps(&attempt, vec![(gap, damage)]);
            rig.store
                .script(Kind::Commit, vec![Act::hold().after_all(steps)], None);
            let doubt = in_doubt(rig.commit(attempt.clone()).await);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            assert!(doubt.may_still_commit, "{at}: damage settles nothing");
            assert_record_stop(&doubt.cause, expected.as_ref(), &at);
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: no retry");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.releases(&attempt), 0, "{at}");
            if restore_slot {
                repair.push(rig.plant(slot.clone(), required_slot(&rig, &attempt)));
            }
            rig.run(repair).await;
            assert_eq!(
                rig.store.release_held().await,
                CommitOutcome::Committed,
                "{at}: the repaired record lets the held batch land"
            );
            assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
            assert_eq!(rig.releases(&attempt), 1, "{at}");
            assert_eq!(rig.slots_of(&attempt).await, 0, "{at}");
        })
        .await;
    }

    // Damage at the part key after the held commit landed: the own-commit proof comes first.
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 228)).await;
        let damage = rig.plant(part_key(&rig.upload, part(1)), Bytes::from_static(b"torn"));
        rig.store.script(
            Kind::Commit,
            vec![Act::hold()
                .after(rig.land_held(CommitOutcome::Committed))
                .after(damage)],
            None,
        );
        committed_as(&rig.commit(attempt.clone()).await, &attempt);
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.releases(&attempt), 1);
    })
    .await;

    // A prior part record gone while the session is still `Open@E`, under a held re-upload: a
    // `part:` record goes only after the session leaves `Open`, so this is damage, not a rival, and
    // the doubt is kept; putting the prior back lets the held batch land.
    within(async {
        let mut rig = Rig::new().await;
        rig.committed(1, &body(CHUNK, 236)).await;
        let key = part_key(&rig.upload, part(1));
        let prior = part_bytes(&rig, 1).await;
        let attempt = rig.stage(1, &body(CHUNK, 237)).await;
        let gone = rig.erased(vec![key.clone(), psum_key(&rig.upload, part(1))]);
        rig.store
            .script(Kind::Commit, vec![Act::hold().after(gone)], None);
        let doubt = in_doubt(rig.commit(attempt.clone()).await);
        assert_eq!(doubt.batch, DoubtfulBatch::Commit);
        assert!(doubt.may_still_commit, "damage settles nothing");
        assert_record_stop(
            &doubt.cause,
            Some(&RecordError::PartRecordMissing { part_number: 1 }),
            "prior gone",
        );
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "no retry");
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        rig.run(vec![rig.plant(key, prior)]).await;
        assert_eq!(rig.store.release_held().await, CommitOutcome::Committed);
        assert!(is_own(&rig.part(1).await.unwrap().0, &attempt));
        assert_eq!(rig.releases(&attempt), 1);
    })
    .await;
}

/// **(R) A missing session behind a held commit.** The held commit lands and then only the session
/// record is erased; or the commit never lands and the terminal delete takes the session, the slots
/// and the owned entries in one batch. Either way the last session read finds no record and the
/// slot key read once more is absent: a terminal delete, which proves nothing about a commit that
/// may have landed before it. The in-doubt error (`SessionMoved`), saying the commit cannot land;
/// never `Committed`, never `NoSuchUpload`, and no compensation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_a_missing_session_behind_a_held_commit_is_in_doubt() {
    for landed in [true, false] {
        within(async {
            let at = format!("landed {landed}");
            let mut rig = Rig::new().await;
            let attempt = rig.stage(1, &body(CHUNK, 229)).await;
            let act = if landed {
                Act::hold()
                    .after(rig.land_held(CommitOutcome::Committed))
                    .after(rig.erased(vec![mpu_key(&rig.upload)]))
            } else {
                Act::hold().after(rig.terminal_delete())
            };
            rig.store.script(Kind::Commit, vec![act], None);
            let doubt = in_doubt(rig.commit(attempt.clone()).await);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            assert!(!doubt.may_still_commit, "{at}: that session is gone");
            assert_session_moved(&doubt.cause, &at);
            let mut reads = lap(&rig, &attempt);
            reads.push(attempt.slot_key().to_vec());
            assert_eq!(rig.log.reads_after(Kind::Commit, 0), reads, "{at}");
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.releases(&attempt), usize::from(landed), "{at}");
            if !landed {
                assert_eq!(rig.store.release_held().await, CommitOutcome::Conflict);
            }
        })
        .await;
    }
}

/// **(R) Damage repaired mid-lap is no evidence of anything.** The first lap's first session read
/// finds damage and its last finds the session repaired, so the lap ends with the slot held under
/// the unchanged session. Read as a moved session, that first read would say the held commit
/// cannot land. Required: the in-doubt error keeping `may_still_commit: true`, no compensation, and
/// the held batch landing once the bytes it requires are back. Run with the repair on either side
/// of the slot read and each kind of damage, over a rival's record on an empty prior and over no
/// prior. A held commit that lands while the session is damaged is still proved by the slot and
/// the session read after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r_a_session_damaged_and_repaired_mid_lap_settles_nothing() {
    for repair_at in [2usize, 3] {
        for kind in 0..SESSION_DAMAGE_KINDS {
            for over_a_rival in [true, false] {
                within(async {
                    let mut rig = Rig::new().await;
                    let older = rig.session().await;
                    rig.run(vec![rig.rollback()]).await;
                    let mut gaps = Vec::new();
                    let mut required = None;
                    if over_a_rival {
                        rig.committed(1, &[]).await;
                        required = Some(part_bytes(&rig, 1).await);
                    }
                    let attempt = rig.stage(1, &body(CHUNK, 230)).await;
                    if over_a_rival {
                        let rival = rig.stage(1, &body(CHUNK, 231)).await;
                        gaps.push((0, vec![rig.rival(rival)]));
                    }
                    let session = rig.session().await;
                    let key = mpu_key(&rig.upload);
                    let (label, damage, _) =
                        session_damage(&rig, &session, &older).swap_remove(kind);
                    let at = format!(
                        "{label}, repaired at gap {repair_at}, over a rival {over_a_rival}"
                    );
                    gaps.push((1, vec![rig.plant(key.clone(), damage)]));
                    gaps.push((repair_at, vec![rig.plant(key.clone(), session)]));
                    let steps = rig.at_gaps(&attempt, gaps);
                    rig.store
                        .script(Kind::Commit, vec![Act::hold().after_all(steps)], None);
                    let doubt = in_doubt(rig.commit(attempt.clone()).await);
                    assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
                    assert!(doubt.may_still_commit, "{at}");
                    assert_outcome_unknown(&doubt.cause, &at);
                    assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}");
                    assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
                    assert_eq!(rig.slots_of(&attempt).await, 1, "{at}");
                    if let Some(prior) = required {
                        rig.run(vec![rig.plant(part_key(&rig.upload, part(1)), prior)])
                            .await;
                    }
                    assert_eq!(
                        rig.store.release_held().await,
                        CommitOutcome::Committed,
                        "{at}: the held commit could still land"
                    );
                    assert!(is_own(&rig.part(1).await.unwrap().0, &attempt), "{at}");
                    assert_eq!(rig.releases(&attempt), 1, "{at}");
                })
                .await;
            }
        }
    }

    // The held commit lands while the session is damaged, and the repair comes before the slot read.
    for kind in 0..SESSION_DAMAGE_KINDS {
        within(async {
            let mut rig = Rig::new().await;
            let older = rig.session().await;
            rig.run(vec![rig.rollback()]).await;
            let attempt = rig.stage(1, &body(CHUNK, 233)).await;
            let session = rig.session().await;
            let key = mpu_key(&rig.upload);
            let (label, damage, _) = session_damage(&rig, &session, &older).swap_remove(kind);
            let steps = rig.at_gaps(
                &attempt,
                vec![
                    (1, vec![rig.plant(key.clone(), damage)]),
                    (
                        2,
                        vec![
                            rig.plant(key.clone(), session),
                            rig.land_held(CommitOutcome::Committed),
                        ],
                    ),
                ],
            );
            rig.store
                .script(Kind::Commit, vec![Act::hold().after_all(steps)], None);
            committed_as(&rig.commit(attempt.clone()).await, &attempt);
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{label}");
            assert_eq!(rig.releases(&attempt), 1, "{label}");
        })
        .await;
    }
}
