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
//! * **(H4)** the stops later slices replace.
//!
//! The wrapper is handed each batch's submitter apart from the batch, by the [`As`] view each
//! attempt's store calls go through. On **every** batch of every leg it checks that a batch naming a
//! `slot:` key is either that submitter's reservation or requires exactly the bytes the submitter
//! last landed there; that a part commit is sent with the dispatch margin left on the lease the
//! submitter last wrote; and that every value a batch puts decodes back unchanged.
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
    psum_range, sidx_range, slot_range, AbortOutcome, AttemptId, Budget, CreateOutcome, Digest,
    PartNumber, PartRecord, PartSummary, RecordError, Refusal, ReserveOutcome, RetireMode,
    RetireToken, SessionState, UploadId, UploadPartOutcome, MAX_INFLIGHT_PARTS, MAX_PART_CHUNKS,
    MAX_STAGED_CHUNKS, PART_PREFIX, PSUM_PREFIX, RETIRE_BYTES_PREFIX, SIDX_PREFIX, SLOT_PREFIX,
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
    /// The slot bytes each attempt holds from its own last landed write, per slot key; gone once
    /// that attempt's own batch released the slot. Recorded only from the attempt's own batches as
    /// they landed, so another attempt's write to the key never changes it.
    slots: HashMap<(Vec<u8>, AttemptId), Bytes>,
    /// Every batch that landed through the wrapper, held ones released included, with its
    /// submitter.
    landed: Vec<(AttemptId, WriteBatch)>,
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
            }
        }
    }

    fn on_read(&self, key: &[u8]) {
        self.log.push(Event::Read { key: key.to_vec() });
        self.clock.advance(self.state.lock().unwrap().read_tick);
    }

    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        if self.read_faults(key) {
            return Err("scripted read fault".into());
        }
        let found = self.inner.get(key).await;
        self.on_read(key);
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
/// upload id, the batch in doubt and whether it may still be applied, and it claims no release.
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
            ] {
                assert!(text.contains(&wanted), "{text:?} names {wanted:?}");
            }
            assert_eq!(doubt.may_still_commit, doubt.outcome.may_still_commit);
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
                     commit was sent"
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

// ─────────────── (H4) the stops later slices replace ───────────────

/// **(H4)** A commit answered `Conflict` returns a typed stop that says the slot is still held, with
/// no compensation and no second commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_commit_answered_conflict_stops_with_the_slot_held() {
    within(async {
        let mut rig = Rig::new().await;
        let attempt = rig.stage(1, &body(CHUNK, 161)).await;
        rig.store
            .script(Kind::Commit, vec![], Some(Act::conflict()));
        let cause = stopped(rig.commit(attempt.clone()).await, false);
        assert!(matches!(cause, Stop::CommitConflict), "{cause:?}");
        assert!(cause.to_string().contains("slot is still held"), "{cause}");
        assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "no second commit");
        assert_eq!(rig.log.commits(Kind::Compensation).len(), 0);
        assert_eq!(rig.slots_of(&attempt).await, 1, "the slot stays");
        assert_eq!(rig.owned_of(&attempt).await, 1, "the owned entry stays");
        assert!(rig.part(1).await.is_none());
        rig.assert_every_fragment_named().await;
    })
    .await;
}

/// **(H4)** A commit answered `CommitUnknownResult` returns `CommitError::InDoubt` keeping the flag
/// as reported, with no further batch, even for `may_still_commit: false`, since it may have landed.
/// `Committed` is never answered, even for the one that did land.
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
            let doubt = in_doubt(rig.commit(attempt.clone()).await);
            assert_eq!(doubt.batch, DoubtfulBatch::Commit, "{at}");
            assert_eq!(doubt.may_still_commit, may_still_commit, "{at}");
            assert_eq!(rig.log.commits(Kind::Commit).len(), 1, "{at}: no retry");
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 0, "{at}");
            assert_eq!(rig.part(1).await.is_some(), applied, "{at}");
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

/// **(H4)** A compensation answered `Conflict` returns a typed stop that claims no release, on each
/// route to a compensation, with no further batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_compensation_answered_conflict_claims_no_release() {
    for route in ["renewal", "lease", "ceiling"] {
        within(async {
            let (rig, attempt) = compensating(route, 164).await;
            rig.store
                .script(Kind::Compensation, vec![], Some(Act::conflict()));
            let cause = stopped(rig.commit(attempt.clone()).await, false);
            assert!(
                matches!(cause, Stop::CompensationConflict),
                "{route}: {cause:?}"
            );
            assert_eq!(rig.log.commits(Kind::Compensation).len(), 1, "{route}");
            assert_eq!(rig.log.commits(Kind::Commit).len(), 0, "{route}");
            assert_eq!(rig.slots_of(&attempt).await, 1, "{route}: the slot stays");
            assert!(rig.part_obligations().await.is_empty(), "{route}");
            rig.assert_every_fragment_named().await;
        })
        .await;
    }
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

        // A commit stopped with nothing in doubt is not.
        let mut rig = Rig::new().await;
        rig.store.script(Kind::Commit, vec![Act::conflict()], None);
        let err = rig
            .upload(1, &body(CHUNK, 194), MAX_PART_CHUNKS)
            .await
            .expect_err("the commit conflicted");
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
