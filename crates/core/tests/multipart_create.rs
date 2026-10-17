//! Issue #894 — `CreateMultipartUpload`'s **admission and session creation**,
//! `wyrd_core::multipart::create::create_session`, driven over a real store
//! (`RedbMetadataStore::in_memory()`), real preconditions and real contention, on a multi-thread
//! tokio runtime (proposal 0016's *Create session* row, `0016:657`, and its unknown-outcome rule,
//! `0016:2010-2028`).
//!
//! Terms: an **identity** is one minted (upload id, segment-group nonce) pair, and **R** is the
//! exact session record bytes every batch under that identity puts. "Writes nothing" means the
//! store is byte-identical before and after: no `mpu:`, no `seggrp:` and no `mpuctl` write.
//!
//! * **(A)** free-running storms of 8, 10 and 16 creates on an empty store all admit — the
//!   levels #508's rejected attempts falsely refused.
//! * **(A′)** a store wrapper forces the losing schedule: every commit is held until every live
//!   creator has built its batch, so each round has one winner and every other creator loses the
//!   `mpuctl` CAS. The last creator loses far more than `MAX_UPLOAD_ID_ATTEMPTS` times and is
//!   still admitted, and under a profile larger than the shipped one, one creator more than the
//!   shipped `MAX_SESSIONS` is admitted too. The waits each create asks for are recorded.
//! * **(B)** the real bounds, each writing nothing: a full ledger, a profile skew, a missing
//!   bucket (judged before a full ledger), a bucket marker deleted or rewritten under the create,
//!   a ledger filled under the create (judged before contention), a torn ledger, a contention
//!   budget spent on `Conflict`s that moved nothing (TiKV's lock conflict), and the size headroom,
//!   found on encoded bytes through a wrapper that enforces the value ceiling, on one fixture
//!   where `Completing` is the widest state and one where `Completed` is.
//! * **(C)** the collision budget and the contention budget never share; an id or a nonce taken
//!   under the commit gets a re-mint; and neither a byte-identical record that was there first
//!   (under the first identity or a re-minted one) nor another create's session under the same
//!   upload id (even one carrying this create's nonce, or its request under another nonce) is
//!   ever adopted.
//! * **(F)** an unknown commit outcome is recovered under the original identity, with the
//!   `mpuctl` precondition rebuilt from a fresh read, the doubt kept past every later fault, a
//!   settling read before every ending the create gives up on, an in-doubt error that classifies
//!   as indeterminate, and a plain error left standing once a read has settled the doubt.
//! * **(F10)** a create whose own session landed and was fenced before it read it back ends in
//!   doubt naming that session, never in a re-mint that would leave it counted and unnamed.
//!
//! The wrappers keep their state in struct fields, never in a `static` (ADR-0035).

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::watch;
use wyrd_core::metadata::{self, InodeId, SegmentNonce, MAX_SEGMENT_INDEX, MAX_VALUE_BYTES};
use wyrd_core::multipart::create::{
    create_session, CreateError, CreateRequest, CreateRuntime, InDoubt, Unsettled,
};
use wyrd_core::multipart::{
    admission_backoff_millis, decode_admission_record, decode_retire_obligation,
    decode_session_record, mpu_key, parse_mpu_key, AdmissionRecord, Backpressure, Budget,
    CreateOutcome, Refusal, SessionRecord, SessionState, UploadId, MAX_ADMISSION_CAS_ATTEMPTS,
    MAX_INFLIGHT_PARTS, MAX_PARTS_PER_SESSION, MAX_PART_CHUNKS, MAX_PART_NUMBER, MAX_SESSIONS,
    MAX_STAGED_CHUNKS, MAX_UPLOAD_ID_ATTEMPTS, MPUCTL_KEY, MPU_PREFIX, W_REF,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{
    classify, BoxError, CommitOutcome, CommitUnknownResult, ErrorClass, MetadataStore,
    Result as StoreResult, ScanPage, TransientFault, WriteBatch,
};

// ─────────────────────────────── fixtures ───────────────────────────────

const BUCKET: &str = "photos";
/// The root inode every Alpha object dirent lives under, as `<bucket>/<key>` (ADR-0046 decision 3).
const ROOT: InodeId = 0;
const CLOCK: &str = "wall";
const CREATED_AT: u64 = 1_000;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
/// The marker of a bucket deleted and created again under the same name.
const RECREATED_MARKER: &[u8] = br#"{"name":"photos","created_at_millis":2}"#;
/// XORed into an identity's number to derive its nonce, so a nonce never spells its upload id.
const NONCE_SALT: u128 = 0x5a5a_5a5a_5a5a_5a5a_5a5a_5a5a_5a5a_5a5a;
/// The identity number whose nonce an interloper's record carries (C3).
const INTERLOPER: u128 = 0x1_0000;
/// The identity number whose nonce a rival create is minted with (C6).
const RIVAL: u128 = 0x2_0000;
/// No leg may hang the suite: a schedule that never settles fails here instead.
const LEG_TIMEOUT: Duration = Duration::from_secs(120);
/// More commits than any leg's create may make: `max_sessions + MAX_UPLOAD_ID_ATTEMPTS` at the
/// largest profile here, with room to spare. A create past it never stops committing.
const RUNAWAY_COMMITS: usize = 1_000;

fn object(name: &str) -> String {
    format!("{BUCKET}/{name}")
}

fn request<'a>(object: &'a str, content_type: Option<&'a str>) -> CreateRequest<'a> {
    CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object,
        content_type,
        created_at_millis: CREATED_AT,
        clock_source: CLOCK,
    }
}

/// A [`Budget`] decoded from its wire spelling — the only way one comes into existence.
fn budget(w_ref: u64, mpc: u32, mpps: u32, mip: u32, msc: u32) -> Budget {
    let json = format!(
        "{{\"w_ref\":{w_ref},\"max_part_chunks\":{mpc},\"max_parts_per_session\":{mpps},\
         \"max_inflight_parts\":{mip},\"max_staged_chunks\":{msc}}}"
    );
    metadata::decode(json.as_bytes()).expect("a legal profile")
}

/// The shipped profile: the five numbers `mpuctl` stores, at their shipped values.
fn shipped() -> Budget {
    let profile = budget(
        W_REF,
        MAX_PART_CHUNKS,
        MAX_PARTS_PER_SESSION,
        MAX_INFLIGHT_PARTS,
        MAX_STAGED_CHUNKS,
    );
    assert_eq!(profile.max_sessions(), MAX_SESSIONS);
    profile
}

/// A profile admitting exactly `n` sessions: every per-session cap at 1, so `U_ref` is 2 and
/// `W_ref = 2n` derives `max_sessions = n`.
fn profile_of(n: u64) -> Budget {
    let profile = budget(2 * n, 1, 1, 1, 1);
    assert_eq!(profile.max_sessions(), n);
    profile
}

/// The `mpuctl` bytes of a ledger at `count` under `profile` — this codec's own spelling.
fn ledger_bytes(count: u64, profile: &Budget) -> Bytes {
    let profile_json = metadata::encode(profile);
    let bytes = format!(
        "{{\"count\":{count},\"max_sessions\":{},\"profile\":{}}}",
        profile.max_sessions(),
        std::str::from_utf8(&profile_json).unwrap()
    );
    let record = decode_admission_record(bytes.as_bytes()).expect("a canonical ledger");
    assert_eq!(metadata::encode(&record), bytes.as_bytes());
    Bytes::from(bytes)
}

/// Identity number `n`: upload id `n`, nonce `n ^ NONCE_SALT`, both 32 lowercase hex.
fn identity(n: u128) -> (UploadId, SegmentNonce) {
    (
        UploadId::new(format!("{n:032x}")).unwrap(),
        SegmentNonce::new(format!("{:032x}", n ^ NONCE_SALT)).unwrap(),
    )
}

/// The JSON spelling of a string, escapes included — the encoder's own.
fn json_string(text: &str) -> String {
    String::from_utf8(metadata::encode(&text).to_vec()).unwrap()
}

/// The record an **interloper** writes under another create's `mpu:<id>` (C3): a canonical
/// `Open` session, but for another object, under another nonce, outside any ledger.
fn interloper() -> Bytes {
    let (_, nonce) = identity(INTERLOPER);
    let bytes = format!(
        "{{\"parent\":{ROOT},\"object\":{},\"created_at_millis\":{CREATED_AT},\
         \"clock_source\":{},\"segment_nonce\":\"{nonce}\",\"epoch\":0,\"attempts\":0,\
         \"state\":{{\"kind\":\"Open\"}}}}",
        json_string(&object("interloper")),
        json_string(CLOCK),
    );
    let record = decode_session_record(bytes.as_bytes()).expect("a canonical session");
    assert_eq!(metadata::encode(&record), bytes.as_bytes());
    Bytes::from(bytes)
}

/// The caller's runtime: an identity source, a fixed jitter, and a wait that only records.
struct Script {
    /// Identities handed out first, in order (a repeating source puts an existing one here).
    queue: VecDeque<(UploadId, SegmentNonce)>,
    /// Then fresh identities `next + 1`, `next + 2`, …
    next: u128,
    minted: Vec<(UploadId, SegmentNonce)>,
    jitter: u64,
    /// Every wait asked for, in order.
    waits: Vec<u64>,
}

impl Script {
    fn fresh(base: u128) -> Self {
        Self::repeating(Vec::new(), base)
    }

    fn repeating(first: Vec<(UploadId, SegmentNonce)>, base: u128) -> Self {
        Self {
            queue: first.into(),
            next: base,
            minted: Vec::new(),
            jitter: u64::MAX,
            waits: Vec::new(),
        }
    }
}

impl CreateRuntime for Script {
    fn mint(&mut self) -> (UploadId, SegmentNonce) {
        let minted = self.queue.pop_front().unwrap_or_else(|| {
            self.next += 1;
            identity(self.next)
        });
        self.minted.push(minted.clone());
        minted
    }

    fn jitter(&mut self) -> u64 {
        self.jitter
    }

    fn wait(&mut self, millis: u64) -> impl Future<Output = ()> + Send {
        self.waits.push(millis);
        std::future::ready(())
    }
}

async fn store_with_bucket() -> RedbMetadataStore {
    let store = RedbMetadataStore::in_memory().unwrap();
    store
        .commit(WriteBatch::new().put(metadata::bucket_key(BUCKET), MARKER))
        .await
        .unwrap();
    store
}

/// Every record in the store, in key order.
async fn snapshot(store: &impl MetadataStore) -> Vec<(Vec<u8>, Bytes)> {
    let mut all = store.scan(b"").await.unwrap();
    all.sort();
    all
}

/// The stored ledger, decoded — and re-encoded to exactly the bytes stored: every later create
/// and the terminal delete CAS it on those bytes (serialization identity).
async fn ledger(store: &impl MetadataStore) -> Option<AdmissionRecord> {
    let bytes = store.get(MPUCTL_KEY).await.unwrap()?;
    let record = decode_admission_record(&bytes).expect("a valid ledger");
    assert_eq!(
        metadata::encode(&record),
        bytes,
        "mpuctl round-trips byte for byte"
    );
    Some(record)
}

/// Every session in the store, decoded, by upload id — each re-encoded to exactly the bytes
/// stored, since every later transition CASes the session on them (serialization identity).
async fn sessions(store: &impl MetadataStore) -> BTreeMap<String, SessionRecord> {
    let mut found = BTreeMap::new();
    for (key, value) in store.scan(MPU_PREFIX).await.unwrap() {
        let id = parse_mpu_key(&key).expect("a canonical mpu: key");
        let record = decode_session_record(&value).expect("a valid session record");
        assert_eq!(
            metadata::encode(&record),
            value,
            "{id} round-trips byte for byte"
        );
        found.insert(id.as_str().to_owned(), record);
    }
    found
}

/// Every `seggrp:` key in the store; each value is the one constant marker.
async fn markers(store: &impl MetadataStore) -> BTreeSet<Vec<u8>> {
    store
        .scan(metadata::SEGGRP_PREFIX)
        .await
        .unwrap()
        .into_iter()
        .map(|(key, value)| {
            assert_eq!(&value[..], metadata::SEGGRP_MARKER);
            key
        })
        .collect()
}

/// The admission invariant over the whole store: exactly the `expected` sessions (upload id →
/// object), each `Open` under the root for its object, one `seggrp:` marker per session nonce and
/// none other, and `mpuctl` counting exactly them.
async fn assert_population(store: &impl MetadataStore, expected: &BTreeMap<String, String>) {
    let found = sessions(store).await;
    let ids: BTreeSet<&String> = found.keys().collect();
    assert_eq!(
        ids,
        expected.keys().collect(),
        "the store must hold exactly the admitted sessions"
    );
    let mut nonces = BTreeSet::new();
    for (id, record) in &found {
        assert!(
            matches!(record.state(), SessionState::Open {}),
            "{id}: a created session is Open"
        );
        assert_eq!(record.parent(), ROOT, "{id}");
        assert_eq!(record.object(), expected[id], "{id}");
        assert_eq!(record.epoch(), 0, "{id}");
        assert_eq!(record.attempts(), 0, "{id}");
        assert_eq!(record.clock_source(), CLOCK, "{id}");
        assert_eq!(record.created_at_millis(), CREATED_AT, "{id}");
        assert!(nonces.insert(metadata::seggrp_key(record.segment_nonce())));
    }
    assert_eq!(
        markers(store).await,
        nonces,
        "one seggrp: marker per session nonce"
    );
    let count = ledger(store).await.map_or(0, |ledger| ledger.count());
    assert_eq!(
        count,
        expected.len() as u64,
        "mpuctl counts every session, in the batch that installed it"
    );
}

/// Every session in the store, by upload id → object: for an `assert_population` over whatever
/// the legs' other creates admitted.
async fn population(store: &impl MetadataStore) -> BTreeMap<String, String> {
    sessions(store)
        .await
        .into_iter()
        .map(|(id, record)| (id, record.object().to_owned()))
        .collect()
}

fn created(outcome: &Result<CreateOutcome, CreateError>) -> UploadId {
    match outcome {
        Ok(CreateOutcome::Created { upload_id }) => upload_id.clone(),
        other => panic!("expected Created, got {other:?}"),
    }
}

/// **F9**, for one in-doubt ending: it is `InDoubt` naming `identity`, with `may_still_commit`
/// as given; [`classify`] reads it as `Indeterminate` (never transient), whatever stopped the
/// create, because its source is the store's own `CommitUnknownResult`; and its `Display` names
/// the upload id and the nonce.
fn in_doubt(
    outcome: Result<CreateOutcome, CreateError>,
    identity: &(UploadId, SegmentNonce),
    may_still_commit: bool,
) -> Box<InDoubt> {
    let err = match outcome {
        Err(err @ CreateError::InDoubt(_)) => err,
        other => panic!("expected InDoubt naming {}, got {other:?}", identity.0),
    };
    assert_eq!(
        classify(&err),
        ErrorClass::Indeterminate,
        "an in-doubt create must read as indeterminate, whatever stopped it: {err}"
    );
    assert!(
        !classify(&err).is_transient(),
        "an in-doubt create is never transient"
    );
    let unknown = std::error::Error::source(&err)
        .and_then(|source| source.downcast_ref::<CommitUnknownResult>())
        .expect("the source is the store's unknown outcome");
    assert_eq!(unknown.may_still_commit, may_still_commit);
    let text = err.to_string();
    assert!(
        text.contains(identity.0.as_str()) && text.contains(identity.1.as_str()),
        "the in-doubt Display names the upload id and the nonce: {text}"
    );
    assert!(
        text.contains("in doubt") && text.contains("do not retry"),
        "{text}"
    );
    let CreateError::InDoubt(doubt) = err else {
        unreachable!()
    };
    assert_eq!(doubt.upload_id, identity.0, "the doubtful upload id");
    assert_eq!(doubt.segment_nonce, identity.1, "the doubtful nonce");
    assert_eq!(doubt.may_still_commit, may_still_commit);
    doubt
}

fn unknown(may_still_commit: bool) -> BoxError {
    BoxError::from(CommitUnknownResult {
        backend: "test wrapper",
        code: None,
        detail: "the wrapper withheld the outcome".to_owned(),
        may_still_commit,
    })
}

/// What a scripted plain fault says.
const FAULT: &str = "the test wrapper's store failed";

fn fault(transient: bool) -> BoxError {
    if transient {
        BoxError::from(TransientFault::new(FAULT))
    } else {
        BoxError::from(FAULT)
    }
}

/// The `mpu:<id>` key a create's batch puts — how a wrapper tells creators apart.
fn session_key_of(batch: &WriteBatch) -> Vec<u8> {
    batch
        .puts
        .iter()
        .map(|(key, _)| key)
        .find(|key| key.starts_with(MPU_PREFIX))
        .expect("a create batch puts its session")
        .clone()
}

/// The value a batch puts at `key`.
fn put_of(batch: &WriteBatch, key: &[u8]) -> Bytes {
    batch
        .puts
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, value)| value.clone())
        .expect("the batch puts the key")
}

/// What the batch requires of `mpuctl`: `Some(bytes)`, or `None` for `require_absent`.
fn ledger_precondition(batch: &WriteBatch) -> Option<Bytes> {
    batch
        .preconditions
        .iter()
        .find(|pre| pre.key == MPUCTL_KEY)
        .expect("a create batch preconditions the ledger")
        .expected
        .clone()
}

async fn within<T>(leg: impl Future<Output = T>) -> T {
    tokio::time::timeout(LEG_TIMEOUT, leg)
        .await
        .expect("the leg settled in time")
}

// ─────────────────────── (A) a free-running storm ───────────────────────

/// **(A)** On a fresh, empty store, 8, 10 and 16 concurrent creates all answer `Created`, each
/// level on its own store. These are the levels #508's rejected attempts falsely refused
/// (8 → 2 refused, 10 → 3, 16 → 3; aws-cli's default `max_concurrent_requests` is 10). redb never
/// awaits inside `get` or `commit`, so a valid schedule may serialize them; (A′) is the leg that
/// forces the losing one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_free_running_create_storm_on_an_empty_store_admits_every_create() {
    within(async {
        for creators in [8u128, 10, 16] {
            let store = Arc::new(store_with_bucket().await);
            let mut tasks = Vec::new();
            for i in 0..creators {
                let store = Arc::clone(&store);
                tasks.push(tokio::spawn(async move {
                    let name = object(&format!("storm-{i}"));
                    // Half declare a content type and half do not, so both spellings are stored.
                    let content_type = i.is_multiple_of(2).then_some("application/octet-stream");
                    let asked = request(&name, content_type);
                    let mut script = Script::fresh(i << 64);
                    let outcome = create_session(&*store, &shipped(), &asked, &mut script).await;
                    (name, outcome)
                }));
            }
            let mut expected = BTreeMap::new();
            for task in tasks {
                let (name, outcome) = task.await.unwrap();
                let id = created(&outcome);
                assert!(
                    expected.insert(id.as_str().to_owned(), name).is_none(),
                    "upload ids are pairwise distinct"
                );
            }
            assert_eq!(expected.len() as u128, creators);
            assert_population(&*store, &expected).await;
            for record in sessions(&*store).await.values() {
                let index: u128 = record.object().rsplit('-').next().unwrap().parse().unwrap();
                let declared = index
                    .is_multiple_of(2)
                    .then_some("application/octet-stream");
                assert_eq!(record.content_type(), declared, "{}", record.object());
            }
        }
    })
    .await;
}

// ──────────────────── (A′) the forced losing schedule ────────────────────

/// A wrapper that holds every commit until **every live creator** has read the ledger and built
/// its batch, then releases the round at once. Every batch of a round carries the same `mpuctl`
/// precondition, so redb's serialized commit admits exactly one and every other creator loses the
/// CAS. Creators are told apart by the `mpu:<id>` key their batch puts.
struct Barrier {
    inner: RedbMetadataStore,
    round: Mutex<Round>,
    released: watch::Sender<u64>,
}

#[derive(Default)]
struct Round {
    /// Creators still running.
    live: usize,
    /// Creators whose commit is held in the current round.
    waiting: usize,
    generation: u64,
    /// CAS losses per creator, by its `mpu:<id>` key.
    losses: BTreeMap<Vec<u8>, u32>,
}

impl Barrier {
    fn new(inner: RedbMetadataStore, creators: usize) -> Self {
        Self {
            inner,
            round: Mutex::new(Round {
                live: creators,
                ..Round::default()
            }),
            released: watch::channel(0).0,
        }
    }

    fn release_if_full(round: &mut Round, released: &watch::Sender<u64>) {
        if round.live > 0 && round.waiting == round.live {
            round.waiting = 0;
            round.generation += 1;
            released.send_replace(round.generation);
        }
    }

    /// A creator returned, whatever it answered: it commits no more.
    fn retire(&self) {
        let mut round = self.round.lock().unwrap();
        round.live -= 1;
        Self::release_if_full(&mut round, &self.released);
    }

    fn losses(&self) -> BTreeMap<Vec<u8>, u32> {
        self.round.lock().unwrap().losses.clone()
    }
}

#[async_trait]
impl MetadataStore for Barrier {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
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
        let creator = session_key_of(&batch);
        let mut released = self.released.subscribe();
        let generation = {
            let mut round = self.round.lock().unwrap();
            let generation = round.generation;
            round.waiting += 1;
            Self::release_if_full(&mut round, &self.released);
            generation
        };
        released
            .wait_for(|current| *current != generation)
            .await
            .expect("the barrier outlives its creators");
        let outcome = self.inner.commit(batch).await?;
        if outcome == CommitOutcome::Conflict {
            *self
                .round
                .lock()
                .unwrap()
                .losses
                .entry(creator)
                .or_default() += 1;
        }
        Ok(outcome)
    }
}

/// `creators` creates through the [`Barrier`], each with its own identity source and a jitter of
/// `u64::MAX`, so each wait it asks for is exactly the backoff window.
async fn forced_storm(profile: Budget, creators: usize) -> (Arc<Barrier>, Vec<(UploadId, Script)>) {
    let barrier = Arc::new(Barrier::new(store_with_bucket().await, creators));
    let mut tasks = Vec::new();
    for i in 0..creators {
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let name = object(&format!("forced-{i}"));
            let mut script = Script::fresh((i as u128) << 64);
            let outcome =
                create_session(&*barrier, &profile, &request(&name, None), &mut script).await;
            barrier.retire();
            (outcome, script)
        }));
    }
    let mut results = Vec::new();
    for task in tasks {
        let (outcome, script) = task.await.unwrap();
        results.push((created(&outcome), script));
    }
    (barrier, results)
}

/// The per-creator half of (A′): every creator minted once (a lost CAS never re-mints), lost the
/// CAS once per round it did not win, and asked after its `k`-th loss for
/// `admission_backoff_millis(k, jitter)`, so its window grows.
fn assert_forced_schedule(barrier: &Barrier, results: &[(UploadId, Script)]) -> u32 {
    let losses = barrier.losses();
    let mut per_creator: Vec<u32> = results
        .iter()
        .map(|(id, _)| losses.get(&mpu_key(id)).copied().unwrap_or(0))
        .collect();
    for ((id, script), lost) in results.iter().zip(&per_creator) {
        assert_eq!(
            script.minted.len(),
            1,
            "{id}: a lost CAS never spends an identity"
        );
        let asked: Vec<u64> = (0..*lost)
            .map(|k| admission_backoff_millis(k, script.jitter))
            .collect();
        assert_eq!(
            script.waits, asked,
            "{id}: after its k-th lost CAS a create asks for admission_backoff_millis(k, jitter)"
        );
        assert!(
            script.waits.windows(2).all(|pair| pair[0] <= pair[1]),
            "{id}: the backoff window never shrinks"
        );
    }
    per_creator.sort_unstable();
    let rounds: Vec<u32> = (0..results.len() as u32).collect();
    assert_eq!(
        per_creator, rounds,
        "the barrier forces one winner per round: the k-th admitted creator lost exactly k times"
    );
    *per_creator.last().unwrap()
}

/// **(A′)** Eight creates through the forced losing schedule: all eight answer `Created`, the
/// ledger counts eight, and one creator lost the CAS seven times — far more than
/// `MAX_UPLOAD_ID_ATTEMPTS` — and still got its session, with one identity. A contention budget
/// capped at the collision budget refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prime_a_create_that_loses_the_cas_many_times_is_still_admitted() {
    within(async {
        let (barrier, results) = forced_storm(shipped(), 8).await;
        let most = assert_forced_schedule(&barrier, &results);
        assert!(
            most > MAX_UPLOAD_ID_ATTEMPTS,
            "the forced schedule must make one creator lose more than MAX_UPLOAD_ID_ATTEMPTS \
             ({MAX_UPLOAD_ID_ATTEMPTS}) times; it lost {most}"
        );
        assert_eq!(ledger(&barrier.inner).await.unwrap().count(), 8);
        let longest = results
            .iter()
            .map(|(_, s)| &s.waits)
            .max_by_key(|w| w.len())
            .unwrap();
        assert!(
            longest.last() > longest.first(),
            "the window grows across a create's losses: {longest:?}"
        );
        let expected = population(&barrier.inner).await;
        assert_eq!(expected.len(), 8);
        assert_population(&barrier.inner, &expected).await;
    })
    .await;
}

/// **(A′), second run.** Under a profile whose `max_sessions` is above the shipped
/// `MAX_SESSIONS`, one creator more than the shipped limit goes through the same barrier. All are
/// admitted, and the last lost the CAS `MAX_SESSIONS` times — the whole of the fixed
/// `MAX_ADMISSION_CAS_ATTEMPTS`. The contention budget follows the profile in effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prime_the_contention_budget_follows_the_profile_in_effect() {
    within(async {
        let profile = profile_of(MAX_SESSIONS + 18);
        assert!(profile.max_sessions() > MAX_SESSIONS);
        let creators = MAX_SESSIONS as usize + 1;
        let (barrier, results) = forced_storm(profile, creators).await;
        let most = assert_forced_schedule(&barrier, &results);
        assert_eq!(
            most, MAX_ADMISSION_CAS_ATTEMPTS,
            "the last creator lost as many CASes as the fixed budget allows, and was admitted"
        );
        let ledger = ledger(&barrier.inner).await.unwrap();
        assert_eq!(ledger.count(), creators as u64);
        assert_eq!(ledger.profile(), &profile);
    })
    .await;
}

// ──────────────── the scripted wrapper (B, C and F legs) ────────────────

/// What [`Scripted`] does with the create's next commit.
#[derive(Debug, Clone, Copy)]
enum OnCommit {
    /// Delegate unchanged.
    Pass,
    /// Admit this many other sessions (real creates on the inner store) first, so this commit's
    /// `mpuctl` CAS loses.
    AdmitFirst(usize),
    /// Delete the bucket marker first.
    DeleteMarkerFirst,
    /// Rewrite the bucket marker first (the bucket deleted and created again).
    RewriteMarkerFirst,
    /// An interloper writes this batch's `mpu:<id>` first — [`interloper`]'s record, another
    /// nonce and another object — without moving `mpuctl` (C3).
    PlantSessionFirst,
    /// An interloper writes this batch's `seggrp:<nonce>` first, and nothing else (C4).
    PlantGroupFirst,
    /// A **real** second create, minted with this batch's upload id but identity `nonce`'s nonce
    /// ([`RIVAL`]'s, or this create's own), for the object named `object` (another one, or this
    /// create's own), installs its session first (C6). Then this commit answers
    /// `CommitUnknownResult { may_still_commit }` unapplied: held, when `may_still_commit`.
    RivalFirst {
        may_still_commit: bool,
        nonce: u128,
        object: &'static str,
    },
    /// Answer `Ok(Conflict)` without applying the batch or changing any record, as TiKV does
    /// when a locking read meets another writer's lock.
    ConflictUnmoved,
    /// Answer a plain store error (not an unknown outcome), unapplied.
    Fail,
    /// Apply the batch or not, then answer `CommitUnknownResult { may_still_commit }`.
    Unknown {
        applied: bool,
        may_still_commit: bool,
    },
    /// Keep the batch unapplied and answer `CommitUnknownResult { may_still_commit: true }`; it
    /// lands later, when the wrapper or the test releases it.
    Hold,
    /// Answer `CommitUnknownResult { may_still_commit }` unapplied, and admit this many other
    /// sessions before answering.
    UnknownThenAdmit {
        may_still_commit: bool,
        admit: usize,
    },
    /// Apply the batch, tear the session it installed down ([`Scripted::fence`]), then answer
    /// `CommitUnknownResult { may_still_commit }`: the session moved on before any re-read.
    UnknownThenFence { may_still_commit: bool },
}

/// Which of the create's reads an [`OnRead`] hook acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Watch {
    /// The `seggrp:<nonce>` read.
    Group,
    /// The `mpuctl` read.
    Ledger,
    /// The `bucket:<name>` read.
    Bucket,
}

impl Watch {
    fn matches(self, key: &[u8]) -> bool {
        match self {
            Self::Group => key.starts_with(metadata::SEGGRP_PREFIX),
            Self::Ledger => key == MPUCTL_KEY,
            Self::Bucket => key == metadata::bucket_key(BUCKET).as_slice(),
        }
    }
}

/// What an [`OnRead`] hook writes to the inner store, after its optional release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Meddle {
    /// Nothing more.
    Nothing,
    /// Admit this many other sessions, through real creates.
    Admit(usize),
    /// Delete the bucket marker.
    DeleteMarker,
    /// Rewrite `mpuctl` at its count under [`SKEWED`]'s profile.
    SkewLedger,
    /// Overwrite `mpuctl` with [`TORN_LEDGER`].
    TearLedger,
    /// Tear down the session of the create's latest commit ([`Scripted::fence`]).
    Fence,
}

/// A one-shot hook on the create's first read of `key` while a batch is held: release the held
/// batch first when `release` (it must land), then `meddle`, then answer the read from the inner
/// store. So whatever the hook does lands between two of one re-read's four gets.
#[derive(Debug, Clone, Copy)]
struct OnRead {
    key: Watch,
    release: bool,
    meddle: Meddle,
}

/// The profile [`Meddle::SkewLedger`] rewrites `mpuctl` under: one no leg's create runs with.
const SKEWED: u64 = 9;

/// `mpuctl` bytes that do not decode as an admission record.
const TORN_LEDGER: &[u8] = br#"{"count":1}"#;

/// A scripted failure of the create's reads.
#[derive(Debug, Clone, Copy)]
struct GetFault {
    /// Armed once the create under test has made this many commits.
    after_commits: usize,
    /// Only reads of an `mpu:` key count, and fail; otherwise every read does.
    sessions_only: bool,
    /// Counted reads let through first, once armed.
    skip: usize,
    /// Counted reads failed after that (`usize::MAX`: every one).
    fail: usize,
    /// A `TransientFault` rather than a plain error.
    transient: bool,
}

/// One commit the create under test made.
#[derive(Debug, Clone)]
struct Seen {
    session_key: Vec<u8>,
    /// The session record the batch puts: R, for its identity.
    session: Bytes,
    /// What the batch requires of `mpuctl` (`None`: `require_absent`).
    ledger: Option<Bytes>,
    /// The `mpuctl` value the batch puts.
    ledger_put: Bytes,
}

#[derive(Default)]
struct ScriptedState {
    plan: VecDeque<OnCommit>,
    /// What every commit past `plan` does (`Pass` when unset).
    then: Option<OnCommit>,
    /// Any commit carrying a put to this key answers `CommitUnknownResult { may_still_commit:
    /// false }` and is not applied.
    poisoned: Option<Vec<u8>>,
    get_fault: Option<GetFault>,
    held: Option<WriteBatch>,
    /// Acts once, on the create's first read of its key while a batch is held.
    on_read: Option<OnRead>,
    /// What the held batch met when it was released.
    late: Option<CommitOutcome>,
    /// The rival's session (C6): its upload id and the exact bytes its create wrote.
    rival: Option<(UploadId, Bytes)>,
    commits: Vec<Seen>,
    next_other: u128,
}

/// A store wrapper that acts on the create's commits (and, for F5, F8 and F9, its reads) as
/// scripted. Other creates it admits run directly on the inner store, so only the create under
/// test is scripted.
struct Scripted {
    inner: RedbMetadataStore,
    profile: Budget,
    state: Mutex<ScriptedState>,
}

impl Scripted {
    async fn new(profile: Budget, plan: &[OnCommit]) -> Self {
        Self {
            inner: store_with_bucket().await,
            profile,
            state: Mutex::new(ScriptedState {
                plan: plan.iter().copied().collect(),
                next_other: 1 << 100,
                ..ScriptedState::default()
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ScriptedState> {
        self.state.lock().unwrap()
    }

    /// Admit `n` other sessions through real creates on the inner store.
    async fn admit(&self, n: usize) {
        for _ in 0..n {
            let id = {
                let mut state = self.state();
                state.next_other += 1;
                state.next_other
            };
            let name = object(&format!("other-{id}"));
            let mut script = Script::repeating(vec![identity(id)], 0);
            let outcome = create_session(
                &self.inner,
                &self.profile,
                &request(&name, None),
                &mut script,
            )
            .await;
            created(&outcome);
        }
    }

    /// Tear the session at `session_key` down by the Abort fence's own route, as the reaper or
    /// the restore fence would: `Open@0` becomes `Aborting@1` ([`SessionRecord::open_teardown`]),
    /// CASed on its exact bytes, with its retirement obligation beside it under `require_absent`.
    async fn fence(&self, session_key: &[u8]) {
        let stored = self.inner.get(session_key).await.unwrap();
        let stored = stored.expect("a session to fence");
        let id = parse_mpu_key(session_key).unwrap();
        let teardown = decode_session_record(&stored)
            .unwrap()
            .open_teardown(&id)
            .expect("an Open session has a teardown");
        let obligation = teardown.obligation();
        let fence = WriteBatch::new()
            .require(session_key.to_vec(), stored)
            .require_absent(obligation.key())
            .put(session_key.to_vec(), metadata::encode(teardown.session()))
            .put(obligation.key(), metadata::encode(obligation.payload()));
        assert_eq!(
            self.inner.commit(fence).await.unwrap(),
            CommitOutcome::Committed
        );
    }

    /// Do what an [`OnRead`] hook asks, on the inner store.
    async fn meddle(&self, meddle: Meddle) {
        let write = match meddle {
            Meddle::Nothing => return,
            Meddle::Admit(others) => return self.admit(others).await,
            Meddle::Fence => {
                let latest = self
                    .state()
                    .commits
                    .last()
                    .map(|seen| seen.session_key.clone());
                return self.fence(&latest.expect("a commit to fence")).await;
            }
            Meddle::DeleteMarker => WriteBatch::new().delete(metadata::bucket_key(BUCKET)),
            Meddle::SkewLedger => {
                let count = ledger(&self.inner).await.map_or(0, |ledger| ledger.count());
                WriteBatch::new().put(MPUCTL_KEY, ledger_bytes(count, &profile_of(SKEWED)))
            }
            Meddle::TearLedger => WriteBatch::new().put(MPUCTL_KEY, TORN_LEDGER),
        };
        assert_eq!(
            self.inner.commit(write).await.unwrap(),
            CommitOutcome::Committed
        );
    }

    /// Apply the held batch now; what it met.
    async fn release_held(&self) -> CommitOutcome {
        let held = self.state().held.take().expect("a batch is held");
        let outcome = self.inner.commit(held).await.unwrap();
        self.state().late = Some(outcome);
        outcome
    }

    fn commits(&self) -> Vec<Seen> {
        self.state().commits.clone()
    }

    /// The scripted read fault for this read, if any.
    fn read_fault(&self, key: &[u8]) -> Option<BoxError> {
        let mut state = self.state();
        let commits = state.commits.len();
        let armed = state.get_fault.as_mut()?;
        if commits < armed.after_commits || (armed.sessions_only && !key.starts_with(MPU_PREFIX)) {
            return None;
        }
        if armed.skip > 0 {
            armed.skip -= 1;
            return None;
        }
        if armed.fail == 0 {
            return None;
        }
        armed.fail -= 1;
        Some(fault(armed.transient))
    }
}

#[async_trait]
impl MetadataStore for Scripted {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        if let Some(fault) = self.read_fault(key) {
            return Err(fault);
        }
        let trigger = {
            let mut state = self.state();
            match state.on_read {
                Some(hook) if hook.key.matches(key) && state.held.is_some() => state.on_read.take(),
                _ => None,
            }
        };
        if let Some(hook) = trigger {
            if hook.release {
                assert_eq!(self.release_held().await, CommitOutcome::Committed);
            }
            self.meddle(hook.meddle).await;
        }
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
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
        let session_key = session_key_of(&batch);
        let (act, poisoned, made) = {
            let mut state = self.state();
            state.commits.push(Seen {
                session_key: session_key.clone(),
                session: put_of(&batch, &session_key),
                ledger: ledger_precondition(&batch),
                ledger_put: put_of(&batch, MPUCTL_KEY),
            });
            let poisoned = state.poisoned.as_ref() == Some(&session_key);
            let act = state.plan.pop_front().or(state.then);
            (act.unwrap_or(OnCommit::Pass), poisoned, state.commits.len())
        };
        assert!(
            made <= RUNAWAY_COMMITS,
            "the create never stopped committing ({made} commits)"
        );
        if poisoned {
            return Err(unknown(false));
        }
        let bucket = metadata::bucket_key(BUCKET);
        match act {
            OnCommit::Pass => self.inner.commit(batch).await,
            OnCommit::AdmitFirst(others) => {
                self.admit(others).await;
                self.inner.commit(batch).await
            }
            OnCommit::DeleteMarkerFirst => {
                self.inner.commit(WriteBatch::new().delete(bucket)).await?;
                self.inner.commit(batch).await
            }
            OnCommit::RewriteMarkerFirst => {
                let rewrite = WriteBatch::new().put(bucket, RECREATED_MARKER);
                self.inner.commit(rewrite).await?;
                self.inner.commit(batch).await
            }
            OnCommit::PlantSessionFirst => {
                let plant = WriteBatch::new().put(session_key, interloper());
                self.inner.commit(plant).await?;
                self.inner.commit(batch).await
            }
            OnCommit::PlantGroupFirst => {
                let group = batch
                    .puts
                    .iter()
                    .map(|(key, _)| key)
                    .find(|key| key.starts_with(metadata::SEGGRP_PREFIX))
                    .expect("a create batch puts its seggrp: marker")
                    .clone();
                let plant = WriteBatch::new().put(group, metadata::SEGGRP_MARKER);
                self.inner.commit(plant).await?;
                self.inner.commit(batch).await
            }
            OnCommit::RivalFirst {
                may_still_commit,
                nonce,
                object: name,
            } => {
                let shared = parse_mpu_key(&session_key).unwrap();
                let mut script = Script::repeating(vec![(shared, identity(nonce).1)], 0);
                let rival = create_session(
                    &self.inner,
                    &self.profile,
                    &request(&object(name), None),
                    &mut script,
                )
                .await;
                let rival = created(&rival);
                let bytes = self.inner.get(&session_key).await?.expect("the rival's");
                let mut state = self.state();
                state.rival = Some((rival, bytes));
                if may_still_commit {
                    state.held = Some(batch);
                }
                Err(unknown(may_still_commit))
            }
            OnCommit::ConflictUnmoved => {
                // A real store yields here; so must this one, or no timeout could fire.
                tokio::task::yield_now().await;
                Ok(CommitOutcome::Conflict)
            }
            OnCommit::Fail => Err(fault(false)),
            OnCommit::Unknown {
                applied,
                may_still_commit,
            } => {
                if applied {
                    assert_eq!(self.inner.commit(batch).await?, CommitOutcome::Committed);
                }
                Err(unknown(may_still_commit))
            }
            OnCommit::Hold => {
                self.state().held = Some(batch);
                Err(unknown(true))
            }
            OnCommit::UnknownThenAdmit {
                may_still_commit,
                admit,
            } => {
                self.admit(admit).await;
                Err(unknown(may_still_commit))
            }
            OnCommit::UnknownThenFence { may_still_commit } => {
                assert_eq!(self.inner.commit(batch).await?, CommitOutcome::Committed);
                self.fence(&session_key).await;
                Err(unknown(may_still_commit))
            }
        }
    }
}

/// Run one create through `store` as `name`, with identity source `script`.
async fn create_through(
    store: &impl MetadataStore,
    profile: &Budget,
    name: &str,
    script: &mut Script,
) -> Result<CreateOutcome, CreateError> {
    create_session(store, profile, &request(name, None), script).await
}

/// The store's records, minus the bucket marker the wrapper itself moved.
async fn snapshot_without_marker(store: &impl MetadataStore) -> Vec<(Vec<u8>, Bytes)> {
    let bucket = metadata::bucket_key(BUCKET);
    let mut all = snapshot(store).await;
    all.retain(|(key, _)| *key != bucket);
    all
}

// ──────────────────── (B) the real bounds, writing nothing ────────────────────

/// **(B)** A ledger at its stored `max_sessions` answers `SlowDown { SessionCap }`, naming the
/// ledger's own numbers, and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_full_ledger_answers_session_cap_and_writes_nothing() {
    within(async {
        let store = store_with_bucket().await;
        let profile = profile_of(3);
        let full = ledger_bytes(3, &profile);
        store
            .commit(WriteBatch::new().put(MPUCTL_KEY, full))
            .await
            .unwrap();
        let before = snapshot(&store).await;
        let mut script = Script::fresh(1);
        let outcome = create_through(&store, &profile, &object("full"), &mut script).await;
        let full = Backpressure::SessionCap {
            count: 3,
            max_sessions: 3,
        };
        assert_eq!(
            outcome.unwrap(),
            CreateOutcome::Refused(Refusal::SlowDown { pressure: full })
        );
        assert_eq!(snapshot(&store).await, before, "a refusal writes nothing");
    })
    .await;
}

/// **(B)** A create whose own profile differs from the one stored in `mpuctl` answers
/// `ProfileSkew`, naming both, and writes nothing — even with room in the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_profile_skew_is_refused_and_writes_nothing() {
    within(async {
        let store = store_with_bucket().await;
        let stored = profile_of(3);
        store
            .commit(WriteBatch::new().put(MPUCTL_KEY, ledger_bytes(1, &stored)))
            .await
            .unwrap();
        let before = snapshot(&store).await;
        let local = profile_of(4);
        let mut script = Script::fresh(1);
        let outcome = create_through(&store, &local, &object("skew"), &mut script).await;
        assert_eq!(
            outcome.unwrap(),
            CreateOutcome::Refused(Refusal::ProfileSkew {
                local,
                ledger: stored
            })
        );
        assert_eq!(snapshot(&store).await, before);
    })
    .await;
}

/// **(B)** A bucket with no `bucket:` record answers `NoSuchBucket` and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_missing_bucket_answers_no_such_bucket_and_writes_nothing() {
    within(async {
        let store = RedbMetadataStore::in_memory().unwrap();
        let before = snapshot(&store).await;
        let mut script = Script::fresh(1);
        let outcome = create_through(&store, &shipped(), &object("nobucket"), &mut script).await;
        assert_eq!(
            outcome.unwrap(),
            CreateOutcome::Refused(Refusal::NoSuchBucket)
        );
        assert_eq!(snapshot(&store).await, before);
    })
    .await;
}

/// **(B), the read order: bucket, then ledger.** With no `bucket:` record **and** `mpuctl` at its
/// stored `max_sessions` (a profile admitting one session, the ledger at count 1), the create
/// answers `NoSuchBucket`, not `SlowDown { SessionCap { count: 1, max_sessions: 1 } }`: the bucket
/// is judged before the ledger, so a create for a bucket that does not exist is never told to
/// slow down. It asks for no wait and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_missing_bucket_is_judged_before_a_full_ledger() {
    within(async {
        let store = RedbMetadataStore::in_memory().unwrap();
        let profile = profile_of(1);
        store
            .commit(WriteBatch::new().put(MPUCTL_KEY, ledger_bytes(1, &profile)))
            .await
            .unwrap();
        let full = ledger(&store).await.expect("the ledger is stored");
        assert_eq!(
            (full.count(), full.max_sessions()),
            (1, 1),
            "the ledger is at its stored max_sessions"
        );
        assert!(
            store
                .get(&metadata::bucket_key(BUCKET))
                .await
                .unwrap()
                .is_none(),
            "no bucket marker"
        );
        let before = snapshot(&store).await;
        let mut script = Script::fresh(1);
        let outcome = create_through(&store, &profile, &object("nobucket"), &mut script).await;
        assert_eq!(
            outcome.unwrap(),
            CreateOutcome::Refused(Refusal::NoSuchBucket),
            "the bucket is judged before the ledger"
        );
        assert!(
            script.waits.is_empty(),
            "a refusal from the read asks for no wait: {:?}",
            script.waits
        );
        assert_eq!(snapshot(&store).await, before, "a refusal writes nothing");
    })
    .await;
}

/// **(B)** A bucket marker deleted, or rewritten, between the create's read and its commit (the
/// wrapper acts at commit) answers `NoSuchBucket`. The create writes nothing: no `mpu:`, no
/// `seggrp:`, no `mpuctl`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_bucket_marker_moved_under_the_commit_answers_no_such_bucket() {
    within(async {
        for act in [OnCommit::DeleteMarkerFirst, OnCommit::RewriteMarkerFirst] {
            let store = Scripted::new(shipped(), &[act]).await;
            let before = snapshot_without_marker(&store.inner).await;
            let mut script = Script::fresh(1);
            let outcome = create_through(&store, &shipped(), &object("moved"), &mut script).await;
            assert_eq!(
                outcome.unwrap(),
                CreateOutcome::Refused(Refusal::NoSuchBucket),
                "{act:?}"
            );
            assert_eq!(
                store.commits().len(),
                1,
                "{act:?}: one commit, then the refusal"
            );
            assert!(
                script.waits.is_empty(),
                "{act:?}: the bucket is judged before contention, so no wait: {:?}",
                script.waits
            );
            assert_eq!(
                snapshot_without_marker(&store.inner).await,
                before,
                "{act:?}"
            );
        }
    })
    .await;
}

/// **(B), the read order: bucket, then ledger, then contention.** On a profile admitting one
/// session, with no ledger yet, another create fills the ledger inside this create's first commit,
/// so that commit loses its `require_absent(mpuctl)`. The re-read finds the ledger full, and the
/// create answers `SlowDown { SessionCap { count: 1, max_sessions: 1 } }` from that read, before
/// it treats the lost commit as contention: it asks for no wait and spends no unit (a contention
/// refusal here would read `AdmissionContention { attempts: 1 }`). It writes nothing of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_ledger_filled_under_the_commit_is_judged_before_contention() {
    within(async {
        let profile = profile_of(1);
        let store = Scripted::new(profile, &[OnCommit::AdmitFirst(1)]).await;
        assert!(
            ledger(&store.inner).await.is_none(),
            "no ledger at the start"
        );
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("late"), &mut script).await;
        let full = Backpressure::SessionCap {
            count: 1,
            max_sessions: 1,
        };
        assert_eq!(
            outcome.unwrap(),
            CreateOutcome::Refused(Refusal::SlowDown { pressure: full })
        );
        assert!(
            script.waits.is_empty(),
            "the ledger is judged before contention, so no wait: {:?}",
            script.waits
        );
        assert_eq!(store.commits().len(), 1);
        assert_eq!(script.minted.len(), 1);
        let all = population(&store.inner).await;
        assert_eq!(all.len(), 1, "only the other create's session");
        assert!(!all.contains_key(script.minted[0].0.as_str()));
        assert_population(&store.inner, &all).await;
    })
    .await;
}

/// **(B), row 9.** A stored `mpuctl` that does not decode as an admission record ends
/// `CreateError::Ledger`, a terminal error naming the torn ledger, and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_a_torn_ledger_ends_in_a_ledger_error_and_writes_nothing() {
    within(async {
        let store = store_with_bucket().await;
        store
            .commit(WriteBatch::new().put(MPUCTL_KEY, TORN_LEDGER))
            .await
            .unwrap();
        let before = snapshot(&store).await;
        let mut script = Script::fresh(1);
        let err = create_through(&store, &shipped(), &object("torn"), &mut script)
            .await
            .expect_err("a torn ledger admits nothing");
        assert!(matches!(err, CreateError::Ledger(_)), "{err:?}");
        let text = err.to_string();
        assert!(text.contains("the mpuctl ledger is torn"), "{text}");
        assert_eq!(classify(&err), ErrorClass::Terminal);
        assert_eq!(snapshot(&store).await, before);
    })
    .await;
}

/// **(B), rows 7 and 10: contention spent with nothing moved.** On the shipped profile the
/// wrapper answers every commit `Ok(Conflict)` without applying it and without changing any
/// record, as TiKV does when a locking read meets another writer's lock
/// (`crates/metadata-tikv/src/lib.rs:1386-1393`, `:928-939`). The create re-reads, finds every
/// precondition still holding, and spends one unit of contention per commit — never an identity.
/// It makes exactly `max_sessions` commits, all under the one identity it minted, and refuses
/// `SlowDown { AdmissionContention }` naming that number. It writes nothing; `mpuctl` is
/// byte-identical. This is the bounded refusal on a ledger with room the narrowed guarantee
/// states.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_contention_spent_with_nothing_moved_refuses_after_the_whole_budget() {
    within(async {
        let profile = shipped();
        let store = Scripted::new(profile, &[]).await;
        store.state().then = Some(OnCommit::ConflictUnmoved);
        let room = ledger_bytes(1, &profile);
        store
            .inner
            .commit(WriteBatch::new().put(MPUCTL_KEY, room.clone()))
            .await
            .unwrap();
        let before = snapshot(&store.inner).await;
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("locked"), &mut script).await;
        let budget = profile.max_sessions() as u32;
        assert_eq!(budget, MAX_ADMISSION_CAS_ATTEMPTS);
        assert_eq!(
            outcome.unwrap(),
            CreateOutcome::Refused(Refusal::SlowDown {
                pressure: Backpressure::AdmissionContention { attempts: budget }
            })
        );
        assert_eq!(
            script.minted.len(),
            1,
            "a Conflict that moved nothing never re-mints"
        );
        let commits = store.commits();
        assert_eq!(
            commits.len(),
            budget as usize,
            "one commit per unit of the contention budget"
        );
        let minted = mpu_key(&script.minted[0].0);
        assert!(commits.iter().all(|seen| seen.session_key == minted));
        let asked: Vec<u64> = (0..budget - 1)
            .map(|k| admission_backoff_millis(k, script.jitter))
            .collect();
        assert_eq!(script.waits, asked);
        assert_eq!(snapshot(&store.inner).await, before, "it writes nothing");
        assert_eq!(store.inner.get(MPUCTL_KEY).await.unwrap(), Some(room));
    })
    .await;
}

/// A store wrapper that enforces the value ceiling every backend inherits, as FoundationDB does:
/// a batch putting a value over [`MAX_VALUE_BYTES`] fails, whole. It counts every read and commit.
struct Ceiling {
    inner: RedbMetadataStore,
    reads: AtomicUsize,
    commits: AtomicUsize,
}

#[async_trait]
impl MetadataStore for Ceiling {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<ScanPage> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if let Some((key, value)) = batch.puts.iter().find(|(_, v)| v.len() > MAX_VALUE_BYTES) {
            return Err(BoxError::from(format!(
                "value too large: {} bytes under {} (limit {MAX_VALUE_BYTES})",
                value.len(),
                String::from_utf8_lossy(key)
            )));
        }
        self.inner.commit(batch).await
    }
}

/// The reachable states, in the order [`widest_states`] spells them.
const STATES: [&str; 4] = ["Open", "Aborting", "Completing", "Completed"];

/// This test's own spelling of every state a session created from `request` with `nonce` can
/// reach, every number at the widest the decoder allows, in [`STATES`] order. Each is decoded by
/// the production decoder, which proves it a legal, canonical record of that session.
fn widest_states(request: &CreateRequest<'_>, nonce: &SegmentNonce) -> Vec<Vec<u8>> {
    let max = u64::MAX;
    let object = json_string(request.object);
    let content_type = request
        .content_type
        .map(|ct| format!("\"content_type\":{},", json_string(ct)))
        .unwrap_or_default();
    let digest = "f".repeat(64);
    let states = [
        r#"{"kind":"Open"}"#.to_owned(),
        r#"{"kind":"Aborting"}"#.to_owned(),
        format!(
            "{{\"kind\":\"Completing\",\"fenced_at_millis\":{max},\"segments_written\":{},\
             \"publish_target\":{{\"parent\":{},\"name\":{object},\"epoch\":{max}}}}}",
            MAX_SEGMENT_INDEX + 1,
            request.parent,
        ),
        format!(
            "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":{max},\"version\":{max},\
             \"etag\":\"{digest}-{MAX_PART_NUMBER}\",\"completed_at_millis\":{max},\
             \"complete_fingerprint\":\"{digest}\"}}}}"
        ),
    ];
    states
        .iter()
        .map(|state| {
            let bytes = format!(
                "{{\"parent\":{},\"object\":{object},{content_type}\"created_at_millis\":{},\
                 \"clock_source\":{},\"segment_nonce\":\"{nonce}\",\"epoch\":{max},\
                 \"attempts\":{},\"state\":{state}}}",
                request.parent,
                request.created_at_millis,
                json_string(request.clock_source),
                u32::MAX,
            )
            .into_bytes();
            let record = decode_session_record(&bytes).expect("a state the decoder accepts");
            assert_eq!(
                metadata::encode(&record),
                bytes,
                "the encoder's own spelling"
            );
            bytes
        })
        .collect()
}

/// The widest of `request`'s reachable states, which must be **strictly** `state` (an index
/// into [`STATES`]): the fixture cannot drift into the other regime without failing here.
fn widest_len(request: &CreateRequest<'_>, nonce: &SegmentNonce, state: usize) -> usize {
    let lengths: Vec<usize> = widest_states(request, nonce).iter().map(Vec::len).collect();
    for (other, len) in lengths.iter().enumerate() {
        assert!(
            other == state || *len < lengths[state],
            "{} must be strictly the widest state, but {} is {len} bytes to its {}",
            STATES[state],
            STATES[other],
            lengths[state]
        );
    }
    lengths[state]
}

/// The three size points for an object `name` whose widest reachable state is `STATES[state]`:
/// padded with `content_type` (which every state carries once) to one byte under the ceiling,
/// exactly at it, and one byte over. The first two are admitted, and can be torn down and moved
/// through every widest state through a store that enforces the ceiling; the third is refused
/// before any read or commit, naming the size and the ceiling, and writes nothing.
async fn size_points(name: &str, state: usize) {
    let (_, nonce) = identity(7);
    let bare = widest_len(&request(name, Some("")), &nonce, state);
    assert!(bare < MAX_VALUE_BYTES);
    let at_ceiling = MAX_VALUE_BYTES - bare;
    for (pad, widest) in [
        (at_ceiling - 1, MAX_VALUE_BYTES - 1),
        (at_ceiling, MAX_VALUE_BYTES),
        (at_ceiling + 1, MAX_VALUE_BYTES + 1),
    ] {
        let content_type = "a".repeat(pad);
        let asked = request(name, Some(&content_type));
        assert_eq!(
            widest_len(&asked, &nonce, state),
            widest,
            "the probe is where it claims"
        );
        let store = Ceiling {
            inner: store_with_bucket().await,
            reads: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
        };
        let before = snapshot(&store.inner).await;
        let mut script = Script::repeating(vec![identity(7)], 100);
        let outcome = create_session(&store, &shipped(), &asked, &mut script).await;
        if widest > MAX_VALUE_BYTES {
            assert_eq!(
                outcome.unwrap(),
                CreateOutcome::Refused(Refusal::SessionTooLarge {
                    bytes: widest as u64,
                    limit: MAX_VALUE_BYTES as u64,
                }),
                "{} widest, one byte over",
                STATES[state]
            );
            assert_eq!(
                store.reads.load(Ordering::SeqCst),
                0,
                "refused before any read"
            );
            assert_eq!(store.commits.load(Ordering::SeqCst), 0, "or any commit");
            assert_eq!(
                snapshot(&store.inner).await,
                before,
                "and it writes nothing"
            );
            continue;
        }
        let id = created(&outcome);
        let key = mpu_key(&id);
        let stored = store.inner.get(&key).await.unwrap().unwrap();
        let open = decode_session_record(&stored).unwrap();
        // Torn down by the Abort fence's own route, through the ceiling.
        let teardown = open
            .open_teardown(&id)
            .expect("an Open session has a teardown");
        let obligation = teardown.obligation();
        let payload = metadata::encode(obligation.payload());
        decode_retire_obligation(&obligation.key(), &payload).unwrap();
        let fence = WriteBatch::new()
            .require(key.clone(), stored.clone())
            .require_absent(obligation.key())
            .put(key.clone(), metadata::encode(teardown.session()))
            .put(obligation.key(), payload);
        assert_eq!(
            store.commit(fence).await.unwrap(),
            CommitOutcome::Committed,
            "a session admitted {} byte(s) under the ceiling can be torn down",
            MAX_VALUE_BYTES - widest
        );
        // And every state it can reach, at its widest, is a value the ceiling accepts.
        let mut current = store.inner.get(&key).await.unwrap().unwrap();
        for state in widest_states(&asked, &nonce) {
            let next = Bytes::from(state);
            let write = WriteBatch::new()
                .require(key.clone(), current.clone())
                .put(key.clone(), next.clone());
            assert_eq!(store.commit(write).await.unwrap(), CommitOutcome::Committed);
            current = next;
        }
    }
}

/// **(B) Size headroom, `Completing` widest.** The boundary is found on **encoded bytes**: the
/// object name is made of characters JSON escapes six bytes wide, so its character count is a
/// sixth of what it costs, and `Completing` carries it twice (as `object` and as
/// `publish_target.name`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_the_size_headroom_is_measured_on_encoded_bytes_completing_widest() {
    within(async {
        let name = format!("{BUCKET}/{}", "\u{1}".repeat(8_000));
        assert!(
            name.len() * 2 < MAX_VALUE_BYTES / 4,
            "a check on character length would see room to spare"
        );
        size_points(&name, 2).await;
    })
    .await;
}

/// **(B) Size headroom, `Completed` widest.** A short object name, padded to the boundary with
/// `content_type`: `Completed`'s fixed payload (two digests, an ETag and three `u64`s) is then
/// wider than `Completing`'s, so a measure that left `Completed` out would admit one byte over.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn b_the_size_headroom_is_measured_on_encoded_bytes_completed_widest() {
    within(async {
        size_points(&object("k"), 3).await;
    })
    .await;
}

// ───────────── (C) two retry budgets, and no adopted session ─────────────

/// **C1** When the first minted identity collides — its upload id with an existing `mpu:`
/// record, or its nonce with an existing `seggrp:` marker — the create re-mints and succeeds,
/// leaving the existing session as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c1_a_colliding_identity_is_re_minted() {
    within(async {
        let profile = shipped();
        for (label, repeat) in [("upload id", false), ("nonce", true)] {
            let store = store_with_bucket().await;
            let mut first = Script::repeating(vec![identity(1)], 0);
            let existing =
                created(&create_through(&store, &profile, &object("a"), &mut first).await);
            let planted = store.get(&mpu_key(&existing)).await.unwrap().unwrap();
            let colliding = if repeat {
                // A fresh upload id that brings the existing session's nonce with it.
                (identity(2).0, identity(1).1)
            } else {
                // The existing upload id with a fresh nonce.
                (identity(1).0, identity(3).1)
            };
            let mut script = Script::repeating(vec![colliding.clone(), identity(4)], 0);
            let id = created(&create_through(&store, &profile, &object("b"), &mut script).await);
            assert_eq!(id, identity(4).0, "{label}: re-minted after the collision");
            assert_eq!(script.minted, vec![colliding, identity(4)], "{label}");
            assert_eq!(
                store.get(&mpu_key(&existing)).await.unwrap().unwrap(),
                planted,
                "{label}: the existing session is untouched"
            );
            let expected = BTreeMap::from([
                (existing.as_str().to_owned(), object("a")),
                (id.as_str().to_owned(), object("b")),
            ]);
            assert_population(&store, &expected).await;
        }
    })
    .await;
}

/// **C1** When every one of `MAX_UPLOAD_ID_ATTEMPTS` minted identities collides, the create ends
/// with a typed error and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c1_every_identity_colliding_ends_in_a_typed_error_writing_nothing() {
    within(async {
        let store = store_with_bucket().await;
        let profile = shipped();
        let mut first = Script::repeating(vec![identity(1)], 0);
        created(&create_through(&store, &profile, &object("a"), &mut first).await);
        let before = snapshot(&store).await;
        let repeats = vec![identity(1); MAX_UPLOAD_ID_ATTEMPTS as usize + 1];
        let mut script = Script::repeating(repeats, 0);
        let err = create_through(&store, &profile, &object("b"), &mut script)
            .await
            .expect_err("every identity collided");
        let CreateError::IdentityCollisions { attempts } = &err else {
            panic!("expected IdentityCollisions, got {err:?}");
        };
        assert_eq!(*attempts, MAX_UPLOAD_ID_ATTEMPTS);
        let text = err.to_string();
        assert!(
            text.contains(&format!(
                "all {MAX_UPLOAD_ID_ATTEMPTS} minted session identities"
            )) && text.contains("nothing was written"),
            "{text}"
        );
        assert_eq!(script.minted.len(), MAX_UPLOAD_ID_ATTEMPTS as usize);
        assert_eq!(snapshot(&store).await, before);
    })
    .await;
}

/// **C2** The two budgets never share.
///
/// * A create that loses the `mpuctl` CAS three times — more than `MAX_UPLOAD_ID_ATTEMPTS` — is
///   admitted with its first identity.
/// * Under a profile whose contention budget is a single attempt, a collision met on a read, or
///   arriving as a lost commit, re-mints without spending it.
/// * One lost CAS and then one collision (two events, as many as `MAX_UPLOAD_ID_ATTEMPTS`) still
///   admit: the loss spent no identity.
/// * One collision and then one lost CAS under a two-attempt contention budget still admit: the
///   collision spent no contention.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c2_the_contention_and_collision_budgets_never_share() {
    within(async {
        // Lost CASes spend no identity.
        let losses = MAX_UPLOAD_ID_ATTEMPTS as usize + 1;
        let plan = vec![OnCommit::AdmitFirst(1); losses];
        let store = Scripted::new(shipped(), &plan).await;
        let mut script = Script::fresh(0);
        let id = created(&create_through(&store, &shipped(), &object("cas"), &mut script).await);
        assert_eq!(script.minted.len(), 1, "a lost CAS never re-mints");
        assert_eq!(store.commits().len(), losses + 1);
        assert_eq!(script.waits.len(), losses);
        assert_eq!(
            ledger(&store.inner).await.unwrap().count(),
            losses as u64 + 1
        );
        assert!(store.inner.get(&mpu_key(&id)).await.unwrap().is_some());

        // A collision spends no contention budget: profile_of(1) allows a single attempt.
        let tight = profile_of(1);
        assert_eq!(tight.admission_cas_attempts(), 1);
        for (label, plan) in [
            ("on a read", vec![]),
            ("as a lost commit", vec![OnCommit::PlantSessionFirst]),
        ] {
            let store = Scripted::new(tight, &plan).await;
            if plan.is_empty() {
                // A record already under the first identity's upload id, outside any ledger.
                let planted = WriteBatch::new().put(mpu_key(&identity(1).0), interloper());
                store.inner.commit(planted).await.unwrap();
            }
            let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
            let outcome = create_through(&store, &tight, &object("tight"), &mut script).await;
            assert_eq!(created(&outcome), identity(2).0, "{label}");
            assert!(
                script.waits.is_empty(),
                "{label}: a collision is not a lost CAS"
            );
            assert_eq!(ledger(&store.inner).await.unwrap().count(), 1, "{label}");
        }

        // A lost CAS, then a collision: neither budget is spent past its own count.
        let plan = [OnCommit::AdmitFirst(1), OnCommit::PlantSessionFirst];
        let store = Scripted::new(shipped(), &plan).await;
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome =
            create_through(&store, &shipped(), &object("loss-then-hit"), &mut script).await;
        assert_eq!(
            created(&outcome),
            identity(2).0,
            "one loss and one collision spend one unit of each budget"
        );
        assert_eq!(
            script.waits.len(),
            1,
            "the lost CAS, and only it, backed off"
        );
        assert_eq!(store.commits().len(), 3);

        // A collision, then a lost CAS, on a two-attempt contention budget.
        let two = profile_of(2);
        let store = Scripted::new(two, &[OnCommit::AdmitFirst(1)]).await;
        let planted = WriteBatch::new().put(mpu_key(&identity(1).0), interloper());
        store.inner.commit(planted).await.unwrap();
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_through(&store, &two, &object("hit-then-loss"), &mut script).await;
        assert_eq!(
            created(&outcome),
            identity(2).0,
            "one collision and one loss spend one unit of each budget"
        );
        assert_eq!(script.waits.len(), 1);
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 2);
    })
    .await;
}

/// **C3, the id taken under the commit.** An interloper writes `mpu:<id>` — another nonce,
/// another object — between the create's read and its commit, without moving `mpuctl`. The
/// commit loses its `require_absent(mpu:<id>)`; the re-read finds the identity taken with no
/// unknown outcome of the create's own, which is a collision. The create answers `Created` under
/// a **new** identity, and the interloper's record is byte-identical afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c3_an_id_taken_under_the_commit_is_re_minted_and_left_as_it_is() {
    within(async {
        let profile = shipped();
        let store = Scripted::new(profile, &[OnCommit::PlantSessionFirst]).await;
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_through(&store, &profile, &object("c3"), &mut script).await;
        assert_eq!(created(&outcome), identity(2).0, "re-minted");
        assert_eq!(script.minted, vec![identity(1), identity(2)]);
        assert!(script.waits.is_empty(), "a collision is not contention");
        assert_eq!(
            store.inner.get(&mpu_key(&identity(1).0)).await.unwrap(),
            Some(interloper()),
            "the interloper's record is byte-identical afterwards"
        );
        let ours = &sessions(&store.inner).await[identity(2).0.as_str()];
        assert_eq!(ours.object(), object("c3"));
        assert_eq!(ours.segment_nonce(), &identity(2).1);
        assert_eq!(
            markers(&store.inner).await,
            BTreeSet::from([metadata::seggrp_key(&identity(2).1)])
        );
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 1);
    })
    .await;
}

/// **C4, the nonce taken under the commit.** An interloper writes `seggrp:<nonce>` only,
/// between the create's read and its commit. The create answers `Created` under a new identity:
/// its session's nonce is not the interloper's. (The marker's value is a constant, so only the
/// identity the create answers under can tell a re-mint from an overwrite.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c4_a_nonce_taken_under_the_commit_is_re_minted() {
    within(async {
        let profile = shipped();
        let store = Scripted::new(profile, &[OnCommit::PlantGroupFirst]).await;
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_through(&store, &profile, &object("c4"), &mut script).await;
        assert_eq!(created(&outcome), identity(2).0, "re-minted");
        assert!(store
            .inner
            .get(&mpu_key(&identity(1).0))
            .await
            .unwrap()
            .is_none());
        let ours = &sessions(&store.inner).await[identity(2).0.as_str()];
        assert_eq!(ours.segment_nonce(), &identity(2).1);
        assert_ne!(
            ours.segment_nonce(),
            &identity(1).1,
            "not the interloper's nonce"
        );
        assert_eq!(
            markers(&store.inner).await,
            BTreeSet::from([
                metadata::seggrp_key(&identity(1).1),
                metadata::seggrp_key(&identity(2).1),
            ])
        );
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 1);
    })
    .await;
}

/// **C5, a byte-identical record is never adopted.** The identity source repeats an **existing**
/// session's identity, and the planted record is exactly R — written before the create's first
/// read, with the same upload id, nonce, bucket, object, content type, `created_at_millis` and
/// `clock_source` — so no field comparison can tell them apart. The wrapper makes any commit for
/// that identity return `CommitUnknownResult { may_still_commit: false }` unapplied. The create
/// answers `Created` under a **different** upload id, the planted record is byte-identical
/// afterwards, and `mpuctl.count()` went up by exactly one. A create that skipped the absence
/// read would commit, meet the unknown outcome, re-read, and adopt the planted session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c5_a_byte_identical_record_is_never_adopted() {
    within(async {
        let profile = shipped();
        let store = Scripted::new(profile, &[]).await;
        let name = object("same-request");
        let mut first = Script::repeating(vec![identity(1)], 0);
        let planted_id = created(
            &create_session(
                &store.inner,
                &profile,
                &request(&name, Some("text/plain")),
                &mut first,
            )
            .await,
        );
        let planted_key = mpu_key(&planted_id);
        let planted = store.inner.get(&planted_key).await.unwrap().unwrap();
        store.state().poisoned = Some(planted_key.clone());
        let before = ledger(&store.inner).await.unwrap().count();

        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_session(
            &store,
            &profile,
            &request(&name, Some("text/plain")),
            &mut script,
        )
        .await;
        let id = created(&outcome);
        assert_ne!(
            id, planted_id,
            "the existing session is never answered as Created"
        );
        assert_eq!(id, identity(2).0);
        assert!(
            store
                .commits()
                .iter()
                .all(|seen| seen.session_key != planted_key),
            "no batch is ever built under an identity read present"
        );
        assert_eq!(
            store.inner.get(&planted_key).await.unwrap().unwrap(),
            planted
        );
        assert_eq!(ledger(&store.inner).await.unwrap().count(), before + 1);
    })
    .await;
}

/// **C5 on a re-minted identity.** The absence read guards every identity the create mints, not
/// only its first: what the create knew about its commits under one identity is dropped when it
/// re-mints. Identity 5's session is planted first, by a real create for the same request, so it
/// is exactly the R this create would build under identity 5. The create's first identity meets
/// C6 (i)'s schedule: a real rival create takes its upload id, and its commit returns
/// `CommitUnknownResult { may_still_commit: false }` unapplied. The re-read finds the rival's
/// record, not R, with nothing held: a collision, so the create re-mints, and the identity source
/// hands it identity 5. That read finds R already there before any commit of the create's own
/// under identity 5: a collision too, never its session. With `MAX_UPLOAD_ID_ATTEMPTS` spent, the
/// create ends `IdentityCollisions`; the planted and the rival sessions are byte-identical
/// afterwards, and the create wrote nothing of its own. A create that carried the first
/// identity's unknown outcome over to identity 5 would answer `Created` for the planted session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c5_a_byte_identical_record_is_never_adopted_after_a_re_mint() {
    within(async {
        let profile = shipped();
        let plan = [OnCommit::RivalFirst {
            may_still_commit: false,
            nonce: RIVAL,
            object: "rival",
        }];
        let store = Scripted::new(profile, &plan).await;
        let name = object("same-request");
        let mut first = Script::repeating(vec![identity(5)], 0);
        let planted_id = created(&create_through(&store.inner, &profile, &name, &mut first).await);
        assert_eq!(planted_id, identity(5).0);
        let planted = store.inner.get(&mpu_key(&planted_id)).await.unwrap();
        let before = ledger(&store.inner).await.unwrap().count();

        let mut script = Script::repeating(vec![identity(1), identity(5), identity(6)], 0);
        let err = create_through(&store, &profile, &name, &mut script)
            .await
            .expect_err("both identities collided");
        assert!(
            matches!(
                err,
                CreateError::IdentityCollisions { attempts } if attempts == MAX_UPLOAD_ID_ATTEMPTS
            ),
            "the planted session is never answered as Created: {err:?}"
        );
        assert_eq!(script.minted, vec![identity(1), identity(5)]);
        let commits = store.commits();
        assert_eq!(commits.len(), 1, "no batch is built under identity 5");
        assert_eq!(commits[0].session_key, mpu_key(&identity(1).0));
        assert_eq!(
            store.inner.get(&mpu_key(&planted_id)).await.unwrap(),
            planted,
            "the planted session is byte-identical afterwards"
        );
        let (rival, rival_bytes) = store.state().rival.clone().expect("the rival ran");
        assert_eq!(rival, identity(1).0);
        assert_eq!(
            store.inner.get(&mpu_key(&rival)).await.unwrap(),
            Some(rival_bytes)
        );
        assert_eq!(
            ledger(&store.inner).await.unwrap().count(),
            before + 1,
            "the rival's admission, and no other"
        );
        let expected = BTreeMap::from([
            (planted_id.as_str().to_owned(), name.clone()),
            (rival.as_str().to_owned(), object("rival")),
        ]);
        assert_population(&store.inner, &expected).await;
    })
    .await;
}

/// **C6 (i), another session under this create's upload id is never adopted.** The create reads
/// its identity absent; then, before its commit returns, a **real** second create — the same
/// function, minted with the **same upload id**, another nonce and another object — installs its
/// session, and the first create's commit returns `CommitUnknownResult { may_still_commit:
/// false }` unapplied. The re-read finds a record under the upload id that is not R, with nothing
/// held: a collision. The first create answers `Created` under a **new** identity, never the
/// shared id; both sessions exist, the second's record is byte-identical to what the second
/// create wrote, and `mpuctl.count()` is up by two.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c6_i_another_session_under_the_upload_id_is_never_adopted() {
    within(async {
        let profile = shipped();
        let plan = [OnCommit::RivalFirst {
            may_still_commit: false,
            nonce: RIVAL,
            object: "rival",
        }];
        let store = Scripted::new(profile, &plan).await;
        store.admit(1).await;
        let c = ledger(&store.inner).await.unwrap().count();
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_through(&store, &profile, &object("first"), &mut script).await;
        let id = created(&outcome);
        assert_ne!(
            id,
            identity(1).0,
            "never Created under the shared upload id"
        );
        assert_eq!(id, identity(2).0);
        let (rival, rival_bytes) = store.state().rival.clone().expect("the rival ran");
        assert_eq!(rival, identity(1).0, "the rival took the shared upload id");
        assert_eq!(
            store.inner.get(&mpu_key(&rival)).await.unwrap(),
            Some(rival_bytes),
            "the second create's record is byte-identical to what it wrote"
        );
        let all = sessions(&store.inner).await;
        assert_eq!(all[rival.as_str()].object(), object("rival"));
        assert_eq!(all[rival.as_str()].segment_nonce(), &identity(RIVAL).1);
        assert_eq!(all[id.as_str()].object(), object("first"));
        assert_eq!(ledger(&store.inner).await.unwrap().count(), c + 2);
        assert_population(&store.inner, &population(&store.inner).await).await;
    })
    .await;
}

/// **C6 (ii), the same, while held.** The first create's commit returns `CommitUnknownResult {
/// may_still_commit: true }` and is held. A batch of its own may still land, so it must neither
/// adopt the rival's session nor mint a new identity: it ends `InDoubt { may_still_commit: true
/// }` naming **its own** upload id and nonce. Released afterwards, the held batch answers
/// `Conflict`, and the store holds the second create's session only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c6_ii_while_held_another_session_under_the_upload_id_ends_in_doubt() {
    within(async {
        let profile = shipped();
        let plan = [OnCommit::RivalFirst {
            may_still_commit: true,
            nonce: RIVAL,
            object: "rival",
        }];
        let store = Scripted::new(profile, &plan).await;
        store.admit(1).await;
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_through(&store, &profile, &object("first"), &mut script).await;
        let doubt = in_doubt(outcome, &identity(1), true);
        assert!(
            matches!(doubt.cause, Unsettled::IdentityTaken),
            "{:?}",
            doubt.cause
        );
        assert_eq!(
            script.minted,
            vec![identity(1)],
            "no second identity minted"
        );
        let (rival, rival_bytes) = store.state().rival.clone().expect("the rival ran");
        assert_eq!(
            store.release_held().await,
            CommitOutcome::Conflict,
            "the held batch loses to the rival's session"
        );
        assert_eq!(
            store.inner.get(&mpu_key(&rival)).await.unwrap(),
            Some(rival_bytes)
        );
        let all = population(&store.inner).await;
        assert_eq!(all[rival.as_str()], object("rival"));
        assert!(!all.values().any(|name| *name == object("first")));
        assert_population(&store.inner, &all).await;
    })
    .await;
}

/// **C6 (iii), this create's nonce under another request is still a collision.** As C6 (i), but
/// the rival is minted with this create's own upload id **and** nonce, for another object. The
/// re-read after the unapplied 1021 finds a record under the upload id that carries this
/// create's nonce but not its request, so it cannot be this create's session moved on (no
/// transition changes the object): a collision. The create answers `Created` under a new
/// identity, and the rival's record is byte-identical afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c6_iii_another_request_under_this_creates_nonce_is_still_a_collision() {
    within(async {
        let profile = shipped();
        let plan = [OnCommit::RivalFirst {
            may_still_commit: false,
            nonce: 1,
            object: "rival",
        }];
        let store = Scripted::new(profile, &plan).await;
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_through(&store, &profile, &object("first"), &mut script).await;
        assert_eq!(
            created(&outcome),
            identity(2).0,
            "re-minted, never in doubt over another request's session"
        );
        let (rival, rival_bytes) = store.state().rival.clone().expect("the rival ran");
        assert_eq!(rival, identity(1).0);
        assert_eq!(
            store.inner.get(&mpu_key(&rival)).await.unwrap(),
            Some(rival_bytes)
        );
        let all = sessions(&store.inner).await;
        assert_eq!(
            all[rival.as_str()].segment_nonce(),
            &identity(1).1,
            "the rival carries this create's first nonce"
        );
        assert_eq!(all[rival.as_str()].object(), object("rival"));
        assert_eq!(all[identity(2).0.as_str()].object(), object("first"));
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 2);
        assert_population(&store.inner, &population(&store.inner).await).await;
    })
    .await;
}

/// **C6 (iv), this create's request under another nonce is still a collision.** As C6 (i), but
/// the rival is minted with this create's upload id and the **same request** (bucket, object,
/// content type, clock stamp and clock source), under [`RIVAL`]'s nonce. The re-read after the
/// unapplied 1021 finds a record under the upload id that differs from R in the segment-group
/// nonce alone. No transition changes a session's nonce, so that record cannot be this create's
/// session moved on: a collision, never `InDoubt`. The create answers `Created` under a new
/// identity, asks for no wait (a collision spends no contention), and the rival's record is
/// byte-identical afterwards. This is the leg that pins the nonce half of
/// `SessionRecord::same_session`: without it, a same-request rival would end this create in
/// doubt over a session it never wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c6_iv_this_creates_request_under_another_nonce_is_still_a_collision() {
    within(async {
        let profile = shipped();
        let name = object("first");
        let plan = [OnCommit::RivalFirst {
            may_still_commit: false,
            nonce: RIVAL,
            object: "first",
        }];
        let store = Scripted::new(profile, &plan).await;
        let mut script = Script::repeating(vec![identity(1), identity(2)], 0);
        let outcome = create_through(&store, &profile, &name, &mut script).await;
        assert_eq!(
            created(&outcome),
            identity(2).0,
            "re-minted, never in doubt over another nonce's session"
        );
        assert_eq!(script.minted, vec![identity(1), identity(2)]);
        assert!(
            script.waits.is_empty(),
            "a collision spends no contention: {:?}",
            script.waits
        );
        let commits = store.commits();
        assert_eq!(commits.len(), 2, "one commit per identity");
        assert_eq!(commits[0].session_key, mpu_key(&identity(1).0));
        assert_eq!(commits[1].session_key, mpu_key(&identity(2).0));
        let (rival, rival_bytes) = store.state().rival.clone().expect("the rival ran");
        assert_eq!(rival, identity(1).0, "the rival took the shared upload id");
        assert_eq!(
            store.inner.get(&mpu_key(&rival)).await.unwrap(),
            Some(rival_bytes.clone()),
            "the rival's record is byte-identical afterwards"
        );
        // The fixture's premise: the rival's record and this create's R under the shared id
        // agree on every field `same_session` compares, except the nonce.
        let r = decode_session_record(&commits[0].session).unwrap();
        let all = sessions(&store.inner).await;
        let theirs = &all[rival.as_str()];
        assert_ne!(rival_bytes, commits[0].session, "not R");
        assert_eq!(theirs.parent(), r.parent());
        assert_eq!(theirs.object(), r.object());
        assert_eq!(theirs.content_type(), r.content_type());
        assert_eq!(theirs.created_at_millis(), r.created_at_millis());
        assert_eq!(theirs.clock_source(), r.clock_source());
        assert_eq!(theirs.segment_nonce(), &identity(RIVAL).1);
        assert_ne!(theirs.segment_nonce(), r.segment_nonce());
        assert_eq!(all[identity(2).0.as_str()].object(), name);
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 2);
        assert_population(&store.inner, &population(&store.inner).await).await;
    })
    .await;
}

// ───────────── (F) an unknown outcome, recovered under the original id ─────────────

/// The shared end state of F1–F3: `Created` under the original identity, one session under it
/// that is exactly R, one marker, one count, one identity minted, every commit under it, and
/// every commit putting the same R.
async fn assert_recovered(
    store: &Scripted,
    script: &Script,
    outcome: &Result<CreateOutcome, CreateError>,
) {
    let id = created(outcome);
    assert_eq!(script.minted.len(), 1, "no second identity was minted");
    assert_eq!(
        id, script.minted[0].0,
        "recovered under the original upload id"
    );
    let commits = store.commits();
    let r = commits[0].session.clone();
    for seen in &commits {
        assert_eq!(
            seen.session_key,
            mpu_key(&id),
            "every commit is under the one identity"
        );
        assert_eq!(
            seen.session, r,
            "every batch under an identity puts exactly R"
        );
    }
    assert_eq!(
        store.inner.get(&mpu_key(&id)).await.unwrap(),
        Some(r),
        "the session is exactly R"
    );
    let record = &sessions(&store.inner).await[id.as_str()];
    assert_eq!(
        record.segment_nonce(),
        &script.minted[0].1,
        "and the original nonce"
    );
    assert_eq!(
        markers(&store.inner).await,
        BTreeSet::from([metadata::seggrp_key(&script.minted[0].1)]),
        "one seggrp: marker"
    );
}

/// **F1**: the unknown outcome's batch was applied before the error. The re-read finds R.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f1_an_applied_unknown_outcome_is_adopted_by_the_re_read() {
    within(async {
        let plan = [OnCommit::Unknown {
            applied: true,
            may_still_commit: true,
        }];
        let store = Scripted::new(shipped(), &plan).await;
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &shipped(), &object("f"), &mut script).await;
        assert_recovered(&store, &script, &outcome).await;
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 1);
        assert_eq!(store.commits().len(), 1, "settled by the read, no re-drive");
    })
    .await;
}

/// **F2**: the unknown outcome's batch was not applied (and never is). The re-drive commits under
/// the same identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f2_an_unapplied_unknown_outcome_is_re_driven_under_the_same_identity() {
    within(async {
        let plan = [OnCommit::Unknown {
            applied: false,
            may_still_commit: true,
        }];
        let store = Scripted::new(shipped(), &plan).await;
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &shipped(), &object("f"), &mut script).await;
        assert_recovered(&store, &script, &outcome).await;
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 1);
        assert_eq!(store.commits().len(), 2);
    })
    .await;
}

/// **F3: the re-drive's exclusion is by the identity's keys, not by the ledger.** The store
/// starts with `mpuctl` at count `c`, with room, so the held batch carries `require(mpuctl ==
/// <count-c bytes>)`. The re-drive lands; then the test writes the count-`c` bytes back into
/// `mpuctl` (as #659's terminal delete of some other session would) and releases the held batch.
/// Its ledger precondition holds again, so only its `require_absent`s stop it: it answers
/// `Conflict`, `mpu:<id>` is exactly R, one `seggrp:` marker exists, and `mpuctl` is still exactly
/// the written-back bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f3_a_late_batch_is_excluded_by_the_identity_keys_not_the_ledger() {
    within(async {
        let profile = shipped();
        let c = 3;
        let at_c = ledger_bytes(c, &profile);
        let store = Scripted::new(profile, &[OnCommit::Hold]).await;
        store
            .inner
            .commit(WriteBatch::new().put(MPUCTL_KEY, at_c.clone()))
            .await
            .unwrap();
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        assert_recovered(&store, &script, &outcome).await;
        assert_eq!(
            store.commits().len(),
            2,
            "the held batch, then the re-drive"
        );
        assert_eq!(
            store.commits()[0].ledger,
            Some(at_c.clone()),
            "the held batch requires the count-c ledger"
        );
        assert_eq!(
            ledger(&store.inner).await.unwrap().count(),
            c + 1,
            "one increment, before the write-back"
        );

        store
            .inner
            .commit(WriteBatch::new().put(MPUCTL_KEY, at_c.clone()))
            .await
            .unwrap();
        assert_eq!(
            store.release_held().await,
            CommitOutcome::Conflict,
            "the late batch loses to its own identity's keys, though its ledger CAS now holds"
        );
        assert_recovered(&store, &script, &outcome).await;
        assert_eq!(store.inner.get(MPUCTL_KEY).await.unwrap(), Some(at_c));
    })
    .await;
}

/// **F4**: the unknown outcome lands on the **last** attempt the contention budget allows. The
/// create settles it by reading before it returns, and answers `Created` because the batch
/// landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f4_an_unknown_outcome_on_the_last_budgeted_attempt_is_settled_before_returning() {
    within(async {
        let profile = profile_of(3);
        let plan = [
            OnCommit::AdmitFirst(1),
            OnCommit::AdmitFirst(1),
            OnCommit::Unknown {
                applied: true,
                may_still_commit: false,
            },
        ];
        assert_eq!(plan.len() as u32, profile.admission_cas_attempts());
        let store = Scripted::new(profile, &plan).await;
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        let id = created(&outcome);
        assert_eq!(id, script.minted[0].0);
        assert_eq!(script.minted.len(), 1);
        assert_eq!(store.commits().len(), 3);
        let ledger = ledger(&store.inner).await.unwrap();
        assert_eq!(
            ledger.count(),
            3,
            "two others and this one, each counted once"
        );
        assert!(store.inner.get(&mpu_key(&id)).await.unwrap().is_some());
    })
    .await;
}

/// **F5**: another create fills the ledger between the recovery's reads. The create re-checks its
/// own `mpu:<id>` before giving up on the capacity refusal, and answers `Created` because its
/// earlier batch is there. The wrapper acts on the recovery's ledger read: it releases the held
/// batch (which lands) and admits creates until the ledger is full, so that read finds it full.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f5_a_capacity_refusal_met_in_recovery_is_checked_against_the_earlier_batch() {
    within(async {
        let profile = profile_of(3);
        let store = Scripted::new(profile, &[OnCommit::Hold]).await;
        store.state().on_read = Some(OnRead {
            key: Watch::Ledger,
            release: true,
            meddle: Meddle::Admit(2),
        });
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        assert_eq!(store.state().late, Some(CommitOutcome::Committed));
        let id = created(&outcome);
        assert_eq!(id, script.minted[0].0);
        assert_eq!(script.minted.len(), 1);
        assert_eq!(
            store.commits().len(),
            1,
            "the held batch is the one that landed"
        );
        let ledger = ledger(&store.inner).await.unwrap();
        assert_eq!(ledger.count(), ledger.max_sessions(), "the ledger is full");
        assert_eq!(sessions(&store.inner).await.len(), 3);
        assert!(store.inner.get(&mpu_key(&id)).await.unwrap().is_some());
    })
    .await;
}

/// **F5, every other ending met in recovery (rows 5, 6 and 9 "after the settling rule").** The
/// create settles before **every** ending other than `Created`, not only the capacity refusal.
/// Commit 1 is held. On the recovery's read of the bucket marker, or of `mpuctl`, the wrapper
/// first releases the held batch, which lands, and then deletes the marker, rewrites `mpuctl` at
/// its count under another profile, or tears `mpuctl`. So that read meets `NoSuchBucket`,
/// `ProfileSkew` or a torn ledger while the create's own session is stored and counted. The
/// create reads its `mpu:<id>` once more, finds R, and answers `Created` under its original
/// identity. A create that returned the refusal or the ledger error straight away would strand
/// that session.
///
/// Run again without the release, the batch is still held when the ending is met: the create
/// ends `InDoubt { may_still_commit: true }` naming its identity, with that ending as its cause.
/// Released afterwards, the held batch meets `Conflict` (its bucket or ledger precondition moved),
/// and no session exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f5_every_other_ending_met_in_recovery_is_checked_against_the_earlier_batch() {
    within(async {
        let profile = shipped();
        for (key, meddle) in [
            (Watch::Bucket, Meddle::DeleteMarker),
            (Watch::Ledger, Meddle::SkewLedger),
            (Watch::Ledger, Meddle::TearLedger),
        ] {
            for release in [true, false] {
                let label = format!("{meddle:?}, released under the read: {release}");
                let store = Scripted::new(profile, &[OnCommit::Hold]).await;
                store.state().on_read = Some(OnRead {
                    key,
                    release,
                    meddle,
                });
                let mut script = Script::fresh(0);
                let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
                assert!(store.state().on_read.is_none(), "{label}: the hook fired");
                let original = script.minted[0].clone();
                assert_eq!(script.minted.len(), 1, "{label}: no second identity");
                let commits = store.commits();
                assert_eq!(commits.len(), 1, "{label}: the held batch only");
                assert_eq!(
                    commits[0].ledger_put,
                    ledger_bytes(1, &profile),
                    "{label}: the held batch counts its session"
                );
                if release {
                    assert_eq!(created(&outcome), original.0, "{label}");
                    assert_eq!(
                        store.inner.get(&mpu_key(&original.0)).await.unwrap(),
                        Some(commits[0].session.clone()),
                        "{label}: the session is exactly R"
                    );
                    assert_eq!(sessions(&store.inner).await.len(), 1, "{label}");
                    assert_eq!(
                        markers(&store.inner).await,
                        BTreeSet::from([metadata::seggrp_key(&original.1)]),
                        "{label}"
                    );
                    continue;
                }
                let doubt = in_doubt(outcome, &original, true);
                let text = doubt.cause.to_string();
                match (meddle, &doubt.cause) {
                    (Meddle::DeleteMarker, Unsettled::Refused(Refusal::NoSuchBucket)) => {}
                    (
                        Meddle::SkewLedger,
                        Unsettled::Refused(Refusal::ProfileSkew { local, ledger }),
                    ) => {
                        assert_eq!(*local, profile, "{label}");
                        assert_eq!(*ledger, profile_of(SKEWED), "{label}");
                    }
                    (Meddle::TearLedger, Unsettled::Ledger(_)) => {
                        assert!(
                            text.contains("the mpuctl ledger is torn"),
                            "{label}: {text}"
                        );
                    }
                    (_, cause) => panic!("{label}: stopped by the wrong ending: {cause:?}"),
                }
                assert_eq!(
                    store.release_held().await,
                    CommitOutcome::Conflict,
                    "{label}: the held batch's bucket or ledger precondition moved"
                );
                assert!(sessions(&store.inner).await.is_empty(), "{label}");
                assert!(markers(&store.inner).await.is_empty(), "{label}");
            }
        }
    })
    .await;
}

/// **F5, a held batch landing between the identity's two reads (row 3, "after the settling
/// rule").** Commit 1 is held. On the recovery's `seggrp:<nonce>` read, after its `mpu:<id>` read
/// found nothing, the wrapper releases the held batch, and it lands. That read finds the create's
/// own marker with no session beside it in the view, so the identity looks taken while the create
/// is held. The create reads its `mpu:<id>` once more before the in-doubt ending, finds R, and
/// answers `Created` under its original identity: one session, one marker, counted once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f5_a_held_batch_landing_between_the_identity_reads_is_answered_created() {
    within(async {
        let profile = shipped();
        let store = Scripted::new(profile, &[OnCommit::Hold]).await;
        store.state().on_read = Some(OnRead {
            key: Watch::Group,
            release: true,
            meddle: Meddle::Nothing,
        });
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        assert_eq!(
            store.state().late,
            Some(CommitOutcome::Committed),
            "the held batch landed under the re-read"
        );
        assert_recovered(&store, &script, &outcome).await;
        assert_eq!(
            store.commits().len(),
            1,
            "the held batch is the one that landed"
        );
        assert_eq!(ledger(&store.inner).await.unwrap().count(), 1);
    })
    .await;
}

/// **F6, the doubt is kept past a later 1021.** On a profile whose contention budget is two
/// attempts, commit 1 is held (`may_still_commit: true`) and commit 2 returns
/// `CommitUnknownResult { may_still_commit: false }`, unapplied. The create ends `InDoubt {
/// may_still_commit: true }` naming the upload id and nonce — never a contention refusal.
/// Released after the create returned, the held batch lands, and the store then holds that one
/// session, counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f6_a_batch_that_may_still_land_keeps_the_create_in_doubt() {
    within(async {
        let profile = profile_of(2);
        let plan = [
            OnCommit::Hold,
            OnCommit::Unknown {
                applied: false,
                may_still_commit: false,
            },
        ];
        assert_eq!(plan.len() as u32, profile.admission_cas_attempts());
        let store = Scripted::new(profile, &plan).await;
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        let original = script.minted[0].clone();
        assert_eq!(script.minted.len(), 1);
        let doubt = in_doubt(outcome, &original, true);
        assert!(
            matches!(
                doubt.cause,
                Unsettled::Refused(Refusal::SlowDown {
                    pressure: Backpressure::AdmissionContention { attempts: 2 }
                })
            ),
            "{:?}",
            doubt.cause
        );
        assert!(store
            .inner
            .get(&mpu_key(&original.0))
            .await
            .unwrap()
            .is_none());

        assert_eq!(store.release_held().await, CommitOutcome::Committed);
        let expected = BTreeMap::from([(original.0.as_str().to_owned(), object("f"))]);
        assert_population(&store.inner, &expected).await;
    })
    .await;
}

/// **F7, the `mpuctl` precondition is rebuilt, not replayed (`0016:2018-2025`).** Commit 1
/// returns `CommitUnknownResult { may_still_commit: false }` and is not applied; **inside** commit
/// 1, before it returns, the wrapper admits a second create, moving `mpuctl` from `c` to `c + 1`
/// with room left, so the move lands before any of the recovery's reads. The create commits
/// exactly two batches, and batch 2 — the first recovery batch — requires the fresh count-`c + 1`
/// bytes and puts the ledger at `c + 2`. A recovery that replayed batch 1 and then retried would
/// commit three batches, its batch 2 naming count `c`. The first create answers `Created` under
/// its original identity, both sessions exist with their markers, and the ledger counts `c + 2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f7_the_recovery_rebuilds_the_ledger_precondition_from_a_fresh_read() {
    within(async {
        let profile = shipped();
        let plan = [OnCommit::UnknownThenAdmit {
            may_still_commit: false,
            admit: 1,
        }];
        let store = Scripted::new(profile, &plan).await;
        store.admit(2).await;
        let c = ledger(&store.inner).await.unwrap().count();
        assert_eq!(c, 2);
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        let id = created(&outcome);
        assert_eq!(script.minted.len(), 1);
        assert_eq!(id, script.minted[0].0);
        let commits = store.commits();
        assert_eq!(commits.len(), 2, "exactly two batches: no replay");
        assert_eq!(commits[0].ledger, Some(ledger_bytes(c, &profile)));
        assert_eq!(commits[0].ledger_put, ledger_bytes(c + 1, &profile));
        assert_eq!(
            commits[1].ledger,
            Some(ledger_bytes(c + 1, &profile)),
            "the first recovery batch requires the fresh read"
        );
        assert_eq!(
            commits[1].ledger_put,
            ledger_bytes(c + 2, &profile),
            "and puts the ledger at c + 2"
        );
        assert_eq!(ledger(&store.inner).await.unwrap().count(), c + 2);
        let all = population(&store.inner).await;
        assert_eq!(all.len() as u64, c + 2);
        assert!(all.contains_key(id.as_str()));
        assert_population(&store.inner, &all).await;
    })
    .await;
}

/// The shared tail of F8 (a)–(c): the held batch, released after the create returned, lands,
/// and the store then holds that one session, counted.
async fn assert_held_batch_lands(store: &Scripted, original: &(UploadId, SegmentNonce)) {
    assert!(store
        .inner
        .get(&mpu_key(&original.0))
        .await
        .unwrap()
        .is_none());
    assert_eq!(store.release_held().await, CommitOutcome::Committed);
    let expected = BTreeMap::from([(original.0.as_str().to_owned(), object("f"))]);
    assert_population(&store.inner, &expected).await;
    assert_eq!(
        sessions(&store.inner).await[original.0.as_str()].segment_nonce(),
        &original.1
    );
}

/// **F8 (a), the doubt is kept past a read fault.** Commit 1 is held (`may_still_commit:
/// true`), and the next read fails with a `TransientFault`. The create ends `InDoubt {
/// may_still_commit: true }` — indeterminate, though a transient fault is what stopped it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f8_a_a_held_create_stays_in_doubt_past_a_read_fault() {
    within(async {
        let store = Scripted::new(shipped(), &[OnCommit::Hold]).await;
        store.state().get_fault = Some(GetFault {
            after_commits: 1,
            sessions_only: false,
            skip: 0,
            fail: 1,
            transient: true,
        });
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &shipped(), &object("f"), &mut script).await;
        let original = script.minted[0].clone();
        let doubt = in_doubt(outcome, &original, true);
        let Unsettled::Store(cause) = &doubt.cause else {
            panic!("stopped by the read fault, got {:?}", doubt.cause);
        };
        assert!(
            cause.downcast_ref::<TransientFault>().is_some(),
            "the cause is the TransientFault"
        );
        assert!(doubt.cause.to_string().contains(FAULT), "{}", doubt.cause);
        assert_eq!(script.minted.len(), 1);
        assert_held_batch_lands(&store, &original).await;
    })
    .await;
}

/// **F8 (b), the doubt is kept past a plain commit fault.** Commit 1 is held, and commit 2 (the
/// re-drive) fails with a plain store error. The create ends `InDoubt { may_still_commit: true
/// }`, not `CreateError::Store`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f8_b_a_held_create_stays_in_doubt_past_a_commit_fault() {
    within(async {
        let store = Scripted::new(shipped(), &[OnCommit::Hold, OnCommit::Fail]).await;
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &shipped(), &object("f"), &mut script).await;
        let original = script.minted[0].clone();
        let doubt = in_doubt(outcome, &original, true);
        assert!(
            matches!(&doubt.cause, Unsettled::Store(cause) if cause.to_string() == FAULT),
            "{:?}",
            doubt.cause
        );
        assert_eq!(store.commits().len(), 2);
        assert_eq!(script.minted.len(), 1);
        assert_held_batch_lands(&store, &original).await;
    })
    .await;
}

/// **F8 (c), the doubt is kept past a failed settling read.** On a one-attempt contention budget,
/// commit 1 is held — the last budgeted attempt — and the create's read after it succeeds, but its
/// settling read of `mpu:<id>` before the contention refusal fails. The create ends `InDoubt {
/// may_still_commit: true }`, never the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f8_c_a_held_create_stays_in_doubt_past_a_failed_settling_read() {
    within(async {
        let profile = profile_of(1);
        let store = Scripted::new(profile, &[OnCommit::Hold]).await;
        store.state().get_fault = Some(GetFault {
            after_commits: 1,
            sessions_only: true,
            skip: 1,
            fail: usize::MAX,
            transient: false,
        });
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        let original = script.minted[0].clone();
        let doubt = in_doubt(outcome, &original, true);
        assert!(
            matches!(
                doubt.cause,
                Unsettled::Refused(Refusal::SlowDown {
                    pressure: Backpressure::AdmissionContention { attempts: 1 }
                })
            ),
            "{:?}",
            doubt.cause
        );
        assert!(
            doubt.cause.to_string().contains("AdmissionContention"),
            "{}",
            doubt.cause
        );
        assert_held_batch_lands(&store, &original).await;
    })
    .await;
}

/// **F8 (d), a 1021 whose settling read fails.** Commit 1 returns `CommitUnknownResult {
/// may_still_commit: false }` — out of flight, applied or not on the wrapper's choice — with
/// nothing earlier in doubt, and every read after it fails, the settling read included. The
/// create ends `InDoubt { may_still_commit: false }`, never `CreateError::Store`, because the
/// batch may have landed. Applied and unapplied give the same answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f8_d_an_unsettled_1021_with_a_failed_settling_read_ends_in_doubt() {
    within(async {
        let mut answers = Vec::new();
        for applied in [true, false] {
            let plan = [OnCommit::Unknown {
                applied,
                may_still_commit: false,
            }];
            let store = Scripted::new(shipped(), &plan).await;
            store.state().get_fault = Some(GetFault {
                after_commits: 1,
                sessions_only: false,
                skip: 0,
                fail: usize::MAX,
                transient: false,
            });
            let mut script = Script::fresh(0);
            let outcome = create_through(&store, &shipped(), &object("f"), &mut script).await;
            let original = script.minted[0].clone();
            let doubt = in_doubt(outcome, &original, false);
            assert!(
                matches!(doubt.cause, Unsettled::Store(_)),
                "applied {applied}: {:?}",
                doubt.cause
            );
            assert_eq!(store.commits().len(), 1);
            let expected = if applied {
                BTreeMap::from([(original.0.as_str().to_owned(), object("f"))])
            } else {
                BTreeMap::new()
            };
            assert_population(&store.inner, &expected).await;
            answers.push((
                doubt.upload_id.clone(),
                doubt.segment_nonce.clone(),
                doubt.may_still_commit,
            ));
        }
        assert_eq!(
            answers[0], answers[1],
            "applied or not, the create gives the same answer"
        );
    })
    .await;
}

/// **F8 (e), a held doubt outranks a later 1021 when every read fails.** Commit 1 is held
/// (`may_still_commit: true`), commit 2 (the re-drive) returns `CommitUnknownResult {
/// may_still_commit: false }` unapplied, and every read after it fails, the settling read
/// included. The create ends `InDoubt { may_still_commit: true }` naming its identity: the held
/// outcome is the one it reports, never the later out-of-flight one. Released afterwards, the held
/// batch lands, and the store then holds that one session, counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f8_e_a_held_create_stays_in_doubt_past_a_later_1021_and_failing_reads() {
    within(async {
        let plan = [
            OnCommit::Hold,
            OnCommit::Unknown {
                applied: false,
                may_still_commit: false,
            },
        ];
        let store = Scripted::new(shipped(), &plan).await;
        store.state().get_fault = Some(GetFault {
            after_commits: 2,
            sessions_only: false,
            skip: 0,
            fail: usize::MAX,
            transient: false,
        });
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &shipped(), &object("f"), &mut script).await;
        let original = script.minted[0].clone();
        let doubt = in_doubt(outcome, &original, true);
        assert!(
            matches!(&doubt.cause, Unsettled::Store(cause) if cause.to_string() == FAULT),
            "{:?}",
            doubt.cause
        );
        assert_eq!(store.commits().len(), 2, "the held batch, then the 1021");
        assert_eq!(script.minted.len(), 1);
        assert_held_batch_lands(&store, &original).await;
    })
    .await;
}

/// **F9, the error contract when nothing is in doubt.** A read fault before any commit ends
/// `CreateError::Store`, and a `TransientFault` cause keeps its class: it classifies
/// `Transient`, because no batch is in doubt and a retry is safe. Its `Display` names the fault.
/// (Every in-doubt ending above is checked by [`in_doubt`]: `Indeterminate`, never transient,
/// naming the upload id and nonce.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f9_a_store_fault_with_nothing_in_doubt_keeps_its_class() {
    within(async {
        let store = Scripted::new(shipped(), &[]).await;
        store.state().get_fault = Some(GetFault {
            after_commits: 0,
            sessions_only: false,
            skip: 0,
            fail: 1,
            transient: true,
        });
        let before = snapshot(&store.inner).await;
        let mut script = Script::fresh(0);
        let err = create_through(&store, &shipped(), &object("f"), &mut script)
            .await
            .expect_err("the read fault ends the create");
        assert!(matches!(err, CreateError::Store(_)), "{err:?}");
        assert_eq!(classify(&err), ErrorClass::Transient);
        assert!(classify(&err).is_transient());
        let text = err.to_string();
        assert!(
            text.contains("multipart create: store fault") && text.contains(FAULT),
            "{text}"
        );
        assert!(store.commits().is_empty());
        assert_eq!(snapshot(&store.inner).await, before);
    })
    .await;
}

/// **F9, a 1021 settled absent leaves nothing in doubt** (the settling rule: "if it finds
/// anything else and the create is not held, the ending stands"). An out-of-flight unknown
/// outcome (`CommitUnknownResult { may_still_commit: false }`, unapplied) is settled by the next
/// successful read of `mpu:<id>` that does not find R. From then on the plain ending stands, with
/// its cause's class.
///
/// * Settled by the read before the ending: the re-read after the 1021 fails with a
///   `TransientFault`, and the settling read then succeeds and finds nothing. The create ends
///   `CreateError::Store`, which classifies `Transient`: no batch is in doubt, so a retry is safe.
/// * Settled by the loop's re-read: the re-read after the 1021 finds nothing, then the re-drive's
///   commit fails with a plain store error, and every `mpu:` read after it fails. The create ends
///   `CreateError::Store`, never `InDoubt`: the re-read already settled the 1021, and a failing
///   read cannot undo that.
///
/// Neither writes anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f9_a_1021_settled_absent_leaves_nothing_in_doubt() {
    within(async {
        let unapplied = OnCommit::Unknown {
            applied: false,
            may_still_commit: false,
        };

        // Settled by the read before the ending.
        let store = Scripted::new(shipped(), &[unapplied]).await;
        store.state().get_fault = Some(GetFault {
            after_commits: 1,
            sessions_only: false,
            skip: 0,
            fail: 1,
            transient: true,
        });
        let before = snapshot(&store.inner).await;
        let mut script = Script::fresh(0);
        let err = create_through(&store, &shipped(), &object("f"), &mut script)
            .await
            .expect_err("the read fault ends the create");
        let CreateError::Store(cause) = &err else {
            panic!("the settled 1021 leaves the plain store fault standing, got {err:?}");
        };
        assert!(cause.downcast_ref::<TransientFault>().is_some(), "{err:?}");
        assert_eq!(classify(&err), ErrorClass::Transient, "{err}");
        assert_eq!(store.commits().len(), 1);
        assert_eq!(script.minted.len(), 1);
        assert_eq!(snapshot(&store.inner).await, before, "it writes nothing");

        // Settled by the loop's re-read.
        let store = Scripted::new(shipped(), &[unapplied, OnCommit::Fail]).await;
        store.state().get_fault = Some(GetFault {
            after_commits: 2,
            sessions_only: true,
            skip: 0,
            fail: usize::MAX,
            transient: false,
        });
        let before = snapshot(&store.inner).await;
        let mut script = Script::fresh(0);
        let err = create_through(&store, &shipped(), &object("f"), &mut script)
            .await
            .expect_err("the commit fault ends the create");
        assert!(
            matches!(&err, CreateError::Store(cause) if cause.to_string() == FAULT),
            "the settled 1021 leaves the plain store fault standing, got {err:?}"
        );
        assert_ne!(classify(&err), ErrorClass::Indeterminate, "{err}");
        assert_eq!(store.commits().len(), 2, "the 1021, then the re-drive");
        assert_eq!(script.minted.len(), 1);
        assert_eq!(snapshot(&store.inner).await, before, "it writes nothing");
    })
    .await;
}

// ──────── (F10) this create's own session, moved on before it is read back ────────

/// The end state of every F10 leg: the create's own session is the one session under its
/// original upload id, fenced to `Aborting@1` with its nonce and object kept, its `seggrp:` marker
/// beside it, and `mpuctl` counting it once. `others` sessions were admitted before the create.
async fn assert_own_session_moved_on(
    store: &Scripted,
    original: &(UploadId, SegmentNonce),
    others: u64,
) {
    let all = sessions(&store.inner).await;
    assert_eq!(
        all.len() as u64,
        others + 1,
        "one session of the create's own"
    );
    let own = &all[original.0.as_str()];
    assert!(
        matches!(own.state(), SessionState::Aborting {}),
        "{:?}",
        own.state()
    );
    assert_eq!(own.epoch(), 1, "fenced once");
    assert_eq!(own.segment_nonce(), &original.1, "its own nonce");
    assert_eq!(own.object(), object("f"), "its own object");
    let nonces: BTreeSet<Vec<u8>> = all
        .values()
        .map(|record| metadata::seggrp_key(record.segment_nonce()))
        .collect();
    assert_eq!(
        markers(&store.inner).await,
        nonces,
        "one marker per session"
    );
    assert_eq!(
        ledger(&store.inner).await.unwrap().count(),
        others + 1,
        "mpuctl is up by exactly one"
    );
}

/// **F10, this create's own session fenced before the re-read (the T4 finding).** Commit 1 lands
/// and returns `CommitUnknownResult { may_still_commit }`, and before the recovery's re-read a
/// teardown fence (`SessionRecord::open_teardown`, the reaper's and the restore fence's route)
/// moves the session from `Open@0` to `Aborting@1`. The re-read finds a record that is not R but
/// carries this create's nonce and request: its own session, not a collision. The create mints no
/// second identity, makes no second commit, never answers `Created`, and ends `InDoubt` naming its
/// original upload id and nonce, with `may_still_commit` as the outcome said (the held rule:
/// `true` when held). `mpuctl.count()` is up by exactly one. A create that read the fenced record
/// as a collision would re-mint and admit a second session, leaving this one counted and unnamed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f10_its_own_session_fenced_before_the_re_read_ends_in_doubt_never_in_a_re_mint() {
    within(async {
        let profile = shipped();
        for may_still_commit in [false, true] {
            let label = format!("may_still_commit: {may_still_commit}");
            let plan = [OnCommit::UnknownThenFence { may_still_commit }];
            let store = Scripted::new(profile, &plan).await;
            store.admit(1).await;
            let c = ledger(&store.inner).await.unwrap().count();
            let mut script = Script::fresh(0);
            let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
            let original = script.minted[0].clone();
            let doubt = in_doubt(outcome, &original, may_still_commit);
            assert!(
                matches!(doubt.cause, Unsettled::SessionMoved),
                "{label}: {:?}",
                doubt.cause
            );
            let text = doubt.cause.to_string();
            assert!(text.contains("has since moved on"), "{label}: {text}");
            assert_eq!(script.minted.len(), 1, "{label}: no second identity minted");
            assert_eq!(store.commits().len(), 1, "{label}: no re-drive");
            assert_own_session_moved_on(&store, &original, c).await;
        }
    })
    .await;
}

/// **F10, the same, met by the settling read.** Commit 1 lands, a fence moves its session on, and
/// commit 1 returns `CommitUnknownResult { may_still_commit: false }`. The recovery's first read
/// then fails with a `TransientFault`, so the create gives up on a store fault, and its settling
/// read finds the fenced session: its own, so the doubt stands. It ends `InDoubt {
/// may_still_commit: false }` naming its identity, never `CreateError::Store`, whose `Transient`
/// class would invite a retry under a fresh identity and leave this session unnamed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f10_its_own_session_moved_on_is_kept_by_the_settling_read() {
    within(async {
        let profile = shipped();
        let plan = [OnCommit::UnknownThenFence {
            may_still_commit: false,
        }];
        let store = Scripted::new(profile, &plan).await;
        store.state().get_fault = Some(GetFault {
            after_commits: 1,
            sessions_only: false,
            skip: 0,
            fail: 1,
            transient: true,
        });
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        let original = script.minted[0].clone();
        let doubt = in_doubt(outcome, &original, false);
        assert!(
            matches!(doubt.cause, Unsettled::SessionMoved),
            "{:?}",
            doubt.cause
        );
        assert_eq!(script.minted.len(), 1);
        assert_eq!(store.commits().len(), 1);
        assert_own_session_moved_on(&store, &original, 0).await;
    })
    .await;
}

/// **F10, the same, while held, between the identity's two reads.** Commit 1 is held. On the
/// recovery's `seggrp:<nonce>` read, after its `mpu:<id>` read found nothing, the wrapper releases
/// the held batch, which lands, and fences the session it installed. The view shows the
/// identity's marker with no session beside it, so the identity looks taken while the create is
/// held. Its settling read finds its own session moved on, so it ends `InDoubt {
/// may_still_commit: true }` with [`Unsettled::SessionMoved`] as the cause, not
/// [`Unsettled::IdentityTaken`]: no other record holds the identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f10_its_own_session_moved_on_while_held_is_not_an_identity_taken() {
    within(async {
        let profile = shipped();
        let store = Scripted::new(profile, &[OnCommit::Hold]).await;
        store.state().on_read = Some(OnRead {
            key: Watch::Group,
            release: true,
            meddle: Meddle::Fence,
        });
        let mut script = Script::fresh(0);
        let outcome = create_through(&store, &profile, &object("f"), &mut script).await;
        assert!(store.state().on_read.is_none(), "the hook fired");
        assert_eq!(store.state().late, Some(CommitOutcome::Committed));
        let original = script.minted[0].clone();
        let doubt = in_doubt(outcome, &original, true);
        assert!(
            matches!(doubt.cause, Unsettled::SessionMoved),
            "{:?}",
            doubt.cause
        );
        assert_eq!(script.minted.len(), 1);
        assert_eq!(store.commits().len(), 1);
        assert_own_session_moved_on(&store, &original, 0).await;
    })
    .await;
}
