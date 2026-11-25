//! The reaper pass over an in-memory store whose pages hold at most three entries, so every paged
//! walk turns pages, under a small budget profile, so every bound the pass reads is a handful of
//! records. Each pass runs under a capturing `tracing` subscriber of its own: no test here hits a
//! callsite with none installed, and the operator signals are asserted from what the pass emitted.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing_subscriber::layer::SubscriberExt;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, SegmentNonce, MAX_VALUE_BYTES};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    mpu_key, part_key, psum_key, retire_session_range, sidx_key, CreateOutcome, OwnedEntry,
    PartNumber, PartRecord, PartSummary, Refusal, RetireMode, SlotRecord, StagedPlacement,
    MAX_INFLIGHT_PARTS, MAX_PART_NUMBER, SLOT_INDEX_WIDTH, TOKEN_HEX_LEN, WALL_CLOCK_SOURCE,
};
use wyrd_testkit::ManualClock;
use wyrd_traits::{BoxError, ChunkId, CommitUnknownResult, ScanPage};

use super::*;

const DAY: u64 = DAY_MILLIS;
const HOUR: u64 = 60 * 60 * 1_000;
/// The instant every pass here judges at.
const T: u64 = 1_000 * DAY;

/// Decode `json` as `$record` and re-encode it: the canonical bytes the decoders require.
macro_rules! canonical {
    ($record:ty, $json:expr) => {{
        let value: $record = metadata::decode($json.as_bytes()).expect("a legal record");
        metadata::encode(&value)
    }};
}

// ───────────────────────────── the store ─────────────────────────────

/// An in-memory store. A page holds at most three entries whatever limit was asked — a short page
/// the `scan_page` contract allows. It can fail every page under one prefix, fail the page asked
/// for after one key, fail every commit, answer one commit with an outcome it does not report
/// ([`Unknown`]), fail every read of one key, refuse a page asked for after a key longer than a
/// backend accepts, land one batch just before the first commit it is handed, land one write next
/// to a read of `mpuctl` ([`LedgerRead`]), and fail one numbered read of `mpuctl`.
#[derive(Default)]
struct Store {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    failing_prefix: Option<Vec<u8>>,
    failing_commits: bool,
    /// Answer the next commit with an outcome the store does not report, once.
    unknown_commit: Mutex<Option<Unknown>>,
    failing_get: Option<Vec<u8>>,
    /// The longest `after` key a page may be asked for — FoundationDB refuses a longer key.
    max_scan_key: Option<usize>,
    /// Fail the page asked for after exactly this key.
    failing_after: Option<Vec<u8>>,
    interleaved: Mutex<Option<WriteBatch>>,
    around_ledger_read: Mutex<Option<(LedgerRead, Interleave)>>,
    ledger_reads: Mutex<u32>,
    /// Fail the read of `mpuctl` with this number — 1 is a pass's first, 2 its last.
    failing_ledger_read: Option<u32>,
    /// Set while an interleaved write runs: its own reads are plain ones, never counted or hooked.
    interleaving: AtomicBool,
}

/// A commit whose outcome the store does not report (`wyrd_traits::CommitUnknownResult`).
#[derive(Debug, Clone, Copy)]
struct Unknown {
    /// Whether the batch lands before the store answers.
    lands: bool,
    /// What the store says of the batch: whether it may still land after the answer.
    may_still_commit: bool,
}

/// Where an interleaved write lands, relative to the pass's reads of `mpuctl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LedgerRead {
    /// Just after the first read has answered: before the pass reads the listing.
    AfterFirst,
    /// Just before the second read answers: after the pass has read the listing.
    BeforeSecond,
}

/// The interleaved write.
enum Interleave {
    /// One batch, as an operator's compare-and-set of the ledger lands.
    Batch(WriteBatch),
    /// The production Create (`create_session`) of the upload `id` under `profile`, at [`T`]:
    /// on a store with no ledger it installs the ledger and the session in one batch.
    Create(Budget, UploadId),
}

impl Store {
    fn seeded(records: impl IntoIterator<Item = (Vec<u8>, Bytes)>) -> Self {
        let store = Self::default();
        store.kv.lock().unwrap().extend(records);
        store
    }

    fn snapshot(&self) -> BTreeMap<Vec<u8>, Bytes> {
        self.kv.lock().unwrap().clone()
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    fn state(&self, id: &UploadId) -> SessionState {
        let value = self.value(&mpu_key(id)).expect("the session record");
        decode_session_record(&value)
            .expect("the session decodes")
            .state()
            .clone()
    }

    /// Answer the next commit with `unknown`, once.
    fn unknown_at_next_commit(&self, unknown: Unknown) {
        *self.unknown_commit.lock().unwrap() = Some(unknown);
    }

    /// Land `write` next to a read of `mpuctl`, once.
    fn interleave_at(&self, at: LedgerRead, write: Interleave) {
        *self.around_ledger_read.lock().unwrap() = Some((at, write));
    }

    /// Land the write due `at`, if one is.
    async fn interleave(&self, at: LedgerRead) {
        let due = {
            let mut hook = self.around_ledger_read.lock().unwrap();
            match &*hook {
                Some((when, _)) if *when == at => hook.take().map(|(_, write)| write),
                _ => None,
            }
        };
        match due {
            None => {}
            Some(Interleave::Batch(batch)) => {
                let mut kv = self.kv.lock().unwrap();
                assert_eq!(apply(&mut kv, batch), CommitOutcome::Committed);
            }
            Some(Interleave::Create(budget, id)) => {
                self.interleaving.store(true, Ordering::SeqCst);
                create(self, &budget, &id).await;
                self.interleaving.store(false, Ordering::SeqCst);
            }
        }
    }
}

/// Apply `batch` if every precondition holds.
fn apply(kv: &mut BTreeMap<Vec<u8>, Bytes>, batch: WriteBatch) -> CommitOutcome {
    if batch
        .preconditions
        .iter()
        .any(|pre| kv.get(&pre.key) != pre.expected.as_ref())
    {
        return CommitOutcome::Conflict;
    }
    for (key, value) in batch.puts {
        kv.insert(key, value);
    }
    for key in batch.deletes {
        kv.remove(&key);
    }
    CommitOutcome::Committed
}

#[async_trait]
impl MetadataStore for Store {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.failing_get.as_deref() == Some(key) {
            return Err(BoxError::from("an injected read fault"));
        }
        if key != MPUCTL_KEY || self.interleaving.load(Ordering::SeqCst) {
            return Ok(self.value(key));
        }
        let read = {
            let mut reads = self.ledger_reads.lock().unwrap();
            *reads += 1;
            *reads
        };
        if self.failing_ledger_read == Some(read) {
            return Err(BoxError::from("an injected read fault"));
        }
        if read == 2 {
            self.interleave(LedgerRead::BeforeSecond).await;
        }
        let value = self.value(key);
        if read == 1 {
            self.interleave(LedgerRead::AfterFirst).await;
        }
        Ok(value)
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        if self.failing_prefix.as_deref() == Some(prefix) {
            return Err(BoxError::from("an injected read fault"));
        }
        if after.is_some() && self.failing_after.as_deref() == after {
            return Err(BoxError::from("an injected read fault"));
        }
        if let (Some(max), Some(after)) = (self.max_scan_key, after) {
            if after.len() > max {
                return Err(BoxError::from(format!(
                    "a {}-byte key is past the backend's {max}-byte key limit",
                    after.len()
                )));
            }
        }
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit.min(3)).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        if self.failing_commits {
            return Err(BoxError::from("an injected commit fault"));
        }
        let mut kv = self.kv.lock().unwrap();
        if let Some(interleaved) = self.interleaved.lock().unwrap().take() {
            assert_eq!(apply(&mut kv, interleaved), CommitOutcome::Committed);
        }
        if let Some(unknown) = self.unknown_commit.lock().unwrap().take() {
            if unknown.lands {
                assert_eq!(apply(&mut kv, batch), CommitOutcome::Committed);
            }
            return Err(BoxError::from(CommitUnknownResult {
                backend: "test",
                code: None,
                detail: "an injected unknown commit result".to_owned(),
                may_still_commit: unknown.may_still_commit,
            }));
        }
        Ok(apply(&mut kv, batch))
    }
}

// ───────────────────────────── the records ─────────────────────────────

/// Two slot indices, three parts, three chunks a part, four staged chunks: `U_ref` 15, so
/// `max_sessions` is `⌊w_ref / 15⌋`. The idle arm's owned bound is 2 × 3 = 6 entries, and its
/// summary bound 3 + 2 = 5.
fn profile(w_ref: u64) -> Budget {
    let json = format!(
        "{{\"w_ref\":{w_ref},\"max_part_chunks\":3,\"max_parts_per_session\":3,\
         \"max_inflight_parts\":2,\"max_staged_chunks\":4}}"
    );
    metadata::decode(json.as_bytes()).expect("a legal profile")
}

fn policy(profile: Budget) -> ReaperPolicy {
    ReaperPolicy {
        profile,
        ..ReaperPolicy::default()
    }
}

/// `mpuctl` counting `count` sessions under `profile`.
fn ledger(count: u64, profile: &Budget) -> (Vec<u8>, Bytes) {
    let stored = String::from_utf8(metadata::encode(profile).to_vec()).unwrap();
    let json = format!(
        "{{\"count\":{count},\"max_sessions\":{},\"profile\":{stored}}}",
        profile.max_sessions()
    );
    (MPUCTL_KEY.to_vec(), canonical!(AdmissionRecord, json))
}

fn upload(n: u8) -> UploadId {
    UploadId::new(format!("{n:02x}").repeat(TOKEN_HEX_LEN / 2)).expect("an upload id")
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number")
}

/// The session record of `id`: created at `created`, under `clock`, at `epoch`, in `state`.
fn session_at(
    id: &UploadId,
    created: u64,
    clock: &str,
    epoch: u64,
    state: &str,
) -> (Vec<u8>, Bytes) {
    let json = format!(
        "{{\"parent\":42,\"object\":\"bucket/{id}\",\"created_at_millis\":{created},\
         \"clock_source\":\"{clock}\",\"segment_nonce\":\"{id}\",\"epoch\":{epoch},\
         \"attempts\":0,\"state\":{{\"kind\":\"{state}\"}}}}"
    );
    (mpu_key(id), canonical!(SessionRecord, json))
}

/// A wall-clocked `Open@0` session created at `created`.
fn open(id: &UploadId, created: u64) -> (Vec<u8>, Bytes) {
    session_at(id, created, WALL_CLOCK_SOURCE, 0, "Open")
}

/// Slot `index` of `id`, reserved at `reserved`, leased to `lease`.
fn slot(id: &UploadId, index: u32, reserved: u64, lease: u64) -> (Vec<u8>, Bytes) {
    let json = format!(
        "{{\"part_number\":1,\"attempt_id\":\"{}\",\"reserved_at_millis\":{reserved},\
         \"lease_expiry_millis\":{lease}}}",
        "ab".repeat(TOKEN_HEX_LEN / 2)
    );
    let index = SlotIndex::new(index).expect("a slot index");
    (slot_key(id, index), canonical!(SlotRecord, json))
}

/// The summary of part `n` of `id`, committed at `at`.
fn psum(id: &UploadId, n: u32, at: u64) -> (Vec<u8>, Bytes) {
    let json =
        format!("{{\"chunks\":1,\"len\":1,\"digest\":\"{n:064x}\",\"committed_at_millis\":{at}}}");
    (psum_key(id, part(n)), canonical!(PartSummary, json))
}

/// The owned staging entry of chunk `chunk` of part `n` of `id`, leased to `lease`.
fn owned(id: &UploadId, n: u32, chunk: ChunkId, lease: u64) -> (Vec<u8>, Bytes) {
    let staged = StagedPlacement::new(EcScheme::ReedSolomon { k: 2, m: 1 }, vec![1, 2, 3]).unwrap();
    let entry = OwnedEntry::new(id.clone(), lease, staged).to_pending();
    (sidx_key(id, part(n), chunk), metadata::encode(&entry))
}

/// A value no record decodes from.
fn garbage() -> Bytes {
    Bytes::from_static(b"not a record")
}

/// One RS(2,1) chunk on servers 1, 2 and 3.
fn chunk(id: ChunkId) -> ChunkRef {
    ChunkRef {
        id,
        scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
        len: 4_096,
        placement: vec![1, 2, 3],
    }
}

/// Part `n` of `id` committed over `chunk` at `at`: its `part:` and `psum:` records.
fn committed(id: &UploadId, n: u32, chunk: &ChunkRef, at: u64) -> [(Vec<u8>, Bytes); 2] {
    let chunks = String::from_utf8(metadata::encode(&vec![chunk.clone()]).to_vec()).unwrap();
    let digest = format!("{n:064x}");
    let record = format!(
        "{{\"chunks\":{chunks},\"len\":4096,\"digest\":\"{digest}\",\
         \"committed_at_millis\":{at},\"session_epoch\":0}}"
    );
    let summary = format!(
        "{{\"chunks\":1,\"len\":4096,\"digest\":\"{digest}\",\"committed_at_millis\":{at}}}"
    );
    [
        (part_key(id, part(n)), canonical!(PartRecord, record)),
        (psum_key(id, part(n)), canonical!(PartSummary, summary)),
    ]
}

/// The bucket every production Create here admits into, and its marker.
const BUCKET: &str = "bucket";

fn bucket() -> (Vec<u8>, Bytes) {
    (
        metadata::bucket_key(BUCKET),
        Bytes::from_static(br#"{"name":"bucket","created_at_millis":1}"#),
    )
}

/// Create's runtime for one fixed identity: no jitter, no wait.
struct OneIdentity(Option<(UploadId, SegmentNonce)>);

impl CreateRuntime for OneIdentity {
    fn mint(&mut self) -> (UploadId, SegmentNonce) {
        self.0.take().expect("one identity per create")
    }

    fn jitter(&mut self) -> u64 {
        0
    }

    async fn wait(&mut self, _millis: u64) {}
}

/// The production Create of `id` under `profile` on `store`, at [`T`]: it must be admitted.
async fn create(store: &Store, profile: &Budget, id: &UploadId) {
    let object = format!("{BUCKET}/{id}");
    let created = create_as(store, profile, id, &object, None, T).await;
    assert!(
        matches!(created, CreateOutcome::Created { .. }),
        "{created:?}"
    );
}

/// The production Create of `id` under `profile` on `store`, publishing `object` with
/// `content_type`, stamped `created` on the wall clock: what it answered.
async fn create_as(
    store: &Store,
    profile: &Budget,
    id: &UploadId,
    object: &str,
    content_type: Option<&str>,
    created: u64,
) -> CreateOutcome {
    let request = CreateRequest {
        bucket: BUCKET,
        parent: 0,
        object,
        content_type,
        created_at_millis: created,
        clock_source: WALL_CLOCK_SOURCE,
    };
    let nonce = SegmentNonce::new("5a".repeat(TOKEN_HEX_LEN / 2)).expect("a nonce");
    create_session(
        store,
        profile,
        &request,
        &mut OneIdentity(Some((id.clone(), nonce))),
    )
    .await
    .expect("the create answers")
}

// ───────────────────────────── one pass ─────────────────────────────

/// A `MakeWriter` collecting what a pass emits.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
    type Writer = Self;
    fn make_writer(&'w self) -> Self::Writer {
        self.clone()
    }
}

/// Run `f` over a reaper context on `store` at [`T`] under `policy`, with what it emits captured.
async fn traced<R>(
    store: &Store,
    policy: &ReaperPolicy,
    f: impl AsyncFnOnce(&ReaperContext<'_>) -> R,
) -> (R, String) {
    traced_on(store, &ManualClock::new(T), policy, f).await
}

/// [`traced`], reading `clock`.
async fn traced_on<R>(
    store: &Store,
    clock: &(dyn Clock + Sync),
    policy: &ReaperPolicy,
    f: impl AsyncFnOnce(&ReaperContext<'_>) -> R,
) -> (R, String) {
    let ctx = ReaperContext {
        meta: store,
        clock,
        clock_source: WALL_CLOCK_SOURCE,
        policy,
    };
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(capture.clone()),
    );
    let guard = tracing::subscriber::set_default(subscriber);
    let answer = f(&ctx).await;
    drop(guard);
    let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    (answer, log)
}

/// One pass's tallies (`None` when refused), and what it emitted.
async fn judge(store: &Store, policy: &ReaperPolicy) -> (Option<Pass>, String) {
    traced(store, policy, async |ctx| {
        pass(ctx).await.expect("mpuctl reads")
    })
    .await
}

// ───────────────────────────── the tests ─────────────────────────────

/// The windows sit inside the bounds the slice was given, and the default policy carries them,
/// the shipped profile and the retain tombstone.
#[test]
fn the_windows_and_the_default_policy_are_the_shipped_values() {
    assert_eq!(W_OPEN_MILLIS, 604_800_000);
    assert_eq!(W_SESSION_MILLIS, 2_592_000_000);
    let policy = ReaperPolicy::default();
    assert_eq!(policy.w_open_millis, W_OPEN_MILLIS);
    assert_eq!(policy.w_session_millis, W_SESSION_MILLIS);
    assert_eq!(policy.profile, Budget::shipped());
    assert_eq!(policy.tombstone(), Tombstone::Retain);
    assert_eq!(Arm::Idle.as_str(), "idle");
    assert_eq!(Arm::Ceiling.as_str(), "residency-ceiling");
}

/// The comparison is of the whole stored profile, and a ledger that is absent or that does not
/// decode is named as such.
#[tokio::test]
async fn the_profile_check_compares_the_whole_stored_profile() {
    let local = profile(1_500);
    let check = async |store: Store| check_admission_profile(&store, &local).await.unwrap();
    assert_eq!(check(Store::default()).await, AdmissionProfile::Absent);
    let (key, value) = ledger(0, &local);
    let stored = decode_admission_record(&value).unwrap();
    assert_eq!(
        check(Store::seeded([(key, value)])).await,
        AdmissionProfile::Matches(stored)
    );
    let other = profile(3_000);
    assert_eq!(
        check(Store::seeded([ledger(0, &other)])).await,
        AdmissionProfile::Differs { stored: other }
    );
    assert!(matches!(
        check(Store::seeded([(MPUCTL_KEY.to_vec(), garbage())])).await,
        AdmissionProfile::Unreadable { .. }
    ));
}

/// A stored profile other than the custodian's, or a ledger that does not decode, refuses the
/// pass: the operator signal, and not one byte written, though an over-age and an idle session
/// are there to fence. Through [`reconcile`] it is [`Reaped::Refused`].
#[tokio::test]
async fn a_refused_pass_writes_nothing() {
    let local = profile(1_500);
    for stored in [ledger(2, &profile(3_000)), (MPUCTL_KEY.to_vec(), garbage())] {
        let store = Store::seeded([
            stored,
            open(&upload(1), T - 40 * DAY),
            open(&upload(2), T - 10 * DAY),
        ]);
        let before = store.snapshot();
        let (refused, log) = judge(&store, &policy(local)).await;
        assert_eq!(refused, None);
        assert!(log.contains("reaper-profile-mismatch"), "{log}");
        assert!(log.contains("reaper_profile_refusals"), "{log}");
        // Refused at the first read: it says so, and there is no pass to count.
        assert!(log.contains("refused before acting"), "{log}");
        assert!(!log.contains("stopped after fencing"), "{log}");
        assert!(!log.contains("reaper_passes"), "{log}");
        let (reaped, _) = traced(&store, &policy(local), async |ctx| {
            reconcile(ctx).await.unwrap()
        })
        .await;
        assert_eq!(reaped, Reaped::Refused);
        assert_eq!(store.snapshot(), before);
    }
}

/// A store with nothing to judge is [`Reconciled::Satisfied`], and the pass's counts go out on
/// the durability seam.
#[tokio::test]
async fn an_empty_pass_is_satisfied_and_counted() {
    let local = profile(1_500);
    let store = Store::seeded([ledger(0, &local)]);
    let (reaped, log) = traced(&store, &policy(local), async |ctx| {
        reconcile(ctx).await.unwrap()
    })
    .await;
    assert_eq!(reaped, Reaped::Ran(Reconciled::Satisfied));
    assert!(log.contains("monotonic_counter.reaper_passes=1"), "{log}");
}

/// Each arm judges at its boundary and not before, over sessions only one piece of evidence
/// apart. The ceiling fences a live session one millisecond past `W_session` and not at it; the
/// idle arm fences one millisecond past `W_open` and not at it; a slot or owned lease expiring at
/// `now` is lapsed, one a millisecond later is live; a fresh summary or a fresh slot reservation
/// (its lease lapsed) is progress. A foreign-clocked session is never judged: held while it is
/// `Open`, which only an operator can end, and left to the retirement leg once it is past `Open`,
/// as an owned session past `Open` is.
#[tokio::test]
async fn each_arm_judges_at_its_boundary() {
    let local = profile(1_500);
    let (ceiling_at, ceiling_past, idle_at) = (&upload(0x10), &upload(0x11), &upload(0x12));
    let (idle_past, slot_now, slot_live) = (&upload(0x13), &upload(0x14), &upload(0x15));
    let (owned_now, owned_live, psum_fresh) = (&upload(0x16), &upload(0x17), &upload(0x18));
    let (reserved_fresh, foreign, aborting) = (&upload(0x19), &upload(0x1a), &upload(0x1b));
    let foreign_aborting = &upload(0x1c);
    let old = T - 10 * DAY;
    let records = vec![
        ledger(13, &local),
        open(ceiling_at, T - W_SESSION_MILLIS),
        slot(ceiling_at, 0, T - HOUR, T + 1),
        open(ceiling_past, T - W_SESSION_MILLIS - 1),
        slot(ceiling_past, 0, T - HOUR, T + 1),
        open(idle_at, T - W_OPEN_MILLIS),
        open(idle_past, T - W_OPEN_MILLIS - 1),
        open(slot_now, old),
        slot(slot_now, 1, old, T),
        open(slot_live, old),
        slot(slot_live, 1, old, T + 1),
        open(owned_now, old),
        owned(owned_now, 1, 1, T),
        open(owned_live, old),
        owned(owned_live, 1, 1, old),
        owned(owned_live, 1, 2, old),
        owned(owned_live, 1, 3, old),
        owned(owned_live, 2, 4, T + 1),
        open(psum_fresh, old),
        psum(psum_fresh, 1, old),
        psum(psum_fresh, 2, T - HOUR),
        psum(psum_fresh, 3, old),
        psum(psum_fresh, 4, old),
        open(reserved_fresh, old),
        slot(reserved_fresh, 0, T - HOUR, T - HOUR / 2),
        session_at(foreign, T - 40 * DAY, "logical", 0, "Open"),
        session_at(aborting, T - 40 * DAY, WALL_CLOCK_SOURCE, 1, "Aborting"),
        session_at(foreign_aborting, T - 40 * DAY, "logical", 1, "Aborting"),
    ];
    let store = Store::seeded(records);

    let (pass, log) = judge(&store, &policy(local)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!(
        pass,
        Pass {
            fenced_idle: 3,
            fenced_ceiling: 1,
            lost: 0,
            unknown: 0,
            held: 1,
            foreign: 2,
            kept: 6,
            whole: true,
        }
    );
    assert_eq!(pass.outcome(), Reconciled::Blocked);
    for id in [ceiling_past, idle_past, slot_now, owned_now] {
        assert_eq!(store.state(id), SessionState::Aborting {}, "{id}");
        let owed = retire_session_range(RetireMode::Bytes, id);
        assert!(
            store.snapshot().keys().any(|key| key.starts_with(&owed)),
            "{id} owes its bytes"
        );
    }
    for id in [
        ceiling_at,
        idle_at,
        slot_live,
        owned_live,
        psum_fresh,
        reserved_fresh,
        foreign,
    ] {
        assert_eq!(store.state(id), SessionState::Open {}, "{id}");
    }
    assert_eq!(store.state(aborting), SessionState::Aborting {});
    assert_eq!(store.state(foreign_aborting), SessionState::Aborting {});
    assert!(log.contains("reaper-foreign-clock"), "{log}");
    assert!(log.contains("hold=\"foreign-clock\""), "{log}");
    assert_eq!(log.matches("reaper-session-held").count(), 1, "{log}");
    assert!(log.contains("arm=\"residency-ceiling\""), "{log}");
    assert!(log.contains("fenced an abandoned upload"), "{log}");
}

/// What the pass cannot judge or read, it holds — named, counted, the pass [`Reconciled::Blocked`]
/// — and it goes on to judge the rest. Each held session sits beside one a record shorter, which
/// is judged: the bounds are the profile's (summaries 3 + 2, owned entries 2 × 3).
#[tokio::test]
async fn damage_holds_the_session_and_the_pass_goes_on() {
    let local = profile(1_500);
    let (undecodable, exhausted, stray_slot) = (&upload(0x20), &upload(0x21), &upload(0x22));
    let (bad_slot, five_psums, six_psums) = (&upload(0x23), &upload(0x24), &upload(0x25));
    let (bad_psum, six_owned, seven_owned) = (&upload(0x26), &upload(0x27), &upload(0x28));
    let (bad_owned, unreadable, healthy) = (&upload(0x29), &upload(0x2a), &upload(0x2b));
    let bad_psum_key = &upload(0x2c);
    let old = T - 10 * DAY;
    let mut records = vec![
        ledger(13, &local),
        (b"mpu:not-an-upload-id".to_vec(), garbage()),
        (mpu_key(undecodable), garbage()),
        session_at(exhausted, T - 40 * DAY, WALL_CLOCK_SOURCE, u64::MAX, "Open"),
        open(stray_slot, old),
        slot(stray_slot, 2, old, old + 1),
        open(bad_slot, old),
        (slot(bad_slot, 0, old, old + 1).0, garbage()),
        open(bad_psum, old),
        (psum(bad_psum, 1, old).0, garbage()),
        // A summary that decodes, and is fresh, under a key that names no part of the session.
        open(bad_psum_key, old),
        (
            format!("psum:{bad_psum_key}:not-a-part").into_bytes(),
            psum(bad_psum_key, 1, T - HOUR).1,
        ),
        open(bad_owned, old),
        (owned(bad_owned, 1, 1, old).0, garbage()),
        open(unreadable, old),
        open(healthy, old),
    ];
    for (id, psums) in [(five_psums, 5), (six_psums, 6)] {
        records.push(open(id, old));
        records.extend((1..=psums).map(|n| psum(id, n, old)));
    }
    for (id, entries) in [(six_owned, 6), (seven_owned, 7)] {
        records.push(open(id, old));
        records.extend((1..=entries).map(|chunk| owned(id, 1, chunk, old)));
    }
    let store = Store {
        failing_prefix: Some(wyrd_core::multipart::psum_range(unreadable)),
        ..Store::seeded(records)
    };

    let (pass, log) = judge(&store, &policy(local)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!(
        (pass.held, pass.fenced_idle, pass.kept),
        (11, 3, 0),
        "{pass:?}"
    );
    assert_eq!(pass.outcome(), Reconciled::Blocked);
    for id in [five_psums, six_owned, healthy] {
        assert_eq!(store.state(id), SessionState::Aborting {}, "{id}");
    }
    for id in [
        exhausted,
        stray_slot,
        bad_slot,
        six_psums,
        bad_psum,
        bad_psum_key,
        seven_owned,
        bad_owned,
        unreadable,
    ] {
        assert_eq!(store.state(id), SessionState::Open {}, "{id}");
    }
    assert!(log.contains("not-a-part"), "the key is named:\n{log}");
    for reason in [
        "key-names-no-upload",
        "session-undecodable",
        "epoch-exhausted",
        "slot-range-unjudgeable",
        "psum-range-unjudgeable",
        "sidx-range-unjudgeable",
        "store-fault",
    ] {
        assert!(log.contains(reason), "{reason} named:\n{log}");
    }
    assert!(log.contains("reaper-session-held"), "{log}");
}

/// A listing longer than the ledger's `max_sessions` is not damage: an operator may lower the
/// limit below the live count (`0016:2082-2084`), and sessions deleted and created between two
/// pages are listed more than the ledger ever held at once. Every session is judged, none held.
#[tokio::test]
async fn a_listing_past_max_sessions_is_judged_whole() {
    let local = profile(45);
    assert_eq!(local.max_sessions(), 3);
    let ids: Vec<UploadId> = (0x30..0x35).map(upload).collect();
    let store = Store::seeded(
        ids.iter()
            .map(|id| open(id, T - 10 * DAY))
            .chain([ledger(3, &local)]),
    );

    let (pass, _) = judge(&store, &policy(local)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!((pass.fenced_idle, pass.held), (5, 0), "{pass:?}");
    assert_eq!(pass.outcome(), Reconciled::Changed);
    assert!(ids
        .iter()
        .all(|id| store.state(id) == SessionState::Aborting {}));
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        None,
        "a whole listing moves no cursor"
    );
}

/// The session budget is a resumable one: a pass judges [`SESSIONS_PER_PASS`] sessions, leaves
/// the cursor after the last, and the next pass goes on from there to the end and puts the
/// cursor back at the head — so a session listed after a budget's worth of others is reached,
/// and no pass that read only part of the listing certifies it.
#[tokio::test]
async fn the_session_budget_resumes_where_the_last_pass_stopped() {
    let local = profile(1_500);
    let ids: Vec<UploadId> = (0..SESSIONS_PER_PASS + 2)
        .map(|n| upload(u8::try_from(n).unwrap()))
        .collect();
    let store = Store::seeded(
        ids.iter()
            .map(|id| open(id, T - 10 * DAY))
            .chain([ledger(ids.len() as u64, &local)]),
    );

    let (first, _) = judge(&store, &policy(local)).await;
    let first = first.expect("the profile matches");
    assert_eq!(first.fenced_idle, SESSIONS_PER_PASS as u64, "{first:?}");
    assert!(!first.whole);
    let last_judged = mpu_key(&ids[SESSIONS_PER_PASS - 1]);
    assert_eq!(
        store.value(REAPER_CURSOR_KEY).as_deref(),
        Some(&last_judged[..])
    );
    for id in &ids[SESSIONS_PER_PASS..] {
        assert_eq!(store.state(id), SessionState::Open {}, "{id} waits a pass");
    }

    let (second, _) = judge(&store, &policy(local)).await;
    let second = second.expect("the profile matches");
    assert_eq!((second.fenced_idle, second.held), (2, 0), "{second:?}");
    assert!(!second.whole, "it did not read from the head");
    assert!(ids
        .iter()
        .all(|id| store.state(id) == SessionState::Aborting {}));
    assert_eq!(store.value(REAPER_CURSOR_KEY).as_deref(), Some(&b""[..]));

    // From the head again: nothing left to fence, but a budget's worth read is not the whole.
    let (third, _) = judge(&store, &policy(local)).await;
    let third = third.expect("the profile matches");
    assert_eq!(third.outcome(), Reconciled::Partial, "{third:?}");
}

/// `count` wall-clocked `Open` sessions created a day before [`T`] — every one kept by both arms
/// — under a ledger counting them, in listing order.
fn retained(local: &Budget, count: usize) -> (Vec<UploadId>, Store) {
    let ids: Vec<UploadId> = (0..count)
        .map(|n| upload(u8::try_from(n).unwrap()))
        .collect();
    let store = Store::seeded(
        ids.iter()
            .map(|id| open(id, T - DAY))
            .chain([ledger(count as u64, local)]),
    );
    (ids, store)
}

/// One pass over `store`: what it answered and how many sessions it kept.
async fn lap_pass(store: &Store, local: Budget) -> (Reconciled, u64) {
    let (pass, _) = judge(store, &policy(local)).await;
    let pass = pass.expect("the profile matches");
    (pass.outcome(), pass.kept)
}

/// The cursor value that resumes after `id` in a lap that is `clean` so far.
fn cursor_after(id: &UploadId, clean: bool) -> Option<Bytes> {
    let tag: &[u8] = if clean { b"clean-lap:" } else { b"" };
    Some(Bytes::from([tag, &mpu_key(id)[..]].concat()))
}

/// **A listing that ends exactly at the budget is read whole.** [`SESSIONS_PER_PASS`] sessions,
/// every one kept: the budget runs out on a full page, which alone says nothing of what follows.
/// The pass reads one entry further, finds none, and answers `Satisfied` — on every pass, with no
/// cursor ever written — rather than `Partial` for as long as the listing stays that long.
#[tokio::test]
async fn a_listing_that_ends_at_the_budget_is_read_whole() {
    let local = profile(1_500);
    let (_, store) = retained(&local, SESSIONS_PER_PASS);
    let before = store.snapshot();

    for pass in 0..3 {
        assert_eq!(
            lap_pass(&store, local).await,
            (Reconciled::Satisfied, SESSIONS_PER_PASS as u64),
            "pass {pass}"
        );
        assert_eq!(store.snapshot(), before, "pass {pass} wrote nothing");
    }
}

/// **A clean lap past the budget is certified by the pass that ends it.** One session more than
/// the budget, every one kept. The first pass judges a budget's worth, finds one more listed and
/// leaves the cursor after the last it judged, tagged clean: `Partial`. The second judges the
/// rest, reaches the end of a lap in which nothing was fenced, lost or held, and answers
/// `Satisfied`, the cursor back at the head. So does every lap after it.
#[tokio::test]
async fn a_clean_lap_past_the_budget_is_satisfied_at_its_end() {
    let local = profile(1_500);
    let (ids, store) = retained(&local, SESSIONS_PER_PASS + 1);
    let budget = SESSIONS_PER_PASS as u64;

    for lap in 0..2 {
        assert_eq!(
            lap_pass(&store, local).await,
            (Reconciled::Partial, budget),
            "lap {lap}"
        );
        assert_eq!(
            store.value(REAPER_CURSOR_KEY),
            cursor_after(&ids[SESSIONS_PER_PASS - 1], true),
            "lap {lap}"
        );
        assert_eq!(
            lap_pass(&store, local).await,
            (Reconciled::Satisfied, 1),
            "lap {lap}"
        );
        assert_eq!(
            store.value(REAPER_CURSOR_KEY).as_deref(),
            Some(&b""[..]),
            "lap {lap}"
        );
    }
}

/// **A lap in which something happened certifies nothing at its end.** One session past the
/// budget, and in the first pass of the lap one session is fenced, or held, or its fence lost, or
/// its fence's outcome left unknown.
/// That pass leaves the cursor untagged, so the pass that ends the lap answers `Partial` however
/// little it found itself: the lap's hold, fence or contention is not accounted for by it. The
/// lap after a fence, or after contention that settled, is clean and ends `Satisfied`; a held
/// session blocks every lap.
#[tokio::test]
async fn a_lap_that_fenced_lost_or_held_is_not_certified_at_its_end() {
    let local = profile(1_500);
    let budget = SESSIONS_PER_PASS as u64;
    let last = SESSIONS_PER_PASS - 1;

    // Fenced: the first session is idle.
    let (ids, store) = retained(&local, SESSIONS_PER_PASS + 1);
    let idle = open(&ids[0], T - 10 * DAY);
    store.kv.lock().unwrap().insert(idle.0, idle.1);
    assert_eq!(
        lap_pass(&store, local).await,
        (Reconciled::Changed, budget - 1)
    );
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(&ids[last], false)
    );
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Partial, 1));
    // The next lap judges the same listing, the fenced session past `Open`, and finds nothing.
    assert_eq!(
        lap_pass(&store, local).await,
        (Reconciled::Partial, budget - 1)
    );
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(&ids[last], true)
    );
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Satisfied, 1));

    // Held: the first session is `Open` under a clock the reaper does not read.
    let (ids, store) = retained(&local, SESSIONS_PER_PASS + 1);
    let foreign = session_at(&ids[0], T - DAY, "logical", 0, "Open");
    store.kv.lock().unwrap().insert(foreign.0, foreign.1);
    for lap in 0..2 {
        assert_eq!(
            lap_pass(&store, local).await,
            (Reconciled::Blocked, budget - 1),
            "lap {lap}"
        );
        assert_eq!(
            store.value(REAPER_CURSOR_KEY),
            cursor_after(&ids[last], false),
            "lap {lap}"
        );
        assert_eq!(
            lap_pass(&store, local).await,
            (Reconciled::Partial, 1),
            "lap {lap}: a held session earlier in the lap is never certified away"
        );
    }

    // Lost: the first session is idle, and its slot is renewed between the reads and the fence.
    let (ids, store) = retained(&local, SESSIONS_PER_PASS + 1);
    let old = T - 10 * DAY;
    let idle = open(&ids[0], old);
    let lapsed = slot(&ids[0], 0, old, old + 1);
    let renewed = slot(&ids[0], 0, old, T + HOUR);
    store.kv.lock().unwrap().extend([idle, lapsed.clone()]);
    *store.interleaved.lock().unwrap() = Some(
        WriteBatch::new()
            .require(lapsed.0, lapsed.1)
            .put(renewed.0, renewed.1),
    );
    assert_eq!(
        lap_pass(&store, local).await,
        (Reconciled::Partial, budget - 1)
    );
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(&ids[last], false)
    );
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Partial, 1));
    // The renewed lease keeps the session from here on: the next lap is clean.
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Partial, budget));
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Satisfied, 1));

    // In doubt: the first session is idle, and its fence's commit comes back unknown, not landed.
    let (ids, store) = retained(&local, SESSIONS_PER_PASS + 1);
    let idle = open(&ids[0], T - 10 * DAY);
    store.kv.lock().unwrap().insert(idle.0, idle.1);
    store.unknown_at_next_commit(Unknown {
        lands: false,
        may_still_commit: true,
    });
    assert_eq!(
        lap_pass(&store, local).await,
        (Reconciled::Blocked, budget - 1)
    );
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(&ids[last], false),
        "a fence left in doubt leaves the lap unclean"
    );
    assert_eq!(
        lap_pass(&store, local).await,
        (Reconciled::Partial, 1),
        "a fence left in doubt earlier in the lap is never certified away"
    );
}

/// **What happened early in a lap is carried to its end, through every pass between.** Two
/// budgets and one session: a lap of three passes. The first fences, the second finds nothing —
/// and still leaves the cursor untagged, because the lap is not clean — so the third, which ends
/// the lap, answers `Partial`. The lap after it is clean in all three passes, and its cursor is
/// tagged after each of the first two.
#[tokio::test]
async fn a_lap_stays_unclean_through_the_passes_after_the_one_that_acted() {
    let local = profile(4_000);
    let budget = SESSIONS_PER_PASS as u64;
    let (ids, store) = retained(&local, 2 * SESSIONS_PER_PASS + 1);
    let idle = open(&ids[0], T - 10 * DAY);
    store.kv.lock().unwrap().insert(idle.0, idle.1);
    let (first_stop, second_stop) = (&ids[SESSIONS_PER_PASS - 1], &ids[2 * SESSIONS_PER_PASS - 1]);

    assert_eq!(
        lap_pass(&store, local).await,
        (Reconciled::Changed, budget - 1)
    );
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(first_stop, false)
    );
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Partial, budget));
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(second_stop, false),
        "a pass that found nothing does not clean a lap that fenced"
    );
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Partial, 1));

    assert_eq!(
        lap_pass(&store, local).await,
        (Reconciled::Partial, budget - 1)
    );
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(first_stop, true)
    );
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Partial, budget));
    assert_eq!(
        store.value(REAPER_CURSOR_KEY),
        cursor_after(second_stop, true)
    );
    assert_eq!(lap_pass(&store, local).await, (Reconciled::Satisfied, 1));
}

/// **A fault reading past the budget is a listing fault like any other.** The listing ends
/// exactly at the budget, but the one read that would show it fails. The pass judged every
/// session it listed, yet whether more are listed is unknown, so it holds: named, `Blocked` —
/// never `Satisfied`, and never a `Partial` the one-shot would exit 0 on — and no position is
/// kept at the fault, so for as long as it lasts every pass judges the budget's worth of sessions
/// from the head again.
#[tokio::test]
async fn a_fault_reading_past_the_budget_is_held_and_not_read_as_the_end() {
    let local = profile(1_500);
    let (ids, store) = retained(&local, SESSIONS_PER_PASS);
    let last = mpu_key(&ids[SESSIONS_PER_PASS - 1]);
    let store = Store {
        failing_after: Some(last.clone()),
        ..store
    };
    let before = store.snapshot();

    for pass in 0..3 {
        let (held, log) = judge(&store, &policy(local)).await;
        let held = held.expect("the profile matches");
        assert_eq!(
            (held.outcome(), held.kept, held.held),
            (Reconciled::Blocked, SESSIONS_PER_PASS as u64, 1),
            "pass {pass}: {held:?}"
        );
        assert!(
            log.lines().any(|line| {
                line.contains("hold=\"listing-unreadable\"") && line.contains(&object_name(&last))
            }),
            "pass {pass}: the hold names where the listing stopped:\n{log}"
        );
        assert_eq!(
            store.snapshot(),
            before,
            "pass {pass}: no position is kept at the fault"
        );
    }
}

/// **A listing fault on a resumed pass puts the position back at the head.** A listing longer
/// than the budget, with a page past the budget that cannot be read. The first pass of a lap
/// judges a budget's worth and leaves its position; the second resumes there, judges the page it
/// can read, holds the one it cannot, and writes the position back to the head — so the lap
/// starts over and the sessions before the fault are judged every lap, not once.
#[tokio::test]
async fn a_listing_fault_on_a_resumed_pass_puts_the_position_back_at_the_head() {
    let local = profile(1_500);
    let (ids, store) = retained(&local, SESSIONS_PER_PASS + 5);
    // The page after the first three sessions past the budget fails.
    let store = Store {
        failing_after: Some(mpu_key(&ids[SESSIONS_PER_PASS + 2])),
        ..store
    };
    let budget = SESSIONS_PER_PASS as u64;

    for lap in 0..2 {
        assert_eq!(
            lap_pass(&store, local).await,
            (Reconciled::Partial, budget),
            "lap {lap}"
        );
        assert_eq!(
            store.value(REAPER_CURSOR_KEY),
            cursor_after(&ids[SESSIONS_PER_PASS - 1], true),
            "lap {lap}"
        );
        let (resumed, log) = judge(&store, &policy(local)).await;
        let resumed = resumed.expect("the profile matches");
        assert_eq!(
            (resumed.outcome(), resumed.kept, resumed.held),
            (Reconciled::Blocked, 3, 1),
            "lap {lap}: {resumed:?}"
        );
        assert!(log.contains("listing-unreadable"), "lap {lap}: {log}");
        assert_eq!(
            store.value(REAPER_CURSOR_KEY).as_deref(),
            Some(&b""[..]),
            "lap {lap}: after a listing fault the next pass starts at the head"
        );
    }
}

/// **A listing fault that persists does not starve the sessions listed before it.** The page
/// after the third session cannot be read, on any pass. Each pass judges the three it can read,
/// holds the rest of the listing — named once, `Blocked` — and leaves the position at the head,
/// so the next pass judges those three again instead of resuming at the fault and judging none.
/// Once the clock is past `W_open` they are idle, and the pass that runs then fences them.
#[tokio::test]
async fn a_persistent_listing_fault_does_not_starve_the_sessions_before_it() {
    let local = profile(1_500);
    let (ids, store) = retained(&local, 5);
    let store = Store {
        failing_after: Some(mpu_key(&ids[2])),
        ..store
    };
    let before = store.snapshot();
    let clock = ManualClock::new(T);
    let judge_at = async |clock: &ManualClock| {
        let (pass, log) = traced_on(&store, clock, &policy(local), async |ctx| {
            pass(ctx).await.expect("mpuctl reads")
        })
        .await;
        (pass.expect("the profile matches"), log)
    };

    for pass in 0..2 {
        let (kept, log) = judge_at(&clock).await;
        assert_eq!(
            (kept.outcome(), kept.kept, kept.held, kept.fenced()),
            (Reconciled::Blocked, 3, 1, 0),
            "pass {pass}: {kept:?}"
        );
        assert_eq!(
            log.matches("hold=\"listing-unreadable\"").count(),
            1,
            "pass {pass}: the page that failed is held once, not asked for twice:\n{log}"
        );
        assert_eq!(
            store.snapshot(),
            before,
            "pass {pass}: no position is kept at the fault"
        );
    }

    // The sessions were created a day before `T`: a week on, the readable ones are idle.
    clock.advance(W_OPEN_MILLIS);
    let (fenced, log) = judge_at(&clock).await;
    assert_eq!(
        (fenced.outcome(), fenced.fenced_idle, fenced.held),
        (Reconciled::Blocked, 3, 1),
        "{fenced:?}"
    );
    assert!(log.contains("listing-unreadable"), "{log}");
    for id in &ids[..3] {
        assert_eq!(store.state(id), SessionState::Aborting {}, "{id}");
    }
    for id in &ids[3..] {
        assert_eq!(store.state(id), SessionState::Open {}, "{id} is unread");
    }
    assert_eq!(store.value(REAPER_CURSOR_KEY), None);
}

/// A stored cursor reads as the head, as a position in a clean lap or in any other, or as no
/// position at all — the tag on its own, or on a key that is not one, included.
#[test]
fn a_stored_cursor_reads_as_the_head_a_position_or_neither() {
    let key = mpu_key(&upload(0x3d));
    let tagged = [CLEAN_LAP_TAG, &key[..]].concat();
    assert_eq!(read_cursor(None), Resume::Head);
    assert_eq!(read_cursor(Some(b"")), Resume::Head);
    assert_eq!(
        read_cursor(Some(&key)),
        Resume::After {
            key: &key,
            clean: false
        }
    );
    assert_eq!(
        read_cursor(Some(&tagged)),
        Resume::After {
            key: &key,
            clean: true
        }
    );
    assert_eq!(cursor_value(&key, false), key);
    assert_eq!(cursor_value(&key, true), tagged);
    let oversized = [MPU_PREFIX, &[b'f'; LEDGER_KEY_MAX_LEN][..]].concat();
    for unusable in [
        &b"garbage"[..],
        CLEAN_LAP_TAG,
        b"clean-lap:garbage",
        &[CLEAN_LAP_TAG, &oversized[..]].concat(),
    ] {
        assert_eq!(read_cursor(Some(unusable)), Resume::Unusable);
    }
}

/// A cursor that is not an `mpu:` key is no position: the pass reads from the head, names it,
/// and rewrites it; one past the last session reads nothing and puts the cursor back at the head.
#[tokio::test]
async fn an_unusable_or_spent_cursor_reads_from_the_head() {
    let local = profile(1_500);
    let id = upload(0x3a);
    for (cursor, fenced) in [(&b"garbage"[..], 1), (&b"mpu:~"[..], 0)] {
        let store = Store::seeded([
            ledger(1, &local),
            open(&id, T - 10 * DAY),
            (REAPER_CURSOR_KEY.to_vec(), Bytes::copy_from_slice(cursor)),
        ]);

        let (pass, log) = judge(&store, &policy(local)).await;

        let pass = pass.expect("the profile matches");
        assert_eq!(pass.fenced_idle, fenced, "{pass:?}");
        assert_eq!(
            log.contains("unusable-reaper-cursor"),
            fenced == 1,
            "only the unusable one is named:\n{log}"
        );
        assert_eq!(store.value(REAPER_CURSOR_KEY).as_deref(), Some(&b""[..]));
    }
}

/// A cursor the store cannot read is a hold: the pass judges nothing, writes nothing, and the
/// step's later legs still run.
#[tokio::test]
async fn an_unreadable_cursor_is_held() {
    let local = profile(1_500);
    let store = Store {
        failing_get: Some(REAPER_CURSOR_KEY.to_vec()),
        ..Store::seeded([ledger(1, &local), open(&upload(0x3b), T - 10 * DAY)])
    };
    let before = store.snapshot();

    let (pass, log) = judge(&store, &policy(local)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!((pass.held, pass.fenced_idle), (1, 0), "{pass:?}");
    assert!(log.contains("cursor-unreadable"), "{log}");
    assert_eq!(store.snapshot(), before);
}

/// A listing page the store cannot read is held, not an error: the step's later legs still run.
#[tokio::test]
async fn an_unreadable_listing_is_held() {
    let local = profile(1_500);
    let store = Store {
        failing_prefix: Some(MPU_PREFIX.to_vec()),
        ..Store::seeded([ledger(1, &local), open(&upload(0x40), T - 40 * DAY)])
    };

    let (pass, log) = judge(&store, &policy(local)).await;

    assert_eq!(pass.expect("the profile matches").held, 1);
    assert!(log.contains("listing-unreadable"), "{log}");
}

/// The idle fence pins the slot it read: a renewal landing after the reads makes it lose, the
/// session stays `Open` with the renewed lease, and the pass is [`Reconciled::Partial`]. The same
/// renewal does not save a session past `W_session`: the ceiling pins nothing.
#[tokio::test]
async fn the_idle_fence_loses_to_a_renewal_and_the_ceiling_does_not() {
    let local = profile(1_500);
    let id = upload(0x50);
    let old = T - 10 * DAY;
    for (created, outcome) in [
        (old, Reconciled::Partial),
        (T - 40 * DAY, Reconciled::Changed),
    ] {
        let lapsed = slot(&id, 0, old, old + 1);
        let renewed = slot(&id, 0, old, T + HOUR);
        let store = Store::seeded([ledger(1, &local), open(&id, created), lapsed.clone()]);
        *store.interleaved.lock().unwrap() = Some(
            WriteBatch::new()
                .require(lapsed.0, lapsed.1)
                .put(renewed.0.clone(), renewed.1.clone()),
        );

        let (pass, log) = judge(&store, &policy(local)).await;

        let pass = pass.expect("the profile matches");
        assert_eq!(pass.outcome(), outcome, "{pass:?}");
        if outcome == Reconciled::Partial {
            assert_eq!(pass.lost, 1);
            assert_eq!(store.state(&id), SessionState::Open {});
            assert_eq!(store.value(&renewed.0), Some(renewed.1));
            assert!(log.contains("lost to evidence"), "{log}");
        } else {
            assert_eq!(pass.fenced_ceiling, 1);
            assert_eq!(store.state(&id), SessionState::Aborting {});
        }
    }
}

/// A fence whose commit fails outright — nothing in doubt — is held, never counted as fenced,
/// lost or of unknown outcome.
#[tokio::test]
async fn a_failed_fence_commit_is_held() {
    let local = profile(1_500);
    let id = upload(0x60);
    let store = Store {
        failing_commits: true,
        ..Store::seeded([ledger(1, &local), open(&id, T - 10 * DAY)])
    };

    let (pass, log) = judge(&store, &policy(local)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!(
        (pass.held, pass.fenced_idle, pass.lost, pass.unknown),
        (1, 0, 0, 0)
    );
    assert_eq!(store.state(&id), SessionState::Open {});
    assert!(log.contains("fence-commit-failed"), "{log}");
    assert!(!log.contains("reaper-fence-outcome-unknown"), "{log}");
}

/// **A fence whose outcome the store could not report is not a session left in place.** For each
/// arm, the fence's commit comes back unknown — having landed or not, and with the store saying
/// the batch may still land or not. Whichever it is, the pass counts one fence of unknown outcome
/// and nothing else: not a hold (no `reaper-session-held`, no "left a session in place"), not a
/// fence, not a lost fence. It names the session, the arm and what the store said, and is
/// `Blocked`. Nothing sends the batch again; the next pass reads which way it went — a session
/// still `Open` is judged and fenced, one already `Aborting` is the retirement leg's.
#[tokio::test]
async fn a_fence_of_unknown_outcome_is_counted_as_that_and_settled_by_the_next_pass() {
    let local = profile(1_500);
    let id = upload(0x61);
    for (created, arm) in [(T - 10 * DAY, "idle"), (T - 40 * DAY, "residency-ceiling")] {
        for (lands, may_still_commit) in
            [(true, false), (true, true), (false, false), (false, true)]
        {
            let case = format!("{arm}, lands: {lands}, may still commit: {may_still_commit}");
            let store = Store::seeded([ledger(1, &local), open(&id, created)]);
            store.unknown_at_next_commit(Unknown {
                lands,
                may_still_commit,
            });

            let (pass, log) = judge(&store, &policy(local)).await;

            let pass = pass.expect("the profile matches");
            assert_eq!(
                (
                    pass.unknown,
                    pass.held,
                    pass.fenced_idle,
                    pass.fenced_ceiling,
                    pass.lost,
                    pass.kept
                ),
                (1, 0, 0, 0, 0, 0),
                "{case}: {pass:?}"
            );
            assert_eq!(pass.outcome(), Reconciled::Blocked, "{case}");
            let signal = log
                .lines()
                .find(|line| line.contains("reaper-fence-outcome-unknown"))
                .unwrap_or_else(|| panic!("{case}: the outcome is named:\n{log}"));
            assert!(
                signal.contains(&object_name(&mpu_key(&id))),
                "{case}: {signal}"
            );
            assert!(
                signal.contains(&format!("arm=\"{arm}\"")),
                "{case}: {signal}"
            );
            assert!(
                signal.contains("an injected unknown commit result"),
                "{case}: {signal}"
            );
            assert_eq!(
                signal.contains("may still be applied AFTER"),
                may_still_commit,
                "{case}: the signal carries what the store said of the batch: {signal}"
            );
            for wrong in [
                "reaper-session-held",
                "left a session in place",
                "fence-commit-failed",
                "reaper_sessions_held",
                "fenced an abandoned upload",
            ] {
                assert!(!log.contains(wrong), "{case}: `{wrong}` in:\n{log}");
            }
            for counter in [
                "reaper_fences_unknown=1",
                "reaper_sessions_fenced_idle=0",
                "reaper_sessions_fenced_ceiling=0",
                "reaper_fences_lost=0",
            ] {
                assert!(
                    log.contains(&format!("monotonic_counter.{counter}")),
                    "{case}: {counter}:\n{log}"
                );
            }
            let landed = if lands {
                SessionState::Aborting {}
            } else {
                SessionState::Open {}
            };
            assert_eq!(store.state(&id), landed, "{case}");

            // The next pass, the store answering again: it reads which way the fence went.
            let (next, log) = judge(&store, &policy(local)).await;
            let next = next.expect("the profile matches");
            assert_eq!(next.unknown, 0, "{case}: {next:?}");
            assert_eq!(
                next.fenced(),
                u64::from(!lands),
                "{case}: a session still Open is fenced, one already fenced is not judged: {next:?}"
            );
            assert_eq!(store.state(&id), SessionState::Aborting {}, "{case}");
            assert!(
                !log.contains("reaper-fence-outcome-unknown"),
                "{case}: {log}"
            );
        }
    }
}

/// **A fence that landed unreported is torn down by the same step, and is never called left in
/// place.** The idle session's fence lands, and the store answers that it cannot say. Through the
/// real step — the reaper, then the retirement leg record-only — the leg finds the session
/// `Aborting` and starts its teardown at once: the committed part's positions are marked. So the
/// step's signals must not say the session was left in place, and its tallies must not be silent
/// about the fence: one fence of unknown outcome, the step `Blocked`.
#[tokio::test]
async fn a_fence_that_landed_unreported_is_drained_in_the_same_step() {
    let local = profile(1_500);
    let id = upload(0x62);
    let mut records = vec![ledger(1, &local), open(&id, T - 10 * DAY)];
    records.extend(committed(&id, 1, &chunk(0x62_0001), T - 10 * DAY));
    let store = Store::seeded(records);
    store.unknown_at_next_commit(Unknown {
        lands: true,
        may_still_commit: false,
    });

    let (outcome, log) = step_logged(&store, &policy(local)).await;

    assert_eq!(outcome, Reconciled::Blocked);
    let marks = store
        .snapshot()
        .keys()
        .filter(|key| key.starts_with(b"orphan:"))
        .count();
    assert_eq!(
        marks, 3,
        "the retirement leg marked the fenced upload's committed part in this step:\n{log}"
    );
    assert_eq!(
        log.matches("reaper-fence-outcome-unknown").count(),
        1,
        "{log}"
    );
    for wrong in [
        "reaper-session-held",
        "left a session in place",
        "reaper_sessions_held",
    ] {
        assert!(!log.contains(wrong), "`{wrong}` in:\n{log}");
    }
    for counter in [
        "reaper_passes=1",
        "reaper_fences_unknown=1",
        "reaper_sessions_fenced_idle=0",
        "reaper_fences_lost=0",
    ] {
        assert!(
            log.contains(&format!("monotonic_counter.{counter}")),
            "{counter}:\n{log}"
        );
    }
}

/// **A late refusal counts the fences of unknown outcome beside the ones that committed.** Two
/// idle sessions: the first one's fence lands unreported, the second's commits. Then the ledger
/// is gone at the pass's last read, so the step is refused after the pass acted. The signal says
/// both numbers — a fence the store could not report may be a fenced session too — and the tallies
/// go out; nothing is drained.
#[tokio::test]
async fn a_late_refusal_counts_the_fences_of_unknown_outcome() {
    let local = profile(1_500);
    let (first, second) = (upload(0x63), upload(0x64));
    let store = Store::seeded([
        ledger(2, &local),
        open(&first, T - 10 * DAY),
        open(&second, T - 10 * DAY),
    ]);
    store.unknown_at_next_commit(Unknown {
        lands: true,
        may_still_commit: false,
    });
    store.interleave_at(
        LedgerRead::BeforeSecond,
        Interleave::Batch(WriteBatch::new().delete(MPUCTL_KEY.to_vec())),
    );

    let (outcome, log) = step_logged(&store, &policy(local)).await;

    assert_eq!(outcome, Reconciled::Blocked);
    for id in [&first, &second] {
        assert_eq!(store.state(id), SessionState::Aborting {});
    }
    assert!(!log.contains("retire-"), "the retirement leg ran:\n{log}");
    assert!(
        log.contains(
            "stopped after fencing 1 session(s), and after 1 fence(s) whose outcome the store \
             could not report"
        ),
        "{log}"
    );
    for counter in [
        "reaper_profile_refusals=1",
        "reaper_sessions_fenced_idle=1",
        "reaper_fences_unknown=1",
    ] {
        assert!(
            log.contains(&format!("monotonic_counter.{counter}")),
            "{counter}:\n{log}"
        );
    }
}

/// The widest slot record the codec spells: every field at its widest rendering.
fn widest_slot() -> Bytes {
    let json = format!(
        "{{\"part_number\":{MAX_PART_NUMBER},\"attempt_id\":\"{}\",\
         \"reserved_at_millis\":{},\"lease_expiry_millis\":{}}}",
        "f".repeat(TOKEN_HEX_LEN),
        u64::MAX - 1,
        u64::MAX
    );
    canonical!(SlotRecord, json)
}

/// **The largest `Open` session Create admits**, as a later fence finds it: its upload id, its
/// record's bytes and the record.
///
/// Create measures a session in the widest state it can later reach, with the real encoder, and
/// refuses one that would cross [`MAX_VALUE_BYTES`] there (`SessionRecord::widest_reachable_bytes`,
/// `crates/core/src/multipart/create.rs`). So the ceiling is found by asking the production
/// Create: a content type far too long is refused with the size it measured, one byte of content
/// type is one byte of record, and the length that measures exactly the limit follows. It is
/// checked both ways — that length is admitted, one byte more is refused. The object name is
/// short and every character of its key is one the encoder escapes, so the widest state is
/// `Completed`, which carries the name once: a long name would bind at `Completing`, which
/// carries it twice, and leave an `Open` record half the size.
///
/// What Create admitted is `Open@0` with no Complete attempt spent. A fence can find the same
/// session after rollbacks, at any epoch that still has a successor and any attempt count — the
/// numbers Create's headroom already paid for — so the record is re-spelled at the widest of both.
async fn largest_admitted_session() -> (UploadId, Bytes, SessionRecord) {
    let shipped = Budget::shipped();
    let id = UploadId::new("f".repeat(TOKEN_HEX_LEN)).expect("an upload id");
    // A quote, a backslash and a control character: two, two and six bytes once encoded.
    let object = format!("{BUCKET}/\"\\\u{1}");
    let attempt = async |content_type_len: usize| {
        let store = Store::seeded([bucket()]);
        let content_type = "t".repeat(content_type_len);
        let outcome = create_as(
            &store,
            &shipped,
            &id,
            &object,
            Some(&content_type),
            T - 10 * DAY,
        )
        .await;
        (outcome, store)
    };
    let too_large = |outcome: CreateOutcome| match outcome {
        CreateOutcome::Refused(Refusal::SessionTooLarge { bytes, limit }) => {
            assert_eq!(limit, MAX_VALUE_BYTES as u64);
            bytes
        }
        other => panic!("refused as too large: {other:?}"),
    };

    let (far_over, _) = attempt(MAX_VALUE_BYTES).await;
    let over_by = too_large(far_over) - MAX_VALUE_BYTES as u64;
    let ceiling = MAX_VALUE_BYTES - usize::try_from(over_by).unwrap();
    let (one_more, _) = attempt(ceiling + 1).await;
    assert_eq!(
        too_large(one_more),
        MAX_VALUE_BYTES as u64 + 1,
        "one byte past the ceiling is refused"
    );
    let (admitted, store) = attempt(ceiling).await;
    assert!(
        matches!(admitted, CreateOutcome::Created { .. }),
        "{admitted:?}"
    );

    let created = store
        .value(&mpu_key(&id))
        .expect("the session Create wrote");
    let created = std::str::from_utf8(&created).expect("a record is JSON");
    assert!(
        created.contains(r#""object":"bucket/\"\\\u0001""#),
        "the object name is stored escaped"
    );
    let as_created = "\"epoch\":0,\"attempts\":0,";
    assert_eq!(created.matches(as_created).count(), 1);
    let widest = created.replace(
        as_created,
        &format!("\"epoch\":{},\"attempts\":{},", u64::MAX - 1, u32::MAX),
    );
    let bytes = Bytes::from(widest);
    let record = decode_session_record(&bytes).expect("the same session, later in its life");
    (id, bytes, record)
}

/// **The largest fence this slice builds fits both batch budgets.** The idle arm's fence over the
/// largest `Open` session Create admits ([`largest_admitted_session`]), pinning every one of the
/// `MAX_INFLIGHT_PARTS` indices of the profile the profile check admits (the shipped one) with
/// the widest slot record. It costs exactly what its parts add up to — counted here from the
/// parts, not by [`fence_cost`] — and that is inside [`MAX_BATCH_OPS`] and [`MAX_BATCH_BYTES`].
/// So is the most any admitted session's fence could cost: two session values at the value
/// ceiling itself.
///
/// The production pass then fences that session: seeded idle, every slot index reserved and
/// lapsed, it is moved to `Aborting` by the idle arm and not held for its size.
#[tokio::test]
async fn largest_fence_fits_both_batch_budgets() {
    let policy = ReaperPolicy::default();
    assert_eq!(policy.profile.max_inflight_parts(), MAX_INFLIGHT_PARTS);
    let (upload, read, record) = largest_admitted_session().await;
    let teardown = record
        .open_teardown(&upload)
        .expect("an Open session fences");
    let fenced = metadata::encode(teardown.session());
    // Both session values the fence carries are within one value, and near it: the record is
    // the large one Create admits, not a short stand-in.
    for (name, value) in [("the record read", &read), ("the record put", &fenced)] {
        assert!(value.len() <= MAX_VALUE_BYTES, "{name}: {}", value.len());
        assert!(
            value.len() > MAX_VALUE_BYTES - 1_000,
            "{name} is at the admission ceiling: {}",
            value.len()
        );
    }
    let slot = widest_slot();
    let slots: Vec<SlotSeen> = (0..MAX_INFLIGHT_PARTS)
        .map(|index| SlotSeen {
            index,
            bytes: slot.to_vec(),
            reserved_at_millis: 0,
            lease_expiry_millis: 0,
        })
        .collect();
    let session_key = mpu_key(&upload);
    let batch = fence_batch(
        &session_key,
        &read,
        teardown.session(),
        slice::from_ref(teardown.obligation()),
    );
    let batch = pin_slots(batch, &upload, MAX_INFLIGHT_PARTS, &slots).expect("pins");

    // Counted from the parts: the session's require and put, the obligation's require_absent and
    // put, and one require per slot index.
    let obligation_key = teardown.obligation().key();
    let obligation = metadata::encode(teardown.obligation().payload());
    let pins: usize = (0..MAX_INFLIGHT_PARTS)
        .map(|index| slot_key(&upload, SlotIndex::new(index).unwrap()).len() + slot.len())
        .sum();
    let expected_ops = 1 + 1 + 2 + u64::from(MAX_INFLIGHT_PARTS);
    let expected_bytes = (session_key.len() + read.len())
        + obligation_key.len()
        + (session_key.len() + fenced.len())
        + (obligation_key.len() + obligation.len())
        + pins;
    assert_eq!(expected_ops, 20);
    assert_eq!(fence_cost(&batch), (expected_ops, expected_bytes as u64));
    assert!(expected_ops <= u64::from(MAX_BATCH_OPS));
    assert!(expected_bytes as u64 <= MAX_BATCH_BYTES);
    assert!(within_budgets(expected_ops, expected_bytes as u64));
    // Whatever its shape, an admitted session's two values are at most one value each.
    let any_session = 2 * (session_key.len() + MAX_VALUE_BYTES)
        + 2 * obligation_key.len()
        + obligation.len()
        + pins;
    assert!(expected_bytes <= any_session);
    assert!(any_session as u64 <= MAX_BATCH_BYTES);
    // Every pin is a whole slot key at the widest index the key space spells.
    assert!(SLOT_INDEX_WIDTH >= MAX_INFLIGHT_PARTS.to_string().len());

    // The pass itself, over that session: idle, every slot index reserved and lapsed.
    let old = T - 10 * DAY;
    let store = Store::seeded(
        (0..MAX_INFLIGHT_PARTS)
            .map(|index| self::slot(&upload, index, old, old + 1))
            .chain([
                ledger(1, &policy.profile),
                (session_key.clone(), read.clone()),
            ]),
    );
    let (pass, log) = judge(&store, &policy).await;
    let pass = pass.expect("the profile matches");
    assert_eq!((pass.fenced_idle, pass.held), (1, 0), "{pass:?}\n{log}");
    assert_eq!(store.value(&session_key), Some(fenced));
}

/// Each batch budget admits its own limit and refuses one past it.
#[test]
fn within_budgets_includes_each_limit() {
    let (ops, bytes) = (u64::from(MAX_BATCH_OPS), MAX_BATCH_BYTES);
    assert!(within_budgets(ops, bytes));
    assert!(!within_budgets(ops + 1, bytes));
    assert!(!within_budgets(ops, bytes + 1));
}

/// The idle arm pins every index of the key space: the present ones to their bytes, the free
/// ones absent — never only the ones it saw.
#[test]
fn idle_pins_cover_the_whole_slot_key_space() {
    let upload = upload(0xaa);
    let seen = [SlotSeen {
        index: 3,
        bytes: b"slot-three".to_vec(),
        reserved_at_millis: 0,
        lease_expiry_millis: 0,
    }];
    let batch = pin_slots(WriteBatch::new(), &upload, 5, &seen).expect("pins");
    let pins: Vec<(Vec<u8>, Option<Vec<u8>>)> = batch
        .preconditions
        .iter()
        .map(|pre| (pre.key.clone(), pre.expected.as_ref().map(|v| v.to_vec())))
        .collect();
    let expected: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..5)
        .map(|index| {
            let key = slot_key(&upload, SlotIndex::new(index).unwrap());
            (key, (index == 3).then(|| b"slot-three".to_vec()))
        })
        .collect();
    assert_eq!(pins, expected);
}

/// One fenced step over `store`, as the operator one-shot runs it: the reaper, then the retirement
/// leg record-only — what [`crate::reconcile_step`] answers, and what the step emitted.
async fn step_logged(store: &Store, policy: &ReaperPolicy) -> (Reconciled, String) {
    let (outcome, log) = try_step_logged(store, policy).await;
    (outcome.expect("the step runs"), log)
}

/// [`step_logged`], for a step that may fail.
async fn try_step_logged(
    store: &Store,
    policy: &ReaperPolicy,
) -> (
    std::result::Result<Reconciled, crate::ReconcileError>,
    String,
) {
    let coord = wyrd_coordination_mem::MemCoordination::new();
    let leader = crate::Custodian::elect(&coord, "zone-reaper")
        .await
        .unwrap();
    let mut zone = crate::FencedZone::new();
    zone.install(leader.leadership());
    traced(store, policy, async |ctx| {
        crate::reconcile_step(&zone, &leader, None, None, None, None, Some(ctx), T).await
    })
    .await
}

/// An upload already `Aborting` with its `{session, all}` obligation and one committed part: the
/// retirement leg's work, if it runs.
fn aborting_with_obligation(id: &UploadId) -> Vec<(Vec<u8>, Bytes)> {
    let open = decode_session_record(&open(id, T - 10 * DAY).1).unwrap();
    let teardown = open.open_teardown(id).unwrap();
    let obligation = teardown.obligation();
    vec![
        (mpu_key(id), metadata::encode(teardown.session())),
        (obligation.key(), metadata::encode(obligation.payload())),
        psum(id, 1, T - 10 * DAY),
    ]
}

/// **A lost ledger refuses the step: the drain writes nothing.** An upload already `Aborting`
/// with its `{session, all}` obligation, a committed part and a crashed part's owned residue —
/// every piece of it work the retirement leg does — and no `mpuctl`. Every Create writes the
/// ledger with its session, so the profile this upload was admitted under is unknown, and the
/// step is refused as it is for a ledger that does not decode: the operator signal, `Blocked`,
/// and the store byte for byte as it was — no mark, no deletion.
#[tokio::test]
async fn a_lost_ledger_refuses_the_step_and_the_drain_writes_nothing() {
    let local = profile(1_500);
    let ended = upload(0x75);
    let open = decode_session_record(&open(&ended, T - 14 * DAY).1).unwrap();
    let teardown = open.open_teardown(&ended).unwrap();
    let obligation = teardown.obligation();
    let mut records = vec![
        (mpu_key(&ended), metadata::encode(teardown.session())),
        (obligation.key(), metadata::encode(obligation.payload())),
        owned(&ended, 2, 0x75_0002, T - 9 * DAY),
    ];
    records.extend(committed(&ended, 1, &chunk(0x75_0001), T - 10 * DAY));
    let store = Store::seeded(records);
    let before = store.snapshot();

    let (outcome, log) = step_logged(&store, &policy(local)).await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert_eq!(store.snapshot(), before, "nothing marked, nothing deleted");
    assert!(log.contains("reaper-profile-mismatch"), "{log}");
    assert!(log.contains("mpuctl is absent"), "{log}");
}

/// **A ledger lost while the pass ran refuses the step: the drain writes nothing.** The ledger
/// matches at the pass's first read, and is deleted just before its last — no writer does that, so
/// it is lost. Beside it sit an upload already `Aborting` with its `{session, all}` obligation, a
/// committed part and a crashed part's owned residue, all of it work the retirement leg would do,
/// and an idle `Open` session. The pass fenced the idle one under the profile its first read
/// matched; its last read then refuses the step, so the leg does not run: the operator signal,
/// `Blocked`, no mark anywhere, and every record of both uploads as the pass left it.
#[tokio::test]
async fn a_ledger_lost_during_the_pass_refuses_the_step_and_the_drain_writes_nothing() {
    let local = profile(1_500);
    let (ended, idle) = (upload(0x77), upload(0x78));
    let teardown_of = |id: &UploadId| {
        decode_session_record(&open(id, T - 14 * DAY).1)
            .unwrap()
            .open_teardown(id)
            .unwrap()
    };
    let teardown = teardown_of(&ended);
    let obligation = teardown.obligation();
    let mut records = vec![
        ledger(2, &local),
        (mpu_key(&ended), metadata::encode(teardown.session())),
        (obligation.key(), metadata::encode(obligation.payload())),
        owned(&ended, 2, 0x77_0002, T - 9 * DAY),
        open(&idle, T - 14 * DAY),
    ];
    records.extend(committed(&ended, 1, &chunk(0x77_0001), T - 10 * DAY));
    records.extend(committed(&idle, 1, &chunk(0x78_0001), T - 10 * DAY));
    let store = Store::seeded(records);
    store.interleave_at(
        LedgerRead::BeforeSecond,
        Interleave::Batch(WriteBatch::new().delete(MPUCTL_KEY.to_vec())),
    );
    // What the pass alone leaves: the ledger gone, and the idle session fenced with its
    // obligation, as the first read's profile allowed.
    let fenced = teardown_of(&idle);
    let mut expected = store.snapshot();
    expected.remove(MPUCTL_KEY);
    expected.insert(mpu_key(&idle), metadata::encode(fenced.session()));
    expected.insert(
        fenced.obligation().key(),
        metadata::encode(fenced.obligation().payload()),
    );

    let (outcome, log) = step_logged(&store, &policy(local)).await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert_eq!(
        store.snapshot(),
        expected,
        "nothing marked, nothing deleted, nothing drained"
    );
    assert!(log.contains("reaper-profile-mismatch"), "{log}");
    assert!(log.contains("absent at its last read"), "{log}");
    assert!(!log.contains("retire-"), "the retirement leg ran:\n{log}");
    // The refusal came after the pass had acted, and says so: the fence it committed is named
    // in the signal and counted on the durability seam, as every other pass's are.
    assert!(
        log.contains("stopped after fencing 1 session(s)"),
        "the signal says what the pass did before it stopped:\n{log}"
    );
    assert!(
        !log.contains("refused before acting"),
        "a pass that fenced did not refuse before acting:\n{log}"
    );
    for counter in [
        "reaper_profile_refusals=1",
        "reaper_passes=1",
        "reaper_sessions_fenced_idle=1",
        "reaper_sessions_fenced_ceiling=0",
        "reaper_fences_lost=0",
        "reaper_fences_unknown=0",
        "reaper_sessions_kept=0",
    ] {
        assert!(
            log.contains(&format!("monotonic_counter.{counter}")),
            "{counter} goes out though the step was refused:\n{log}"
        );
    }
}

/// **A fault at the pass's last read of the ledger still reports what the pass did.** The ledger
/// matches at the first read; the pass fences an idle session and keeps a fresh one; then its last
/// read of `mpuctl` fails. The step fails with that fault and the retirement leg does not run —
/// the profile is unknown — but the fence is committed, so the pass's tallies go out on the
/// durability seam all the same: a fence nothing counted would be an abort no metric saw.
#[tokio::test]
async fn a_fault_at_the_last_ledger_read_fails_the_step_with_the_pass_counted() {
    let local = profile(1_500);
    let (ended, idle, fresh) = (upload(0x7b), upload(0x7c), upload(0x7d));
    let mut records = vec![
        ledger(3, &local),
        open(&idle, T - 10 * DAY),
        open(&fresh, T - DAY),
    ];
    records.extend(aborting_with_obligation(&ended));
    let store = Store {
        failing_ledger_read: Some(2),
        ..Store::seeded(records)
    };
    // What the pass alone leaves: the idle session fenced with its obligation.
    let fenced = decode_session_record(&open(&idle, T - 10 * DAY).1)
        .unwrap()
        .open_teardown(&idle)
        .unwrap();
    let mut expected = store.snapshot();
    expected.insert(mpu_key(&idle), metadata::encode(fenced.session()));
    expected.insert(
        fenced.obligation().key(),
        metadata::encode(fenced.obligation().payload()),
    );

    let (outcome, log) = try_step_logged(&store, &policy(local)).await;

    let fault = outcome.expect_err("the last read of mpuctl failed");
    assert!(
        fault.to_string().contains("an injected read fault"),
        "{fault}"
    );
    assert_eq!(store.snapshot(), expected, "fenced, and nothing drained");
    assert!(!log.contains("retire-"), "the retirement leg ran:\n{log}");
    assert!(!log.contains("reaper-profile-mismatch"), "{log}");
    for counter in [
        "reaper_passes=1",
        "reaper_sessions_fenced_idle=1",
        "reaper_sessions_fenced_ceiling=0",
        "reaper_fences_lost=0",
        "reaper_fences_unknown=0",
        "reaper_sessions_kept=1",
    ] {
        assert!(
            log.contains(&format!("monotonic_counter.{counter}")),
            "{counter} goes out though the step failed:\n{log}"
        );
    }
}

/// **With no fleet, what could only be retired by chunk id is held, unchanged.** The one-shot's
/// step — the reaper and the retirement leg record-only — over two uploads already `Aborting`,
/// each with its `{session, all}` obligation. One holds an owned entry that does not decode, the
/// other a committed part naming two D servers for a scheme of three fragments (two uploads,
/// because an upload's obligation waits for its owned residue, so one upload shows only the
/// first). Neither record says where its bytes are, so retiring them means finding them by chunk
/// id on the whole fleet, and this pass lists no D server. Each is held as `partial-fleet` and
/// named — the entry by the session leg, the part by the obligation walk — the step is `Blocked`,
/// and the store is byte for byte as it was: no record deleted on the strength of a listing of
/// nothing, and no mark.
#[tokio::test]
async fn a_record_only_step_holds_what_only_a_fleet_listing_could_retire() {
    let local = profile(1_500);
    let (residue, damaged) = (upload(0x79), upload(0x7a));
    let aborting = |id: &UploadId| {
        let open = decode_session_record(&open(id, T - 14 * DAY).1).unwrap();
        let teardown = open.open_teardown(id).unwrap();
        let obligation = teardown.obligation();
        [
            (mpu_key(id), metadata::encode(teardown.session())),
            (obligation.key(), metadata::encode(obligation.payload())),
        ]
    };
    let undecodable = sidx_key(&residue, part(2), 0x79_0002);
    let short = ChunkRef {
        placement: vec![1, 2],
        ..chunk(0x7a_0001)
    };
    let mut records = vec![ledger(2, &local), (undecodable.clone(), garbage())];
    records.extend(aborting(&residue));
    records.extend(aborting(&damaged));
    records.extend(committed(&damaged, 1, &short, T - 10 * DAY));
    let store = Store::seeded(records);
    let before = store.snapshot();

    for pass in 0..2 {
        let (outcome, log) = step_logged(&store, &policy(local)).await;

        assert_eq!(outcome, Reconciled::Blocked, "pass {pass}");
        assert_eq!(
            store.snapshot(),
            before,
            "pass {pass}: nothing marked, nothing deleted"
        );
        let held = |action: &str, record: &[u8]| {
            log.lines().any(|line| {
                line.contains(action)
                    && line.contains("hold=\"partial-fleet\"")
                    && line.contains("a record-only pass")
                    && line.contains(&object_name(record))
            })
        };
        assert!(
            held("retire-session-held", &undecodable),
            "pass {pass}: the owned entry is held for the fleet:\n{log}"
        );
        assert!(
            held("retire-obligation-held", &part_key(&damaged, part(1))),
            "pass {pass}: the part is held for the fleet:\n{log}"
        );
    }
}

/// **A first Create during the pass, under another profile, refuses the step.** A fresh store: no
/// ledger, no session. Right after the pass's first read of `mpuctl` the production Create runs,
/// under a profile that is not this custodian's, and installs the ledger and its session in one
/// batch. The pass judges nothing — no session existed when it read the ledger — and its last
/// read finds the mismatch, so the step is refused: the signal, `Blocked`, and the store is what
/// that Create alone leaves, byte for byte.
#[tokio::test]
async fn a_first_create_during_the_pass_under_another_profile_refuses_the_step() {
    let (local, other) = (profile(1_500), profile(3_000));
    let id = upload(0x70);
    let store = Store::seeded([bucket()]);
    store.interleave_at(
        LedgerRead::AfterFirst,
        Interleave::Create(other, id.clone()),
    );
    let created_alone = Store::seeded([bucket()]);
    create(&created_alone, &other, &id).await;

    let (outcome, log) = step_logged(&store, &policy(local)).await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert!(log.contains("at the pass's last read"), "{log}");
    // It judged no session under the absent ledger, so there was no pass to stop.
    assert!(log.contains("refused before acting"), "{log}");
    assert!(!log.contains("reaper_passes"), "{log}");
    assert_eq!(
        store.snapshot(),
        created_alone.snapshot(),
        "nothing but the Create moved"
    );
    assert_eq!(store.state(&id), SessionState::Open {});
}

/// **A first Create during the pass, under this profile, is the next pass's.** The same race under
/// the custodian's own profile is a fresh session, not a mismatch: the pass leaves it, holds
/// nothing, and certifies nothing — `Partial`, whether the Create landed before the listing was
/// read or after it was read empty. The next pass reads the ledger first and judges the session:
/// fresh, so it keeps it.
#[tokio::test]
async fn a_first_create_during_the_pass_under_this_profile_is_judged_next_pass() {
    let local = profile(1_500);
    for (at, id) in [
        (LedgerRead::AfterFirst, upload(0x72)),
        (LedgerRead::BeforeSecond, upload(0x76)),
    ] {
        let store = Store::seeded([bucket()]);
        store.interleave_at(at, Interleave::Create(local, id.clone()));

        let (pass, log) = judge(&store, &policy(local)).await;

        let pass = pass.expect("the profile matches at the pass's last read");
        assert_eq!(pass.outcome(), Reconciled::Partial, "{at:?}: {pass:?}");
        assert!(!log.contains("reaper-session-held"), "{log}");
        let (next, _) = judge(&store, &policy(local)).await;
        let next = next.expect("matches");
        assert_eq!((next.kept, next.held), (1, 0), "{at:?}: {next:?}");
        assert_eq!(store.state(&id), SessionState::Open {});
    }
}

/// With no ledger, an empty listing is the whole store, read: `Satisfied`. A listing that holds a
/// session while the ledger is still absent at the pass's last read — or one the store cannot
/// read — refuses the pass: the profile its sessions were admitted under is unknown.
#[tokio::test]
async fn with_no_ledger_an_empty_store_is_satisfied_and_a_session_refuses_the_pass() {
    let local = profile(1_500);
    let (reaped, log) = traced(&Store::default(), &policy(local), async |ctx| {
        reconcile(ctx).await.unwrap()
    })
    .await;
    assert_eq!(reaped, Reaped::Ran(Reconciled::Satisfied));
    assert!(log.contains("monotonic_counter.reaper_passes=1"), "{log}");

    let id = upload(0x73);
    let store = Store::seeded([open(&id, T - 40 * DAY)]);
    let before = store.snapshot();
    let (pass, log) = judge(&store, &policy(local)).await;
    assert_eq!(pass, None, "refused");
    assert!(log.contains("reaper-profile-mismatch"), "{log}");
    assert!(log.contains("mpuctl is absent, yet mpu: lists"), "{log}");
    assert!(log.contains("refused before acting"), "{log}");
    assert!(!log.contains("reaper_passes"), "{log}");
    assert_eq!(store.snapshot(), before);

    let store = Store {
        failing_prefix: Some(MPU_PREFIX.to_vec()),
        ..Store::default()
    };
    let (pass, log) = judge(&store, &policy(local)).await;
    assert_eq!(pass, None, "refused");
    assert!(log.contains("could not be read"), "{log}");

    // Under a ledger installed during the pass, an unreadable listing is one session's hold.
    let store = Store {
        failing_prefix: Some(MPU_PREFIX.to_vec()),
        ..Store::seeded([bucket()])
    };
    store.interleave_at(
        LedgerRead::BeforeSecond,
        Interleave::Batch(WriteBatch::new().put(ledger(0, &local).0, ledger(0, &local).1)),
    );
    let (pass, log) = judge(&store, &policy(local)).await;
    assert_eq!(pass.expect("the profile matches").held, 1);
    assert!(log.contains("listing-unreadable"), "{log}");
}

/// A ledger replaced under another profile while the pass ran refuses the step at the pass's last
/// read, though the first read matched: the `Aborting` upload's obligation is left for a step the
/// profile check admits.
#[tokio::test]
async fn a_ledger_replaced_during_the_pass_refuses_the_step() {
    let local = profile(1_500);
    let ended = upload(0x74);
    let store = Store::seeded(
        [ledger(1, &local)]
            .into_iter()
            .chain(aborting_with_obligation(&ended)),
    );
    let replaced = ledger(1, &profile(3_000));
    store.interleave_at(
        LedgerRead::AfterFirst,
        Interleave::Batch(WriteBatch::new().put(replaced.0.clone(), replaced.1.clone())),
    );
    let mut expected = store.snapshot();
    expected.insert(replaced.0, replaced.1);

    let (outcome, log) = step_logged(&store, &policy(local)).await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert_eq!(store.snapshot(), expected);
    // The pass ran before the refusal, and had fenced nothing: both are said.
    assert!(log.contains("at the pass's last read"), "{log}");
    assert!(log.contains("stopped after fencing 0 session(s)"), "{log}");
    assert!(log.contains("monotonic_counter.reaper_passes=1"), "{log}");
}

/// **An obligation key already taken beside the unchanged session is damage, not contention.**
/// The idle fence loses because its `require_absent` on the obligation key fails, while the
/// session is exactly as read — a state no writer leaves. Classified by the client Abort's own
/// classifier, the session is held and named, never counted as a fence that lost to moved
/// evidence, and the pass is `Blocked`.
#[tokio::test]
async fn an_obligation_key_taken_beside_an_unchanged_session_is_held() {
    let local = profile(1_500);
    let id = upload(0x61);
    let session = open(&id, T - 10 * DAY);
    let stray = decode_session_record(&session.1)
        .unwrap()
        .open_teardown(&id)
        .unwrap()
        .obligation()
        .key();
    let store = Store::seeded([ledger(1, &local), session, (stray, garbage())]);

    let (pass, log) = judge(&store, &policy(local)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!(
        (pass.held, pass.lost, pass.fenced_idle),
        (1, 0, 0),
        "{pass:?}"
    );
    assert_eq!(pass.outcome(), Reconciled::Blocked);
    assert!(log.contains("obligation-key-taken"), "{log}");
    assert_eq!(store.state(&id), SessionState::Open {});
}

/// **A cursor past the backend's key limit is never handed to it.** Stored, it is a legal value;
/// asked for as a page's `after` key, the backend refuses it — on every pass, before the cursor
/// could move. The pass names it, reads from the head, fences the idle session there, and puts the
/// cursor back at the head. A cursor exactly at the limit is a position.
#[tokio::test]
async fn an_oversized_cursor_is_reset_and_never_handed_to_the_store() {
    let local = profile(1_500);
    let id = upload(0x3c);
    let at_limit = [MPU_PREFIX, &[b'f'; LEDGER_KEY_MAX_LEN - 4][..]].concat();
    let oversized = [&at_limit[..], b"f"].concat();
    assert!(is_resumable_cursor(&at_limit));
    assert!(!is_resumable_cursor(&oversized));
    let store = Store {
        max_scan_key: Some(LEDGER_KEY_MAX_LEN),
        ..Store::seeded([
            ledger(1, &local),
            open(&id, T - 10 * DAY),
            (REAPER_CURSOR_KEY.to_vec(), Bytes::from(oversized)),
        ])
    };

    let (pass, log) = judge(&store, &policy(local)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!((pass.fenced_idle, pass.held), (1, 0), "{pass:?}");
    assert!(log.contains("unusable-reaper-cursor"), "{log}");
    assert!(log.contains("len=10001"), "named by its length: {log}");
    assert_eq!(store.value(REAPER_CURSOR_KEY).as_deref(), Some(&b""[..]));
    assert_eq!(store.state(&id), SessionState::Aborting {});
}

/// A clock that answers its readings in turn, then the last one for good.
struct Readings(Mutex<Vec<u64>>);

impl Clock for Readings {
    fn now_millis(&self) -> u64 {
        let mut left = self.0.lock().unwrap();
        if left.len() > 1 {
            left.remove(0)
        } else {
            left[0]
        }
    }
}

/// **A stamp ahead of the reaper's clock is held, not judged.** A session created, or progressing,
/// more than the clock-skew budget after the reaper's reading was stamped by a clock that is not
/// this one; judged, it would be kept for as long as the stamp is ahead. Exactly at the budget it
/// is judged (kept: nothing is old). The progress stamps are measured against a reading taken
/// after the ranges, so a part that commits while they are read is never one from the future.
#[tokio::test]
async fn a_stamp_ahead_of_the_clock_is_held() {
    let local = profile(1_500);
    let edge = T + DELTA_CLOCK_MILLIS;
    let (created_at_edge, created_past) = (upload(0x80), upload(0x81));
    let (psum_at_edge, psum_past, reserved_past) = (upload(0x82), upload(0x83), upload(0x84));
    let old = T - 10 * DAY;
    let store = Store::seeded([
        ledger(5, &local),
        open(&created_at_edge, edge),
        open(&created_past, edge + 1),
        open(&psum_at_edge, old),
        psum(&psum_at_edge, 1, edge),
        open(&psum_past, old),
        psum(&psum_past, 1, edge + 1),
        open(&reserved_past, old),
        slot(&reserved_past, 0, edge + 1, edge + 2),
    ]);

    let (first, log) = judge(&store, &policy(local)).await;

    let first = first.expect("the profile matches");
    assert_eq!(
        (first.held, first.kept, first.fenced_idle),
        (3, 2, 0),
        "{first:?}"
    );
    assert_eq!(log.matches("stamp-ahead-of-clock").count(), 3, "{log}");
    assert!(log.contains("created_at_millis"), "{log}");
    assert!(log.contains("the progress maximum"), "{log}");

    // A part committed while the ranges were read: ahead of the reading before them, not of the
    // one after.
    let id = upload(0x85);
    let store = Store::seeded([
        ledger(1, &local),
        open(&id, old),
        psum(&id, 1, T + 5 * DELTA_CLOCK_MILLIS),
    ]);
    let clock = Readings(Mutex::new(vec![T, T + 10 * DELTA_CLOCK_MILLIS]));
    let (judged, _) = traced_on(&store, &clock, &policy(local), async |ctx| {
        pass(ctx).await.unwrap()
    })
    .await;
    let judged = judged.expect("the profile matches");
    assert_eq!((judged.held, judged.kept), (0, 1), "{judged:?}");
}

/// The slot range is read in pages: a live lease past the first page keeps the session, as it
/// would on the first. (Four slot indices, three to a page.)
#[tokio::test]
async fn a_live_slot_past_the_first_page_keeps_the_session() {
    let wide: Budget = metadata::decode(
        b"{\"w_ref\":1500,\"max_part_chunks\":3,\"max_parts_per_session\":5,\
          \"max_inflight_parts\":5,\"max_staged_chunks\":4}",
    )
    .expect("a legal profile");
    let id = upload(0x90);
    let old = T - 10 * DAY;
    let store = Store::seeded([
        ledger(1, &wide),
        open(&id, old),
        slot(&id, 0, old, old + 1),
        slot(&id, 1, old, old + 1),
        slot(&id, 2, old, old + 1),
        slot(&id, 3, old, T + 1),
    ]);

    let (pass, _) = judge(&store, &policy(wide)).await;

    let pass = pass.expect("the profile matches");
    assert_eq!((pass.kept, pass.fenced_idle), (1, 0), "{pass:?}");
    assert_eq!(store.state(&id), SessionState::Open {});
}

/// The cost counts every operation and byte of a batch — its deletes as well, which no fence has.
#[test]
fn fence_cost_counts_every_operation_and_byte() {
    let batch = WriteBatch::new()
        .require(b"k1".to_vec(), Bytes::from_static(b"vvv"))
        .require_absent(b"k22".to_vec())
        .put(b"k333".to_vec(), Bytes::from_static(b"vvvvv"))
        .delete(b"k4444".to_vec());
    assert_eq!(fence_cost(&batch), (4, (2 + 3) + 3 + (4 + 5) + 5));
}
