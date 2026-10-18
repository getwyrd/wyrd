//! Issue #895 — `AbortMultipartUpload`'s **teardown fence**,
//! `wyrd_core::multipart::abort::abort_session`, and the one fence batch builder it shares with
//! the post-restore fence (`wyrd_core::multipart::abort::fence_batch`), driven over a real store
//! (`RedbMetadataStore::in_memory()`) on a multi-thread tokio runtime (proposal 0016's
//! *Abort / reap fence* row, `0016:665`, and decision 3's Abort row, `0016:977`).
//!
//! Every `Open` session here is made by Create (`create_session`, #894), so it is counted by
//! `mpuctl`; a `Completing` or `Completed` session, or the same session moved to a later epoch,
//! is that session's record overwritten with canonical bytes the decoder accepts.
//!
//! * **(D)** Abort per decision 3: `Open@E` is fenced to `Aborting@E+1` with its `{session, all}`
//!   obligation in the same batch; a second Abort, and the loser of two forced to race, answer
//!   `AlreadyAborting`; `Completing` is refused `OperationAborted` writing nothing; `Completed`
//!   and an absent record answer `NoSuchUpload`; the last round's re-read is answered like any
//!   other; a plain store fault with nothing in doubt is returned as that fault. Beside those: a
//!   session moved to a later `Open` epoch is fenced there, and an `Open` session at the last
//!   epoch or a torn record is a typed error writing nothing.
//! * **(E)** Abort never moves `mpuctl`: every leg asserts the count is unchanged, a `Completed`
//!   tombstone included.
//! * **(H)** a lost commit is classified from what the store holds: a wrapper answers
//!   `Ok(Conflict)` without applying anything, as TiKV does when its locking read meets another
//!   writer's lock (`crates/metadata-tikv/src/lib.rs:1388-1393`), and places a rival fence between
//!   two of the Abort's reads.
//! * **(I)** the doubt of a fence that may still land is kept until a read finds the session
//!   moved, and a read that does find it moved clears it; **(I2)** an out-of-flight outcome no
//!   read settled stays unknown, and one a read did settle is not kept.
//! * **(J)** the shared builder puts restore's preconditions and records in restore's order.
//!
//! The wrappers keep their state in struct fields, never in a `static` (ADR-0035).

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self, InodeId, SegmentGroup, SegmentNonce};
use wyrd_core::multipart::abort::{
    abort_session, fence_batch, AbortError, InDoubt, Stop, ABORT_ROUNDS,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    decode_admission_record, decode_retire_obligation, decode_session_record, mpu_key, retire_key,
    AbortOutcome, Budget, CreateOutcome, PartScope, Refusal, RetireMode, RetireToken, SessionState,
    UploadId, MAX_COMPLETE_ATTEMPTS, MPUCTL_KEY, MPU_PREFIX, RETIRE_BYTES_PREFIX,
    RETIRE_RECORDS_PREFIX,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{
    classify, BoxError, CommitOutcome, CommitUnknownResult, ErrorClass, MetadataStore,
    Precondition, Result as StoreResult, ScanPage, TransientFault, WriteBatch,
};

// ─────────────────────────────── fixtures ───────────────────────────────

const BUCKET: &str = "photos";
/// The root inode every Alpha object dirent lives under, as `<bucket>/<key>` (ADR-0046).
const ROOT: InodeId = 0;
const CLOCK: &str = "wall";
const CREATED_AT: u64 = 1_000;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
/// No leg may hang the suite: a schedule that never settles fails here instead.
const LEG_TIMEOUT: Duration = Duration::from_secs(120);
/// More commits than any Abort may make. An Abort past it never stops committing.
const RUNAWAY_COMMITS: usize = 100;

async fn within<T>(leg: impl Future<Output = T>) -> T {
    tokio::time::timeout(LEG_TIMEOUT, leg)
        .await
        .expect("the leg settled in time")
}

/// A profile admitting `n` sessions: every per-session cap at 1, so `U_ref` is 2.
fn profile_of(n: u64) -> Budget {
    let json = format!(
        "{{\"w_ref\":{},\"max_part_chunks\":1,\"max_parts_per_session\":1,\
         \"max_inflight_parts\":1,\"max_staged_chunks\":1}}",
        2 * n
    );
    let profile: Budget = metadata::decode(json.as_bytes()).expect("a legal profile");
    assert_eq!(profile.max_sessions(), n);
    profile
}

/// Identity number `n`: upload id `n` and a nonce derived from it, both 32 lowercase hex.
fn identity(n: u128) -> (UploadId, SegmentNonce) {
    (
        UploadId::new(format!("{n:032x}")).unwrap(),
        SegmentNonce::new(format!("{:032x}", n ^ 0x5a5a_5a5a)).unwrap(),
    )
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

async fn store_with_bucket() -> RedbMetadataStore {
    let store = RedbMetadataStore::in_memory().unwrap();
    store
        .commit(WriteBatch::new().put(metadata::bucket_key(BUCKET), MARKER))
        .await
        .unwrap();
    store
}

/// Create session `n` through Create (#894): `Open@0`, counted by `mpuctl`. Returns its id and
/// the exact bytes Create stored.
async fn open_session(store: &impl MetadataStore, n: u128) -> (UploadId, Bytes) {
    let object = format!("{BUCKET}/object-{n}");
    let request = CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object: &object,
        content_type: None,
        created_at_millis: CREATED_AT,
        clock_source: CLOCK,
    };
    let outcome = create_session(
        store,
        &profile_of(8),
        &request,
        &mut Fixed(Some(identity(n))),
    )
    .await
    .expect("the create ran");
    let CreateOutcome::Created { upload_id } = outcome else {
        panic!("expected Created, got {outcome:?}");
    };
    let bytes = store.get(&mpu_key(&upload_id)).await.unwrap().unwrap();
    let record = decode_session_record(&bytes).unwrap();
    assert!(matches!(record.state(), SessionState::Open {}));
    assert_eq!(record.epoch(), 0);
    (upload_id, bytes)
}

/// `mpuctl.count()`.
async fn count(store: &impl MetadataStore) -> u64 {
    let bytes = store.get(MPUCTL_KEY).await.unwrap().expect("a ledger");
    decode_admission_record(&bytes).expect("a ledger").count()
}

/// Every record in the store, in key order.
async fn snapshot(store: &impl MetadataStore) -> Vec<(Vec<u8>, Bytes)> {
    let mut all = store.scan(b"").await.unwrap();
    all.sort();
    all
}

/// Every `retire:` key in the store.
async fn obligations(store: &impl MetadataStore) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    for prefix in [RETIRE_BYTES_PREFIX, RETIRE_RECORDS_PREFIX] {
        keys.extend(
            store
                .scan(prefix)
                .await
                .unwrap()
                .into_iter()
                .map(|(k, _)| k),
        );
    }
    keys.sort();
    keys
}

/// The JSON spelling of a string, escapes included — the encoder's own.
fn json_string(text: &str) -> String {
    String::from_utf8(metadata::encode(&text).to_vec()).unwrap()
}

/// `open`'s session at `epoch` and `attempts` in `state` (a JSON state object), in the codec's
/// canonical spelling — checked by decoding it and re-encoding it.
fn session_bytes(open: &Bytes, epoch: u64, attempts: u32, state: &str) -> Bytes {
    let record = decode_session_record(open).unwrap();
    let bytes = format!(
        "{{\"parent\":{},\"object\":{},\"created_at_millis\":{},\"clock_source\":{},\
         \"segment_nonce\":\"{}\",\"epoch\":{epoch},\"attempts\":{attempts},\"state\":{state}}}",
        record.parent(),
        json_string(record.object()),
        record.created_at_millis(),
        json_string(record.clock_source()),
        record.segment_nonce(),
    );
    let decoded = decode_session_record(bytes.as_bytes()).expect("a canonical session");
    assert_eq!(metadata::encode(&decoded), bytes.as_bytes());
    Bytes::from(bytes)
}

/// `open`'s session fenced by a Complete at epoch 1: `Completing@1`, one attempt spent.
fn completing(open: &Bytes) -> Bytes {
    let record = decode_session_record(open).unwrap();
    let state = format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":2000,\"segments_written\":0,\
         \"publish_target\":{{\"parent\":{},\"name\":{},\"epoch\":1}}}}",
        record.parent(),
        json_string(record.object()),
    );
    session_bytes(open, 1, 1, &state)
}

/// `open`'s session published: a `Completed` tombstone at epoch 2.
fn completed(open: &Bytes) -> Bytes {
    let state = format!(
        "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":7,\"version\":1,\"etag\":\
         \"{}-1\",\"completed_at_millis\":3000,\"complete_fingerprint\":\"{}\"}}}}",
        "ab".repeat(32),
        "cd".repeat(32)
    );
    session_bytes(open, 2, 1, &state)
}

/// Overwrite `id`'s session record with `bytes`.
async fn plant(store: &impl MetadataStore, id: &UploadId, bytes: Bytes) {
    let put = WriteBatch::new().put(mpu_key(id), bytes);
    assert_eq!(store.commit(put).await.unwrap(), CommitOutcome::Committed);
}

/// The `retire:bytes:s:<id>:<epoch>` key — the `{session, all}` obligation's (`0016:2187`).
fn obligation_key(id: &UploadId, epoch: u64) -> Vec<u8> {
    let token = RetireToken::Session {
        upload_id: id.clone(),
        epoch,
        part: None,
    };
    retire_key(RetireMode::Bytes, &token)
}

/// **(D)** The store holds `id`'s `Open@E` session (`open`) fenced: `Aborting@E+1`, exactly as
/// `SessionRecord::open_teardown` leaves it, and `retire:bytes:s:<id>:<E>` holding exactly the
/// `{session, all}` obligation it mints — decoding against its own key — and no other `retire:`
/// key.
async fn assert_fenced(store: &impl MetadataStore, id: &UploadId, open: &Bytes) {
    let opened = decode_session_record(open).unwrap();
    let epoch = opened.epoch();
    let teardown = opened.open_teardown(id).expect("Open@E has a teardown");
    let stored = store.get(&mpu_key(id)).await.unwrap().expect("the session");
    let record = decode_session_record(&stored).expect("a valid session");
    assert!(
        matches!(record.state(), SessionState::Aborting {}),
        "{id}: the fence leaves the session Aborting"
    );
    assert_eq!(record.epoch(), epoch + 1, "{id}: Aborting@E+1");
    assert_eq!(stored, metadata::encode(teardown.session()));
    let key = obligation_key(id, epoch);
    assert_eq!(teardown.obligation().key(), key);
    let value = store.get(&key).await.unwrap().expect("the obligation");
    assert_eq!(value, metadata::encode(teardown.obligation().payload()));
    let (mode, token, payload) = decode_retire_obligation(&key, &value).expect("it decodes");
    assert_eq!(mode, RetireMode::Bytes);
    assert_eq!(
        token,
        RetireToken::Session {
            upload_id: id.clone(),
            epoch,
            part: None,
        }
    );
    assert!(payload.session() && payload.parts() == Some(&PartScope::All));
    assert_eq!(
        obligations(store).await,
        vec![key],
        "one obligation, no other"
    );
}

/// `id`'s `Open@0` session is still exactly `open`, and no obligation exists.
async fn assert_still_open(store: &impl MetadataStore, id: &UploadId, open: &Bytes) {
    assert_eq!(store.get(&mpu_key(id)).await.unwrap().as_ref(), Some(open));
    assert!(obligations(store).await.is_empty(), "no obligation");
}

/// The fence batch [`fence_batch`] builds for `id`'s `open` session.
fn expected_fence(id: &UploadId, open: &Bytes) -> WriteBatch {
    let teardown = decode_session_record(open)
        .unwrap()
        .open_teardown(id)
        .unwrap();
    fence_batch(
        &mpu_key(id),
        open,
        teardown.session(),
        std::slice::from_ref(teardown.obligation()),
    )
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

/// An Abort that ended in doubt: `InDoubt` naming `id`, with `may_still_commit` as given;
/// [`classify`] reads it as `Indeterminate` (never transient), whatever stopped the Abort; its
/// source is the store's own `CommitUnknownResult`; and its `Display` names the upload id.
fn in_doubt(
    outcome: Result<AbortOutcome, AbortError>,
    id: &UploadId,
    may_still_commit: bool,
) -> Box<InDoubt> {
    let err = match outcome {
        Err(err @ AbortError::InDoubt(_)) => err,
        other => panic!("expected InDoubt naming {id}, got {other:?}"),
    };
    assert_eq!(
        classify(&err),
        ErrorClass::Indeterminate,
        "an Abort in doubt reads as indeterminate, whatever stopped it: {err}"
    );
    let source = std::error::Error::source(&err)
        .and_then(|source| source.downcast_ref::<CommitUnknownResult>())
        .expect("the source is the store's unknown outcome");
    assert_eq!(source.may_still_commit, may_still_commit);
    let text = err.to_string();
    assert!(text.contains(id.as_str()), "it names the upload id: {text}");
    assert!(text.contains("in doubt"), "{text}");
    let AbortError::InDoubt(doubt) = err else {
        unreachable!()
    };
    assert_eq!(&doubt.upload_id, id);
    assert_eq!(doubt.may_still_commit, may_still_commit);
    doubt
}

/// An Abort that ended with nothing in doubt, stopped by the scripted transient commit fault:
/// `Stopped` naming `id`, by [`Stop::Store`] wrapping that fault, and [`classify`] reads it as
/// `Transient` — never `Indeterminate`, which a kept doubt would make it.
fn stopped_by_a_transient_fault(outcome: Result<AbortOutcome, AbortError>, id: &UploadId) {
    let err = match outcome {
        Err(err @ AbortError::Stopped { .. }) => err,
        other => panic!("expected Stopped with nothing in doubt, got {other:?}"),
    };
    assert_eq!(
        classify(&err),
        ErrorClass::Transient,
        "nothing is in doubt, so the fault keeps its class: {err}"
    );
    let AbortError::Stopped { upload_id, cause } = err else {
        unreachable!()
    };
    assert_eq!(&upload_id, id);
    assert!(
        matches!(&cause, Stop::Store(fault) if fault.is::<TransientFault>()),
        "{cause:?}"
    );
}

// ──────────────────────────── the scripted wrapper ────────────────────────────

/// What [`Scripted`] does with the Abort's next commit.
#[derive(Debug, Clone)]
enum OnCommit {
    /// Delegate unchanged.
    Pass,
    /// Answer `Ok(Conflict)` without applying the batch or changing any record, as TiKV does when
    /// a locking read meets another writer's lock.
    ConflictUnmoved,
    /// Answer a plain store error (not an unknown outcome), unapplied.
    Fail { transient: bool },
    /// Apply the batch or not, then answer `CommitUnknownResult { may_still_commit }`.
    Unknown {
        applied: bool,
        may_still_commit: bool,
    },
    /// Keep the batch unapplied and answer `CommitUnknownResult { may_still_commit: true }`; it
    /// lands when the test releases it.
    Hold,
    /// As [`Self::Hold`], and before answering, overwrite the session with these bytes (a
    /// Complete fence moving it on).
    HoldThenMove(Bytes),
    /// Overwrite the session with these bytes first (a Complete fence and its release moving it
    /// to a later `Open` epoch); then delegate, so the batch conflicts for real.
    MoveFirst(Bytes),
    /// A real rival Abort of the same session runs on the inner store first; then delegate.
    RivalFirst,
}

#[derive(Default)]
struct ScriptState {
    plan: VecDeque<OnCommit>,
    /// What every commit past `plan` does (`Pass` when unset).
    then: Option<OnCommit>,
    /// Once the Abort under test has made this many commits, every read of an `mpu:` key fails
    /// with a plain fault.
    fail_session_reads_after: Option<usize>,
    /// On the Abort's first read of a `retire:` key, a real rival Abort of this upload runs on
    /// the inner store first.
    rival_on_obligation_read: Option<UploadId>,
    /// What the rival Abort answered.
    rival: Option<String>,
    held: Option<WriteBatch>,
    /// Every batch the Abort under test committed through this wrapper.
    commits: Vec<WriteBatch>,
}

/// A store wrapper that acts on the Abort's commits, and some of its reads, as scripted. Rival
/// Aborts it starts run directly on the inner store, so only the Abort under test is scripted.
struct Scripted {
    inner: RedbMetadataStore,
    state: Mutex<ScriptState>,
}

impl Scripted {
    async fn new() -> Self {
        Self {
            inner: store_with_bucket().await,
            state: Mutex::new(ScriptState::default()),
        }
    }

    fn script(&self, plan: Vec<OnCommit>, then: Option<OnCommit>) {
        let mut state = self.state();
        state.plan = plan.into();
        state.then = then;
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ScriptState> {
        self.state.lock().unwrap()
    }

    fn commits(&self) -> Vec<WriteBatch> {
        self.state().commits.clone()
    }

    /// Run a real rival Abort of `id` on the inner store, and record its answer.
    async fn rival(&self, id: &UploadId) {
        let outcome = abort_session(&self.inner, id).await;
        assert!(
            matches!(outcome, Ok(AbortOutcome::Fenced)),
            "the rival fences: {outcome:?}"
        );
        self.state().rival = Some(format!("{outcome:?}"));
    }

    /// Apply the held batch now; what it met.
    async fn release_held(&self) -> CommitOutcome {
        let held = self.state().held.take().expect("a batch is held");
        self.inner.commit(held).await.unwrap()
    }
}

/// The upload id a fence batch's session put names.
fn upload_of(batch: &WriteBatch) -> UploadId {
    let key = batch
        .puts
        .iter()
        .map(|(key, _)| key)
        .find(|key| key.starts_with(MPU_PREFIX))
        .expect("a fence puts its session");
    wyrd_core::multipart::parse_mpu_key(key).unwrap()
}

#[async_trait]
impl MetadataStore for Scripted {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let (fails, rival) = {
            let mut state = self.state();
            let fails = key.starts_with(MPU_PREFIX)
                && state
                    .fail_session_reads_after
                    .is_some_and(|after| state.commits.len() >= after);
            let rival = if key.starts_with(RETIRE_BYTES_PREFIX) {
                state.rival_on_obligation_read.take()
            } else {
                None
            };
            (fails, rival)
        };
        if fails {
            return Err(fault(false));
        }
        if let Some(id) = rival {
            self.rival(&id).await;
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
        let (act, made) = {
            let mut state = self.state();
            state.commits.push(batch.clone());
            let act = state.plan.pop_front().or_else(|| state.then.clone());
            (act.unwrap_or(OnCommit::Pass), state.commits.len())
        };
        assert!(
            made <= RUNAWAY_COMMITS,
            "the abort never stopped committing ({made} commits)"
        );
        match act {
            OnCommit::Pass => self.inner.commit(batch).await,
            OnCommit::ConflictUnmoved => {
                // A real store yields here; so must this one, or no timeout could fire.
                tokio::task::yield_now().await;
                Ok(CommitOutcome::Conflict)
            }
            OnCommit::Fail { transient } => Err(fault(transient)),
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
            OnCommit::HoldThenMove(bytes) => {
                let id = upload_of(&batch);
                self.state().held = Some(batch);
                plant(&self.inner, &id, bytes).await;
                Err(unknown(true))
            }
            OnCommit::MoveFirst(bytes) => {
                plant(&self.inner, &upload_of(&batch), bytes).await;
                self.inner.commit(batch).await
            }
            OnCommit::RivalFirst => {
                self.rival(&upload_of(&batch)).await;
                self.inner.commit(batch).await
            }
        }
    }
}

/// A scripted store holding one `Open@0` session made by Create, with its id and bytes, and the
/// ledger count before the leg.
async fn scripted_open() -> (Scripted, UploadId, Bytes, u64) {
    let store = Scripted::new().await;
    let (id, open) = open_session(&store.inner, 1).await;
    let before = count(&store.inner).await;
    assert_eq!(before, 1, "Create counted the session");
    (store, id, open, before)
}

// ──────────────────────── (D) decision 3, and (E) ────────────────────────

/// **(D, E)** On `Open@0` the Abort answers `Fenced`, in **one** commit: the batch requires the
/// exact bytes read and the obligation key absent, and puts the session at `Aborting@1` and the
/// `{session, all}` obligation together — exactly what [`fence_batch`] builds, and nothing on
/// `mpuctl`. Afterwards the session decodes `Aborting@1` and `retire:bytes:s:<id>:0` holds the
/// obligation `open_teardown` mints. A second Abort answers `AlreadyAborting` and writes nothing.
/// `mpuctl.count()` is unchanged throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_an_open_session_is_fenced_with_its_obligation_in_one_batch() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(outcome, Ok(AbortOutcome::Fenced)),
            "Open@0 is fenced: {outcome:?}"
        );
        let commits = store.commits();
        assert_eq!(commits.len(), 1, "one commit");
        let batch = &commits[0];
        let expected = expected_fence(&id, &open);
        assert_eq!(batch.preconditions, expected.preconditions);
        assert_eq!(batch.puts, expected.puts);
        assert!(batch.deletes.is_empty());
        let key = obligation_key(&id, 0);
        assert_eq!(
            batch.preconditions,
            vec![
                Precondition {
                    key: mpu_key(&id),
                    expected: Some(open.clone()),
                },
                Precondition {
                    key: key.clone(),
                    expected: None,
                },
            ],
            "require the exact bytes read, and the obligation key absent"
        );
        let put_keys: Vec<&Vec<u8>> = batch.puts.iter().map(|(k, _)| k).collect();
        assert_eq!(
            put_keys,
            vec![&mpu_key(&id), &key],
            "the session and its obligation, in one batch"
        );
        assert!(
            !batch.puts.iter().any(|(k, _)| k == MPUCTL_KEY),
            "the fence never touches mpuctl"
        );
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");

        let fenced = snapshot(&store.inner).await;
        let again = abort_session(&store, &id).await;
        assert!(
            matches!(again, Ok(AbortOutcome::AlreadyAborting)),
            "a second Abort is idempotent: {again:?}"
        );
        assert_eq!(store.commits().len(), 1, "a second Abort commits nothing");
        assert_eq!(snapshot(&store.inner).await, fenced, "and writes nothing");
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// The wrapper that forces two Aborts to race: each one's first commit waits until both Aborts
/// have built theirs, from the same `Open@0` read, and only then reaches the store.
struct Racing {
    inner: RedbMetadataStore,
    barrier: tokio::sync::Barrier,
    commits: AtomicUsize,
}

#[async_trait]
impl MetadataStore for Racing {
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
        if self.commits.fetch_add(1, Ordering::SeqCst) < 2 {
            self.barrier.wait().await;
        }
        self.inner.commit(batch).await
    }
}

/// **(D, E) Two Aborts forced to race one `Open` session** answer one `Fenced` and one
/// `AlreadyAborting`, never `OperationAborted`: both read `Open@0` and build their fence before
/// either commits, so one commit lands and the other conflicts, re-reads, and finds the rival's
/// `Aborting`. The store holds one fence and one obligation; `mpuctl.count()` is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d_two_aborts_forced_to_race_answer_fenced_and_already_aborting() {
    within(async {
        let store = Arc::new(Racing {
            inner: store_with_bucket().await,
            barrier: tokio::sync::Barrier::new(2),
            commits: AtomicUsize::new(0),
        });
        let (id, open) = open_session(&store.inner, 1).await;
        let before = count(&store.inner).await;
        let tasks: Vec<_> = (0..2)
            .map(|_| {
                let store = Arc::clone(&store);
                let id = id.clone();
                tokio::spawn(async move { abort_session(store.as_ref(), &id).await })
            })
            .collect();
        let mut answers = Vec::new();
        for task in tasks {
            answers.push(task.await.unwrap().expect("each Abort answers"));
        }
        assert_eq!(
            store.commits.load(Ordering::SeqCst),
            2,
            "each Abort committed once: the loser answered from its re-read"
        );
        answers.sort_by_key(|answer| format!("{answer:?}"));
        assert_eq!(
            answers,
            vec![AbortOutcome::AlreadyAborting, AbortOutcome::Fenced],
            "one Fenced and one AlreadyAborting, never OperationAborted"
        );
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(D, E)** Abort of a `Completing` session answers `Refused(OperationAborted)` and leaves the
/// store byte-identical: no commit is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_completing_session_is_refused_operation_aborted_and_nothing_is_written() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        plant(&store.inner, &id, completing(&open)).await;
        let unchanged = snapshot(&store.inner).await;
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(
                outcome,
                Ok(AbortOutcome::Refused(Refusal::OperationAborted))
            ),
            "Completing answers 409: {outcome:?}"
        );
        assert!(store.commits().is_empty(), "no commit");
        assert_eq!(snapshot(&store.inner).await, unchanged, "byte-identical");
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(D, E)** Abort of a `Completed` tombstone, or of an upload id with no record, answers
/// `Refused(NoSuchUpload)` and writes nothing. The tombstone stays counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_completed_session_or_an_absent_one_answers_no_such_upload() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        plant(&store.inner, &id, completed(&open)).await;
        let unchanged = snapshot(&store.inner).await;
        let tombstone = abort_session(&store, &id).await;
        assert!(
            matches!(tombstone, Ok(AbortOutcome::Refused(Refusal::NoSuchUpload))),
            "Completed answers 404: {tombstone:?}"
        );
        let (absent, _) = identity(99);
        let outcome = abort_session(&store, &absent).await;
        assert!(
            matches!(outcome, Ok(AbortOutcome::Refused(Refusal::NoSuchUpload))),
            "no record answers 404: {outcome:?}"
        );
        assert!(store.commits().is_empty(), "no commit");
        assert_eq!(snapshot(&store.inner).await, unchanged, "byte-identical");
        assert_eq!(
            count(&store.inner).await,
            before,
            "(E) a Completed tombstone stays counted"
        );
    })
    .await;
}

/// **(D, E) The last round's re-read is answered like any other.** Every round but the last loses
/// an unexplained conflict; the last returns `CommitUnknownResult` with its batch applied. The
/// re-read finds this Abort's own fence, so the answer is `Fenced` — not "rounds spent".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_an_unknown_outcome_on_the_last_round_whose_batch_landed_answers_fenced() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        let mut plan = vec![OnCommit::ConflictUnmoved; ABORT_ROUNDS as usize - 1];
        plan.push(OnCommit::Unknown {
            applied: true,
            may_still_commit: false,
        });
        store.script(plan, None);
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(outcome, Ok(AbortOutcome::Fenced)),
            "the last round's landed fence is Fenced: {outcome:?}"
        );
        assert_eq!(store.commits().len(), ABORT_ROUNDS as usize);
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(D, E)** Every round but the last loses an unexplained conflict; on the last, a rival Abort
/// fences first, so this Abort's commit conflicts for real. The re-read finds the rival's
/// `Aborting`: the answer is `AlreadyAborting`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_rival_abort_winning_the_last_round_answers_already_aborting() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        let mut plan = vec![OnCommit::ConflictUnmoved; ABORT_ROUNDS as usize - 1];
        plan.push(OnCommit::RivalFirst);
        store.script(plan, None);
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(outcome, Ok(AbortOutcome::AlreadyAborting)),
            "the rival won the last round: {outcome:?}"
        );
        assert_eq!(store.commits().len(), ABORT_ROUNDS as usize);
        assert!(store.state().rival.is_some(), "the rival ran");
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(D, E)** A plain store fault on the fence commit, with nothing in doubt, is returned as that
/// fault: it keeps its own class (transient stays transient, anything else terminal), its source
/// is that fault, and the session is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_plain_fault_with_nothing_in_doubt_is_returned_as_that_fault() {
    within(async {
        for (transient, class) in [(true, ErrorClass::Transient), (false, ErrorClass::Terminal)] {
            let (store, id, open, before) = scripted_open().await;
            store.script(vec![OnCommit::Fail { transient }], None);
            let outcome = abort_session(&store, &id).await;
            let err = match outcome {
                Err(err @ AbortError::Stopped { .. }) => err,
                other => panic!("expected the fault, got {other:?}"),
            };
            assert_eq!(classify(&err), class, "{err}");
            let source = std::error::Error::source(&err).expect("the fault is the source");
            assert!(source.to_string().contains(FAULT), "{source}");
            assert_eq!(source.is::<TransientFault>(), transient);
            let AbortError::Stopped { upload_id, cause } = err else {
                unreachable!()
            };
            assert_eq!(upload_id, id);
            assert!(matches!(cause, Stop::Store(_)), "{cause:?}");
            assert_eq!(store.commits().len(), 1, "one commit, not retried");
            assert_still_open(&store.inner, &id, &open).await;
            assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
        }
    })
    .await;
}

/// **(D, E) A session moved to a later `Open` epoch is fenced again there.** Before the first
/// fence reaches the store, a Complete fence and its release move the session to `Open@2`, so the
/// fence conflicts for real. The re-read finds the session moved and still `Open`: the Abort
/// fences it at epoch 2, and `retire:bytes:s:<id>:2` holds the obligation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_session_moved_to_a_later_open_epoch_is_fenced_there() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        let reopened = session_bytes(&open, 2, 1, r#"{"kind":"Open"}"#);
        store.script(vec![OnCommit::MoveFirst(reopened.clone())], None);
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(outcome, Ok(AbortOutcome::Fenced)),
            "the later epoch is fenced: {outcome:?}"
        );
        let commits = store.commits();
        assert_eq!(commits.len(), 2, "the lost fence, then the one at epoch 2");
        let second = &commits[1];
        assert_eq!(
            second.preconditions,
            expected_fence(&id, &reopened).preconditions
        );
        assert_fenced(&store.inner, &id, &reopened).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(D, E)** An `Open` session at the last epoch cannot be fenced (every fence bumps it): the
/// Abort ends with a typed error, nothing in doubt, and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_an_open_session_at_the_last_epoch_is_a_typed_error_writing_nothing() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        plant(
            &store.inner,
            &id,
            session_bytes(&open, u64::MAX, 0, r#"{"kind":"Open"}"#),
        )
        .await;
        let unchanged = snapshot(&store.inner).await;
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(
                &outcome,
                Err(AbortError::Stopped {
                    cause: Stop::EpochExhausted,
                    ..
                })
            ),
            "{outcome:?}"
        );
        assert!(store.commits().is_empty(), "no commit");
        assert_eq!(snapshot(&store.inner).await, unchanged, "byte-identical");
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(D, E)** A stored `mpu:` value that is not a valid session record is a typed error, never a
/// value (ADR-0045): the Abort names the decode fault, classifies terminal, and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_a_torn_session_record_is_a_typed_error_writing_nothing() {
    within(async {
        let (store, id, _, before) = scripted_open().await;
        plant(&store.inner, &id, Bytes::from_static(br#"{"epoch":0}"#)).await;
        let unchanged = snapshot(&store.inner).await;
        let outcome = abort_session(&store, &id).await;
        let err = match outcome {
            Err(err @ AbortError::Stopped { .. }) => err,
            other => panic!("expected the decode fault, got {other:?}"),
        };
        assert_eq!(classify(&err), ErrorClass::Terminal, "{err}");
        let AbortError::Stopped { cause, .. } = err else {
            unreachable!()
        };
        assert!(matches!(cause, Stop::Record(_)), "{cause:?}");
        assert!(store.commits().is_empty(), "no commit");
        assert_eq!(snapshot(&store.inner).await, unchanged, "byte-identical");
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

// ────────────────── (H) a lost commit is classified, not inferred ──────────────────

/// **H1.** One `Conflict` that applied nothing, with the session unchanged and the obligation key
/// absent: the Abort reads the key, does **not** report it taken, spends a round, and answers
/// `Fenced` on the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_an_unexplained_conflict_spends_a_round_and_the_abort_fences() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(vec![OnCommit::ConflictUnmoved], None);
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(outcome, Ok(AbortOutcome::Fenced)),
            "an unexplained conflict is not damage: {outcome:?}"
        );
        assert_eq!(store.commits().len(), 2, "one round spent, then the fence");
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **H2.** Round 1 returns `CommitUnknownResult { may_still_commit: true }` with its batch held;
/// every later round gets a `Conflict` that applied nothing, until all [`ABORT_ROUNDS`] are
/// spent. The Abort ends `InDoubt` saying `may_still_commit: true`, stopped by the spent rounds —
/// never by a "taken" key it did not read. Released after the Abort returned, the held batch
/// lands: the session is `Aborting` with its one obligation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_a_held_fence_and_unexplained_conflicts_end_in_doubt_and_the_fence_lands() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(vec![OnCommit::Hold], Some(OnCommit::ConflictUnmoved));
        let outcome = abort_session(&store, &id).await;
        let doubt = in_doubt(outcome, &id, true);
        assert!(
            matches!(doubt.cause, Stop::RoundsSpent { rounds } if rounds == ABORT_ROUNDS),
            "every round was spent: {:?}",
            doubt.cause
        );
        assert_eq!(store.commits().len(), ABORT_ROUNDS as usize);
        assert_still_open(&store.inner, &id, &open).await;
        assert_eq!(store.release_held().await, CommitOutcome::Committed);
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **H3.** The obligation key really present (planted) and the session unchanged: the commit
/// conflicts, the key is read present, and the session read again is still unchanged. The Abort
/// ends with a typed fault naming that key, nothing in doubt, and writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h3_a_taken_obligation_key_under_an_unchanged_session_is_named_damage() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        let key = obligation_key(&id, 0);
        let minted = expected_fence(&id, &open).puts[1].1.clone();
        let squatter = WriteBatch::new().put(key.clone(), minted);
        assert_eq!(
            store.inner.commit(squatter).await.unwrap(),
            CommitOutcome::Committed
        );
        let unchanged = snapshot(&store.inner).await;
        let outcome = abort_session(&store, &id).await;
        let err = match outcome {
            Err(err @ AbortError::Stopped { .. }) => err,
            other => panic!("expected the damage fault, got {other:?}"),
        };
        assert_eq!(classify(&err), ErrorClass::Terminal, "{err}");
        let text = err.to_string();
        let named = String::from_utf8(key).unwrap();
        assert!(text.contains(&named), "the fault names the key: {text}");
        let AbortError::Stopped { cause, .. } = err else {
            unreachable!()
        };
        assert!(
            matches!(&cause, Stop::ObligationKeyTaken { key } if *key == named),
            "{cause:?}"
        );
        assert_eq!(store.commits().len(), 1, "damage is not retried");
        assert_eq!(snapshot(&store.inner).await, unchanged, "nothing written");
        let session = store.inner.get(&mpu_key(&id)).await.unwrap();
        assert_eq!(session, Some(open), "the session is still Open@0");
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **H4, a rival fence between the two reads is not damage.** Abort A's commit gets a `Conflict`
/// that applied nothing, and A re-reads the session: `Open@0`, unchanged. Before A reads the
/// obligation key, the wrapper lets Abort B commit its fence (session and obligation, one batch).
/// A finds the key present, reads the session again, finds it moved, and answers
/// `AlreadyAborting` — not a damage fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_rival_fence_between_the_reads_answers_already_aborting() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(vec![OnCommit::ConflictUnmoved], None);
        store.state().rival_on_obligation_read = Some(id.clone());
        let outcome = abort_session(&store, &id).await;
        assert!(
            store.state().rival.is_some(),
            "B fenced between A's session read and its key read"
        );
        assert!(
            matches!(outcome, Ok(AbortOutcome::AlreadyAborting)),
            "a rival's fence is a valid transition, not damage: {outcome:?}"
        );
        assert_eq!(store.commits().len(), 1, "A made one commit");
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

// ──────────────── (I) the doubt of a fence that may still land is kept ────────────────

/// **(I) across a later out-of-flight unknown.** Round 1 is held (`may_still_commit: true`);
/// round 2 returns `CommitUnknownResult { may_still_commit: false }`, unapplied; then every read
/// of the session fails. The Abort ends `InDoubt` still saying `may_still_commit: true` — the
/// held outcome, not the latest one — and the batch released after it returned lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i_a_held_fence_survives_a_later_out_of_flight_unknown() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(
            vec![
                OnCommit::Hold,
                OnCommit::Unknown {
                    applied: false,
                    may_still_commit: false,
                },
            ],
            None,
        );
        store.state().fail_session_reads_after = Some(2);
        let outcome = abort_session(&store, &id).await;
        let doubt = in_doubt(outcome, &id, true);
        assert!(doubt.outcome.may_still_commit, "the held outcome is kept");
        assert_eq!(store.commits().len(), 2);
        assert_eq!(store.release_held().await, CommitOutcome::Committed);
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(I) across a failed re-read.** Round 1 is held, and the re-read of the session fails. The
/// Abort ends `InDoubt` saying `may_still_commit: true`, stopped by that fault; the batch
/// released after it returned lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i_a_held_fence_survives_a_failed_re_read() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(vec![OnCommit::Hold], None);
        store.state().fail_session_reads_after = Some(1);
        let outcome = abort_session(&store, &id).await;
        let doubt = in_doubt(outcome, &id, true);
        assert!(matches!(doubt.cause, Stop::Store(_)), "{:?}", doubt.cause);
        assert_eq!(store.release_held().await, CommitOutcome::Committed);
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(I) across a failed read after an unexplained conflict.** Round 1 is held
/// (`may_still_commit: true`), and the re-read finds the session unchanged. Round 2 gets a
/// `Conflict` that applied nothing, and the read of the session that classifies it fails. The
/// Abort ends `InDoubt` still saying `may_still_commit: true`, stopped by that read's plain
/// fault — never `Stopped` as if the fault were all there was. The batch released after it
/// returned lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i_a_held_fence_survives_a_failed_read_after_an_unexplained_conflict() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(vec![OnCommit::Hold, OnCommit::ConflictUnmoved], None);
        store.state().fail_session_reads_after = Some(2);
        let outcome = abort_session(&store, &id).await;
        let doubt = in_doubt(outcome, &id, true);
        assert!(doubt.outcome.may_still_commit, "the held outcome is kept");
        assert!(
            matches!(
                &doubt.cause,
                Stop::Store(fault) if !fault.is::<TransientFault>() && fault.to_string() == FAULT
            ),
            "stopped by the failed read's plain fault: {:?}",
            doubt.cause
        );
        let commits = store.commits();
        assert_eq!(commits.len(), 2, "the held fence, then the conflicted one");
        let expected = expected_fence(&id, &open);
        for commit in &commits {
            assert_eq!(
                commit.preconditions, expected.preconditions,
                "both rounds fenced the unchanged Open@0"
            );
        }
        assert_still_open(&store.inner, &id, &open).await;
        assert_eq!(store.release_held().await, CommitOutcome::Committed);
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(I) across a later plain store fault.** Round 1 is held; the re-read finds the session
/// unchanged; round 2's commit fails with a transient fault. The Abort ends `InDoubt` saying
/// `may_still_commit: true` and classifies indeterminate, never transient; the batch released
/// after it returned lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i_a_held_fence_survives_a_later_plain_fault() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(
            vec![OnCommit::Hold, OnCommit::Fail { transient: true }],
            None,
        );
        let outcome = abort_session(&store, &id).await;
        let doubt = in_doubt(outcome, &id, true);
        assert!(matches!(doubt.cause, Stop::Store(_)), "{:?}", doubt.cause);
        assert_eq!(store.commits().len(), 2);
        assert_eq!(store.release_held().await, CommitOutcome::Committed);
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(I) only a moved session clears the doubt.** Round 1 is held, and before the re-read a
/// Complete fence moves the session to `Completing@1`. The re-read finds it moved: the held
/// batch can never land, so the Abort answers from the new state, `OperationAborted`, with no
/// doubt — and the batch released afterwards loses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i_a_session_moved_under_a_held_fence_ends_without_the_doubt() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        let moved = completing(&open);
        store.script(vec![OnCommit::HoldThenMove(moved.clone())], None);
        let outcome = abort_session(&store, &id).await;
        assert!(
            matches!(
                outcome,
                Ok(AbortOutcome::Refused(Refusal::OperationAborted))
            ),
            "the moved session is answered, without the doubt: {outcome:?}"
        );
        assert_eq!(store.release_held().await, CommitOutcome::Conflict);
        assert_eq!(
            store.inner.get(&mpu_key(&id)).await.unwrap(),
            Some(moved),
            "the released fence lost"
        );
        assert!(obligations(&store.inner).await.is_empty(), "no obligation");
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(I) a moved session clears the doubt, and an Abort that goes on fencing shows it.** Round 1
/// is held (`may_still_commit: true`), and before the re-read a Complete fence and its release
/// move the session to `Open@2`. The re-read finds the session moved, so the held batch, which
/// requires `Open@0`, can never land. The Abort fences again at epoch 2, and that commit fails
/// with a plain transient fault. Nothing of this Abort is in doubt any more, so it ends
/// `Stopped` and classifies transient, not `InDoubt` and indeterminate; the batch released
/// afterwards loses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i_a_session_moved_to_a_later_open_epoch_clears_the_held_doubt() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        let reopened = session_bytes(&open, 2, 1, r#"{"kind":"Open"}"#);
        store.script(
            vec![
                OnCommit::HoldThenMove(reopened.clone()),
                OnCommit::Fail { transient: true },
            ],
            None,
        );
        let outcome = abort_session(&store, &id).await;
        stopped_by_a_transient_fault(outcome, &id);
        let commits = store.commits();
        assert_eq!(commits.len(), 2, "the held fence, then the one at epoch 2");
        assert_eq!(
            commits[1].preconditions,
            expected_fence(&id, &reopened).preconditions,
            "the second round fenced the session it found moved"
        );
        assert_eq!(store.release_held().await, CommitOutcome::Conflict);
        assert_eq!(
            store.inner.get(&mpu_key(&id)).await.unwrap(),
            Some(reopened),
            "the released fence lost; the session is still Open@2"
        );
        assert!(obligations(&store.inner).await.is_empty(), "no obligation");
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

// ──────────────── (I2) an unsettled out-of-flight outcome stays unknown ────────────────

/// **(I2)** With no earlier `may_still_commit: true`: the wrapper applies the fence, returns
/// `CommitUnknownResult { may_still_commit: false }`, and then fails every read of `mpu:<id>`
/// until the Abort returns. The fence landed but the Abort never learned it, so it ends with an
/// error of the unknown-outcome class naming the upload id — not the read's own fault, and not
/// "no fence of this Abort can land". The store holds `Aborting@1` with its one obligation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i2_an_out_of_flight_outcome_no_read_settled_ends_indeterminate() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(
            vec![OnCommit::Unknown {
                applied: true,
                may_still_commit: false,
            }],
            None,
        );
        store.state().fail_session_reads_after = Some(1);
        let outcome = abort_session(&store, &id).await;
        let doubt = in_doubt(outcome, &id, false);
        assert!(matches!(doubt.cause, Stop::Store(_)), "{:?}", doubt.cause);
        assert_eq!(store.commits().len(), 1);
        assert_fenced(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

/// **(I2) one successful read settles an out-of-flight outcome.** Round 1 returns
/// `CommitUnknownResult { may_still_commit: false }`, unapplied, and the re-read succeeds and
/// finds the session unchanged: that fence did not land and cannot land later. Round 2's commit
/// fails with a plain transient fault. Nothing of this Abort is in doubt, so it ends `Stopped`
/// and classifies transient, not `InDoubt` and indeterminate, and the session is still `Open@0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn i2_an_out_of_flight_outcome_a_read_settled_is_not_kept() {
    within(async {
        let (store, id, open, before) = scripted_open().await;
        store.script(
            vec![
                OnCommit::Unknown {
                    applied: false,
                    may_still_commit: false,
                },
                OnCommit::Fail { transient: true },
            ],
            None,
        );
        let outcome = abort_session(&store, &id).await;
        stopped_by_a_transient_fault(outcome, &id);
        let commits = store.commits();
        assert_eq!(commits.len(), 2, "the unknown round, then the failed one");
        let expected = expected_fence(&id, &open);
        for commit in &commits {
            assert_eq!(
                commit.preconditions, expected.preconditions,
                "both rounds fenced the unchanged Open@0"
            );
        }
        assert_still_open(&store.inner, &id, &open).await;
        assert_eq!(count(&store.inner).await, before, "(E) mpuctl unchanged");
    })
    .await;
}

// ──────────────── (J) one builder, restore's shape ────────────────

/// **(J)** The shared builder puts what restore's `fence_session` put before it, in the same
/// order, for the restore fence's widest case — a `Completing@E` teardown with two obligations:
/// `require(mpu:<id> == read)`, then `require_absent` on each obligation key in order; then the
/// session, then each obligation in order; no delete. Every obligation it writes decodes against
/// the key it goes under.
#[test]
fn j_the_shared_builder_keeps_restores_preconditions_and_puts_in_order() {
    let (id, nonce) = identity(7);
    let open = Bytes::from(format!(
        "{{\"parent\":0,\"object\":\"photos/o\",\"created_at_millis\":1000,\
         \"clock_source\":\"wall\",\"segment_nonce\":\"{nonce}\",\"epoch\":0,\"attempts\":0,\
         \"state\":{{\"kind\":\"Open\"}}}}"
    ));
    let read = completing(&open);
    let teardown = decode_session_record(&read)
        .unwrap()
        .completing_teardown(&id)
        .expect("Completing@1 has a teardown");
    let owed = vec![teardown.bytes().clone(), teardown.records().clone()];
    let key = mpu_key(&id);
    let batch = fence_batch(&key, &read, teardown.session(), &owed);

    let token = RetireToken::Session {
        upload_id: id.clone(),
        epoch: 1,
        part: None,
    };
    let bytes_key = retire_key(RetireMode::Bytes, &token);
    let records_key = retire_key(RetireMode::Records, &token);
    assert_eq!(
        batch.preconditions,
        vec![
            Precondition {
                key: key.clone(),
                expected: Some(read.clone()),
            },
            Precondition {
                key: bytes_key.clone(),
                expected: None,
            },
            Precondition {
                key: records_key.clone(),
                expected: None,
            },
        ]
    );
    assert_eq!(
        batch.puts,
        vec![
            (key, metadata::encode(teardown.session())),
            (bytes_key, metadata::encode(teardown.bytes().payload())),
            (records_key, metadata::encode(teardown.records().payload())),
        ]
    );
    assert!(batch.deletes.is_empty());
    for (at, value) in &batch.puts[1..] {
        decode_retire_obligation(at, value).expect("every obligation decodes at its key");
    }
    let group = SegmentGroup::from_nonce(nonce, 1);
    let (_, _, records) = decode_retire_obligation(&batch.puts[2].0, &batch.puts[2].1).unwrap();
    assert_eq!(records.segments(), Some(&group));
}

/// The round budget is sized from `MAX_COMPLETE_ATTEMPTS`, as its derivation says: one round more
/// than the Complete fences that can move a session back to `Open`.
#[test]
fn the_round_budget_outlasts_every_complete_fence_by_one() {
    assert_eq!(ABORT_ROUNDS, MAX_COMPLETE_ATTEMPTS + 1);
}
