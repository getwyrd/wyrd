//! Issue #1007 — `CompleteMultipartUpload`'s **fence phase**,
//! `wyrd_core::multipart::complete::fence_complete`, driven over a real store
//! (`RedbMetadataStore::in_memory()`) on a multi-thread tokio runtime (proposal 0016's
//! *Complete fence* and *Fence release* rows, `0016:661`, `:664`, and decision 3, `0016:895-1036`).
//!
//! Every session here is made by Create (`create_session`, #894). Parts are planted as canonical
//! `part:` / `psum:` records through the key and record helpers, with no staging. A
//! `Completing`, `Completed` or `Aborting` session is that session's record overwritten with
//! canonical bytes the decoder accepts.
//!
//! * **(E)** Answers by state (`0016:976`): a `Completed` tombstone answers an identical request
//!   with the recorded publication and a different list `NoSuchUpload`; another publisher's
//!   `Completing` fence answers `OperationAborted` and is left byte-for-byte as it was, `seg:`
//!   range included; `Aborting`, absent and another target answer `NoSuchUpload`. None of them
//!   commits.
//! * **(D1)** Typos — an absent part, a digest mismatch, part numbers out of order — answer
//!   `InvalidPart` and leave the session `Open` with `attempts` as it was.
//! * **(D2)** A part replaced between the first read and the fence: the request fences, fails
//!   against the frozen set, releases, and answers `InvalidPart`, with `attempts` given back.
//!   After D1 and D2 a valid request still fences. After a rejected request an Abort fences
//!   (`0016:1033`).
//! * **(D4)** A fence the Complete owns and cannot release stops with the error that says the
//!   fence still stands, never with an answer.
//! * **(H)** A bucket deleted while the session is open answers `NoSuchBucket`, and the session is
//!   `Open` again with `attempts` given back (`0016:1035`).
//! * **(G0-fence)** An unknown outcome on the fence or the release returns the in-doubt error at
//!   once; a failed first read returns an error, never an answer.
//! * **The budget, away from zero.** D2, H and a held fence from `Open@E` with attempts already
//!   spent: the fence charges one more, a release restores that exact count.
//! * **Paging.** A frozen set larger than one page is read whole, and a page breaking the paging
//!   contract is a store fault.
//! * **A store fault behind the fence** tries one release; the error says what became of the
//!   fence, for each thing that release can do.
//! * **The stated bounds** are numbers written here, not the constants: a release that loses a
//!   `Conflict` (the TiKV lock) tries again and lands, and it stops at that bound.
//! * **A later `Open`.** A fence that loses to a session moved to a later `Open` charges and
//!   restores the attempts of the session it fenced, not of the one it first read.
//! * **The order of the checks.** The bucket comes first, the first bad part is the one reported,
//!   and an `Open` session's checks run in the order `fence_complete` documents.
//! * Beside those: the budget at its cap, a foreign clock, a fence that loses its CAS, a release
//!   that finds the session moved, a definite fence failure and a torn session record.
//!
//! The wrapper keeps its state in struct fields, never in a `static` (ADR-0035).

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, SegmentGroup, SegmentNonce};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::complete::{
    fence_complete, CompleteError, CompleteRequest, DoubtfulCommit, FenceOutcome, FenceState,
    HeldFence, InDoubt, NamedParts, Stop, Stopped,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    complete_fingerprint, decode_part_record, decode_part_summary, decode_session_record, mpu_key,
    multipart_etag, part_key, psum_key, psum_range, AbortOutcome, Budget, CompleteOutcome,
    CreateOutcome, Digest, InvalidPart, PartNumber, PartSummary, Publication, RecordError, Refusal,
    SessionRecord, SessionState, UploadId, MAX_COMPLETE_ATTEMPTS, MPU_PREFIX,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_traits::{
    classify, BoxError, CommitOutcome, CommitUnknownResult, ErrorClass, MetadataStore,
    Result as StoreResult, ScanPage, TransientFault, WriteBatch,
};

// ─────────────────────────────── fixtures ───────────────────────────────

const BUCKET: &str = "photos";
/// The root inode every Alpha object dirent lives under, as `<bucket>/<key>` (ADR-0046).
const ROOT: InodeId = 0;
const CLOCK: &str = "wall";
const CREATED_AT: u64 = 1_000;
const COMMITTED_AT: u64 = 1_500;
const FENCED_AT: u64 = 2_000;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
/// No leg may hang the suite: a schedule that never settles fails here instead.
const LEG_TIMEOUT: Duration = Duration::from_secs(120);
/// More commits than any Complete may make. A Complete past it never stops committing.
const RUNAWAY_COMMITS: usize = 100;
/// What a scripted plain fault says.
const FAULT: &str = "the test wrapper's store failed";
/// The stated bound on one Complete's release commits, written out rather than read from the
/// constant, so a change to the bound has to change this number too.
const STATED_RELEASE_ROUNDS: u32 = 4;
/// The stated bound on one Complete's fence commits, written out for the same reason.
const STATED_FENCE_ROUNDS: u32 = 4;

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

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
}

/// The digest whose 32 bytes are all `byte`.
fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}

/// The named list `(part number, digest byte)`, in the order given.
fn named(list: &[(u32, u8)]) -> NamedParts {
    NamedParts::new(list.iter().map(|&(n, d)| (part(n), digest(d))).collect()).unwrap()
}

/// The summary a one-chunk part `n` with `digest` carries.
fn summary(n: u32, digest: Digest) -> PartSummary {
    PartSummary {
        chunks: 1,
        len: u64::from(n) * 10,
        digest,
        committed_at_millis: COMMITTED_AT,
    }
}

/// The blind batch that plants part `n` with `digest`: its `part:` record and its `psum:`
/// summary, together, as the part commit writes them (`0016:660`), each in the codec's own
/// canonical spelling — checked by decoding it and re-encoding it.
fn plant_part(id: &UploadId, n: u32, digest: Digest) -> WriteBatch {
    let summary = summary(n, digest);
    let record = format!(
        "{{\"chunks\":[{{\"id\":{},\"scheme\":\"None\",\"len\":{},\"placement\":[]}}],\
         \"len\":{},\"digest\":\"{digest}\",\"committed_at_millis\":{COMMITTED_AT},\
         \"session_epoch\":0}}",
        1_000 + n,
        summary.len,
        summary.len,
    );
    let decoded = decode_part_record(record.as_bytes()).expect("a canonical part");
    assert_eq!(metadata::encode(&decoded), record.as_bytes());
    assert_eq!(decoded.digest(), &digest);
    let psum = metadata::encode(&summary);
    assert_eq!(decode_part_summary(&psum).unwrap(), summary);
    WriteBatch::new()
        .put(part_key(id, part(n)), Bytes::from(record))
        .put(psum_key(id, part(n)), psum)
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

const OPEN: &str = r#"{"kind":"Open"}"#;
const ABORTING: &str = r#"{"kind":"Aborting"}"#;

/// `open`'s session fenced by **another** publisher at epoch 1: `Completing@1`, one attempt
/// spent, two segments written.
fn completing(open: &Bytes) -> Bytes {
    let record = decode_session_record(open).unwrap();
    let state = format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":1900,\"segments_written\":2,\
         \"publish_target\":{{\"parent\":{},\"name\":{},\"epoch\":1}}}}",
        record.parent(),
        json_string(record.object()),
    );
    session_bytes(open, 1, 1, &state)
}

/// What the `Completed` tombstone below records.
fn recorded(list: &NamedParts) -> Publication {
    Publication {
        inode: 7,
        version: 3,
        etag: multipart_etag(list.as_slice()).unwrap(),
        completed_at_millis: 3_000,
    }
}

/// `open`'s session published by a Complete naming `list`: a `Completed` tombstone at epoch 2
/// recording [`recorded`] and `list`'s fingerprint.
fn completed(open: &Bytes, list: &NamedParts) -> Bytes {
    let publication = recorded(list);
    let state = format!(
        "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":{},\"version\":{},\"etag\":\"{}\",\
         \"completed_at_millis\":{},\"complete_fingerprint\":\"{}\"}}}}",
        publication.inode,
        publication.version,
        publication.etag,
        publication.completed_at_millis,
        complete_fingerprint(list.as_slice()).unwrap(),
    );
    session_bytes(open, 2, 1, &state)
}

fn unknown(may_still_commit: bool) -> BoxError {
    BoxError::from(CommitUnknownResult {
        backend: "test wrapper",
        code: None,
        detail: "the wrapper withheld the outcome".to_owned(),
        may_still_commit,
    })
}

fn fault(transient: bool) -> BoxError {
    if transient {
        BoxError::from(TransientFault::new(FAULT))
    } else {
        BoxError::from(FAULT)
    }
}

// ──────────────────────────── the logging wrapper ────────────────────────────

/// What [`Logged`] does with one commit of the Complete under test. Later slices add steps here
/// (a hold step that keeps a batch back until the test releases it, say).
#[derive(Debug, Clone)]
enum OnCommit {
    /// Delegate unchanged.
    Pass,
    /// Answer `Ok(Conflict)` without applying the batch or changing any record, as TiKV does when
    /// its locking read meets another writer's lock (`abort.rs:249-251`).
    ConflictUnmoved,
    /// Answer a definite store error (not an unknown outcome), unapplied.
    Fail { transient: bool },
    /// Apply the batch or not, then answer `CommitUnknownResult { may_still_commit }`.
    Unknown {
        applied: bool,
        may_still_commit: bool,
    },
    /// Apply this blind batch to the inner store first — re-plant a part, remove the bucket,
    /// move the session — then do the step it carries.
    WriteFirst(WriteBatch, Box<OnCommit>),
    /// Arm this for the reads after it — a read that fails only behind the fence — then do the
    /// step it carries.
    Arm(Arm, Box<OnCommit>),
}

/// Something [`Logged`] does to reads from the moment it is armed.
#[derive(Debug, Clone)]
enum Arm {
    /// Every `get` of a key under this prefix fails with a plain fault.
    FailGets(Vec<u8>),
    /// Every `scan_page` under this prefix fails with a plain fault.
    FailScans(Vec<u8>),
    /// The next `scan_page` calls answer these pages, one each, instead of asking the store: what
    /// a backend breaking the paging contract would answer.
    NextPages(Vec<ScanPage>),
}

impl OnCommit {
    fn write_first(write: WriteBatch, then: Self) -> Self {
        Self::WriteFirst(write, Box::new(then))
    }

    fn arm(arm: Arm, then: Self) -> Self {
        Self::Arm(arm, Box::new(then))
    }
}

#[derive(Default)]
struct Script {
    plan: VecDeque<OnCommit>,
    /// What every commit past `plan` does (`Pass` when unset).
    then: Option<OnCommit>,
    /// Every `get` of a key under this prefix fails with a plain fault.
    fail_gets_under: Option<Vec<u8>>,
    /// Every `scan_page` under this prefix fails with a plain fault.
    fail_scans_under: Option<Vec<u8>>,
    /// What the next `scan_page` calls answer.
    next_pages: VecDeque<ScanPage>,
    /// Every `scan_page` asked of this wrapper: `(after, limit)`, in order.
    scans: Vec<(Option<Vec<u8>>, usize)>,
    /// Every batch the Complete under test committed through this wrapper, in order.
    commits: Vec<WriteBatch>,
}

/// A store wrapper that logs every batch of the Complete under test and acts on each as scripted.
/// Fixture writes go to `inner` directly, so they are neither logged nor scripted.
struct Logged {
    inner: RedbMetadataStore,
    script: Mutex<Script>,
}

impl Logged {
    fn script(&self, plan: Vec<OnCommit>, then: Option<OnCommit>) {
        let mut script = self.state();
        script.plan = plan.into();
        script.then = then;
    }

    fn arm(&self, arm: Arm) {
        let mut script = self.state();
        match arm {
            Arm::FailGets(prefix) => script.fail_gets_under = Some(prefix),
            Arm::FailScans(prefix) => script.fail_scans_under = Some(prefix),
            Arm::NextPages(pages) => script.next_pages = pages.into(),
        }
    }

    /// Every armed read fault gone, for a request after the faulty one.
    fn clear_read_faults(&self) {
        let mut script = self.state();
        script.fail_gets_under = None;
        script.fail_scans_under = None;
    }

    /// Every `scan_page` asked so far, and the log emptied.
    fn take_scans(&self) -> Vec<(Option<Vec<u8>>, usize)> {
        std::mem::take(&mut self.state().scans)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, Script> {
        self.script.lock().unwrap()
    }

    /// Every batch committed so far, and the log emptied.
    fn take_commits(&self) -> Vec<WriteBatch> {
        std::mem::take(&mut self.state().commits)
    }

    async fn step(&self, act: OnCommit, batch: WriteBatch) -> StoreResult<CommitOutcome> {
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
            OnCommit::WriteFirst(write, then) => {
                assert_eq!(self.inner.commit(write).await?, CommitOutcome::Committed);
                Box::pin(self.step(*then, batch)).await
            }
            OnCommit::Arm(arm, then) => {
                self.arm(arm);
                Box::pin(self.step(*then, batch)).await
            }
        }
    }
}

#[async_trait]
impl MetadataStore for Logged {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        let fails = self
            .state()
            .fail_gets_under
            .as_ref()
            .is_some_and(|prefix| key.starts_with(prefix));
        if fails {
            return Err(fault(false));
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
        {
            let mut script = self.state();
            script.scans.push((after.map(<[u8]>::to_vec), limit));
            let fails = script
                .fail_scans_under
                .as_ref()
                .is_some_and(|under| prefix.starts_with(under));
            if fails {
                return Err(fault(false));
            }
            if let Some(page) = script.next_pages.pop_front() {
                return Ok(page);
            }
        }
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        let (act, made) = {
            let mut script = self.state();
            script.commits.push(batch.clone());
            let act = script.plan.pop_front().or_else(|| script.then.clone());
            (act.unwrap_or(OnCommit::Pass), script.commits.len())
        };
        assert!(
            made <= RUNAWAY_COMMITS,
            "the complete never stopped committing ({made} commits)"
        );
        self.step(act, batch).await
    }
}

// ─────────────────────────────── the upload ───────────────────────────────

/// One session made by Create on a store with its bucket, with parts planted.
struct Upload {
    store: Logged,
    id: UploadId,
    object: String,
    /// The exact bytes Create stored: `Open@0`, no attempt spent.
    open: Bytes,
}

/// A store holding the bucket and session 1, made by Create, with each `(n, digest byte)` of
/// `parts` planted as a committed part.
async fn upload(parts: &[(u32, u8)]) -> Upload {
    let inner = RedbMetadataStore::in_memory().unwrap();
    let bucket = WriteBatch::new().put(metadata::bucket_key(BUCKET), MARKER);
    assert_eq!(
        inner.commit(bucket).await.unwrap(),
        CommitOutcome::Committed
    );
    let object = format!("{BUCKET}/object-1");
    let request = CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object: &object,
        content_type: None,
        created_at_millis: CREATED_AT,
        clock_source: CLOCK,
    };
    let outcome = create_session(
        &inner,
        &profile_of(8),
        &request,
        &mut Fixed(Some(identity(1))),
    )
    .await
    .expect("the create ran");
    let CreateOutcome::Created { upload_id: id } = outcome else {
        panic!("expected Created, got {outcome:?}");
    };
    for &(n, d) in parts {
        let plant = plant_part(&id, n, digest(d));
        assert_eq!(inner.commit(plant).await.unwrap(), CommitOutcome::Committed);
    }
    let open = inner.get(&mpu_key(&id)).await.unwrap().unwrap();
    let record = decode_session_record(&open).unwrap();
    assert!(matches!(record.state(), SessionState::Open {}));
    assert_eq!((record.epoch(), record.attempts()), (0, 0));
    Upload {
        store: Logged {
            inner,
            script: Mutex::new(Script::default()),
        },
        id,
        object,
        open,
    }
}

/// [`upload`], with the session moved to `Open@epoch` and `attempts` already spent. Its bytes are
/// what an earlier fence and rollback leave.
async fn upload_at(parts: &[(u32, u8)], epoch: u64, attempts: u32) -> Upload {
    let up = upload(parts).await;
    up.plant(session_bytes(&up.open, epoch, attempts, OPEN))
        .await;
    up
}

impl Upload {
    /// A request for this upload's own target naming `parts`, on the session's own clock.
    fn request<'a>(&'a self, parts: &'a NamedParts) -> CompleteRequest<'a> {
        CompleteRequest {
            bucket: BUCKET,
            parent: ROOT,
            object: &self.object,
            parts,
            fenced_at_millis: FENCED_AT,
            clock_source: CLOCK,
        }
    }

    async fn complete(&self, parts: &NamedParts) -> Result<FenceOutcome, CompleteError> {
        fence_complete(&self.store, &self.id, &self.request(parts)).await
    }

    /// The session's stored bytes and record.
    async fn session(&self) -> (Bytes, SessionRecord) {
        let bytes = self.store.inner.get(&mpu_key(&self.id)).await.unwrap();
        let bytes = bytes.expect("the session");
        let record = decode_session_record(&bytes).expect("a valid session");
        (bytes, record)
    }

    /// Overwrite the session record with `bytes`.
    async fn plant(&self, bytes: Bytes) {
        let put = WriteBatch::new().put(mpu_key(&self.id), bytes);
        assert_eq!(
            self.store.inner.commit(put).await.unwrap(),
            CommitOutcome::Committed
        );
    }

    /// Every record in the store, in key order.
    async fn snapshot(&self) -> Vec<(Vec<u8>, Bytes)> {
        let mut all = self.store.inner.scan(b"").await.unwrap();
        all.sort();
        all
    }
}

// ─────────────────────────────── assertions ───────────────────────────────

/// The refusal an answered request was refused with.
fn refused(outcome: Result<FenceOutcome, CompleteError>) -> Refusal {
    match outcome {
        Ok(FenceOutcome::Answered(CompleteOutcome::Refused(refusal))) => refusal,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

fn held(outcome: Result<FenceOutcome, CompleteError>) -> Box<HeldFence> {
    match outcome {
        Ok(FenceOutcome::Held(fence)) => fence,
        other => panic!("expected the held fence, got {other:?}"),
    }
}

/// The stopped error for `id`, which never classifies as indeterminate and carries no
/// `CommitUnknownResult` anywhere on its source chain.
fn stopped_error(
    outcome: Result<FenceOutcome, CompleteError>,
    id: &UploadId,
) -> (String, Box<Stopped>) {
    let err = match outcome {
        Err(err @ CompleteError::Stopped(_)) => err,
        other => panic!("expected the stopped error, got {other:?}"),
    };
    assert_ne!(
        classify(&err),
        ErrorClass::Indeterminate,
        "nothing is in doubt: {err}"
    );
    let mut source = std::error::Error::source(&err);
    while let Some(cause) = source {
        assert!(
            !cause.is::<CommitUnknownResult>(),
            "no unknown outcome: {err}"
        );
        source = cause.source();
    }
    let text = err.to_string();
    assert!(text.contains(id.as_str()), "it names the upload id: {text}");
    let has_source = std::error::Error::source(&err).is_some();
    let CompleteError::Stopped(stopped) = err else {
        unreachable!()
    };
    assert_eq!(&stopped.upload_id, id);
    assert_says_only(&text, &stopped.fence);
    assert!(
        text.contains(&stopped.cause.to_string()),
        "it says the cause: {text}"
    );
    assert_eq!(
        has_source,
        matches!(stopped.cause, Stop::Store(_) | Stop::Record(_)),
        "the source is the cause's own error: {text}"
    );
    (text, stopped)
}

/// The error's text says what `fence` says and none of what the other states say: a caller reading
/// it is never told a fence that stands has gone, or the other way round.
fn assert_says_only(text: &str, fence: &FenceState) {
    let said = match fence {
        FenceState::NotPlaced => "no fence of this complete landed".to_owned(),
        FenceState::Stands { epoch } => format!("fence at Completing@{epoch} still stands"),
        FenceState::Released { epoch } => format!("fence at Completing@{epoch} was taken down"),
        FenceState::Gone { epoch } => format!("fence at Completing@{epoch} no longer stands"),
        FenceState::Unknown { epoch } => {
            format!("fence at Completing@{epoch} may or may not stand")
        }
    };
    assert!(text.contains(&said), "expected {said:?} in: {text}");
    for other in [
        "no fence of this complete landed",
        "still stands",
        "was taken down",
        "no longer stands",
        "may or may not stand",
    ] {
        assert_eq!(
            text.contains(other),
            said.contains(other),
            "{other:?} in: {text}"
        );
    }
}

/// `cause` is the plain fault the wrapper's armed reads fail with, not a commit's fault.
fn assert_read_fault(cause: &Stop) {
    assert!(
        matches!(cause, Stop::Store(fault) if fault.to_string() == FAULT && !fault.is::<TransientFault>()),
        "{cause:?}"
    );
}

/// The in-doubt error for `id` about `commit`: it classifies as indeterminate, its source is the
/// store's own unknown outcome, and it carries `may_still_commit` as the store said it.
fn in_doubt_error(
    outcome: Result<FenceOutcome, CompleteError>,
    id: &UploadId,
    commit: DoubtfulCommit,
    may_still_commit: bool,
) -> Box<InDoubt> {
    let err = match outcome {
        Err(err @ CompleteError::InDoubt(_)) => err,
        other => panic!("expected the in-doubt error, never an answer: got {other:?}"),
    };
    assert_eq!(classify(&err), ErrorClass::Indeterminate, "{err}");
    let source = std::error::Error::source(&err)
        .and_then(|source| source.downcast_ref::<CommitUnknownResult>())
        .expect("the source is the store's unknown outcome");
    assert_eq!(source.may_still_commit, may_still_commit);
    let text = err.to_string();
    assert!(
        text.contains(id.as_str()) && text.contains("in doubt"),
        "{text}"
    );
    let about = match commit {
        DoubtfulCommit::Fence { epoch } => format!("its fence at Completing@{epoch}"),
        DoubtfulCommit::Release { epoch } => {
            format!("the release of its fence at Completing@{epoch}")
        }
    };
    assert!(text.contains(&about), "expected {about:?} in: {text}");
    let may = if may_still_commit {
        "may still be applied"
    } else {
        "may have been applied"
    };
    assert!(text.contains(may), "expected {may:?} in: {text}");
    let CompleteError::InDoubt(doubt) = err else {
        unreachable!()
    };
    assert_eq!(
        text.contains("followed a fault behind the fence"),
        doubt.fault_behind_fence.is_some(),
        "the text says a fault only when there was one: {text}"
    );
    if let Some(fault) = &doubt.fault_behind_fence {
        assert!(
            text.contains(&fault.to_string()),
            "it says the fault: {text}"
        );
    }
    assert_eq!(&doubt.upload_id, id);
    assert_eq!(doubt.commit, commit);
    assert_eq!(doubt.may_still_commit, may_still_commit);
    assert_eq!(doubt.outcome.may_still_commit, may_still_commit);
    doubt
}

/// A fence or release batch for `id`: one `require` on `mpu:<id>` and one put there, nothing
/// else. Returns the bytes it requires and the bytes it puts.
fn session_cas(batch: &WriteBatch, id: &UploadId) -> (Bytes, Bytes) {
    assert_eq!(batch.preconditions.len(), 1, "{batch:?}");
    assert_eq!(batch.preconditions[0].key, mpu_key(id));
    assert_eq!(batch.puts.len(), 1, "{batch:?}");
    assert_eq!(batch.puts[0].0, mpu_key(id));
    assert!(batch.deletes.is_empty(), "{batch:?}");
    let required = batch.preconditions[0].expected.clone();
    (
        required.expect("an exact-value require"),
        batch.puts[0].1.clone(),
    )
}

/// `before`'s session fenced by this Complete: `Completing@E+1`, stamped `FENCED_AT`, no segment
/// written, its own target at the new epoch, one more attempt spent.
fn assert_fenced(fenced: &SessionRecord, before: &SessionRecord) {
    let epoch = before.epoch() + 1;
    assert_eq!(fenced.epoch(), epoch);
    assert_eq!(
        fenced.attempts(),
        before.attempts() + 1,
        "the fence spends one attempt"
    );
    let SessionState::Completing {
        fenced_at_millis,
        segments_written,
        publish_target,
    } = fenced.state()
    else {
        panic!("expected Completing, got {:?}", fenced.state());
    };
    assert_eq!((*fenced_at_millis, *segments_written), (FENCED_AT, 0));
    assert_eq!(publish_target.parent, before.parent());
    assert_eq!(publish_target.name, before.object());
    assert_eq!(publish_target.epoch, epoch);
}

/// A rejected request that fenced and released: exactly two commits, the fence (requiring the
/// bytes read, putting `Completing@E+1`) and the release (requiring the fence's bytes, putting
/// `Open@E+2` with `attempts` as before), and the store holds what the release put.
async fn assert_fenced_and_released(up: &Upload, before: (&Bytes, &SessionRecord)) {
    let commits = up.store.take_commits();
    assert_eq!(commits.len(), 2, "a fence and its release: {commits:?}");
    assert_batches_fenced_and_released(up, &commits, before).await;
}

/// `commits` are a fence and its release, as [`assert_fenced_and_released`] states, and `before` is
/// the session that fence was built on.
async fn assert_batches_fenced_and_released(
    up: &Upload,
    commits: &[WriteBatch],
    before: (&Bytes, &SessionRecord),
) {
    let (before_bytes, before) = before;
    let (required, fence) = session_cas(&commits[0], &up.id);
    assert_eq!(&required, before_bytes, "the fence requires the bytes read");
    assert_fenced(&decode_session_record(&fence).unwrap(), before);
    let (required, release) = session_cas(&commits[1], &up.id);
    assert_eq!(
        required, fence,
        "the release requires the fence's own bytes"
    );
    let (stored, after) = up.session().await;
    assert_eq!(stored, release, "the release landed");
    assert!(matches!(after.state(), SessionState::Open {}), "Open again");
    assert_eq!(after.epoch(), before.epoch() + 2, "Open@E+2");
    assert_eq!(
        after.attempts(),
        before.attempts(),
        "the attempt is given back"
    );
    let expected = session_bytes(&up.open, before.epoch() + 2, before.attempts(), OPEN);
    assert_eq!(
        release, expected,
        "Create's session, at Open@E+2, nothing else changed"
    );
}

/// The parts every D leg plants, and the valid list over them.
const PARTS: [(u32, u8); 3] = [(1, 0x11), (2, 0x22), (3, 0x33)];

/// `attempts` already spent when a request arrives, away from zero: a release that restores a
/// constant, a fence that charges one, and a report of the count that is always zero all fail here.
const SPENT: [u32; 2] = [1, MAX_COMPLETE_ATTEMPTS - 1];
/// The epoch a session with attempts already spent is planted at. Its fence is at `E+1`.
const LATE_EPOCH: u64 = 4;

/// The fence commit goes through, with `arm` armed for the reads behind it.
fn fence_then(arm: Arm) -> OnCommit {
    OnCommit::arm(arm, OnCommit::Pass)
}

/// The fence commit goes through, with part 2 replaced just before it lands: D2's fixture.
fn fence_with_part_2_replaced(id: &UploadId) -> OnCommit {
    OnCommit::write_first(plant_part(id, 2, digest(0x23)), OnCommit::Pass)
}

/// The refusal D2's fixture answers.
fn part_2_replaced() -> Refusal {
    Refusal::InvalidPart {
        part_number: part(2),
        reason: InvalidPart::DigestMismatch,
    }
}

// ───────────────────────────── (E) answers by state ─────────────────────────────

/// **(E)** A `Completed` tombstone planted with a publication answers an identical request with
/// the **recorded** publication — same ETag, inode and version — and a different list
/// `NoSuchUpload`. Neither commits, and the store is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_a_completed_tombstone_answers_an_identical_retry_with_the_recorded_publication() {
    within(async {
        let up = upload(&PARTS).await;
        let list = named(&PARTS);
        up.plant(completed(&up.open, &list)).await;
        let before = up.snapshot().await;

        let outcome = up.complete(&list).await;
        let Ok(FenceOutcome::Answered(CompleteOutcome::AlreadyCompleted(publication))) = outcome
        else {
            panic!("expected AlreadyCompleted, got {outcome:?}");
        };
        assert_eq!(publication, recorded(&list), "the recorded publication");

        for other in [
            named(&PARTS[..2]),
            named(&[(1, 0x11), (2, 0x22), (3, 0x34)]),
        ] {
            assert_eq!(refused(up.complete(&other).await), Refusal::NoSuchUpload);
        }
        assert!(up.store.take_commits().is_empty(), "zero commits");
        assert_eq!(up.snapshot().await, before, "nothing written");
    })
    .await;
}

/// **(E)** Against another publisher's `Completing` fence the request answers
/// `OperationAborted`, sends no commit, and leaves that session record and its `seg:` range
/// byte-for-byte as they were (`0016:953-958`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_another_publishers_completing_fence_is_refused_and_left_untouched() {
    within(async {
        let up = upload(&PARTS).await;
        let theirs = completing(&up.open);
        up.plant(theirs.clone()).await;
        let group = SegmentGroup::from_nonce(up.session().await.1.segment_nonce().clone(), 1);
        let mut segments = WriteBatch::new();
        for index in 0..2u32 {
            let chunk = ChunkRef {
                id: 900 + u128::from(index),
                scheme: EcScheme::None,
                len: 10,
                placement: Vec::new(),
            };
            let record = metadata::SegmentRecord::new(vec![chunk], u64::from(index) * 10).unwrap();
            segments = segments.put(
                metadata::seg_key(&group, index).unwrap(),
                metadata::encode(&record),
            );
        }
        assert_eq!(
            up.store.inner.commit(segments).await.unwrap(),
            CommitOutcome::Committed
        );
        let seg_range = metadata::seg_range_prefix(&group);
        let segs_before = up.store.inner.scan(&seg_range).await.unwrap();
        assert_eq!(segs_before.len(), 2);
        let before = up.snapshot().await;

        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::OperationAborted
        );

        assert!(up.store.take_commits().is_empty(), "zero commits");
        assert_eq!(
            up.session().await.0,
            theirs,
            "their fence is byte-for-byte unchanged"
        );
        let mut segs_after = up.store.inner.scan(&seg_range).await.unwrap();
        let mut segs_before = segs_before;
        segs_after.sort();
        segs_before.sort();
        assert_eq!(segs_after, segs_before, "their seg: range is unchanged");
        assert_eq!(up.snapshot().await, before, "nothing written");
    })
    .await;
}

/// **(E)** A request naming another target than the session's own, by parent or by object, answers
/// `NoSuchUpload` whatever state the session is in: the list and the state are of no interest for
/// an upload the request cannot complete. Nothing is written, and an identical list against a
/// tombstone gets no publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_another_target_answers_no_such_upload_in_every_state() {
    within(async {
        let up = upload(&PARTS).await;
        let list = named(&PARTS);
        let other_object = format!("{BUCKET}/another-object");
        let by_parent = CompleteRequest {
            parent: ROOT + 1,
            ..up.request(&list)
        };
        let by_object = CompleteRequest {
            object: &other_object,
            ..up.request(&list)
        };
        let states = [
            ("Open", up.open.clone()),
            ("Completing", completing(&up.open)),
            ("Completed", completed(&up.open, &list)),
        ];
        for (state, bytes) in states {
            up.plant(bytes.clone()).await;
            for (how, request) in [("parent", by_parent), ("object", by_object)] {
                assert_eq!(
                    refused(fence_complete(&up.store, &up.id, &request).await),
                    Refusal::NoSuchUpload,
                    "{state}, another {how}"
                );
                assert!(up.store.take_commits().is_empty(), "{state}: zero commits");
                assert_eq!(up.session().await.0, bytes, "{state}: left as it was");
            }
        }
    })
    .await;
}

/// **(E)** `Aborting`, an absent record, and a request naming another target than the session's
/// own all answer `NoSuchUpload`, with no commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e_aborting_absent_or_another_target_answers_no_such_upload() {
    within(async {
        let up = upload(&PARTS).await;
        let list = named(&PARTS);

        let other = format!("{BUCKET}/another-object");
        let request = CompleteRequest {
            object: &other,
            ..up.request(&list)
        };
        let outcome = fence_complete(&up.store, &up.id, &request).await;
        assert_eq!(refused(outcome), Refusal::NoSuchUpload, "another target");
        assert_eq!(up.session().await.0, up.open);

        let (absent, _) = identity(2);
        let outcome = fence_complete(&up.store, &absent, &up.request(&list)).await;
        assert_eq!(refused(outcome), Refusal::NoSuchUpload, "no record");

        up.plant(session_bytes(&up.open, 1, 0, ABORTING)).await;
        assert_eq!(
            refused(up.complete(&list).await),
            Refusal::NoSuchUpload,
            "Aborting"
        );
        assert!(up.store.take_commits().is_empty(), "zero commits");
    })
    .await;
}

// ─────────────────── (D1, D2) the budget a client's mistakes never spend ───────────────────

/// **(D1, D2)** On one session:
///
/// * **D1** — `MAX_COMPLETE_ATTEMPTS` requests in a row with invalid lists (an absent part, a
///   digest mismatch, part numbers out of order) each answer `InvalidPart`, and after each the
///   session is `Open` with `attempts` as it was before that request.
/// * **D2** — `MAX_COMPLETE_ATTEMPTS` requests in a row, each naming a list valid against the
///   part set when first read, while the wrapper re-plants part 2 with a new digest just before
///   the fence commits. Each fences, fails against the frozen set, releases, and answers
///   `InvalidPart`, with the session `Open` again and `attempts` given back.
///
/// Then a request with a valid list **fences** — the held fence at `Completing@E'+1` with
/// `attempts` one higher — rather than answering `CompleteAttemptsExhausted`: the #636 wedge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_typos_and_a_part_replaced_before_the_fence_never_spend_the_budget() {
    within(async {
        let up = upload(&PARTS).await;

        // D1.
        let typos = [
            (named(&[(1, 0x11), (4, 0x44)]), 4, InvalidPart::Absent),
            (
                named(&[(1, 0x11), (2, 0x99)]),
                2,
                InvalidPart::DigestMismatch,
            ),
            (named(&[(2, 0x22), (1, 0x11)]), 1, InvalidPart::OutOfOrder),
            // A part named twice is out of order, at the second naming.
            (
                named(&[(1, 0x11), (2, 0x22), (2, 0x22)]),
                2,
                InvalidPart::OutOfOrder,
            ),
            // The first part out of place is the one reported, not the lowest or the last.
            (
                named(&[(1, 0x11), (3, 0x33), (2, 0x22), (1, 0x11)]),
                2,
                InvalidPart::OutOfOrder,
            ),
        ];
        assert!(typos.len() >= MAX_COMPLETE_ATTEMPTS as usize);
        for (k, (list, part_number, reason)) in typos.iter().enumerate() {
            let (before_bytes, before) = up.session().await;
            let refusal = refused(up.complete(list).await);
            assert_eq!(
                refusal,
                Refusal::InvalidPart {
                    part_number: part(*part_number),
                    reason: *reason,
                },
                "typo {k}"
            );
            let (after_bytes, after) = up.session().await;
            assert!(
                matches!(after.state(), SessionState::Open {}),
                "typo {k}: Open"
            );
            assert_eq!(
                after.attempts(),
                before.attempts(),
                "typo {k}: no attempt spent"
            );
            // A typo does not fence: a fence would stop every in-flight `UploadPart`.
            assert!(up.store.take_commits().is_empty(), "typo {k}: zero commits");
            assert_eq!(after_bytes, before_bytes, "typo {k}: the session's bytes");
        }

        // D2.
        let mut current = 0x22u8;
        for k in 0..MAX_COMPLETE_ATTEMPTS {
            let list = named(&[(1, 0x11), (2, current), (3, 0x33)]);
            let replaced = current + 1;
            up.store.script(
                vec![OnCommit::write_first(
                    plant_part(&up.id, 2, digest(replaced)),
                    OnCommit::Pass,
                )],
                None,
            );
            let (before_bytes, before) = up.session().await;
            let refusal = refused(up.complete(&list).await);
            assert_eq!(
                refusal,
                Refusal::InvalidPart {
                    part_number: part(2),
                    reason: InvalidPart::DigestMismatch,
                },
                "replacement {k}"
            );
            assert_fenced_and_released(&up, (&before_bytes, &before)).await;
            current = replaced;
        }

        // A valid request fences.
        let (before_bytes, before) = up.session().await;
        assert_eq!(before.attempts(), 0, "no request spent the budget");
        let list = named(&[(1, 0x11), (2, current), (3, 0x33)]);
        let fence = held(up.complete(&list).await);
        assert_fenced(fence.session(), &before);
        assert_eq!(fence.epoch(), before.epoch() + 1, "Completing@E'+1");
        assert_eq!(fence.session().attempts(), 1, "one attempt higher");
        assert_eq!(fence.attempts_before(), 0);
        assert_eq!(fence.upload_id(), &up.id);
        let (stored, _) = up.session().await;
        assert_eq!(&stored, fence.session_bytes(), "the store holds the fence");
        assert_eq!(fence.named_parts(), list.as_slice());
        let frozen: BTreeMap<PartNumber, PartSummary> = [(1, 0x11), (2, current), (3, 0x33)]
            .into_iter()
            .map(|(n, d)| (part(n), summary(n, digest(d))))
            .collect();
        assert_eq!(fence.frozen_parts(), &frozen, "the frozen set");
        assert_eq!(
            fence.bucket_marker().as_ref(),
            MARKER,
            "the marker bytes read"
        );
        let commits = up.store.take_commits();
        assert_eq!(commits.len(), 1, "one fence commit and nothing after it");
        let (required, put) = session_cas(&commits[0], &up.id);
        assert_eq!((required, &put), (before_bytes, fence.session_bytes()));
    })
    .await;
}

/// **(D)** After a rejected request, an Abort answers `Fenced` (`0016:1033`): the request left the
/// session `Open`, never in `Completing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_after_a_rejected_request_an_abort_fences() {
    within(async {
        let up = upload(&PARTS).await;
        up.store.script(
            vec![OnCommit::write_first(
                plant_part(&up.id, 2, digest(0x23)),
                OnCommit::Pass,
            )],
            None,
        );
        let refusal = refused(up.complete(&named(&PARTS)).await);
        assert!(
            matches!(refusal, Refusal::InvalidPart { .. }),
            "{refusal:?}"
        );

        let outcome = abort_session(&up.store.inner, &up.id).await;
        assert!(matches!(outcome, Ok(AbortOutcome::Fenced)), "{outcome:?}");
    })
    .await;
}

// ──────────────────── (D4) a fence it owns and cannot release ────────────────────

/// **(D4)** Set up as D2: the fence lands with a definite success and the frozen list is
/// invalid. The wrapper answers every release `Conflict` without applying it or changing the
/// session (the TiKV lock case). The request returns the **stopped** error: it names the upload
/// and the epoch of the fence it still holds (1), says the fence still stands, its cause says the
/// release rounds ran out, and it carries no `CommitUnknownResult`. The session is `Completing@1`,
/// byte-identical to the fence's bytes, after exactly [`STATED_RELEASE_ROUNDS`] release commits.
///
/// A second run answers the release with a definite store error: the same stopped error, with
/// that store error as its cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d4_a_fence_it_owns_and_cannot_release_stops_with_the_fence_standing() {
    within(async {
        // Release rounds run out.
        let up = upload(&PARTS).await;
        up.store.script(
            vec![fence_with_part_2_replaced(&up.id)],
            Some(OnCommit::ConflictUnmoved),
        );
        let (text, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::Stands { epoch: 1 });
        assert!(
            matches!(stopped.cause, Stop::ReleaseRoundsSpent { rounds } if rounds == STATED_RELEASE_ROUNDS),
            "{:?}",
            stopped.cause
        );
        assert!(text.contains("Completing@1") && text.contains("still stands"), "{text}");
        assert!(text.contains("release rounds"), "{text}");
        let commits = up.store.take_commits();
        let (_, fence) = session_cas(&commits[0], &up.id);
        let releases = &commits[1..];
        assert_eq!(releases.len() as u32, STATED_RELEASE_ROUNDS, "at most the stated bound");
        for release in releases {
            assert_eq!(session_cas(release, &up.id).0, fence, "each release requires the fence");
        }
        let (stored, record) = up.session().await;
        assert_eq!(stored, fence, "Completing@1, byte-identical to the fence's bytes");
        assert!(matches!(record.state(), SessionState::Completing { .. }));
        assert_eq!(record.epoch(), 1);

        // The release fails for good.
        let up = upload(&PARTS).await;
        up.store.script(
            vec![
                OnCommit::write_first(plant_part(&up.id, 2, digest(0x23)), OnCommit::Pass),
                OnCommit::Fail { transient: true },
            ],
            None,
        );
        let outcome = up.complete(&named(&PARTS)).await;
        let err = outcome.as_ref().err();
        assert_eq!(err.map(|err| classify(err)), Some(ErrorClass::Transient));
        let source = err
            .and_then(std::error::Error::source)
            .and_then(|source| source.downcast_ref::<TransientFault>())
            .map(ToString::to_string);
        assert!(
            source.as_ref().is_some_and(|text| text.contains(FAULT)),
            "the store error is the cause: {source:?}"
        );
        let (text, stopped) = stopped_error(outcome, &up.id);
        assert_eq!(stopped.fence, FenceState::Stands { epoch: 1 });
        assert!(
            matches!(&stopped.cause, Stop::Store(fault) if fault.is::<TransientFault>()),
            "{:?}",
            stopped.cause
        );
        assert!(text.contains("Completing@1") && text.contains("still stands"), "{text}");
        let commits = up.store.take_commits();
        assert_eq!(commits.len(), 2, "the fence and one release");
        assert_eq!(up.session().await.0, session_cas(&commits[0], &up.id).1);
    })
    .await;
}

/// **(D4's other side: the TiKV lock case)** A release that loses a `Conflict` with the session
/// unchanged tries again, and the one that lands answers the rejection, with the session `Open`
/// and the attempts it had: one lost `Conflict`, and as many as the bound leaves room for. A
/// bound of one release would leave a fence standing on a single lock conflict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_that_loses_a_conflict_with_the_session_unchanged_tries_again_and_lands() {
    within(async {
        for lost in [1, STATED_RELEASE_ROUNDS as usize - 1] {
            let up = upload_at(&PARTS, LATE_EPOCH, SPENT[0]).await;
            let mut plan = vec![fence_with_part_2_replaced(&up.id)];
            plan.extend(vec![OnCommit::ConflictUnmoved; lost]);
            up.store.script(plan, None);
            let (before_bytes, before) = up.session().await;

            assert_eq!(
                refused(up.complete(&named(&PARTS)).await),
                part_2_replaced(),
                "{lost} lost"
            );

            let commits = up.store.take_commits();
            assert_eq!(
                commits.len(),
                1 + lost + 1,
                "the fence, {lost} lost, the one that lands"
            );
            let (_, fence) = session_cas(&commits[0], &up.id);
            let (_, release) = session_cas(&commits[lost + 1], &up.id);
            for attempt in &commits[1..] {
                assert_eq!(
                    session_cas(attempt, &up.id),
                    (fence.clone(), release.clone()),
                    "every try is the same batch"
                );
            }
            assert_batches_fenced_and_released(
                &up,
                &[commits[0].clone(), commits[lost + 1].clone()],
                (&before_bytes, &before),
            )
            .await;
        }
    })
    .await;
}

// ───────────────────────────── (H) bucket deleted ─────────────────────────────

/// **(H)** With the bucket record removed while the session is open, the request answers
/// `NoSuchBucket` (`0016:1035`). Removed before the request, nothing is fenced; removed by the
/// wrapper just before the fence commits, the request fences, finds the bucket gone behind its
/// fence, and releases it. Either way the session is `Open` with `attempts` as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h_a_bucket_deleted_while_the_session_is_open_answers_no_such_bucket() {
    within(async {
        let remove = || WriteBatch::new().delete(metadata::bucket_key(BUCKET));

        let up = upload(&PARTS).await;
        assert_eq!(
            up.store.inner.commit(remove()).await.unwrap(),
            CommitOutcome::Committed
        );
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::NoSuchBucket
        );
        assert!(up.store.take_commits().is_empty(), "nothing is fenced");
        assert_eq!(up.session().await.0, up.open, "the session's bytes");

        let up = upload(&PARTS).await;
        up.store
            .script(vec![OnCommit::write_first(remove(), OnCommit::Pass)], None);
        let (before_bytes, before) = up.session().await;
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::NoSuchBucket
        );
        assert_fenced_and_released(&up, (&before_bytes, &before)).await;
    })
    .await;
}

// ──────────────────── (G0-fence) unknown outcomes stop in doubt ────────────────────

/// **(G0-fence)** The wrapper answers `CommitUnknownResult` on the initial fence, applied or not,
/// with either `may_still_commit`. Each time the request returns the in-doubt error carrying that
/// outcome, never a `CompleteOutcome` (the list is valid, so a fence taken as won would be held),
/// and sends no further commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g0_an_unknown_fence_outcome_returns_the_in_doubt_error_and_nothing_more() {
    within(async {
        for (applied, may_still_commit) in
            [(true, false), (false, true), (true, true), (false, false)]
        {
            let up = upload(&PARTS).await;
            up.store.script(
                vec![OnCommit::Unknown {
                    applied,
                    may_still_commit,
                }],
                None,
            );
            let outcome = up.complete(&named(&PARTS)).await;
            let doubt = in_doubt_error(
                outcome,
                &up.id,
                DoubtfulCommit::Fence { epoch: 1 },
                may_still_commit,
            );
            assert!(doubt.fault_behind_fence.is_none(), "{doubt:?}");
            let commits = up.store.take_commits();
            assert_eq!(commits.len(), 1, "no commit after the unknown fence");
            let (stored, _) = up.session().await;
            let expected = if applied {
                session_cas(&commits[0], &up.id).1
            } else {
                up.open.clone()
            };
            assert_eq!(stored, expected, "applied={applied}");
        }
    })
    .await;
}

/// **(G0-fence)** The wrapper answers `CommitUnknownResult` on the release of a fence the request
/// owns (set up as D2). The request returns the in-doubt error about the release, carrying that
/// outcome, and sends no further commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g0_an_unknown_release_outcome_returns_the_in_doubt_error_and_nothing_more() {
    within(async {
        for (applied, may_still_commit) in [(false, true), (true, false)] {
            let up = upload(&PARTS).await;
            up.store.script(
                vec![
                    OnCommit::write_first(plant_part(&up.id, 2, digest(0x23)), OnCommit::Pass),
                    OnCommit::Unknown {
                        applied,
                        may_still_commit,
                    },
                ],
                None,
            );
            let outcome = up.complete(&named(&PARTS)).await;
            let doubt = in_doubt_error(
                outcome,
                &up.id,
                DoubtfulCommit::Release { epoch: 1 },
                may_still_commit,
            );
            assert!(doubt.fault_behind_fence.is_none(), "a rejection: {doubt:?}");
            let commits = up.store.take_commits();
            assert_eq!(
                commits.len(),
                2,
                "the fence, the release, and nothing after"
            );
            let (_, record) = up.session().await;
            let expect_open = applied;
            assert_eq!(
                matches!(record.state(), SessionState::Open {}),
                expect_open,
                "applied={applied}: {:?}",
                record.state()
            );
        }
    })
    .await;
}

/// **(G0-fence)** The first read of `mpu:<id>` fails with a store error: the request returns an
/// error, never an answer, and sends no commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g0_a_failed_first_read_of_the_session_is_an_error_never_an_answer() {
    within(async {
        let up = upload(&PARTS).await;
        up.store.arm(Arm::FailGets(MPU_PREFIX.to_vec()));
        let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::NotPlaced);
        assert!(
            matches!(&stopped.cause, Stop::Store(fault) if fault.to_string() == FAULT),
            "{:?}",
            stopped.cause
        );
        assert!(up.store.take_commits().is_empty(), "zero commits");
    })
    .await;
}

// ──────────────────────────────── beside those ────────────────────────────────

/// A session that has spent `MAX_COMPLETE_ATTEMPTS` fences answers `CompleteAttemptsExhausted`
/// with no commit (`0016:661`), and stays abortable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_budget_at_its_cap_refuses_without_a_commit() {
    within(async {
        let up = upload(&PARTS).await;
        let spent = session_bytes(&up.open, 6, MAX_COMPLETE_ATTEMPTS, OPEN);
        up.plant(spent.clone()).await;
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::CompleteAttemptsExhausted
        );
        assert!(up.store.take_commits().is_empty(), "zero commits");
        assert_eq!(up.session().await.0, spent);
        let outcome = abort_session(&up.store.inner, &up.id).await;
        assert!(matches!(outcome, Ok(AbortOutcome::Fenced)), "{outcome:?}");

        // `attempts` above the cap, as a lowered knob leaves it, refuses the same way.
        let up = upload(&PARTS).await;
        let above = session_bytes(&up.open, 6, MAX_COMPLETE_ATTEMPTS + 1, OPEN);
        up.plant(above.clone()).await;
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::CompleteAttemptsExhausted
        );
        assert!(up.store.take_commits().is_empty(), "zero commits");
        assert_eq!(up.session().await.0, above);
    })
    .await;
}

/// A request whose clock is not the session's is stopped before anything is written: its fence
/// stamp would mix clocks in one session's lifecycle (`0016:1966-1976`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_on_a_foreign_clock_is_stopped_before_anything_is_written() {
    within(async {
        let up = upload(&PARTS).await;
        let list = named(&PARTS);
        let request = CompleteRequest {
            clock_source: "logical",
            ..up.request(&list)
        };
        let outcome = fence_complete(&up.store, &up.id, &request).await;
        let (_, stopped) = stopped_error(outcome, &up.id);
        assert_eq!(stopped.fence, FenceState::NotPlaced);
        assert!(
            matches!(&stopped.cause, Stop::ForeignClock { session, request }
                if session == CLOCK && request == "logical"),
            "{:?}",
            stopped.cause
        );
        assert!(up.store.take_commits().is_empty(), "zero commits");
        assert_eq!(up.session().await.0, up.open);
    })
    .await;
}

/// A fence commit that answers `Conflict` is answered from what the store then holds: a rival
/// publisher's fence that landed first answers `OperationAborted` and is left as it is; a
/// conflict with the session unchanged spends a round and the request fences on the next; and a
/// session that stays unchanged through every round stops with no fence placed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fence_that_loses_its_cas_is_answered_from_what_the_store_holds() {
    within(async {
        let up = upload(&PARTS).await;
        let theirs = completing(&up.open);
        let rival = WriteBatch::new().put(mpu_key(&up.id), theirs.clone());
        up.store
            .script(vec![OnCommit::write_first(rival, OnCommit::Pass)], None);
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::OperationAborted
        );
        assert_eq!(up.store.take_commits().len(), 1, "one fence, lost");
        assert_eq!(up.session().await.0, theirs, "their fence is untouched");

        let up = upload(&PARTS).await;
        up.store.script(vec![OnCommit::ConflictUnmoved], None);
        let fence = held(up.complete(&named(&PARTS)).await);
        assert_eq!(fence.epoch(), 1);
        assert_eq!(
            up.store.take_commits().len(),
            2,
            "one round spent, then the fence"
        );

        let up = upload(&PARTS).await;
        up.store.script(Vec::new(), Some(OnCommit::ConflictUnmoved));
        let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::NotPlaced);
        assert!(
            matches!(stopped.cause, Stop::FenceRoundsSpent { rounds } if rounds == STATED_FENCE_ROUNDS),
            "{:?}",
            stopped.cause
        );
        assert_eq!(up.store.take_commits().len() as u32, STATED_FENCE_ROUNDS);
        assert_eq!(up.session().await.0, up.open, "no fence placed");

        // The last round the bound leaves still fences.
        let up = upload(&PARTS).await;
        let lost = STATED_FENCE_ROUNDS as usize - 1;
        up.store.script(vec![OnCommit::ConflictUnmoved; lost], None);
        let fence = held(up.complete(&named(&PARTS)).await);
        assert_eq!(fence.epoch(), 1);
        assert_eq!(up.store.take_commits().len(), lost + 1, "the bound's last round");
    })
    .await;
}

/// A release that finds the session moved off its fence is answered from the new state, and what
/// it finds is never overwritten: a teardown fence (`Aborting`) answers `NoSuchUpload`, a reaper
/// rollback (`Open` again) leaves the rejection as the answer, and a rival's `Completing` answers
/// `OperationAborted`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_that_finds_the_session_moved_answers_from_its_new_state() {
    within(async {
        type MovedTo = fn(&Bytes) -> Bytes;
        let cases: [(&str, MovedTo, Refusal); 3] = [
            (
                "Aborting",
                |open| session_bytes(open, 2, 1, ABORTING),
                Refusal::NoSuchUpload,
            ),
            (
                "Open",
                |open| session_bytes(open, 2, 1, OPEN),
                part_2_replaced(),
            ),
            (
                "a rival's Completing",
                completing,
                Refusal::OperationAborted,
            ),
        ];
        for (label, moved_to, expected) in cases {
            let up = upload(&PARTS).await;
            let moved = moved_to(&up.open);
            up.store.script(
                vec![
                    fence_with_part_2_replaced(&up.id),
                    OnCommit::write_first(
                        WriteBatch::new().put(mpu_key(&up.id), moved.clone()),
                        OnCommit::Pass,
                    ),
                ],
                None,
            );
            assert_eq!(
                refused(up.complete(&named(&PARTS)).await),
                expected,
                "{label}"
            );
            assert_eq!(
                up.store.take_commits().len(),
                2,
                "{label}: the fence, one release"
            );
            assert_eq!(up.session().await.0, moved, "{label}: left as it moved");
        }
    })
    .await;
}

/// A release that finds a `Completed` tombstone answers by the request's fingerprint, one that
/// finds no record answers `NoSuchUpload`, and one that finds a torn record stops with the fence
/// gone: this Complete did not take it down, and the read says it is no longer there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_that_finds_a_tombstone_no_record_or_a_torn_record() {
    within(async {
        let list = named(&PARTS);
        let moved_and_replaced = |up: &Upload, write: WriteBatch| {
            up.store.script(
                vec![
                    fence_with_part_2_replaced(&up.id),
                    OnCommit::write_first(write, OnCommit::Pass),
                ],
                None,
            );
        };

        // Its own list: the recorded publication.
        let up = upload(&PARTS).await;
        let tombstone = completed(&up.open, &list);
        moved_and_replaced(&up, WriteBatch::new().put(mpu_key(&up.id), tombstone));
        let outcome = up.complete(&list).await;
        let Ok(FenceOutcome::Answered(CompleteOutcome::AlreadyCompleted(publication))) = outcome
        else {
            panic!("expected AlreadyCompleted, got {outcome:?}");
        };
        assert_eq!(publication, recorded(&list));

        // Another list: the upload id is consumed.
        let up = upload(&PARTS).await;
        let tombstone = completed(&up.open, &named(&PARTS[..1]));
        moved_and_replaced(
            &up,
            WriteBatch::new().put(mpu_key(&up.id), tombstone.clone()),
        );
        assert_eq!(refused(up.complete(&list).await), Refusal::NoSuchUpload);
        assert_eq!(up.session().await.0, tombstone);

        // No record.
        let up = upload(&PARTS).await;
        moved_and_replaced(&up, WriteBatch::new().delete(mpu_key(&up.id)));
        assert_eq!(refused(up.complete(&list).await), Refusal::NoSuchUpload);

        // A torn record: an error, with the torn record as its cause.
        let up = upload(&PARTS).await;
        let torn = Bytes::from_static(br#"{"parent":0}"#);
        moved_and_replaced(&up, WriteBatch::new().put(mpu_key(&up.id), torn.clone()));
        let (_, stopped) = stopped_error(up.complete(&list).await, &up.id);
        assert_eq!(stopped.fence, FenceState::Gone { epoch: 1 });
        assert!(
            matches!(
                stopped.cause,
                Stop::Record(RecordError::MalformedRecordValue { .. })
            ),
            "{:?}",
            stopped.cause
        );
        assert_eq!(
            up.store.inner.get(&mpu_key(&up.id)).await.unwrap(),
            Some(torn)
        );
    })
    .await;
}

/// A release that answers `Conflict` and whose re-read then fails cannot say whether the fence
/// stands, and does not claim it: a `Conflict` on an exact-bytes check usually means another writer
/// moved the session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_release_conflict_whose_re_read_fails_does_not_claim_the_fence_stands() {
    within(async {
        let up = upload(&PARTS).await;
        up.store.script(
            vec![
                fence_with_part_2_replaced(&up.id),
                OnCommit::arm(
                    Arm::FailGets(MPU_PREFIX.to_vec()),
                    OnCommit::ConflictUnmoved,
                ),
            ],
            None,
        );
        let (text, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::Unknown { epoch: 1 });
        assert!(!text.contains("still stands"), "{text}");
        assert_read_fault(&stopped.cause);
        assert_eq!(up.store.take_commits().len(), 2, "the fence, one release");
    })
    .await;
}

// ──────────────────────── the budget, away from zero ────────────────────────

/// **(D2, away from zero)** A session with attempts already spent: the fence charges one more,
/// and the release restores exactly the count it read, not zero and not a constant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_request_restores_the_attempts_it_read() {
    within(async {
        for spent in SPENT {
            let up = upload_at(&PARTS, LATE_EPOCH, spent).await;
            up.store
                .script(vec![fence_with_part_2_replaced(&up.id)], None);
            let (before_bytes, before) = up.session().await;
            assert_eq!(before.attempts(), spent);
            assert_eq!(
                refused(up.complete(&named(&PARTS)).await),
                part_2_replaced(),
                "spent {spent}"
            );
            assert_fenced_and_released(&up, (&before_bytes, &before)).await;
            assert_eq!(up.session().await.1.attempts(), spent, "spent {spent}");
        }
    })
    .await;
}

/// **(H, away from zero)** The bucket removed just before the fence commits: `NoSuchBucket`, and
/// the session is `Open` again with exactly the attempts it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bucket_deleted_behind_the_fence_restores_the_attempts_it_read() {
    within(async {
        for spent in SPENT {
            let up = upload_at(&PARTS, LATE_EPOCH, spent).await;
            let remove = WriteBatch::new().delete(metadata::bucket_key(BUCKET));
            up.store
                .script(vec![OnCommit::write_first(remove, OnCommit::Pass)], None);
            let (before_bytes, before) = up.session().await;
            assert_eq!(
                refused(up.complete(&named(&PARTS)).await),
                Refusal::NoSuchBucket,
                "spent {spent}"
            );
            assert_fenced_and_released(&up, (&before_bytes, &before)).await;
            assert_eq!(up.session().await.1.attempts(), spent, "spent {spent}");
        }
    })
    .await;
}

/// **(A held fence, away from zero)** It charges one more than the session had, and it reports the
/// count it read: the value the next slice's release puts back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_fence_charges_one_more_and_reports_the_attempts_it_read() {
    within(async {
        for spent in SPENT {
            let up = upload_at(&PARTS, LATE_EPOCH, spent).await;
            let (_, before) = up.session().await;
            let fence = held(up.complete(&named(&PARTS)).await);
            assert_fenced(fence.session(), &before);
            assert_eq!(fence.epoch(), LATE_EPOCH + 1);
            assert_eq!(fence.session().attempts(), spent + 1, "spent {spent}");
            assert_eq!(fence.attempts_before(), spent, "spent {spent}");
            assert_eq!(up.session().await.1.attempts(), spent + 1, "spent {spent}");
        }
    })
    .await;
}

// ──────────────────────────── a fence that lost to a later Open ────────────────────────────

/// **A fence that lost to a later `Open`.** The first fence commit loses because the session moved
/// to a later `Open` with **different** attempts, in either direction. The retry fences that
/// session, and the request is then rejected. The retry charges one more than **that** session
/// had, and the release restores that count, not the one first read: restoring the first read's
/// would hand back, or take away, an attempt a rival settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_fence_that_is_rejected_restores_the_attempts_of_the_session_it_fenced() {
    within(async {
        let first_read = 1;
        for later_attempts in [first_read + 1, first_read - 1] {
            let up = upload_at(&PARTS, LATE_EPOCH, first_read).await;
            let (first_bytes, _) = up.session().await;
            let later = session_bytes(&up.open, LATE_EPOCH + 2, later_attempts, OPEN);
            let later_record = decode_session_record(&later).unwrap();
            let moved = WriteBatch::new().put(mpu_key(&up.id), later.clone());
            up.store.script(
                vec![
                    OnCommit::write_first(moved, OnCommit::Pass),
                    fence_with_part_2_replaced(&up.id),
                ],
                None,
            );

            assert_eq!(
                refused(up.complete(&named(&PARTS)).await),
                part_2_replaced(),
                "later attempts {later_attempts}"
            );

            let commits = up.store.take_commits();
            assert_eq!(commits.len(), 3, "the lost fence, the fence, its release");
            assert_eq!(session_cas(&commits[0], &up.id).0, first_bytes);
            assert_batches_fenced_and_released(&up, &commits[1..], (&later, &later_record)).await;
            let (_, after) = up.session().await;
            assert_eq!(after.attempts(), later_attempts, "that session's count");
            assert_eq!(
                after.epoch(),
                LATE_EPOCH + 4,
                "Open@E'+2, from the later Open"
            );
        }
    })
    .await;
}

/// **A fence that lost to a later `Open`, then held.** The held fence charges one more than the
/// session it fenced, and reports that session's count as the one a release puts back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_fence_that_is_held_reports_the_attempts_of_the_session_it_fenced() {
    within(async {
        let first_read = 1;
        for later_attempts in [first_read + 1, first_read - 1] {
            let up = upload_at(&PARTS, LATE_EPOCH, first_read).await;
            let later = session_bytes(&up.open, LATE_EPOCH + 2, later_attempts, OPEN);
            let moved = WriteBatch::new().put(mpu_key(&up.id), later.clone());
            up.store
                .script(vec![OnCommit::write_first(moved, OnCommit::Pass)], None);

            let fence = held(up.complete(&named(&PARTS)).await);

            assert_fenced(fence.session(), &decode_session_record(&later).unwrap());
            assert_eq!(fence.attempts_before(), later_attempts);
            assert_eq!(fence.session().attempts(), later_attempts + 1);
            assert_eq!(fence.epoch(), LATE_EPOCH + 3);
            let commits = up.store.take_commits();
            assert_eq!(commits.len(), 2, "the lost fence, the fence");
            assert_eq!(session_cas(&commits[1], &up.id).0, later);
        }
    })
    .await;
}

// ──────────────────────────── the order of the checks ────────────────────────────

/// **The first bad part is the one reported**, walking the list in the client's order, whatever is
/// wrong with the ones after it. Before the fence nothing is written; behind it, the request is
/// released as any rejection is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_bad_part_of_a_list_is_the_one_reported() {
    within(async {
        let up = upload(&PARTS).await;
        let lists = [
            (
                "two wrong digests",
                named(&[(1, 0x11), (2, 0x99), (3, 0x98)]),
                part_2_replaced(),
            ),
            (
                "a wrong digest, then an absent part",
                named(&[(1, 0x11), (2, 0x99), (4, 0x44)]),
                part_2_replaced(),
            ),
            (
                "an absent part, then another",
                named(&[(1, 0x11), (4, 0x44), (5, 0x55)]),
                Refusal::InvalidPart {
                    part_number: part(4),
                    reason: InvalidPart::Absent,
                },
            ),
        ];
        for (label, list, expected) in lists {
            assert_eq!(refused(up.complete(&list).await), expected, "{label}");
            assert!(up.store.take_commits().is_empty(), "{label}: zero commits");
        }
        assert_eq!(up.session().await.0, up.open);

        // Behind the fence: both of the last two parts replaced as it commits.
        let up = upload(&PARTS).await;
        let replaced = OnCommit::write_first(
            plant_part(&up.id, 2, digest(0x23)),
            OnCommit::write_first(plant_part(&up.id, 3, digest(0x34)), OnCommit::Pass),
        );
        up.store.script(vec![replaced], None);
        let (before_bytes, before) = up.session().await;
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            part_2_replaced()
        );
        assert_fenced_and_released(&up, (&before_bytes, &before)).await;
    })
    .await;
}

/// **The bucket is checked first.** A deleted bucket and a bad part together answer `NoSuchBucket`,
/// and the part set is not read for it: that check is one read, and the parts of a bucket that is
/// gone have nothing to say. Before the fence nothing is written; behind it, the fence is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_bucket_answers_before_any_bad_part() {
    within(async {
        let remove = || WriteBatch::new().delete(metadata::bucket_key(BUCKET));

        let up = upload(&PARTS).await;
        assert_eq!(
            up.store.inner.commit(remove()).await.unwrap(),
            CommitOutcome::Committed
        );
        let bad = named(&[(1, 0x11), (2, 0x99)]);
        assert_eq!(refused(up.complete(&bad).await), Refusal::NoSuchBucket);
        assert!(up.store.take_commits().is_empty(), "nothing is fenced");
        assert!(up.store.take_scans().is_empty(), "the parts are not read");
        assert_eq!(up.session().await.0, up.open);

        let up = upload(&PARTS).await;
        up.store.script(
            vec![OnCommit::write_first(
                plant_part(&up.id, 2, digest(0x23)).delete(metadata::bucket_key(BUCKET)),
                OnCommit::Pass,
            )],
            None,
        );
        let (before_bytes, before) = up.session().await;
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::NoSuchBucket
        );
        assert_fenced_and_released(&up, (&before_bytes, &before)).await;
        assert_eq!(
            up.store.take_scans().len(),
            1,
            "only the check before the fence read the parts"
        );
    })
    .await;
}

/// **An `Open` session's checks run in the order `fence_complete` documents**: the order of the
/// list, then the clock, then the budget, then the epoch ceiling, then the bucket and the parts.
/// Each leg fails two adjacent checks at once and expects the earlier one, with nothing written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_sessions_checks_run_in_the_documented_order() {
    within(async {
        let out_of_order = named(&[(2, 0x22), (1, 0x11)]);
        let bad_digest = named(&[(1, 0x99)]);

        // The order of the list, then the clock.
        let up = upload(&PARTS).await;
        let request = CompleteRequest {
            clock_source: "logical",
            ..up.request(&out_of_order)
        };
        assert_eq!(
            refused(fence_complete(&up.store, &up.id, &request).await),
            Refusal::InvalidPart {
                part_number: part(1),
                reason: InvalidPart::OutOfOrder,
            },
            "the order, before the clock"
        );

        // The clock, then the budget.
        let up = upload_at(&PARTS, LATE_EPOCH, MAX_COMPLETE_ATTEMPTS).await;
        let list = named(&PARTS);
        let request = CompleteRequest {
            clock_source: "logical",
            ..up.request(&list)
        };
        let outcome = fence_complete(&up.store, &up.id, &request).await;
        let (_, stopped) = stopped_error(outcome, &up.id);
        assert!(
            matches!(stopped.cause, Stop::ForeignClock { .. }),
            "the clock, before the budget: {:?}",
            stopped.cause
        );
        assert!(up.store.take_commits().is_empty());

        // The budget, then the epoch ceiling.
        let up = upload_at(&PARTS, u64::MAX, MAX_COMPLETE_ATTEMPTS).await;
        assert_eq!(
            refused(up.complete(&list).await),
            Refusal::CompleteAttemptsExhausted,
            "the budget, before the epoch"
        );
        assert!(up.store.take_commits().is_empty());

        // The budget, then the parts: a session out of budget reads no part.
        let up = upload_at(&PARTS, LATE_EPOCH, MAX_COMPLETE_ATTEMPTS).await;
        assert_eq!(
            refused(up.complete(&bad_digest).await),
            Refusal::CompleteAttemptsExhausted,
            "the budget, before the parts"
        );
        assert!(up.store.take_scans().is_empty(), "no part read");
        assert!(up.store.take_commits().is_empty());

        // The epoch ceiling, then the parts.
        let up = upload_at(&PARTS, u64::MAX, 0).await;
        let (_, stopped) = stopped_error(up.complete(&bad_digest).await, &up.id);
        assert!(
            matches!(stopped.cause, Stop::EpochExhausted),
            "the epoch, before the parts: {:?}",
            stopped.cause
        );
        assert!(up.store.take_scans().is_empty(), "no part read");
        assert!(up.store.take_commits().is_empty());
    })
    .await;
}

/// **What is frozen is what the fence froze.** A part the list does not name, planted just before
/// the fence commits, is in the held fence's set: it is read behind the fence, not carried over from
/// the check before it, and the next slice assembles from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_held_fence_carries_the_set_read_behind_the_fence() {
    within(async {
        let up = upload(&PARTS).await;
        up.store.script(
            vec![OnCommit::write_first(
                plant_part(&up.id, 4, digest(0x44)),
                OnCommit::Pass,
            )],
            None,
        );
        let fence = held(up.complete(&named(&PARTS)).await);
        assert_eq!(fence.frozen_parts().len(), 4, "named or not");
        assert_eq!(
            fence.frozen_parts().get(&part(4)),
            Some(&summary(4, digest(0x44)))
        );
    })
    .await;
}

// ──────────────────────────── a frozen set past one page ────────────────────────────

/// Two more parts than a page holds, so the second page is short and the walk ends on it.
const PAGED_PARTS: u32 = metadata::SEGMENT_PAGE_LIMIT as u32 + 2;

fn paged_parts() -> Vec<(u32, u8)> {
    (1..=PAGED_PARTS)
        .map(|n| (n, u8::try_from(n % 251).unwrap()))
        .collect()
}

/// The frozen set is read whole across pages: a part on the second page validates, and the held
/// fence carries every part. Each of the two validations walks two pages, the second continuing
/// from the last key of the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_on_the_second_page_is_found_and_the_whole_set_is_frozen() {
    within(async {
        let parts = paged_parts();
        let up = upload(&parts).await;
        let tail = parts[parts.len() - 1];
        let list = named(&[(1, parts[0].1), tail]);

        let fence = held(up.complete(&list).await);

        let frozen = fence.frozen_parts();
        assert_eq!(frozen.len(), parts.len(), "named or not");
        for &(n, d) in &parts {
            assert_eq!(
                frozen.get(&part(n)),
                Some(&summary(n, digest(d))),
                "part {n}"
            );
        }
        let limit = metadata::SEGMENT_PAGE_LIMIT;
        let cursor = psum_key(&up.id, part(PAGED_PARTS - 2));
        let walk = [(None, limit), (Some(cursor), limit)];
        let expected: Vec<_> = walk.iter().chain(walk.iter()).cloned().collect();
        assert_eq!(up.store.take_scans(), expected, "two walks of two pages");
    })
    .await;
}

/// A part on the second page, changed before the fence commits: the check behind the fence reads
/// that page too, so the request is rejected and the fence released. A wrong digest for a part
/// on that page is refused before any fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_on_the_second_page_is_seen_behind_the_fence() {
    within(async {
        let parts = paged_parts();
        let tail = parts[parts.len() - 1];
        let list = named(&[tail]);

        let up = upload(&parts).await;
        up.store.script(
            vec![OnCommit::write_first(
                plant_part(&up.id, tail.0, digest(tail.1.wrapping_add(1))),
                OnCommit::Pass,
            )],
            None,
        );
        let (before_bytes, before) = up.session().await;
        assert_eq!(
            refused(up.complete(&list).await),
            Refusal::InvalidPart {
                part_number: part(tail.0),
                reason: InvalidPart::DigestMismatch,
            }
        );
        assert_fenced_and_released(&up, (&before_bytes, &before)).await;

        let up = upload(&parts).await;
        let wrong = named(&[(tail.0, tail.1.wrapping_add(1))]);
        assert_eq!(
            refused(up.complete(&wrong).await),
            Refusal::InvalidPart {
                part_number: part(tail.0),
                reason: InvalidPart::DigestMismatch,
            }
        );
        assert!(up.store.take_commits().is_empty(), "refused before a fence");
    })
    .await;
}

/// The `psum:` page of part `n` under `id`, as a backend would answer it.
fn psum_entry(id: &UploadId, n: u32) -> (Vec<u8>, Bytes) {
    (
        psum_key(id, part(n)),
        metadata::encode(&summary(n, digest(0x11))),
    )
}

/// A page that breaks the paging contract is a store fault, never a part set: a key repeated in a
/// page or across pages, a key below the last one read, or a key of another upload. Before the
/// fence, that stops with no fence placed; behind it, the fence is taken down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_that_breaks_the_paging_contract_is_a_store_fault() {
    within(async {
        let (other_upload, _) = identity(2);
        let up = upload(&PARTS).await;
        let id = up.id.clone();
        let cases: Vec<(&str, Vec<ScanPage>)> = vec![
            (
                "a key repeated in a page",
                vec![(vec![psum_entry(&id, 1), psum_entry(&id, 1)], None)],
            ),
            (
                "a key repeated across pages",
                vec![
                    (vec![psum_entry(&id, 1)], Some(psum_key(&id, part(1)))),
                    (vec![psum_entry(&id, 1)], None),
                ],
            ),
            (
                "a key below the last one read",
                vec![(vec![psum_entry(&id, 2), psum_entry(&id, 1)], None)],
            ),
            (
                "a key of another upload, ascending",
                vec![(vec![psum_entry(&id, 1), psum_entry(&other_upload, 2)], None)],
            ),
        ];
        let list = named(&[(1, 0x11)]);
        let says_broken = |stopped: &Stopped, label: &str| {
            assert!(
                matches!(&stopped.cause, Stop::Store(fault)
                    if fault.to_string().contains("out of order or outside the range")),
                "{label}: {:?}",
                stopped.cause
            );
        };
        for (label, pages) in cases {
            // Before the fence.
            up.store.arm(Arm::NextPages(pages.clone()));
            let (_, stopped) = stopped_error(up.complete(&list).await, &up.id);
            assert_eq!(stopped.fence, FenceState::NotPlaced, "{label}");
            says_broken(&stopped, label);
            assert!(up.store.take_commits().is_empty(), "{label}: zero commits");
            assert_eq!(up.session().await.0, up.open, "{label}: no fence");

            // Behind it, on a session of its own (every upload here is identity 1, so the
            // same page answers there too).
            let behind = upload(&PARTS).await;
            behind
                .store
                .script(vec![fence_then(Arm::NextPages(pages))], None);
            let (_, stopped) = stopped_error(behind.complete(&list).await, &behind.id);
            assert_eq!(stopped.fence, FenceState::Released { epoch: 1 }, "{label}");
            says_broken(&stopped, label);
        }
    })
    .await;
}

/// A key under the session's `psum:` range that does not parse, and a summary that does not decode,
/// are typed errors, never parts (ADR-0045).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torn_part_set_is_a_typed_error() {
    within(async {
        let list = named(&PARTS);
        let mut bad_key = psum_range(&identity(1).0);
        bad_key.extend_from_slice(b"not-a-number");
        let torn: [(&str, Vec<u8>, Bytes); 2] = [
            (
                "a key that does not parse",
                bad_key,
                metadata::encode(&1u32),
            ),
            (
                "a summary that does not decode",
                psum_key(&identity(1).0, part(2)),
                Bytes::from_static(b"{}"),
            ),
        ];
        for (label, key, value) in torn {
            // Already torn before the fence: no fence is placed.
            let up = upload(&PARTS).await;
            let plant = WriteBatch::new().put(key.clone(), value.clone());
            up.store.inner.commit(plant.clone()).await.unwrap();
            let (_, stopped) = stopped_error(up.complete(&list).await, &up.id);
            assert_eq!(stopped.fence, FenceState::NotPlaced, "{label}");
            assert!(
                matches!(stopped.cause, Stop::Record(_)),
                "{label}: {:?}",
                stopped.cause
            );
            assert!(up.store.take_commits().is_empty(), "{label}: zero commits");

            // Torn behind the fence: the fence is taken down, and the cause is the torn record.
            let up = upload(&PARTS).await;
            up.store
                .script(vec![OnCommit::write_first(plant, OnCommit::Pass)], None);
            let (before_bytes, before) = up.session().await;
            let (_, stopped) = stopped_error(up.complete(&list).await, &up.id);
            assert_eq!(stopped.fence, FenceState::Released { epoch: 1 }, "{label}");
            assert!(
                matches!(stopped.cause, Stop::Record(_)),
                "{label}: {:?}",
                stopped.cause
            );
            assert_fenced_and_released(&up, (&before_bytes, &before)).await;
        }
    })
    .await;
}

// ──────────────────────── a store fault behind the fence ────────────────────────

/// The reads a validation behind the fence can fail on, as an arm for the fence commit: the
/// session's `psum:` range, and the bucket marker.
fn faults_behind_the_fence(up: &Upload) -> [(&'static str, Arm); 2] {
    [
        ("a psum: page", Arm::FailScans(psum_range(&up.id))),
        (
            "the bucket marker",
            Arm::FailGets(metadata::bucket_key(BUCKET)),
        ),
    ]
}

/// **A store fault behind the fence, the release lands.** One best-effort release takes the fence
/// down: the error says so, its cause is the fault, and the session is `Open@E+2` with `attempts`
/// exactly as read — at zero and away from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_behind_the_fence_takes_the_fence_down_when_the_release_lands() {
    within(async {
        for spent in [0, SPENT[0], SPENT[1]] {
            for which in 0..2 {
                let up = upload_at(&PARTS, LATE_EPOCH, spent).await;
                let [first, second] = faults_behind_the_fence(&up);
                let (label, arm) = if which == 0 { first } else { second };
                up.store.script(vec![fence_then(arm)], None);
                let (before_bytes, before) = up.session().await;

                let (text, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);

                let epoch = LATE_EPOCH + 1;
                assert_eq!(stopped.fence, FenceState::Released { epoch }, "{label}");
                assert_read_fault(&stopped.cause);
                assert!(text.contains("was taken down"), "{text}");
                assert_fenced_and_released(&up, (&before_bytes, &before)).await;
                assert_eq!(up.session().await.1.attempts(), spent, "{label}");
            }
        }
    })
    .await;
}

/// **Store faults never wedge a session.** More faults in a row than the budget allows fences,
/// and then a request that meets no fault still fences: the #636 wedge, reached through the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_faults_behind_the_fence_never_spend_the_budget() {
    within(async {
        let up = upload(&PARTS).await;
        for _ in 0..=MAX_COMPLETE_ATTEMPTS {
            up.store
                .script(vec![fence_then(Arm::FailScans(psum_range(&up.id)))], None);
            let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
            assert!(matches!(stopped.fence, FenceState::Released { .. }));
            up.store.clear_read_faults();
        }
        assert_eq!(up.session().await.1.attempts(), 0);
        let fence = held(up.complete(&named(&PARTS)).await);
        assert_eq!(fence.attempts_before(), 0);
    })
    .await;
}

/// **A store fault behind the fence, the release fails for good.** The fence stands and the error
/// says so — never `NotPlaced`, which would tell the caller no fence landed. The cause is the fault
/// that stopped validation, and only one release was tried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_behind_the_fence_says_the_fence_stands_when_the_release_fails() {
    within(async {
        for which in 0..2 {
            let up = upload(&PARTS).await;
            let [first, second] = faults_behind_the_fence(&up);
            let (label, arm) = if which == 0 { first } else { second };
            up.store.script(
                vec![fence_then(arm), OnCommit::Fail { transient: true }],
                None,
            );

            let (text, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);

            assert_eq!(stopped.fence, FenceState::Stands { epoch: 1 }, "{label}");
            assert_read_fault(&stopped.cause);
            assert!(
                text.contains("Completing@1") && text.contains("still stands"),
                "{text}"
            );
            let commits = up.store.take_commits();
            assert_eq!(commits.len(), 2, "{label}: the fence, one release");
            let (stored, record) = up.session().await;
            assert_eq!(
                stored,
                session_cas(&commits[0], &up.id).1,
                "{label}: the fence's bytes"
            );
            assert!(matches!(record.state(), SessionState::Completing { .. }));
        }
    })
    .await;
}

/// **A store fault behind the fence, the release answers `Conflict` with the session unchanged**
/// (the TiKV lock). One release is all a best-effort release gets: the fence stands, and the cause
/// is still the fault, not spent rounds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_behind_the_fence_tries_one_release_only() {
    within(async {
        let up = upload(&PARTS).await;
        up.store.script(
            vec![fence_then(Arm::FailScans(psum_range(&up.id)))],
            Some(OnCommit::ConflictUnmoved),
        );
        let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::Stands { epoch: 1 });
        assert_read_fault(&stopped.cause);
        let commits = up.store.take_commits();
        assert_eq!(commits.len(), 2, "the fence, one release");
        assert_eq!(up.session().await.0, session_cas(&commits[0], &up.id).1);
    })
    .await;
}

/// **A store fault behind the fence, the release's outcome is unknown.** The in-doubt error about
/// the release, carrying that outcome and the fault that made it try, and nothing sent after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_behind_the_fence_with_an_unknown_release_is_in_doubt() {
    within(async {
        for (applied, may_still_commit) in [(false, true), (true, false)] {
            let up = upload(&PARTS).await;
            up.store.script(
                vec![
                    fence_then(Arm::FailScans(psum_range(&up.id))),
                    OnCommit::Unknown {
                        applied,
                        may_still_commit,
                    },
                ],
                None,
            );
            let doubt = in_doubt_error(
                up.complete(&named(&PARTS)).await,
                &up.id,
                DoubtfulCommit::Release { epoch: 1 },
                may_still_commit,
            );
            assert_read_fault(
                doubt
                    .fault_behind_fence
                    .as_ref()
                    .expect("an operator still sees the fault"),
            );
            assert_eq!(up.store.take_commits().len(), 2, "the fence, the release");
        }
    })
    .await;
}

/// **A store fault behind the fence, the release finds the session moved.** Answered from the new
/// state, as a rejection's release is. An `Open` session has no rejection to answer, so that is the
/// error, saying the fence is gone and it was not this Complete that took it down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_behind_the_fence_whose_release_finds_the_session_moved() {
    within(async {
        let moved_by = |up: &Upload, write: WriteBatch| {
            up.store.script(
                vec![
                    fence_then(Arm::FailScans(psum_range(&up.id))),
                    OnCommit::write_first(write, OnCommit::Pass),
                ],
                None,
            );
        };

        let up = upload(&PARTS).await;
        let aborting = session_bytes(&up.open, 2, 1, ABORTING);
        moved_by(
            &up,
            WriteBatch::new().put(mpu_key(&up.id), aborting.clone()),
        );
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::NoSuchUpload
        );
        assert_eq!(up.session().await.0, aborting, "left as it moved");

        let up = upload(&PARTS).await;
        moved_by(&up, WriteBatch::new().delete(mpu_key(&up.id)));
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::NoSuchUpload
        );

        // A rival's fence: refused, and left as it is.
        let up = upload(&PARTS).await;
        let theirs = completing(&up.open);
        moved_by(&up, WriteBatch::new().put(mpu_key(&up.id), theirs.clone()));
        assert_eq!(
            refused(up.complete(&named(&PARTS)).await),
            Refusal::OperationAborted
        );
        assert_eq!(up.session().await.0, theirs, "left as it moved");

        // A tombstone answers by the request's own list, as a rejection's release does.
        let list = named(&PARTS);
        let up = upload(&PARTS).await;
        let tombstone = completed(&up.open, &list);
        moved_by(&up, WriteBatch::new().put(mpu_key(&up.id), tombstone));
        let outcome = up.complete(&list).await;
        let Ok(FenceOutcome::Answered(CompleteOutcome::AlreadyCompleted(publication))) = outcome
        else {
            panic!("expected AlreadyCompleted, got {outcome:?}");
        };
        assert_eq!(publication, recorded(&list));

        let up = upload(&PARTS).await;
        let tombstone = completed(&up.open, &named(&PARTS[..1]));
        moved_by(&up, WriteBatch::new().put(mpu_key(&up.id), tombstone));
        assert_eq!(refused(up.complete(&list).await), Refusal::NoSuchUpload);

        let up = upload(&PARTS).await;
        let rolled_back = session_bytes(&up.open, 2, 1, OPEN);
        moved_by(
            &up,
            WriteBatch::new().put(mpu_key(&up.id), rolled_back.clone()),
        );
        let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::Gone { epoch: 1 });
        assert_read_fault(&stopped.cause);
        assert_eq!(up.session().await.0, rolled_back, "left as it moved");

        let up = upload(&PARTS).await;
        let torn = Bytes::from_static(br#"{"parent":0}"#);
        moved_by(&up, WriteBatch::new().put(mpu_key(&up.id), torn));
        let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::Gone { epoch: 1 });
        assert_read_fault(&stopped.cause);
    })
    .await;
}

/// **A store fault behind the fence, the release answers `Conflict` and the re-read fails.** It
/// does not claim the fence stands, and the cause stays the fault that stopped validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_behind_the_fence_whose_re_read_fails_does_not_claim_the_fence_stands() {
    within(async {
        let up = upload(&PARTS).await;
        up.store.script(
            vec![
                fence_then(Arm::FailScans(psum_range(&up.id))),
                OnCommit::arm(
                    Arm::FailGets(MPU_PREFIX.to_vec()),
                    OnCommit::ConflictUnmoved,
                ),
            ],
            None,
        );
        let (text, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::Unknown { epoch: 1 });
        assert!(!text.contains("still stands"), "{text}");
        assert_read_fault(&stopped.cause);
        assert_eq!(up.store.take_commits().len(), 2, "the fence, one release");
    })
    .await;
}

/// A read that fails **before** the fence, on the part set or the bucket, or after a fence commit
/// that answered `Conflict`, stops with no fence placed: that is the one error that may say so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_before_the_fence_lands_is_stopped_with_no_fence_placed() {
    within(async {
        let up = upload(&PARTS).await;
        let [(_, psum), (_, bucket)] = faults_behind_the_fence(&up);
        for arm in [psum, bucket] {
            up.store.arm(arm);
            let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
            assert_eq!(stopped.fence, FenceState::NotPlaced);
            assert_read_fault(&stopped.cause);
            assert!(up.store.take_commits().is_empty(), "zero commits");
            up.store.clear_read_faults();
        }

        up.store.script(
            vec![OnCommit::arm(
                Arm::FailGets(MPU_PREFIX.to_vec()),
                OnCommit::ConflictUnmoved,
            )],
            None,
        );
        let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::NotPlaced);
        assert_read_fault(&stopped.cause);
        assert_eq!(up.store.take_commits().len(), 1, "one fence, not applied");
        assert_eq!(up.session().await.0, up.open);
    })
    .await;
}

/// The state answers before the list: a list out of order or naming a part twice is refused for its
/// order only by an `Open` session (`0016:976`). Against the other states it gets what any list
/// not matching the tombstone gets, with no commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_state_answers_before_the_order_of_the_list() {
    within(async {
        let lists = [
            named(&[(2, 0x22), (1, 0x11)]),
            named(&[(1, 0x11), (1, 0x11)]),
        ];
        let up = upload(&PARTS).await;
        let tombstone = named(&PARTS);
        let states = [
            (completing(&up.open), Refusal::OperationAborted),
            (completed(&up.open, &tombstone), Refusal::NoSuchUpload),
            (
                session_bytes(&up.open, 1, 0, ABORTING),
                Refusal::NoSuchUpload,
            ),
        ];
        for (state, expected) in states {
            up.plant(state.clone()).await;
            for list in &lists {
                assert_eq!(refused(up.complete(list).await), expected);
            }
            assert!(up.store.take_commits().is_empty(), "zero commits");
            assert_eq!(up.session().await.0, state, "left as it was");
        }
    })
    .await;
}

/// A session too near the epoch ceiling for a fence **and** its release is stopped before anything
/// is written: a fence that could not be taken down again would wedge the session on a client's
/// mistake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_at_the_epoch_ceiling_is_stopped_before_anything_is_written() {
    within(async {
        for epoch in [u64::MAX - 1, u64::MAX] {
            let up = upload_at(&PARTS, epoch, 0).await;
            let (before, _) = up.session().await;
            let (text, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
            assert_eq!(stopped.fence, FenceState::NotPlaced);
            assert!(
                matches!(stopped.cause, Stop::EpochExhausted),
                "{:?}",
                stopped.cause
            );
            assert!(text.contains("epoch too high"), "{text}");
            assert!(up.store.take_commits().is_empty(), "zero commits");
            assert_eq!(up.session().await.0, before);
        }
    })
    .await;
}

/// A fence commit the store refuses for good stops the request with no fence placed, and a
/// stored session record that does not decode is a typed error writing nothing (ADR-0045).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_definite_fence_failure_or_a_torn_session_is_stopped_with_no_fence_placed() {
    within(async {
        let up = upload(&PARTS).await;
        up.store
            .script(vec![OnCommit::Fail { transient: true }], None);
        let outcome = up.complete(&named(&PARTS)).await;
        assert_eq!(
            outcome.as_ref().err().map(|err| classify(err)),
            Some(ErrorClass::Transient)
        );
        let (_, stopped) = stopped_error(outcome, &up.id);
        assert_eq!(stopped.fence, FenceState::NotPlaced);
        assert!(
            matches!(stopped.cause, Stop::Store(_)),
            "{:?}",
            stopped.cause
        );
        assert_eq!(up.store.take_commits().len(), 1);
        assert_eq!(up.session().await.0, up.open);

        let up = upload(&PARTS).await;
        let torn = Bytes::from_static(br#"{"parent":0}"#);
        let put = WriteBatch::new().put(mpu_key(&up.id), torn.clone());
        assert_eq!(
            up.store.inner.commit(put).await.unwrap(),
            CommitOutcome::Committed
        );
        let (_, stopped) = stopped_error(up.complete(&named(&PARTS)).await, &up.id);
        assert_eq!(stopped.fence, FenceState::NotPlaced);
        assert!(
            matches!(
                stopped.cause,
                Stop::Record(RecordError::MalformedRecordValue { .. })
            ),
            "{:?}",
            stopped.cause
        );
        assert!(up.store.take_commits().is_empty(), "zero commits");
    })
    .await;
}

/// An empty list is not a request: `NamedParts` refuses it, so no Complete can be asked for one.
#[test]
fn an_empty_list_is_not_a_request() {
    assert_eq!(NamedParts::new(Vec::new()), Err(RecordError::NoPartsNamed));
}
