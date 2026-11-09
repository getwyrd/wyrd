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
//!
//! # Issue #1008: the flat publication
//!
//! The legs from *the flat publication* on drive the verb,
//! `wyrd_core::multipart::complete::complete_upload`: the fence above, then one root flip through
//! #653's committer (proposal 0016's *Root flip* row, `0016:663`; decision 3, `0016:910-941`).
//! Their parts are staged by `UploadPart` (`commit::upload_part`, #1027) onto a placement-aware
//! chunk-store double, so the object they publish is read back through `read::read_object`. An
//! ordinary `PutObject` here goes through the single-PUT write path (`write.rs`). Every
//! interference is forced by the same wrapper, at the flip's commit point:
//!
//! * **(A)** parts staged out of order publish one flat root that reads back byte for byte, with
//!   the multipart ETag on the inode and in the `Completed` session; an identical retry answers
//!   the recorded publication.
//! * **(D2-tail)** after D1's typos and D2's parts replaced before the fence, a valid list
//!   publishes.
//! * **(C)** a subset Complete retires the published parts' records and the unnamed part's bytes,
//!   in two obligations that never name a published chunk.
//! * **(F)** a flip that loses to an ordinary overwrite publishes the version after the one it
//!   lost to, and retires that generation; flips that all lose release the fence.
//! * **(F2)** a zero-byte prior is superseded with no obligation for it.
//! * **(H2)** a bucket marker deleted just before the flip answers `NoSuchBucket`.
//! * **(J)** a flip the store refuses for good releases the fence and stops.
//! * **(G0-flip)** an unknown flip outcome stops in doubt, with nothing sent after it.
//! * **(K)** a list past one flat map stops, with no commit or after one release.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, Stream};
use sha2::{Digest as _, Sha256};
use wyrd_core::metadata::{
    self, ChunkMap, ChunkRef, DirentRecord, EcScheme, InodeId, InodeRecord, ObjectMeta,
    SegmentGroup, SegmentNonce,
};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::commit::upload_part;
use wyrd_core::multipart::complete::{
    complete_upload, fence_complete, CompleteError, CompleteRequest, DoubtfulCommit, FenceOutcome,
    FenceState, HeldFence, InDoubt, NamedParts, PublishRequest, Stop, Stopped,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::stage::{StageClock, StageRequest, StageRuntime};
use wyrd_core::multipart::{
    complete_fingerprint, decode_part_record, decode_part_summary, decode_retire_obligation,
    decode_session_record, mpu_key, multipart_etag, part_key, part_range, psum_key, psum_range,
    AbortOutcome, AttemptId, Budget, CompleteOutcome, CreateOutcome, Digest, InvalidPart,
    PartNumber, PartNumberSet, PartScope, PartSummary, Publication, RecordError, Refusal,
    RetireMode, RetirePayload, RetireToken, RetiredMap, SessionRecord, SessionState, UploadId,
    UploadPartOutcome, MAX_COMPLETE_ATTEMPTS, MAX_INFLIGHT_PARTS, MAX_PART_CHUNKS, MPU_PREFIX,
    RETIRE_BYTES_PREFIX, RETIRE_RECORDS_PREFIX,
};
use wyrd_core::placement::Topology;
use wyrd_core::read::read_object;
use wyrd_core::staged_publication::{self, PublicationError, SegmentBatchInfo};
use wyrd_core::write;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::{Clock, ManualClock};
use wyrd_traits::{
    classify, BoxError, ChunkId, ChunkStore, CommitOutcome, CommitUnknownResult, DServerId,
    ErrorClass, FragmentId, Health, MetadataStore, PlacementChunkStore, Result as StoreResult,
    ScanPage, TransientFault, WriteBatch, WriteDeadlineExpired,
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
    /// Run this real operation on the inner store first — an `UploadPart`, an ordinary
    /// `PutObject` — then do the step it carries (#1008).
    Run(Interfere, Box<OnCommit>),
}

/// A real operation [`Logged`] runs on its inner store just before a commit of the Complete under
/// test: what another client does at that instant, through the production path it would use.
#[async_trait]
trait Interference: Send + Sync {
    async fn run(&self, store: &RedbMetadataStore);
}

/// An [`Interference`] a script step carries.
#[derive(Clone)]
struct Interfere(Arc<dyn Interference>);

impl fmt::Debug for Interfere {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Interfere")
    }
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
            OnCommit::Run(interfere, then) => {
                interfere.0.run(&self.inner).await;
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
    upload_with(parts, None).await
}

/// [`upload`], with the session created declaring `content_type`.
async fn upload_with(parts: &[(u32, u8)], content_type: Option<&str>) -> Upload {
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
        content_type,
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
        matches!(
            stopped.cause,
            Stop::Store(_) | Stop::Record(_) | Stop::Target(_) | Stop::Publication(_)
        ),
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
        DoubtfulCommit::Flip { epoch } => format!("its root flip from Completing@{epoch}"),
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

// ═════════════════════════════ #1008: the flat publication ═════════════════════════════

/// Bytes per staged chunk: small, so a part spans several chunks.
const CHUNK: usize = 64;
/// Six fragments per chunk.
const RS42: EcScheme = EcScheme::ReedSolomon { k: 4, m: 2 };
/// Twelve D servers, each its own failure domain.
const SERVERS: u64 = 12;
/// The lease every part attempt is staged with: above the part commit's five-second dispatch
/// margin, which it must exceed.
const TTL: u64 = 10_000;
/// `W_write` for a staged fragment.
const W: u64 = 3_000;
/// The body-idle bound: never reached, since no leg moves the staging clock.
const IDLE: u64 = 100 * TTL;
/// When the caller read the clock for the flip: the session's clock, after the fence.
const COMPLETED_AT: u64 = 2_500;
/// The inode a flip onto a free name creates.
const FRESH_INODE: InodeId = 500;
/// The content type the (A) session is created with.
const CONTENT_TYPE: &str = "video/mp4";
/// The inode an ordinary `PutObject` creates the target under, when a leg needs a prior.
const PUT_INODE: InodeId = 900;
/// The stated bound on one Complete's flips, written out rather than read from `R_PUBLISH`, so a
/// change to the bound has to change this number too.
const STATED_PUBLISH_ROUNDS: u32 = 3;
/// The stated flat ceiling, written out rather than read from `MAX_MAP_CHUNKS`, for the same
/// reason.
const STATED_FLAT_CHUNKS: u64 = 158;

/// What every leg's Complete publishes with.
const PUBLISH: PublishRequest = PublishRequest {
    fresh_inode: FRESH_INODE,
    completed_at_millis: COMPLETED_AT,
};

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

fn topology() -> Topology {
    let mut topology = Topology::default();
    for id in 0..SERVERS {
        topology.register(id, format!("d{id:02}"));
    }
    topology
}

// ──────────────────────── the placement-aware chunk store ────────────────────────

/// In-process D servers addressed by id: what staging writes each fragment to, at the server its
/// placement names, and what `read_object` reads them back from. A put past its deadline is
/// refused at its publication point, as `PlacementChunkStore::put_fragment_at` requires.
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
        Err("Fleet: a write must address a D server".into())
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

// ─────────────────────────── staging and ordinary writes ───────────────────────────

/// The clock every part is staged and committed on, and a timer that never fires: no leg moves
/// the clock, so no renewal falls due and no staging bound is reached.
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

/// What every `UploadPart` and ordinary `PutObject` of one leg shares: the chunk store, the
/// staging clock, one chunk-id sequence (so no two writes mint the same chunk) and the session.
#[derive(Clone)]
struct Stager {
    fleet: Fleet,
    clock: ManualClock,
    topology: Arc<Topology>,
    upload: UploadId,
    next_chunk: Arc<AtomicU64>,
    next_attempt: Arc<AtomicU64>,
}

impl Stager {
    /// `UploadPart` of `bytes` as part `n` in chunks of `chunk_size`, through #1027's composed
    /// verb (reserve, stage, commit) on `store`. It must commit; returns the part's digest.
    async fn upload(
        &self,
        store: &RedbMetadataStore,
        n: u32,
        bytes: &[u8],
        chunk_size: usize,
    ) -> Digest {
        let attempt = self.next_attempt.fetch_add(1, Ordering::SeqCst) + 1;
        let attempt =
            AttemptId::new(format!("{:032x}", 0xa77e_0000_u128 + u128::from(attempt))).unwrap();
        let request = StageRequest {
            upload_id: &self.upload,
            part_number: part(n),
            attempt_id: &attempt,
            max_inflight_parts: MAX_INFLIGHT_PARTS,
            lease_ttl_millis: TTL,
            chunk_size,
            scheme: RS42,
            topology: &self.topology,
            write_window_millis: W,
            max_part_chunks: MAX_PART_CHUNKS,
            body_idle_millis: IDLE,
        };
        let mut runtime = Runtime {
            clock: self.clock.clone(),
            next_chunk: Arc::clone(&self.next_chunk),
        };
        let outcome =
            upload_part(store, &self.fleet, &mut runtime, &request, stream_of(bytes)).await;
        match outcome {
            Ok(UploadPartOutcome::Committed {
                part_number,
                digest,
            }) => {
                assert_eq!(part_number, part(n));
                assert_eq!(digest, sha256(bytes), "part {n}'s digest is its body's");
                digest
            }
            other => panic!("expected part {n} committed, got {other:?}"),
        }
    }

    /// An ordinary `PutObject` of `bytes` at `name`, through the single-PUT write path
    /// (`write.rs`: plan, place, intent, fragments, commit, release), stamped `at`: a create under
    /// `fresh` when the name is free, else an overwrite of the inode its dirent names. Returns
    /// the inode, the version it committed and that version's chunk list.
    async fn put_object(
        &self,
        store: &RedbMetadataStore,
        name: &str,
        fresh: InodeId,
        bytes: &[u8],
        at: u64,
    ) -> (InodeId, u64, Vec<ChunkRef>) {
        let next = Arc::clone(&self.next_chunk);
        let mut plan = write::plan_write(bytes, CHUNK, RS42, || {
            ChunkId::from(next.fetch_add(1, Ordering::SeqCst) + 1)
        })
        .unwrap();
        plan.place(&self.topology).unwrap();
        plan.object_meta = ObjectMeta {
            etag: Some(sha256(bytes).to_hex()),
            content_type: None,
            modified: Some(at),
        };
        write::intent(store, &plan, at + TTL).await.unwrap();
        write::write_fragments(&self.fleet, &plan).await.unwrap();
        let dirent = store.get(&metadata::dirent_key(ROOT, name)).await.unwrap();
        let (id, outcome) = match dirent {
            None => (
                fresh,
                write::commit_create(store, ROOT, name, fresh, &plan, at)
                    .await
                    .unwrap(),
            ),
            Some(dirent) => {
                let DirentRecord { inode: id } = metadata::decode(&dirent).unwrap();
                let prior = inode_at(store, id).await.expect("the bound inode");
                let outcome = write::commit_overwrite(store, id, &prior, &plan, at)
                    .await
                    .unwrap();
                (id, outcome)
            }
        };
        assert_eq!(outcome, CommitOutcome::Committed, "the PutObject lands");
        write::release(store, &plan).await.unwrap();
        let record = inode_at(store, id).await.expect("the published inode");
        (id, record.version, plan.chunk_refs())
    }
}

/// The inode record `id`, if present.
async fn inode_at(store: &impl MetadataStore, id: InodeId) -> Option<InodeRecord> {
    let bytes = store.get(&metadata::inode_key(id)).await.unwrap();
    bytes.map(|bytes| metadata::decode(&bytes).expect("a valid inode"))
}

/// Another client's `UploadPart` of part `n`, landing just before a commit of the Complete.
struct ReUpload {
    stager: Stager,
    n: u32,
    bytes: Vec<u8>,
    chunk_size: usize,
}

#[async_trait]
impl Interference for ReUpload {
    async fn run(&self, store: &RedbMetadataStore) {
        self.stager
            .upload(store, self.n, &self.bytes, self.chunk_size)
            .await;
    }
}

/// Another client's ordinary `PutObject` overwrite of `name`, landing just before a commit of the
/// Complete. It keeps what it published, so a leg can name the generation it superseded.
struct Overwrite {
    stager: Stager,
    name: String,
    bytes: Vec<u8>,
    at: u64,
    landed: Mutex<Vec<(u64, Vec<ChunkRef>)>>,
}

#[async_trait]
impl Interference for Overwrite {
    async fn run(&self, store: &RedbMetadataStore) {
        let (_, version, chunks) = self
            .stager
            .put_object(store, &self.name, PUT_INODE, &self.bytes, self.at)
            .await;
        self.landed.lock().unwrap().push((version, chunks));
    }
}

/// One session made by Create, with a stager for it and nothing staged yet.
struct Rig {
    up: Upload,
    stager: Stager,
}

async fn rig() -> Rig {
    rig_with(None).await
}

/// [`rig`], with the session created declaring `content_type`.
async fn rig_with(content_type: Option<&str>) -> Rig {
    let up = upload_with(&[], content_type).await;
    let clock = ManualClock::new(CREATED_AT);
    let stager = Stager {
        fleet: Fleet::new(&clock),
        clock,
        topology: Arc::new(topology()),
        upload: up.id.clone(),
        next_chunk: Arc::new(AtomicU64::new(0)),
        next_attempt: Arc::new(AtomicU64::new(0)),
    };
    Rig { up, stager }
}

impl Rig {
    /// `UploadPart` of part `n` in [`CHUNK`]-byte chunks, outside the wrapper. Returns its digest.
    async fn stage(&self, n: u32, bytes: &[u8]) -> Digest {
        self.stager
            .upload(&self.up.store.inner, n, bytes, CHUNK)
            .await
    }

    /// The verb, through the wrapper.
    async fn complete(&self, parts: &NamedParts) -> Result<CompleteOutcome, CompleteError> {
        complete_upload(
            &self.up.store,
            &self.up.id,
            &self.up.request(parts),
            &PUBLISH,
        )
        .await
    }

    /// An ordinary `PutObject` at the session's own target, outside the wrapper.
    async fn put_target(&self, bytes: &[u8], at: u64) -> (InodeId, u64, Vec<ChunkRef>) {
        self.stager
            .put_object(&self.up.store.inner, &self.up.object, PUT_INODE, bytes, at)
            .await
    }

    /// A step that re-uploads part `n` as `bytes` in `chunk_size`-byte chunks, then does `then`.
    fn re_upload(&self, n: u32, bytes: &[u8], chunk_size: usize, then: OnCommit) -> OnCommit {
        let interference = ReUpload {
            stager: self.stager.clone(),
            n,
            bytes: bytes.to_vec(),
            chunk_size,
        };
        OnCommit::Run(Interfere(Arc::new(interference)), Box::new(then))
    }

    /// An overwrite of the session's target as `bytes`, for a [`Self::run`] step.
    fn overwrite(&self, bytes: &[u8], at: u64) -> Arc<Overwrite> {
        Arc::new(Overwrite {
            stager: self.stager.clone(),
            name: self.up.object.clone(),
            bytes: bytes.to_vec(),
            at,
            landed: Mutex::new(Vec::new()),
        })
    }

    /// The inode the session's target name is bound to, if any.
    async fn target(&self) -> Option<InodeId> {
        let key = metadata::dirent_key(ROOT, &self.up.object);
        let dirent = self.up.store.inner.get(&key).await.unwrap()?;
        let DirentRecord { inode } = metadata::decode(&dirent).unwrap();
        Some(inode)
    }

    /// What `read_object` returns for `inode`, which must be a committed object.
    async fn read(&self, inode: InodeId) -> Vec<u8> {
        read_object(&self.up.store.inner, &self.stager.fleet, inode)
            .await
            .expect("the read ran")
            .expect("a committed object")
    }

    /// Every `retire:` obligation under `prefix`, each decoded against its own key: one that does
    /// not decode fails the leg.
    async fn obligations(&self, prefix: &[u8]) -> Vec<(RetireMode, RetireToken, RetirePayload)> {
        let mut found = Vec::new();
        for (key, value) in self.up.store.inner.scan(prefix).await.unwrap() {
            let decoded = decode_retire_obligation(&key, &value);
            found.push(decoded.unwrap_or_else(|torn| {
                panic!("{} does not decode: {torn}", String::from_utf8_lossy(&key))
            }));
        }
        found
    }

    /// The chunk ids part `n`'s `part:` record names, which must exist.
    async fn part_chunks(&self, n: u32) -> BTreeSet<ChunkId> {
        let key = part_key(&self.up.id, part(n));
        let value = self.up.store.inner.get(&key).await.unwrap();
        let record = decode_part_record(&value.expect("the part record")).unwrap();
        record.chunks().iter().map(|chunk| chunk.id).collect()
    }

    /// **C-1, whole-store**: every chunk a fragment of which the fleet stores is named by a
    /// published root, a `part:` record, an owned `sidx:` entry or a `retire:bytes:` obligation's
    /// chunk list (an ordinary overwrite's superseded chunks carry `orphan:` marks instead, so
    /// `superseded` names them).
    async fn assert_every_fragment_named(&self, superseded: &BTreeSet<ChunkId>) {
        let inner = &self.up.store.inner;
        let mut named = superseded.clone();
        for (_, value) in inner.scan(b"inode:").await.unwrap() {
            let record: InodeRecord = metadata::decode(&value).unwrap();
            let chunks = record.chunk_map.as_flat().expect("a flat root");
            named.extend(chunks.iter().map(|chunk| chunk.id));
        }
        for (_, value) in inner.scan(wyrd_core::multipart::PART_PREFIX).await.unwrap() {
            let record = decode_part_record(&value).unwrap();
            named.extend(record.chunks().iter().map(|chunk| chunk.id));
        }
        for (key, _) in inner.scan(wyrd_core::multipart::SIDX_PREFIX).await.unwrap() {
            named.insert(wyrd_core::multipart::parse_sidx_key(&key).unwrap().2);
        }
        for (_, _, payload) in self.obligations(RETIRE_BYTES_PREFIX).await {
            named.extend(payload.chunks().iter().map(|chunk| chunk.id));
            if let Some(RetiredMap::Flat(chunks)) = payload.generation().map(|g| g.map()) {
                named.extend(chunks.iter().map(|chunk| chunk.id));
            }
        }
        let unnamed: Vec<_> = self
            .stager
            .fleet
            .chunks()
            .difference(&named)
            .copied()
            .collect();
        assert!(
            unnamed.is_empty(),
            "stored chunks nothing names: {unnamed:?}"
        );
    }
}

/// `outcome` as the fence phase's, for the assertions above that read one.
fn as_fence(
    outcome: Result<CompleteOutcome, CompleteError>,
) -> Result<FenceOutcome, CompleteError> {
    outcome.map(FenceOutcome::Answered)
}

/// The publication a request answered `Published` with.
fn published(outcome: Result<CompleteOutcome, CompleteError>) -> Publication {
    match outcome {
        Ok(CompleteOutcome::Published(publication)) => publication,
        other => panic!("expected Published, got {other:?}"),
    }
}

/// The named list over `parts`, each with its staged digest.
fn list_of(parts: &[(u32, Digest)]) -> NamedParts {
    NamedParts::new(parts.iter().map(|&(n, d)| (part(n), d)).collect()).unwrap()
}

/// The set of part numbers `numbers`.
fn set_of(numbers: &[u32]) -> PartScope {
    PartScope::Set(PartNumberSet::from_numbers(numbers.iter().map(|&n| part(n))).unwrap())
}

/// The session token at the fence epoch `epoch`.
fn session_token(id: &UploadId, epoch: u64) -> RetireToken {
    RetireToken::Session {
        upload_id: id.clone(),
        epoch,
        part: None,
    }
}

/// `batch`'s exact-value precondition at `key`, if it has one.
fn required(batch: &WriteBatch, key: &[u8]) -> Option<Bytes> {
    batch
        .preconditions
        .iter()
        .find(|p| p.key == key)
        .and_then(|p| p.expected.clone())
}

/// The version a flip `batch` puts at inode `id`.
fn flip_version(batch: &WriteBatch, id: InodeId) -> u64 {
    let key = metadata::inode_key(id);
    let (_, value) = batch
        .puts
        .iter()
        .find(|(k, _)| *k == key)
        .expect("the flip puts the root");
    metadata::decode::<InodeRecord>(value).unwrap().version
}

/// `flip` is a root flip of the fence `fence` put: it requires the fence's bytes and the bucket
/// marker validated behind it, puts the session at `Completed`, and installs every obligation it
/// puts under `require_absent`.
fn assert_flip(flip: &WriteBatch, up: &Upload, fence: &Bytes) {
    assert_eq!(
        required(flip, &mpu_key(&up.id)).as_ref(),
        Some(fence),
        "the fence pin"
    );
    assert_eq!(
        required(flip, &metadata::bucket_key(BUCKET)).as_deref(),
        Some(MARKER),
        "the bucket marker pin"
    );
    let (_, session) = flip
        .puts
        .iter()
        .find(|(k, _)| *k == mpu_key(&up.id))
        .expect("the flip moves the session");
    let session = decode_session_record(session).unwrap();
    assert!(matches!(session.state(), SessionState::Completed { .. }));
    for (key, _) in flip.puts.iter().filter(|(k, _)| k.starts_with(b"retire:")) {
        assert!(
            flip.preconditions
                .iter()
                .any(|p| p.key == *key && p.expected.is_none()),
            "{} is installed under require_absent",
            String::from_utf8_lossy(key)
        );
    }
}

/// `up`'s session is `Open@2` with no attempt spent — the fence at `Completing@1` released by
/// `release`, the last batch, which required the fence's bytes — and no name is bound.
async fn assert_released_with_nothing_published(
    rig: &Rig,
    fence: &WriteBatch,
    release: &WriteBatch,
) {
    assert_released(rig, fence, release).await;
    assert_eq!(rig.target().await, None, "no dirent for the key");
}

/// `up`'s session is `Open@2` with no attempt spent — the fence at `Completing@1` released by
/// `release`, which required the fence's bytes — and the flip published nothing: no inode under
/// [`FRESH_INODE`] and no flip obligation.
async fn assert_released(rig: &Rig, fence: &WriteBatch, release: &WriteBatch) {
    let up = &rig.up;
    let (_, fenced) = session_cas(fence, &up.id);
    let (required, put) = session_cas(release, &up.id);
    assert_eq!(
        required, fenced,
        "the release requires the fence's own bytes"
    );
    let expected = session_bytes(&up.open, 2, 0, OPEN);
    assert_eq!(put, expected, "Open@E+2, the attempt given back");
    assert_eq!(up.session().await.0, expected, "the release landed");
    assert_eq!(
        inode_at(&up.store.inner, FRESH_INODE).await,
        None,
        "no inode"
    );
    assert!(
        rig.obligations(RETIRE_RECORDS_PREFIX).await.is_empty(),
        "no flip obligation"
    );
}

/// Three parts of several chunks each: part 1 is 100 bytes, a non-final part whose length is not
/// a whole multiple of [`CHUNK`].
fn three_bodies() -> [(u32, Vec<u8>); 3] {
    [(1, body(100, 1)), (2, body(128, 2)), (3, body(70, 3))]
}

/// Stage `bodies` in the order given; returns each part's digest by part number.
async fn stage_all(rig: &Rig, bodies: &[(u32, Vec<u8>)]) -> BTreeMap<u32, Digest> {
    let mut digests = BTreeMap::new();
    for (n, bytes) in bodies {
        digests.insert(*n, rig.stage(*n, bytes).await);
    }
    digests
}

// ───────────────────────── (A) publish and read back ─────────────────────────

/// **(A)** Parts staged out of order — 3, 1, 2 — with part 1 a non-final part of 100 bytes in
/// 64-byte chunks. A Complete naming all three answers `Published` in two commits, the fence and
/// the flip, and:
///
/// * `read_object` returns the three bodies concatenated in part order, byte for byte;
/// * the root is a flat map of every part's chunks, in part order, at version 1 under the fresh
///   inode, stamped with the publication time and the session's content type, and the dirent
///   names it;
/// * the inode's ETag and the session's `Completion.etag` are both `multipart_etag` over the
///   named `(part, digest)` list, and the session is `Completed@2` with that list's
///   `complete_fingerprint`, the publication stamp, and the fence's attempt still spent.
///
/// An identical retry then answers `AlreadyCompleted` with that same publication, sending nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parts_staged_out_of_order_publish_one_flat_object_that_reads_back() {
    within(async {
        let rig = rig_with(Some(CONTENT_TYPE)).await;
        let bodies = three_bodies();
        assert_ne!(
            bodies[0].1.len() % CHUNK,
            0,
            "a non-final part off the chunk size"
        );
        let out_of_order = [bodies[2].clone(), bodies[0].clone(), bodies[1].clone()];
        let digests = stage_all(&rig, &out_of_order).await;
        let list = list_of(&[(1, digests[&1]), (2, digests[&2]), (3, digests[&3])]);
        let (_, before) = rig.up.session().await;

        let publication = published(rig.complete(&list).await);

        let commits = rig.up.store.take_commits();
        assert_eq!(commits.len(), 2, "the fence and the flip");
        let (_, fence) = session_cas(&commits[0], &rig.up.id);
        assert_flip(&commits[1], &rig.up, &fence);
        let etag = multipart_etag(list.as_slice()).unwrap();
        assert_eq!(
            publication,
            Publication {
                inode: FRESH_INODE,
                version: 1,
                etag,
                completed_at_millis: COMPLETED_AT,
            }
        );
        let concatenated: Vec<u8> = bodies.iter().flat_map(|(_, b)| b.clone()).collect();
        assert_eq!(rig.read(FRESH_INODE).await, concatenated, "byte for byte");
        assert_eq!(rig.target().await, Some(FRESH_INODE), "the dirent names it");

        let inode = inode_at(&rig.up.store.inner, FRESH_INODE).await.unwrap();
        let ChunkMap::Flat(chunks) = &inode.chunk_map else {
            panic!("expected a flat root, got {:?}", inode.chunk_map);
        };
        let mut in_part_order = Vec::new();
        for n in 1..=3 {
            let key = part_key(&rig.up.id, part(n));
            let record = rig.up.store.inner.get(&key).await.unwrap().unwrap();
            in_part_order.extend(decode_part_record(&record).unwrap().chunks().to_vec());
        }
        assert_eq!(chunks, &in_part_order, "every part's chunks, in part order");
        assert_eq!(inode.size, concatenated.len() as u64);
        assert_eq!(inode.version, 1);
        assert_eq!(inode.etag, Some(etag.to_string()), "the inode's ETag");
        assert_eq!(inode.modified, Some(COMPLETED_AT));
        assert_eq!(
            inode.content_type.as_deref(),
            Some(CONTENT_TYPE),
            "the session's content type"
        );

        let (_, after) = rig.up.session().await;
        let SessionState::Completed { completion } = after.state() else {
            panic!("expected Completed, got {:?}", after.state());
        };
        assert_eq!(completion.etag, etag, "the session's ETag");
        assert_eq!(
            completion.complete_fingerprint,
            complete_fingerprint(list.as_slice()).unwrap()
        );
        assert_eq!((completion.inode, completion.version), (FRESH_INODE, 1));
        assert_eq!(completion.completed_at_millis, COMPLETED_AT);
        assert_eq!(after.epoch(), before.epoch() + 2, "Completed@E+2");
        assert_eq!(
            after.attempts(),
            before.attempts() + 1,
            "the fence's attempt stays spent"
        );
        rig.assert_every_fragment_named(&BTreeSet::new()).await;

        let again = rig.complete(&list).await;
        let Ok(CompleteOutcome::AlreadyCompleted(recorded)) = again else {
            panic!("expected AlreadyCompleted, got {again:?}");
        };
        assert_eq!(recorded, publication, "the recorded publication");
        assert!(
            rig.up.store.take_commits().is_empty(),
            "the retry sends nothing"
        );
    })
    .await;
}

// ─────────────────── (D2-tail) after D1 and D2, a valid list publishes ───────────────────

/// **(D2-tail)** On one session: `MAX_COMPLETE_ATTEMPTS` typos (D1), each refused with nothing
/// written; then `MAX_COMPLETE_ATTEMPTS` requests each valid when first read while another
/// client's `UploadPart` replaces part 2 just before the fence commits (D2), each fenced,
/// rejected and released. Then a valid list answers `Published`, not
/// `CompleteAttemptsExhausted`, and the object reads back with part 2's last body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d2_tail_after_typos_and_replaced_parts_a_valid_list_publishes() {
    within(async {
        let rig = rig().await;
        let mut bodies = three_bodies();
        let digests = stage_all(&rig, &bodies).await;
        let mut current = digests[&2];

        // D1.
        let typos = [
            list_of(&[(1, digests[&1]), (4, digests[&1])]),
            list_of(&[(1, digests[&1]), (2, digests[&1])]),
            list_of(&[(2, digests[&2]), (1, digests[&1])]),
        ];
        assert!(typos.len() >= MAX_COMPLETE_ATTEMPTS as usize);
        for typo in &typos {
            let refusal = refused(as_fence(rig.complete(typo).await));
            assert!(
                matches!(refusal, Refusal::InvalidPart { .. }),
                "{refusal:?}"
            );
            assert!(
                rig.up.store.take_commits().is_empty(),
                "a typo fences nothing"
            );
        }

        // D2.
        for k in 0..MAX_COMPLETE_ATTEMPTS {
            let list = list_of(&[(1, digests[&1]), (2, current), (3, digests[&3])]);
            let replacement = body(128, 0x40 + k as u8);
            rig.up.store.script(
                vec![rig.re_upload(2, &replacement, CHUNK, OnCommit::Pass)],
                None,
            );
            let (before_bytes, before) = rig.up.session().await;
            assert_eq!(
                refused(as_fence(rig.complete(&list).await)),
                part_2_replaced(),
                "replacement {k}"
            );
            assert_fenced_and_released(&rig.up, (&before_bytes, &before)).await;
            current = sha256(&replacement);
            bodies[1].1 = replacement;
        }

        let (_, before) = rig.up.session().await;
        assert_eq!(before.attempts(), 0, "no request spent the budget");
        let list = list_of(&[(1, digests[&1]), (2, current), (3, digests[&3])]);
        let publication = published(rig.complete(&list).await);
        let concatenated: Vec<u8> = bodies.iter().flat_map(|(_, b)| b.clone()).collect();
        assert_eq!(rig.read(publication.inode).await, concatenated);
        assert_eq!(rig.up.session().await.1.attempts(), 1);
    })
    .await;
}

// ─────────────────────────── (C) a subset Complete ───────────────────────────

/// **(C)** Stage parts 1, 2 and 3; a Complete naming 1 and 3 publishes parts 1 + 3 (`0016:1034`).
/// The flip installs exactly two obligations, each decoding against its key: `retire:records:`
/// naming **only** parts 1 and 3, and `retire:bytes:` naming part 2, its bytes and then its
/// records.
/// No chunk of part 1 or 3 is in any `retire:bytes:` obligation, and part 2's `part:` record is
/// still there for that obligation's drain to read (`0016:910-925`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn c_a_subset_complete_retires_the_published_records_and_the_unnamed_bytes() {
    within(async {
        let rig = rig().await;
        let bodies = three_bodies();
        let digests = stage_all(&rig, &bodies).await;
        let list = list_of(&[(1, digests[&1]), (3, digests[&3])]);

        let publication = published(rig.complete(&list).await);

        let expected: Vec<u8> = [&bodies[0].1, &bodies[2].1]
            .into_iter()
            .flatten()
            .copied()
            .collect();
        assert_eq!(rig.read(publication.inode).await, expected, "parts 1 + 3");
        let token = session_token(&rig.up.id, 1);

        let records = rig.obligations(RETIRE_RECORDS_PREFIX).await;
        assert_eq!(
            records.len(),
            1,
            "one retire:records: obligation: {records:?}"
        );
        let (mode, found, payload) = &records[0];
        assert_eq!((mode, found), (&RetireMode::Records, &token));
        assert_eq!(
            payload.parts(),
            Some(&set_of(&[1, 3])),
            "only the published parts"
        );
        assert!(!payload.session() && payload.chunks().is_empty());
        assert!(payload.generation().is_none() && payload.segments().is_none());

        let bytes = rig.obligations(RETIRE_BYTES_PREFIX).await;
        assert_eq!(bytes.len(), 1, "one retire:bytes: obligation: {bytes:?}");
        let (mode, found, payload) = &bytes[0];
        assert_eq!((mode, found), (&RetireMode::Bytes, &token));
        assert_eq!(payload.parts(), Some(&set_of(&[2])), "the unnamed part");
        assert!(!payload.session() && payload.generation().is_none());

        let published_chunks: BTreeSet<ChunkId> =
            [rig.part_chunks(1).await, rig.part_chunks(3).await]
                .into_iter()
                .flatten()
                .collect();
        for (_, token, payload) in &bytes {
            let mut owed: BTreeSet<ChunkId> = payload.chunks().iter().map(|c| c.id).collect();
            if let Some(RetiredMap::Flat(chunks)) = payload.generation().map(|g| g.map()) {
                owed.extend(chunks.iter().map(|c| c.id));
            }
            assert!(
                owed.is_disjoint(&published_chunks),
                "{token}: no published chunk is owed"
            );
        }
        assert!(
            !rig.part_chunks(2).await.is_empty(),
            "part 2's record is still there"
        );
        rig.assert_every_fragment_named(&BTreeSet::new()).await;
    })
    .await;
}

// ──────────────────── (F) the version comes from the re-read prior ────────────────────

/// **(F)** The target exists at version 1, from an ordinary `PutObject`. An ordinary overwrite
/// lands just before the first flip, taking it to version 2, so that flip loses. The retry
/// re-reads the prior and publishes version **3**, never 2 again (`0016:1025`), and installs a
/// `retire:bytes:g:` obligation naming generation 2 by its chunk list. The flip that lost
/// offered 2; the object reads back as the parts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_a_flip_that_loses_to_an_overwrite_publishes_the_next_version_and_retires_it() {
    within(async {
        let rig = rig().await;
        let (id, version, v1_chunks) = rig.put_target(&body(90, 0x71), 1_100).await;
        assert_eq!((id, version), (PUT_INODE, 1));
        let bodies = [(1, body(100, 1)), (2, body(80, 2))];
        let digests = stage_all(&rig, &bodies).await;
        let interloper = rig.overwrite(&body(150, 0x72), 1_200);
        rig.up.store.script(
            vec![
                OnCommit::Pass,
                OnCommit::Run(Interfere(interloper.clone()), Box::new(OnCommit::Pass)),
            ],
            None,
        );
        let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

        let publication = published(rig.complete(&list).await);

        assert_eq!((publication.inode, publication.version), (PUT_INODE, 3));
        let commits = rig.up.store.take_commits();
        assert_eq!(
            commits.len(),
            3,
            "the fence, the flip that lost, the flip that won"
        );
        assert_eq!(
            flip_version(&commits[1], PUT_INODE),
            2,
            "offered against version 1"
        );
        assert_eq!(
            flip_version(&commits[2], PUT_INODE),
            3,
            "recomputed from version 2"
        );
        let landed = interloper.landed.lock().unwrap().clone();
        let [(2, v2_chunks)] = landed.as_slice() else {
            panic!("expected one overwrite to version 2, got {landed:?}");
        };
        let inode = inode_at(&rig.up.store.inner, PUT_INODE).await.unwrap();
        assert_eq!(inode.version, 3);
        assert_eq!(
            inode.etag,
            Some(multipart_etag(list.as_slice()).unwrap().to_string())
        );
        let (_, after) = rig.up.session().await;
        let SessionState::Completed { completion } = after.state() else {
            panic!("expected Completed, got {:?}", after.state());
        };
        assert_eq!((completion.inode, completion.version), (PUT_INODE, 3));
        let concatenated: Vec<u8> = bodies.iter().flat_map(|(_, b)| b.clone()).collect();
        assert_eq!(rig.read(PUT_INODE).await, concatenated);

        let generations: Vec<_> = rig
            .obligations(RETIRE_BYTES_PREFIX)
            .await
            .into_iter()
            .filter(|(_, token, _)| matches!(token, RetireToken::Generation { .. }))
            .collect();
        assert_eq!(generations.len(), 1, "{generations:?}");
        let (_, token, payload) = &generations[0];
        assert_eq!(
            token,
            &RetireToken::Generation {
                inode: PUT_INODE,
                version: 2
            }
        );
        let owed = payload.generation().expect("a generation");
        assert_eq!(
            owed.map(),
            &RetiredMap::Flat(v2_chunks.clone()),
            "generation 2's chunks"
        );
        // Generation 1 was superseded by the ordinary overwrite, which marked its fragments.
        let superseded: BTreeSet<ChunkId> = v1_chunks.iter().map(|c| c.id).collect();
        rig.assert_every_fragment_named(&superseded).await;
    })
    .await;
}

/// **(F, exhausted)** Every flip loses, each to an ordinary overwrite that lands just before it:
/// [`STATED_PUBLISH_ROUNDS`] flips, each offering the version after the one just read, then the
/// release. The request answers `OperationAborted`, the session is `Open@2` with the attempt
/// given back, and nothing it named is published. The session stays usable: a later request
/// with no interference publishes the next version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f_when_every_flip_loses_the_fence_is_released_and_the_session_stays_usable() {
    within(async {
        let rig = rig().await;
        rig.put_target(&body(90, 0x71), 1_100).await;
        let digests = stage_all(&rig, &[(1, body(100, 1)), (2, body(80, 2))]).await;
        let mut plan = vec![OnCommit::Pass];
        for k in 0..STATED_PUBLISH_ROUNDS {
            let interloper = rig.overwrite(&body(150, 0x72 + k as u8), 1_200 + u64::from(k));
            plan.push(OnCommit::Run(
                Interfere(interloper),
                Box::new(OnCommit::Pass),
            ));
        }
        rig.up.store.script(plan, None);
        let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

        let refusal = refused(as_fence(rig.complete(&list).await));

        assert_eq!(refusal, Refusal::OperationAborted);
        let commits = rig.up.store.take_commits();
        let flips = STATED_PUBLISH_ROUNDS as usize;
        assert_eq!(
            commits.len(),
            1 + flips + 1,
            "the fence, the flips, the release"
        );
        for (k, flip) in commits[1..=flips].iter().enumerate() {
            assert_eq!(
                flip_version(flip, PUT_INODE),
                k as u64 + 2,
                "flip {k} recomputed from the prior it read"
            );
        }
        let (_, fenced) = session_cas(&commits[0], &rig.up.id);
        let (required, put) = session_cas(&commits[flips + 1], &rig.up.id);
        assert_eq!(
            required, fenced,
            "the release requires the fence's own bytes"
        );
        let open = session_bytes(&rig.up.open, 2, 0, OPEN);
        assert_eq!(put, open, "Open@E+2, the attempt given back");
        assert_eq!(rig.up.session().await.0, open);
        let inode = inode_at(&rig.up.store.inner, PUT_INODE).await.unwrap();
        assert_eq!(
            inode.version,
            1 + u64::from(STATED_PUBLISH_ROUNDS),
            "the last overwrite's"
        );
        assert_ne!(
            inode.etag,
            Some(multipart_etag(list.as_slice()).unwrap().to_string())
        );
        assert!(
            rig.obligations(RETIRE_RECORDS_PREFIX).await.is_empty(),
            "no flip obligation"
        );

        let publication = published(rig.complete(&list).await);
        assert_eq!(
            (publication.inode, publication.version),
            (PUT_INODE, 2 + u64::from(STATED_PUBLISH_ROUNDS))
        );
    })
    .await;
}

// ───────────────────────── (F2) an empty prior owes nothing ─────────────────────────

/// **(F2)** The target exists as a zero-byte object, from an ordinary `PutObject` of an empty
/// body (an empty chunk map, `write.rs:166-168`, `:277-281`). A Complete over it answers
/// `Published` at version 2 and installs **no** generation obligation for that prior: it names no
/// chunk, and an obligation naming none would not decode. Every obligation it installs decodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f2_superseding_a_zero_byte_prior_installs_no_obligation_for_it() {
    within(async {
        let rig = rig().await;
        let (id, version, chunks) = rig.put_target(&[], 1_100).await;
        assert_eq!((id, version), (PUT_INODE, 1));
        assert!(chunks.is_empty(), "a zero-byte object names no chunk");
        let prior = inode_at(&rig.up.store.inner, PUT_INODE).await.unwrap();
        assert_eq!(prior.chunk_map, ChunkMap::Flat(Vec::new()));
        let bodies = [(1, body(100, 1)), (2, body(80, 2))];
        let digests = stage_all(&rig, &bodies).await;
        let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

        let publication = published(rig.complete(&list).await);

        assert_eq!((publication.inode, publication.version), (PUT_INODE, 2));
        let concatenated: Vec<u8> = bodies.iter().flat_map(|(_, b)| b.clone()).collect();
        assert_eq!(rig.read(PUT_INODE).await, concatenated);
        let bytes = rig.obligations(RETIRE_BYTES_PREFIX).await;
        assert!(
            bytes.is_empty(),
            "no generation, and no part left unnamed: {bytes:?}"
        );
        let records = rig.obligations(RETIRE_RECORDS_PREFIX).await;
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0].2.parts(), Some(&set_of(&[1, 2])));
        // Every `retire:` key in the store decodes, under either mode.
        assert_eq!(rig.obligations(b"retire:").await.len(), 1);
    })
    .await;
}

// ──────────────────────── (H2) the bucket removed just before the flip ────────────────────────

/// **(H2)** The bucket marker is deleted after the validation behind the fence, just before the
/// flip commits. The flip pins the marker's exact bytes (`create.rs:698-705`, `0016:663`), so it
/// loses, and the request answers `NoSuchBucket`: never `OperationAborted`, and never a second
/// flip. No dirent or inode exists for the key, and the session is `Open@2` with the attempt
/// given back. A bucket deleted and created again in that window, its marker present but no
/// longer the bytes validated, answers the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_a_bucket_removed_just_before_the_flip_answers_no_such_bucket() {
    within(async {
        let marker = metadata::bucket_key(BUCKET);
        let recreated = br#"{"name":"photos","created_at_millis":2}"#;
        for (label, write) in [
            ("removed", WriteBatch::new().delete(marker.clone())),
            (
                "recreated",
                WriteBatch::new().put(marker.clone(), &recreated[..]),
            ),
        ] {
            let rig = rig().await;
            let digests = stage_all(&rig, &[(1, body(100, 1)), (2, body(80, 2))]).await;
            rig.up.store.script(
                vec![OnCommit::Pass, OnCommit::write_first(write, OnCommit::Pass)],
                None,
            );
            let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

            assert_eq!(
                refused(as_fence(rig.complete(&list).await)),
                Refusal::NoSuchBucket,
                "{label}"
            );

            let commits = rig.up.store.take_commits();
            assert_eq!(
                commits.len(),
                3,
                "{label}: the fence, one flip, the release"
            );
            let (_, fence) = session_cas(&commits[0], &rig.up.id);
            assert_flip(&commits[1], &rig.up, &fence);
            assert_released_with_nothing_published(&rig, &commits[0], &commits[2]).await;
        }
    })
    .await;
}

// ───────────────────── a flip that loses its fence to another writer ─────────────────────

/// **Another writer takes the fence before the flip.** Just before the flip commits, the session
/// moves off the fence: the reaper's rollback to `Open@2`, or a teardown fence to `Aborting@2`.
/// The flip loses, and the request is answered from what the session now is —
/// `OperationAborted`, and `NoSuchUpload` — with no release, since the fence is no longer this
/// Complete's to take down, and no other flip. Nothing is published, and the session is left as
/// it moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flip_that_loses_its_fence_to_another_writer_is_answered_from_the_new_state() {
    within(async {
        for (label, state, expected) in [
            ("rolled back", OPEN, Refusal::OperationAborted),
            ("torn down", ABORTING, Refusal::NoSuchUpload),
        ] {
            let rig = rig().await;
            let digests = stage_all(&rig, &[(1, body(100, 1))]).await;
            let moved = session_bytes(&rig.up.open, 2, 1, state);
            let write = WriteBatch::new().put(mpu_key(&rig.up.id), moved.clone());
            rig.up.store.script(
                vec![OnCommit::Pass, OnCommit::write_first(write, OnCommit::Pass)],
                None,
            );

            let outcome = rig.complete(&list_of(&[(1, digests[&1])])).await;

            assert_eq!(refused(as_fence(outcome)), expected, "{label}");
            let commits = rig.up.store.take_commits();
            assert_eq!(
                commits.len(),
                2,
                "{label}: the fence and one flip, no release"
            );
            assert_eq!(rig.up.session().await.0, moved, "{label}: left as it moved");
            assert_eq!(rig.target().await, None, "{label}: nothing published");
            assert!(rig.obligations(RETIRE_RECORDS_PREFIX).await.is_empty());
        }
    })
    .await;
}

// ──────────────────── a damaged part or target behind the fence ────────────────────

/// **A damaged part or target behind the fence stops, after one release.** In each case the list
/// is valid against the part set the fence froze, and a record publication reads next is not
/// what it must be:
///
/// * part 2's `part:` record deleted, or replaced by one whose summary is not the frozen one,
///   just before the fence commits ([`Stop::PartDisagrees`]): the pair is written in one batch,
///   so this is damage;
/// * the target's dirent names an inode that has no record ([`Stop::Target`]);
/// * the target's inode is at the last version, which has no successor ([`Stop::Publication`]).
///
/// No flip is sent. The fence is released (`Open@2`, the attempt given back) and the stopped error
/// says so, with that cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_damaged_part_or_target_behind_the_fence_stops_after_one_release() {
    within(async {
        // The part record.
        for label in ["deleted", "replaced"] {
            let rig = rig().await;
            let digests = stage_all(&rig, &[(1, body(100, 1)), (2, body(80, 2))]).await;
            let key = part_key(&rig.up.id, part(2));
            let damage = if label == "deleted" {
                WriteBatch::new().delete(key)
            } else {
                // A canonical part record, without the `psum:` summary that would pair with it.
                let planted = plant_part(&rig.up.id, 2, digests[&2]);
                assert_eq!(planted.puts[0].0, key);
                WriteBatch::new().put(key, planted.puts[0].1.clone())
            };
            rig.up
                .store
                .script(vec![OnCommit::write_first(damage, OnCommit::Pass)], None);
            let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

            let (_, stopped) = stopped_error(as_fence(rig.complete(&list).await), &rig.up.id);

            assert_eq!(stopped.fence, FenceState::Released { epoch: 1 }, "{label}");
            assert!(
                matches!(
                    stopped.cause,
                    Stop::PartDisagrees { part_number } if part_number == part(2)
                ),
                "{label}: {:?}",
                stopped.cause
            );
            let commits = rig.up.store.take_commits();
            assert_eq!(
                commits.len(),
                2,
                "{label}: the fence and its release, no flip"
            );
            assert_released_with_nothing_published(&rig, &commits[0], &commits[1]).await;
        }

        // The target.
        let dangling = WriteBatch::new().put(
            metadata::dirent_key(ROOT, "photos/object-1"),
            metadata::encode(&DirentRecord { inode: 777 }),
        );
        let last = InodeRecord {
            version: u64::MAX,
            state: metadata::InodeState::Committed,
            ..InodeRecord::default()
        };
        let at_last_version = WriteBatch::new()
            .put(
                metadata::dirent_key(ROOT, "photos/object-1"),
                metadata::encode(&DirentRecord { inode: PUT_INODE }),
            )
            .put(metadata::inode_key(PUT_INODE), metadata::encode(&last));
        for (label, plant) in [
            ("dangling", dangling),
            ("at the last version", at_last_version),
        ] {
            let rig = rig().await;
            assert_eq!(rig.up.object, "photos/object-1");
            let digests = stage_all(&rig, &[(1, body(100, 1))]).await;
            let planted = rig.up.store.inner.commit(plant).await.unwrap();
            assert_eq!(planted, CommitOutcome::Committed);

            let outcome = rig.complete(&list_of(&[(1, digests[&1])])).await;

            let (_, stopped) = stopped_error(as_fence(outcome), &rig.up.id);
            assert_eq!(stopped.fence, FenceState::Released { epoch: 1 }, "{label}");
            if label == "dangling" {
                assert!(
                    matches!(
                        &stopped.cause,
                        Stop::Target(torn) if torn.to_string().contains("777")
                    ),
                    "{label}: {:?}",
                    stopped.cause
                );
            } else {
                assert!(
                    matches!(
                        stopped.cause,
                        Stop::Publication(PublicationError::VersionOverflow { inode })
                            if inode == PUT_INODE
                    ),
                    "{label}: {:?}",
                    stopped.cause
                );
            }
            let commits = rig.up.store.take_commits();
            assert_eq!(
                commits.len(),
                2,
                "{label}: the fence and its release, no flip"
            );
            assert_released(&rig, &commits[0], &commits[1]).await;
        }
    })
    .await;
}

// ─────────────────── a read that fails after a lost flip, or before one ───────────────────

/// **A read that fails after a lost flip is a fault behind the fence.** The flip answers
/// `Conflict` with nothing changed (the TiKV lock), and the read that would judge it fails: the
/// session, the bucket marker, or the target's dirent before the next flip. A part record that
/// cannot be read before any flip is the same. Each ends in the stopped error with that read's
/// fault as its cause, after one release that takes the fence down, and no other flip is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_that_fails_after_a_lost_flip_releases_the_fence_and_stops() {
    within(async {
        let dirent = metadata::dirent_key(ROOT, "photos/object-1");
        let after_a_lost_flip = [
            ("the session", MPU_PREFIX.to_vec()),
            ("the bucket marker", metadata::bucket_key(BUCKET)),
            ("the target's dirent", dirent),
        ];
        for (label, prefix) in after_a_lost_flip {
            let rig = rig().await;
            let digests = stage_all(&rig, &[(1, body(100, 1))]).await;
            rig.up.store.script(
                vec![
                    OnCommit::Pass,
                    OnCommit::arm(Arm::FailGets(prefix), OnCommit::ConflictUnmoved),
                ],
                None,
            );

            let outcome = rig.complete(&list_of(&[(1, digests[&1])])).await;

            let (_, stopped) = stopped_error(as_fence(outcome), &rig.up.id);
            assert_eq!(stopped.fence, FenceState::Released { epoch: 1 }, "{label}");
            assert_read_fault(&stopped.cause);
            let commits = rig.up.store.take_commits();
            assert_eq!(
                commits.len(),
                3,
                "{label}: the fence, one flip, the release"
            );
            assert_released_with_nothing_published(&rig, &commits[0], &commits[2]).await;
        }

        let rig = rig().await;
        let digests = stage_all(&rig, &[(1, body(100, 1))]).await;
        let parts = part_range(&rig.up.id);
        rig.up
            .store
            .script(vec![fence_then(Arm::FailGets(parts))], None);
        let outcome = rig.complete(&list_of(&[(1, digests[&1])])).await;
        let (_, stopped) = stopped_error(as_fence(outcome), &rig.up.id);
        assert_eq!(stopped.fence, FenceState::Released { epoch: 1 });
        assert_read_fault(&stopped.cause);
        let commits = rig.up.store.take_commits();
        assert_eq!(commits.len(), 2, "the fence and its release, no flip");
        assert_released_with_nothing_published(&rig, &commits[0], &commits[1]).await;
    })
    .await;
}

// ──────────────────────── (J) a flip the store refuses for good ────────────────────────

/// **(J)** The store answers the flip with a definite error that is neither `Conflict` nor an
/// unknown outcome. The request sends no other flip and releases the fence (`Open@2`, the attempt
/// given back), then returns the stopped error with that store error as its cause, saying the
/// fence was taken down. No dirent or inode exists. When that release fails too, the stopped
/// error says the fence still stands, with the flip's error still its cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn j_a_flip_the_store_refuses_for_good_releases_the_fence_and_stops() {
    within(async {
        let rig = rig().await;
        let digests = stage_all(&rig, &[(1, body(100, 1)), (2, body(80, 2))]).await;
        rig.up.store.script(
            vec![OnCommit::Pass, OnCommit::Fail { transient: true }],
            None,
        );
        let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

        let outcome = rig.complete(&list).await;

        assert_eq!(
            outcome.as_ref().err().map(|err| classify(err)),
            Some(ErrorClass::Transient),
            "the store error's own class"
        );
        let (text, stopped) = stopped_error(as_fence(outcome), &rig.up.id);
        assert_eq!(stopped.fence, FenceState::Released { epoch: 1 });
        assert!(
            matches!(&stopped.cause, Stop::Store(fault) if fault.is::<TransientFault>()),
            "{:?}",
            stopped.cause
        );
        assert!(text.contains("was taken down"), "{text}");
        let commits = rig.up.store.take_commits();
        assert_eq!(commits.len(), 3, "the fence, one flip, the release");
        assert_released_with_nothing_published(&rig, &commits[0], &commits[2]).await;

        // The release fails for good as well: the fence stands.
        let rig = self::rig().await;
        let digests = stage_all(&rig, &[(1, body(100, 1))]).await;
        rig.up.store.script(
            vec![
                OnCommit::Pass,
                OnCommit::Fail { transient: true },
                OnCommit::Fail { transient: false },
            ],
            None,
        );
        let list = list_of(&[(1, digests[&1])]);
        let (_, stopped) = stopped_error(as_fence(rig.complete(&list).await), &rig.up.id);
        assert_eq!(stopped.fence, FenceState::Stands { epoch: 1 });
        assert!(
            matches!(&stopped.cause, Stop::Store(fault) if fault.is::<TransientFault>()),
            "the flip's error is the cause: {:?}",
            stopped.cause
        );
        let commits = rig.up.store.take_commits();
        assert_eq!(commits.len(), 3, "the fence, one flip, one release");
        assert_eq!(
            rig.up.session().await.0,
            session_cas(&commits[0], &rig.up.id).1
        );
        assert_eq!(rig.target().await, None);
    })
    .await;
}

// ──────────────────────── (G0-flip) an unknown flip outcome ────────────────────────

/// **(G0-flip)** The store answers the flip `CommitUnknownResult`, once applied and once not. Each
/// time the request returns the in-doubt error about the flip, carrying that outcome, and sends no
/// further commit: no release, which could not tell a flip that landed from one that did not.
/// Applied, the object is published and the session `Completed`; not applied, the session is
/// still the fence's bytes and no name is bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g0_an_unknown_flip_outcome_returns_the_in_doubt_error_and_nothing_more() {
    within(async {
        for (applied, may_still_commit) in [(true, false), (false, true)] {
            let rig = rig().await;
            let digests = stage_all(&rig, &[(1, body(100, 1)), (2, body(80, 2))]).await;
            rig.up.store.script(
                vec![
                    OnCommit::Pass,
                    OnCommit::Unknown {
                        applied,
                        may_still_commit,
                    },
                ],
                None,
            );
            let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

            let doubt = in_doubt_error(
                as_fence(rig.complete(&list).await),
                &rig.up.id,
                DoubtfulCommit::Flip { epoch: 1 },
                may_still_commit,
            );

            assert!(doubt.fault_behind_fence.is_none(), "{doubt:?}");
            let commits = rig.up.store.take_commits();
            assert_eq!(commits.len(), 2, "the fence, the flip, and nothing after");
            let (stored, record) = rig.up.session().await;
            if applied {
                assert!(matches!(record.state(), SessionState::Completed { .. }));
                assert_eq!(rig.target().await, Some(FRESH_INODE));
            } else {
                assert_eq!(stored, session_cas(&commits[0], &rig.up.id).1, "the fence");
                assert_eq!(rig.target().await, None);
            }
        }
    })
    .await;
}

// ─────────────────────────── (K) flat only, for now ───────────────────────────

/// **(K)** Parts totalling [`STATED_FLAT_CHUNKS`]` + 1` chunks by their summaries: the request
/// stops with the cause that names segmented publication as unsupported, sending **no** commit,
/// since the check before the fence sees the excess. The session is as Create left it. Naming
/// part 1 alone, exactly [`STATED_FLAT_CHUNKS`] chunks, then publishes one flat root of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k_a_list_past_one_flat_map_stops_before_the_fence() {
    within(async {
        let rig = rig().await;
        let flat = STATED_FLAT_CHUNKS as usize;
        let digests = stage_all(&rig, &[(1, body(flat * CHUNK, 1)), (2, body(10, 2))]).await;
        let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

        let (text, stopped) = stopped_error(as_fence(rig.complete(&list).await), &rig.up.id);

        assert_eq!(stopped.fence, FenceState::NotPlaced);
        assert!(
            matches!(stopped.cause, Stop::SegmentedUnsupported { chunks, limit }
                if chunks == STATED_FLAT_CHUNKS + 1 && limit == STATED_FLAT_CHUNKS),
            "{:?}",
            stopped.cause
        );
        assert!(
            text.contains("segmented publication is not supported"),
            "{text}"
        );
        assert!(rig.up.store.take_commits().is_empty(), "zero commits");
        assert_eq!(rig.up.session().await.0, rig.up.open);

        let publication = published(rig.complete(&list_of(&[(1, digests[&1])])).await);
        let inode = inode_at(&rig.up.store.inner, publication.inode)
            .await
            .unwrap();
        assert_eq!(inode.chunk_map.as_flat().map(<[ChunkRef]>::len), Some(flat));
        assert_eq!(rig.read(publication.inode).await, body(flat * CHUNK, 1));
    })
    .await;
}

/// **(K, behind the fence)** Parts totalling [`STATED_FLAT_CHUNKS`] chunks when first read; just
/// before the fence commits, another client re-uploads part 2 with the **same** body in smaller
/// chunks, so its digest is unchanged and the list still valid, but the frozen set totals one
/// chunk more. The request stops with that cause after one release: the session is `Open@2`
/// with the attempt given back, and nothing is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k_a_list_only_the_frozen_set_shows_past_one_flat_map_releases_the_fence() {
    within(async {
        let rig = rig().await;
        let first = STATED_FLAT_CHUNKS as usize - 1;
        let part_2 = body(CHUNK, 2);
        let digests = stage_all(&rig, &[(1, body(first * CHUNK, 1)), (2, part_2.clone())]).await;
        rig.up.store.script(
            vec![rig.re_upload(2, &part_2, CHUNK / 2, OnCommit::Pass)],
            None,
        );
        let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

        let (_, stopped) = stopped_error(as_fence(rig.complete(&list).await), &rig.up.id);

        assert_eq!(stopped.fence, FenceState::Released { epoch: 1 });
        assert!(
            matches!(stopped.cause, Stop::SegmentedUnsupported { chunks, limit }
                if chunks == STATED_FLAT_CHUNKS + 1 && limit == STATED_FLAT_CHUNKS),
            "{:?}",
            stopped.cause
        );
        let commits = rig.up.store.take_commits();
        assert_eq!(commits.len(), 2, "the fence and its release");
        assert_released_with_nothing_published(&rig, &commits[0], &commits[1]).await;
    })
    .await;
}

// ─────────────────────── (F3) a segmented prior is retired by its group ───────────────────────

/// A segmented object at `name` under inode `id`, published by #653's committer: `count` chunk
/// refs, past one flat map, under a fence record of the test's own whose value is the segment
/// cursor. Its chunks are refs only; no leg reads them. Returns the group its root names.
async fn put_segmented(
    store: &RedbMetadataStore,
    name: &str,
    id: InodeId,
    count: u64,
) -> SegmentGroup {
    let fence_key = b"test-fence".to_vec();
    let cursor = |n: u32| Bytes::from(n.to_string());
    let seed = WriteBatch::new().put(fence_key.clone(), cursor(0));
    assert_eq!(store.commit(seed).await.unwrap(), CommitOutcome::Committed);
    let chunks: Vec<ChunkRef> = (0..count)
        .map(|i| ChunkRef {
            id: 70_000 + u128::from(i),
            scheme: EcScheme::None,
            len: 10,
            placement: vec![0],
        })
        .collect();
    let segments = staged_publication::planned_segments(&chunks).unwrap();
    assert!(segments > 0, "a segmented map");
    let group = SegmentGroup::new(format!("{:032x}", 0xfeed_u32), 1).unwrap();
    let step = |info: SegmentBatchInfo| {
        WriteBatch::new()
            .require(fence_key.clone(), cursor(info.first_index))
            .put(fence_key.clone(), cursor(info.written))
    };
    let flip = WriteBatch::new()
        .require(fence_key.clone(), cursor(segments))
        .put(fence_key.clone(), Bytes::from_static(b"done"));
    let publication = staged_publication::Publication {
        parent: ROOT,
        name,
        root: staged_publication::RootTarget::Fresh { id },
        group: group.clone(),
        chunks: &chunks,
        meta: &ObjectMeta::default(),
        fence_key: &fence_key,
        segment_contribution: &step,
        resume_from: 0,
        byte_budget: None,
        ops_budget: None,
        flip_contribution: flip,
    };
    let outcome = staged_publication::publish(store, &publication)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    group
}

/// **(F3)** The target is a segmented object at version 1. A Complete over it publishes version 2
/// and retires the superseded generation by the segment group its root named
/// (`retire:bytes:g:<inode>:1`, `{generation: {segments}}`, `0016:2417`), never by a chunk list:
/// its chunks live in `seg:` records the drain resolves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f3_a_segmented_prior_is_retired_by_its_segment_group() {
    within(async {
        let rig = rig().await;
        let group = put_segmented(
            &rig.up.store.inner,
            &rig.up.object,
            PUT_INODE,
            STATED_FLAT_CHUNKS + 1,
        )
        .await;
        let prior = inode_at(&rig.up.store.inner, PUT_INODE).await.unwrap();
        assert!(prior.chunk_map.is_segmented());
        let bodies = [(1, body(100, 1))];
        let digests = stage_all(&rig, &bodies).await;

        let publication = published(rig.complete(&list_of(&[(1, digests[&1])])).await);

        assert_eq!((publication.inode, publication.version), (PUT_INODE, 2));
        assert_eq!(rig.read(PUT_INODE).await, bodies[0].1);
        let generations: Vec<_> = rig
            .obligations(RETIRE_BYTES_PREFIX)
            .await
            .into_iter()
            .filter(|(_, token, _)| matches!(token, RetireToken::Generation { .. }))
            .collect();
        assert_eq!(generations.len(), 1, "{generations:?}");
        let (_, token, payload) = &generations[0];
        assert_eq!(
            token,
            &RetireToken::Generation {
                inode: PUT_INODE,
                version: 1
            }
        );
        let owed = payload.generation().expect("a generation");
        assert_eq!(
            owed.map(),
            &RetiredMap::Segmented(group),
            "by its segment group"
        );
    })
    .await;
}

/// **(K) is the verb's, not the fence phase's.** `fence_complete` on its own holds a fence for a
/// list past one flat map: the flat-only stop is [`complete_upload`]'s until #1009, and the fence
/// phase it composes stays the one #1009 builds segmented publication on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn k_the_fence_phase_on_its_own_holds_a_list_past_one_flat_map() {
    within(async {
        let rig = rig().await;
        let flat = STATED_FLAT_CHUNKS as usize;
        let digests = stage_all(&rig, &[(1, body(flat * CHUNK, 1)), (2, body(10, 2))]).await;
        let list = list_of(&[(1, digests[&1]), (2, digests[&2])]);

        let fence = held(rig.up.complete(&list).await);

        let chunks: u64 = fence
            .frozen_parts()
            .values()
            .map(|summary| u64::from(summary.chunks))
            .sum();
        assert_eq!(chunks, STATED_FLAT_CHUNKS + 1);
        assert_eq!(rig.up.store.take_commits().len(), 1, "the fence alone");
    })
    .await;
}
