//! Issue #841 (809.3) — the post-restore pass **fences** every upload session the restored image
//! holds `Open` (proposal 0016 D-B, `0016:717-728`; the fence rows `:664`, `:823`). On the base it
//! reads a session by key alone and never writes one, so a restored image can resurrect a session
//! torn down after the restore point, and a retried Complete could publish over reclaimed bytes.
//!
//! Legs: **F** fenced whole; **F-atomic** all or nothing; **F-race** never fenced blind;
//! **F-collision** never over an obligation; **H** what cannot be fenced is named; **K** a second
//! pass is idempotent; **P3** a fence fault hides no verdict; **Paging**.
//!
//! Every leg drives the production `reconcile_after_restore` over the doubles in this file. Every
//! session is seeded as raw JSON in the codec's own spelling (`segment_nonce` included) and
//! round-tripped through `decode_session_record`. The report fields this slice adds are read
//! through `Debug`, so the file builds on the base and fails there by assertion.

// deferred: #843 — seeded Tier-0 DST coverage of this fence (809.5); these legs are Tokio-only.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_core::metadata::{self, inode_key, orphan_key, ChunkRef, EcScheme, InodeRecord};
use wyrd_core::multipart::{
    decode_retire_obligation, decode_session_record, mpu_key, part_key, retire_key,
    retire_session_range, PartNumber, PartScope, RetireMode, RetireToken, UploadId, MPU_PREFIX,
};
use wyrd_custodian::{reconcile_after_restore, ExpiredPendingPolicy, GcContext, RestoreReport};
use wyrd_traits::{
    chunk_hex, page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome,
    CommitUnknownResult, DServerId, FragmentId, Health, MetadataStore, PageStart, Result,
    ScanCapExceeded, ScanPage, WriteBatch, SCAN_CAP,
};

const NOW: u64 = 10_000;
/// Every seeded session's epoch `E`, unless a leg says otherwise.
const EPOCH: u64 = 3;
const RS_2_1: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };

// ---- the metadata double ------------------------------------------------------------------------

/// A concurrent batch the double lands just before the first commit putting its trigger key:
/// (trigger, batch, its outcome once applied).
type BeforeCommit = (Vec<u8>, Option<WriteBatch>, Option<CommitOutcome>);

/// An in-memory `MetadataStore` whose `scan` refuses a result past `cap` and whose `scan_page`
/// clamps to it through the seam's own helpers. It can fail every commit that puts a key (with an
/// unknown outcome), land a concurrent batch just before the first commit that puts a key, and it
/// logs every batch it applied.
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    cap: usize,
    failing: Mutex<Vec<Vec<u8>>>,
    before: Mutex<Vec<BeforeCommit>>,
    applied: Mutex<Vec<WriteBatch>>,
}

impl Meta {
    fn with_cap(cap: usize) -> Self {
        Self {
            kv: Mutex::default(),
            cap,
            failing: Mutex::default(),
            before: Mutex::default(),
            applied: Mutex::default(),
        }
    }

    fn new() -> Self {
        Self::with_cap(SCAN_CAP)
    }

    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key.into(), value.into());
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    fn keys_under(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let kv = self.kv.lock().unwrap();
        kv.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    fn snapshot(&self) -> BTreeMap<Vec<u8>, Bytes> {
        self.kv.lock().unwrap().clone()
    }

    fn fail_commits_putting(&self, key: &[u8]) {
        self.failing.lock().unwrap().push(key.to_vec());
    }

    fn before_commit_putting(&self, trigger: &[u8], batch: WriteBatch) {
        let hook = (trigger.to_vec(), Some(batch), None);
        self.before.lock().unwrap().push(hook);
    }

    fn apply(&self, batch: &WriteBatch) -> CommitOutcome {
        let mut kv = self.kv.lock().unwrap();
        if batch
            .preconditions
            .iter()
            .any(|pre| kv.get(&pre.key) != pre.expected.as_ref())
        {
            return CommitOutcome::Conflict;
        }
        for key in &batch.deletes {
            kv.remove(key);
        }
        for (key, value) in &batch.puts {
            kv.insert(key.clone(), value.clone());
        }
        CommitOutcome::Committed
    }
}

fn puts(batch: &WriteBatch, key: &[u8]) -> bool {
    batch.puts.iter().any(|(put, _)| put.as_slice() == key)
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.value(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let hits: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if hits.len() > self.cap {
            let (cap, prefix) = (self.cap, prefix.to_vec());
            return Err(BoxError::from(ScanCapExceeded { cap, prefix }));
        }
        Ok(hits)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.cap, prefix)?;
        let lower = match page_start(prefix, after) {
            PageStart::After(cursor) => Bound::Excluded(cursor),
            PageStart::Prefix => Bound::Included(prefix),
            PageStart::PastPrefix => return Ok((Vec::new(), None)),
        };
        let items: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range::<[u8], _>((lower, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        if self.failing.lock().unwrap().iter().any(|k| puts(&batch, k)) {
            return Err(BoxError::from(CommitUnknownResult {
                backend: "restore-open-fence-double",
                code: None,
                detail: "injected".to_owned(),
                may_still_commit: false,
            }));
        }
        for (trigger, concurrent, outcome) in self.before.lock().unwrap().iter_mut() {
            if puts(&batch, trigger) {
                if let Some(concurrent) = concurrent.take() {
                    *outcome = Some(self.apply(&concurrent));
                }
            }
        }
        let outcome = self.apply(&batch);
        if outcome == CommitOutcome::Committed {
            self.applied.lock().unwrap().push(batch);
        }
        Ok(outcome)
    }
}

// ---- the D-server double ------------------------------------------------------------------------

#[derive(Default)]
struct Disk {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for Disk {
    async fn put_fragment(&self, id: FragmentId, bytes: Bytes, _: Option<u64>) -> Result<()> {
        self.frags.lock().unwrap().insert(id, bytes);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&id).cloned())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.frags.lock().unwrap().keys().copied().collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.frags.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

fn place(d: &[Disk; 4], dserver: DServerId, frag: FragmentId) {
    let bytes = Bytes::from_static(b"fragment");
    d[dserver as usize]
        .frags
        .lock()
        .unwrap()
        .insert(frag, bytes);
}

/// One post-restore pass at [`NOW`], over the whole fleet.
async fn restore_pass(meta: &Meta, d: &[Disk; 4]) -> Result<RestoreReport> {
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let ctx = GcContext {
        meta,
        fleet: &fleet,
        grace_window_millis: 50,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    reconcile_after_restore(&ctx, NOW).await
}

// ---- the records --------------------------------------------------------------------------------

/// An upload id from a hex pair. Every session in this file has its own.
fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).expect("32 lowercase-hex characters")
}

fn name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).expect("every key here is ASCII")
}

/// A session in `state` at `epoch`, in the codec's own spelling — `segment_nonce` right after
/// `clock_source`, in every state — checked against `decode_session_record`, byte for byte.
fn session(state: &str, epoch: u64) -> Bytes {
    let bytes = format!(
        "{{\"parent\":42,\"object\":\"o\",\"created_at_millis\":100,\"clock_source\":\"wall\",\
         \"segment_nonce\":\"0123456789abcdef0123456789abcdef\",\"epoch\":{epoch},\
         \"attempts\":1,\"state\":{state}}}"
    );
    let record = decode_session_record(bytes.as_bytes())
        .unwrap_or_else(|fault| panic!("the seeded session {state} must decode: {fault}"));
    assert_eq!(metadata::encode(&record), bytes.as_bytes());
    Bytes::from(bytes)
}

fn open(epoch: u64) -> Bytes {
    session("{\"kind\":\"Open\"}", epoch)
}

fn aborting(epoch: u64) -> Bytes {
    session("{\"kind\":\"Aborting\"}", epoch)
}

fn completing(epoch: u64) -> Bytes {
    let target = format!("{{\"parent\":42,\"name\":\"o\",\"epoch\":{epoch}}}");
    let state = format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":0,\
         \"publish_target\":{target}}}"
    );
    session(&state, epoch)
}

fn completed(epoch: u64) -> Bytes {
    let (etag, print) = ("ab".repeat(32), "cd".repeat(32));
    let state = format!(
        "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":7,\"version\":1,\"etag\":\
         \"{etag}-1\",\"completed_at_millis\":950,\"complete_fingerprint\":\"{print}\"}}}}"
    );
    session(&state, epoch)
}

fn chunk_ref(id: ChunkId, scheme: EcScheme, placement: &[DServerId]) -> ChunkRef {
    let placement = placement.to_vec();
    ChunkRef {
        id,
        scheme,
        len: 5,
        placement,
    }
}

/// Seed part `n` of `id` naming `chunk`, in the part codec's canonical spelling.
fn seed_part(meta: &Meta, id: &UploadId, n: u32, chunk: ChunkRef) {
    let value = format!(
        "{{\"chunks\":[{}],\"len\":5,\"digest\":\"{}\",\"committed_at_millis\":800,\
         \"session_epoch\":{EPOCH}}}",
        String::from_utf8(metadata::encode(&chunk).to_vec()).unwrap(),
        "ef".repeat(32)
    );
    let n = PartNumber::new(n).expect("a part number");
    meta.seed(part_key(id, n), value.into_bytes());
}

fn commit_object(meta: &Meta, inode: u64, chunk: ChunkRef) {
    let record = InodeRecord {
        size: chunk.len,
        chunk_map: vec![chunk].into(),
        state: metadata::InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    meta.seed(inode_key(inode), metadata::encode(&record));
}

/// The token, and the key, a fence from `epoch` installs its obligation under.
fn teardown_token(id: &UploadId, epoch: u64) -> RetireToken {
    let (upload_id, part) = (id.clone(), None);
    RetireToken::Session {
        upload_id,
        epoch,
        part,
    }
}

fn teardown_key(id: &UploadId, epoch: u64) -> Vec<u8> {
    retire_key(RetireMode::Bytes, &teardown_token(id, epoch))
}

/// Every `retire:` key naming `id`'s session, in either mode.
fn retire_keys_of(meta: &Meta, id: &UploadId) -> Vec<Vec<u8>> {
    let modes = RetireMode::ALL.iter();
    modes
        .flat_map(|&mode| meta.keys_under(&retire_session_range(mode, id)))
        .collect()
}

/// A fenced session: `Aborting@E+1` byte for byte, and `{session, all}` under
/// `retire:bytes:s:<id>:<E>`, decoding against that key — both put by ONE applied commit.
fn assert_fenced(meta: &Meta, id: &UploadId, epoch: u64, report: &RestoreReport) {
    let key = mpu_key(id);
    let (fenced, n) = (meta.value(&key), name(&key));
    assert_eq!(fenced, Some(aborting(epoch + 1)), "{n}: {report:?}");
    let obligation = teardown_key(id, epoch);
    let value = meta.value(&obligation).expect("an obligation");
    let (mode, token, payload) = decode_retire_obligation(&obligation, &value)
        .unwrap_or_else(|fault| panic!("the obligation must decode against its key: {fault}"));
    let expected = (RetireMode::Bytes, teardown_token(id, epoch));
    assert_eq!((mode, token), expected);
    assert!(
        payload.session() && payload.parts() == Some(&PartScope::All),
        "it must owe the staged residue AND every part, or the parts have no deleter: {payload:?}"
    );
    assert_eq!(retire_keys_of(meta, id), vec![obligation.clone()]);
    let carrying: Vec<WriteBatch> = meta.applied.lock().unwrap().clone();
    let carrying: Vec<&WriteBatch> = carrying
        .iter()
        .filter(|batch| puts(batch, &key) || puts(batch, &obligation))
        .collect();
    assert!(
        matches!(carrying.as_slice(), [one] if puts(one, &key) && puts(one, &obligation)),
        "the fence and its obligation must land in ONE commit, not {}",
        carrying.len()
    );
}

/// The report's `Debug` from `sessions_unsettled` on, or empty on the base (no such field).
fn unsettled_debug(report: &RestoreReport) -> String {
    let rendered = format!("{report:?}");
    let rest = rendered.split_once("sessions_unsettled: ");
    rest.map(|(_, rest)| rest.to_owned()).unwrap_or_default()
}

fn names_unsettled(report: &RestoreReport, id: &UploadId) -> bool {
    unsettled_debug(report).contains(&format!("{:?}", name(&mpu_key(id))))
}

fn fenced_count(report: &RestoreReport, n: usize) -> bool {
    format!("{report:?}").contains(&format!("sessions_fenced: {n},"))
}

// ---- the audit seam -----------------------------------------------------------------------------

/// One restore audit event, every field by name, and the thread that emitted it.
struct AuditEvent {
    thread: ThreadId,
    fields: Vec<(String, String)>,
}

fn audit_log() -> &'static Mutex<Vec<AuditEvent>> {
    static LOG: OnceLock<Mutex<Vec<AuditEvent>>> = OnceLock::new();
    LOG.get_or_init(Mutex::default)
}

/// Install the capture once for the whole binary, before any pass runs.
fn capture_audit() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let registry = tracing_subscriber::registry().with(AuditCapture);
        registry.try_init().expect("the only global subscriber");
    });
}

struct AuditCapture;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != "wyrd.custodian.restore.audit" {
            return;
        }
        let mut fields = FieldText(Vec::new());
        event.record(&mut fields);
        let thread = std::thread::current().id();
        let fields = fields.0;
        audit_log()
            .lock()
            .unwrap()
            .push(AuditEvent { thread, fields });
    }
}

struct FieldText(Vec<(String, String)>);

impl Visit for FieldText {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}

/// This thread's restore audit events with `action`, as `field -> value`.
fn audited(action: &str) -> Vec<HashMap<String, String>> {
    let thread = std::thread::current().id();
    let log = audit_log().lock().unwrap();
    let events = log.iter().filter(|event| event.thread == thread);
    let events = events.map(|event| event.fields.iter().cloned().collect::<HashMap<_, _>>());
    events
        .filter(|fields| fields.get("action").map(String::as_str) == Some(action))
        .collect()
}

/// The `cause` this thread's `session-unsettled` lines give `id`'s session, one per line.
fn unsettled_causes(id: &UploadId) -> Vec<String> {
    let session = name(&mpu_key(id));
    let lines = audited("session-unsettled").into_iter();
    let lines = lines.filter(|fields| fields.get("session") == Some(&session));
    lines.map(|fields| fields["cause"].clone()).collect()
}

// ---- (F) an Open session is fenced, whole -------------------------------------------------------

/// **(F)** An `Open@3` session with two committed parts ends `Aborting@4`, and the same commit
/// installs `retire:bytes:s:<id>:3` = `{session, all}`, decoding against its key. `Aborting` and
/// `Completed` sessions are left byte-identical and not counted. A fence is work, not a human's.
///
/// Base: still `Open@3`.
#[tokio::test]
async fn an_open_session_is_fenced_whole() {
    capture_audit();
    let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
    let id = upload("f1");
    meta.seed(mpu_key(&id), open(EPOCH));
    for (n, chunk) in [(1, 0xF11), (2, 0xF12)] {
        seed_part(&meta, &id, n, chunk_ref(chunk, EcScheme::None, &[n.into()]));
        place(&d, n.into(), frag(chunk, 0));
    }
    let settled = [
        (upload("f2"), aborting(EPOCH)),
        (upload("f3"), completed(EPOCH)),
    ];
    for (id, value) in &settled {
        meta.seed(mpu_key(id), value.clone());
    }
    let parts = meta.keys_under(b"part:");

    let report = restore_pass(&meta, &d).await.expect("the pass runs");

    assert_fenced(&meta, &id, EPOCH, &report);
    assert_eq!(
        meta.keys_under(b"part:"),
        parts,
        "the fence deletes nothing itself"
    );
    for (id, value) in &settled {
        assert_eq!(meta.value(&mpu_key(id)).as_ref(), Some(value), "{report:?}");
        assert!(retire_keys_of(&meta, id).is_empty(), "{report:?}");
    }
    assert!(fenced_count(&report, 1), "{report:?}");
    assert!(!report.is_clean() && !report.needs_human(), "{report:?}");
    assert_eq!(audited("session-fenced").len(), 1);
}

// ---- (F-atomic) the fence lands whole or not at all ---------------------------------------------

/// **(F-atomic)** Two runs: every commit putting the session's key fails, then every commit
/// putting the obligation's key fails — each with an unknown outcome. After either, neither write
/// is present and the pass is `Err`: an unknown outcome is never read as a `Conflict` or a fence.
///
/// Base: `Ok` (the pass writes no fence).
#[tokio::test]
async fn a_failed_fence_commit_leaves_neither_write() {
    capture_audit();
    for (pair, at_session) in [("a1", true), ("a2", false)] {
        let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
        let id = upload(pair);
        meta.seed(mpu_key(&id), open(EPOCH));
        seed_part(&meta, &id, 1, chunk_ref(0xA71, EcScheme::None, &[1]));
        let failed = match at_session {
            true => mpu_key(&id),
            false => teardown_key(&id, EPOCH),
        };
        meta.fail_commits_putting(&failed);

        let outcome = restore_pass(&meta, &d).await;

        let untouched = meta.value(&mpu_key(&id)) == Some(open(EPOCH));
        let whole = untouched && retire_keys_of(&meta, &id).is_empty();
        assert!(whole, "{pair}: half a fence");
        let err = outcome.expect_err("an unknown fence outcome must fail the pass");
        let top: &(dyn std::error::Error + 'static) = err.as_ref();
        let mut chain = std::iter::successors(Some(top), |at| at.source());
        let unknown = chain.any(|at| at.is::<CommitUnknownResult>());
        assert!(unknown, "{pair}: not the unknown result: {err}");
    }
}

// ---- (F-race) a session that changes under the pass is not fenced blind ------------------------

/// **(F-race)** A Complete fence (`Open@3` → `Completing@4`) lands on `b1` between the pass's read
/// and the fence's commit. The fence writes nothing for `b1`, which is named for a human as
/// changed (not as a taken key); `b2` is still fenced, and the pass is `Ok`.
///
/// Base: the fence never commits, so the race never lands and `b2` is not fenced.
#[tokio::test]
async fn a_session_that_changes_under_the_pass_is_not_fenced_blind() {
    capture_audit();
    let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
    let (raced, other) = (upload("b1"), upload("b2"));
    for id in [&raced, &other] {
        meta.seed(mpu_key(id), open(EPOCH));
    }
    let complete_fence = WriteBatch::new()
        .require(mpu_key(&raced), open(EPOCH))
        .put(mpu_key(&raced), completing(EPOCH + 1));
    meta.before_commit_putting(&mpu_key(&raced), complete_fence);

    let report = restore_pass(&meta, &d).await.expect("named, never an Err");

    let landed = meta.before.lock().unwrap()[0].2;
    let race = "the race was not exercised";
    assert_eq!(landed, Some(CommitOutcome::Committed), "{race}");
    let raced_value = meta.value(&mpu_key(&raced));
    assert_eq!(raced_value, Some(completing(EPOCH + 1)), "{report:?}");
    assert!(retire_keys_of(&meta, &raced).is_empty(), "{report:?}");
    assert!(
        report.needs_human() && names_unsettled(&report, &raced),
        "{report:?}"
    );
    let causes = unsettled_causes(&raced);
    let changed = |cause: &String| cause.contains("changed") && !cause.contains("taken");
    assert!(
        matches!(causes.as_slice(), [cause] if changed(cause)),
        "{causes:?}"
    );
    assert_fenced(&meta, &other, EPOCH, &report);
    assert!(
        fenced_count(&report, 1) && !names_unsettled(&report, &other),
        "{report:?}"
    );
}

// ---- (F-collision) the fence never overwrites an obligation -------------------------------------

/// **(F-collision)** `retire:bytes:s:<c1>:3` already holds a `{session}` obligation — decodable,
/// and not what the fence writes. Both records stay byte-identical, no commit writes for `c1`, and
/// `c1` is named once for a human with a "key taken" cause; `c2` is still fenced; `Ok`.
///
/// Base: the fence never commits, so `c1` is not named and `c2` is not fenced.
#[tokio::test]
async fn the_fence_never_overwrites_an_obligation() {
    capture_audit();
    let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
    let (collided, other) = (upload("c1"), upload("c2"));
    for id in [&collided, &other] {
        meta.seed(mpu_key(id), open(EPOCH));
    }
    let taken = teardown_key(&collided, EPOCH);
    let foreign = Bytes::from_static(b"{\"session\":true}");
    let (_, _, payload) = decode_retire_obligation(&taken, &foreign).expect("it decodes");
    assert!(
        payload.parts().is_none(),
        "it differs from the fence's `{{session, all}}`"
    );
    meta.seed(taken.clone(), foreign.clone());

    let report = restore_pass(&meta, &d).await.expect("named, never an Err");

    let session = meta.value(&mpu_key(&collided));
    assert_eq!(session, Some(open(EPOCH)), "{report:?}");
    let obligation = meta.value(&taken);
    assert_eq!(obligation, Some(foreign), "an obligation overwritten");
    assert_eq!(retire_keys_of(&meta, &collided), vec![taken.clone()]);
    let applied = meta.applied.lock().unwrap().clone();
    let keys = applied
        .iter()
        .flat_map(|b| b.puts.iter().map(|(k, _)| k).chain(&b.deletes));
    let touched: Vec<String> = keys
        .map(|k| name(k))
        .filter(|k| k.contains(collided.as_str()))
        .collect();
    assert!(
        touched.is_empty(),
        "a commit wrote for the collided session: {touched:?}"
    );
    assert!(
        report.needs_human() && names_unsettled(&report, &collided),
        "{report:?}"
    );
    let causes = unsettled_causes(&collided);
    let taken_key = |cause: &String| {
        cause.contains("taken") && cause.contains(&name(&taken)) && !cause.contains("changed")
    };
    assert!(
        matches!(causes.as_slice(), [cause] if taken_key(cause)),
        "classified once, as a taken key: {causes:?}"
    );
    assert_fenced(&meta, &other, EPOCH, &report);
    assert!(fenced_count(&report, 1), "{report:?}");
}

// ---- (H) what this pass cannot fence is named --------------------------------------------------

/// Leg H's sessions — (i) undecodable, its part's fragment on server 1; (ii) `Open@u64::MAX`;
/// (iii) `Completing` — then an `Open@3` control the fence does fence, in that order, plus a stray
/// fragment on server 3 that the mark half does mark.
fn seed_unfenceable(meta: &Meta, d: &[Disk; 4], digit: char) -> [UploadId; 4] {
    let id = |n: u8| upload(&format!("{digit}{n}"));
    let undecodable = b"not a session record";
    assert!(decode_session_record(undecodable).is_err());
    meta.seed(mpu_key(&id(1)), Bytes::from_static(undecodable));
    seed_part(meta, &id(1), 1, chunk_ref(0xD11, EcScheme::None, &[1]));
    place(d, 1, frag(0xD11, 0));
    meta.seed(mpu_key(&id(2)), open(u64::MAX));
    meta.seed(mpu_key(&id(3)), completing(EPOCH));
    meta.seed(mpu_key(&id(4)), open(EPOCH));
    place(d, 3, frag(0xD1F, 0));
    [id(1), id(2), id(3), id(4)]
}

/// **(H)** An undecodable session, an `Open` one at `u64::MAX` and a `Completing` one are each
/// left byte-identical with no `retire:` key, and named for a human in the report and on the
/// audit seam. The undecodable one's part stays protected, the control is fenced, the stray marked.
///
/// Base: `needs_human()` is false.
#[tokio::test]
async fn what_the_pass_cannot_fence_is_named_never_passed_off_as_done() {
    capture_audit();
    let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
    let [i, ii, iii, control] = seed_unfenceable(&meta, &d, 'd');
    let before: Vec<_> = [&i, &ii, &iii].map(|id| meta.value(&mpu_key(id))).into();

    let report = restore_pass(&meta, &d).await.expect("named, never an Err");

    for (id, before) in [&i, &ii, &iii].into_iter().zip(before) {
        let n = name(&mpu_key(id));
        assert_eq!(meta.value(&mpu_key(id)), before, "{n}");
        assert!(retire_keys_of(&meta, id).is_empty(), "{n}");
        assert!(names_unsettled(&report, id), "{n}: {report:?}");
        assert_eq!(unsettled_causes(id).len(), 1, "{n}");
    }
    assert!(
        report.needs_human() && report.unresolvable.is_empty(),
        "{report:?}"
    );
    assert!(
        meta.value(&orphan_key(1, frag(0xD11, 0))).is_none(),
        "the part must stay protected"
    );
    assert!(
        meta.value(&orphan_key(3, frag(0xD1F, 0))).is_some(),
        "the stray must be marked"
    );
    assert_fenced(&meta, &control, EPOCH, &report);
    assert!(
        fenced_count(&report, 1) && !names_unsettled(&report, &control),
        "{report:?}"
    );
}

// ---- (K) a second pass is idempotent ------------------------------------------------------------

/// **(K)** Over leg H's store, an `Aborting` session and an untrusted part on the fenced session:
/// a second pass leaves the WHOLE store byte-identical — no second obligation, no mark re-stamped
/// — and names the same sessions and the same untrusted record again (#664 iteration 1 dropped an
/// already-`Aborting` session's findings).
///
/// Base: the first pass fences and names nothing.
#[tokio::test]
async fn a_second_pass_is_idempotent() {
    capture_audit();
    let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
    let [i, ii, iii, control] = seed_unfenceable(&meta, &d, 'e');
    meta.seed(mpu_key(&upload("e5")), aborting(EPOCH));
    seed_part(&meta, &control, 1, chunk_ref(0xE51, RS_2_1, &[0]));
    place(&d, 0, frag(0xE51, 0));

    let first = restore_pass(&meta, &d).await.expect("the first pass runs");
    let after_first = meta.snapshot();
    let second = restore_pass(&meta, &d).await.expect("the second pass runs");

    assert_fenced(&meta, &control, EPOCH, &first);
    assert!(
        fenced_count(&first, 1) && fenced_count(&second, 0),
        "{first:?}\n{second:?}"
    );
    assert!(
        meta.snapshot() == after_first,
        "the second pass changed the store"
    );
    for id in [&i, &ii, &iii] {
        let named = names_unsettled(&first, id) && names_unsettled(&second, id);
        assert!(named, "{}: {first:?}\n{second:?}", name(&mpu_key(id)));
    }
    assert_eq!(unsettled_debug(&first), unsettled_debug(&second));
    let untrusted = vec![name(&part_key(&control, PartNumber::new(1).unwrap()))];
    assert_eq!(
        (&first.staged_untrusted, &second.staged_untrusted),
        (&untrusted, &untrusted)
    );
}

// ---- (P3) a fence fault never hides the pass's verdicts -----------------------------------------

/// **(P3)** A dangling committed chunk, an under-replicated one (two of `RS(2,1)`'s three at the
/// placement), and an `Open` session whose fence commit fails. The pass is `Err`, and the audit
/// seam still carries the `dangling` line and the summary — `under_replicated` 1, its only record
/// — which does not read "complete".
///
/// Base: the pass returns `Ok`.
#[tokio::test]
async fn a_fence_fault_never_hides_the_pass_verdicts() {
    capture_audit();
    let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
    commit_object(&meta, 1, chunk_ref(0x9A1, EcScheme::None, &[0]));
    commit_object(&meta, 2, chunk_ref(0x9A2, RS_2_1, &[0, 1, 2]));
    place(&d, 0, frag(0x9A2, 0));
    place(&d, 1, frag(0x9A2, 1));
    let id = upload("9a");
    meta.seed(mpu_key(&id), open(EPOCH));
    meta.fail_commits_putting(&mpu_key(&id));

    let outcome = restore_pass(&meta, &d).await;

    assert!(outcome.is_err(), "{:?}", outcome.ok());
    let dangling = audited("dangling");
    assert!(
        dangling
            .iter()
            .any(|line| line["chunk"] == chunk_hex(0x9A1)),
        "{dangling:?}"
    );
    let summaries = audited("summary");
    let [summary] = summaries.as_slice() else {
        panic!("one summary must survive the fence fault: {summaries:?}");
    };
    assert_eq!(
        (&*summary["under_replicated"], &*summary["dangling"]),
        ("1", "1")
    );
    let message = &summary["message"];
    assert!(
        message.contains("post-restore reconciliation")
            && !message.contains("reconciliation complete"),
        "{message}"
    );
    assert_eq!(meta.value(&mpu_key(&id)), Some(open(EPOCH)));
    assert!(retire_keys_of(&meta, &id).is_empty());
}

/// **(P3)** The same fault over a store with nothing else wrong in it: the summary must not
/// certify the run `clean`, nor leave it to no human, while an `Open` session may still be live.
///
/// Base: the pass returns `Ok`.
#[tokio::test]
async fn a_fence_fault_on_an_otherwise_clean_store_is_never_certified_clean() {
    capture_audit();
    let (meta, d) = (Meta::new(), <[Disk; 4]>::default());
    let id = upload("9b");
    meta.seed(mpu_key(&id), open(EPOCH));
    meta.fail_commits_putting(&mpu_key(&id));

    let outcome = restore_pass(&meta, &d).await;

    assert!(outcome.is_err(), "{:?}", outcome.ok());
    let summaries = audited("summary");
    let [summary] = summaries.as_slice() else {
        panic!("one summary must survive the fence fault: {summaries:?}");
    };
    let verdict = (&*summary["clean"], &*summary["needs_human"]);
    assert_eq!(verdict, ("false", "true"), "{summary:?}");
}

// ---- (Paging) sessions across several pages are all fenced --------------------------------------

/// **(Paging)** Five `Open` sessions and an `Aborting` one, at a scan cap of 2: every `Open` one is
/// fenced. A fence that read the listing in one `scan` fails; one that stopped after a page fences
/// two. Base: none fenced.
#[tokio::test]
async fn sessions_listed_across_pages_are_all_fenced() {
    capture_audit();
    let (meta, d) = (Meta::with_cap(2), <[Disk; 4]>::default());
    let ids: Vec<UploadId> = (0..5).map(|n| upload(&format!("5{n}"))).collect();
    for id in &ids {
        meta.seed(mpu_key(id), open(EPOCH));
    }
    meta.seed(mpu_key(&upload("55")), aborting(EPOCH));
    assert!(
        meta.scan(MPU_PREFIX).await.is_err(),
        "the listing must span pages"
    );

    let report = restore_pass(&meta, &d).await.expect("the pass runs");

    for id in &ids {
        assert_fenced(&meta, id, EPOCH, &report);
    }
    assert_eq!(meta.value(&mpu_key(&upload("55"))), Some(aborting(EPOCH)));
    assert!(fenced_count(&report, 5), "{report:?}");
}
