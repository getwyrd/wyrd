//! Issue #842 (809.4): the post-restore pass fences every `Completing` session, one batch moving it
//! to `Aborting@E+1` with `retire:bytes:{session, all}` and `retire:records:{seg:<nonce>:<E>}`
//! (`0016:665`, X57 `0016:880`). Each leg drives the production `reconcile_after_restore` over
//! this file's doubles; new report fields are read through `Debug`, so the base builds and fails.

// deferred: #843 — seeded Tier-0 DST coverage of this fence (809.5); these legs are Tokio-only.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Mutex;
use std::thread::ThreadId;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_core::metadata::{self as md, ChunkRef, EcScheme, SegmentGroup, SegmentRecord};
use wyrd_core::multipart::{self as mp, PartScope, RetireMode, RetirePayload, UploadId};
use wyrd_custodian::{reconcile_after_restore, ExpiredPendingPolicy, GcContext, RestoreReport};
use wyrd_traits::{
    page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome,
    CommitUnknownResult, DServerId, FragmentId, Health, MetadataStore, PageStart, Result,
    ScanCapExceeded, ScanPage, WriteBatch, SCAN_CAP,
};

const EPOCH: u64 = 3;
const JUNK: &[u8] = b"not a segment";
type Kv = Vec<(Vec<u8>, Bytes)>;

/// An in-memory `MetadataStore` paging through the seam's helpers. It fails every commit putting a
/// `failing` key (unknown outcome), refuses as FoundationDB does (`2103 value_too_large`) any batch
/// putting a value past `MAX_VALUE_BYTES`, and logs every batch it applied.
#[derive(Default)]
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    failing: Mutex<Vec<Vec<u8>>>,
    applied: Mutex<Vec<WriteBatch>>,
}

impl Meta {
    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key.into(), value.into());
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    fn range(&self, lower: Bound<&[u8]>, prefix: &[u8], limit: usize) -> Kv {
        let kv = self.kv.lock().unwrap();
        let hits = kv.range::<[u8], _>((lower, Bound::Unbounded));
        let hits = hits.take_while(|(key, _)| key.starts_with(prefix));
        hits.take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn keys_under(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let hits = self.range(Bound::Included(prefix), prefix, usize::MAX);
        hits.into_iter().map(|(key, _)| key).collect()
    }

    fn snapshot(&self) -> BTreeMap<Vec<u8>, Bytes> {
        self.kv.lock().unwrap().clone()
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

    async fn scan(&self, prefix: &[u8]) -> Result<Kv> {
        let hits = self.range(Bound::Included(prefix), prefix, usize::MAX);
        if hits.len() > SCAN_CAP {
            let (cap, prefix) = (SCAN_CAP, prefix.to_vec());
            return Err(BoxError::from(ScanCapExceeded { cap, prefix }));
        }
        Ok(hits)
    }

    async fn scan_page(&self, prefix: &[u8], after: Option<&[u8]>, n: usize) -> Result<ScanPage> {
        let limit = page_limit(n, SCAN_CAP, prefix)?;
        let lower = match page_start(prefix, after) {
            PageStart::After(cursor) => Bound::Excluded(cursor),
            PageStart::Prefix => Bound::Included(prefix),
            PageStart::PastPrefix => return Ok((Vec::new(), None)),
        };
        let items = self.range(lower, prefix, limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        if self.failing.lock().unwrap().iter().any(|k| puts(&batch, k)) {
            let (backend, code) = ("fence-double", None);
            let (detail, may_still_commit) = ("injected".to_owned(), false);
            let unknown = CommitUnknownResult {
                backend,
                code,
                detail,
                may_still_commit,
            };
            return Err(BoxError::from(unknown));
        }
        let largest = batch.puts.iter().map(|(_, value)| value.len()).max();
        if largest.is_some_and(|len| len > md::MAX_VALUE_BYTES) {
            return Err(BoxError::from(format!("value_too_large: {largest:?}")));
        }
        let mut kv = self.kv.lock().unwrap();
        let mut preconditions = batch.preconditions.iter();
        if preconditions.any(|pre| kv.get(&pre.key) != pre.expected.as_ref()) {
            return Ok(CommitOutcome::Conflict);
        }
        for key in &batch.deletes {
            kv.remove(key);
        }
        for (key, value) in &batch.puts {
            kv.insert(key.clone(), value.clone());
        }
        self.applied.lock().unwrap().push(batch);
        Ok(CommitOutcome::Committed)
    }
}

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

async fn restore_pass(meta: &Meta, d: &[Disk; 4]) -> Result<RestoreReport> {
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let (fleet, grace_window_millis) = (&fleet, 50);
    let expired_pending = ExpiredPendingPolicy::Defer;
    let ctx = GcContext {
        meta,
        fleet,
        grace_window_millis,
        expired_pending,
    };
    reconcile_after_restore(&ctx, 10_000).await
}

fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).unwrap()
}

/// `id`'s segment nonce: its hex reversed (`a7…` to `7a…`), so never the upload id itself.
fn nonce(id: &UploadId) -> String {
    let nonce: String = id.as_str().chars().rev().collect();
    assert_ne!(nonce, id.as_str());
    nonce
}

fn name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).unwrap()
}

/// `id`'s session in `state` at `epoch`, in the codec's own spelling, checked byte for byte.
fn session(id: &UploadId, state: &str, epoch: u64) -> Bytes {
    let bytes = format!(
        "{{\"parent\":42,\"object\":\"o\",\"created_at_millis\":100,\"clock_source\":\"wall\",\
         \"segment_nonce\":\"{}\",\"epoch\":{epoch},\"attempts\":1,\"state\":{state}}}",
        nonce(id)
    );
    let record = mp::decode_session_record(bytes.as_bytes()).expect(state);
    assert_eq!(md::encode(&record), bytes.as_bytes());
    Bytes::from(bytes)
}

fn open(id: &UploadId) -> Bytes {
    session(id, "{\"kind\":\"Open\"}", EPOCH)
}

fn aborting(id: &UploadId) -> Bytes {
    session(id, "{\"kind\":\"Aborting\"}", EPOCH + 1)
}

fn completing(id: &UploadId, epoch: u64, written: u32) -> Bytes {
    let state = format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":{written},\
         \"publish_target\":{{\"parent\":42,\"name\":\"o\",\"epoch\":{epoch}}}}}"
    );
    session(id, &state, epoch)
}

fn group(id: &UploadId) -> SegmentGroup {
    SegmentGroup::new(nonce(id), EPOCH).unwrap()
}

fn chunk_ref(id: ChunkId) -> ChunkRef {
    let (scheme, len, placement) = (EcScheme::None, 5, vec![1]);
    ChunkRef {
        id,
        scheme,
        len,
        placement,
    }
}

/// Seed part `n` of `id` naming `chunk`, in the part codec's canonical spelling.
fn seed_part(meta: &Meta, id: &UploadId, n: u32, chunk: &ChunkRef) {
    let chunk = name(&md::encode(chunk));
    let value = format!(
        "{{\"chunks\":[{chunk}],\"len\":5,\"digest\":\"{}\",\"committed_at_millis\":800,\
         \"session_epoch\":{EPOCH}}}",
        "ef".repeat(32)
    );
    let n = mp::PartNumber::new(n).unwrap();
    meta.seed(mp::part_key(id, n), value.into_bytes());
}

fn seed_segment(meta: &Meta, id: &UploadId, index: u32, chunk: ChunkRef) -> Vec<u8> {
    let record = SegmentRecord::new(vec![chunk], u64::from(index) * 5).unwrap();
    let key = md::seg_key(&group(id), index).unwrap();
    meta.seed(key.clone(), md::encode(&record));
    key
}

/// A `Completing@3` session that wrote two segments over its two parts; the segment keys.
fn seed_attempt(meta: &Meta, id: &UploadId, base: ChunkId) -> Vec<Vec<u8>> {
    meta.seed(mp::mpu_key(id), completing(id, EPOCH, 2));
    let segments = (0..2_u32).map(|n| {
        let chunk = chunk_ref(base + ChunkId::from(n));
        seed_part(meta, id, n + 1, &chunk);
        seed_segment(meta, id, n, chunk)
    });
    segments.collect()
}

fn token(id: &UploadId) -> mp::RetireToken {
    let (upload_id, epoch, part) = (id.clone(), EPOCH, None);
    mp::RetireToken::Session {
        upload_id,
        epoch,
        part,
    }
}

/// The key of `id`'s obligation in `mode` from a fence at [`EPOCH`].
fn owed(mode: RetireMode, id: &UploadId) -> Vec<u8> {
    mp::retire_key(mode, &token(id))
}

/// Every `retire:` key naming `id`'s session, bytes first.
fn retire_keys(meta: &Meta, id: &UploadId) -> Vec<Vec<u8>> {
    let ranges = RetireMode::ALL.map(|mode| mp::retire_session_range(mode, id));
    ranges.iter().flat_map(|r| meta.keys_under(r)).collect()
}

fn obligation(meta: &Meta, mode: RetireMode, id: &UploadId) -> RetirePayload {
    let key = owed(mode, id);
    let value = meta.value(&key).expect("an obligation");
    let (found, at, payload) = mp::decode_retire_obligation(&key, &value).unwrap();
    assert_eq!((found, at), (mode, token(id)));
    payload
}

/// `id`'s session at `Aborting@4` byte for byte, owing `{session, all}` as its only bytes key.
fn assert_aborted(meta: &Meta, id: &UploadId, report: &RestoreReport) {
    let fenced = meta.value(&mp::mpu_key(id));
    assert_eq!(fenced, Some(aborting(id)), "{}: {report:?}", id.as_str());
    let bytes = obligation(meta, RetireMode::Bytes, id);
    let all = bytes.session() && bytes.parts() == Some(&PartScope::All);
    assert!(all, "it must owe the residue and every part: {bytes:?}");
}

/// A fenced `Completing@3` session: [`assert_aborted`], `{seg: (nonce, 3)}` decoding against its
/// key, no other `retire:` key — and all three writes put by ONE applied commit.
fn assert_fenced(meta: &Meta, id: &UploadId, report: &RestoreReport) {
    assert_aborted(meta, id, report);
    let records = obligation(meta, RetireMode::Records, id);
    let only_seg = !records.session() && records.parts().is_none();
    assert_eq!(records.segments(), Some(&group(id)), "{records:?}");
    let keys = RetireMode::ALL.map(|mode| owed(mode, id));
    assert!(only_seg && retire_keys(meta, id) == keys, "{records:?}");
    let all = [mp::mpu_key(id), keys[0].clone(), keys[1].clone()];
    let applied = meta.applied.lock().unwrap();
    let carrying = applied.iter().filter(|b| all.iter().any(|k| puts(b, k)));
    let carrying: Vec<&WriteBatch> = carrying.collect();
    let whole = |b: &WriteBatch| all.iter().all(|k| puts(b, k));
    let one = matches!(carrying[..], [one] if whole(one));
    assert!(one, "the fence must be ONE commit");
}

fn assert_open_fenced(meta: &Meta, id: &UploadId, report: &RestoreReport) {
    assert_aborted(meta, id, report);
    assert_eq!(retire_keys(meta, id), [owed(RetireMode::Bytes, id)]);
}

/// Whether the report names `key` (quoted by `Debug`), as it does only for a human.
fn names(report: &RestoreReport, key: &[u8]) -> bool {
    format!("{report:?}").contains(&format!("{:?}", name(key)))
}

static AUDIT: Mutex<Vec<(ThreadId, HashMap<String, String>)>> = Mutex::new(Vec::new());

struct AuditCapture;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() == "wyrd.custodian.restore.audit" {
            let mut fields = FieldText(HashMap::new());
            event.record(&mut fields);
            let event = (std::thread::current().id(), fields.0);
            AUDIT.lock().unwrap().push(event);
        }
    }
}

struct FieldText(HashMap<String, String>);

impl Visit for FieldText {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

fn capture_audit() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let capture = tracing_subscriber::registry().with(AuditCapture);
        capture.try_init().expect("this binary's only subscriber");
    });
}

/// This thread's restore audit events with `action`.
fn audited(action: &str) -> Vec<HashMap<String, String>> {
    let thread = std::thread::current().id();
    let log = AUDIT.lock().unwrap();
    let mine = log.iter().filter(|(at, _)| *at == thread);
    let mine = mine.map(|(_, fields)| fields);
    let mine = mine.filter(|fields| fields.get("action").is_some_and(|a| a == action));
    mine.cloned().collect()
}

/// How often this thread's audit log names `id`'s session at `record`, its fault in words.
fn audit_names(id: &UploadId, record: &[u8]) -> usize {
    let (session, record) = (name(&mp::mpu_key(id)), name(record));
    let events = audited("session-segments-unaccounted");
    let named = events.iter();
    let named = named.filter(|e| e["session"] == session && e["record"] == record);
    named.filter(|e| !e["fault"].is_empty()).count()
}

/// **(G)** A `Completing@3` session ends `Aborting@4`, ONE commit installing `{session, all}` and
/// `{seg: (nonce, 3)}`, even when its cursor says 0 (`0016:665`'s "1 put"). Nothing is deleted; a
/// clean fence is not a human's.
#[tokio::test]
async fn a_completing_session_is_fenced_with_its_segments_deleter() {
    let (meta, d) = (Meta::default(), <[Disk; 4]>::default());
    let (written, cursorless) = (upload("a1"), upload("a2"));
    let segments = seed_attempt(&meta, &written, 0xA10);
    meta.seed(mp::mpu_key(&cursorless), completing(&cursorless, EPOCH, 0));
    seed_part(&meta, &cursorless, 1, &chunk_ref(0xA20));
    let before = (meta.keys_under(b"part:"), segments);

    let report = restore_pass(&meta, &d).await.unwrap();

    for id in [&written, &cursorless] {
        assert_fenced(&meta, id, &report);
        assert!(!names(&report, &mp::mpu_key(id)), "{report:?}");
    }
    let after = (meta.keys_under(b"part:"), meta.keys_under(b"seg:"));
    assert_eq!(after, before, "the fence deletes nothing");
    assert_eq!(report.sessions_fenced, 2, "{report:?}");
    assert!(!report.is_clean() && !report.needs_human(), "{report:?}");
}

/// **(G-atomic)** Failing (unknown outcome) the commit putting the session, then either
/// obligation: none of the three writes lands, and the pass is that `Err`.
#[tokio::test]
async fn a_failed_completing_fence_commit_leaves_none_of_its_writes() {
    for at in 0..3 {
        let (meta, d) = (Meta::default(), <[Disk; 4]>::default());
        let id = upload(&format!("b{at}"));
        seed_attempt(&meta, &id, 0xB10);
        let keys = RetireMode::ALL.map(|mode| owed(mode, &id));
        let failing = [mp::mpu_key(&id), keys[0].clone(), keys[1].clone()];
        meta.failing.lock().unwrap().push(failing[at].clone());

        let err = restore_pass(&meta, &d).await.expect_err("must fail");

        let untouched = meta.value(&mp::mpu_key(&id)) == Some(completing(&id, EPOCH, 2));
        assert!(untouched && retire_keys(&meta, &id).is_empty(), "{at}");
        let top: &(dyn std::error::Error + 'static) = err.as_ref();
        let mut chain = std::iter::successors(Some(top), |at| at.source());
        assert!(chain.any(|at| at.is::<CommitUnknownResult>()), "{err}");
    }
}

/// A decodable `retire:records:` value owing a segment group no session here has.
const FOREIGN: &[u8] = b"{\"seg\":{\"nonce\":\"9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f\",\"epoch\":3}}";

/// **(G-collision)** `c1`'s `retire:records:s:<c1>:3` already holds a decodable `{seg}` naming a
/// DIFFERENT group, `c4`'s `retire:bytes:s:<c4>:3` a `{session}`: each stays byte-identical, none
/// of that session's writes lands, and it is named "key taken". `c2` (uncollided) and `c3` (`Open`)
/// are fenced.
#[tokio::test]
async fn neither_obligation_overwrites_one_already_there() {
    let (meta, d) = (Meta::default(), <[Disk; 4]>::default());
    let [records, clean, control, bytes] = ["c1", "c2", "c3", "c4"].map(upload);
    seed_attempt(&meta, &clean, 0xC20);
    meta.seed(mp::mpu_key(&control), open(&control));
    let session_only: &[u8] = b"{\"session\":true}";
    let taken = [
        (records, RetireMode::Records, FOREIGN, 0xC10),
        (bytes, RetireMode::Bytes, session_only, 0xC40),
    ];
    for (id, mode, value, base) in &taken {
        seed_attempt(&meta, id, *base);
        let key = owed(*mode, id);
        let (_, _, payload) = mp::decode_retire_obligation(&key, value).unwrap();
        assert_ne!(payload.segments(), Some(&group(id)));
        meta.seed(key, value.to_vec());
    }

    let report = restore_pass(&meta, &d).await.unwrap();

    for (id, mode, value, _) in &taken {
        let key = owed(*mode, id);
        assert_eq!(meta.value(&key).as_deref(), Some(*value), "overwritten");
        assert_eq!(meta.value(&mp::mpu_key(id)), Some(completing(id, EPOCH, 2)));
        assert_eq!(retire_keys(&meta, id), std::slice::from_ref(&key));
        let (session, key) = (name(&mp::mpu_key(id)), name(&key));
        let cause = format!("{session:?}, cause: ObligationKeyTaken {{ key: {key:?} }}");
        assert!(format!("{report:?}").contains(&cause), "{report:?}");
    }
    assert_fenced(&meta, &clean, &report);
    assert_open_fenced(&meta, &control, &report);
    let fenced_clean = !names(&report, &mp::mpu_key(&clean));
    assert!(fenced_clean && report.sessions_fenced == 2, "{report:?}");
}

/// **(G-sparse)** Over a store refusing any value past `MAX_VALUE_BYTES`, a `Completing@3` session
/// with parts 1, 3, …, 19,999 (128,916 bytes as `{session, parts}`) is fenced as in G, every value
/// fits, and an `Open` session after it is fenced.
#[tokio::test]
async fn a_sparse_completing_session_is_fenced_inside_the_value_ceiling() {
    let (meta, d) = (Meta::default(), <[Disk; 4]>::default());
    let probe = vec![0; md::MAX_VALUE_BYTES + 1];
    let probe = WriteBatch::new().put(b"probe".to_vec(), probe);
    assert!(meta.commit(probe).await.is_err(), "the ceiling is enforced");
    let (sparse, control) = (upload("e1"), upload("e2"));
    meta.seed(mp::mpu_key(&sparse), completing(&sparse, EPOCH, 2));
    for n in 0..10_000_u32 {
        let chunk = chunk_ref(0xE_0000 + ChunkId::from(n));
        seed_part(&meta, &sparse, 2 * n + 1, &chunk);
        // Segment 1 names part 19,999's chunk, past the first page of the `part:` range.
        if n == 0 || n == 9_999 {
            seed_segment(&meta, &sparse, n.min(1), chunk);
        }
    }
    meta.seed(mp::mpu_key(&control), open(&control));

    let report = restore_pass(&meta, &d).await.unwrap();

    assert_fenced(&meta, &sparse, &report);
    assert!(!names(&report, &mp::mpu_key(&sparse)), "{report:?}");
    assert_open_fenced(&meta, &control, &report);
    let applied = meta.applied.lock().unwrap();
    let puts = applied.iter().flat_map(|b| &b.puts);
    let largest = puts.map(|(_, value)| value.len()).max();
    assert!(largest.is_some_and(|n| n <= md::MAX_VALUE_BYTES));
}

/// Seed leg H's case as `id`: the record to name beside it, if any, and whether it is fenced.
fn seed_case(meta: &Meta, case: &str, id: &UploadId) -> (Option<Vec<u8>>, bool) {
    match case {
        // (i) a `Completing` record with no nonce, the shape before child-2: it fails decode.
        "i" => {
            let nonce = format!("\"segment_nonce\":\"{}\",", nonce(id));
            let value = name(&completing(id, EPOCH, 1)).replace(&nonce, "");
            assert!(mp::decode_session_record(value.as_bytes()).is_err());
            meta.seed(mp::mpu_key(id), value.into_bytes());
            seed_part(meta, id, 1, &chunk_ref(0xD11));
            (None, false)
        }
        // (ii) segment 1 names part 2's chunk, then one none of the session's parts holds.
        "ii" => {
            let segments = seed_attempt(meta, id, 0xD20);
            let chunks = vec![chunk_ref(0xD21), chunk_ref(0xD2F)];
            let record = SegmentRecord::new(chunks, 5).unwrap();
            meta.seed(segments[1].clone(), md::encode(&record));
            (Some(segments[1].clone()), true)
        }
        // (iv) segment 1's value will not decode.
        "iv" => {
            let segments = seed_attempt(meta, id, 0xD40);
            meta.seed(segments[1].clone(), JUNK);
            (Some(segments[1].clone()), true)
        }
        // (v) a key under the group's range that is not one of its segment keys.
        "v" => {
            let segments = seed_attempt(meta, id, 0xD50);
            let stray = [md::seg_range_prefix(&group(id)), b"7".to_vec()].concat();
            meta.seed(stray.clone(), meta.value(&segments[0]).unwrap());
            (Some(stray), true)
        }
        // (iv) on the range's second page (`gc::STAGED_PAGE`, 512): 512 good segments, a bad one.
        "paged" => {
            seed_attempt(meta, id, 0xD60);
            for n in 2..512 {
                seed_segment(meta, id, n, chunk_ref(0xD60));
            }
            let last = md::seg_key(&group(id), 512).unwrap();
            meta.seed(last.clone(), JUNK);
            (Some(last), true)
        }
        // (vi) a decodable `Completing@u64::MAX`, its target's epoch equal: there is no `E+1`.
        _ => {
            meta.seed(mp::mpu_key(id), completing(id, u64::MAX, 0));
            (None, false)
        }
    }
}

/// **(H)** Each case beside an `Open` control sorting after it: (i) and (vi) are left
/// byte-identical with no `retire:` key, (ii), (iv) and (v) are FENCED; each is named, its bad
/// record too, `needs_human()`, and the control is fenced.
#[tokio::test]
async fn what_cannot_be_fenced_cleanly_is_never_passed_off_as_done() {
    let cases = ["i", "ii", "iv", "v", "paged", "vi"];
    for (n, case) in cases.into_iter().enumerate() {
        let (meta, d) = (Meta::default(), <[Disk; 4]>::default());
        let (id, control) = (upload(&format!("d{n}")), upload("df"));
        let (record, fenced) = seed_case(&meta, case, &id);
        meta.seed(mp::mpu_key(&control), open(&control));
        let before = meta.value(&mp::mpu_key(&id));

        let report = restore_pass(&meta, &d).await.expect(case);

        if fenced {
            assert_fenced(&meta, &id, &report);
        } else {
            assert_eq!(meta.value(&mp::mpu_key(&id)), before, "({case})");
            assert!(retire_keys(&meta, &id).is_empty(), "({case})");
        }
        let named = record.iter().all(|record| names(&report, record));
        let named = named && names(&report, &mp::mpu_key(&id));
        assert!(named && report.needs_human(), "({case}) {report:?}");
        assert_open_fenced(&meta, &control, &report);
    }
}

/// **(K)** Over H(ii), H(iv) (both pages), H(v), a clean `Completing`, an `Open`, and `Aborting@4`
/// sessions whose `retire:records:s:<id>:3` owes another group (`fa`), will not decode (`fb`),
/// owes only parts (`fc`), owes its own group AND parts (`f1`, X104) or is absent (`fd`), a second
/// pass writes nothing and names again each session (and record) the first named. With those
/// obligations dropped, a third names all five at their first `seg:` record. Each naming is one
/// audit event, its fault in words. `fe` (`Aborting@4`, no obligation, no segment) is never named.
#[tokio::test]
async fn a_second_pass_is_idempotent_and_still_names_what_needs_a_human() {
    capture_audit();
    let (meta, d) = (Meta::default(), <[Disk; 4]>::default());
    let cases = [("ii", "f2"), ("iv", "f4"), ("v", "f5"), ("paged", "f6")];
    let cases = cases.map(|(case, pair)| {
        let id = upload(pair);
        let (record, _) = seed_case(&meta, case, &id);
        (id, record.unwrap())
    });
    let [clean, control, bare] = ["f7", "f8", "fe"].map(upload);
    seed_attempt(&meta, &clean, 0xF70);
    meta.seed(mp::mpu_key(&control), open(&control));
    meta.seed(mp::mpu_key(&bare), aborting(&bare));
    // `fa`'s first segment and `fd`'s second will not decode.
    let own = name(&md::encode(&group(&upload("f1"))));
    let own = format!("{{\"parts\":[[1,2]],\"seg\":{own}}}");
    let aborted: [(&str, Option<&[u8]>); 5] = [
        ("fa", Some(FOREIGN)),
        ("fb", Some(b"not json")),
        ("fc", Some(b"{\"parts\":[[1,1]]}")),
        ("fd", None),
        ("f1", Some(own.as_bytes())),
    ];
    let aborted = aborted.map(|(pair, owes)| {
        let id = upload(pair);
        let base = ChunkId::from_str_radix(pair, 16).unwrap() << 4;
        let segments = seed_attempt(&meta, &id, base);
        meta.seed(mp::mpu_key(&id), aborting(&id));
        if let Some(bad) = ["fa", "fd"].iter().position(|at| *at == pair) {
            meta.seed(segments[bad].clone(), JUNK);
        }
        let key = owed(RetireMode::Records, &id);
        let Some(value) = owes else {
            return (id, segments[0].clone());
        };
        let owes = mp::decode_retire_obligation(&key, value).ok();
        assert_eq!(owes.is_some(), pair != "fb", "{pair}");
        let own = owes.is_some_and(|(_, _, owes)| owes.segments() == Some(&group(&id)));
        assert_eq!(own, pair == "f1", "{pair}");
        meta.seed(key.clone(), value.to_vec());
        (id, key)
    });

    let first = restore_pass(&meta, &d).await.unwrap();
    let after_first = meta.snapshot();
    let second = restore_pass(&meta, &d).await.unwrap();

    for id in cases.iter().map(|(id, _)| id).chain([&clean]) {
        assert_fenced(&meta, id, &first);
    }
    assert_open_fenced(&meta, &control, &first);
    let fenced = (first.sessions_fenced, second.sessions_fenced);
    assert_eq!(fenced, (6, 0), "{first:?}\n{second:?}");
    assert!(meta.snapshot() == after_first, "the second pass wrote");
    for (id, record) in cases.iter().chain(&aborted) {
        for key in [&mp::mpu_key(id), record] {
            let both = names(&first, key) && names(&second, key);
            assert!(both, "{}:\n{first:?}\n{second:?}", name(key));
        }
        assert_eq!(audit_names(id, record), 2, "{}", id.as_str());
    }

    for (id, _) in &aborted {
        let key = owed(RetireMode::Records, id);
        meta.kv.lock().unwrap().remove(&key);
    }
    let third = restore_pass(&meta, &d).await.unwrap();

    for (id, record) in &aborted {
        let seg = md::seg_key(&group(id), 0).unwrap();
        let named = names(&third, &mp::mpu_key(id)) && names(&third, &seg);
        let audits = if *record == seg { 3 } else { 1 };
        let named = named && audit_names(id, &seg) == audits;
        assert!(named, "{}: {third:?}", id.as_str());
    }
    for report in [&first, &second, &third] {
        let unnamed = [&clean, &control, &bare].map(|id| !names(report, &mp::mpu_key(id)));
        assert!(unnamed == [true; 3] && report.needs_human(), "{report:?}");
    }
}

/// **(Order)** Child-3's P3: a dangling chunk, an under-replicated one, and a `Completing` session
/// whose fence commit fails. The pass is `Err`, yet the audit seam has the `dangling` line and a
/// summary counting both, not "complete": the fence ran after Pass 3.
#[tokio::test]
async fn a_completing_fence_fault_never_hides_the_pass_verdicts() {
    capture_audit();
    let (meta, d) = (Meta::default(), <[Disk; 4]>::default());
    let rs = EcScheme::ReedSolomon { k: 2, m: 1 };
    for (inode, scheme, placement) in [(1, EcScheme::None, vec![0]), (2, rs, vec![0, 1, 2])] {
        let chunk = ChunkRef {
            scheme,
            placement,
            ..chunk_ref(0x9C0 + ChunkId::from(inode))
        };
        let (size, chunk_map) = (5, vec![chunk].into());
        let state = md::InodeState::Committed;
        let record = md::InodeRecord {
            size,
            chunk_map,
            state,
            version: 1,
            ..Default::default()
        };
        meta.seed(md::inode_key(inode), md::encode(&record));
    }
    for index in 0..2 {
        let frag = FragmentId {
            chunk: 0x9C2,
            index,
        };
        let disk = &d[usize::from(index)];
        disk.frags.lock().unwrap().insert(frag, Bytes::new());
    }
    let id = upload("9c");
    seed_attempt(&meta, &id, 0x9C10);
    meta.failing.lock().unwrap().push(mp::mpu_key(&id));

    let outcome = restore_pass(&meta, &d).await;

    assert!(outcome.is_err(), "{:?}", outcome.ok());
    let dangling = audited("dangling");
    let lost = wyrd_traits::chunk_hex(0x9C1);
    assert!(dangling.iter().any(|l| l["chunk"] == lost), "{dangling:?}");
    let summaries = audited("summary");
    let [summary] = &summaries[..] else {
        panic!("one summary must survive the fence fault: {summaries:?}");
    };
    let counts = (&*summary["under_replicated"], &*summary["dangling"]);
    let complete = summary["message"].contains("reconciliation complete");
    assert!(counts == ("1", "1") && !complete, "{summary:?}");
    let untouched = meta.value(&mp::mpu_key(&id)) == Some(completing(&id, EPOCH, 2));
    assert!(untouched && retire_keys(&meta, &id).is_empty());
}
