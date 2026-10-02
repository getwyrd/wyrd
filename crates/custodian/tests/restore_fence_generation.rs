//! Issue #810 (#664 child 3): the post-restore pass keeps a **restore-fence generation** record
//! (`0016:723-728`, X17b) under the raw key `mpufence`: `{N+1, complete: false}` written before
//! its first other write, and `{N+1, complete: true}` after its last — only when every write it
//! made was acknowledged and no upload session needs a human, and never over a newer pass's
//! record. Each leg drives the production `reconcile_after_restore` over this file's doubles and
//! reads the record by raw key in its codec's own spelling, so the file builds on its base and
//! fails there by assertion.
//!
//! **(P)** Every later pass runs over [`Meta::fresh`], a new store holding only the bytes the last
//! one left — no fault, nothing in flight, no log — so whatever carries a finding from one pass to
//! the next is durable state. The one exception is Q(d), whose late write is in flight IN the
//! store, which is the point of it.
//!
//! The report's own copy of the generation (`RestoreReport::fence_generation`, which the operator
//! command prints) is not a base symbol, so it is checked in `restore_open_fence.rs` and
//! `crates/server/src/cli.rs` instead.

// deferred: #843 — seeded Tier-0 DST coverage of the restore fence (809.5); these legs are
// Tokio-only.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_core::metadata::{self as md, ChunkRef, EcScheme, SegmentGroup, SegmentRecord};
use wyrd_core::multipart::{self as mp, PartScope, RetireMode, UploadId};
use wyrd_custodian::{reconcile_after_restore, ExpiredPendingPolicy, GcContext, RestoreReport};
use wyrd_traits::{
    page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome,
    CommitUnknownResult, DServerId, FragmentId, Health, MetadataStore, PageStart, Result,
    ScanCapExceeded, ScanPage, WriteBatch, SCAN_CAP,
};

/// The record's raw key — spelled here, not imported, so this file builds on a base without it.
const FENCE: &[u8] = b"mpufence";
const EPOCH: u64 = 3;
/// The chunk H(ii)'s second segment names that none of its parts holds.
const UNHELD: ChunkId = 0xD2F;
type Kv = BTreeMap<Vec<u8>, Bytes>;

/// The record's value in its codec's own spelling: generation `n`, complete or not.
fn generation(n: u64, complete: bool) -> Bytes {
    Bytes::from(format!("{{\"generation\":{n},\"complete\":{complete}}}"))
}

/// How a faulted commit answers.
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// A definite error: nothing applied.
    Refused,
    /// `CommitUnknownResult` that may still commit, after applying the batch (`true`) or not.
    Unknown { applied: bool },
}

/// Which commits the double faults.
enum Fault {
    /// Every commit putting a key under `prefix` (with exactly `value`, if one is given).
    On {
        prefix: Vec<u8>,
        value: Option<Bytes>,
        answer: Answer,
    },
    /// Every commit after the first putting a key under `prefix` has landed: the pass stops there.
    StopAfter { prefix: Vec<u8>, stopped: bool },
    /// The first commit putting `value` under [`FENCE`] answers unknown and stays in flight; it
    /// lands — its preconditions judged then — right after the next [`FENCE`] write lands.
    Late { value: Bytes },
    /// Another pass writes `theirs` under [`FENCE`] between this pass's read of it and its commit:
    /// the double lands `theirs` just before the first commit putting `trigger` there (any value,
    /// if none is given), then judges that commit as usual.
    Race {
        trigger: Option<Bytes>,
        theirs: Bytes,
    },
}

#[derive(Default)]
struct Store {
    kv: Kv,
    fault: Option<Fault>,
    in_flight: Option<WriteBatch>,
    /// How the in-flight batch fared when it landed, and what [`FENCE`] read right after.
    landed: Option<(CommitOutcome, Option<Bytes>)>,
    /// Every commit offered, in order: what [`FENCE`] read as it arrived, and the keys it puts.
    offered: Vec<(Option<Bytes>, Vec<Vec<u8>>)>,
}

/// An in-memory `MetadataStore` paging through the seam's helpers, faulting commits as told.
#[derive(Default)]
struct Meta(Mutex<Store>);

impl Meta {
    /// A new store holding only this one's bytes: no fault, nothing in flight, no log (leg P).
    fn fresh(&self) -> Self {
        let kv = self.0.lock().unwrap().kv.clone();
        Self(Mutex::new(Store {
            kv,
            ..Default::default()
        }))
    }

    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.0.lock().unwrap().kv.insert(key.into(), value.into());
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.0.lock().unwrap().kv.get(key).cloned()
    }

    /// The restore-fence generation record, by raw key.
    fn record(&self) -> Option<Bytes> {
        self.value(FENCE)
    }

    fn fault(&self, fault: Fault) {
        self.0.lock().unwrap().fault = Some(fault);
    }

    fn offered(&self) -> Vec<(Option<Bytes>, Vec<Vec<u8>>)> {
        self.0.lock().unwrap().offered.clone()
    }

    fn landed(&self) -> Option<(CommitOutcome, Option<Bytes>)> {
        self.0.lock().unwrap().landed.clone()
    }

    fn range(&self, lower: Bound<&[u8]>, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, Bytes)> {
        let store = self.0.lock().unwrap();
        let hits = store.kv.range::<[u8], _>((lower, Bound::Unbounded));
        let hits = hits.take_while(|(key, _)| key.starts_with(prefix));
        hits.take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn entries_under(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Bytes)> {
        self.range(Bound::Included(prefix), prefix, usize::MAX)
    }
}

fn puts_under(batch: &WriteBatch, prefix: &[u8], value: Option<&Bytes>) -> bool {
    let mut puts = batch.puts.iter();
    puts.any(|(key, put)| key.starts_with(prefix) && value.is_none_or(|value| value == put))
}

/// Judge `batch`'s preconditions against `kv` now, and apply it if they hold.
fn apply(kv: &mut Kv, batch: &WriteBatch) -> CommitOutcome {
    let mut preconditions = batch.preconditions.iter();
    if preconditions.any(|pre| kv.get(&pre.key) != pre.expected.as_ref()) {
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

fn unknown() -> BoxError {
    let (backend, code) = ("fence-generation-double", None);
    let (detail, may_still_commit) = ("injected".to_owned(), true);
    BoxError::from(CommitUnknownResult {
        backend,
        code,
        detail,
        may_still_commit,
    })
}

/// What the double does with one offered commit.
enum Act {
    Apply,
    Answer(Answer),
    Hold,
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.value(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let hits = self.entries_under(prefix);
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
        let mut store = self.0.lock().unwrap();
        let store = &mut *store;
        let puts = batch.puts.iter().map(|(key, _)| key.clone()).collect();
        store.offered.push((store.kv.get(FENCE).cloned(), puts));
        let act = match &store.fault {
            Some(Fault::StopAfter { stopped: true, .. }) => Act::Answer(Answer::Refused),
            Some(Fault::On {
                prefix,
                value,
                answer,
            }) if puts_under(&batch, prefix, value.as_ref()) => Act::Answer(*answer),
            Some(Fault::Late { value }) if puts_under(&batch, FENCE, Some(value)) => Act::Hold,
            Some(Fault::Race { trigger, theirs })
                if puts_under(&batch, FENCE, trigger.as_ref()) =>
            {
                store.kv.insert(FENCE.to_vec(), theirs.clone());
                store.fault = None;
                Act::Apply
            }
            _ => Act::Apply,
        };
        match act {
            Act::Hold => {
                store.fault = None;
                store.in_flight = Some(batch);
                Err(unknown())
            }
            Act::Answer(Answer::Refused) => Err(BoxError::from("refused by the double")),
            Act::Answer(Answer::Unknown { applied }) => {
                if applied {
                    apply(&mut store.kv, &batch);
                }
                Err(unknown())
            }
            Act::Apply => {
                let outcome = apply(&mut store.kv, &batch);
                if outcome == CommitOutcome::Committed {
                    if let Some(Fault::StopAfter { prefix, stopped }) = &mut store.fault {
                        *stopped |= puts_under(&batch, prefix, None);
                    }
                    if puts_under(&batch, FENCE, None) {
                        if let Some(late) = store.in_flight.take() {
                            let landed = apply(&mut store.kv, &late);
                            store.landed = Some((landed, store.kv.get(FENCE).cloned()));
                        }
                    }
                }
                Ok(outcome)
            }
        }
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

fn disks() -> [Disk; 4] {
    <[Disk; 4]>::default()
}

/// A fragment of `chunk` on D server 0 that nothing references: the pass marks it.
fn stray(d: &[Disk; 4], chunk: ChunkId) {
    let frag = FragmentId { chunk, index: 0 };
    d[0].frags.lock().unwrap().insert(frag, Bytes::new());
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

fn is_unknown(err: &BoxError) -> bool {
    let top: &(dyn std::error::Error + 'static) = err.as_ref();
    let mut chain = std::iter::successors(Some(top), |at| at.source());
    chain.any(|at| at.is::<CommitUnknownResult>())
}

fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).unwrap()
}

/// `id`'s segment nonce: its hex reversed, so never the upload id itself.
fn nonce(id: &UploadId) -> String {
    let nonce: String = id.as_str().chars().rev().collect();
    assert_ne!(nonce, id.as_str());
    nonce
}

fn name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).unwrap()
}

/// Whether the report names `key` (quoted by `Debug`), as it does only for a human.
fn names(report: &RestoreReport, key: &[u8]) -> bool {
    format!("{report:?}").contains(&format!("{:?}", name(key)))
}

/// `id`'s session in `state` at `epoch`, `segment_nonce` right after `clock_source` (#840),
/// checked byte for byte against the codec.
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

fn completing(id: &UploadId, written: u32) -> Bytes {
    let state = format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":{written},\
         \"publish_target\":{{\"parent\":42,\"name\":\"o\",\"epoch\":{EPOCH}}}}}"
    );
    session(id, &state, EPOCH)
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

fn seed_segment(meta: &Meta, id: &UploadId, index: u32, chunks: Vec<ChunkRef>) -> Vec<u8> {
    let record = SegmentRecord::new(chunks, u64::from(index) * 5).unwrap();
    let key = md::seg_key(&group(id), index).unwrap();
    meta.seed(key.clone(), md::encode(&record));
    key
}

/// Seed one of #842's two cases that need a human as `id`; the keys the report must name.
fn seed_case(meta: &Meta, case: &str, id: &UploadId) -> Vec<Vec<u8>> {
    match case {
        // H(i): a `Completing` record with no nonce, the shape before #840 — it fails decode, and
        // the fence leaves it as read and names it.
        "i" => {
            let nonce = format!("\"segment_nonce\":\"{}\",", nonce(id));
            let value = name(&completing(id, 1)).replace(&nonce, "");
            assert!(mp::decode_session_record(value.as_bytes()).is_err());
            meta.seed(mp::mpu_key(id), value.into_bytes());
            seed_part(meta, id, 1, &chunk_ref(0xD11));
            vec![mp::mpu_key(id)]
        }
        // H(ii): a `Completing@3` session over parts 1 and 2 whose second segment names part 2's
        // chunk and then [`UNHELD`], which no part holds — fenced, and still named.
        _ => {
            meta.seed(mp::mpu_key(id), completing(id, 2));
            seed_part(meta, id, 1, &chunk_ref(0xD20));
            seed_part(meta, id, 2, &chunk_ref(0xD21));
            seed_segment(meta, id, 0, vec![chunk_ref(0xD20)]);
            let chunks = vec![chunk_ref(0xD21), chunk_ref(UNHELD)];
            let segment = seed_segment(meta, id, 1, chunks);
            vec![mp::mpu_key(id), segment]
        }
    }
}

/// O(a): put back H(ii)'s missing part, the one holding [`UNHELD`].
fn repair_unheld(meta: &Meta, id: &UploadId) {
    seed_part(meta, id, 3, &chunk_ref(UNHELD));
}

/// O(b): replace H(i)'s record with a decodable one carrying its nonce (the operator's repair).
fn repair_no_nonce(meta: &Meta, id: &UploadId) {
    meta.seed(mp::mpu_key(id), completing(id, 1));
}

/// Every `retire:` record naming `id`'s session, key and value.
fn retire_entries(meta: &Meta, id: &UploadId) -> Vec<(Vec<u8>, Bytes)> {
    let ranges = RetireMode::ALL.map(|mode| mp::retire_session_range(mode, id));
    ranges.iter().flat_map(|r| meta.entries_under(r)).collect()
}

/// `id`, a `Completing@3` session, fenced to `Aborting@4` with `{session, all}` and
/// `{seg: (nonce, 3)}`, each decoding against its own key through `decode_retire_obligation`.
fn assert_completing_fenced(meta: &Meta, id: &UploadId) {
    assert_eq!(meta.value(&mp::mpu_key(id)), Some(aborting(id)));
    let (upload_id, epoch, part) = (id.clone(), EPOCH, None);
    let token = mp::RetireToken::Session {
        upload_id,
        epoch,
        part,
    };
    let owed = RetireMode::ALL.map(|mode| {
        let key = mp::retire_key(mode, &token);
        let value = meta.value(&key).expect("an obligation");
        let (found, at, payload) = mp::decode_retire_obligation(&key, &value).unwrap();
        assert_eq!((found, &at), (mode, &token));
        payload
    });
    let [bytes, records] = owed;
    assert!(
        bytes.session() && bytes.parts() == Some(&PartScope::All),
        "{bytes:?}"
    );
    assert_eq!(records.segments(), Some(&group(id)), "{records:?}");
    assert_eq!(retire_entries(meta, id).len(), 2, "no other obligation");
}

/// **(I i–iii)** A store no pass has touched holds no record. At the pass's first fence commit
/// the record already names generation 1, not complete; after the pass, 1 complete; and the
/// next pass, over a fresh store, opens and completes generation 2.
#[tokio::test]
async fn the_generation_record_is_durable_and_names_the_pass_that_wrote_it() {
    let (meta, d) = (Meta::default(), disks());
    let id = upload("a1");
    meta.seed(mp::mpu_key(&id), open(&id));
    assert_eq!(meta.record(), None, "(i) no pass has run");

    let report = restore_pass(&meta, &d).await.unwrap();

    assert_eq!(report.sessions_fenced, 1, "{report:?}");
    let offered = meta.offered();
    let fenced = offered
        .iter()
        .find(|(_, puts)| puts.contains(&mp::mpu_key(&id)));
    let at_fence = fenced.map(|(reading, _)| reading.clone());
    assert_eq!(
        at_fence,
        Some(Some(generation(1, false))),
        "(ii) during the pass"
    );
    assert_eq!(meta.record(), Some(generation(1, true)), "(iii) after it");

    let next = meta.fresh();
    let report = restore_pass(&next, &d).await.unwrap();
    assert_eq!(report.sessions_fenced, 0, "{report:?}");
    assert_eq!(next.record(), Some(generation(2, true)));
}

/// **(I iv)** An image captured after pass 5 completed brings the record back reading 5
/// complete, beside a resurrected `Open` session and a stray fragment. The pass's FIRST commit
/// opens generation 6, not complete, over it; every later commit — the mark batch and the fence
/// among them — arrives with the record reading 6 not complete; and only its LAST commit, after
/// all of them, completes 6.
#[tokio::test]
async fn a_restored_complete_is_replaced_before_any_mark_or_fence() {
    let (meta, d) = (Meta::default(), disks());
    meta.seed(FENCE, generation(5, true));
    let id = upload("b1");
    meta.seed(mp::mpu_key(&id), open(&id));
    stray(&d, 0x5A);

    let report = restore_pass(&meta, &d).await.unwrap();

    let done = (report.stranded_marked, report.sessions_fenced);
    assert_eq!(done, (1, 1), "{report:?}");
    let offered = meta.offered();
    let Some(((opening, opens), rest)) = offered.split_first() else {
        panic!("no commit was offered");
    };
    assert_eq!(opening.as_ref(), Some(&generation(5, true)));
    assert_eq!(
        opens,
        &[FENCE.to_vec()],
        "the first commit opens the generation"
    );
    let Some(((_, closes), _)) = rest.split_last() else {
        panic!("one commit only: {offered:?}");
    };
    assert_eq!(closes, &[FENCE.to_vec()], "the last commit completes it");
    for (reading, puts) in rest {
        let puts: Vec<String> = puts.iter().map(|key| name(key)).collect();
        assert_eq!(reading.as_ref(), Some(&generation(6, false)), "{puts:?}");
    }
    let wrote = |prefix: &[u8]| {
        rest.iter()
            .any(|(_, puts)| puts.iter().any(|k| k.starts_with(prefix)))
    };
    assert!(
        wrote(md::ORPHAN_PREFIX) && wrote(mp::MPU_PREFIX),
        "{offered:?}"
    );
    assert_eq!(meta.record(), Some(generation(6, true)));
}

/// **(M)** Each of #842's cases that needs a human — H(i), a `Completing` record with no nonce
/// (left unfenced, named), and H(ii), a `Completing` session whose segment names a chunk no part
/// holds (fenced, still named) — ends the pass needing a human with generation 1 NOT complete,
/// while an `Open` control beside it is fenced.
#[tokio::test]
async fn a_session_that_needs_a_human_leaves_the_generation_not_complete() {
    for case in ["i", "ii"] {
        let (meta, d) = (Meta::default(), disks());
        let (id, control) = (upload("c1"), upload("cf"));
        let named = seed_case(&meta, case, &id);
        meta.seed(mp::mpu_key(&control), open(&control));

        let report = restore_pass(&meta, &d).await.expect(case);

        let all_named = named.iter().all(|key| names(&report, key));
        assert!(all_named && report.needs_human(), "H({case}): {report:?}");
        assert_eq!(meta.value(&mp::mpu_key(&control)), Some(aborting(&control)));
        assert_eq!(
            meta.record(),
            Some(generation(1, false)),
            "H({case}): {report:?}"
        );
    }
}

/// **(M-scope)** Only a session finding withholds completion. Beside an `Open` session the pass
/// fences cleanly, each finding that is not a session's — a dangling committed chunk (fewer than
/// k fragments anywhere), an `inode:` record that will not decode, a `pending:` entry that will
/// not read as a lease, a `part:` record of that session that will not decode, and one whose
/// placement cannot be trusted — leaves the generation COMPLETE, needing a human or not. An
/// `mpu:` key naming no upload, a session the pass cannot read, leaves it not complete.
#[tokio::test]
async fn only_a_session_finding_withholds_completion() {
    let cases = [
        ("dangling", true, true),
        ("inode", true, true),
        ("pending", true, true),
        ("part-unreadable", true, true),
        ("part-untrusted", false, true),
        ("mpu-key", true, false),
    ];
    for (case, human, complete) in cases {
        let (meta, d) = (Meta::default(), disks());
        let id = upload("e1");
        meta.seed(mp::mpu_key(&id), open(&id));
        let first = mp::part_key(&id, mp::PartNumber::new(1).unwrap());
        match case {
            "dangling" => {
                let (size, chunk_map) = (5, vec![chunk_ref(0x9C1)].into());
                let state = md::InodeState::Committed;
                let record = md::InodeRecord {
                    size,
                    chunk_map,
                    state,
                    version: 1,
                    ..Default::default()
                };
                meta.seed(md::inode_key(1), md::encode(&record));
            }
            "inode" => meta.seed(md::inode_key(2), b"not an inode".as_slice()),
            "pending" => meta.seed(md::pending_key(0x9C3), b"not a lease".as_slice()),
            "part-unreadable" => meta.seed(first, b"not a part".as_slice()),
            "part-untrusted" => {
                let placement = vec![1, 2];
                let chunk = ChunkRef {
                    placement,
                    ..chunk_ref(0x9C4)
                };
                seed_part(&meta, &id, 1, &chunk);
            }
            _ => meta.seed(b"mpu:not-an-upload".to_vec(), open(&id)),
        }

        let report = restore_pass(&meta, &d).await.expect(case);

        assert_eq!(report.needs_human(), human, "{case}: {report:?}");
        assert_eq!(meta.value(&mp::mpu_key(&id)), Some(aborting(&id)), "{case}");
        assert_eq!(
            meta.record(),
            Some(generation(1, complete)),
            "{case}: {report:?}"
        );
    }
}

/// **(M → N → O)** One store holds both of M's cases and an `Open` control.
///
/// **N**: a second pass over a fresh store with NOTHING repaired — H(ii) is already `Aborting`,
/// H(i) still will not decode — names both again and leaves generation 2 NOT complete (#664
/// iteration 1 skipped every `Aborting` session and certified it complete here).
///
/// **O**: from that store, (a) H(ii)'s missing `part:` record put back: the next pass names
/// nothing of it and its fence obligations stay byte-identical (no second one); (b) H(i)'s record
/// replaced by a decodable one with its nonce: the next pass fences it, both obligations decoding
/// against their keys. Either repair alone leaves generation 3 NOT complete; both complete it.
#[tokio::test]
async fn residue_survives_a_re_fence_and_only_both_repairs_complete_the_generation() {
    let (meta, d) = (Meta::default(), disks());
    let (no_nonce, unheld, control) = (upload("f1"), upload("f2"), upload("fe"));
    let mut named = seed_case(&meta, "i", &no_nonce);
    named.extend(seed_case(&meta, "ii", &unheld));
    meta.seed(mp::mpu_key(&control), open(&control));

    let first = restore_pass(&meta, &d).await.unwrap();

    assert!(named.iter().all(|key| names(&first, key)), "{first:?}");
    assert_eq!(meta.value(&mp::mpu_key(&unheld)), Some(aborting(&unheld)));
    assert_eq!(meta.record(), Some(generation(1, false)), "M: {first:?}");

    let unrepaired = meta.fresh();
    let second = restore_pass(&unrepaired, &d).await.unwrap();

    assert!(named.iter().all(|key| names(&second, key)), "{second:?}");
    assert!(
        second.sessions_fenced == 0 && second.needs_human(),
        "{second:?}"
    );
    assert_eq!(
        unrepaired.record(),
        Some(generation(2, false)),
        "N: {second:?}"
    );
    let fenced = retire_entries(&unrepaired, &unheld);
    assert_eq!(fenced.len(), 2, "H(ii)'s two fence obligations");

    for (repaired, part, record) in [
        ("(a)", true, false),
        ("(b)", false, true),
        ("both", true, true),
    ] {
        let store = unrepaired.fresh();
        if part {
            repair_unheld(&store, &unheld);
        }
        if record {
            repair_no_nonce(&store, &no_nonce);
        }

        let report = restore_pass(&store, &d).await.expect(repaired);

        let unheld_named = names(&report, &mp::mpu_key(&unheld));
        assert_eq!(unheld_named, !part, "{repaired}: {report:?}");
        assert_eq!(
            retire_entries(&store, &unheld),
            fenced,
            "{repaired}: H(ii)'s obligations"
        );
        if record {
            assert_completing_fenced(&store, &no_nonce);
            assert!(!names(&report, &mp::mpu_key(&no_nonce)), "{report:?}");
        } else {
            assert!(
                names(&report, &mp::mpu_key(&no_nonce)),
                "{repaired}: {report:?}"
            );
        }
        let complete = part && record;
        assert_eq!(
            store.record(),
            Some(generation(3, complete)),
            "{repaired}: {report:?}"
        );
    }
}

/// **(N-crash)** Residue survives an interrupted pass, twice. (a) The first pass stops right
/// after H(ii)'s durable `Completing → Aborting` fence: every later commit is refused (here the
/// `Open` control's fence). (b) A later pass stops right after it opens its generation, before
/// any fence (a newly seeded `Open` session's fence is refused). After each, a pass over a fresh
/// store with nothing repaired names H(ii) and leaves its generation not complete; after O's
/// repair, a pass completes it.
#[tokio::test]
async fn residue_survives_an_interrupted_pass() {
    let d = disks();
    let meta = Meta::default();
    // Key order: H(ii) `a2…`, then `b9…` (seeded for (b)), then the control `f9…`.
    let (unheld, control, later) = (upload("a2"), upload("f9"), upload("b9"));
    let named = seed_case(&meta, "ii", &unheld);
    meta.seed(mp::mpu_key(&control), open(&control));
    let prefix = mp::mpu_key(&unheld);
    meta.fault(Fault::StopAfter {
        prefix,
        stopped: false,
    });

    let cut = restore_pass(&meta, &d).await;

    assert!(cut.is_err(), "(a) {:?}", cut.ok());
    assert_eq!(meta.value(&mp::mpu_key(&unheld)), Some(aborting(&unheld)));
    assert_eq!(meta.value(&mp::mpu_key(&control)), Some(open(&control)));
    assert_eq!(meta.record(), Some(generation(1, false)), "(a)");

    let after_a = meta.fresh();
    let report = restore_pass(&after_a, &d).await.unwrap();

    assert!(named.iter().all(|key| names(&report, key)), "{report:?}");
    assert_eq!(after_a.record(), Some(generation(2, false)), "{report:?}");

    let stopped = after_a.fresh();
    stopped.seed(mp::mpu_key(&later), open(&later));
    let prefix = FENCE.to_vec();
    stopped.fault(Fault::StopAfter {
        prefix,
        stopped: false,
    });

    let cut = restore_pass(&stopped, &d).await;

    assert!(cut.is_err(), "(b) {:?}", cut.ok());
    assert_eq!(stopped.value(&mp::mpu_key(&later)), Some(open(&later)));
    assert_eq!(stopped.record(), Some(generation(3, false)), "(b)");

    let after_b = stopped.fresh();
    let report = restore_pass(&after_b, &d).await.unwrap();

    assert!(named.iter().all(|key| names(&report, key)), "{report:?}");
    assert_eq!(after_b.record(), Some(generation(4, false)), "{report:?}");

    let repaired = after_b.fresh();
    repair_unheld(&repaired, &unheld);
    let report = restore_pass(&repaired, &d).await.unwrap();

    assert!(!report.needs_human(), "{report:?}");
    assert_eq!(repaired.record(), Some(generation(5, true)), "{report:?}");
}

/// **(Q a)** Over a restored `complete` (generation 3), a fence commit or a mark commit fails —
/// definitely, or with an unknown outcome that applied or did not. The pass is `Err` and the
/// record reads generation 4, NOT complete.
#[tokio::test]
async fn a_failed_mark_or_fence_commit_never_completes_the_generation() {
    let failures = [
        Answer::Refused,
        Answer::Unknown { applied: true },
        Answer::Unknown { applied: false },
    ];
    for (what, prefix) in [("fence", mp::MPU_PREFIX), ("mark", md::ORPHAN_PREFIX)] {
        for answer in failures {
            let (meta, d) = (Meta::default(), disks());
            meta.seed(FENCE, generation(3, true));
            let id = upload("9a");
            meta.seed(mp::mpu_key(&id), open(&id));
            stray(&d, 0x5A);
            let (prefix, value) = (prefix.to_vec(), None);
            meta.fault(Fault::On {
                prefix,
                value,
                answer,
            });

            let outcome = restore_pass(&meta, &d).await;

            assert!(outcome.is_err(), "{what} {answer:?}: {:?}", outcome.ok());
            let read = meta.record();
            assert_eq!(read, Some(generation(4, false)), "{what} {answer:?}");
        }
    }
}

/// **(Q b)** Over a restored `complete` (generation 3), the opening write answers an unknown
/// outcome that may still commit — once applied, once not. The pass is that `Err`, the session
/// is untouched and nothing is marked or owed; the record reads generation 4 not complete where
/// the write applied, and the restored `complete` where it did not (the pre-pass state #508's
/// restore-scoped signal is for).
#[tokio::test]
async fn an_unsettled_opening_write_runs_no_mark_and_no_fence() {
    for applied in [true, false] {
        let (meta, d) = (Meta::default(), disks());
        meta.seed(FENCE, generation(3, true));
        let id = upload("9b");
        meta.seed(mp::mpu_key(&id), open(&id));
        stray(&d, 0x5A);
        let (prefix, value, answer) = (FENCE.to_vec(), None, Answer::Unknown { applied });
        meta.fault(Fault::On {
            prefix,
            value,
            answer,
        });

        let outcome = restore_pass(&meta, &d).await;

        let err = outcome.expect_err("must fail");
        assert!(is_unknown(&err), "{err}");
        assert_eq!(meta.value(&mp::mpu_key(&id)), Some(open(&id)), "{applied}");
        let written = (
            meta.entries_under(b"retire:"),
            meta.entries_under(md::ORPHAN_PREFIX),
        );
        assert_eq!(written, (Vec::new(), Vec::new()), "{applied}");
        let expected = if applied {
            generation(4, false)
        } else {
            generation(3, true)
        };
        assert_eq!(meta.record(), Some(expected), "{applied}");
    }
}

/// **(Q c)** The completing write answers an unknown outcome — once applied, once not. The pass
/// is that `Err` either way: an unknown outcome is never a clean finish. Where it applied, the
/// record reads generation 1 complete, which is true (every earlier write was acknowledged);
/// where it did not, 1 not complete — and the next pass opens and completes generation 2.
#[tokio::test]
async fn an_unknown_completing_write_is_true_or_finished_by_the_next_pass() {
    for applied in [true, false] {
        let (meta, d) = (Meta::default(), disks());
        let id = upload("9c");
        meta.seed(mp::mpu_key(&id), open(&id));
        let (prefix, value) = (FENCE.to_vec(), Some(generation(1, true)));
        let answer = Answer::Unknown { applied };
        meta.fault(Fault::On {
            prefix,
            value,
            answer,
        });

        let outcome = restore_pass(&meta, &d).await;

        let err = outcome.expect_err("must fail");
        assert!(is_unknown(&err), "{err}");
        assert_eq!(meta.value(&mp::mpu_key(&id)), Some(aborting(&id)));
        assert_eq!(meta.record(), Some(generation(1, applied)), "{applied}");

        let next = meta.fresh();
        restore_pass(&next, &d).await.unwrap();
        assert_eq!(next.record(), Some(generation(2, true)), "{applied}");
    }
}

/// **(Q d)** Generation 1's completing write answers unknown and stays in flight. A new pass opens
/// generation 2, and only then does the generation-1 write land, its precondition judged as it
/// lands: it finds generation 2's record rather than the bytes it requires and writes nothing. The
/// record still reads 2 not complete, and the new pass completes 2. Both passes run over the ONE
/// store the late write is in flight in.
#[tokio::test]
async fn a_late_completion_never_masks_a_newer_pass() {
    let (meta, d) = (Meta::default(), disks());
    let id = upload("9d");
    meta.seed(mp::mpu_key(&id), open(&id));
    meta.fault(Fault::Late {
        value: generation(1, true),
    });

    let first = restore_pass(&meta, &d).await;

    assert!(first.is_err(), "{:?}", first.ok());
    assert_eq!(
        meta.record(),
        Some(generation(1, false)),
        "in flight, not landed"
    );

    let second = restore_pass(&meta, &d).await.unwrap();

    let late = (CommitOutcome::Conflict, Some(generation(2, false)));
    assert_eq!(meta.landed(), Some(late), "{second:?}");
    assert_eq!(meta.record(), Some(generation(2, true)), "{second:?}");
}

/// **(Q, both writes conditioned on the bytes read)** Another pass writes the record between this
/// pass's read of it and this pass's commit.
///
/// *Opening*, over a restored `complete` (3) and over no record: the other pass opened the same
/// next generation first (4, or 1), byte for byte what this pass would write. This pass's opening
/// write conflicts, so the pass is `Err` having offered no other commit: the session stays `Open`,
/// nothing is marked or owed, and the record is the other pass's. A pass that wrote over it, or
/// carried on, could later complete the generation while the other pass was still fencing.
///
/// *Completing*: a newer pass opens generation 2 just before this pass's completion lands. The
/// completion conflicts, the pass is `Err`, and the record reads the newer pass's 2 not complete.
#[tokio::test]
async fn a_record_changed_under_the_pass_is_never_written_over() {
    let opening = [
        (Some(generation(3, true)), generation(4, false)),
        (None, generation(1, false)),
    ];
    for (prior, theirs) in opening {
        let (meta, d) = (Meta::default(), disks());
        if let Some(prior) = &prior {
            meta.seed(FENCE, prior.clone());
        }
        let id = upload("9f");
        meta.seed(mp::mpu_key(&id), open(&id));
        stray(&d, 0x5A);
        let (trigger, at) = (None, theirs.clone());
        meta.fault(Fault::Race {
            trigger,
            theirs: at,
        });

        let outcome = restore_pass(&meta, &d).await;

        assert!(outcome.is_err(), "over {prior:?}: {:?}", outcome.ok());
        assert_eq!(meta.offered().len(), 1, "over {prior:?}: only the opening");
        assert_eq!(meta.value(&mp::mpu_key(&id)), Some(open(&id)), "{prior:?}");
        let written = (
            meta.entries_under(b"retire:"),
            meta.entries_under(md::ORPHAN_PREFIX),
        );
        assert_eq!(written, (Vec::new(), Vec::new()), "over {prior:?}");
        assert_eq!(meta.record(), Some(theirs), "over {prior:?}");
    }

    let (meta, d) = (Meta::default(), disks());
    let id = upload("9f");
    meta.seed(mp::mpu_key(&id), open(&id));
    let (trigger, theirs) = (Some(generation(1, true)), generation(2, false));
    meta.fault(Fault::Race { trigger, theirs });

    let outcome = restore_pass(&meta, &d).await;

    assert!(outcome.is_err(), "completing: {:?}", outcome.ok());
    assert_eq!(meta.value(&mp::mpu_key(&id)), Some(aborting(&id)));
    assert_eq!(meta.record(), Some(generation(2, false)), "completing");
}

/// A record the pass cannot read — not JSON, generation 0, an unknown field, another spelling of
/// a valid record — or one at generation `u64::MAX`, with no next: the pass is `Err` before it
/// offers a single commit, the record stays byte-identical, and the session stays `Open`.
#[tokio::test]
async fn a_torn_or_exhausted_generation_record_stops_the_pass_before_any_write() {
    let stored = [
        Bytes::from_static(b"not json"),
        Bytes::from_static(b"{\"generation\":0,\"complete\":true}"),
        Bytes::from_static(b"{\"generation\":3,\"complete\":true,\"by\":1}"),
        Bytes::from_static(b"{\"complete\":true,\"generation\":3}"),
        generation(u64::MAX, true),
    ];
    for stored in stored {
        let (meta, d) = (Meta::default(), disks());
        meta.seed(FENCE, stored.clone());
        let id = upload("9e");
        meta.seed(mp::mpu_key(&id), open(&id));
        stray(&d, 0x5A);

        let outcome = restore_pass(&meta, &d).await;

        assert!(outcome.is_err(), "{stored:?}: {:?}", outcome.ok());
        assert_eq!(meta.record(), Some(stored.clone()));
        assert_eq!(meta.value(&mp::mpu_key(&id)), Some(open(&id)), "{stored:?}");
        assert!(
            meta.offered().is_empty(),
            "{stored:?}: a commit was offered"
        );
    }
}
