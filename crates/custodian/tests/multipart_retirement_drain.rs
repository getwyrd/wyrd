//! Issue #1061 — the custodian drains `retire:records:` obligations: the retirement leg, driven
//! **only** through `reconcile_step` with the contexts the deployed loop passes (a reconstruction
//! context, every other input `None`, `crates/server/src/custodian.rs`, `reconcile_pass`), over a
//! real metadata backend (`RedbMetadataStore::in_memory()`) behind a recording wrapper. Every pass
//! builds its leadership fence and context afresh, as the deployed loop does, so nothing the drain
//! knows survives from one pass to the next except what is in the store.
//!
//! * **(B-rec)** A production Complete naming a subset of the staged parts, then passes: the
//!   published parts' `part:`/`psum:` records are gone, no `orphan:` record exists at all, the
//!   object reads back byte-identical, and the unnamed parts' records and their `retire:bytes:`
//!   obligation are byte-identical to before.
//! * **(C)** Convergence at scale: a `{parts}` obligation over 4,001 non-adjacent part numbers,
//!   a `{seg}` one over more records than `MAX_BATCH_OPS` — on a store that returns whole pages
//!   and on one that cuts them short (a lowered scan cap) — and a `{parts, seg}` one crossing
//!   between its components at a commit boundary, each drained to empty, every commit inside the
//!   batch budgets, the stored value advancing every step, each step continuing from the stored
//!   cursor, the obligation deleted only with its last record, and a second drain a no-op.
//! * **(L)** Per-pass bounds under a recording wrapper, over ledgers three times the visit bound:
//!   a productive backlog (L1: `{parts}` steps in key order with none passed over, `{seg}` steps
//!   that read, and a store whose short pages make the page cap the bound) and a prefix of
//!   obligations that cannot progress (L2).
//! * **(S)** What a pass certifies: one that resumed part-way, or whose every step lost its
//!   precondition, never answers `Satisfied` while a records obligation is left.
//! * **(W)** A wrapper that refuses any put past `MAX_VALUE_BYTES`, as FoundationDB does: an
//!   obligation at the installer ceiling drains, one whose advanced value is exactly
//!   `MAX_VALUE_BYTES` drains, one just under `MAX_VALUE_BYTES` is held and its over-ceiling put
//!   never sent, no pass fails, and the obligation after it drains meanwhile.
//! * **(G-rec)** The production restore fence's `{seg}` obligation drained one step at a time,
//!   with the production `reconcile_after_restore` naming nothing after any step.
//! * **(K, `{seg}`)** A `{seg}` cursor past the last record, with records below it, is a hold.
//! * **(H)** The other ways one obligation is held or retried without stopping the rest: a commit
//!   the store refuses, a precondition another writer moved (re-read and re-planned), a damaged
//!   resume point (the walk restarts at the head), a stray key in a segment range.
//!
//! Every bound and every hold reason is stated here as a literal, never read back from the code
//! under test. Obligations not installed by a production writer here are planted in the codec's
//! own spelling, checked by decoding and re-encoding. The audit seam is read through a subscriber
//! attached to each pass's future, never a global one.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::Dispatch;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::{encode as encode_fragment, FragmentHeader};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self as md, seg_key, ChunkRef, EcScheme, InodeId, SegmentGroup, SegmentNonce, SegmentRecord,
    MAX_VALUE_BYTES,
};
use wyrd_core::multipart::complete::{
    complete_upload, CompleteRequest, NamedParts, PublishRequest,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::{
    self as mp, decode_part_record, decode_part_summary, decode_retire_obligation, part_key,
    psum_key, retire_key, Budget, CompleteOutcome, CreateOutcome, Digest, PartNumber, PartSummary,
    RetireComponent, RetireMode, RetireToken, UploadId, MAX_BATCH_BYTES, MAX_BATCH_OPS,
    RETIRE_VALUE_CEILING,
};
use wyrd_core::read::read_object;
use wyrd_core::repair::REPAIR_PREFIX;
use wyrd_custodian::gc::W_WRITE_MILLIS;
use wyrd_custodian::retirement::{
    COMMITS_PER_PASS, LEDGER_CURSOR_KEY, LEDGER_PAGE, LEDGER_PAGES_PER_PASS, READS_PER_PASS,
    READS_PER_STEP, STEPS_PER_PASS, VISITS_PER_PASS,
};
use wyrd_custodian::{
    reconcile_after_restore, reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone,
    GcContext, ReconcileError, Reconciled, ReconstructionContext, RestoreReport, Topology,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::ManualClock;
use wyrd_traits::{
    BoxError, ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result as StoreResult, ScanPage, WriteBatch,
};

// ─────────────────────────────── fixtures ───────────────────────────────

/// The instant every pass runs at. Nothing in the drain reads a clock; the reconstruction
/// context still names one, the deployed loop's shape.
const NOW: u64 = 1_000_000;
/// The epoch every planted obligation's token and segment group carry.
const EPOCH: u64 = 1;
/// Every planted part is one chunk of this many bytes.
const CHUNK_LEN: u64 = 1_024;
const COMMITTED_AT: u64 = 1_500;
/// Parts planted per fixture batch: two puts each, well inside one batch.
const PLANT_BATCH: usize = 200;
/// The audit target the retirement leg names a held obligation on.
const AUDIT_TARGET: &str = "wyrd.custodian.retirement.audit";

/// The per-pass bounds the retirement leg promises (its module doc), as numbers: obligations
/// visited, ledger pages read, steps committed, store reads, commits. Checked against the built
/// constants once ([`the_built_bounds_are_the_promised_ones`]); every pass below is held to these.
const VISIT_BOUND: usize = 64;
const LEDGER_PAGE_BOUND: usize = 5;
const STEP_BOUND: usize = 16;
/// The resume point, the ledger pages, and four per step: a re-read after a lost precondition,
/// a page of segments, one row past a page the store cut short, the whole-range read.
const READ_BOUND: usize = 1 + LEDGER_PAGE_BOUND + 4 * STEP_BOUND;
const COMMIT_BOUND: usize = STEP_BOUND + 1;

fn upload(n: u64) -> UploadId {
    UploadId::new(format!("{n:032x}")).unwrap()
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
}

fn name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).unwrap()
}

/// Part `n`'s digest: its number in the first four bytes.
fn digest_of(n: u32) -> Digest {
    let mut bytes = [0x5a; 32];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    Digest::from_bytes(bytes)
}

/// The chunk part `n` of the upload numbered `owner` names.
fn chunk_of(owner: u64, n: u32) -> ChunkId {
    ChunkId::from(owner) << 40 | ChunkId::from(n)
}

fn chunk_ref(id: ChunkId) -> ChunkRef {
    ChunkRef {
        id,
        scheme: EcScheme::None,
        len: CHUNK_LEN,
        placement: vec![0],
    }
}

/// The puts that plant part `n` of `id` as the part commit writes it: its `part:` record naming
/// one chunk and its `psum:` summary, each in the codec's own spelling.
fn plant_part(id: &UploadId, owner: u64, n: u32) -> Vec<(Vec<u8>, Bytes)> {
    let digest = digest_of(n);
    let record = format!(
        "{{\"chunks\":[{{\"id\":{},\"scheme\":\"None\",\"len\":{CHUNK_LEN},\"placement\":[0]}}],\
         \"len\":{CHUNK_LEN},\"digest\":\"{digest}\",\"committed_at_millis\":{COMMITTED_AT},\
         \"session_epoch\":0}}",
        chunk_of(owner, n),
    );
    let decoded = decode_part_record(record.as_bytes()).expect("a canonical part");
    assert_eq!(md::encode(&decoded), record.as_bytes());
    let summary = PartSummary {
        chunks: 1,
        len: CHUNK_LEN,
        digest,
        committed_at_millis: COMMITTED_AT,
    };
    let psum = md::encode(&summary);
    assert_eq!(decode_part_summary(&psum).unwrap(), summary);
    vec![
        (part_key(id, part(n)), Bytes::from(record)),
        (psum_key(id, part(n)), psum),
    ]
}

/// The range encoding of `numbers` (`0016:382-388`), spelled here: each maximal run `[lo,hi]`.
fn bare_set(numbers: &[u32]) -> String {
    assert!(numbers.windows(2).all(|pair| pair[1] > pair[0]));
    let mut runs: Vec<(u32, u32)> = Vec::new();
    for &n in numbers {
        match runs.last_mut() {
            Some(run) if run.1 + 1 == n => run.1 = n,
            _ => runs.push((n, n)),
        }
    }
    let runs: Vec<String> = runs.iter().map(|(lo, hi)| format!("[{lo},{hi}]")).collect();
    format!("[{}]", runs.join(","))
}

/// `count` part numbers from `first`, no two adjacent.
fn sparse(first: u32, count: u32) -> Vec<u32> {
    (0..count).map(|i| first + 2 * i).collect()
}

/// The suffix-free session token of `id` at [`EPOCH`].
fn token(id: &UploadId) -> RetireToken {
    RetireToken::Session {
        upload_id: id.clone(),
        epoch: EPOCH,
        part: None,
    }
}

fn records_key(id: &UploadId) -> Vec<u8> {
    retire_key(RetireMode::Records, &token(id))
}

/// The segment group of the upload numbered `owner`, at [`EPOCH`].
fn group_of(owner: u64) -> SegmentGroup {
    SegmentGroup::new(format!("{:032x}", 0xfeed_0000_u64 + owner), EPOCH).unwrap()
}

fn seg_payload(group: &SegmentGroup) -> String {
    format!(
        "{{\"nonce\":\"{}\",\"epoch\":{}}}",
        group.nonce(),
        group.epoch()
    )
}

// ──────────────────────────── the recording store ────────────────────────────

/// What one pass did to the store, as the wrapper saw it.
#[derive(Default)]
struct Log {
    /// Every read: `get`, `scan`, `scan_page` — less the reconstruction leg's one read of the
    /// repair queue, which is not the drain's.
    reads: usize,
    /// The obligations the `retire:` ledger pages handed back.
    ledger_entries: usize,
    /// The `retire:` ledger pages read.
    ledger_pages: usize,
    /// The reads of a `seg:` range.
    seg_reads: usize,
    /// Every batch sent, refused or not.
    sent: Vec<WriteBatch>,
}

/// Another writer that moves one obligation before every commit preconditioned on it, putting
/// each of `values` in turn: a drainer that keeps winning the race.
struct Contender {
    key: Vec<u8>,
    values: [Bytes; 2],
    turns: usize,
}

/// `RedbMetadataStore` behind a wrapper that logs every read and every batch, and — when
/// `refuse_past_ceiling` — refuses, as FoundationDB does (`2103 value_too_large`), any batch
/// putting a value past [`MAX_VALUE_BYTES`], with nothing applied.
struct Store {
    inner: RedbMetadataStore,
    refuse_past_ceiling: bool,
    log: Mutex<Log>,
    /// Applied to the inner store just before the next commit is acted on: another writer at
    /// that instant.
    before_next_commit: Mutex<Option<WriteBatch>>,
    /// Every commit preconditioned on this key fails with a definite error, nothing applied.
    failing: Mutex<Option<Vec<u8>>>,
    contender: Mutex<Option<Contender>>,
}

impl Store {
    fn new() -> Self {
        Self::over(RedbMetadataStore::in_memory().unwrap())
    }

    fn over(inner: RedbMetadataStore) -> Self {
        Self {
            inner,
            refuse_past_ceiling: false,
            log: Mutex::new(Log::default()),
            before_next_commit: Mutex::new(None),
            failing: Mutex::new(None),
            contender: Mutex::new(None),
        }
    }

    /// A store whose pages hold at most `cap` rows, whatever a caller asks for: the page bound
    /// `scan_page` allows every backend (`with_scan_cap`), with `next` set on a full page.
    fn capped(cap: usize) -> Self {
        Self::over(RedbMetadataStore::in_memory().unwrap().with_scan_cap(cap))
    }

    fn refusing_past_ceiling() -> Self {
        Self {
            refuse_past_ceiling: true,
            ..Self::new()
        }
    }

    /// The log since the last call, and a fresh one started.
    fn take_log(&self) -> Log {
        std::mem::take(&mut *self.log.lock().unwrap())
    }

    /// Fixture writes, straight to the inner store: neither logged nor refused.
    async fn plant(&self, puts: Vec<(Vec<u8>, Bytes)>) {
        for chunk in puts.chunks(2 * PLANT_BATCH) {
            let mut batch = WriteBatch::new();
            batch.puts.extend(chunk.iter().cloned());
            let outcome = self.inner.commit(batch).await.unwrap();
            assert_eq!(outcome, CommitOutcome::Committed);
        }
    }

    /// Plant `value` at `key`, checked to be a valid obligation in the codec's own spelling.
    async fn plant_obligation(&self, key: Vec<u8>, value: String) {
        let (_, _, payload) =
            decode_retire_obligation(&key, value.as_bytes()).expect("a valid obligation");
        assert_eq!(
            md::encode(&payload),
            value.as_bytes(),
            "the canonical spelling"
        );
        self.plant(vec![(key, Bytes::from(value))]).await;
    }

    async fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.inner.get(key).await.unwrap()
    }

    /// Every record under `prefix`, read straight from the inner store, page by page so a capped
    /// store answers too.
    async fn under(&self, prefix: &[u8]) -> BTreeMap<Vec<u8>, Bytes> {
        let mut records = BTreeMap::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let (page, next) = self
                .inner
                .scan_page(prefix, after.as_deref(), usize::MAX)
                .await
                .unwrap();
            records.extend(page);
            match next {
                None => return records,
                Some(next) => after = Some(next),
            }
        }
    }

    /// Fixture delete, straight to the inner store.
    async fn remove(&self, key: &[u8]) {
        let outcome = self.inner.commit(WriteBatch::new().delete(key.to_vec()));
        assert_eq!(outcome.await.unwrap(), CommitOutcome::Committed);
    }

    /// The whole store.
    async fn snapshot(&self) -> BTreeMap<Vec<u8>, Bytes> {
        self.under(b"").await
    }
}

#[async_trait]
impl MetadataStore for Store {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.log.lock().unwrap().reads += 1;
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        if prefix != REPAIR_PREFIX {
            self.log.lock().unwrap().reads += 1;
        }
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<ScanPage> {
        let page = self.inner.scan_page(prefix, after, limit).await?;
        let mut log = self.log.lock().unwrap();
        log.reads += 1;
        if prefix == b"retire:" {
            log.ledger_pages += 1;
            log.ledger_entries += page.0.len();
        }
        if prefix.starts_with(b"seg:") {
            log.seg_reads += 1;
        }
        Ok(page)
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        self.log.lock().unwrap().sent.push(batch.clone());
        let contended = {
            let mut contender = self.contender.lock().unwrap();
            match contender.as_mut() {
                Some(c) if batch.preconditions.iter().any(|p| p.key == c.key) => {
                    c.turns += 1;
                    let value = c.values[c.turns % 2].clone();
                    Some(WriteBatch::new().put(c.key.clone(), value))
                }
                _ => None,
            }
        };
        let other_writer = contended.or_else(|| self.before_next_commit.lock().unwrap().take());
        if let Some(write) = other_writer {
            let landed = self.inner.commit(write).await?;
            assert_eq!(
                landed,
                CommitOutcome::Committed,
                "the other writer's batch lands"
            );
        }
        let failing = self.failing.lock().unwrap().clone();
        if let Some(key) = failing {
            if batch.preconditions.iter().any(|p| p.key == key) {
                return Err(BoxError::from(format!("refused: {}", name(&key))));
            }
        }
        if self.refuse_past_ceiling {
            if let Some((key, value)) = batch.puts.iter().find(|(_, v)| v.len() > MAX_VALUE_BYTES) {
                return Err(BoxError::from(format!(
                    "2103 value_too_large: {} is {} bytes",
                    name(key),
                    value.len()
                )));
            }
        }
        self.inner.commit(batch).await
    }
}

// ─────────────────────────────── the D server ───────────────────────────────

/// One in-process D server, id 0, holding whole fragments by id: what a planted part's chunk is
/// written to and what `read_object` reads it back from.
#[derive(Default)]
struct Disk {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for Disk {
    async fn put_fragment(&self, id: FragmentId, bytes: Bytes, _: Option<u64>) -> StoreResult<()> {
        self.frags.lock().unwrap().insert(id, bytes);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> StoreResult<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&id).cloned())
    }

    async fn list_fragments(&self) -> StoreResult<Vec<FragmentId>> {
        Ok(self.frags.lock().unwrap().keys().copied().collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> StoreResult<()> {
        self.frags.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn health(&self) -> StoreResult<Health> {
        Ok(Health::Healthy)
    }
}

/// One server, so it is its own location authority (the trait's defaults).
impl PlacementChunkStore for Disk {}

// ─────────────────────────────── the audit seam ───────────────────────────────

/// The retirement leg's audit events, captured by a subscriber attached to each pass's future.
#[derive(Clone, Default)]
struct Audit(Arc<Mutex<Vec<HashMap<String, String>>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Audit {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() == AUDIT_TARGET {
            let mut fields = FieldText(HashMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
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

impl Audit {
    /// How many events carried `action`.
    fn count(&self, action: &str) -> usize {
        let events = self.0.lock().unwrap();
        let named = events
            .iter()
            .filter(|event| event.get("action").map(String::as_str) == Some(action));
        named.count()
    }

    fn dispatch(&self) -> Dispatch {
        Dispatch::new(tracing_subscriber::registry().with(self.clone()))
    }

    /// The hold reasons the leg named `key` with.
    fn holds(&self, key: &[u8]) -> Vec<String> {
        let key = name(key);
        let events = self.0.lock().unwrap();
        let held = events.iter().filter(|event| {
            event.get("action").map(String::as_str) == Some("retire-obligation-held")
                && event.get("key") == Some(&key)
        });
        held.filter_map(|event| event.get("hold").cloned())
            .collect()
    }
}

// ─────────────────────────────── one pass ───────────────────────────────

/// One fenced reconciliation pass with the contexts the deployed loop passes
/// (`crates/server/src/custodian.rs`, `reconcile_pass` with only a reconstruction context), built
/// afresh: a new leadership term, a new reconstruction context over `store` and `fleet`.
async fn pass_over(
    store: &Store,
    fleet: &[(DServerId, &dyn ChunkStore)],
    audit: &Audit,
) -> Result<Reconciled, ReconcileError> {
    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "zone-retirement")
        .await
        .expect("leader election over the in-memory coordination seam");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());
    let topology = Topology::default();
    let clock = ManualClock::new(NOW);
    let ctx = ReconstructionContext {
        meta: store,
        fleet,
        topology: &topology,
        unreachable: &[],
        clock: &clock,
        staged_write_window_millis: W_WRITE_MILLIS,
    };
    reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, NOW)
        .with_subscriber(audit.dispatch())
        .await
}

/// [`pass_over`] with no D server, which no drain step reads.
async fn pass(store: &Store, audit: &Audit) -> Reconciled {
    pass_over(store, &[], audit)
        .await
        .expect("the pass never fails")
}

// ─────────────────────────────── batch budgets ───────────────────────────────

/// The bytes a batch carries, preconditions counted: every precondition's key and expected value,
/// every put's key and value, every delete's key. Spelled here, not borrowed from production.
fn batch_bytes(batch: &WriteBatch) -> u64 {
    let pre: usize = batch
        .preconditions
        .iter()
        .map(|p| p.key.len() + p.expected.as_ref().map_or(0, Bytes::len))
        .sum();
    let puts: usize = batch.puts.iter().map(|(k, v)| k.len() + v.len()).sum();
    let deletes: usize = batch.deletes.iter().map(Vec::len).sum();
    (pre + puts + deletes) as u64
}

fn batch_ops(batch: &WriteBatch) -> usize {
    batch.preconditions.len() + batch.puts.len() + batch.deletes.len()
}

fn assert_within_budgets(batch: &WriteBatch) {
    assert!(
        batch_bytes(batch) <= MAX_BATCH_BYTES,
        "{} bytes",
        batch_bytes(batch)
    );
    assert!(
        batch_ops(batch) <= MAX_BATCH_OPS as usize,
        "{} ops",
        batch_ops(batch)
    );
}

/// The batches of `sent` that touch the obligation at `key` — the drain's steps on it.
fn steps_on<'a>(sent: &'a [WriteBatch], key: &[u8]) -> Vec<&'a WriteBatch> {
    sent.iter()
        .filter(|batch| batch.preconditions.iter().any(|p| p.key == key))
        .collect()
}

/// The stored cursor of the obligation at `key`, read through the codec.
fn cursor_of(key: &[u8], value: &[u8]) -> Option<mp::RetireCursor> {
    let (_, _, payload) = decode_retire_obligation(key, value).expect("the obligation decodes");
    payload.cursor().copied()
}

// ─────────────────────────────── (B-rec) ───────────────────────────────

const BUCKET: &str = "photos";
const ROOT: InodeId = 0;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
const FRESH_INODE: InodeId = 42;

/// A profile admitting `n` sessions, every per-session cap at 1 (Complete reads no budget).
fn profile_of(n: u64) -> Budget {
    let json = format!(
        "{{\"w_ref\":{},\"max_part_chunks\":1,\"max_parts_per_session\":1,\
         \"max_inflight_parts\":1,\"max_staged_chunks\":1}}",
        2 * n
    );
    md::decode(json.as_bytes()).expect("a legal profile")
}

/// Create's runtime: one fixed identity, no jitter, no wait.
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

/// Part `n`'s body: `CHUNK_LEN` bytes no other part's equals.
fn body_of(n: u32) -> Vec<u8> {
    (0..CHUNK_LEN as u32)
        .map(|i| (n.wrapping_mul(31).wrapping_add(i) % 251) as u8)
        .collect()
}

/// **(B-rec)** Parts 1 to 80 staged with their bytes on the D server, a production Complete
/// naming the odd ones (#1008): the flip installs `retire:records:{parts}` for the 40 published
/// parts and `retire:bytes:{parts}` for the 40 unnamed ones. Passes then drain the first: every
/// published part's `part:` and `psum:` record is gone, the drain wrote **no** `orphan:` record at
/// all (`0016:1503`), the object reads back byte-identical, and the unnamed parts' records and
/// their `retire:bytes:` obligation are byte-identical to before — that mode is #1062's.
#[tokio::test]
async fn b_rec_a_completes_published_part_records_are_drained_and_nothing_else_is_touched() {
    let (store, disk, audit) = (Store::new(), Disk::default(), Audit::default());
    let bucket = vec![(md::bucket_key(BUCKET), Bytes::from_static(MARKER))];
    store.plant(bucket).await;
    let object = format!("{BUCKET}/object-1");
    let identity = (
        upload(1),
        SegmentNonce::new(format!("{:032x}", 0xabc)).unwrap(),
    );
    let request = CreateRequest {
        bucket: BUCKET,
        parent: ROOT,
        object: &object,
        content_type: None,
        created_at_millis: 1_000,
        clock_source: "wall",
    };
    let created = create_session(
        &store.inner,
        &profile_of(1),
        &request,
        &mut Fixed(Some(identity)),
    )
    .await
    .unwrap();
    let CreateOutcome::Created { upload_id: id } = created else {
        panic!("expected Created, got {created:?}");
    };
    let all: Vec<u32> = (1..=80).collect();
    let mut puts = Vec::new();
    for &n in &all {
        puts.extend(plant_part(&id, 1, n));
        let chunk = chunk_of(1, n);
        let body = body_of(n);
        let fragment = encode_fragment(&FragmentHeader::new_v1(chunk, CHUNK_LEN), &body);
        let at = FragmentId { chunk, index: 0 };
        disk.put_fragment(at, Bytes::from(fragment), None)
            .await
            .unwrap();
    }
    store.plant(puts).await;
    let named: Vec<u32> = all.iter().copied().filter(|n| n % 2 == 1).collect();
    let unnamed: Vec<u32> = all.iter().copied().filter(|n| n % 2 == 0).collect();
    let list = NamedParts::new(named.iter().map(|&n| (part(n), digest_of(n))).collect()).unwrap();
    let complete = CompleteRequest {
        bucket: BUCKET,
        parent: ROOT,
        object: &object,
        parts: &list,
        fenced_at_millis: 2_000,
        clock_source: "wall",
    };
    let publish = PublishRequest {
        fresh_inode: FRESH_INODE,
        completed_at_millis: 3_000,
    };
    let outcome = complete_upload(&store.inner, &id, &complete, &publish).await;
    assert!(
        matches!(outcome, Ok(CompleteOutcome::Published(_))),
        "{outcome:?}"
    );

    let expected: Vec<u8> = named.iter().flat_map(|&n| body_of(n)).collect();
    let read = read_object(&store.inner, &disk, FRESH_INODE).await.unwrap();
    assert_eq!(read.as_deref(), Some(expected.as_slice()), "published");
    let records_key = records_key(&id);
    let bytes_key = retire_key(RetireMode::Bytes, &token(&id));
    assert!(
        store.value(&records_key).await.is_some(),
        "the flip's records obligation"
    );
    let unnamed_before = unnamed_records(&store, &id, &unnamed).await;
    let bytes_before = store
        .value(&bytes_key)
        .await
        .expect("the flip's bytes obligation");
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &disk)];

    let mut passes = 0;
    while store.value(&records_key).await.is_some() {
        passes += 1;
        assert!(passes <= 3, "the records obligation drains");
        pass_over(&store, &fleet, &audit).await.unwrap();
    }

    for &n in &named {
        assert_eq!(store.value(&part_key(&id, part(n))).await, None, "part:{n}");
        assert_eq!(store.value(&psum_key(&id, part(n))).await, None, "psum:{n}");
    }
    assert!(
        store.under(b"orphan:").await.is_empty(),
        "no orphan: record at all"
    );
    let read = read_object(&store.inner, &disk, FRESH_INODE).await.unwrap();
    assert_eq!(read.as_deref(), Some(expected.as_slice()), "byte-identical");
    let unnamed_after = unnamed_records(&store, &id, &unnamed).await;
    assert_eq!(unnamed_after, unnamed_before, "the unnamed parts' records");
    assert_eq!(
        store.value(&bytes_key).await,
        Some(bytes_before),
        "their obligation"
    );
    assert!(audit.0.lock().unwrap().is_empty(), "nothing held");
}

/// The `part:` and `psum:` records of `numbers`, as stored.
async fn unnamed_records(store: &Store, id: &UploadId, numbers: &[u32]) -> Vec<Option<Bytes>> {
    let mut records = Vec::new();
    for &n in numbers {
        records.push(store.value(&part_key(id, part(n))).await);
        records.push(store.value(&psum_key(id, part(n))).await);
    }
    assert!(
        records.iter().all(Option::is_some),
        "every unnamed record present"
    );
    records
}

// ─────────────────────────────── (C) ───────────────────────────────

/// What a drain left behind, step by step.
struct Drained {
    /// Every step's batch on the obligation, in order.
    steps: Vec<WriteBatch>,
    /// The obligation's stored value before each step.
    before: Vec<Option<Bytes>>,
    /// How many times each step's pass read a `seg:` range.
    seg_reads: Vec<usize>,
}

/// Drive passes until the obligation at `key` is gone, at most `max_passes`, and check what every
/// step must: each pass makes exactly one step on it, inside the batch budgets and the per-pass
/// bounds, and leaves its stored value changed — advanced, or gone with the last step.
async fn drain(store: &Store, key: &[u8], max_passes: usize) -> Drained {
    let audit = Audit::default();
    store.take_log();
    let mut drained = Drained {
        steps: Vec::new(),
        before: Vec::new(),
        seg_reads: Vec::new(),
    };
    while let Some(before) = store.value(key).await {
        assert!(
            drained.steps.len() < max_passes,
            "drained within {max_passes} passes"
        );
        let outcome = pass(store, &audit).await;
        assert_eq!(outcome, Reconciled::Changed);
        let log = store.take_log();
        assert_pass_bounds(&log);
        let steps = steps_on(&log.sent, key);
        assert_eq!(steps.len(), 1, "one step on the obligation per pass");
        let step = steps[0];
        assert_within_budgets(step);
        let after = store.value(key).await;
        assert_ne!(
            after.as_ref(),
            Some(&before),
            "the stored value changes every step"
        );
        drained.before.push(Some(before));
        drained.steps.push(step.clone());
        drained.seg_reads.push(log.seg_reads);
    }
    assert!(audit.0.lock().unwrap().is_empty(), "nothing held");
    drained
}

impl Drained {
    /// The obligation is deleted only by the last step, and put back (advanced) by every other.
    fn assert_deleted_only_by_the_last_step(&self, key: &[u8]) {
        let (last, rest) = self.steps.split_last().expect("at least one step");
        assert!(
            last.deletes.iter().any(|k| k == key),
            "the last step deletes it"
        );
        assert!(last.puts.is_empty(), "and puts nothing");
        for step in rest {
            assert!(
                !step.deletes.iter().any(|k| k == key),
                "not before the last"
            );
            assert_eq!(step.puts.len(), 1);
            assert_eq!(step.puts[0].0, key, "it is put back advanced");
        }
    }

    /// Each step of a `{seg}` drain deletes from the segment its stored cursor names (the first,
    /// with no cursor): it continues from the store, never from the start.
    fn assert_each_step_starts_at_the_stored_cursor(&self, key: &[u8], group: &SegmentGroup) {
        for (step, before) in self.steps.iter().zip(&self.before) {
            let first = cursor_of(key, before.as_ref().unwrap()).map_or(0, |cursor| {
                assert_eq!(cursor.component(), RetireComponent::Seg);
                cursor.entry()
            });
            assert_eq!(
                step.deletes[0],
                seg_key(group, first).unwrap(),
                "from the cursor"
            );
        }
    }

    /// Every record any step deleted besides the obligation, in order, each exactly once.
    fn records(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let deleted: Vec<Vec<u8>> = self
            .steps
            .iter()
            .flat_map(|step| step.deletes.iter().filter(|k| k.as_slice() != key).cloned())
            .collect();
        let mut sorted = deleted.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), deleted.len(), "no record deleted twice");
        deleted
    }
}

/// A second full drain is a no-op: two more passes send nothing and change nothing.
async fn assert_second_drain_is_a_no_op(store: &Store) {
    let before = store.snapshot().await;
    let audit = Audit::default();
    for _ in 0..2 {
        assert_eq!(pass(store, &audit).await, Reconciled::Satisfied);
        assert!(store.take_log().sent.is_empty(), "nothing sent");
    }
    assert_eq!(store.snapshot().await, before, "nothing changed");
}

/// The `part:`/`psum:` keys of `numbers`, in deletion order.
fn part_records(id: &UploadId, numbers: &[u32]) -> Vec<Vec<u8>> {
    numbers
        .iter()
        .flat_map(|&n| [part_key(id, part(n)), psum_key(id, part(n))])
        .collect()
}

/// Plant `numbers`' part records for the upload numbered `owner`.
async fn plant_parts(store: &Store, id: &UploadId, owner: u64, numbers: &[u32]) {
    let puts = numbers
        .iter()
        .flat_map(|&n| plant_part(id, owner, n))
        .collect();
    store.plant(puts).await;
}

/// Plant `count` segment records of `group`, each naming one chunk; their keys, in order.
async fn plant_segments(store: &Store, group: &SegmentGroup, count: u32) -> Vec<Vec<u8>> {
    let mut puts = Vec::new();
    for index in 0..count {
        let record = SegmentRecord::new(
            vec![chunk_ref(ChunkId::from(index) + 7)],
            u64::from(index) * CHUNK_LEN,
        )
        .unwrap();
        puts.push((seg_key(group, index).unwrap(), md::encode(&record)));
    }
    let keys = puts.iter().map(|(key, _)| key.clone()).collect();
    store.plant(puts).await;
    keys
}

/// **(C, `{parts}`)** A `retire:records:` `{parts}` obligation over 4,001 part numbers, no two
/// adjacent (one run each), with their records planted: drained to empty one step per pass, each
/// step deleting the members from the stored cursor on (so a drain resumed from fresh contexts
/// continues from it — and one that derived from the start would delete the first members
/// again), every record deleted exactly once, the obligation only with the last of them.
#[tokio::test]
async fn c_a_sparse_parts_obligation_drains_to_empty_from_its_stored_cursor() {
    let store = Store::new();
    let id = upload(2);
    let numbers = sparse(1, 4_001);
    plant_parts(&store, &id, 2, &numbers).await;
    let key = records_key(&id);
    let value = format!("{{\"parts\":{}}}", bare_set(&numbers));
    store.plant_obligation(key.clone(), value).await;

    let drained = drain(&store, &key, 40).await;

    assert!(drained.steps.len() > 1, "more than one batch");
    for (step, before) in drained.steps.iter().zip(&drained.before) {
        let first = match cursor_of(&key, before.as_ref().unwrap()) {
            None => numbers[0],
            Some(cursor) => {
                assert_eq!(cursor.component(), RetireComponent::Parts);
                cursor.entry()
            }
        };
        assert_eq!(
            step.deletes[0],
            part_key(&id, part(first)),
            "from the stored cursor"
        );
    }
    assert_eq!(
        drained.records(&key),
        part_records(&id, &numbers),
        "every record, once"
    );
    drained.assert_deleted_only_by_the_last_step(&key);
    assert!(store.under(b"part:").await.is_empty() && store.under(b"psum:").await.is_empty());
    assert_second_drain_is_a_no_op(&store).await;
}

/// **(C, `{seg}`)** A `retire:records:` `{seg}` obligation over more `seg:` records than
/// `MAX_BATCH_OPS`: drained page by page from the stored cursor, the group's range empty at the
/// end, the obligation deleted only with its last record.
#[tokio::test]
async fn c_a_seg_obligation_past_one_batch_drains_to_empty() {
    let store = Store::new();
    let id = upload(3);
    let group = group_of(3);
    let count = 2 * MAX_BATCH_OPS + 200;
    let keys = plant_segments(&store, &group, count).await;
    let key = records_key(&id);
    store
        .plant_obligation(key.clone(), format!("{{\"seg\":{}}}", seg_payload(&group)))
        .await;

    let drained = drain(&store, &key, 10).await;

    assert_eq!(drained.steps.len(), 3, "1,200 records in batches of 498");
    drained.assert_each_step_starts_at_the_stored_cursor(&key, &group);
    // One page per step — its own extra row says where the cursor goes — and on the step that
    // finds no record after its batch, one read of the whole range from its start.
    assert_eq!(drained.seg_reads, [1, 1, 2]);
    assert_eq!(drained.records(&key), keys, "every segment, once");
    drained.assert_deleted_only_by_the_last_step(&key);
    assert!(store.under(&md::seg_range_prefix(&group)).await.is_empty());
    assert_second_drain_is_a_no_op(&store).await;
}

/// **(C, `{seg}`, short pages)** The same drain on a store whose pages hold at most 50 rows
/// (`with_scan_cap`, which `scan_page` allows every backend), so every page a step asks for — one
/// batch and one row more — comes back cut short, with `next` set. No step takes a short page for
/// the end of the group: each reads one row past its page to find where the cursor goes, and the
/// obligation outlives every step but the last. 200 records make the last page full with nothing
/// after it, so the last step learns the group ends only from that extra read, then reads the
/// whole range from its start, and deletes the obligation with the group's last record.
#[tokio::test]
async fn c_a_seg_obligation_drains_to_empty_on_a_store_that_cuts_pages_short() {
    let store = Store::capped(50);
    let id = upload(16);
    let group = group_of(16);
    let keys = plant_segments(&store, &group, 200).await;
    let key = records_key(&id);
    store
        .plant_obligation(key.clone(), format!("{{\"seg\":{}}}", seg_payload(&group)))
        .await;

    let drained = drain(&store, &key, 10).await;

    assert_eq!(drained.steps.len(), 4, "one short page per step");
    for (n, step) in drained.steps.iter().enumerate() {
        let deleted = step.deletes.iter().filter(|k| k.as_slice() != key).count();
        assert_eq!(deleted, 50, "step {n} deleted the page it read");
    }
    for (n, before) in drained.before.iter().enumerate().skip(1) {
        let cursor = cursor_of(&key, before.as_ref().unwrap()).expect("a cursor");
        assert_eq!(
            cursor.entry(),
            50 * n as u32,
            "the row after the short page"
        );
    }
    drained.assert_each_step_starts_at_the_stored_cursor(&key, &group);
    assert_eq!(drained.seg_reads, [2, 2, 2, 3]);
    assert_eq!(drained.records(&key), keys, "every segment, once");
    drained.assert_deleted_only_by_the_last_step(&key);
    assert!(store.under(&md::seg_range_prefix(&group)).await.is_empty());
    assert_second_drain_is_a_no_op(&store).await;
}

/// **(C, combined)** A `retire:records:` `{parts, seg}` obligation — the row the decoder accepts
/// and no production writer installs yet — whose `parts` take exactly two batches. The step that
/// deletes the last part record leaves the obligation in place with its cursor at the start of
/// `seg`; a drain resumed from fresh contexts exactly there continues into the segments; the
/// obligation goes only with the last segment record.
#[tokio::test]
async fn c_a_combined_obligation_crosses_components_at_a_commit_boundary() {
    let store = Store::new();
    let id = upload(4);
    let group = group_of(4);
    let per_step = (MAX_BATCH_OPS as usize - 2) / 2;
    let numbers = sparse(1, 2 * per_step as u32);
    plant_parts(&store, &id, 4, &numbers).await;
    let segments = plant_segments(&store, &group, MAX_BATCH_OPS + 100).await;
    let key = records_key(&id);
    let value = format!(
        "{{\"parts\":{},\"seg\":{}}}",
        bare_set(&numbers),
        seg_payload(&group)
    );
    store.plant_obligation(key.clone(), value).await;
    let audit = Audit::default();

    // The parts' two batches.
    for _ in 0..2 {
        assert_eq!(pass(&store, &audit).await, Reconciled::Changed);
    }
    assert!(
        store.under(b"part:").await.is_empty(),
        "every part record gone"
    );
    assert!(store.under(b"psum:").await.is_empty());
    let at_boundary = store
        .value(&key)
        .await
        .expect("the obligation outlives its parts");
    let cursor = cursor_of(&key, &at_boundary).expect("a cursor");
    assert_eq!(
        (cursor.component(), cursor.entry()),
        (RetireComponent::Seg, 0),
        "its cursor in the second component"
    );
    let left = store.under(&md::seg_range_prefix(&group)).await;
    assert_eq!(left.len(), segments.len(), "no segment touched yet");
    store.take_log();

    // Resumed from fresh contexts at the boundary: the rest is the segments'.
    let drained = drain(&store, &key, 10).await;

    assert_eq!(
        drained.steps[0].deletes[0], segments[0],
        "into the second component"
    );
    assert_eq!(drained.records(&key), segments);
    drained.assert_deleted_only_by_the_last_step(&key);
    assert!(store.under(&md::seg_range_prefix(&group)).await.is_empty());
    assert_second_drain_is_a_no_op(&store).await;
}

// ─────────────────────────────── (L) ───────────────────────────────

/// The built constants are the bounds this file holds every pass to, so changing one is a
/// deliberate change to the other.
#[test]
fn the_built_bounds_are_the_promised_ones() {
    assert_eq!(VISITS_PER_PASS, VISIT_BOUND);
    assert_eq!(LEDGER_PAGE, 16);
    assert_eq!(LEDGER_PAGES_PER_PASS, LEDGER_PAGE_BOUND);
    assert_eq!(STEPS_PER_PASS, STEP_BOUND);
    assert_eq!(READS_PER_STEP, 4);
    assert_eq!(READS_PER_PASS, READ_BOUND);
    assert_eq!(READ_BOUND, 70);
    assert_eq!(COMMITS_PER_PASS, COMMIT_BOUND);
}

/// A pass stayed within the leg's per-pass bounds: obligations visited, ledger pages, steps,
/// reads, commits.
fn assert_pass_bounds(log: &Log) {
    assert!(
        log.ledger_entries <= VISIT_BOUND,
        "{} obligations visited",
        log.ledger_entries
    );
    assert!(
        log.ledger_pages <= LEDGER_PAGE_BOUND,
        "{} ledger pages",
        log.ledger_pages
    );
    let steps = log
        .sent
        .iter()
        .filter(|batch| {
            batch
                .preconditions
                .iter()
                .any(|p| p.key.starts_with(b"retire:"))
        })
        .count();
    assert!(steps <= STEP_BOUND, "{steps} steps");
    assert!(log.reads <= READ_BOUND, "{} reads", log.reads);
    assert!(log.sent.len() <= COMMIT_BOUND, "{} commits", log.sent.len());
}

/// Plant one `{parts: [[n,n]]}` obligation for the upload numbered `owner`, with its records.
async fn plant_one_part_obligation(store: &Store, owner: u64) -> Vec<u8> {
    let id = upload(owner);
    plant_parts(store, &id, owner, &[1]).await;
    let key = records_key(&id);
    store
        .plant_obligation(key.clone(), "{\"parts\":[[1,1]]}".to_owned())
        .await;
    key
}

/// Plant one `retire:bytes:` `{parts: [[1,1]]}` obligation for the upload numbered `owner`: one
/// this build visits and leaves as it is.
async fn plant_bytes_obligation(store: &Store, owner: u64) -> Vec<u8> {
    let key = retire_key(RetireMode::Bytes, &token(&upload(owner)));
    store
        .plant_obligation(key.clone(), "{\"parts\":[[1,1]]}".to_owned())
        .await;
    key
}

/// `count` one-step `{parts}` obligations, for the uploads numbered from `first`; their keys, in
/// key order.
async fn one_step_backlog(store: &Store, first: u64, count: usize) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    for owner in first..first + count as u64 {
        keys.push(plant_one_part_obligation(store, owner).await);
    }
    assert!(
        keys.windows(2).all(|pair| pair[0] < pair[1]),
        "in key order"
    );
    keys
}

/// The `retire:records:` obligations still stored, in key order.
async fn records_obligations(store: &Store) -> Vec<Vec<u8>> {
    store.under(b"retire:records:").await.into_keys().collect()
}

/// **(L1, `{parts}`)** A productive backlog three times the visit bound, each obligation one step
/// from done, behind eight `retire:bytes:` obligations this build leaves alone — so the second
/// ledger page a pass reads holds more records obligations than the pass has steps left. Every
/// pass stays within the bounds, takes its sixteen steps in key order, and stops at the first
/// obligation it has no step for instead of passing it by: after pass `k` exactly the first `16k`
/// are gone. With the backlog gone, the pass that resumed at its end answers `Partial`, and the
/// next, reading the whole ledger from its head, `Satisfied`.
#[tokio::test]
async fn l1_a_productive_backlog_drains_in_key_order_within_fixed_per_pass_bounds() {
    let store = Store::new();
    for owner in 1..=8 {
        plant_bytes_obligation(&store, owner).await;
    }
    let bytes_before = store.under(b"retire:bytes:").await;
    let backlog = 3 * VISIT_BOUND;
    let keys = one_step_backlog(&store, 101, backlog).await;
    let audit = Audit::default();
    store.take_log();

    for k in 1..=backlog / STEP_BOUND {
        assert_eq!(pass(&store, &audit).await, Reconciled::Changed, "pass {k}");
        let log = store.take_log();
        assert_pass_bounds(&log);
        // The first pass reads two pages (eight bytes obligations and eight records ones, then
        // sixteen records ones, of which it steps eight); every later pass one page of sixteen.
        // None reads a page it has no step for.
        let pages = if k == 1 { 2 } else { 1 };
        assert_eq!(log.ledger_pages, pages, "pass {k}");
        let left = records_obligations(&store).await;
        assert_eq!(
            left,
            keys[k * STEP_BOUND..],
            "pass {k}: the first {} gone",
            k * 16
        );
    }

    assert_eq!(
        pass(&store, &audit).await,
        Reconciled::Partial,
        "resumed at the end"
    );
    assert_eq!(
        pass(&store, &audit).await,
        Reconciled::Satisfied,
        "from the head"
    );
    assert_eq!(store.under(b"retire:bytes:").await, bytes_before);
    assert!(store.under(b"part:").await.is_empty());
    assert!(audit.0.lock().unwrap().is_empty(), "nothing held");
}

/// **(L1, `{seg}`)** A productive backlog of `{seg}` obligations three times the visit bound, each
/// over a three-record group and one step from done; every second one carries a cursor at
/// segment 1, its segment 0 already gone. These steps read: one page of the group each, and the
/// ones that start past segment 0 one more, the group's whole range from its start. Every pass
/// stays within the bounds, takes sixteen steps, and reads exactly what those steps need.
#[tokio::test]
async fn l1_a_seg_backlog_reads_within_fixed_per_pass_bounds() {
    let store = Store::new();
    let backlog = 3 * VISIT_BOUND;
    let mut resumed = Vec::new();
    for owner in 201..201 + backlog as u64 {
        let group = group_of(owner);
        let segments = plant_segments(&store, &group, 3).await;
        let key = records_key(&upload(owner));
        let value = if owner % 2 == 0 {
            store.remove(&segments[0]).await;
            resumed.push(key.clone());
            format!(
                "{{\"seg\":{},\"cursor\":{{\"component\":\"seg\",\"entry\":1}}}}",
                seg_payload(&group)
            )
        } else {
            format!("{{\"seg\":{}}}", seg_payload(&group))
        };
        store.plant_obligation(key, value).await;
    }
    let audit = Audit::default();
    store.take_log();

    for k in 1..=backlog / STEP_BOUND {
        let before = records_obligations(&store).await;
        assert_eq!(pass(&store, &audit).await, Reconciled::Changed, "pass {k}");
        let log = store.take_log();
        assert_pass_bounds(&log);
        let after = records_obligations(&store).await;
        let gone: Vec<&Vec<u8>> = before.iter().filter(|key| !after.contains(key)).collect();
        assert_eq!(gone.len(), STEP_BOUND, "pass {k}");
        let needed: usize = gone
            .iter()
            .map(|key| if resumed.contains(key) { 2 } else { 1 })
            .sum();
        assert_eq!(log.seg_reads, needed, "pass {k}: the reads its steps need");
    }

    assert!(records_obligations(&store).await.is_empty());
    assert!(store.under(b"seg:").await.is_empty());
    assert!(audit.0.lock().unwrap().is_empty(), "nothing held");
}

/// **(L1, short pages)** A one-step `{parts}` backlog three times the visit bound on a store whose
/// pages hold at most three rows. The walk's pages come back short, so neither the visits nor the
/// steps end a pass but the page bound: five pages, fifteen obligations. Every pass stays within
/// the bounds, and the backlog drains in key order.
#[tokio::test]
async fn l1_short_ledger_pages_make_the_page_bound_end_a_pass() {
    let store = Store::capped(3);
    let backlog = 3 * VISIT_BOUND;
    let keys = one_step_backlog(&store, 301, backlog).await;
    let audit = Audit::default();
    let per_pass = 3 * LEDGER_PAGE_BOUND;
    store.take_log();

    for k in 1..=backlog.div_ceil(per_pass) {
        assert_eq!(pass(&store, &audit).await, Reconciled::Changed, "pass {k}");
        let log = store.take_log();
        assert_pass_bounds(&log);
        assert_eq!(log.ledger_pages, LEDGER_PAGE_BOUND, "pass {k}");
        let left = records_obligations(&store).await;
        let gone = (k * per_pass).min(backlog);
        assert_eq!(left, keys[gone..], "pass {k}: the first {gone} gone");
    }
    assert!(store.under(b"part:").await.is_empty());
}

/// **(L2)** Three visit bounds' worth of obligations whose values will not decode, sorting before
/// one productive obligation: every pass stays within the same bounds, each held obligation stays
/// byte-identical and is named with the `undecodable` hold, and the productive one drains within
/// `⌈(held + 1) / VISIT_BOUND⌉` passes — the walk resumes past what it held instead of
/// restarting at the head.
#[tokio::test]
async fn l2_a_held_prefix_neither_breaks_the_bounds_nor_starves_what_follows() {
    let store = Store::new();
    let held = 3 * VISIT_BOUND;
    let mut held_keys = Vec::new();
    for owner in 1..=held as u64 {
        let key = records_key(&upload(owner));
        store
            .plant(vec![(
                key.clone(),
                Bytes::from_static(b"not an obligation"),
            )])
            .await;
        held_keys.push(key);
    }
    let productive = plant_one_part_obligation(&store, held as u64 + 1).await;
    assert!(
        held_keys.iter().all(|key| *key < productive),
        "the held prefix sorts first"
    );
    let audit = Audit::default();
    let passes = (held + 1).div_ceil(VISIT_BOUND);

    for _ in 0..passes {
        let outcome = pass(&store, &audit).await;
        let expected = [Reconciled::Blocked, Reconciled::Changed];
        assert!(expected.contains(&outcome), "{outcome:?}");
        assert_pass_bounds(&store.take_log());
        for key in &held_keys {
            let value = store.value(key).await;
            assert_eq!(
                value.as_deref(),
                Some(&b"not an obligation"[..]),
                "byte-identical"
            );
        }
    }

    assert_eq!(
        store.value(&productive).await,
        None,
        "drained in {passes} passes"
    );
    for key in &held_keys {
        let holds = audit.holds(key);
        assert!(!holds.is_empty(), "{} named", name(key));
        assert!(holds.iter().all(|hold| hold == "undecodable"), "{holds:?}");
    }
}

// ─────────────────────────────── (S) ───────────────────────────────

/// A `retire:records:` `{parts}` obligation over `numbers` for `id`, its cursor at `numbers[n]`,
/// in the codec's own spelling.
fn parts_at(id: &UploadId, numbers: &[u32], n: usize) -> Bytes {
    let value = format!(
        "{{\"parts\":{},\"cursor\":{{\"component\":\"parts\",\"entry\":{}}}}}",
        bare_set(numbers),
        numbers[n]
    );
    let key = records_key(id);
    let (_, _, payload) = decode_retire_obligation(&key, value.as_bytes()).expect("valid");
    assert_eq!(
        md::encode(&payload),
        value.as_bytes(),
        "the canonical spelling"
    );
    Bytes::from(value)
}

/// **(S, resumed)** 63 `retire:bytes:` obligations, then one `retire:records:` obligation over 600
/// sparse parts, three steps' worth. Each pass that starts at the head spends its last visit on
/// one step of it, so the pass after resumes past it, at the end of the ledger. That pass read
/// nothing of the ledger before where it resumed — the half-drained obligation is there — so it
/// answers `Partial`, never `Satisfied`, and the walk returns to the head. Only a pass that reads
/// the whole ledger from its head, once the obligation and its records are gone, certifies it.
#[tokio::test]
async fn s_a_pass_that_resumed_part_way_does_not_certify_the_ledger() {
    let store = Store::new();
    for owner in 1..=63 {
        plant_bytes_obligation(&store, owner).await;
    }
    let id = upload(64);
    let numbers = sparse(1, 600);
    plant_parts(&store, &id, 64, &numbers).await;
    let key = records_key(&id);
    let value = format!("{{\"parts\":{}}}", bare_set(&numbers));
    store.plant_obligation(key.clone(), value).await;
    let audit = Audit::default();

    let mut outcomes = Vec::new();
    while outcomes.last() != Some(&Reconciled::Satisfied) {
        assert!(outcomes.len() < 10, "{outcomes:?}");
        outcomes.push(pass(&store, &audit).await);
        let satisfied = outcomes.last() == Some(&Reconciled::Satisfied);
        let owed = store.value(&key).await.is_some();
        assert!(
            !(satisfied && owed),
            "certified with work left: {outcomes:?}"
        );
    }

    let (changed, partial) = (Reconciled::Changed, Reconciled::Partial);
    let expected = [changed, partial, changed, partial, changed, partial];
    assert_eq!(outcomes[..6], expected, "{outcomes:?}");
    assert!(store.under(b"part:").await.is_empty());
    assert!(store.under(b"psum:").await.is_empty());
    assert!(audit.0.lock().unwrap().is_empty(), "nothing held");
}

/// **(S, contended)** One `retire:records:` obligation, the last in the ledger, and another writer
/// that moves it before every commit conditioned on it — a second drainer that keeps winning.
/// Every step the pass plans loses its precondition and re-reads. When the steps are spent the
/// obligation is still there, and the pass — though it read the whole ledger from its head —
/// answers `Partial`, never `Satisfied`, within the per-pass bounds. Nothing a lost batch carried
/// landed. With the other writer gone, the next pass drains from where it left the obligation.
#[tokio::test]
async fn s_a_pass_whose_every_step_lost_its_precondition_does_not_certify() {
    let store = Store::new();
    let id = upload(65);
    let numbers = sparse(1, 600);
    plant_parts(&store, &id, 65, &numbers).await;
    let key = records_key(&id);
    let value = format!("{{\"parts\":{}}}", bare_set(&numbers));
    store.plant_obligation(key.clone(), value).await;
    let values = [parts_at(&id, &numbers, 100), parts_at(&id, &numbers, 200)];
    let contender = Contender {
        key: key.clone(),
        values,
        turns: 0,
    };
    *store.contender.lock().unwrap() = Some(contender);
    let audit = Audit::default();
    store.take_log();

    let outcome = pass(&store, &audit).await;

    assert_eq!(outcome, Reconciled::Partial);
    let log = store.take_log();
    assert_pass_bounds(&log);
    assert_eq!(
        steps_on(&log.sent, &key).len(),
        STEP_BOUND,
        "every step lost"
    );
    let records = store.under(&mp::part_range(&id)).await.len();
    assert_eq!(
        records,
        numbers.len(),
        "nothing a lost batch carried landed"
    );
    let moved = store.value(&key).await.expect("still owed");
    assert!(audit.0.lock().unwrap().is_empty(), "nothing held");

    *store.contender.lock().unwrap() = None;
    assert_eq!(pass(&store, &audit).await, Reconciled::Changed);
    let sent = store.take_log().sent;
    let steps = steps_on(&sent, &key);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].preconditions[0].expected.as_ref(), Some(&moved));
    let from = cursor_of(&key, &moved)
        .expect("the other writer's cursor")
        .entry();
    assert_eq!(steps[0].deletes[0], part_key(&id, part(from)));
}

// ─────────────────────────────── (W) ───────────────────────────────

/// The part numbers of an obligation whose value is exactly `MAX_VALUE_BYTES` long once a cursor
/// on a six-digit member is added: four five-digit runs, the run `[99999,100000]`, then 6,242
/// six-digit ones — 99,954 bytes bare, and the cursor
/// `,"cursor":{"component":"parts","entry":NNNNNN}` is 46 more.
fn at_the_value_ceiling() -> Vec<u32> {
    let mut numbers = vec![90_001, 90_003, 90_005, 90_007, 99_999, 100_000];
    numbers.extend(sparse(100_002, 6_242));
    numbers
}

/// **(W)** Under a store that refuses any put past `MAX_VALUE_BYTES`: a `{parts}` obligation at
/// exactly the installer ceiling (6,245 six-digit part numbers, no two adjacent: 99,931 bytes)
/// drains to empty, its cursor always fitting. One installed without that room, whose advanced
/// value is **exactly** `MAX_VALUE_BYTES`, drains too: the store takes a value of that size, so
/// the drain sends it. One planted just under `MAX_VALUE_BYTES` (6,249 numbers, 99,995 bytes),
/// sorting first, cannot carry a cursor: it is held byte-identical with the `value-past-ceiling`
/// hold, its over-ceiling put is never sent, no pass fails, and the ones after it drain in those
/// same passes.
#[tokio::test]
async fn w_an_obligation_whose_cursor_cannot_fit_is_held_and_the_next_one_drains() {
    let store = Store::refusing_past_ceiling();
    let (over_id, fits_id, edge_id) = (upload(5), upload(6), upload(17));
    let over_numbers = sparse(100_000, 6_249);
    let fits_numbers = sparse(300_000, 6_245);
    let edge_numbers = at_the_value_ceiling();
    plant_parts(&store, &over_id, 5, &over_numbers).await;
    plant_parts(&store, &fits_id, 6, &fits_numbers).await;
    plant_parts(&store, &edge_id, 17, &edge_numbers).await;
    let (over_key, fits_key, edge_key) = (
        records_key(&over_id),
        records_key(&fits_id),
        records_key(&edge_id),
    );
    assert!(over_key < fits_key, "the held one sorts first");
    assert!(over_key < edge_key);
    let over_value = format!("{{\"parts\":{}}}", bare_set(&over_numbers));
    let fits_value = format!("{{\"parts\":{}}}", bare_set(&fits_numbers));
    let edge_value = format!("{{\"parts\":{}}}", bare_set(&edge_numbers));
    assert_eq!(over_value.len(), 99_995);
    assert_eq!(fits_value.len(), RETIRE_VALUE_CEILING);
    assert_eq!(RETIRE_VALUE_CEILING, 99_931);
    assert_eq!(edge_value.len(), 99_954);
    store
        .plant_obligation(over_key.clone(), over_value.clone())
        .await;
    store.plant_obligation(fits_key.clone(), fits_value).await;
    store.plant_obligation(edge_key.clone(), edge_value).await;
    let audit = Audit::default();
    let per_step = (MAX_BATCH_OPS as usize - 2) / 2;
    let passes = fits_numbers.len().div_ceil(per_step);
    assert_eq!(edge_numbers.len().div_ceil(per_step), passes);
    let mut edge_puts = Vec::new();

    for _ in 0..passes {
        let outcome = pass_over(&store, &[], &audit).await;
        assert_eq!(outcome.ok(), Some(Reconciled::Blocked), "no pass fails");
        let log = store.take_log();
        for batch in &log.sent {
            assert_within_budgets(batch);
            assert!(
                batch
                    .puts
                    .iter()
                    .all(|(_, value)| value.len() <= MAX_VALUE_BYTES),
                "no over-ceiling put is ever sent"
            );
        }
        for batch in steps_on(&log.sent, &edge_key) {
            edge_puts.extend(batch.puts.iter().map(|(_, value)| value.len()));
        }
        let value = store.value(&over_key).await;
        assert_eq!(
            value.as_deref(),
            Some(over_value.as_bytes()),
            "held byte-identical"
        );
    }

    for key in [&fits_key, &edge_key] {
        assert_eq!(store.value(key).await, None, "drained in {passes} passes");
    }
    assert!(store.under(&mp::part_range(&fits_id)).await.is_empty());
    assert!(store.under(&mp::part_range(&edge_id)).await.is_empty());
    assert_eq!(
        edge_puts.iter().max(),
        Some(&MAX_VALUE_BYTES),
        "a value of exactly the store's ceiling was sent, and landed"
    );
    assert_eq!(
        store.under(&mp::part_range(&over_id)).await.len(),
        over_numbers.len(),
        "the held obligation's records untouched"
    );
    let holds = audit.holds(&over_key);
    assert_eq!(holds.len(), passes, "named every pass");
    assert!(
        holds.iter().all(|hold| hold == "value-past-ceiling"),
        "{holds:?}"
    );
    assert!(audit.holds(&fits_key).is_empty());
    assert!(audit.holds(&edge_key).is_empty());
}

// ─────────────────────────────── (G-rec) ───────────────────────────────

/// A session record over segment group `nonce` in `state` at `epoch`, in the codec's own spelling.
fn session(nonce: &str, state: &str, epoch: u64) -> Bytes {
    let bytes = format!(
        "{{\"parent\":42,\"object\":\"o\",\"created_at_millis\":100,\"clock_source\":\"wall\",\
         \"segment_nonce\":\"{nonce}\",\"epoch\":{epoch},\"attempts\":1,\"state\":{state}}}"
    );
    let record = mp::decode_session_record(bytes.as_bytes()).expect(state);
    assert_eq!(md::encode(&record), bytes.as_bytes());
    Bytes::from(bytes)
}

/// The production post-restore pass over `store` and four empty D servers.
async fn restore_pass(store: &Store, disks: &[Disk; 4]) -> RestoreReport {
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [
        (0, &disks[0]),
        (1, &disks[1]),
        (2, &disks[2]),
        (3, &disks[3]),
    ];
    let ctx = GcContext {
        meta: &store.inner,
        fleet: &fleet,
        grace_window_millis: 50,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    reconcile_after_restore(&ctx, NOW).await.unwrap()
}

/// **(G-rec)** A `Completing@1` session that wrote more segments than one drain batch holds,
/// each naming a chunk one of its parts holds, fenced to `Aborting@2` by the production
/// post-restore pass — which installs `retire:records:{seg}` for that attempt (`0016:665`).
/// Its drain runs **one step at a time**, and after every step the production
/// `reconcile_after_restore` names nothing of the session — in particular no segment with no
/// deleter — until the obligation is gone with its last record. The fence's `retire:bytes:`
/// obligation is never touched.
#[tokio::test]
async fn g_rec_a_restore_fenced_attempt_is_drained_with_restore_naming_nothing_between_steps() {
    let (store, disks, audit) = (Store::new(), <[Disk; 4]>::default(), Audit::default());
    let id = upload(7);
    let group = group_of(7);
    let nonce = group.nonce().to_string();
    let completing = format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":{},\
         \"publish_target\":{{\"parent\":42,\"name\":\"o\",\"epoch\":{EPOCH}}}}}",
        MAX_BATCH_OPS + 400
    );
    let parts: Vec<u32> = (1..=7).collect();
    let mut puts = vec![(mp::mpu_key(&id), session(&nonce, &completing, EPOCH))];
    for &n in &parts {
        puts.extend(plant_part(&id, 7, n));
    }
    store.plant(puts).await;
    let mut segments = Vec::new();
    for index in 0..MAX_BATCH_OPS + 400 {
        let chunk = chunk_ref(chunk_of(7, 1 + index % 7));
        let record = SegmentRecord::new(vec![chunk], u64::from(index) * CHUNK_LEN).unwrap();
        segments.push((seg_key(&group, index).unwrap(), md::encode(&record)));
    }
    store.plant(segments).await;

    let fenced = restore_pass(&store, &disks).await;
    let mpu = name(&mp::mpu_key(&id));
    assert!(!format!("{fenced:?}").contains(&mpu), "{fenced:?}");
    let records = records_key(&id);
    let bytes = retire_key(RetireMode::Bytes, &token(&id));
    let owed = store
        .value(&records)
        .await
        .expect("the fence's {seg} obligation");
    let (_, _, payload) = decode_retire_obligation(&records, &owed).unwrap();
    assert_eq!(payload.segments(), Some(&group));
    let bytes_owed = store
        .value(&bytes)
        .await
        .expect("the fence's bytes obligation");
    let range = md::seg_range_prefix(&group);

    let mut steps = 0;
    while store.value(&records).await.is_some() {
        steps += 1;
        assert!(steps <= 4, "drained");
        let left = store.under(&range).await.len();
        pass(&store, &audit).await;
        let after = store.under(&range).await.len();
        assert!(after < left, "one step deleted segments");

        let report = restore_pass(&store, &disks).await;
        assert!(
            !format!("{report:?}").contains(&mpu),
            "step {steps}: {report:?}"
        );
        assert!(report.segments_unaccounted.is_empty(), "{report:?}");
        let present = store.value(&records).await.is_some();
        assert_eq!(
            present,
            after > 0,
            "the obligation goes only with its last record"
        );
    }

    assert!(steps > 1, "more than one step");
    assert!(store.under(&range).await.is_empty());
    assert_eq!(
        store.value(&bytes).await,
        Some(bytes_owed),
        "the bytes obligation untouched"
    );
    assert_eq!(store.under(&mp::part_range(&id)).await.len(), parts.len());
}

// ─────────────────────────────── (K, `{seg}`) ───────────────────────────────

/// **(K, `{seg}`)** A `{seg}` obligation whose stored cursor stands past the group's last record
/// while records remain below it: decode cannot judge it (the payload names no extent), so the
/// drain must, by reading the group's whole range from its start. It is held — the obligation
/// byte-identical, every record in place, the `segment-cursor-past-records` hold named — never
/// taken for a finished group.
#[tokio::test]
async fn k_a_seg_cursor_past_the_last_record_with_records_below_it_is_held() {
    let store = Store::new();
    let id = upload(8);
    let group = group_of(8);
    let keys = plant_segments(&store, &group, 10).await;
    let key = records_key(&id);
    let value = format!(
        "{{\"seg\":{},\"cursor\":{{\"component\":\"seg\",\"entry\":50}}}}",
        seg_payload(&group)
    );
    store.plant_obligation(key.clone(), value.clone()).await;
    let audit = Audit::default();

    let outcome = pass(&store, &audit).await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert_eq!(store.value(&key).await.as_deref(), Some(value.as_bytes()));
    let left: Vec<Vec<u8>> = store
        .under(&md::seg_range_prefix(&group))
        .await
        .into_keys()
        .collect();
    assert_eq!(left, keys, "every record in place");
    let holds = audit.holds(&key);
    assert_eq!(holds, ["segment-cursor-past-records"]);
    assert!(store.take_log().sent.is_empty(), "nothing sent");
}

// ─────────────────────────────── the other holds ───────────────────────────────

/// **(H, commit refused)** A store that refuses one obligation's batch outright: that obligation
/// is held with the `commit-failed` hold, byte-identical, and the pass goes on — the obligation
/// after it drains in the same pass, and the pass does not fail. One obligation's fault holds that
/// obligation only.
#[tokio::test]
async fn h_a_refused_commit_holds_its_obligation_only() {
    let store = Store::new();
    let refused = plant_one_part_obligation(&store, 11).await;
    let next = plant_one_part_obligation(&store, 12).await;
    assert!(refused < next);
    let before = store.value(&refused).await;
    *store.failing.lock().unwrap() = Some(refused.clone());
    let audit = Audit::default();

    let outcome = pass(&store, &audit).await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert_eq!(store.value(&refused).await, before, "held byte-identical");
    assert_eq!(audit.holds(&refused), ["commit-failed"]);
    assert_eq!(
        store.value(&next).await,
        None,
        "the next one drained in the same pass"
    );
    assert!(store.under(&mp::part_range(&upload(12))).await.is_empty());
    assert_eq!(store.under(&mp::part_range(&upload(11))).await.len(), 1);
}

/// **(H, precondition lost)** Another writer advances the obligation between the drain's read and
/// its commit (a second drainer's step, here). The commit's precondition fails, nothing of it
/// lands, and the drain reads the obligation again and plans from what is there: its second batch
/// requires the other writer's bytes and starts at the other writer's cursor. When the other
/// writer instead finished the obligation, the drain reads it gone and stops.
#[tokio::test]
async fn h_a_lost_precondition_rereads_and_replans() {
    let store = Store::new();
    let id = upload(13);
    let numbers = sparse(1, 600);
    plant_parts(&store, &id, 13, &numbers).await;
    let key = records_key(&id);
    let value = format!("{{\"parts\":{}}}", bare_set(&numbers));
    store.plant_obligation(key.clone(), value.clone()).await;
    let moved = format!(
        "{{\"parts\":{},\"cursor\":{{\"component\":\"parts\",\"entry\":{}}}}}",
        bare_set(&numbers),
        numbers[300]
    );
    decode_retire_obligation(&key, moved.as_bytes()).expect("a valid advanced obligation");
    let other = WriteBatch::new().put(key.clone(), Bytes::from(moved.clone()));
    *store.before_next_commit.lock().unwrap() = Some(other);
    let audit = Audit::default();

    assert_eq!(pass(&store, &audit).await, Reconciled::Changed);

    let steps = store.take_log().sent;
    let steps = steps_on(&steps, &key);
    assert_eq!(steps.len(), 2, "the lost commit and the re-planned one");
    assert_eq!(
        steps[0].preconditions[0].expected.as_deref(),
        Some(value.as_bytes())
    );
    assert_eq!(
        steps[1].preconditions[0].expected.as_deref(),
        Some(moved.as_bytes())
    );
    assert_eq!(steps[1].deletes[0], part_key(&id, part(numbers[300])));
    assert!(
        store
            .value(&part_key(&id, part(numbers[0])))
            .await
            .is_some(),
        "the lost batch left nothing"
    );
    assert!(audit.0.lock().unwrap().is_empty());

    // The other writer finishes it instead: the drain finds it gone and stops.
    let gone = WriteBatch::new().delete(key.clone());
    *store.before_next_commit.lock().unwrap() = Some(gone);
    pass(&store, &audit).await;
    let sent = store.take_log().sent;
    assert_eq!(
        steps_on(&sent, &key).len(),
        1,
        "no re-plan over an obligation that is gone"
    );
    assert_eq!(store.value(&key).await, None);
}

/// **(H, resume point)** A resume point that is not a `retire:` key is named on the audit seam,
/// the walk starts from the head of the ledger, drains what it finds there, and rewrites the
/// record to the head.
#[tokio::test]
async fn h_an_unusable_resume_point_restarts_the_walk_at_the_head() {
    let store = Store::new();
    let key = plant_one_part_obligation(&store, 14).await;
    let damaged = vec![(
        LEDGER_CURSOR_KEY.to_vec(),
        Bytes::from_static(b"orphan:not-ours"),
    )];
    store.plant(damaged).await;
    let audit = Audit::default();

    assert_eq!(pass(&store, &audit).await, Reconciled::Changed);

    assert_eq!(audit.count("unusable-retire-cursor"), 1);
    assert_eq!(store.value(&key).await, None, "drained from the head");
    let resume = store.value(LEDGER_CURSOR_KEY).await;
    assert_eq!(resume.as_deref(), Some(&b""[..]), "rewritten to the head");
}

/// **(H, stray key)** A key under a `{seg}` group's range that is not one of its segments' keys
/// is held with the `malformed-segment-key` hold: nothing of the group is deleted, the obligation
/// is byte-identical.
#[tokio::test]
async fn h_a_stray_key_in_a_segment_range_is_held() {
    let store = Store::new();
    let id = upload(15);
    let group = group_of(15);
    plant_segments(&store, &group, 3).await;
    let mut stray = md::seg_range_prefix(&group);
    stray.extend_from_slice(b"00000x");
    store
        .plant(vec![(stray, Bytes::from_static(b"stray"))])
        .await;
    let key = records_key(&id);
    let value = format!("{{\"seg\":{}}}", seg_payload(&group));
    store.plant_obligation(key.clone(), value.clone()).await;
    let audit = Audit::default();

    assert_eq!(pass(&store, &audit).await, Reconciled::Blocked);

    assert_eq!(store.value(&key).await.as_deref(), Some(value.as_bytes()));
    assert_eq!(store.under(&md::seg_range_prefix(&group)).await.len(), 4);
    assert_eq!(audit.holds(&key), ["malformed-segment-key"]);
}
