//! Issue #1062 — the custodian drains `retire:bytes:` obligations: every fragment position an
//! obligation names is orphan-marked, and only then is the record that named it deleted (proposal
//! 0016 decision 4 rule 2, `0016:1123`). Driven **only** through `reconcile_step` with the contexts
//! the deployed loop passes (`crates/server/src/custodian.rs`, `run_reconstruction_until`: a
//! reconstruction pass, every other input `None`, its `now_millis` and its context's clock one
//! source; GC a pass of its own), over a real metadata backend (`RedbMetadataStore::in_memory()`)
//! behind a recording wrapper, and a placement-aware chunk-store double. Every pass builds its
//! leadership fence and context afresh, as the deployed loop does.
//!
//! * **(B)** A production subset Complete (#1008), then passes: no `orphan:` mark names a chunk of
//!   the published map; every position of every unnamed part is marked in a batch **before** the
//!   batch deleting its `part:` record; no `part:` or `psum:` record of the session remains; and
//!   after passes past the orphan grace the unnamed parts' fragments are gone.
//! * **(T)** A wrapper that, while the pass's reconstruction loop runs, moves the clock past the
//!   grace and installs an obligation: every mark the drain writes for it is stamped no earlier
//!   than the installation, and a GC pass straight after reclaims none of its fragments.
//! * **(C′)** The mark guard: a `reclaiming` mark (another event's) on a position no record the
//!   drain keeps names stays byte-identical, the cursor stops at or before it, real GC passes
//!   finish it, and the next step marks it afresh and removes the obligation. Then the three arms,
//!   and a mark that is none of the three shapes: held, never re-stamped.
//! * **(H)** A `reclaiming` mark over a position a `part:` record the drain keeps names: a typed,
//!   audited hold every step, GC never moves, nothing changes, no pass fails, and an obligation
//!   later in the ledger still drains.
//! * **(D)** `{chunks}` from a part re-upload, a flat `{generation}` from a Complete over a flat
//!   object, `{session, all}` from a client Abort with committed parts and no residue: each drains
//!   to completion, marks before deletes, and its fragments are gone after the grace.
//! * **(E-hold)** A `part:` record whose placement is not its scheme's fragment count is never
//!   marked from or deleted; the obligation stays; GC keeps every fragment of the chunk. The same
//!   for such a chunk in a `{chunks}` or flat `{generation}` list, and in a `seg:` record.
//! * **(R)** A `{session, all}` obligation whose session still owns a `sidx:` entry is held,
//!   byte-identical, the entry kept.
//! * **(S)** A part, and a segmented generation's segment, with more positions than one step can
//!   mark: the cursor advances inside the entry every step, and each finishes. The segmented
//!   generation's marks follow each `seg:` record's current placement.
//! * **(G)** A `Completing` session fenced by the production restore pass, drained one step at a
//!   time: after every step the production `reconcile_after_restore` names nothing of it, until
//!   both its obligations are gone.
//! * **(paging)** Part ranges, part-number sets and segment groups longer than one page of
//!   sixteen, including a group of segments small enough that one step finishes a whole page.
//!
//! Holds are read through stored records and the audit seam, never through a type this change
//! adds. Every bound is stated here as a literal. The audit seam is read through a subscriber
//! attached to each pass's future, never a global one.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::Dispatch;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self as md, decode_orphan_mark, encode_orphan_mark, orphan_key, seg_key, ChunkMap, ChunkRef,
    EcScheme, InodeId, InodeRecord, OrphanMark, SegmentGroup, SegmentNonce, SegmentRecord,
};
use wyrd_core::multipart::abort::abort_session;
use wyrd_core::multipart::commit::upload_part;
use wyrd_core::multipart::complete::{
    complete_upload, CompleteRequest, NamedParts, PublishRequest,
};
use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
use wyrd_core::multipart::stage::{StageClock, StageRequest, StageRuntime};
use wyrd_core::multipart::{
    self as mp, decode_part_record, decode_part_summary, decode_retire_obligation, part_key,
    psum_key, retire_key, sidx_key, AbortOutcome, AttemptId, Budget, CompleteOutcome,
    CreateOutcome, Digest, OwnedEntry, PartNumber, PartSummary, RetireMode, RetireToken,
    RetiredMap, StagedPlacement, UploadId, UploadPartOutcome, MAX_INFLIGHT_PARTS, MAX_PART_CHUNKS,
};
use wyrd_core::placement::Topology;
use wyrd_custodian::gc::W_WRITE_MILLIS;
use wyrd_custodian::{
    reconcile_after_restore, reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone,
    GcContext, ReconcileError, Reconciled, ReconstructionContext, RestoreReport,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::{Clock, ManualClock};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result as StoreResult, WriteBatch,
};

// ─────────────────────────────── fixtures ───────────────────────────────

/// Where every rig's clock starts.
const T0: u64 = 10_000_000;
/// The orphan grace every GC pass here honours.
const GRACE: u64 = 60_000;
/// The audit target the retirement leg names a held obligation on.
const AUDIT_TARGET: &str = "wyrd.custodian.retirement.audit";
/// The target the retirement leg reports a step that waited on: unchanged, nothing at fault.
const WAIT_TARGET: &str = "wyrd.custodian.retirement";
/// The late-write deadline GC's sweep of fragment-less marks waits out (`W_repoint + W_write +
/// δ_clock`, `LATE_WRITE_DEADLINE_MILLIS`), stated here; inside [`GRACE`].
const LATE_WRITE: u64 = 41_000;
/// The most positions one marking step marks (`MARKS_PER_STEP`), stated here.
const STATED_MARKS_PER_STEP: usize = 233;
const BUCKET: &str = "photos";
const ROOT: InodeId = 0;
const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
const COMMITTED_AT: u64 = 1_500;
/// Bytes per chunk of a planted part.
const CHUNK_LEN: u64 = 64;
/// Two data fragments and one parity: three positions a chunk.
const RS21: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
/// Four data fragments and two parity: six positions a chunk.
const RS42: EcScheme = EcScheme::ReedSolomon { k: 4, m: 2 };

fn upload(n: u64) -> UploadId {
    UploadId::new(format!("{n:032x}")).unwrap()
}

fn attempt(n: u64) -> AttemptId {
    AttemptId::new(format!("{:032x}", 0xa77e_0000_u64 + n)).unwrap()
}

fn part(n: u32) -> PartNumber {
    PartNumber::new(n).unwrap()
}

fn name(key: &[u8]) -> String {
    String::from_utf8(key.to_vec()).unwrap()
}

fn digest_of(n: u32) -> Digest {
    let mut bytes = [0x3c; 32];
    bytes[..4].copy_from_slice(&n.to_be_bytes());
    Digest::from_bytes(bytes)
}

/// A chunk of `scheme`, `CHUNK_LEN` bytes, at `placement`.
fn chunk(id: ChunkId, scheme: EcScheme, placement: Vec<DServerId>) -> ChunkRef {
    ChunkRef {
        id,
        scheme,
        len: CHUNK_LEN,
        placement,
    }
}

/// `count` RS(2,1) chunks from id `first`, each on servers `0, 1, 2`.
fn chunks_from(first: ChunkId, count: u64) -> Vec<ChunkRef> {
    (0..count)
        .map(|i| chunk(first + ChunkId::from(i), RS21, vec![0, 1, 2]))
        .collect()
}

/// Every fragment position `chunks` names, in order.
fn positions(chunks: &[ChunkRef]) -> Vec<(DServerId, FragmentId)> {
    chunks
        .iter()
        .flat_map(|c| {
            c.fragments()
                .map(|(index, dserver)| (dserver, FragmentId { chunk: c.id, index }))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The `orphan:` keys of `positions`.
fn mark_keys(positions: &[(DServerId, FragmentId)]) -> Vec<Vec<u8>> {
    positions.iter().map(|&(d, f)| orphan_key(d, f)).collect()
}

/// The `part:` value of a part over `chunks` with `digest`, in the codec's own spelling.
fn part_value(chunks: &[ChunkRef], digest: Digest) -> Bytes {
    let len: u64 = chunks.iter().map(|c| c.len).sum();
    let record = format!(
        "{{\"chunks\":{},\"len\":{len},\"digest\":\"{digest}\",\
         \"committed_at_millis\":{COMMITTED_AT},\"session_epoch\":0}}",
        String::from_utf8(md::encode(&chunks).to_vec()).unwrap()
    );
    let decoded = decode_part_record(record.as_bytes()).expect("a canonical part");
    assert_eq!(md::encode(&decoded), record.as_bytes());
    Bytes::from(record)
}

/// The puts planting part `n` of `id` over `chunks`: its `part:` record and `psum:` summary.
fn plant_part(id: &UploadId, n: u32, chunks: &[ChunkRef]) -> Vec<(Vec<u8>, Bytes)> {
    let digest = digest_of(n);
    let summary = PartSummary {
        chunks: chunks.len() as u32,
        len: chunks.iter().map(|c| c.len).sum(),
        digest,
        committed_at_millis: COMMITTED_AT,
    };
    let psum = md::encode(&summary);
    assert_eq!(decode_part_summary(&psum).unwrap(), summary);
    vec![
        (part_key(id, part(n)), part_value(chunks, digest)),
        (psum_key(id, part(n)), psum),
    ]
}

/// A structured mark of `event`, stamped `at`, as stored.
fn structured(at: u64, event: &str) -> Bytes {
    encode_orphan_mark(&OrphanMark::structured(at, event).unwrap())
}

// ─────────────────────────── the recording store ───────────────────────────

/// An obligation another writer installs while the pass's reconstruction loop reads the repair
/// queue, after moving the clock by `advance`.
struct Install {
    clock: ManualClock,
    advance: u64,
    batch: WriteBatch,
    /// The clock reading at the installation, once it has run.
    at: Arc<AtomicU64>,
}

/// `RedbMetadataStore` behind a wrapper that records every batch sent, in order, and can run one
/// [`Install`] at the reconstruction loop's read of the repair queue.
struct Store {
    inner: RedbMetadataStore,
    sent: Mutex<Vec<WriteBatch>>,
    install: Mutex<Option<Install>>,
}

impl Store {
    fn new() -> Self {
        Self {
            inner: RedbMetadataStore::in_memory().unwrap(),
            sent: Mutex::new(Vec::new()),
            install: Mutex::new(None),
        }
    }

    /// Fixture writes, straight to the inner store.
    async fn plant(&self, puts: Vec<(Vec<u8>, Bytes)>) {
        for chunk in puts.chunks(400) {
            let mut batch = WriteBatch::new();
            batch.puts.extend(chunk.iter().cloned());
            assert_eq!(
                self.inner.commit(batch).await.unwrap(),
                CommitOutcome::Committed
            );
        }
    }

    /// Plant `value` at `key`, checked to be a valid obligation in the codec's own spelling.
    async fn plant_obligation(&self, key: &[u8], value: &str) {
        let (_, _, payload) = decode_retire_obligation(key, value.as_bytes()).expect("valid");
        assert_eq!(md::encode(&payload), value.as_bytes(), "canonical");
        self.plant(vec![(key.to_vec(), Bytes::from(value.to_owned()))])
            .await;
    }

    async fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.inner.get(key).await.unwrap()
    }

    async fn under(&self, prefix: &[u8]) -> BTreeMap<Vec<u8>, Bytes> {
        let mut records = BTreeMap::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let (page, next) = self
                .inner
                .scan_page(prefix, after.as_deref(), 10_000)
                .await
                .unwrap();
            records.extend(page);
            match next {
                None => return records,
                Some(next) => after = Some(next),
            }
        }
    }

    /// Every batch sent since the last call.
    fn take_sent(&self) -> Vec<WriteBatch> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

#[async_trait]
impl MetadataStore for Store {
    async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
        if prefix == wyrd_core::repair::REPAIR_PREFIX {
            let install = self.install.lock().unwrap().take();
            if let Some(install) = install {
                install.clock.advance(install.advance);
                let landed = self.inner.commit(install.batch).await?;
                assert_eq!(landed, CommitOutcome::Committed, "the installation lands");
                install
                    .at
                    .store(install.clock.now_millis(), Ordering::SeqCst);
            }
        }
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<wyrd_traits::ScanPage> {
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
        self.sent.lock().unwrap().push(batch.clone());
        self.inner.commit(batch).await
    }
}

// ─────────────────────────────── the D servers ───────────────────────────────

/// In-process D servers addressed by id: staging writes each fragment at the server its placement
/// names, the read path reads it there, and each server's own view ([`Server`]) is what GC and
/// reconstruction list and delete through.
#[derive(Clone, Default)]
struct Fleet(Arc<Mutex<BTreeMap<DServerId, HashMap<FragmentId, Bytes>>>>);

impl Fleet {
    fn serving(ids: impl IntoIterator<Item = DServerId>) -> Self {
        let servers = ids.into_iter().map(|id| (id, HashMap::new())).collect();
        Self(Arc::new(Mutex::new(servers)))
    }

    fn put(&self, dserver: DServerId, frag: FragmentId, bytes: Bytes) {
        let mut servers = self.0.lock().unwrap();
        servers
            .get_mut(&dserver)
            .expect("a server")
            .insert(frag, bytes);
    }

    fn holds(&self, dserver: DServerId, frag: FragmentId) -> bool {
        let servers = self.0.lock().unwrap();
        servers
            .get(&dserver)
            .is_some_and(|held| held.contains_key(&frag))
    }

    fn take(&self, dserver: DServerId, frag: FragmentId) -> Option<Bytes> {
        let mut servers = self.0.lock().unwrap();
        servers
            .get_mut(&dserver)
            .and_then(|held| held.remove(&frag))
    }

    /// Put a fragment at every position of `chunks`.
    fn store_all(&self, chunks: &[ChunkRef]) {
        for (dserver, frag) in positions(chunks) {
            self.put(dserver, frag, Bytes::from(format!("{frag:?}")));
        }
    }

    fn ids(&self) -> Vec<DServerId> {
        self.0.lock().unwrap().keys().copied().collect()
    }
}

#[async_trait]
impl ChunkStore for Fleet {
    async fn put_fragment(&self, _: FragmentId, _: Bytes, _: Option<u64>) -> StoreResult<()> {
        Err("Fleet: a write must address a D server".into())
    }

    async fn get_fragment(&self, id: FragmentId) -> StoreResult<Option<Bytes>> {
        let servers = self.0.lock().unwrap();
        Ok(servers.values().find_map(|held| held.get(&id).cloned()))
    }

    async fn list_fragments(&self) -> StoreResult<Vec<FragmentId>> {
        let servers = self.0.lock().unwrap();
        Ok(servers
            .values()
            .flat_map(|held| held.keys().copied())
            .collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> StoreResult<()> {
        for held in self.0.lock().unwrap().values_mut() {
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
        let servers = self.0.lock().unwrap();
        Ok(servers
            .get(&dserver)
            .and_then(|held| held.get(&id).cloned()))
    }

    async fn put_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> StoreResult<()> {
        self.put(dserver, id, fragment);
        Ok(())
    }
}

/// One D server of a [`Fleet`], as a pass's fleet lists it.
struct Server {
    id: DServerId,
    fleet: Fleet,
}

#[async_trait]
impl ChunkStore for Server {
    async fn put_fragment(&self, id: FragmentId, bytes: Bytes, _: Option<u64>) -> StoreResult<()> {
        self.fleet.put(self.id, id, bytes);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> StoreResult<Option<Bytes>> {
        let servers = self.fleet.0.lock().unwrap();
        Ok(servers
            .get(&self.id)
            .and_then(|held| held.get(&id).cloned()))
    }

    async fn list_fragments(&self) -> StoreResult<Vec<FragmentId>> {
        let servers = self.fleet.0.lock().unwrap();
        let held = servers.get(&self.id);
        Ok(held
            .map(|h| h.keys().copied().collect())
            .unwrap_or_default())
    }

    async fn delete_fragment(&self, id: FragmentId) -> StoreResult<()> {
        self.fleet.take(self.id, id);
        Ok(())
    }

    async fn health(&self) -> StoreResult<Health> {
        Ok(Health::Healthy)
    }
}

// ─────────────────────────────── the audit seam ───────────────────────────────

/// The retirement leg's events, captured by a subscriber attached to each pass's future: its
/// audit events, the waits it reports (a step that changed nothing with nothing at fault), and its
/// counters, summed.
#[derive(Clone, Default)]
struct Audit {
    events: Arc<Mutex<Vec<HashMap<String, String>>>>,
    waits: Arc<Mutex<Vec<(String, String)>>>,
    counters: Arc<Mutex<HashMap<String, u64>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Audit {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = FieldText(HashMap::new());
        event.record(&mut fields);
        let target = event.metadata().target();
        if target == AUDIT_TARGET {
            self.events.lock().unwrap().push(fields.0);
        } else if target == WAIT_TARGET {
            if let (Some(key), Some(wait)) = (fields.0.get("key"), fields.0.get("wait")) {
                self.waits.lock().unwrap().push((key.clone(), wait.clone()));
            }
        } else {
            let mut counters = self.counters.lock().unwrap();
            for (field, value) in fields.0 {
                if field.starts_with("monotonic_counter.retirement_") {
                    let value: u64 = value.parse().expect("a counter is a number");
                    *counters.entry(field).or_default() += value;
                }
            }
        }
    }
}

struct FieldText(HashMap<String, String>);

impl Visit for FieldText {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl Audit {
    fn dispatch(&self) -> Dispatch {
        Dispatch::new(tracing_subscriber::registry().with(self.clone()))
    }

    /// The hold events naming the obligation at `key`, and empties the capture.
    fn holds(&self, key: &[u8]) -> Vec<HashMap<String, String>> {
        let key = name(key);
        let mut events = self.events.lock().unwrap();
        let held = events
            .iter()
            .filter(|event| {
                event.get("action").map(String::as_str) == Some("retire-obligation-held")
                    && event.get("key") == Some(&key)
            })
            .cloned()
            .collect();
        events.clear();
        held
    }

    /// No audit event since the capture was last emptied.
    fn is_empty(&self) -> bool {
        self.events.lock().unwrap().is_empty()
    }

    /// The wait reasons reported for the obligation at `key`, and empties the wait capture.
    fn waits(&self, key: &[u8]) -> Vec<String> {
        let key = name(key);
        let mut waits = self.waits.lock().unwrap();
        let named = waits
            .iter()
            .filter(|(k, _)| *k == key)
            .map(|(_, wait)| wait.clone())
            .collect();
        waits.clear();
        named
    }

    /// Each retirement counter's sum since the counters were last emptied, and empties them.
    fn counted(&self) -> HashMap<String, u64> {
        std::mem::take(&mut *self.counters.lock().unwrap())
    }
}

// ─────────────────────────────── the rig ───────────────────────────────

/// One store, one fleet, one clock — the deployed loop's one time source — and the audit seam.
struct Rig {
    store: Store,
    fleet: Fleet,
    clock: ManualClock,
    audit: Audit,
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

/// A profile admitting eight sessions; Complete and the part commit read no other cap of it.
fn profile() -> Budget {
    let json = "{\"w_ref\":16,\"max_part_chunks\":1,\"max_parts_per_session\":1,\
                \"max_inflight_parts\":1,\"max_staged_chunks\":1}";
    md::decode(json.as_bytes()).expect("a legal profile")
}

impl Rig {
    /// A rig over D servers `0..servers`, the bucket in place.
    async fn new(servers: u64) -> Self {
        let rig = Self {
            store: Store::new(),
            fleet: Fleet::serving(0..servers),
            clock: ManualClock::new(T0),
            audit: Audit::default(),
        };
        let bucket = vec![(md::bucket_key(BUCKET), Bytes::from_static(MARKER))];
        rig.store.plant(bucket).await;
        rig
    }

    fn servers(&self) -> Vec<Server> {
        let ids = self.fleet.ids();
        ids.into_iter()
            .map(|id| Server {
                id,
                fleet: self.fleet.clone(),
            })
            .collect()
    }

    /// One fenced reconstruction pass as the deployed loop runs it: a fresh leadership term and
    /// context, `now_millis` and the context's clock read from the one clock.
    async fn drain(&self) -> Result<Reconciled, ReconcileError> {
        let (zone, custodian) = elect().await;
        let servers = self.servers();
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
            .iter()
            .map(|s| (s.id, s as &dyn ChunkStore))
            .collect();
        let topology = Topology::default();
        let ctx = ReconstructionContext {
            meta: &self.store,
            fleet: &fleet,
            topology: &topology,
            unreachable: &[],
            clock: &self.clock,
            staged_write_window_millis: W_WRITE_MILLIS,
        };
        let now = self.clock.now_millis();
        reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, now)
            .with_subscriber(self.audit.dispatch())
            .await
    }

    /// One fenced GC pass over the whole fleet at the clock's reading, with the rig's grace.
    async fn gc(&self) -> Reconciled {
        let (zone, custodian) = elect().await;
        let servers = self.servers();
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
            .iter()
            .map(|s| (s.id, s as &dyn ChunkStore))
            .collect();
        let ctx = GcContext {
            meta: &self.store,
            fleet: &fleet,
            grace_window_millis: GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        let now = self.clock.now_millis();
        reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, now)
            .await
            .expect("the GC pass never fails")
    }

    /// Drain passes until the obligation at `key` is gone, at most `max` of them; none fails.
    async fn drain_until_gone(&self, key: &[u8], max: usize) -> usize {
        let mut passes = 0;
        while self.store.value(key).await.is_some() {
            assert!(passes < max, "{} drains within {max} passes", name(key));
            self.drain().await.expect("the pass never fails");
            passes += 1;
        }
        passes
    }

    /// Move past the grace of every mark written so far, and run GC passes until one reclaims
    /// nothing more.
    async fn reclaim_past_grace(&self) {
        self.clock.advance(GRACE + 1);
        for _ in 0..4 {
            if self.gc().await != Reconciled::Changed {
                return;
            }
        }
    }

    /// A session made by Create, numbered `n`, targeting `object`.
    async fn session(&self, n: u64, object: &str) -> UploadId {
        let request = CreateRequest {
            bucket: BUCKET,
            parent: ROOT,
            object,
            content_type: None,
            created_at_millis: 1_000,
            clock_source: "wall",
        };
        let nonce = SegmentNonce::new(format!("{:032x}", 0x5e60_0000 + n)).unwrap();
        let created = create_session(
            &self.store.inner,
            &profile(),
            &request,
            &mut Fixed(Some((upload(n), nonce))),
        )
        .await
        .unwrap();
        let CreateOutcome::Created { upload_id } = created else {
            panic!("expected Created, got {created:?}");
        };
        upload_id
    }

    /// Plant `parts` of `id` (each a part number and its chunks), their fragments on the fleet.
    async fn plant_parts(&self, id: &UploadId, parts: &[(u32, Vec<ChunkRef>)]) {
        let mut puts = Vec::new();
        for (n, chunks) in parts {
            puts.extend(plant_part(id, *n, chunks));
            self.fleet.store_all(chunks);
        }
        self.store.plant(puts).await;
    }

    /// A production Complete of `id` at `object`, naming `named`, publishing under `inode` when
    /// the name is free.
    async fn complete(&self, id: &UploadId, object: &str, named: &[u32], inode: InodeId) {
        let list = NamedParts::new(named.iter().map(|&n| (part(n), digest_of(n))).collect());
        let request = CompleteRequest {
            bucket: BUCKET,
            parent: ROOT,
            object,
            parts: &list.unwrap(),
            fenced_at_millis: 2_000,
            clock_source: "wall",
        };
        let publish = PublishRequest {
            fresh_inode: inode,
            completed_at_millis: 3_000,
        };
        let outcome = complete_upload(&self.store.inner, id, &request, &publish).await;
        assert!(
            matches!(outcome, Ok(CompleteOutcome::Published(_))),
            "{outcome:?}"
        );
    }

    /// The decoded `orphan:` mark at `position`, if any.
    async fn mark_at(&self, position: (DServerId, FragmentId)) -> Option<OrphanMark> {
        let stored = self
            .store
            .value(&orphan_key(position.0, position.1))
            .await?;
        Some(decode_orphan_mark(&stored).expect("a mark of one of the three shapes"))
    }

    /// Every position of `positions` carries a structured mark of `event`, not `reclaiming`.
    async fn assert_marked(&self, positions: &[(DServerId, FragmentId)], event: &str) {
        for &position in positions {
            let mark = self.mark_at(position).await;
            let mark = mark.unwrap_or_else(|| panic!("{position:?} is unmarked"));
            assert_eq!(mark.event(), Some(event), "{position:?}");
            assert!(!mark.is_reclaiming(), "{position:?}");
        }
    }

    fn assert_gone(&self, positions: &[(DServerId, FragmentId)]) {
        for &(dserver, frag) in positions {
            assert!(
                !self.fleet.holds(dserver, frag),
                "{frag:?} on {dserver} survived"
            );
        }
    }

    fn assert_kept(&self, positions: &[(DServerId, FragmentId)]) {
        for &(dserver, frag) in positions {
            assert!(
                self.fleet.holds(dserver, frag),
                "{frag:?} on {dserver} was reclaimed"
            );
        }
    }
}

async fn elect() -> (FencedZone, Custodian) {
    let coord = MemCoordination::new();
    let custodian = Custodian::elect(&coord, "zone-retirement-marks")
        .await
        .expect("leader election over the in-memory coordination seam");
    let mut zone = FencedZone::new();
    zone.install(custodian.leadership());
    (zone, custodian)
}

/// The bytes-mode key of the suffix-free session token of `id` at `epoch`.
fn session_bytes_key(id: &UploadId, epoch: u64) -> Vec<u8> {
    retire_key(RetireMode::Bytes, &session_token(id, epoch))
}

fn session_token(id: &UploadId, epoch: u64) -> RetireToken {
    RetireToken::Session {
        upload_id: id.clone(),
        epoch,
        part: None,
    }
}

/// The index in `sent` of the first batch that puts `key`, and of the first that deletes it.
fn put_at(sent: &[WriteBatch], key: &[u8]) -> Option<usize> {
    sent.iter()
        .position(|batch| batch.puts.iter().any(|(k, _)| k == key))
}

fn deleted_at(sent: &[WriteBatch], key: &[u8]) -> Option<usize> {
    sent.iter()
        .position(|batch| batch.deletes.iter().any(|k| k == key))
}

/// Every position of `positions` was marked by a batch strictly before the one deleting `record`.
fn assert_marked_before_deleted(
    sent: &[WriteBatch],
    positions: &[(DServerId, FragmentId)],
    record: &[u8],
) {
    let deleted = deleted_at(sent, record).unwrap_or_else(|| panic!("{} deleted", name(record)));
    for key in mark_keys(positions) {
        let marked = put_at(sent, &key).unwrap_or_else(|| panic!("{} marked", name(&key)));
        assert!(
            marked < deleted,
            "{} marked in batch {marked}, its record {} deleted in batch {deleted}",
            name(&key),
            name(record)
        );
    }
}

/// The batches of `sent` preconditioned on the obligation at `key`: the drain's steps on it.
fn steps_on<'a>(sent: &'a [WriteBatch], key: &[u8]) -> Vec<&'a WriteBatch> {
    sent.iter()
        .filter(|batch| batch.preconditions.iter().any(|p| p.key == key))
        .collect()
}

/// The stored cursor `(component, entry, item)` of the obligation at `key`.
fn cursor_of(key: &[u8], value: &[u8]) -> Option<(String, u32, u32)> {
    let (_, _, payload) = decode_retire_obligation(key, value).expect("the obligation decodes");
    payload
        .cursor()
        .map(|c| (c.component().name().to_owned(), c.entry(), c.item()))
}

// ─────────────────────────────── (B) ───────────────────────────────

/// **(B)** Parts 1 to 6 planted, each one RS(2,1) chunk on servers 0–2 with its fragments, and a
/// production Complete naming the odd ones. Passes drain both obligations the flip installed. No
/// `orphan:` mark names a chunk of the published map; every position of every unnamed part is
/// marked in a batch before the one that deletes its `part:` record; no `part:` or `psum:` record
/// of the session remains; and past the grace GC removes the unnamed parts' fragments while the
/// published ones stay.
#[tokio::test]
async fn b_a_subset_complete_marks_the_unnamed_parts_before_deleting_their_records() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/b");
    let id = rig.session(1, &object).await;
    let parts: Vec<(u32, Vec<ChunkRef>)> = (1..=6)
        .map(|n| (n, chunks_from(100 * ChunkId::from(n), 1)))
        .collect();
    rig.plant_parts(&id, &parts).await;
    rig.complete(&id, &object, &[1, 3, 5], 42).await;
    let bytes_key = session_bytes_key(&id, 1);
    let records_key = retire_key(RetireMode::Records, &session_token(&id, 1));
    assert!(
        rig.store.value(&bytes_key).await.is_some(),
        "the unnamed parts' obligation"
    );
    rig.store.take_sent();

    rig.drain_until_gone(&bytes_key, 6).await;
    rig.drain_until_gone(&records_key, 2).await;

    let sent = rig.store.take_sent();
    let published: Vec<_> = parts.iter().filter(|(n, _)| n % 2 == 1).collect();
    let unnamed: Vec<_> = parts.iter().filter(|(n, _)| n % 2 == 0).collect();
    let marks = rig.store.under(b"orphan:").await;
    for (_, chunks) in &published {
        for key in mark_keys(&positions(chunks)) {
            assert!(
                !marks.contains_key(&key),
                "{} marks a published chunk",
                name(&key)
            );
        }
    }
    let event = session_token(&id, 1).to_string();
    for (n, chunks) in &unnamed {
        let at = positions(chunks);
        assert_marked_before_deleted(&sent, &at, &part_key(&id, part(*n)));
        rig.assert_marked(&at, &event).await;
    }
    assert!(
        rig.store.under(&mp::part_range(&id)).await.is_empty(),
        "no part: record"
    );
    assert!(
        rig.store.under(&mp::psum_range(&id)).await.is_empty(),
        "no psum: record"
    );
    assert!(rig.audit.is_empty(), "nothing held");

    rig.reclaim_past_grace().await;
    for (_, chunks) in &unnamed {
        rig.assert_gone(&positions(chunks));
    }
    for (_, chunks) in &published {
        rig.assert_kept(&positions(chunks));
    }
    assert!(
        rig.store.value(&md::inode_key(42)).await.is_some(),
        "the object stays published"
    );
}

// ─────────────────────────────── (T) ───────────────────────────────

/// **(T)** A `{chunks}` obligation over two RS(2,1) chunks is installed by another writer while
/// the pass's reconstruction loop reads the repair queue, after the clock moved two graces past the
/// reading the pass was handed. The same pass drains it: every mark it writes is stamped at or after
/// the clock's reading at the installation, and a GC pass straight after reclaims none of its
/// fragments.
#[tokio::test]
async fn t_marks_are_stamped_no_earlier_than_the_retirement_they_evidence() {
    let rig = Rig::new(3).await;
    let id = upload(2);
    let chunks = chunks_from(2_000, 2);
    rig.fleet.store_all(&chunks);
    let token = RetireToken::Session {
        upload_id: id.clone(),
        epoch: 0,
        part: Some((part(1), attempt(1))),
    };
    let key = retire_key(RetireMode::Bytes, &token);
    let value = format!("{{\"chunks\":{}}}", name(&md::encode(&chunks)));
    decode_retire_obligation(&key, value.as_bytes()).expect("a valid obligation");
    let installed = Arc::new(AtomicU64::new(0));
    *rig.store.install.lock().unwrap() = Some(Install {
        clock: rig.clock.clone(),
        advance: 2 * GRACE,
        batch: WriteBatch::new().put(key.clone(), Bytes::from(value)),
        at: Arc::clone(&installed),
    });

    rig.drain().await.expect("the pass never fails");

    let at = installed.load(Ordering::SeqCst);
    assert_eq!(at, T0 + 2 * GRACE, "installed during the pass");
    assert_eq!(
        rig.store.value(&key).await,
        None,
        "drained in the same pass"
    );
    let all = positions(&chunks);
    rig.assert_marked(&all, &token.to_string()).await;
    for &position in &all {
        let stamp = rig.mark_at(position).await.unwrap().orphaned_at_millis();
        assert!(
            stamp >= at,
            "{position:?} stamped {stamp}, before its retirement at {at}"
        );
    }
    rig.gc().await;
    rig.assert_kept(&all);
}

// ─────────────────────────────── (C′) ───────────────────────────────

/// A `{chunks}` obligation for upload `n`, over `chunks`, planted; its key and its token's event.
async fn plant_chunks_obligation(rig: &Rig, n: u64, chunks: &[ChunkRef]) -> (Vec<u8>, String) {
    let token = RetireToken::Session {
        upload_id: upload(n),
        epoch: 0,
        part: Some((part(1), attempt(n))),
    };
    let key = retire_key(RetireMode::Bytes, &token);
    let value = format!("{{\"chunks\":{}}}", name(&md::encode(&chunks)));
    rig.store.plant_obligation(&key, &value).await;
    (key, token.to_string())
}

/// A `{generation}` obligation for version 1 of `inode`, whose one source is `source` — a flat
/// generation's `"chunks":[…]` or a segmented one's `"segments":{…}`, as the codec spells them —
/// planted; its key and its token's event.
async fn plant_generation_obligation(rig: &Rig, inode: InodeId, source: &str) -> (Vec<u8>, String) {
    let token = RetireToken::Generation { inode, version: 1 };
    let key = retire_key(RetireMode::Bytes, &token);
    let value = format!("{{\"generation\":{{\"inode\":{inode},\"version\":1,{source}}}}}");
    rig.store.plant_obligation(&key, &value).await;
    (key, token.to_string())
}

/// The `"segments"` source naming `group`, as the codec spells it.
fn segments_source(group: &SegmentGroup) -> String {
    format!("\"segments\":{}", name(&md::encode(group)))
}

/// A segment group no published root names, for a planted segmented generation.
fn planted_group(n: u64) -> SegmentGroup {
    SegmentGroup::new(format!("{:032x}", 0x5e65_0000_u64 + n), 1).unwrap()
}

/// Plant `group`'s `seg:` records, segment `i` over `segments[i]`, back to back from offset 0; the
/// records' keys, in order. The fragments are the caller's to place.
async fn plant_segments(
    rig: &Rig,
    group: &SegmentGroup,
    segments: &[Vec<ChunkRef>],
) -> Vec<Vec<u8>> {
    let mut puts = Vec::new();
    let mut offset = 0;
    for (index, chunks) in segments.iter().enumerate() {
        let record = SegmentRecord::new(chunks.clone(), offset).unwrap();
        offset += record.byte_len();
        let key = seg_key(group, u32::try_from(index).unwrap()).unwrap();
        puts.push((key, md::encode(&record)));
    }
    let keys = puts.iter().map(|(key, _)| key.clone()).collect();
    rig.store.plant(puts).await;
    keys
}

/// **(C′, arm 0)** A `{chunks}` obligation over one RS(2,1) chunk — a position no record the drain
/// keeps names — whose first position carries a `reclaiming` mark of another event. A step leaves
/// that mark and the obligation byte-identical: the cursor stays at the start, before the
/// position, and with nothing written and nothing at fault the pass answers `Partial`, reports the
/// step waiting on GC, and audits nothing. Real GC passes then destroy the fragment and delete the
/// key themselves. The next step marks the position afresh with this event (arm c) and removes the
/// obligation, its last position marked. Past the grace GC reclaims the two fragments left and
/// deletes their marks, and its sweep deletes the fresh mark over the position whose fragment it
/// destroyed: every mark the drain wrote has a deleter, and none is left.
#[tokio::test]
async fn c_a_reclaiming_mark_is_never_replaced_and_the_drain_resumes_once_gc_deletes_it() {
    let rig = Rig::new(3).await;
    let chunks = chunks_from(3_000, 1);
    rig.fleet.store_all(&chunks);
    let all = positions(&chunks);
    let (key, event) = plant_chunks_obligation(&rig, 3, &chunks).await;
    let owed = rig.store.value(&key).await.expect("planted");
    let held = orphan_key(all[0].0, all[0].1);
    let reclaiming = OrphanMark::structured(T0 - 5 * GRACE, "g:77:1")
        .unwrap()
        .into_reclaiming();
    let reclaiming = encode_orphan_mark(&reclaiming);
    rig.store
        .plant(vec![(held.clone(), reclaiming.clone())])
        .await;

    let outcome = rig.drain().await.expect("the pass never fails");
    assert_eq!(
        outcome,
        Reconciled::Partial,
        "owed, unchanged, nothing held"
    );
    assert!(rig.audit.is_empty(), "a wait on GC is audited as nothing");
    assert_eq!(
        rig.audit.waits(&key),
        ["awaiting-gc"],
        "the wait it reports"
    );
    assert_eq!(
        rig.store.value(&held).await,
        Some(reclaiming.clone()),
        "byte-identical"
    );
    assert_eq!(
        rig.store.value(&key).await,
        Some(owed),
        "the obligation byte-identical: its cursor at the start, before the position"
    );
    for &position in &all[1..] {
        assert!(
            rig.mark_at(position).await.is_none(),
            "nothing past it marked"
        );
    }
    rig.assert_kept(&all);

    for _ in 0..2 {
        rig.gc().await;
    }
    assert!(
        !rig.fleet.holds(all[0].0, all[0].1),
        "GC destroyed the fragment"
    );
    assert_eq!(
        rig.store.value(&held).await,
        None,
        "and deleted the key itself"
    );
    let now = rig.clock.now_millis();

    rig.drain().await.expect("the pass never fails");
    let fresh = rig.mark_at(all[0]).await.expect("marked afresh");
    assert_eq!(fresh.event(), Some(event.as_str()));
    assert_eq!(fresh.orphaned_at_millis(), now);
    assert!(!fresh.is_reclaiming());
    assert_eq!(
        rig.store.value(&key).await,
        None,
        "removed with its last position marked"
    );
    rig.assert_marked(&all, &event).await;
    assert!(rig.audit.is_empty(), "nothing held");

    rig.reclaim_past_grace().await;
    rig.assert_gone(&all);
    assert_eq!(
        rig.store.under(b"orphan:").await,
        BTreeMap::new(),
        "no mark is left: the fresh one over the destroyed fragment was swept"
    );
}

/// **(C′, the sweep)** A drain's mark over a position with no fragment is kept while its
/// obligation is present, however old, and swept once the obligation is gone. A `{chunks}`
/// obligation over two RS(2,1) chunks — positions no record names, so nothing but the obligation
/// protects them — whose first chunk's first fragment was never written (a compensated attempt's
/// lost write) and whose second chunk's first position carries a `reclaiming` mark of another
/// event. One step marks the first chunk and stops at the `reclaiming` mark. Past the late-write
/// deadline and the grace, GC passes finish that reclaim and keep the drain's mark over the empty
/// position, byte-identical, while the obligation is present. The next step marks the rest and
/// removes the obligation; past the grace no mark is left and no fragment.
#[tokio::test]
async fn c_a_drain_mark_over_no_fragment_is_swept_only_once_its_obligation_is_gone() {
    let rig = Rig::new(3).await;
    let chunks = chunks_from(3_200, 2);
    rig.fleet.store_all(&chunks);
    let all = positions(&chunks);
    let lost = all[0];
    rig.fleet.take(lost.0, lost.1).expect("the fragment");
    let (key, event) = plant_chunks_obligation(&rig, 17, &chunks).await;
    let reclaiming = OrphanMark::structured(T0 - 5 * GRACE, "g:79:3")
        .unwrap()
        .into_reclaiming();
    let at = orphan_key(all[3].0, all[3].1);
    rig.store
        .plant(vec![(at.clone(), encode_orphan_mark(&reclaiming))])
        .await;

    rig.drain().await.expect("the pass never fails");
    rig.assert_marked(&all[..3], &event).await;
    assert!(rig.store.value(&key).await.is_some(), "still owed");
    let lost_key = orphan_key(lost.0, lost.1);
    let mark = rig.store.value(&lost_key).await.expect("marked");
    rig.clock.advance(GRACE + LATE_WRITE);
    for _ in 0..2 {
        rig.gc().await;
        assert_eq!(
            rig.store.value(&lost_key).await,
            Some(mark.clone()),
            "kept while the obligation is present"
        );
    }
    assert_eq!(rig.store.value(&at).await, None, "GC finished its reclaim");
    rig.assert_kept(&all[1..3]);

    rig.drain().await.expect("the pass never fails");
    assert_eq!(rig.store.value(&key).await, None, "drained");
    rig.assert_marked(&all, &event).await;
    rig.reclaim_past_grace().await;
    rig.assert_gone(&all);
    assert_eq!(rig.store.under(b"orphan:").await, BTreeMap::new());
}

/// **(C′, three arms)** A `{chunks}` obligation over two RS(2,1) chunks whose six positions carry,
/// in order: this event's mark, a legacy mark, another event's mark, nothing, this event's mark,
/// nothing. One step: (a) a position carrying this event's mark is not touched — no precondition,
/// no put, its bytes unchanged; (b) a legacy or other-event mark is re-stamped under
/// `require(== prior)` with this event and the step's stamp; (c) an absent one is written under
/// `require_absent`. The obligation goes with that step.
#[tokio::test]
async fn c_the_three_arms_skip_restamp_and_write() {
    let rig = Rig::new(3).await;
    let chunks = chunks_from(3_100, 2);
    let all = positions(&chunks);
    let (key, event) = plant_chunks_obligation(&rig, 4, &chunks).await;
    let keys = mark_keys(&all);
    let own = structured(T0 - 10, &event);
    let legacy = encode_orphan_mark(&OrphanMark::legacy(T0 - 20));
    let other = structured(T0 - 30, "g:5:9");
    let before = [
        Some(&own),
        Some(&legacy),
        Some(&other),
        None,
        Some(&own),
        None,
    ];
    let mut puts = Vec::new();
    for (k, value) in keys.iter().zip(before) {
        if let Some(value) = value {
            puts.push((k.clone(), value.clone()));
        }
    }
    rig.store.plant(puts).await;
    rig.store.take_sent();
    rig.clock.advance(7);
    let now = rig.clock.now_millis();

    rig.drain().await.expect("the pass never fails");

    let counted = rig.audit.counted();
    let count = |name: &str| counted.get(name).copied().unwrap_or_default();
    assert_eq!(count("monotonic_counter.retirement_steps_committed"), 1);
    assert_eq!(
        count("monotonic_counter.retirement_marks_written"),
        4,
        "two re-stamped, two written, two skipped"
    );
    assert_eq!(count("monotonic_counter.retirement_records_deleted"), 0);
    let sent = rig.store.take_sent();
    let steps = steps_on(&sent, &key);
    assert_eq!(steps.len(), 1, "one step");
    let step = steps[0];
    let precondition = |k: &[u8]| step.preconditions.iter().find(|p| p.key == k).cloned();
    let put = |k: &[u8]| {
        step.puts
            .iter()
            .find(|(p, _)| p == k)
            .map(|(_, v)| v.clone())
    };
    let fresh = structured(now, &event);
    for (index, (k, value)) in keys.iter().zip(before).enumerate() {
        match value {
            Some(v) if *v == own => {
                assert!(precondition(k).is_none(), "(a) {index}: no precondition");
                assert!(put(k).is_none(), "(a) {index}: no put");
                assert_eq!(rig.store.value(k).await, Some(own.clone()), "(a) {index}");
            }
            Some(v) => {
                let pre = precondition(k).expect("(b) a precondition");
                assert_eq!(pre.expected.as_ref(), Some(v), "(b) {index}: == prior");
                assert_eq!(put(k), Some(fresh.clone()), "(b) {index}: re-stamped");
                assert_eq!(rig.store.value(k).await, Some(fresh.clone()), "(b) {index}");
            }
            None => {
                let pre = precondition(k).expect("(c) a precondition");
                assert_eq!(pre.expected, None, "(c) {index}: require_absent");
                assert_eq!(put(k), Some(fresh.clone()), "(c) {index}: written");
            }
        }
    }
    assert_eq!(rig.store.value(&key).await, None, "the obligation goes");
}

/// **(C′, a mark of none of the three shapes)** A `{chunks}` obligation over two RS(2,1) chunks
/// whose first position holds a value the codec refuses: a `reclaiming` mark in a shape this build
/// does not know (a field it has never seen). It might be GC's commitment to reclaim, so arm (b)
/// must never write over it. Every pass holds the obligation as `unreadable-mark`, naming that
/// position: the value and the obligation byte-identical, no other position marked. Past the
/// late-write deadline and the grace, GC keeps the value and every fragment too (it keeps an
/// unreadable mark's fragment until a human repairs it).
#[tokio::test]
async fn c_a_mark_of_none_of_the_three_shapes_is_held_and_never_restamped() {
    let rig = Rig::new(3).await;
    let chunks = chunks_from(3_300, 2);
    rig.fleet.store_all(&chunks);
    let all = positions(&chunks);
    let (key, _) = plant_chunks_obligation(&rig, 24, &chunks).await;
    let owed = rig.store.value(&key).await.expect("planted");
    let at = orphan_key(all[0].0, all[0].1);
    let unknown = Bytes::from_static(
        br#"{"orphaned_at_millis":9400000,"event":"g:80:4","reclaiming":true,"by":"gc-v2"}"#,
    );
    assert!(
        decode_orphan_mark(&unknown).is_err(),
        "a value of none of the three shapes"
    );
    rig.store.plant(vec![(at.clone(), unknown.clone())]).await;

    for round in 0..2 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.holds(&key);
        assert_eq!(holds.len(), 1, "round {round}: {holds:?}");
        assert_eq!(holds[0]["hold"], "unreadable-mark");
        assert_eq!(holds[0]["position"], name(&at));
        assert_eq!(
            rig.store.value(&at).await,
            Some(unknown.clone()),
            "round {round}: the value byte-identical"
        );
        assert_eq!(
            rig.store.value(&key).await,
            Some(owed.clone()),
            "round {round}: the obligation byte-identical"
        );
        for &position in &all[1..] {
            assert!(rig.mark_at(position).await.is_none(), "{position:?} marked");
        }
    }
    let marks = BTreeMap::from([(at, unknown)]);
    assert_gc_keeps_a_held_obligation(&rig, &key, &all, &marks).await;
}

// ─────────────────────────────── (H) ───────────────────────────────

/// **(H)** A production subset Complete leaves part 2 unnamed under a `retire:bytes:{parts}`
/// obligation; the first position of its chunk carries a `reclaiming` mark of another event —
/// damage, since no in-tree path puts one there. Drain steps alternate with real GC passes past the
/// grace. Every pass: the fragment, the mark, the `part:` record and the obligation stay
/// byte-identical; the step reports the `reclaiming-under-retained-record` hold, naming the
/// position and the record and saying it needs a human; no pass fails. A `{chunks}` obligation
/// later in the ledger drains meanwhile.
#[tokio::test]
async fn h_a_reclaiming_mark_over_a_retained_record_is_an_audited_hold_and_nothing_moves() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/h");
    let id = rig.session(5, &object).await;
    let parts = vec![(1, chunks_from(5_100, 1)), (2, chunks_from(5_200, 1))];
    rig.plant_parts(&id, &parts).await;
    rig.complete(&id, &object, &[1], 43).await;
    let key = session_bytes_key(&id, 1);
    let position = positions(&parts[1].1)[0];
    let mark_key = orphan_key(position.0, position.1);
    let reclaiming = OrphanMark::structured(T0 - 5 * GRACE, "g:78:2")
        .unwrap()
        .into_reclaiming();
    rig.store
        .plant(vec![(mark_key.clone(), encode_orphan_mark(&reclaiming))])
        .await;
    let later_chunks = chunks_from(5_900, 1);
    rig.fleet.store_all(&later_chunks);
    let (later, later_event) = plant_chunks_obligation(&rig, 9, &later_chunks).await;
    assert!(
        later > key,
        "the second obligation sorts after the held one"
    );
    let record = part_key(&id, part(2));
    let pinned = [
        key.clone(),
        mark_key.clone(),
        record.clone(),
        psum_key(&id, part(2)),
    ];
    let mut before = Vec::new();
    for k in &pinned {
        before.push(rig.store.value(k).await.expect("present"));
    }

    for round in 0..3 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        if round == 0 {
            assert_eq!(
                rig.store.value(&later).await,
                None,
                "the later obligation drained"
            );
            rig.assert_marked(&positions(&later_chunks), &later_event)
                .await;
        }
        let holds = rig.audit.holds(&key);
        assert_eq!(holds.len(), 1, "round {round}: one hold");
        let hold = &holds[0];
        assert_eq!(hold["hold"], "reclaiming-under-retained-record");
        assert_eq!(hold["position"], name(&mark_key));
        assert_eq!(hold["record"], name(&record));
        assert!(hold["detail"].contains("needs a human"), "{hold:?}");
        rig.clock.advance(GRACE + 1);
        rig.gc().await;
        for (k, value) in pinned.iter().zip(&before) {
            assert_eq!(
                rig.store.value(k).await.as_ref(),
                Some(value),
                "{}",
                name(k)
            );
        }
        assert!(
            rig.fleet.holds(position.0, position.1),
            "the fragment stays"
        );
    }
    rig.assert_gone(&positions(&later_chunks));
}

// ─────────────────────────────── (D) ───────────────────────────────

/// The clock staging runs on, and a timer that never fires.
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

/// `UploadPart` of `body` as part `n` of `id`, attempt `a`, through the production verb (reserve,
/// stage, commit), RS(2,1) over servers 0–2. It must commit.
async fn upload_part_of(
    rig: &Rig,
    id: &UploadId,
    n: u32,
    a: u64,
    body: &[u8],
    next: &Arc<AtomicU64>,
) {
    let mut topology = Topology::default();
    for server in 0..3 {
        topology.register(server, format!("d{server}"));
    }
    let attempt_id = attempt(a);
    let request = StageRequest {
        upload_id: id,
        part_number: part(n),
        attempt_id: &attempt_id,
        max_inflight_parts: MAX_INFLIGHT_PARTS,
        lease_ttl_millis: 10_000,
        chunk_size: 32,
        scheme: RS21,
        topology: &topology,
        write_window_millis: 3_000,
        max_part_chunks: MAX_PART_CHUNKS,
        body_idle_millis: 1_000_000,
    };
    let mut runtime = Runtime {
        clock: rig.clock.clone(),
        next_chunk: Arc::clone(next),
    };
    let piece: StoreResult<Bytes> = Ok(Bytes::copy_from_slice(body));
    let outcome = upload_part(
        &rig.store.inner,
        &rig.fleet,
        &mut runtime,
        &request,
        stream::iter(vec![piece]),
    )
    .await;
    assert!(
        matches!(outcome, Ok(UploadPartOutcome::Committed { .. })),
        "{outcome:?}"
    );
}

/// The chunks part `n` of `id` names now.
async fn part_chunks(rig: &Rig, id: &UploadId, n: u32) -> Vec<ChunkRef> {
    let value = rig
        .store
        .value(&part_key(id, part(n)))
        .await
        .expect("the part");
    decode_part_record(&value).unwrap().chunks().to_vec()
}

/// **(D, `{chunks}`)** Part 1 uploaded, then uploaded again: the second commit installs
/// `retire:bytes:{chunks}` for the first upload's chunks (`RetireObligation::part_chunks`). Passes
/// drain it: every position it names carries its mark, it is removed, and past the grace every one
/// of those fragments is gone while the part's current fragments stay.
#[tokio::test]
async fn d_a_reuploaded_parts_chunks_drain_to_completion() {
    let rig = Rig::new(3).await;
    let id = rig.session(6, &format!("{BUCKET}/d-chunks")).await;
    let next = Arc::new(AtomicU64::new(6_000));
    upload_part_of(&rig, &id, 1, 1, &[0x61; 100], &next).await;
    let old = part_chunks(&rig, &id, 1).await;
    upload_part_of(&rig, &id, 1, 2, &[0x62; 70], &next).await;
    let current = part_chunks(&rig, &id, 1).await;
    let obligations = rig.store.under(mp::RETIRE_BYTES_PREFIX).await;
    assert_eq!(obligations.len(), 1, "the re-upload's obligation");
    let (key, value) = obligations.into_iter().next().unwrap();
    let (_, token, payload) = decode_retire_obligation(&key, &value).unwrap();
    assert_eq!(
        payload.chunks(),
        old.as_slice(),
        "it owes the first upload's chunks"
    );
    let old_positions = positions(&old);
    rig.assert_kept(&old_positions);

    rig.drain_until_gone(&key, 4).await;

    rig.assert_marked(&old_positions, &token.to_string()).await;
    assert!(rig.audit.is_empty(), "nothing held");
    rig.reclaim_past_grace().await;
    rig.assert_gone(&old_positions);
    rig.assert_kept(&positions(&current));
}

/// **(D, flat `{generation}`)** A Complete publishes a flat object; a second session's Complete
/// over the same name supersedes it, installing `retire:bytes:{generation}` for the first
/// generation's chunks (`superseded`). Passes drain it: every position marked with its event, the
/// obligation removed, and past the grace the first generation's fragments are gone while the
/// second's stay, published.
#[tokio::test]
async fn d_a_superseded_flat_generation_drains_to_completion() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/d-flat");
    let first = rig.session(7, &object).await;
    let first_chunks = chunks_from(7_000, 3);
    rig.plant_parts(&first, &[(1, first_chunks.clone())]).await;
    rig.complete(&first, &object, &[1], 44).await;
    let second = rig.session(8, &object).await;
    let second_chunks = chunks_from(8_000, 2);
    rig.plant_parts(&second, &[(1, second_chunks.clone())])
        .await;
    rig.complete(&second, &object, &[1], 45).await;
    let generations = rig.store.under(b"retire:bytes:g:").await;
    assert_eq!(
        generations.len(),
        1,
        "the superseded generation's obligation"
    );
    let (key, value) = generations.into_iter().next().unwrap();
    let (_, token, payload) = decode_retire_obligation(&key, &value).unwrap();
    let Some(RetiredMap::Flat(owed)) = payload.generation().map(|g| g.map()) else {
        panic!("a flat generation");
    };
    assert_eq!(owed.as_slice(), first_chunks.as_slice());

    rig.drain_until_gone(&key, 4).await;

    let first_positions = positions(&first_chunks);
    rig.assert_marked(&first_positions, &token.to_string())
        .await;
    assert!(rig.audit.is_empty(), "nothing held");
    // The two sessions' records obligations drain alongside; let them finish too.
    for _ in 0..2 {
        rig.drain().await.unwrap();
    }
    rig.reclaim_past_grace().await;
    rig.assert_gone(&first_positions);
    rig.assert_kept(&positions(&second_chunks));
    let root: InodeRecord =
        md::decode(&rig.store.value(&md::inode_key(44)).await.unwrap()).unwrap();
    assert_eq!(
        root.chunk_map.as_flat(),
        Some(second_chunks.as_slice()),
        "the second generation"
    );
}

/// **(D, `{session, all}`)** A client Abort of an `Open` session with three committed parts and an
/// empty `sidx:` range installs `retire:bytes:{session, all}` (`abort_session`). Passes drain it:
/// every part's positions are marked in a batch before the one deleting its `part:` record, no
/// `part:` or `psum:` record remains, the obligation is removed, and past the grace every fragment
/// is gone. The counterpart of (R): the same shape with no residue completes.
#[tokio::test]
async fn d_an_aborted_sessions_parts_drain_to_completion() {
    let rig = Rig::new(3).await;
    let id = rig.session(10, &format!("{BUCKET}/d-abort")).await;
    let parts: Vec<(u32, Vec<ChunkRef>)> = (1..=3)
        .map(|n| (n, chunks_from(10_000 + 100 * ChunkId::from(n), 2)))
        .collect();
    rig.plant_parts(&id, &parts).await;
    assert!(rig.store.under(&mp::sidx_range(&id)).await.is_empty());
    let aborted = abort_session(&rig.store.inner, &id).await;
    assert!(matches!(aborted, Ok(AbortOutcome::Fenced)), "{aborted:?}");
    let key = session_bytes_key(&id, 0);
    let value = rig.store.value(&key).await.expect("the abort's obligation");
    let (_, token, payload) = decode_retire_obligation(&key, &value).unwrap();
    assert!(payload.session() && matches!(payload.parts(), Some(mp::PartScope::All)));
    rig.store.take_sent();

    rig.drain_until_gone(&key, 4).await;

    let sent = rig.store.take_sent();
    let event = token.to_string();
    for (n, chunks) in &parts {
        assert_marked_before_deleted(&sent, &positions(chunks), &part_key(&id, part(*n)));
        rig.assert_marked(&positions(chunks), &event).await;
    }
    assert!(rig.store.under(&mp::part_range(&id)).await.is_empty());
    assert!(rig.store.under(&mp::psum_range(&id)).await.is_empty());
    assert!(rig.audit.is_empty(), "nothing held");
    rig.reclaim_past_grace().await;
    for (_, chunks) in &parts {
        rig.assert_gone(&positions(chunks));
    }
}

// ─────────────────────────────── (E-hold) ───────────────────────────────

/// **(E-hold)** Part 1 of a live session is an RS(4,2) chunk placed on five servers — a decodable
/// record whose placement is not its scheme's fragment count — under a planted
/// `retire:bytes:{parts}` obligation. Every pass holds it as `untrusted-placement`, naming the
/// record: no mark anywhere, the `part:` and `psum:` records and the obligation byte-identical. GC
/// past the grace keeps every fragment of that chunk (its staged class holds it whole).
#[tokio::test]
async fn e_an_untrusted_placement_is_never_marked_from_or_deleted() {
    let rig = Rig::new(6).await;
    let id = rig.session(11, &format!("{BUCKET}/e")).await;
    let bad = chunk(11_000, RS42, vec![0, 1, 2, 3, 4]);
    let puts = plant_part(&id, 1, std::slice::from_ref(&bad));
    rig.store.plant(puts).await;
    let fragments: Vec<(DServerId, FragmentId)> = (0..6)
        .map(|index| {
            (
                u64::from(index),
                FragmentId {
                    chunk: bad.id,
                    index,
                },
            )
        })
        .collect();
    for &(dserver, frag) in &fragments {
        rig.fleet
            .put(dserver, frag, Bytes::from_static(b"fragment"));
    }
    let key = session_bytes_key(&id, 1);
    rig.store
        .plant_obligation(&key, "{\"parts\":[[1,1]]}")
        .await;
    let pinned = [key.clone(), part_key(&id, part(1)), psum_key(&id, part(1))];
    let mut before = Vec::new();
    for k in &pinned {
        before.push(rig.store.value(k).await.unwrap());
    }

    for round in 0..3 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.holds(&key);
        assert_eq!(holds.len(), 1, "round {round}");
        assert_eq!(holds[0]["hold"], "untrusted-placement");
        assert_eq!(holds[0]["record"], name(&part_key(&id, part(1))));
        for (k, value) in pinned.iter().zip(&before) {
            assert_eq!(
                rig.store.value(k).await.as_ref(),
                Some(value),
                "{}",
                name(k)
            );
        }
        assert!(rig.store.under(b"orphan:").await.is_empty(), "no mark");
    }
    rig.reclaim_past_grace().await;
    rig.assert_kept(&fragments);
}

/// Every fragment position of an RS(4,2) chunk `id` placed on five servers: index `i` on server
/// `i`, all six of them, each holding a fragment on the fleet.
fn store_untrusted(rig: &Rig, id: ChunkId) -> Vec<(DServerId, FragmentId)> {
    let fragments: Vec<(DServerId, FragmentId)> = (0..6)
        .map(|index| (u64::from(index), FragmentId { chunk: id, index }))
        .collect();
    for &(dserver, frag) in &fragments {
        rig.fleet
            .put(dserver, frag, Bytes::from_static(b"fragment"));
    }
    fragments
}

/// The hold every pass reports for an untrusted chunk `id` in an obligation's own list or in
/// `record`: `untrusted-placement`, the obligation byte-identical, the `pinned` records too, and no
/// mark anywhere.
async fn assert_held_untrusted(
    rig: &Rig,
    key: &[u8],
    id: ChunkId,
    record: &str,
    pinned: &[Vec<u8>],
) {
    let owed = rig.store.value(key).await.expect("owed");
    let mut before = Vec::new();
    for k in pinned {
        before.push(rig.store.value(k).await.expect("present"));
    }
    for round in 0..2 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.holds(key);
        assert_eq!(holds.len(), 1, "round {round}: {holds:?}");
        assert_eq!(holds[0]["hold"], "untrusted-placement");
        assert_eq!(holds[0]["record"], record, "round {round}");
        assert!(
            holds[0]["detail"].contains(&format!("chunk {id} names")),
            "names the chunk: {:?}",
            holds[0]
        );
        assert_eq!(
            rig.store.value(key).await,
            Some(owed.clone()),
            "round {round}: the obligation byte-identical"
        );
        for (k, value) in pinned.iter().zip(&before) {
            assert_eq!(
                rig.store.value(k).await.as_ref(),
                Some(value),
                "{}",
                name(k)
            );
        }
        assert!(rig.store.under(b"orphan:").await.is_empty(), "no mark");
    }
}

/// An obligation whose own list holds an RS(4,2) chunk placed on five servers, before an RS(2,1)
/// chunk whose placement holds: as `{chunks}` (held to the staged rule) or, when `generation`, as a
/// flat `{generation}` (held to the committed one). Every pass holds it as `untrusted-placement`,
/// naming the chunk and no record: the obligation byte-identical and no mark anywhere, the sound
/// chunk's positions included, so the step stopped at the chunk rather than passing it by. Past the
/// late-write deadline and the grace GC keeps every fragment of both chunks.
async fn list_with_an_untrusted_placement(generation: bool) {
    let rig = Rig::new(6).await;
    let bad = chunk(23_000, RS42, vec![0, 1, 2, 3, 4]);
    let sound = chunk(23_001, RS21, vec![0, 1, 2]);
    let list = vec![bad.clone(), sound.clone()];
    let mut fragments = store_untrusted(&rig, bad.id);
    rig.fleet.store_all(std::slice::from_ref(&sound));
    fragments.extend(positions(std::slice::from_ref(&sound)));
    let (key, _) = if generation {
        let source = format!("\"chunks\":{}", name(&md::encode(&list)));
        plant_generation_obligation(&rig, 53, &source).await
    } else {
        plant_chunks_obligation(&rig, 23, &list).await
    };

    assert_held_untrusted(&rig, &key, bad.id, "", &[]).await;
    assert_gc_keeps_a_held_obligation(&rig, &key, &fragments, &BTreeMap::new()).await;
}

/// **(E-hold, `{chunks}`)** An untrusted chunk in a `{chunks}` list is held, never passed by.
#[tokio::test]
async fn e_an_untrusted_chunk_in_a_chunks_list_is_held() {
    list_with_an_untrusted_placement(false).await;
}

/// **(E-hold, flat `{generation}`)** An untrusted chunk in a flat generation's list is held, never
/// passed by.
#[tokio::test]
async fn e_an_untrusted_chunk_in_a_flat_generation_is_held() {
    list_with_an_untrusted_placement(true).await;
}

/// **(E-hold, a `seg:` record)** A segmented `{generation}` over two planted segments: segment 0
/// names three RS(2,1) chunks, the middle one placed on four servers; segment 1 one chunk whose
/// placement holds. Every pass holds the obligation as `untrusted-placement`, naming segment 0's
/// record and the chunk: the obligation and both `seg:` records byte-identical, and no mark
/// anywhere — not segment 0's sound chunks, the first of them read before the bad one, nor segment
/// 1's. Past the late-write deadline and the grace GC keeps every fragment.
#[tokio::test]
async fn e_an_untrusted_chunk_in_a_segment_record_is_held() {
    let rig = Rig::new(4).await;
    let bad = chunk(24_001, RS21, vec![0, 1, 2, 3]);
    let first = vec![
        chunk(24_000, RS21, vec![0, 1, 2]),
        bad.clone(),
        chunk(24_002, RS21, vec![0, 1, 2]),
    ];
    let second = chunks_from(24_100, 1);
    let sound: Vec<ChunkRef> = [&first[..1], &first[2..], &second[..]].concat();
    rig.fleet.store_all(&sound);
    let mut fragments = positions(&sound);
    for index in 0..3 {
        let frag = FragmentId {
            chunk: bad.id,
            index,
        };
        rig.fleet
            .put(u64::from(index), frag, Bytes::from_static(b"fragment"));
        fragments.push((u64::from(index), frag));
    }
    let group = planted_group(24);
    let records = plant_segments(&rig, &group, &[first, second]).await;
    let (key, _) = plant_generation_obligation(&rig, 54, &segments_source(&group)).await;

    assert_held_untrusted(&rig, &key, bad.id, &name(&records[0]), &records).await;
    assert_gc_keeps_a_held_obligation(&rig, &key, &fragments, &BTreeMap::new()).await;
}

// ─────────────────────────────── (R) ───────────────────────────────

/// **(R)** An `Open` session owning one staged `sidx:` entry (a crashed part attempt's residue) and
/// one committed part is aborted by a client: `retire:bytes:{session, all}`. Every pass holds it as
/// `owned-residue`, naming the entry: the obligation and the entry byte-identical, the part's record
/// in place. The owned walk that empties the range is #1013's.
#[tokio::test]
async fn r_a_session_obligation_waits_for_its_owned_residue() {
    let rig = Rig::new(3).await;
    let id = rig.session(12, &format!("{BUCKET}/r")).await;
    let parts = vec![(1, chunks_from(12_000, 1))];
    rig.plant_parts(&id, &parts).await;
    let residue = 12_500;
    let placement = StagedPlacement::new(RS21, vec![0, 1, 2]).unwrap();
    let entry = OwnedEntry::new(id.clone(), T0 + 10_000, placement).to_pending();
    let sidx = sidx_key(&id, part(2), residue);
    let sidx_value = md::encode(&entry);
    mp::decode_owned_entry(&sidx, &sidx_value).expect("an owned entry");
    rig.store
        .plant(vec![(sidx.clone(), sidx_value.clone())])
        .await;
    let aborted = abort_session(&rig.store.inner, &id).await;
    assert!(matches!(aborted, Ok(AbortOutcome::Fenced)), "{aborted:?}");
    let key = session_bytes_key(&id, 0);
    let owed = rig.store.value(&key).await.expect("the abort's obligation");

    for round in 0..3 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.holds(&key);
        assert_eq!(holds.len(), 1, "round {round}");
        assert_eq!(holds[0]["hold"], "owned-residue");
        assert_eq!(holds[0]["record"], name(&sidx));
        assert_eq!(
            rig.store.value(&key).await,
            Some(owed.clone()),
            "byte-identical"
        );
        assert_eq!(
            rig.store.value(&sidx).await,
            Some(sidx_value.clone()),
            "the entry stays"
        );
        assert!(rig.store.value(&part_key(&id, part(1))).await.is_some());
    }
}

// ─────────────────────────────── (S) ───────────────────────────────

/// **(S, a part)** A `{parts}` obligation over one part of 80 RS(2,1) chunks — 240 positions, more
/// than one step marks. Every step makes progress: the cursor advances inside the part while it is
/// being marked, then the part's records go and the obligation with them.
#[tokio::test]
async fn s_a_part_larger_than_a_batch_advances_inside_the_entry_and_finishes() {
    let rig = Rig::new(3).await;
    let id = rig.session(13, &format!("{BUCKET}/s")).await;
    let chunks = chunks_from(13_000, 80);
    let all = positions(&chunks);
    assert!(all.len() > STATED_MARKS_PER_STEP, "{} positions", all.len());
    rig.plant_parts(&id, &[(1, chunks.clone())]).await;
    let key = session_bytes_key(&id, 1);
    rig.store
        .plant_obligation(&key, "{\"parts\":[[1,1]]}")
        .await;

    let mut items = Vec::new();
    while let Some(value) = rig.store.value(&key).await {
        assert!(items.len() < 4, "finishes within four steps: {items:?}");
        items.push(cursor_of(&key, &value).map(|(_, entry, item)| (entry, item)));
        rig.drain().await.expect("the pass never fails");
        let after = rig.store.value(&key).await;
        assert_ne!(
            after.as_ref(),
            Some(&value),
            "every step changes the obligation"
        );
    }

    let item = |at: &Option<(u32, u32)>| at.map_or(0, |(_, item)| item);
    assert_eq!(items.len(), 3, "mark, mark, delete: {items:?}");
    assert_eq!(
        items[1],
        Some((1, STATED_MARKS_PER_STEP as u32)),
        "inside the part"
    );
    assert_eq!(
        items[2],
        Some((1, all.len() as u32)),
        "every position marked"
    );
    assert!(item(&items[0]) < item(&items[1]) && item(&items[1]) < item(&items[2]));
    rig.assert_marked(&all, &session_token(&id, 1).to_string())
        .await;
    assert!(rig.store.value(&part_key(&id, part(1))).await.is_none());
}

/// **(S, a segmented generation)** A Complete publishes a segmented object (two parts of 100
/// RS(2,1) chunks, more than one flat map holds); a repoint then moves one fragment of segment 0 to
/// server 3, rewriting that `seg:` record's placement; a second session's Complete over the same
/// name supersedes the generation, installing `retire:bytes:{generation: {segments}}`. Passes drain
/// it: its segment holds more positions than one step marks, so the cursor advances inside it; the
/// drain marks each record's **current** placement — the moved fragment on server 3, never server
/// 0; the `seg:` records go; the obligation is removed; and past the grace every fragment of the
/// first generation is gone.
#[tokio::test]
async fn s_a_segmented_generation_marks_current_placements_and_drains_to_completion() {
    let rig = Rig::new(4).await;
    let object = format!("{BUCKET}/s-seg");
    let first = rig.session(14, &object).await;
    let parts = vec![(1, chunks_from(14_000, 100)), (2, chunks_from(14_200, 100))];
    rig.plant_parts(&first, &parts).await;
    rig.complete(&first, &object, &[1, 2], 46).await;
    let inode: InodeRecord =
        md::decode(&rig.store.value(&md::inode_key(46)).await.unwrap()).unwrap();
    let ChunkMap::Segmented(map) = &inode.chunk_map else {
        panic!("a segmented root");
    };
    let group: SegmentGroup = map.group().clone();
    // The repoint: fragment 0 of segment 0's first chunk moves from server 0 to server 3.
    let seg0 = seg_key(&group, 0).unwrap();
    let record: SegmentRecord = md::decode(&rig.store.value(&seg0).await.unwrap()).unwrap();
    let offset = record.byte_offset();
    let mut moved_chunks = record.into_chunks();
    let moved = FragmentId {
        chunk: moved_chunks[0].id,
        index: 0,
    };
    moved_chunks[0].placement[0] = 3;
    let rewritten = SegmentRecord::new(moved_chunks, offset).unwrap();
    rig.store
        .plant(vec![(seg0.clone(), md::encode(&rewritten))])
        .await;
    let bytes = rig.fleet.take(0, moved).expect("the fragment on server 0");
    rig.fleet.put(3, moved, bytes);

    let second = rig.session(15, &object).await;
    let second_chunks = chunks_from(15_000, 1);
    rig.plant_parts(&second, &[(1, second_chunks.clone())])
        .await;
    rig.complete(&second, &object, &[1], 47).await;
    let generations = rig.store.under(b"retire:bytes:g:").await;
    assert_eq!(generations.len(), 1);
    let (key, value) = generations.into_iter().next().unwrap();
    let (_, token, payload) = decode_retire_obligation(&key, &value).unwrap();
    assert!(matches!(
        payload.generation().map(|g| g.map()),
        Some(RetiredMap::Segmented(owed)) if *owed == group
    ));

    let seg0_positions = {
        let record: SegmentRecord = md::decode(&rig.store.value(&seg0).await.unwrap()).unwrap();
        positions(record.chunks()).len()
    };
    assert!(
        seg0_positions > STATED_MARKS_PER_STEP,
        "{seg0_positions} positions"
    );
    let mut inside = false;
    let mut steps = 0;
    while let Some(value) = rig.store.value(&key).await {
        steps += 1;
        assert!(steps <= 12, "drains within twelve steps");
        if let Some((_, entry, item)) = cursor_of(&key, &value) {
            inside |= entry == 0 && item > 0 && (item as usize) < seg0_positions;
        }
        rig.drain().await.expect("the pass never fails");
        assert_ne!(
            rig.store.value(&key).await,
            Some(value),
            "every step changes it"
        );
    }

    assert!(inside, "a step stopped inside segment 0");
    let event = token.to_string();
    let mut generation: Vec<ChunkRef> = parts.iter().flat_map(|(_, c)| c.clone()).collect();
    generation[0].placement[0] = 3;
    let first_positions = positions(&generation);
    rig.assert_marked(&first_positions, &event).await;
    assert!(
        rig.mark_at((0, moved)).await.is_none(),
        "the old position is not marked"
    );
    assert!(
        rig.store
            .under(&md::seg_range_prefix(&group))
            .await
            .is_empty(),
        "no seg: left"
    );
    for _ in 0..2 {
        rig.drain().await.unwrap();
    }
    rig.reclaim_past_grace().await;
    rig.assert_gone(&first_positions);
    rig.assert_kept(&positions(&second_chunks));
}

// ─────────────────────────────── (G) ───────────────────────────────

/// A session record over segment group `nonce` in `state` at `epoch`, in the codec's own spelling.
fn session_value(nonce: &str, state: &str, epoch: u64) -> Bytes {
    let bytes = format!(
        "{{\"parent\":42,\"object\":\"o\",\"created_at_millis\":100,\"clock_source\":\"wall\",\
         \"segment_nonce\":\"{nonce}\",\"epoch\":{epoch},\"attempts\":1,\"state\":{state}}}"
    );
    let record = mp::decode_session_record(bytes.as_bytes()).expect(state);
    assert_eq!(md::encode(&record), bytes.as_bytes());
    Bytes::from(bytes)
}

/// The production post-restore pass over the rig's store and four empty D servers.
async fn restore_pass(rig: &Rig) -> RestoreReport {
    let empty = Fleet::serving(0..4);
    let servers: Vec<Server> = (0..4)
        .map(|id| Server {
            id,
            fleet: empty.clone(),
        })
        .collect();
    let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
        .iter()
        .map(|s| (s.id, s as &dyn ChunkStore))
        .collect();
    let ctx = GcContext {
        meta: &rig.store.inner,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    reconcile_after_restore(&ctx, rig.clock.now_millis())
        .await
        .unwrap()
}

/// **(G)** A `Completing@1` session that wrote 1,100 segments — three records steps' worth — each
/// naming a chunk one of its seven parts holds, fenced to `Aborting@2` by the production
/// post-restore pass, which installs `retire:bytes:s:<id>:1` `{session, all}` and
/// `retire:records:s:<id>:1` `{seg}`. Drained one step at a time; after every step the production
/// `reconcile_after_restore` names nothing of the session — no chunk held by no part, no segment
/// without a deleter — until both obligations are gone. The parts' records stay while the segments
/// are owed, and go after.
#[tokio::test]
async fn g_restore_stays_accurate_between_drain_steps() {
    let rig = Rig::new(4).await;
    let id = upload(16);
    let group = SegmentGroup::new(format!("{:032x}", 0xfeed_0016_u64), 1).unwrap();
    let nonce = group.nonce().to_string();
    let segments = 1_100;
    let completing = format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":{segments},\
         \"publish_target\":{{\"parent\":42,\"name\":\"o\",\"epoch\":1}}}}"
    );
    let parts: Vec<(u32, Vec<ChunkRef>)> = (1..=7)
        .map(|n| (n, chunks_from(16_000 + ChunkId::from(n), 1)))
        .collect();
    let mut puts = vec![(mp::mpu_key(&id), session_value(&nonce, &completing, 1))];
    for (n, chunks) in &parts {
        puts.extend(plant_part(&id, *n, chunks));
    }
    for index in 0..segments {
        let chunks = parts[(index % 7) as usize].1.clone();
        let record = SegmentRecord::new(chunks, u64::from(index) * CHUNK_LEN).unwrap();
        puts.push((seg_key(&group, index).unwrap(), md::encode(&record)));
    }
    rig.store.plant(puts).await;

    let fenced = restore_pass(&rig).await;
    let mpu = name(&mp::mpu_key(&id));
    assert!(!format!("{fenced:?}").contains(&mpu), "{fenced:?}");
    let bytes = session_bytes_key(&id, 1);
    let records = retire_key(RetireMode::Records, &session_token(&id, 1));
    assert!(
        rig.store.value(&bytes).await.is_some(),
        "the fence's bytes obligation"
    );
    assert!(
        rig.store.value(&records).await.is_some(),
        "the fence's records obligation"
    );
    let range = md::seg_range_prefix(&group);

    let mut steps = 0;
    let mut waited = Vec::new();
    while rig.store.value(&bytes).await.is_some() || rig.store.value(&records).await.is_some() {
        steps += 1;
        assert!(steps <= 8, "both drain within eight steps");
        rig.drain().await.expect("the pass never fails");
        waited.extend(rig.audit.waits(&bytes));
        let report = restore_pass(&rig).await;
        assert!(
            !format!("{report:?}").contains(&mpu),
            "step {steps}: {report:?}"
        );
        assert!(
            report.segments_unaccounted.is_empty(),
            "step {steps}: {report:?}"
        );
        let segs_left = !rig.store.under(&range).await.is_empty();
        let parts_left = rig.store.under(&mp::part_range(&id)).await.len();
        if segs_left {
            assert_eq!(
                parts_left,
                parts.len(),
                "step {steps}: parts stay while segments do"
            );
        }
    }

    assert!(steps >= 4, "more than one step each: {steps}");
    assert!(
        waited.iter().any(|wait| wait == "segments-first"),
        "the bytes obligation, every position marked, waited for the segments: {waited:?}"
    );
    assert!(rig.store.under(&range).await.is_empty());
    assert!(rig.store.under(&mp::part_range(&id)).await.is_empty());
    let event = session_token(&id, 1).to_string();
    for (_, chunks) in &parts {
        rig.assert_marked(&positions(chunks), &event).await;
    }
    assert!(rig.audit.is_empty(), "nothing held");
}

// ──────────────────── paging, and a record gone under a partial cursor ────────────────────

/// The most entries one marking step reads (`ENTRIES_PER_MARKING_STEP`), stated here.
const STATED_ENTRIES_PER_STEP: u32 = 16;

/// A segmented generation superseded by production Completes: session `n` publishes `parts` at
/// `object` as a segmented root under `inode`, and session `n + 1` publishes one small part over
/// the same name, which installs `retire:bytes:{generation}` naming the first root's segment group.
/// Its key, its event and the group.
async fn superseded_segmented(
    rig: &Rig,
    n: u64,
    object: &str,
    parts: &[(u32, Vec<ChunkRef>)],
    inode: InodeId,
) -> (Vec<u8>, String, SegmentGroup) {
    let first = rig.session(n, object).await;
    rig.plant_parts(&first, parts).await;
    let named: Vec<u32> = parts.iter().map(|(number, _)| *number).collect();
    rig.complete(&first, object, &named, inode).await;
    let root: InodeRecord =
        md::decode(&rig.store.value(&md::inode_key(inode)).await.unwrap()).unwrap();
    let ChunkMap::Segmented(map) = &root.chunk_map else {
        panic!("a segmented root");
    };
    let group = map.group().clone();
    let second = rig.session(n + 1, object).await;
    let replacement = chunks_from(900_000_000 + ChunkId::from(n), 1);
    rig.plant_parts(&second, &[(1, replacement)]).await;
    rig.complete(&second, object, &[1], inode + 1).await;
    let generations = rig.store.under(b"retire:bytes:g:").await;
    assert_eq!(
        generations.len(),
        1,
        "the superseded generation's obligation"
    );
    let (key, value) = generations.into_iter().next().unwrap();
    let (_, token, payload) = decode_retire_obligation(&key, &value).unwrap();
    assert!(matches!(
        payload.generation().map(|g| g.map()),
        Some(RetiredMap::Segmented(owed)) if *owed == group
    ));
    (key, token.to_string(), group)
}

/// Past the late-write deadline and the grace, GC passes keep every fragment of `kept` and every
/// mark of `marks` byte-identical while the obligation at `key` is held.
async fn assert_gc_keeps_a_held_obligation(
    rig: &Rig,
    key: &[u8],
    kept: &[(DServerId, FragmentId)],
    marks: &BTreeMap<Vec<u8>, Bytes>,
) {
    let owed = rig.store.value(key).await;
    rig.clock.advance(GRACE + LATE_WRITE);
    for _ in 0..2 {
        rig.gc().await;
    }
    rig.assert_kept(kept);
    for (mark, value) in marks {
        assert_eq!(
            rig.store.value(mark).await.as_ref(),
            Some(value),
            "{}",
            name(mark)
        );
    }
    assert_eq!(
        rig.store.value(key).await,
        owed,
        "the obligation byte-identical"
    );
}

/// The marks of `positions` as stored, absent ones left out.
async fn stored_marks(
    rig: &Rig,
    positions: &[(DServerId, FragmentId)],
) -> BTreeMap<Vec<u8>, Bytes> {
    let mut marks = BTreeMap::new();
    for key in mark_keys(positions) {
        if let Some(value) = rig.store.value(&key).await {
            marks.insert(key, value);
        }
    }
    marks
}

/// A `{session, all}` obligation from a client Abort, over a part of 80 RS(2,1) chunks — 240
/// positions — and, when `trailing`, a one-chunk part after it. One step marks the first 233
/// positions of the big part and stores that count in the cursor. Its `part:` record then goes
/// (damage, or an edit). Every step after holds the obligation, `record-unreadable`, naming that
/// record: the obligation byte-identical, the big part's remaining positions and the trailing
/// part unmarked, the trailing part's records in place; and GC keeps every fragment and mark.
async fn part_gone_under_a_partial_cursor(trailing: bool) {
    let rig = Rig::new(3).await;
    let id = rig.session(18, &format!("{BUCKET}/part-gone")).await;
    let mut parts = vec![(1, chunks_from(18_000, 80))];
    if trailing {
        parts.push((2, chunks_from(18_100, 1)));
    }
    rig.plant_parts(&id, &parts).await;
    let aborted = abort_session(&rig.store.inner, &id).await;
    assert!(matches!(aborted, Ok(AbortOutcome::Fenced)), "{aborted:?}");
    let key = session_bytes_key(&id, 0);
    let event = session_token(&id, 0).to_string();
    let big = positions(&parts[0].1);
    let marked = STATED_MARKS_PER_STEP;

    rig.drain().await.expect("the pass never fails");
    let owed = rig.store.value(&key).await.expect("still owed");
    let cursor = cursor_of(&key, &owed);
    assert_eq!(cursor, Some(("parts".to_owned(), 1, marked as u32)));
    rig.assert_marked(&big[..marked], &event).await;
    let record = part_key(&id, part(1));
    let mut gone = WriteBatch::new();
    gone.deletes.push(record.clone());
    rig.store.inner.commit(gone).await.unwrap();

    for round in 0..2 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.holds(&key);
        assert_eq!(holds.len(), 1, "round {round}: {holds:?}");
        assert_eq!(holds[0]["hold"], "record-unreadable");
        assert_eq!(holds[0]["record"], name(&record));
        assert_eq!(
            rig.store.value(&key).await,
            Some(owed.clone()),
            "round {round}"
        );
        for &position in &big[marked..] {
            assert!(rig.mark_at(position).await.is_none(), "{position:?} marked");
        }
        if trailing {
            for &position in &positions(&parts[1].1) {
                assert!(rig.mark_at(position).await.is_none(), "{position:?} marked");
            }
            assert!(rig.store.value(&part_key(&id, part(2))).await.is_some());
            assert!(rig.store.value(&psum_key(&id, part(2))).await.is_some());
        }
    }
    let every: Vec<_> = parts.iter().flat_map(|(_, c)| positions(c)).collect();
    let marks = stored_marks(&rig, &every).await;
    assert_eq!(marks.len(), marked, "only the first step's marks");
    assert_gc_keeps_a_held_obligation(&rig, &key, &every, &marks).await;
}

/// **(regression: the last part gone under a partial cursor)** An empty page while the cursor
/// counts marks inside a part is a hold, never the end of the component.
#[tokio::test]
async fn a_partial_cursors_part_gone_with_no_record_after_it_is_held() {
    part_gone_under_a_partial_cursor(false).await;
}

/// **(regression: a part gone under a partial cursor, a later part left)** A first record past the
/// cursor's part is the same hold.
#[tokio::test]
async fn a_partial_cursors_part_gone_with_a_record_after_it_is_held() {
    part_gone_under_a_partial_cursor(true).await;
}

/// A segmented generation of two segments — two parts of 100 RS(2,1) chunks, the first segment
/// holding 474 positions — superseded by a production Complete. One step marks the first 233
/// positions of segment 0 and stores that count in the cursor. Segment 0's `seg:` record then goes,
/// and, unless `trailing`, segment 1's too. Every step after holds the obligation,
/// `record-unreadable`, naming segment 0's record: the obligation byte-identical, nothing more
/// marked; and GC keeps every fragment and mark.
async fn segment_gone_under_a_partial_cursor(trailing: bool) {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/segment-gone");
    let parts = vec![(1, chunks_from(19_000, 100)), (2, chunks_from(19_200, 100))];
    let (key, event, group) = superseded_segmented(&rig, 19, &object, &parts, 48).await;
    let seg0 = seg_key(&group, 0).unwrap();
    let seg1 = seg_key(&group, 1).unwrap();
    let record: SegmentRecord = md::decode(&rig.store.value(&seg0).await.unwrap()).unwrap();
    let first = positions(record.chunks());
    assert!(rig.store.value(&seg1).await.is_some(), "two segments");
    let marked = STATED_MARKS_PER_STEP;
    assert!(first.len() > marked, "{} positions", first.len());

    rig.drain().await.expect("the pass never fails");
    let owed = rig.store.value(&key).await.expect("still owed");
    let cursor = cursor_of(&key, &owed);
    assert_eq!(cursor, Some(("generation".to_owned(), 0, marked as u32)));
    rig.assert_marked(&first[..marked], &event).await;
    let mut gone = WriteBatch::new();
    gone.deletes.push(seg0.clone());
    if !trailing {
        gone.deletes.push(seg1.clone());
    }
    rig.store.inner.commit(gone).await.unwrap();

    for round in 0..2 {
        let outcome = rig.drain().await.expect("no pass fails");
        assert_eq!(outcome, Reconciled::Blocked, "round {round}");
        let holds = rig.audit.holds(&key);
        assert_eq!(holds.len(), 1, "round {round}: {holds:?}");
        assert_eq!(holds[0]["hold"], "record-unreadable");
        assert_eq!(holds[0]["record"], name(&seg0));
        assert_eq!(
            rig.store.value(&key).await,
            Some(owed.clone()),
            "round {round}"
        );
        assert_eq!(rig.store.value(&seg1).await.is_some(), trailing);
    }
    let every: Vec<_> = parts.iter().flat_map(|(_, c)| positions(c)).collect();
    let marks = stored_marks(&rig, &every).await;
    assert_eq!(marks.len(), marked, "only the first step's marks");
    assert_gc_keeps_a_held_obligation(&rig, &key, &every, &marks).await;
}

/// **(regression: every segment gone under a partial cursor)** An empty page while the cursor
/// counts marks inside a segment is a hold, never the end of the group.
#[tokio::test]
async fn a_partial_cursors_segment_gone_with_no_record_after_it_is_held() {
    segment_gone_under_a_partial_cursor(false).await;
}

/// **(regression: a segment gone under a partial cursor, a later one left)** A first record past
/// the cursor's segment is the same hold.
#[tokio::test]
async fn a_partial_cursors_segment_gone_with_a_record_after_it_is_held() {
    segment_gone_under_a_partial_cursor(true).await;
}

/// Drain the obligation at `key`, whose entries are `parts` (one chunk each), across a page of
/// sixteen: the first step marks exactly the first sixteen parts and deletes nothing; the second
/// deletes those sixteen parts' records, marks nothing, and moves the cursor to `next`; later
/// steps finish the rest. Every position marked in a batch before the one deleting its record, no
/// `part:` or `psum:` record of the session left, nothing held, and past the grace every fragment
/// gone.
async fn drain_across_a_page(
    rig: &Rig,
    id: &UploadId,
    key: &[u8],
    event: &str,
    parts: &[(u32, Vec<ChunkRef>)],
    next: u32,
) {
    let (first_page, rest) = parts.split_at(STATED_ENTRIES_PER_STEP as usize);
    rig.store.take_sent();

    assert_eq!(rig.drain().await.unwrap(), Reconciled::Changed, "step 1");
    for (n, chunks) in first_page {
        rig.assert_marked(&positions(chunks), event).await;
        assert!(
            rig.store.value(&part_key(id, part(*n))).await.is_some(),
            "{n}"
        );
    }
    for (n, chunks) in rest {
        for &position in &positions(chunks) {
            assert!(
                rig.mark_at(position).await.is_none(),
                "part {n} marked by step 1"
            );
        }
    }

    rig.drain().await.expect("the pass never fails");
    let value = rig.store.value(key).await.expect("still owed");
    assert_eq!(
        cursor_of(key, &value),
        Some(("parts".to_owned(), next, 0)),
        "step 2 finished the first page"
    );
    for (n, _) in first_page {
        assert!(
            rig.store.value(&part_key(id, part(*n))).await.is_none(),
            "{n}"
        );
        assert!(
            rig.store.value(&psum_key(id, part(*n))).await.is_none(),
            "{n}"
        );
    }
    for (n, chunks) in rest {
        assert!(
            rig.store.value(&part_key(id, part(*n))).await.is_some(),
            "{n}"
        );
        for &position in &positions(chunks) {
            assert!(
                rig.mark_at(position).await.is_none(),
                "part {n} marked by step 2"
            );
        }
    }

    rig.drain_until_gone(key, 2).await;
    let sent = rig.store.take_sent();
    for (n, chunks) in parts {
        assert_marked_before_deleted(&sent, &positions(chunks), &part_key(id, part(*n)));
        rig.assert_marked(&positions(chunks), event).await;
    }
    assert!(rig.audit.is_empty(), "nothing held");
}

/// **(paging, `{session, all}`)** A client Abort of a session with twenty one-chunk parts —
/// numbers 1 to 16 and 18 to 21, none at 17 — installs `{session, all}`. Its part range is read a
/// page of sixteen at a time: the first step marks parts 1–16 only; the second deletes their
/// records and moves the cursor to part 17, which has none; the next reads on from there, finds part
/// 18 first, and goes on; the obligation drains, and past the grace every fragment is gone.
#[tokio::test]
async fn paging_an_aborted_sessions_parts_past_one_page() {
    let rig = Rig::new(3).await;
    let id = rig.session(20, &format!("{BUCKET}/paging-abort")).await;
    let parts: Vec<(u32, Vec<ChunkRef>)> = (1..=16)
        .chain(18..=21)
        .map(|n| (n, chunks_from(20_000 + 10 * ChunkId::from(n), 1)))
        .collect();
    rig.plant_parts(&id, &parts).await;
    let aborted = abort_session(&rig.store.inner, &id).await;
    assert!(matches!(aborted, Ok(AbortOutcome::Fenced)), "{aborted:?}");
    let key = session_bytes_key(&id, 0);
    let event = session_token(&id, 0).to_string();

    drain_across_a_page(&rig, &id, &key, &event, &parts, 17).await;

    assert!(rig.store.under(&mp::part_range(&id)).await.is_empty());
    assert!(rig.store.under(&mp::psum_range(&id)).await.is_empty());
    rig.reclaim_past_grace().await;
    for (_, chunks) in &parts {
        rig.assert_gone(&positions(chunks));
    }
}

/// **(paging, `{parts: <set>}`)** A production Complete of a session with twenty one-chunk parts
/// naming only part 1 installs `{parts: [[2,20]]}` for the nineteen unnamed ones. Each step reads
/// sixteen members at most: the first marks parts 2–17 only; the second deletes their records and
/// moves the cursor to part 18; the obligation drains, and past the grace the unnamed parts'
/// fragments are gone while the published part's stay.
#[tokio::test]
async fn paging_a_completes_unnamed_parts_past_one_step() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/paging-set");
    let id = rig.session(21, &object).await;
    let parts: Vec<(u32, Vec<ChunkRef>)> = (1..=20)
        .map(|n| (n, chunks_from(21_000 + 10 * ChunkId::from(n), 1)))
        .collect();
    rig.plant_parts(&id, &parts).await;
    rig.complete(&id, &object, &[1], 49).await;
    let key = session_bytes_key(&id, 1);
    let value = rig
        .store
        .value(&key)
        .await
        .expect("the unnamed parts' obligation");
    let (_, token, payload) = decode_retire_obligation(&key, &value).unwrap();
    assert!(matches!(payload.parts(), Some(mp::PartScope::Set(_))));

    drain_across_a_page(&rig, &id, &key, &token.to_string(), &parts[1..], 18).await;

    for _ in 0..2 {
        rig.drain().await.unwrap();
    }
    assert!(rig.store.under(&mp::part_range(&id)).await.is_empty());
    rig.reclaim_past_grace().await;
    for (_, chunks) in &parts[1..] {
        rig.assert_gone(&positions(chunks));
    }
    rig.assert_kept(&positions(&parts[0].1));
}

/// **(paging, a segmented generation)** A production Complete of seventeen parts of 158 RS(2,1)
/// chunks publishes a root of seventeen segments — more than one page of sixteen — and a second
/// Complete over the name supersedes it. Passes drain it to completion: every step changes it, its
/// cursor reaches segment 16, past the first page; every position is marked with its event; no
/// `seg:` record is left; the obligation is removed; and past the grace every fragment is gone.
#[tokio::test]
async fn paging_a_segmented_generation_of_more_segments_than_one_page() {
    let rig = Rig::new(3).await;
    let object = format!("{BUCKET}/paging-segments");
    let parts: Vec<(u32, Vec<ChunkRef>)> = (1..=17)
        .map(|n| {
            (
                n,
                chunks_from(1_000 * ChunkId::from(n), u64::from(MAX_PART_CHUNKS)),
            )
        })
        .collect();
    let (key, event, group) = superseded_segmented(&rig, 22, &object, &parts, 51).await;
    let range = md::seg_range_prefix(&group);
    assert_eq!(
        rig.store.under(&range).await.len(),
        17,
        "seventeen segments"
    );

    let mut entries = Vec::new();
    let mut steps = 0;
    while let Some(value) = rig.store.value(&key).await {
        steps += 1;
        assert!(steps <= 60, "drains within sixty steps");
        if let Some((_, entry, _)) = cursor_of(&key, &value) {
            entries.push(entry);
        }
        rig.drain().await.expect("the pass never fails");
        assert_ne!(
            rig.store.value(&key).await,
            Some(value),
            "every step changes it"
        );
    }

    assert_eq!(
        entries.last(),
        Some(&16),
        "the cursor reached the last segment: {entries:?}"
    );
    let every: Vec<_> = parts.iter().flat_map(|(_, c)| positions(c)).collect();
    rig.assert_marked(&every, &event).await;
    assert!(rig.store.under(&range).await.is_empty(), "no seg: left");
    assert!(rig.audit.is_empty(), "nothing held");
    for _ in 0..2 {
        rig.drain().await.unwrap();
    }
    rig.reclaim_past_grace().await;
    rig.assert_gone(&every);
}

/// **(paging, segments small enough to finish a page in one step)** A segmented `{generation}`
/// over twenty planted `seg:` records of one RS(2,1) chunk each: more than a page of sixteen, and
/// small enough that one step marks a whole page (48 positions). The first step marks segments
/// 0–15 and deletes nothing; the second deletes those sixteen records, marks nothing, and moves
/// the cursor to segment 16, the first record past the page; the third marks segments 16–19; the
/// fourth deletes their records and the obligation. Every segment's positions are marked in a batch
/// before the one deleting its record, and every record is deleted no later than the obligation —
/// so none is left behind unmarked. Past the grace every fragment is gone and no mark is left.
#[tokio::test]
async fn paging_a_group_of_small_segments_past_one_page() {
    let rig = Rig::new(3).await;
    let segments: Vec<Vec<ChunkRef>> = (0..20_u32)
        .map(|index| chunks_from(25_000 + 10 * ChunkId::from(index), 1))
        .collect();
    for chunks in &segments {
        rig.fleet.store_all(chunks);
    }
    let group = planted_group(25);
    let records = plant_segments(&rig, &group, &segments).await;
    let (key, event) = plant_generation_obligation(&rig, 55, &segments_source(&group)).await;
    rig.store.take_sent();

    let mut cursors = Vec::new();
    while let Some(value) = rig.store.value(&key).await {
        assert!(cursors.len() < 6, "drains within six steps: {cursors:?}");
        rig.drain().await.expect("the pass never fails");
        let after = rig.store.value(&key).await;
        assert_ne!(after.as_ref(), Some(&value), "every step changes it");
        cursors.push(after.map(|after| cursor_of(&key, &after)));
    }

    let sent = rig.store.take_sent();
    let removed = deleted_at(&sent, &key).expect("the obligation deleted");
    for (chunks, record) in segments.iter().zip(&records) {
        assert_marked_before_deleted(&sent, &positions(chunks), record);
        let deleted = deleted_at(&sent, record).unwrap();
        assert!(
            deleted <= removed,
            "{} deleted in batch {deleted}, after the obligation in batch {removed}",
            name(record)
        );
        rig.assert_marked(&positions(chunks), &event).await;
    }
    let at = |entry, item| Some(Some(("generation".to_owned(), entry, item)));
    assert_eq!(
        cursors,
        [at(0, 3), at(16, 0), at(16, 3), None],
        "mark a page, delete it and move past it, mark the rest, delete it"
    );
    let range = md::seg_range_prefix(&group);
    assert!(rig.store.under(&range).await.is_empty(), "no seg: left");
    assert!(rig.audit.is_empty(), "nothing held");

    rig.reclaim_past_grace().await;
    for chunks in &segments {
        rig.assert_gone(&positions(chunks));
    }
    assert_eq!(rig.store.under(b"orphan:").await, BTreeMap::new());
}
