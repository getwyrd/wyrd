//! Issue #814 (663.2) — reconstruction **rebuilds a staged chunk under the session fence**
//! (proposal 0016 decision 2's reconstruction row, `0016:825`; its failure rows `0016:885-889`;
//! the pre-mark and write-deadline rules `0016:1285-1358`,
//! `docs/design/proposals/draft/0016-multipart-commit-protocol.md`).
//!
//! A multipart upload's committed part names its chunks' fragments in a `part:` record, and no
//! committed chunk map names them until the upload is published. Scrub already checks those
//! fragments and queues the ordinary repair obligation, and reconstruction already keeps that
//! obligation rather than draining it (#813) — but nothing rebuilt the fragment, so the part
//! stayed a fragment short until it was published, or for good if it never was. This file pins
//! the rebuild: while the upload is `Open`, one reconstruction pass rebuilds the lost fragment,
//! pre-marks its destination, writes it under a deadline, and adopts it into the `part:` record in
//! one commit pinned to the session — and no losing branch strands a fragment it wrote.
//!
//! The legs:
//! - **A** the whole rebuild: an intact fragment with the right identity on a new D server, the
//!   `part:` record naming it, the destination's pre-mark consumed, the vacated position marked,
//!   the obligation drained.
//! - **B** losing branches strand nothing (X29, `0016:888`): a session fenced, or the part record
//!   rewritten, between the destination write and the adoption — or before the pre-mark, where
//!   nothing is written at all.
//! - **C** the pre-mark and deadline rules, one case each: (i) the pre-mark is durable when the
//!   write arrives; (ii) a destination's existing mark is re-stamped fresh; (iii) a `reclaiming`
//!   (or unreadable) mark rules out its position, never its server; (iv) the write deadline is
//!   the clock at the pre-mark plus the window, never the pass's start — and a refused or
//!   unverifiable write adopts nothing; (v) no write is authorized on a pre-mark `W_repoint` old;
//!   (vi) a vacated position whose mark cannot be read withholds the move; (vii) a slow but legal
//!   write never stops a multi-fragment move, whose writes are sent together; (viii) GC
//!   reclaiming the destination between the write and the adoption makes the adoption lose,
//!   before the pre-mark makes the pre-mark lose, and reclaiming the vacated position after the
//!   assessment read its mark makes the adoption lose.
//! - **D** the drain fence on the destination: a server with any desired-state record is never
//!   chosen, and a drain recorded after the choice makes the pre-mark or the adoption lose.
//! - **E** kept, not rebuilt: an owned-entry-only chunk; a chunk of an upload that has left
//!   `Open` — whose repair runs once the chunk is published; a chunk a corrupt owned entry holds
//!   as untrusted, though a valid committed part names it too; and a degraded chunk with no usable
//!   destination.
//! - Beside them: an unreadable session record withholds the move; an intact chunk's obligation
//!   drains as a duplicate finding; a chunk below `k` only behind an outage is not data loss; a
//!   chunk two parts name is repaired against the first; and a fragment lost from a live server
//!   is rebuilt in place.
//!   (A single-copy chunk in an `Open` upload is `Unrepairable`, as a committed one is:
//!   `staged_protection.rs`'s leg G.)
//!
//! Every leg drives the production `reconcile_step` over in-memory doubles. No client creates a
//! session before the S3 verbs (#508), and Abort and Complete (#656, #658) do not exist yet, so
//! every staged record is seeded as raw JSON the base decoders accept and round-tripped through
//! them, and a leg that needs a fence applies it itself: a fence is a compare-and-swap on the
//! `mpu:` record, whoever writes it. The D-server double **enforces** the deadline a
//! `put_fragment` carries, as the real D server does since #638: it judges the deadline before it
//! publishes, refusing at or after it through `WriteDeadlineExpired::if_elapsed`, and reads its
//! clock again after publishing, refusing to acknowledge through
//! `WriteDeadlineExpired::if_publication_unverified`. Its clock is the `ManualClock` the
//! reconstruction context reads — the one deployment clock both sides judge the deadline on.
//!
//! This file names only symbols on `main` (which carries #813).

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::ThreadId;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, decode_orphan_mark, encode_orphan_mark, orphan_key, ChunkRef, DirentRecord, EcScheme,
    InodeId, InodeRecord, InodeState, OrphanMark,
};
use wyrd_core::multipart::{
    decode_part_record, decode_session_record, mpu_key, part_key, sidx_key, OwnedEntry, PartNumber,
    StagedPlacement, UploadId,
};
use wyrd_core::placement::Topology;
use wyrd_core::repair::{enqueue_repair, header_matches_identity, intact_shard, repair_key};
use wyrd_core::write::encode_ec_fragment;
use wyrd_custodian::desired_state::desired_key;
use wyrd_custodian::gc::{W_REPOINT_MILLIS, W_WRITE_MILLIS};
use wyrd_custodian::{
    reconcile_step, Custodian, FencedZone, ReconcileError, Reconciled, ReconstructionContext,
};
use wyrd_testkit::{Clock, ManualClock};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    WriteBatch, WriteDeadlineExpired,
};

/// The instant every pass starts at, unless a leg moves the clock itself.
const NOW: u64 = 10_000;
/// The bucket and object every seeded session targets.
const PARENT: InodeId = 42;
const OBJECT: &str = "staged/object";
/// Every seeded session's epoch; a fence moves it to `EPOCH + 1`.
const EPOCH: u64 = 3;
/// The inode a publication writes (leg E).
const PUBLISHED: InodeId = 7;
/// The committed part every fixture seeds.
const PART: u32 = 1;
/// When the seeded part record says it committed.
const COMMITTED_AT: u64 = 800;

const RS_2_1: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
const RS_2_2: EcScheme = EcScheme::ReedSolomon { k: 2, m: 2 };

/// The chunk every fixture stages. The coder pads each data shard to a 64-byte multiple
/// (`wyrd_core::erasure::encode`), so a chunk of 64 bytes or fewer leaves every data shard past
/// the first all zeros — and, under RS(2,1), a parity shard equal to the first. This one is long
/// enough that every data shard carries bytes of its own, so no two shards the writer encodes are
/// alike ([`Fixture::new`] checks it) and a byte check can tell a rebuilt shard from any other.
const DATA: &[u8] = b"a committed part's chunk, rebuilt while its upload is still Open: long \
    enough that the second data shard starts past the coder's 64-byte alignment and carries \
    bytes of its own, so that no shard of it equals another one";

const OPEN: &str = "{\"kind\":\"Open\"}";
const ABORTING: &str = "{\"kind\":\"Aborting\"}";

const RECONSTRUCTION_AUDIT: &str = "wyrd.custodian.reconstruction.audit";

// ---- the metadata double ------------------------------------------------------------------------

/// Something a leg does to the store the instant a read or a commit it watches completes — a
/// concurrent writer, landing where the leg puts it rather than where the pass happens to yield.
type Action = Box<dyn FnOnce(&Meta) + Send>;

/// A batch predicate a commit hook watches for.
type Matches = Box<dyn Fn(&WriteBatch) -> bool + Send>;

/// An in-memory `MetadataStore` over an ordered map, with read- and commit-triggered writers.
#[derive(Default)]
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    /// Run once, right after the first completed read (`get` of a key, or `scan` of a prefix)
    /// naming exactly this subject.
    after_read: Mutex<Vec<(Vec<u8>, Action)>>,
    /// Run once, right after the first committed batch the predicate matches.
    after_commit: Mutex<Vec<(Matches, Action)>>,
}

impl Meta {
    /// Put a fixture record in place — not a pass's write.
    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key.into(), value.into());
    }

    fn value(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    fn holds(&self, key: &[u8]) -> bool {
        self.kv.lock().unwrap().contains_key(key)
    }

    /// Every key under `prefix`.
    fn keys_under(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.kv
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Apply `batch` atomically: every precondition holds, or nothing changes.
    fn apply(&self, batch: &WriteBatch) -> CommitOutcome {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key) != pre.expected.as_ref() {
                return CommitOutcome::Conflict;
            }
        }
        for key in &batch.deletes {
            kv.remove(key);
        }
        for (key, value) in &batch.puts {
            kv.insert(key.clone(), value.clone());
        }
        CommitOutcome::Committed
    }

    fn after_read_of(&self, subject: &[u8], action: impl FnOnce(&Meta) + Send + 'static) {
        self.after_read
            .lock()
            .unwrap()
            .push((subject.to_vec(), Box::new(action)));
    }

    fn after_commit_of(
        &self,
        matches: impl Fn(&WriteBatch) -> bool + Send + 'static,
        action: impl FnOnce(&Meta) + Send + 'static,
    ) {
        self.after_commit
            .lock()
            .unwrap()
            .push((Box::new(matches), Box::new(action)));
    }

    /// Run (and retire) every read hook watching `subject`.
    fn read_completed(&self, subject: &[u8]) {
        let due: Vec<Action> = {
            let mut hooks = self.after_read.lock().unwrap();
            let (due, kept) = std::mem::take(&mut *hooks)
                .into_iter()
                .partition(|(watched, _)| watched.as_slice() == subject);
            *hooks = kept;
            due.into_iter().map(|(_, action)| action).collect()
        };
        for action in due {
            action(self);
        }
    }

    /// Run (and retire) every commit hook whose predicate matches `batch`.
    fn commit_completed(&self, batch: &WriteBatch) {
        let due: Vec<Action> = {
            let mut hooks = self.after_commit.lock().unwrap();
            let (due, kept) = std::mem::take(&mut *hooks)
                .into_iter()
                .partition(|(matches, _)| matches(batch));
            *hooks = kept;
            due.into_iter().map(|(_, action)| action).collect()
        };
        for action in due {
            action(self);
        }
    }
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let value = self.value(key);
        self.read_completed(key);
        Ok(value)
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let hits: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.read_completed(prefix);
        Ok(hits)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let outcome = self.apply(&batch);
        if outcome == CommitOutcome::Committed {
            self.commit_completed(&batch);
        }
        Ok(outcome)
    }
}

// ---- the D-server double ------------------------------------------------------------------------

/// Something a leg does the instant a fragment write reaches a D server.
type Hook = Box<dyn Fn(FragmentId) + Send + Sync>;

/// One D server's fragments, **enforcing** the deadline a write carries on its own clock — the
/// production seam's three-phase shape (`ChunkStore::put_fragment`, #638): judge the deadline
/// before publishing (`WriteDeadlineExpired::if_elapsed`, nothing stored), publish, then read the
/// clock again and refuse to acknowledge a publication it cannot certify landed in time
/// (`WriteDeadlineExpired::if_publication_unverified`, the bytes left where they landed).
struct Disk {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
    clock: ManualClock,
    /// How long a write takes from its arrival to its publication; zero, the default, takes no
    /// time at all ([`Disk::taking`]).
    latency: AtomicU64,
    /// Every write that reached this server, and the deadline it carried.
    arrivals: Mutex<Vec<(FragmentId, Option<u64>)>>,
    /// Run when a write arrives, before the deadline is judged.
    on_arrival: Mutex<Vec<Hook>>,
    /// Run once the bytes are published, before the second clock reading.
    on_stored: Mutex<Vec<Hook>>,
}

impl Disk {
    fn new(clock: &ManualClock) -> Self {
        Self {
            frags: Mutex::new(HashMap::new()),
            clock: clock.clone(),
            latency: AtomicU64::new(0),
            arrivals: Mutex::new(Vec::new()),
            on_arrival: Mutex::new(Vec::new()),
            on_stored: Mutex::new(Vec::new()),
        }
    }

    /// Make every write this server accepts publish `millis` after it arrived, on the shared
    /// clock. The write yields once between its arrival and its publication, so writes sent
    /// together all arrive before any of them publishes, and they overlap as writes to separate
    /// servers do; the clock never moves back, so two writes that arrived together publish
    /// together. Writes sent one after another take `millis` each.
    fn taking(&self, millis: u64) {
        self.latency.store(millis, Ordering::Relaxed);
    }

    fn holds(&self, id: FragmentId) -> bool {
        self.frags.lock().unwrap().contains_key(&id)
    }

    fn bytes(&self, id: FragmentId) -> Option<Bytes> {
        self.frags.lock().unwrap().get(&id).cloned()
    }

    fn arrivals(&self) -> Vec<(FragmentId, Option<u64>)> {
        self.arrivals.lock().unwrap().clone()
    }

    fn when_a_write_arrives(&self, hook: impl Fn(FragmentId) + Send + Sync + 'static) {
        self.on_arrival.lock().unwrap().push(Box::new(hook));
    }

    fn when_a_write_is_stored(&self, hook: impl Fn(FragmentId) + Send + Sync + 'static) {
        self.on_stored.lock().unwrap().push(Box::new(hook));
    }
}

#[async_trait]
impl ChunkStore for Disk {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.arrivals.lock().unwrap().push((id, deadline_millis));
        for hook in self.on_arrival.lock().unwrap().iter() {
            hook(id);
        }
        if let Some(deadline) = deadline_millis {
            if let Some(refusal) =
                WriteDeadlineExpired::if_elapsed(id, deadline, self.clock.now_millis())
            {
                return Err(Box::new(refusal));
            }
        }
        let latency = self.latency.load(Ordering::Relaxed);
        if latency > 0 {
            let published = self.clock.now_millis() + latency;
            tokio::task::yield_now().await;
            if self.clock.now_millis() < published {
                self.clock.set(published);
            }
        }
        self.frags.lock().unwrap().insert(id, fragment);
        for hook in self.on_stored.lock().unwrap().iter() {
            hook(id);
        }
        if let Some(deadline) = deadline_millis {
            if let Some(unverified) = WriteDeadlineExpired::if_publication_unverified(
                id,
                deadline,
                self.clock.now_millis(),
            ) {
                return Err(Box::new(unverified));
            }
        }
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.bytes(id))
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

// ---- the records --------------------------------------------------------------------------------

/// An upload id: 32 lowercase-hex characters from a 2-character pair, one per leg.
fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).expect("32 lowercase-hex characters")
}

fn part_no(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number in range")
}

/// A session record in `state` (its JSON) at `epoch`, spelled as the base decoder's own encoding
/// and round-tripped through it.
fn session(state: &str, epoch: u64) -> Bytes {
    let bytes = format!(
        "{{\"parent\":{PARENT},\"object\":\"{OBJECT}\",\"created_at_millis\":100,\
         \"clock_source\":\"wall\",\"epoch\":{epoch},\"attempts\":1,\"state\":{state}}}"
    )
    .into_bytes();
    let record = decode_session_record(&bytes)
        .unwrap_or_else(|fault| panic!("the seeded session must decode: {fault}"));
    assert_eq!(
        metadata::encode(&record).as_ref(),
        bytes.as_slice(),
        "the seeded session must be the decoder's own spelling"
    );
    Bytes::from(bytes)
}

/// The `Completing` state at `epoch` (the shape `multipart_session_records.rs` spells).
fn completing(epoch: u64) -> String {
    format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":900,\"segments_written\":0,\
         \"publish_target\":{{\"parent\":{PARENT},\"name\":\"{OBJECT}\",\"epoch\":{epoch}}}}}"
    )
}

/// The `Completed` state, publishing [`PUBLISHED`].
fn completed() -> String {
    format!(
        "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":{PUBLISHED},\"version\":1,\
         \"etag\":\"{}-1\",\"completed_at_millis\":950,\"complete_fingerprint\":\"{}\"}}}}",
        "ab".repeat(32),
        "cd".repeat(32)
    )
}

/// A committed part record naming `chunks`, spelled as the base decoder's own encoding and
/// round-tripped through it.
fn part_record(chunks: &[ChunkRef], committed_at: u64) -> Bytes {
    let refs: Vec<String> = chunks
        .iter()
        .map(|chunk| String::from_utf8(metadata::encode(chunk).to_vec()).unwrap())
        .collect();
    let len: u64 = chunks.iter().map(|chunk| chunk.len).sum();
    let bytes = format!(
        "{{\"chunks\":[{}],\"len\":{len},\"digest\":\"{}\",\"committed_at_millis\":{committed_at},\
         \"session_epoch\":{EPOCH}}}",
        refs.join(","),
        "ef".repeat(32)
    )
    .into_bytes();
    let record = decode_part_record(&bytes)
        .unwrap_or_else(|fault| panic!("the seeded part record must decode: {fault}"));
    assert_eq!(
        metadata::encode(&record).as_ref(),
        bytes.as_slice(),
        "the seeded part record must be the decoder's own spelling"
    );
    Bytes::from(bytes)
}

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

// ---- the fixture --------------------------------------------------------------------------------

/// One D server of a fixture: its id, its failure domain (`None`: not in the topology at all),
/// and whether it is in the pass's fleet.
#[derive(Clone, Copy)]
struct Server {
    id: DServerId,
    domain: Option<&'static str>,
    live: bool,
}

const fn live(id: DServerId, domain: &'static str) -> Server {
    Server {
        id,
        domain: Some(domain),
        live: true,
    }
}

/// A server that died: gone from the fleet, and (`domain: None`) from the topology.
const fn dead(id: DServerId, domain: Option<&'static str>) -> Server {
    Server {
        id,
        domain,
        live: false,
    }
}

/// What a fixture stages.
struct Spec {
    servers: Vec<Server>,
    scheme: EcScheme,
    placement: Vec<DServerId>,
    /// Fragment indices NOT on disk.
    lost: Vec<u16>,
    /// The session's state (its JSON).
    state: String,
}

impl Spec {
    /// Four live servers `0..=3` in domains `A..=D`; RS(2,1) placed on `[0, 1, 3]`, fragment 2 —
    /// on server 3 — lost; an `Open` session. The one free domain a survivor does not hold besides
    /// the lost fragment's own is C, so the rebuilt fragment goes to server 2.
    fn standard() -> Self {
        Self {
            servers: vec![live(0, "A"), live(1, "B"), live(2, "C"), live(3, "D")],
            scheme: RS_2_1,
            placement: vec![0, 1, 3],
            lost: vec![2],
            state: OPEN.to_owned(),
        }
    }
}

/// A seeded store: an upload with one committed part naming one erasure-coded chunk, some of its
/// fragments lost, and the chunk's repair obligation queued (standing in for scrub).
struct Fixture {
    meta: Arc<Meta>,
    clock: ManualClock,
    /// Indexed by server id.
    disks: Vec<Arc<Disk>>,
    servers: Vec<Server>,
    topology: Topology,
    window: u64,
    /// The servers each pass reports as dropped from the fleet this pass only
    /// (`ReconstructionContext::unreachable`): none, unless a leg says so.
    unreachable: Vec<DServerId>,
    upload: UploadId,
    chunk: ChunkId,
    scheme: EcScheme,
    /// The chunk's shards, as its writer encoded them.
    shards: Vec<Vec<u8>>,
    part_key: Vec<u8>,
    /// The part record as seeded.
    part: Bytes,
    /// The session record as seeded.
    session: Bytes,
}

impl Fixture {
    async fn new(pair: &str, chunk: ChunkId, spec: Spec) -> Self {
        let clock = ManualClock::new(NOW);
        let meta = Arc::new(Meta::default());
        let top = spec.servers.iter().map(|s| s.id).max().unwrap_or(0);
        let disks: Vec<Arc<Disk>> = (0..=top).map(|_| Arc::new(Disk::new(&clock))).collect();
        let mut topology = Topology::default();
        for server in &spec.servers {
            if let Some(domain) = server.domain {
                topology.register(server.id, domain);
            }
        }
        let (k, m) = match spec.scheme {
            EcScheme::ReedSolomon { k, m } => (k, m),
            EcScheme::None => panic!("the fixture stages an erasure-coded chunk"),
        };
        let shards = wyrd_core::erasure::encode(usize::from(k), usize::from(m), DATA)
            .expect("the chunk encodes");
        for (i, shard) in shards.iter().enumerate() {
            assert!(
                !shards[i + 1..].contains(shard),
                "shard {i} equals a later one, so a byte check could not tell them apart"
            );
        }
        for (index, &dserver) in spec.placement.iter().enumerate() {
            let index = index as u16;
            if spec.lost.contains(&index) {
                continue;
            }
            let bytes = encode_ec_fragment(chunk, index, k, m, &shards[usize::from(index)]);
            disks[dserver as usize]
                .frags
                .lock()
                .unwrap()
                .insert(frag(chunk, index), bytes);
        }
        let upload = upload(pair);
        let session = session(&spec.state, EPOCH);
        meta.seed(mpu_key(&upload), session.clone());
        let part_key = part_key(&upload, part_no(PART));
        let chunk_ref = ChunkRef {
            id: chunk,
            scheme: spec.scheme,
            len: DATA.len() as u64,
            placement: spec.placement.clone(),
        };
        let part = part_record(&[chunk_ref], COMMITTED_AT);
        meta.seed(part_key.clone(), part.clone());
        enqueue_repair(&*meta, chunk, "scrub")
            .await
            .expect("seeding the obligation");
        Self {
            meta,
            clock,
            disks,
            servers: spec.servers,
            topology,
            window: W_WRITE_MILLIS,
            unreachable: Vec::new(),
            upload,
            chunk,
            scheme: spec.scheme,
            shards,
            part_key,
            part,
            session,
        }
    }

    async fn standard(pair: &str, chunk: ChunkId) -> Self {
        Self::new(pair, chunk, Spec::standard()).await
    }

    /// One reconstruction pass through the fenced control point, starting at the clock's current
    /// reading — the pass's `now_millis` and the context's clock are one source (ADR-0009).
    async fn pass(&self) -> std::result::Result<Reconciled, ReconcileError> {
        let coord = MemCoordination::new();
        let custodian = Custodian::elect(&coord, "zone-staged-repair")
            .await
            .expect("leader election over the in-memory coordination seam");
        let mut zone = FencedZone::new();
        zone.install(custodian.leadership());
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = self
            .servers
            .iter()
            .filter(|server| server.live)
            .map(|server| {
                (
                    server.id,
                    &*self.disks[server.id as usize] as &dyn ChunkStore,
                )
            })
            .collect();
        let ctx = ReconstructionContext {
            meta: &*self.meta,
            fleet: &fleet,
            topology: &self.topology,
            unreachable: &self.unreachable,
            clock: &self.clock,
            staged_write_window_millis: self.window,
        };
        let now = self.clock.now_millis();
        reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, now).await
    }

    async fn run(&self) -> Reconciled {
        self.pass().await.expect("the reconstruction pass runs")
    }

    fn disk(&self, id: DServerId) -> &Disk {
        &self.disks[id as usize]
    }

    /// The `orphan:` key of fragment `index` of this fixture's chunk on `dserver`.
    fn mark_key(&self, dserver: DServerId, index: u16) -> Vec<u8> {
        orphan_key(dserver, frag(self.chunk, index))
    }

    fn mark(&self, dserver: DServerId, index: u16) -> Option<OrphanMark> {
        self.meta
            .value(&self.mark_key(dserver, index))
            .map(|bytes| decode_orphan_mark(&bytes).expect("a mark the codec reads"))
    }

    fn queued(&self) -> bool {
        self.meta.holds(&repair_key(self.chunk))
    }

    /// The chunk's placement as the stored part record names it now.
    fn placement(&self) -> Vec<DServerId> {
        let bytes = self.meta.value(&self.part_key).expect("the part record");
        let record = decode_part_record(&bytes).expect("the part record decodes");
        record.chunks()[0].placement.clone()
    }

    /// Every write any D server received.
    fn arrivals(&self) -> Vec<(DServerId, FragmentId, Option<u64>)> {
        self.disks
            .iter()
            .enumerate()
            .flat_map(|(id, disk)| {
                disk.arrivals()
                    .into_iter()
                    .map(move |(frag, deadline)| (id as DServerId, frag, deadline))
            })
            .collect()
    }

    /// Whether `dserver` holds fragment `index` intact, with the identity and scheme this chunk's
    /// part record expects, and the shard its writer encoded.
    fn holds_intact(&self, dserver: DServerId, index: u16) -> bool {
        let id = frag(self.chunk, index);
        let Some(bytes) = self.disk(dserver).bytes(id) else {
            return false;
        };
        let identity = matches!(
            wyrd_chunk_format::decode(&bytes),
            Ok(decoded) if header_matches_identity(&decoded.header, id, self.scheme)
        );
        identity
            && intact_shard(&bytes, id, self.scheme).as_deref()
                == Some(self.shards[usize::from(index)].as_slice())
    }

    /// **Nothing is stranded**: every fragment of this chunk on every D server is named by the
    /// part record's placement or covered by an `orphan:` mark at its position.
    fn assert_every_fragment_named_or_marked(&self, leg: &str) {
        let placement = self.placement();
        for (id, disk) in self.disks.iter().enumerate() {
            let id = id as DServerId;
            for fragment in disk.frags.lock().unwrap().keys() {
                if fragment.chunk != self.chunk {
                    continue;
                }
                let named = placement.get(usize::from(fragment.index)) == Some(&id);
                let marked = self.meta.holds(&orphan_key(id, *fragment));
                assert!(
                    named || marked,
                    "{leg}: fragment {} of the chunk on server {id} is named by no record and \
                     covered by no mark — stranded",
                    fragment.index
                );
            }
        }
    }

    /// A batch that fences the session `Open@EPOCH` → `Aborting@EPOCH + 1`, as an Abort (or a
    /// reaper) would: a compare-and-swap on the `mpu:` record.
    fn fence(&self) -> WriteBatch {
        WriteBatch::new()
            .require(mpu_key(&self.upload), self.session.clone())
            .put(mpu_key(&self.upload), session(ABORTING, EPOCH + 1))
    }

    /// Whether `batch` is the re-place's pre-mark: it writes the mark at `premark` and not the part
    /// record.
    fn is_premark(&self, premark: &[u8]) -> impl Fn(&WriteBatch) -> bool + Send + 'static {
        let (premark, part) = (premark.to_vec(), self.part_key.clone());
        move |batch: &WriteBatch| {
            batch.puts.iter().any(|(key, _)| *key == premark)
                && !batch.puts.iter().any(|(key, _)| *key == part)
        }
    }
}

/// Whether `bytes` are a fresh pre-mark of a staged re-place stamped at `at`: a structured mark,
/// not `reclaiming`, naming a move event.
fn is_fresh_premark(bytes: &[u8], at: u64) -> bool {
    matches!(
        decode_orphan_mark(bytes),
        Ok(mark) if !mark.is_reclaiming()
            && mark.orphaned_at_millis() == at
            && mark.event().is_some_and(|event| event.starts_with("replace:"))
    )
}

// ---- the audit seam -----------------------------------------------------------------------------

/// One custodian audit event, as the thread that emitted it saw it.
struct AuditEvent {
    thread: ThreadId,
    target: String,
    fields: Vec<String>,
}

fn audit_log() -> &'static Mutex<Vec<AuditEvent>> {
    static LOG: OnceLock<Mutex<Vec<AuditEvent>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::new()))
}

/// Install the capture once for the whole test binary, before any pass on any thread runs.
fn capture_audit() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        tracing_subscriber::registry()
            .with(AuditCapture)
            .try_init()
            .expect("this test binary installs the only global subscriber");
    });
}

struct AuditCapture;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target();
        if !target.starts_with("wyrd.custodian.") {
            return;
        }
        let mut fields = FieldText(Vec::new());
        event.record(&mut fields);
        audit_log().lock().unwrap().push(AuditEvent {
            thread: std::thread::current().id(),
            target: target.to_owned(),
            fields: fields.0,
        });
    }
}

struct FieldText(Vec<String>);

impl Visit for FieldText {
    fn record_str(&mut self, _field: &Field, value: &str) {
        self.0.push(value.to_owned());
    }

    fn record_debug(&mut self, _field: &Field, value: &dyn Debug) {
        self.0.push(format!("{value:?}"));
    }
}

/// Whether a pass on this thread named `text` in an event on the reconstruction audit seam.
fn named_on_audit_seam(text: &str) -> bool {
    audit_event_naming(&[text])
}

/// Whether a pass on this thread emitted ONE event on the reconstruction audit seam naming every
/// one of `texts`.
fn audit_event_naming(texts: &[&str]) -> bool {
    let thread = std::thread::current().id();
    audit_log().lock().unwrap().iter().any(|event| {
        event.thread == thread
            && event.target == RECONSTRUCTION_AUDIT
            && texts
                .iter()
                .all(|text| event.fields.iter().any(|field| field.contains(text)))
    })
}

// ---- (A) the whole rebuild ----------------------------------------------------------------------

/// **(A)** An `Open` upload's committed part has one fragment lost (fragment 2, on server 3) and
/// its obligation queued. One pass answers `Changed`, and every part of the rebuild holds — a
/// changed placement alone is not enough:
///
/// - server 2, in the free failure domain C, holds an intact fragment 2 with the chunk's identity
///   and scheme (`header_matches_identity`) and the very shard the writer encoded;
/// - the `part:` record's placement names server 2 for it, and nothing else in the record moved
///   (it decodes, is its decoder's own spelling, and differs from the seeded bytes only in that
///   placement);
/// - the destination's pre-mark `orphan:2:<chunk>:2` is gone, and the vacated position
///   `orphan:3:<chunk>:2` carries a mark;
/// - the obligation has drained, and the session record is untouched;
/// - the one write was server 2's, carrying the deadline `pre-mark stamp + W_write`.
///
/// Base: the obligation is kept and nothing is written.
#[tokio::test]
async fn an_open_uploads_committed_part_is_rebuilt_and_adopted() {
    capture_audit();
    let fx = Fixture::standard("a1", 0x8141).await;

    let outcome = fx.run().await;

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "one pass must rebuild and adopt the lost fragment"
    );
    assert!(
        fx.holds_intact(2, 2),
        "server 2 must hold an intact fragment 2 with the chunk's identity and scheme"
    );
    assert_eq!(
        fx.placement(),
        vec![0, 1, 2],
        "the part record must name server 2 for fragment 2"
    );
    let repointed = fx.meta.value(&fx.part_key).expect("the part record");
    let record = decode_part_record(&repointed).expect("the repointed part record decodes");
    assert_eq!(
        metadata::encode(&record).as_ref(),
        repointed.as_ref(),
        "the repointed part record must be its decoder's own spelling"
    );
    let seeded = decode_part_record(&fx.part).unwrap();
    assert_eq!(
        (
            record.len(),
            record.digest(),
            record.committed_at_millis(),
            record.session_epoch()
        ),
        (
            seeded.len(),
            seeded.digest(),
            seeded.committed_at_millis(),
            seeded.session_epoch()
        ),
        "the adoption must move the placement and nothing else"
    );
    let mut expected = seeded.chunks().to_vec();
    expected[0].placement = vec![0, 1, 2];
    assert_eq!(record.chunks(), expected.as_slice());
    assert!(
        fx.mark(2, 2).is_none(),
        "the destination's pre-mark must be consumed by the adoption"
    );
    assert!(
        fx.mark(3, 2).is_some(),
        "the vacated position must carry an orphan mark"
    );
    assert!(!fx.queued(), "the obligation must drain with the adoption");
    assert_eq!(
        fx.meta.value(&mpu_key(&fx.upload)),
        Some(fx.session.clone()),
        "the session record must be untouched"
    );
    assert_eq!(
        fx.arrivals(),
        vec![(2, frag(fx.chunk, 2), Some(NOW + W_WRITE_MILLIS))],
        "exactly one write, to server 2, with the deadline its pre-mark's stamp fixes"
    );
}

// ---- (B) losing branches strand nothing ---------------------------------------------------------

/// **(B)** X29 (`0016:888`): the session is fenced `Open@E` → `Aborting@E+1` after the destination
/// write lands and before the adoption commits. Nothing is adopted: the part record is
/// byte-identical, the destination's pre-mark still stands over the fragment the move wrote, the
/// obligation is still queued, and no fragment is stranded.
///
/// Base: nothing is written, so the fence never fires.
#[tokio::test]
async fn a_session_fenced_between_the_write_and_the_adoption_strands_nothing() {
    capture_audit();
    let fx = Fixture::standard("b1", 0x8142).await;
    let fenced = Arc::new(Mutex::new(None));
    {
        let (meta, fence, fenced) = (Arc::clone(&fx.meta), fx.fence(), Arc::clone(&fenced));
        fx.disk(2).when_a_write_is_stored(move |_| {
            let mut fenced = fenced.lock().unwrap();
            if fenced.is_none() {
                *fenced = Some(meta.apply(&fence));
            }
        });
    }

    let outcome = fx.run().await;

    assert_eq!(
        *fenced.lock().unwrap(),
        Some(CommitOutcome::Committed),
        "the fence never landed between the write and the adoption, so the race was not run"
    );
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert_eq!(
        fx.meta.value(&fx.part_key),
        Some(fx.part.clone()),
        "the part record must be byte-identical after a lost adoption"
    );
    let premark = fx.meta.value(&fx.mark_key(2, 2));
    assert!(
        premark.as_deref().is_some_and(|b| is_fresh_premark(b, NOW)),
        "the pre-mark must still stand after a lost adoption: {premark:?}"
    );
    assert!(
        fx.disk(2).holds(frag(fx.chunk, 2)),
        "the written fragment is there, under its pre-mark"
    );
    assert!(fx.queued(), "the obligation must stay queued");
    assert!(
        fx.mark(3, 2).is_none(),
        "a lost adoption marks no vacated position"
    );
    fx.assert_every_fragment_named_or_marked("session fenced before the adoption");
}

/// **(B)** The same race, with the part record rewritten instead of the session fenced — a writer
/// replacing the part (`0016:825`'s `require(part == prior)`). The adoption loses on it, the
/// rewritten record stands, and the pre-mark covers what was written.
#[tokio::test]
async fn a_part_record_rewritten_between_the_write_and_the_adoption_strands_nothing() {
    capture_audit();
    let fx = Fixture::standard("b2", 0x8143).await;
    let rewritten = {
        let seeded = decode_part_record(&fx.part).unwrap();
        part_record(seeded.chunks(), COMMITTED_AT + 1)
    };
    let replaced = Arc::new(Mutex::new(None));
    {
        let batch = WriteBatch::new()
            .require(fx.part_key.clone(), fx.part.clone())
            .put(fx.part_key.clone(), rewritten.clone());
        let (meta, replaced) = (Arc::clone(&fx.meta), Arc::clone(&replaced));
        fx.disk(2).when_a_write_is_stored(move |_| {
            let mut replaced = replaced.lock().unwrap();
            if replaced.is_none() {
                *replaced = Some(meta.apply(&batch));
            }
        });
    }

    let outcome = fx.run().await;

    assert_eq!(
        *replaced.lock().unwrap(),
        Some(CommitOutcome::Committed),
        "the part record was never rewritten between the write and the adoption"
    );
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert_eq!(
        fx.meta.value(&fx.part_key),
        Some(rewritten),
        "the adoption must lose to the rewritten part record, never overwrite it"
    );
    let premark = fx.meta.value(&fx.mark_key(2, 2));
    assert!(
        premark.as_deref().is_some_and(|b| is_fresh_premark(b, NOW)),
        "the pre-mark must still stand: {premark:?}"
    );
    assert!(fx.queued(), "the obligation must stay queued");
    fx.assert_every_fragment_named_or_marked("part record rewritten before the adoption");
}

/// **(B)** A fence that lands BEFORE the pre-mark — right after the pass reads the destination
/// position — makes the pre-mark batch lose: nothing is marked and nothing is written at all.
#[tokio::test]
async fn a_session_fenced_before_the_premark_writes_nothing() {
    capture_audit();
    let fx = Fixture::standard("b3", 0x8144).await;
    let fence = fx.fence();
    fx.meta.after_read_of(&fx.mark_key(2, 2), move |meta| {
        assert_eq!(meta.apply(&fence), CommitOutcome::Committed);
    });

    let outcome = fx.run().await;

    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert!(
        fx.arrivals().is_empty(),
        "no write may be sent once the pre-mark lost to the fence: {:?}",
        fx.arrivals()
    );
    assert!(
        fx.mark(2, 2).is_none(),
        "the pre-mark lost to the fence, so there is none"
    );
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    assert!(fx.queued(), "the obligation must stay queued");
    assert!(
        named_on_audit_seam("pre-mark-lost"),
        "the aborted move must say why on the audit seam"
    );
}

/// **(B)** The part record rewritten at the same point — after the pass reads the destination
/// position, before the pre-mark commits — makes the pre-mark batch lose on its own
/// `require(part == prior)`: nothing is marked and nothing is written. The adoption pins the same
/// bytes, but a pre-mark that took no notice would send a write the adoption then throws away.
///
/// Base: no destination is chosen, so the rewrite never lands.
#[tokio::test]
async fn a_part_record_rewritten_before_the_premark_writes_nothing() {
    capture_audit();
    let fx = Fixture::standard("b7", 0x8155).await;
    let rewritten = {
        let seeded = decode_part_record(&fx.part).unwrap();
        part_record(seeded.chunks(), COMMITTED_AT + 1)
    };
    let replaced = Arc::new(Mutex::new(None));
    {
        let batch = WriteBatch::new()
            .require(fx.part_key.clone(), fx.part.clone())
            .put(fx.part_key.clone(), rewritten.clone());
        let replaced = Arc::clone(&replaced);
        fx.meta.after_read_of(&fx.mark_key(2, 2), move |meta| {
            *replaced.lock().unwrap() = Some(meta.apply(&batch));
        });
    }

    let outcome = fx.run().await;

    assert_eq!(
        *replaced.lock().unwrap(),
        Some(CommitOutcome::Committed),
        "the part record was never rewritten between the choice and the pre-mark"
    );
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert!(
        fx.arrivals().is_empty(),
        "no write may be sent once the pre-mark lost to the rewritten part record: {:?}",
        fx.arrivals()
    );
    assert!(
        fx.mark(2, 2).is_none(),
        "the pre-mark lost, so there is none"
    );
    assert_eq!(
        fx.meta.value(&fx.part_key),
        Some(rewritten),
        "the rewritten part record stands"
    );
    assert!(fx.queued(), "the obligation must stay queued");
    assert!(
        audit_event_naming(&["pre-mark-lost", &wyrd_traits::chunk_hex(fx.chunk)]),
        "the aborted move must say why on the audit seam"
    );
}

// ---- (C) the pre-mark and deadline rules --------------------------------------------------------

/// **(C)(i)** The pre-mark is durable BEFORE the destination write: the D server double reads the
/// store the instant the write arrives and finds `orphan:<P_new>` there — a fresh structured mark
/// naming the move.
#[tokio::test]
async fn the_premark_is_durable_when_the_write_arrives() {
    capture_audit();
    let fx = Fixture::standard("c1", 0x8145).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    {
        let (meta, key, seen) = (Arc::clone(&fx.meta), fx.mark_key(2, 2), Arc::clone(&seen));
        fx.disk(2)
            .when_a_write_arrives(move |_| seen.lock().unwrap().push(meta.value(&key)));
    }

    let outcome = fx.run().await;

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        1,
        "exactly one write must reach the destination"
    );
    assert!(
        seen[0].as_deref().is_some_and(|b| is_fresh_premark(b, NOW)),
        "the destination's pre-mark must be durable before its write is sent: {seen:?}"
    );
    assert_eq!(outcome, Reconciled::Changed);
    assert!(fx.mark(2, 2).is_none(), "and consumed by the adoption");
}

/// **(C)(ii)** A destination position that already carries a mark — from another unreference
/// event, from an earlier move of this very chunk, or a legacy one — is re-stamped FRESH, never
/// reused with its old stamp: the mark at the destination when the write arrives is a new
/// structured mark stamped now, not the bytes that were there.
#[tokio::test]
async fn a_destinations_existing_mark_is_restamped_fresh() {
    capture_audit();
    let stale: [(&str, Bytes); 3] = [
        (
            "another event",
            encode_orphan_mark(&OrphanMark::structured(1, "g:9:2").unwrap()),
        ),
        (
            "an earlier move",
            encode_orphan_mark(
                &OrphanMark::structured(1, format!("replace:{:032x}:1", 0x8146)).unwrap(),
            ),
        ),
        ("legacy", encode_orphan_mark(&OrphanMark::legacy(1))),
    ];
    for (i, (what, old)) in stale.into_iter().enumerate() {
        let fx = Fixture::standard(&format!("c{}", 2 + i), 0x8146).await;
        fx.meta.seed(fx.mark_key(2, 2), old.clone());
        let seen = Arc::new(Mutex::new(Vec::new()));
        {
            let (meta, key, seen) = (Arc::clone(&fx.meta), fx.mark_key(2, 2), Arc::clone(&seen));
            fx.disk(2)
                .when_a_write_arrives(move |_| seen.lock().unwrap().push(meta.value(&key)));
        }

        let outcome = fx.run().await;

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "{what}: exactly one write must arrive");
        let at_write = seen[0].clone().expect("a mark at the destination");
        assert_ne!(
            at_write, old,
            "{what}: the destination's old mark was reused as the pre-mark"
        );
        assert!(
            is_fresh_premark(&at_write, NOW),
            "{what}: the destination must carry a fresh pre-mark stamped now when the write \
             arrives: {at_write:?}"
        );
        assert_eq!(outcome, Reconciled::Changed, "{what}");
        assert_eq!(fx.placement(), vec![0, 1, 2], "{what}");
    }
}

/// **(C)(iii)** A position whose mark rules it out — a stale `reclaiming` mark, or one no writer
/// can read — is never written, and ruling it out removes only that POSITION, not its server.
/// RS(2,2) with fragments 2 and 3 lost (their servers died), two free failure domains (servers 4
/// and 5), and the bad mark on the position the selector's first pick would give fragment 2
/// (server 4). Both fragments are repaired in ONE pass — the two servers swap fragments — where
/// excluding server 4 outright leaves one server for two fragments and stalls the chunk for good
/// while every pass answers `Satisfied` (v2's defect).
#[tokio::test]
async fn a_ruled_out_position_rules_out_only_itself() {
    capture_audit();
    let bad: [(&str, Bytes); 2] = [
        (
            "reclaiming",
            encode_orphan_mark(&OrphanMark::legacy(1).into_reclaiming()),
        ),
        ("unreadable", Bytes::from_static(b"not a mark")),
    ];
    for (i, (what, mark)) in bad.into_iter().enumerate() {
        let spec = Spec {
            servers: vec![
                live(0, "A"),
                live(1, "B"),
                dead(2, None),
                dead(3, None),
                live(4, "C"),
                live(5, "D"),
            ],
            scheme: RS_2_2,
            placement: vec![0, 1, 2, 3],
            lost: vec![2, 3],
            state: OPEN.to_owned(),
        };
        let fx = Fixture::new(&format!("d{i}"), 0x8147, spec).await;
        fx.meta.seed(fx.mark_key(4, 2), mark.clone());

        let outcome = fx.run().await;

        assert_eq!(
            outcome,
            Reconciled::Changed,
            "{what}: one pass must repair both lost fragments"
        );
        assert_eq!(
            fx.placement(),
            vec![0, 1, 5, 4],
            "{what}: fragment 2 must move to server 5 and fragment 3 to server 4"
        );
        assert!(
            fx.holds_intact(5, 2) && fx.holds_intact(4, 3),
            "{what}: both rebuilt fragments must be intact where the part record names them"
        );
        assert!(
            !fx.disk(4).holds(frag(fx.chunk, 2)),
            "{what}: the ruled-out position must never be written"
        );
        assert_eq!(
            fx.meta.value(&fx.mark_key(4, 2)),
            Some(mark),
            "{what}: the ruled-out position's mark must be left byte-identical"
        );
        assert!(!fx.queued(), "{what}: the obligation must drain");
        assert!(
            fx.mark(2, 2).is_some() && fx.mark(3, 3).is_some(),
            "{what}: both vacated positions must be marked"
        );
    }
    assert!(
        named_on_audit_seam(&String::from_utf8(orphan_key(4, frag(0x8147, 2))).unwrap()),
        "an unreadable destination mark must be named on the audit seam"
    );
}

/// **(C)(iv)** The write deadline is the clock's reading when the pre-mark is built, plus
/// `staged_write_window_millis` — never the pass's start. The clock moves on by five windows
/// between the pass's start and its pre-mark (a read hook on the pass's `inode:` scan): a deadline
/// taken from the pass's start would already have passed and the D server would refuse every
/// write, pass after pass (v1's stall). The write goes through, carrying the deadline its
/// pre-mark's own stamp fixes.
#[tokio::test]
async fn the_write_deadline_runs_from_the_premark_not_the_pass_start() {
    capture_audit();
    let mut fx = Fixture::standard("e1", 0x8148).await;
    fx.window = 1_000;
    let moved = NOW + 5 * fx.window;
    {
        let clock = fx.clock.clone();
        fx.meta.after_read_of(b"inode:", move |_| clock.set(moved));
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    {
        let (meta, key, seen) = (Arc::clone(&fx.meta), fx.mark_key(2, 2), Arc::clone(&seen));
        fx.disk(2)
            .when_a_write_arrives(move |_| seen.lock().unwrap().push(meta.value(&key)));
    }

    let outcome = fx.run().await;

    assert_eq!(
        fx.arrivals(),
        vec![(2, frag(fx.chunk, 2), Some(moved + fx.window))],
        "the write must carry the deadline the pre-mark's own stamp fixes"
    );
    let seen = seen.lock().unwrap().clone();
    assert!(
        seen.first()
            .and_then(Option::as_deref)
            .is_some_and(|b| is_fresh_premark(b, moved)),
        "the pre-mark must be stamped when it is built, not when the pass began: {seen:?}"
    );
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the write is live, so the move must complete"
    );
    assert!(fx.holds_intact(2, 2));
}

/// **(C)(iv)** A write the D server refuses as expired — the clock passes the deadline between
/// the write's dispatch and the server's judgment — aborts the re-place: nothing landed, nothing
/// is adopted, the pre-mark still stands, and the obligation stays queued.
#[tokio::test]
async fn a_write_refused_past_its_deadline_adopts_nothing() {
    capture_audit();
    let mut fx = Fixture::standard("e2", 0x8149).await;
    fx.window = 1_000;
    {
        let (clock, window) = (fx.clock.clone(), fx.window);
        fx.disk(2)
            .when_a_write_arrives(move |_| clock.advance(2 * window));
    }

    let outcome = fx.run().await;

    assert_eq!(
        fx.arrivals().len(),
        1,
        "the write must reach the D server, which refuses it"
    );
    assert!(
        !fx.disk(2).holds(frag(fx.chunk, 2)),
        "a refused write leaves nothing on the D server"
    );
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    let premark = fx.meta.value(&fx.mark_key(2, 2));
    assert!(
        premark.as_deref().is_some_and(|b| is_fresh_premark(b, NOW)),
        "the pre-mark must still stand: {premark:?}"
    );
    assert!(fx.queued(), "the obligation must stay queued");
    assert!(
        named_on_audit_seam("write-deadline-expired"),
        "the aborted move must say why on the audit seam"
    );
}

/// **(C)(iv)** A write whose landing the D server cannot certify — the clock passes the deadline
/// while it publishes — adopts nothing either, and what landed is under the pre-mark.
#[tokio::test]
async fn a_write_the_server_cannot_certify_adopts_nothing() {
    capture_audit();
    let mut fx = Fixture::standard("e3", 0x814A).await;
    fx.window = 1_000;
    {
        let (clock, window) = (fx.clock.clone(), fx.window);
        fx.disk(2)
            .when_a_write_is_stored(move |_| clock.advance(2 * window));
    }

    let outcome = fx.run().await;

    assert!(
        fx.disk(2).holds(frag(fx.chunk, 2)),
        "the unverified write's bytes may have landed — here they did"
    );
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    assert!(
        fx.mark(2, 2).is_some(),
        "the landed bytes are under the pre-mark"
    );
    assert!(fx.queued(), "the obligation must stay queued");
    fx.assert_every_fragment_named_or_marked("unverified write");
    assert!(named_on_audit_seam("write-unverified"));
}

// Leg (C)(v) needs a deadline that alone would still accept a write authorized past `W_repoint`,
// so that only the gate can refuse it.
const _: () = assert!(W_WRITE_MILLIS > W_REPOINT_MILLIS);

/// **(C)(v)** No destination write is authorized on a pre-mark `W_repoint` old or older
/// (`0016:1339-1349`). A hook moves the clock exactly `W_repoint` on right after the pre-mark
/// commits: no write is sent at all — the pre-mark stands and nothing is adopted — although the
/// write's own deadline (`W_write` after the pre-mark, longer than `W_repoint`) would still have
/// let the D server accept it. One tick less, and the move completes.
#[tokio::test]
async fn no_write_is_authorized_on_a_premark_w_repoint_old() {
    capture_audit();
    for (i, (age, gated)) in [(W_REPOINT_MILLIS, true), (W_REPOINT_MILLIS - 1, false)]
        .into_iter()
        .enumerate()
    {
        let fx = Fixture::standard(&format!("f{i}"), 0x814B).await;
        {
            let clock = fx.clock.clone();
            fx.meta
                .after_commit_of(fx.is_premark(&fx.mark_key(2, 2)), move |_| {
                    clock.advance(age)
                });
        }

        let outcome = fx.run().await;

        if gated {
            assert!(
                fx.arrivals().is_empty(),
                "a pre-mark {age} ms old authorized a write: {:?}",
                fx.arrivals()
            );
            assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
            assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
            let premark = fx.meta.value(&fx.mark_key(2, 2));
            assert!(
                premark.as_deref().is_some_and(|b| is_fresh_premark(b, NOW)),
                "the stale pre-mark stands: {premark:?}"
            );
            assert!(fx.queued(), "the obligation must stay queued");
            assert!(named_on_audit_seam("pre-mark-stale"));
        } else {
            assert_eq!(
                outcome,
                Reconciled::Changed,
                "a pre-mark {age} ms old is still inside W_repoint, so the move completes"
            );
            assert!(fx.holds_intact(2, 2));
        }
    }
}

/// **(C)(vi)** A vacated position whose existing `orphan:` value decodes as none of the three mark
/// shapes withholds the move before anything is written — no pre-mark, no write, no adoption —
/// keeps the obligation queued, names the mark on the audit seam, and never overwrites it
/// (ADR-0045); the pass does not certify. Beside it, a vacated position carrying a legacy mark is
/// re-stamped by the adoption, and one already `reclaiming` is left to GC untouched.
#[tokio::test]
async fn an_unreadable_vacated_mark_withholds_the_move() {
    capture_audit();
    let unreadable = Bytes::from_static(b"{\"orphaned_at_millis\":\"soon\"}");
    let fx = Fixture::standard("a2", 0x814C).await;
    fx.meta.seed(fx.mark_key(3, 2), unreadable.clone());

    let outcome = fx.run().await;

    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a withheld repair must not be certified over"
    );
    assert!(
        fx.arrivals().is_empty(),
        "nothing may be written: {:?}",
        fx.arrivals()
    );
    assert!(fx.mark(2, 2).is_none(), "no pre-mark may be written");
    assert_eq!(
        fx.meta.value(&fx.mark_key(3, 2)),
        Some(unreadable),
        "the unreadable mark must never be overwritten"
    );
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    assert!(fx.queued(), "the obligation must stay queued");
    assert!(
        named_on_audit_seam(&String::from_utf8(fx.mark_key(3, 2)).unwrap()),
        "the unreadable mark must be named on the audit seam"
    );

    // The readable shapes, beside it: the move goes ahead.
    let legacy = encode_orphan_mark(&OrphanMark::legacy(1));
    let reclaiming = encode_orphan_mark(&OrphanMark::legacy(1).into_reclaiming());
    for (i, (what, mark)) in [("legacy", legacy), ("reclaiming", reclaiming.clone())]
        .into_iter()
        .enumerate()
    {
        let fx = Fixture::standard(&format!("a{}", 3 + i), 0x814D).await;
        fx.meta.seed(fx.mark_key(3, 2), mark.clone());

        assert_eq!(fx.run().await, Reconciled::Changed, "{what}");
        let after = fx.meta.value(&fx.mark_key(3, 2)).expect("a vacated mark");
        if mark == reclaiming {
            assert_eq!(after, mark, "a reclaiming mark is GC's, and left as it is");
        } else {
            assert_eq!(
                decode_orphan_mark(&after).unwrap(),
                OrphanMark::legacy(NOW),
                "{what}: the vacated position is marked with the adoption's own instant"
            );
        }
    }
}

/// How long every stored write takes in leg (C)(vii): longer than `W_repoint`, so a gate re-checked
/// against the one pre-mark before a second write refuses it, and yet two of them back to back
/// still land inside `W_write`, so the D server refuses neither for timing alone, whatever order
/// the move sends them in.
const SLOW_WRITE_MILLIS: u64 = 12_000;
const _: () = assert!(SLOW_WRITE_MILLIS > W_REPOINT_MILLIS);
const _: () = assert!(2 * SLOW_WRITE_MILLIS < W_WRITE_MILLIS);

/// **(C)(vii)** A slow but legal write never stops a multi-fragment move. RS(2,2) with fragments 2
/// and 3 lost (their servers died), two free destinations in two free failure domains (servers 4
/// and 5), and every stored write moves the clock on by [`SLOW_WRITE_MILLIS`] — longer than
/// `W_repoint`, and twice that still inside `W_write`. Within at most TWO passes both fragments are
/// rebuilt, the part record names both new servers, and the obligation has drained.
///
/// A move that re-checks the `W_repoint` gate against its one pre-mark before each write lets the
/// first write's latency use up the second's authorization: every pass refuses the second write
/// and rewrites the first, and the chunk stays degraded while passes keep running.
#[tokio::test]
async fn a_slow_but_legal_write_never_stops_a_multi_fragment_move() {
    capture_audit();
    let spec = Spec {
        servers: vec![
            live(0, "A"),
            live(1, "B"),
            dead(2, None),
            dead(3, None),
            live(4, "C"),
            live(5, "D"),
        ],
        scheme: RS_2_2,
        placement: vec![0, 1, 2, 3],
        lost: vec![2, 3],
        state: OPEN.to_owned(),
    };
    let fx = Fixture::new("d2", 0x8156, spec).await;
    for dserver in [4, 5] {
        let clock = fx.clock.clone();
        fx.disk(dserver)
            .when_a_write_is_stored(move |_| clock.advance(SLOW_WRITE_MILLIS));
    }

    let mut outcomes = Vec::new();
    while outcomes.len() < 2 && fx.queued() {
        outcomes.push(fx.run().await);
    }

    assert!(
        !fx.queued(),
        "two passes must rebuild and adopt both fragments, so the obligation drains: {outcomes:?}, \
         writes {:?}",
        fx.arrivals()
    );
    assert_eq!(
        outcomes.last(),
        Some(&Reconciled::Changed),
        "the pass that drained the obligation adopted the move"
    );
    let placement = fx.placement();
    assert_eq!(placement[..2], [0, 1], "the survivors stay where they are");
    let mut moved = placement[2..].to_vec();
    moved.sort_unstable();
    assert_eq!(
        moved,
        vec![4, 5],
        "the part record must name both new servers: {placement:?}"
    );
    assert!(
        fx.holds_intact(placement[2], 2) && fx.holds_intact(placement[3], 3),
        "both rebuilt fragments must be byte-for-byte the shards the writer encoded, where the \
         part record names them"
    );
    assert!(
        fx.mark(2, 2).is_some() && fx.mark(3, 3).is_some(),
        "both vacated positions must be marked"
    );
    assert!(
        fx.mark(placement[2], 2).is_none() && fx.mark(placement[3], 3).is_none(),
        "both pre-marks must be consumed by the adoption"
    );
}

/// How long each destination write takes in leg (C)(vii)'s second case, from its arrival at its D
/// server to its publication: legal on its own, inside `W_write`, and yet two of them one after the
/// other cannot both land inside it — so the move completes only if it sends them together.
const PARALLEL_WRITE_MILLIS: u64 = 20_000;
const _: () = assert!(PARALLEL_WRITE_MILLIS < W_WRITE_MILLIS);
const _: () = assert!(2 * PARALLEL_WRITE_MILLIS >= W_WRITE_MILLIS);

/// **(C)(vii)** The writes of one move are SENT TOGETHER, so each has the whole of `W_write` from
/// the pre-mark to land in, however long the others take. RS(2,2) with fragments 2 and 3 lost
/// (their servers died), destinations servers 4 and 5, and each write taking
/// [`PARALLEL_WRITE_MILLIS`] from its arrival to its publication — legal on its own, but more than
/// half of `W_write`. Both writes arrive at the pre-mark's own instant, before either publishes,
/// both land, and ONE pass adopts the move.
///
/// A move that awaits its writes one after another sends the second only once the first has
/// landed, so the second publishes past the deadline its pre-mark fixed and the D server cannot
/// certify it. Every pass then aborts the move again, and the chunk stays degraded for good under
/// a legal timing.
#[tokio::test]
async fn a_moves_writes_are_sent_together() {
    capture_audit();
    let spec = Spec {
        servers: vec![
            live(0, "A"),
            live(1, "B"),
            dead(2, None),
            dead(3, None),
            live(4, "C"),
            live(5, "D"),
        ],
        scheme: RS_2_2,
        placement: vec![0, 1, 2, 3],
        lost: vec![2, 3],
        state: OPEN.to_owned(),
    };
    let fx = Fixture::new("d4", 0x815A, spec).await;
    let arrived = Arc::new(Mutex::new(Vec::new()));
    for dserver in [4, 5] {
        fx.disk(dserver).taking(PARALLEL_WRITE_MILLIS);
        let (clock, arrived) = (fx.clock.clone(), Arc::clone(&arrived));
        fx.disk(dserver)
            .when_a_write_arrives(move |_| arrived.lock().unwrap().push(clock.now_millis()));
    }

    let mut outcomes = Vec::new();
    while outcomes.len() < 2 && fx.queued() {
        outcomes.push(fx.run().await);
    }

    assert_eq!(
        outcomes,
        vec![Reconciled::Changed],
        "one pass must rebuild and adopt both fragments: writes arrived at {:?}",
        arrived.lock().unwrap()
    );
    assert!(!fx.queued(), "the obligation must drain with the adoption");
    assert_eq!(
        *arrived.lock().unwrap(),
        vec![NOW, NOW],
        "both writes must arrive at the pre-mark's instant, before either publishes"
    );
    assert!(
        fx.arrivals()
            .iter()
            .all(|&(_, _, deadline)| deadline == Some(NOW + W_WRITE_MILLIS)),
        "every write carries the deadline the one pre-mark's stamp fixes: {:?}",
        fx.arrivals()
    );
    let placement = fx.placement();
    let mut moved = placement[2..].to_vec();
    moved.sort_unstable();
    assert_eq!(
        moved,
        vec![4, 5],
        "the part record must name both new servers: {placement:?}"
    );
    assert!(
        fx.holds_intact(placement[2], 2) && fx.holds_intact(placement[3], 3),
        "both rebuilt fragments must be byte-for-byte the shards the writer encoded, where the \
         part record names them"
    );
    assert!(
        fx.mark(2, 2).is_some() && fx.mark(3, 3).is_some(),
        "both vacated positions must be marked"
    );
    assert!(
        fx.mark(placement[2], 2).is_none() && fx.mark(placement[3], 3).is_none(),
        "both pre-marks must be consumed by the adoption"
    );
}

/// **(C)(viii)** GC reclaiming the destination between the write and the adoption makes the
/// adoption lose. The instant the destination write is stored, a hook does what GC's reclaim does,
/// in GC's order: it swaps `orphan:<P_new>` to its `reclaiming` form with the same stamp and event
/// (`OrphanMark::into_reclaiming`, the exact-value swap GC commits before it deletes anything), and
/// then deletes the fragment from the destination. The pass then resumes: nothing is adopted, the
/// `part:` record is byte-identical, the `reclaiming` mark is left as GC wrote it, and the
/// obligation stays queued. The destination did receive the write, so this cannot pass on a move
/// that writes nothing; only the adoption's precondition on the pre-mark's own bytes can make it
/// lose here.
#[tokio::test]
async fn gc_reclaiming_the_destination_before_the_adoption_makes_it_lose() {
    capture_audit();
    let fx = Fixture::standard("d3", 0x8157).await;
    let reclaimed = Arc::new(Mutex::new(None));
    {
        let (meta, key, reclaimed) = (
            Arc::clone(&fx.meta),
            fx.mark_key(2, 2),
            Arc::clone(&reclaimed),
        );
        let disk = Arc::downgrade(&fx.disks[2]);
        fx.disk(2).when_a_write_is_stored(move |id| {
            let mut reclaimed = reclaimed.lock().unwrap();
            if reclaimed.is_some() {
                return;
            }
            let premark = meta.value(&key).expect("the pre-mark GC reclaims");
            let reclaiming = encode_orphan_mark(
                &decode_orphan_mark(&premark)
                    .expect("a pre-mark the codec reads")
                    .into_reclaiming(),
            );
            let swap = WriteBatch::new()
                .require(key.clone(), premark)
                .put(key.clone(), reclaiming.clone());
            assert_eq!(meta.apply(&swap), CommitOutcome::Committed);
            let disk = disk.upgrade().expect("the destination is still up");
            disk.frags.lock().unwrap().remove(&id);
            *reclaimed = Some(reclaiming);
        });
    }

    let outcome = fx.run().await;

    let reclaiming = reclaimed
        .lock()
        .unwrap()
        .clone()
        .expect("GC never reclaimed the destination between the write and the adoption");
    assert_eq!(
        fx.disk(2).arrivals().len(),
        1,
        "the destination must have received the move's write"
    );
    assert!(
        !fx.disk(2).holds(frag(fx.chunk, 2)),
        "GC deleted what the move wrote"
    );
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert_eq!(
        fx.meta.value(&fx.part_key),
        Some(fx.part.clone()),
        "the part record must be byte-identical: an adoption naming reclaimed bytes is a loss"
    );
    assert_eq!(
        fx.meta.value(&fx.mark_key(2, 2)),
        Some(reclaiming),
        "the `reclaiming` mark must be left exactly as GC wrote it"
    );
    assert!(
        fx.mark(3, 2).is_none(),
        "a lost adoption marks no vacated position"
    );
    assert!(fx.queued(), "the obligation must stay queued");
    fx.assert_every_fragment_named_or_marked("GC reclaimed the destination before the adoption");
}

/// **(C)(viii)** GC reclaiming the destination EARLIER — after the pass reads the destination
/// position and before the pre-mark commits — makes the pre-mark lose, on the precondition it
/// takes on the mark it read: nothing is written, and the `reclaiming` mark is left exactly as GC
/// wrote it. A pre-mark that replaced the mark without that pin would overwrite GC's decision, and
/// once its swap has committed GC deletes the fragment at the position without reading the mark
/// again (`record_intents`, `crates/custodian/src/gc.rs`) — so the move's write could be deleted
/// beneath an adoption that names it. Two cases, one per arm of the pin:
///
/// - a structured mark from another event, read and pinned as those bytes, which GC then swaps
///   to `reclaiming` (`OrphanMark::into_reclaiming`, the exact-value swap GC commits);
/// - no mark, read and pinned as absent, where another event marks the position and GC swaps
///   that mark to `reclaiming`.
///
/// Base: no destination is chosen, so GC's swap never lands.
#[tokio::test]
async fn gc_reclaiming_the_destination_before_the_premark_writes_nothing() {
    capture_audit();
    let other = encode_orphan_mark(&OrphanMark::structured(1, "g:9:2").unwrap());
    for (i, seeded) in [true, false].into_iter().enumerate() {
        let what = if seeded { "a stamped mark" } else { "no mark" };
        let fx = Fixture::standard(&format!("d{}", 6 + i), 0x815E + i as ChunkId).await;
        if seeded {
            fx.meta.seed(fx.mark_key(2, 2), other.clone());
        }
        let reclaimed = Arc::new(Mutex::new(None));
        {
            let (key, other, reclaimed) =
                (fx.mark_key(2, 2), other.clone(), Arc::clone(&reclaimed));
            fx.meta.after_read_of(&fx.mark_key(2, 2), move |meta| {
                if !seeded {
                    let marked = WriteBatch::new()
                        .require_absent(key.clone())
                        .put(key.clone(), other.clone());
                    assert_eq!(meta.apply(&marked), CommitOutcome::Committed);
                }
                let reclaiming = encode_orphan_mark(
                    &decode_orphan_mark(&other)
                        .expect("a mark the codec reads")
                        .into_reclaiming(),
                );
                let swap = WriteBatch::new()
                    .require(key.clone(), other)
                    .put(key, reclaiming.clone());
                assert_eq!(meta.apply(&swap), CommitOutcome::Committed);
                *reclaimed.lock().unwrap() = Some(reclaiming);
            });
        }

        let outcome = fx.run().await;

        let reclaiming = reclaimed
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| panic!("{what}: GC never reclaimed the destination position"));
        assert_ne!(outcome, Reconciled::Changed, "{what}: nothing was adopted");
        assert!(
            fx.arrivals().is_empty(),
            "{what}: no write may be sent to a position GC is reclaiming: {:?}",
            fx.arrivals()
        );
        assert_eq!(
            fx.meta.value(&fx.mark_key(2, 2)),
            Some(reclaiming),
            "{what}: the `reclaiming` mark must be left exactly as GC wrote it"
        );
        assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()), "{what}");
        assert!(fx.queued(), "{what}: the obligation must stay queued");
        assert!(
            audit_event_naming(&["pre-mark-lost", &wyrd_traits::chunk_hex(fx.chunk)]),
            "{what}: the aborted move must say why on the audit seam"
        );
    }
}

/// **(C)(viii)** GC reclaiming the VACATED position — the one the part record still names for the
/// lost fragment — after the assessment read its mark and before the adoption makes the adoption
/// lose. The instant the pass has read `orphan:<P_old>`, a hook commits GC's own swap of that
/// mark to `reclaiming` (`OrphanMark::into_reclaiming`, the exact-value swap GC commits before it
/// deletes anything). The move goes on as it would: the pre-mark commits, the destination write
/// lands, and the adoption is tried — and loses, on the precondition it takes on the vacated mark
/// as read. Nothing is adopted, the `part:` record is byte-identical, the `reclaiming` mark is
/// left exactly as GC wrote it — never overwritten with the adoption's own `legacy(now)`, which
/// would replace GC's decision (no writer replaces a `reclaiming` mark) — the pre-mark still
/// stands over the written fragment, and the obligation stays queued. The destination did receive
/// the write, so this cannot pass on a move that writes nothing, nor on one the pre-mark batch
/// stopped. Two cases, one per arm of the pin:
///
/// - a structured mark from another event, read and pinned as those bytes, which GC then swaps
///   to `reclaiming`;
/// - no mark, read and pinned as absent, where another event marks the position and GC swaps
///   that mark to `reclaiming`.
///
/// Base: nothing reads the vacated position, so GC's swap never lands.
#[tokio::test]
async fn gc_reclaiming_the_vacated_position_before_the_adoption_makes_it_lose() {
    capture_audit();
    let other = encode_orphan_mark(&OrphanMark::structured(1, "g:9:2").unwrap());
    for (i, seeded) in [true, false].into_iter().enumerate() {
        let what = if seeded { "a stamped mark" } else { "no mark" };
        let fx = Fixture::standard(&format!("d{}", 8 + i), 0x8161 + i as ChunkId).await;
        if seeded {
            fx.meta.seed(fx.mark_key(3, 2), other.clone());
        }
        let reclaimed = Arc::new(Mutex::new(None));
        {
            let (key, other, reclaimed) =
                (fx.mark_key(3, 2), other.clone(), Arc::clone(&reclaimed));
            fx.meta.after_read_of(&fx.mark_key(3, 2), move |meta| {
                if !seeded {
                    let marked = WriteBatch::new()
                        .require_absent(key.clone())
                        .put(key.clone(), other.clone());
                    assert_eq!(meta.apply(&marked), CommitOutcome::Committed);
                }
                let reclaiming = encode_orphan_mark(
                    &decode_orphan_mark(&other)
                        .expect("a mark the codec reads")
                        .into_reclaiming(),
                );
                let swap = WriteBatch::new()
                    .require(key.clone(), other)
                    .put(key, reclaiming.clone());
                assert_eq!(meta.apply(&swap), CommitOutcome::Committed);
                *reclaimed.lock().unwrap() = Some(reclaiming);
            });
        }

        let outcome = fx.run().await;

        let reclaiming = reclaimed.lock().unwrap().clone().unwrap_or_else(|| {
            panic!("{what}: GC never reclaimed the vacated position before the adoption")
        });
        assert_eq!(
            fx.disk(2).arrivals().len(),
            1,
            "{what}: the destination must have received the move's write"
        );
        assert!(
            fx.disk(2).holds(frag(fx.chunk, 2)),
            "{what}: the written fragment is there, under its pre-mark"
        );
        assert_ne!(outcome, Reconciled::Changed, "{what}: nothing was adopted");
        assert_eq!(
            fx.meta.value(&fx.part_key),
            Some(fx.part.clone()),
            "{what}: the part record must be byte-identical after a lost adoption"
        );
        assert_eq!(
            fx.meta.value(&fx.mark_key(3, 2)),
            Some(reclaiming),
            "{what}: the `reclaiming` mark must be left exactly as GC wrote it"
        );
        let premark = fx.meta.value(&fx.mark_key(2, 2));
        assert!(
            premark.as_deref().is_some_and(|b| is_fresh_premark(b, NOW)),
            "{what}: the pre-mark must still stand after a lost adoption: {premark:?}"
        );
        assert!(fx.queued(), "{what}: the obligation must stay queued");
        assert!(
            audit_event_naming(&["conflict", &wyrd_traits::chunk_hex(fx.chunk)]),
            "{what}: the lost adoption must be a conflict on the audit seam"
        );
        fx.assert_every_fragment_named_or_marked(what);
    }
}

/// **(Withheld)** An upload whose session record will not decode: this pass cannot tell whether it
/// is `Open`, so it writes nothing, keeps the obligation, names the record and does not certify.
#[tokio::test]
async fn an_unreadable_session_withholds_the_move() {
    capture_audit();
    let fx = Fixture::standard("a5", 0x814E).await;
    let damaged = Bytes::from_static(b"{\"parent\":42}");
    fx.meta.seed(mpu_key(&fx.upload), damaged.clone());

    let outcome = fx.run().await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert!(fx.arrivals().is_empty());
    assert!(fx.meta.keys_under(b"orphan:").is_empty());
    assert_eq!(fx.meta.value(&mpu_key(&fx.upload)), Some(damaged));
    assert!(fx.queued());
    assert!(named_on_audit_seam(
        &String::from_utf8(mpu_key(&fx.upload)).unwrap()
    ));
}

// ---- (D) the drain fence on the destination -----------------------------------------------------

/// **(D)** A server with ANY `desired:dserver:<S>` record — whatever its value, an unrecognised
/// `maintenance` included — is never chosen, so selection and the adoption's
/// `require_absent(desired:dserver:<S_new>)` test the same fact. The selector's first pick,
/// server 2, carries one; its next, server 3, is the dead server the lost fragment lived on (in
/// the topology, not in the fleet). The move lands on server 4, in ONE pass — where choosing a
/// server the CAS then refuses stalls the chunk pass after pass, every one answering `Satisfied`
/// (v1's `maintenance` repro).
#[tokio::test]
async fn a_server_with_any_desired_state_record_is_never_chosen() {
    capture_audit();
    for (i, value) in ["maintenance", "draining"].into_iter().enumerate() {
        let spec = Spec {
            servers: vec![
                live(0, "A"),
                live(1, "B"),
                live(2, "C"),
                dead(3, Some("D")),
                live(4, "E"),
            ],
            ..Spec::standard()
        };
        let fx = Fixture::new(&format!("b{}", 4 + i), 0x814F, spec).await;
        fx.meta.seed(desired_key(2), value.as_bytes().to_vec());

        let outcome = fx.run().await;

        assert_eq!(
            outcome,
            Reconciled::Changed,
            "{value}: one pass must repair the chunk"
        );
        assert!(
            fx.disk(2).arrivals().is_empty(),
            "{value}: a server with a desired-state record received a write"
        );
        assert_eq!(fx.placement(), vec![0, 1, 4], "{value}");
        assert!(fx.holds_intact(4, 2), "{value}");
        assert_eq!(
            fx.meta.value(&desired_key(2)),
            Some(Bytes::from(value.as_bytes().to_vec())),
            "{value}: the desired-state record is the operator's, untouched"
        );
    }
}

/// **(D)** A drain recorded on the destination between its selection and the adoption — here, the
/// instant the write lands — makes the adoption lose on `require_absent(desired:dserver:<S_new>)`:
/// nothing is adopted, and the pre-mark stands over what was written.
#[tokio::test]
async fn a_drain_recorded_before_the_adoption_makes_it_lose() {
    capture_audit();
    let fx = Fixture::standard("b6", 0x8150).await;
    {
        let meta = Arc::clone(&fx.meta);
        fx.disk(2).when_a_write_is_stored(move |_| {
            meta.seed(desired_key(2), b"draining".to_vec());
        });
    }

    let outcome = fx.run().await;

    assert!(fx.meta.holds(&desired_key(2)), "the drain was recorded");
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    let premark = fx.meta.value(&fx.mark_key(2, 2));
    assert!(
        premark.as_deref().is_some_and(|b| is_fresh_premark(b, NOW)),
        "the pre-mark must still stand: {premark:?}"
    );
    assert!(fx.queued(), "the obligation must stay queued");
    fx.assert_every_fragment_named_or_marked("drain recorded before the adoption");
}

/// **(D)** A drain recorded on the destination EARLIER — after the pass has chosen it and read its
/// position, before the pre-mark commits — makes the pre-mark lose on its own
/// `require_absent(desired:dserver:<S_new>)`: nothing is marked, and no write is sent to a server
/// that is being drained. The adoption pins the same fact, but a pre-mark that took no notice
/// would write to the draining server first.
///
/// Base: no destination is chosen, so the drain is never recorded.
#[tokio::test]
async fn a_drain_recorded_before_the_premark_writes_nothing() {
    capture_audit();
    let fx = Fixture::standard("b8", 0x8160).await;
    fx.meta.after_read_of(&fx.mark_key(2, 2), |meta| {
        meta.seed(desired_key(2), b"draining".to_vec());
    });

    let outcome = fx.run().await;

    assert!(
        fx.meta.holds(&desired_key(2)),
        "the drain was never recorded between the choice and the pre-mark"
    );
    assert_ne!(outcome, Reconciled::Changed, "nothing was adopted");
    assert!(
        fx.arrivals().is_empty(),
        "no write may be sent once the pre-mark lost to the drain: {:?}",
        fx.arrivals()
    );
    assert!(
        fx.mark(2, 2).is_none(),
        "the pre-mark lost, so there is none"
    );
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    assert!(fx.queued(), "the obligation must stay queued");
    assert!(
        audit_event_naming(&["pre-mark-lost", &wyrd_traits::chunk_hex(fx.chunk)]),
        "the aborted move must say why on the audit seam"
    );
}

// ---- (E) kept, not rebuilt ----------------------------------------------------------------------

/// **(E)** A chunk only an in-flight owned staging entry names — its part is still streaming, with
/// no committed scheme to rebuild from — keeps its obligation, and nothing is written.
#[tokio::test]
async fn a_chunk_only_an_owned_entry_names_is_kept() {
    capture_audit();
    let fx = Fixture::standard("c5", 0x8151).await;
    // Replace the committed part with an owned entry planning the same chunk.
    fx.meta.kv.lock().unwrap().remove(&fx.part_key);
    let key = sidx_key(&fx.upload, part_no(PART + 1), fx.chunk);
    let staged = StagedPlacement::new(RS_2_1, vec![0, 1, 3]).expect("a supported scheme");
    let value =
        metadata::encode(&OwnedEntry::new(fx.upload.clone(), NOW * 1_000, staged).to_pending());
    fx.meta.seed(key, value);

    let outcome = fx.run().await;

    assert_eq!(outcome, Reconciled::Blocked);
    assert!(fx.queued(), "the obligation must be kept");
    assert!(fx.arrivals().is_empty(), "nothing may be written");
    assert!(
        fx.meta.keys_under(b"orphan:").is_empty(),
        "nothing may be marked"
    );
}

/// **(E)** A chunk the staged reading holds as untrusted (`StagedSet::held`) — a corrupt owned
/// entry under a key naming it — keeps its obligation and nothing is written, even though a valid
/// committed part in an `Open` upload names it too and would otherwise be repaired: nothing is
/// rebuilt over a chunk a staged record says it cannot be trusted about.
#[tokio::test]
async fn a_chunk_an_untrusted_owned_entry_holds_is_kept() {
    capture_audit();
    let fx = Fixture::standard("cb", 0x8158).await;
    // The committed part stays; beside it, an owned entry for the same chunk that will not decode.
    let damaged = sidx_key(&fx.upload, part_no(PART + 1), fx.chunk);
    fx.meta
        .seed(damaged.clone(), Bytes::from_static(b"not an owned entry"));

    let outcome = fx.run().await;

    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a kept obligation is not certified over"
    );
    assert!(fx.queued(), "the obligation must be kept");
    assert!(
        fx.arrivals().is_empty(),
        "nothing may be written: {:?}",
        fx.arrivals()
    );
    assert!(
        fx.meta.keys_under(b"orphan:").is_empty(),
        "nothing may be marked"
    );
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    assert!(
        named_on_audit_seam(&wyrd_traits::chunk_hex(fx.chunk)),
        "the kept chunk must be named on the audit seam"
    );
}

/// **(E)** A degraded chunk with no usable destination — every server a survivor does not already
/// occupy carries a desired-state record — is kept: nothing is marked or written, the obligation
/// stays queued, and each server passed over is named. As for a committed chunk with no free
/// failure domain, it is off the repairable backlog and not a hole in the pass (`Satisfied`); it
/// is repaired once a destination is usable again.
#[tokio::test]
async fn a_degraded_chunk_with_no_usable_destination_is_kept() {
    capture_audit();
    let fx = Fixture::standard("cc", 0x8159).await;
    for dserver in [2, 3] {
        fx.meta.seed(desired_key(dserver), b"draining".to_vec());
    }

    let outcome = fx.run().await;

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "blocked for want of a destination, as a committed chunk with no free domain is"
    );
    assert!(fx.queued(), "the obligation must be kept");
    assert!(
        fx.arrivals().is_empty(),
        "nothing may be written: {:?}",
        fx.arrivals()
    );
    assert!(
        fx.meta.keys_under(b"orphan:").is_empty(),
        "nothing may be marked"
    );
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
    assert!(
        named_on_audit_seam("desired-state"),
        "a server passed over must be named on the audit seam"
    );

    // A destination becomes usable again: the next pass repairs the chunk there.
    fx.meta.kv.lock().unwrap().remove(&desired_key(2));
    assert_eq!(fx.run().await, Reconciled::Changed);
    assert_eq!(fx.placement(), vec![0, 1, 2]);
    assert!(fx.holds_intact(2, 2));
    assert!(!fx.queued());
}

/// **(E)** A committed part's chunk in an upload that has left `Open` — `Completing`, `Aborting`
/// or `Completed` — keeps its obligation, and nothing is written: every other state has already
/// fenced a re-place. The chunk is RS(2,2) with one fragment lost, so it is repairable: the
/// refusal is the session state's, not the scheme's. The pass answers `Blocked`, as the kept path
/// does. The repair blocked by a Complete is retried after publication (`0016:825`): once the
/// committed inode names the chunk, the next pass repairs it through the committed map.
#[tokio::test]
async fn a_chunk_of_an_upload_that_has_left_open_is_kept_then_repaired_once_published() {
    capture_audit();
    let states = [
        ("completing", completing(EPOCH)),
        ("aborting", ABORTING.to_owned()),
        ("completed", completed()),
    ];
    for (i, (what, state)) in states.into_iter().enumerate() {
        // Fragment 3 is lost with its server; the one free failure domain is server 4's.
        let spec = Spec {
            servers: vec![
                live(0, "A"),
                live(1, "B"),
                live(2, "C"),
                dead(3, None),
                live(4, "E"),
            ],
            scheme: RS_2_2,
            placement: vec![0, 1, 2, 3],
            lost: vec![3],
            state,
        };
        let fx = Fixture::new(&format!("c{}", 6 + i), 0x8152, spec).await;

        let outcome = fx.run().await;

        assert_eq!(outcome, Reconciled::Blocked, "{what}");
        assert!(fx.queued(), "{what}: the obligation must be kept");
        assert!(fx.arrivals().is_empty(), "{what}: nothing may be written");
        assert!(
            fx.meta.keys_under(b"orphan:").is_empty(),
            "{what}: nothing may be marked"
        );
        assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()), "{what}");

        if what == "completing" {
            // The publication lands: the committed inode names the chunk where the part did.
            let seeded = decode_part_record(&fx.part).unwrap();
            let published = InodeRecord {
                size: DATA.len() as u64,
                chunk_map: seeded.chunks().to_vec().into(),
                state: InodeState::Committed,
                version: 1,
                ..Default::default()
            };
            fx.meta
                .seed(metadata::inode_key(PUBLISHED), metadata::encode(&published));
            fx.meta.seed(
                metadata::dirent_key(PARENT, OBJECT),
                metadata::encode(&DirentRecord { inode: PUBLISHED }),
            );
            fx.meta
                .seed(mpu_key(&fx.upload), session(&completed(), EPOCH));

            assert_eq!(
                fx.run().await,
                Reconciled::Changed,
                "the kept obligation must be repaired once the chunk is published"
            );
            let inode: InodeRecord = metadata::decode(
                &fx.meta
                    .value(&metadata::inode_key(PUBLISHED))
                    .expect("the published inode"),
            )
            .unwrap();
            assert_eq!(
                inode.chunk_map.as_flat().expect("a flat map")[0].placement,
                vec![0, 1, 2, 4]
            );
            assert!(fx.holds_intact(4, 3));
            assert!(!fx.queued());
        }
    }
}

/// **(Drain)** A committed part's chunk in an `Open` upload found at FULL redundancy resolves as a
/// committed chunk does: its obligation drains as a duplicate finding, the pass is `Satisfied`,
/// and nothing is written.
#[tokio::test]
async fn an_open_uploads_intact_chunk_drains_its_obligation() {
    capture_audit();
    let spec = Spec {
        lost: Vec::new(),
        ..Spec::standard()
    };
    let fx = Fixture::new("c9", 0x8153, spec).await;

    let outcome = fx.run().await;

    assert_eq!(outcome, Reconciled::Satisfied);
    assert!(!fx.queued(), "the duplicate obligation must drain");
    assert!(fx.arrivals().is_empty());
    assert!(fx.meta.keys_under(b"orphan:").is_empty());
    assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
}

/// **(Outage)** A committed part's chunk in an `Open` upload that is below `k` only because a
/// placed server is unreachable this pass resolves as a committed chunk does: degraded for now,
/// not lost. RS(2,1) on `[0, 1, 3]`: fragment 0 survives, fragment 1's server is out of the
/// fleet, and fragment 2 is lost from server 3's disk. With server 1 reported unreachable, the one
/// survivor and the one fragment behind the outage reach `k`: no data-loss signal, nothing written
/// or marked, the obligation kept for the server's return, and the pass not held back
/// (`Satisfied`, as for a committed chunk). With server 1 NOT reported unreachable, the same
/// chunk is below `k` for good, and the pass raises the data-loss signal naming it.
#[tokio::test]
async fn a_chunk_below_k_only_behind_an_outage_is_not_data_loss() {
    capture_audit();
    for (i, reported) in [true, false].into_iter().enumerate() {
        let spec = Spec {
            servers: vec![live(0, "A"), dead(1, Some("B")), live(2, "C"), live(3, "D")],
            ..Spec::standard()
        };
        let chunk = 0x815B + i as ChunkId;
        let mut fx = Fixture::new(&format!("e{}", 4 + i), chunk, spec).await;
        if reported {
            fx.unreachable = vec![1];
        }

        let outcome = fx.run().await;

        assert_eq!(
            outcome,
            Reconciled::Satisfied,
            "unreachable {reported}: not a hole in the pass, as for a committed chunk"
        );
        assert!(
            fx.queued(),
            "unreachable {reported}: the obligation must be kept"
        );
        assert!(
            fx.arrivals().is_empty(),
            "unreachable {reported}: nothing may be written: {:?}",
            fx.arrivals()
        );
        assert!(
            fx.meta.keys_under(b"orphan:").is_empty(),
            "unreachable {reported}: nothing may be marked"
        );
        assert_eq!(fx.meta.value(&fx.part_key), Some(fx.part.clone()));
        let lost = audit_event_naming(&["data-loss", &wyrd_traits::chunk_hex(chunk)]);
        if reported {
            assert!(
                !lost,
                "a chunk below k only behind an outage must not raise the data-loss signal"
            );
        } else {
            assert!(
                lost,
                "a chunk below k with no fragment behind an outage is lost, and must say so"
            );
        }
    }
}

/// **(First reference)** A chunk two committed parts name is repaired against ONE of them, the
/// first in key order — the rule the committed reading applies to a chunk two objects name
/// (`read_committed`) — and the move pins and repoints only that record: the other part record is
/// left byte-identical.
#[tokio::test]
async fn a_chunk_two_parts_name_is_repaired_against_the_first() {
    capture_audit();
    let fx = Fixture::standard("d5", 0x815D).await;
    let second = part_key(&fx.upload, part_no(PART + 1));
    assert!(fx.part_key < second, "the seeded part sorts first");
    fx.meta.seed(second.clone(), fx.part.clone());

    let outcome = fx.run().await;

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "one pass must repair the chunk"
    );
    assert_eq!(
        fx.placement(),
        vec![0, 1, 2],
        "the first part in key order must be the one repointed"
    );
    assert!(fx.holds_intact(2, 2));
    assert_eq!(
        fx.meta.value(&second),
        Some(fx.part.clone()),
        "the second part record must be left byte-identical"
    );
}

/// **(In place)** A fragment lost from a server that is still up, and still the selector's pick for
/// it, is rebuilt in place: the pre-mark goes on the fragment's own position, the adoption consumes
/// it and marks nothing vacated, and the part record — naming the same server — keeps its bytes.
#[tokio::test]
async fn a_fragment_lost_from_a_live_server_is_rebuilt_in_place() {
    capture_audit();
    let spec = Spec {
        placement: vec![0, 1, 2],
        ..Spec::standard()
    };
    let fx = Fixture::new("ca", 0x8154, spec).await;

    let outcome = fx.run().await;

    assert_eq!(outcome, Reconciled::Changed);
    assert!(
        fx.holds_intact(2, 2),
        "the fragment must be rebuilt on its own server"
    );
    assert_eq!(
        fx.meta.value(&fx.part_key),
        Some(fx.part.clone()),
        "the placement is unchanged, so the part record keeps its bytes"
    );
    assert!(
        fx.mark(2, 2).is_none(),
        "the pre-mark is consumed and nothing is marked vacated"
    );
    assert!(!fx.queued(), "the obligation must drain");
    assert_eq!(
        fx.arrivals(),
        vec![(2, frag(fx.chunk, 2), Some(NOW + W_WRITE_MILLIS))]
    );
}
