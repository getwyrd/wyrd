//! Issue #722: **a D-server drain holding a `seg:`-resident fragment completes.**
//!
//! #696 stopped the rebalance pass aborting on a segmented object, but deliberately wrote
//! nothing for one: an evacuation owed by a chunk whose `ChunkRef` lives in a `seg:` record was
//! refused and stayed owed, every pass, forever — so the drain never certified and the server
//! could never be retired. This file drives the drain through the placement move that
//! addresses whichever record holds a chunk, and observes the **store** and the audit seam.
//!
//! Every leg goes through the REAL fenced control point
//! [`reconcile_step`](wyrd_custodian::reconcile_step) over in-memory trait doubles:
//!
//! 1. a `seg:`-resident fragment is moved off the draining server, the `seg:` record is
//!    repointed, the vacated position is orphan-marked, and the root is untouched;
//! 2. two objects naming ONE chunk are BOTH repointed, so neither still names the drained
//!    server and every fragment either names exists where it says;
//! 3. a repoint that would push a `seg:` record past the value ceiling is refused: nothing is
//!    written, nothing copied, and the drain does not certify;
//! 4. a competing writer that takes the `seg:` record before the move commits wins: the move
//!    writes no metadata, publishes no orphan mark, and the copied fragment is left in place.
//!
//! Legs 5–11 pin how the drain answers each verdict the move can give before anything is
//! written, and the objects the scan must contain because the move cannot address them: a
//! ceiling refusal on a superseded root is a retry, not an operator signal (5); a version that
//! cannot advance (6) and a `seg:` record torn under the move (7) contain the object, named
//! ONCE however many of its chunks meet it; a store fault under the move ends the pass (8); a
//! chunk edited before the move reads it is a conflict that copied nothing (9); chunk lengths
//! past `u64` (10) and a segmented root under a non-canonical key (11) contain the object at
//! the scan, and the rest of the store still drains. Leg 12 pins which generation the move is
//! conditioned on: a root superseded mid-resolve is moved on its successor in the same pass.
//!
//! On the base legs 1, 2 and 4 are red (the refusal plans no move at all — so leg 4 is red for
//! the ordinary reason, not because it caught a race); leg 3 passes there for the other reason.
//! Legs 5–12 are red on the base too: it refuses every segmented object outright, panics on
//! leg 6's version, and moves leg 10's chunk instead of containing the object.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Once};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::{encode as encode_fragment, EcSchemeType, FragmentHeader};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    decode, encode, inode_key, orphan_key, seg_key, ChunkMap, ChunkRef, EcScheme, InodeId,
    InodeRecord, InodeState, SegmentGroup, SegmentRecord, SegmentRef, SegmentedMap,
    MAX_VALUE_BYTES,
};
use wyrd_core::placement::Topology;
use wyrd_custodian::desired_state::{
    reconciliation_status, set_lifecycle, DServerLifecycle, ReconciliationStatus,
};
use wyrd_custodian::{reconcile_step, Custodian, FencedZone, RebalanceContext, Reconciled};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    ScanPage, WriteBatch,
};

// ---- the fixture's fleet ----

/// The server the operator marks draining.
const DRAINING: DServerId = 0;
/// The chunk's two other fragments, which stay put.
const SURVIVORS: [DServerId; 2] = [1, 2];
/// The ONLY server in a failure domain distinct from every survivor's, so an evacuation that
/// keeps the chunk's spread must land here. Twenty digits on purpose: leg 3's growth vector.
const FREE: DServerId = u64::MAX;
/// RS(2,1): three fragments, one per domain — the drained one at index 0.
const SCHEME: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
const CHUNK: ChunkId = 0xC4_A1;
const SIBLING: ChunkId = 0x51_B1;
const OTHER: ChunkId = 0x07_E4;
const CHUNK_LEN: u64 = 5;
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const A: InodeId = 3;
const B: InodeId = 4;
/// Leg 8's injected fault — a plain STORE error, deliberately not a typed chunk-map anomaly.
const STORE_FAULT: &str = "metadata store unavailable";

/// What the fenced step answers.
type Answer = std::result::Result<Reconciled, wyrd_custodian::ReconcileError>;

// ---- in-memory trait stores ----

/// What the move's own re-read of a `seg:` record meets. The resolve reads a group's `seg:`
/// records with `scan_page`, never `get`, so a `seg:` `get` during a pass is the move's.
enum AtMoveRead {
    /// A competing write lands just before the read.
    Write(Vec<u8>, Bytes),
    /// The store fails under the read.
    Fail,
}

/// A `BTreeMap` metadata store with two seams: a write or fault at the move's own `seg:` read
/// (before the move has prepared anything), and a competing write applied inside `commit`,
/// AFTER the move has read and prepared its batch and BEFORE the batch's preconditions are
/// checked — the window a racing writer takes the record in.
#[derive(Default)]
struct MemMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    /// Leg 4: `(key, bytes)` written by the first commit that `put`s a `seg:` key.
    racer: Mutex<Option<(Vec<u8>, Bytes)>>,
    /// Legs 5 and 7–9: fired, once, by the first `seg:` `get`.
    at_move_read: Mutex<Option<AtMoveRead>>,
    /// Leg 12: `(key, bytes)` written, once, right after the first `seg:` page is read — the
    /// resolve's own range read, before its root re-read.
    after_seg_page: Mutex<Option<(Vec<u8>, Bytes)>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if key.starts_with(b"seg:") {
            let armed = self.at_move_read.lock().unwrap().take();
            match armed {
                Some(AtMoveRead::Write(key, bytes)) => {
                    self.kv.lock().unwrap().insert(key, bytes);
                }
                Some(AtMoveRead::Fail) => return Err(STORE_FAULT.into()),
                None => {}
            }
        }
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let kv = self.kv.lock().unwrap();
        let hits = kv.iter().filter(|(k, _)| k.starts_with(prefix));
        Ok(hits.map(|(k, v)| (k.clone(), v.clone())).collect())
    }
    async fn scan_page(&self, prefix: &[u8], after: Option<&[u8]>, n: usize) -> Result<ScanPage> {
        let page = wyrd_testkit::test_double_scan_page(self, prefix, after, n).await?;
        if prefix.starts_with(b"seg:") {
            if let Some((key, bytes)) = self.after_seg_page.lock().unwrap().take() {
                self.kv.lock().unwrap().insert(key, bytes);
            }
        }
        Ok(page)
    }
    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        if batch.puts.iter().any(|(k, _)| k.starts_with(b"seg:")) {
            if let Some((key, bytes)) = self.racer.lock().unwrap().take() {
                kv.insert(key, bytes);
            }
        }
        let mut checks = batch.preconditions.iter();
        if checks.any(|pre| kv.get(&pre.key).cloned() != pre.expected) {
            return Ok(CommitOutcome::Conflict);
        }
        assert!(batch.deletes.is_empty(), "no leg deletes metadata");
        kv.extend(batch.puts);
        Ok(CommitOutcome::Committed)
    }
}

#[derive(Default)]
struct MemDServer(Mutex<HashMap<FragmentId, Bytes>>);

#[async_trait]
impl ChunkStore for MemDServer {
    async fn put_fragment(&self, id: FragmentId, bytes: Bytes, _: Option<u64>) -> Result<()> {
        self.0.lock().unwrap().insert(id, bytes);
        Ok(())
    }
    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.0.lock().unwrap().get(&id).cloned())
    }
    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.0.lock().unwrap().keys().copied().collect())
    }
    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.0.lock().unwrap().remove(&id);
        Ok(())
    }
    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

// ---- the fixture ----

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// The chunk every leg drains: RS(2,1), index 0 on [`DRAINING`].
fn drained(id: ChunkId) -> ChunkRef {
    ChunkRef {
        id,
        scheme: SCHEME,
        len: CHUNK_LEN,
        placement: vec![DRAINING, SURVIVORS[0], SURVIVORS[1]],
    }
}

/// A chunk holding nothing on the draining server.
fn settled(id: ChunkId, len: u64) -> ChunkRef {
    ChunkRef {
        id,
        scheme: SCHEME,
        len,
        placement: vec![SURVIVORS[0], SURVIVORS[1], FREE],
    }
}

fn answered(answer: Answer) -> Reconciled {
    answer.unwrap_or_else(|err| panic!("the pass must COMPLETE and answer: {err}"))
}

/// Exactly one containment on the audit seam — one event and one counter tick — naming
/// `object` by its store key.
fn assert_contained_once(logged: &str, object: &str) {
    let events = logged
        .matches(r#""action":"unresolvable-chunk-map""#)
        .count();
    let ticks = r#""monotonic_counter.rebalance_unresolvable_records":1"#;
    let counted = logged.matches(ticks).count();
    assert_eq!((events, counted), (1, 1), "once per object: {logged}");
    let named = logged.contains(&format!(r#""inode":"{object}""#));
    assert!(named, "{object} not named: {logged}");
}

struct Fixture {
    meta: MemMeta,
    d: [(DServerId, MemDServer); 4],
    topology: Topology,
}

impl Fixture {
    /// Four failure domains, one server each; [`DRAINING`] marked draining.
    async fn new() -> Self {
        let meta = MemMeta::default();
        set_lifecycle(&meta, DRAINING, DServerLifecycle::Draining)
            .await
            .unwrap();
        let mut topology = Topology::default();
        topology
            .register(DRAINING, "A")
            .register(SURVIVORS[0], "B")
            .register(SURVIVORS[1], "C")
            .register(FREE, "D");
        let d = [DRAINING, SURVIVORS[0], SURVIVORS[1], FREE].map(|id| (id, MemDServer::default()));
        Self { meta, d, topology }
    }

    /// ONE pass through the real fenced control point, its audit stream captured as JSON into
    /// a per-pass temp file.
    async fn pass(&self) -> (Answer, String) {
        // `tracing` caches each callsite's interest process-globally on first hit, so a sibling
        // test hitting an audit callsite with no subscriber installed would leave the capture
        // below empty (wyrd #214). A permissive global default, installed once.
        static INIT: Once = Once::new();
        let permissive = || tracing::subscriber::set_global_default(tracing_subscriber::registry());
        INIT.call_once(|| permissive().unwrap());
        let fleet: Vec<(DServerId, &dyn ChunkStore)> = self
            .d
            .iter()
            .map(|(id, s)| (*id, s as &dyn ChunkStore))
            .collect();
        let ctx = RebalanceContext {
            meta: &self.meta,
            fleet: &fleet,
            topology: &self.topology,
        };
        let coord = MemCoordination::new();
        let leader = Custodian::elect(&coord, "zone-722").await.unwrap();
        let mut zone = FencedZone::new();
        zone.install(leader.leadership());
        let audit = tempfile::NamedTempFile::new().unwrap();
        let json = tracing_subscriber::fmt::layer().json();
        let layer = json.with_writer(Arc::new(audit.reopen().unwrap()));
        let logging = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
        let step = reconcile_step(&zone, &leader, None, None, None, Some(&ctx), 10_000);
        let answer = step.with_subscriber(logging).await;
        (answer, std::fs::read_to_string(audit.path()).unwrap())
    }

    /// Arm the move's own `seg:` read; [`Self::fired`] then proves the move reached it.
    fn arm(&self, at: AtMoveRead) {
        *self.meta.at_move_read.lock().unwrap() = Some(at);
    }

    fn fired(&self) -> bool {
        self.meta.at_move_read.lock().unwrap().is_none()
    }

    fn store(&self, dserver: DServerId) -> &MemDServer {
        &self.d.iter().find(|(id, _)| *id == dserver).unwrap().1
    }

    /// Seed a committed **segmented** object: raw `seg:` records plus a segmented root (this
    /// build ships no committer of segmented maps). Segment `i` holds `segments[i]`; returns
    /// the `seg:` keys in table order. Epoch = inode, so two objects never share a range.
    async fn seed_segmented(&self, inode: InodeId, segments: Vec<SegmentRecord>) -> Vec<Vec<u8>> {
        let (root, keys) = self.seed_group(inode, segments, 0).await;
        self.put(inode_key(inode), encode(&root)).await;
        keys
    }

    /// Seed one segment group's raw `seg:` records under `epoch`; returns the root that would
    /// name them (at `version`, NOT written) and their keys in table order.
    async fn seed_group(
        &self,
        epoch: u64,
        segments: Vec<SegmentRecord>,
        version: u64,
    ) -> (InodeRecord, Vec<Vec<u8>>) {
        let group = SegmentGroup::new(NONCE, epoch).unwrap();
        let mut table = Vec::new();
        let mut keys = Vec::new();
        for (index, record) in segments.iter().enumerate() {
            let index = index as u32;
            table.push(SegmentRef {
                index,
                byte_offset: record.byte_offset(),
                byte_len: record.byte_len(),
            });
            let key = seg_key(&group, index).unwrap();
            self.put(key.clone(), encode(record)).await;
            keys.push(key);
        }
        let size = segments.iter().map(SegmentRecord::byte_len).sum();
        let map = ChunkMap::Segmented(SegmentedMap::new(group, table).unwrap());
        (root(size, map, version), keys)
    }

    /// Seed a committed root record at `inode` (`version` as given).
    async fn seed_root(&self, inode: InodeId, size: u64, chunk_map: ChunkMap, version: u64) {
        let root = root(size, chunk_map, version);
        self.put(inode_key(inode), encode(&root)).await;
    }

    /// Store each fragment of `chunk` on the server its placement names, as the on-disk
    /// writer stamps it, so the intact-fragment check accepts it.
    async fn put_fragments(&self, chunk: &ChunkRef) {
        for (index, dserver) in chunk.placement.iter().enumerate() {
            let id = frag(chunk.id, index as u16);
            let bytes = fragment_bytes(id);
            self.store(*dserver)
                .put_fragment(id, bytes, None)
                .await
                .unwrap();
        }
    }

    async fn put(&self, key: Vec<u8>, value: impl Into<Bytes>) {
        let landed = self.meta.commit(WriteBatch::new().put(key, value)).await;
        assert_eq!(landed.unwrap(), CommitOutcome::Committed);
    }

    async fn get(&self, key: &[u8]) -> Bytes {
        let bytes = self.meta.get(key).await.unwrap();
        bytes.expect("fixture: record present")
    }

    async fn segment(&self, key: &[u8]) -> SegmentRecord {
        decode(&self.get(key).await).unwrap()
    }

    async fn holds(&self, dserver: DServerId, id: FragmentId) -> bool {
        self.store(dserver)
            .get_fragment(id)
            .await
            .unwrap()
            .is_some()
    }

    async fn orphaned(&self, dserver: DServerId, id: FragmentId) -> bool {
        let mark = self.meta.get(&orphan_key(dserver, id)).await.unwrap();
        mark.is_some()
    }

    async fn no_orphan_marks(&self) -> bool {
        self.meta.scan(b"orphan:").await.unwrap().is_empty()
    }

    async fn status(&self) -> ReconciliationStatus {
        reconciliation_status(&self.meta, DRAINING).await.unwrap()
    }
}

fn fragment_bytes(id: FragmentId) -> Bytes {
    let header = FragmentHeader {
        ec_scheme_type: EcSchemeType::ReedSolomon,
        ec_k: 2,
        ec_m: 1,
        ec_fragment_index: id.index,
        ..FragmentHeader::new_v1(id.chunk, CHUNK_LEN)
    };
    Bytes::from(encode_fragment(&header, b"drain"))
}

fn segment(chunks: Vec<ChunkRef>, byte_offset: u64) -> SegmentRecord {
    SegmentRecord::new(chunks, byte_offset).unwrap()
}

/// A committed root record.
fn root(size: u64, chunk_map: ChunkMap, version: u64) -> InodeRecord {
    InodeRecord {
        size,
        chunk_map,
        state: InodeState::Committed,
        version,
        ..InodeRecord::new_empty()
    }
}

// ---- leg 1 ----

/// **Leg 1 — BINDING.** A segmented object whose second segment holds a chunk with a fragment
/// on the draining server. The pass must copy that fragment to the one server whose domain is
/// distinct from the survivors', repoint the `seg:` record (and only it), orphan-mark the
/// vacated position, and answer `Changed`. On the base it is refused and nothing moves.
#[tokio::test]
async fn a_seg_resident_fragment_is_evacuated_off_a_draining_server() {
    let fx = Fixture::new().await;
    let lead = settled(SIBLING, CHUNK_LEN);
    let chunk = drained(CHUNK);
    let segments = vec![
        segment(vec![lead.clone()], 0),
        segment(vec![chunk.clone()], CHUNK_LEN),
    ];
    let keys = fx.seed_segmented(A, segments).await;
    fx.put_fragments(&lead).await;
    fx.put_fragments(&chunk).await;
    let root_before = fx.get(&inode_key(A)).await;
    let lead_before = fx.get(&keys[0]).await;

    let (answer, _) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Changed, "the move landed");
    let moved = fx.segment(&keys[1]).await;
    let placement = &moved.chunks()[0].placement;
    assert_eq!(
        placement,
        &vec![FREE, SURVIVORS[0], SURVIVORS[1]],
        "seg: repointed"
    );
    assert!(
        !placement.contains(&DRAINING),
        "still names the draining server"
    );
    assert!(
        fx.holds(FREE, frag(CHUNK, 0)).await,
        "fragment copied to its new home"
    );
    assert!(
        fx.orphaned(DRAINING, frag(CHUNK, 0)).await,
        "vacated position orphan-marked"
    );
    assert_eq!(
        fx.get(&inode_key(A)).await,
        root_before,
        "the root is never rewritten"
    );
    assert_eq!(
        fx.get(&keys[0]).await,
        lead_before,
        "an untouched segment stays untouched"
    );
    assert_eq!(
        fx.status().await,
        ReconciliationStatus::Satisfied,
        "drain converged"
    );
}

// ---- leg 2 ----

/// **Leg 2 — BINDING.** Two committed segmented objects whose maps both name ONE chunk, one of
/// whose fragments is on the draining server. Every committed reference must be repointed: an
/// object left naming the drained server keeps that server referenced, so the drain never
/// certifies. (GC never reclaims a fragment a committed map names, so the cost is the drain,
/// not the bytes.) On the base both are refused.
#[tokio::test]
async fn every_committed_reference_to_a_shared_chunk_is_repointed() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let a = fx
        .seed_segmented(A, vec![segment(vec![chunk.clone()], 0)])
        .await;
    let b = fx
        .seed_segmented(B, vec![segment(vec![chunk.clone()], 0)])
        .await;
    fx.put_fragments(&chunk).await;

    let (answer, _) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Changed, "both moves landed");
    for key in [&a[0], &b[0]] {
        let record = fx.segment(key).await;
        let named = &record.chunks()[0];
        assert!(
            !named.placement.contains(&DRAINING),
            "still names the drained server"
        );
        for (index, dserver) in named.placement.iter().enumerate() {
            let present = fx.holds(*dserver, frag(named.id, index as u16)).await;
            assert!(present, "fragment {index} absent from server {dserver}");
        }
    }
    assert_eq!(
        fx.status().await,
        ReconciliationStatus::Satisfied,
        "drain converged"
    );
}

// ---- leg 3 ----

/// A `seg:` record encoding to EXACTLY `target` bytes: `head` first, then one-byte filler
/// chunks, the first four of which have their id widened digit by digit to land on the byte.
fn padded_segment(head: ChunkRef, target: usize) -> SegmentRecord {
    let filler = |id: ChunkId| settled(id, 1);
    let build = |fillers: &[ChunkId]| {
        let mut chunks = vec![head.clone()];
        chunks.extend(fillers.iter().copied().map(filler));
        segment(chunks, 0)
    };
    let len = |fillers: &[ChunkId]| encode(&build(fillers)).len();
    let mut fillers: Vec<ChunkId> = vec![1; 4];
    while len(&[fillers.as_slice(), &[1]].concat()) <= target {
        fillers.push(1);
    }
    // Each widened digit adds one byte; four ids give up to 152, more than one filler adds.
    for slot in 0..4 {
        while len(&fillers) < target && fillers[slot] < 10u128.pow(38) {
            fillers[slot] *= 10;
        }
    }
    let record = build(&fillers);
    assert_eq!(
        encode(&record).len(),
        target,
        "fixture: exact record length"
    );
    record
}

/// **Leg 3 — the ceiling refusal holds over a `seg:` record on the drain arm.** The record
/// sits exactly ON the value ceiling ([`MAX_VALUE_BYTES`], admissible); moving the drained
/// fragment to [`FREE`] widens one placement entry from one digit to twenty, pushing it past.
/// Refused: the record is byte-identical, nothing is copied, and the drain does not certify.
///
/// Keyed to [`MAX_VALUE_BYTES`] because that is the ceiling the placement move weighs a `seg:`
/// record against (`flat_value_ceiling_crossed`); a crossing of half of it would commit.
/// Passes on the base too (refused for the segmented reason). Its negation is deleting the
/// ceiling check: the move then commits an oversized record.
#[tokio::test]
async fn a_seg_repoint_past_the_value_ceiling_is_refused_and_writes_nothing() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let record = padded_segment(chunk.clone(), MAX_VALUE_BYTES);
    let keys = fx.seed_segmented(A, vec![record]).await;
    fx.put_fragments(&chunk).await;
    let before = fx.meta.scan(b"").await.unwrap();

    let (answer, _) = fx.pass().await;

    assert_eq!(
        answered(answer),
        Reconciled::Blocked,
        "the drain is not certified"
    );
    assert_eq!(
        fx.meta.scan(b"").await.unwrap(),
        before,
        "a refusal wrote metadata"
    );
    assert!(
        fx.get(&keys[0]).await.len() <= MAX_VALUE_BYTES,
        "past the ceiling"
    );
    assert!(
        !fx.holds(FREE, frag(CHUNK, 0)).await,
        "a refused move copied a fragment"
    );
    assert!(
        fx.holds(DRAINING, frag(CHUNK, 0)).await,
        "fragment stays put"
    );
    assert_eq!(
        fx.status().await,
        ReconciliationStatus::Pending,
        "drain still owed"
    );
}

// ---- leg 4 ----

/// **Leg 4 — a lost CAS writes no metadata and retracts nothing.** A competing writer edits a
/// sibling chunk of the same `seg:` record after the move has read it and before its commit.
/// The record must hold exactly the competing writer's bytes, no orphan mark for this move may
/// exist, the drain is not certified — and the fragment already copied to [`FREE`] is LEFT IN
/// PLACE, not deleted: a losing write never retracts what it published.
///
/// On the base no move is planned, so the racer never fires and this leg is red for that
/// ordinary reason. Its conflict property is bound by the mutation oracle (delete the `seg:`
/// precondition and the move overwrites the competing bytes) and by the DST property.
#[tokio::test]
async fn a_lost_cas_writes_no_metadata_and_leaves_the_copy_in_place() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let sibling = settled(SIBLING, CHUNK_LEN);
    let seeded = segment(vec![chunk.clone(), sibling.clone()], 0);
    let keys = fx.seed_segmented(A, vec![seeded]).await;
    fx.put_fragments(&chunk).await;
    fx.put_fragments(&sibling).await;
    let mut edited = sibling.clone();
    edited.placement = vec![SURVIVORS[1], FREE, SURVIVORS[0]];
    let competing = encode(&segment(vec![chunk.clone(), edited], 0));
    *fx.meta.racer.lock().unwrap() = Some((keys[0].clone(), competing.clone()));

    let (answer, _) = fx.pass().await;

    assert!(
        fx.meta.racer.lock().unwrap().is_none(),
        "the racer never fired"
    );
    assert_eq!(
        answered(answer),
        Reconciled::Blocked,
        "a lost move does not certify"
    );
    assert_eq!(
        fx.get(&keys[0]).await,
        competing,
        "the competing write was overwritten"
    );
    let marks = fx.meta.scan(b"orphan:").await.unwrap();
    assert!(
        marks.is_empty(),
        "a lost move published orphan evidence: {marks:?}"
    );
    assert!(
        fx.holds(FREE, frag(CHUNK, 0)).await,
        "the copied fragment was retracted"
    );
    assert!(
        fx.holds(DRAINING, frag(CHUNK, 0)).await,
        "the source fragment moved"
    );
    assert_eq!(
        fx.status().await,
        ReconciliationStatus::Pending,
        "drain still owed"
    );
}

// ---- legs 5–9: the drain's answer to each verdict the move gives before it writes ----

/// **Leg 5 — a ceiling refusal on a SUPERSEDED root is a retry, not an operator signal.** Leg
/// 3's at-ceiling record, but a supersede moves the root to a fresh flat generation just before
/// the move re-reads the (not yet collected) `seg:` record. The refusal was weighed on a
/// generation the root has left, so the drain names a conflict — never `refused-ceiling`,
/// which tells an operator the object must shrink — and writes and copies nothing.
#[tokio::test]
async fn a_ceiling_refusal_on_a_superseded_root_is_a_conflict() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let record = padded_segment(chunk.clone(), MAX_VALUE_BYTES);
    let keys = fx.seed_segmented(A, vec![record]).await;
    fx.put_fragments(&chunk).await;
    let seg_before = fx.get(&keys[0]).await;
    let successor = InodeRecord {
        state: InodeState::Committed,
        version: 1,
        ..InodeRecord::new_empty()
    };
    fx.arm(AtMoveRead::Write(inode_key(A), encode(&successor)));

    let (answer, logged) = fx.pass().await;

    assert!(fx.fired(), "the move never re-read the seg: record");
    assert_eq!(answered(answer), Reconciled::Blocked, "no certification");
    let conflict = logged.contains(r#""action":"conflict""#);
    let refused = logged.contains(r#""action":"refused-ceiling""#);
    assert!(conflict && !refused, "a stale plan escalated: {logged}");
    assert_eq!(fx.get(&keys[0]).await, seg_before, "a refusal wrote");
    assert!(!fx.holds(FREE, frag(CHUNK, 0)).await, "a refusal copied");
}

/// **Leg 6 — a flat record whose version cannot advance is contained, named ONCE.** Two chunks
/// owe a move in one flat object at `version == u64::MAX`; the move refuses to wrap the version
/// for each. The object is named once — one line per chunk floods the seam — and nothing is
/// written or copied. (The base wraps-or-panics on `version + 1`.)
#[tokio::test]
async fn a_version_that_cannot_advance_contains_the_object_once() {
    let fx = Fixture::new().await;
    let (one, two) = (drained(CHUNK), drained(SIBLING));
    let map = ChunkMap::Flat(vec![one.clone(), two.clone()]);
    fx.seed_root(A, 2 * CHUNK_LEN, map, u64::MAX).await;
    fx.put_fragments(&one).await;
    fx.put_fragments(&two).await;
    let before = fx.meta.scan(b"").await.unwrap();

    let (answer, logged) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Blocked, "no certification");
    assert_contained_once(&logged, "inode:3");
    let after = fx.meta.scan(b"").await.unwrap();
    assert_eq!(after, before, "a contained move wrote metadata");
    for id in [CHUNK, SIBLING] {
        assert!(
            !fx.holds(FREE, frag(id, 0)).await,
            "a contained move copied"
        );
    }
}

/// **Leg 7 — a `seg:` record torn under the move contains the object, named ONCE.** Two chunks
/// owe a move in one `seg:` record, which is torn just before the move re-reads it while the
/// root still names it. Both moves find it unusable; the object is named once, and nothing is
/// marked or copied.
#[tokio::test]
async fn a_seg_record_torn_under_the_move_contains_the_object_once() {
    let fx = Fixture::new().await;
    let (one, two) = (drained(CHUNK), drained(SIBLING));
    let keys = fx
        .seed_segmented(A, vec![segment(vec![one.clone(), two.clone()], 0)])
        .await;
    fx.put_fragments(&one).await;
    fx.put_fragments(&two).await;
    let root_before = fx.get(&inode_key(A)).await;
    fx.arm(AtMoveRead::Write(
        keys[0].clone(),
        Bytes::from_static(b"torn"),
    ));

    let (answer, logged) = fx.pass().await;

    assert!(fx.fired(), "the move never re-read the seg: record");
    assert_eq!(answered(answer), Reconciled::Blocked, "no certification");
    assert_contained_once(&logged, "inode:3");
    assert_eq!(fx.get(&inode_key(A)).await, root_before, "root rewritten");
    assert!(fx.no_orphan_marks().await, "a contained move marked");
    for id in [CHUNK, SIBLING] {
        assert!(
            !fx.holds(FREE, frag(id, 0)).await,
            "a contained move copied"
        );
    }
}

/// **Leg 8 — a store fault under the move's own read ends the pass.** Not this object's
/// fault, so it is not contained as one: the pass answers `Err`, carrying the fault, with
/// nothing copied and nothing marked.
#[tokio::test]
async fn a_store_fault_under_the_moves_read_ends_the_pass() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    fx.seed_segmented(A, vec![segment(vec![chunk.clone()], 0)])
        .await;
    fx.put_fragments(&chunk).await;
    fx.arm(AtMoveRead::Fail);

    let (answer, _) = fx.pass().await;

    let err = answer.expect_err("a store fault under the move must end the pass");
    assert!(
        err.to_string().contains(STORE_FAULT),
        "not the fault: {err}"
    );
    assert!(
        !fx.holds(FREE, frag(CHUNK, 0)).await,
        "copied before failing"
    );
    assert!(fx.no_orphan_marks().await, "marked before failing");
}

/// **Leg 9 — a chunk edited before the move reads it is a conflict that copied nothing.** A
/// competing writer re-places the planned chunk's last fragment (bytes first) just before the
/// move re-reads the `seg:` record. The move no longer finds the chunk it planned, so it
/// prepares nothing: the competing bytes stand, nothing is marked, and — unlike leg 4's
/// commit-time loss — no fragment was copied to [`FREE`].
#[tokio::test]
async fn a_chunk_edited_before_the_move_reads_it_is_a_conflict_with_nothing_copied() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let sibling = settled(SIBLING, CHUNK_LEN);
    let seeded = segment(vec![chunk.clone(), sibling.clone()], 0);
    let keys = fx.seed_segmented(A, vec![seeded]).await;
    fx.put_fragments(&chunk).await;
    fx.put_fragments(&sibling).await;
    let mut edited = chunk.clone();
    edited.placement[2] = FREE;
    let last = frag(CHUNK, 2);
    let store = fx.store(FREE);
    store
        .put_fragment(last, fragment_bytes(last), None)
        .await
        .unwrap();
    let competing = encode(&segment(vec![edited, sibling], 0));
    fx.arm(AtMoveRead::Write(keys[0].clone(), competing.clone()));

    let (answer, logged) = fx.pass().await;

    assert!(fx.fired(), "the move never re-read the seg: record");
    assert_eq!(answered(answer), Reconciled::Blocked, "no certification");
    assert!(logged.contains(r#""action":"conflict""#), "{logged}");
    assert_eq!(fx.get(&keys[0]).await, competing, "competing write lost");
    assert!(fx.no_orphan_marks().await, "a stale plan marked");
    assert!(!fx.holds(FREE, frag(CHUNK, 0)).await, "a stale plan copied");
}

// ---- legs 10–11: objects the move cannot address are contained at the scan ----

/// **Leg 10 — chunk lengths past `u64` contain the object, and the walk goes on.** A flat
/// object (flat lengths are not bounded at decode) whose drained chunk starts past
/// `u64::MAX`: no offset can address it, so the object is named once — never a conflict every
/// pass — and nothing of it moves, while a healthy object later in key order still drains.
#[tokio::test]
async fn chunk_lengths_past_u64_contain_the_object_and_the_rest_still_drains() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let lengths = vec![settled(SIBLING, u64::MAX), settled(1, 1), chunk.clone()];
    fx.seed_root(A, u64::MAX, ChunkMap::Flat(lengths), 0).await;
    fx.put_fragments(&chunk).await;
    let healthy = drained(OTHER);
    let keys = fx
        .seed_segmented(B, vec![segment(vec![healthy.clone()], 0)])
        .await;
    fx.put_fragments(&healthy).await;
    let root_before = fx.get(&inode_key(A)).await;

    let (answer, logged) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Blocked, "no certification");
    assert_contained_once(&logged, "inode:3");
    assert_eq!(fx.get(&inode_key(A)).await, root_before, "root rewritten");
    assert!(
        !fx.holds(FREE, frag(CHUNK, 0)).await,
        "an unaddressable copy"
    );
    let moved = fx.segment(&keys[0]).await;
    let placement = &moved.chunks()[0].placement;
    assert!(!placement.contains(&DRAINING), "the walk stopped");
    assert!(
        fx.holds(FREE, frag(OTHER, 0)).await,
        "healthy bytes not moved"
    );
}

/// **Leg 11 — a segmented root under a non-canonical key is contained, nothing copied.** The
/// root sits at `inode:03`; the move would pin `inode:3`, which is not the row the scan read,
/// so it could only copy the fragment and lose its CAS every pass. Contained and named by the
/// row's own key instead, with nothing written, copied or marked.
#[tokio::test]
async fn a_segmented_root_under_a_non_canonical_key_is_contained() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let keys = fx
        .seed_segmented(A, vec![segment(vec![chunk.clone()], 0)])
        .await;
    fx.put_fragments(&chunk).await;
    let root = fx.meta.kv.lock().unwrap().remove(&inode_key(A)).unwrap();
    fx.put(b"inode:03".to_vec(), root).await;
    let seg_before = fx.get(&keys[0]).await;

    let (answer, logged) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Blocked, "no certification");
    assert_contained_once(&logged, "inode:03");
    assert_eq!(fx.get(&keys[0]).await, seg_before, "seg: rewritten");
    assert!(fx.no_orphan_marks().await, "a contained object marked");
    assert!(!fx.holds(FREE, frag(CHUNK, 0)).await, "a doomed copy");
}

// ---- leg 12: the move pins the generation the resolve answered from ----

/// **Leg 12 — a root superseded mid-resolve is moved on the SUCCESSOR, in one pass.** The scan
/// reads root R1; right after the resolve reads R1's `seg:` page, a supersede installs R2 (a
/// new group, its `seg:` record already written, naming the same drained chunk), so the
/// resolve's root re-read restarts onto R2. The move must walk and pin R2 — the generation the
/// resolve answered from — and land: R2's `seg:` record repointed, R2's root untouched, the
/// retired R1 record left alone. Pinning the scan's own snapshot (R1) instead would copy the
/// fragment, lose the root CAS, and leave the drain owed.
#[tokio::test]
async fn a_root_superseded_mid_resolve_is_moved_on_the_successor() {
    let fx = Fixture::new().await;
    let chunk = drained(CHUNK);
    let retired = fx
        .seed_segmented(A, vec![segment(vec![chunk.clone()], 0)])
        .await;
    let (successor, keys) = fx
        .seed_group(33, vec![segment(vec![chunk.clone()], 0)], 1)
        .await;
    fx.put_fragments(&chunk).await;
    let retired_before = fx.get(&retired[0]).await;
    let successor = encode(&successor);
    *fx.meta.after_seg_page.lock().unwrap() = Some((inode_key(A), successor.clone()));

    let (answer, logged) = fx.pass().await;

    let fired = fx.meta.after_seg_page.lock().unwrap().is_none();
    assert!(fired, "the supersede never landed mid-resolve");
    assert_eq!(answered(answer), Reconciled::Changed, "{logged}");
    let moved = fx.segment(&keys[0]).await;
    let placement = &moved.chunks()[0].placement;
    assert_eq!(placement, &vec![FREE, SURVIVORS[0], SURVIVORS[1]]);
    assert_eq!(fx.get(&inode_key(A)).await, successor, "root rewritten");
    assert_eq!(fx.get(&retired[0]).await, retired_before, "R1 rewritten");
    assert!(fx.holds(FREE, frag(CHUNK, 0)).await, "fragment not copied");
    assert!(fx.orphaned(DRAINING, frag(CHUNK, 0)).await, "not marked");
    assert_eq!(fx.status().await, ReconciliationStatus::Satisfied);
}
