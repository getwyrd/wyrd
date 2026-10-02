//! Issue #777 (0016 decision 7(f)): **a chunk whose `ChunkRef` lives in a `seg:` record is
//! REPAIRED — the reconstruction pass completes the move through the placement primitive
//! instead of refusing it.** On the base #697's containment answered such an obligation with a
//! refusal every pass, forever: never drainable (that would be data loss) and never moved, so a
//! multipart object's redundancy decayed with no actor able to exit the state — what C-1 forbids
//! (`docs/principles.md` §5).
//!
//! Every leg drives the real fenced control point [`reconcile_step`] and observes the **store**
//! — and, where the property is a count of operator signals, the audit seam. **No assertion
//! names a symbol this patch introduces**, and none calls the primitive directly, so the file
//! compiles unchanged with the production change reverted.
//!
//! * Legs 1–2 are RED on the base (it refuses, writes nothing, and answers `Blocked`).
//! * Legs 3–4 are red on the base only because it never attempts the move (leg 3 on its
//!   verdict, leg 4 on "the race never landed"); their pins are bound by each named negation.
//! * Leg 5 passes on the base by construction (a refusal writes nothing either way).
//! * Legs 6–10: an object the move cannot rewrite is contained, once per OBJECT.
//! * Leg 11: a STORE fault under the move ends the pass, never read as the object's damage.
//! * Leg 12: a segmented object under a non-canonical key (`inode:01`) is contained (#698).
//! * Leg 13: a repair planned over a RESTARTED resolve pins the root the resolve answered from.
//!
//! **Reaching the race window:** the resolver reads a group's `seg:` range with `scan_page`,
//! never `get`; the move's own read is the only `get` on a `seg:` key. So [`MemMeta`] lands a
//! racing batch **after the resolver's page**, and leg 11 faults that `get`. A root flip lands
//! **on the way into `commit`**. These are scripted interleavings; the seeded Tier-0 DST
//! property for the move (repoint versus supersede) is deferred: #682.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, ChunkMap, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, SegmentGroup,
    SegmentRecord, SegmentRef, SegmentedMap, MAX_VALUE_BYTES,
};
use wyrd_core::placement::Topology;
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, repair};
use wyrd_custodian::{
    reconcile_step, Custodian, FencedZone, ReconcileError, Reconciled, ReconstructionContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    WriteBatch,
};

// ---- in-memory trait doubles: the pass is proven over the seams, backend-agnostic ----

/// When the racing writer's batch lands, relative to the pass's own reads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Race {
    /// After the resolver's bounded `seg:` page is materialised: the plan is built from the
    /// pre-race bytes, and the move's own `get` sees the race.
    AfterSegmentPage,
    /// On the way into a commit, after the resolve (a root moving *during* it restarts it).
    IntoCommit,
}

/// What the racing writer lands: raw puts with no preconditions — it got there first.
type Racing = Vec<(Vec<u8>, Bytes)>;

/// A `BTreeMap`-backed metadata store that can also play one racing writer.
#[derive(Default)]
struct MemMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    /// The armed race; taken when it fires, so it races once.
    racing: Mutex<Option<(Race, Racing)>>,
    /// Fault the next `get` of a `seg:` key (the move's own read) ONCE, with a plain backend
    /// error, never a `ChunkMapError`.
    faulting: AtomicBool,
}

impl MemMeta {
    fn arm(&self, when: Race, puts: Racing) {
        *self.racing.lock().unwrap() = Some((when, puts));
    }

    /// Whether the armed batch landed — so a leg cannot pass because its race never happened.
    fn raced(&self) -> bool {
        self.racing.lock().unwrap().is_none()
    }

    fn fire(&self, when: Race) {
        let mut racing = self.racing.lock().unwrap();
        if !matches!(&*racing, Some((at, _)) if *at == when) {
            return;
        }
        let (_, puts) = racing.take().expect("armed");
        let mut kv = self.kv.lock().unwrap();
        for (key, value) in puts {
            kv.insert(key, value);
        }
    }
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if key.starts_with(b"seg:") && self.faulting.swap(false, Ordering::SeqCst) {
            return Err(Box::new(std::io::Error::other(STORE_FAULT)));
        }
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let kv = self.kv.lock().unwrap();
        let rows = kv.iter().filter(|(key, _)| key.starts_with(prefix));
        Ok(rows.map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    // The required paginated read (#634): the dev-only testkit helper pages over `scan`.
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        let page = wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await;
        if prefix.starts_with(b"seg:") {
            self.fire(Race::AfterSegmentPage);
        }
        page
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        self.fire(Race::IntoCommit);
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (key, value) in batch.puts {
            kv.insert(key, value);
        }
        for key in batch.deletes {
            kv.remove(&key);
        }
        Ok(CommitOutcome::Committed)
    }
}

/// One D server's fragments, holding the **real** stored bytes so checksums verify.
#[derive(Default)]
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for MemDServer {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.frags.lock().unwrap().insert(id, fragment);
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

// ---- audit capture (the in-tree pattern, `segmented_map_reconstruction.rs`) ----

/// A `MakeWriter` collecting the rows the pass emits on the durability seam.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
    type Writer = Self;
    fn make_writer(&'w self) -> Self::Writer {
        self.clone()
    }
}

/// Install a permissive global `tracing` default **once**, so the audit callsites never latch
/// `Interest::never` under the parallel test harness (#214).
fn enable_audit_callsites() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

/// `(rows naming an unreadable object, ticks of its counter, whether `object` is the one
/// named)` — the once-per-OBJECT accounting legs 6–10 are stated over.
fn contained(logged: &str, object: &str) -> (usize, usize, bool) {
    (
        logged
            .matches(r#""action":"unresolvable-chunk-map""#)
            .count(),
        logged
            .matches(r#""monotonic_counter.reconstruction_unresolvable_records":1"#)
            .count(),
        logged.contains(&format!(r#""inode":"{object}""#)),
    )
}

/// How many times the pass ticked `reconstruction_<name>` on the durability seam.
fn ticks(logged: &str, name: &str) -> usize {
    let tick = format!(r#""monotonic_counter.reconstruction_{name}":1"#);
    logged.matches(&tick).count()
}

// ---- fixture ----

const NOW: u64 = 10_000;
const CHUNK_LEN: u64 = 8;
/// RS(1,1): the one surviving fragment is enough to rebuild the other.
const K: u8 = 1;
const M: u8 = 1;
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const EPOCH: u64 = 7;
const INODE: InodeId = 1;
const STORE_FAULT: &str = "simulated store fault: segment record unreachable";

/// The chunk every leg is owed a repair on — in a segmented object, in the **second** `seg:`
/// record at a non-zero object offset, so the address crosses a boundary the resolved chunk list
/// hides. [`SIBLING`] shares its record; [`DECOY`] sits alone in the first record, owed nothing.
const CHUNK: ChunkId = 0xA2_00;
const SIBLING: ChunkId = 0xA3_00;
const DECOY: ChunkId = 0xA1_00;
/// Owed, but referenced by no record at all: only a COMPLETE reading may drain it.
const DELETED: ChunkId = 0x0E_00;

const SURVIVOR: DServerId = 0;
/// Where the lost fragment was placed — in no fleet and no topology: it is the loss.
const LOST: DServerId = 1;
const FREE: DServerId = 2;
const RACER: DServerId = 7;
/// The widest [`DServerId`]: repointing a one-digit placement entry onto it grows the record by
/// the difference in their encodings, which leg 5 measures rather than assumes.
const HUGE: DServerId = u64::MAX;

fn group() -> SegmentGroup {
    SegmentGroup::new(NONCE, EPOCH).unwrap()
}

fn seg_key(index: u32) -> Vec<u8> {
    metadata::seg_key(&group(), index).unwrap()
}

fn chunk_ref(id: ChunkId, placement: Vec<DServerId>) -> ChunkRef {
    ChunkRef {
        id,
        scheme: EcScheme::ReedSolomon { k: K, m: M },
        len: CHUNK_LEN,
        placement,
    }
}

/// Segment 1 as seeded: the chunk under repair and its sibling, both with a fragment on [`LOST`].
fn segment_one(chunk: Vec<DServerId>, sibling: Vec<DServerId>) -> SegmentRecord {
    let chunks = vec![chunk_ref(CHUNK, chunk), chunk_ref(SIBLING, sibling)];
    SegmentRecord::new(chunks, CHUNK_LEN).unwrap()
}

fn records() -> Vec<SegmentRecord> {
    vec![
        SegmentRecord::new(vec![chunk_ref(DECOY, vec![SURVIVOR, LOST])], 0).unwrap(),
        segment_one(vec![SURVIVOR, LOST], vec![SURVIVOR, LOST]),
    ]
}

async fn put(meta: &MemMeta, key: Vec<u8>, value: Bytes) {
    let outcome = meta.commit(WriteBatch::new().put(key, value)).await;
    assert_eq!(
        outcome.unwrap(),
        CommitOutcome::Committed,
        "fixture: seeding"
    );
}

/// One committed **segmented** generation of `records` as the raw rows its writer leaves: each
/// `seg:` record under `group`, then — LAST — the root at `key` naming that group at `version`.
/// Hand-written as `segmented_map_restore.rs`'s `seed_segmented` does: no producer of segmented
/// maps ships.
fn generation(group: &SegmentGroup, version: u64, key: &[u8], records: &[SegmentRecord]) -> Racing {
    let mut rows = Racing::new();
    let mut segments = Vec::new();
    for (index, record) in records.iter().enumerate() {
        let index = index as u32;
        segments.push(SegmentRef {
            index,
            byte_offset: record.byte_offset(),
            byte_len: record.byte_len(),
        });
        let at = metadata::seg_key(group, index).unwrap();
        rows.push((at, metadata::encode(record)));
    }
    let map = SegmentedMap::new(group.clone(), segments).unwrap();
    let root = InodeRecord {
        size: map.span(),
        chunk_map: ChunkMap::Segmented(map),
        state: InodeState::Committed,
        version,
        ..Default::default()
    };
    rows.push((key.to_vec(), metadata::encode(&root)));
    rows
}

/// Seed a committed segmented object at `key`: [`generation`] 1 under [`group`].
async fn seed(meta: &MemMeta, key: &[u8], records: &[SegmentRecord]) {
    let rows = generation(&group(), 1, key, records);
    let root: InodeRecord = metadata::decode(&rows.last().expect("the root row").1).unwrap();
    for (row, value) in rows {
        put(meta, row, value).await;
    }
    let resolved = metadata::resolve_chunk_map(meta, key, &root).await;
    assert!(
        matches!(resolved, Ok(Some(_))),
        "fixture: the seeded segmented object must resolve"
    );
}

/// Store `chunk`'s surviving fragment 0 on `d0` (real encoded shards, so the verify passes) and
/// owe it a repair. Fragment 1, on [`LOST`], is the loss.
async fn owe(meta: &MemMeta, d0: &MemDServer, chunk: ChunkId) {
    let data = vec![b'w'; CHUNK_LEN as usize];
    let shards = erasure::encode(K.into(), M.into(), &data).expect("shards encode");
    let frag = FragmentId { chunk, index: 0 };
    let bytes = encode_ec_fragment(chunk, 0, K, M, &shards[0]);
    d0.put_fragment(frag, bytes, None).await.unwrap();
    repair::enqueue_repair(meta, chunk, "scrub").await.unwrap();
}

async fn queued(meta: &MemMeta) -> Vec<ChunkId> {
    let mut chunks = repair::queued_repairs(meta).await.unwrap();
    chunks.sort_unstable();
    chunks
}

async fn segment(meta: &MemMeta, index: u32) -> Bytes {
    meta.get(&seg_key(index))
        .await
        .unwrap()
        .expect("fixture: seeded")
}

async fn placements(meta: &MemMeta, index: u32) -> Vec<Vec<DServerId>> {
    placements_in(&segment(meta, index).await)
}

fn placements_in(record: &Bytes) -> Vec<Vec<DServerId>> {
    let record: SegmentRecord = metadata::decode(record).unwrap();
    record
        .chunks()
        .iter()
        .map(|c| c.placement.clone())
        .collect()
}

async fn root(meta: &MemMeta) -> Bytes {
    let key = metadata::inode_key(INODE);
    meta.get(&key).await.unwrap().expect("fixture: seeded")
}

/// Every orphan mark in the store — the evidence a move publishes per displaced position.
async fn orphans(meta: &MemMeta) -> Vec<Vec<u8>> {
    let rows = meta.scan(b"orphan:").await.unwrap();
    rows.into_iter().map(|(key, _)| key).collect()
}

/// The survivor in domain "a", and ONE free domain "c" held by `free`.
fn zone(free: DServerId) -> Topology {
    let mut topology = Topology::default();
    topology.register(SURVIVOR, "a").register(free, "c");
    topology
}

/// One pass through the real fenced control point, which must not error, and what it emitted.
async fn run(
    meta: &MemMeta,
    d0: &MemDServer,
    free: (DServerId, &MemDServer),
) -> (Reconciled, String) {
    let (outcome, logged) = attempt(meta, d0, free).await;
    let outcome = outcome.expect("one multipart object must not stop repair for the whole store");
    (outcome, logged)
}

/// [`run`], answering whatever the pass answered — an error included.
async fn attempt(
    meta: &MemMeta,
    d0: &MemDServer,
    free: (DServerId, &MemDServer),
) -> (std::result::Result<Reconciled, ReconcileError>, String) {
    enable_audit_callsites();
    let coord = MemCoordination::new();
    let leader = Custodian::elect(&coord, "zone-repoint").await.unwrap();
    let mut fenced = FencedZone::new();
    fenced.install(leader.leadership());
    let topology = zone(free.0);
    let fleet: [(DServerId, &dyn ChunkStore); 2] = [(SURVIVOR, d0), (free.0, free.1)];
    let ctx = ReconstructionContext {
        meta,
        fleet: &fleet,
        topology: &topology,
        unreachable: &[],
        clock: &wyrd_testkit::ManualClock::new(NOW),
        staged_write_window_millis: 0,
    };
    let capture = Capture::default();
    let layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(capture.clone());
    let outcome = reconcile_step(&fenced, &leader, None, None, Some(&ctx), None, NOW)
        .with_subscriber(tracing::Dispatch::new(
            tracing_subscriber::registry().with(layer),
        ))
        .await;
    let logged = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    (outcome, logged)
}

/// The shared fixture: the records above, owed a repair on [`CHUNK`] alone.
async fn fixture() -> (MemMeta, MemDServer, MemDServer) {
    let (meta, d0, free) = Default::default();
    seed(&meta, &metadata::inode_key(INODE), &records()).await;
    owe(&meta, &d0, CHUNK).await;
    (meta, d0, free)
}

/// Segment 1 as a racing writer would leave it — a real record, not a marker.
fn rewritten(record: &SegmentRecord) -> (Vec<u8>, Bytes) {
    (seg_key(1), metadata::encode(record))
}

/// What a repair that LOST must leave behind: segment 1 holding exactly `record`, the obligation
/// still queued, no orphan mark (only REPAIR-owned metadata: leg 4 writes a competing root) —
/// and `Satisfied`, as the flat arm answers: a lost CAS is a retry, not a hole (2026-08-19).
/// The loss is counted as ONE conflict and no abort: an abort says the repair could not
/// proceed, a different operator signal. Named negation: answer the lost race as an abort.
async fn assert_lost(meta: &MemMeta, (outcome, logged): (Reconciled, String), record: &Bytes) {
    assert!(meta.raced(), "fixture: the race never landed");
    assert_eq!(
        &segment(meta, 1).await,
        record,
        "the segment record holds EXACTLY the bytes it held before the move: a stale plan \
         never lands over a newer placement"
    );
    assert_eq!(
        queued(meta).await,
        vec![CHUNK],
        "the obligation is kept, never discarded"
    );
    assert!(
        orphans(meta).await.is_empty(),
        "no orphan mark is published for a move that did not happen"
    );
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "a lost CAS is a retry, not a hole: the next pass re-plans onto the winner's bytes"
    );
    assert_eq!(
        (ticks(&logged, "conflict"), ticks(&logged, "aborted")),
        (1, 0),
        "a lost race is ONE conflict, never relabelled an abort: {logged}"
    );
}

// ---- leg 1: a `seg:`-resident under-replicated chunk is repaired (RED on the base) ----

/// The rebuilt fragment lands on a healthy D server in a distinct domain, the **`seg:` record's
/// own** `ChunkRef` names it, and the obligation drains in the mutation that orphans exactly the
/// displaced position; the root is byte-identical.
#[tokio::test]
async fn a_segmented_objects_under_replicated_chunk_is_repaired_in_its_own_record() {
    let (meta, d0, free) = fixture().await;
    let root_before = root(&meta).await;
    let decoy_before = segment(&meta, 0).await;

    let (outcome, logged) = run(&meta, &d0, (FREE, &free)).await;

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "a repair that moved a placement: {logged}"
    );
    assert_eq!(
        placements(&meta, 1).await,
        vec![vec![SURVIVOR, FREE], vec![SURVIVOR, LOST]],
        "the `seg:` record names the rebuilt fragment's server, and only the owed chunk moved"
    );
    assert_ne!(
        zone(FREE).domain_of(SURVIVOR),
        zone(FREE).domain_of(FREE),
        "fixture: the rebuild's target is in a domain distinct from the survivor's"
    );
    let rebuilt = FragmentId {
        chunk: CHUNK,
        index: 1,
    };
    assert!(
        free.get_fragment(rebuilt).await.unwrap().is_some(),
        "the rebuilt fragment is on the D server the record now names"
    );
    assert!(
        queued(&meta).await.is_empty(),
        "the obligation is drained — what exits the refused-forever state"
    );
    assert_eq!(
        orphans(&meta).await,
        vec![metadata::orphan_key(LOST, rebuilt)],
        "the displaced position — fragment 1 on the lost server — and nothing else is orphaned \
         in the same batch: never the survivor, never the rebuilt destination"
    );
    assert_eq!(
        root(&meta).await,
        root_before,
        "the root is never rewritten"
    );
    assert_eq!(
        segment(&meta, 0).await,
        decoy_before,
        "the move addresses ONE record"
    );
}

// ---- leg 2: a racing edit of a SIBLING in the same record is merged (RED on the base) ----

/// A competing writer moves a **different** chunk of the same `seg:` record between the
/// resolver's page and the move's own read. Both survive: the move pins the record as IT
/// re-reads it, never the bytes the resolve saw.
#[tokio::test]
async fn a_racing_move_of_a_sibling_chunk_in_the_same_segment_record_is_merged() {
    let (meta, d0, free) = fixture().await;
    let racing = segment_one(vec![SURVIVOR, LOST], vec![SURVIVOR, RACER]);
    meta.arm(Race::AfterSegmentPage, vec![rewritten(&racing)]);

    let (outcome, _) = run(&meta, &d0, (FREE, &free)).await;

    assert!(meta.raced(), "fixture: the race never landed");
    assert_eq!(outcome, Reconciled::Changed, "the repair still lands");
    assert_eq!(
        placements(&meta, 1).await,
        vec![vec![SURVIVOR, FREE], vec![SURVIVOR, RACER]],
        "BOTH survive: the repair's own chunk moved and the sibling's racing move was merged"
    );
    assert!(queued(&meta).await.is_empty(), "the obligation is drained");
}

// ---- leg 3: a racing edit of the PLANNED chunk is a conflict (not independently red) ----

/// The competing writer moves **the planned chunk itself**. Repointing anyway would revert a
/// newer placement, so the move writes no metadata. Named negation: delete the `ChunkRef`
/// equality pin (`chunk_at`'s `chunk == prior`), and the repair lands over the racer's
/// placement. The destination fragment is not asserted on (#723).
#[tokio::test]
async fn a_racing_move_of_the_planned_chunk_itself_is_a_conflict() {
    let (meta, d0, free) = fixture().await;
    let root_before = root(&meta).await;
    let racing = segment_one(vec![SURVIVOR, RACER], vec![SURVIVOR, LOST]);
    let (key, competing) = rewritten(&racing);
    meta.arm(Race::AfterSegmentPage, vec![(key, competing.clone())]);

    let answered = run(&meta, &d0, (FREE, &free)).await;

    assert_lost(&meta, answered, &competing).await;
    assert_eq!(root(&meta).await, root_before, "the root is untouched");
}

// ---- leg 4: a superseded root generation is a conflict (not independently red) ----

/// The root flips to a new generation after the resolve and before the commit. A supersede
/// moves the root first (`0016:2452-2462`), so the move must lose rather than repoint a
/// generation nothing references. Named negation: drop the root-generation pin.
#[tokio::test]
async fn a_superseded_root_generation_makes_the_repair_lose() {
    let (meta, d0, free) = fixture().await;
    let segment_before = segment(&meta, 1).await;
    let seeded: InodeRecord = metadata::decode(&root(&meta).await).unwrap();
    let superseding = metadata::encode(&InodeRecord {
        version: seeded.version + 1,
        ..seeded
    });
    let root_key = metadata::inode_key(INODE);
    meta.arm(Race::IntoCommit, vec![(root_key, superseding.clone())]);

    let answered = run(&meta, &d0, (FREE, &free)).await;

    assert_lost(&meta, answered, &segment_before).await;
    assert_eq!(
        root(&meta).await,
        superseding,
        "the only root the store holds is the competing one this leg wrote"
    );
}

// ---- leg 5: the value ceiling holds over a `seg:` record (passes on the base) ----

/// A filler chunk no leg owes a repair on, widened by `extra` bytes: each placement entry
/// encodes as two bytes (`,0`), and a two-digit id pays an odd byte.
fn filler(extra: usize) -> ChunkRef {
    let (id, entries) = match extra {
        0 => (7, 0),
        odd if odd % 2 == 1 => (7, odd / 2 + 1),
        even => (17, even / 2),
    };
    ChunkRef {
        id,
        scheme: EcScheme::None,
        len: 1,
        placement: vec![0; entries],
    }
}

/// Segment 1 holding [`CHUNK`] on `placement`, padded with fillers until it encodes to EXACTLY
/// `target` bytes — measured, so a codec change moves the padding, not the property.
fn padded(placement: Vec<DServerId>, target: usize) -> SegmentRecord {
    let build = |chunks: &[ChunkRef]| SegmentRecord::new(chunks.to_vec(), CHUNK_LEN).unwrap();
    let width = |chunks: &[ChunkRef]| metadata::encode(&build(chunks)).len();
    let mut chunks = vec![chunk_ref(CHUNK, placement), filler(0)];
    let per = width(&[chunks.clone(), vec![filler(0)]].concat()) - width(&chunks);
    // Bulk first, then one at a time: a record near the ceiling holds thousands of fillers.
    let bulk = (target - width(&chunks)) / per;
    chunks.extend(vec![filler(0); bulk.saturating_sub(1)]);
    while width(&[chunks.clone(), vec![filler(0)]].concat()) <= target {
        chunks.push(filler(0));
    }
    let last = chunks.len() - 1;
    chunks[last] = filler(target - width(&chunks));
    let record = build(&chunks);
    let bytes = metadata::encode(&record).len();
    assert_eq!(bytes, target, "fixture: the padding missed its target");
    record
}

/// A `seg:` record under the FULL [`MAX_VALUE_BYTES`] whose repoint would re-encode to ONE byte
/// past it: refused, byte-identical, queued, and `Blocked` (a ceiling refusal IS a hole).
/// Named negation: drop the weigh in the segmented arm, and the record commits past the ceiling.
#[tokio::test]
async fn a_repoint_one_byte_past_the_value_ceiling_is_refused() {
    let growth = {
        let wide = metadata::encode(&segment_one(vec![SURVIVOR, HUGE], vec![SURVIVOR, LOST]));
        let seeded = metadata::encode(&segment_one(vec![SURVIVOR, LOST], vec![SURVIVOR, LOST]));
        wide.len() - seeded.len()
    };
    let (meta, d0, free) = <(MemMeta, MemDServer, MemDServer)>::default();
    let seeded = padded(vec![SURVIVOR, LOST], MAX_VALUE_BYTES + 1 - growth);
    let decoy = SegmentRecord::new(vec![chunk_ref(DECOY, vec![SURVIVOR, LOST])], 0).unwrap();
    seed(&meta, &metadata::inode_key(INODE), &[decoy, seeded]).await;
    owe(&meta, &d0, CHUNK).await;
    let before = segment(&meta, 1).await;
    assert!(
        before.len() < MAX_VALUE_BYTES,
        "fixture: seeded under the ceiling"
    );

    let (outcome, _) = run(&meta, &d0, (HUGE, &free)).await;

    assert_eq!(
        segment(&meta, 1).await,
        before,
        "a refusal writes NOTHING: the segment record is byte-identical"
    );
    assert_eq!(
        queued(&meta).await,
        vec![CHUNK],
        "the obligation stays queued"
    );
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a ceiling refusal is a hole in what the pass may certify"
    );
}

// ---- legs 6–7: a record torn under the MOVE is contained, once per object ----

/// Bytes a racing writer can leave in a `seg:` record the root still names: no reader decodes
/// them, so the move cannot rewrite the record — and must not read it as a lost race.
const TORN: &[u8] = b"{not a segment record";

/// Owe `owed` (chunks of segment 1, ascending), tear segment 1 between the resolver's page and
/// the move's own read, and run one pass: the MOVE meets the damage. The object is contained
/// exactly ONCE however many obligations meet it, nothing is written or drained, the pass does
/// not certify, and each dispatched repair is offset as an abort. Named negations: answer the
/// move's typed error as a conflict (`Satisfied`); drop the abort (a success that never was).
async fn torn_under_the_move(owed: &[ChunkId]) {
    let (meta, d0, free) = <(MemMeta, MemDServer, MemDServer)>::default();
    seed(&meta, &metadata::inode_key(INODE), &records()).await;
    for &chunk in owed {
        owe(&meta, &d0, chunk).await;
    }
    let root_before = root(&meta).await;
    let torn = Bytes::from_static(TORN);
    meta.arm(Race::AfterSegmentPage, vec![(seg_key(1), torn.clone())]);

    let (outcome, logged) = run(&meta, &d0, (FREE, &free)).await;

    assert!(meta.raced(), "fixture: the race never landed");
    assert_eq!(
        outcome,
        Reconciled::Blocked,
        "a record the move cannot rewrite is a hole, not a retry: {logged}"
    );
    assert_eq!(queued(&meta).await, owed, "every obligation is kept");
    assert_eq!(
        segment(&meta, 1).await,
        torn,
        "the torn record is not rewritten"
    );
    assert_eq!(root(&meta).await, root_before, "the root is untouched");
    assert!(
        orphans(&meta).await.is_empty(),
        "no orphan mark is published"
    );
    assert!(
        free.list_fragments().await.unwrap().is_empty(),
        "no fragment is written ahead of a move that was never prepared"
    );
    assert_eq!(
        contained(&logged, "inode:1"),
        (1, 1, true),
        "ONE name and ONE count for the object, however many obligations meet it: {logged}"
    );
    let n = owed.len();
    assert_eq!(
        (
            ticks(&logged, "repaired"),
            ticks(&logged, "aborted"),
            logged
                .matches(r#""reason":"unresolvable-chunk-map""#)
                .count(),
        ),
        (n, n, n),
        "every repair dispatched is offset as an abort naming why, never a success: {logged}"
    );
}

#[tokio::test]
async fn a_segment_record_torn_under_the_move_is_contained() {
    torn_under_the_move(&[CHUNK]).await;
}

#[tokio::test]
async fn two_obligations_meeting_one_torn_record_are_one_containment() {
    torn_under_the_move(&[CHUNK, SIBLING]).await;
}

// ---- legs 8 and 12: an object the move cannot address is contained, never skipped or retried ----

/// Seed the segmented object at `key` — one the move cannot CAS under — owe [`CHUNK`] and run a
/// pass. Its owed chunk is still REFERENCED: contained, named once under its own key, nothing
/// written, and the reading has a hole, so even the obligation no record references is held.
async fn unaddressable(key: &str) {
    let (meta, d0, free) = <(MemMeta, MemDServer, MemDServer)>::default();
    seed(&meta, key.as_bytes(), &records()).await;
    owe(&meta, &d0, CHUNK).await;
    repair::enqueue_repair(&meta, DELETED, "scrub")
        .await
        .unwrap();
    let before = meta.scan(b"").await.unwrap();

    let (outcome, logged) = run(&meta, &d0, (FREE, &free)).await;

    assert_eq!(outcome, Reconciled::Blocked, "{logged}");
    assert_eq!(
        queued(&meta).await,
        vec![DELETED, CHUNK],
        "nothing drains over a reading with a hole in it"
    );
    assert_eq!(
        meta.scan(b"").await.unwrap(),
        before,
        "no metadata is written: every row in the store is byte-identical"
    );
    assert!(
        free.list_fragments().await.unwrap().is_empty(),
        "no fragment is written for a move the pass may not attempt"
    );
    assert_eq!(
        contained(&logged, key),
        (1, 1, true),
        "the object is named under its own key: {logged}"
    );
}

/// A key that names no id at all. A silent skip would drain both obligations.
#[tokio::test]
async fn a_segmented_object_under_an_unparsable_key_is_contained_not_skipped() {
    unaddressable("inode:x").await;
}

/// Leg 12 — `inode:01` parses to id 1, but the move pins the root at the canonical `inode:1`,
/// which holds nothing. Unguarded, every pass rewrites the fragment, loses that CAS and answers
/// `Satisfied` with the obligation never drained. Named negation: drop the canonical-key guard.
#[tokio::test]
async fn a_segmented_object_under_a_noncanonical_key_is_contained_not_retried_forever() {
    unaddressable("inode:01").await;
}

// ---- legs 9–10: a FLAT record the move cannot address or advance is contained ----

/// Seed a committed FLAT object over `chunks` at `version`, owe [`CHUNK`] (held on
/// `[SURVIVOR, LOST]`), and run one pass: the flat arm runs through the same move, so an object
/// it cannot rewrite is contained as a segmented one is — named once, nothing written or drained.
async fn flat_contained(chunks: Vec<ChunkRef>, version: u64) {
    let (meta, d0, free) = <(MemMeta, MemDServer, MemDServer)>::default();
    let flat = InodeRecord {
        size: CHUNK_LEN,
        chunk_map: ChunkMap::from(chunks),
        state: InodeState::Committed,
        version,
        ..Default::default()
    };
    put(&meta, metadata::inode_key(INODE), metadata::encode(&flat)).await;
    owe(&meta, &d0, CHUNK).await;
    let before = root(&meta).await;

    let (outcome, logged) = run(&meta, &d0, (FREE, &free)).await;

    assert_eq!(outcome, Reconciled::Blocked, "{logged}");
    assert_eq!(root(&meta).await, before, "the record is not rewritten");
    assert_eq!(queued(&meta).await, vec![CHUNK], "the obligation is kept");
    assert!(
        free.list_fragments().await.unwrap().is_empty(),
        "no fragment is written ahead of a move that was never prepared"
    );
    assert_eq!(contained(&logged, "inode:1"), (1, 1, true), "{logged}");
}

/// A record already at `version` `u64::MAX` cannot take the repoint's next one, and a wrapped
/// version would read as an older generation.
#[tokio::test]
async fn a_flat_record_whose_version_cannot_advance_is_contained_not_wrapped() {
    flat_contained(vec![chunk_ref(CHUNK, vec![SURVIVOR, LOST])], u64::MAX).await;
}

/// A flat map is not length-checked at decode, so its lengths can leave `u64` before the owed
/// chunk, which then has no byte address: contained, never a conflict every pass.
#[tokio::test]
async fn a_flat_chunk_past_a_length_overflow_is_contained_not_left_conflicting() {
    let huge = |id| ChunkRef {
        id,
        scheme: EcScheme::None,
        len: u64::MAX,
        placement: vec![SURVIVOR],
    };
    let chunks = vec![
        huge(DECOY),
        huge(SIBLING),
        chunk_ref(CHUNK, vec![SURVIVOR, LOST]),
    ];
    flat_contained(chunks, 1).await;
}

// ---- leg 11: a STORE fault under the move ends the pass (RED on the base) ----

/// The move's own `seg:` read meets a plain backend fault: the store failing, not this object's
/// damage, so the pass ends with it rather than naming `inode:1` unreadable and walking on
/// (`06-runtime-view.md:29`); nothing written or drained. Named negation: contain the fault like
/// the move's typed error (`Blocked`, a row naming the object).
#[tokio::test]
async fn a_store_fault_under_the_moves_own_read_ends_the_pass() {
    let (meta, d0, free) = fixture().await;
    let (root_before, segment_before) = (root(&meta).await, segment(&meta, 1).await);
    meta.faulting.store(true, Ordering::SeqCst);

    let (outcome, logged) = attempt(&meta, &d0, (FREE, &free)).await;

    assert!(
        !meta.faulting.load(Ordering::SeqCst),
        "fixture: the fault never fired"
    );
    let err = outcome.expect_err("a store fault is not one object's damage: it ends the pass");
    assert!(err.to_string().contains(STORE_FAULT), "{err}");
    assert_eq!(
        contained(&logged, "inode:1"),
        (0, 0, false),
        "the store's fault is never reported as the object's: {logged}"
    );
    assert_eq!(queued(&meta).await, vec![CHUNK], "nothing drains");
    assert_eq!(segment(&meta, 1).await, segment_before, "nothing rewritten");
    assert_eq!(root(&meta).await, root_before, "the root is untouched");
    assert!(orphans(&meta).await.is_empty(), "no orphan mark");
    assert!(
        free.list_fragments().await.unwrap().is_empty(),
        "no fragment is written ahead of a move that was never prepared"
    );
}

// ---- leg 13: the move pins the root the resolve ANSWERED from (RED on the base) ----

/// A supersede lands whole under the resolve: the same records under a NEW group and a
/// version-2 root naming it, after the resolver's page of the old group. The resolve restarts
/// onto that live root and the plan is built from ITS chunks, so the move must pin IT. Pinned to
/// the root the scan returned instead, the move prepares a rewrite of the retired group and
/// loses the CAS on a root the store no longer holds. Named negation: build `Object::prior`
/// from the scanned record rather than `resolved.record`.
#[tokio::test]
async fn a_repair_planned_over_a_restarted_resolve_pins_the_root_it_answered_from() {
    let (meta, d0, free) = fixture().await;
    let retired = [segment(&meta, 0).await, segment(&meta, 1).await];
    let live = SegmentGroup::new(NONCE, EPOCH + 1).unwrap();
    let superseding = generation(&live, 2, &metadata::inode_key(INODE), &records());
    let live_root = superseding.last().expect("the root row").1.clone();
    meta.arm(Race::AfterSegmentPage, superseding);

    let (outcome, logged) = run(&meta, &d0, (FREE, &free)).await;

    assert!(meta.raced(), "fixture: the race never landed");
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the repair lands on the generation the resolve answered from: {logged}"
    );
    let live_one = metadata::seg_key(&live, 1).unwrap();
    let moved = meta.get(&live_one).await.unwrap().expect("the live record");
    assert_eq!(
        placements_in(&moved),
        vec![vec![SURVIVOR, FREE], vec![SURVIVOR, LOST]],
        "the LIVE group's record names the rebuilt fragment's server"
    );
    assert_eq!(
        [segment(&meta, 0).await, segment(&meta, 1).await],
        retired,
        "the retired group is not rewritten"
    );
    assert_eq!(
        root(&meta).await,
        live_root,
        "the live root is the superseding writer's, byte for byte"
    );
    assert!(queued(&meta).await.is_empty(), "the obligation is drained");
}
