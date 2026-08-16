//! Issue #697 (child 3 of 3 of the #681 split, 0016 decision 7(e)): **the maintenance pass
//! that restores redundancy reads every committed object through the resolver every other
//! consumer already shares — ONCE per pass, not once per obligation — contains per object
//! what it cannot read, and REFUSES, rather than aborts or silently drains, the repair it
//! does not own.**
//!
//! On the base one `seg:`-backed committed object (or one record that will not decode) made
//! `reconstruction::reconcile` return `Err` for the WHOLE store, so a store holding one
//! published multipart object stopped restoring redundancy altogether; and the assessment
//! scanned all of `inode:` once per queued obligation — #647's open Q×N finding.
//!
//! **No assertion names a symbol this patch introduces** (the per-fix red leg reverts the
//! production file and keeps this one, so such a reference would degrade the red to a compile
//! error), every leg drives the fenced control point `reconcile_step`, and every leg asserts
//! the call SUCCEEDED before reading its outcome. Leg 6 is a REGRESSION guard rather than a
//! base red: the base already scans zero times over an empty queue, and lifting the reading to
//! the top of the pass instead would silently make it read, and claim over, a store it owed
//! nothing on.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, ChunkMap, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, SegmentGroup,
    SegmentRecord, SegmentRef, SegmentedMap,
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

use Seed::{SegmentHole, Segmented, Undecodable, UnderReplicated};

// ---- in-memory trait doubles (the pass is proven over the seams, backend-agnostic) ----

/// A `BTreeMap`-backed metadata store, so `scan` answers in key order and "the damaged record
/// is met FIRST" is a fixture property rather than luck — otherwise "the healthy object was
/// still repaired" could pass on an implementation that abandons the walk at the first
/// blocker, simply because the healthy object had already been handled by then. It also meters
/// what the complexity property is stated over: the two reads on their SEPARATE seam methods
/// (the committed-namespace walk uses `scan`, the resolver's bounded `seg:` range uses
/// `scan_page`, which it can also fault).
#[derive(Default)]
struct MemMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    /// `scan(b"inode:")` calls — readings of the committed namespace itself.
    inode_scans: AtomicUsize,
    /// `scan_page(b"seg:…")` calls — bounded range pages the RESOLVER asked for.
    seg_pages: AtomicUsize,
    /// Whether a `seg:` page answers with a plain, non-`ChunkMapError` fault: a backend outage
    /// under the read the resolver performs, never under `scan(b"inode:")` itself — a fault
    /// there aborts before anything is named, the opposite of what leg 5 asserts.
    faulting: AtomicBool,
}

impl MemMeta {
    /// `(readings of the committed namespace, bounded `seg:` pages the resolver asked for)`.
    fn counts(&self) -> (usize, usize) {
        (
            self.inode_scans.load(Ordering::SeqCst),
            self.seg_pages.load(Ordering::SeqCst),
        )
    }

    /// Every committed record and every `seg:` row, byte for byte — what a refusal, which
    /// writes nothing at all, must leave untouched.
    fn records(&self) -> BTreeMap<Vec<u8>, Bytes> {
        let kv = self.kv.lock().unwrap();
        let rows = kv.iter().filter(|(k, _)| !k.starts_with(b"repair:"));
        rows.map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if prefix.starts_with(b"inode:") {
            self.inode_scans.fetch_add(1, Ordering::SeqCst);
        }
        let kv = self.kv.lock().unwrap();
        let rows = kv.iter().filter(|(k, _)| k.starts_with(prefix));
        Ok(rows.map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    // The required paginated read (#634): a test double needs *a* body, not a backend's —
    // the dev-only testkit helper pages over this store's own `scan`.
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        if prefix.starts_with(b"seg:") {
            self.seg_pages.fetch_add(1, Ordering::SeqCst);
            if self.faulting.load(Ordering::SeqCst) {
                return Err(Box::new(std::io::Error::other(STORE_FAULT)));
            }
        }
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (k, v) in batch.puts {
            kv.insert(k, v);
        }
        for k in batch.deletes {
            kv.remove(&k);
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

// ---- audit capture (the proven in-tree pattern, `crates/core/tests/read_repair.rs`) ----

/// A `MakeWriter` collecting what the pass emits, so a leg asserts on the record the
/// durability seam actually carried rather than assuming one exists.
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

/// How many audit rows carried `action` — the accounting legs 2, 3 and 5 are stated over (one
/// refusal per OBJECT, one name per damaged record).
fn rows(logged: &str, action: &str) -> usize {
    logged.matches(&format!(r#""action":"{action}""#)).count()
}

/// Whether the pass NAMED `object` — the one thing an operator can act on about a record no
/// consumer can read (#694), spelled as the store spells its key (`gc::object_name`).
fn names(logged: &str, object: &str) -> bool {
    logged.contains(&format!(r#""inode":"{object}""#))
}

/// How many times `counter` was incremented on the durability seam — the metric half of each
/// audit row, asserted beside it so a label without its counter (or a counter that ticks per
/// chunk where the rule is per object) is a failure rather than a silence.
fn ticks(logged: &str, counter: &str) -> usize {
    logged
        .matches(&format!(r#""monotonic_counter.{counter}":1"#))
        .count()
}

// ---- fixture ----

const NOW: u64 = 10_000;
const CHUNK_LEN: u64 = 8;
/// RS(1,1): one data shard plus one parity shard — the smallest scheme carrying redundancy, so
/// the ONE surviving fragment (k = 1) is enough to rebuild the other.
const K: u8 = 1;
const M: u8 = 1;
/// Segment-group nonces (32 lowercase hex characters, `0016:354`) and the fence epoch their
/// segments are scoped by. The damaged object gets its own group, so nothing about it is inside
/// a healthy object's bounded `seg:` range.
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const DAMAGED_NONCE: &str = "fedcba9876543210fedcba9876543210";
const EPOCH: u64 = 7;
/// Bytes that are not an [`InodeRecord`] — a record whose own bytes will not `decode`.
const UNREADABLE_RECORD: &[u8] = b"not a record";
/// The injected fault's text, so leg 5 proves THIS fault came back and not another failure.
const STORE_FAULT: &str = "simulated store fault: segment range unreachable";
/// The one healthy multipart object every leg that needs one seeds.
const HEALTHY_SEGMENTED: Seed = Segmented { chunks: &SEG };

/// Chunks inside a segmented object; chunks each alone in a flat object of their own; the chunk
/// whose `seg:` record was never written; and one **no** committed record references — the
/// obligation whose draining is the loss this slice must not introduce while removing the
/// abort.
const SEG: [ChunkId; 2] = [0xA1_00, 0xA2_00];
const FLAT: [ChunkId; 2] = [0xF1_00, 0xF2_00];
const UNWRITTEN: ChunkId = 0xD1_00;
const DELETED: ChunkId = 0x0E_00;

/// Eight chunks inside ONE flat object, of which only the four at ODD indices ([`OWED`]) are
/// queued: the object's map is materially larger than the obligations inside it, so the pass
/// must address the scanned generation's own chunk index (a repair driven by queue position
/// would repoint a neighbour) and only what is owed may move.
const SHARED: [ChunkId; 8] = [
    0xB0_00, 0xB1_00, 0xB2_00, 0xB3_00, 0xB4_00, 0xB5_00, 0xB6_00, 0xB7_00,
];
const OWED: [ChunkId; 4] = [SHARED[1], SHARED[3], SHARED[5], SHARED[7]];

/// What a fixture object is. One parameterised helper plants every shape, so a leg names the
/// store it wants instead of assembling one.
#[derive(Clone, Copy)]
enum Seed<'a> {
    /// A committed **flat** object holding `chunks`, each placed on servers `[0, 1]` with only
    /// fragment 0 on disk: server 1 is the loss — in neither the fleet nor the topology below —
    /// so every one is under-replicated and its repair must MOVE the placement. More than one
    /// chunk is the many-obligations-in-ONE-object shape.
    UnderReplicated { chunks: &'a [ChunkId] },
    /// A committed **segmented** root over `chunks` (one per segment, under [`NONCE`]), every
    /// `seg:` record written: an ordinary healthy multipart object. Raw records throughout,
    /// never a committer — this slice lands no producer of segmented maps (#653 owns one).
    Segmented { chunks: &'a [ChunkId] },
    /// A committed **segmented** root (its own group) naming a segment whose `seg:` record was
    /// never written — a hole on a generation the root still names, which the resolver reports
    /// as `ChunkMapError::SegmentAbsent`.
    SegmentHole,
    /// A committed record whose own bytes will not `decode`.
    Undecodable,
}

fn doubles() -> (MemMeta, MemDServer, MemDServer) {
    Default::default()
}

fn chunk_ref(chunk: ChunkId) -> ChunkRef {
    ChunkRef {
        id: chunk,
        scheme: EcScheme::ReedSolomon { k: K, m: M },
        len: CHUNK_LEN,
        placement: vec![0, 1],
    }
}

async fn commit(meta: &MemMeta, batch: WriteBatch) {
    assert_eq!(meta.commit(batch).await.unwrap(), CommitOutcome::Committed);
}

/// Seed one fixture object at `inode`, and **prove the fixture is what the leg thinks it is**:
/// exactly the damaged shape may fail to resolve, so no leg passes because the fault it was
/// built around silently stopped being one — nor because a shape meant to be healthy quietly
/// became damaged.
async fn seed(meta: &MemMeta, d0: &MemDServer, inode: InodeId, what: Seed<'_>) {
    let key = metadata::inode_key(inode);
    let (chunk_map, size) = match what {
        Undecodable => {
            commit(meta, WriteBatch::new().put(key, UNREADABLE_RECORD.to_vec())).await;
            return;
        }
        UnderReplicated { chunks } => {
            for &chunk in chunks {
                let data = vec![b'w'; CHUNK_LEN as usize];
                let shards = erasure::encode(K.into(), M.into(), &data).expect("shards encode");
                let frag = FragmentId { chunk, index: 0 };
                let bytes = encode_ec_fragment(chunk, 0, K, M, &shards[0]);
                d0.put_fragment(frag, bytes, None).await.unwrap();
            }
            let refs: Vec<ChunkRef> = chunks.iter().copied().map(chunk_ref).collect();
            (ChunkMap::from(refs), chunks.len() as u64 * CHUNK_LEN)
        }
        Segmented { chunks } => seed_group(meta, NONCE, chunks, chunks.len()).await,
        SegmentHole => seed_group(meta, DAMAGED_NONCE, &[UNWRITTEN], 0).await,
    };
    let root = InodeRecord {
        size,
        chunk_map,
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let root_bytes = metadata::encode(&root);
    commit(meta, WriteBatch::new().put(key.clone(), root_bytes)).await;
    let resolves = metadata::resolve_chunk_map(meta, &key, &root).await;
    assert_eq!(
        resolves.is_err(),
        matches!(what, SegmentHole),
        "fixture: exactly the seeded hole may fail to resolve"
    );
}

/// Write the first `written` of `chunks.len()` `seg:` records and return the root map naming
/// all of them, with the size its segment table spans. `written < chunks.len()` is the real gap
/// the containment rule exists for: a segment the root's own table names, on a generation it
/// still names, that genuinely never got written.
async fn seed_group(
    meta: &MemMeta,
    nonce: &str,
    chunks: &[ChunkId],
    written: usize,
) -> (ChunkMap, u64) {
    let group = SegmentGroup::new(nonce, EPOCH).unwrap();
    let mut segments = Vec::new();
    for (index, &chunk) in chunks.iter().enumerate() {
        let byte_offset = index as u64 * CHUNK_LEN;
        let index = index as u32;
        segments.push(SegmentRef {
            index,
            byte_offset,
            byte_len: CHUNK_LEN,
        });
        if (index as usize) < written {
            let record = SegmentRecord::new(vec![chunk_ref(chunk)], byte_offset).unwrap();
            let key = metadata::seg_key(&group, index).unwrap();
            commit(meta, WriteBatch::new().put(key, metadata::encode(&record))).await;
        }
    }
    let map = SegmentedMap::new(group, segments).unwrap();
    (ChunkMap::Segmented(map.clone()), map.span())
}

async fn enqueue(meta: &MemMeta, chunks: &[ChunkId]) {
    for &chunk in chunks {
        repair::enqueue_repair(meta, chunk, "scrub").await.unwrap();
    }
}

/// The obligations still on the shared repair queue, in a stable order.
async fn queued(meta: &MemMeta) -> Vec<ChunkId> {
    let mut chunks = repair::queued_repairs(meta).await.unwrap();
    chunks.sort_unstable();
    chunks
}

/// `inode`'s committed placement vectors, in chunk order, and its `version`: `[0, 1]` and 1 as
/// seeded; `[0, 2]` once a repair has rebuilt the lost fragment onto the free failure domain,
/// with one version bump per version-conditional repoint that landed.
async fn committed(meta: &MemMeta, inode: InodeId) -> (Vec<Vec<DServerId>>, u64) {
    let key = metadata::inode_key(inode);
    let bytes = meta.get(&key).await.unwrap().unwrap();
    let record: InodeRecord = metadata::decode(&bytes).unwrap();
    let ChunkMap::Flat(chunks) = record.chunk_map else {
        panic!("fixture: inode {inode} is not a flat object");
    };
    let placements = chunks.into_iter().map(|c| c.placement).collect();
    (placements, record.version)
}

/// One reconstruction pass through the **real fenced control point**, over two failure domains
/// — the survivor's (server 0) and one free one (server 2). Server 1, every under-replicated
/// chunk's other placed server, is deliberately in neither the fleet nor the topology: it is
/// the loss.
///
/// Reconstruction is the ONLY loop wired: the other loops walk `inode:` themselves, so a
/// store-wide scan count would be measuring theirs too. The seam counters are reset here, so a
/// leg's counts are the PASS's and not the seeding's. Returns the pass's answer beside
/// everything it emitted on the durability seam.
async fn run(
    meta: &MemMeta,
    (d0, d2): (&MemDServer, &MemDServer),
) -> (std::result::Result<Reconciled, ReconcileError>, String) {
    enable_audit_callsites();
    let coord = MemCoordination::new();
    let leader = Custodian::elect(&coord, "zone-reconstruction")
        .await
        .unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    let mut topology = Topology::default();
    topology.register(0, "A").register(2, "C");
    let fleet: [(DServerId, &dyn ChunkStore); 2] = [(0, d0), (2, d2)];
    let ctx = ReconstructionContext {
        meta,
        fleet: &fleet,
        topology: &topology,
        unreachable: &[],
    };
    let capture = Capture::default();
    let layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(capture.clone());
    meta.inode_scans.store(0, Ordering::SeqCst);
    meta.seg_pages.store(0, Ordering::SeqCst);
    let outcome = reconcile_step(&zone, &leader, None, None, Some(&ctx), None, NOW)
        .with_subscriber(tracing::Dispatch::new(
            tracing_subscriber::registry().with(layer),
        ))
        .await;
    let logged = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    (outcome, logged)
}

/// Install a permissive global `tracing` default **once**, so the audit callsites never latch
/// `Interest::never` under the parallel test harness (issue #214): `tracing` caches each
/// callsite's interest in process-global state the first time it is hit.
fn enable_audit_callsites() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

// ---- leg 1: a healthy segmented object ends nothing and blocks nothing ----

/// A segmented object the pass READ successfully and owes no repair on is ordinary and healthy:
/// not named, not counted, and no reason to withhold certification. Get this wrong and every
/// store holding one multipart object is `Blocked` forever — this slice's own defect in mirror
/// image.
#[tokio::test]
async fn a_healthy_segmented_object_neither_ends_the_pass_nor_blocks_it() {
    let (meta, d0, d2) = doubles();
    // `inode:1` sorts first, so the segmented object is met BEFORE the flat work.
    seed(&meta, &d0, 1, HEALTHY_SEGMENTED).await;
    seed(&meta, &d0, 2, UnderReplicated { chunks: &FLAT[..1] }).await;
    enqueue(&meta, &[FLAT[0], DELETED]).await;

    let (outcome, logged) = run(&meta, (&d0, &d2)).await;

    assert_eq!(
        outcome.expect("one multipart object must not stop repair for the whole store"),
        Reconciled::Changed,
        "a segmented object nothing is owed on blocks nothing: {logged}"
    );
    assert_eq!(
        committed(&meta, 2).await,
        (vec![vec![0, 2]], 2),
        "the flat chunk was repointed onto the free failure domain"
    );
    assert!(
        queued(&meta).await.is_empty(),
        "the repair commit discharged its obligation, and this COMPLETE reading drained the \
         one no committed map references"
    );
}

// ---- leg 2: an obligation inside a `seg:` record is refused, not discarded ----

/// A chunk whose `ChunkRef` lives in a `seg:` record is **refused**: the segmented write path
/// is #682's, so nothing is written, the obligation stays queued (it is the last record saying
/// live data is under-replicated), and the pass does not certify. Two obligations inside ONE
/// segmented object are ONE refusal — the accounting is per object.
#[tokio::test]
async fn an_obligation_inside_a_segmented_object_is_refused_never_discarded() {
    let (meta, d0, d2) = doubles();
    seed(&meta, &d0, 1, HEALTHY_SEGMENTED).await;
    enqueue(&meta, &SEG).await;
    let before = meta.records();

    let (outcome, logged) = run(&meta, (&d0, &d2)).await;

    assert_eq!(
        outcome.expect("a refusal is a refusal, not an abort of the whole pass"),
        Reconciled::Blocked,
        "a pass holding back a repair it may not perform must not certify: {logged}"
    );
    assert_eq!(
        queued(&meta).await,
        SEG.to_vec(),
        "both obligations still queued: refused, never discarded for want of a writer"
    );
    assert_eq!(
        meta.records(),
        before,
        "a refusal writes NOTHING: the `seg:` records and the root are byte-identical"
    );
    assert_eq!(
        (
            rows(&logged, "refused-segmented"),
            ticks(&logged, "reconstruction_refused_records")
        ),
        (1, 1),
        "ONE refusal row and ONE count per object, not one per chunk: {logged}"
    );
    assert!(
        names(&logged, "inode:1"),
        "the refusal names the object an operator has to act on: {logged}"
    );
    assert!(
        logged.contains(r#""gauge.reconstruction_under_replicated":0"#),
        "a refused chunk is never repaired, so counting it on the repairable backlog would \
         floor the day-one return-to-zero signal: {logged}"
    );
}

// ---- leg 3: an unreadable object is named and contained, and nothing is drained ----

/// Containment is per object and the answer still gets made for the rest — but an obligation is
/// **discharged or kept, never discarded for want of a reading**: "I could not read the map"
/// and "no committed map references this chunk" are different facts, and only the second
/// permits draining.
#[tokio::test]
async fn an_unreadable_object_is_named_and_the_pass_drains_nothing() {
    let (meta, d0, d2) = doubles();
    // `inode:1` and `inode:2` sort first, so both damaged records are met BEFORE the healthy
    // repair — a property of the `BTreeMap`-backed store, not luck.
    seed(&meta, &d0, 1, SegmentHole).await;
    seed(&meta, &d0, 2, Undecodable).await;
    seed(&meta, &d0, 3, UnderReplicated { chunks: &FLAT[..1] }).await;
    enqueue(&meta, &[FLAT[0], DELETED]).await;

    let (outcome, logged) = run(&meta, (&d0, &d2)).await;

    assert_eq!(
        outcome.expect("an unreadable record is one object's fault, not the store's"),
        Reconciled::Blocked,
        "never `Satisfied`: an operator reading that is told redundancy is restored and acts \
         on it, and this pass could not see every committed object: {logged}"
    );
    assert_eq!(
        committed(&meta, 3).await,
        (vec![vec![0, 2]], 2),
        "containment is per object: the healthy repair still lands"
    );
    assert_eq!(
        queued(&meta).await,
        vec![DELETED],
        "while the reading is INCOMPLETE the pass drains nothing — an object it could not \
         read is a hole in the very conclusion 'no committed map references this chunk'"
    );
    assert!(
        rows(&logged, "unresolvable-chunk-map") == 2
            && ticks(&logged, "reconstruction_unresolvable_records") == 2
            && names(&logged, "inode:1")
            && names(&logged, "inode:2"),
        "both damaged objects are named and counted, each under its own `inode:` key, so a \
         repair guided by the trail fixes the right record: {logged}"
    );
}

// ---- leg 4: the namespace is read ONCE per pass — O(N), not O(Q×N) ----

/// The pass's READING is bounded by the obligations it holds, not by their product with the
/// namespace: a loop re-reading it once per obligation stops converging as a store grows —
/// the permanent failure mode C-1 forbids (#647). That, not per-repair cost, is what binds.
///
/// Q = 6 obligations over N = 3 committed flat objects, **four of them inside ONE eight-chunk
/// object**, so the property is measured with the queue both spread across objects and piled
/// inside one:
///
/// * **reads** — ONE `scan(b"inode:")` for the whole queue, whatever it holds (the base makes
///   one per obligation), and one bounded `seg:` page per segmented object (S = 1);
/// * **the write path is the base's, untouched** — one version-conditional commit per repaired
///   chunk, in repair-priority order, each conditioned on the generation the scan returned. So
///   two obligations inside one object behave exactly as on `origin/main`: the first repoint
///   lands and the second loses the CAS its own precondition just superseded and stays queued;
/// * **convergence** — every obligation is discharged eventually, each pass still reading the
///   namespace exactly ONCE, and only the four chunks actually owed a repair ever move, each at
///   its own index in the scanned map (a repair driven by queue position would repoint a
///   neighbour). HOW MANY passes a queue piled inside one object takes is the base's, unchanged.
#[tokio::test]
async fn the_committed_namespace_is_read_once_per_pass() {
    let (meta, d0, d2) = doubles();
    // S = 1 segmented object (2 segments, far under `SEGMENT_PAGE_LIMIT`, so its bounded range
    // is exactly one page) beside N = 3 committed flat objects.
    seed(&meta, &d0, 1, HEALTHY_SEGMENTED).await;
    seed(&meta, &d0, 2, UnderReplicated { chunks: &SHARED }).await;
    seed(&meta, &d0, 3, UnderReplicated { chunks: &FLAT[..1] }).await;
    seed(&meta, &d0, 4, UnderReplicated { chunks: &FLAT[1..] }).await;
    enqueue(&meta, &OWED).await;
    enqueue(&meta, &FLAT).await;

    let (outcome, logged) = run(&meta, (&d0, &d2)).await;

    assert_eq!(
        outcome.expect("the pass completes over a store with a segmented object"),
        Reconciled::Changed,
        "the repairs landed: {logged}"
    );
    assert_eq!(
        meta.counts(),
        (1, 1),
        "ONE reading of the committed namespace for the whole pass, independent of the \
         queue's depth (the base makes one per obligation, 6 here), and the resolver's \
         bounded `seg:` reads are one per SEGMENTED object (S = 1) — never one per \
         obligation, and never zero, which would mean it went unresolved"
    );
    for inode in 3..=4 {
        assert_eq!(
            committed(&meta, inode).await,
            (vec![vec![0, 2]], 2),
            "every repair in an object of its own landed under that one reading"
        );
    }
    assert_eq!(
        queued(&meta).await,
        OWED[1..].to_vec(),
        "and inside the shared object the write path is the base's, unchanged: the first \
         repoint landed and its siblings lost the CAS on the generation it superseded, so \
         they stay QUEUED for the next pass — kept, never discarded: {logged}"
    );

    // Convergence: the obligations that lost the CAS are repaired by later passes, and EVERY
    // pass reads the committed namespace exactly once, however many obligations it holds.
    let mut passes = 0;
    while !queued(&meta).await.is_empty() {
        passes += 1;
        assert!(passes <= OWED.len(), "the queue must converge, not churn");
        let (outcome, logged) = run(&meta, (&d0, &d2)).await;
        outcome.expect("each following pass completes too");
        assert_eq!(
            meta.counts().0,
            1,
            "one reading per pass, whatever the queue holds: {logged}"
        );
    }
    let repointed: Vec<Vec<DServerId>> = (0..SHARED.len())
        .map(|at| if at % 2 == 1 { vec![0, 2] } else { vec![0, 1] })
        .collect();
    assert_eq!(
        committed(&meta, 2).await,
        (repointed, 1 + OWED.len() as u64),
        "all four repairs inside the eight-chunk object landed, one version-conditional \
         repoint each — and only the four chunks actually owed a repair moved, at their own \
         indices in the scanned map"
    );
}

// ---- leg 5: a fault that is not one object's map still ends the pass ----

/// The over-containment guard — containing EVERY error would pass legs 1–4. A store fault under
/// the resolver is not one object's fault, so it still propagates; **and** the record this walk
/// had already found unreadable is named by then, because a name this pass held must not go
/// down with the `?` that ends it (a corrupt root has no repair path and no operator tooling
/// yet, #694, so that name is the operator's whole situational awareness).
#[tokio::test]
async fn a_store_fault_under_the_resolver_ends_the_pass_after_the_name_is_out() {
    let (meta, d0, d2) = doubles();
    // `inode:1` — undecodable, so it is contained and named FIRST; `inode:2` — a segmented root
    // met AFTER it, whose `seg:` read the store answers with a plain backend fault.
    seed(&meta, &d0, 1, Undecodable).await;
    seed(&meta, &d0, 2, Segmented { chunks: &SEG[..1] }).await;
    enqueue(&meta, &SEG[..1]).await;
    meta.faulting.store(true, Ordering::SeqCst);

    let (outcome, logged) = run(&meta, (&d0, &d2)).await;

    let err = outcome.expect_err("a store fault is not one object's map: it ends the pass");
    assert!(
        err.to_string().contains(STORE_FAULT),
        "the injected fault is what came back, not another failure: {err}"
    );
    assert!(
        rows(&logged, "unresolvable-chunk-map") == 1 && names(&logged, "inode:1"),
        "the record already found unreadable is named BEFORE the later fault ends the pass: \
         {logged}"
    );
}

// ---- leg 6: an empty queue performs no reading ----

/// The pass certifies only over the reading it performed — and with nothing owed it performs
/// none, so it makes no claim about objects it never read, over a store that holds one it could
/// not have read anyway.
#[tokio::test]
async fn an_empty_queue_reads_nothing_and_answers_satisfied() {
    let (meta, d0, d2) = doubles();
    seed(&meta, &d0, 1, Undecodable).await;

    let (outcome, logged) = run(&meta, (&d0, &d2)).await;

    assert_eq!(
        outcome.expect("an empty queue is not an error"),
        Reconciled::Satisfied,
        "nothing owed, so nothing read and nothing claimed about what was not: {logged}"
    );
    assert_eq!(
        meta.counts().0,
        0,
        "with an empty queue the committed namespace is not read at all"
    );
}
