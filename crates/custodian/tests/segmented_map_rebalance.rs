//! Issue #696 (child 2 of 3 of the #681 split, 0016 decision 7(e)): **the rebalance loop
//! reads every committed object through the shared resolver, contains per object what it
//! cannot read, and refuses — rather than aborts or silently discards — the evacuation it
//! does not own.**
//!
//! The base reads the chunk map inline out of the inode record at two sites
//! (`crates/custodian/src/rebalance.rs:158-164`, `:255-261` on `origin/main @ 339da46`), each
//! `?`-ing out of the WHOLE scan on the first segmented record — so ONE multipart object stops
//! every drain in the store and no server can be decommissioned while one exists.
//!
//! All five legs are therefore red on the base, for that one reason. Legs 4 and 5 are here
//! anyway because they are the only two that go red against an **over-broad** fix: one that
//! blocks every store holding a segmented object (then no decommission ever certifies — this
//! slice's own defect in mirror image), or one that contains a store fault as if it were one
//! object's. Every leg drives the REAL fenced control point
//! [`reconcile_step`](wyrd_custodian::reconcile_step) over in-memory trait doubles —
//! `rebalance::reconcile` is `pub(crate)`, and a test-only entry would prove nothing.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Once};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::{encode as encode_fragment, FragmentHeader};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::*;
use wyrd_core::placement::Topology;
use wyrd_custodian::desired_state::{set_lifecycle, DServerLifecycle::Draining};
use wyrd_custodian::{reconcile_step, Custodian, FencedZone, RebalanceContext, Reconciled};
use wyrd_traits::*;

/// What the fenced step answers (`Result` on its own is `wyrd_traits`').
type Answer = std::result::Result<Reconciled, wyrd_custodian::ReconcileError>;

// ---- in-memory trait stores (the loop is proven over the seams, backend-agnostic) ----

/// A `BTreeMap`-backed metadata store carrying the one injected fault leg 5 needs.
///
/// `BTreeMap` because leg 3 needs the damaged records met FIRST: key order (`inode:1` <
/// `inode:2` < `inode:9`) makes that a fixture property rather than luck of a hash walk —
/// otherwise "the healthy object was still evacuated" could pass on a loop that quits at the
/// first blocker, simply because the healthy object happened to come first.
#[derive(Default)]
struct MemMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    /// Leg 5 sets this: every `get` then fails with a plain STORE error — deliberately NOT a
    /// `metadata::ChunkMapError`, since that is the class that must still end the pass.
    get_fails: Mutex<bool>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if *self.get_fails.lock().unwrap() {
            return Err(STORE_FAULT.into());
        }
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let kv = self.kv.lock().unwrap();
        let hits = kv.iter().filter(|(k, _)| k.starts_with(prefix));
        Ok(hits.map(|(k, v)| (k.clone(), v.clone())).collect())
    }
    /// The required paginated read (#634): the dev-only testkit helper pages over `scan`.
    /// `n` is the trait's `limit`, shortened only so the signature stays on one line.
    async fn scan_page(&self, prefix: &[u8], after: Option<&[u8]>, n: usize) -> Result<ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, n).await
    }
    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        let mut checks = batch.preconditions.iter();
        if checks.any(|pre| kv.get(&pre.key).cloned() != pre.expected) {
            return Ok(CommitOutcome::Conflict);
        }
        assert!(batch.deletes.is_empty(), "no leg deletes metadata");
        kv.extend(batch.puts);
        Ok(CommitOutcome::Committed)
    }
}

/// One D server's fragment bytes — a dumb `ChunkStore` holding REAL v1 fragments, so an
/// evacuation that reaches the copy finds an intact one (`repair::fragment_intact`) and really
/// commits.
#[derive(Default)]
struct MemDServer(Mutex<HashMap<FragmentId, Bytes>>);

#[async_trait]
impl ChunkStore for MemDServer {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.0.lock().unwrap().insert(id, fragment);
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

// ---- the ONE fixture every leg is built from ----

/// The D server the operator marked draining, and where a one-fragment evacuation off it
/// lands: the pool excludes the draining server and the selector takes the lowest-labelled
/// domain left.
const DRAINING: DServerId = 0;
const TARGET: DServerId = 1;
/// Leg 3's two damaged objects, in the key order the walk meets them: `inode:1` is a root
/// naming a `seg:` record that was never written, `inode:2` a record whose own bytes will not
/// decode. Both sort BEFORE `inode:3` (the segmented object) and `inode:9` (the flat one), so
/// "the healthy work still happened" is never explained by the walk reaching it first.
const DAMAGED: [InodeId; 2] = [1, 2];
const SEGMENTED: InodeId = 3;
const FLAT: InodeId = 9;
const FLAT_CHUNK: ChunkId = 0xF1A7;
const SEG: [ChunkId; 3] = [0x5E_6A, 0x5E_6B, 0x5E_6C];
const CHUNK_LEN: u64 = 5;
/// Two chunks, NEITHER on the draining server: the healthy segmented object legs 1, 4 and 5
/// are built around, and (with its last `seg:` record withheld) leg 3's damaged one.
const OFF_DRAIN: [(ChunkId, DServerId); 2] = [(SEG[0], 1), (SEG[1], 2)];
/// A segment-group nonce (32 lowercase hex characters, `0016:354`). One group per store is
/// enough: no leg seeds two segmented objects, so no `seg:` range can overlap another's.
const NONCE: &str = "0123456789abcdef0123456789abcdef";
/// Leg 5's injected fault, as it must appear in the error the pass surfaces — asserting on
/// THIS string separates "the store fault ended the pass" from the base's own abort.
const STORE_FAULT: &str = "metadata store unavailable";

/// One chunk, one fragment (`EcScheme::None`), on one D server — the smallest placement, and a
/// WELL-FORMED one, so no leg's answer can be explained by the malformed-placement arm
/// (`rebalance.rs:177-183`, frozen at the base by this slice).
fn chunk_ref(chunk: ChunkId, dserver: DServerId) -> ChunkRef {
    ChunkRef {
        id: chunk,
        scheme: EcScheme::None,
        len: CHUNK_LEN,
        placement: vec![dserver],
    }
}

/// The pass's answer, insisting the call SUCCEEDED first: folding `Err` into "did not certify"
/// would score the defect itself — a pass that aborts over every object — as containment,
/// which is how round 3 of this bundle lost its gate.
fn answered(answer: Answer) -> Reconciled {
    answer.unwrap_or_else(|err| panic!("the pass must COMPLETE and answer: {err}"))
}

struct Fixture {
    meta: MemMeta,
    d: [MemDServer; 3],
    topology: Topology,
}

impl Fixture {
    /// Three failure domains A..C over servers 0..2, with server 0 marked draining.
    async fn new() -> Self {
        let meta = MemMeta::default();
        set_lifecycle(&meta, DRAINING, Draining).await.unwrap();
        let mut topology = Topology::default();
        topology.register(0, "A").register(1, "B").register(2, "C");
        let d = Default::default();
        Self { meta, d, topology }
    }

    /// ONE pass through the real fenced control point, its audit stream captured — into a
    /// per-pass temp file, because `Arc<File>` is a `MakeWriter` as it stands (`&File` is a
    /// `Write`), so legs 2 and 3 read the record the seam actually produced without this
    /// fixture carrying a writer type of its own.
    async fn pass(&self) -> (Answer, String) {
        // `tracing` caches each callsite's interest process-globally on first hit, so a
        // sibling test in this binary hitting an audit callsite with no subscriber installed
        // would leave the capture below empty (wyrd #214) — a silently vacuous assertion in
        // exactly the legs that read the stream. A permissive global default, installed once.
        static INIT: Once = Once::new();
        let permissive = || tracing::subscriber::set_global_default(tracing_subscriber::registry());
        INIT.call_once(|| permissive().unwrap());
        let d = &self.d;
        let fleet: [(DServerId, &dyn ChunkStore); 3] = [(0, &d[0]), (1, &d[1]), (2, &d[2])];
        let ctx = RebalanceContext {
            meta: &self.meta,
            fleet: &fleet,
            topology: &self.topology,
        };
        let coord = MemCoordination::new();
        let leader = Custodian::elect(&coord, "zone-696").await.unwrap();
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

    /// Seed a committed **segmented** root at `inode`, one chunk per `(chunk, dserver)` entry,
    /// each written chunk's fragment on the D server its placement names. `whole` writes every
    /// `seg:` record the root's table names; without it the LAST is left unwritten — a segment
    /// the root still names that genuinely never got written
    /// (`metadata::ChunkMapError::SegmentAbsent`, as surfaced by `metadata::resolve_chunk_map`).
    ///
    /// Raw `WriteBatch` puts through the real validating constructors (#653 owns the real
    /// committer), and the seeding then ASSERTS ITS OWN SHAPE through the resolver: the object
    /// reads back healthy — resolvable, with every placement well-formed — exactly when it is
    /// `whole`. So no leg passes because the fault it was built around silently stopped being
    /// one, and leg 4's "genuinely healthy" object is PROVEN, never assumed.
    ///
    /// The segment group's epoch is the inode id, so two seeded objects never share a `seg:`
    /// range.
    async fn seed_segmented(&self, inode: InodeId, chunks: &[(ChunkId, DServerId)], whole: bool) {
        let group = SegmentGroup::new(NONCE, inode).unwrap();
        let mut segments = Vec::new();
        for (index, &(chunk, dserver)) in chunks.iter().enumerate() {
            let byte_offset = index as u64 * CHUNK_LEN;
            let index = index as u32;
            segments.push(SegmentRef {
                index,
                byte_offset,
                byte_len: CHUNK_LEN,
            });
            if !whole && index as usize == chunks.len() - 1 {
                continue;
            }
            let refs = vec![chunk_ref(chunk, dserver)];
            let record = SegmentRecord::new(refs, byte_offset).unwrap();
            let key = seg_key(&group, index).unwrap();
            self.put(key, encode(&record)).await;
            self.put_fragment(chunk, dserver).await;
        }
        let map = ChunkMap::Segmented(SegmentedMap::new(group, segments).unwrap());
        self.commit_root(inode, chunks.len() as u64 * CHUNK_LEN, map)
            .await;

        let root = self.record(inode).await;
        let live = resolve_chunk_map(&self.meta, &inode_key(inode), &root).await;
        let sound = |m: ResolvedChunkMap| m.chunks.iter().all(|c| c.checked_fragments().is_ok());
        let healthy = live.is_ok_and(|m| m.is_some_and(sound));
        assert_eq!(healthy, whole, "fixture: healthy iff whole");
    }

    /// One chunk in a **flat** map — the shape rebalance evacuates today, and must go on doing
    /// so beside every leg's segmented object.
    async fn seed_flat(&self, inode: InodeId, chunk: ChunkId, dserver: DServerId) {
        self.put_fragment(chunk, dserver).await;
        let map = ChunkMap::Flat(vec![chunk_ref(chunk, dserver)]);
        self.commit_root(inode, CHUNK_LEN, map).await;
    }

    async fn commit_root(&self, inode: InodeId, size: u64, chunk_map: ChunkMap) {
        let root = InodeRecord {
            size,
            chunk_map,
            state: InodeState::Committed,
            ..InodeRecord::new_empty()
        };
        self.put(inode_key(inode), encode(&root)).await;
    }

    async fn put(&self, key: Vec<u8>, value: impl Into<Bytes>) {
        let landed = self.meta.commit(WriteBatch::new().put(key, value)).await;
        assert_eq!(landed.unwrap(), CommitOutcome::Committed);
    }

    /// The fragment as the on-disk-format writer stamps it, on the D server its placement
    /// names — so `repair::fragment_intact` accepts it and an evacuation really commits.
    async fn put_fragment(&self, chunk: ChunkId, dserver: DServerId) {
        let header = FragmentHeader::new_v1(chunk, CHUNK_LEN);
        let bytes = Bytes::from(encode_fragment(&header, b"drain"));
        let store = &self.d[dserver as usize];
        let id = FragmentId { chunk, index: 0 };
        store.put_fragment(id, bytes).await.unwrap();
    }

    async fn holds(&self, dserver: DServerId, chunk: ChunkId) -> bool {
        let store = &self.d[dserver as usize];
        let id = FragmentId { chunk, index: 0 };
        store.get_fragment(id).await.unwrap().is_some()
    }

    async fn record(&self, inode: InodeId) -> InodeRecord {
        let bytes = self.meta.get(&inode_key(inode)).await.unwrap();
        decode(&bytes.expect("fixture: record present")).unwrap()
    }

    /// The flat fragment really moved: the committed placement names the target, bytes on it.
    async fn assert_flat_evacuated(&self) {
        let record = self.record(FLAT).await;
        let placement = &record.chunk_map.as_flat().unwrap()[0].placement;
        assert_eq!(placement, &vec![TARGET], "flat placement repointed");
        assert!(self.holds(TARGET, FLAT_CHUNK).await, "flat bytes moved");
    }
}

// ---- the five legs ----

/// **Leg 1 — a segmented object no longer ends the pass, and the flat work in the same store
/// still happens.** A healthy segmented object holding NOTHING on the draining server, beside a
/// flat chunk whose fragment sits on it. On the base the pass returns `Err` and that flat
/// fragment is never evacuated — one multipart object anywhere stops the whole drain.
#[tokio::test]
async fn a_segmented_object_no_longer_ends_the_pass_and_the_flat_work_still_happens() {
    let fx = Fixture::new().await;
    fx.seed_segmented(SEGMENTED, &OFF_DRAIN, true).await;
    fx.seed_flat(FLAT, FLAT_CHUNK, DRAINING).await;

    let (answer, _) = fx.pass().await;

    // Real work converged, so `Changed`: `Blocked` would withhold a drain nothing is owed on.
    assert_eq!(answered(answer), Reconciled::Changed, "pass converged");
    fx.assert_flat_evacuated().await;
}

/// **Leg 2 — an evacuation this pass may not perform is refused ONCE, mutates nothing, and the
/// pass does not certify.** Three chunks, two of them on the draining server: two evacuations
/// owed, one object to name.
#[tokio::test]
async fn an_owed_segmented_evacuation_is_refused_once_and_mutates_nothing() {
    let fx = Fixture::new().await;
    let owing = [(SEG[0], DRAINING), (SEG[1], DRAINING), (SEG[2], 2)];
    fx.seed_segmented(SEGMENTED, &owing, true).await;
    let before = fx.meta.scan(b"").await.unwrap();

    let (answer, logged) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Blocked, "drain withheld");
    for chunk in [SEG[0], SEG[1]] {
        assert!(fx.holds(DRAINING, chunk).await, "fragment stayed put");
    }
    // A refusal writes NOTHING — the segmented write path is #682's. The compare is over EVERY
    // key the store holds, byte for byte: every `seg:` record and the root record, whose
    // encoding carries its `version` — so an unchanged root generation is part of the equality.
    let after = fx.meta.scan(b"").await.unwrap();
    assert_eq!(after, before, "a refusal wrote something");
    // Once per OBJECT, not once per chunk — two evacuations are owed here.
    let refusals = logged.matches(r#""action":"refused-segmented""#).count();
    assert_eq!(refusals, 1, "one refusal per object: {logged}");
    // ...and attributed: a blocker an operator cannot name is a stall with no way out. The
    // counter is matched by its WHOLE emitted field, prefix and value — a bare name substring
    // still passes if the emitter loses the `monotonic_counter.` prefix the metric needs.
    let named = logged.contains(&format!(r#""inode":"inode:{SEGMENTED}""#));
    let reason = logged.contains(r#""reason":"segmented-chunk-map""#);
    let counted = logged.contains(r#""monotonic_counter.rebalance_refused_records":1"#);
    assert!(named && reason && counted, "unattributed: {logged}");
}

/// **Leg 3 — an unreadable committed object is named, the walk continues, and nothing
/// certifies.** Both ways a committed record can be unreadable, met FIRST in key order, with
/// healthy flat work behind them: (a) a root naming a `seg:` record that was never written —
/// the seeding proves the resolver really refuses it — and (b) a record whose own bytes will
/// not decode.
#[tokio::test]
async fn an_unreadable_committed_object_is_named_and_the_walk_continues() {
    let fx = Fixture::new().await;
    fx.seed_segmented(DAMAGED[0], &OFF_DRAIN, false).await;
    let garbage = b"not an inode record".to_vec();
    let undecodable = decode::<InodeRecord>(&garbage).is_err();
    assert!(undecodable, "fixture: these bytes must not decode");
    fx.put(inode_key(DAMAGED[1]), garbage).await;
    fx.seed_flat(FLAT, FLAT_CHUNK, DRAINING).await;

    let (answer, logged) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Blocked, "never certifies");
    fx.assert_flat_evacuated().await;
    for inode in DAMAGED {
        let named = logged.contains(&format!(r#""inode":"inode:{inode}""#));
        let action = r#""action":"unresolvable-chunk-map""#;
        assert!(named && logged.contains(action), "{inode}: {logged}");
    }
    // The COUNTER as well, by its own whole field and ONCE PER DAMAGED OBJECT. Asserting only
    // the action string leaves `rebalance_unresolvable_records` free to be dropped or renamed
    // with this leg still green — and that number is what an operator's alert is wired to, so
    // a silent one is a drain blocked on a record nobody is paged to repair.
    let counter = logged.matches(r#""monotonic_counter.rebalance_unresolvable_records":1"#);
    assert_eq!(counter.count(), DAMAGED.len(), "one per object: {logged}");
}

/// **Leg 4 — the containment is not over-broad.** REQUIRED: at #681 v7 an adversary replaced
/// this guard's body with a no-op and every other leg plus the whole `wyrd-custodian` suite
/// still passed, while the pass flipped `Satisfied`→`Blocked` over exactly this store — i.e. no
/// decommission would ever certify on a store holding one multipart object.
#[tokio::test]
async fn a_healthy_segmented_object_owing_nothing_still_certifies_the_drain() {
    let fx = Fixture::new().await;
    // Genuinely healthy (the seeding proves it), holding nothing on the draining server...
    fx.seed_segmented(SEGMENTED, &OFF_DRAIN, true).await;
    // ...over a store whose flat evacuation is already complete.
    fx.seed_flat(FLAT, FLAT_CHUNK, TARGET).await;

    let (answer, _) = fx.pass().await;

    assert_eq!(answered(answer), Reconciled::Satisfied, "drain certified");
}

/// **Leg 5 — a fault that is not one object's map still ends the pass.** Without this leg, the
/// over-broad fix that contains EVERY error — reading a whole-store outage as one unreadable
/// record — would pass legs 1–4.
#[tokio::test]
async fn a_store_fault_under_the_resolve_still_ends_the_pass() {
    let fx = Fixture::new().await;
    fx.seed_segmented(SEGMENTED, &OFF_DRAIN, true).await;
    // Only now: every `get` underneath the resolver fails with a plain store error.
    *fx.meta.get_fails.lock().unwrap() = true;

    let (answer, _) = fx.pass().await;

    let err = answer.expect_err("a store fault must end the pass");
    let carried = err.to_string().contains(STORE_FAULT);
    assert!(carried, "not the injected store fault: {err}");
}
