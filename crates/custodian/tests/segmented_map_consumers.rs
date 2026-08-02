//! Issue #650 (slice 3 of 6 of the #635 re-slicing, 0016 decision 7(e)): **GC and scrub
//! build their reference set through the shared resolver, and both answer honestly over an
//! incomplete one.**
//!
//! The fixture (in-memory trait stores, raw-record `seg:`/root seeding) is adapted from the
//! closed PR's GC/scrub legs, trimmed to what this slice's production code touches —
//! `crates/custodian/src/{gc,scrub,reconciliation}.rs`, plus the one non-regression the shared
//! reference build forces on `desired_state` (a drain that used to fail closed with an error
//! over an unreadable record must not start certifying `Satisfied`). Restore / reconstruction
//! / rebalance / backfill and their containment legs are #651's; they are not pulled forward,
//! and this file names none of their symbols, so #651 can add its own binding legs without
//! touching it.
//!
//! Every leg drives the real fenced control point
//! [`reconcile_step`](wyrd_custodian::reconcile_step), never a parallel entry — exactly the
//! shape an earlier attempt got wrong (a resolver wired into the read path alone while
//! `gc.rs` kept walking the inline chunk list, so a later GC pass deleted a live segmented
//! object's fragments). The observables are deliberately **positive** — a fragment still on
//! disk, an unrelated fragment genuinely reclaimed, a drain answering `Pending`, a repair
//! obligation actually queued, an audit line naming the record — never "no error was
//! raised", which a pass that did nothing at all also produces.
//!
//! **Why the outcome assertions spell out `!= Satisfied && != Changed` instead of naming the
//! answer:** the third outcome (`Reconciled::Blocked`) is added by the same patch as the fix,
//! so naming it here would make the pre-fix leg a COMPILE error and score a red as a pass.
//! The two base-visible variants are exhaustive before the fix, so excluding both is exactly
//! "the answer this tree cannot give" — assertion-red pre-fix, and no weaker than naming the
//! variant post-fix. The positive matches on `Reconciled::Blocked` ship in
//! `tests/{gc,scrub}.rs`, which the whole-tree gate runs.
//!
//! 1. `segmented_objects_fragments_survive_gc_and_scrub_and_a_drain_answers_pending` —
//!    criterion (1).
//! 2. `one_unreadable_committed_inode_blocks_every_certifying_answer_and_reclaims_nothing` —
//!    criterion (2), including the operator attribution on both audit seams and the
//!    drain-status surface's refusal to certify.
//! 3. `one_damaged_object_does_not_end_the_walk_the_rest_of_the_store_is_still_handled` and
//!    `a_structurally_unreadable_committed_root_is_contained_not_propagated` — criterion (3),
//!    over both ways a committed object can be unreadable (its `seg:` records, and its own
//!    root bytes).
//! 4. `a_genuine_store_fault_during_resolve_propagates_rather_than_being_absorbed` — the
//!    other half of criterion (3): a fault that is NOT one object's map still ends the pass.
//! 5. `a_blocked_loop_beside_a_converging_one_still_reports_the_blocked_answer` — the
//!    cross-loop rule: a step never claims more than its weakest loop.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
use wyrd_core::repair;
use wyrd_custodian::desired_state::*;
use wyrd_custodian::{
    mark_orphaned, reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone, GcContext,
    ReconcileError, Reconciled, ScrubContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    WriteBatch,
};

// ---- in-memory trait stores (the loops are proven over the seams, backend-agnostic) ----

/// A trivial in-memory metadata store.
///
/// `BTreeMap`, not the `HashMap` the sibling test doubles use: the reference build walks
/// `scan(b"inode:")` in whatever order the store answers in, and the continuation legs below
/// need the DAMAGED record to be met FIRST — otherwise "the healthy object was still
/// handled" could pass on an implementation that abandons the walk at the first blocker,
/// simply because the healthy object had already been handled by then.
#[derive(Default)]
struct MemMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    // The required paginated read (#634): a test double needs *a* body, not a backend's —
    // the dev-only testkit helper pages over this store's own `scan` (and therefore inherits
    // `SCAN_CAP`, which a backend may not). Mirrors `crates/custodian/tests/gc.rs:73-80`.
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
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

/// One D server's fragment bytes — a deliberately dumb `ChunkStore`. Content is never real
/// erasure-coded payload: presence-on-disk (never `delete_fragment`d) is what these legs
/// assert, not checksum validity — scrub's own corruption path is proven in
/// `crates/custodian/tests/scrub.rs`, and dummy content failing `fragment_intact` is exactly
/// what makes "scrub reached and enqueued this fragment" a usable positive observable below.
#[derive(Default)]
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

impl MemDServer {
    async fn put(&self, frag: FragmentId) {
        self.frags
            .lock()
            .unwrap()
            .insert(frag, Bytes::from_static(b"bytes"));
    }
}

#[async_trait]
impl ChunkStore for MemDServer {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
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

/// The injected fault's exact text, so the propagation leg can prove THIS error came back
/// rather than accepting any error at all (a pre-fix tree fails that read differently).
const STORE_FAULT: &str = "simulated store fault: segment range unreachable";

/// A [`MetadataStore`] that fails one configured `seg:` group's own range with a plain,
/// non-[`metadata::ChunkMapError`] fault — a genuine backend outage while a generation's
/// segments are being read, as opposed to a structural anomaly the resolver itself describes
/// and a maintenance pass recovers by downcast. Everything else delegates unchanged.
struct PoisonedMeta {
    inner: MemMeta,
    poison_prefix: Vec<u8>,
}

#[async_trait]
impl MetadataStore for PoisonedMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        if prefix.starts_with(self.poison_prefix.as_slice()) {
            return Err(Box::new(std::io::Error::other(STORE_FAULT)));
        }
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        self.inner.commit(batch).await
    }
}

/// A [`MetadataStore`] that rewrites ONE root under a pass that is already reading it: the
/// `scan` a maintenance pass takes its snapshot from still answers the seeded record, while
/// each **re-read of that root** — the settle-read that decides whether an anomaly is a
/// concurrent retirement, and the re-read a restart begins with — is answered by the next
/// scripted value, as a concurrent writer would have left it.
///
/// The race is *driven*, not simulated: the scripted bytes are committed into the inner
/// store as they are handed out, so every later read sees exactly what the writer left, and
/// the production resolver ([`metadata::resolve_chunk_map`]) does all the deciding.
struct RewrittenRoot {
    inner: MemMeta,
    root_key: Vec<u8>,
    rewrites: Mutex<VecDeque<Bytes>>,
}

impl RewrittenRoot {
    fn new(inner: MemMeta, inode: InodeId, rewrites: impl IntoIterator<Item = Bytes>) -> Self {
        Self {
            inner,
            root_key: metadata::inode_key(inode),
            rewrites: Mutex::new(rewrites.into_iter().collect()),
        }
    }

    /// How many scripted rewrites are still unspent — so a leg can prove the race it was
    /// built around actually happened rather than passing because nothing ever re-read the
    /// root.
    fn unspent(&self) -> usize {
        self.rewrites.lock().unwrap().len()
    }
}

#[async_trait]
impl MetadataStore for RewrittenRoot {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if key == self.root_key.as_slice() {
            let next = self.rewrites.lock().unwrap().pop_front();
            if let Some(bytes) = next {
                self.inner
                    .commit(WriteBatch::new().put(key.to_vec(), bytes.clone()))
                    .await?;
                return Ok(Some(bytes));
            }
        }
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        self.inner.commit(batch).await
    }
}

// ---- audit capture (the operator-facing half of the contract) ----

/// A `MakeWriter` collecting what the subscriber emits, so a leg asserts on the record the
/// pass actually produced rather than assuming one exists. The proven in-tree pattern
/// (`crates/core/tests/read_repair.rs:643-697`, `crates/server/tests/custodian_day_one.rs`).
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

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
/// `Interest::never` under the parallel test harness. `tracing` caches each callsite's
/// interest in process-global state the first time it is hit, so a sibling test in this
/// binary that hits `emit_unresolvable` with no subscriber installed could otherwise disable
/// the callsite for the whole process and leave the capture below empty (issue #214; the
/// same guard as `crates/custodian/tests/rebalance.rs`'s `enable_metric_callsites`).
///
/// Called at the top of EVERY test here — all of them drive passes that fire those
/// callsites, so whichever the harness schedules first has to be the one that installs the
/// default, not merely the one leg that reads the capture back.
fn enable_audit_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

fn capturing_dispatch(capture: Capture) -> tracing::Dispatch {
    tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().json().with_writer(capture)),
    )
}

/// Every DISTINCT record name a pass's audit trail attributed, read back out of the JSON
/// the subscriber wrote (`"inode":"<name>"`).
///
/// A set, and read rather than assumed: what makes an attribution usable is that two damaged
/// records arrive under two names. A renderer that is not injective — `from_utf8_lossy` maps
/// every invalid byte to one replacement character — emits two lines that an operator, and
/// this set, cannot tell apart.
fn attributed_objects(logged: &str) -> BTreeSet<String> {
    logged
        .split(r#""inode":""#)
        .skip(1)
        .filter_map(|rest| rest.split('"').next().map(str::to_owned))
        .collect()
}

/// Assert that one pass's audit trail attributes the blocker: the right seam, the right
/// action, and — the part an operator actually needs — the NAME of the record to repair.
fn assert_attributes_blocker(logged: &str, seam: &str, object: &str) {
    assert!(
        logged.contains(&format!(r#""target":"{seam}""#)),
        "the blocker must be reported on {seam} so a collector can select on it. got: {logged}"
    );
    assert!(
        logged.contains(r#""action":"unresolvable-chunk-map""#),
        "the audit line must classify the blocker as an unreadable chunk map. got: {logged}"
    );
    assert!(
        logged.contains(&format!(r#""inode":"{object}""#)),
        "the audit line must NAME the record to repair — a refusal an operator cannot \
         attribute is a stall with no way out. got: {logged}"
    );
}

// ---- fixture: raw-record `seg:` + root seeding (this slice lands no producer of segmented
// maps; #653 owns the real staged-publication committer) ----

const GRACE: u64 = 1_000;

/// `inode:1` sorts BEFORE `inode:2`, so in the mixed-store legs the DAMAGED object is always
/// the one the reference build meets first.
const DAMAGED_INODE: InodeId = 1;
const HEALTHY_INODE: InodeId = 2;

/// The healthy object's segment-group nonce (32 lowercase hex characters, `0016:354`) and
/// the fence epoch its segments are scoped by.
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const EPOCH: u64 = 7;
/// The damaged object's group — its own, so nothing about it is inside the healthy object's
/// bounded `seg:` range.
const DAMAGED_NONCE: &str = "fedcba9876543210fedcba9876543210";
const DAMAGED_EPOCH: u64 = 11;

/// Each fixture chunk is one fragment (`EcScheme::None`) placed on exactly one D server — the
/// smallest shape that still puts a segmented object's fragments on more than one server, so
/// a drain of ONE of them is a meaningful `Pending`.
const CHUNK_LEN: u64 = 5;

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

async fn commit(meta: &MemMeta, batch: WriteBatch) {
    assert_eq!(meta.commit(batch).await.unwrap(), CommitOutcome::Committed);
}

async fn elect(coord: &MemCoordination, zone_name: &str) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, zone_name).await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

/// Seed a committed **segmented** root at `inode` naming `chunks.len()` segments (one chunk
/// each, `(chunk id, placed dserver)`), but WRITE only the first `written` of their `seg:`
/// records. `written == chunks.len()` is the healthy shape; a smaller `written` is the real
/// gap this slice's containment rule exists for — a segment the root's own table names, on a
/// generation it still names, that genuinely never got written
/// (`metadata::ChunkMapError::SegmentAbsent`, as surfaced by `metadata::resolve_chunk_map`).
///
/// Built with the real validating constructors rather than hand-typed JSON, so a fixture typo
/// cannot silently change WHICH rule the leg exercises; the bytes that land are the same ones
/// `metadata::encode` / `metadata::seg_key` put in a store, and the seeding is a raw
/// `WriteBatch` put — never a publish path, since no producer of segmented maps exists yet.
async fn seed_segmented(
    meta: &MemMeta,
    inode: InodeId,
    group: &SegmentGroup,
    chunks: &[(ChunkId, DServerId)],
    written: usize,
) {
    let mut segments = Vec::new();
    let mut offset = 0u64;
    for (index, &(chunk_id, dserver)) in chunks.iter().enumerate() {
        segments.push(SegmentRef {
            index: index as u32,
            byte_offset: offset,
            byte_len: CHUNK_LEN,
        });
        if index < written {
            let chunk_ref = ChunkRef {
                id: chunk_id,
                scheme: EcScheme::None,
                len: CHUNK_LEN,
                placement: vec![dserver],
            };
            let record = SegmentRecord::new(vec![chunk_ref], offset).unwrap();
            let key = metadata::seg_key(group, index as u32).unwrap();
            commit(meta, WriteBatch::new().put(key, metadata::encode(&record))).await;
        }
        offset += CHUNK_LEN;
    }
    let map = SegmentedMap::new(group.clone(), segments).unwrap();
    let root = InodeRecord {
        size: offset,
        chunk_map: ChunkMap::Segmented(map),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    commit(
        meta,
        WriteBatch::new().put(metadata::inode_key(inode), metadata::encode(&root)),
    )
    .await;
}

/// Seed the damaged object used by the containment legs: two segments named, only the first
/// ever written, plus its readable fragment on `d0`. Returns the chunk that fragment belongs
/// to. Asserts the fixture really is unreadable, so a leg can never pass because the fault it
/// was built around silently stopped being one.
async fn seed_damaged(meta: &MemMeta, d0: &MemDServer, a: ChunkId, b: ChunkId) {
    let group = SegmentGroup::new(DAMAGED_NONCE, DAMAGED_EPOCH).unwrap();
    seed_segmented(meta, DAMAGED_INODE, &group, &[(a, 0), (b, 0)], 1).await;
    d0.put(frag(a, 0)).await;

    let root_key = metadata::inode_key(DAMAGED_INODE);
    let root: InodeRecord = metadata::decode(&meta.get(&root_key).await.unwrap().unwrap()).unwrap();
    assert!(
        metadata::resolve_chunk_map(meta, &root_key, &root)
            .await
            .is_err(),
        "fixture: the seeded root's map must genuinely fail to resolve"
    );
}

/// An expired pending lease over `chunk`: real, genuinely collectable garbage, so "nothing
/// was reclaimed" can be told apart from "the pass did nothing at all".
async fn seed_expired_lease(meta: &MemMeta, d0: &MemDServer, chunk: ChunkId) {
    d0.put(frag(chunk, 0)).await;
    metadata::put_pending(
        meta,
        chunk,
        &metadata::PendingEntry {
            lease_expiry_millis: 10,
        },
    )
    .await
    .unwrap();
}

// ---- criterion (1): a segmented object's fragments survive GC + scrub, and a drain of a
// server holding one answers Pending ----

#[tokio::test]
async fn segmented_objects_fragments_survive_gc_and_scrub_and_a_drain_answers_pending() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let d1 = MemDServer::default();

    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let chunk_a: ChunkId = 0xA1_00;
    let chunk_b: ChunkId = 0xA2_00;
    seed_segmented(
        &meta,
        HEALTHY_INODE,
        &group,
        &[(chunk_a, 0), (chunk_b, 1)],
        2,
    )
    .await;
    d0.put(frag(chunk_a, 0)).await;
    d1.put(frag(chunk_b, 0)).await;

    // DELETION EVIDENCE on the segmented object's own fragments — a grace record that
    // lapsed long before this pass's clock, so each of them is one unreferenced verdict away
    // from `delete_fragment`. Without it, "still on disk afterwards" says nothing about the
    // reference set: GC deletes only what it has a deadline for, so the assertions below
    // would hold just as well on a build that never resolved the object at all. With it, a
    // build whose reference set misses a segmented object's chunks destroys both fragments
    // on this very pass, and the only thing that saves them is the safety gate finding them
    // in the set.
    mark_orphaned(&meta, 0, frag(chunk_a, 0), 0).await.unwrap();
    mark_orphaned(&meta, 1, frag(chunk_b, 0), 0).await.unwrap();

    // Genuinely collectable garbage, unrelated to the segmented object: it MUST be gone
    // after the pass for the fragments-survive assertions to mean anything — otherwise a GC
    // pass that reclaimed nothing because it did nothing would pass this leg by omission.
    let lease_chunk: ChunkId = 0xE1_00;
    seed_expired_lease(&meta, &d0, lease_chunk).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let fleet: [(DServerId, &dyn ChunkStore); 2] = [(0, &d0), (1, &d1)];
    let gc_ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let scrub_ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    // Past the grace window: nothing here is being saved by a window that has not elapsed.
    reconcile_step(
        &zone,
        &custodian,
        Some(&gc_ctx),
        Some(&scrub_ctx),
        None,
        None,
        GRACE + 1_000,
    )
    .await
    .unwrap();

    // The pass ran for real.
    assert!(
        d0.get_fragment(frag(lease_chunk, 0))
            .await
            .unwrap()
            .is_none(),
        "the pass must have reclaimed the unrelated expired-lease fragment for real"
    );

    // Positive observable (1): every fragment the segmented object owns is still on the D
    // server that holds it — each one past its grace window and reclaimable on the spot if
    // the reference build had not seen it.
    assert!(
        d0.get_fragment(frag(chunk_a, 0)).await.unwrap().is_some(),
        "segment 0's chunk must survive GC"
    );
    assert!(
        d1.get_fragment(frag(chunk_b, 0)).await.unwrap().is_some(),
        "segment 1's chunk must survive GC"
    );

    // Positive observable (2): a drain of a server holding a segmented fragment answers
    // Pending, not (wrongly) Satisfied — the leg that catches a build which decodes the
    // segmented shape but never reads the `seg:` range, for which the object owns nothing.
    set_lifecycle(&meta, 0, DServerLifecycle::Draining)
        .await
        .unwrap();
    assert_eq!(
        reconciliation_status(&meta, 0).await.unwrap(),
        ReconciliationStatus::Pending,
        "d0 genuinely holds a referenced segmented fragment; the drain must not certify \
         satisfied"
    );

    // ...and scrub verified those fragments rather than skipping them: dummy content fails
    // `fragment_intact`, so a fetched fragment becomes a durable repair obligation. A
    // reference set that never saw them queues nothing.
    let queued = repair::queued_repairs(&meta).await.unwrap();
    assert!(
        queued.contains(&chunk_a) && queued.contains(&chunk_b),
        "scrub must have fetched and verified the segmented object's fragments: {queued:?}"
    );
}

// ---- criterion (2): one unreadable committed inode blocks every certifying answer and
// reclaims nothing ----

#[tokio::test]
async fn one_unreadable_committed_inode_blocks_every_certifying_answer_and_reclaims_nothing() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    let chunk_a: ChunkId = 0xD1_00;
    let chunk_b: ChunkId = 0xD2_00;
    seed_damaged(&meta, &d0, chunk_a, chunk_b).await;

    // Unrelated, genuinely collectable garbage. The only thing that can keep it is the
    // fail-safe containment rule — an incomplete reference set authorizes NO reclamation
    // anywhere in the fleet, not merely around the object it cannot read: no fragment can be
    // shown not to be one of that object's own, unknown chunks.
    let lease_chunk: ChunkId = 0xD3_00;
    seed_expired_lease(&meta, &d0, lease_chunk).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let gc_ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let scrub_ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    // GC alone: `Ok(_)`, and neither of the two answers that CERTIFY something. (`Satisfied`
    // claims the store converged; `Changed` claims the pass converged it. Both are claims
    // about a picture with a known hole in it.)
    let gc_audit = Capture::default();
    let gc_outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_ctx),
        None,
        None,
        None,
        GRACE + 1_000,
    )
    .with_subscriber(capturing_dispatch(gc_audit.clone()))
    .await
    .expect("one unreadable object is contained, not an error that ends the pass");
    assert!(
        gc_outcome != Reconciled::Satisfied && gc_outcome != Reconciled::Changed,
        "GC must not report convergence over an incomplete reference set; got {gc_outcome:?}"
    );
    assert_attributes_blocker(
        &gc_audit.contents(),
        "wyrd.custodian.gc.audit",
        "inode:1", // DAMAGED_INODE, as the store spells its key
    );

    // Scrub alone: the identical condition, the identical answer, the same attribution.
    let scrub_audit = Capture::default();
    let scrub_outcome = reconcile_step(&zone, &custodian, None, Some(&scrub_ctx), None, None, 0)
        .with_subscriber(capturing_dispatch(scrub_audit.clone()))
        .await
        .expect("one unreadable object is contained, not an error that ends the pass");
    assert!(
        scrub_outcome != Reconciled::Satisfied && scrub_outcome != Reconciled::Changed,
        "scrub must not certify a store it could only partly read; got {scrub_outcome:?}"
    );
    assert_eq!(
        gc_outcome, scrub_outcome,
        "two passes reading ONE set must give the SAME answer about it — a disagreement is a \
         state an operator cannot resolve from outside"
    );
    assert_attributes_blocker(
        &scrub_audit.contents(),
        "wyrd.custodian.scrub.audit",
        "inode:1",
    );

    // Nothing reclaimed: neither the damaged object's own readable fragment nor the
    // unrelated, otherwise-collectable garbage.
    assert!(
        d0.get_fragment(frag(chunk_a, 0)).await.unwrap().is_some(),
        "the damaged object's own readable fragment must not be reclaimed"
    );
    assert!(
        d0.get_fragment(frag(lease_chunk, 0))
            .await
            .unwrap()
            .is_some(),
        "an unrelated, otherwise-collectable fragment must not be reclaimed while the \
         reference set is incomplete — the containment is fleet-wide, not scoped to the \
         object that has the fault"
    );

    // The third reader of this set — the drain-status surface — must not certify either.
    // "This server holds nothing referenced" over an incomplete set is the same claim as the
    // reclaim, in the form an operator acts on by decommissioning the box. Before this slice an
    // unreadable committed record made this query FAIL outright (`referenced_fragments`
    // propagated the decode error / the segmented-map refusal), so the binding property here is
    // that routing the build through the resolver does not turn that fail-closed answer into a
    // certification: it stays the existing, non-certifying `Pending`.
    //
    // Naming the blocker IN THE ANSWER (as `PendingMalformed` names chunk ids) is the drain
    // surface's own attribution work and belongs to the slice that owns `desired_state` (#651,
    // deferred — see `desired_state::reconciliation_status`); the record is already named on
    // this pass's audit seam, asserted above.
    set_lifecycle(&meta, 0, DServerLifecycle::Draining)
        .await
        .unwrap();
    let status = reconciliation_status(&meta, 0).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::Pending,
        "a drain over an incomplete reference set must never be certified `Satisfied` — that \
         tells an operator to decommission a server an unreadable object may still own bytes \
         on; got {status:?}"
    );
}

// ---- criterion (3): one damaged object does not end the walk ----

#[tokio::test]
async fn one_damaged_object_does_not_end_the_walk_the_rest_of_the_store_is_still_handled() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    let d1 = MemDServer::default();

    // The damaged object FIRST (`inode:1`), so the healthy one below is reached only if the
    // build walks past it — the store answers `scan` in key order.
    let damaged_a: ChunkId = 0xD1_01;
    let damaged_b: ChunkId = 0xD2_01;
    seed_damaged(&meta, &d0, damaged_a, damaged_b).await;

    // The healthy object, in its own group, in the SAME store and the same pass.
    let healthy_group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let healthy_a: ChunkId = 0xC1_00;
    let healthy_b: ChunkId = 0xC2_00;
    seed_segmented(
        &meta,
        HEALTHY_INODE,
        &healthy_group,
        &[(healthy_a, 0), (healthy_b, 1)],
        2,
    )
    .await;
    d0.put(frag(healthy_a, 0)).await;
    d1.put(frag(healthy_b, 0)).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let fleet: [(DServerId, &dyn ChunkStore); 2] = [(0, &d0), (1, &d1)];
    let gc_ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let scrub_ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    // The walk must COMPLETE — `Ok`, never propagated as an `Err` for the whole store
    // because ONE object's map could not be read.
    let outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_ctx),
        Some(&scrub_ctx),
        None,
        None,
        GRACE + 1_000,
    )
    .await
    .expect("one damaged object must not end the walk for the whole store");
    assert!(
        outcome != Reconciled::Satisfied && outcome != Reconciled::Changed,
        "the incomplete set must not certify; got {outcome:?}"
    );

    // PROTECTED: the healthy object's fragments are still on disk.
    assert!(
        d0.get_fragment(frag(healthy_a, 0)).await.unwrap().is_some(),
        "the healthy object's fragment must still be protected"
    );
    assert!(
        d1.get_fragment(frag(healthy_b, 0)).await.unwrap().is_some(),
        "the healthy object's fragment must still be protected"
    );

    // VERIFIED: scrub reached the healthy object — met SECOND, after the damaged one — and
    // enqueued its (dummy-content, checksum-failing) fragments for repair. That is the proof
    // the reference build iterated PAST the blocker rather than abandoning the scan at it.
    let queued = repair::queued_repairs(&meta).await.unwrap();
    assert!(
        queued.contains(&healthy_a) && queued.contains(&healthy_b),
        "the healthy object must still be verified past the damaged one: {queued:?}"
    );
}

/// The other shape of "unreadable": the root's OWN bytes will not decode. A `segment_count`
/// that disagrees with the table it carries is rejected at decode (structural invariants
/// surface as errors, never as values — ADR-0045), so this record never reaches the resolver
/// at all. It must be contained exactly the same way: an undecodable record is one object's
/// fault, and ending the walk over it costs every healthy object in the store its protection
/// and its verification.
#[tokio::test]
async fn a_structurally_unreadable_committed_root_is_contained_not_propagated() {
    enable_audit_callsites();
    // Raw stored bytes, necessarily hand-written: the validating constructors exist
    // precisely to make this value unconstructible in process.
    const MISMATCHED_ROOT: &[u8] = br#"{"size":12,"chunk_map":{"group":{"nonce":"fedcba9876543210fedcba9876543210","epoch":11},"segment_count":3,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;

    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    commit(
        &meta,
        WriteBatch::new().put(metadata::inode_key(DAMAGED_INODE), MISMATCHED_ROOT),
    )
    .await;
    assert!(
        metadata::decode::<InodeRecord>(MISMATCHED_ROOT).is_err(),
        "fixture: the seeded root must genuinely fail to decode"
    );

    // A healthy flat object beside it, met SECOND, whose fragment is genuinely collectable
    // garbage on any pass that certifies: it stays put, because an incomplete set authorizes
    // nothing.
    let healthy_chunk: ChunkId = 0xB1_00;
    let record = InodeRecord {
        size: CHUNK_LEN,
        chunk_map: vec![ChunkRef {
            id: healthy_chunk,
            scheme: EcScheme::None,
            len: CHUNK_LEN,
            placement: vec![0],
        }]
        .into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    commit(
        &meta,
        WriteBatch::new().put(
            metadata::inode_key(HEALTHY_INODE),
            metadata::encode(&record),
        ),
    )
    .await;
    d0.put(frag(healthy_chunk, 0)).await;
    let lease_chunk: ChunkId = 0xB2_00;
    seed_expired_lease(&meta, &d0, lease_chunk).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let gc_ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let scrub_ctx = ScrubContext {
        meta: &meta,
        fleet: &fleet,
    };

    let outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_ctx),
        Some(&scrub_ctx),
        None,
        None,
        GRACE + 1_000,
    )
    .await
    .expect("an undecodable record is one object's fault; it must not end the whole pass");
    assert!(
        outcome != Reconciled::Satisfied && outcome != Reconciled::Changed,
        "a record the pass could not read must block certification; got {outcome:?}"
    );
    assert!(
        d0.get_fragment(frag(lease_chunk, 0))
            .await
            .unwrap()
            .is_some(),
        "nothing is reclaimed while the reference set is incomplete"
    );
    // The walk continued past the undecodable record: the healthy object beside it was still
    // fetched and verified (its dummy content fails `fragment_intact`).
    let queued = repair::queued_repairs(&meta).await.unwrap();
    assert!(
        queued.contains(&healthy_chunk),
        "the healthy object must still be verified past the undecodable record: {queued:?}"
    );
}

/// The third shape of "unreadable", and the one only a **race** produces: the root was fine
/// when the pass read it and is rewritten, by a concurrent writer, into bytes this build
/// cannot parse before the resolver re-reads it. The resolver re-reads a root twice — once to
/// settle whether an anomaly is a concurrent retirement, once when a supersede makes it
/// restart onto the live generation — and BOTH must attribute an unparsable root to that
/// object. A decode fault that arrives untyped is indistinguishable from a backend outage at
/// the maintenance pass, which reads it as the whole store's fault and ends the walk: one
/// rewritten record would then cost every healthy object in the store its protection.
///
/// Driven, not simulated: the rewrite lands through the store the production resolver reads,
/// and the leg asserts the scripted rewrites were actually spent — a re-read that never
/// happened would leave the object resolving perfectly and prove nothing.
#[tokio::test]
async fn a_root_rewritten_unreadable_under_the_resolve_is_contained_not_propagated() {
    enable_audit_callsites();

    // Arm 1: the settle re-read (`metadata::root_dropped`) meets the unparsable bytes.
    assert_a_rewritten_root_is_contained(vec![Bytes::from_static(b"{\"size\":")], "settle-read")
        .await;

    // Arm 2: the settle re-read meets a LIVE successor generation — a flat map, so the
    // generation being read was superseded — and the restart onto that live root then meets
    // the unparsable bytes (`metadata::resolve_current_chunk_map`). Two writes, the second
    // of which left a record this build cannot parse.
    let successor = metadata::encode(&InodeRecord {
        size: CHUNK_LEN,
        chunk_map: vec![ChunkRef {
            id: 0xA7_00,
            scheme: EcScheme::None,
            len: CHUNK_LEN,
            placement: vec![0],
        }]
        .into(),
        state: InodeState::Committed,
        version: 2,
        ..Default::default()
    });
    assert_a_rewritten_root_is_contained(
        vec![successor, Bytes::from_static(b"{\"size\":")],
        "restart-read",
    )
    .await;
}

/// One arm of the rewritten-root race: seed a readable segmented object, script `rewrites`
/// onto its root's re-reads, and require the pass to contain it — complete, refuse to
/// certify, name the record, and reclaim nothing.
async fn assert_a_rewritten_root_is_contained(rewrites: Vec<Bytes>, arm: &str) {
    let inner = MemMeta::default();
    let d0 = MemDServer::default();
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let chunk: ChunkId = 0xA5_00;
    // Seeded WHOLE: every `seg:` record written, so the object resolves perfectly and the
    // only thing that can stop it is the rewrite this leg scripts.
    seed_segmented(&inner, DAMAGED_INODE, &group, &[(chunk, 0)], 1).await;
    d0.put(frag(chunk, 0)).await;
    let lease_chunk: ChunkId = 0xA6_00;
    seed_expired_lease(&inner, &d0, lease_chunk).await;

    let meta = RewrittenRoot::new(inner, DAMAGED_INODE, rewrites);
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let gc_ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let audit = Capture::default();
    let outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_ctx),
        None,
        None,
        None,
        GRACE + 1_000,
    )
    .with_subscriber(capturing_dispatch(audit.clone()))
    .await
    .unwrap_or_else(|e| {
        panic!("[{arm}] a root rewritten under the resolve is ONE object's fault: {e:?}")
    });
    assert_eq!(
        meta.unspent(),
        0,
        "fixture [{arm}]: the resolver must have re-read the root — an unspent rewrite means \
         the race never happened and the leg proved nothing"
    );
    assert!(
        outcome != Reconciled::Satisfied && outcome != Reconciled::Changed,
        "[{arm}] a record the pass could not read must block certification; got {outcome:?}"
    );
    let logged = audit.contents();
    assert_attributes_blocker(&logged, "wyrd.custodian.gc.audit", "inode:1");
    assert!(
        logged.contains("root record could not be decoded"),
        "[{arm}] the attribution must say what stopped the read — an unparsable ROOT, not \
         some other anomaly this leg did not build. got: {logged}"
    );
    assert!(
        d0.get_fragment(frag(lease_chunk, 0))
            .await
            .unwrap()
            .is_some(),
        "[{arm}] nothing is reclaimed while the reference set is incomplete"
    );
}

/// **Two blockers, two names.** A record's `inode:` key is bytes, and a key that is not UTF-8
/// is still a record an operator has to go and repair. Naming it with a lossy rendering maps
/// every invalid byte onto one replacement character, so two damaged records collapse into
/// one entry in the blocker set — one of them silently unreported, holding every reclamation
/// in the fleet with nothing left pointing at it. That is the silent skip the whole
/// containment rule exists to prevent, so the name has to be injective.
///
/// Asserted as a COUNT OF DISTINCT names read back out of the pass's own audit trail, never as
/// a spelling: the property is "two records are two names", not any particular escape.
#[tokio::test]
async fn two_blockers_whose_keys_are_not_utf8_are_each_attributed_under_their_own_name() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // Three records under the `inode:` prefix, none of whose values decode, whose keys are
    // pairwise distinct but collide under a naming that is not injective:
    //   * the first two differ only in a byte no UTF-8 rendering can distinguish;
    //   * the third is the ESCAPE's own collision — its key carries a literal backslash, so a
    //     renderer that escapes the invalid byte but passes `\` through unchanged spells it
    //     exactly as it spells the second, and one of the two vanishes.
    for key in [
        b"inode:\xfe".as_slice(),
        b"inode:\xff".as_slice(),
        br"inode:\xff".as_slice(),
    ] {
        commit(
            &meta,
            WriteBatch::new().put(key.to_vec(), Bytes::from_static(b"not a record")),
        )
        .await;
    }

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let gc_ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let audit = Capture::default();
    reconcile_step(
        &zone,
        &custodian,
        Some(&gc_ctx),
        None,
        None,
        None,
        GRACE + 1_000,
    )
    .with_subscriber(capturing_dispatch(audit.clone()))
    .await
    .expect("records that will not decode are contained, not an error that ends the pass");
    let logged = audit.contents();
    assert_eq!(
        attributed_objects(&logged).len(),
        3,
        "three damaged records must reach the operator as THREE names — a repair guided by a \
         name that stands for two records fixes one and leaves the other blocking the fleet. \
         got: {logged}"
    );

    // ...and the surface an operator acts on by decommissioning a box still refuses to certify
    // while any of them is unreadable (the attributed answer is #651's — see
    // `desired_state::reconciliation_status`).
    set_lifecycle(&meta, 0, DServerLifecycle::Draining)
        .await
        .unwrap();
    let status = reconciliation_status(&meta, 0).await.unwrap();
    assert_eq!(
        status,
        ReconciliationStatus::Pending,
        "a drain must not be certified while a committed record cannot be read; got {status:?}"
    );
}

// ---- the other half of criterion (3): a genuine store fault still propagates ----

#[tokio::test]
async fn a_genuine_store_fault_during_resolve_propagates_rather_than_being_absorbed() {
    enable_audit_callsites();
    let inner = MemMeta::default();
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let chunk: ChunkId = 0xF1_00;
    seed_segmented(&inner, HEALTHY_INODE, &group, &[(chunk, 0)], 1).await;

    let poison_prefix = metadata::seg_range_prefix(&group);
    let d0 = MemDServer::default();
    d0.put(frag(chunk, 0)).await;
    let meta = PoisonedMeta {
        inner,
        poison_prefix,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &d0)];
    let gc_ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_ctx),
        None,
        None,
        None,
        GRACE + 1_000,
    )
    .await;
    // Not merely "an error": THE injected one. A pass that refused the segmented shape
    // outright would also fail this leg with an error of its own, and that must not read as
    // this property holding.
    let Err(ReconcileError::Store(err)) = outcome else {
        panic!("a store fault under the resolve must propagate as a store error: {outcome:?}")
    };
    let reported = err.to_string();
    assert!(
        reported.contains(STORE_FAULT),
        "the fault that comes back must be the injected store fault — a fault that is not \
         one object's map must NOT be folded into 'this object is unreadable'; got {reported}"
    );
}

// ---- the cross-loop rule: a step never claims more than its weakest loop ----

/// Two loops, two stores, two different answers: GC over a store it cannot fully read, scrub
/// over a store where it genuinely converges something (a corrupt fragment enqueued for
/// repair). The step must report the BLOCKED answer, in both loop orders — the converging
/// loop's work is durable in the store either way, while the refusal is the only thing that
/// tells the caller its picture has a hole in it.
///
/// Both orders combine a refusal with a loop that genuinely **converged** something, not one
/// that merely found nothing to do: the converging store carries real collectable garbage for
/// each order, and the leg checks the converging context's own answer is `Changed` before
/// pairing it. Otherwise the reverse order would only ever prove `Satisfied` + refusal, and a
/// precedence rule that let a *conversion* outrank a refusal would sail through it.
#[tokio::test]
async fn a_blocked_loop_beside_a_converging_one_still_reports_the_blocked_answer() {
    enable_audit_callsites();
    let blocked_meta = MemMeta::default();
    let blocked_d0 = MemDServer::default();
    seed_damaged(&blocked_meta, &blocked_d0, 0x91_00, 0x92_00).await;

    // The converging store: a committed reference whose placed fragment is dummy content,
    // so scrub fetches it, fails `fragment_intact`, and enqueues a repair — `Changed`.
    let converging_meta = MemMeta::default();
    let converging_d0 = MemDServer::default();
    let live: ChunkId = 0x93_00;
    let record = InodeRecord {
        size: CHUNK_LEN,
        chunk_map: vec![ChunkRef {
            id: live,
            scheme: EcScheme::None,
            len: CHUNK_LEN,
            placement: vec![0],
        }]
        .into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    commit(
        &converging_meta,
        WriteBatch::new().put(
            metadata::inode_key(HEALTHY_INODE),
            metadata::encode(&record),
        ),
    )
    .await;
    converging_d0.put(frag(live, 0)).await;

    // Two independent pieces of genuinely collectable garbage in the converging store, seeded
    // ONE AT A TIME: the first is spent by the fixture probe below (proving that context really
    // answers `Changed` on its own), and the second is seeded only AFTER that probe has run, so
    // the two-loop step has garbage of its own left to converge. Seeding both up front is the
    // trap this shape exists to avoid — the probe pass sweeps the whole fleet, so it would
    // reclaim both, leave the two-loop step with nothing to do, and turn the final "the garbage
    // is gone" assertion into a fact the PROBE established.
    let probe_garbage: ChunkId = 0x94_00;
    let order_b_garbage: ChunkId = 0x95_00;
    seed_expired_lease(&converging_meta, &converging_d0, probe_garbage).await;

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-1").await;
    let blocked_fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &blocked_d0)];
    let converging_fleet: [(DServerId, &dyn ChunkStore); 1] = [(0, &converging_d0)];

    // Order A — the blocked loop first: a later converging loop must not overwrite it.
    let gc_blocked = GcContext {
        meta: &blocked_meta,
        fleet: &blocked_fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let scrub_converging = ScrubContext {
        meta: &converging_meta,
        fleet: &converging_fleet,
    };
    let outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_blocked),
        Some(&scrub_converging),
        None,
        None,
        GRACE + 1_000,
    )
    .await
    .unwrap();
    assert!(
        outcome != Reconciled::Satisfied && outcome != Reconciled::Changed,
        "a step with one blocked loop cannot report convergence, whatever its other loops \
         did; got {outcome:?}"
    );
    assert_eq!(
        repair::queued_repairs(&converging_meta).await.unwrap(),
        vec![live],
        "the converging loop's work is durable either way — the blocked answer reports the \
         hole, it does not undo the other loop"
    );

    // Order B — the converging loop first: the blocked loop must still lower the answer.
    let gc_converging = GcContext {
        meta: &converging_meta,
        fleet: &converging_fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };
    let scrub_blocked = ScrubContext {
        meta: &blocked_meta,
        fleet: &blocked_fleet,
    };

    // FIXTURE CHECK, spending the first piece of garbage: this GC context, on its own, really
    // does answer `Changed`. Without it the order below would be `Satisfied` + refusal, which
    // says nothing about whether a conversion can outrank a refusal.
    let gc_alone = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_converging),
        None,
        None,
        None,
        GRACE + 1_000,
    )
    .await
    .unwrap();
    assert_eq!(
        gc_alone,
        Reconciled::Changed,
        "fixture: the converging GC context must genuinely converge something, or this leg \
         proves only that a SATISFIED loop yields to a refusal"
    );

    // NOW the garbage the two-loop step must converge, seeded after the probe spent its own and
    // asserted present here — so its absence afterwards can only be that step's doing. Without
    // this the leg would test `Satisfied` + refusal and a precedence rule that let a conversion
    // outrank a refusal would sail through it.
    seed_expired_lease(&converging_meta, &converging_d0, order_b_garbage).await;
    assert!(
        converging_d0
            .get_fragment(frag(order_b_garbage, 0))
            .await
            .unwrap()
            .is_some(),
        "fixture: the two-loop step's own garbage must be on disk BEFORE that step runs, or \
         its later absence proves nothing about that step"
    );

    let outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc_converging),
        Some(&scrub_blocked),
        None,
        None,
        GRACE + 1_000,
    )
    .await
    .unwrap();
    assert!(
        outcome != Reconciled::Satisfied && outcome != Reconciled::Changed,
        "a blocked loop lowers the step's answer whichever order the loops ran in; got \
         {outcome:?}"
    );
    // ...and the converging loop converged something in THAT step, not merely in the probe
    // above: the second piece of garbage — present a moment ago — is gone, so the answer above
    // really is `Changed` lowered to a refusal rather than `Satisfied` beside one.
    assert!(
        converging_d0
            .get_fragment(frag(order_b_garbage, 0))
            .await
            .unwrap()
            .is_none(),
        "the GC loop in the two-loop step must have reclaimed its garbage — the refusal \
         lowers the ANSWER, it does not stop the other loop working"
    );
}
