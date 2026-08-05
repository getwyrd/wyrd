//! Issue #651 (slice 4a of 7 of the #635 re-slicing, 0016 decision 7(e)/(f)): **the two
//! surfaces that report whether a reconciliation is COMPLETE answer contained and attributed
//! over a reference set with a hole in it** — post-restore reconciliation
//! (`wyrd_custodian::reconcile_after_restore`) and the drain status
//! (`wyrd_custodian::desired_state::reconciliation_status`).
//!
//! #650 built the containment vocabulary these two consume (`gc::ReferenceSet`, its
//! `unresolvable` map, `protects`) and deferred both of these surfaces here **by name, in its
//! own code**. What was left:
//!
//!   * restore's *report* half re-read every committed record itself and `?`d out on a
//!     segmented map, so ONE segmented object — or one committed record that would not decode
//!     — turned the whole pass into an `Err` and the operator command produced **no report at
//!     all**: not a stranded count, not the dangling/misplaced chunks of the objects it COULD
//!     read; and
//!   * the drain surface answered a bare `Pending` over an incomplete set — the same word it
//!     uses for a server that genuinely still holds referenced fragments — so an operator
//!     watching a decommission stall could not learn WHICH record was blocking it, and the
//!     stall is a state nothing exits.
//!
//! The fixture (in-memory trait stores, raw-record `seg:`/root seeding) is the pruned #650
//! fixture (`crates/custodian/tests/segmented_map_consumers.rs`) — integration-test crates
//! cannot import across files — carrying only what these criteria need: no loop dispatch, no
//! coordination, no scrub context. The post-restore pass is an operator one-shot, never a loop
//! step, so it is driven directly exactly as `tests/restore_reconcile.rs` drives it.
//!
//! **Why no assertion here names the answer's new shape:** the production files are reverted
//! under the per-fix red leg, so naming a symbol this patch introduces (`RestoreReport`'s new
//! field, a new `ReconciliationStatus` variant) would make that leg fail to COMPILE and the red
//! would degrade to "a symbol is missing" instead of "the behaviour was wrong". Every leg below
//! is therefore expressed in base-visible entries only, and the positive matches on the new
//! shapes ship in `tests/restore_reconcile.rs`, `tests/segmented_map_consumers.rs` and
//! `crates/server/src/cli.rs`'s own `mod tests`, which the whole-tree gate runs.
//!
//! 1. `a_segmented_object_no_longer_stops_the_post_restore_pass` — criterion (1).
//! 2. `an_unreadable_object_is_contained_and_neither_surface_certifies_it` — criteria (2a)
//!    and (3): non-certification with the incomplete reading as the SOLE cause, and both
//!    surfaces naming the blocking record on their own audit seam. One store, because
//!    criterion (3) is stated over criterion (2a)'s.
//! 3. `an_unreadable_object_does_not_starve_the_objects_the_pass_could_read` — criterion (2b):
//!    containment — the damaged object costs the readable one's loss nothing.
//! 4. `marks_and_report_rest_on_one_reading` — criterion (2c): the pass never both marks a
//!    fragment and reports a record it could not read.
//! 5. `a_record_already_known_unreadable_is_named_before_a_later_read_can_fail` — criterion (3)
//!    again, at the seam's hardest moment: a store fault in a LATER read still ends the pass,
//!    and the record it had already found unreadable must be attributed by then anyway.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_core::metadata::{
    self, ChunkMap, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, SegmentGroup,
    SegmentRecord, SegmentRef, SegmentedMap,
};
use wyrd_custodian::desired_state::{
    reconciliation_status, set_lifecycle, DServerLifecycle, ReconciliationStatus,
};
use wyrd_custodian::{reconcile_after_restore, ExpiredPendingPolicy, GcContext, RestoreReport};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, Result,
    WriteBatch,
};

// ---- in-memory trait stores (the pass is proven over the seams, backend-agnostic) ----

/// A trivial in-memory metadata store, optionally **decaying** one committed record (see
/// [`MemMeta::decay_after_first_read`]).
///
/// `BTreeMap`, so `scan` answers in key order and the DAMAGED record (`inode:1`) is always the
/// one the reference build meets FIRST — otherwise "the readable object was still reported"
/// could pass on an implementation that abandons the walk at the first blocker, simply because
/// the readable object had already been handled by then.
#[derive(Default)]
struct MemMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    /// The record to serve **readable on the first `inode:` scan and unreadable on every later
    /// one** — a record damaged (or a snapshot superseded) in the instant between a pass's two
    /// reads of the committed namespace. The only fixture that can tell a pass which reads that
    /// namespace ONCE from one which reads it twice and gates its marking on the first read.
    decaying: Mutex<Option<Vec<u8>>>,
    /// How many times the committed namespace was scanned, so the decay above fires on every
    /// read after the first.
    inode_scans: Mutex<usize>,
    /// A ledger prefix whose `scan` fails with a plain, non-`ChunkMapError` fault — a genuine
    /// backend outage under one of the reads the pass makes AFTER the committed namespace, which
    /// is what tests whether a record it has ALREADY found unreadable was attributed by then or
    /// only batched for a report the fault will stop it from ever returning.
    failing: Mutex<Option<Vec<u8>>>,
}

impl MemMeta {
    fn decay_after_first_read(self, inode: InodeId) -> Self {
        *self.decaying.lock().unwrap() = Some(metadata::inode_key(inode));
        self
    }

    fn fail_scans_of(self, prefix: &[u8]) -> Self {
        *self.failing.lock().unwrap() = Some(prefix.to_vec());
        self
    }
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        if let Some(failing) = self.failing.lock().unwrap().as_deref() {
            if prefix.starts_with(failing) {
                return Err(Box::new(std::io::Error::other(STORE_FAULT)));
            }
        }
        let mut rows: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if prefix.starts_with(b"inode:") {
            let mut scans = self.inode_scans.lock().unwrap();
            *scans += 1;
            if let (2.., Some(decaying)) = (*scans, self.decaying.lock().unwrap().as_deref()) {
                for (_key, value) in rows.iter_mut().filter(|(key, _)| key == decaying) {
                    *value = Bytes::from_static(UNREADABLE_RECORD);
                }
            }
        }
        Ok(rows)
    }

    // The required paginated read (#634): a test double needs *a* body, not a backend's — the
    // dev-only testkit helper pages over this store's own `scan`.
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

/// Bytes that are not an [`InodeRecord`] — what a record damaged under a running pass looks
/// like to the very next read of it.
const UNREADABLE_RECORD: &[u8] = b"not a record";

/// The injected store fault's exact text, so a leg proves THIS fault came back rather than
/// accepting any error at all — a pre-fix tree fails the same read differently.
const STORE_FAULT: &str = "simulated store fault: ledger unreachable";

/// One D server's fragment bytes — a deliberately dumb `ChunkStore`. Content is never real
/// erasure-coded payload: what these legs assert is presence on disk and the absence of an
/// `orphan:` record, never checksum validity.
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

// ---- audit capture (the proven in-tree pattern, `crates/core/tests/read_repair.rs`) ----

/// A `MakeWriter` collecting what the subscriber emits, so a leg asserts on the record the
/// surface actually produced rather than assuming one exists.
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
/// `Interest::never` under the parallel test harness: `tracing` caches each callsite's interest
/// in process-global state the first time it is hit, so a sibling test in this binary that hits
/// an audit callsite with no subscriber installed could otherwise disable it for the whole
/// process and leave the capture below empty (issue #214).
///
/// Called at the top of EVERY test here — all of them drive passes that fire those callsites,
/// so whichever the harness schedules first has to be the one that installs the default.
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

/// Whether an audit trail reported a record it could not read at all — the observable half of
/// "this run cannot speak for the whole store", named without naming a symbol this patch adds.
fn reports_an_unreadable_record(logged: &str) -> bool {
    logged.contains(r#""action":"unresolvable-chunk-map""#)
}

/// Assert that a surface's audit trail attributes the blocker: the right seam (so a collector
/// can select on it), the right action, and — the part an operator actually needs — the NAME of
/// the record to repair.
fn assert_attributes_blocker(logged: &str, seam: &str, object: &str) {
    assert!(
        logged.contains(&format!(r#""target":"{seam}""#))
            && reports_an_unreadable_record(logged)
            && logged.contains(&format!(r#""inode":"{object}""#)),
        "{seam} must classify the blocker as an unreadable chunk map AND name {object}: a \
         refusal an operator cannot attribute is a stall with no way out. got: {logged}"
    );
}

// ---- fixture: raw-record `seg:` + root seeding (this slice lands no producer of segmented
// maps; #653 owns the real staged-publication committer) ----

const NOW: u64 = 10_000;
const GRACE: u64 = 1_000;

/// `inode:1` sorts BEFORE `inode:2`, so in the mixed-store legs the DAMAGED object is always
/// the one the reference build meets first. `DAMAGED_OBJECT` is its key as the store spells it
/// — the name the blocker must reach the operator under.
const DAMAGED_INODE: InodeId = 1;
const READABLE_INODE: InodeId = 2;
const DAMAGED_OBJECT: &str = "inode:1";

/// The damaged object's two chunks: the one whose `seg:` record was written (its fragment is on
/// disk, and must survive the pass) and the one whose record never was — the hole that makes the
/// root unresolvable. Each leg seeds its own store, so one pair of ids serves them all.
const DAMAGED_HELD: ChunkId = 0xD1_00;
const DAMAGED_UNWRITTEN: ChunkId = 0xD2_00;

/// Segment-group nonces (32 lowercase hex characters, `0016:354`) and the fence epochs their
/// segments are scoped by. The damaged object gets its own group, so nothing about it is inside
/// the readable object's bounded `seg:` range.
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const EPOCH: u64 = 7;
const DAMAGED_NONCE: &str = "fedcba9876543210fedcba9876543210";
const DAMAGED_EPOCH: u64 = 11;

/// Every fixture chunk is one fragment (`EcScheme::None`: k = 1, and the lone fragment IS the
/// data) placed on exactly one D server — the smallest shape that still spreads a segmented
/// object's fragments over more than one server.
const CHUNK_LEN: u64 = 5;

/// A D server holding nothing any valid placement names, so a drain of it is decided by the
/// completeness of the reference set alone rather than by a genuine reference.
const EMPTY_DSERVER: DServerId = 9;

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

fn chunk_ref(chunk: ChunkId, dserver: DServerId) -> ChunkRef {
    ChunkRef {
        id: chunk,
        scheme: EcScheme::None,
        len: CHUNK_LEN,
        placement: vec![dserver],
    }
}

async fn commit(meta: &MemMeta, batch: WriteBatch) {
    assert_eq!(meta.commit(batch).await.unwrap(), CommitOutcome::Committed);
}

/// Commit `inode`'s root record. Raw `WriteBatch` puts throughout — never a publish path, since
/// no producer of segmented maps exists yet — but built with the real validating constructors,
/// so a fixture typo cannot silently change WHICH rule a leg exercises.
async fn commit_root(meta: &MemMeta, inode: InodeId, size: u64, chunk_map: ChunkMap) {
    let root = InodeRecord {
        size,
        chunk_map,
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

/// One post-restore pass over `meta` and `fleet` at [`NOW`] — the operator one-shot, driven
/// directly, exactly as `tests/restore_reconcile.rs` drives it (it is never a loop step).
async fn reconcile<'a>(
    meta: &'a dyn MetadataStore,
    fleet: &'a [(DServerId, &'a dyn ChunkStore)],
) -> Result<RestoreReport> {
    reconcile_after_restore(
        &GcContext {
            meta,
            fleet,
            grace_window_millis: GRACE,
            expired_pending: ExpiredPendingPolicy::Reclaim,
        },
        NOW,
    )
    .await
}

/// Seed a committed **segmented** root at `inode` naming `chunks.len()` segments (one chunk
/// each, `(chunk id, placed dserver)`), but WRITE only the first `written` of their `seg:`
/// records. `written == chunks.len()` is the readable shape; a smaller `written` is the real
/// gap this slice's containment rule exists for — a segment the root's own table names, on a
/// generation it still names, that genuinely never got written
/// (`metadata::ChunkMapError::SegmentAbsent`, as surfaced by `metadata::resolve_chunk_map`).
async fn seed_segmented(
    meta: &MemMeta,
    inode: InodeId,
    group: &SegmentGroup,
    chunks: &[(ChunkId, DServerId)],
    written: usize,
) {
    let mut segments = Vec::new();
    for (index, &(chunk, dserver)) in chunks.iter().enumerate() {
        let offset = index as u64 * CHUNK_LEN;
        segments.push(SegmentRef {
            index: index as u32,
            byte_offset: offset,
            byte_len: CHUNK_LEN,
        });
        if index < written {
            let record = SegmentRecord::new(vec![chunk_ref(chunk, dserver)], offset).unwrap();
            let key = metadata::seg_key(group, index as u32).unwrap();
            commit(meta, WriteBatch::new().put(key, metadata::encode(&record))).await;
        }
    }
    let map = SegmentedMap::new(group.clone(), segments).unwrap();
    let size = chunks.len() as u64 * CHUNK_LEN;
    commit_root(meta, inode, size, ChunkMap::Segmented(map)).await;
}

/// Seed the object the containment legs are built around: a committed root naming two segments,
/// only the first of which was ever written, plus that first segment's fragment on `d0`.
/// Asserts the fixture really is unreadable, so a leg can never pass because the fault it was
/// built around silently stopped being one.
async fn seed_damaged(meta: &MemMeta, d0: &MemDServer) {
    let group = SegmentGroup::new(DAMAGED_NONCE, DAMAGED_EPOCH).unwrap();
    let chunks = [(DAMAGED_HELD, 0), (DAMAGED_UNWRITTEN, 0)];
    seed_segmented(meta, DAMAGED_INODE, &group, &chunks, 1).await;
    d0.put(frag(DAMAGED_HELD, 0)).await;

    let root_key = metadata::inode_key(DAMAGED_INODE);
    let root: InodeRecord = metadata::decode(&meta.get(&root_key).await.unwrap().unwrap()).unwrap();
    assert!(
        metadata::resolve_chunk_map(meta, &root_key, &root)
            .await
            .is_err(),
        "fixture: the seeded root's map must genuinely fail to resolve"
    );
}

/// Seed a committed object holding one chunk in a **flat** map — the smallest committed record
/// that can carry a placement, for the legs whose subject is not the map's shape.
async fn seed_flat(meta: &MemMeta, inode: InodeId, chunk: ChunkId, dserver: DServerId) {
    let map = vec![chunk_ref(chunk, dserver)].into();
    commit_root(meta, inode, CHUNK_LEN, map).await;
}

/// Whether `frag` on `dserver` was handed to GC — the front half of a deletion, and the only
/// durable trace this pass leaves. "Still on disk" alone proves nothing (this pass deletes
/// nothing); an `orphan:` record is the pass saying *these bytes may be reclaimed*.
async fn is_marked_collectable(meta: &MemMeta, dserver: DServerId, frag: FragmentId) -> bool {
    meta.get(&metadata::orphan_key(dserver, frag))
        .await
        .unwrap()
        .is_some()
}

// ---- criterion (1): a segmented object no longer stops the pass ----

/// The pass re-read every committed record through its own scan and refused a segmented map
/// outright, so a store holding ONE segmented object returned `Err` and the operator command
/// produced no report at all.
///
/// Two positive observables, because "returned `Ok`" alone would also be produced by a pass
/// that quietly skipped the object: the data-loss trap underneath it (a build whose reference
/// set cannot see a segmented object's chunks calls every fragment it owns unreferenced and
/// MARKS them, handing live bytes to GC on its next grace window), and the report half actually
/// judging a chunk of that object — the segmented map was read, not stepped over.
#[tokio::test]
async fn a_segmented_object_no_longer_stops_the_post_restore_pass() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    // Two segments, both written and both readable. `held`'s fragment is on the D server its
    // placement names; `gone`'s was reclaimed after the restore point, so a pass that reads this
    // object's map owes the operator a loss verdict for it.
    let group = SegmentGroup::new(NONCE, EPOCH).unwrap();
    let held: ChunkId = 0xA1_00;
    let gone: ChunkId = 0xA2_00;
    seed_segmented(&meta, READABLE_INODE, &group, &[(held, 0), (gone, 0)], 2).await;
    d0.put(frag(held, 0)).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let report = reconcile(&meta, &fleet)
        .await
        .expect("one segmented object must not turn the whole post-restore pass into an error");

    assert_eq!(
        report.stranded_marked, 0,
        "marking a fragment this committed map references hands live bytes to GC: {report:?}"
    );
    assert!(
        !is_marked_collectable(&meta, 0, frag(held, 0)).await
            && d0.get_fragment(frag(held, 0)).await.unwrap().is_some(),
        "an `orphan:` record here is GC reclaiming live data after the grace window"
    );
    assert_eq!(
        report.dangling,
        vec![gone],
        "the report half must judge this object's chunks: a pass that returns `Ok` by SKIPPING \
         the segmented map reports nothing here — the same blind spot, quieter: {report:?}"
    );
}

// ---- criteria (2a) + (3): neither completeness surface certifies, and both NAME the
// blocking record — with the incomplete reading the SOLE cause ----

/// An otherwise **fully healthy** store — nothing dangling, nothing misplaced, nothing
/// under-replicated, nothing to mark — plus one committed object whose chunk map cannot be
/// read. The pass must complete, mark nothing, and refuse to call the run clean.
///
/// The "otherwise healthy" part is what makes this leg binding: `is_clean()` is already false
/// whenever any loss is reported, so a scenario carrying one would satisfy it without the fix.
/// Here the incomplete reading is the only thing that can make it false.
#[tokio::test]
async fn an_unreadable_object_is_contained_and_neither_surface_certifies_it() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    seed_damaged(&meta, &d0).await;

    // A wholly healthy object beside it: its fragment present, at the D server its own
    // placement names.
    let healthy: ChunkId = 0xC1_00;
    seed_flat(&meta, READABLE_INODE, healthy, 0).await;
    d0.put(frag(healthy, 0)).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let audit = Capture::default();
    let report = reconcile(&meta, &fleet)
        .with_subscriber(capturing_dispatch(audit.clone()))
        .await
        .expect("one unreadable object is contained, not an error that blanks the whole report");

    assert_eq!(
        report.stranded_marked, 0,
        "an unreadable map hides WHICH chunks its object owns: {report:?}"
    );
    assert!(
        report.dangling.is_empty()
            && report.misplaced.is_empty()
            && report.under_replicated.is_empty(),
        "a loss here would make the non-certification below pass for the wrong reason: {report:?}"
    );
    assert!(
        !report.is_clean(),
        "the one record the pass could not read is all that is wrong here, and `is_clean` is \
         its claim that the run was a clean bill — about a reading that FINISHED: {report:?}"
    );
    let logged = audit.contents();
    assert_attributes_blocker(&logged, "wyrd.custodian.restore.audit", DAMAGED_OBJECT);
    // ...and the pass's own summary — the one line an operator greps, and the one a log
    // collector alerts on — must not certify the run either. Asserted on the emitted text, so
    // no symbol this fix introduces is named here (see the module note).
    assert!(
        logged.contains("post-restore reconciliation INCOMPLETE"),
        "the summary must say the reading did not finish — 'complete' here is the \
         certification every other refusal in this pass withholds. got: {logged}"
    );

    // CRITERION (3), over this same store: the OTHER completeness surface. A drain of a server
    // no VALID committed placement names is decided by the completeness of the reference set
    // alone — which is where `reconciliation_status` is most dangerous, because it is where it
    // would otherwise answer `Satisfied`, i.e. "you may decommission this box". It answered a
    // bare `Pending` before: the same word a server that genuinely still holds referenced
    // fragments gets, so an operator watching the stall could not learn WHICH record to repair,
    // and rebalance cannot evacuate fragments of a map it cannot read. Its own capture, so the
    // restore pass's lines above cannot stand in for the drain's.
    set_lifecycle(&meta, EMPTY_DSERVER, DServerLifecycle::Draining)
        .await
        .unwrap();
    let drain_audit = Capture::default();
    let status = reconciliation_status(&meta, EMPTY_DSERVER)
        .with_subscriber(capturing_dispatch(drain_audit.clone()))
        .await
        .expect("one unreadable object must not blank the drain surface fleet-wide either");
    assert_attributes_blocker(
        &drain_audit.contents(),
        "wyrd.custodian.drain.audit",
        DAMAGED_OBJECT,
    );
    assert!(
        !matches!(status, ReconciliationStatus::Satisfied),
        "a drain over an incomplete reference set must never be certified: that tells an \
         operator to decommission a server an unreadable object may still own bytes on"
    );
}

// ---- criterion (2b): containment — the damaged object does not starve the readable ones ----

/// The same unreadable object, seeded beside a readable object that has a **genuine loss**.
/// Refusing to certify is only half the rule: the pass must still report what it *could* read,
/// or one damaged record blanks the post-restore picture for the whole store.
#[tokio::test]
async fn an_unreadable_object_does_not_starve_the_objects_the_pass_could_read() {
    enable_audit_callsites();
    let meta = MemMeta::default();
    let d0 = MemDServer::default();

    seed_damaged(&meta, &d0).await;

    // The readable object's chunk has no fragment anywhere: k = 1, so it is unreadable AND
    // unreconstructible. That verdict is exactly what an `Err` over the damaged record used to
    // cost the operator.
    let lost: ChunkId = 0xC2_01;
    seed_flat(&meta, READABLE_INODE, lost, 0).await;

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let report = reconcile(&meta, &fleet)
        .await
        .expect("one unreadable object must not end the walk over the ones that are readable");

    assert!(
        report.dangling.contains(&lost) || report.misplaced.contains(&lost),
        "withholding the readable object's genuine loss because ANOTHER object could not be \
         read is the failure this containment rule exists to prevent: {report:?}"
    );
    assert_eq!(
        report.stranded_marked, 0,
        "the containment is fleet-wide: nothing may be marked anywhere: {report:?}"
    );
    let held = frag(DAMAGED_HELD, 0);
    assert!(
        !is_marked_collectable(&meta, 0, held).await
            && d0.get_fragment(held).await.unwrap().is_some(),
        "the damaged object's own readable fragment must be neither marked nor gone"
    );
}

// ---- criterion (3), continued: the name outlives a store fault that ends the pass ----

/// Attribution a later fault can swallow is not attribution. The pass reads more of the store
/// after the reference build — the `orphan:` and `pending:` ledgers, then the fleet — and any of
/// those reads can fail for reasons that have nothing to do with the damaged record: a backend
/// blip, a partitioned store. That error propagates, and rightly (a pass that cannot read the
/// store has no answer for any object). But by then this pass already KNOWS which record it
/// could not read, and if that name were held back for a report the fault stops it from
/// returning, the operator would be left with an error naming nothing and the record still
/// blocking every future pass — the stall with no way out that C-1 forbids.
///
/// So the name must already be on the durability seam. Asserted per intervening read, and on the
/// injected fault's own text, so a leg cannot pass on some *other* error.
#[tokio::test]
async fn a_record_already_known_unreadable_is_named_before_a_later_read_can_fail() {
    enable_audit_callsites();
    for ledger in [b"orphan:".as_slice(), b"pending:".as_slice()] {
        let meta = MemMeta::default();
        let d0 = MemDServer::default();
        seed_damaged(&meta, &d0).await;
        let meta = meta.fail_scans_of(ledger);

        let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
        let audit = Capture::default();
        let failed = reconcile(&meta, &fleet)
            .with_subscriber(capturing_dispatch(audit.clone()))
            .await
            .expect_err("fixture: the poisoned ledger read must end the pass with an error");

        assert!(
            failed.to_string().contains(STORE_FAULT),
            "fixture: the pass must have failed on the INJECTED store fault: {failed}"
        );
        assert_attributes_blocker(
            &audit.contents(),
            "wyrd.custodian.restore.audit",
            DAMAGED_OBJECT,
        );
    }
}

// ---- criterion (2c): the marks and the report rest on ONE reading ----

/// A conclusion and the reading it rests on are one. This pass may read the committed namespace
/// once or twice — both are honest implementations — but it must never **mark a fragment** under
/// one reading while **reporting a record it could not read** from another. A mark is an
/// authorization to delete, and an operator shown a report naming an unreadable record has no
/// way to tell that the marks beside it were decided before the damage was seen.
///
/// Stated as that conjunction, so the leg is implementation-neutral: a pass reading once sees the
/// record fine and marks the stray (passes); a pass reading twice but withholding marks while
/// EITHER reading found a hole marks nothing and names the record (passes); a pass reading twice
/// and gating on the FIRST reading marks the stray *and* reports the record unreadable (fails —
/// the defect this leg exists to catch).
#[tokio::test]
async fn marks_and_report_rest_on_one_reading() {
    enable_audit_callsites();
    let healthy: ChunkId = 0xC1_02;
    let stray = frag(0x5A_02, 0);

    // CONTROL: over a store that never changes under the pass, the stray IS marked. Without it
    // "nothing was marked" below would be satisfied by a pass that marks nothing, ever.
    let control = MemMeta::default();
    let control_d0 = MemDServer::default();
    seed_flat(&control, READABLE_INODE, healthy, 0).await;
    control_d0.put(frag(healthy, 0)).await;
    control_d0.put(stray).await;
    let control_fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &control_d0)];
    let control_report = reconcile(&control, &control_fleet).await.unwrap();
    assert_eq!(
        control_report.stranded_marked, 1,
        "fixture: the stray is unreferenced and evidence-free, so a COMPLETE reading marks it \
         — which is what makes the run below a refusal, not a no-op: {control_report:?}"
    );

    // ...and now the same store, with the committed record readable on the first `inode:` read
    // and unreadable on every later one.
    let meta = MemMeta::default();
    let d0 = MemDServer::default();
    seed_flat(&meta, READABLE_INODE, healthy, 0).await;
    d0.put(frag(healthy, 0)).await;
    d0.put(stray).await;
    let meta = meta.decay_after_first_read(READABLE_INODE);

    let fleet: Vec<(DServerId, &dyn ChunkStore)> = vec![(0, &d0)];
    let audit = Capture::default();
    let report = reconcile(&meta, &fleet)
        .with_subscriber(capturing_dispatch(audit.clone()))
        .await
        .expect("a record damaged between two reads must not blank the whole report either");
    let logged = audit.contents();

    assert!(
        !(report.stranded_marked > 0 && reports_an_unreadable_record(&logged)),
        "this run both MARKED a fragment and reported a record it could not read: two readings, \
         two conclusions, and the operator is shown one of them. Read the set once, or withhold \
         every mark while EITHER reading found a hole. {report:?} / {logged}"
    );
    // The fixture really did what it says: the record is genuinely unreadable to every read
    // after the first, so this leg cannot pass because the fault never fired. (How many times
    // the pass read the namespace is the implementation's choice, and not asserted.)
    let rows = meta.scan(b"inode:").await.unwrap();
    assert!(
        rows.iter()
            .any(|(_key, value)| metadata::decode::<InodeRecord>(value).is_err()),
        "fixture: the record must be unreadable after the first read"
    );
}
