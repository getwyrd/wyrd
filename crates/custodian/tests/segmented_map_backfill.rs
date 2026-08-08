//! Issue #695 (child 1 of 3 of the #681 split, 0016 decision 7(e)): **backfill reads every
//! committed object through the ONE resolver every consumer shares, contains per object what it
//! cannot read, declines — rather than aborts or silently mutates — the work it does not own,
//! reports the placement gauge from that same single reading, and refuses to certify a pass that
//! answered over less than the committed store.**
//!
//! On `origin/main` the chunk map was read inline out of the record at two sites
//! (`backfill.rs:98-101` in `reconcile`, `:180-183` in `emit_remaining`), so ONE segmented object
//! made the pass return `Err` for the WHOLE store and the drain gauge was never published at all.
//! GC (#650) and restore (#651) already read through `metadata::resolve_chunk_map` and contain per
//! object; backfill is the last of the four custodian loops to do so. Every leg drives the real
//! public entry over in-memory trait doubles and asserts **positive** observables — a record
//! actually filled, bytes actually unchanged, a name actually on the audit seam — never "no error
//! was raised", which a pass that did nothing at all also produces.
//!
//! **No leg names a symbol this patch introduces:** the red leg reverts `backfill.rs` and keeps
//! this file, so naming a new variant, field or helper would make the tree fail to COMPILE and
//! score a missing symbol as a behavioural red. `Reconciled::Blocked` is on the base already
//! (`reconciliation.rs:44`); the added audit/metric vocabulary is asserted as the strings the
//! durability seam publishes.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Once};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_core::metadata::{
    self, ChunkMap, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, SegmentGroup,
    SegmentRecord, SegmentRef, SegmentedMap,
};
use wyrd_custodian::backfill::{reconcile, BackfillContext};
use wyrd_custodian::Reconciled;
use wyrd_traits::{ChunkId, CommitOutcome, DServerId, MetadataStore, Result, ScanPage, WriteBatch};

/// The injected fault's exact text, so leg 5 proves THIS error came back rather than any error.
const STORE_FAULT: &str = "simulated store fault: root re-read unreachable";

// The vocabulary the durability seam publishes, as the capture below sees it — asserted, never
// assumed, and every added item pinned by a leg.
const DECLINED: &str = "action=\"declined-segmented\";";
const UNREADABLE: &str = "action=\"unresolvable-chunk-map\";";
const DECLINES: &str = "monotonic_counter.backfill_declined_records=1;";
const UNREADS: &str = "monotonic_counter.backfill_unresolvable_records=1;";
const REMAINING: &str = "gauge.backfill_placement_remaining";
const INCOMPLETE: &str = "gauge.backfill_placement_incomplete";

/// THE metadata double. `BTreeMap`-backed so key order is a fixture *property*: the damaged
/// records sort FIRST, so "the healthy record was still filled" can never pass merely because the
/// healthy one had been handled before the walk met a blocker. Every prefix read is logged —
/// including a `seg:` range read, since `scan_page` pages over this same `scan` (leg 4) — and
/// `get` can fail with a plain, non-`ChunkMapError` fault: a backend outage under the resolver, as
/// opposed to an anomaly the resolver itself describes and a pass recovers by downcast (leg 5).
#[derive(Default)]
struct MemMeta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    reads: Mutex<Vec<Vec<u8>>>,
    fail_get: bool,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.fail_get {
            return Err(Box::new(std::io::Error::other(STORE_FAULT)));
        }
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.reads.lock().unwrap().push(prefix.to_vec());
        let kv = self.kv.lock().unwrap();
        let rows = kv.iter().filter(|(k, _)| k.starts_with(prefix));
        Ok(rows.map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    // The required paginated read (#634): a test double needs *a* body, not a backend's — the
    // dev-only testkit helper pages over this store's own `scan`
    // (`crates/custodian/tests/segmented_map_consumers.rs:109-116`).
    async fn scan_page(&self, p: &[u8], after: Option<&[u8]>, n: usize) -> Result<ScanPage> {
        wyrd_testkit::test_double_scan_page(self, p, after, n).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        let lost = |p: &wyrd_traits::Precondition| kv.get(&p.key) != p.expected.as_ref();
        if batch.preconditions.iter().any(lost) {
            return Ok(CommitOutcome::Conflict);
        }
        kv.extend(batch.puts);
        kv.retain(|key, _| !batch.deletes.contains(key));
        Ok(CommitOutcome::Committed)
    }
}

/// How many reads of the namespace under `prefix` this store answered.
fn reads(meta: &MemMeta, prefix: &[u8]) -> usize {
    let seen = meta.reads.lock().unwrap();
    seen.iter().filter(|p| p.starts_with(prefix)).count()
}

// ---- the shared fixture: ONE seeding helper, ONE audit-capture helper ----

/// Every fixture chunk is `ReedSolomon{k:2,m:1}` — `fragment_count() == 3`, so a FULL placement is
/// `[0,1,2]` and the identity fill is visibly full-length, not a single element.
const FULL: [DServerId; 3] = [0, 1, 2];
const CHUNK_LEN: u64 = 5;

/// `inode:1` / `inode:2` sort before `inode:9`, so the damaged records are met FIRST.
const DAMAGED: InodeId = 1;
const UNDECODABLE: InodeId = 2;
const SEGMENTED: InodeId = 3;
const FLAT: InodeId = 9;

/// What to seed under one committed inode key, every chunk carrying the same `placement` (`&[]` is
/// the pre-M3 empty placement this pass fills, `&FULL` an explicit full-length one):
/// `Flat(placement)` puts one chunk inline in the root; `Segmented(placement, segments, written)`
/// writes a root naming `segments` `seg:` records of which only the first `written` were ever
/// written (a smaller `written` is a segment the root's own table names, on a generation it still
/// names, that genuinely never got written); `Undecodable` puts bytes that will not
/// `metadata::decode`.
#[derive(Clone, Copy)]
enum Shape<'a> {
    Flat(&'a [DServerId]),
    Segmented(&'a [DServerId], usize, usize),
    Undecodable,
}

fn chunk(inode: InodeId, index: usize, placement: &[DServerId]) -> ChunkRef {
    ChunkRef {
        id: ChunkId::from(inode) << 8 | index as ChunkId,
        scheme: EcScheme::ReedSolomon { k: 2, m: 1 },
        len: CHUNK_LEN,
        placement: placement.to_vec(),
    }
}

async fn commit(meta: &MemMeta, batch: WriteBatch) {
    assert_eq!(meta.commit(batch).await.unwrap(), CommitOutcome::Committed);
}

/// THE seeding helper: one committed object of `shape` at `inode`. Raw `WriteBatch` puts — no
/// producer of segmented maps exists yet (#653 owns the committer) — but built through the real
/// validating constructors, so a fixture typo cannot silently change WHICH rule a leg exercises. A
/// damaged shape asserts it really is damaged the way its leg needs, so no leg can pass because
/// the fault it was built around quietly stopped being one.
async fn seed(meta: &MemMeta, inode: InodeId, shape: Shape<'_>) {
    let root_key = metadata::inode_key(inode);
    let (chunk_map, count, unreadable) = match shape {
        Shape::Undecodable => {
            let bytes = Bytes::from_static(b"{\"not\":\"an inode record\"");
            let read = metadata::decode::<InodeRecord>(&bytes);
            assert!(read.is_err(), "fixture: must not decode");
            return commit(meta, WriteBatch::new().put(root_key, bytes)).await;
        }
        Shape::Flat(place) => (ChunkMap::from(vec![chunk(inode, 0, place)]), 1, false),
        Shape::Segmented(place, segments, written) => {
            let group = SegmentGroup::new(format!("{inode:032x}"), 7).unwrap();
            let table = (0..segments as u32)
                .map(|index| SegmentRef {
                    index,
                    byte_offset: u64::from(index) * CHUNK_LEN,
                    byte_len: CHUNK_LEN,
                })
                .collect();
            for i in 0..written {
                let at = i as u64 * CHUNK_LEN;
                let seg = SegmentRecord::new(vec![chunk(inode, i, place)], at).unwrap();
                let key = metadata::seg_key(&group, i as u32).unwrap();
                commit(meta, WriteBatch::new().put(key, metadata::encode(&seg))).await;
            }
            let map = SegmentedMap::new(group, table).unwrap();
            (ChunkMap::Segmented(map), segments, written < segments)
        }
    };
    let root = InodeRecord {
        // The record's own decode invariant: the declared size is the span its map covers.
        size: count as u64 * CHUNK_LEN,
        chunk_map,
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let bytes = metadata::encode(&root);
    commit(meta, WriteBatch::new().put(root_key.clone(), bytes)).await;
    if unreadable {
        // Through `decode` FIRST: this object's fault must be the RESOLVE, on a root whose own
        // bytes are fine. Without that, a seeding slip (a `size` disagreeing with the segment
        // table, say) would make it a second undecodable record and leave the resolver-refusal
        // arm unexercised while the leg still passed.
        let bytes = meta.get(&root_key).await.unwrap().unwrap();
        let root: InodeRecord = metadata::decode(&bytes).expect("fixture: the ROOT decodes");
        let read = metadata::resolve_chunk_map(meta, &root_key, &root).await;
        assert!(read.is_err(), "fixture: its map must fail to resolve");
    }
}

/// THE audit-capture helper: every field the durability seam published, `name=value;` per event.
#[derive(Clone, Default)]
struct Audit(Arc<Mutex<String>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Audit {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = self.0.lock().unwrap();
        event.record(&mut |f: &tracing::field::Field, v: &dyn std::fmt::Debug| {
            fields.push_str(&format!("{f}={v:?};"));
        });
    }
}

/// One pass over `meta`: its outcome, and what it told the operator. A permissive global default
/// is installed **once** first, so the audit/metric callsites never latch `Interest::never` under
/// the parallel harness and leave a sibling leg's capture empty (issue #214; the same guard as
/// `segmented_map_consumers.rs:317-332`).
async fn run_pass(meta: &MemMeta) -> (Result<Reconciled>, String) {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
    let audit = Audit::default();
    let ctx = BackfillContext { meta };
    let layered = tracing_subscriber::registry().with(audit.clone());
    let outcome = reconcile(&ctx).with_subscriber(layered).await;
    let logged = audit.0.lock().unwrap().clone();
    (outcome, logged)
}

/// Assert the trail carries `needle` exactly `want` times — one counter tick, one action line.
fn assert_hits(logged: &str, needle: &str, want: usize, why: &str) {
    assert_eq!(logged.matches(needle).count(), want, "{why}\n{logged}");
}

/// The last value a numeric field carried — a gauge is a sample, not a tally.
fn gauge(logged: &str, name: &str) -> Option<u64> {
    let field = format!("{name}=");
    let samples = logged.split(&field).skip(1);
    let values = samples.filter_map(|s| s.split(';').next());
    values.filter_map(|v| v.parse().ok()).last()
}

/// The committed placement `inode`'s first chunk carries in the store now.
async fn placement(meta: &MemMeta, inode: InodeId) -> Vec<DServerId> {
    let bytes = meta.get(&metadata::inode_key(inode)).await.unwrap();
    let record: InodeRecord = metadata::decode(&bytes.unwrap()).expect("decodes");
    record.chunk_map.as_flat().unwrap()[0].placement.clone()
}

// ---- (1) a healthy segmented object is ordinary: it ends nothing and blocks nothing ----

#[tokio::test]
async fn a_healthy_segmented_object_no_longer_ends_the_pass_and_blocks_nothing() {
    // S = 2 segmented objects, both sorting BEFORE the flat one, all placements already full.
    let meta = MemMeta::default();
    seed(&meta, SEGMENTED, Shape::Segmented(&FULL, 1, 1)).await;
    seed(&meta, SEGMENTED + 1, Shape::Segmented(&FULL, 1, 1)).await;
    seed(&meta, FLAT, Shape::Flat(&[])).await;

    let (outcome, logged) = run_pass(&meta).await;

    // A segmented object the pass READ that needs no fill is ordinary and healthy: it withholds no
    // certification — get that wrong and every store holding one multipart object is `Blocked`
    // forever, which is worse than the defect being fixed. The flat record met after both of them
    // is filled with the full-length identity vector regardless, and reading their maps cost at
    // most ONE bounded `seg:` range read each: each object's map is read once per pass, not once
    // to fill it and again to report on it.
    assert_eq!(placement(&meta, FLAT).await, FULL, "{logged}");
    assert_eq!(outcome.expect("not ended"), Reconciled::Changed, "{logged}");
    assert_eq!(gauge(&logged, INCOMPLETE), Some(0), "{logged}");
    assert!(reads(&meta, b"seg:") <= 2, "one range read per object");
}

// ---- (2) a fill this pass may not perform is declined, not aborted and not written ----

#[tokio::test]
async fn a_fill_this_pass_may_not_perform_is_declined_not_mutated_and_does_not_certify() {
    let meta = MemMeta::default();
    seed(&meta, SEGMENTED, Shape::Segmented(&[], 1, 1)).await;
    // Every row the store holds, so "byte-identical" is over the object's whole footprint — root
    // and `seg:` record alike — rather than over the rows this leg remembered to name.
    let before = meta.kv.lock().unwrap().clone();

    let (outcome, logged) = run_pass(&meta).await;

    // The pass declined work it was asked to do, so it must NOT certify the drain — and it leaves
    // every row byte-identical, the root (its `version` with it) and the `seg:` record alike: the
    // segmented write path is #682's, so a decline writes NOTHING at all.
    assert_eq!(outcome.unwrap(), Reconciled::Blocked, "{logged}");
    assert_eq!(*meta.kv.lock().unwrap(), before, "byte-identical");
    // The decline is named on the seam and told APART from an unreadable record: this object was
    // READ perfectly well, so the operator waits for #682 rather than going to repair a record
    // nothing is wrong with. And it is counted.
    assert_hits(&logged, DECLINED, 1, "declined, not unreadable");
    assert_hits(&logged, UNREADABLE, 0, "declined, not unreadable");
    assert_hits(&logged, DECLINES, 1, "the decline is counted");
    // Its empty placement is still OWED — only a committed fill takes one off the operator's drain
    // signal — and the count of objects this pass could not stand that number behind rides along.
    assert_eq!(gauge(&logged, REMAINING), Some(1), "{logged}");
    assert_eq!(gauge(&logged, INCOMPLETE), Some(1), "{logged}");
}

// ---- (3) an unreadable object is named, contained, and certifies nothing — each class alone ----

#[tokio::test]
async fn an_unreadable_committed_object_is_named_the_walk_continues_and_nothing_certifies() {
    // The two ways a committed object is unreadable: (a) a root naming a segment whose `seg:`
    // record was never written, (b) a record whose own bytes will not decode. Both sort BEFORE the
    // healthy one in the `BTreeMap` the walk reads, and `seed` asserts each really is damaged that
    // way — so "the healthy record was still filled" cannot pass on a walk that abandons the store
    // at its first blocker.
    let damaged = [Shape::Segmented(&FULL, 2, 1), Shape::Undecodable];

    let meta = MemMeta::default();
    seed(&meta, FLAT, Shape::Flat(&[])).await;
    for (inode, shape) in [DAMAGED, UNDECODABLE].into_iter().zip(damaged) {
        seed(&meta, inode, shape).await;

        // EACH class ALONE withholds certification and is counted on its own, over its own store:
        // without this, either one could stop counting entirely and every assertion below would
        // still hold on the other's.
        let alone = MemMeta::default();
        seed(&alone, inode, shape).await;
        seed(&alone, FLAT, Shape::Flat(&[])).await;
        let (outcome, logged) = run_pass(&alone).await;
        assert_eq!(outcome.expect("contained"), Reconciled::Blocked, "{logged}");
        assert_eq!(gauge(&logged, INCOMPLETE), Some(1), "inode:{inode}");
        assert_eq!(placement(&alone, FLAT).await, FULL, "still filled");
    }

    let (outcome, logged) = run_pass(&meta).await;
    let outcome = outcome.expect("an unreadable record must not end the walk");
    // Containment is PER OBJECT: the healthy record met after both damaged ones is still filled,
    // and the pass still refuses to certify — an operator reading `Satisfied` is being told the
    // store converged, and will act on it. BOTH damaged records are NAMED by their own `inode:`
    // key (`gc::object_name`'s escaping shape, `gc.rs:470-480`) and counted once each: a refusal
    // an operator cannot attribute is a stall with no way out.
    assert_eq!(placement(&meta, FLAT).await, FULL, "{logged}");
    assert_eq!(outcome, Reconciled::Blocked, "{logged}");
    for inode in [DAMAGED, UNDECODABLE] {
        assert_hits(&logged, &format!("inode=inode:{inode};"), 1, "named once");
    }
    assert_hits(&logged, UNREADS, 2, "each counted once");
    // TWO objects went unread, so TWO are published on the number the refusal reads: a class whose
    // hole is silently absorbed by the other's undercounts the store this pass could not answer
    // over, and the drain signal beside it is then a floor nobody can size.
    assert_eq!(gauge(&logged, INCOMPLETE), Some(2), "{logged}");
}

// ---- (4) one reading of the namespace per pass, and the same gauge off it ----

#[tokio::test]
async fn one_reading_of_the_namespace_per_pass() {
    // Two ORDINARY flat records — one fillable, one already full (`inode:1` is simply the one the
    // walk meets first here) — so this store is one the BASE could also report a gauge over.
    let meta = MemMeta::default();
    seed(&meta, DAMAGED, Shape::Flat(&[])).await;
    seed(&meta, FLAT, Shape::Flat(&FULL)).await;

    let (outcome, logged) = run_pass(&meta).await;
    assert_eq!(outcome.unwrap(), Reconciled::Changed, "{logged}");

    // ONE reading of the committed namespace per pass: the population is counted in the walk that
    // fills, not by a second scan that re-reads — and, over segmented objects, re-resolves —
    // everything the first already held. And the number an operator watches is unchanged by that:
    // the base counted it by that second scan, as the empty placements over the committed flat
    // records straight off the store. Read this store back the same way — one chunk per record
    // here, so per-record and per-chunk agree — and hold the published gauge to it.
    assert_eq!(reads(&meta, b"inode:"), 1, "one namespace reading");
    let after = [
        placement(&meta, DAMAGED).await,
        placement(&meta, FLAT).await,
    ];
    let base = after.iter().filter(|p| p.is_empty()).count() as u64;
    assert_eq!(gauge(&logged, REMAINING), Some(base), "{logged}");
    // (the other half of this leg — at most S `seg:` range reads over a store of S segmented
    // objects — is asserted on leg 1's store, which already holds S = 2 of them.)
}

// ---- (5) the over-containment guard: a store fault is not one object's map ----

#[tokio::test]
async fn a_fault_that_is_not_one_objects_map_still_ends_the_pass() {
    let mut meta = MemMeta::default();
    seed(&meta, SEGMENTED, Shape::Segmented(&FULL, 1, 1)).await;
    meta.fail_get = true;

    // A store failing underneath the resolver is not one object's chunk map: a walk that cannot
    // read the store has no answer for ANY object, so it must not be contained — and THAT fault
    // must be what came back, not a chunk-map verdict wearing its place.
    let err = run_pass(&meta).await.0.expect_err("a store fault ends it");
    assert!(err.to_string().contains(STORE_FAULT), "wrong error: {err}");
}
