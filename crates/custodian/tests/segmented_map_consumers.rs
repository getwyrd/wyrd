//! **Every maintenance consumer resolves a segmented chunk map** — issue #635, proposal
//! 0016 decision 7 (`0016:2280-2496`), and the one thing this slice exists to make
//! mechanically impossible to get wrong.
//!
//! A published chunk map larger than one backend value is split into `seg:` records
//! (decision 7(a)) and the root carries only the segment table. That changes the shape of
//! `InodeRecord.chunk_map` for **every** consumer of it, and the recorded failure of
//! #508's fourth attempt was a resolver wired into the read path **alone** while `gc.rs`
//! and `restore.rs` still walked the field: a restore pass stranded a live segmented
//! object and a later GC pass deleted its fragments. So the observables here are
//! deliberately **positive** — a pass that succeeds, a drain that answers `Pending`, a
//! fragment that is still on disk — never "no error was raised".
//!
//! The fixture seeds a **committed segmented object by raw record bytes** (the encoding
//! settled in the design proposal), so nothing here names a symbol this slice adds and
//! the whole file compiles on the pre-fix tree: its RED is an assertion, not a build
//! error. On the pre-fix tree `metadata::decode` cannot parse a segmented value at all,
//! so every consumer's strict `?` propagates and these legs fail.
//!
//! The legs, each through the REAL production entry point:
//!
//! 1. `reconcile_step` **succeeds** with GC + scrub + reconstruction + rebalance supplied
//!    (`crates/custodian/src/reconciliation.rs:65-73`).
//! 2. The segmented object's fragments are in the **protected set**, asserted positively:
//!    a drain of a server holding one answers `Pending`
//!    (`crates/custodian/src/desired_state.rs:167-183`). A resolver that decodes the new
//!    shape but never reads the `seg:` range answers `Satisfied` — this is the leg that
//!    catches it.
//! 3. **Restore does not strand it**: `RestoreReport::stranded_marked == 0`
//!    (`crates/custodian/src/restore.rs:104-145`,`:179`).
//! 4. **The data loss that would follow is pinned**: past the orphan grace window, a
//!    second GC pass leaves every fragment the resolved map names on disk. Asserted
//!    TWICE — once in the pass sequence above, and once **standing alone** in its own
//!    test, where the restore marks are laid by the real pass with no assertion in
//!    between, so the pin can fail independently of leg 3.
//! 5. A **flat** object in the same store is unaffected, and its stored `inode:` bytes are
//!    unchanged byte-for-byte after every pass.
//! 6. The consumers `reconcile_step` does not dispatch, or does not reach without their
//!    own trigger, get their own positive observable: the **core read path** returns the
//!    object's bytes; **reconstruction** resolves a segmented chunk for an explicitly
//!    enqueued repair and repoints its `seg:` record; **rebalance** evacuates a segmented
//!    chunk off a draining server (its leg returns before planning anything unless a
//!    server is draining, so nothing above reaches it); **backfill** takes its stated
//!    decision — driven by the input that discriminates it, a segmented chunk with an
//!    EMPTY placement, which is the one shape its rewrite would otherwise reach.
//!
//! (The gateway's whole-object and ranged read paths are the two remaining consumers;
//! they are exercised where they live, in `crates/server/src/lib.rs`'s co-located tests,
//! because `wyrd-custodian` does not depend on the gateway.)

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId};
use wyrd_core::placement::Topology;
use wyrd_core::read::read_object;
use wyrd_core::repair;
use wyrd_core::write::{plan_write, write_fragments};
use wyrd_custodian::{
    backfill::{self, BackfillContext},
    reconcile_after_restore, reconcile_step, reconciliation_status, set_lifecycle, Custodian,
    DServerLifecycle, ExpiredPendingPolicy, FencedZone, GcContext, RebalanceContext, Reconciled,
    ReconciliationStatus, ReconstructionContext, ScrubContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
};

// ---- in-memory trait stores (the loops are proven over the seams) ----

#[derive(Default)]
struct MemMeta {
    kv: Mutex<HashMap<Vec<u8>, Bytes>>,
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

    // The required paginated read (#634): a test double needs *a* body, not a
    // backend's — the dev-only testkit helper pages over this store's own `scan`
    // (and therefore inherits `SCAN_CAP`, which a backend may not). Mirrors
    // `crates/custodian/tests/gc.rs:73-80`.
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

#[derive(Default)]
struct MemDServer {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
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

/// A **placement-aware** fleet: `_at` calls route to the D server the placement record
/// names, so the write fan-out and the read path resolve each fragment from its recorded
/// location (the same shape `tests/gc.rs` / `tests/reconstruction.rs` use).
struct Fleet<'a> {
    servers: Vec<(DServerId, &'a MemDServer)>,
}

impl<'a> Fleet<'a> {
    fn store(&self, dserver: DServerId) -> Option<&'a MemDServer> {
        self.servers
            .iter()
            .find(|(id, _)| *id == dserver)
            .map(|(_, s)| *s)
    }
}

#[async_trait]
impl ChunkStore for Fleet<'_> {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        if let Some(store) = self.store(DServerId::from(id.index)) {
            store.put_fragment(id, fragment).await?;
        }
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        for (_, store) in &self.servers {
            if let Some(bytes) = store.get_fragment(id).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        let mut all = Vec::new();
        for (_, store) in &self.servers {
            all.extend(store.list_fragments().await?);
        }
        Ok(all)
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        for (_, store) in &self.servers {
            store.delete_fragment(id).await?;
        }
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

#[async_trait]
impl PlacementChunkStore for Fleet<'_> {
    async fn get_fragment_at(&self, dserver: DServerId, id: FragmentId) -> Result<Option<Bytes>> {
        match self.store(dserver) {
            Some(store) => store.get_fragment(id).await,
            None => Ok(None),
        }
    }

    async fn put_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
    ) -> Result<()> {
        if let Some(store) = self.store(dserver) {
            store.put_fragment(id, fragment).await?;
        }
        Ok(())
    }
}

// ---- the fixture: a committed SEGMENTED object, seeded by raw record bytes ----

/// The segment-group nonce (32 lowercase hex characters) and the `Completing` fence
/// epoch its segments are scoped by — the settled key shape
/// `seg:<nonce>:<epoch>:<index>`, the index zero-padded to six digits (`0016:354`).
const NONCE: &str = "0123456789abcdef0123456789abcdef";
const EPOCH: u64 = 7;

const SEGMENTED_INODE: InodeId = 1;
const FLAT_INODE: InodeId = 2;
/// Two chunks per segment, so a segmented map has a real interior boundary.
const CHUNK_SIZE: usize = 8;
const RS: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
const GRACE: u64 = 1_000;

fn data(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
}

/// A minter of distinct chunk ids from a fixed base.
fn minter(base: ChunkId) -> impl FnMut() -> ChunkId {
    let mut next = base;
    move || {
        next += 1;
        next
    }
}

/// One segment record's stored bytes, in the settled encoding:
/// `{"chunks":[…],"byte_offset":…,"byte_len":…}`. The chunk array is produced by the
/// production encoder, so the fixture cannot drift from what `ChunkRef` serializes to.
fn segment_record(chunks: &[ChunkRef], byte_offset: u64) -> Vec<u8> {
    let chunks_json = String::from_utf8(metadata::encode(&chunks.to_vec()).to_vec()).unwrap();
    let byte_len: u64 = chunks.iter().map(|c| c.len).sum();
    format!(r#"{{"chunks":{chunks_json},"byte_offset":{byte_offset},"byte_len":{byte_len}}}"#)
        .into_bytes()
}

/// The key of segment `index` of the fixture's group: `seg:<nonce>:<epoch>:<000000>`.
fn seg_key(index: u32) -> Vec<u8> {
    seg_key_of(NONCE, EPOCH, index)
}

/// [`seg_key`] for an arbitrary group — the damaged object lives in its own.
fn seg_key_of(nonce: &str, epoch: u64, index: u32) -> Vec<u8> {
    format!("seg:{nonce}:{epoch}:{index:06}").into_bytes()
}

/// A committed **segmented** root: the group identity plus the segment table, with the
/// chunks themselves in the `seg:` records.
fn segmented_root(segments: &[(u64, u64)], size: u64) -> Vec<u8> {
    segmented_root_of(NONCE, EPOCH, segments, size)
}

/// [`segmented_root`] for an arbitrary group.
fn segmented_root_of(nonce: &str, epoch: u64, segments: &[(u64, u64)], size: u64) -> Vec<u8> {
    let table: Vec<String> = segments
        .iter()
        .enumerate()
        .map(|(index, (offset, len))| {
            format!(r#"{{"index":{index},"byte_offset":{offset},"byte_len":{len}}}"#)
        })
        .collect();
    format!(
        r#"{{"size":{size},"chunk_map":{{"group":{{"nonce":"{nonce}","epoch":{epoch}}},"segment_count":{count},"segments":[{table}]}},"state":"Committed","version":1}}"#,
        count = segments.len(),
        table = table.join(","),
    )
    .into_bytes()
}

/// A committed **flat** root — today's shape, byte-for-byte.
fn flat_root(chunks: &[ChunkRef], size: u64) -> Vec<u8> {
    let chunks_json = String::from_utf8(metadata::encode(&chunks.to_vec()).to_vec()).unwrap();
    format!(r#"{{"size":{size},"chunk_map":{chunks_json},"state":"Committed","version":1}}"#)
        .into_bytes()
}

/// Erasure-code `payload` into real fragments, place them across `topology`, write them
/// to the fleet, and return the resulting chunk map. Nothing is committed to metadata —
/// the caller decides which SHAPE of record names these chunks.
async fn stage_object(
    fleet: &Fleet<'_>,
    payload: &[u8],
    topology: &Topology,
    base_chunk_id: ChunkId,
) -> Vec<ChunkRef> {
    let mut plan = plan_write(payload, CHUNK_SIZE, RS, minter(base_chunk_id)).unwrap();
    plan.place(topology).unwrap();
    write_fragments(fleet, &plan).await.unwrap();
    plan.chunk_refs()
}

fn domains(servers: &[(DServerId, &str)]) -> Topology {
    let mut t = Topology::default();
    for (id, label) in servers {
        t.register(*id, *label);
    }
    t
}

/// Servers 0,1,2 carry the SEGMENTED object; 3,4,5 carry the flat one. Disjoint on
/// purpose: leg 2's drain of server 0 is then answered by the segmented object alone.
fn segmented_topology() -> Topology {
    domains(&[(0, "A"), (1, "B"), (2, "C")])
}

fn flat_topology() -> Topology {
    domains(&[(3, "D"), (4, "E"), (5, "F")])
}

fn whole_topology() -> Topology {
    domains(&[(0, "A"), (1, "B"), (2, "C"), (3, "D"), (4, "E"), (5, "F")])
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

/// Every fragment of `chunks` that is present anywhere on the fleet.
async fn present(fleet: &Fleet<'_>, chunks: &[ChunkRef]) -> Vec<FragmentId> {
    let mut found = Vec::new();
    for chunk in chunks {
        for index in 0..chunk.fragment_count() {
            let frag = FragmentId {
                chunk: chunk.id,
                index,
            };
            if fleet.get_fragment(frag).await.unwrap().is_some() {
                found.push(frag);
            }
        }
    }
    found
}

/// The whole fixture: a segmented object and a flat one, both committed, both with real
/// erasure-coded fragments on disjoint halves of the fleet.
struct Fixture {
    segmented: Vec<ChunkRef>,
    flat: Vec<ChunkRef>,
    segmented_bytes: Vec<u8>,
    flat_bytes: Vec<u8>,
    payload: Vec<u8>,
    flat_payload: Vec<u8>,
    /// The stored bytes of each `seg:` record, in index order.
    segment_bytes: Vec<Vec<u8>>,
}

async fn seed(meta: &MemMeta, fleet: &Fleet<'_>) -> Fixture {
    seed_with(meta, fleet, false).await
}

/// `strip_first_placement`: store the first segmented chunk with an EMPTY `placement`
/// vector — the pre-M3 spelling backfill exists to fill. It is the discriminating input
/// for backfill's segmented decision: with the decision removed, backfill rewrites the
/// segmented root into a FLAT map. With every placement already full-length, backfill
/// short-circuits before the rewrite and passes for the wrong reason.
async fn seed_with(meta: &MemMeta, fleet: &Fleet<'_>, strip_first_placement: bool) -> Fixture {
    let payload = data(4 * CHUNK_SIZE, 0x40);
    let segmented = stage_object(fleet, &payload, &segmented_topology(), 0x100).await;
    let flat_payload = data(2 * CHUNK_SIZE, 0x90);
    let flat = stage_object(fleet, &flat_payload, &flat_topology(), 0x200).await;

    // Two segments of two chunks each — the map has a real interior boundary, so a
    // consumer that reads only the first segment record is caught.
    let (first, second) = segmented.split_at(2);
    let mut first = first.to_vec();
    if strip_first_placement {
        // The identity fallback resolves an empty placement to servers 0,1,2 — exactly
        // where the fragments already are — so every other consumer is unaffected.
        first[0].placement = Vec::new();
    }
    let first_len: u64 = first.iter().map(|c| c.len).sum();
    let second_len: u64 = second.iter().map(|c| c.len).sum();
    let segmented_bytes = segmented_root(
        &[(0, first_len), (first_len, second_len)],
        first_len + second_len,
    );
    let flat_bytes = flat_root(&flat, flat_payload.len() as u64);
    let segment_bytes = vec![segment_record(&first, 0), segment_record(second, first_len)];

    commit(
        meta,
        WriteBatch::new()
            .put(seg_key(0), segment_bytes[0].clone())
            .put(seg_key(1), segment_bytes[1].clone())
            .put(
                metadata::inode_key(SEGMENTED_INODE),
                segmented_bytes.clone(),
            )
            .put(metadata::inode_key(FLAT_INODE), flat_bytes.clone()),
    )
    .await;

    Fixture {
        segmented,
        flat,
        segmented_bytes,
        flat_bytes,
        payload,
        flat_payload,
        segment_bytes,
    }
}

/// The **damaged** third object of leg 7: a committed segmented root naming two segments
/// whose second `seg:` record was never written. Its group is its own, so nothing about it
/// is inside the healthy object's bounded `seg:` range.
const DAMAGED_INODE: InodeId = 3;
const DAMAGED_NONCE: &str = "fedcba9876543210fedcba9876543210";
const DAMAGED_EPOCH: u64 = 11;

/// Seed that damaged object into the same store, with real fragments on the fleet, and
/// return the chunks its **readable** segment names — the ones no pass may reclaim.
async fn seed_damaged(meta: &MemMeta, fleet: &Fleet<'_>) -> Vec<ChunkRef> {
    let payload = data(4 * CHUNK_SIZE, 0xC0);
    let chunks = stage_object(fleet, &payload, &segmented_topology(), 0x300).await;
    let (first, second) = chunks.split_at(2);
    let first_len: u64 = first.iter().map(|c| c.len).sum();
    let second_len: u64 = second.iter().map(|c| c.len).sum();
    commit(
        meta,
        WriteBatch::new()
            // Segment 0 is written; segment 1 — which the root names — is NOT.
            .put(
                seg_key_of(DAMAGED_NONCE, DAMAGED_EPOCH, 0),
                segment_record(first, 0),
            )
            .put(
                metadata::inode_key(DAMAGED_INODE),
                segmented_root_of(
                    DAMAGED_NONCE,
                    DAMAGED_EPOCH,
                    &[(0, first_len), (first_len, second_len)],
                    first_len + second_len,
                ),
            ),
    )
    .await;
    first.to_vec()
}

/// The **malformed** damaged objects — leg 7's second spelling, and the one that catches a
/// containment written for only the first.
///
/// The object above is damaged by an **absent** record; these two are damaged by records
/// that are *present and unreadable*: a segment whose declared span disagrees with its own
/// chunks, and a root whose segment table contradicts itself. Both fail at **decode**,
/// which is a different code path from an absent record and reaches a consumer through a
/// different error — so a containment rule that recognised only the absent spelling would
/// let one of these end the fleet-wide reference build and blank the drain surface for
/// every healthy object in the store.
const MALFORMED_SEGMENT_INODE: InodeId = 4;
const MALFORMED_ROOT_INODE: InodeId = 5;
const MALFORMED_SEGMENT_NONCE: &str = "aaaabbbbccccddddeeeeffff00001111";
const MALFORMED_ROOT_NONCE: &str = "11112222333344445555666677778888";
const MALFORMED_EPOCH: u64 = 13;

/// Seed both malformed objects, with real fragments on the fleet, and return every chunk
/// whose fragments they own — none of which any pass may reclaim.
async fn seed_malformed(meta: &MemMeta, fleet: &Fleet<'_>) -> Vec<ChunkRef> {
    let unreadable_segment = stage_object(
        fleet,
        &data(2 * CHUNK_SIZE, 0xD0),
        &segmented_topology(),
        0x400,
    )
    .await;
    let unreadable_root = stage_object(
        fleet,
        &data(2 * CHUNK_SIZE, 0xE0),
        &segmented_topology(),
        0x500,
    )
    .await;
    let span: u64 = unreadable_segment.iter().map(|c| c.len).sum();
    // The record is present, and its declared span is one byte more than its chunks carry:
    // structural corruption, caught at decode rather than by an absence.
    let torn = String::from_utf8(segment_record(&unreadable_segment, 0))
        .unwrap()
        .replace(
            &format!(r#""byte_len":{span}}}"#),
            &format!(r#""byte_len":{}}}"#, span + 1),
        );
    // …and a root whose own table contradicts itself: `segment_count` names two segments
    // where one is listed. Its `seg:` record is written and perfectly readable, so nothing
    // about this object is missing — only its root is unreadable.
    let root_span: u64 = unreadable_root.iter().map(|c| c.len).sum();
    let miscounted = String::from_utf8(segmented_root_of(
        MALFORMED_ROOT_NONCE,
        MALFORMED_EPOCH,
        &[(0, root_span)],
        root_span,
    ))
    .unwrap()
    .replace(r#""segment_count":1"#, r#""segment_count":2"#);
    commit(
        meta,
        WriteBatch::new()
            .put(
                seg_key_of(MALFORMED_SEGMENT_NONCE, MALFORMED_EPOCH, 0),
                torn.into_bytes(),
            )
            .put(
                metadata::inode_key(MALFORMED_SEGMENT_INODE),
                segmented_root_of(MALFORMED_SEGMENT_NONCE, MALFORMED_EPOCH, &[(0, span)], span),
            )
            .put(
                seg_key_of(MALFORMED_ROOT_NONCE, MALFORMED_EPOCH, 0),
                segment_record(&unreadable_root, 0),
            )
            .put(
                metadata::inode_key(MALFORMED_ROOT_INODE),
                miscounted.into_bytes(),
            ),
    )
    .await;
    [unreadable_segment, unreadable_root].concat()
}

/// Six in-memory D servers and a placement-aware fleet view over them.
fn fleet_servers() -> Vec<MemDServer> {
    (0..6).map(|_| MemDServer::default()).collect()
}

fn fleet_of(servers: &[MemDServer]) -> Fleet<'_> {
    Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u64, s))
            .collect(),
    }
}

fn dyn_fleet_of(servers: &[MemDServer]) -> Vec<(DServerId, &dyn ChunkStore)> {
    servers
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u64, s as &dyn ChunkStore))
        .collect()
}

// ---- legs 1, 3, 4, 5: one reconcile → restore → post-grace reconcile pass ----

#[tokio::test]
async fn maintenance_resolves_a_segmented_map_and_never_reclaims_its_fragments() {
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..6).map(|_| MemDServer::default()).collect();
    let fleet = Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u64, s))
            .collect(),
    };
    let fixture = seed(&meta, &fleet).await;

    let dyn_fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u64, s as &dyn ChunkStore))
        .collect();
    let topology = whole_topology();
    let gc = GcContext {
        meta: &meta,
        fleet: &dyn_fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    let scrub = ScrubContext {
        meta: &meta,
        fleet: &dyn_fleet,
    };
    let reconstruction = ReconstructionContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topology,
        unreachable: &[],
    };
    let rebalance = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topology,
    };
    let coord = MemCoordination::default();
    let (zone, custodian) = elect(&coord, "zone-segmented").await;

    // LEG 1 — the fenced control point SUCCEEDS with all four loops supplied. On the
    // pre-fix tree GC's reference build cannot decode a segmented `inode:` value, and the
    // `?` at `gc.rs:256` propagates out through `reconcile_step`.
    let outcome = reconcile_step(
        &zone,
        &custodian,
        Some(&gc),
        Some(&scrub),
        Some(&reconstruction),
        Some(&rebalance),
        10,
    )
    .await;
    assert!(
        outcome.is_ok(),
        "reconcile_step must resolve a segmented chunk map, not fail on it: {:?}",
        outcome.err().map(|e| e.to_string()),
    );

    // LEG 3 — restore does not strand it. The segmented object's fragments are all
    // referenced, so a post-restore pass marks NOTHING.
    let report = reconcile_after_restore(&gc, 20).await.unwrap();
    assert_eq!(
        report.stranded_marked, 0,
        "restore marked a live segmented object's fragments as orphans: {report:?}",
    );
    assert!(
        report.dangling.is_empty() && report.misplaced.is_empty(),
        "the segmented object reads as lost/misplaced to restore: {report:?}",
    );

    // LEG 4 — the data loss that would follow is pinned. Advance well past the orphan
    // grace window and run GC again: every fragment the RESOLVED map names is still on
    // disk. (Asserted on the fragments themselves, not on a `Reconciled` value: under the
    // leg-3 failure the marks laid by restore would be reclaimed right here.)
    let expected_segmented = present(&fleet, &fixture.segmented).await;
    assert_eq!(
        expected_segmented.len(),
        fixture.segmented.len() * 3,
        "fixture: every RS(2,1) fragment of the segmented object is on disk to begin with",
    );
    reconcile_step(
        &zone,
        &custodian,
        Some(&gc),
        Some(&scrub),
        Some(&reconstruction),
        Some(&rebalance),
        20 + GRACE * 10,
    )
    .await
    .unwrap();
    assert_eq!(
        present(&fleet, &fixture.segmented).await,
        expected_segmented,
        "a GC pass past the orphan grace window reclaimed fragments of a LIVE segmented object",
    );

    // LEG 5 — the flat object in the same store is untouched, and its stored bytes are
    // unchanged byte-for-byte after every pass.
    assert_eq!(
        present(&fleet, &fixture.flat).await.len(),
        fixture.flat.len() * 3,
        "the flat object's fragments are unaffected",
    );
    assert_eq!(
        meta.get(&metadata::inode_key(FLAT_INODE))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        fixture.flat_bytes.as_slice(),
        "a pre-existing flat record's stored bytes must survive every pass byte-for-byte",
    );
    assert_eq!(
        meta.get(&metadata::inode_key(SEGMENTED_INODE))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        fixture.segmented_bytes.as_slice(),
        "no pass rewrote the segmented root",
    );
}

/// **Leg 4, standing on its own.** The pin above shares a test body with leg 3, so it
/// can only run once `stranded_marked == 0` has already held — it cannot fail
/// independently, and a pin that only fires after the thing it pins has passed proves
/// nothing. Here the marks are laid by the REAL restore pass and then the grace window is
/// crossed with no assertion in between: if a consumer's resolution misses the `seg:`
/// range, restore marks the live segmented object's fragments as orphans and this GC
/// pass reclaims them — the #508-attempt-4 data loss, asserted on the fragments
/// themselves.
#[tokio::test]
async fn a_gc_pass_past_the_grace_window_never_reclaims_a_live_segmented_objects_fragments() {
    let meta = MemMeta::default();
    let servers = fleet_servers();
    let fleet = fleet_of(&servers);
    let fixture = seed(&meta, &fleet).await;
    let dyn_fleet = dyn_fleet_of(&servers);

    let gc = GcContext {
        meta: &meta,
        fleet: &dyn_fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    let before = present(&fleet, &fixture.segmented).await;
    assert_eq!(
        before.len(),
        fixture.segmented.len() * 3,
        "fixture: every RS(2,1) fragment is on disk to begin with",
    );

    // The restore pass runs for real and lays whatever marks it decides to lay — no
    // assertion here, deliberately, so this test's verdict does not depend on leg 3's.
    reconcile_after_restore(&gc, 20).await.unwrap();

    // …then the grace window elapses and GC runs. Whatever restore concluded is now
    // ACTED on.
    let coord = MemCoordination::default();
    let (zone, custodian) = elect(&coord, "zone-segmented-grace").await;
    reconcile_step(
        &zone,
        &custodian,
        Some(&gc),
        None,
        None,
        None,
        20 + GRACE * 10,
    )
    .await
    .unwrap();

    assert_eq!(
        present(&fleet, &fixture.segmented).await,
        before,
        "a GC pass past the orphan grace window reclaimed fragments of a LIVE segmented \
         object — the recorded #508-attempt-4 data loss",
    );
    // …and the object still reads back whole, which is the same statement from the
    // client's side.
    assert_eq!(
        read_object(&meta, &fleet, SEGMENTED_INODE)
            .await
            .unwrap()
            .as_deref(),
        Some(fixture.payload.as_slice()),
    );
}

// ---- leg 2: the protected set, asserted POSITIVELY through the drain surface ----

#[tokio::test]
async fn a_drain_of_a_server_holding_a_segmented_fragment_is_pending_not_satisfied() {
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..6).map(|_| MemDServer::default()).collect();
    let fleet = Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u64, s))
            .collect(),
    };
    let fixture = seed(&meta, &fleet).await;

    // Server 0 holds fragments of the SEGMENTED object only (the two objects are placed
    // on disjoint halves of the fleet), so `Pending` here can only come from resolving
    // the `seg:` range. A resolver that decodes the new root shape but never reads that
    // range answers `Satisfied` — nothing else in this file catches that.
    let holder = fixture.segmented[0].placed_dserver(0);
    assert!(
        holder < 3,
        "fixture: the segmented object sits on servers 0..3"
    );
    set_lifecycle(&meta, holder, DServerLifecycle::Draining)
        .await
        .unwrap();

    assert_eq!(
        reconciliation_status(&meta, holder).await.unwrap(),
        ReconciliationStatus::Pending,
        "a drain of a server holding a live SEGMENTED object's fragment must be Pending: \
         `Satisfied` would certify the drain over bytes the object still owns",
    );

    // And the mirror: a server holding nothing is `Satisfied`, so the assertion above is
    // not vacuously true of every server.
    set_lifecycle(&meta, 9, DServerLifecycle::Draining)
        .await
        .unwrap();
    assert_eq!(
        reconciliation_status(&meta, 9).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "a server no committed map references is Satisfied",
    );
}

// ---- leg 6: the consumers `reconcile_step` does not dispatch ----

#[tokio::test]
async fn the_core_read_path_resolves_a_segmented_object_byte_for_byte() {
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..6).map(|_| MemDServer::default()).collect();
    let fleet = Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u64, s))
            .collect(),
    };
    let fixture = seed(&meta, &fleet).await;

    let got = read_object(&meta, &fleet, SEGMENTED_INODE).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(fixture.payload.as_slice()),
        "the core read path must reassemble a segmented object's bytes across the segment \
         boundary (`crates/core/src/read.rs:92`)",
    );
}

#[tokio::test]
async fn reconstruction_repairs_a_chunk_that_lives_in_a_segment_record() {
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..6).map(|_| MemDServer::default()).collect();
    let fleet = Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u64, s))
            .collect(),
    };
    let fixture = seed(&meta, &fleet).await;

    // Lose one fragment of a chunk that lives in the SECOND segment record — so a
    // resolver that stops at the first segment cannot find it either.
    let victim = fixture.segmented[3].clone();
    let lost_index: u16 = 0;
    let lost_server = victim.placed_dserver(lost_index);
    let frag = FragmentId {
        chunk: victim.id,
        index: lost_index,
    };
    servers[lost_server as usize]
        .delete_fragment(frag)
        .await
        .unwrap();
    let seg_bytes_before = meta.get(&seg_key(1)).await.unwrap().unwrap();

    // Explicitly enqueue the repair: `reconcile_step` reaches `find_chunk` only for a
    // QUEUED obligation, so nothing else in this file exercises it.
    repair::enqueue_repair(&meta, victim.id, "test")
        .await
        .unwrap();

    let dyn_fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u64, s as &dyn ChunkStore))
        .collect();
    // The rebuild is forced to MOVE: the topology offers the survivors' domains (B, C)
    // and one free domain (D → server 3), so the repaired fragment cannot land back on
    // its old server and the repoint has to be written somewhere observable.
    let topology = domains(&[(1, "B"), (2, "C"), (3, "D")]);
    let reconstruction = ReconstructionContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topology,
        unreachable: &[],
    };
    let coord = MemCoordination::default();
    let (zone, custodian) = elect(&coord, "zone-segmented-repair").await;

    let outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&reconstruction),
        None,
        30,
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "reconstruction must REPAIR a chunk resolved out of a `seg:` record, not drop its \
         obligation as `referenced by no committed chunk map`",
    );

    // Positive observables: the rebuilt fragment exists again, and the SEGMENT record —
    // not the inode root — carries the new placement (proposal 0016 decision 7(f)).
    assert!(
        fleet.get_fragment(frag).await.unwrap().is_some(),
        "the lost fragment was rebuilt",
    );
    assert!(
        servers[3].get_fragment(frag).await.unwrap().is_some(),
        "…on the free distinct domain the selector chose",
    );
    let seg_bytes_after = meta.get(&seg_key(1)).await.unwrap().unwrap();
    assert_ne!(
        seg_bytes_before, seg_bytes_after,
        "the repoint must land in the segment record that carries the chunk",
    );
    assert!(
        String::from_utf8_lossy(&seg_bytes_after).contains(r#""placement":[3,1,2]"#),
        "the segment record now names the rebuilt fragment's new home: {}",
        String::from_utf8_lossy(&seg_bytes_after),
    );
    assert_eq!(
        meta.get(&metadata::inode_key(SEGMENTED_INODE))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        fixture.segmented_bytes.as_slice(),
        "the root is the generation fence, not the repoint target: it must be unchanged",
    );
    assert!(
        meta.get(&repair::repair_key(victim.id))
            .await
            .unwrap()
            .is_none(),
        "the obligation is drained by the repair that completed it",
    );
    // The object still reads back whole after the repoint.
    assert_eq!(
        read_object(&meta, &fleet, SEGMENTED_INODE)
            .await
            .unwrap()
            .as_deref(),
        Some(fixture.payload.as_slice()),
    );
}

/// **Rebalance evacuation over a segmented chunk.** `reconcile_step`'s rebalance leg
/// returns before it plans anything unless a server is actually draining, so the
/// all-consumer legs above never reach it: this one drains a server the SEGMENTED object
/// sits on and asserts the positive observable — the fragment moves, and the repoint
/// lands in the `seg:` record that carries the chunk (proposal 0016 decision 7(f)), not
/// in the root.
#[tokio::test]
async fn rebalance_evacuates_a_chunk_that_lives_in_a_segment_record() {
    let meta = MemMeta::default();
    let servers = fleet_servers();
    let fleet = fleet_of(&servers);
    let fixture = seed(&meta, &fleet).await;
    let dyn_fleet = dyn_fleet_of(&servers);

    // Drain a server the segmented object sits on. Its chunks are placed on 0,1,2 and the
    // flat object's on 3,4,5, so evacuating server 0 can only be driven by resolving the
    // `seg:` range — and the free domains D/E/F are available to receive the fragments.
    let draining = fixture.segmented[0].placed_dserver(0);
    assert!(draining < 3, "fixture: the segmented object sits on 0..3");
    set_lifecycle(&meta, draining, DServerLifecycle::Draining)
        .await
        .unwrap();

    let topology = whole_topology();
    let rebalance = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topology,
    };
    let coord = MemCoordination::default();
    let (zone, custodian) = elect(&coord, "zone-segmented-drain").await;

    // The first pass must MOVE something: a rebalance that resolved no segmented chunk
    // would report a drained fleet it never looked at.
    let first = reconcile_step(&zone, &custodian, None, None, None, Some(&rebalance), 40)
        .await
        .unwrap();
    assert_eq!(
        first,
        Reconciled::Changed,
        "rebalance must evacuate a chunk resolved out of a `seg:` record",
    );
    // Two chunks share each `seg:` record, and a repoint CASes that record's exact prior
    // bytes, so the second chunk of a record loses its race and is re-planned on the next
    // pass — the drain CONVERGES over passes rather than in one. Bounded, so a pass that
    // stops making progress fails here instead of spinning.
    let mut passes = 1;
    while reconciliation_status(&meta, draining).await.unwrap() != ReconciliationStatus::Satisfied {
        assert!(passes < 8, "the drain did not converge in {passes} passes");
        let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&rebalance), 40)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            Reconciled::Changed,
            "pass {passes} made no progress while the drain is still Pending",
        );
        passes += 1;
    }

    // Positive observables: the segment records no longer name the draining server, the
    // ROOT is untouched (it is the generation fence, not the repoint target), and the
    // vacated fragments carry `orphan:` evidence.
    for index in 0..2u32 {
        let after = meta.get(&seg_key(index)).await.unwrap().unwrap();
        assert_ne!(
            after.as_ref(),
            fixture.segment_bytes[index as usize].as_slice(),
            "segment {index} carries chunks placed on the draining server, so its record \
             must have been repointed",
        );
        assert!(
            !String::from_utf8_lossy(&after).contains(&format!("[{draining},")),
            "segment {index} still names the draining server: {}",
            String::from_utf8_lossy(&after),
        );
    }
    assert_eq!(
        meta.get(&metadata::inode_key(SEGMENTED_INODE))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        fixture.segmented_bytes.as_slice(),
        "the root is not the repoint target: a segmented evacuation CASes the `seg:` \
         record and leaves the root byte-identical",
    );
    for chunk in &fixture.segmented {
        assert!(
            meta.get(&metadata::orphan_key(
                draining,
                FragmentId {
                    chunk: chunk.id,
                    index: 0
                }
            ))
            .await
            .unwrap()
            .is_some(),
            "the vacated position is evidenced by an `orphan:` record",
        );
    }
    // …and the object still reads back whole from its new placement.
    assert_eq!(
        read_object(&meta, &fleet, SEGMENTED_INODE)
            .await
            .unwrap()
            .as_deref(),
        Some(fixture.payload.as_slice()),
    );
    // The drain of that server is now reported `Satisfied` — the same positive surface
    // leg 2 uses, on the other side of the evacuation.
    assert_eq!(
        reconciliation_status(&meta, draining).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "once no committed map names it, the drain is satisfied",
    );
}

/// Backfill's stated decision, driven by the input that DISCRIMINATES it: a segmented
/// chunk whose stored `placement` is empty — the one shape this pass exists to fill.
/// With every placement already full-length the pass short-circuits before its rewrite,
/// so a missing segmented decision would pass unnoticed; here the rewrite is genuinely
/// reachable, and rewriting it would flatten the segmented root into an inline map and
/// strand the `seg:` records.
///
/// The decision has two halves and BOTH are asserted here: the record is left
/// byte-identical, **and** the pass fails explicitly rather than reporting a clean sweep
/// over an entry no pass in the system drains. A count on a gauge is not a decision — an
/// unsupported entry is an error or a queued repair obligation, never a silent skip
/// (`AGENTS.md:175-177`).
#[tokio::test]
async fn backfill_never_rewrites_a_segmented_map_even_with_an_empty_placement() {
    let meta = MemMeta::default();
    let servers = fleet_servers();
    let fleet = fleet_of(&servers);
    let fixture = seed_with(&meta, &fleet, true).await;

    let err = backfill::reconcile(&BackfillContext { meta: &meta })
        .await
        .expect_err("an entry this pass cannot drain must fail it, never be skipped");
    let reported = err.to_string();
    assert!(
        reported.contains("segmented chunk map") && reported.contains(&SEGMENTED_INODE.to_string()),
        "the failure must name the record and WHY it cannot be filled — an actionable \
         decision about a shape this pass understands, not a decode error from one it \
         never did: {reported}",
    );

    assert_eq!(
        meta.get(&metadata::inode_key(SEGMENTED_INODE))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        fixture.segmented_bytes.as_slice(),
        "backfill rewrote a segmented root — the map it would produce is FLAT, which \
         strands every `seg:` record the object's chunks live in",
    );
    assert_eq!(
        meta.get(&seg_key(0)).await.unwrap().unwrap().as_ref(),
        fixture.segment_bytes[0].as_slice(),
        "…and the segment record is not the backfill target either",
    );
    // The flat object in the same store still gets the ordinary treatment: its placements
    // are full-length, so it too is left alone — and it is reached at all, which is why
    // the failure is raised after the sweep rather than at the first record.
    assert_eq!(
        meta.get(&metadata::inode_key(FLAT_INODE))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        fixture.flat_bytes.as_slice(),
    );
}

// ---- leg 7: containment — one damaged object does not take the store down ----

/// **A per-object fault stays a per-object fault.**
///
/// A committed segmented object whose root still names its group while one of its `seg:`
/// records is absent cannot be resolved by anyone. That is an invariant violation and it
/// must fail closed — but *how wide* the failure reaches is the whole question, and
/// "everywhere, uniformly" is the wrong answer: the id-allocator floor is read by
/// `Gateway::recover` before the gateway serves anything
/// (`crates/server/src/lib.rs:123-124`), so a floor that resolved roots would turn one
/// damaged object into a gateway that refuses to start and every *healthy* object
/// becoming unavailable.
///
/// So, in one store holding the healthy flat object, the healthy segmented object and the
/// damaged one:
///
/// * **(a)** `metadata::high_water_marks` answers `Ok`, and its chunk floor is at or above
///   every chunk id any `seg:` record in the store names — total *and* not an
///   under-approximation, since a floor of zero would satisfy totality alone and re-mint
///   live ids (issue #364).
/// * **(b)** both healthy objects still read, byte-for-byte.
/// * **(c)** a read of the damaged object is an error — never bytes, never `None` (which
///   would be a 404 for an object that exists, i.e. silent loss). Its *typedness* is
///   asserted where the type is nameable: the co-located gateway test
///   `a_damaged_segmented_object_fails_closed_without_taking_the_store_down`.
/// * **(d)** nothing of it is reclaimed: the fragments its readable segment names are
///   still on disk after a restore pass and a GC pass past the grace window. Whether the
///   deletion-capable pass aborts or completes while protecting them is an implementation
///   choice; deleting is not.
#[tokio::test]
async fn a_damaged_segmented_object_never_costs_the_store_its_other_objects() {
    let meta = MemMeta::default();
    let servers = fleet_servers();
    let fleet = fleet_of(&servers);
    let fixture = seed(&meta, &fleet).await;
    // Three damaged objects, in the three spellings damage has: a record that is ABSENT,
    // a segment record that is present and UNREADABLE, and a ROOT that is present and
    // unreadable. The last two fail at decode — a different path from the first, reaching
    // a consumer through a different error — so a containment rule written for one
    // spelling is caught here rather than in production.
    let damaged = [
        seed_damaged(&meta, &fleet).await,
        seed_malformed(&meta, &fleet).await,
    ]
    .concat();
    let dyn_fleet = dyn_fleet_of(&servers);

    // (a) The id floor is TOTAL over a store containing the damaged objects…
    let (max_inode, max_chunk) = metadata::high_water_marks(&meta)
        .await
        .expect("one damaged object must not fail the id floor the gateway starts from");
    assert_eq!(max_inode, MALFORMED_ROOT_INODE);
    // …and it is a floor, not merely an answer: every id in every `seg:` record is at or
    // below it, the damaged object's readable segment included.
    let in_segments = fixture
        .segmented
        .iter()
        .chain(damaged.iter())
        .map(|c| c.id)
        .max()
        .unwrap();
    assert!(
        max_chunk >= in_segments,
        "the id floor ({max_chunk}) is below a chunk id a seg: record still names \
         ({in_segments}): the allocator would re-mint it over live fragments",
    );

    // (b) Every healthy object still reads, whole and byte-identical.
    assert_eq!(
        read_object(&meta, &fleet, SEGMENTED_INODE)
            .await
            .unwrap()
            .as_deref(),
        Some(fixture.payload.as_slice()),
        "a damaged object in the same store must not cost a healthy segmented object its \
         availability",
    );
    assert_eq!(
        read_object(&meta, &fleet, FLAT_INODE)
            .await
            .unwrap()
            .as_deref(),
        Some(fixture.flat_payload.as_slice()),
    );

    // (c) …and each damaged object fails closed, for itself alone — in every spelling.
    for inode in [DAMAGED_INODE, MALFORMED_SEGMENT_INODE, MALFORMED_ROOT_INODE] {
        match read_object(&meta, &fleet, inode).await {
            Err(_) => {}
            Ok(other) => panic!(
                "a read of an unresolvable map (inode {inode}) must fail closed, never \
                 answer bytes or absence: {other:?}",
            ),
        }
    }

    // (d) Nothing of it is reclaimed. The passes run for real; whether they abort (fail
    // closed, the shape `decode(&value)?` already has today) or complete while treating
    // the damaged object as fully referenced is not asserted — deleting is.
    let before = present(&fleet, &damaged).await;
    assert_eq!(
        before.len(),
        damaged.len() * 3,
        "fixture: every RS(2,1) fragment of the damaged object's readable segment is on \
         disk to begin with",
    );
    let gc = GcContext {
        meta: &meta,
        fleet: &dyn_fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    let coord = MemCoordination::default();
    let (zone, custodian) = elect(&coord, "zone-damaged").await;
    let _ = reconcile_after_restore(&gc, 20).await;
    let _ = reconcile_step(
        &zone,
        &custodian,
        Some(&gc),
        None,
        None,
        None,
        20 + GRACE * 10,
    )
    .await;

    assert_eq!(
        present(&fleet, &damaged).await,
        before,
        "a pass reclaimed fragments of a damaged object's readable segment — an \
         incomplete reference set may not authorize any reclamation",
    );

    // (e) …and the drain surface — which is READ per D server and can delete nothing —
    // keeps answering for every server in the store. It may not certify (the reference
    // set the answer is computed from is incomplete while one object cannot be resolved),
    // and it may not fail either: an `Err` here would take the whole fleet's drain status
    // down for one damaged object, which is the blast radius this leg exists to bound.
    // The healthy segmented object's holder is asserted as well as an unrelated server,
    // so "never Satisfied" is not bought by the damage alone.
    for server in [fixture.segmented[0].placed_dserver(0), 9] {
        set_lifecycle(&meta, server, DServerLifecycle::Draining)
            .await
            .unwrap();
        let status = reconciliation_status(&meta, server)
            .await
            .expect("one damaged object must not blank the drain surface for the fleet");
        assert_ne!(
            status,
            ReconciliationStatus::Satisfied,
            "a drain was certified while an object's map cannot be resolved: its chunks \
             are unknown, so nothing can show they are not on this server",
        );
    }
    // Server 9 holds nothing at all, so the ONLY thing keeping it from `Satisfied` is the
    // damaged object — and the answer has to say so, or the stall is unexplained and an
    // operator has nothing to repair. Asserted through the rendering rather than the
    // variant, which is a symbol this slice adds and this file may not name (see the
    // module docs); the typed assertion is
    // `a_drain_stays_blocked_and_attributed_while_a_map_cannot_be_resolved`
    // (`crates/custodian/tests/rebalance.rs`).
    let unheld = reconciliation_status(&meta, 9).await.unwrap();
    for inode in [DAMAGED_INODE, MALFORMED_SEGMENT_INODE, MALFORMED_ROOT_INODE] {
        assert!(
            format!("{unheld:?}").contains(&format!("inode:{inode}")),
            "the answer must ATTRIBUTE every blocking object — inode {inode} is missing \
             from it, so an operator has one fewer record to repair than the store has: \
             {unheld:?}",
        );
    }
    assert_eq!(
        present(&fleet, &fixture.segmented).await.len(),
        fixture.segmented.len() * 3,
        "…and the healthy segmented object's fragments are untouched too",
    );
    assert_eq!(
        present(&fleet, &fixture.flat).await.len(),
        fixture.flat.len() * 3,
    );
}

#[tokio::test]
async fn backfill_resolves_a_segmented_map_and_leaves_it_byte_identical() {
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..6).map(|_| MemDServer::default()).collect();
    let fleet = Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u64, s))
            .collect(),
    };
    let fixture = seed(&meta, &fleet).await;

    // Backfill is a `.chunk_map` consumer the issue body omits: it iterates the map AND
    // rewrites it (`crates/custodian/src/backfill.rs:76-130`). Its stated decision for a
    // segmented map is resolve-then-skip, and the record must come out untouched.
    let outcome = backfill::reconcile(&BackfillContext { meta: &meta })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "every record here already carries a full-length placement, so backfill has \
         nothing to fill — and a segmented map is never mangled into an inode rewrite",
    );
    assert_eq!(
        meta.get(&metadata::inode_key(SEGMENTED_INODE))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        fixture.segmented_bytes.as_slice(),
        "backfill must leave a segmented root byte-identical",
    );
    assert_eq!(
        meta.get(&seg_key(0)).await.unwrap().unwrap().as_ref(),
        segment_record(&fixture.segmented[..2], 0).as_slice(),
        "…and its segment records too",
    );
}

// ---- leg 7, second half: the passes leg 7 does not dispatch keep working too ----
//
// Leg 7 above pins what a damaged object may not COST: no deletion, no lost availability,
// no blanked drain surface. It exercises GC, restore and the drain surface. The three
// remaining `scan("inode:")` passes — backfill, rebalance's evacuation planner and
// reconstruction's store-wide lookup — each walk every committed record on their own, and
// each would abort at the damaged one and never reach the healthy records after it. That
// is a per-object fault converted into a fleet-wide stall: an empty-placement drain that
// never drains, a decommission that never completes, and a queued repair for a healthy
// under-replicated chunk that is never even assessed. The three tests below assert the
// positive observable in each pass — the work on the HEALTHY object actually happened —
// with the damaged object sitting in the same store.

/// The flat object's stored record, as text — the surface these legs assert over, since
/// the decoded shape names a type this file may not (see the module docs).
async fn flat_record(meta: &MemMeta) -> String {
    let bytes = meta
        .get(&metadata::inode_key(FLAT_INODE))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Every `placement` vector in a stored record, parsed out of its JSON. Parsed rather than
/// substring-matched: a D-server id can appear inside a chunk id or a length, and an
/// assertion that answered "still draining" to `"id":513` would pass or fail for reasons
/// that have nothing to do with placement.
fn placements_of(record: &str) -> Vec<Vec<DServerId>> {
    record
        .split(r#""placement":["#)
        .skip(1)
        .filter_map(|rest| rest.split(']').next())
        .map(|list| {
            list.split(',')
                .filter(|token| !token.is_empty())
                .map(|token| token.parse().expect("a placement entry is a D-server id"))
                .collect()
        })
        .collect()
}

/// A committed flat object whose chunks carry an EMPTY `placement` — the pre-M3 spelling
/// backfill exists to fill, staged on servers 0,1,2 so the identity fill it materializes
/// names the servers the fragments are really on.
const FILLABLE_INODE: InodeId = 6;

async fn seed_fillable_flat(meta: &MemMeta, fleet: &Fleet<'_>) -> Vec<ChunkRef> {
    let payload = data(2 * CHUNK_SIZE, 0x70);
    let chunks = stage_object(fleet, &payload, &segmented_topology(), 0x600).await;
    let stored: Vec<ChunkRef> = chunks
        .iter()
        .cloned()
        .map(|mut chunk| {
            chunk.placement = Vec::new();
            chunk
        })
        .collect();
    commit(
        meta,
        WriteBatch::new().put(
            metadata::inode_key(FILLABLE_INODE),
            flat_root(&stored, payload.len() as u64),
        ),
    )
    .await;
    chunks
}

/// **Backfill drains the store it CAN read.**
///
/// The pass scans every committed record and materializes the identity placement of each
/// pre-M3 chunk. One damaged object in that scan must not stop the drain of every other
/// record — nor suppress the remaining-population gauge an operator watches the drain by —
/// and must not be reported as swept either: the object was skipped, so the pass says so.
#[tokio::test]
async fn backfill_fills_a_healthy_record_while_one_object_cannot_be_read() {
    let meta = MemMeta::default();
    let servers = fleet_servers();
    let fleet = fleet_of(&servers);
    seed(&meta, &fleet).await;
    // Exactly ONE damaged object, so the population the pass reports is deterministic.
    seed_damaged(&meta, &fleet).await;
    let fillable = seed_fillable_flat(&meta, &fleet).await;

    let err = backfill::reconcile(&BackfillContext { meta: &meta })
        .await
        .expect_err("a record the pass could not read may not be reported as swept");

    // The positive observable, and the whole point: the healthy record was FILLED. A pass
    // that propagated the damaged object's fault would have returned before touching it —
    // whichever order the scan happened to visit them in, since the fault ends the walk.
    let filled = meta
        .get(&metadata::inode_key(FILLABLE_INODE))
        .await
        .unwrap()
        .unwrap();
    let filled = String::from_utf8_lossy(&filled);
    assert!(
        filled.contains(r#""placement":[0,1,2]"#),
        "backfill did not materialize the empty placement of a HEALTHY record while \
         another object in the store could not be read: one damaged object stopped the \
         drain of the whole store ({filled})",
    );
    assert!(
        !filled.contains(r#""placement":[]"#),
        "…and no chunk of it was left empty: {filled}",
    );

    // …and the skip is reported, not silent: an entry a maintenance pass cannot act on is
    // an explicit error, never a count that reads as a clean sweep (`AGENTS.md:175-177`).
    let reported = err.to_string();
    assert!(
        reported.contains(&format!("inode:{DAMAGED_INODE}")),
        "the failure must ATTRIBUTE the object that could not be read, or an operator has \
         nothing to repair: {reported}",
    );
    assert!(
        reported.contains('1'),
        "…and report the SIZE of the population, not merely that one exists: {reported}",
    );

    // Nothing of the damaged object was rewritten on the way past it.
    assert!(
        meta.get(&seg_key_of(DAMAGED_NONCE, DAMAGED_EPOCH, 1))
            .await
            .unwrap()
            .is_none(),
        "fixture: the damaged object's second segment is still absent",
    );
    for chunk in &fillable {
        assert_eq!(
            present(&fleet, std::slice::from_ref(chunk)).await.len(),
            chunk.fragment_count() as usize,
            "backfill moves no bytes: every fragment of the filled record is untouched",
        );
    }
}

/// **A decommission completes for the servers the damaged object has nothing to do with.**
///
/// Rebalance plans an evacuation per committed chunk with a fragment on a draining server.
/// Aborting the plan walk at a damaged record leaves every *other* object's fragments
/// parked on the draining server forever — a decommission that can never finish because
/// one unrelated object is corrupt. The drain of a server the damaged object DOES sit on
/// still refuses to certify: the two halves are asserted together, because a containment
/// that made progress by lowering the bar would satisfy the first alone.
#[tokio::test]
async fn rebalance_evacuates_a_healthy_object_while_another_cannot_be_read() {
    let meta = MemMeta::default();
    let servers = fleet_servers();
    let fleet = fleet_of(&servers);
    let fixture = seed(&meta, &fleet).await;
    seed_damaged(&meta, &fleet).await;
    let dyn_fleet = dyn_fleet_of(&servers);

    // Drain a server the FLAT object sits on (3,4,5): the damaged object's fragments are on
    // 0,1,2, so nothing about this evacuation needs the record that cannot be read.
    let draining = fixture.flat[0].placed_dserver(0);
    assert!(
        (3..6).contains(&draining),
        "fixture: the flat object sits on 3..6"
    );
    set_lifecycle(&meta, draining, DServerLifecycle::Draining)
        .await
        .unwrap();

    let topology = whole_topology();
    let rebalance = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topology,
    };
    let coord = MemCoordination::default();
    let (zone, custodian) = elect(&coord, "zone-contained-drain").await;

    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&rebalance), 40)
        .await
        .expect("one unreadable object must not end evacuation planning for the fleet");
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "rebalance evacuated nothing while a healthy object had fragments on a draining \
         server: the damaged record stopped the walk before reaching it",
    );

    // The positive observable: the healthy object's record no longer names the draining
    // server anywhere, and its vacated position carries `orphan:` evidence. Both chunks of
    // that record share one root, and a repoint CASes the root's exact prior bytes, so the
    // second loses its race and is re-planned next pass — the drain CONVERGES over passes
    // (the same shape `rebalance_evacuates_a_chunk_that_lives_in_a_segment_record` bounds),
    // and the bound is what makes a pass that stops making progress fail here.
    let mut passes = 1;
    while placements_of(&flat_record(&meta).await)
        .iter()
        .any(|placement| placement.contains(&draining))
    {
        assert!(
            passes < 8,
            "the drain of a healthy object did not converge in {passes} passes: {}",
            flat_record(&meta).await,
        );
        let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&rebalance), 40)
            .await
            .expect("one unreadable object must not end evacuation planning for the fleet");
        assert_eq!(
            outcome,
            Reconciled::Changed,
            "pass {passes} moved nothing while a healthy fragment was still on the \
             draining server",
        );
        passes += 1;
    }
    assert!(
        meta.get(&metadata::orphan_key(
            draining,
            FragmentId {
                chunk: fixture.flat[0].id,
                index: 0
            }
        ))
        .await
        .unwrap()
        .is_some(),
        "the vacated position must be evidenced by an `orphan:` record",
    );

    // …and the drain of a server the DAMAGED object sits on is never certified while its
    // map cannot be read: progress for the readable store, no certification over the
    // unreadable part of it.
    let blocked = fixture.segmented[0].placed_dserver(0);
    set_lifecycle(&meta, blocked, DServerLifecycle::Draining)
        .await
        .unwrap();
    assert_ne!(
        reconciliation_status(&meta, blocked).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "a drain was certified while an object's chunk map cannot be read: its chunks are \
         unknown, so nothing can show they are not on this server",
    );
}

/// **A queued repair for a healthy chunk is still assessed — and one for an unreadable
/// object is never drained.**
///
/// Reconstruction resolves a queued obligation by scanning every committed record for the
/// chunk. Aborting that lookup at a damaged record starves every repair whose chunk sorts
/// after it; draining the obligation because the chunk "was not found" is worse — the
/// chunk may live in exactly the map that could not be read, and its repair would be
/// retired while it is still under-replicated.
#[tokio::test]
async fn reconstruction_repairs_a_healthy_chunk_and_keeps_an_unreadable_objects_obligation() {
    let meta = MemMeta::default();
    let servers = fleet_servers();
    let fleet = fleet_of(&servers);
    let fixture = seed(&meta, &fleet).await;
    let damaged = seed_damaged(&meta, &fleet).await;
    let dyn_fleet = dyn_fleet_of(&servers);

    // A healthy chunk loses a fragment and its repair is queued…
    let victim = fixture.segmented[3].clone();
    let frag = FragmentId {
        chunk: victim.id,
        index: 0,
    };
    servers[victim.placed_dserver(0) as usize]
        .delete_fragment(frag)
        .await
        .unwrap();
    repair::enqueue_repair(&meta, victim.id, "test")
        .await
        .unwrap();
    // …and so is a repair for a chunk that lives in the object nobody can read.
    let stranded = damaged[0].clone();
    repair::enqueue_repair(&meta, stranded.id, "test")
        .await
        .unwrap();
    let stranded_before = present(&fleet, std::slice::from_ref(&stranded)).await;

    let topology = domains(&[(1, "B"), (2, "C"), (3, "D")]);
    let reconstruction = ReconstructionContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topology,
        unreachable: &[],
    };
    let coord = MemCoordination::default();
    let (zone, custodian) = elect(&coord, "zone-contained-repair").await;

    let outcome = reconcile_step(
        &zone,
        &custodian,
        None,
        None,
        Some(&reconstruction),
        None,
        30,
    )
    .await
    .expect("one unreadable object must not end the store-wide lookup for every repair");
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "no repair was performed while a healthy chunk was under-replicated: the damaged \
         record ended the lookup before the chunk was found",
    );
    assert!(
        fleet.get_fragment(frag).await.unwrap().is_some(),
        "the healthy chunk's lost fragment must be rebuilt",
    );
    assert!(
        meta.get(&repair::repair_key(victim.id))
            .await
            .unwrap()
            .is_none(),
        "…and its obligation drained by the repair that completed it",
    );

    // The safety half: the obligation for a chunk of the UNREADABLE object survives. "Not
    // found" over the records the pass could read is not "deleted", and retiring it would
    // silently drop the repair of a chunk that is still under-replicated.
    assert!(
        meta.get(&repair::repair_key(stranded.id))
            .await
            .unwrap()
            .is_some(),
        "a repair obligation for a chunk whose object could not be read was drained: the \
         pass retired a repair it never assessed",
    );
    assert_eq!(
        present(&fleet, std::slice::from_ref(&stranded)).await,
        stranded_before,
        "…and nothing of that object's fragments was moved or reclaimed",
    );
}
