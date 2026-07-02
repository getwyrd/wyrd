//! M3.7 (issue #145, proposal 0005 slice 7, `0005:537-540`): the **rebalance custodian
//! loop** — declarative drain/decommission evacuation + the capacity plane — driven
//! through the real [`reconcile_step`] fenced control point.
//!
//! The BINDING legs of the success criterion, proven in-process over the trait stores
//! (Option A — no deployed custodian process exists yet, `0005:519-523`):
//!
//! 1. **Drain-and-evacuate, spread preserved** (`0005:297-303`, the central DoD): an
//!    operator writes desired state marking a D server draining; through `reconcile_step`
//!    the custodian evacuates that server's referenced fragment onto a healthy D server
//!    in a **distinct failure domain** via the **same commit-point-atomic,
//!    version-conditional `MetadataStore::commit` re-place as a reconstruction**
//!    (`0005:298-299`, `0005:486`) — after which the drained server holds **no referenced
//!    fragment** and the chunk retains **full redundancy across `n` distinct domains**.
//!    Flippable (recorded in build-notes): skip the desired-state read in
//!    `rebalance::reconcile` (treat the drain as absent) and nothing is evacuated — the
//!    assertions here fire.
//! 2. **"Changed" vs "satisfied" are distinct, observable moments** (`0005:351-352`): the
//!    operator write records desired state (changed → `Pending`); after reconciliation
//!    reality matches (satisfied → `Satisfied`).
//! 3. **Spread wins** (`0005:302-303`, durability is gate-zero): where the move cannot
//!    keep the chunk on `n` distinct domains, the evacuation is **refused** rather than
//!    collapse the spread — the fragment stays put.
//! 4. **Per-failure-domain utilization** is emitted on the `DurabilityTelemetry` seam
//!    (`0005:341-343`, ADR-0011/0012) and read back in-process via `gather_prometheus`.
//! 5. **Multi-fragment evacuation in ONE commit** (`0005:298`, the distinctness invariant
//!    `0005:491`): when **two** servers holding fragments of the *same* chunk are drained
//!    at once, the loop re-places **both** evacuated fragments — each into a domain
//!    distinct from the survivor and from each other — in a **single** version-conditional
//!    commit, so the chunk keeps `n` distinct domains. This exercises the `evac.len() > 1`
//!    leg of `select_distinct_domains_excluding` (`count = 2`).
//! 6. **Lost-CAS: a racing writer loses rather than corrupts the record** (`0005:298-299`,
//!    `0005:486`; ADR-0015 the second fence): when a concurrent inode mutation lands
//!    between the loop's read (`plan_evacuations`) and its commit, the version-conditional
//!    `MetadataStore::commit` precondition fails — the custodian's repoint is **rejected**,
//!    the placement record reflects the racing writer (not the custodian), the copied
//!    fragment is left as **collectable garbage** (no orphan record, no torn chunk), and
//!    the conflict is surfaced on the durability seam.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_core::placement::Topology;
use wyrd_core::read::read_object;
use wyrd_core::write::{plan_write, write_fragments, write_new_object_placed};
use wyrd_custodian::{
    reconcile_step, reconciliation_status, set_lifecycle, Custodian, DServerLifecycle,
    DurabilityTelemetry, ExporterConfig, FencedZone, RebalanceContext, Reconciled,
    ReconciliationStatus,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the loop is proven over the seams) ----

/// A trivial in-memory metadata store (with version-conditional commit).
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

/// One D server's fragment bytes — a deliberately dumb `ChunkStore` holding the **real**
/// stored fragment bytes (so their checksums verify and the moved fragment round-trips).
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

/// A **placement-aware** fleet over several [`MemDServer`]s: it routes `_at` calls to the
/// D server the placement record names, so the read path (and the write fan-out) resolve
/// each fragment from its recorded location — the seam a custodian re-placement flips.
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

// ---- helpers ----

const ROOT: InodeId = 0;
const CHUNK: ChunkId = 0xC0FFEE;

fn frag(index: u16) -> FragmentId {
    FragmentId {
        chunk: CHUNK,
        index,
    }
}

/// A four-domain topology A..D (servers 0..3). `select_distinct_domains` places a 3-wide
/// chunk on the first three domains A,B,C → servers 0,1,2 (all util 0, lowest labels).
fn four_domains() -> Topology {
    let mut t = Topology::default();
    t.register(0, "A")
        .register(1, "B")
        .register(2, "C")
        .register(3, "D");
    t
}

/// A five-domain topology A..E (servers 0..4). `select_distinct_domains` still places a
/// 3-wide chunk on the first three domains A,B,C → servers 0,1,2 (all util 0, lowest
/// labels); the two spare domains D,E are the re-placement pool when **two** of the
/// chunk's servers are drained at once.
fn five_domains() -> Topology {
    let mut t = Topology::default();
    t.register(0, "A")
        .register(1, "B")
        .register(2, "C")
        .register(3, "D")
        .register(4, "E");
    t
}

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-rebalance").await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

async fn read_inode(meta: &MemMeta) -> InodeRecord {
    let bytes = meta
        .get(&metadata::inode_key(1))
        .await
        .unwrap()
        .expect("inode present");
    metadata::decode(&bytes).unwrap()
}

/// Write one RS(2,1) chunk via the real write path, placed across distinct domains: n =
/// 3 fragments on servers 0,1,2 (domains A,B,C). Returns the original object bytes.
async fn write_rs_2_1(meta: &MemMeta, fleet: &Fleet<'_>, topo: &Topology) -> Vec<u8> {
    let data = b"evacuate this erasure-coded chunk, every byte of it".to_vec();
    let outcome = write_new_object_placed(
        meta,
        fleet,
        ROOT,
        "obj",
        1,
        &data,
        data.len(),
        EcScheme::ReedSolomon { k: 2, m: 1 },
        topo,
        0,
        1_000,
        || CHUNK,
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    assert_eq!(
        read_inode(meta).await.chunk_map[0].placement,
        vec![0, 1, 2],
        "RS(2,1) placed across distinct domains A,B,C (servers 0,1,2)"
    );
    data
}

/// Write one chunk's fragments to the fleet at IDENTITY locations (fragment `i` lands
/// on D-server `i`) and commit its `InodeRecord` with an **empty** `ChunkRef.placement`
/// — the genuine pre-M3 / mixed-era shape (`#[serde(default)]`, `core/src/metadata.rs:93`),
/// which every placement-consuming path resolves via the identity-placement fallback
/// (`ChunkRef::placed_dserver`, `core/src/metadata.rs:119`).
///
/// Deliberately bypasses `write_new_object[_placed]`: every live writer always emits a
/// full, explicit placement vector now (`core/src/write.rs:171`), so the only way to
/// reproduce a pre-M3 record is to construct it directly — exactly the shape a chunk
/// committed before the `placement` field shipped decodes to today.
async fn write_pre_m3_chunk(meta: &MemMeta, fleet: &Fleet<'_>, scheme: EcScheme) -> Vec<u8> {
    let data = b"a pre-M3 / mixed-era chunk: empty placement, identity-resolved fragments".to_vec();
    let plan = plan_write(&data, data.len(), scheme, || CHUNK).unwrap();
    write_fragments(fleet, &plan).await.unwrap();
    let mut chunk_refs = plan.chunk_refs();
    assert_eq!(chunk_refs.len(), 1, "one chunk for this small an object");
    chunk_refs[0].placement = Vec::new(); // the pre-M3 shape: decoded via #[serde(default)]
    let record = InodeRecord {
        size: plan.size,
        chunk_map: chunk_refs,
        state: InodeState::Committed,
        version: 1,
    };
    let outcome = metadata::create(meta, ROOT, "obj", 1, &record)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    data
}

// ---- #346: rebalance must apply the identity-placement fallback to a pre-M3 chunk ----
//
// BINDING success criterion (issue #346 / ADR-0040 decisions 1, 2, 4, 5): committing an
// inode whose `ChunkRef.placement` is `vec![]` and whose identity-resolved D server is
// draining MUST still produce and commit an `EvacPlan` that (a) moves the fragment off
// the draining server to a healthy, non-draining, distinct-domain server, and (b) writes
// back a placement record that is FULL-LENGTH (`== fragment_count()`), never the raw
// empty/short vector. Pre-fix, `plan_evacuations` iterates the raw (empty) `placement`
// vector: `evac` comes back empty, the chunk is silently skipped, and `Reconciled` stays
// `Satisfied` with the chunk still wholly unprotected on the draining server — these
// tests assert `Reconciled::Changed` and a full-length, repointed placement instead.

/// `EcScheme::None` leg: the single fragment lives at index 0.
#[tokio::test]
async fn evacuates_a_pre_m3_chunk_with_empty_placement_ec_none() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3)],
    };
    let topo = four_domains();
    let data = write_pre_m3_chunk(&meta, &fleet, EcScheme::None).await;

    // Sanity: the committed record really is the pre-M3 shape — an EMPTY placement —
    // yet the identity fallback resolves fragment 0 to server 0, which is where the
    // bytes actually live.
    let prior = read_inode(&meta).await;
    assert!(
        prior.chunk_map[0].placement.is_empty(),
        "pre-M3 record: an empty, not full-length, placement vector"
    );
    assert_eq!(prior.chunk_map[0].placed_dserver(0), 0);

    // The operator marks the IDENTITY-resolved server (0) draining.
    set_lifecycle(&meta, 0, DServerLifecycle::Draining)
        .await
        .unwrap();
    assert_eq!(
        reconciliation_status(&meta, 0).await.unwrap(),
        ReconciliationStatus::Pending,
        "server 0 still (identity-)resolves a referenced fragment"
    );

    let dyn_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "BINDING (#346): a plan IS produced and committed for the pre-M3 chunk — pre-fix \
         the raw-vector scan sees an empty `placement`, `evac` comes back empty, and the \
         chunk is silently `continue`-skipped (Satisfied, nothing moved)"
    );

    // The committed placement is FULL-LENGTH (== fragment_count() == 1) and the moved
    // index no longer names the draining server.
    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![1],
        "full-length (fragment_count() == 1) placement, repointed off server 0 onto the \
         one free distinct domain (B / server 1) — never an empty or short vector"
    );
    assert_eq!(
        reconciliation_status(&meta, 0).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "server 0 is no longer referenced by any committed placement"
    );

    // The fragment is intact at its new home, the old one is orphaned, and the object
    // still reads correctly (full redundancy, atomic flip — no panic, no corruption).
    assert!(
        wyrd_core::repair::fragment_intact(
            &d1.get_fragment(frag(0))
                .await
                .unwrap()
                .expect("moved fragment present"),
            CHUNK
        ),
        "the evacuated fragment verifies its checksum at its new home (server 1)"
    );
    assert!(
        !meta.scan(b"orphan:").await.unwrap().is_empty(),
        "the displaced fragment on the draining server is orphaned for GC"
    );
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object still reads correctly after the evacuation"
    );
}

/// `ReedSolomon` leg: the draining fragment sits at index > 0 (index 1 of 3), exercising
/// both the evacuation selection AND the `survivor_domains` resolution (ADR-0040 scope:
/// "Apply the same resolution to the `survivor_domains` computation") over the FULL,
/// identity-resolved fragment set rather than the (empty) raw vector.
#[tokio::test]
async fn evacuates_a_pre_m3_chunk_with_empty_placement_reed_solomon_index_gt_zero() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3)],
    };
    let topo = four_domains();
    let data = write_pre_m3_chunk(&meta, &fleet, EcScheme::ReedSolomon { k: 2, m: 1 }).await;

    let prior = read_inode(&meta).await;
    assert!(
        prior.chunk_map[0].placement.is_empty(),
        "pre-M3 record: an empty, not full-length, placement vector"
    );
    // Identity fallback: fragment i -> server i. Drain server 1 (fragment index 1, NOT
    // the first fragment — the brief's "index > 0" leg).
    assert_eq!(prior.chunk_map[0].placed_dserver(1), 1);

    set_lifecycle(&meta, 1, DServerLifecycle::Draining)
        .await
        .unwrap();
    assert_eq!(
        reconciliation_status(&meta, 1).await.unwrap(),
        ReconciliationStatus::Pending,
        "server 1 still (identity-)resolves a referenced fragment"
    );

    let dyn_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "BINDING (#346): a plan is produced and committed for the pre-M3 RS(2,1) chunk \
         with the draining fragment at index > 0"
    );

    // FULL-LENGTH committed placement (== fragment_count() == k+m == 3): survivors 0 and
    // 2 stay identity-resolved, the evacuated index 1 is repointed off server 1 onto the
    // one free distinct domain (D / server 3) — never an empty or short vector.
    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 3, 2],
        "full-length placement: survivors identity-resolved, the draining index moved"
    );

    // Spread preserved: n = 3 fragments still span n = 3 DISTINCT failure domains — only
    // possible because `survivor_domains` was resolved through the identity fallback too
    // (over the raw empty vector it would see no survivor domains at all).
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        3,
        "n fragments on n distinct failure domains"
    );

    assert_eq!(
        reconciliation_status(&meta, 1).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "server 1 is no longer referenced by any committed placement"
    );
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object still reads correctly after the evacuation (full redundancy)"
    );
}

// ---- issue #349 (mixed-era placement matrix): the brief's required RS{6,3} cell,
//      alongside the full RS{2,1} criterion-1+2 case below and the pre-M3 empty-
//      placement cases above — closing the scheme-size gap (this suite's evacuation
//      coverage existed only at RS{2,1}).

/// A ten-domain topology A..J (servers 0..9). An rs(6,3) chunk (n=9) placed across the
/// first nine domains A..I (servers 0..8); domain J (server 9) is the free spare.
fn ten_domains() -> Topology {
    let mut t = Topology::default();
    for (id, label) in [
        (0, "A"),
        (1, "B"),
        (2, "C"),
        (3, "D"),
        (4, "E"),
        (5, "F"),
        (6, "G"),
        (7, "H"),
        (8, "I"),
        (9, "J"),
    ] {
        t.register(id, label);
    }
    t
}

/// Write one rs(6,3) chunk via the real write path, placed across nine distinct
/// domains (servers 0..8). Returns the original object bytes.
async fn write_rs_6_3(meta: &MemMeta, fleet: &Fleet<'_>, topo: &Topology) -> Vec<u8> {
    let data = b"evacuate this rs(6,3) chunk, every one of its nine fragments".to_vec();
    let outcome = write_new_object_placed(
        meta,
        fleet,
        ROOT,
        "obj",
        1,
        &data,
        data.len(),
        EcScheme::ReedSolomon { k: 6, m: 3 },
        topo,
        0,
        1_000,
        || CHUNK,
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    assert_eq!(
        read_inode(meta).await.chunk_map[0].placement,
        (0u64..9).collect::<Vec<_>>(),
        "rs(6,3) placed across distinct domains A..I (servers 0..8)"
    );
    data
}

/// Drain D server 4 (domain E) of a FULL rs(6,3) placement: the loop must evacuate
/// fragment index 4 onto the one free distinct domain, J (server 9), keeping the
/// chunk on n=9 distinct domains throughout.
#[tokio::test]
async fn drains_a_d_server_and_evacuates_an_rs_6_3_chunk_to_a_distinct_domain() {
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..10).map(|_| MemDServer::default()).collect();
    let fleet = Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as DServerId, s))
            .collect(),
    };
    let topo = ten_domains();
    let data = write_rs_6_3(&meta, &fleet, &topo).await;

    // The operator marks D server 4 (domain E) DRAINING.
    set_lifecycle(&meta, 4, DServerLifecycle::Draining)
        .await
        .unwrap();
    assert_eq!(
        reconciliation_status(&meta, 4).await.unwrap(),
        ReconciliationStatus::Pending,
        "policy changed but not yet satisfied: server 4 still holds a referenced fragment"
    );

    let dyn_fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
        .iter()
        .enumerate()
        .map(|(i, s)| (i as DServerId, s as &dyn ChunkStore))
        .collect();
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the draining server's rs(6,3) fragment was evacuated"
    );

    assert_eq!(
        reconciliation_status(&meta, 4).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "policy satisfied: server 4 is no longer referenced by any committed placement"
    );

    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 1, 2, 3, 9, 5, 6, 7, 8],
        "fragment 4 evacuated off server 4 onto the one free distinct domain J (server 9)"
    );
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        9,
        "n=9 fragments on n=9 distinct failure domains"
    );

    assert!(
        wyrd_core::repair::fragment_intact(
            &servers[9]
                .get_fragment(frag(4))
                .await
                .unwrap()
                .expect("moved fragment present"),
            CHUNK
        ),
        "the evacuated fragment verifies its checksum at its new home (server 9)"
    );
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object still reads correctly after the rs(6,3) evacuation"
    );
}

// ---- criterion 1+2: drain a D server, evacuate to a distinct domain, observe moments ----

#[tokio::test]
async fn drains_a_d_server_and_evacuates_to_a_distinct_domain_through_reconcile_step() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3)],
    };
    let topo = four_domains();
    let data = write_rs_2_1(&meta, &fleet, &topo).await;

    // The operator marks D server 1 (domain B) DRAINING — the "policy changed" moment.
    set_lifecycle(&meta, 1, DServerLifecycle::Draining)
        .await
        .unwrap();
    assert_eq!(
        reconciliation_status(&meta, 1).await.unwrap(),
        ReconciliationStatus::Pending,
        "policy changed but not yet satisfied: server 1 still holds a referenced fragment"
    );

    // Reconcile through the real fenced control point. Server 1 is alive (graceful drain),
    // so the topology/fleet still include it; the loop excludes it as a re-placement
    // target and copies its fragment to the one free distinct domain, D (server 3).
    let dyn_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the draining server's fragment was evacuated"
    );

    // "Policy satisfied": reality now matches the desired state — server 1 holds no
    // referenced fragment. A distinct, observable moment from "changed".
    assert_eq!(
        reconciliation_status(&meta, 1).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "policy satisfied: server 1 is no longer referenced by any committed placement"
    );

    // ONE version-conditional commit: the inode version bumped by EXACTLY one and the
    // placement flipped atomically — fragment 1 now lives on server 3 (domain D).
    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 3, 2],
        "the evacuated fragment was re-placed on a healthy server in a distinct domain"
    );

    // FULL REDUNDANCY across n=3 DISTINCT domains, spread preserved.
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        3,
        "n fragments on n distinct failure domains"
    );

    // The copied fragment is intact at its new home; the displaced fragment is orphaned
    // (a `orphan:` grace record GC reclaims after its window) — collectable garbage, not
    // a referenced fragment.
    assert!(
        wyrd_core::repair::fragment_intact(
            &d3.get_fragment(frag(1))
                .await
                .unwrap()
                .expect("moved fragment present"),
            CHUNK
        ),
        "the evacuated fragment verifies its checksum at its new home (server 3)"
    );
    assert!(
        !meta.scan(b"orphan:").await.unwrap().is_empty(),
        "the displaced fragment on the draining server is orphaned for GC"
    );

    // Reads still succeed and return the same bytes — full redundancy, atomic flip, no
    // torn chunk (readers resolve fragment 1 from its new location, server 3).
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object reads correctly after the drain evacuation (atomic flip)"
    );
}

// ---- criterion 3: spread wins — refuse the move rather than collapse the spread ----

#[tokio::test]
async fn spread_wins_when_no_free_distinct_domain_remains() {
    let meta = MemMeta::default();
    let (d0, d1, d2) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2)],
    };
    // Exactly three domains for a 3-wide chunk — no spare domain to evacuate into.
    let mut topo = Topology::default();
    topo.register(0, "A").register(1, "B").register(2, "C");
    let data = b"three fragments, three domains, no spare".to_vec();
    let outcome = write_new_object_placed(
        &meta,
        &fleet,
        ROOT,
        "obj",
        1,
        &data,
        data.len(),
        EcScheme::ReedSolomon { k: 2, m: 1 },
        &topo,
        0,
        1_000,
        || CHUNK,
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    // Mark server 1 draining. Its domain B has no other server, and A/C are held by the
    // survivors — there is NO free distinct domain. Spread wins: refuse to move.
    set_lifecycle(&meta, 1, DServerLifecycle::Decommissioning)
        .await
        .unwrap();
    let dyn_fleet: [(DServerId, &dyn ChunkStore); 3] = [(0, &d0), (1, &d1), (2, &d2)];
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "no move: collapsing the chunk's spread would violate durability (gate-zero)"
    );
    let record = read_inode(&meta).await;
    assert_eq!(record.version, 1, "the placement record is untouched");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 1, 2],
        "the fragment stays on the draining server rather than collapse the spread"
    );
    assert_eq!(
        reconciliation_status(&meta, 1).await.unwrap(),
        ReconciliationStatus::Pending,
        "the drain remains unsatisfied — surfaced, not silently collapsed"
    );
}

// ---- criterion 4: per-failure-domain utilization on the durability seam, read back ----

#[tokio::test]
async fn emits_per_failure_domain_utilization_on_the_durability_seam() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let dyn_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];

    // Two servers share domain A (10 + 5 → 15); B has 7; D has none (→ 0).
    let mut topo = Topology::default();
    topo.register(0, "A").register(1, "A").register(2, "B");
    topo.register(3, "D");
    topo.set_utilization(0, 10)
        .set_utilization(1, 5)
        .set_utilization(2, 7);
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());

    // A pass with no desired state still emits the capacity plane as a by-product.
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 0)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Satisfied);

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert!(
        exposed.contains("capacity_domain_utilization"),
        "the per-failure-domain capacity metric is exported; got:\n{exposed}"
    );
    for domain in ["domain=\"A\"", "domain=\"B\"", "domain=\"D\""] {
        assert!(
            exposed.contains(domain),
            "per-failure-domain utilization carries a `{domain}` series; got:\n{exposed}"
        );
    }
}

// ---- criterion 5: TWO servers drained at once → both fragments evacuated in ONE commit ----

#[tokio::test]
async fn evacuates_two_drained_servers_of_one_chunk_in_a_single_commit() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3, d4) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3), (4, &d4)],
    };
    let topo = five_domains();
    // RS(2,1): n = 3 fragments on servers 0,1,2 (domains A,B,C). Spare domains D,E.
    let data = write_rs_2_1(&meta, &fleet, &topo).await;

    // The operator marks BOTH server 0 (domain A) and server 1 (domain B) draining — two
    // fragments of the SAME chunk must move at once (the `evac.len() > 1` leg).
    set_lifecycle(&meta, 0, DServerLifecycle::Draining)
        .await
        .unwrap();
    set_lifecycle(&meta, 1, DServerLifecycle::Decommissioning)
        .await
        .unwrap();
    assert_eq!(
        reconciliation_status(&meta, 0).await.unwrap(),
        ReconciliationStatus::Pending,
        "policy changed: server 0 still holds a referenced fragment"
    );
    assert_eq!(
        reconciliation_status(&meta, 1).await.unwrap(),
        ReconciliationStatus::Pending,
        "policy changed: server 1 still holds a referenced fragment"
    );

    let dyn_fleet: [(DServerId, &dyn ChunkStore); 5] =
        [(0, &d0), (1, &d1), (2, &d2), (3, &d3), (4, &d4)];
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "both draining servers' fragments were evacuated"
    );

    // ONE version-conditional commit moved BOTH fragments: the version bumped by EXACTLY
    // one (not two) even though two fragments were re-placed — the move is one atomic
    // repoint of the chunk's placement vector.
    let record = read_inode(&meta).await;
    assert_eq!(
        record.version, 2,
        "exactly ONE version-conditional commit re-placed both evacuated fragments"
    );
    assert_eq!(
        record.chunk_map[0].placement,
        vec![3, 4, 2],
        "fragment 0 → server 3 (domain D), fragment 1 → server 4 (domain E); survivor \
         fragment 2 stays on server 2 (domain C)"
    );

    // The chunk still spans n = 3 DISTINCT failure domains — distinctness preserved across
    // a multi-fragment move (closes the `count = 2` leg of select_distinct_domains_excluding).
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        3,
        "two evacuated fragments land in domains distinct from each other and the survivor"
    );

    // Both drains are now satisfied — neither drained server is referenced any longer.
    assert_eq!(
        reconciliation_status(&meta, 0).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "policy satisfied: server 0 no longer referenced"
    );
    assert_eq!(
        reconciliation_status(&meta, 1).await.unwrap(),
        ReconciliationStatus::Satisfied,
        "policy satisfied: server 1 no longer referenced"
    );

    // Both copied fragments verify their checksums at their new homes.
    assert!(
        wyrd_core::repair::fragment_intact(
            &d3.get_fragment(frag(0))
                .await
                .unwrap()
                .expect("moved fragment 0 present"),
            CHUNK
        ),
        "evacuated fragment 0 verifies at its new home (server 3)"
    );
    assert!(
        wyrd_core::repair::fragment_intact(
            &d4.get_fragment(frag(1))
                .await
                .unwrap()
                .expect("moved fragment 1 present"),
            CHUNK
        ),
        "evacuated fragment 1 verifies at its new home (server 4)"
    );

    // Both displaced fragments are orphaned for GC — collectable garbage, not referenced.
    assert_eq!(
        meta.scan(b"orphan:").await.unwrap().len(),
        2,
        "both displaced fragments (on servers 0 and 1) are orphaned in the same commit"
    );

    // Reads still succeed and return the original bytes (atomic flip, full redundancy).
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object reads correctly after evacuating two of its three fragments"
    );
}

// ---- criterion 6: a racing writer loses the version-conditional commit (lost CAS) ----

/// A [`MetadataStore`] that injects a **single** concurrent inode mutation the first time
/// the custodian attempts a version-conditional commit — modelling a racing writer that
/// supersedes the inode between the rebalance loop's read (`plan_evacuations`) and its
/// commit. The injected write bumps the inode version (placement unchanged) so the
/// custodian's `require(prior)` precondition fails: the custodian loses the CAS rather
/// than corrupting the placement record.
struct RacingMeta {
    inner: MemMeta,
    /// Becomes `true` once the test has finished setup and wants the race injected.
    armed: Mutex<bool>,
    /// Ensures the race is injected exactly once (on the first inode-conditional commit).
    raced: Mutex<bool>,
}

impl RacingMeta {
    fn new() -> Self {
        Self {
            inner: MemMeta::default(),
            armed: Mutex::new(false),
            raced: Mutex::new(false),
        }
    }

    /// Arm the race: subsequent inode-conditional commits trigger one concurrent mutation.
    fn arm(&self) {
        *self.armed.lock().unwrap() = true;
    }
}

#[async_trait]
impl MetadataStore for RacingMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let inject = {
            let armed = *self.armed.lock().unwrap();
            let mut raced = self.raced.lock().unwrap();
            let targets_inode = batch
                .preconditions
                .iter()
                .any(|p| p.key.starts_with(b"inode:"));
            if armed && !*raced && targets_inode {
                *raced = true;
                true
            } else {
                false
            }
        };
        if inject {
            // A racing writer touches the same inode between the loop's read and commit:
            // bump its version (placement unchanged) so the custodian's CAS will miss.
            let key = batch
                .preconditions
                .iter()
                .find(|p| p.key.starts_with(b"inode:"))
                .unwrap()
                .key
                .clone();
            if let Some(bytes) = self.inner.get(&key).await? {
                let mut record: InodeRecord = metadata::decode(&bytes).unwrap();
                record.version += 1;
                let outcome = self
                    .inner
                    .commit(WriteBatch::new().put(key, metadata::encode(&record)))
                    .await?;
                assert_eq!(outcome, CommitOutcome::Committed, "racing writer commits");
            }
        }
        self.inner.commit(batch).await
    }
}

#[tokio::test]
async fn a_racing_writer_loses_the_version_conditional_commit_and_leaves_only_garbage() {
    let racing = RacingMeta::new();
    let (d0, d1, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3)],
    };
    let topo = four_domains();

    // Setup (race disarmed): RS(2,1) on servers 0,1,2 (A,B,C), then mark server 1 draining.
    let data = b"a racing writer must lose, not corrupt the placement record".to_vec();
    let outcome = write_new_object_placed(
        &racing,
        &fleet,
        ROOT,
        "obj",
        1,
        &data,
        data.len(),
        EcScheme::ReedSolomon { k: 2, m: 1 },
        &topo,
        0,
        1_000,
        || CHUNK,
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    set_lifecycle(&racing, 1, DServerLifecycle::Draining)
        .await
        .unwrap();

    // Snapshot the inode the custodian will read and try to CAS on.
    let prior: InodeRecord =
        metadata::decode(&racing.get(&metadata::inode_key(1)).await.unwrap().unwrap()).unwrap();
    assert_eq!(prior.version, 1);
    assert_eq!(prior.chunk_map[0].placement, vec![0, 1, 2]);

    // Arm the race: the next inode-conditional commit (the custodian's evac repoint) will
    // find the inode mutated underneath it.
    racing.arm();

    let dyn_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];
    let ctx = RebalanceContext {
        meta: &racing,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;

    // Capture the durability seam so the lost-CAS conflict is observable as a metric.
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    // No chunk converged: the only candidate move lost its CAS race.
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "the only evacuation lost the CAS, so no placement was repointed this pass"
    );

    // SAFETY: the placement record reflects the RACING WRITER (version bumped, placement
    // untouched), NOT the custodian's repoint. The custodian lost rather than corrupting.
    let record: InodeRecord =
        metadata::decode(&racing.get(&metadata::inode_key(1)).await.unwrap().unwrap()).unwrap();
    assert_eq!(
        record.version, 2,
        "the racing writer's commit landed (version 1 → 2)"
    );
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 1, 2],
        "the placement record is the racing writer's, NOT the custodian's repoint \
         (which would be [0, 3, 2]) — a racing writer loses rather than corrupts"
    );

    // The copied fragment IS at its new home (server 3) — but it is collectable GARBAGE:
    // the atomic commit that would have referenced it and orphaned the source was rejected,
    // so no orphan record exists and the record never points at it.
    assert!(
        d3.get_fragment(frag(1)).await.unwrap().is_some(),
        "the pre-commit copy landed on server 3 — now unreferenced collectable garbage"
    );
    assert!(
        racing.scan(b"orphan:").await.unwrap().is_empty(),
        "the lost commit was atomic: the displaced fragment was NOT orphaned (no torn move)"
    );

    // The drain is still unsatisfied — server 1 remains referenced; reconciliation re-tries.
    assert_eq!(
        reconciliation_status(&racing, 1).await.unwrap(),
        ReconciliationStatus::Pending,
        "the drain is re-assessed next pass; reality has not converged"
    );

    // The conflict is surfaced on the durability seam (emit_conflict).
    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    assert!(
        exposed.contains("rebalance_conflict"),
        "the lost-CAS conflict is emitted on the durability seam; got:\n{exposed}"
    );
}

// ---- malformed committed placement — rebalance skips + NEEDS-HUMAN (issue #348) ----

/// **Issue #348 — rebalance skips a chunk with a malformed committed placement.**
///
/// ADR-0040 decisions 3–4: a committed `placement` that is non-empty but of the wrong
/// length (`len != fragment_count()`) is malformed (truncation / corruption). The liberal
/// `fragments()` expansion would identity-fill the missing tail, so a draining server that
/// the fabricated tail happens to name would trigger an evacuation that **repoints the
/// committed record over an invented placement**. The strict companion `checked_fragments()`
/// rejects the vector first: rebalance **skips** the chunk (leaving the fragment in place)
/// and flags it NEEDS-HUMAN, so the corrupt record is never rewritten.
///
/// Setup: a real RS(2,1) object (placement `[0,1,2]`, fragments on 0,1,2) is truncated to
/// a malformed `placement: [0]`; server 0 (the index-0 fragment's home) is marked draining.
///
/// Pre-fix: `plan_evacuations` expands via `fragments()` → fabricated `[0,1,2]`; index 0
/// names draining server 0 → the chunk is evacuated and the placement is repointed to a
/// full-length vector (the committed record is rewritten over a fabricated placement).
/// Post-fix: the vector classifies malformed → the chunk is skipped, the committed
/// `placement` stays `[0]`, the fragment stays put (no orphan), and the drain stays
/// unsatisfied.
///
/// Flippable: revert `rebalance.rs:plan_evacuations` to expand via `chunk.fragments()` and
/// this goes red (the placement is repointed to length 3 and the fragment is moved).
#[tokio::test]
async fn malformed_placement_rebalance_skips_and_leaves_fragment_in_place() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3)],
    };
    let topo = four_domains();

    // A real, fully-placed RS(2,1) object: placement [0,1,2], fragments on servers 0,1,2.
    write_rs_2_1(&meta, &fleet, &topo).await;

    // Corrupt the committed record into a MALFORMED placement (len 1 != fragment_count 3).
    let mut record = read_inode(&meta).await;
    record.chunk_map[0].placement = vec![0];
    meta.commit(WriteBatch::new().put(metadata::inode_key(1), metadata::encode(&record)))
        .await
        .unwrap();

    // The operator marks server 0 draining — the index-0 fragment's home.
    set_lifecycle(&meta, 0, DServerLifecycle::Draining)
        .await
        .unwrap();

    let dyn_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];
    let ctx = RebalanceContext {
        meta: &meta,
        fleet: &dyn_fleet,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), 500)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "malformed placement: rebalance must move nothing and repoint nothing"
    );

    // The committed placement is UNCHANGED — never rewritten over a fabricated vector.
    let after = read_inode(&meta).await;
    assert_eq!(
        after.chunk_map[0].placement,
        vec![0],
        "malformed placement is left exactly as committed (never repointed) (#348)"
    );
    assert_eq!(
        after.version, record.version,
        "no version-conditional commit landed for a malformed-placement chunk"
    );

    // The fragment is left on the draining server (not evacuated) and nothing is orphaned.
    assert!(
        d0.get_fragment(frag(0)).await.unwrap().is_some(),
        "the index-0 fragment stays on the draining server — no fabricated evacuation (#348)"
    );
    assert!(
        meta.scan(b"orphan:").await.unwrap().is_empty(),
        "no fragment is displaced/orphaned for a malformed-placement chunk"
    );

    // The drain cannot be reported satisfied while a malformed placement is unresolved —
    // AND the stall is ATTRIBUTED in the answer itself (issue #348 rework): the status
    // names the blocking malformed chunk id, so `Pending` is never unexplained. The block
    // is cluster-wide fail-safe (not scoped to servers the corrupt vector names); here no
    // *valid* reference names server 0, so the ONLY reason it stays blocked is the
    // malformed chunk — surfaced as `PendingMalformed { chunks: [CHUNK] }`.
    assert_eq!(
        reconciliation_status(&meta, 0).await.unwrap(),
        ReconciliationStatus::PendingMalformed {
            chunks: vec![CHUNK]
        },
        "a drain stays blocked while a malformed committed placement is unresolved, and \
         the answer attributes the stall to the specific corrupt chunk id (#348)"
    );
}
