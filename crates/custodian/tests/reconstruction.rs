//! M3.6 (issue #144, proposal 0005 slice 6, `0005:531-536`): the **reconstruction
//! custodian loop**, driven through the real [`reconcile_step`] fenced control point.
//!
//! The BINDING legs of the success criterion, proven in-process over the trait stores
//! (Option A — no deployed custodian process exists yet, `0005:524-527`):
//!
//! 1. **Kill-and-reconstruct to full redundancy** (`0005:273-279`, the central DoD):
//!    a D server holding a fragment of an EC-coded chunk is lost, so the chunk goes
//!    under-replicated; through `reconcile_step`, reconstruction gathers any `k`
//!    surviving fragments, rebuilds the missing shard scheme-driven from the chunk's
//!    **per-chunk** `EcScheme`, re-places it on a healthy D server in a **distinct
//!    failure domain**, and repoints the placement record — after which the chunk is
//!    back to **full redundancy** across `n` distinct domains and the obligation is
//!    drained off the shared repair queue ([`wyrd_core::repair`]).
//! 2. **Reads never error throughout the repair** (`0005:31-32`): the object reads back
//!    correctly **before** reconstruction (degraded, read around the loss via any `k`)
//!    and **after** (full redundancy), with no read error and no torn/hybrid chunk —
//!    because the location update is **one version-conditional commit** (the inode
//!    version bumps by exactly one and the placement flips atomically).
//!    Flippable (recorded in build-notes): skip the version-conditional commit in
//!    `reconstruction::repair_chunk` and the obligation is never drained / the chunk
//!    stays under-replicated — assertions here fire.
//! 3. **A checksum-failing shard is never decoded** (`0005:275`): a present-but-corrupt
//!    fragment (a scrub / read checksum finding) is excluded and rebuilt around, exactly
//!    like a lost one.
//! 4. **Durability-plane emission** (`0005:326-332`, ADR-0011/0012): the three M3 repair
//!    metrics — under-replicated chunk count, repair-queue depth, time-to-repair — are
//!    emitted on the `DurabilityTelemetry` seam and read back in-process.
//! 5. **Repair-vs-serve priority** (`0005:305-317`): the priority function rises as
//!    redundancy falls, so a near-floor chunk is ordered ahead of a comfortable one.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::CORE_HEADER_LEN;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, EcScheme, InodeId, InodeRecord};
use wyrd_core::placement::Topology;
use wyrd_core::read::read_object;
use wyrd_core::repair;
use wyrd_core::write::write_new_object_placed;
use wyrd_custodian::{
    reconcile_step, repair_priority, Custodian, DurabilityTelemetry, ExporterConfig, FencedZone,
    Reconciled, ReconstructionContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the loop is proven over the seams) ----

/// A trivial in-memory metadata store.
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
/// stored fragment bytes (so their checksums verify and the rebuilt shard round-trips).
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
        // Unused: the write path places via `put_fragment_at`. Route to id-as-server so
        // the trait is total.
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

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-reconstruction")
        .await
        .unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

async fn read_inode(meta: &MemMeta) -> InodeRecord {
    read_inode_id(meta, 1).await
}

async fn read_inode_id(meta: &MemMeta, id: InodeId) -> InodeRecord {
    let bytes = meta
        .get(&metadata::inode_key(id))
        .await
        .unwrap()
        .expect("inode present");
    metadata::decode(&bytes).unwrap()
}

/// Write one RS(2,3? no) chunk via the real write path, placed across distinct domains.
/// Returns the original object bytes. Uses RS(2,1): n = 3 fragments on servers 0,1,2.
async fn write_rs_2_1(meta: &MemMeta, fleet: &Fleet<'_>) -> Vec<u8> {
    let data = b"reconstruct this erasure-coded chunk, every byte of it".to_vec();
    let topo = four_domains();
    let outcome = write_new_object_placed(
        meta,
        fleet,
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
    // The write placed the 3 fragments on the first three domains → servers 0,1,2.
    assert_eq!(
        read_inode(meta).await.chunk_map[0].placement,
        vec![0, 1, 2],
        "RS(2,1) placed across distinct domains A,B,C (servers 0,1,2)"
    );
    data
}

// ---- criterion 1+2: kill a D server, reconstruct to full redundancy, reads never err ----

#[tokio::test]
async fn kills_a_d_server_and_reconstructs_to_full_redundancy_through_reconcile_step() {
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

    let data = write_rs_2_1(&meta, &fleet).await;

    // KILL D server 1 (domain B): its fragment of the chunk is lost, so the chunk is now
    // under-replicated. A health report enqueues the chunk on the shared repair queue.
    d1.delete_fragment(frag(1)).await.unwrap();
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // Reads succeed THROUGHOUT — degraded, read around the loss via the k=2 survivors.
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data.clone()),
        "the object reads correctly while under-replicated (read around the lost fragment)"
    );

    // Reconstruction sees only the HEALTHY fleet/topology (server 1 is gone): survivors
    // on domains A,C; the rebuilt fragment must land on the one free domain, D (server 3).
    let mut healthy_topo = Topology::default();
    healthy_topo
        .register(0, "A")
        .register(2, "C")
        .register(3, "D");
    let healthy_fleet: [(DServerId, &dyn ChunkStore); 3] = [(0, &d0), (2, &d2), (3, &d3)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy_fleet,
        topology: &healthy_topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the under-replicated chunk was reconstructed"
    );

    // The obligation is DRAINED off the shared repair queue.
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the repair obligation is drained by the reconstruction commit"
    );

    // ONE version-conditional commit: the inode version bumped by EXACTLY one and the
    // placement flipped atomically — fragment 1 now lives on server 3 (domain D).
    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 3, 2],
        "the rebuilt fragment was re-placed on a healthy D server in a distinct domain"
    );

    // FULL REDUNDANCY: all n=3 fragments present and intact across 3 distinct domains.
    for (index, server) in [(0u16, &d0), (1, &d3), (2, &d2)] {
        let bytes = server
            .get_fragment(frag(index))
            .await
            .unwrap()
            .expect("fragment present after repair");
        assert!(
            repair::fragment_intact(&bytes, CHUNK),
            "fragment {index} verifies its checksum and belongs to the chunk"
        );
    }
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| healthy_topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        3,
        "n fragments on n distinct failure domains"
    );

    // Reads still succeed and return the same bytes — full redundancy, no torn chunk.
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object reads correctly after repair (full redundancy, atomic flip)"
    );
}

// ---- criterion 3: a checksum-failing fragment is excluded and rebuilt around ----

#[tokio::test]
async fn a_checksum_failing_fragment_is_excluded_and_reconstructed() {
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

    let data = write_rs_2_1(&meta, &fleet).await;

    // Corrupt server 1's fragment in place (bit rot): a present-but-checksum-failing
    // shard — the scrub / read finding. It must be EXCLUDED (never decoded), not absorbed.
    let mut rotten = d1.get_fragment(frag(1)).await.unwrap().unwrap().to_vec();
    rotten[CORE_HEADER_LEN as usize] ^= 0xff;
    d1.put_fragment(frag(1), Bytes::from(rotten)).await.unwrap();
    repair::enqueue_repair(&meta, CHUNK, "scrub").await.unwrap();

    // Reconstruction over the full (alive) fleet: the corrupt fragment is treated as
    // missing and rebuilt; the free domain among {B,D} excluding survivors {A,C} is B,
    // so the rebuilt fragment is re-placed in place on server 1 (overwriting the rot).
    let topo = four_domains();
    let full_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &full_fleet,
        topology: &topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed);

    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the corruption obligation is drained once the shard is rebuilt"
    );
    // Server 1's bytes are now intact again (the checksum-failing shard was never decoded
    // into the chunk — it was rebuilt from the survivors).
    let rebuilt = d1.get_fragment(frag(1)).await.unwrap().unwrap();
    assert!(
        repair::fragment_intact(&rebuilt, CHUNK),
        "the rebuilt fragment verifies its checksum"
    );
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object reads correctly after the corrupt shard is reconstructed around"
    );
}

// ---- issue #349 (mixed-era placement matrix): Reconstruction — empty/short placement
//      and an RS{6,3} cell, closing the gaps the #292 audit found (this suite covered
//      only full RS{2,1}; empty/short and the empty -> re-placement case were unpinned).
//
// `assess` (`crates/custodian/src/reconstruction.rs:226-232`) expands the chunk's
// committed `placement` through the SAME `ChunkRef::placed_dserver` identity-fallback
// resolution read/GC/scrub share, and `repair_chunk` (`reconstruction.rs:388-418`)
// clones THAT expanded vector as the base it repoints — so a chunk reconstructed from
// an empty or short committed `placement` must come back FULL-LENGTH
// (`fragment_count()`), never the raw empty/short vector it started from.
//
// Flippable (recorded in build-notes): replace `assess`'s expansion (`reconstruction.rs
// :230-232`, `let placement = (0..chunk_ref.fragment_count()).map(|i|
// chunk_ref.placed_dserver(i)).collect();`) with raw `chunk_ref.placement.clone()`. For
// an empty/short committed vector the iteration loop below then runs over 0 (or fewer
// than `n`) entries, so `missing` stays empty -> `Assessment::Drain` -> the obligation
// is drained with NOTHING rebuilt and the placement record never repointed — every
// assertion in `reconstructs_a_pre_m3_chunk_with_empty_placement_to_a_full_length_record`
// and `reconstructs_a_short_placement_chunk_resolving_the_fallback_index` below fires.

/// **The brief's required re-placement pin.** A chunk committed with an EMPTY
/// `placement` (the genuine pre-M3 shape, `core/src/metadata.rs:93`) is kept under
/// repair after a D-server loss; the rebuilt record must be FULL-LENGTH
/// (`== fragment_count()`), with the rebuilt fragment in a domain distinct from the
/// survivors — never a re-committed short/empty vector.
#[tokio::test]
async fn reconstructs_a_pre_m3_chunk_with_empty_placement_to_a_full_length_record() {
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

    let data = write_rs_2_1(&meta, &fleet).await;

    // Downgrade the just-committed FULL placement to the pre-M3 EMPTY shape. The
    // physical fragments are untouched (still on servers 0,1,2, exactly where the
    // identity fallback resolves them), so this is a faithful pre-M3 fixture: a chunk
    // committed before the `placement` field shipped decodes to precisely this shape.
    let prior = read_inode(&meta).await;
    let mut chunk_map = prior.chunk_map.clone();
    chunk_map[0].placement = Vec::new();
    assert_eq!(
        metadata::commit_chunk_map(&meta, 1, &prior, chunk_map, prior.size)
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    let downgraded = read_inode(&meta).await;
    assert!(
        downgraded.chunk_map[0].placement.is_empty(),
        "pre-M3 record: an empty, not full-length, placement vector"
    );

    // KILL D server 1 (domain B): its fragment is lost, so the chunk is under-replicated.
    d1.delete_fragment(frag(1)).await.unwrap();
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // Reads succeed THROUGHOUT — degraded, read around the loss via the k=2 survivors,
    // resolved entirely through the identity fallback (no explicit placement at all).
    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data.clone()),
        "the object reads correctly while under-replicated, via the empty-placement \
         identity fallback"
    );

    // Reconstruction sees only the HEALTHY fleet/topology (server 1 is gone); the
    // rebuilt fragment must land on the one free domain, D (server 3).
    let mut healthy_topo = Topology::default();
    healthy_topo
        .register(0, "A")
        .register(2, "C")
        .register(3, "D");
    let healthy_fleet: [(DServerId, &dyn ChunkStore); 3] = [(0, &d0), (2, &d2), (3, &d3)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy_fleet,
        topology: &healthy_topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the empty-placement, under-replicated chunk was reconstructed"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the repair obligation is drained by the reconstruction commit"
    );

    let record = read_inode(&meta).await;
    assert_eq!(
        record.version, 3,
        "exactly one version-conditional commit on top of the pre-M3 downgrade"
    );
    // THE re-placement pin: FULL-LENGTH (== fragment_count() == 3), never the raw
    // empty vector the chunk was committed with going into this repair.
    assert_eq!(
        record.chunk_map[0].placement.len(),
        usize::from(record.chunk_map[0].fragment_count()),
        "the re-placed record is full-length, not the short/empty vector it started from"
    );
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 3, 2],
        "survivors identity-resolved (0, 2); the lost fragment re-placed on the free \
         distinct domain D (server 3)"
    );
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| healthy_topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        3,
        "n fragments on n distinct failure domains"
    );

    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object reads correctly after repair (full redundancy, atomic flip)"
    );
}

/// A SHORT committed placement (`vec![<explicit dserver>]`, length 1 < `fragment_count()
/// == 3`) — index 0 explicit on an off-identity D server, indices 1-2 resolved by
/// identity fallback. Reconstruction must resolve the mix correctly AND re-commit a
/// FULL-LENGTH record, preserving the genuinely-explicit entry.
#[tokio::test]
async fn reconstructs_a_short_placement_chunk_resolving_the_fallback_index() {
    let meta = MemMeta::default();
    let (d0, d1, d2, d3, d9) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let fleet = Fleet {
        servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3), (9, &d9)],
    };

    let data = write_rs_2_1(&meta, &fleet).await; // commits [0, 1, 2] on domains A,B,C

    // Move index 0's bytes to an out-of-band D server (9, domain "Z") and commit a
    // SHORT placement: only index 0 explicit; indices 1,2 keep resolving via identity
    // fallback to their real, untouched locations (servers 1, 2).
    let bytes0 = d0.get_fragment(frag(0)).await.unwrap().unwrap();
    d9.put_fragment(frag(0), bytes0).await.unwrap();
    let prior = read_inode(&meta).await;
    let mut chunk_map = prior.chunk_map.clone();
    chunk_map[0].placement = vec![9];
    assert_eq!(
        metadata::commit_chunk_map(&meta, 1, &prior, chunk_map, prior.size)
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    let downgraded = read_inode(&meta).await;
    assert_eq!(
        downgraded.chunk_map[0].placement,
        vec![9],
        "a genuinely SHORT vector: 1 explicit entry, not the full 3"
    );

    // KILL D server 2 (the fallback-resolved index 2's home, domain C).
    d2.delete_fragment(frag(2)).await.unwrap();
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data.clone()),
        "the object reads correctly while under-replicated, via the mixed \
         explicit/fallback resolution"
    );

    // Reconstruction's healthy view excludes domain A (server 0 — stale, no longer
    // referenced once index 0 moved to 9) and domain C (server 2 — dead): only the
    // ACTUAL survivor domains (B, Z) plus the one free target domain (D) are visible.
    let mut healthy_topo = Topology::default();
    healthy_topo
        .register(1, "B")
        .register(3, "D")
        .register(9, "Z");
    let healthy_fleet: [(DServerId, &dyn ChunkStore); 3] = [(1, &d1), (3, &d3), (9, &d9)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy_fleet,
        topology: &healthy_topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the short-placement, under-replicated chunk was reconstructed"
    );

    let record = read_inode(&meta).await;
    assert_eq!(
        record.version, 3,
        "one commit on top of the short-placement downgrade"
    );
    assert_eq!(
        record.chunk_map[0].placement,
        vec![9, 1, 3],
        "explicit index 0 preserved (server 9); fallback index 1 untouched (server 1); \
         lost index 2 re-placed on the free distinct domain D (server 3)"
    );
    assert_eq!(
        record.chunk_map[0].placement.len(),
        usize::from(record.chunk_map[0].fragment_count()),
        "full-length record, not the short vector it started from"
    );
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| healthy_topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        3,
        "n fragments on n distinct failure domains"
    );

    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object reads correctly after repair (full redundancy, atomic flip)"
    );
}

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
async fn write_rs_6_3(meta: &MemMeta, fleet: &Fleet<'_>) -> Vec<u8> {
    let data = b"reconstruct this rs(6,3) chunk across all nine of its fragments".to_vec();
    let topo = ten_domains();
    let outcome = write_new_object_placed(
        meta,
        fleet,
        ROOT,
        "obj",
        1,
        &data,
        data.len(),
        EcScheme::ReedSolomon { k: 6, m: 3 },
        &topo,
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

/// The brief's required RS{6,3} cell: kill one D server of a FULL rs(6,3) placement
/// and reconstruct to full redundancy — the scheme-size gap every other reconstruction
/// case above (RS{2,1}) leaves open.
#[tokio::test]
async fn kills_a_d_server_and_reconstructs_an_rs_6_3_chunk_to_full_redundancy() {
    let meta = MemMeta::default();
    let servers: Vec<MemDServer> = (0..10).map(|_| MemDServer::default()).collect();
    let fleet = Fleet {
        servers: servers
            .iter()
            .enumerate()
            .map(|(i, s)| (i as DServerId, s))
            .collect(),
    };

    let data = write_rs_6_3(&meta, &fleet).await;

    // KILL D server 4 (domain E): its fragment is lost, so the chunk is now
    // under-replicated (8 of 9 survive, well within m=3).
    servers[4].delete_fragment(frag(4)).await.unwrap();
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data.clone()),
        "the object reads correctly while under-replicated (read around the lost fragment)"
    );

    // Healthy view excludes domain E (server 4); the rebuilt fragment must land on the
    // one free domain, J (server 9).
    let healthy_topo = ten_domains().excluding(&std::collections::BTreeSet::from([4]));
    let healthy_fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 4)
        .map(|(i, s)| (i as DServerId, s as &dyn ChunkStore))
        .collect();
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy_fleet,
        topology: &healthy_topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the under-replicated rs(6,3) chunk was reconstructed"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the repair obligation is drained by the reconstruction commit"
    );

    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 1, 2, 3, 9, 5, 6, 7, 8],
        "survivors identity-resolved; the lost fragment (index 4) re-placed on the free \
         distinct domain J (server 9)"
    );
    let domains: std::collections::HashSet<_> = record.chunk_map[0]
        .placement
        .iter()
        .map(|id| healthy_topo.domain_of(*id).unwrap().clone())
        .collect();
    assert_eq!(
        domains.len(),
        9,
        "n=9 fragments on n=9 distinct failure domains"
    );

    assert_eq!(
        read_object(&meta, &fleet, 1).await.unwrap(),
        Some(data),
        "the object reads correctly after repair (full redundancy, atomic flip)"
    );
}

// ---- issue #251: a placed-fragment READ FAULT must not abort the per-chunk drain ----

/// A `ChunkStore` whose `get_fragment` always fails with a caller-supplied error — the
/// in-process stand-in (issue #251 / the #195 disk-fault harness) for a D server whose
/// disk faults the *read*: a permanent block-layer fault (a dead sector / `dm-error`
/// `EIO`) or a transient unreachable/busy error. Every other operation delegates to a
/// healthy inner store, so the loop can still *place* a rebuilt fragment here.
struct FaultGetStore {
    inner: MemDServer,
    error: fn() -> wyrd_traits::BoxError,
}

#[async_trait]
impl ChunkStore for FaultGetStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
        Err((self.error)())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// A backend error that **wraps** its underlying `io::Error`, exposing it only through
/// [`source`](std::error::Error::source) (and never re-surfacing the OS errno on its own
/// `Display`). This is the shape a store that boxes the raw fault inside its *own* error
/// type produces — so the permanent-vs-transient classifier must walk the source chain to
/// find the `EIO` underneath, not merely inspect the top-level boxed error. It is the
/// non-trivial-chain companion to the depth-0 fault `chunkstore-fs` surfaces directly.
#[derive(Debug)]
struct WrappedReadError(std::io::Error);

impl std::fmt::Display for WrappedReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "backend read failed")
    }
}

impl std::error::Error for WrappedReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// A **permanent** block-layer read fault surfaced at the **top of the box** — a raw `EIO`
/// `io::Error`, exactly the shape `chunkstore-fs` propagates out of `fs::read` when the
/// device cannot satisfy the read (`crates/chunkstore-fs/src/lib.rs:241`, `Err(e.into())`
/// boxes the `io::Error` directly, depth 0). POSIX `EIO` is errno 5.
fn permanent_eio_fault() -> wyrd_traits::BoxError {
    Box::new(std::io::Error::from_raw_os_error(5))
}

/// The same **permanent** `EIO` fault, but **wrapped** inside a backend error so the
/// `io::Error` is reachable only by walking [`source`](std::error::Error::source) (depth
/// 1). Drives the classifier's source-chain walk at non-zero depth — closing the gap that
/// a depth-0-only fixture would leave: a backend that wraps the raw fault must still be
/// classified as a permanent read-around loss, never mistaken for a transient fault.
fn wrapped_permanent_eio_fault() -> wyrd_traits::BoxError {
    Box::new(WrappedReadError(std::io::Error::from_raw_os_error(5)))
}

/// A **transient** healthy-server fault: the D server is up but momentarily unreachable /
/// busy. It carries no durability signal (not a corruption fault, not an `EIO`), so the
/// loop must propagate it to the retry policy — never treat it as permanent fragment loss.
fn transient_fault() -> wyrd_traits::BoxError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "D server busy",
    ))
}

/// BINDING (issue #251, the read-around leg): a placed fragment whose store returns a
/// **permanent** read fault (an `EIO`-class `Err`, NOT `Ok(None)`/`NotFound`) is read
/// AROUND — `assess` rebuilds from the >=k survivors and returns a repairable plan,
/// instead of letting the `?` propagate the `Err` and abort the whole per-chunk drain.
///
/// Driven for BOTH fault shapes (`make_error`): the depth-0 `EIO` `chunkstore-fs` surfaces
/// straight from `fs::read`, and a wrapped `EIO` reachable only via `source()` — so the
/// classifier's source-chain walk is proven at non-zero depth, not just at depth 0.
///
/// Flippable: pre-fix `reconstruction::assess` does `store.get_fragment(frag).await?`, so
/// the `EIO` propagates → `reconcile_step` returns `Err` and the chunk is never repaired
/// (RED). Post-fix the permanent fault is classified and read around → `Reconciled::Changed`
/// and the chunk is back to full redundancy (GREEN).
async fn reads_around_a_permanent_read_fault(make_error: fn() -> wyrd_traits::BoxError) {
    let meta = MemMeta::default();
    let (d0, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    // Server 1's disk faults every read (a dead sector / `dm-error` `EIO`), but it is
    // still in the fleet — so `assess` actually calls `get_fragment` on it and must
    // classify the fault, not route around it by absence.
    let faulted1 = FaultGetStore {
        inner: MemDServer::default(),
        error: make_error,
    };
    {
        // Place the chunk's three fragments on servers 0,1,2 via the real write path.
        // `d1`'s real fragment bytes are irrelevant: the reconstruction fleet swaps in
        // the faulting store at server 1, so the read fault — not absence — is exercised.
        let d1 = MemDServer::default();
        let fleet = Fleet {
            servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3)],
        };
        write_rs_2_1(&meta, &fleet).await;
    }

    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // Reconstruction sees server 1 as a faulting disk (EIO) and the healthy survivors on
    // domains A,C; the rebuilt fragment must land on the one free domain, D (server 3).
    let mut topo = Topology::default();
    topo.register(0, "A").register(2, "C").register(3, "D");
    let recon_fleet: [(DServerId, &dyn ChunkStore); 4] =
        [(0, &d0), (1, &faulted1), (2, &d2), (3, &d3)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &recon_fleet,
        topology: &topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let result = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500).await;
    let outcome =
        result.expect("a permanent read fault is read around, not propagated out of assess");
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the under-replicated chunk was reconstructed despite the EIO read fault"
    );

    // The obligation is drained and the placement flipped with ONE version-conditional
    // commit — fragment 1 now lives on server 3 (domain D), read around the dead disk.
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the obligation is drained once the chunk is rebuilt around the read fault"
    );
    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert_eq!(
        record.chunk_map[0].placement,
        vec![0, 3, 2],
        "the rebuilt fragment was re-placed on a healthy D server in a distinct domain"
    );
    let rebuilt = d3
        .get_fragment(frag(1))
        .await
        .unwrap()
        .expect("the rebuilt fragment is present on its new server");
    assert!(
        repair::fragment_intact(&rebuilt, CHUNK),
        "the rebuilt fragment verifies its checksum and belongs to the chunk"
    );
}

/// The read-around leg with the **depth-0** `EIO` `chunkstore-fs` surfaces directly
/// (`Err(e.into())`, `lib.rs:241`) — the exact production fault shape for the fs D server.
#[tokio::test]
async fn reads_around_a_depth0_permanent_read_fault_on_a_placed_fragment() {
    reads_around_a_permanent_read_fault(permanent_eio_fault).await;
}

/// The read-around leg with a **wrapped** `EIO` reachable only via `source()` (depth 1) —
/// exercising the classifier's source-chain walk at non-zero depth, so a backend that
/// boxes the raw fault inside its own error type is still read around, not mistaken for a
/// transient fault and propagated (the gap a depth-0-only fixture would leave open).
#[tokio::test]
async fn reads_around_a_wrapped_permanent_read_fault_on_a_placed_fragment() {
    reads_around_a_permanent_read_fault(wrapped_permanent_eio_fault).await;
}

/// BINDING (issue #251, the no-spurious-re-placement leg): a placed fragment whose store
/// returns a **transient** healthy-server error is NOT converted into permanent fragment
/// loss / a re-placement. The transient fault carries no durability signal, so `assess`
/// propagates it — the obligation stays queued for the retry policy and the fragment is
/// neither dropped nor moved off its server.
///
/// Discriminating guard (not a red→green flip): it is green with the correct
/// classify-and-propagate fix, and RED with the rejected over-broad `.ok().flatten()`
/// candidate — which would swallow the transient error into `None`, rebuild, and re-place
/// the fragment (a spurious permanent re-placement: `Changed`, drained queue, version
/// bump), failing every assertion below.
#[tokio::test]
async fn a_transient_fault_is_not_turned_into_a_spurious_re_placement() {
    let meta = MemMeta::default();
    let (d0, d2, d3) = (
        MemDServer::default(),
        MemDServer::default(),
        MemDServer::default(),
    );
    let faulted1 = FaultGetStore {
        inner: MemDServer::default(),
        error: transient_fault,
    };
    {
        let d1 = MemDServer::default();
        let fleet = Fleet {
            servers: vec![(0, &d0), (1, &d1), (2, &d2), (3, &d3)],
        };
        let _data = write_rs_2_1(&meta, &fleet).await;
    }

    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // The full four-domain topology and fleet: were the transient fault wrongly swallowed
    // into a loss, the selector would re-place the "missing" fragment (free domain B,
    // server 1) and commit — exactly the spurious re-placement this guard forbids.
    let topo = four_domains();
    let recon_fleet: [(DServerId, &dyn ChunkStore); 4] =
        [(0, &d0), (1, &faulted1), (2, &d2), (3, &d3)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &recon_fleet,
        topology: &topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let result = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500).await;

    // The transient fault is propagated to the retry policy, not absorbed as a loss.
    assert!(
        result.is_err(),
        "a transient healthy-server fault propagates out of the pass (the retry policy \
         decides), rather than being silently converted into permanent fragment loss"
    );
    // Nothing was re-placed: the obligation stays queued and the inode is untouched, so
    // the fragment is neither dropped nor moved off its server.
    assert!(
        repair::queued_repairs(&meta)
            .await
            .unwrap()
            .contains(&CHUNK),
        "the obligation stays queued for retry — no spurious drain"
    );
    assert_eq!(
        read_inode(&meta).await.version,
        1,
        "no version-conditional commit ran — the fragment was not re-placed"
    );
    assert_eq!(
        read_inode(&meta).await.chunk_map[0].placement,
        vec![0, 1, 2],
        "the placement record is unchanged — the transiently-faulting fragment stays put"
    );
}

// ---- criterion 4: the three M3 repair metrics on the durability seam, read back ----

#[tokio::test]
async fn emits_the_three_repair_metrics_on_the_durability_seam() {
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
    write_rs_2_1(&meta, &fleet).await;

    d1.delete_fragment(frag(1)).await.unwrap();
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    let mut healthy_topo = Topology::default();
    healthy_topo
        .register(0, "A")
        .register(2, "C")
        .register(3, "D");
    let healthy_fleet: [(DServerId, &dyn ChunkStore); 3] = [(0, &d0), (2, &d2), (3, &d3)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy_fleet,
        topology: &healthy_topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;

    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());

    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed);

    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    for metric in [
        "reconstruction_under_replicated",
        "reconstruction_queue_depth",
        "reconstruction_time_to_repair",
    ] {
        assert!(
            exposed.contains(metric),
            "the M3 repair metric `{metric}` is exported on the durability seam; got:\n{exposed}"
        );
    }
}

// ---- criterion 5: repair priority rises as redundancy falls (the priority function) ----

#[test]
fn repair_priority_rises_as_redundancy_falls() {
    // A chunk one fragment from its floor (survivors == k) is more urgent than one with
    // slack — its priority key sorts strictly smaller (ahead) in the drain order.
    let at_floor = repair_priority(2, 2); // 0 slack
    let one_spare = repair_priority(3, 2); // 1 spare
    let comfortable = repair_priority(5, 2); // 3 spare
    assert!(
        at_floor < one_spare && one_spare < comfortable,
        "priority rises (sort key falls) as redundancy falls"
    );

    // Draining by this key puts the near-floor chunk first.
    let mut keys = [comfortable, at_floor, one_spare];
    keys.sort();
    assert_eq!(
        keys[0], at_floor,
        "the near-floor chunk preempts comfortable ones in the drain order"
    );
}

// ---- success identity: an Aborted plan must NOT be counted as a successful repair ----

const INODE_COMMIT: InodeId = 11;
const INODE_ABORT: InodeId = 12;
const CHUNK_COMMIT: ChunkId = 0xC0FFEE;
const CHUNK_ABORT: ChunkId = 0xA80872;

/// Write one RS(2,1) object via the real write path, under `inode_id`/`name`/`chunk_id`.
/// `four_domains()` places its 3 fragments on the first three domains A,B,C → servers
/// 0,1,2 (the same placement [`write_rs_2_1`] asserts), so the two chunks this test
/// builds share the same three servers and differ only in which fragment is lost.
async fn write_rs_2_1_as(
    meta: &MemMeta,
    fleet: &Fleet<'_>,
    inode_id: InodeId,
    name: &str,
    chunk_id: ChunkId,
) -> Vec<u8> {
    let data = format!("reconstruct erasure-coded chunk {chunk_id:#x}, every byte").into_bytes();
    let topo = four_domains();
    let outcome = write_new_object_placed(
        meta,
        fleet,
        ROOT,
        name,
        inode_id,
        &data,
        data.len(),
        EcScheme::ReedSolomon { k: 2, m: 1 },
        &topo,
        0,
        1_000,
        || chunk_id,
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    assert_eq!(
        read_inode_id(meta, inode_id).await.chunk_map[0].placement,
        vec![0, 1, 2],
        "RS(2,1) placed across distinct domains A,B,C (servers 0,1,2)"
    );
    data
}

/// The value of a `monotonic_counter` metric read back off the Prometheus surface, summed
/// over every exported sample (one per scope label-set). The OTel→Prometheus exporter
/// suffixes a counter with `_total`; we accept either spelling so the read-back does not
/// depend on that convention. A counter never emitted is absent from the surface → `0`.
fn counter_total(exposed: &str, name: &str) -> u64 {
    let with_suffix = format!("{name}_total");
    exposed
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let key = fields.next()?;
            let value = fields.next()?;
            let metric = key.split('{').next().unwrap_or(key);
            if metric == name || metric == with_suffix {
                value.parse::<f64>().ok().map(|v| v as u64)
            } else {
                None
            }
        })
        .sum()
}

/// BINDING success identity (proposal 0005 §326-332, ADR-0011; the in-code contract at
/// `reconstruction.rs`): over a reconstruction pass, the quantity the durability-plane
/// telemetry defines as "successful repairs" must equal **exactly** the count of plans
/// whose outcome was `Committed`.
///
/// `reconstruction_repaired` is emitted once per plan UP FRONT — deliberately, before
/// the heavy erasure-decode/commit section, because the `tracing`→OTel bridge can drop a
/// metric emitted *after* that section under load. The outcome loop then offsets every
/// NON-success on its own counter: a lost CAS race on `reconstruction_conflict`, an abort
/// (the selector chose a server outside the fleet view, so nothing committed) on
/// `reconstruction_aborted`. So derived successes = repaired − conflict − aborted.
///
/// This pass mixes a Committed plan with an Aborted one. Pre-fix the Aborted arm offset
/// NOTHING, so derived successes = repaired − conflict over-counted by the one Aborted
/// plan (2 instead of 1) — this assertion is RED. Post-fix the abort is offset and the
/// identity holds.
#[tokio::test]
async fn an_aborted_repair_is_not_counted_as_a_successful_repair() {
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

    // Two RS(2,1) chunks, both placed on servers 0,1,2 (domains A,B,C).
    write_rs_2_1_as(&meta, &fleet, INODE_COMMIT, "commit", CHUNK_COMMIT).await;
    write_rs_2_1_as(&meta, &fleet, INODE_ABORT, "abort", CHUNK_ABORT).await;

    // Differential loss, so the two plans take DIFFERENT outcomes in one pass:
    //   * the COMMIT chunk loses domain C (server 2) → survivors on A,B → the rebuilt
    //     fragment re-places on the free domain C (server 2, in the fleet) → Committed.
    //   * the ABORT chunk loses domain B (server 1) → survivors on A,C → its only free
    //     domain by the selector's util order is G (server 7), which is NOT in the fleet
    //     view → the repair cannot place the rebuilt shard → Aborted (nothing committed).
    d2.delete_fragment(FragmentId {
        chunk: CHUNK_COMMIT,
        index: 2,
    })
    .await
    .unwrap();
    d1.delete_fragment(FragmentId {
        chunk: CHUNK_ABORT,
        index: 1,
    })
    .await
    .unwrap();
    repair::enqueue_repair(&meta, CHUNK_COMMIT, "health")
        .await
        .unwrap();
    repair::enqueue_repair(&meta, CHUNK_ABORT, "health")
        .await
        .unwrap();

    // Reconstruction topology: A,B,C in the fleet, plus a GHOST domain G (server 7) the
    // topology knows but the fleet does NOT hold. Loading domain B (server 1) makes the
    // selector prefer the least-utilized free domain G for the abort chunk (whose free
    // domains are {B, G}), while the commit chunk's free domains {C, G} tie on util and
    // resolve to C by label — so one repair places in-fleet and one selects the ghost.
    let mut topo = Topology::default();
    topo.register(0, "A")
        .register(1, "B")
        .register(2, "C")
        .register(7, "G")
        .set_utilization(1, 100);
    let recon_fleet: [(DServerId, &dyn ChunkStore); 3] = [(0, &d0), (1, &d1), (2, &d2)];
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &recon_fleet,
        topology: &topo,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;

    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let subscriber = tracing_subscriber::registry().with(telemetry.metrics_layer());

    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .with_subscriber(subscriber)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "at least one plan (the Committed one) repointed a placement record"
    );

    // Observe the committed count independently of the metric: exactly one obligation was
    // drained by a real commit; the aborted one stays queued and its inode never bumped.
    let remaining = repair::queued_repairs(&meta).await.unwrap();
    assert_eq!(
        remaining.len(),
        1,
        "only the aborted plan's obligation stays queued; got {remaining:?}"
    );
    assert!(
        remaining.contains(&CHUNK_ABORT),
        "the queued obligation is the aborted chunk; got {remaining:?}"
    );
    assert_eq!(
        read_inode_id(&meta, INODE_COMMIT).await.version,
        2,
        "the committed plan bumped its inode with one version-conditional commit"
    );
    assert_eq!(
        read_inode_id(&meta, INODE_ABORT).await.version,
        1,
        "the aborted plan committed nothing (its inode is unchanged)"
    );
    let committed_count = 2 - remaining.len() as u64; // 2 obligations enqueued, 1 still queued
    assert_eq!(committed_count, 1, "exactly one plan committed");

    // BINDING: the telemetry's derived successes equal the committed count.
    telemetry.flush().unwrap();
    let exposed = telemetry
        .gather_prometheus()
        .expect("Prometheus surface configured");
    let repaired = counter_total(&exposed, "reconstruction_repaired");
    let conflict = counter_total(&exposed, "reconstruction_conflict");
    let aborted = counter_total(&exposed, "reconstruction_aborted");
    let derived_successes = repaired.saturating_sub(conflict).saturating_sub(aborted);
    assert_eq!(
        derived_successes, committed_count,
        "successful repairs (reconstruction_repaired − conflict − aborted) must equal the \
         committed count: got repaired={repaired} conflict={conflict} aborted={aborted} \
         (derived {derived_successes}) vs committed {committed_count}\n{exposed}"
    );
}
