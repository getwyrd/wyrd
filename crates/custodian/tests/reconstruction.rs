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
    let bytes = meta
        .get(&metadata::inode_key(1))
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
