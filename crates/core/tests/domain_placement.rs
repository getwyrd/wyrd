//! Option B (proposal 0005, `0005:510-513` + §"Failure-domain-aware placement",
//! `0005:235-245`): the write path **retires identity `index % n`** at the commit.
//!
//! The flippable red→green for the production write rewire. A write into a topology
//! where `index % n` would **collide failure domains** must record a
//! **distinct-domain** placement (NOT the identity vector `0..n`) in the chunk map,
//! and the read must resolve every fragment from that record. These are the
//! backend-agnostic, in-process properties; the over-the-wire `rs(6,3)`-over-tonic
//! and the DST seed sweep are supplementary (`cargo xtask ci`).
//!
//! Pre-rewire the write recorded the identity placement (`core/write.rs:73`, the old
//! `(0..n)` vector), which is domain-blind — two of a chunk's nine fragments land in
//! the same domain. Post-rewire `WritePlan::place` runs the failure-domain selector,
//! so the recorded placement spans nine distinct domains.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use pollster::block_on;
use wyrd_core::metadata::EcScheme;
use wyrd_core::placement::{FailureDomain, Topology};
use wyrd_core::{read, write};
use wyrd_traits::{
    ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore, PlacementChunkStore,
    Result, WriteBatch,
};

const CHUNK: usize = 1 << 16; // one chunk per payload
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };
const N: u16 = 9; // k + m fragments per chunk
const ROOT: u64 = 0;

/// A topology in which the identity placement `0..9` collides failure domains:
/// D servers 0 and 1 share domain "A" (so identity puts fragments 0 and 1 in one
/// domain), while extra singleton domains B..K give the selector ≥ 9 distinct
/// domains to spread across.
fn colliding_topology() -> (Topology, HashMap<DServerId, FailureDomain>) {
    let mut domains: HashMap<DServerId, FailureDomain> = HashMap::new();
    let mut topo = Topology::default();
    // Servers 0 and 1 share domain "A".
    topo.register(0, "A");
    topo.register(1, "A");
    domains.insert(0, FailureDomain::new("A"));
    domains.insert(1, FailureDomain::new("A"));
    // Servers 2..=11 are singletons in domains B..K.
    for (id, label) in (2u64..).zip(["B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]) {
        topo.register(id, label);
        domains.insert(id, FailureDomain::new(label));
    }
    (topo, domains)
}

/// A fleet of in-process D servers addressed by **stable id**: a fragment physically
/// lives on exactly one server (placed via `put_fragment_at`), so a read that does
/// not consult the placement record looks at the wrong server and finds nothing.
struct Fleet {
    servers: HashMap<DServerId, Mutex<HashMap<FragmentId, Bytes>>>,
}

impl Fleet {
    fn new(ids: impl IntoIterator<Item = DServerId>) -> Self {
        Self {
            servers: ids
                .into_iter()
                .map(|id| (id, Mutex::new(HashMap::new())))
                .collect(),
        }
    }
}

#[async_trait]
impl ChunkStore for Fleet {
    // Supertrait obligation. The placement-aware path uses `*_at`; a stateless
    // `index % n` caller would route here and miss a moved fragment.
    async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> Result<()> {
        Err("Fleet: write must address a D server by id (use put_fragment_at)".into())
    }

    async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self
            .servers
            .values()
            .flat_map(|s| s.lock().unwrap().keys().copied().collect::<Vec<_>>())
            .collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        for s in self.servers.values() {
            s.lock().unwrap().remove(&id);
        }
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

#[async_trait]
impl PlacementChunkStore for Fleet {
    async fn get_fragment_at(&self, dserver: DServerId, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self
            .servers
            .get(&dserver)
            .and_then(|s| s.lock().unwrap().get(&id).cloned()))
    }

    async fn put_fragment_at(
        &self,
        dserver: DServerId,
        id: FragmentId,
        fragment: Bytes,
    ) -> Result<()> {
        self.servers
            .get(&dserver)
            .ok_or_else(|| format!("Fleet: unknown D server {dserver}"))?
            .lock()
            .unwrap()
            .insert(id, fragment);
        Ok(())
    }
}

/// A trivial in-memory metadata store so the property stays backend-agnostic.
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

/// BINDING: a write into a domain-colliding topology records a **distinct-domain**
/// placement (not the identity vector) and the read reconstructs by resolving every
/// fragment from that record.
#[test]
fn write_records_distinct_domain_placement_not_identity() {
    block_on(async {
        let (topo, domains) = colliding_topology();
        let fleet = Fleet::new(domains.keys().copied());
        let meta = MemMeta::default();

        let payload = b"option B retires index % n at the write commit; ".repeat(16);

        let mut next = 0x141u128;
        let outcome = write::write_new_object_placed(
            &meta,
            &fleet,
            ROOT,
            "obj",
            1,
            &payload,
            CHUNK,
            RS,
            &topo,
            0,
            10_000,
            || {
                next += 1;
                next
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome, CommitOutcome::Committed);

        let inode = read::read_inode(&meta, 1).await.unwrap().unwrap();
        assert_eq!(inode.chunk_map.len(), 1, "single-chunk object");
        let placement = &inode.chunk_map[0].placement;
        assert_eq!(placement.len(), N as usize, "one D-server id per fragment");

        // (1) The committed placement is NOT the identity `index % n` vector — the
        // retirement Option B demands.
        let identity: Vec<DServerId> = (0..u64::from(N)).collect();
        assert_ne!(
            *placement, identity,
            "Option B: the write must record the selector's distinct-domain choice, \
             not the identity index % n vector"
        );

        // (2) The recorded servers occupy n DISTINCT failure domains (the invariant).
        let chosen_domains: HashSet<&FailureDomain> =
            placement.iter().map(|id| &domains[id]).collect();
        assert_eq!(
            chosen_domains.len(),
            N as usize,
            "n fragments placed across n distinct failure domains"
        );

        // (3) The read resolves every fragment from the record and reconstructs.
        let got = read::read_object(&meta, &fleet, 1).await.unwrap().unwrap();
        assert_eq!(got, payload, "object reassembled from the placement record");
    });
}
