//! Tier-0 **custodian property campaign** — the consolidated M3 verification gate
//! (proposal 0005 §"DST and tests (the heart of M3)", `0005:369-411`; the graduation
//! criteria `0005:500-502`; PR-sequence slice 8 `0005:541-545`; ADR-0009). M3's four
//! custodian loops (GC #142, scrub #143, reconstruction #144, rebalance #145) shipped
//! with *per-slice* tests; this suite is the **campaign** that sweeps seeds over the
//! six §13/§10 properties continuously inside the deterministic simulator (`0005:371`).
//!
//! Every property runs through the **real** [`reconcile_step`] fenced control point over
//! the `MetadataStore` / `ChunkStore` trait seams (Option A — no deployed custodian
//! process exists yet, `0005:524-527`). The faults are drawn from the run **seed** via
//! the testkit storage-fault seam ([`SeededStorageFaults`] — the bit-rot / fragment-loss
//! and D-server-kill seam this slice adds, `0005:434-435`), so the whole campaign is a pure
//! function of its seed: a bug-finding seed replays the *same* killed/rotted servers and
//! is committed as a permanent regression ([`REGRESSION_SEEDS`], ADR-0009).
//!
//! The six Tier-0 properties (`0005:378-403`):
//!   1. **Reconstruct-to-full-redundancy (Q1)** — kill a D server; reconstruction
//!      rebuilds onto a healthy server in a **distinct failure domain**, and reads
//!      **never error during repair** (`0005:381-384`).
//!   2. **Commit-point-atomic repair under crash** — a crash before the
//!      version-conditional commit leaves the chunk **fully old, never a hybrid**, and
//!      the placed-but-uncommitted fragment is **collectable garbage, not corruption**
//!      (`0005:385-389`).
//!   3. **Scrub detects bit-rot then reconstructs (Q2)** — scrub excludes a
//!      checksum-failing shard, flags corruption, and reconstruction restores
//!      redundancy; a failing shard is **never decoded** (`0005:390-393`).
//!   4. **GC reclaims only true orphans (Q3)** — interrupted-write garbage is reclaimed
//!      after the grace window; a **referenced** fragment is **never** deleted, and an
//!      in-flight reader within the grace window is **never torn** (`0005:394-397`).
//!   5. **Fenced stale leader** — a deposed custodian lands **no** location update
//!      (fencing token + version CAS), even racing the new leader (`0005:398-399`).
//!   6. **Durability-plane emission** — under-replicated count **rises then returns to
//!      zero** as repair completes; queue depth + time-to-repair are emitted and correct
//!      (`0005:400-403`).
//!
//! Tier-1 (dm-flakey/dm-error + Jepsen) and Tier-2 (single-node kill-and-reconstruct)
//! are the **deferred-posture** deliverables (`0005:405-411`): they need the block layer
//! / a real node and are observed off-Check, not in this worktree.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`, which sweeps 50 seeds); a normal
//! `cargo test` neither builds nor runs this file.
#![cfg(madsim)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;
use wyrd_chunk_format::CORE_HEADER_LEN;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{
    self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState, PendingEntry,
};
use wyrd_core::placement::Topology;
use wyrd_core::read::read_object;
use wyrd_core::repair;
use wyrd_core::write::write_new_object_placed;
use wyrd_custodian::{
    mark_orphaned, reconcile_step, Custodian, FencedZone, GcContext, Reconciled,
    ReconstructionContext, ScrubContext,
};
use wyrd_testkit::{SeededStorageFaults, StorageFault};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
};

// ---- in-memory trait stores (backend-agnostic; the loops are proven over the seams) ----

/// A trivial in-memory metadata store (the same shape the per-slice custodian tests use).
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

/// A **crash-injecting** metadata store wrapping a [`MemMeta`]: while *armed*, it drops
/// the reconstruction loop's **version-conditional repoint commit** — the single batch
/// carrying a positive precondition (`require`, `0005:351-354`) — without applying it,
/// modelling the custodian **dying just before its commit lands** (`0005:385-386`). The
/// rebuilt fragments are already written (repair writes them *before* the commit,
/// `0005:325`), so what survives a crash is exactly a placed-but-uncommitted fragment;
/// the committed chunk map is untouched. At the store boundary a crash-before-commit and
/// a lost CAS are indistinguishable — both leave the inode at its prior value — so this
/// is a faithful Tier-0 crash model. Disarm to let the restarted custodian complete.
struct CrashMeta {
    inner: MemMeta,
    armed: AtomicBool,
}

impl CrashMeta {
    fn new() -> Self {
        Self {
            inner: MemMeta::default(),
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Relaxed);
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::Relaxed);
    }
}

#[async_trait]
impl MetadataStore for CrashMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        // The version-conditional repoint is the *only* commit with a positive
        // precondition; crash on it (apply nothing) when armed. The intent / enqueue /
        // drain commits carry no positive precondition and are left to apply.
        if self.armed.load(Ordering::Relaxed)
            && batch.preconditions.iter().any(|p| p.expected.is_some())
        {
            return Ok(CommitOutcome::Conflict);
        }
        self.inner.commit(batch).await
    }
}

/// One D server's fragment bytes — a deliberately dumb `ChunkStore` holding the **real**
/// stored fragment bytes (so checksums verify and a rebuilt shard round-trips).
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

// ---- a lightweight `tracing` metric capture (import-light; deterministic under madsim) ----

/// A minimal [`tracing_subscriber::Layer`] that records the **numeric values** a metric
/// event carries, so the durability-plane emission (property 6) can be asserted by exact
/// emitted value — the ILLUSTRATIVE in-process assertion mechanism the telemetry seam
/// permits (`crates/custodian/src/telemetry.rs`). It pulls in no OpenTelemetry runtime,
/// so it is fully deterministic under the simulator (the dual-export surface itself is
/// BINDING and proven under the per-slice tests, ADR-0012).
#[derive(Clone, Default)]
struct MetricCapture {
    events: Arc<Mutex<Vec<(String, u64)>>>,
}

impl MetricCapture {
    /// Every value emitted for the metric field `name`, in emission order.
    fn values(&self, name: &str) -> Vec<u64> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k == name)
            .map(|(_, v)| *v)
            .collect()
    }
}

struct CaptureVisitor<'a>(&'a mut Vec<(String, u64)>);

impl tracing::field::Visit for CaptureVisitor<'_> {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.push((field.name().to_string(), value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if value >= 0 {
            self.0.push((field.name().to_string(), value as u64));
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for MetricCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut guard = self.events.lock().unwrap();
        let mut visitor = CaptureVisitor(&mut guard);
        event.record(&mut visitor);
    }
}

// ---- helpers ----

const ROOT: InodeId = 0;
const INODE: InodeId = 1;
const CHUNK: ChunkId = 0xC0FFEE;
/// RS(2,1): `k = 2` data + `m = 1` parity = `n = 3` fragments, placed on servers 0,1,2
/// across domains A,B,C (server 3 = domain D is the spare a rebuild can flip onto). The
/// smallest scheme that is genuinely erasure-coded and survives one loss — so a read is
/// always satisfiable from `k` survivors throughout a single-server kill.
const K: usize = 2;
const M: usize = 1;
const N: usize = K + M;

fn frag(index: u16) -> FragmentId {
    FragmentId {
        chunk: CHUNK,
        index,
    }
}

/// Domain label for a server id: 0→A, 1→B, 2→C, 3→D (the four-domain topology).
fn domain_letter(id: DServerId) -> &'static str {
    ["A", "B", "C", "D"][id as usize]
}

/// A four-domain topology A..D (servers 0..3).
fn four_domains() -> Topology {
    let mut t = Topology::default();
    t.register(0, "A")
        .register(1, "B")
        .register(2, "C")
        .register(3, "D");
    t
}

/// The **healthy** view reconstruction sees after server `victim` is killed: every server
/// except the victim, each registered under its domain. The victim's domain leaves the
/// topology, so the one free domain a rebuilt fragment lands on is D (server 3) — distinct
/// from both survivors (`0005:382-383`).
fn healthy_view(victim: u16, d: &[MemDServer; 4]) -> (Topology, Vec<(DServerId, &dyn ChunkStore)>) {
    let mut topo = Topology::default();
    let mut fleet: Vec<(DServerId, &dyn ChunkStore)> = Vec::new();
    for id in 0u64..4 {
        if id as u16 == victim {
            continue;
        }
        topo.register(id, domain_letter(id));
        fleet.push((id, &d[id as usize]));
    }
    (topo, fleet)
}

async fn elect(coord: &MemCoordination, zone_key: &str) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, zone_key).await.unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

async fn read_inode(meta: &dyn MetadataStore) -> InodeRecord {
    let bytes = meta
        .get(&metadata::inode_key(INODE))
        .await
        .unwrap()
        .expect("inode present");
    metadata::decode(&bytes).unwrap()
}

/// Write one RS(2,1) chunk via the real write path, placed across distinct domains
/// (servers 0,1,2). Returns the original object bytes. Generic over the metadata store so
/// both [`MemMeta`] and [`CrashMeta`] drive it.
async fn write_rs_2_1(meta: &impl MetadataStore, fleet: &Fleet<'_>) -> Vec<u8> {
    let data = b"reconstruct this erasure-coded chunk, every byte of it".to_vec();
    let topo = four_domains();
    let outcome = write_new_object_placed(
        meta,
        fleet,
        ROOT,
        "obj",
        INODE,
        &data,
        data.len(),
        EcScheme::ReedSolomon {
            k: K as u8,
            m: M as u8,
        },
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
        vec![0, 1, 2],
        "RS(2,1) placed across distinct domains A,B,C (servers 0,1,2)"
    );
    data
}

/// Apply a storage-fault plan to the stored fragment bytes: `Lost` drops the byte (a
/// killed D server / disk loss), `BitRot` flips a payload byte so the shard fails its
/// self-describing checksum. Fragment index `i` lives on server `i` (placement [0,1,2]).
async fn apply_storage_faults(d: &[MemDServer; 4], plan: &SeededStorageFaults) {
    for (&i, &fault) in plan.faults() {
        let f = frag(i as u16);
        match fault {
            StorageFault::Lost => {
                d[i].delete_fragment(f).await.unwrap();
            }
            StorageFault::BitRot => {
                let mut bytes = d[i].get_fragment(f).await.unwrap().unwrap().to_vec();
                // Flip the first payload byte (past the self-describing header) so the
                // crc32c no longer matches — bit rot the checksum must catch.
                bytes[CORE_HEADER_LEN as usize] ^= 0xff;
                d[i].put_fragment(f, Bytes::from(bytes)).await.unwrap();
            }
        }
    }
}

/// Assert the chunk is back at **full redundancy**: every placed fragment is present and
/// verifies its checksum, and the `n` fragments occupy `n` distinct failure domains.
async fn assert_full_redundancy(record: &InodeRecord, d: &[MemDServer; 4]) {
    let placement = &record.chunk_map[0].placement;
    assert_eq!(placement.len(), N, "n fragments placed");
    let mut domains = HashSet::new();
    for (index, &server) in placement.iter().enumerate() {
        let bytes = d[server as usize]
            .get_fragment(frag(index as u16))
            .await
            .unwrap()
            .expect("fragment present after repair");
        assert!(
            repair::fragment_intact(&bytes, CHUNK),
            "fragment {index} verifies its checksum and belongs to the chunk"
        );
        domains.insert(domain_letter(server));
    }
    assert_eq!(
        domains.len(),
        N,
        "n fragments on n distinct failure domains"
    );
}

fn servers() -> [MemDServer; 4] {
    Default::default()
}

fn fleet_of(d: &[MemDServer; 4]) -> Fleet<'_> {
    Fleet {
        servers: vec![(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])],
    }
}

// ---- property 1 (Q1): reconstruct-to-full-redundancy; reads never error during repair --

async fn prop_reconstruct_to_full_redundancy(rng: &mut ChaCha8Rng) {
    let meta = MemMeta::default();
    let d = servers();
    let fleet = fleet_of(&d);
    let data = write_rs_2_1(&meta, &fleet).await;

    // KILL a seed-chosen D server holding a fragment (0..N): its fragment is lost, so the
    // chunk is under-replicated. A health report enqueues it on the shared repair queue.
    let kill = SeededStorageFaults::kill(rng, N);
    let victim = *kill.faults().keys().next().unwrap() as u16;
    apply_storage_faults(&d, &kill).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // Reads succeed THROUGHOUT — degraded, read around the loss via the k survivors.
    assert_eq!(
        read_object(&meta, &fleet, INODE).await.unwrap(),
        Some(data.clone()),
        "object reads correctly while under-replicated (seed killed D server {victim})"
    );

    // Reconstruction sees only the healthy fleet/topology (the victim is gone).
    let (topo, healthy) = healthy_view(victim, &d);
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-reconstruction").await;
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed, "the chunk was reconstructed");

    // Obligation drained; exactly ONE version-conditional commit; full redundancy.
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the repair obligation is drained by the reconstruction commit"
    );
    let record = read_inode(&meta).await;
    assert_eq!(record.version, 2, "exactly one version-conditional commit");
    assert!(
        !record.chunk_map[0].placement.contains(&victim.into()),
        "the killed server no longer holds a referenced fragment"
    );
    assert_full_redundancy(&record, &d).await;

    // Reads still succeed and return the same bytes — full redundancy, atomic flip.
    assert_eq!(
        read_object(&meta, &fleet, INODE).await.unwrap(),
        Some(data),
        "object reads correctly after repair (full redundancy, atomic flip)"
    );
}

// ---- property 2: commit-point-atomic repair under crash (never a hybrid) ----

async fn prop_commit_point_atomic_under_crash(rng: &mut ChaCha8Rng) {
    let meta = CrashMeta::new();
    let d = servers();
    let fleet = fleet_of(&d);
    let data = write_rs_2_1(&meta, &fleet).await;

    let kill = SeededStorageFaults::kill(rng, N);
    let victim = *kill.faults().keys().next().unwrap() as u16;
    apply_storage_faults(&d, &kill).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    let (topo, healthy) = healthy_view(victim, &d);
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-crash").await;

    // CRASH the custodian just before its version-conditional commit lands.
    meta.arm();
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "a crash before the commit changes nothing the store committed"
    );

    // FULLY OLD, NEVER A HYBRID: the inode is untouched — version and placement unchanged.
    let crashed = read_inode(&meta).await;
    assert_eq!(crashed.version, 1, "no version-conditional commit landed");
    assert_eq!(
        crashed.chunk_map[0].placement,
        vec![0, 1, 2],
        "the committed placement is fully old — never a torn/hybrid chunk"
    );
    assert!(
        !repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the obligation stays queued for the restarted custodian"
    );

    // The placed-but-uncommitted rebuilt fragment is on server 3 (the free domain) but is
    // referenced by NO committed chunk map — collectable garbage, not corruption.
    assert!(
        d[3].get_fragment(frag(victim)).await.unwrap().is_some(),
        "the rebuilt fragment was placed before the (crashed) commit"
    );
    assert!(
        !crashed.chunk_map[0].placement.contains(&3),
        "the placed-but-uncommitted fragment is unreferenced garbage, not part of the chunk"
    );

    // Reads STILL succeed (degraded, read around the loss) — no corruption from the crash.
    assert_eq!(
        read_object(&meta, &fleet, INODE).await.unwrap(),
        Some(data.clone()),
        "the object reads correctly after the crash (no hybrid, no corruption)"
    );

    // RESTART: the custodian comes back and completes to full redundancy — fully new.
    meta.disarm();
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 600)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the restarted custodian repairs"
    );
    let record = read_inode(&meta).await;
    assert_eq!(
        record.version, 2,
        "exactly one commit on the successful pass"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the obligation is drained once repair commits"
    );
    assert_full_redundancy(&record, &d).await;
    assert_eq!(
        read_object(&meta, &fleet, INODE).await.unwrap(),
        Some(data),
        "fully new after restart: the chunk reads correctly at full redundancy"
    );
}

// ---- property 3 (Q2): scrub detects bit-rot then reconstructs ----

async fn prop_scrub_detects_bit_rot_then_reconstructs(rng: &mut ChaCha8Rng) {
    let meta = MemMeta::default();
    let d = servers();
    let fleet = fleet_of(&d);
    let data = write_rs_2_1(&meta, &fleet).await;

    // BIT-ROT a seed-chosen referenced fragment (0..N) in place — a present-but-corrupt
    // shard scrub must catch, exclude, and enqueue (never silently absorb).
    let rot = SeededStorageFaults::pick(rng, N, 1, StorageFault::BitRot);
    let victim = *rot.faults().keys().next().unwrap() as u16;
    apply_storage_faults(&d, &rot).await;

    // SCRUB through the real control point: walk referenced fragments, verify checksums.
    let full: Vec<(DServerId, &dyn ChunkStore)> =
        vec![(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let scrub_ctx = ScrubContext {
        meta: &meta,
        fleet: &full,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-scrub").await;
    let scrubbed = reconcile_step(&zone, &custodian, None, Some(&scrub_ctx), None, None, 100)
        .await
        .unwrap();
    assert_eq!(
        scrubbed,
        Reconciled::Changed,
        "scrub detected the bit-flip on server {victim} and enqueued it"
    );
    assert_eq!(
        repair::queued_repairs(&meta).await.unwrap(),
        vec![CHUNK],
        "scrub enqueued the corrupt chunk on the shared repair queue"
    );

    // RECONSTRUCT: the checksum-failing shard is excluded (never decoded) and rebuilt in
    // place from the survivors; the free domain among {victim's, D} is the victim's own.
    let topo = four_domains();
    let recon_ctx = ReconstructionContext {
        meta: &meta,
        fleet: &full,
        topology: &topo,
    };
    let repaired = reconcile_step(&zone, &custodian, None, None, Some(&recon_ctx), None, 200)
        .await
        .unwrap();
    assert_eq!(repaired, Reconciled::Changed);
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the corruption obligation is drained once the shard is rebuilt"
    );
    let rebuilt = d[victim as usize]
        .get_fragment(frag(victim))
        .await
        .unwrap()
        .unwrap();
    assert!(
        repair::fragment_intact(&rebuilt, CHUNK),
        "the rebuilt fragment verifies its checksum (the corrupt shard was never decoded)"
    );
    let record = read_inode(&meta).await;
    assert_full_redundancy(&record, &d).await;
    assert_eq!(
        read_object(&meta, &fleet, INODE).await.unwrap(),
        Some(data),
        "the object reads correctly after the corrupt shard is reconstructed around"
    );
}

// ---- property 4 (Q3): GC reclaims only true orphans ----

const LIVE: ChunkId = 0x11;
const LEASED: ChunkId = 0x22;
const ORPH_OLD: ChunkId = 0x33;
const ORPH_NEW: ChunkId = 0x44;

/// Commit an inode whose single (un-erasure-coded) chunk's fragment 0 is placed on
/// `dserver` — a committed reference GC must never reclaim.
async fn commit_reference(meta: &MemMeta, dserver: DServerId) {
    let record = InodeRecord {
        size: 5,
        chunk_map: vec![ChunkRef {
            id: LIVE,
            scheme: EcScheme::None,
            len: 5,
            placement: vec![dserver],
        }],
        state: InodeState::Committed,
        version: 1,
    };
    let outcome = metadata::create(meta, ROOT, "live", INODE, &record)
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
}

async fn prop_gc_reclaims_only_true_orphans(rng: &mut ChaCha8Rng) {
    let meta = MemMeta::default();
    let d = servers();

    // Seed-vary the clock so the timing invariants are exercised across the sweep while
    // the grace inequalities hold by construction.
    let grace = 50u64;
    let now = 1_000 + (rng.next_u32() as u64 % 1_000); // 1000..2000
    let reclaimable_at = now - grace - 1; // strictly past the grace window
    let within_at = now; // now < within_at + grace → still within grace
    let lease_expiry = now - 1; // an expired pending lease

    // A committed reference GC must leave alone — with a STALE, long-expired orphan
    // record pointing at the very same fragment, so the **reference check** is the only
    // thing protecting the bytes (negating it reclaims a referenced fragment — the
    // silent-corruption flip).
    let live = FragmentId {
        chunk: LIVE,
        index: 0,
    };
    d[0].put_fragment(live, Bytes::from_static(b"live"))
        .await
        .unwrap();
    commit_reference(&meta, 0).await;
    mark_orphaned(&meta, 0, live, reclaimable_at).await.unwrap();

    // (a) leased garbage behind an expired pending lease (interrupted write, `0005:289`).
    d[1].put_fragment(
        FragmentId {
            chunk: LEASED,
            index: 0,
        },
        Bytes::from_static(b"leak"),
    )
    .await
    .unwrap();
    metadata::put_pending(
        &meta,
        LEASED,
        &PendingEntry {
            lease_expiry_millis: lease_expiry,
        },
    )
    .await
    .unwrap();

    // (b) an orphan past its grace window (reclaimable) and (c) one within it (reader-safe).
    let old = FragmentId {
        chunk: ORPH_OLD,
        index: 0,
    };
    let new = FragmentId {
        chunk: ORPH_NEW,
        index: 0,
    };
    d[2].put_fragment(old, Bytes::from_static(b"old"))
        .await
        .unwrap();
    d[3].put_fragment(new, Bytes::from_static(b"new"))
        .await
        .unwrap();
    mark_orphaned(&meta, 2, old, reclaimable_at).await.unwrap();
    mark_orphaned(&meta, 3, new, within_at).await.unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-gc").await;
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let ctx = GcContext {
        meta: &meta,
        fleet: &fleet,
        grace_window_millis: grace,
    };

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, now)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "GC reclaimed collectable bytes"
    );

    // NEVER reclaim a referenced fragment; reclaim the expired-lease byte and the
    // past-grace orphan; NEVER tear the within-grace orphan an in-flight reader holds.
    assert!(
        d[0].get_fragment(FragmentId {
            chunk: LIVE,
            index: 0
        })
        .await
        .unwrap()
        .is_some(),
        "a fragment a committed chunk map references is NEVER reclaimed"
    );
    assert!(
        d[1].get_fragment(FragmentId {
            chunk: LEASED,
            index: 0
        })
        .await
        .unwrap()
        .is_none(),
        "the byte behind the expired pending lease is reclaimed"
    );
    assert!(
        d[2].get_fragment(old).await.unwrap().is_none(),
        "an orphan past its reader-safe grace window is reclaimed"
    );
    assert!(
        d[3].get_fragment(new).await.unwrap().is_some(),
        "an orphan within its grace window is never reclaimed (the in-flight reader is safe)"
    );
}

// ---- property 5: a fenced stale leader lands no location update ----

async fn prop_fenced_stale_leader_lands_nothing(rng: &mut ChaCha8Rng) {
    let meta = MemMeta::default();
    let d = servers();
    let fleet = fleet_of(&d);
    write_rs_2_1(&meta, &fleet).await;

    let kill = SeededStorageFaults::kill(rng, N);
    let victim = *kill.faults().keys().next().unwrap() as u16;
    apply_storage_faults(&d, &kill).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    let (topo, healthy) = healthy_view(victim, &d);
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy,
        topology: &topo,
    };

    // Two leadership terms: the first leader is deposed, the second is current. `zone`
    // installs both, so its fence rises to the usurper's term.
    let coord = MemCoordination::new();
    let deposed = Custodian::elect(&coord, "zone-fence").await.unwrap();
    let usurper = Custodian::elect(&coord, "zone-fence").await.unwrap();
    assert!(
        usurper.term() > deposed.term(),
        "a later leadership term carries a strictly greater fencing token"
    );
    let mut zone = FencedZone::new();
    zone.install(deposed.leadership());
    zone.install(usurper.leadership());

    // The DEPOSED leader is fenced out — its reconciliation is rejected and NOTHING lands.
    let rejected = reconcile_step(&zone, &deposed, None, None, Some(&ctx), None, 500).await;
    assert!(
        rejected.is_err(),
        "a deposed leader's reconciliation is rejected by its stale fencing token"
    );
    let after_deposed = read_inode(&meta).await;
    assert_eq!(
        after_deposed.version, 1,
        "the fenced leader landed no location update"
    );
    assert!(
        !repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the obligation is untouched by the fenced leader"
    );

    // The CURRENT leader acts and repairs.
    let outcome = reconcile_step(&zone, &usurper, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed);
    let after_current = read_inode(&meta).await;
    assert_eq!(
        after_current.version, 2,
        "the current leader's repair commits exactly once"
    );

    // Even RACING after the new leader, the deposed leader still lands nothing.
    let raced = reconcile_step(&zone, &deposed, None, None, Some(&ctx), None, 600).await;
    assert!(raced.is_err(), "the deposed leader stays fenced");
    assert_eq!(
        read_inode(&meta).await.version,
        2,
        "no further update lands — the deposed leader changed nothing, even racing"
    );
}

// ---- property 6: durability-plane emission rises then returns to zero ----

async fn prop_durability_emission_rises_then_returns_to_zero(rng: &mut ChaCha8Rng) {
    let meta = MemMeta::default();
    let d = servers();
    let fleet = fleet_of(&d);
    write_rs_2_1(&meta, &fleet).await;

    let kill = SeededStorageFaults::kill(rng, N);
    let victim = *kill.faults().keys().next().unwrap() as u16;
    apply_storage_faults(&d, &kill).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    let (topo, healthy) = healthy_view(victim, &d);
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy,
        topology: &topo,
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-telemetry").await;

    // PASS 1 — under-replicated: the count RISES, queue depth and time-to-repair emit.
    let rise = MetricCapture::default();
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .with_subscriber(tracing_subscriber::registry().with(rise.clone()))
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed);
    assert_eq!(
        rise.values("monotonic_counter.reconstruction_under_replicated"),
        vec![1],
        "the under-replicated count rises to 1 after the injected loss"
    );
    assert_eq!(
        rise.values("histogram.reconstruction_queue_depth"),
        vec![1],
        "the repair-queue depth is emitted and correct (one obligation observed)"
    );
    assert_eq!(
        rise.values("histogram.reconstruction_time_to_repair_millis"),
        vec![500],
        "a time-to-repair sample is emitted at the repair instant"
    );

    // PASS 2 — repaired: the count RETURNS TO ZERO and the queue is drained.
    let settle = MetricCapture::default();
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 600)
        .with_subscriber(tracing_subscriber::registry().with(settle.clone()))
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Satisfied);
    assert_eq!(
        settle.values("monotonic_counter.reconstruction_under_replicated"),
        vec![0],
        "the under-replicated count returns to zero once repair completes"
    );
    assert_eq!(
        settle.values("histogram.reconstruction_queue_depth"),
        vec![0],
        "the repair-queue depth is back to zero (drained)"
    );
    assert!(
        settle
            .values("histogram.reconstruction_time_to_repair_millis")
            .is_empty(),
        "no repair is dispatched once the chunk is at full redundancy"
    );
}

// ---- the seed sweep: each property over the run seed (madsim sweeps MADSIM_TEST_NUM) ----

/// A fresh ChaCha RNG seeded from the madsim run seed, so the whole campaign — *which*
/// server is killed/rotted included — reproduces from the run seed (ADR-0009), exactly
/// as the network DST campaign does (`tests/network.rs`).
fn rand_seed() -> ChaCha8Rng {
    ChaCha8Rng::seed_from_u64(madsim::runtime::Handle::current().seed())
}

#[madsim::test]
async fn reconstruct_to_full_redundancy_q1() {
    prop_reconstruct_to_full_redundancy(&mut rand_seed()).await;
}

#[madsim::test]
async fn commit_point_atomic_repair_under_crash() {
    prop_commit_point_atomic_under_crash(&mut rand_seed()).await;
}

#[madsim::test]
async fn scrub_detects_bit_rot_then_reconstructs_q2() {
    prop_scrub_detects_bit_rot_then_reconstructs(&mut rand_seed()).await;
}

#[madsim::test]
async fn gc_reclaims_only_true_orphans_q3() {
    prop_gc_reclaims_only_true_orphans(&mut rand_seed()).await;
}

#[madsim::test]
async fn fenced_stale_leader_lands_nothing() {
    prop_fenced_stale_leader_lands_nothing(&mut rand_seed()).await;
}

#[madsim::test]
async fn durability_emission_rises_then_returns_to_zero() {
    prop_durability_emission_rises_then_returns_to_zero(&mut rand_seed()).await;
}

// ---- committed regression seeds (ADR-0009: a bug-finding seed is a permanent test) ----

/// Seeds committed as **permanent regressions** (ADR-0009, `0005:374`): the campaign
/// replays this fixed set on every run, independent of the madsim sweep, so a seed that
/// ever surfaces a custodian bug stays green forever after the fix. Seeded directly
/// (not via the madsim scheduler), so each is a deterministic, reproducible run of all
/// six properties. New bug-finding seeds are appended here.
const REGRESSION_SEEDS: &[u64] = &[
    0x5EED_0000_0000_0001,
    0x5EED_0000_0000_0002,
    0x5EED_0000_0000_0003,
    0x5EED_0000_0000_0005,
    0x5EED_0000_0000_0008,
    0x5EED_0000_0000_000D,
    0x5EED_0000_0000_0015,
    0x5EED_0000_0000_0022,
];

#[madsim::test]
async fn committed_regression_seeds_stay_green() {
    for &seed in REGRESSION_SEEDS {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        prop_reconstruct_to_full_redundancy(&mut rng).await;
        prop_commit_point_atomic_under_crash(&mut rng).await;
        prop_scrub_detects_bit_rot_then_reconstructs(&mut rng).await;
        prop_gc_reclaims_only_true_orphans(&mut rng).await;
        prop_fenced_stale_leader_lands_nothing(&mut rng).await;
        prop_durability_emission_rises_then_returns_to_zero(&mut rng).await;
    }
}
