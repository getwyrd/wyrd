//! Tier-0 **custodian property campaign** — the consolidated M3 verification gate
//! (proposal 0005 §"DST and tests (the heart of M3)", `0005:369-411`; the graduation
//! criteria `0005:500-502`; PR-sequence slice 8 `0005:541-545`; ADR-0009). M3's four
//! custodian loops (GC #142, scrub #143, reconstruction #144, rebalance #145) shipped
//! with *per-slice* tests; this suite is the **campaign** that sweeps seeds over the
//! eight §13/§10 properties continuously inside the deterministic simulator (`0005:371`).
//!
//! Every property runs through the **real** [`reconcile_step`] fenced control point over
//! the `MetadataStore` / `ChunkStore` trait seams (Option A — no deployed custodian
//! process exists yet, `0005:524-527`). The faults are drawn from the run **seed** via
//! the testkit storage-fault seam ([`SeededStorageFaults`] — the bit-rot / fragment-loss
//! and D-server-kill seam this slice adds, `0005:434-435`), so the whole campaign is a pure
//! function of its seed: a bug-finding seed replays the *same* killed/rotted servers and
//! is committed as a permanent regression ([`REGRESSION_SEEDS`], ADR-0009).
//!
//! The eight Tier-0 properties (the six of `0005:378-403`, plus the two crash-window
//! edges #199 adds — property 2 covers the commit-boundary crash, properties 7 and 8 the
//! near edge of the write step and the reader's atomic flip across the repoint):
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
//!   7. **Crash mid-write commits nothing** (#199) — a crash *inside* the fragment-write
//!      step (before the write is durable) places **nothing** and never reaches the commit:
//!      the chunk map is fully old, the obligation stays queued, and the restart repairs.
//!      The near edge of the window property 2 covers from the commit boundary.
//!   8. **Reader flips atomically across the repoint** (#199) — a reader racing the single
//!      version-conditional commit observes the placement **fully old XOR fully new, never a
//!      mix**; both an old-placement and a new-placement reader read the correct object.
//!
//! Tier-1 (dm-flakey/dm-error + Jepsen) and Tier-2 (single-node kill-and-reconstruct)
//! are the **deferred-posture** deliverables (`0005:405-411`): they need the block layer
//! / a real node and are observed off-Check, not in this worktree.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`, which sweeps 50 seeds); a normal
//! `cargo test` neither builds nor runs this file.

#![forbid(unsafe_code)]
#![cfg(madsim)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
use wyrd_core::read::{read_object, read_object_from};
use wyrd_core::repair;
use wyrd_core::write;
use wyrd_core::write::write_new_object_placed;
use wyrd_custodian::{
    mark_orphaned, reconcile_after_restore, reconcile_step, Custodian, ExpiredPendingPolicy,
    FencedZone, GcContext, Reconciled, ReconstructionContext, ScrubContext,
};
// The DST determinism barrier preamble (ADR-0035): declaring every campaign property
// through this macro installs the permissive global `tracing` default unbypassably.
use wyrd_dst::dst_campaign_test;
use wyrd_testkit::{SeededStorageFaults, StorageFault};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
    PlacementChunkStore, Result, WriteBatch,
};

// The DST tier's **second** `MetadataStore` implementation — the deterministic
// simulated-TiKV model whose every read and commit spans real madsim await boundaries
// (`network_hop`). Property 11 below needs those boundaries: they are what lets a genuinely
// concurrent writer land *between* the post-restore pass's two readings of the committed
// namespace, which an in-memory store that never yields cannot produce.
#[path = "support/mod.rs"]
mod support;
use support::SimTikvMetadataStore;

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

    // The required paginated read (#634): a test double needs *a* body, not a
    // backend's — the dev-only testkit helper pages over this store's own `scan`
    // (and therefore inherits `SCAN_CAP`, which a backend may not).
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

    // The required paginated read (#634): a test double needs *a* body, not a
    // backend's — the dev-only testkit helper pages over this store's own `scan`
    // (and therefore inherits `SCAN_CAP`, which a backend may not).
    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
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

/// A **crash-injecting** D server wrapping a [`MemDServer`]: while *armed*, every
/// `put_fragment` **fails without storing**, modelling the custodian **dying mid-write** —
/// the rebuilt fragment never reaching durable storage. This crashes the repair *strictly
/// earlier* than [`CrashMeta`] (which drops the commit *after* the fragment is written):
/// the two bracket the whole "fragment writes → commit" window of the heart-of-M3 loop
/// (`reconstruction.rs:389-414` then `416-445`). The repair writes the fragment **before**
/// the commit (`0005:325`), so a `put` that never completes leaves NOTHING placed — not
/// even collectable garbage — and the version-conditional commit is never reached, so the
/// chunk map is untouched (`0005:277`). The error propagates out of `repair_chunk`'s
/// `put_fragment(..).await?` (`reconstruction.rs:407`) as a `ReconcileError::Store`, the
/// trait-boundary shape of a custodian that died with the write in flight. Disarm to let
/// the restarted custodian finish.
struct CrashStore<'a> {
    inner: &'a MemDServer,
    armed: &'a AtomicBool,
}

#[async_trait]
impl ChunkStore for CrashStore<'_> {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        if self.armed.load(Ordering::Relaxed) {
            // The write dies in flight: nothing is stored, and the fault surfaces to the
            // reconciler exactly as a real backend's interrupted write would.
            return Err(Box::new(std::io::Error::other(
                "simulated mid-write crash: the rebuilt fragment write never completed",
            )));
        }
        self.inner.put_fragment(id, fragment, deadline_millis).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        self.inner.health().await
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
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> Result<()> {
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
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        if let Some(store) = self.store(DServerId::from(id.index)) {
            store.put_fragment(id, fragment, deadline_millis).await?;
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
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        if let Some(store) = self.store(dserver) {
            store.put_fragment(id, fragment, deadline_millis).await?;
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

// The `tracing` interest-cache determinism barrier this campaign depends on is no longer a
// per-test convention here: it is a substrate property installed unconditionally by the
// `dst_campaign_test!` preamble (`crates/dst/src/lib.rs`, ADR-0035). Every property below is
// declared through that macro, so the permissive global default is installed (fail-loud,
// once) before any callsite is hit — a property cannot be written without it. The superseded
// per-test `install_metric_dispatch()` is gone (#242, #243).

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
        || 0,
        1_000,
        || CHUNK,
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    assert_eq!(
        read_inode(meta).await.chunk_map.as_flat().unwrap()[0].placement,
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
                d[i].put_fragment(f, Bytes::from(bytes), None)
                    .await
                    .unwrap();
            }
        }
    }
}

/// Assert the chunk is back at **full redundancy**: every placed fragment is present and
/// verifies its checksum, and the `n` fragments occupy `n` distinct failure domains.
async fn assert_full_redundancy(record: &InodeRecord, d: &[MemDServer; 4]) {
    let placement = &record.chunk_map.as_flat().unwrap()[0].placement;
    assert_eq!(placement.len(), N, "n fragments placed");
    let mut domains = HashSet::new();
    for (index, &server) in placement.iter().enumerate() {
        let bytes = d[server as usize]
            .get_fragment(frag(index as u16))
            .await
            .unwrap()
            .expect("fragment present after repair");
        assert!(
            repair::fragment_intact(
                &bytes,
                frag(index as u16),
                EcScheme::ReedSolomon {
                    k: K as u8,
                    m: M as u8
                }
            ),
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
        unreachable: &[],
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
        !record.chunk_map.as_flat().unwrap()[0]
            .placement
            .contains(&victim.into()),
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
        unreachable: &[],
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
        crashed.chunk_map.as_flat().unwrap()[0].placement,
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
        !crashed.chunk_map.as_flat().unwrap()[0]
            .placement
            .contains(&3),
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
        unreachable: &[],
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
        repair::fragment_intact(
            &rebuilt,
            frag(victim),
            EcScheme::ReedSolomon {
                k: K as u8,
                m: M as u8
            }
        ),
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
        }]
        .into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
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
    d[0].put_fragment(live, Bytes::from_static(b"live"), None)
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
        None,
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
    d[2].put_fragment(old, Bytes::from_static(b"old"), None)
        .await
        .unwrap();
    d[3].put_fragment(new, Bytes::from_static(b"new"), None)
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
        expired_pending: ExpiredPendingPolicy::Reclaim,
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
        unreachable: &[],
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
        unreachable: &[],
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
        rise.values("gauge.reconstruction_under_replicated"),
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
        settle.values("gauge.reconstruction_under_replicated"),
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

// ---- property 7: a crash DURING the fragment write commits nothing (the window's near edge) --

/// **Crash mid-write — strictly earlier than [`prop_commit_point_atomic_under_crash`].**
/// That property crashes at the commit boundary (the fragment already written, surviving as
/// collectable garbage); this one crashes *inside* the fragment-write step, before the write
/// is durable, so the two **bracket the whole "fragment writes → commit" window** the heart
/// of M3 is structurally safe across (`reconstruction.rs:389-414` then `416-445`;
/// `0005:277`, `0005:385-389`). RS(2,1) rebuilds exactly one fragment, so the finest crash
/// point before the commit is the rebuilt write itself failing in flight ([`CrashStore`]).
///
/// The invariant: a write that never completes leaves the committed chunk map **fully old**
/// (no version-conditional commit ran), places **nothing** — not even garbage — and the
/// obligation **stays queued**, so the restarted custodian repairs cleanly. A crash here is
/// never a torn/hybrid chunk and never silent data loss.
async fn prop_crash_mid_write_commits_nothing(rng: &mut ChaCha8Rng) {
    let meta = MemMeta::default();
    let d = servers();
    let fleet = fleet_of(&d);
    let data = write_rs_2_1(&meta, &fleet).await;

    let kill = SeededStorageFaults::kill(rng, N);
    let victim = *kill.faults().keys().next().unwrap() as u16;
    apply_storage_faults(&d, &kill).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // The rebuilt fragment always lands on the one free domain (D = server 3) for any victim
    // in 0..N, so a crash store at server 3 intercepts the rebuild write whichever server the
    // seed killed. The survivors (the non-victim servers in 0,1,2) stay plain D servers.
    let armed = AtomicBool::new(false);
    let crash3 = CrashStore {
        inner: &d[3],
        armed: &armed,
    };
    let mut topo = Topology::default();
    let mut healthy: Vec<(DServerId, &dyn ChunkStore)> = Vec::new();
    for id in 0u64..4 {
        if id as u16 == victim {
            continue;
        }
        topo.register(id, domain_letter(id));
        if id == 3 {
            healthy.push((3, &crash3 as &dyn ChunkStore));
        } else {
            healthy.push((id, &d[id as usize] as &dyn ChunkStore));
        }
    }
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy,
        topology: &topo,
        unreachable: &[],
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-midwrite").await;

    // CRASH the custodian inside the fragment-write step — the put never completes.
    armed.store(true, Ordering::Relaxed);
    let crashed = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500).await;
    assert!(
        crashed.is_err(),
        "a write that dies in flight surfaces as a store error — the custodian died mid-repair"
    );

    // FULLY OLD: no version-conditional commit ran, so the inode is byte-for-byte its prior.
    let after = read_inode(&meta).await;
    assert_eq!(after.version, 1, "no commit landed");
    assert_eq!(
        after.chunk_map.as_flat().unwrap()[0].placement,
        vec![0, 1, 2],
        "the committed placement is fully old — never a torn/hybrid chunk"
    );
    // NOTHING PLACED — not even collectable garbage: the interrupted write stored no bytes
    // (the stricter sibling of the commit-boundary crash, where the fragment IS written).
    assert!(
        d[3].get_fragment(frag(victim)).await.unwrap().is_none(),
        "the in-flight write left no rebuilt fragment on the target server"
    );
    assert!(
        !repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the obligation stays queued for the restarted custodian"
    );
    // Reads STILL succeed — degraded, read around the loss; the crash caused no corruption.
    assert_eq!(
        read_object(&meta, &fleet, INODE).await.unwrap(),
        Some(data.clone()),
        "the object reads correctly after the mid-write crash (no hybrid, no corruption)"
    );

    // RESTART: the custodian comes back, the write completes, and the repair commits once.
    armed.store(false, Ordering::Relaxed);
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
        "fully repaired after restart: the chunk reads correctly at full redundancy"
    );
}

// ---- property 8: a reader racing the commit window flips atomically (old XOR new, never a mix) --

/// **The reader's view flips atomically across the repoint.** The location update is ONE
/// version-conditional commit (`reconstruction.rs:416-445`, `0005:277`), and a reader
/// resolves placement from the single inode record it carries — so the *only* states a
/// reader racing the commit can observe are the **fully-old** inode (v1) and the
/// **fully-new** inode (v2); there is no third, hybrid inode, so {old, new} is the
/// **exhaustive** race surface, not a sample of it. (The in-memory trait ops never yield
/// mid-commit, so this boundary check is complete — a spawned reader could observe nothing
/// the two snapshots here do not.)
///
/// The property models both racers against the **live** fleet *after* the flip has landed:
/// - a reader that resolved the **old** placement before the commit ([`read_object_from`]
///   with the v1 inode) still reads the correct, complete object — degraded, reconstructing
///   around the killed fragment from its `k` survivors (which the repair never touched); and
/// - a reader that resolves the **new** placement after the commit ([`read_object`]) reads
///   the correct, complete object at full redundancy.
///
/// Both return byte-identical original data, and the placement repoints as a **whole vector**
/// (the new differs from the old at exactly the rebuilt index) — never a per-index mix.
async fn prop_reader_flips_atomically_across_commit(rng: &mut ChaCha8Rng) {
    let meta = MemMeta::default();
    let d = servers();
    let fleet = fleet_of(&d);
    let data = write_rs_2_1(&meta, &fleet).await;

    let kill = SeededStorageFaults::kill(rng, N);
    let victim = *kill.faults().keys().next().unwrap() as u16;
    apply_storage_faults(&d, &kill).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // A reader that ENTERS the commit window resolves the OLD inode (v1, placement [0,1,2]).
    let old = read_inode(&meta).await;
    assert_eq!(old.version, 1);
    assert_eq!(old.chunk_map.as_flat().unwrap()[0].placement, vec![0, 1, 2]);

    // The repoint lands as a single atomic commit.
    let (topo, healthy) = healthy_view(victim, &d);
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &healthy,
        topology: &topo,
        unreachable: &[],
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-reader-race").await;
    let outcome = reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .unwrap();
    assert_eq!(outcome, Reconciled::Changed, "the chunk was reconstructed");

    // A reader on the far side of the window resolves the NEW inode (v2).
    let new = read_inode(&meta).await;

    // ATOMIC, WHOLE-VECTOR FLIP: exactly one version transition (no hybrid inode between),
    // and the placement changed only at the rebuilt index — never a per-index mix.
    assert_eq!(new.version, 2, "exactly one atomic transition (v1 → v2)");
    let differing: Vec<usize> = (0..N)
        .filter(|&i| {
            new.chunk_map.as_flat().unwrap()[0].placement[i]
                != old.chunk_map.as_flat().unwrap()[0].placement[i]
        })
        .collect();
    assert_eq!(
        differing,
        vec![victim as usize],
        "the repoint flips the whole placement vector, changing only the rebuilt index"
    );
    assert_eq!(
        new.chunk_map.as_flat().unwrap()[0].placement[victim as usize],
        3,
        "the rebuilt fragment moved to the free failure domain (D = server 3)"
    );

    // OLD reader, finishing AFTER the flip: still fully consistent — reads around the killed
    // fragment from the `k` survivors the repair never disturbed. Never a torn/mixed read.
    assert_eq!(
        read_object_from(&fleet, &old).await.unwrap(),
        data,
        "a reader holding the old placement still reads the correct, complete object"
    );
    // NEW reader: fully consistent at full redundancy.
    assert_eq!(
        read_object(&meta, &fleet, INODE).await.unwrap(),
        Some(data),
        "a reader resolving the new placement reads the correct, complete object"
    );
}

// ---- property 9: the chunk-map RESOLVER never tears (issue #649, proposal 0016
//      decision 7(h)) ----
//
// Shipped in THIS slice and exercised by the gating `cargo xtask ci` / `dst` tier over
// the whole madsim seed sweep — deliberately NOT by the per-fix `C4-verify` check, which
// would have to build the `--cfg madsim` tree and sweep 50 seeds to see it. Built and run
// this cycle; not deferred work.

/// A segment-group nonce for this property's fixtures: 32 lowercase hex characters.
const RESOLVE_TEAR_NONCE: &str = "0123456789abcdef0123456789abcdef";

/// A store that, the FIRST time it is asked to page a `seg:` range, applies a pending
/// mutation — the exact race the resolve-retry rule exists for (`0016:2452-2462`: the root
/// always moves first): a reader that read the OLD root and is now paging its `seg:` range
/// meets a root that has already moved on.
struct RetireMidResolve {
    inner: MemMeta,
    pending: Mutex<Option<WriteBatch>>,
}

#[async_trait]
impl MetadataStore for RetireMidResolve {
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
        if prefix.starts_with(b"seg:") {
            let pending = self.pending.lock().unwrap().take();
            if let Some(batch) = pending {
                assert_eq!(
                    self.inner.commit(batch).await.unwrap(),
                    CommitOutcome::Committed
                );
            }
        }
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        self.inner.commit(batch).await
    }
}

fn seg_row(group: &metadata::SegmentGroup, index: u32) -> Vec<u8> {
    metadata::seg_key(group, index).expect("addressable index")
}

/// Every `(dserver, fragment)` a committed chunk list places — the same expansion the
/// reference build performs, so an expectation built from it is in the units GC reasons in.
fn fragments_of(chunks: &[metadata::ChunkRef]) -> Vec<(DServerId, FragmentId)> {
    chunks
        .iter()
        .flat_map(|chunk| {
            chunk.fragments().map(move |(index, dserver)| {
                (
                    dserver,
                    FragmentId {
                        chunk: chunk.id,
                        index,
                    },
                )
            })
        })
        .collect()
}

/// The fragments of whichever generation the store holds **now**, as the production resolver
/// itself reports it (`None` when its map cannot be read) — the store's own answer, never a
/// restatement of the fixture.
async fn live_fragments(meta: &MemMeta, root_key: &[u8]) -> Option<Vec<(DServerId, FragmentId)>> {
    match metadata::resolve_current_chunk_map(meta, root_key).await {
        Ok(Some(resolved)) => Some(fragments_of(&resolved.chunks)),
        Ok(None) => panic!("the object is live here"),
        Err(_) => None,
    }
}

/// Seed a genuine flat object at `INODE` (real fragments over `fleet`, `EcScheme::None` so
/// a single fragment resolves through the identity placement onto server 0), then re-spell
/// it as a **segmented** generation directly — raw `seg:` records plus a segmented root,
/// written by hand, never via a committer (this slice ships no producer). Hands back the
/// group its segments are keyed by, so the campaign can retire one of its records the way
/// a drain would.
async fn seed_segmented(meta: &MemMeta, fleet: &Fleet<'_>, data: &[u8]) -> metadata::SegmentGroup {
    let mut next = 0u128;
    let plan = write::plan_write(data, 4, EcScheme::None, || {
        next += 1;
        next
    })
    .unwrap();
    write::intent(meta, &plan, 1_000).await.unwrap();
    write::write_fragments(fleet, &plan).await.unwrap();
    assert_eq!(
        write::commit_create(meta, ROOT, "obj", INODE, &plan, 0)
            .await
            .unwrap(),
        CommitOutcome::Committed
    );
    write::release(meta, &plan).await.unwrap();

    let chunks = plan.chunk_refs();
    let half = chunks.len() / 2;
    let group = metadata::SegmentGroup::new(RESOLVE_TEAR_NONCE, 1).unwrap();
    let first = metadata::SegmentRecord::new(chunks[..half].to_vec(), 0).unwrap();
    let second = metadata::SegmentRecord::new(chunks[half..].to_vec(), first.byte_len()).unwrap();
    let seg_ref = |index: u32, byte_offset: u64, byte_len: u64| metadata::SegmentRef {
        index,
        byte_offset,
        byte_len,
    };
    let table = metadata::SegmentedMap::new(
        group.clone(),
        vec![
            seg_ref(0, 0, first.byte_len()),
            seg_ref(1, first.byte_len(), second.byte_len()),
        ],
    )
    .unwrap();
    let root = InodeRecord {
        size: table.span(),
        chunk_map: metadata::ChunkMap::Segmented(table),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let batch = WriteBatch::new()
        .put(seg_row(&group, 0), metadata::encode(&first))
        .put(seg_row(&group, 1), metadata::encode(&second))
        .put(metadata::inode_key(INODE), metadata::encode(&root));
    assert_eq!(meta.commit(batch).await.unwrap(), CommitOutcome::Committed);
    group
}

/// **The resolver never tears.** A segmented object's root is retired — superseded by a
/// fresh flat generation — in the exact window between a reader's root read and its `seg:`
/// range read, and on half the seeds the drain has *already reclaimed* one of the retired
/// generation's segment records by the time that range read lands (`0016:2452-2462`: the
/// root moves first, its records are deleted after). The resolution the reader began is
/// retired either way, so the only whole answer left is the live generation: never a byte
/// mix of the two, never a short read, and never `NoSuchKey` (an overwrite is not a
/// deletion).
///
/// The reclaiming arm is what makes this bind the *restart*: with the old generation's map
/// genuinely incompletable, a reader that did not re-read the root and start again could
/// only fail or answer short — it has no old-generation answer left to succeed with by
/// accident.
async fn prop_segmented_resolve_never_tears(rng: &mut ChaCha8Rng) {
    let d = servers();
    let fleet = fleet_of(&d);
    let meta = MemMeta::default();
    let old_data: Vec<u8> = (0..32u16).map(|i| i as u8).collect();
    let group = seed_segmented(&meta, &fleet, &old_data).await;

    // The generation that replaces it while the reader's `seg:` range read is in flight —
    // seed-chosen content and length, so the campaign is a pure function of the run seed.
    let new_len = 16 + (rng.next_u32() as usize % 32);
    let new_data: Vec<u8> = (0..new_len).map(|_| rng.next_u32() as u8).collect();
    let mut next = 0x1000u128;
    let new_plan = write::plan_write(&new_data, 4, EcScheme::None, || {
        next += 1;
        next
    })
    .unwrap();
    write::write_fragments(&fleet, &new_plan).await.unwrap();
    let new_root = InodeRecord {
        size: new_plan.size,
        chunk_map: new_plan.chunk_refs().into(),
        state: InodeState::Committed,
        version: 2,
        ..Default::default()
    };
    let mut flip = WriteBatch::new().put(metadata::inode_key(INODE), metadata::encode(&new_root));
    // The nemesis: on half the seeds the drain has already taken segment 1 of the retired
    // generation, so its map can no longer be completed at all.
    let reclaimed = rng.next_u32().is_multiple_of(2);
    if reclaimed {
        flip = flip.delete(seg_row(&group, 1));
    }

    let store = RetireMidResolve {
        inner: meta,
        pending: Mutex::new(Some(flip)),
    };
    match read_object(&store, &fleet, INODE).await.unwrap() {
        Some(bytes) => assert_eq!(
            bytes,
            new_data,
            "a resolve retired mid-read must answer the WHOLE live generation (old \
             generation's segment reclaimed: {reclaimed}); the old bytes were {} long",
            old_data.len()
        ),
        None => panic!(
            "an overwrite is not a deletion: NoSuchKey is the wrong answer to a root \
             retired mid-resolve"
        ),
    }
}

// ---- property 10: GC's reference build over a SEGMENTED map — never reclaims a live
//      segmented object's bytes, and never certifies a store it could only partly read
//      (issue #650, proposal 0016 decision 7(e)) ----
//
// The deletion-capable pass is the one that cannot be wrong here: it holds
// `delete_fragment`. Seeded, because the two ways a resolve can go sideways are both RACES
// — a generation retired under the build (property 9's nemesis, here met by the pass that
// deletes rather than the one that reads) and a generation left incomplete — and the arm is
// drawn from the run seed, so the campaign stays a pure function of it.

/// A genuinely collectable orphan, on a server the segmented object does not use, past its
/// grace window: reclaiming it is the POSITIVE observable that the pass ran at all, so
/// "nothing of the segmented object was deleted" cannot pass by the pass having done
/// nothing.
const SEG_ORPHAN: ChunkId = 0x650;

async fn prop_gc_over_a_segmented_map_never_reclaims_it_and_never_over_certifies(
    rng: &mut ChaCha8Rng,
) {
    let d = servers();
    let fleet = fleet_of(&d);
    let meta = MemMeta::default();
    let data: Vec<u8> = (0..32u16).map(|i| i as u8).collect();
    let group = seed_segmented(&meta, &fleet, &data).await;

    let grace = 50u64;
    let now = 1_000 + (rng.next_u32() as u64 % 1_000);
    let orphan = FragmentId {
        chunk: SEG_ORPHAN,
        index: 0,
    };
    d[3].put_fragment(orphan, Bytes::from_static(b"garbage"), None)
        .await
        .unwrap();
    mark_orphaned(&meta, 3, orphan, now - grace - 1)
        .await
        .unwrap();

    // The segmented generation's own fragments, as the RESOLVER reports them, read while
    // everything is still readable — every arm starts from this generation.
    let root_key = metadata::inode_key(INODE);
    let seeded = live_fragments(&meta, &root_key)
        .await
        .expect("the seeded segmented generation resolves before any arm disturbs it");

    // The seed picks which race (if any) the reference build meets.
    let arm = rng.next_u32() % 3;
    // Arm 2 leaves the LIVE generation incomplete: one of its segment records is gone while
    // the root still names the group, so the map cannot be read at all and the build must
    // fail closed rather than conclude the object owns no chunks.
    if arm == 2 {
        assert_eq!(
            meta.commit(WriteBatch::new().delete(seg_row(&group, 1)))
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
    }

    // Arm 1 retires the generation under the build — the root moves first, and on this arm
    // the drain has already taken one of the retired generation's records, so the resolution
    // in flight cannot be completed and only a restart onto the live root can answer.
    let mut new_plan = None;
    let mut flip = WriteBatch::new();
    if arm == 1 {
        let mut next = 0x1000u128;
        let plan = write::plan_write(&data, 4, EcScheme::None, || {
            next += 1;
            next
        })
        .unwrap();
        write::write_fragments(&fleet, &plan).await.unwrap();
        let new_root = InodeRecord {
            size: plan.size,
            chunk_map: plan.chunk_refs().into(),
            state: InodeState::Committed,
            version: 2,
            ..Default::default()
        };
        flip = flip
            .put(metadata::inode_key(INODE), metadata::encode(&new_root))
            .delete(seg_row(&group, 1));
        new_plan = Some(plan);
    }

    // The fragments that belong to the generation which is LIVE **while the pass runs** —
    // the successor on the retirement arm, since its root flip lands inside the pass, and the
    // seeded generation otherwise. Getting this wrong is how the arm stops binding: expecting
    // the RETIRED generation's fragments would pass on a build that never restarted onto the
    // live root at all.
    let live: Vec<(DServerId, FragmentId)> = match &new_plan {
        Some(plan) => fragments_of(&plan.chunk_refs()),
        None => seeded.clone(),
    };

    // DELETION EVIDENCE on every one of them: a grace record that lapsed before this pass's
    // clock, so each live fragment is one unreferenced verdict away from `delete_fragment`.
    // Without it the survival assertions below are vacuous — GC deletes only what it has a
    // deadline for, so a build that never resolved the object (or never restarted onto the
    // live generation) would leave them alone anyway and the arm would pass on a defect.
    for &(dserver, frag) in &live {
        mark_orphaned(&meta, dserver, frag, now - grace - 1)
            .await
            .unwrap();
    }

    let store = RetireMidResolve {
        inner: meta,
        pending: Mutex::new(Some(flip)),
    };
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-gc-segmented").await;
    let dyn_fleet: [(DServerId, &dyn ChunkStore); 4] =
        [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let ctx = GcContext {
        meta: &store,
        fleet: &dyn_fleet,
        grace_window_millis: grace,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, now)
        .await
        .expect("a segmented map is resolved, or contained — never an error that ends the pass");

    if arm == 2 {
        // Incomplete: the pass may not certify, and may not reclaim a byte — not even the
        // orphan, which no fragment can be shown not to be one of the unreadable object's.
        assert_eq!(
            outcome,
            Reconciled::Blocked,
            "a reference set the build could not finish must not report convergence"
        );
        assert!(
            d[3].get_fragment(orphan).await.unwrap().is_some(),
            "an incomplete reference set authorizes NO reclamation, fleet-wide"
        );
        // ...and the damaged object's OWN bytes are still there, each carrying a lapsed grace
        // record: on this arm nothing at all names them, and the only thing between them and
        // `delete_fragment` is the refusal to reclaim on a set that could not be finished.
        for &(dserver, frag) in &live {
            assert!(
                d[dserver as usize]
                    .get_fragment(frag)
                    .await
                    .unwrap()
                    .is_some(),
                "a live object's fragment is never reclaimed on the strength of a reference \
                 set the build could not finish (server {dserver})"
            );
        }
        return;
    }

    assert_eq!(
        outcome,
        Reconciled::Changed,
        "with the map readable the pass reclaims the genuine orphan and says so"
    );
    assert!(
        d[3].get_fragment(orphan).await.unwrap().is_none(),
        "the genuine, past-grace orphan IS reclaimed — the pass really ran (arm {arm})"
    );
    for (dserver, frag) in live {
        assert!(
            d[dserver as usize]
                .get_fragment(frag)
                .await
                .unwrap()
                .is_some(),
            "a fragment the live generation's chunk map references is NEVER reclaimed, even \
             carrying a lapsed grace record (arm {arm}, server {dserver})"
        );
    }
    if let Some(plan) = new_plan {
        // The retired generation's root is gone, so the restart resolved the NEW map: the
        // fragments asserted above are the live ones, not the retired object's.
        assert_eq!(
            read_inode(&store).await.chunk_map.as_flat().unwrap(),
            plan.chunk_refs().as_slice(),
            "the build resolved the live generation after the retirement, not the retired one"
        );
    }
}

// ---- property 11: the post-restore pass's TWO readings of the committed namespace never
//      license a mark between them (issue #651) ----
//
// `reconcile_after_restore` is deletion-capable at one remove: an `orphan:` record is the
// evidence GC requires before it reclaims bytes, so a mark this pass writes IS the front half
// of a deletion. The pass reads the committed namespace **twice** — once for the reference set
// its mark gate consults, once for the per-reference verdicts it reports — and a writer can
// land between the two: an object that COMMITS (its fragments referenced by the later reading
// and not the earlier) or a record that stops DECODING (a hole one reading met and the other
// never did).
//
// The per-slice tests in `crates/custodian/tests` drive that through Tokio doubles that publish
// at a hard-coded seam (the instant the first scan is answered): they pin the decision, but they
// choose the schedule. Here the readings are separated by REAL await boundaries — the simulated
// TiKV model's network hops, the DST tier's second `MetadataStore` implementation — and the
// writer is a genuinely concurrent task whose landing point comes from the run seed, so the
// simulator chooses the schedule and sweeps it.
//
// The property is the one the pass's own docs state: a conclusion and the reading it rests on
// are ONE. No fragment is marked while any reading in the pass found a record it could not read,
// and a fragment EITHER reading protects is never marked. It is written implementation-neutrally
// — every assertion is conditioned on what the pass's own readings returned — so a pass that
// read the namespace once would satisfy it too.

/// The chunk of the object that is committed and readable for the whole run: one fragment on
/// server 0, referenced by a record that exists before the pass starts.
const RESTORE_HELD: ChunkId = 0x6511;
/// The chunk of the object whose RECORD commits while the pass runs. Its fragment is on server 1
/// from the start, so the pass's two readings can disagree about whether anything references it.
const RESTORE_LATE: ChunkId = 0x6512;
/// A genuine stray: bytes on server 2 that no record ever references and no ledger accounts for.
/// A reading that FINISHED must mark it — the positive observable that stops every "nothing was
/// marked" assertion below from passing on a pass that did nothing at all.
const RESTORE_STRAY: ChunkId = 0x6513;
/// The inode the late-committing object is published under ([`INODE`] is the readable one).
const LATE_INODE: InodeId = 2;
/// How far past the pass's start the concurrent writer's landing point is drawn from, in
/// simulated milliseconds. One `network_hop` is 1 ms and the pass's two `inode:` readings are
/// three hops apart, so this span spans "before the second reading", the tie with it, and "after
/// the whole pass" — and the coverage property below **proves** the middle one is reached rather
/// than assuming it.
const RESTORE_NEMESIS_SPAN: u32 = 6;

/// A recording tap over the DST tier's simulated-TiKV store: every trait call is forwarded
/// unchanged — including the network hops that make a concurrent task's landing point matter —
/// and each `inode:` scan's ANSWER is kept.
///
/// What it records is what the PRODUCTION pass's own readings returned, so the assertions below
/// are conditioned on what the pass actually saw rather than on the fixture's intended timing:
/// under a scheduler that resolves ties from the seed, "when did the writer land" is not a fact
/// the test may assume. Instance state only (never a `static`), so it lives inside the simulated
/// world and cannot leak across seeds or threads (ADR-0035).
struct RecordingMeta {
    inner: SimTikvMetadataStore,
    inode_readings: Mutex<Vec<NamespaceReading>>,
}

/// One reading of the committed namespace: the key/value pairs a single `inode:` scan answered
/// the pass with.
type NamespaceReading = Vec<(Vec<u8>, Bytes)>;

impl RecordingMeta {
    fn new() -> Self {
        Self {
            inner: SimTikvMetadataStore::new(),
            inode_readings: Mutex::new(Vec::new()),
        }
    }

    /// How many readings of the committed namespace the pass made. A pass that reads it once has
    /// no divergence to reconcile, which the coverage property is careful not to punish.
    fn readings(&self) -> usize {
        self.inode_readings.lock().unwrap().len()
    }

    /// The indices of the `inode:` readings that returned `key` holding exactly `value` — "which
    /// of the pass's readings saw this record", asked of the answers the store actually gave.
    fn readings_that_saw(&self, key: &[u8], value: &[u8]) -> Vec<usize> {
        self.inode_readings
            .lock()
            .unwrap()
            .iter()
            .enumerate()
            .filter(|(_, reading)| reading.iter().any(|(k, v)| k == key && v == value))
            .map(|(index, _)| index)
            .collect()
    }
}

#[async_trait]
impl MetadataStore for RecordingMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let answer = self.inner.scan(prefix).await?;
        if prefix == b"inode:" {
            self.inode_readings.lock().unwrap().push(answer.clone());
        }
        Ok(answer)
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

/// What the concurrent writer does while the pass runs — the two ways this pass's own two
/// readings of the committed namespace can disagree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Nemesis {
    /// A second object COMMITS mid-pass. Its fragment is already on disk, so a reading that
    /// missed the record sees bytes nothing references while the other sees them referenced —
    /// and marking on the older reading hands GC a live object's only copy.
    LateCommit,
    /// A committed record STOPS DECODING mid-pass. One reading then has a hole in it the other
    /// never met, and concluding over the whole pass marks fragments while reporting a record
    /// the run could not read.
    Damage,
}

/// What the pass's readings saw of the writer's landing — the interleaving that actually
/// happened, as observed at the store seam rather than assumed from the fixture.
struct Interleaving {
    /// How many times the pass read the committed namespace.
    readings: usize,
    /// Which of those readings returned the writer's record.
    saw: Vec<usize>,
}

/// A committed record whose single un-erasure-coded chunk places fragment 0 on `dserver` — the
/// smallest committed reference the mark gate must honour.
fn flat_committed(chunk: ChunkId, dserver: DServerId) -> InodeRecord {
    InodeRecord {
        size: 5,
        chunk_map: vec![ChunkRef {
            id: chunk,
            scheme: EcScheme::None,
            len: 5,
            placement: vec![dserver],
        }]
        .into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    }
}

fn frag_of(chunk: ChunkId) -> FragmentId {
    FragmentId { chunk, index: 0 }
}

/// Whether `frag` on `dserver` carries an `orphan:` record — the mark itself, and the only
/// durable trace this pass leaves. "Still on disk" proves nothing here (the pass deletes
/// nothing); the record is the pass saying *these bytes may be reclaimed*.
async fn is_marked(meta: &dyn MetadataStore, dserver: DServerId, frag: FragmentId) -> bool {
    meta.get(&metadata::orphan_key(dserver, frag))
        .await
        .unwrap()
        .is_some()
}

/// One run of the post-restore pass against a writer landing `delay_millis` into it, with the
/// full invariant set asserted over what the pass's readings actually saw. Returns the
/// interleaving that occurred, so the coverage property can prove the divergence window is
/// genuinely reached instead of assuming it.
async fn restore_under_a_concurrent_writer(nemesis: Nemesis, delay_millis: u64) -> Interleaving {
    let d = servers();
    let meta = Arc::new(RecordingMeta::new());
    let now = 10_000;

    // Three fragments on disk before the pass starts: one referenced throughout, one whose
    // record is still to come, and one nothing will ever reference.
    let held = frag_of(RESTORE_HELD);
    let late = frag_of(RESTORE_LATE);
    let stray = frag_of(RESTORE_STRAY);
    d[0].put_fragment(held, Bytes::from_static(b"held"), None)
        .await
        .unwrap();
    d[1].put_fragment(late, Bytes::from_static(b"late"), None)
        .await
        .unwrap();
    d[2].put_fragment(stray, Bytes::from_static(b"stray"), None)
        .await
        .unwrap();

    let held_record = flat_committed(RESTORE_HELD, 0);
    let held_key = metadata::inode_key(INODE);
    let held_bytes = metadata::encode(&held_record);
    assert_eq!(
        metadata::create(&*meta, ROOT, "held", INODE, &held_record)
            .await
            .unwrap(),
        CommitOutcome::Committed
    );

    let late_record = flat_committed(RESTORE_LATE, 1);
    let damaged = Bytes::from_static(b"these bytes are not an inode record");
    // The key/value each reading is asked about afterwards: exactly what the writer lands.
    let (watch_key, watch_value) = match nemesis {
        Nemesis::LateCommit => (
            metadata::inode_key(LATE_INODE),
            metadata::encode(&late_record),
        ),
        Nemesis::Damage => (held_key.clone(), damaged.clone()),
    };

    // The genuinely concurrent writer. madsim schedules it against the pass at the await
    // boundaries the store's network hops open, and `delay_millis` is where the campaign's seed
    // (or the coverage walk) puts its landing point — never a seam the double hard-codes.
    let writer = madsim::task::spawn({
        let meta = Arc::clone(&meta);
        let held_key = held_key.clone();
        async move {
            madsim::time::sleep(Duration::from_millis(delay_millis)).await;
            let outcome = match nemesis {
                Nemesis::LateCommit => {
                    metadata::create(&*meta, ROOT, "late", LATE_INODE, &late_record).await
                }
                Nemesis::Damage => meta.commit(WriteBatch::new().put(held_key, damaged)).await,
            };
            assert_eq!(
                outcome.unwrap(),
                CommitOutcome::Committed,
                "the concurrent writer's own commit must land, or this run tests nothing"
            );
        }
    });

    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let ctx = GcContext {
        meta: &*meta,
        fleet: &fleet,
        grace_window_millis: 50,
        expired_pending: ExpiredPendingPolicy::Reclaim,
    };

    // (1) CONTAINED. A record the pass cannot read may not turn the whole answer into an `Err`:
    //     one damaged object would otherwise blank the post-restore picture for every object the
    //     pass COULD read, at the moment an operator needs it most.
    let report = reconcile_after_restore(&ctx, now).await.expect(
        "a record this pass cannot read is contained, never an error that blanks the report",
    );
    writer
        .await
        .expect("the concurrent writer ran to completion");

    let interleaving = Interleaving {
        readings: meta.readings(),
        saw: meta.readings_that_saw(&watch_key, &watch_value),
    };
    let saw = &interleaving.saw;

    // (2) THE MARKS AND THE REPORT REST ON ONE READING. Two readings that disagree are two
    //     conclusions, and the operator is shown one of them: a pass may not both authorize a
    //     deletion and report a record it could not read.
    assert!(
        report.stranded_marked == 0 || report.unresolvable.is_empty(),
        "the pass marked {} fragment(s) AND reported a record it could not read ({:?}) — the \
         mark half acted as though the reading were complete while the report half says it was \
         not (nemesis {nemesis:?}, landing at {delay_millis} ms, readings that saw it: {saw:?})",
        report.stranded_marked,
        report.unresolvable,
    );

    // (3) A FRAGMENT EITHER READING PROTECTS IS NEVER MARKED. The object committed in the
    //     instant between the two readings is absent from the older one and present in the
    //     newer; marking on the older hands GC its only copy after the grace window.
    if nemesis == Nemesis::LateCommit && !saw.is_empty() {
        assert!(
            !is_marked(&*meta, 1, late).await,
            "reading(s) {saw:?} of THIS pass returned the record that places {late:?}, and the \
             pass marked that fragment collectable anyway — GC deletes a live object's only copy \
             once the grace window elapses (landing at {delay_millis} ms)"
        );
    }

    // (4) EITHER READING'S HOLE WITHHOLDS EVERY MARK, AND THE RECORD IS NAMED. An unreadable map
    //     hides WHICH chunks its object owns, so no fragment in the fleet can be shown not to be
    //     one of them — and an operator who cannot learn which record blocked the pass cannot
    //     repair it.
    if nemesis == Nemesis::Damage && !saw.is_empty() {
        assert_eq!(
            report.stranded_marked, 0,
            "reading(s) {saw:?} met a record this pass could not read, and it marked {} \
             fragment(s) anyway (landing at {delay_millis} ms): {report:?}",
            report.stranded_marked
        );
        // `inode:1` is printable ASCII, so the escaped name the report carries is the key itself.
        assert!(
            report
                .unresolvable
                .iter()
                .any(|name| name.as_bytes() == held_key.as_slice()),
            "the blocking record is not NAMED in the report — a stall an operator cannot exit \
             (landing at {delay_millis} ms): {:?}",
            report.unresolvable
        );
        assert!(
            !is_marked(&*meta, 2, stray).await,
            "not even a genuine stray may be marked under a reading with a hole in it: no \
             fragment can be shown not to belong to the object the pass could not read"
        );
    }

    // (5) The readable object's own fragment is never marked, on any schedule: referenced while
    //     both readings could see it, and withheld with everything else once one could not.
    assert!(
        !is_marked(&*meta, 0, held).await,
        "the fragment of the object this pass could read is marked collectable (nemesis \
         {nemesis:?}, landing at {delay_millis} ms, readings that saw the writer: {saw:?})"
    );

    if report.unresolvable.is_empty() {
        // (6) POSITIVE OBSERVABLE — a reading that FINISHED marks the genuine stray. Without it
        //     every "nothing was marked" assertion above would also pass on a pass that did
        //     nothing at all.
        assert!(
            report.stranded_marked >= 1 && is_marked(&*meta, 2, stray).await,
            "the pass read the whole committed namespace and left a fragment nothing references \
             unmarked — the leak this pass exists to close: {report:?}"
        );
    } else {
        // ...and where it did NOT finish, the withholding is a WITHHOLDING rather than a pass
        // that died: repair the record the run named, re-run over the same store and the same
        // fleet, and what was held back is marked at once.
        assert_eq!(
            meta.commit(WriteBatch::new().put(held_key.clone(), held_bytes))
                .await
                .unwrap(),
            CommitOutcome::Committed
        );
        let repaired = reconcile_after_restore(&ctx, now)
            .await
            .expect("the repaired store reads whole");
        assert!(
            repaired.unresolvable.is_empty()
                && repaired.stranded_marked >= 1
                && is_marked(&*meta, 2, stray).await,
            "after the named record is repaired the pass must mark what it withheld — otherwise \
             the containment above is indistinguishable from a pass that never ran: {repaired:?}"
        );
        assert!(
            !is_marked(&*meta, 0, held).await,
            "the repaired object's fragment is referenced again, and is never marked"
        );
    }

    interleaving
}

/// The campaign leg: the seed picks which disagreement the writer causes and where it lands,
/// so 50 seeds sweep the schedule space around the pass's two readings.
async fn prop_restore_two_readings_never_license_a_mark(rng: &mut ChaCha8Rng) {
    let nemesis = if rng.next_u32().is_multiple_of(2) {
        Nemesis::LateCommit
    } else {
        Nemesis::Damage
    };
    let delay = u64::from(rng.next_u32() % (RESTORE_NEMESIS_SPAN + 1));
    restore_under_a_concurrent_writer(nemesis, delay).await;
}

/// **The window this property exists for is genuinely REACHED.** A concurrency test that never
/// reaches the interleaving it is written about is a green light with nothing behind it, so this
/// leg walks the writer's whole landing span in one run — asserting the full invariant set at
/// every point — and then asserts the two schedules the campaign depends on actually occurred:
/// one where only the LATER reading saw the writer's record (the divergence the mark gate has to
/// reconcile) and one where NEITHER did (the writer landing past the pass, the runbook's
/// writers-stopped case).
///
/// The divergence clause is conditioned on the pass having made more than one reading: a pass
/// that reads the committed namespace ONCE has no divergence to cover, and this leg then reduces
/// to the invariant walk rather than punishing the better implementation.
async fn prop_restore_two_readings_cover_the_divergence_window() {
    let mut readings = 0;
    let mut divergent: Vec<(Nemesis, u64)> = Vec::new();
    let mut past_the_pass: Vec<(Nemesis, u64)> = Vec::new();
    for nemesis in [Nemesis::LateCommit, Nemesis::Damage] {
        for delay in 0..=u64::from(RESTORE_NEMESIS_SPAN) {
            let seen = restore_under_a_concurrent_writer(nemesis, delay).await;
            assert!(
                !seen.saw.contains(&0) || seen.saw.len() == seen.readings,
                "a record is committed (or damaged) once and stays that way, so a reading before \
                 the writer landed cannot have seen what a later one missed: {:?}",
                seen.saw
            );
            readings = readings.max(seen.readings);
            match seen.saw.len() {
                0 => past_the_pass.push((nemesis, delay)),
                n if n < seen.readings => divergent.push((nemesis, delay)),
                _ => {}
            }
        }
    }
    assert!(
        !past_the_pass.is_empty(),
        "no landing point in 0..={RESTORE_NEMESIS_SPAN} ms left the pass's readings untouched — \
         the span no longer covers the whole pass, so the sweep is stuck in one regime"
    );
    if readings > 1 {
        assert!(
            !divergent.is_empty(),
            "no landing point in 0..={RESTORE_NEMESIS_SPAN} ms fell BETWEEN the pass's {readings} \
             readings, so the divergence this property exists for was never exercised: the \
             invariants above passed without the schedule that can break them"
        );
    }
}

// ---- the seed sweep: each property over the run seed (madsim sweeps MADSIM_TEST_NUM) ----

/// A fresh ChaCha RNG seeded from the madsim run seed, so the whole campaign — *which*
/// server is killed/rotted included — reproduces from the run seed (ADR-0009), exactly
/// as the network DST campaign does (`tests/network.rs`).
fn rand_seed() -> ChaCha8Rng {
    ChaCha8Rng::seed_from_u64(madsim::runtime::Handle::current().seed())
}

dst_campaign_test! {
    async fn reconstruct_to_full_redundancy_q1() {
        prop_reconstruct_to_full_redundancy(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn commit_point_atomic_repair_under_crash() {
        prop_commit_point_atomic_under_crash(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn scrub_detects_bit_rot_then_reconstructs_q2() {
        prop_scrub_detects_bit_rot_then_reconstructs(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn gc_reclaims_only_true_orphans_q3() {
        prop_gc_reclaims_only_true_orphans(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn fenced_stale_leader_lands_nothing() {
        prop_fenced_stale_leader_lands_nothing(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn crash_mid_fragment_write_commits_nothing() {
        prop_crash_mid_write_commits_nothing(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn reader_flips_atomically_across_commit() {
        prop_reader_flips_atomically_across_commit(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn segmented_resolve_never_tears() {
        prop_segmented_resolve_never_tears(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn gc_over_a_segmented_map_never_reclaims_it_and_never_over_certifies() {
        prop_gc_over_a_segmented_map_never_reclaims_it_and_never_over_certifies(&mut rand_seed())
            .await;
    }
}

dst_campaign_test! {
    async fn durability_emission_rises_then_returns_to_zero() {
        prop_durability_emission_rises_then_returns_to_zero(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn restore_two_readings_never_license_a_mark() {
        prop_restore_two_readings_never_license_a_mark(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn restore_two_readings_cover_the_divergence_window() {
        prop_restore_two_readings_cover_the_divergence_window().await;
    }
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

dst_campaign_test! {
    async fn committed_regression_seeds_stay_green() {
        for &seed in REGRESSION_SEEDS {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            prop_reconstruct_to_full_redundancy(&mut rng).await;
            prop_commit_point_atomic_under_crash(&mut rng).await;
            prop_scrub_detects_bit_rot_then_reconstructs(&mut rng).await;
            prop_gc_reclaims_only_true_orphans(&mut rng).await;
            prop_fenced_stale_leader_lands_nothing(&mut rng).await;
            prop_durability_emission_rises_then_returns_to_zero(&mut rng).await;
            prop_crash_mid_write_commits_nothing(&mut rng).await;
            prop_reader_flips_atomically_across_commit(&mut rng).await;
            prop_gc_over_a_segmented_map_never_reclaims_it_and_never_over_certifies(&mut rng).await;
            prop_restore_two_readings_never_license_a_mark(&mut rng).await;
        }
    }
}

// ---- the barrier's own regression test (ADR-0035 §5) ----

/// A metric callsite **only this test** touches. The production callsites
/// (`reconstruction_under_replicated`, …) are process-global and a sibling property may
/// have cached their interest already, so they cannot test *first* touch deterministically;
/// a private probe lets this test own the first touch and assert the barrier's effect on it.
fn emit_poison_probe() {
    tracing::info!(monotonic_counter.__dst_barrier_poison_probe = 1_u64);
}

dst_campaign_test! {
    /// Pin the two things the barrier's containment rests on (ADR-0035 §5), each with teeth:
    ///
    /// 1. **The barrier was actually installed.** The `dst_campaign_test!` preamble must have
    ///    set a global `tracing` default; a no-op or forgotten barrier leaves `NoSubscriber`
    ///    and reds this assertion. (This is the half a removed barrier breaks.)
    /// 2. **`registry()` keeps callsite interest non-`never`.** A scoped capture over a bare
    ///    `tracing_subscriber::registry()` must observe an info metric callsite. The barrier
    ///    relies on `registry()` reporting interest (so a callsite never latches `never`);
    ///    nothing else pins that `tracing-subscriber` behaviour, so a dependency upgrade that
    ///    changed `Registry`'s callsite interest would empty the capture and red this instead
    ///    of silently re-breaking seed-determinism. The non-capturing first touch mirrors the
    ///    poison race #242 describes.
    ///
    /// What this CANNOT do: deterministically reproduce the *cross-thread* poison itself.
    /// `Dispatch::new` rebuilds the interest cache (`tracing-core` `dispatcher.rs`), so any
    /// in-thread scoped capture re-evaluates the callsite; the genuine flake is a timing race
    /// between parallel `cargo test` threads over the process-global cache, which is exactly
    /// what the barrier (assertion 1) removes — it is not reproducible in one deterministic
    /// test. Pinning the two invariants above is the achievable, teeth-bearing guard.
    async fn barrier_installed_and_registry_keeps_callsites_capturable() {
        // (1) The barrier installed a global default — teeth against a forgotten/no-op barrier.
        assert!(
            tracing::dispatcher::has_been_set(),
            "the dst_campaign_test! barrier must have installed a global tracing default (ADR-0035 §2)"
        );

        // (2) A non-capturing first touch, then a scoped capture that must still observe it.
        emit_poison_probe();
        let cap = MetricCapture::default();
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(cap.clone()),
            emit_poison_probe,
        );
        assert_eq!(
            cap.values("monotonic_counter.__dst_barrier_poison_probe"),
            vec![1],
            "a scoped capture over registry() must observe the metric — registry() interest must stay non-`never`"
        );
    }
}
