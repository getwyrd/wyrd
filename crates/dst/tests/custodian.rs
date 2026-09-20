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

use std::collections::{BTreeMap, HashMap, HashSet};
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
use wyrd_core::multipart::{
    decode_part_record, decode_session_record, mpu_key, part_key, part_range, sidx_key, sidx_range,
    OwnedEntry, PartNumber, StagedPlacement, UploadId,
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
    chunk_hex, BoxError, ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health,
    MetadataStore, PlacementChunkStore, Result, WriteBatch,
};

// The DST tier's **second** `MetadataStore` implementation — the deterministic
// simulated-TiKV model whose every read and commit spans real madsim await boundaries
// (`network_hop`). Property 11 below needs those boundaries: they are what lets a genuinely
// concurrent writer land *between* the post-restore pass's two readings of the committed
// namespace, which an in-memory store that never yields cannot produce.
#[path = "support/mod.rs"]
mod support;
use support::{sim_commit_unknown_result, SimTikvMetadataStore, SIM_COMMIT_UNKNOWN_RESULT};

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
            owner: None,
            staged: None,
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
/// two hops apart, with the `pending:` scan between them (the `orphan:` ledger is read after both,
/// and only for the fragments the pass may mark — #661), so this span spans "before the second
/// reading", the tie with it, and "after the whole pass" — and the coverage property below
/// **proves** the middle one is reached rather than assuming it. A writer drawn to land at 0 ms
/// starts at once rather than sleeping: the simulator's `sleep` never finishes inside the tick it
/// was called in, so a zero sleep would start the writer a hop late — onto the tie with the
/// second reading, never ahead of it.
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
            // Zero means now: see `RESTORE_NEMESIS_SPAN`.
            if delay_millis > 0 {
                madsim::time::sleep(Duration::from_millis(delay_millis)).await;
            }
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

// ---- property 12: GC's paged walk of the `orphan:` ledger under a concurrent unlink (#661) ----
//
// GC reads the orphan ledger in cursor-keyed `scan_page` pages, never with one `scan` (proposal
// 0016, `0016:1392-1408`), so a pass's view of the ledger is assembled over several reads with
// real await boundaries between them — and a writer can land in between. The per-pass legs in
// `crates/custodian/tests/gc_ledger_walk.rs` pin the walk's decisions over doubles that never
// yield; here the ledger is paged over the simulated-TiKV model with a page cap the seed picks,
// and a genuinely concurrent task unlinks an object at an instant the seed picks, writing fresh
// marks (inside grace) behind the walk's current page and ahead of it.
//
// The ledger stays below the production window (`gc::ORPHAN_WINDOW`): that is a constant, and a
// test could lower it only through a context field or a global, both ruled out. So every pass
// reads the whole ledger in several pages, and what the simulator sweeps is where between those
// pages the writer lands — the no-skip clause (`scan_page` clause 4) doing its work under a real
// interleaving rather than a scripted one.
//
// The property is the retention rule itself: no fragment that is referenced, or whose own mark is
// inside its grace window, is ever deleted — and every fragment actionable when the run starts is
// reclaimed by its end. The unlinked object's chunks also carry a stale, EXPIRED `pending:` lease
// (a writer that died between its commit and its release), so a walk that let the lease outrank a
// mark it had not read would delete those fragments inside their grace window.

/// The object referenced for the whole run: chunk 7, one fragment on server 1. Its fragment also
/// carries a stale mark long past grace, so the reference is the only thing keeping it.
const WALK_LIVE: ChunkId = 7;
/// The unlinked object's two chunks: chunk 1 on server 0 and chunk 9 on server 3. Their marks —
/// written by the unlink — are the first (`orphan:0:1:0`) and last (`orphan:3:9:0`) keys of the
/// ledger, so wherever the walk is mid-way when they land, one is behind it and one ahead.
const WALK_VICTIM: [(ChunkId, DServerId); 2] = [(1, 0), (9, 3)];
/// Where the actionable ledger's chunk ids start: `5000..`, four digits each, so they sort after
/// `orphan:0:1:0` and `orphan:2:4…:0` and before `orphan:1:7:0` and `orphan:3:9:0`.
const WALK_ACTIONABLE_BASE: ChunkId = 5000;
/// Two unreferenced fragments on server 2 whose marks are inside grace for the whole run.
const WALK_WITHIN_GRACE: [ChunkId; 2] = [4000, 4001];
const WALK_VICTIM_INODE: InodeId = 3;
const WALK_LIVE_INODE: InodeId = 4;
const WALK_GRACE: u64 = 1_000;
/// Every pass runs at this instant; the unlink stamps it too, so its marks are inside grace for
/// the whole run while everything stamped at zero is long past it.
const WALK_NOW: u64 = 10_000;
/// Passes per run: the first reclaims the actionable ledger, the rest walk what the unlink wrote.
const WALK_PASSES: usize = 3;
/// How far past the run's start the unlink's landing point is drawn from, in simulated
/// milliseconds. One `network_hop` is 1 ms; a pass is its two scans, the cursor read, one hop per
/// page and a cleanup commit, and the unlink itself spans four hops (two reads and a commit) — so
/// this spans "before the first page", every gap between pages of the first walk, the gaps between
/// passes, and "after the whole run". The coverage property below proves the mid-walk landing is
/// reached rather than assuming it.
const WALK_NEMESIS_SPAN: u32 = 32;

/// One observation at the store seam, in the order the simulation produced them. Keys are kept as
/// text: every key in this run is ASCII, so text order is the store's byte order, and a failure
/// message stays readable.
#[derive(Clone, Debug)]
enum WalkEvent {
    /// The run began a GC pass.
    Pass,
    /// A `scan_page` of the `orphan:` ledger answered, ending on `last`; `more` is whether it
    /// handed back a cursor (the walk was not done).
    Page { last: Option<String>, more: bool },
    /// A commit wrote these `orphan:` marks — the unlink, the only writer of marks in the run.
    Marked(Vec<String>),
}

/// A recording tap over the simulated-TiKV store: every call is forwarded unchanged, network hops
/// included, and the orphan-ledger pages and mark-writing commits are logged in the order they
/// complete. Instance state only (ADR-0035).
struct WalkMeta {
    inner: SimTikvMetadataStore,
    events: Mutex<Vec<WalkEvent>>,
}

impl WalkMeta {
    fn log(&self, event: WalkEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[async_trait]
impl MetadataStore for WalkMeta {
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
        let (items, next) = self.inner.scan_page(prefix, after, limit).await?;
        if prefix == metadata::ORPHAN_PREFIX {
            self.log(WalkEvent::Page {
                last: items
                    .last()
                    .map(|(key, _)| String::from_utf8_lossy(key).into_owned()),
                more: next.is_some(),
            });
        }
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let marks: Vec<String> = batch
            .puts
            .iter()
            .filter(|(key, _)| key.starts_with(metadata::ORPHAN_PREFIX))
            .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
            .collect();
        let outcome = self.inner.commit(batch).await?;
        if outcome == CommitOutcome::Committed && !marks.is_empty() {
            self.log(WalkEvent::Marked(marks));
        }
        Ok(outcome)
    }
}

/// Where the unlink's marks landed relative to the walk — observed at the store seam.
struct WalkLanding {
    /// Between two pages of one pass's walk: after a page that handed back a cursor, before the
    /// next page of the same pass.
    mid_walk: bool,
    /// Mid-walk, with marks both behind the walk's cursor and ahead of it.
    behind_and_ahead: bool,
}

fn walk_landing(events: &[WalkEvent]) -> WalkLanding {
    let mut cursor: Option<&String> = None;
    for event in events {
        match event {
            WalkEvent::Pass => cursor = None,
            WalkEvent::Page { last, more } => cursor = last.as_ref().filter(|_| *more),
            WalkEvent::Marked(keys) => {
                let Some(at) = cursor else {
                    return WalkLanding {
                        mid_walk: false,
                        behind_and_ahead: false,
                    };
                };
                return WalkLanding {
                    mid_walk: true,
                    behind_and_ahead: keys.iter().any(|key| key < at)
                        && keys.iter().any(|key| key > at),
                };
            }
        }
    }
    WalkLanding {
        mid_walk: false,
        behind_and_ahead: false,
    }
}

/// A committed flat record placing each `(chunk, dserver)` as an un-erasure-coded chunk.
fn flat_committed_on(chunks: &[(ChunkId, DServerId)]) -> InodeRecord {
    InodeRecord {
        size: 5 * chunks.len() as u64,
        chunk_map: chunks
            .iter()
            .map(|&(id, dserver)| ChunkRef {
                id,
                scheme: EcScheme::None,
                len: 5,
                placement: vec![dserver],
            })
            .collect::<Vec<_>>()
            .into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    }
}

/// One run: `WALK_PASSES` GC passes, each with a fresh `GcContext`, over a ledger of `actionable`
/// reclaimable marks (plus the live object's stale mark and two within-grace ones) paged `page_cap`
/// at a time, while the unlink lands `delay_millis` into the run. Asserts the retention property
/// and returns where the unlink landed.
async fn gc_walk_under_a_concurrent_unlink(
    page_cap: usize,
    actionable: usize,
    delay_millis: u64,
) -> WalkLanding {
    let d = servers();
    let meta = Arc::new(WalkMeta {
        inner: SimTikvMetadataStore::new().with_scan_cap(page_cap),
        events: Mutex::new(Vec::new()),
    });

    // The live object, referenced for the whole run — and a stale mark on its fragment, long past
    // grace, so only the reference keeps it.
    let live = frag_of(WALK_LIVE);
    d[1].put_fragment(live, Bytes::from_static(b"live"), None)
        .await
        .unwrap();
    let live_record = flat_committed_on(&[(WALK_LIVE, 1)]);
    let created = metadata::create(&*meta, ROOT, "walk-live", WALK_LIVE_INODE, &live_record);
    assert_eq!(created.await.unwrap(), CommitOutcome::Committed);
    mark_orphaned(&*meta, 1, live, 0).await.unwrap();

    // The object the concurrent task unlinks: referenced until it lands. Its chunks keep a stale,
    // expired lease, so after the unlink a mark is all that stands between them and a reclaim.
    for &(chunk, dserver) in &WALK_VICTIM {
        d[dserver as usize]
            .put_fragment(frag_of(chunk), Bytes::from_static(b"victim"), None)
            .await
            .unwrap();
        let lease = PendingEntry {
            lease_expiry_millis: 1,
            owner: None,
            staged: None,
        };
        metadata::put_pending(&*meta, chunk, &lease).await.unwrap();
    }
    let victim_record = flat_committed_on(&WALK_VICTIM);
    let created = metadata::create(
        &*meta,
        ROOT,
        "walk-victim",
        WALK_VICTIM_INODE,
        &victim_record,
    );
    assert_eq!(created.await.unwrap(), CommitOutcome::Committed);

    // Unreferenced fragments whose marks are inside grace for the whole run.
    for &chunk in &WALK_WITHIN_GRACE {
        d[2].put_fragment(frag_of(chunk), Bytes::from_static(b"held"), None)
            .await
            .unwrap();
        mark_orphaned(&*meta, 2, frag_of(chunk), WALK_NOW)
            .await
            .unwrap();
    }

    // The actionable ledger: unreferenced fragments whose marks are long past grace.
    let reclaimable: Vec<(DServerId, FragmentId)> = (0..actionable)
        .map(|i| {
            let chunk = WALK_ACTIONABLE_BASE + i as ChunkId;
            ((i % 4) as DServerId, frag_of(chunk))
        })
        .collect();
    for &(dserver, frag) in &reclaimable {
        d[dserver as usize]
            .put_fragment(frag, Bytes::from_static(b"garbage"), None)
            .await
            .unwrap();
        mark_orphaned(&*meta, dserver, frag, 0).await.unwrap();
    }

    // The run starts here: what the fixture wrote above is not the concurrent writer.
    meta.events.lock().unwrap().clear();

    // The genuinely concurrent unlink, landing where the seed (or the coverage walk) puts it.
    let writer = madsim::task::spawn({
        let meta = Arc::clone(&meta);
        async move {
            // Zero means now: the simulator's `sleep` never finishes inside the tick it was called
            // in (see `RESTORE_NEMESIS_SPAN`).
            if delay_millis > 0 {
                madsim::time::sleep(Duration::from_millis(delay_millis)).await;
            }
            let unlinked = metadata::unlink(&*meta, ROOT, "walk-victim", WALK_NOW)
                .await
                .unwrap()
                .expect("the victim's name is bound until this unlink");
            assert_eq!(
                unlinked.outcome,
                CommitOutcome::Committed,
                "the concurrent unlink must land, or this run tests nothing"
            );
        }
    });

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-gc-walk").await;
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    for pass in 1..=WALK_PASSES {
        meta.log(WalkEvent::Pass);
        // A FRESH context every pass, as the deployed loop builds one
        // (`crates/server/src/custodian.rs:600-608`).
        let ctx = GcContext {
            meta: &*meta,
            fleet: &fleet,
            grace_window_millis: WALK_GRACE,
            expired_pending: ExpiredPendingPolicy::Reclaim,
        };
        let outcome =
            reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, WALK_NOW).await;
        assert!(
            outcome.is_ok(),
            "GC pass {pass} failed (page cap {page_cap}, unlink at {delay_millis} ms): {:?}",
            outcome.err()
        );
    }
    writer
        .await
        .expect("the concurrent unlink ran to completion");
    let events = meta.events.lock().unwrap().clone();

    // (1) NEVER DELETE A REFERENCED FRAGMENT, OR ONE WHOSE OWN MARK IS INSIDE GRACE.
    assert!(
        d[1].get_fragment(live).await.unwrap().is_some(),
        "the referenced object's fragment was deleted (page cap {page_cap}, unlink at \
         {delay_millis} ms): {events:?}"
    );
    for &(chunk, dserver) in &WALK_VICTIM {
        assert!(
            d[dserver as usize]
                .get_fragment(frag_of(chunk))
                .await
                .unwrap()
                .is_some(),
            "chunk {chunk} on server {dserver} was deleted — referenced until the unlink, and \
             marked inside grace after it (page cap {page_cap}, unlink at {delay_millis} ms): \
             {events:?}"
        );
        assert_eq!(
            meta.get(&metadata::orphan_key(dserver, frag_of(chunk)))
                .await
                .unwrap(),
            Some(Bytes::from(WALK_NOW.to_string())),
            "the unlink's mark for chunk {chunk} was consumed inside its grace window"
        );
    }
    for &chunk in &WALK_WITHIN_GRACE {
        assert!(
            d[2].get_fragment(frag_of(chunk)).await.unwrap().is_some(),
            "a fragment whose mark is inside grace was deleted (page cap {page_cap}, unlink at \
             {delay_millis} ms)"
        );
    }

    // (2) EVERY FRAGMENT ACTIONABLE AT THE START IS RECLAIMED BY THE END, its mark consumed.
    for &(dserver, frag) in &reclaimable {
        assert!(
            d[dserver as usize]
                .get_fragment(frag)
                .await
                .unwrap()
                .is_none()
                && !is_marked(&*meta, dserver, frag).await,
            "{frag:?} on server {dserver} was actionable from the start and survived the run \
             (page cap {page_cap}, unlink at {delay_millis} ms): {events:?}"
        );
    }

    walk_landing(&events)
}

/// The campaign leg: the seed picks the page cap, how long the ledger is, and where the unlink
/// lands, so 50 seeds sweep the schedule space around the walk's pages.
async fn prop_gc_orphan_walk_under_a_concurrent_unlink(rng: &mut ChaCha8Rng) {
    let page_cap = 3 + (rng.next_u32() % 4) as usize;
    let actionable =
        page_cap * (3 + (rng.next_u32() % 3) as usize) + (rng.next_u32() as usize % page_cap);
    let delay = u64::from(rng.next_u32() % (WALK_NEMESIS_SPAN + 1));
    gc_walk_under_a_concurrent_unlink(page_cap, actionable, delay).await;
}

/// **The mid-walk landing is genuinely REACHED.** Walks the unlink's landing point across the
/// whole span in one run, asserting the full property at every point, then asserts that at least
/// one landing fell between two pages of a pass's walk with marks both behind the walk's cursor and
/// ahead of it — the interleaving this property is about. Without it, a span that drifted past the
/// walk would leave the campaign green with nothing behind it.
async fn prop_gc_orphan_walk_reaches_the_mid_walk_landing() {
    let mut mid_walk = Vec::new();
    for delay in 0..=u64::from(WALK_NEMESIS_SPAN) {
        let landing = gc_walk_under_a_concurrent_unlink(3, 12, delay).await;
        if landing.mid_walk && landing.behind_and_ahead {
            mid_walk.push(delay);
        }
    }
    assert!(
        !mid_walk.is_empty(),
        "no landing point in 0..={WALK_NEMESIS_SPAN} ms fell between two pages of a walk with \
         marks both behind and ahead of it — the interleaving this property exists for was never \
         exercised"
    );
}

// ---- property 13: GC's staged reads across a part commit, a publication flip and its
//      retirement drain (issue #803) ----
//
// GC protects a multipart upload's staged bytes as a class of their own, read through each
// session's bounded ranges in a fixed order — its owned staging entries (`sidx:`), then its
// committed parts (`part:`), then the `inode:` scan — because three moves hand one chunk's
// protection from one of those classes to the next, each in a single batch: a part commit
// (`sidx:` → `part:`), the publication's root flip (`part:` → a committed inode, the part record
// kept), and the retirement drain that later deletes the part record (`0016:782-800`, X67
// `0016:2596`). A reading that took a destination before its source could see the chunk in
// neither class.
//
// The per-pass legs in `crates/custodian/tests/staged_protection.rs` pin that order with a double
// that lands each move at a scripted instant. Here a genuinely concurrent task makes all three
// moves over the simulated-TiKV model, each at an instant the seed picks, while GC passes run back
// to back; the chunk's fragment carries an `orphan:` mark past grace for the whole run, so its
// protection is all that keeps it. The property: it is never reclaimed. The coverage leg proves the
// sweep reaches, for each move, a landing between the two reads it hands protection across and a
// landing outside them.

/// The upload whose part the concurrent task commits and publishes: 32 lowercase-hex characters.
const HANDOFF_UPLOAD: &str = "80380380380380380380380380380380";
/// The chunk that moves: one un-erasure-coded fragment on server 1, marked past grace all run.
const HANDOFF_CHUNK: ChunkId = 0x8030;
/// An unprotected fragment on server 2, marked past grace: the first pass reclaims it — the
/// positive observable that GC is reclaiming at all in this run.
const HANDOFF_STRAY: ChunkId = 0x8031;
/// The inode the publication flip writes.
const HANDOFF_INODE: InodeId = 6;
const HANDOFF_OBJECT: &str = "handoff";
const HANDOFF_GRACE: u64 = 50;
const HANDOFF_NOW: u64 = 10_000;
/// How long the tap takes to carry a read's answer back, in simulated milliseconds, after the
/// model's own 1 ms request hop. Three, so that the gap between two consecutive reads' answers
/// (4 ms) holds two commits landing 2 ms apart strictly inside it — a flip and the drain right
/// behind it, the one schedule a reading that took committed inodes before part records sees in
/// neither class — rather than only on ties the scheduler breaks.
const HANDOFF_REPLY_MILLIS: u64 = 3;
/// How far apart the concurrent task's moves are drawn, in simulated milliseconds: the part commit
/// lands up to this far into the run, the flip up to this far after the fence, and the drain up to
/// this far after the flip. A read here takes 4 ms and a GC pass is six reads (plus a cleanup
/// commit when it reclaims), so this spans about one pass per gap — the coverage leg below proves
/// the landings between the reads are reached rather than assuming it.
const HANDOFF_SPAN: u32 = 24;
/// The most GC passes one run makes before it gives up on the concurrent task finishing.
const HANDOFF_MAX_PASSES: usize = 64;

/// The three moves, each one batch handing a chunk's protection from one class to the next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Handoff {
    /// Deletes the owned staging entry and writes the part record naming the chunk.
    PartCommit,
    /// Writes the committed inode naming the chunk and moves the session to `Completed`, keeping
    /// the part record.
    Flip,
    /// Deletes the part record (the retirement drain; its `retire:records:` key is left out — no
    /// pass in this slice reads `retire:`).
    Drain,
}

impl Handoff {
    const ALL: [Handoff; 3] = [Handoff::PartCommit, Handoff::Flip, Handoff::Drain];

    /// This move's position in [`Handoff::ALL`].
    fn index(self) -> usize {
        match self {
            Handoff::PartCommit => 0,
            Handoff::Flip => 1,
            Handoff::Drain => 2,
        }
    }

    /// The two reads this move hands protection across: its source, then its destination.
    fn reads(self, upload: &UploadId) -> (Vec<u8>, Vec<u8>) {
        match self {
            Handoff::PartCommit => (sidx_range(upload), part_range(upload)),
            Handoff::Flip | Handoff::Drain => (part_range(upload), b"inode:".to_vec()),
        }
    }
}

/// One observation at the store seam, in the order the simulation produced them. Keys are kept as
/// text — every key in this run is ASCII — so a failure message stays readable.
#[derive(Clone, Debug)]
enum HandoffEvent {
    /// A GC pass began.
    Pass,
    /// A read's answer was taken, of this key or prefix.
    Read(String),
    /// A move's batch applied.
    Landed(Handoff),
}

/// A recording tap over the simulated-TiKV store. Every call is forwarded, network hops included,
/// and each read's answer is logged the instant it is taken — then carried back over a reply hop of
/// its own ([`HANDOFF_REPLY_MILLIS`]), so two consecutive reads are separated by instants a
/// concurrent commit can land on strictly between them, rather than only by a tie the scheduler
/// breaks. Instance state only (ADR-0035).
struct HandoffMeta {
    inner: SimTikvMetadataStore,
    events: Mutex<Vec<HandoffEvent>>,
}

impl HandoffMeta {
    fn log(&self, event: HandoffEvent) {
        self.events.lock().unwrap().push(event);
    }

    /// Log that a read of `subject` was answered, then carry the answer back.
    async fn answered(&self, subject: &[u8]) {
        self.log(HandoffEvent::Read(
            String::from_utf8_lossy(subject).into_owned(),
        ));
        madsim::time::sleep(Duration::from_millis(HANDOFF_REPLY_MILLIS)).await;
    }
}

#[async_trait]
impl MetadataStore for HandoffMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let value = self.inner.get(key).await?;
        self.answered(key).await;
        Ok(value)
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let answer = self.inner.scan(prefix).await?;
        self.answered(prefix).await;
        Ok(answer)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<wyrd_traits::ScanPage> {
        let page = self.inner.scan_page(prefix, after, limit).await?;
        self.answered(prefix).await;
        Ok(page)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        self.inner.commit(batch).await
    }
}

/// A session record targeting [`HANDOFF_OBJECT`] in `state` (its JSON), as the base decoder spells
/// it and checked against it.
fn handoff_session(state: &str) -> Bytes {
    let bytes = format!(
        "{{\"parent\":{ROOT},\"object\":\"{HANDOFF_OBJECT}\",\"created_at_millis\":100,\
         \"clock_source\":\"wall\",\"epoch\":1,\"attempts\":1,\"state\":{state}}}"
    )
    .into_bytes();
    decode_session_record(&bytes).expect("the seeded session record decodes");
    Bytes::from(bytes)
}

fn handoff_chunk_ref() -> ChunkRef {
    ChunkRef {
        id: HANDOFF_CHUNK,
        scheme: EcScheme::None,
        len: 5,
        placement: vec![1],
    }
}

/// Where each move landed relative to the two reads it hands protection across — observed at the
/// store seam.
struct HandoffLandings {
    /// Whether each move landed at all.
    landed: [bool; 3],
    /// For each move, the GC pass it landed inside of — after that pass's source read and before
    /// its destination read — by the log position of the pass's start; `None` when it landed in no
    /// such window.
    between: [Option<usize>; 3],
}

impl HandoffLandings {
    fn between(&self, handoff: Handoff) -> bool {
        self.between[handoff.index()].is_some()
    }

    fn outside(&self, handoff: Handoff) -> bool {
        self.landed[handoff.index()] && self.between[handoff.index()].is_none()
    }

    /// Whether the flip AND the drain both landed between the SAME pass's part read and its
    /// `inode:` scan — the schedule in which a reading that took committed inodes first sees the
    /// chunk in neither class.
    fn published_within_one_window(&self) -> bool {
        let flip = self.between[Handoff::Flip.index()];
        flip.is_some() && flip == self.between[Handoff::Drain.index()]
    }
}

fn handoff_landings(events: &[HandoffEvent], upload: &UploadId) -> HandoffLandings {
    let mut landings = HandoffLandings {
        landed: [false; 3],
        between: [None; 3],
    };
    for handoff in Handoff::ALL {
        let Some(at) = events
            .iter()
            .position(|event| matches!(event, HandoffEvent::Landed(h) if *h == handoff))
        else {
            continue;
        };
        landings.landed[handoff.index()] = true;
        let (source, destination) = handoff.reads(upload);
        let Some(start) = events[..at]
            .iter()
            .rposition(|event| matches!(event, HandoffEvent::Pass))
        else {
            continue;
        };
        let end = events[at..]
            .iter()
            .position(|event| matches!(event, HandoffEvent::Pass))
            .map_or(events.len(), |offset| at + offset);
        let read_in = |subject: &[u8], window: &[HandoffEvent]| {
            window
                .iter()
                .any(|event| matches!(event, HandoffEvent::Read(s) if s.as_bytes() == subject))
        };
        if read_in(&source, &events[start..at]) && read_in(&destination, &events[at..end]) {
            landings.between[handoff.index()] = Some(start);
        }
    }
    landings
}

/// One run: GC passes back to back while the concurrent task commits the part `gaps[0]` ms into
/// the run, fences the session to `Completing` and flips `gaps[1]` ms after the commit, and drains
/// `gaps[2]` ms after the flip, as a batch of its own. Asserts after every pass that the chunk's
/// fragment is still on disk, and returns where each move landed.
async fn staged_handoffs_under_gc(gaps: [u64; 3]) -> HandoffLandings {
    let d = servers();
    let meta = Arc::new(HandoffMeta {
        inner: SimTikvMetadataStore::new(),
        events: Mutex::new(Vec::new()),
    });
    let upload = UploadId::new(HANDOFF_UPLOAD).expect("32 lowercase-hex characters");
    let part = PartNumber::new(1).expect("a part number in range");
    let fragment = frag_of(HANDOFF_CHUNK);
    let stray = frag_of(HANDOFF_STRAY);

    // An `Open` session with one owned staging entry planning the chunk on server 1.
    let open = handoff_session("{\"kind\":\"Open\"}");
    let owned_key = sidx_key(&upload, part, HANDOFF_CHUNK);
    let staged = StagedPlacement::new(EcScheme::None, vec![1]).expect("a supported scheme");
    let owned = OwnedEntry::new(upload.clone(), HANDOFF_NOW * 1_000, staged);
    let seeded = meta
        .commit(
            WriteBatch::new()
                .put(mpu_key(&upload), open.clone())
                .put(owned_key.clone(), metadata::encode(&owned.to_pending())),
        )
        .await
        .unwrap();
    assert_eq!(seeded, CommitOutcome::Committed);
    d[1].put_fragment(fragment, Bytes::from_static(b"staged"), None)
        .await
        .unwrap();
    d[2].put_fragment(stray, Bytes::from_static(b"stray"), None)
        .await
        .unwrap();
    mark_orphaned(&*meta, 1, fragment, 0).await.unwrap();
    mark_orphaned(&*meta, 2, stray, 0).await.unwrap();

    // The part record the commit writes, and the records the publication writes.
    let chunk = String::from_utf8(metadata::encode(&handoff_chunk_ref()).to_vec()).unwrap();
    let part_record = Bytes::from(
        format!(
            "{{\"chunks\":[{chunk}],\"len\":5,\"digest\":\"{}\",\"committed_at_millis\":1,\
             \"session_epoch\":1}}",
            "ef".repeat(32)
        )
        .into_bytes(),
    );
    decode_part_record(&part_record).expect("the part record decodes");
    let completing = handoff_session(&format!(
        "{{\"kind\":\"Completing\",\"fenced_at_millis\":1,\"segments_written\":0,\
         \"publish_target\":{{\"parent\":{ROOT},\"name\":\"{HANDOFF_OBJECT}\",\"epoch\":1}}}}"
    ));
    let completed = handoff_session(&format!(
        "{{\"kind\":\"Completed\",\"completion\":{{\"inode\":{HANDOFF_INODE},\"version\":1,\
         \"etag\":\"{}-1\",\"completed_at_millis\":2,\"complete_fingerprint\":\"{}\"}}}}",
        "ab".repeat(32),
        "cd".repeat(32)
    ));
    let published = InodeRecord {
        size: 5,
        chunk_map: vec![handoff_chunk_ref()].into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };

    // The run starts here: what the fixture wrote above is not the concurrent task.
    meta.events.lock().unwrap().clear();

    let done = Arc::new(AtomicBool::new(false));
    let writer = madsim::task::spawn({
        let meta = Arc::clone(&meta);
        let done = Arc::clone(&done);
        let upload = upload.clone();
        async move {
            // Zero means now: see `RESTORE_NEMESIS_SPAN`.
            let pause = |millis: u64| async move {
                if millis > 0 {
                    madsim::time::sleep(Duration::from_millis(millis)).await;
                }
            };
            let land = |batch: WriteBatch, handoff: Handoff| {
                let meta = Arc::clone(&meta);
                async move {
                    let outcome = meta.commit(batch).await.unwrap();
                    assert_eq!(
                        outcome,
                        CommitOutcome::Committed,
                        "the {handoff:?} must land, or this run tests nothing"
                    );
                    meta.log(HandoffEvent::Landed(handoff));
                }
            };

            pause(gaps[0]).await;
            let commit = WriteBatch::new()
                .require(mpu_key(&upload), open.clone())
                .delete(owned_key)
                .put(part_key(&upload, part), part_record);
            land(commit, Handoff::PartCommit).await;

            // The Complete fence: the session goes to `Completing` before the flip.
            let fence = WriteBatch::new()
                .require(mpu_key(&upload), open)
                .put(mpu_key(&upload), completing.clone());
            assert_eq!(meta.commit(fence).await.unwrap(), CommitOutcome::Committed);

            pause(gaps[1]).await;
            let flip = WriteBatch::new()
                .require(mpu_key(&upload), completing)
                .require_absent(metadata::inode_key(HANDOFF_INODE))
                .require_absent(metadata::dirent_key(ROOT, HANDOFF_OBJECT))
                .put(
                    metadata::inode_key(HANDOFF_INODE),
                    metadata::encode(&published),
                )
                .put(
                    metadata::dirent_key(ROOT, HANDOFF_OBJECT),
                    metadata::encode(&metadata::DirentRecord {
                        inode: HANDOFF_INODE,
                    }),
                )
                .put(mpu_key(&upload), completed);
            land(flip, Handoff::Flip).await;

            pause(gaps[2]).await;
            land(
                WriteBatch::new().delete(part_key(&upload, part)),
                Handoff::Drain,
            )
            .await;
            done.store(true, Ordering::Relaxed);
        }
    });

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-staged-handoffs").await;
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let mut passes = 0;
    loop {
        // One more pass once every move has landed, so a pass reads the store the moves left.
        let finished = done.load(Ordering::Relaxed);
        meta.log(HandoffEvent::Pass);
        let ctx = GcContext {
            meta: &*meta,
            fleet: &fleet,
            grace_window_millis: HANDOFF_GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        let outcome =
            reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, HANDOFF_NOW).await;
        passes += 1;
        assert!(
            outcome.is_ok(),
            "GC pass {passes} failed (gaps {gaps:?}): {:?}",
            outcome.err()
        );
        assert!(
            d[1].get_fragment(fragment).await.unwrap().is_some(),
            "GC pass {passes} reclaimed the moving chunk's fragment, which an owned staging entry, \
             a part record or a committed inode named at every instant of the run — the pass's \
             readings saw it in no class (gaps {gaps:?}): {:?}",
            meta.events.lock().unwrap()
        );
        if finished {
            break;
        }
        assert!(
            passes < HANDOFF_MAX_PASSES,
            "the concurrent task never finished its moves (gaps {gaps:?})"
        );
    }
    writer.await.expect("the concurrent task ran to completion");
    // The run's observations end here: the reads below are the property's, not a pass's.
    let events = meta.events.lock().unwrap().clone();

    // Every move landed, the chunk ends where the publication put it, and it was never reclaimed —
    // while the unprotected stray beside it was.
    assert!(
        is_marked(&*meta, 1, fragment).await,
        "the moving chunk's mark was consumed without a reclaim (gaps {gaps:?})"
    );
    assert!(
        d[2].get_fragment(stray).await.unwrap().is_none(),
        "the unprotected stray survived every pass: GC reclaimed nothing in this run, so keeping \
         the chunk proves nothing (gaps {gaps:?})"
    );
    assert!(
        meta.get(&metadata::inode_key(HANDOFF_INODE))
            .await
            .unwrap()
            .is_some()
            && meta.get(&part_key(&upload, part)).await.unwrap().is_none(),
        "the publication did not leave the chunk named by the committed inode alone (gaps {gaps:?})"
    );

    handoff_landings(&events, &upload)
}

/// The campaign leg: the seed picks when each of the three moves lands, so 50 seeds sweep the
/// schedule space around GC's staged and committed reads.
async fn prop_gc_staged_handoffs_never_reclaim_the_chunk(rng: &mut ChaCha8Rng) {
    let gaps = [(); 3].map(|()| u64::from(rng.next_u32() % (HANDOFF_SPAN + 1)));
    staged_handoffs_under_gc(gaps).await;
}

/// **The windows this property exists for are genuinely REACHED.** Walks one spacing across the
/// whole span — for all three moves, and again with the drain right behind the flip — asserting the
/// property at every point. Then it asserts that each move landed between the two reads it hands
/// protection across in at least one run and outside them in at least one other, and that in at
/// least one run the flip and the drain both landed between the same pass's two reads: the schedule
/// a reading that took committed inodes before part records sees the chunk in neither class.
/// Without it, a span that drifted away from GC's reads would leave the campaign green with nothing
/// behind it.
async fn prop_gc_staged_handoffs_reach_between_and_outside_the_reads() {
    let mut between = [false; 3];
    let mut outside = [false; 3];
    let mut published_within_one_window = false;
    for spacing in 0..=u64::from(HANDOFF_SPAN) {
        for drain_gap in [spacing, 0] {
            let landed = staged_handoffs_under_gc([spacing, spacing, drain_gap]).await;
            for handoff in Handoff::ALL {
                between[handoff.index()] |= landed.between(handoff);
                outside[handoff.index()] |= landed.outside(handoff);
            }
            published_within_one_window |= landed.published_within_one_window();
        }
    }
    for handoff in Handoff::ALL {
        assert!(
            between[handoff.index()],
            "no spacing in 0..={HANDOFF_SPAN} ms landed the {handoff:?} between the two reads it \
             hands protection across — the schedule this property exists for was never exercised"
        );
        assert!(
            outside[handoff.index()],
            "no spacing in 0..={HANDOFF_SPAN} ms landed the {handoff:?} outside the two reads it \
             hands protection across — the sweep is stuck in one regime"
        );
    }
    assert!(
        published_within_one_window,
        "no spacing in 0..={HANDOFF_SPAN} ms landed the flip AND the drain between one pass's part \
         read and its inode scan — the one publication schedule a destination-first reading loses \
         the chunk on was never exercised"
    );
}

// ---- property 14: a mover's adoption races GC's reclaim of the position it pre-marked (#804) ----
//
// A repoint pre-marks its destination position, writes the fragment there, and later adopts it in
// one CAS preconditioned on the pre-mark's exact bytes (`0016:1285-1292`). A pre-mark is a
// grace-limited promise, so a mover paused past the grace window meets GC reclaiming the position.
// GC records that reclamation — the mark swapped to `reclaiming` — BEFORE it deletes the fragment
// (`0016:1312-1320`), so the adoption either lands first (GC's intent loses, and the placement names
// bytes that are still there) or loses (GC deleted the bytes, and nothing is published). What it
// must never do is commit after the delete: a ledger still holding the pre-mark's bytes over a
// deleted fragment is the window a delete-first GC leaves open, and a placement over deleted bytes
// is 0016's outcome (c).
//
// The per-pass leg in `crates/custodian/tests/gc_reclaim_intent.rs` (B(iii)) lands the adoption
// inside GC's delete at a scripted instant. Here the mover is a genuinely concurrent task over the
// simulated-TiKV model, landing where the seed puts it, and every D-server call a pass makes — each
// listing and each delete — spans a simulated hop, so the adoption can land before GC reads the
// mark, between that read and GC's intent, inside the delete, and after the whole pass. The
// coverage leg proves both outcomes, the lost intent and the landing inside the delete are reached.

/// The object's chunk: its source fragment on server 0, referenced for the whole run, and the
/// mover's copy of it on server 1 — the pre-marked destination.
const ADOPT_CHUNK: ChunkId = 0x8040;
/// An unreferenced fragment on server 2, marked long past grace: GC reclaims it in every run — the
/// positive observable that GC is reclaiming at all.
const ADOPT_STRAY: ChunkId = 0x8041;
const ADOPT_INODE: InodeId = 8;
const ADOPT_OBJECT: &str = "adopt";
const ADOPT_GRACE: u64 = 50;
const ADOPT_NOW: u64 = 10_000;
/// How far into the run the adoption is drawn, in simulated milliseconds. A pass here takes about
/// 14: four reads before the fleet walk (session listing, inodes, cursor, ledger), four listings of
/// a hop each, the intent commit (two hops), two deletes of a hop each and the cleanup commit (two
/// hops). The adoption itself is a two-hop commit. So this spans "before the pass reads the mark"
/// through "after the whole pass", and the coverage leg proves the landings between are reached.
const ADOPT_SPAN: u32 = 20;
/// The most GC passes one run makes before it gives up on the mover finishing.
const ADOPT_MAX_PASSES: usize = 16;

/// The pre-mark's value: the legacy decimal, or 0016's structured shape naming the move's nonce.
#[derive(Clone, Copy, Debug)]
enum PreMark {
    Legacy,
    Structured,
}

impl PreMark {
    fn bytes(self) -> Bytes {
        match self {
            PreMark::Legacy => Bytes::from_static(b"0"),
            PreMark::Structured => {
                Bytes::from_static(br#"{"orphaned_at_millis":0,"event":"move-8040"}"#)
            }
        }
    }
}

/// One observation at the store seams, in the order the simulation produced them.
#[derive(Clone, Debug, PartialEq, Eq)]
enum AdoptEvent {
    /// A GC pass began.
    Pass,
    /// A ledger page handed the pre-mark to the pass.
    PreMarkRead,
    /// A D server began deleting a fragment; the delete spans a hop.
    DeleteBegan(DServerId, FragmentId),
    /// ...and the bytes are gone.
    Deleted(DServerId, FragmentId),
    /// A commit other than the adoption deleted the pre-mark's key: GC consumed it.
    PreMarkConsumed,
    /// The adoption was answered.
    Adoption(CommitOutcome),
}

type AdoptLog = Arc<Mutex<Vec<AdoptEvent>>>;

fn adopt_premark_key() -> Vec<u8> {
    metadata::orphan_key(1, frag_of(ADOPT_CHUNK))
}

/// A recording tap over the simulated-TiKV store: every call is forwarded, network hops included.
/// Instance state only (ADR-0035).
struct AdoptMeta {
    inner: SimTikvMetadataStore,
    log: AdoptLog,
}

#[async_trait]
impl MetadataStore for AdoptMeta {
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
        let (items, next) = self.inner.scan_page(prefix, after, limit).await?;
        if prefix == metadata::ORPHAN_PREFIX && items.iter().any(|(k, _)| *k == adopt_premark_key())
        {
            self.log.lock().unwrap().push(AdoptEvent::PreMarkRead);
        }
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let consumes = batch.deletes.contains(&adopt_premark_key())
            && !batch.puts.iter().any(|(key, _)| key.starts_with(b"inode:"));
        let outcome = self.inner.commit(batch).await?;
        if consumes && outcome == CommitOutcome::Committed {
            self.log.lock().unwrap().push(AdoptEvent::PreMarkConsumed);
        }
        Ok(outcome)
    }
}

/// A D server whose listing and delete each span a simulated hop, logging when a delete begins and
/// when its bytes are gone.
struct HopDServer<'a> {
    id: DServerId,
    inner: &'a MemDServer,
    log: AdoptLog,
}

#[async_trait]
impl ChunkStore for HopDServer<'_> {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.inner.put_fragment(id, fragment, deadline_millis).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        madsim::time::sleep(Duration::from_millis(1)).await;
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.log
            .lock()
            .unwrap()
            .push(AdoptEvent::DeleteBegan(self.id, id));
        madsim::time::sleep(Duration::from_millis(1)).await;
        self.inner.delete_fragment(id).await?;
        self.log
            .lock()
            .unwrap()
            .push(AdoptEvent::Deleted(self.id, id));
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// What one run's adoption met, observed at the store seams.
struct AdoptRun {
    /// The adoption committed: the mover won.
    adopted: bool,
    /// GC deleted the pre-marked destination's bytes.
    reclaimed: bool,
    /// The adoption committed after a pass had read the pre-mark and before that pass ended: GC's
    /// intent on the bytes it read lost to it.
    lost_to_the_adoption: bool,
    /// The adoption was answered after GC began deleting the destination's bytes and before GC
    /// consumed the mark — where a delete-first GC leaves the pre-mark's bytes standing over a
    /// deleted fragment.
    inside_the_delete: bool,
}

fn adopt_run(events: &[AdoptEvent]) -> AdoptRun {
    let position = |wanted: &AdoptEvent| events.iter().position(|event| event == wanted);
    let destination = frag_of(ADOPT_CHUNK);
    let answered = events
        .iter()
        .position(|event| matches!(event, AdoptEvent::Adoption(_)));
    let adopted = events.contains(&AdoptEvent::Adoption(CommitOutcome::Committed));
    let lost_to_the_adoption = adopted
        && answered.is_some_and(|at| {
            events[..at]
                .iter()
                .rev()
                .take_while(|event| **event != AdoptEvent::Pass)
                .any(|event| *event == AdoptEvent::PreMarkRead)
        });
    let inside_the_delete = match (
        position(&AdoptEvent::DeleteBegan(1, destination)),
        answered,
        position(&AdoptEvent::PreMarkConsumed),
    ) {
        (Some(began), Some(at), Some(consumed)) => began < at && at < consumed,
        _ => false,
    };
    AdoptRun {
        adopted,
        reclaimed: events.contains(&AdoptEvent::Deleted(1, destination)),
        lost_to_the_adoption,
        inside_the_delete,
    }
}

/// One run: GC passes back to back while the mover's adoption lands `delay_millis` into the run.
/// Asserts that no committed placement ever names a deleted fragment, and returns what the
/// adoption met.
async fn adoption_races_gc(premark: PreMark, delay_millis: u64) -> AdoptRun {
    let d = servers();
    let log: AdoptLog = Arc::new(Mutex::new(Vec::new()));
    let meta = Arc::new(AdoptMeta {
        inner: SimTikvMetadataStore::new(),
        log: Arc::clone(&log),
    });
    let chunk = frag_of(ADOPT_CHUNK);
    let stray = frag_of(ADOPT_STRAY);

    // The object, its source fragment on server 0.
    d[0].put_fragment(chunk, Bytes::from_static(b"source"), None)
        .await
        .unwrap();
    let prior = flat_committed_on(&[(ADOPT_CHUNK, 0)]);
    let created = metadata::create(&*meta, ROOT, ADOPT_OBJECT, ADOPT_INODE, &prior);
    assert_eq!(created.await.unwrap(), CommitOutcome::Committed);
    // The mover's destination: pre-marked, then written — and then the mover paused past the
    // grace window, so the pre-mark is stale when GC meets it.
    let premark_key = adopt_premark_key();
    let premarked = meta
        .commit(WriteBatch::new().put(premark_key.clone(), premark.bytes()))
        .await
        .unwrap();
    assert_eq!(premarked, CommitOutcome::Committed);
    d[1].put_fragment(chunk, Bytes::from_static(b"destination"), None)
        .await
        .unwrap();
    // The positive observable.
    d[2].put_fragment(stray, Bytes::from_static(b"stray"), None)
        .await
        .unwrap();
    mark_orphaned(&*meta, 2, stray, 0).await.unwrap();

    // The run starts here: what the fixture wrote above is not the mover.
    log.lock().unwrap().clear();

    // The adoption: the placement moves to server 1 only if the pre-mark still holds exactly the
    // bytes the mover wrote, and the source position is orphaned in the same commit.
    let next = InodeRecord {
        version: 2,
        ..flat_committed_on(&[(ADOPT_CHUNK, 1)])
    };
    let inode_key = metadata::inode_key(ADOPT_INODE);
    let adoption = WriteBatch::new()
        .require(inode_key.clone(), metadata::encode(&prior))
        .put(inode_key.clone(), metadata::encode(&next))
        .require(premark_key.clone(), premark.bytes())
        .delete(premark_key)
        .put(
            metadata::orphan_key(0, chunk),
            Bytes::from(ADOPT_NOW.to_string()),
        );
    let done = Arc::new(AtomicBool::new(false));
    let mover = madsim::task::spawn({
        let meta = Arc::clone(&meta);
        let log = Arc::clone(&log);
        let done = Arc::clone(&done);
        async move {
            // Zero means now: see `RESTORE_NEMESIS_SPAN`.
            if delay_millis > 0 {
                madsim::time::sleep(Duration::from_millis(delay_millis)).await;
            }
            let outcome = meta.commit(adoption).await.unwrap();
            log.lock().unwrap().push(AdoptEvent::Adoption(outcome));
            done.store(true, Ordering::Relaxed);
        }
    });

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-gc-reclaim-intent").await;
    let hops: Vec<HopDServer<'_>> = d
        .iter()
        .enumerate()
        .map(|(id, inner)| HopDServer {
            id: id as DServerId,
            inner,
            log: Arc::clone(&log),
        })
        .collect();
    let fleet: Vec<(DServerId, &dyn ChunkStore)> = hops
        .iter()
        .map(|hop| (hop.id, hop as &dyn ChunkStore))
        .collect();
    let mut passes = 0;
    loop {
        // One more pass once the adoption has been answered, so a pass reads what it left.
        let finished = done.load(Ordering::Relaxed);
        log.lock().unwrap().push(AdoptEvent::Pass);
        let ctx = GcContext {
            meta: &*meta,
            fleet: &fleet,
            grace_window_millis: ADOPT_GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        let outcome =
            reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, ADOPT_NOW).await;
        passes += 1;
        assert!(
            outcome.is_ok(),
            "GC pass {passes} failed ({premark:?} pre-mark, adoption at {delay_millis} ms): {:?}",
            outcome.err()
        );
        if finished {
            break;
        }
        assert!(
            passes < ADOPT_MAX_PASSES,
            "the adoption was never answered ({premark:?} pre-mark, at {delay_millis} ms)"
        );
    }
    mover.await.expect("the mover ran to completion");
    let events = log.lock().unwrap().clone();
    let run = adopt_run(&events);

    // NEVER A PLACEMENT OVER DELETED BYTES (outcome (c)): wherever the committed record places the
    // chunk, that D server still holds it.
    let record: InodeRecord =
        metadata::decode(&meta.get(&inode_key).await.unwrap().unwrap()).unwrap();
    let placed = record.chunk_map.as_flat().unwrap()[0].placement[0];
    assert!(
        d[placed as usize]
            .get_fragment(chunk)
            .await
            .unwrap()
            .is_some(),
        "the committed placement names server {placed}, whose copy of the chunk was deleted — the \
         adoption landed over bytes GC had already destroyed ({premark:?} pre-mark, adoption at \
         {delay_millis} ms): {events:?}"
    );
    // The adoption is the only writer of the placement, so the record says who won.
    assert_eq!(
        run.adopted,
        placed == 1,
        "the adoption's answer and the committed placement disagree: {events:?}"
    );
    assert!(
        !(run.adopted && run.reclaimed),
        "the adoption committed AND GC deleted its destination ({premark:?} pre-mark, adoption at \
         {delay_millis} ms): {events:?}"
    );
    // The source copy is referenced until the adoption, and marked inside grace by it: never lost.
    assert!(
        d[0].get_fragment(chunk).await.unwrap().is_some(),
        "the chunk's source copy was deleted: {events:?}"
    );
    assert!(
        d[2].get_fragment(stray).await.unwrap().is_none(),
        "the unprotected stray survived every pass: GC reclaimed nothing in this run, so the \
         property proves nothing ({premark:?} pre-mark, adoption at {delay_millis} ms)"
    );
    run
}

/// The campaign leg: the seed picks the pre-mark's shape and where the adoption lands, so 50 seeds
/// sweep the schedule space around GC's read, its intent and its delete.
async fn prop_gc_reclaim_intent_never_publishes_over_deleted_bytes(rng: &mut ChaCha8Rng) {
    let premark = if rng.next_u32().is_multiple_of(2) {
        PreMark::Legacy
    } else {
        PreMark::Structured
    };
    let delay = u64::from(rng.next_u32() % (ADOPT_SPAN + 1));
    adoption_races_gc(premark, delay).await;
}

/// **Both outcomes, and the windows between them, are genuinely REACHED.** Walks the adoption's
/// landing across the whole span for both pre-mark shapes, asserting the property at every point,
/// then asserts that the mover won in some run and GC in another; that in some run the adoption
/// won after a pass had read the pre-mark (GC's intent lost to it); and that in some run the
/// adoption was answered inside GC's delete — the one landing a delete-first GC turns into a
/// placement over deleted bytes. Without it, a span that drifted away from the pass would leave
/// the campaign green with nothing behind it.
async fn prop_gc_reclaim_intent_reaches_both_outcomes() {
    let (mut adopted, mut reclaimed, mut lost, mut inside) = (false, false, false, false);
    for premark in [PreMark::Legacy, PreMark::Structured] {
        for delay in 0..=u64::from(ADOPT_SPAN) {
            let run = adoption_races_gc(premark, delay).await;
            adopted |= run.adopted;
            reclaimed |= run.reclaimed;
            lost |= run.lost_to_the_adoption;
            inside |= run.inside_the_delete;
        }
    }
    assert!(
        adopted,
        "no landing in 0..={ADOPT_SPAN} ms let the mover's adoption win"
    );
    assert!(
        reclaimed,
        "no landing in 0..={ADOPT_SPAN} ms let GC reclaim the pre-marked destination"
    );
    assert!(
        lost,
        "no landing in 0..={ADOPT_SPAN} ms committed the adoption after a pass read the pre-mark — \
         GC's intent never met a mark that changed under it"
    );
    assert!(
        inside,
        "no landing in 0..={ADOPT_SPAN} ms answered the adoption inside GC's delete — the \
         interleaving this property exists for was never exercised"
    );
}

// ---- property 15: GC's sweep of fragment-less marks under a concurrent re-stamp (#800) ----
//
// A mark whose position holds no fragment is deleted by GC's sweep once the pass's own listing
// shows the position empty past the mark's late-write deadline (proposal 0016, `0016:1359-1408`).
// The delete is a compare-and-swap from the bytes the pass read, because a writer may re-stamp the
// mark between that read and the delete — a fresh pre-mark over the same position — and a blind
// delete would take the refreshed mark with it, leaving whatever it was written to evidence with
// no evidence at all (#661 round 1's finding). The per-pass legs in
// `crates/custodian/tests/gc_mark_sweep.rs` land that writer at scripted instants (D(i), D(iii));
// here the ledger is paged over the simulated-TiKV model with a page cap the seed picks, and a
// genuinely concurrent task re-stamps one sweep target at an instant the seed picks — before the
// pass reads it, between that read and the delete, inside the delete's own commit, or after it.
//
// The property: the refreshed mark always survives, holding exactly the refreshed value, and every
// other sweep target is gone by the end of the run (the sweep is running). The coverage leg proves
// that some landing point falls between a pass's read of the target and its delete of it, and
// that some does not.

/// Where the sweep targets' chunk ids start: target `i` is chunk `SWEEP_BASE + i`, fragment 0, on
/// D server `i % 4` — a fragment-less mark stamped at zero, so every pass here is past its
/// late-write deadline.
const SWEEP_BASE: ChunkId = 0x8050;
/// The grace every pass runs with: the deployed 60 s, which the late-write deadline (41 s) sits
/// strictly inside.
const SWEEP_GRACE: u64 = 60_000;
/// Every pass runs at this instant: past the late-write deadline of every mark stamped at zero, and
/// inside that of the re-stamp, which is stamped here — so the refreshed mark is never old enough
/// to sweep in this run, and a delete of it can only be the pass mistaking it for the mark it read.
const SWEEP_NOW: u64 = 100_000;
/// Passes per run: the first sweeps the ledger, the rest walk what the re-stamp left.
const SWEEP_PASSES: usize = 3;
/// How far into the run the re-stamp is drawn, in simulated milliseconds. A pass here is its
/// staged listing, its `inode:` scan and cursor read (a hop each), one hop per ledger page, and the
/// sweep's commit (two hops; a lost one adds a retry and a read), so three passes take about 30 ms
/// at the smallest page cap; the re-stamp itself is a two-hop commit. This spans "before the first
/// pass reads the target" through "after the last pass", and the coverage leg proves the landings
/// between are reached.
const SWEEP_SPAN: u32 = 36;

/// The re-stamp's value: a fresh legacy decimal, or 0016's structured shape naming a new move.
#[derive(Clone, Copy, Debug)]
enum Restamp {
    Legacy,
    Structured,
}

impl Restamp {
    fn bytes(self) -> Bytes {
        match self {
            Restamp::Legacy => Bytes::from(SWEEP_NOW.to_string()),
            Restamp::Structured => Bytes::from(format!(
                r#"{{"orphaned_at_millis":{SWEEP_NOW},"event":"move-8050"}}"#
            )),
        }
    }
}

/// One observation at the store seam, in the order the simulation produced them.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SweepEvent {
    /// A GC pass began.
    Pass,
    /// A ledger page handed the target's mark to the pass.
    TargetRead,
    /// A commit deleting the target's key under a precondition on it — the sweep's delete — was
    /// answered.
    DeleteTried(CommitOutcome),
    /// The concurrent writer's re-stamp committed.
    Restamped,
}

/// A recording tap over the simulated-TiKV store: every call is forwarded unchanged, network hops
/// included, and the target's reads, deletes and re-stamp are logged in the order they complete.
/// Instance state only (ADR-0035).
struct SweepMeta {
    inner: SimTikvMetadataStore,
    target: Vec<u8>,
    events: Mutex<Vec<SweepEvent>>,
}

impl SweepMeta {
    fn log(&self, event: SweepEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[async_trait]
impl MetadataStore for SweepMeta {
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
        let (items, next) = self.inner.scan_page(prefix, after, limit).await?;
        if prefix == metadata::ORPHAN_PREFIX && items.iter().any(|(key, _)| *key == self.target) {
            self.log(SweepEvent::TargetRead);
        }
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let deletes_target = batch.deletes.contains(&self.target)
            && batch.preconditions.iter().any(|pre| pre.key == self.target);
        let restamps = batch.puts.iter().any(|(key, _)| *key == self.target);
        let outcome = self.inner.commit(batch).await?;
        if deletes_target {
            self.log(SweepEvent::DeleteTried(outcome));
        }
        if restamps && outcome == CommitOutcome::Committed {
            self.log(SweepEvent::Restamped);
        }
        Ok(outcome)
    }
}

/// Whether the re-stamp landed between a pass's read of the target and that same pass's delete of
/// it: a read after the pass began and before the re-stamp, and a delete tried after the re-stamp
/// and before the next pass began.
fn restamp_between_read_and_delete(events: &[SweepEvent]) -> bool {
    let Some(landed) = events.iter().position(|e| *e == SweepEvent::Restamped) else {
        return false;
    };
    let began = events[..landed]
        .iter()
        .rposition(|e| *e == SweepEvent::Pass)
        .map_or(0, |at| at + 1);
    let ends = events[landed..]
        .iter()
        .position(|e| *e == SweepEvent::Pass)
        .map_or(events.len(), |at| landed + at);
    events[began..landed].contains(&SweepEvent::TargetRead)
        && events[landed..ends]
            .iter()
            .any(|e| matches!(e, SweepEvent::DeleteTried(_)))
}

/// One run: `SWEEP_PASSES` GC passes, each with a fresh `GcContext`, over `targets` fragment-less
/// marks paged `page_cap` at a time, while a concurrent writer re-stamps target `restamped`
/// `delay_millis` into the run. Asserts the property and returns whether the re-stamp landed
/// between a pass's read of the target and its delete.
async fn sweep_under_a_concurrent_restamp(
    page_cap: usize,
    targets: usize,
    restamped: usize,
    restamp: Restamp,
    delay_millis: u64,
) -> bool {
    let d = servers();
    let marks: Vec<(DServerId, FragmentId)> = (0..targets)
        .map(|i| ((i % 4) as DServerId, frag_of(SWEEP_BASE + i as ChunkId)))
        .collect();
    let (target_server, target) = marks[restamped];
    let target_key = metadata::orphan_key(target_server, target);
    let meta = Arc::new(SweepMeta {
        inner: SimTikvMetadataStore::new().with_scan_cap(page_cap),
        target: target_key.clone(),
        events: Mutex::new(Vec::new()),
    });
    for &(dserver, frag) in &marks {
        mark_orphaned(&*meta, dserver, frag, 0).await.unwrap();
    }
    // The run starts here: the fixture's own writes are not the concurrent writer.
    meta.events.lock().unwrap().clear();

    let refreshed = restamp.bytes();
    let writer = madsim::task::spawn({
        let meta = Arc::clone(&meta);
        let key = target_key.clone();
        let value = refreshed.clone();
        async move {
            // Zero means now: see `RESTORE_NEMESIS_SPAN`.
            if delay_millis > 0 {
                madsim::time::sleep(Duration::from_millis(delay_millis)).await;
            }
            // A blind put, retried: one that meets a pass's in-flight commit on the key loses the
            // lock race as `Err` — never `Conflict`, which a blind batch cannot have — and a real
            // writer tries again.
            loop {
                match meta
                    .commit(WriteBatch::new().put(key.clone(), value.clone()))
                    .await
                {
                    Ok(outcome) => {
                        assert_eq!(
                            outcome,
                            CommitOutcome::Committed,
                            "a blind batch is never Conflict"
                        );
                        break;
                    }
                    Err(_) => madsim::time::sleep(Duration::from_millis(1)).await,
                }
            }
        }
    });

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-gc-mark-sweep").await;
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    for pass in 1..=SWEEP_PASSES {
        meta.log(SweepEvent::Pass);
        // A FRESH context every pass, as the deployed loop builds one
        // (`crates/server/src/custodian.rs:600-608`).
        let ctx = GcContext {
            meta: &*meta,
            fleet: &fleet,
            grace_window_millis: SWEEP_GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        let outcome =
            reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, SWEEP_NOW).await;
        assert!(
            outcome.is_ok(),
            "GC pass {pass} failed (page cap {page_cap}, {targets} targets, re-stamp of \
             #{restamped} at {delay_millis} ms): {:?}",
            outcome.err()
        );
    }
    writer
        .await
        .expect("the concurrent re-stamp ran to completion");
    let events = meta.events.lock().unwrap().clone();

    // (1) THE REFRESHED MARK ALWAYS SURVIVES, holding exactly what its writer wrote.
    assert_eq!(
        meta.get(&target_key).await.unwrap(),
        Some(refreshed),
        "the re-stamped mark was deleted or changed ({restamp:?} re-stamp of #{restamped} at \
         {delay_millis} ms, page cap {page_cap}, {targets} targets): {events:?}"
    );
    // (2) EVERY OTHER TARGET IS SWEPT: the sweep ran, so the property above was earned.
    for (i, &(dserver, frag)) in marks.iter().enumerate() {
        if i != restamped {
            assert!(
                !is_marked(&*meta, dserver, frag).await,
                "target #{i} ({frag:?} on server {dserver}) is fragment-less and past its \
                 late-write deadline, and survived the run (page cap {page_cap}, re-stamp at \
                 {delay_millis} ms): {events:?}"
            );
        }
    }
    restamp_between_read_and_delete(&events)
}

/// The campaign leg: the seed picks the page cap, how many targets the ledger holds, which of them
/// the writer re-stamps, the re-stamp's shape and where it lands, so 50 seeds sweep the schedule
/// space around the pass's pages and its delete.
async fn prop_gc_fragment_less_sweep_never_deletes_a_restamped_mark(rng: &mut ChaCha8Rng) {
    let page_cap = 2 + (rng.next_u32() % 4) as usize;
    let targets =
        page_cap * (2 + (rng.next_u32() % 2) as usize) + (rng.next_u32() as usize % page_cap);
    let restamped = rng.next_u32() as usize % targets;
    let restamp = if rng.next_u32().is_multiple_of(2) {
        Restamp::Legacy
    } else {
        Restamp::Structured
    };
    let delay = u64::from(rng.next_u32() % (SWEEP_SPAN + 1));
    sweep_under_a_concurrent_restamp(page_cap, targets, restamped, restamp, delay).await;
}

/// **The window between the read and the delete is genuinely REACHED, and so is the outside of
/// it.** Walks the re-stamp's landing point across the whole span for both shapes, asserting the
/// full property at every point, then asserts that some landing fell between a pass's read of the
/// target and its delete of it — the interleaving a blind delete loses the refreshed mark to — and
/// some did not. Without it, a span that drifted away from the pass would leave the campaign green
/// with nothing behind it.
async fn prop_gc_fragment_less_sweep_reaches_between_the_read_and_the_delete() {
    let (mut between, mut outside) = (Vec::new(), Vec::new());
    for restamp in [Restamp::Legacy, Restamp::Structured] {
        for delay in 0..=u64::from(SWEEP_SPAN) {
            if sweep_under_a_concurrent_restamp(2, 8, 1, restamp, delay).await {
                between.push((restamp, delay));
            } else {
                outside.push((restamp, delay));
            }
        }
    }
    assert!(
        !between.is_empty(),
        "no landing point in 0..={SWEEP_SPAN} ms fell between a pass's read of the target and its \
         delete of it — the interleaving this property exists for was never exercised"
    );
    assert!(
        !outside.is_empty(),
        "every landing point in 0..={SWEEP_SPAN} ms fell between a pass's read and its delete — \
         the span no longer covers the landings before the read and after the delete"
    );
}

// ---- property 16: the sweep's judgement of an ambiguous commit, under a concurrent writer ----
// ---- (#800, PR #823 review) ----
//
// A sweep commit the store answers with an out-of-flight `CommitUnknownResult` landed whole or not
// at all, and the pass judges it from a fresh read of every key in it — sequential reads, each a
// network round-trip, between any two of which another writer (a second custodian across a
// leadership handoff) may delete or re-create a mark of that batch. The judgement must rest on the
// commit's atomicity and never on one read alone: a mark still holding what the pass read proves
// the batch landed nowhere, so a sibling found gone was the other writer's delete and is claimed
// by nobody (`mark-gone`); with no such survivor a gone key is recorded as gone and attributed to
// nobody (`sweep-unattributed`); and in no case is a `sweep-mark` claimed from an ambiguous commit.
// The per-pass legs D(iv) and D(vi) in `crates/custodian/tests/gc_mark_sweep.rs` land the other
// writer at a scripted instant; here the ledger is paged over the simulated-TiKV model, the
// ambiguity strikes the sweep's commit through a tap that applies the batch whole or not at all,
// and a genuinely concurrent task lands its delete or re-stamp at an instant the seed picks —
// before the pass's first read-back, between two of them, or after the last.
//
// The property: the struck pass claims no sweep and answers `Partial`; every key it read back is
// judged by the batch, not alone; and the sweep still converges — by the end of the run every
// target is gone except a re-stamped one, which holds exactly its re-stamp. The coverage leg proves
// that under each fate the other writer's landing is reached both before the pass's first
// read-back of the raced key and after its last read-back.

/// Where this property's targets' chunk ids start: target `i` is chunk `AMBIG_BASE + i`, fragment
/// 0, on D server `i % 4` — a fragment-less mark stamped at zero, past its late-write deadline at
/// [`SWEEP_NOW`].
const AMBIG_BASE: ChunkId = 0x8090;
/// How far after the ambiguous answer the other writer's landing is drawn, in simulated
/// milliseconds. The read-backs follow the answer at one hop each, one per target, so this spans
/// "before the first read-back" through "after the last" for the target counts drawn below.
const AMBIG_SPAN: u32 = 24;

/// What became of the struck commit: applied whole, or not at all. The store cannot say which;
/// the tap knows, so the leg can check the judgement against the truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fate {
    Landed,
    NotLanded,
}

/// What the other writer does to its target once the ambiguous answer is out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OtherWriter {
    /// A blind delete: the mark is gone by another hand.
    Delete,
    /// A blind re-stamp at [`SWEEP_NOW`]: a fresh mark inside its late-write deadline.
    Restamp,
}

/// One observation at the store seam, in the order the simulation produced them.
#[derive(Clone, Debug, PartialEq, Eq)]
enum AmbigEvent {
    /// A GC pass began.
    Pass,
    /// The sweep's delete of the targets was answered with an out-of-flight unknown result.
    Ambiguous,
    /// The pass read target `i` back after the ambiguous answer, and found this.
    ReadBack(usize, Option<Bytes>),
    /// The other writer's commit on its target landed.
    OtherLanded,
}

/// A tap over the simulated-TiKV store: every call is forwarded, network hops included, except the
/// one conditional delete of the targets, which is answered ambiguously — applied whole or not at
/// all, as the `MetadataStore` contract has it, then `CommitUnknownResult` out of flight. Target
/// read-backs after that answer and the other writer's landing are logged in completion order.
/// Instance state only (ADR-0035).
struct AmbiguousSweepMeta {
    inner: SimTikvMetadataStore,
    /// The targets' keys, by target index.
    targets: Vec<Vec<u8>>,
    /// The other writer's key.
    other: Vec<u8>,
    fate: Fate,
    /// The nemesis fires once: set when the sweep's delete is taken.
    struck: AtomicBool,
    /// Set once the ambiguous answer is out — what the other writer waits for.
    answered: AtomicBool,
    events: Mutex<Vec<AmbigEvent>>,
}

impl AmbiguousSweepMeta {
    fn log(&self, event: AmbigEvent) {
        self.events.lock().unwrap().push(event);
    }

    fn answered(&self) -> bool {
        self.answered.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MetadataStore for AmbiguousSweepMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let value = self.inner.get(key).await?;
        if self.answered() {
            if let Some(i) = self.targets.iter().position(|t| t == key) {
                self.log(AmbigEvent::ReadBack(i, value.clone()));
            }
        }
        Ok(value)
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
        let sweeps_a_target = self.targets.iter().any(|t| {
            batch.deletes.contains(t) && batch.preconditions.iter().any(|pre| pre.key == *t)
        });
        if sweeps_a_target && !self.struck.swap(true, Ordering::SeqCst) {
            // The resolver accepts the batch — nothing has moved a target yet, the other writer
            // waits for this very answer — and the reply is lost. Landed: applied whole, by the
            // store itself. Not landed: nothing is applied, and the two round-trips still pass.
            match self.fate {
                Fate::Landed => {
                    let outcome = self.inner.commit(batch).await?;
                    assert_eq!(
                        outcome,
                        CommitOutcome::Committed,
                        "fixture: the struck sweep commit's preconditions hold"
                    );
                }
                Fate::NotLanded => madsim::time::sleep(Duration::from_millis(2)).await,
            }
            self.log(AmbigEvent::Ambiguous);
            self.answered.store(true, Ordering::SeqCst);
            return Err(BoxError::from(sim_commit_unknown_result(
                SIM_COMMIT_UNKNOWN_RESULT,
            )));
        }
        let other = batch.preconditions.is_empty()
            && (batch.deletes.contains(&self.other)
                || batch.puts.iter().any(|(key, _)| *key == self.other));
        let outcome = self.inner.commit(batch).await?;
        if other && outcome == CommitOutcome::Committed {
            self.log(AmbigEvent::OtherLanded);
        }
        Ok(outcome)
    }
}

/// A `tracing` layer that keeps every field of every event as text, so a leg reads back the audit
/// lines a pass emitted — action, reason, counter and the mark they name — with no formatter in
/// between. Instance state only (ADR-0035).
#[derive(Clone, Default)]
struct AuditCapture {
    lines: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

struct TextVisitor<'a>(&'a mut BTreeMap<String, String>);

impl tracing::field::Visit for TextVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditCapture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut line = BTreeMap::new();
        event.record(&mut TextVisitor(&mut line));
        self.lines.lock().unwrap().push(line);
    }
}

impl AuditCapture {
    /// Every audit line of `action` naming `frag` on `dserver`.
    fn of(
        &self,
        action: &str,
        dserver: DServerId,
        frag: FragmentId,
    ) -> Vec<BTreeMap<String, String>> {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter(|line| {
                line.get("action").map(String::as_str) == Some(action)
                    && line.get("dserver") == Some(&dserver.to_string())
                    && line.get("index") == Some(&frag.index.to_string())
                    && line.get("chunk") == Some(&chunk_hex(frag.chunk))
            })
            .cloned()
            .collect()
    }

    /// The reasons of every `skip-mark` line naming `frag` on `dserver`.
    fn skips(&self, dserver: DServerId, frag: FragmentId) -> Vec<String> {
        self.of("skip-mark", dserver, frag)
            .iter()
            .filter_map(|line| line.get("reason").cloned())
            .collect()
    }

    /// How many lines carried the field `name` — for a counter, how often it ticked.
    fn ticks(&self, name: &str) -> usize {
        self.lines
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.contains_key(name))
            .count()
    }
}

/// Where the other writer's landing fell relative to the struck pass's read-backs.
struct AmbigRun {
    /// It landed before the pass read its target back — the read saw the other writer's work.
    before_first_read_back: bool,
    /// It landed after the pass's last read-back — the judgement saw the batch as it was.
    after_last_read_back: bool,
}

/// One run: `SWEEP_PASSES` GC passes over `targets` fragment-less marks paged `page_cap` at a
/// time, the first pass's sweep commit answered ambiguously with `fate`, while a concurrent writer
/// does `other` to target `raced` `delay_millis` after that answer. Asserts the property and
/// returns where the landing fell.
async fn sweep_judged_after_an_ambiguous_commit(
    page_cap: usize,
    targets: usize,
    raced: usize,
    other: OtherWriter,
    fate: Fate,
    delay_millis: u64,
) -> AmbigRun {
    assert!(
        targets >= 2,
        "a sibling is what the judgement reads the batch by"
    );
    let d = servers();
    let marks: Vec<(DServerId, FragmentId)> = (0..targets)
        .map(|i| ((i % 4) as DServerId, frag_of(AMBIG_BASE + i as ChunkId)))
        .collect();
    let keys: Vec<Vec<u8>> = marks
        .iter()
        .map(|&(dserver, frag)| metadata::orphan_key(dserver, frag))
        .collect();
    let meta = Arc::new(AmbiguousSweepMeta {
        inner: SimTikvMetadataStore::new().with_scan_cap(page_cap),
        targets: keys.clone(),
        other: keys[raced].clone(),
        fate,
        struck: AtomicBool::new(false),
        answered: AtomicBool::new(false),
        events: Mutex::new(Vec::new()),
    });
    for &(dserver, frag) in &marks {
        mark_orphaned(&*meta, dserver, frag, 0).await.unwrap();
    }
    let original = meta
        .get(&keys[0])
        .await
        .unwrap()
        .expect("fixture: the marks are written");
    // The run starts here: the fixture's own writes are not the other writer.
    meta.events.lock().unwrap().clear();

    let restamp = Bytes::from(SWEEP_NOW.to_string());
    let writer = madsim::task::spawn({
        let meta = Arc::clone(&meta);
        let key = keys[raced].clone();
        let restamp = restamp.clone();
        async move {
            while !meta.answered() {
                madsim::time::sleep(Duration::from_millis(1)).await;
            }
            if delay_millis > 0 {
                madsim::time::sleep(Duration::from_millis(delay_millis)).await;
            }
            // Blind, retried: one that meets a pass's in-flight commit on the key loses the lock
            // race as `Err` — never `Conflict`, which a blind batch cannot have — and tries again.
            loop {
                let batch = match other {
                    OtherWriter::Delete => WriteBatch::new().delete(key.clone()),
                    OtherWriter::Restamp => WriteBatch::new().put(key.clone(), restamp.clone()),
                };
                match meta.commit(batch).await {
                    Ok(outcome) => {
                        assert_eq!(
                            outcome,
                            CommitOutcome::Committed,
                            "a blind batch is never Conflict"
                        );
                        break;
                    }
                    Err(_) => madsim::time::sleep(Duration::from_millis(1)).await,
                }
            }
        }
    });

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord, "zone-gc-ambiguous-sweep").await;
    let fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])];
    let mut passes: Vec<(Reconciled, AuditCapture)> = Vec::with_capacity(SWEEP_PASSES);
    for pass in 1..=SWEEP_PASSES {
        meta.log(AmbigEvent::Pass);
        let capture = AuditCapture::default();
        // A FRESH context every pass, as the deployed loop builds one.
        let ctx = GcContext {
            meta: &*meta,
            fleet: &fleet,
            grace_window_millis: SWEEP_GRACE,
            expired_pending: ExpiredPendingPolicy::Defer,
        };
        let outcome = reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, SWEEP_NOW)
            .with_subscriber(tracing_subscriber::registry().with(capture.clone()))
            .await;
        let answer = outcome.unwrap_or_else(|err| {
            panic!(
                "GC pass {pass} failed ({fate:?}, {other:?} of #{raced} at {delay_millis} ms, \
                 page cap {page_cap}, {targets} targets): {err}"
            )
        });
        passes.push((answer, capture));
    }
    writer.await.expect("the other writer ran to completion");
    let events = meta.events.lock().unwrap().clone();
    let context = format!(
        "{fate:?}, {other:?} of #{raced} at {delay_millis} ms, page cap {page_cap}, {targets} \
         targets: {events:?}"
    );

    // The ambiguity was exercised, once, and the pass it struck read every key of the batch back.
    let struck = events
        .iter()
        .position(|e| *e == AmbigEvent::Ambiguous)
        .unwrap_or_else(|| panic!("the sweep's commit was never struck — {context}"));
    assert_eq!(
        events
            .iter()
            .filter(|e| **e == AmbigEvent::Ambiguous)
            .count(),
        1,
        "struck once — {context}"
    );
    let struck_end = events[struck..]
        .iter()
        .position(|e| *e == AmbigEvent::Pass)
        .map_or(events.len(), |at| struck + at);
    let read_backs: Vec<(usize, Option<Bytes>)> = events[struck..struck_end]
        .iter()
        .filter_map(|e| match e {
            AmbigEvent::ReadBack(i, value) => Some((*i, value.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        read_backs.len(),
        targets,
        "the struck pass reads every key of the ambiguous batch back, once — {context}"
    );

    // (1) NOTHING IS CLAIMED FROM AN AMBIGUOUS COMMIT: no sweep audited, none counted, and the
    // pass is no certification.
    let (answer, audit) = &passes[0];
    assert_eq!(
        *answer,
        Reconciled::Partial,
        "the struck pass answers Partial — {context}"
    );
    for &(dserver, frag) in &marks {
        assert!(
            audit.of("sweep-mark", dserver, frag).is_empty(),
            "the struck pass claimed a sweep it cannot know it made — {context}"
        );
    }
    assert_eq!(
        audit.ticks("monotonic_counter.gc_orphan_marks_swept"),
        0,
        "the struck pass counted a sweep it cannot know it made — {context}"
    );

    // (2) EACH KEY IS JUDGED BY THE BATCH, NOT ALONE. A survivor holding what the pass read is
    // exactly the not-landed fate: the tap knows, the pass infers.
    let survivor = read_backs
        .iter()
        .any(|(_, value)| value.as_ref() == Some(&original));
    assert_eq!(
        survivor,
        fate == Fate::NotLanded,
        "fixture: a sibling holding the read bytes is the not-landed fate and nothing else — \
         {context}"
    );
    for (i, value) in &read_backs {
        let (dserver, frag) = marks[*i];
        let skips = audit.skips(dserver, frag);
        let unattributed = audit.of("sweep-unattributed", dserver, frag).len();
        match value {
            Some(now) if *now == original => {
                assert_eq!(skips, ["mark-unchanged"], "target #{i} — {context}");
                assert_eq!(unattributed, 0, "target #{i} — {context}");
            }
            Some(_) => {
                assert_eq!(skips, ["mark-changed"], "target #{i} — {context}");
                assert_eq!(unattributed, 0, "target #{i} — {context}");
            }
            None if survivor => {
                assert_eq!(
                    skips,
                    ["mark-gone"],
                    "target #{i}: a sibling holding what the pass read proves the batch did not \
                     land, so this key was the other writer's delete — {context}"
                );
                assert_eq!(unattributed, 0, "target #{i} — {context}");
            }
            None => {
                assert!(skips.is_empty(), "target #{i} — {context}");
                assert_eq!(
                    unattributed, 1,
                    "target #{i}: gone with nothing to say by whose hand is recorded once as \
                     unattributed — {context}"
                );
            }
        }
    }

    // (3) THE SWEEP STILL CONVERGES, and no mark is ever claimed twice.
    for (i, &(dserver, frag)) in marks.iter().enumerate() {
        if i == raced && other == OtherWriter::Restamp {
            assert_eq!(
                meta.get(&keys[i]).await.unwrap(),
                Some(restamp.clone()),
                "the re-stamped mark was deleted or changed — {context}"
            );
        } else {
            assert!(
                !is_marked(&*meta, dserver, frag).await,
                "target #{i} is fragment-less and past its late-write deadline, and survived the \
                 run — {context}"
            );
        }
        let claims: usize = passes
            .iter()
            .map(|(_, audit)| audit.of("sweep-mark", dserver, frag).len())
            .sum();
        assert!(
            claims <= 1,
            "target #{i} claimed swept {claims} times — {context}"
        );
    }

    let landed = events.iter().position(|e| *e == AmbigEvent::OtherLanded);
    let first_read_back = events[struck..struck_end]
        .iter()
        .position(|e| matches!(e, AmbigEvent::ReadBack(i, _) if *i == raced))
        .map(|at| struck + at);
    let last_read_back = events[struck..struck_end]
        .iter()
        .rposition(|e| matches!(e, AmbigEvent::ReadBack(..)))
        .map(|at| struck + at);
    AmbigRun {
        before_first_read_back: matches!((landed, first_read_back), (Some(l), Some(r)) if l < r),
        after_last_read_back: matches!((landed, last_read_back), (Some(l), Some(r)) if l > r),
    }
}

/// The campaign leg: the seed picks the page cap, how many targets the ledger holds, which of them
/// the other writer touches and how, the struck commit's fate and where the landing falls, so 50
/// seeds sweep the schedule space around the pass's read-backs.
async fn prop_gc_sweep_judges_an_ambiguous_commit_as_one_batch(rng: &mut ChaCha8Rng) {
    let page_cap = 2 + (rng.next_u32() % 4) as usize;
    let targets =
        page_cap * (2 + (rng.next_u32() % 2) as usize) + (rng.next_u32() as usize % page_cap);
    let raced = rng.next_u32() as usize % targets;
    let other = if rng.next_u32().is_multiple_of(2) {
        OtherWriter::Delete
    } else {
        OtherWriter::Restamp
    };
    let fate = if rng.next_u32().is_multiple_of(2) {
        Fate::Landed
    } else {
        Fate::NotLanded
    };
    let delay = u64::from(rng.next_u32() % (AMBIG_SPAN + 1));
    sweep_judged_after_an_ambiguous_commit(page_cap, targets, raced, other, fate, delay).await;
}

/// **The landing before the read-back is genuinely REACHED, and so is the one after it, under
/// each fate.** Walks the other writer's landing across the span for both of its shapes and both
/// fates, asserting the full property at every point, then asserts that some landing fell before
/// the pass read the raced key back — the interleaving a per-key judgement misattributes — and
/// some after its last read-back. Without it, a span that drifted away from the read-backs would
/// leave the campaign green with nothing behind it.
async fn prop_gc_sweep_ambiguity_reaches_before_and_after_the_read_back() {
    for fate in [Fate::Landed, Fate::NotLanded] {
        let (mut before, mut after) = (Vec::new(), Vec::new());
        for other in [OtherWriter::Delete, OtherWriter::Restamp] {
            for delay in 0..=u64::from(AMBIG_SPAN) {
                let run = sweep_judged_after_an_ambiguous_commit(2, 8, 6, other, fate, delay).await;
                if run.before_first_read_back {
                    before.push((other, delay));
                }
                if run.after_last_read_back {
                    after.push((other, delay));
                }
            }
        }
        assert!(
            !before.is_empty(),
            "{fate:?}: no landing in 0..={AMBIG_SPAN} ms fell before the struck pass read the \
             raced key back — the interleaving this property exists for was never exercised"
        );
        assert!(
            !after.is_empty(),
            "{fate:?}: every landing in 0..={AMBIG_SPAN} ms fell before the struck pass's last \
             read-back — the span no longer covers the landings after the judgement"
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

dst_campaign_test! {
    async fn gc_orphan_walk_under_a_concurrent_unlink() {
        prop_gc_orphan_walk_under_a_concurrent_unlink(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn gc_orphan_walk_reaches_the_mid_walk_landing() {
        prop_gc_orphan_walk_reaches_the_mid_walk_landing().await;
    }
}

dst_campaign_test! {
    async fn gc_staged_handoffs_never_reclaim_the_chunk() {
        prop_gc_staged_handoffs_never_reclaim_the_chunk(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn gc_staged_handoffs_reach_between_and_outside_the_reads() {
        prop_gc_staged_handoffs_reach_between_and_outside_the_reads().await;
    }
}

dst_campaign_test! {
    async fn gc_reclaim_intent_never_publishes_over_deleted_bytes() {
        prop_gc_reclaim_intent_never_publishes_over_deleted_bytes(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn gc_reclaim_intent_reaches_both_outcomes() {
        prop_gc_reclaim_intent_reaches_both_outcomes().await;
    }
}

dst_campaign_test! {
    async fn gc_fragment_less_sweep_never_deletes_a_restamped_mark() {
        prop_gc_fragment_less_sweep_never_deletes_a_restamped_mark(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn gc_fragment_less_sweep_reaches_between_the_read_and_the_delete() {
        prop_gc_fragment_less_sweep_reaches_between_the_read_and_the_delete().await;
    }
}

dst_campaign_test! {
    async fn gc_sweep_judges_an_ambiguous_commit_as_one_batch() {
        prop_gc_sweep_judges_an_ambiguous_commit_as_one_batch(&mut rand_seed()).await;
    }
}

dst_campaign_test! {
    async fn gc_sweep_ambiguity_reaches_before_and_after_the_read_back() {
        prop_gc_sweep_ambiguity_reaches_before_and_after_the_read_back().await;
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
            prop_gc_orphan_walk_under_a_concurrent_unlink(&mut rng).await;
            prop_gc_staged_handoffs_never_reclaim_the_chunk(&mut rng).await;
            prop_gc_reclaim_intent_never_publishes_over_deleted_bytes(&mut rng).await;
            prop_gc_fragment_less_sweep_never_deletes_a_restamped_mark(&mut rng).await;
            prop_gc_sweep_judges_an_ambiguous_commit_as_one_batch(&mut rng).await;
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
