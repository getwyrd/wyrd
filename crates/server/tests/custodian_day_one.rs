//! **The day-one durability signal, observed through the deployable custodian role —
//! surviving the kill, covering the worst losses, and driving the real run loop.**
//! (Observability-floor proposal 0010 §"Scope boundary" items 1–2 + §"DST and tests";
//! architecture §7.4 single-zone day-one step 4; ADR-0011/0012.)
//!
//! The M3 reconstruction suite proves the durability metrics are *present*, and the DST
//! campaign proves the under-replicated count rises-then-returns-to-zero — but only
//! through a bespoke per-event capture, never the real export surface, and never through
//! a runnable process. This test closes that gap: it drives the **`server`-crate
//! deployable custodian role** ([`wyrd_server::custodian`]) — the same production wiring
//! the `wyrd custodian` binary runs (`cli::cmd_custodian` →
//! `CustodianService::run_reconstruction_until` → `live_reconstruction_view` +
//! `reconcile_pass`) — not the library alone, and not a hand-assembled subscriber.
//!
//! The binding day-one properties (surviving the kill; the backlog gauge's rise-then-return-to
//! -zero holding on a populated store, on a transient outage, and on an un-completable repair;
//! the run loop's survival + fencing; the fleet-assembly seam; and the real binary entry). Among
//! them:
//!
//! 1. [`gauge_rises_then_returns_to_zero_surviving_a_killed_dserver`] — the role is handed
//!    the **real production fleet, including the D-server that dies** (an unreachable
//!    [`DeadDServer`], *not* curated out of the input). The production
//!    [`live_reconstruction_view`] probes reachability and drops the dead node, so the
//!    reconstruction plane reads *around* it and the role does not crash on the transient
//!    fetch fault a killed server raises. The gauge reads **1** after the loss, **0** after
//!    repair — the day-one signal, through a role that survives the kill.
//! 2. [`a_loss_beyond_tolerance_raises_data_loss_and_the_backlog_gauge_returns_to_zero`] —
//!    killing *two* fragments of an RS(2,1) chunk leaves it below `k`, un-reconstructable: a
//!    permanent DATA LOSS. That is *more* severe than a repairable backlog, so it is raised on
//!    its own high-severity `reconstruction_data_loss` signal (NEEDS-HUMAN) and EXCLUDED from
//!    the `reconstruction_under_replicated` backlog gauge — which is a *level* that must return
//!    to zero. On a populated store carrying such a loss, the backlog gauge still rises to **1**
//!    for a repairable loss and returns to **0** after repair. Pre-fix (the count tallied the
//!    un-reconstructable chunk too) the gauge floors at 1 and never returns to zero — RED;
//!    post-fix GREEN.
//! 3. [`run_loop_survives_a_dead_dserver_and_keeps_running`] — the **real continuous run
//!    loop** `run_reconstruction_until`, driven over a fleet that includes the killed node,
//!    returns `Ok(())` at shutdown (it never exited on the kill) and the gauge is observable
//!    off its own export surface. This exercises the Store→continue survival policy.
//! 4. [`run_loop_stops_when_fenced`] — a superseded custodian's run loop returns
//!    `Err(Fenced)` immediately (a deposed leader must not keep acting).
//!
//! Why a gauge is load-bearing: the under-replicated count is a *level*, and only a gauge
//! returns to zero through an accumulating Prometheus registry. A monotonic counter
//! (`add(1)` then `add(0)`) is exported as `..._total` and stays pinned at 1 forever.
//!
//! Its own test binary: `tracing` caches per-callsite *interest* in process-global state,
//! so the reconstruction callsites must not be raced by a no-subscriber sibling test — a
//! separate binary is a separate process.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, EcScheme, InodeId, InodeRecord};
use wyrd_core::placement::Topology;
use wyrd_core::repair;
use wyrd_core::write::write_new_object_placed;
use wyrd_custodian::{
    Custodian, ExpiredPendingPolicy, FencedZone, ReconcileError, Reconciled, ReconstructionContext,
};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::cli::{
    require_aligned_topology, run_reconstruction_over_backend, MetadataBackend,
};
use wyrd_server::custodian::{
    connect_fleet, live_reconstruction_view, ConfiguredDServer, CustodianService, DServerConnector,
};
use wyrd_telemetry::{DurabilityTelemetry, ExporterConfig};
use wyrd_traits::{
    BoxError, ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
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

/// A **killed / unreachable** D-server: every call fails with a *transient* transport
/// error (NOT an integrity fault and NOT `EIO` — the exact shape a dead gRPC endpoint
/// raises, which `reconstruction::assess` propagates rather than reads around). This is
/// the node the day-one runbook kills. Left in the fleet, its first fetch would unwind the
/// whole pass; the role must drop it via its reachability probe (`health()` errs) and read
/// *around* it. It is handed to the role in the fleet input — it is not curated out — so
/// the production survive-the-kill path is exercised end to end.
struct DeadDServer;

fn unreachable() -> wyrd_traits::BoxError {
    // A plain transport error: not `wyrd_traits::IntegrityFault`, not an `io::Error` with
    // `EIO` — so `is_permanent_read_fault` is false and the loop would propagate it.
    "d-server unreachable: connection refused".into()
}

#[async_trait]
impl ChunkStore for DeadDServer {
    async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> Result<()> {
        Err(unreachable())
    }

    async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
        Err(unreachable())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Err(unreachable())
    }

    async fn delete_fragment(&self, _id: FragmentId) -> Result<()> {
        Err(unreachable())
    }

    async fn health(&self) -> Result<Health> {
        Err(unreachable())
    }
}

/// A server that is **reachable but faults every fetch transiently** — its `health()` is
/// `Ok`, so the reachability probe keeps it in the fleet, but a `get_fragment` raises a
/// transient error the reconstruction assessment propagates. That surfaces as a
/// [`ReconcileError::Store`] from a pass — the exact fault the continuous run loop must
/// *log and continue* over (a server that dies AFTER the probe), not exit on.
struct FaultyDServer;

#[async_trait]
impl ChunkStore for FaultyDServer {
    async fn put_fragment(&self, _id: FragmentId, _fragment: Bytes) -> Result<()> {
        Err(unreachable())
    }

    async fn get_fragment(&self, _id: FragmentId) -> Result<Option<Bytes>> {
        Err(unreachable())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Err(unreachable())
    }

    async fn delete_fragment(&self, _id: FragmentId) -> Result<()> {
        Err(unreachable())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// A fake [`DServerConnector`] for headless coverage of `connect_fleet` (the exact
/// production fleet-assembly seam `cmd_custodian` runs — iteration-5 BLOCKING #2a). It maps
/// each endpoint to a pre-built in-memory store; an endpoint whose entry is `None` is a peer
/// **killed before the custodian started**, for which `connect` returns a transport `Err` —
/// the startup-unreachable case BLOCKING #3 requires the role to start degraded around,
/// rather than exit on. No network, no gRPC: the seam is driven through injected in-memory
/// fakes, exactly as the day-one tests already build.
struct FakeConnector {
    stores: HashMap<String, Option<Arc<dyn ChunkStore>>>,
}

#[async_trait]
impl DServerConnector for FakeConnector {
    async fn connect(
        &self,
        endpoint: &str,
        _timeout: Duration,
    ) -> std::result::Result<Arc<dyn ChunkStore>, BoxError> {
        match self.stores.get(endpoint) {
            Some(Some(store)) => Ok(store.clone()),
            // Down at startup (or unknown endpoint): the peer the day-one incident killed.
            Some(None) | None => {
                Err(format!("fake connector: D server `{endpoint}` unreachable at startup").into())
            }
        }
    }
}

/// A **placement-aware** fleet over several [`MemDServer`]s: it routes `_at` calls to the
/// D server the placement record names, so the write fan-out resolves each fragment from
/// its recorded location — the seam a custodian re-placement flips.
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

/// Install a permissive global `tracing` default **once** so the durability metric
/// callsites (`tracing::info!(gauge.reconstruction_under_replicated = …)`) never latch
/// `Interest::never` under the parallel test harness. `tracing` caches each callsite's
/// interest in a process-global table the first time it is hit; the first hit racing
/// against a no-subscriber default can latch the callsite disabled, after which the one
/// test that reads the metric back (`gather_prometheus`) silently sees it missing — the
/// flaky read-back the C4 gate correctly caught (iteration-4 §gating). Registering against
/// an always-enabling default before any callsite is hit makes every first-registration
/// agree, so the callsite can never latch off; each pass's scoped `with_subscriber(...)`
/// still routes its metrics into that pass's own provider. Called at the top of every
/// metric-touching test so whichever runs first sets the default before any callsite fires
/// (the proven pattern from `crates/custodian/tests/scrub.rs:208`).
fn enable_metric_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

const ROOT: InodeId = 0;
const INODE: InodeId = 1;
const CHUNK: ChunkId = 0xC0FFEE;

/// A four-domain topology A..D (servers 0..3).
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
        .get(&metadata::inode_key(INODE))
        .await
        .unwrap()
        .expect("inode present");
    metadata::decode(&bytes).unwrap()
}

/// Write one RS(2,1) chunk via the real write path — n = 3 fragments placed on servers
/// 0,1,2 (domains A,B,C).
async fn write_rs_2_1(meta: &MemMeta, fleet: &Fleet<'_>) {
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
        EcScheme::ReedSolomon { k: 2, m: 1 },
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
}

/// The value of the named **gauge** read back off the Prometheus surface — the last
/// matching sample. A gauge never emitted is absent → `None`. Robust to the exporter's
/// scope-label decoration and to whitespace.
fn gauge_value(exposed: &str, name: &str) -> Option<f64> {
    exposed
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let key = fields.next()?;
            let value = fields.next()?;
            let metric = key.split('{').next().unwrap_or(key);
            if metric == name {
                value.parse::<f64>().ok()
            } else {
                None
            }
        })
        .next_back()
}

/// Read the role's `reconstruction_under_replicated` gauge back off its own export surface.
fn under_replicated(service: &CustodianService) -> Option<f64> {
    service.telemetry().flush().unwrap();
    let exposed = service
        .telemetry()
        .gather_prometheus()
        .expect("Prometheus surface configured");
    gauge_value(&exposed, "reconstruction_under_replicated")
}

/// The value of a `monotonic_counter` read back off the Prometheus surface, summed over every
/// exported sample. The OTel→Prometheus exporter suffixes a counter with `_total`; accept
/// either spelling. A counter never emitted is absent → `0`.
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

/// Read the role's `reconstruction_data_loss` counter back off its own export surface — the
/// distinct high-severity signal an un-reconstructable (below-`k`) loss is raised on.
fn data_loss(service: &CustodianService) -> u64 {
    service.telemetry().flush().unwrap();
    let exposed = service
        .telemetry()
        .gather_prometheus()
        .expect("Prometheus surface configured");
    counter_total(&exposed, "reconstruction_data_loss")
}

/// Read an arbitrary named **gauge** back off the role's own export surface — used for the
/// distinct `reconstruction_unreachable` (transient-degraded) and `reconstruction_repair_blocked`
/// (no-free-domain) levels the backlog gauge routes those non-repairable-now conditions onto.
fn gauge(service: &CustodianService, name: &str) -> Option<f64> {
    service.telemetry().flush().unwrap();
    let exposed = service
        .telemetry()
        .gather_prometheus()
        .expect("Prometheus surface configured");
    gauge_value(&exposed, name)
}

/// Write one RS(2,1) chunk via the real write path under an explicit inode/name/chunk id, so a
/// test can populate a store with more than one object (the day-one populated-store cases). Its
/// 3 fragments land on servers 0,1,2 (domains A,B,C), the same as [`write_rs_2_1`].
async fn write_rs_2_1_as(
    meta: &MemMeta,
    fleet: &Fleet<'_>,
    inode_id: InodeId,
    name: &str,
    chunk_id: ChunkId,
) {
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
}

/// Erase a concrete `Arc<S>` store to the owned `Arc<dyn ChunkStore>` the fleet now holds
/// (the fleet is OWNED so [`connect_fleet`] can *return* it — iteration-5 BLOCKING #2b).
fn dyn_store<S: ChunkStore + 'static>(store: &Arc<S>) -> Arc<dyn ChunkStore> {
    store.clone()
}

/// The real production fleet-input the role is handed: every configured D-server,
/// keyed by its operator-supplied stable id + failure domain (matching each server's
/// registered identity — never fabricated from index). Stores are OWNED (`Arc`).
fn configured(servers: [(DServerId, &str, Arc<dyn ChunkStore>); 4]) -> Vec<ConfiguredDServer> {
    servers
        .into_iter()
        .map(|(id, dom, store)| ConfiguredDServer {
            id,
            failure_domain: dom.to_string(),
            store,
        })
        .collect()
}

// ---- 1: kill a D-server → the role SURVIVES and the gauge rises to 1, then to 0 ----

#[tokio::test]
async fn gauge_rises_then_returns_to_zero_surviving_a_killed_dserver() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    let fleet = Fleet {
        servers: vec![
            (0, d0.as_ref()),
            (1, d1.as_ref()),
            (2, d2.as_ref()),
            (3, d3.as_ref()),
        ],
    };

    write_rs_2_1(&meta, &fleet).await;

    // KILL D server 1 (domain B): the architecture §7.4 day-one step-4 fault. A health
    // report enqueues the chunk on the shared repair queue.
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // THE REAL PRODUCTION FLEET: every configured endpoint, INCLUDING the killed node —
    // server 1 is an unreachable `DeadDServer`, handed to the role in the input, NOT
    // curated out. This is the exact production path `cli::cmd_custodian` builds.
    let dead = Arc::new(DeadDServer);
    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&dead)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    // PRODUCTION reachability derivation: the killed server is dropped, so the plane reads
    // AROUND it (its fragment resolves as missing) rather than crashing on its transient
    // fetch fault. This is the wiring that makes the role survive the kill.
    let (live_fleet, live_topo, unreachable) = live_reconstruction_view(&servers).await;
    assert_eq!(
        live_fleet.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![0, 2, 3],
        "the killed server 1 is dropped from the live fleet; 0,2,3 remain"
    );

    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &live_fleet,
        topology: &live_topo,
        // The killed server(s) dropped this pass: their placed fragments are transiently
        // unavailable, so `assess` must not raise a false data-loss alarm on them.
        unreachable: &unreachable,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);

    // PASS 1 — under-replicated: the count RISES. The pass does NOT error on the dead
    // server (it was read around), so the role survives.
    let outcome = service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .expect("the role survives the killed D-server (no crash on the transient fault)");
    assert_eq!(
        outcome,
        Reconciled::Changed,
        "the under-replicated chunk was reconstructed around the killed server"
    );
    assert_eq!(
        under_replicated(&service),
        Some(1.0),
        "after the kill the under-replicated gauge reads 1 off gather_prometheus"
    );

    // PASS 2 — repaired: the obligation is drained, so the count RETURNS TO ZERO.
    let outcome = service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 600)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        Reconciled::Satisfied,
        "the chunk is at full redundancy — nothing left to reconstruct"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the repair obligation was drained by the reconstruction commit"
    );
    assert_eq!(
        under_replicated(&service),
        Some(0.0),
        "once repair restores redundancy the under-replicated gauge returns to 0 — the \
         day-one signal (0010; architecture §7.4 step 4), through a role that survived the kill"
    );
}

// ---- 2: a loss BEYOND tolerance is a DATA-LOSS event on its own signal — the repairable
//         backlog gauge EXCLUDES it, so the day-one gauge still returns to zero (iteration-6) --

#[tokio::test]
async fn a_loss_beyond_tolerance_raises_data_loss_and_the_backlog_gauge_returns_to_zero() {
    enable_metric_callsites();
    const CHUNK_LOST: ChunkId = 0x0D00_D000;
    const INODE_LOST: InodeId = 2;
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    let fleet = Fleet {
        servers: vec![
            (0, d0.as_ref()),
            (1, d1.as_ref()),
            (2, d2.as_ref()),
            (3, d3.as_ref()),
        ],
    };

    // A POPULATED store: chunk A (the day-one drill) is a REPAIRABLE loss; chunk B is a
    // permanent DATA LOSS (below k). The binding "rise then return to ZERO" must hold for the
    // repairable backlog gauge even while chunk B sits un-reconstructable.
    write_rs_2_1(&meta, &fleet).await;
    write_rs_2_1_as(&meta, &fleet, INODE_LOST, "lost", CHUNK_LOST).await;

    // KILL chunk A on server 1 (domain B): a real, REPAIRABLE under-replication (survivors ≥ k).
    // KILL TWO of chunk B's three fragments (servers 0 AND 2 hold indices 0 and 2): only the
    // fragment on server 1 survives → below k = 2 → un-reconstructable, the data is LOST.
    d0.delete_fragment(FragmentId {
        chunk: CHUNK_LOST,
        index: 0,
    })
    .await
    .unwrap();
    d2.delete_fragment(FragmentId {
        chunk: CHUNK_LOST,
        index: 2,
    })
    .await
    .unwrap();
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();
    repair::enqueue_repair(&meta, CHUNK_LOST, "health")
        .await
        .unwrap();

    // THE REAL PRODUCTION FLEET, including the killed node for chunk A (server 1 unreachable),
    // handed to the role — not curated out. Servers 0,2,3 stay reachable.
    let dead = Arc::new(DeadDServer);
    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&dead)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);
    let (live_fleet, live_topo, unreachable) = live_reconstruction_view(&servers).await;
    assert_eq!(
        live_fleet.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![0, 2, 3],
        "the killed server 1 is dropped; 0,2,3 remain live"
    );
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &live_fleet,
        topology: &live_topo,
        // The killed server(s) dropped this pass: their placed fragments are transiently
        // unavailable, so `assess` must not raise a false data-loss alarm on them.
        unreachable: &unreachable,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);

    // PASS 1 — the backlog gauge RISES to 1 (chunk A only). The lost chunk B is raised on the
    // distinct data-loss signal, NOT the backlog gauge. Pre-fix (Unrepairable counted on the
    // gauge) this reads 2 and no data-loss metric exists.
    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .expect("the role survives the killed D-server");
    assert_eq!(
        under_replicated(&service),
        Some(1.0),
        "the backlog gauge counts only the REPAIRABLE loss (chunk A) — not the un-reconstructable \
         chunk B; pre-fix (data loss counted) this is 2"
    );
    assert!(
        data_loss(&service) >= 1,
        "the below-k loss is raised on the distinct reconstruction_data_loss signal (NEEDS-HUMAN); \
         pre-fix this metric does not exist"
    );

    // PASS 2 — chunk A is repaired; chunk B is still lost (queued). The backlog gauge RETURNS TO
    // ZERO despite the permanent data loss — the day-one signal on a populated store. Pre-fix it
    // stays pinned at 1.
    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 600)
        .await
        .unwrap();
    assert_eq!(
        under_replicated(&service),
        Some(0.0),
        "once chunk A is repaired the backlog gauge returns to ZERO even though chunk B is still \
         un-reconstructable — the binding rise-then-zero signal holds on a store carrying data loss"
    );
    assert!(
        data_loss(&service) >= 1,
        "the data-loss signal is STILL raised while chunk B remains un-reconstructable"
    );
    let queued = repair::queued_repairs(&meta).await.unwrap();
    assert!(
        queued.contains(&CHUNK_LOST),
        "the lost chunk stays queued for a human, off the backlog gauge"
    );
    assert!(
        !queued.contains(&CHUNK),
        "the repairable chunk A's obligation was drained by the reconstruction commit"
    );
}

// ---- 2b: a TRANSIENT below-k outage is NOT a false data-loss page, and it recovers ----

/// A below-`k` shortfall driven purely by **reachability** (a rolling restart / partition
/// isolating m+1 nodes) must NOT fire the high-severity `reconstruction_data_loss` alarm on
/// physically-intact fragments (iteration-7 MUST-FIX). Here TWO of an RS(2,1) chunk's three
/// D-servers are transiently unreachable (health errs → dropped), so only ONE survivor is
/// fetchable — below `k = 2`. Pre-fix the pass classified that as `Unrepairable` and paged
/// "DATA IS LOST; NEEDS-HUMAN"; post-fix it is a recoverable **unreachable-degraded** state on
/// its own distinct, lower-severity signal, and it fully recovers (obligation drained, all
/// gauges zero) when the nodes return with their intact fragments.
///
/// The current day-one suite never covered this — every other drill hands the custodian a spare
/// server, so survivors stay ≥ `k` and the false-alarm path is unexercised.
#[tokio::test]
async fn a_transient_below_k_outage_does_not_false_alarm_data_loss_and_recovers() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    let fleet = Fleet {
        servers: vec![
            (0, d0.as_ref()),
            (1, d1.as_ref()),
            (2, d2.as_ref()),
            (3, d3.as_ref()),
        ],
    };
    write_rs_2_1(&meta, &fleet).await; // fragments on servers 0,1,2 (domains A,B,C)
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);

    // PASS 1 — servers 1 AND 2 are TRANSIENTLY unreachable (a rolling restart / partition):
    // both are `DeadDServer` (health errs), dropped from the live fleet. Only server 0's
    // fragment is fetchable → survivors = 1 < k = 2. Their fragments are physically intact,
    // just unreachable right now.
    let (dead1, dead2) = (Arc::new(DeadDServer), Arc::new(DeadDServer));
    let servers_down = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&dead1)),
        (2, "C", dyn_store(&dead2)),
        (3, "D", dyn_store(&d3)),
    ]);
    let (live_fleet, live_topo, unreachable) = live_reconstruction_view(&servers_down).await;
    assert_eq!(
        unreachable,
        vec![1, 2],
        "servers 1 and 2 failed their probe → reported as transiently unreachable this pass"
    );
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &live_fleet,
        topology: &live_topo,
        unreachable: &unreachable,
    };
    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .expect("the role survives the transient outage (no crash)");

    assert_eq!(
        data_loss(&service),
        0,
        "a below-k shortfall driven ONLY by transient unreachability is NOT confirmed data loss \
         — no reconstruction_data_loss page fires (pre-fix this classified Unrepairable → ≥1)"
    );
    assert_eq!(
        gauge(&service, "reconstruction_unreachable"),
        Some(1.0),
        "the chunk is surfaced on the distinct lower-severity unreachable-degraded signal"
    );
    assert_eq!(
        under_replicated(&service),
        Some(0.0),
        "and it is NOT on the repairable-backlog gauge (it cannot be repaired while below k)"
    );
    assert!(
        repair::queued_repairs(&meta)
            .await
            .unwrap()
            .contains(&CHUNK),
        "the obligation stays queued for re-assessment when the servers return"
    );

    // PASS 2 — the nodes RETURN with their intact fragments (servers 1,2 reachable again). The
    // chunk is at full redundancy → the obligation drains and every signal returns to zero.
    let servers_up = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);
    let (live_fleet2, live_topo2, unreachable2) = live_reconstruction_view(&servers_up).await;
    assert!(
        unreachable2.is_empty(),
        "with the nodes back, no server is unreachable this pass"
    );
    let ctx2 = ReconstructionContext {
        meta: &meta,
        fleet: &live_fleet2,
        topology: &live_topo2,
        unreachable: &unreachable2,
    };
    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx2), None, 600)
        .await
        .unwrap();
    assert_eq!(
        data_loss(&service),
        0,
        "no data loss was ever real — the fragments were only transiently unreachable"
    );
    assert_eq!(
        gauge(&service, "reconstruction_unreachable"),
        Some(0.0),
        "the unreachable-degraded signal returns to ZERO once the nodes are back"
    );
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the obligation drained (the chunk is healthy again) — the outage fully recovered"
    );
}

// ---- 2c: a repairable chunk with NO free domain is blocked OFF the backlog gauge ----

/// The binding "returns to ZERO" must hold even for a `Repairable` chunk whose repair can
/// never complete this pass because there is **no free failure domain** to place the rebuilt
/// shard on (a minimal cluster at exactly `n`, one domain killed). Pre-fix the chunk was
/// counted on `reconstruction_under_replicated` and then `repair_chunk`'s domain selector
/// erred, unwinding the pass — so the backlog gauge floored at 1 forever and the day-one signal
/// never returned to zero (iteration-7 MUST-FIX). Post-fix it is diverted to the distinct
/// `reconstruction_repair_blocked` level, the pass survives, and the backlog gauge stays 0.
#[tokio::test]
async fn a_repair_with_no_free_domain_is_blocked_off_the_backlog_gauge() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let (d0, d1, d2) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    // A MINIMAL cluster: exactly n = 3 servers, one per domain A,B,C — NO spare domain.
    let fleet = Fleet {
        servers: vec![(0, d0.as_ref()), (1, d1.as_ref()), (2, d2.as_ref())],
    };
    write_rs_2_1(&meta, &fleet).await; // fragments on 0,1,2 (A,B,C)
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // KILL server 1 (domain B). Survivors on A,C = 2 ≥ k = 2 → repairable in principle, but the
    // only domain left to place the rebuild is B (server 1), which is down — no FREE distinct
    // domain remains. There is no spare server 3 to absorb the rebuild.
    let dead = Arc::new(DeadDServer);
    let servers = vec![
        ConfiguredDServer {
            id: 0,
            failure_domain: "A".to_string(),
            store: dyn_store(&d0),
        },
        ConfiguredDServer {
            id: 1,
            failure_domain: "B".to_string(),
            store: dyn_store(&dead),
        },
        ConfiguredDServer {
            id: 2,
            failure_domain: "C".to_string(),
            store: dyn_store(&d2),
        },
    ];
    let (live_fleet, live_topo, unreachable) = live_reconstruction_view(&servers).await;
    assert_eq!(
        live_fleet.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![0, 2],
        "the killed server 1 is dropped; only A,C remain — no free domain to re-place into"
    );
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &live_fleet,
        topology: &live_topo,
        unreachable: &unreachable,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);

    // PASS 1 — the chunk cannot be placed. Pre-fix: counted under-replicated (gauge → 1) then
    // the repair selector erred and unwound the pass (the `.expect` below would panic). Post-fix
    // the pass SURVIVES and the chunk is routed to the blocked level, off the backlog gauge.
    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .expect("a no-free-domain chunk does not unwind the pass (it is blocked, not fatal)");
    assert_eq!(
        under_replicated(&service),
        Some(0.0),
        "a chunk that cannot be repaired (no free domain) is NOT on the repairable-backlog gauge \
         — so the binding day-one signal is not floored at 1 by an un-completable repair"
    );
    assert_eq!(
        gauge(&service, "reconstruction_repair_blocked"),
        Some(1.0),
        "it is surfaced on the distinct repair-blocked level instead"
    );
    assert_eq!(
        data_loss(&service),
        0,
        "it is repairable in principle (survivors ≥ k) — not a data-loss event"
    );

    // PASS 2 — the condition persists; the backlog gauge must STAY at zero (never floor at 1).
    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 600)
        .await
        .expect("the blocked chunk keeps the pass alive across repeated passes");
    assert_eq!(
        under_replicated(&service),
        Some(0.0),
        "across repeated passes the repairable-backlog gauge stays 0 — it returns to zero \
         instead of pinning at 1 on the un-completable repair (iteration-7 MUST-FIX)"
    );
    assert_eq!(
        gauge(&service, "reconstruction_repair_blocked"),
        Some(1.0),
        "the repair-blocked level still reflects the stuck chunk (it clears when capacity returns)"
    );
}

// ---- 3: the REAL continuous run loop survives a dead D-server and keeps running ----

#[tokio::test]
async fn run_loop_survives_a_dead_dserver_and_keeps_running() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    let fleet = Fleet {
        servers: vec![
            (0, d0.as_ref()),
            (1, d1.as_ref()),
            (2, d2.as_ref()),
            (3, d3.as_ref()),
        ],
    };
    write_rs_2_1(&meta, &fleet).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    let dead = Arc::new(DeadDServer);
    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&dead)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);

    // Drive the REAL production run loop over the fleet that includes the killed node, with
    // a fast interval and a bounded shutdown. The loop must not exit on the kill: it returns
    // Ok(()) only at shutdown, having run passes through the whole window (survival is
    // monotonic — any exit-on-kill would return early with the pass through `?`).
    let shutdown = async { tokio::time::sleep(Duration::from_millis(120)).await };
    let mut now = 500u64;
    let clock = move || {
        now += 1;
        now
    };
    service
        .run_reconstruction_until(
            &zone,
            &custodian,
            &meta,
            &servers,
            servers.len(),
            ExpiredPendingPolicy::Defer,
            Duration::from_millis(10),
            clock,
            shutdown,
        )
        .await
        .expect(
            "the continuous run loop survives the killed D-server and exits cleanly at shutdown",
        );

    // The chunk was repaired around the dead node across the loop's passes; the obligation
    // drained and the gauge settled to 0 on its own export surface.
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the run loop reconstructed the chunk around the killed server"
    );
    assert_eq!(
        under_replicated(&service),
        Some(0.0),
        "the run loop's final pass reads the under-replicated gauge back at 0 through the role"
    );
}

// ---- 3b: the run loop LOGS-AND-CONTINUES on a per-pass store fault (survives it) ----

#[tokio::test]
async fn run_loop_logs_and_continues_on_a_store_fault() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let (d0, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    // Server 1 is REACHABLE (health Ok, so the probe keeps it) but faults every fetch — a
    // server that dies AFTER the probe / a transient metadata blip. Its held fragment makes
    // the reconstruction assessment propagate a transient fault, so each pass returns
    // `ReconcileError::Store`. The run loop must log-and-continue over it, not exit.
    let faulty = Arc::new(FaultyDServer);
    let fleet = Fleet {
        servers: vec![(0, d0.as_ref()), (2, d2.as_ref()), (3, d3.as_ref())],
    };
    write_rs_2_1(&meta, &fleet).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&faulty)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);

    let shutdown = async { tokio::time::sleep(Duration::from_millis(80)).await };
    service
        .run_reconstruction_until(
            &zone,
            &custodian,
            &meta,
            &servers,
            servers.len(),
            ExpiredPendingPolicy::Defer,
            Duration::from_millis(10),
            || 500,
            shutdown,
        )
        .await
        .expect("a per-pass Store fault is logged-and-continued, so the loop exits Ok at shutdown");

    // The fault blocked every repair, so the obligation is STILL queued — the loop kept
    // trying and survived rather than exiting on the fault.
    assert!(
        !repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the store fault blocked repair; the obligation stays queued (the loop survived, not repaired)"
    );
}

// ---- 4: a superseded (fenced) custodian's run loop stops ----

#[tokio::test]
async fn run_loop_stops_when_fenced() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let d0 = Arc::new(MemDServer::default());
    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d0)),
        (2, "C", dyn_store(&d0)),
        (3, "D", dyn_store(&d0)),
    ]);

    let coord = MemCoordination::new();
    // Elect the DEPOSED custodian first (lower term), then a NEWER leader whose higher
    // term is installed as the zone's current fence — so the deposed custodian is fenced.
    let deposed = Custodian::elect(&coord, "zone-reconstruction")
        .await
        .unwrap();
    let mut zone = FencedZone::new();
    zone.install(deposed.leadership());
    let newer = Custodian::elect(&coord, "zone-reconstruction")
        .await
        .unwrap();
    zone.install(newer.leadership());
    assert!(
        newer.term() > deposed.term(),
        "a re-election raises the fencing token"
    );

    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);

    // The deposed custodian's run loop must STOP immediately with Fenced — a superseded
    // leader must not keep acting (it would be an unfenced actor).
    let shutdown = async { std::future::pending::<()>().await };
    let err = service
        .run_reconstruction_until(
            &zone,
            &deposed,
            &meta,
            &servers,
            servers.len(),
            ExpiredPendingPolicy::Defer,
            Duration::from_millis(10),
            || 500,
            shutdown,
        )
        .await
        .expect_err("a fenced custodian's run loop returns an error rather than continuing");
    assert!(
        matches!(err, ReconcileError::Fenced(_)),
        "the deposed custodian is fenced, not merely a transient store fault"
    );
}

// ---- 5: the day-one signal through the REAL binary's backend-open path (redb) ----

/// Drive the signal through `cli::run_reconstruction_over_backend` — the **exact production
/// path `wyrd custodian` runs** (resolve the backend, `open_local_meta_redb`, then the
/// leader-elected loop) — against a **real on-disk redb store the "cluster" wrote to**, not
/// a hand-built in-memory `MemMeta`. This closes the iteration-3/4 rejection: the deployable
/// role must open the *same* metadata store the cluster populated. Were it routed to the
/// wrong plane (an empty local redb where the cluster ran TiKV), it would see zero chunks
/// and the under-replicated gauge would read a permanent healthy zero — undemonstrable on
/// the very deployment (#367) it gates. Here the custodian opens the redb the object was
/// written to, so the loss it finds is real and the gauge moves through the real backend.
///
/// Two invocations capture the binding rise-then-zero shape end to end (each reopens the
/// redb the prior dropped — redb is single-writer): the first pass finds the loss and repairs
/// it (gauge **1**), a fresh run's pass reassesses the now-restored redundancy and drains the
/// obligation (gauge **0**).
#[tokio::test]
async fn gauge_rises_then_returns_to_zero_through_the_redb_backend_open_path() {
    enable_metric_callsites();

    // The data dir `open_local_meta_redb(data_dir)` opens `data_dir/meta.redb` under.
    let dir = tempfile::tempdir().expect("temp data dir");
    let data_dir = dir.path().to_str().expect("utf-8 temp path").to_string();

    // The persistent D-server fragment stores — these outlive the redb reopen (the fragments
    // stay put while the custodian process reopens the metadata plane).
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );

    // The "cluster" writes an RS(2,1) object into the REAL redb store + the D-servers, then
    // drops the writer handle so the custodian can reopen it (redb holds an exclusive lock).
    {
        let meta = RedbMetadataStore::open(dir.path().join("meta.redb")).expect("open redb");
        let fleet = Fleet {
            servers: vec![
                (0, d0.as_ref()),
                (1, d1.as_ref()),
                (2, d2.as_ref()),
                (3, d3.as_ref()),
            ],
        };
        let data = b"reconstruct this erasure-coded chunk, every byte of it".to_vec();
        let topo = four_domains();
        let outcome = write_new_object_placed(
            &meta,
            &fleet,
            ROOT,
            "obj",
            INODE,
            &data,
            data.len(),
            EcScheme::ReedSolomon { k: 2, m: 1 },
            &topo,
            0,
            1_000,
            || CHUNK,
        )
        .await
        .expect("write RS(2,1) into the redb-backed cluster");
        assert_eq!(outcome, CommitOutcome::Committed);
        // KILL D-server 1 (domain B): the architecture §7.4 day-one step-4 fault, enqueued
        // on the shared repair queue the custodian drains.
        repair::enqueue_repair(&meta, CHUNK, "health")
            .await
            .expect("enqueue the repair obligation into redb");
    } // <- redb writer dropped here; the exclusive lock is released for the custodian.

    // The production fleet input, INCLUDING the killed node (server 1 is unreachable), keyed
    // by operator-supplied stable id + real failure domain — never fabricated from index.
    let dead = Arc::new(DeadDServer);
    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&dead)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);
    // A huge interval means each invocation runs EXACTLY ONE pass before its shutdown fires,
    // so the two phases read the gauge at its rise (1) and at its return (0).
    let one_pass = Duration::from_secs(3600);

    // PHASE 1 — the custodian opens the real redb, finds the loss, repairs it: gauge → 1.
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);
    run_reconstruction_over_backend(
        MetadataBackend::Redb,
        &data_dir,
        &service,
        &zone,
        &custodian,
        &servers,
        servers.len(),
        ExpiredPendingPolicy::Defer,
        one_pass,
        || 500,
        async { tokio::time::sleep(Duration::from_millis(60)).await },
    )
    .await
    .expect("the custodian runs over the real redb backend and survives the killed D-server");
    assert_eq!(
        under_replicated(&service),
        Some(1.0),
        "opening the SAME redb the cluster wrote to, the custodian's pass finds the injected \
         loss and the under-replicated gauge RISES to 1 — through the real backend-open path"
    );

    // PHASE 2 — a fresh custodian run reopens the redb (now repaired) and drains: gauge → 0.
    let coord2 = MemCoordination::new();
    let (zone2, custodian2) = elect(&coord2).await;
    let telemetry2 = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service2 = CustodianService::new(telemetry2);
    run_reconstruction_over_backend(
        MetadataBackend::Redb,
        &data_dir,
        &service2,
        &zone2,
        &custodian2,
        &servers,
        servers.len(),
        ExpiredPendingPolicy::Defer,
        one_pass,
        || 700,
        async { tokio::time::sleep(Duration::from_millis(60)).await },
    )
    .await
    .expect("a fresh custodian run reopens the redb backend cleanly");
    assert_eq!(
        under_replicated(&service2),
        Some(0.0),
        "reopening the redb after repair, the reassessment finds full redundancy and the \
         gauge RETURNS TO ZERO — the day-one signal (0010; architecture §7.4 step 4) through \
         the real binary's backend-open path"
    );

    // The obligation drained through the real redb store — proving the repair persisted.
    let meta = RedbMetadataStore::open(dir.path().join("meta.redb")).expect("reopen redb");
    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the reconstruction commit persisted to redb: the repair obligation drained"
    );
}

// ---- 6: the fleet-assembly seam STARTS DEGRADED around a startup-unreachable peer ----

/// Drive the exact production fleet-assembly seam [`connect_fleet`] `cmd_custodian` runs —
/// the glue iterations 3/4 were rejected on (wrong backend / fabricated topology) and the
/// gap iteration-5 BLOCKING #2/#3 flagged: the dial loop + `id`/`failure_domain` mapping was
/// covered by NO test, and the loop propagated the FIRST unreachable endpoint's `Err`, so a
/// custodian **(re)started during the day-one incident** exited on the very fault it exists
/// to repair.
///
/// Here endpoint `e1` (the killed D-server, domain B) is **down at startup** — the
/// [`FakeConnector`] returns `Err` for it, injected THROUGH the connector (not below it, and
/// with no network). `connect_fleet` must (a) reject nothing — topology is aligned — and
/// (b) **start degraded**: skip `e1`, returning the reachable subset `{0,2,3}` rather than
/// propagating its `Err`. The role then repairs *around* the startup-down peer: the
/// under-replicated gauge returns to **0** through a custodian that came up mid-incident.
#[tokio::test]
async fn connect_fleet_starts_degraded_around_a_startup_down_peer_and_repairs() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    let fleet = Fleet {
        servers: vec![
            (0, d0.as_ref()),
            (1, d1.as_ref()),
            (2, d2.as_ref()),
            (3, d3.as_ref()),
        ],
    };
    write_rs_2_1(&meta, &fleet).await;
    // KILL D-server 1 (domain B) — enqueue the day-one repair obligation.
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // `e1` is DOWN AT STARTUP (its store is `None`): the custodian is (re)started during the
    // incident, exactly the BLOCKING #3 case. The other three answer with their real stores.
    let mut stores: HashMap<String, Option<Arc<dyn ChunkStore>>> = HashMap::new();
    stores.insert("e0".into(), Some(dyn_store(&d0)));
    stores.insert("e1".into(), None);
    stores.insert("e2".into(), Some(dyn_store(&d2)));
    stores.insert("e3".into(), Some(dyn_store(&d3)));
    let connector = FakeConnector { stores };

    let endpoints = vec![
        "e0".to_string(),
        "e1".to_string(),
        "e2".to_string(),
        "e3".to_string(),
    ];
    let ids = vec![0u64, 1, 2, 3];
    let domains = vec![
        "A".to_string(),
        "B".to_string(),
        "C".to_string(),
        "D".to_string(),
    ];

    // The EXACT production seam: same `connect_fleet`, same `require_aligned_topology` guard.
    let servers = connect_fleet(
        &connector,
        &endpoints,
        &ids,
        &domains,
        Duration::from_secs(1),
        require_aligned_topology,
    )
    .await
    .expect("connect_fleet starts degraded (does not propagate the startup-down peer's Err)");
    assert_eq!(
        servers.iter().map(|d| d.id).collect::<Vec<_>>(),
        vec![0, 2, 3],
        "the startup-unreachable peer e1 is skipped; the role comes up on the reachable subset"
    );
    assert_eq!(
        servers
            .iter()
            .map(|d| d.failure_domain.as_str())
            .collect::<Vec<_>>(),
        vec!["A", "C", "D"],
        "each surviving server keeps its operator-supplied failure domain (never fabricated)"
    );

    // The degraded role REPAIRS around the startup-down peer: drive the real run loop → gauge 0.
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let service = CustodianService::new(telemetry);
    let shutdown = async { tokio::time::sleep(Duration::from_millis(120)).await };
    let mut now = 500u64;
    let clock = move || {
        now += 1;
        now
    };
    service
        .run_reconstruction_until(
            &zone,
            &custodian,
            &meta,
            &servers,
            // The OPERATOR wired 4 endpoints; `connect_fleet` dropped the startup-down peer, so
            // `servers` holds only 3. GC gates on the operator fleet size (4), so it correctly
            // DEFERS here — a startup-partial fleet must not sweep chunk-wide evidence (#554).
            endpoints.len(),
            ExpiredPendingPolicy::Defer,
            Duration::from_millis(10),
            clock,
            shutdown,
        )
        .await
        .expect("a custodian started DURING the incident repairs around the down peer, exits Ok");

    assert!(
        repair::queued_repairs(&meta).await.unwrap().is_empty(),
        "the degraded custodian reconstructed the chunk around the startup-down peer"
    );
    assert_eq!(
        under_replicated(&service),
        Some(0.0),
        "the gauge returns to ZERO through a custodian that came up mid-incident on the \
         reachable subset — the day-one signal survives a (re)start during the fault (BLOCKING #3)"
    );
}

// ---- 7: the REAL binary entry `cli::cmd_custodian` end to end (iteration-5/6 T5c) ----
//
// The tests above cover the factored halves (`connect_fleet`, `run_reconstruction_over_backend`).
// These two drive the binary's own entry point `cli::cmd_custodian` — arg parse → resolve_backend
// → connect_fleet (with `require_aligned_topology` + the concrete `GrpcDServerConnector` dial) —
// so the glue iterations 3 (wrong backend) and 4 (fabricated topology) were rejected on cannot
// regress behind green gates without a test through the real entry. They are plain `#[test]`s (not
// `#[tokio::test]`): `cmd_custodian` builds its own runtime, so a nested one would panic.

/// A TCP address nothing is listening on: bind an ephemeral port, read it, drop the listener.
/// A gRPC dial to it fails fast with connection-refused — the real `GrpcDServerConnector` path,
/// no live D-server needed.
fn refused_endpoint() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    format!("http://{addr}")
}

/// `cmd_custodian` must REJECT fabricated / misaligned topology at the real entry point: two
/// endpoints but one `--ids` entry is exactly the iteration-4 rejection (positional-id
/// fabrication). The guard `require_aligned_topology` is wired INTO `cmd_custodian` via
/// `connect_fleet`, so the whole binary path errors — not just the helper in isolation.
#[test]
fn cmd_custodian_rejects_misaligned_topology_through_the_real_entry_point() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let data_dir = dir.path().to_str().expect("utf-8 temp path");
    let (e0, e1) = (refused_endpoint(), refused_endpoint());
    let args = vec![
        "--data-dir".to_string(),
        data_dir.to_string(),
        "--metadata-backend".to_string(),
        "redb".to_string(),
        "--endpoints".to_string(),
        format!("{e0},{e1}"),
        // Only ONE id for TWO endpoints → the role must not fabricate the second positionally.
        "--ids".to_string(),
        "7".to_string(),
        "--failure-domains".to_string(),
        "rack-1,rack-2".to_string(),
        "--connect-timeout-secs".to_string(),
        "1".to_string(),
    ];
    let err = wyrd_server::cli::cmd_custodian(&args).expect_err(
        "cmd_custodian must reject misaligned --ids/--endpoints at the real entry point (iter-4)",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("--ids") && msg.contains("--endpoints"),
        "the rejection names the misaligned topology (not a fabricated fallback); got: {msg}"
    );
}

/// `cmd_custodian` with an ALIGNED topology but EVERY D-server unreachable at startup must
/// **FAIL LOUD** (panic → non-zero exit + diagnostic), NOT exit 0 and vanish (iteration-7
/// MUST-FIX §6.5). A long-running deployable custodian that returned `Ok(())` on a total fleet
/// outage / a bad `--endpoints` would not be restarted by its supervisor, and the operator
/// would see a clean disappearance. The per-peer "start degraded around ONE down server" policy
/// is unchanged (covered by the `connect_fleet` test above) — only the ALL-unreachable case
/// fails loud. Exercises: arg parse → resolve_backend (redb, opens the real on-disk `meta.redb`
/// under the temp dir) → connect_fleet (every dial refused → empty fleet) → the fail-loud panic.
#[test]
#[should_panic(expected = "NO configured D server")]
fn cmd_custodian_fails_loud_when_the_whole_fleet_is_unreachable_at_startup() {
    let dir = tempfile::tempdir().expect("temp data dir");
    let data_dir = dir.path().to_str().expect("utf-8 temp path");
    let endpoint = refused_endpoint();
    let args = vec![
        "--data-dir".to_string(),
        data_dir.to_string(),
        "--metadata-backend".to_string(),
        "redb".to_string(),
        "--endpoints".to_string(),
        endpoint,
        "--ids".to_string(),
        "1".to_string(),
        "--failure-domains".to_string(),
        "rack-1".to_string(),
        "--connect-timeout-secs".to_string(),
        "1".to_string(),
    ];
    // Every endpoint is a refused dial, so connect_fleet yields an EMPTY fleet — and the role
    // must PANIC (fail loud) rather than exit Ok. The whole entry path runs first: parse →
    // backend (opens the real redb) → connect_fleet (all dials refused) → empty-fleet panic.
    // Pre-fix (an `Ok(())` early return) this returned cleanly and the `#[should_panic]` guard
    // is unsatisfied — RED; post-fix it panics — GREEN.
    let _ = wyrd_server::cli::cmd_custodian(&args);
}

// ---- 6: the audit plane reaches a log sink (#527, proposal 0010 items 1 + 3) ----

/// A `MakeWriter` that appends everything the subscriber writes into a shared buffer, so
/// the test can assert on **what the role actually emitted** rather than trusting that it
/// emitted anything.
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

/// **The keystone.** A real reconciliation pass over a store carrying an un-reconstructable
/// chunk must land its `action = "data-loss"` audit line — chunk id and all — in a log sink.
///
/// This is the same fixture as
/// [`a_loss_beyond_tolerance_raises_data_loss_and_the_backlog_gauge_returns_to_zero`], driven
/// through the same production `reconcile_pass`; the only difference is that the role is
/// built with [`CustodianService::with_logging_to`] (what `wyrd custodian` now uses) instead
/// of [`CustodianService::new`], and the sink is captured.
///
/// Pre-fix the role's scoped dispatch was `Registry + MetricsLayer` and nothing else, so
/// `emit_data_loss`'s `tracing::error!` — the loudest alarm the system can raise, *"DATA IS
/// LOST; NEEDS-HUMAN"* — was written into a subscriber with no log layer and discarded. The
/// operator learned only that an unlabelled counter had ticked, and **only** if `--otlp-endpoint`
/// was set. The buffer was empty; this test is RED. Post-fix the line is there, with the chunk
/// named, on stderr, with no collector infrastructure at all.
///
/// Mutation guard: swap `with_logging_to` back to `new` and this test fails — the assertion
/// cannot pass vacuously, because an empty buffer contains none of the substrings.
#[tokio::test]
async fn the_data_loss_audit_line_reaches_the_log_sink_naming_the_chunk() {
    enable_metric_callsites();
    const CHUNK_LOST: ChunkId = 0x0D00_D000;
    const INODE_LOST: InodeId = 2;
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    let fleet = Fleet {
        servers: vec![
            (0, d0.as_ref()),
            (1, d1.as_ref()),
            (2, d2.as_ref()),
            (3, d3.as_ref()),
        ],
    };
    write_rs_2_1_as(&meta, &fleet, INODE_LOST, "lost", CHUNK_LOST).await;

    // Two of the three RS(2,1) fragments destroyed → below k = 2 → un-reconstructable.
    d0.delete_fragment(FragmentId {
        chunk: CHUNK_LOST,
        index: 0,
    })
    .await
    .unwrap();
    d2.delete_fragment(FragmentId {
        chunk: CHUNK_LOST,
        index: 2,
    })
    .await
    .unwrap();
    repair::enqueue_repair(&meta, CHUNK_LOST, "health")
        .await
        .unwrap();

    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&d1)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);
    let (live_fleet, live_topo, unreachable) = live_reconstruction_view(&servers).await;
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &live_fleet,
        topology: &live_topo,
        unreachable: &unreachable,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let capture = Capture::default();
    // The production constructor the `wyrd custodian` role uses: metrics bridge AND log sink
    // on ONE dispatch. With `CustodianService::new` (metrics only) the buffer stays empty.
    let service = CustodianService::with_logging_to(
        telemetry,
        &wyrd_server::logging::LogConfig::new(None, None).unwrap(),
        capture.clone(),
    );

    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .expect("the pass completes; the chunk is simply unrecoverable");

    // The metric still fires — the bridge is not displaced by the log layers.
    assert!(
        data_loss(&service) >= 1,
        "the metrics bridge must survive composition with the log layers"
    );

    let logged = capture.contents();
    assert!(
        logged.contains(r#""action":"data-loss""#),
        "the data-loss audit line must reach the sink; pre-fix the buffer is EMPTY. got: {logged}"
    );
    assert!(
        logged.contains(&format!("{CHUNK_LOST:032x}")),
        "the audit line must NAME the lost chunk — a bare counter tells an operator nothing \
         about WHICH data is gone. got: {logged}"
    );
    assert!(
        logged.contains(r#""target":"wyrd.custodian.reconstruction.audit""#),
        "the audit target must survive to the sink so a collector can select on it. got: {logged}"
    );
}

/// **Lowering log verbosity must not switch off the durability metrics.** (#531 review, P1.)
///
/// The durability plane emits its metrics as `tracing::info!` events
/// (`gauge.reconstruction_under_replicated`, the repair counters). The log subscriber's
/// `EnvFilter` was originally attached to the *registry*, making it a **subscriber-wide**
/// filter — which short-circuits `register_callsite`/`enabled` for the whole stack, so a
/// filtered-out event never reaches **any** layer, `MetricsLayer` included.
///
/// So `wyrd custodian --log-level warn` — an ordinary thing for an operator to do, and the
/// obvious thing to do when JSON logs get noisy — silently starved the metrics bridge and
/// turned off the Prometheus/OTLP durability signals. No error, no warning, no metric. The
/// operator would have been watching a gauge that could never rise, in the one plane that
/// tells them data is being lost.
///
/// The filter is now attached to the fmt layer only (`Layer::with_filter`). This test runs the
/// REAL reconcile pass at `--log-level error` — two levels below the `info` the metrics are
/// emitted at — and asserts the gauge still rises and the audit line is still logged.
///
/// Pre-fix: `under_replicated` is `None` (the registry is empty) — RED.
#[tokio::test]
async fn a_raised_log_level_does_not_starve_the_durability_metrics() {
    enable_metric_callsites();
    let meta = MemMeta::default();
    let (d0, d1, d2, d3) = (
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
        Arc::new(MemDServer::default()),
    );
    let fleet = Fleet {
        servers: vec![
            (0, d0.as_ref()),
            (1, d1.as_ref()),
            (2, d2.as_ref()),
            (3, d3.as_ref()),
        ],
    };
    write_rs_2_1(&meta, &fleet).await;
    repair::enqueue_repair(&meta, CHUNK, "health")
        .await
        .unwrap();

    // Server 1 is dead: a real, repairable under-replication.
    let dead = Arc::new(DeadDServer);
    let servers = configured([
        (0, "A", dyn_store(&d0)),
        (1, "B", dyn_store(&dead)),
        (2, "C", dyn_store(&d2)),
        (3, "D", dyn_store(&d3)),
    ]);
    let (live_fleet, live_topo, unreachable) = live_reconstruction_view(&servers).await;
    let ctx = ReconstructionContext {
        meta: &meta,
        fleet: &live_fleet,
        topology: &live_topo,
        unreachable: &unreachable,
    };

    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let telemetry = DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap();
    let capture = Capture::default();
    // `error` — two levels ABOVE the `info` the durability metrics are emitted at. Pre-fix this
    // is exactly the configuration that silently killed them.
    let service = CustodianService::with_logging_to(
        telemetry,
        &wyrd_server::logging::LogConfig::new(Some("error"), None).unwrap(),
        capture.clone(),
    );

    service
        .reconcile_pass(&zone, &custodian, None, None, Some(&ctx), None, 500)
        .await
        .expect("the pass survives the killed D-server");

    assert_eq!(
        under_replicated(&service),
        Some(1.0),
        "the durability gauge MUST still rise at --log-level error; pre-fix the subscriber-wide \
         EnvFilter starved the MetricsLayer and this reads None — an operator lowering log \
         verbosity silently lost the signal that data is at risk"
    );

    // And the log plane is genuinely quiet at `error` — the filter still does its job on the
    // layer it belongs to. (`info`-level metric events emit no log line of their own.)
    let logged = capture.contents();
    assert!(
        !logged.contains(r#""level":"INFO""#),
        "the fmt layer must still honour --log-level error; the fix scopes the filter, it does \
         not remove it. got: {logged}"
    );
}

// ---- #531 review (P2): installing logging must never take the program down ----

/// **A second in-process `cli::run` must still run the command.**
///
/// `cli::run` is public and in-process callable — that is the module's stated premise ("the
/// command logic lives in the library so it is unit-testable"). Installing the log subscriber
/// used to treat an already-installed global as a hard error, so the SECOND call returned
/// `ExitCode(2)` — *before dispatching the command at all*:
///
/// ```text
/// wyrd: could not install the log subscriber: a global default trace dispatcher has already been set
/// PROBE first=ExitCode(0)  second=ExitCode(2)
/// ```
///
/// Logging refusing to initialise took the whole program down with it — a spectacular
/// inversion for a diagnostics feature, and it would equally have broken any embedder that
/// installed its own subscriber first. An already-present subscriber is an ordinary state, not
/// a fault.
///
/// Pre-fix this is RED on the second call.
#[test]
fn running_the_cli_twice_in_process_still_dispatches_the_command() {
    let argv = || ["wyrd".to_string(), "demo".to_string()].into_iter();

    let first = wyrd_server::cli::run(argv());
    let second = wyrd_server::cli::run(argv());

    // `demo` is the zero-setup in-memory round trip: it must succeed both times.
    assert_eq!(
        format!("{second:?}"),
        format!("{first:?}"),
        "the second in-process `cli::run` must behave exactly like the first; pre-fix it exits \
         2 without ever dispatching `demo`, because the subscriber from the first call is \
         already installed"
    );
    assert_eq!(
        format!("{first:?}"),
        format!("{:?}", std::process::ExitCode::SUCCESS),
        "`wyrd demo` succeeds — so the assertion above is comparing two SUCCESSES, not two \
         identical failures"
    );
}

/// The other half, which the fix must not trade away: a **malformed** `--log-level` is still
/// fatal. Silently running mute because of a typo is the exact failure mode this feature exists
/// to end, so it is a genuine operator error — unlike an already-installed subscriber.
#[test]
fn a_malformed_log_level_still_fails_the_process_loudly() {
    let code = wyrd_server::cli::run(
        [
            "wyrd".to_string(),
            "demo".to_string(),
            "--log-level".to_string(),
            "==nonsense==".to_string(),
        ]
        .into_iter(),
    );
    assert_eq!(
        format!("{code:?}"),
        format!("{:?}", std::process::ExitCode::from(2)),
        "a typo in --log-level must stop the process, not degrade it to silence"
    );
}
