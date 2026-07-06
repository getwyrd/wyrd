//! Issue #455: **close the write→durability loop** — join the gateway's write path to
//! the custodian's repair path over ONE shared cluster metadata store.
//!
//! `s3_gateway_cluster.rs` (#454) proves a gateway S3 PUT fans chunks to real loopback
//! D-servers and a GET reads them back; `custodian_day_one.rs` (#450) proves the
//! under-replicated backlog gauge rises-then-returns-to-zero through the deployable
//! custodian role — but it **hand-writes** the object and **hand-enqueues** the repair
//! obligation (`wyrd_core::repair::enqueue_repair`), never via a gateway PUT + a real
//! repair scan. Neither test proves the other's halves compose: a custodian opened over a
//! store nothing wrote — or one whose repair obligations nothing *derived* from the
//! placement — would see zero repair work, the exact "empty store" symptom the issue names.
//!
//! This test drives the join over real loopback gRPC D-servers and ONE shared redb store,
//! and the custodian **DERIVES** the repair obligation from the placement the gateway PUT
//! recorded — it is never hand-enqueued:
//!
//! 1. a **real gateway S3 PUT** (`wyrd_server::Gateway::put_object`,
//!    `crates/server/src/lib.rs:147` — the SAME composition core `serve_s3_role`/`cmd_s3`
//!    drive, `crates/server/src/cli.rs:1207`; the directly-held `Gateway` wiring the brief's
//!    peer citation names, mirroring `crates/server/tests/e2e.rs:18-24`) writes an RS(2,1)
//!    object over the shared `data_dir/meta.redb`, fanning its fragments across real,
//!    networked loopback D-servers
//!    (`wyrd_chunkstore_grpc::FanoutChunkStore<GrpcChunkStore>`, `connect_fanout`,
//!    `crates/server/src/cli.rs:1118`);
//! 2. the redb writer handle is DROPPED (releasing redb's exclusive OS lock,
//!    `crates/server/src/cli.rs:1005-1009`) before the custodian reopens the SAME file;
//! 3. **D-server 1 (domain B) suffers a durable data loss** — the fragment the gateway
//!    placed on it is deleted over gRPC, the disk-returned-empty loss that scrub's issue-#330
//!    detection exists to catch. The server stays reachable, so the custodian's repair SCAN
//!    can observe the placed-but-absent fragment and DERIVE the obligation from the
//!    gateway-written placement (a wholly process-dead peer is dropped by the reachability
//!    probe and needs the out-of-scope desired-state detector, `custodian.rs:54-60`, to
//!    derive its obligations; the scrub-detectable loss is the honest in-process realization);
//! 4. the deployable custodian role (`run_reconstruction_over_backend`,
//!    `crates/server/src/cli.rs:758` — the exact production backend-open path `wyrd custodian`
//!    runs) opens that SAME store and, in ONE pass, **scrubs** the live fleet to DERIVE the
//!    obligation from the placement then **reconstructs** it — the under-replicated gauge
//!    reads **1** (the load-bearing join assertion); a fresh pass shows the gauge returned to
//!    **0** and the obligation drained; and
//! 5. a fresh gateway GET over the SAME store reads the object back **byte-identical**,
//!    reconstructed from the D-server fragments (including the re-placed one).
//!
//! The load-bearing property is that the custodian's obligation count DERIVES from the
//! gateway-written placement + the observed loss — not from a hand-authored queue entry.
//! Two negations flip the `under_replicated == 1.0` assertion RED, proving the derivation:
//! reverting the deployable role's scrub wiring (`custodian.rs run_reconstruction_until`)
//! leaves the reconstruction queue empty → gauge 0; and dropping the fragment loss (step 3)
//! leaves scrub with nothing to enqueue → gauge 0. Either is the "empty store" symptom.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, GrpcChunkStore};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, repair};
use wyrd_custodian::{Custodian, FencedZone};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::cli::{
    connect_fanout, open_cluster_meta, require_aligned_topology, run_reconstruction_over_backend,
    MetadataBackend,
};
use wyrd_server::custodian::{connect_fleet, CustodianService, DServerConnector};
use wyrd_server::Gateway;
use wyrd_telemetry::{DurabilityTelemetry, ExporterConfig};
use wyrd_traits::{BoxError, ChunkStore, FragmentId};

const KEY: &str = "closed-loop/object";

/// Stand up one D-server service over a fresh `FsChunkStore`, bound to an ephemeral
/// loopback port, and return its dialable endpoint. Identical to the helper
/// `s3_gateway_cluster.rs` and `gateway_cluster.rs` already use: real tonic transport,
/// real HTTP/2 framing, no in-memory double.
async fn spawn_dserver() -> (String, tempfile::TempDir, tokio::task::JoinHandle<()>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    let service = ChunkStoreService::new(store);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChunkStoreServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });

    (format!("http://{addr}"), dir, server)
}

/// The custodian's real gRPC dial seam, mirroring `cli.rs`'s private `GrpcDServerConnector`
/// (`crates/server/src/cli.rs:838-852`) — the one concrete-transport call `connect_fleet`
/// makes. Re-declared here (that type is not exported) so this test drives `connect_fleet`
/// over REAL gRPC, not an in-memory fake.
struct RealDServerConnector;

#[async_trait]
impl DServerConnector for RealDServerConnector {
    async fn connect(
        &self,
        endpoint: &str,
        timeout: Duration,
    ) -> Result<Arc<dyn ChunkStore>, BoxError> {
        let client = GrpcChunkStore::connect_with_timeout(endpoint.to_string(), timeout).await?;
        Ok(Arc::new(client) as Arc<dyn ChunkStore>)
    }
}

/// Install a permissive global `tracing` default **once**, exactly as
/// `custodian_day_one.rs::enable_metric_callsites` does, so the durability metric
/// callsites never latch `Interest::never` under the parallel test harness.
fn enable_metric_callsites() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
    });
}

async fn elect(coord: &MemCoordination) -> (FencedZone, Custodian) {
    let leader = Custodian::elect(coord, "zone-reconstruction")
        .await
        .unwrap();
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());
    (zone, leader)
}

/// The value of the named **gauge** read back off the Prometheus surface — the last
/// matching sample. Identical to `custodian_day_one.rs::gauge_value`.
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

/// One custodian pass over the shared redb, through the exact production backend-open path
/// `wyrd custodian` runs (`run_reconstruction_over_backend`). A huge interval means the loop
/// runs EXACTLY ONE pass before its short shutdown fires, so the caller reads the gauge at a
/// single, well-defined moment. Each call reopens the redb the prior dropped (redb is
/// single-writer), so two calls capture the rise (1) then the return (0).
async fn one_custodian_pass(
    data_dir: &str,
    configured: &[wyrd_server::custodian::ConfiguredDServer],
    clock: u64,
) -> CustodianService {
    let coord = MemCoordination::new();
    let (zone, custodian) = elect(&coord).await;
    let service =
        CustodianService::new(DurabilityTelemetry::new(ExporterConfig::Prometheus).unwrap());
    run_reconstruction_over_backend(
        MetadataBackend::Redb,
        data_dir,
        &service,
        &zone,
        &custodian,
        configured,
        Duration::from_secs(3600),
        move || clock,
        async { tokio::time::sleep(Duration::from_millis(60)).await },
    )
    .await
    .expect("the custodian runs one pass over the real redb backend");
    service
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_written_object_is_a_custodian_derived_repair_obligation_and_round_trips() {
    enable_metric_callsites();

    // Four real, networked loopback D-servers. The RS(2,1) write's identity placement
    // (fragment index i -> D-server i) lands the 3 fragments on servers 0,1,2 (domains
    // A,B,C); server 3 (domain D) is the spare capacity a repair can rebuild into — the same
    // spare-domain shape `custodian_day_one.rs`'s `four_domains()` uses.
    let (endpoint_a, dir_a, server_a) = spawn_dserver().await;
    let (endpoint_b, dir_b, server_b) = spawn_dserver().await;
    let (endpoint_c, dir_c, server_c) = spawn_dserver().await;
    let (endpoint_d, dir_d, server_d) = spawn_dserver().await;
    let endpoints = vec![
        endpoint_a.clone(),
        endpoint_b.clone(),
        endpoint_c.clone(),
        endpoint_d.clone(),
    ];

    // The SHARED cluster metadata store: the custodian reopens this SAME `data_dir/meta.redb`
    // file the gateway PUT commits to.
    let data_dir = tempfile::tempdir().expect("data dir");
    let data_dir_path = data_dir.path().to_str().unwrap().to_string();

    let object: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();

    // ---- 1. A REAL gateway S3 PUT over the shared store + loopback D-servers ----
    {
        let write_fanout = connect_fanout(&endpoints)
            .await
            .expect("connect the gateway's write fan-out (all four D-servers alive)");
        // Scoped so the redb writer handle is DROPPED before the custodian reopens the file.
        let meta = open_cluster_meta(&data_dir_path).expect("open the shared cluster redb");
        let gateway = Gateway::new(meta, write_fanout, MemCoordination::new())
            .with_durability(EcScheme::ReedSolomon { k: 2, m: 1 });
        gateway
            .recover()
            .await
            .expect("recover id allocators (fresh store: a no-op)");
        gateway
            .put_object(KEY, &object)
            .await
            .expect("the gateway S3 PUT must be accepted");
    } // <- gateway (and its redb handle) dropped here; the exclusive lock is released.

    // Resolve the committed chunk id and assert the gateway recorded the identity placement
    // [0,1,2] over the SHARED store — the DServerId space the custodian's fleet is configured
    // over. This is the placement contract ADR-0008 promises; the custodian reads exactly it.
    let chunk_id = {
        let meta = RedbMetadataStore::open(data_dir.path().join("meta.redb"))
            .expect("reopen the shared redb (the gateway's writer handle was dropped)");
        let inode_id = read::resolve(&meta, 0, KEY)
            .await
            .expect("resolve")
            .expect("the gateway PUT committed the object");
        let inode = read::read_inode(&meta, inode_id)
            .await
            .expect("read inode")
            .expect("inode present");
        assert_eq!(
            inode.chunk_map.len(),
            1,
            "the payload fits in one chunk at the gateway's default chunk size"
        );
        assert_eq!(
            inode.chunk_map[0].placement,
            vec![0u64, 1, 2],
            "the gateway's identity placement must land the RS(2,1) fragments on D-servers \
             0,1,2 (domains A,B,C) — the SAME DServerId space the custodian's fleet is \
             configured over; the shared placement contract the repair scan reads"
        );
        inode.chunk_map[0].id
    }; // <- redb handle dropped again; released for the custodian below.

    // ---- 2. D-server 1 (domain B) loses the object's fragment (the day-one durability
    //         fault). Delete it over gRPC; the server STAYS reachable so the custodian's
    //         repair scan (scrub) can observe the placed-but-absent fragment and DERIVE the
    //         obligation from the gateway-written placement — never a hand-enqueue. ----
    {
        let client_b = GrpcChunkStore::connect(endpoint_b.clone())
            .await
            .expect("dial D-server 1 to model its data loss");
        let lost = FragmentId {
            chunk: chunk_id,
            index: 1,
        };
        client_b
            .delete_fragment(lost)
            .await
            .expect("lose the fragment the gateway placed on D-server 1");
        assert!(
            client_b
                .get_fragment(lost)
                .await
                .expect("query D-server 1")
                .is_none(),
            "D-server 1 no longer holds the object's fragment — a real, scrub-detectable loss"
        );
    }

    // The custodian's fleet: all four configured D-servers, dialed through the exact
    // production fleet-assembly seam (`connect_fleet` + `require_aligned_topology`,
    // `crates/server/src/custodian.rs:143`, `crates/server/src/cli.rs:807`) with
    // operator-supplied ids/domains — never fabricated from endpoint order.
    let ids = vec![0u64, 1, 2, 3];
    let domains = vec![
        "A".to_string(),
        "B".to_string(),
        "C".to_string(),
        "D".to_string(),
    ];
    let configured = connect_fleet(
        &RealDServerConnector,
        &endpoints,
        &ids,
        &domains,
        Duration::from_secs(2),
        require_aligned_topology,
    )
    .await
    .expect("connect_fleet dials the reachable fleet");
    assert_eq!(
        configured.iter().map(|d| d.id).collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "all four D-servers are reachable; the role comes up over the whole fleet"
    );

    // ---- 3. PASS 1 — the deployable custodian, opened over the SAME store the gateway PUT
    //         wrote, DERIVES the loss from the placement (scrub) and computes it as a
    //         NON-ZERO repair obligation (reconstruction), all in ONE pass: the load-bearing
    //         join assertion. Were the deployable role NOT wired to scrub (or the write and
    //         repair paths disagreeing on placement), it would run reconstruction over an
    //         EMPTY queue and read the object as fully healthy (0 obligations) — the "empty
    //         store" symptom the issue names — even though a D-server actually lost data. ----
    let service1 = one_custodian_pass(&data_dir_path, &configured, 500).await;
    assert_eq!(
        under_replicated(&service1),
        Some(1.0),
        "the custodian DERIVES the loss from the gateway-written placement (its repair scan) \
         and the under-replicated gauge reads 1 after the D-server data loss — the \
         write→durability loop closes over ONE shared store"
    );

    // ---- 4. PASS 2 — a fresh custodian instance reopens the (now-repaired) store and
    //         reassesses: the gauge must RETURN TO ZERO and the obligation must be drained. ----
    let service2 = one_custodian_pass(&data_dir_path, &configured, 700).await;
    assert_eq!(
        under_replicated(&service2),
        Some(0.0),
        "once the custodian repairs the gateway-written object the backlog gauge returns to \
         zero — the day-one signal, through a gateway-written object over a shared store"
    );
    {
        let meta = RedbMetadataStore::open(data_dir.path().join("meta.redb"))
            .expect("reopen redb to confirm the repair persisted");
        assert!(
            !repair::queued_repairs(&meta)
                .await
                .unwrap()
                .contains(&chunk_id),
            "the repair obligation the custodian derived was drained by the reconstruction commit"
        );
    }

    // ---- 5. A fresh gateway GET, over the SAME store, reads the object back BYTE-IDENTICAL,
    //         reconstructed from the D-server fragments (including the re-placed one). ----
    {
        let read_fanout = connect_fanout(&endpoints)
            .await
            .expect("connect the gateway's read fan-out");
        let meta = RedbMetadataStore::open(data_dir.path().join("meta.redb"))
            .expect("reopen redb for the GET");
        let gateway = Gateway::new(meta, read_fanout, MemCoordination::new());
        gateway.recover().await.expect("recover id allocators");
        let got = gateway
            .get_object(KEY)
            .await
            .expect("GET must reconstruct the object from the D-server fragments")
            .expect("the object exists");
        assert_eq!(
            got, object,
            "the S3 object must round-trip byte-identical, reconstructed from the D-server \
             fragments — surviving the D-server data loss via the custodian's derived repair"
        );
    }

    server_a.abort();
    server_b.abort();
    server_c.abort();
    server_d.abort();
    drop((dir_a, dir_b, dir_c, dir_d));
}
