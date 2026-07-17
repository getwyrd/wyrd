//! Issue #576 / proposal 0010 §"Scope boundary" item 7: the d-server role answers the
//! standard gRPC health-checking protocol (`grpc.health.v1.Health/Check`), with
//! readiness reflecting the backing `ChunkStore`'s own `health()` — fail-closed on
//! `Err(_)` — rather than a supervisor's only signal being process existence.
//!
//! Three success criteria, each its own test:
//! (a) `Check` reports SERVING while the store is healthy;
//! (b) `Check` reports NOT_SERVING within a bounded wait once the store goes
//!     `Health::Unhealthy` **or** once `health()` returns `Err` (fail-closed — both
//!     asserted);
//! (c) the health check still answers (not shed with `RESOURCE_EXHAUSTED`) while the
//!     data plane is saturated at its admission bound.
//! Plus the default-invocation pin: the EMPTY-service `Check` — what `grpcurl` /
//! `grpc_health_probe` send when no service is named — tracks the store exactly like
//! the named service ([`the_default_empty_service_check_tracks_the_store`]; Codex P1
//! on #587: tonic-health defaults "" to SERVING, so an unmirrored empty status would
//! report ready forever).
//!
//! **The probe is dialed on the OPERATOR-CONFIGURED health address** — the same
//! `with_health_bind(..)` knob the `wyrd d-server --health-bind ADDR` flag plumbs
//! (`cli.rs`) — a stable, discoverable address, *not* an OS-assigned ephemeral port
//! read back through an in-process getter. Each test reserves a concrete loopback
//! address, configures the server to bind the probe there, and dials exactly that
//! address (the address it configured), so the deployment boundary a real supervisor
//! crosses is what is exercised.
//!
//! Driven in-process over real loopback gRPC (the same shape as
//! `crates/chunkstore-grpc/tests/round_trip.rs`), against the real `DServer::serve`
//! composition — not a stand-in.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tonic::server::NamedService;
use tonic_health::pb::health_check_response::ServingStatus as WireServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, GrpcChunkStore};
use wyrd_coordination_mem::MemCoordination;
use wyrd_server::dserver::{AdmissionControl, DServer, DSERVER_GROUP};
use wyrd_traits::{ChunkId, ChunkStore, FragmentId, Health, Result};

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

fn fs_store() -> (FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    (store, dir)
}

/// Reserve a concrete loopback address by binding an ephemeral port, reading it back,
/// and dropping the listener — so the test can hand the server a **known, configured**
/// health-bind address (the operator's `--health-bind ADDR`) rather than discovering an
/// OS-assigned port after the fact. A standard "reserve a free port" pattern; the tiny
/// re-bind window is tolerated (loopback, single test process).
async fn reserve_addr() -> SocketAddr {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve an ephemeral loopback port");
    let addr = probe.local_addr().expect("read reserved addr");
    drop(probe);
    addr
}

/// What [`ControllableStore::health`] reports — set by the test at will, read by the
/// production readiness-refresh task inside `DServer::serve`.
#[derive(Clone, Copy, Debug)]
enum HealthMode {
    Healthy,
    Unhealthy,
    /// `health()` itself fails — the fail-closed case (Design, §"Mapping":
    /// "`Err(_)` from `health()` ⇒ NOT_SERVING").
    Erroring,
}

/// A `ChunkStore` whose `health()` the test controls at runtime, and whose
/// `get_fragment` can optionally **gate** (announce entry via `entered`, then park on
/// `gate`) so a data-plane request can be made to hold its admission slot for as long
/// as criterion (c) needs. `put`/`list`/`delete` always delegate straight through.
struct ControllableStore {
    inner: FsChunkStore,
    health: Arc<Mutex<HealthMode>>,
    entered: Option<mpsc::UnboundedSender<()>>,
    gate: Option<Arc<Semaphore>>,
}

#[async_trait]
impl ChunkStore for ControllableStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        if let Some(entered) = &self.entered {
            let _ = entered.send(());
        }
        if let Some(gate) = &self.gate {
            let _permit = gate.acquire().await.expect("gate not closed");
        }
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        match *self.health.lock().unwrap() {
            HealthMode::Healthy => Ok(Health::Healthy),
            HealthMode::Unhealthy => Ok(Health::Unhealthy),
            HealthMode::Erroring => Err("store cannot report its own health".into()),
        }
    }
}

/// The named `grpc.health.v1` service the readiness status is ALSO keyed on — the
/// `ChunkStoreServer`'s own registered name. `DServer::serve` publishes the same
/// store-derived status on this name AND the empty-name overall service (the default
/// probe invocation) from one reading, so they can never disagree
/// ([`the_default_empty_service_check_tracks_the_store`] pins the empty name).
/// `ChunkStoreServer<T>`'s `NamedService::NAME` does not depend on `T`, so any
/// instantiation gives the same constant.
fn readiness_service_name() -> &'static str {
    <ChunkStoreServer<()> as NamedService>::NAME
}

/// Bind, register, and serve one D server over `store` with the given admission posture
/// and health-refresh cadence, binding the health probe on the **caller-supplied,
/// configured** `health_bind` address. Return its data endpoint, a shutdown trigger,
/// and the serve task. The health endpoint the test dials is `health_bind` itself —
/// the configured address, not a getter read-back.
async fn serve_controllable(
    store: ControllableStore,
    admission: AdmissionControl,
    health_bind: SocketAddr,
    health_refresh_interval: Duration,
) -> (
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let coord = Arc::new(MemCoordination::new());
    let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind")
        .with_admission_control(admission)
        .with_health_bind(health_bind)
        .with_health_refresh_interval(health_refresh_interval);
    // The server binds the probe exactly where we configured it — assert the knob is
    // honoured before serving, so the address we dial below is unambiguously the
    // configured one.
    assert_eq!(
        server.health_bind(),
        Some(health_bind),
        "the server binds the health probe on the configured address",
    );
    let endpoint = server.endpoint().to_string();
    let lease = server
        .register(&*coord, DSERVER_GROUP, Duration::from_secs(3600))
        .await
        .expect("register");
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(
        server.serve(coord, lease, Duration::from_secs(3600), async move {
            let _ = rx.await;
        }),
    );
    (endpoint, tx, handle)
}

/// Dial `endpoint`'s `grpc.health.v1.Health` service, retrying the TCP connect within a
/// bounded budget — the probe listener is bound at `serve` time (a background task), so
/// the very first dial can race ahead of it. A real supervisor's probe likewise retries
/// until the socket is up; the retry is not masking a missing bind.
async fn health_client(endpoint: &str) -> HealthClient<tonic::transport::Channel> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let endpoint =
        tonic::transport::Endpoint::try_from(endpoint.to_string()).expect("valid endpoint");
    loop {
        match endpoint.connect().await {
            Ok(channel) => return HealthClient::new(channel),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("connect to the configured health endpoint within budget: {e}");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

/// Poll `Check` for `service` until it reports `expected`, bounded by `budget` — the
/// "within a bounded wait" criterion (a)/(b) name, without coupling the test to the
/// production refresh cadence's exact timing.
async fn wait_for_check(
    client: &mut HealthClient<tonic::transport::Channel>,
    service: &str,
    expected: WireServingStatus,
    budget: Duration,
) -> HealthCheckResponse {
    tokio::time::timeout(budget, async {
        loop {
            let resp = client
                .check(HealthCheckRequest {
                    service: service.to_string(),
                })
                .await
                .expect("Check RPC succeeds (the health service is registered)")
                .into_inner();
            if resp.status == expected as i32 {
                return resp;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("service {service:?} did not reach {expected:?} within {budget:?}"))
}

/// Success criterion (a): `Check` reports SERVING while the backing store's `health()`
/// is `Health::Healthy`, dialed on the operator-configured health address.
///
/// Red pre-fix: `main` registers no `grpc.health.v1.Health` service, and there is no
/// `with_health_bind` / configurable probe address at all — so this fails to compile
/// (dependency + methods absent), which the gate counts as red (Falsifiability).
#[tokio::test]
async fn check_reports_serving_while_the_store_is_healthy() {
    let (store, _dir) = fs_store();
    let health = Arc::new(Mutex::new(HealthMode::Healthy));
    let controllable = ControllableStore {
        inner: store,
        health,
        entered: None,
        gate: None,
    };
    let health_bind = reserve_addr().await;
    let (_endpoint, shutdown, handle) = serve_controllable(
        controllable,
        AdmissionControl::default(),
        health_bind,
        Duration::from_millis(20),
    )
    .await;

    // Dial the CONFIGURED address (not a getter read-back of an ephemeral port).
    let mut client = health_client(&format!("http://{health_bind}")).await;
    let resp = wait_for_check(
        &mut client,
        readiness_service_name(),
        WireServingStatus::Serving,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        resp.status,
        WireServingStatus::Serving as i32,
        "a healthy store reads SERVING",
    );

    let _ = shutdown.send(());
    let _ = handle.await;
}

/// Success criterion (b): `Check` reports NOT_SERVING within a bounded wait once the
/// store reports `Health::Unhealthy` **or** once `health()` returns `Err` — both
/// asserted (fail-closed), dialed on the operator-configured health address.
#[tokio::test]
async fn check_reports_not_serving_once_unhealthy_or_erroring() {
    let (store, _dir) = fs_store();
    let health = Arc::new(Mutex::new(HealthMode::Healthy));
    let controllable = ControllableStore {
        inner: store,
        health: Arc::clone(&health),
        entered: None,
        gate: None,
    };
    let health_bind = reserve_addr().await;
    let (_endpoint, shutdown, handle) = serve_controllable(
        controllable,
        AdmissionControl::default(),
        health_bind,
        Duration::from_millis(20),
    )
    .await;
    let mut client = health_client(&format!("http://{health_bind}")).await;
    let name = readiness_service_name();

    // Baseline: converges to SERVING once the refresher's first read lands.
    wait_for_check(
        &mut client,
        name,
        WireServingStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    // Half (a): an `Health::Unhealthy` store flips readiness to NOT_SERVING.
    *health.lock().unwrap() = HealthMode::Unhealthy;
    wait_for_check(
        &mut client,
        name,
        WireServingStatus::NotServing,
        Duration::from_secs(5),
    )
    .await;

    // Recover, confirming the flip is not one-directional...
    *health.lock().unwrap() = HealthMode::Healthy;
    wait_for_check(
        &mut client,
        name,
        WireServingStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    // ...then half (b): `health()` itself erroring ALSO flips readiness to NOT_SERVING
    // — the fail-closed case (a store that cannot even report its health must not read
    // as ready).
    *health.lock().unwrap() = HealthMode::Erroring;
    wait_for_check(
        &mut client,
        name,
        WireServingStatus::NotServing,
        Duration::from_secs(5),
    )
    .await;

    let _ = shutdown.send(());
    let _ = handle.await;
}

/// The DEFAULT probe invocation — a plain `Health/Check` with the EMPTY `service`
/// field, what `grpcurl … grpc.health.v1.Health/Check` and `grpc_health_probe -addr …`
/// send when no service is named, and what a generic supervisor dials — must track the
/// store exactly like the named readiness service. tonic-health defaults the empty-name
/// status to SERVING, so publishing only the named status would leave the documented
/// invocation reporting ready forever, however unhealthy the store (Codex P1 on #587).
/// Also pins the fail-closed flip and recovery through the empty name.
#[tokio::test]
async fn the_default_empty_service_check_tracks_the_store() {
    let (store, _dir) = fs_store();
    let health = Arc::new(Mutex::new(HealthMode::Healthy));
    let controllable = ControllableStore {
        inner: store,
        health: Arc::clone(&health),
        entered: None,
        gate: None,
    };
    let health_bind = reserve_addr().await;
    let (_endpoint, shutdown, handle) = serve_controllable(
        controllable,
        AdmissionControl::default(),
        health_bind,
        Duration::from_millis(20),
    )
    .await;
    let mut client = health_client(&format!("http://{health_bind}")).await;

    // The empty service name — the request the documented invocations actually send.
    wait_for_check(
        &mut client,
        "",
        WireServingStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    // An unhealthy store must flip the DEFAULT invocation too — this is the probe a
    // supervisor acts on; red on the pre-fix build (the "" status stayed SERVING).
    *health.lock().unwrap() = HealthMode::Unhealthy;
    wait_for_check(
        &mut client,
        "",
        WireServingStatus::NotServing,
        Duration::from_secs(5),
    )
    .await;

    // And it recovers, in step with the named service (one reading, two keys).
    *health.lock().unwrap() = HealthMode::Healthy;
    wait_for_check(
        &mut client,
        "",
        WireServingStatus::Serving,
        Duration::from_secs(5),
    )
    .await;
    wait_for_check(
        &mut client,
        readiness_service_name(),
        WireServingStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    let _ = shutdown.send(());
    let _ = handle.await;
}

/// A connected streaming `Health/Watch` client must not pin shutdown: tonic's graceful
/// shutdown waits for in-flight RPCs, and a Watch stream stays open until its CLIENT
/// hangs up — unbounded, that let one watcher hold the whole role past SIGTERM,
/// blocking lease revocation (Codex P1 on #587). The role now bounds the probe
/// surface's drain (`HEALTH_SHUTDOWN_GRACE`) and aborts a pinned stream. Red pre-fix:
/// `serve` never returns while the watcher stays connected, so this test times out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connected_watch_stream_does_not_pin_shutdown() {
    let (store, _dir) = fs_store();
    let health = Arc::new(Mutex::new(HealthMode::Healthy));
    let controllable = ControllableStore {
        inner: store,
        health,
        entered: None,
        gate: None,
    };
    let health_bind = reserve_addr().await;
    let (_endpoint, shutdown, handle) = serve_controllable(
        controllable,
        AdmissionControl::default(),
        health_bind,
        Duration::from_millis(20),
    )
    .await;
    let mut client = health_client(&format!("http://{health_bind}")).await;

    // Open the standard streaming Watch and receive its first update, proving the
    // stream is genuinely established and in flight — and KEEP it open.
    let mut watch = client
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("Watch RPC establishes")
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(5), watch.message())
        .await
        .expect("first Watch update arrives")
        .expect("stream healthy");
    assert!(first.is_some(), "Watch delivers an initial status");

    // Order shutdown WITH the watcher still connected. The role must complete within
    // the data drain + the bounded probe grace — never hang on the pinned stream.
    let _ = shutdown.send(());
    let joined = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect(
            "serve() returns after shutdown despite a connected Health/Watch stream \
             (an unbounded probe drain would pin the role past SIGTERM)",
        );
    joined
        .expect("serve task joins")
        .expect("serve exits cleanly at shutdown");
}

/// A zero refresh interval must refuse at CONFIGURATION time: `tokio::time::interval`
/// panics on a zero period, and inside the spawned refresher that panic would silently
/// kill the refresh loop — readiness stuck at fail-closed NOT_SERVING while the data
/// plane keeps serving (Codex P2 on #587). The builder is where the misconfiguration
/// exists, so the builder is where it surfaces.
#[tokio::test]
#[should_panic(expected = "health refresh interval must be non-zero")]
async fn a_zero_refresh_interval_refuses_at_the_builder() {
    let (store, _dir) = fs_store();
    let _ = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind")
        .with_health_refresh_interval(Duration::ZERO);
}

/// Success criterion (c): the health check still answers — rather than being shed with
/// `RESOURCE_EXHAUSTED` — while the data plane is saturated at its admission bound
/// (`max_concurrent_requests` held by an in-flight data RPC), dialed on the configured
/// health address.
///
/// Green requires the health service to be composed genuinely outside the admission
/// stack (Design, §"Overload policy"): a health probe wired INSIDE the same
/// admission-layered builder would be shed exactly like a data request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_check_answers_while_the_data_plane_is_saturated() {
    let (store, _dir) = fs_store();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0)); // closed: the held request parks until released
    let health = Arc::new(Mutex::new(HealthMode::Healthy));
    let controllable = ControllableStore {
        inner: store,
        health,
        entered: Some(entered_tx),
        gate: Some(gate.clone()),
    };
    // Server-wide admission limit of 1 — the same shape as
    // `crates/server/tests/dserver.rs`'s
    // `overload_across_connections_sheds_excess_with_a_retryable_status`, adapted here
    // to prove the HEALTH check is exempt from it, not that the data plane sheds.
    let admission = AdmissionControl {
        max_concurrent_requests: 1,
        ..AdmissionControl::default()
    };
    let health_bind = reserve_addr().await;
    let (endpoint, shutdown, handle) = serve_controllable(
        controllable,
        admission,
        health_bind,
        Duration::from_millis(20),
    )
    .await;

    let mut client = health_client(&format!("http://{health_bind}")).await;
    let name = readiness_service_name();
    // Converge to SERVING first, so the later assertion is unambiguous: a NOT_SERVING
    // read would also (trivially) not be RESOURCE_EXHAUSTED, which would not actually
    // prove the bypass.
    wait_for_check(
        &mut client,
        name,
        WireServingStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    // Saturate the one server-wide admission slot with a held data-plane request.
    let data_client = GrpcChunkStore::connect(endpoint)
        .await
        .expect("connect data client");
    let id = fid(0x5_1ED, 0);
    let frag = fragment(
        id,
        b"a fragment that never gets read while the slot is held",
    );
    data_client
        .put_fragment(id, frag)
        .await
        .expect("seed the fragment");
    let admitted = tokio::spawn(async move { data_client.get_fragment(id).await });
    entered_rx
        .recv()
        .await
        .expect("the data request is admitted and holds the one admission slot");

    // The health check must still answer — promptly, and with a real serving status,
    // not RESOURCE_EXHAUSTED — while that slot is held.
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        client.check(HealthCheckRequest {
            service: name.to_string(),
        }),
    )
    .await
    .expect(
        "the health check must be answered within the budget, not left to queue or be shed \
         behind the saturated data-plane admission bound",
    );
    let resp = outcome
        .expect("the health check succeeds (not shed with RESOURCE_EXHAUSTED)")
        .into_inner();
    assert_eq!(
        resp.status,
        WireServingStatus::Serving as i32,
        "the store is still healthy throughout — overload is not unreadiness (Design, \
         §\"Overload policy\")",
    );

    gate.add_permits(8);
    let _ = admitted.await;
    let _ = shutdown.send(());
    let _ = handle.await;
}
