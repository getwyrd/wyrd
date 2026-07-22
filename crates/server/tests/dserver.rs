//! M2.3 definition of done (issue #113): the `d-server` role hosts an injected
//! `ChunkStore` over gRPC and registers its endpoint for discovery through the
//! L5 `Coordination` seam.
//!
//! Two tiers, matching how the rest of the system is proven:
//! - a **deterministic** lease test (a `ManualClock`-driven coordinator) shows a
//!   registration renews and a lapsed one drops out of discovery — no wall-clock;
//! - an **in-process integration** test stands up two real D servers over
//!   loopback gRPC, discovers them, resolves the fan-out endpoint set, and proves
//!   a *discovered* endpoint actually serves a fragment round-trip.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tonic::Code;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{GrpcChunkStore, TransportError};
use wyrd_coordination_mem::MemCoordination;
use wyrd_server::dserver::{
    discover_endpoints, select_fanout, AdmissionControl, DServer, DSERVER_GROUP,
};
use wyrd_testkit::ManualClock;
use wyrd_traits::{ChunkId, ChunkStore, Coordination, FragmentId, Health, Result};

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

/// DoD: leased registration renews; an expired (stale) registration drops out of
/// discovery. Driven by a `ManualClock` so the lifecycle is deterministic — no
/// real time, no flakiness.
#[tokio::test]
async fn lease_renews_and_lapses_deterministically() {
    let clock = ManualClock::new(0);
    let coord = MemCoordination::with_clock(clock.clone());
    let (store, _dir) = fs_store();

    let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let ttl = Duration::from_secs(30);
    let lease = server.register(&coord, DSERVER_GROUP, ttl).await.unwrap();

    // Registered now → discoverable.
    assert_eq!(
        discover_endpoints(&coord, DSERVER_GROUP).await.unwrap(),
        vec![server.endpoint().to_string()],
    );

    // Renew before expiry (at t=20s), then advance to t=40s: re-stamped to expire
    // at t=50s, so still discoverable.
    clock.advance(20_000);
    coord.renew(lease).await.unwrap();
    clock.advance(20_000);
    assert_eq!(
        discover_endpoints(&coord, DSERVER_GROUP)
            .await
            .unwrap()
            .len(),
        1,
        "a renewed registration stays discoverable past its original TTL",
    );

    // Advance past the renewed expiry without renewing → drops out.
    clock.advance(20_000);
    assert!(
        discover_endpoints(&coord, DSERVER_GROUP)
            .await
            .unwrap()
            .is_empty(),
        "a lapsed registration drops out of discovery",
    );
}

/// DoD: `wyrd d-server` hosts `FsChunkStore` over gRPC; a registered D server is
/// discovered via `Coordination::discover`, and the gateway resolves the set of
/// endpoints a chunk's `n` fragments fan out to. Proven in-process over real
/// loopback gRPC with two D servers sharing one coordinator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_servers_register_serve_and_are_discovered() {
    let coord = Arc::new(MemCoordination::new());
    let ttl = Duration::from_secs(3600);
    let renew = Duration::from_secs(1);

    // Two D servers, each over its own filesystem store, bound to ephemeral ports.
    let (store0, _d0) = fs_store();
    let (store1, _d1) = fs_store();
    let s0 = DServer::bind(store0, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let s1 = DServer::bind(store1, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    // Register before serving, so discovery is race-free.
    let l0 = s0.register(&*coord, DSERVER_GROUP, ttl).await.unwrap();
    let l1 = s1.register(&*coord, DSERVER_GROUP, ttl).await.unwrap();

    // Discovery returns both distinct endpoints.
    let mut endpoints = discover_endpoints(&*coord, DSERVER_GROUP).await.unwrap();
    endpoints.sort();
    assert_eq!(endpoints.len(), 2, "both D servers are discovered");
    assert_ne!(endpoints[0], endpoints[1], "endpoints are distinct");

    // The gateway resolves the endpoint set for an rs(6,3) chunk's 9 fragments —
    // best-effort distinct, cycling over the two known D servers.
    let fanout = select_fanout(&endpoints, 9);
    assert_eq!(fanout.len(), 9, "one endpoint chosen per fragment");
    assert!(
        fanout.iter().all(|e| endpoints.contains(e)),
        "every chosen endpoint is a discovered one",
    );
    assert!(
        endpoints.iter().all(|e| fanout.contains(e)),
        "fan-out spreads across the distinct D servers",
    );

    // Start serving both.
    let (tx0, rx0) = oneshot::channel();
    let (tx1, rx1) = oneshot::channel();
    let h0 = tokio::spawn(s0.serve(coord.clone(), l0, renew, async move {
        let _ = rx0.await;
    }));
    let h1 = tokio::spawn(s1.serve(coord.clone(), l1, renew, async move {
        let _ = rx1.await;
    }));

    // A client dialing a *discovered* endpoint round-trips a fragment.
    let client = GrpcChunkStore::connect(endpoints[0].clone()).await.unwrap();
    let id = fid(0xabc_def, 4);
    let frag = fragment(id, b"a fragment to a discovered D server");
    client.put_fragment(id, frag.clone()).await.unwrap();
    assert_eq!(
        client.get_fragment(id).await.unwrap().as_deref(),
        Some(frag.as_ref()),
        "the discovered endpoint serves the fragment byte-identical",
    );

    // Clean shutdown revokes the leases, so discovery converges to empty.
    tx0.send(()).unwrap();
    tx1.send(()).unwrap();
    h0.await.unwrap().unwrap();
    h1.await.unwrap().unwrap();
    assert!(
        discover_endpoints(&*coord, DSERVER_GROUP)
            .await
            .unwrap()
            .is_empty(),
        "a cleanly stopped D server withdraws its registration",
    );
}

/// A `ChunkStore` whose `get_fragment` **gates**: it signals (via `entered`) that a
/// request has been admitted into the handler, then blocks on a [`Semaphore`]
/// (`gate`) until the test releases it. So an admitted request can be made to hold
/// its admission slot for as long as the test needs, while *excess* concurrent
/// requests pile up against the server's configured admission limit.
/// `put`/`list`/`delete`/`health` delegate straight through.
struct GateStore {
    inner: FsChunkStore,
    entered: mpsc::UnboundedSender<()>,
    gate: Arc<Semaphore>,
}

#[async_trait]
impl ChunkStore for GateStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        // Announce that this request was admitted into the handler (it holds an
        // admission slot from here), then park until the test opens the gate.
        let _ = self.entered.send(());
        let _permit = self.gate.acquire().await.expect("gate not closed");
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

/// Bind, register, and serve one D server over `store` with the given admission
/// posture; return its endpoint, a shutdown trigger, and the serve task.
async fn serve_gated(
    store: GateStore,
    admission: AdmissionControl,
) -> (
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let coord = Arc::new(MemCoordination::new());
    let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind")
        .with_admission_control(admission);
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

/// The [`TransportError`] carrying the wire `Status`, found anywhere in `err`'s source
/// chain.
///
/// The `GrpcChunkStore` boxes it directly for a terminal status, and *underneath* a
/// `wyrd_traits::TransientFault` for a known-transient one — the seam class wraps the
/// backend's error rather than replacing it (#577), so both the class and the transport
/// detail survive. Both shed statuses this file asserts on (`RESOURCE_EXHAUSTED` /
/// `UNAVAILABLE`, and the request-timeout cut's `CANCELLED` / `DEADLINE_EXCEEDED`) are
/// transient, so this walks the chain — the same way `wyrd_traits::is_integrity_fault`
/// finds a wrapped fault — rather than downcasting only the outermost error.
fn transport_error(err: &wyrd_traits::BoxError) -> &TransportError {
    let mut next: Option<&(dyn std::error::Error + 'static)> = Some(err.as_ref());
    while let Some(e) = next {
        if let Some(te) = e.downcast_ref::<TransportError>() {
            return te;
        }
        next = e.source();
    }
    panic!("a transport failure carries a TransportError in its chain; got: {err}");
}

/// Pull the gRPC [`tonic::Code`] out of a `ChunkStore` boxed error.
fn status_code(err: &wyrd_traits::BoxError) -> Code {
    match transport_error(err) {
        TransportError::Unavailable(s) | TransportError::Timeout(s) | TransportError::Rpc(s) => {
            s.code()
        }
        TransportError::Connect(e) => panic!("expected a gRPC status, got a connect error: {e}"),
    }
}

/// Success criterion (issue #205, architecture §8.9): offered more concurrent
/// requests than its configured admission limit, the d-server **sheds** the excess
/// with a retryable "busy" status instead of admitting it unboundedly.
///
/// The binding bound is **server-wide**, so this drives the overload across
/// **separate connections**: with a global admission limit of 1, one request on
/// connection A is admitted and made to hold the single slot (parked in the gate),
/// then a request on a *different* connection B — which has its own fresh
/// per-connection budget — must still come back promptly with a retryable
/// `RESOURCE_EXHAUSTED` / `UNAVAILABLE` status. That is what a per-connection-only
/// limit cannot do: B would get its own slot and be admitted. Only a shared,
/// server-wide semaphore sheds B.
///
/// Red pre-fix: the bare `Server::builder()` set **no** admission limit, so B was
/// admitted and queued behind A's held slot — it never gets a shed status, so the
/// bounded wait below elapses and the assertion fails. Green post-fix: the shared
/// server-wide limit + load-shed reject it immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_across_connections_sheds_excess_with_a_retryable_status() {
    let (store, _dir) = fs_store();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0)); // closed: handlers park until released
    let gate_store = GateStore {
        inner: store,
        entered: entered_tx,
        gate: gate.clone(),
    };

    // Server-wide admission limit of 1; a long request timeout so the *timeout*
    // never fires during this test — the only thing that can answer the excess
    // request is the shed path. A roomy per-connection cap proves the shed comes
    // from the SERVER-WIDE bound, not the per-connection one.
    let admission = AdmissionControl {
        max_concurrent_requests: 1,
        max_concurrent_requests_per_connection: 64,
        request_timeout: Duration::from_secs(60),
        ..AdmissionControl::default()
    };
    let (endpoint, shutdown, handle) = serve_gated(gate_store, admission).await;

    // Two SEPARATE clients = two separate connections, so a per-connection-only
    // limit would give each its own slot. Only a server-wide bound sheds the second.
    let client_a = GrpcChunkStore::connect(endpoint.clone())
        .await
        .expect("connect A");
    let client_b = GrpcChunkStore::connect(endpoint).await.expect("connect B");
    let id = fid(0x5_1ED, 0);

    // Request on connection A: admitted, enters the handler, holds the one slot.
    let admitted = tokio::spawn(async move { client_a.get_fragment(id).await });
    entered_rx
        .recv()
        .await
        .expect("the admitted request reaches the handler and holds the slot");

    // Request on connection B: it is over the SERVER-WIDE limit. Bound the wait so a
    // pre-fix server (no limit, so it just queues) fails the test instead of hanging.
    let excess = tokio::time::timeout(Duration::from_secs(5), client_b.get_fragment(id)).await;

    let answered = excess.expect(
        "the excess request on a SECOND connection must be answered (shed) within the budget, \
         not left to queue behind the slot held on the first connection — a server-wide \
         admission bound sheds it; a per-connection-only or unbounded server does not",
    );
    let err = answered.expect_err("an over-limit request is shed, not served a value");
    let code = status_code(&err);
    assert!(
        matches!(code, Code::ResourceExhausted | Code::Unavailable),
        "the excess request is shed with a retryable busy signal \
         (resource-exhausted / unavailable), got {code:?}",
    );

    // Release the held slot so the admitted request completes, then shut down.
    gate.add_permits(8);
    let _ = admitted.await;
    let _ = shutdown.send(());
    let _ = handle.await;
}

/// Success criterion (issue #205): a handler that hangs past the configured request
/// timeout is **cut** with a deadline status rather than pinning its admission slot
/// forever.
///
/// A single request whose handler never returns (the gate is never opened) must,
/// once the request timeout elapses, come back with a deadline status. tonic 0.14
/// surfaces a server-side handler timeout as `CANCELLED` (verified against
/// `tonic-0.14.6` `status.rs`); the client may also observe `DEADLINE_EXCEEDED`.
///
/// Red pre-fix: the bare `Server::builder()` set **no** request timeout, so the hung
/// handler is never cut and the bounded wait below elapses. Green post-fix: the
/// configured timeout cuts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hung_handler_is_cut_by_the_request_timeout() {
    let (store, _dir) = fs_store();
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0)); // never opened: the handler hangs
    let gate_store = GateStore {
        inner: store,
        entered: entered_tx,
        gate: gate.clone(),
    };

    // A short request timeout; a wide admission limit so nothing is *shed* — the
    // only thing that can answer is the deadline cut.
    let admission = AdmissionControl {
        max_concurrent_requests: 64,
        request_timeout: Duration::from_millis(200),
        ..AdmissionControl::default()
    };
    let (endpoint, shutdown, handle) = serve_gated(gate_store, admission).await;

    let client = GrpcChunkStore::connect(endpoint).await.expect("connect");
    let id = fid(0xDEAD, 0);

    let outcome = tokio::time::timeout(Duration::from_secs(5), client.get_fragment(id)).await;
    // The handler did start (it parked in the gate) — proves the timeout cut a
    // genuinely in-flight request, not one that never reached the handler.
    entered_rx
        .recv()
        .await
        .expect("the request reached the handler before being cut");

    let answered = outcome.expect(
        "a hung handler must be cut by the request timeout within the budget, not pin its slot \
         forever — a server with no request timeout never cuts it",
    );
    let err = answered.expect_err("a timed-out request returns an error status, not a value");
    let code = status_code(&err);
    assert!(
        matches!(code, Code::Cancelled | Code::DeadlineExceeded),
        "a handler past the request timeout is cut with a deadline status, got {code:?}",
    );

    gate.add_permits(8);
    let _ = shutdown.send(());
    let _ = handle.await;
}
