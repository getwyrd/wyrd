//! The networked storage role (Milestone 2, proposal 0004 step 3): a `d-server`
//! that hosts a local [`ChunkStore`] over the gRPC `ChunkStore` service and
//! registers its endpoint for discovery through the L5 `Coordination` seam.
//!
//! Discovery goes **only through `Coordination`** (`register` / `discover` under
//! a group key — never an orchestrator API, ADR-0010), generalizing the
//! gateway's node group: a D server announces a dialable endpoint, renews a lease
//! on it, and a client resolves the set via `discover`. The in-memory
//! coordination concrete serves the in-process / DST profile; real etcd-backed
//! dynamic discovery is a later composition swap behind the same trait (ADR-0006),
//! so a D server in a *separate* process is only mutually discoverable once etcd
//! (or static, configured endpoints) backs the seam — not with process-local
//! in-memory coordination.

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::server::NamedService;
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::load_shed::error::Overloaded;
use tower::load_shed::LoadShedLayer;
use tower::{Layer, Service};
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService};
use wyrd_core::placement::Topology;
use wyrd_traits::{BoxError, ChunkStore, Coordination, DServerId, Health, Lease, Result};

/// The discovery group under which D servers register their gRPC endpoints.
pub const DSERVER_GROUP: &str = "chunkstore";

/// The default opaque failure-domain label a D server reports when none is
/// configured — a single-domain zone (the M2 best-effort posture). Real
/// deployments set a per-server rack / power / switch label (architecture §7.3).
pub const DEFAULT_FAILURE_DOMAIN: &str = "default";

/// Default **server-wide** admission limit: the maximum number of concurrent
/// in-flight requests the whole d-server admits — across *all* connections —
/// before it **sheds** the excess (architecture §8.9). 64 is a moderate,
/// SSD-leaning middle ground; see [`AdmissionControl`] for tuning it to the
/// backing device's useful queue depth (shallower for an HDD spindle).
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 64;

/// Default per-connection admission limit: a secondary, transport-level cap on the
/// concurrent in-flight requests a *single* connection may hold. This bounds the
/// fan-in one client can impose, but it is **not** the server-wide bound — that is
/// [`DEFAULT_MAX_CONCURRENT_REQUESTS`], the shared limit that actually fails the
/// server closed under a many-connection overload.
pub const DEFAULT_MAX_CONCURRENT_REQUESTS_PER_CONNECTION: usize = 64;

/// Default request-handler timeout: the hard ceiling on the wall-clock time one
/// request may run before it is cut with a deadline status, so a stuck handler
/// never pins an admission slot forever.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Default per-connection HTTP/2 inbound-stream cap — bounds the request fan-in a
/// single connection can open at the transport layer (the implicit `h2` default
/// leaves it effectively unbounded).
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 256;

/// Default HTTP/2 server keepalive ping interval — reclaims admission slots
/// stranded behind a silently dead peer.
pub const DEFAULT_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// Default cadence at which the readiness-refresh task re-reads the store's
/// [`ChunkStore::health`] and republishes the `grpc.health.v1.Health` readiness
/// status (proposal 0010 §"Scope boundary" item 7). An operator-visible constant
/// rather than "whenever" (a few seconds is a reasonable readiness staleness bound);
/// tighten it per deployment with
/// [`with_health_refresh_interval`](DServer::with_health_refresh_interval).
pub const DEFAULT_HEALTH_REFRESH_INTERVAL: Duration = Duration::from_secs(3);

/// How long the probe surface may drain after the data plane has finished its own
/// graceful shutdown, before it is aborted. A streaming `Health/Watch` RPC stays open
/// until its CLIENT hangs up, and tonic's graceful shutdown waits for in-flight RPCs —
/// so an unbounded wait would let one connected watcher pin the whole role past
/// SIGTERM, blocking lease revocation (Codex P1 on #587). Probe streams carry no user
/// data; cutting them at shutdown is the designed outcome. Generous enough for every
/// unary `Check` in flight to answer.
pub const HEALTH_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// The probe surface's OWN admission bound: concurrent in-flight health RPCs admitted
/// across all probe connections before the excess is shed. The probe transport is
/// deliberately outside the DATA plane's admission budget (a probe must answer through
/// an overloaded data plane), but "outside that budget" must not mean UNBOUNDED — on a
/// publicly reachable probe bind, unauthenticated clients could otherwise open
/// arbitrarily many RPCs until probe traffic starves the process (Codex P1 on #587).
/// Sized for supervisors, not workloads: a handful of probers at a few RPCs each.
pub const HEALTH_MAX_CONCURRENT_REQUESTS: usize = 16;

/// Per-connection HTTP/2 stream cap on the probe surface — bounds the `Health/Watch`
/// fan-in a single prober connection can hold open (the h2 default is effectively
/// unbounded). Watch streams are long-lived by design; a supervisor needs one or two.
pub const HEALTH_MAX_CONCURRENT_STREAMS: u32 = 16;

/// Hard ceiling on producing one health RPC's response HEAD — the unary `Check`, and
/// the head of a `Watch` stream (an established Watch stream then lives on unaffected,
/// its permit freed when the client closes it). Applied inside
/// [`HealthConcurrencyLayer`] around head production only, so a handler that stalls
/// before its head cannot hold its concurrency permit forever and shed legitimate
/// probes (Codex P2 on #587) — deliberately not tonic's builder `.timeout()`, which
/// can also cut a long-lived Watch stream.
pub const HEALTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Default **stable** address the `grpc.health.v1.Health` probe surface binds
/// (proposal 0010 §"Scope boundary" item 7) — a fixed, documented port beside the
/// data plane's default (`127.0.0.1:50051`), so a deployment supervisor has a
/// *known* address to dial rather than an ephemeral one it cannot discover. An
/// operator overrides it with [`with_health_bind`](DServer::with_health_bind) (the
/// `wyrd d-server --health-bind ADDR` flag). It is a **separate** address from the
/// data listener because the probe surface must answer *outside* the data-plane
/// admission layers (see [`DServer::serve`]).
///
/// The loopback IP here matches the library/data-plane default posture only; the
/// deployable `wyrd d-server` role does NOT use this address verbatim — with no
/// `--health-bind` it derives the probe bind from `--bind`'s OWN interface at this
/// constant's port (`cli.rs` `default_health_bind`), so a container binding data on
/// `0.0.0.0` never gets a probe hidden on container loopback where no supervisor,
/// port-forward, or k8s gRPC probe can reach it (Codex P1 on #587).
pub const DEFAULT_HEALTH_BIND: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 50052));

/// Admission-control and backpressure configuration for the d-server's gRPC
/// transport — the knobs that make the request-admission path **fail closed under
/// pressure** (architecture §8.9,
/// `docs/design/architecture/08-crosscutting-concepts.md:98-107`) instead of
/// degrading into unbounded contention and thread-pool exhaustion.
///
/// **The binding bound is server-wide.** Beyond
/// [`max_concurrent_requests`](Self::max_concurrent_requests) admitted *across all
/// connections combined* the server sheds the excess with a retryable
/// `RESOURCE_EXHAUSTED` "busy" signal, so an overloaded client is told to back off
/// and retry rather than having its requests queue without bound and contend for
/// runtime threads. This is enforced with one shared semaphore cloned across every
/// connection (see [`DServer::serve`]) — a *per-connection* limit alone does not
/// fail the server closed, because aggregate in-flight would still grow without
/// bound in the number of connections.
/// [`max_concurrent_requests_per_connection`](Self::max_concurrent_requests_per_connection)
/// is a secondary, per-connection cap; [`request_timeout`](Self::request_timeout)
/// bounds the work a single request can pin, so a hung handler is cut loose with a
/// deadline status rather than holding an admission slot forever.
///
/// **Tuning to the device's useful queue depth.** The server-wide admission limit
/// should track how many concurrent I/Os the backing store serves *usefully*: a
/// single HDD spindle saturates at a shallow queue (tune it *down*, e.g. 8–16),
/// while an SSD/NVMe device sustains a much deeper queue (tune it *up*, e.g. 256+).
/// The [`Default`] is a moderate middle ground
/// ([`DEFAULT_MAX_CONCURRENT_REQUESTS`]) — it is **not** a fixed constant;
/// operators set it per deployment.
#[derive(Debug, Clone)]
pub struct AdmissionControl {
    /// Maximum concurrent in-flight requests admitted **server-wide** (across all
    /// connections); beyond it the excess is shed with a retryable
    /// `RESOURCE_EXHAUSTED` status. This is the operator-tunable bound that fails
    /// the server closed under pressure.
    pub max_concurrent_requests: usize,
    /// Maximum concurrent in-flight requests admitted **per connection** — a
    /// secondary transport-level cap on a single client's fan-in. The server-wide
    /// [`max_concurrent_requests`](Self::max_concurrent_requests) is the binding
    /// bound; this only stops one connection from monopolising the budget.
    pub max_concurrent_requests_per_connection: usize,
    /// Hard per-request timeout: a handler that runs longer is cut with a deadline
    /// status, freeing its admission slot.
    pub request_timeout: Duration,
    /// Maximum concurrent HTTP/2 inbound streams per connection — caps the
    /// transport-level request fan-in.
    pub max_concurrent_streams: u32,
    /// Disable Nagle's algorithm: gRPC frames are small and latency-sensitive, so
    /// coalescing them adds delay for no throughput gain.
    pub tcp_nodelay: bool,
    /// HTTP/2 server keepalive ping interval; `None` disables keepalive.
    pub http2_keepalive_interval: Option<Duration>,
}

impl Default for AdmissionControl {
    fn default() -> Self {
        Self {
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            max_concurrent_requests_per_connection: DEFAULT_MAX_CONCURRENT_REQUESTS_PER_CONNECTION,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            tcp_nodelay: true,
            http2_keepalive_interval: Some(DEFAULT_HTTP2_KEEPALIVE_INTERVAL),
        }
    }
}

// ---- the capacity plane (observability floor, proposal 0010 §"Scope boundary" item 5) ----
//
// The admission stack above decides, per request, whether the server takes the work. Until
// now it did so **silently**: an over-limit request was shed with a `RESOURCE_EXHAUSTED` the
// CLIENT saw and the server never recorded, so "are we shedding load?" — the first question
// of any overload post-mortem — was answerable only from client-side evidence, if anyone had
// kept it. These types make each admission decision an event on the shared telemetry seam.
//
// **Emission only.** Nothing here changes what is admitted, shed, or cut: every layer
// forwards its inner service's outcome unaltered, and the `Server::builder()` options that
// set the actual policy are untouched. The observers are *positioned* around the existing
// stack rather than replacing any part of it.

/// The capacity plane's shared state: where its metric events go, and the live count of
/// admitted-and-not-yet-finished requests.
///
/// Cloned into every layer and (by tonic) per connection, so the in-flight count MUST be
/// shared — an `Arc<AtomicI64>` — or each connection would report only its own share of a
/// bound that is explicitly server-wide.
#[derive(Clone)]
struct CapacityPlane {
    /// The role's metrics sink (`DurabilityTelemetry::metrics_dispatch()`, composed at the
    /// `wyrd d-server` role entry). `None` ⇒ the ambient subscriber: the emission is
    /// unconditional, only the SINK is a role's choice, so a library caller / an existing
    /// test that never wires telemetry is unaffected.
    dispatch: Option<tracing::Dispatch>,
    /// The in-flight level. A `Mutex`, not an atomic, because the level and its *emission*
    /// must move together — see [`CapacityPlane::record_in_flight`].
    in_flight: Arc<Mutex<i64>>,
}

impl CapacityPlane {
    fn new(dispatch: Option<tracing::Dispatch>) -> Self {
        Self {
            dispatch,
            in_flight: Arc::new(Mutex::new(0)),
        }
    }

    /// Emit `f`'s metric events into the role's sink. Entering a dispatch is a thread-local
    /// set for the closure's duration, so it is sound from any task — which is why the sink
    /// is carried here rather than scoped around `serve`: tonic spawns a task per connection
    /// (`tonic-0.14.6` `src/transport/server/mod.rs:925`) and a spawned task does not
    /// inherit a scoped dispatch.
    fn emit(&self, f: impl FnOnce()) {
        match &self.dispatch {
            Some(dispatch) => tracing::dispatcher::with_default(dispatch, f),
            None => f(),
        }
    }

    /// Raise every capacity series at **zero** before the server accepts anything, so a
    /// dashboard reads "0 shed" rather than "no data" on a healthy server — the two are
    /// indistinguishable for a counter that only appears once it first fires (the same
    /// argument #577's `ErrorClass::ALL` makes for the request plane's class labels).
    fn preregister(&self) {
        self.emit(|| {
            tracing::info!(monotonic_counter.capacity_requests_admitted = 0_u64);
            tracing::info!(monotonic_counter.capacity_requests_shed = 0_u64);
            tracing::info!(monotonic_counter.capacity_requests_timed_out = 0_u64);
            tracing::info!(monotonic_counter.capacity_requests_cancelled = 0_u64);
            tracing::info!(gauge.capacity_requests_in_flight = 0_i64);
        });
    }

    /// Move the in-flight level by `delta`, run `also`, and report the new level — all
    /// **atomically with respect to other emitters**.
    ///
    /// The lock spans the *emission*, not merely the arithmetic, and that is the whole point.
    /// With a bare atomic the read and the record are two steps: two requests finishing
    /// concurrently compute their levels (1, then 0) and can then emit them in the OPPOSITE
    /// order, latching a last-value gauge at 1. The server is idle and the gauge says one
    /// request is in flight — forever, or until the next request happens to move it. That is
    /// exactly the "rises but never returns to zero" defect this signal exists to rule out,
    /// and it is invisible to any test that drives one request at a time.
    ///
    /// The critical section is an integer update plus one or two `tracing` events — no I/O, no
    /// await — taken twice per RPC, on a path that already acquires an admission semaphore,
    /// parses HTTP/2, and touches a disk. A poisoned lock is recovered rather than propagated:
    /// telemetry must not take a storage server down, and this runs inside a `Drop`, where a
    /// panic during unwind would abort the process.
    fn record_in_flight(&self, delta: i64, also: impl FnOnce()) {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *in_flight += delta;
        let level = *in_flight;
        self.emit(|| {
            also();
            tracing::info!(gauge.capacity_requests_in_flight = level);
        });
    }

    /// A request cleared the server-wide admission bound and is now in flight.
    fn admitted(&self) {
        self.record_in_flight(1, || {
            tracing::info!(monotonic_counter.capacity_requests_admitted = 1_u64);
        });
    }

    /// An admitted request left the stack (completed, cut, or cancelled) — it no longer
    /// holds a slot.
    fn finished(&self) {
        self.record_in_flight(-1, || {});
    }

    /// A request was refused by the server-wide bound.
    fn shed(&self) {
        self.emit(|| tracing::info!(monotonic_counter.capacity_requests_shed = 1_u64));
    }

    /// An admitted request was cut before it produced a response: by the request timeout
    /// (`timed_out`) or by its caller going away (`cancelled`). Kept as two series because
    /// the operator response differs — a rising timeout rate is the SERVER failing to make
    /// progress within its own deadline, while cancellations are clients leaving.
    fn cut(&self, timed_out: bool) {
        self.emit(|| {
            if timed_out {
                tracing::info!(monotonic_counter.capacity_requests_timed_out = 1_u64);
            } else {
                tracing::info!(monotonic_counter.capacity_requests_cancelled = 1_u64);
            }
        });
    }
}

/// Decrements the in-flight count and reports how an admitted request ended — **whatever**
/// ends it.
///
/// A guard, not a match arm, because the interesting ending is the one with no return value:
/// tonic's request timeout is applied OUTSIDE this whole layer stack (`GrpcTimeout` wraps the
/// user's `Server::layer` stack — `tonic-0.14.6` `src/transport/server/mod.rs:1234-1239`), so
/// when the deadline fires it returns its own error and simply **drops** the inner future.
/// The cut is therefore never observable as a `Poll::Ready(Err(..))` from in here; it is only
/// observable as a drop.
struct AdmissionGuard {
    plane: CapacityPlane,
    /// Stamped at `call` — which runs inside `GrpcTimeout::call`, and specifically *before*
    /// it arms its own `sleep` (that struct literal evaluates `inner: self.inner.call(req)`
    /// first). So this instant is never later than the deadline's start, and when the
    /// deadline fires `elapsed >= request_timeout` holds — tokio's `sleep` is documented to
    /// wait at least its duration, never less. That ordering is what makes the timeout /
    /// cancellation split below exact rather than a guess about wall-clock.
    started: Instant,
    request_timeout: Duration,
    completed: bool,
}

impl AdmissionGuard {
    fn enter(plane: CapacityPlane, request_timeout: Duration) -> Self {
        plane.admitted();
        Self {
            plane,
            started: Instant::now(),
            request_timeout,
            completed: false,
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.plane.finished();
        if !self.completed {
            // Dropped without a response: the request was cut. It was cut by the SERVER's
            // deadline iff it lived at least that long (see `started`). A shorter life means
            // something else took it away — the client hung up, the connection died, or the
            // client's own `grpc-timeout` header set a deadline tighter than ours, which
            // tonic honours by taking the minimum. Attributing that to the server's request
            // timeout would be a lie about whose deadline fired.
            self.plane
                .cut(self.started.elapsed() >= self.request_timeout);
        }
    }
}

/// Counts requests the **server-wide** bound refused.
///
/// It must sit OUTSIDE [`LoadShedLayer`] to see anything: load-shed is what turns the
/// concurrency limit's backpressure into a rejection, so a shed request never reaches any
/// layer below it. `tower`'s `Overloaded` error is that rejection, and it is forwarded
/// unchanged — tonic still maps it to the same retryable `RESOURCE_EXHAUSTED` the client got
/// before.
///
/// It observes only the server-wide shed, which is the bound `AdmissionControl` documents as
/// binding and the one an operator tunes. The secondary *per-connection* cap is applied by
/// tonic outside the user layer stack entirely, so its shed is not reachable from here.
#[derive(Clone)]
struct ShedObserver<S> {
    inner: S,
    plane: CapacityPlane,
}

#[derive(Clone)]
struct ShedObserverLayer {
    plane: CapacityPlane,
}

impl<S> Layer<S> for ShedObserverLayer {
    type Service = ShedObserver<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ShedObserver {
            inner,
            plane: self.plane.clone(),
        }
    }
}

impl<S, R> Service<R> for ShedObserver<S>
where
    S: Service<R>,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
    S::Response: 'static,
{
    type Response = S::Response;
    type Error = BoxError;
    // Boxed rather than a hand-written future: projecting a pinned inner future without a
    // `pin-project` dependency would need `unsafe`, which this crate forbids. It costs one
    // allocation per request on a path that already boxes per request inside tonic
    // (`BoxCloneService`) and then does fragment I/O, and it changes no behaviour.
    type Future = Pin<Box<dyn Future<Output = std::result::Result<S::Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: R) -> Self::Future {
        let plane = self.plane.clone();
        let inner = self.inner.call(req);
        Box::pin(async move {
            match inner.await {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err: BoxError = err.into();
                    if err.is::<Overloaded>() {
                        plane.shed();
                    }
                    Err(err)
                }
            }
        })
    }
}

/// Reports admission, the in-flight level, and how each admitted request ended.
///
/// It must sit INSIDE [`GlobalConcurrencyLimitLayer`], and that placement is what makes
/// "admitted" mean admitted: `tower`'s concurrency limit acquires its semaphore permit in
/// `poll_ready` and only then calls inner, so reaching this layer's `call` *is* holding a
/// slot. An observer placed outside the limit could not tell an admitted request from one
/// about to be shed, and would have to count both before retracting one — which is exactly
/// how an in-flight gauge starts reporting load that was never accepted.
/// A response body that holds a concurrency permit for the LIFETIME of the stream —
/// released only when the body is fully drained or dropped (the stream closes), not
/// when the response head is produced. This is the piece that makes the health
/// surface's concurrency limit bind long-lived `Health/Watch` streams: tower's
/// `GlobalConcurrencyLimitLayer` frees its permit as soon as the streaming `Response`
/// is returned, so established watches would otherwise escape the bound and a public
/// probe port could be held open by unbounded streams (Codex P1 on #587).
struct PermitBody {
    inner: tonic::body::Body,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl http_body::Body for PermitBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>> {
        // `tonic::body::Body` is `Unpin`, so projecting through `get_mut` is sound.
        Pin::new(&mut self.get_mut().inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Bounds concurrent in-flight RPCs on the health probe surface with a permit that
/// each response STREAM retains until it closes ([`PermitBody`]) — so a `Health/Watch`
/// held open counts against the bound, not just the instant it is opened. Over the
/// limit, an RPC is shed with `RESOURCE_EXHAUSTED` (the same retryable "busy" the data
/// plane's `LoadShedLayer` raises), never queued. Separate from the data plane's
/// admission budget by construction (its own semaphore), so a probe never contends with
/// data work; bounded so a public probe port is not a DoS vector (Codex P1 on #587).
#[derive(Clone)]
struct HealthConcurrencyLayer {
    sema: Arc<tokio::sync::Semaphore>,
}

impl<S> Layer<S> for HealthConcurrencyLayer {
    type Service = HealthConcurrency<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HealthConcurrency {
            inner,
            sema: Arc::clone(&self.sema),
        }
    }
}

#[derive(Clone)]
struct HealthConcurrency<S> {
    inner: S,
    sema: Arc<tokio::sync::Semaphore>,
}

impl<S> Service<tonic::codegen::http::Request<tonic::body::Body>> for HealthConcurrency<S>
where
    S: Service<
            tonic::codegen::http::Request<tonic::body::Body>,
            Response = tonic::codegen::http::Response<tonic::body::Body>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        let sema = Arc::clone(&self.sema);
        // Use the instance that was just `poll_ready`'d, and leave a fresh clone in its
        // place (the standard tower clone-and-replace, so the readiness contract holds).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let Ok(permit) = Arc::clone(&sema).try_acquire_owned() else {
                // At capacity: shed with RESOURCE_EXHAUSTED, exactly as the data plane's
                // load-shed does, rather than queue an unbounded backlog.
                let shed =
                    tonic::Status::resource_exhausted("health probe surface at capacity; retry");
                return Ok(shed.into_http());
            };
            // Bound HEAD PRODUCTION only (`HEALTH_REQUEST_TIMEOUT`): a handler that
            // stalls before yielding its response head would otherwise hold this permit
            // forever, and enough of them would exhaust the bound and permanently shed
            // legitimate probes (Codex P2 on #587). On timeout we return BEFORE wrapping
            // the body, so `permit` drops here and is released. Deliberately NOT tonic's
            // builder `.timeout()`, which can also cut a long-lived `Health/Watch`
            // stream: a Watch's head is produced promptly and its stream then lives on
            // (its permit released when the CLIENT closes it, see `PermitBody`).
            let response = match tokio::time::timeout(HEALTH_REQUEST_TIMEOUT, inner.call(req)).await
            {
                Ok(result) => result?,
                Err(_elapsed) => {
                    let timed_out = tonic::Status::deadline_exceeded(
                        "health probe did not produce a response within the deadline",
                    );
                    return Ok(timed_out.into_http());
                }
            };
            let (parts, body) = response.into_parts();
            let body = tonic::body::Body::new(PermitBody {
                inner: body,
                _permit: permit,
            });
            Ok(tonic::codegen::http::Response::from_parts(parts, body))
        })
    }
}

#[derive(Clone)]
struct AdmissionObserver<S> {
    inner: S,
    plane: CapacityPlane,
    request_timeout: Duration,
}

#[derive(Clone)]
struct AdmissionObserverLayer {
    plane: CapacityPlane,
    request_timeout: Duration,
}

impl<S> Layer<S> for AdmissionObserverLayer {
    type Service = AdmissionObserver<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AdmissionObserver {
            inner,
            plane: self.plane.clone(),
            request_timeout: self.request_timeout,
        }
    }
}

impl<S, R> Service<R> for AdmissionObserver<S>
where
    S: Service<R>,
    S::Future: Send + 'static,
    S::Response: 'static,
    S::Error: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        // The guard is built HERE, not inside the async block, for two reasons. Its `started`
        // must be stamped before `GrpcTimeout` arms its deadline (see `AdmissionGuard`), and
        // moving it into the block means a future that is dropped without ever being polled
        // still releases its in-flight slot — otherwise the gauge would leak upward and never
        // return to zero.
        let guard = AdmissionGuard::enter(self.plane.clone(), self.request_timeout);
        let inner = self.inner.call(req);
        Box::pin(async move {
            let mut guard = guard;
            let response = inner.await;
            guard.completed = true;
            response
        })
    }
}

/// What a D server publishes through `Coordination::register` (proposal 0005, "The
/// placement record", `0005:194-196`): its **stable id**, its current dialable
/// **endpoint**, and its opaque **failure-domain label**. Keyed on the stable id
/// (not the URL, which rebinds), this is what lets the write path build a topology
/// and place a chunk's fragments across distinct failure domains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DServerRegistration {
    /// The stable D-server id (the placement record keys on this).
    pub id: DServerId,
    /// The dialable gRPC endpoint (e.g. `http://127.0.0.1:50051`).
    pub endpoint: String,
    /// The opaque failure-domain label (rack / power / switch).
    pub failure_domain: String,
}

impl DServerRegistration {
    /// Decode a registration from its stored coordination bytes. Falls back to
    /// treating the raw value as a bare endpoint (a pre-M3 registration that carried
    /// only the endpoint string), keeping discovery one-version-gap compatible.
    pub fn decode(raw: &[u8]) -> Result<Self> {
        if let Ok(reg) = serde_json::from_slice::<DServerRegistration>(raw) {
            return Ok(reg);
        }
        let endpoint = String::from_utf8(raw.to_vec())
            .map_err(|e| format!("non-utf8 D-server registration: {e}"))?;
        Ok(DServerRegistration {
            id: 0,
            endpoint,
            failure_domain: DEFAULT_FAILURE_DOMAIN.to_string(),
        })
    }
}

/// A D server bound to a port but not yet serving. Binding first means the
/// listener is already accepting into the OS backlog and the advertised endpoint
/// is known, so the server can [`register`](DServer::register) for discovery
/// *before* the serve loop starts — a discoverer sees it the moment registration
/// returns, with no startup race.
pub struct DServer<S> {
    store: S,
    listener: TcpListener,
    endpoint: String,
    id: DServerId,
    failure_domain: String,
    admission: AdmissionControl,
    /// The sink this server's **capacity-plane** signals are emitted into (proposal 0010
    /// item 5) — see [`DServer::with_metrics_dispatch`].
    metrics: Option<tracing::Dispatch>,
    /// The **operator-configurable, stable** address the `grpc.health.v1.Health` probe
    /// surface binds (proposal 0010 item 7), or `None` when no probe surface is served.
    /// A separate socket from `listener` so the probe answers outside the data-plane
    /// admission layers; bound at [`serve`](DServer::serve) time. `None` by default (the
    /// library building block serves no probe unless asked) — the deployable
    /// `wyrd d-server` role always enables it with a stable default (`--health-bind`,
    /// `cli.rs`), so a production node is always probeable, while an in-process caller
    /// that spins several servers is not forced onto one fixed port. Set with
    /// [`with_health_bind`](DServer::with_health_bind).
    health_bind: Option<SocketAddr>,
    /// Cadence of the readiness-refresh task — see [`DEFAULT_HEALTH_REFRESH_INTERVAL`].
    health_refresh_interval: Duration,
    /// Concurrent in-flight RPCs the probe surface admits before shedding — see
    /// [`HEALTH_MAX_CONCURRENT_REQUESTS`]. Bounds long-lived `Health/Watch` streams
    /// (each retains its permit for the stream's lifetime), so a public probe port is
    /// not an unbounded DoS vector.
    health_max_concurrent_requests: usize,
}

impl<S: ChunkStore + 'static> DServer<S> {
    /// Bind the gRPC listener on `bind` (use port 0 for an ephemeral port) over
    /// `store`. The advertised endpoint defaults to the bound address (today's
    /// loopback behaviour); a wildcard/NAT'd/containerized bind can override it with
    /// [`with_advertise_addr`](DServer::with_advertise_addr) — split-horizon
    /// advertisement, decoupled from the listen socket. The server starts with a
    /// default stable id and the default failure domain; set them with
    /// [`with_identity`](DServer::with_identity) before registering.
    pub async fn bind(store: S, bind: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(bind).await?;
        let addr = listener.local_addr()?;
        Ok(Self {
            store,
            listener,
            endpoint: format!("http://{addr}"),
            id: 0,
            failure_domain: DEFAULT_FAILURE_DOMAIN.to_string(),
            admission: AdmissionControl::default(),
            metrics: None,
            health_bind: None,
            health_refresh_interval: DEFAULT_HEALTH_REFRESH_INTERVAL,
            health_max_concurrent_requests: HEALTH_MAX_CONCURRENT_REQUESTS,
        })
    }

    /// Emit this server's **capacity-plane** signals into `dispatch` (observability floor,
    /// proposal 0010 §"Scope boundary" item 5): admitted / shed / timed-out / cancelled
    /// events and the in-flight RPC gauge, raised around the existing admission stack.
    ///
    /// The dispatch is the role's metrics sink (`wyrd_telemetry::DurabilityTelemetry::
    /// metrics_dispatch()`), composed at the `wyrd d-server` role entry (`cli::cmd_d_server`)
    /// with the export surface chosen by `ExporterConfig` — so no telemetry backend is named
    /// here (ADR-0012). It is carried rather than scoped because tonic serves each connection
    /// on its own spawned task, which does not inherit a scoped dispatch.
    ///
    /// Unset, the signals are emitted into the ambient subscriber (today's behaviour). This
    /// changes **no** admission behaviour either way: what is admitted and what is shed is
    /// [`AdmissionControl`]'s business, and the observers only watch.
    pub fn with_metrics_dispatch(mut self, dispatch: tracing::Dispatch) -> Self {
        self.metrics = Some(dispatch);
        self
    }

    /// Set this server's **stable id** and opaque **failure-domain label** — the
    /// placement-relevant facts its registration carries (proposal 0005,
    /// `0005:194-196`, §"Failure-domain-aware placement"). Distinct labels are what
    /// let the write selector place a chunk's fragments across distinct domains.
    pub fn with_identity(mut self, id: DServerId, failure_domain: impl Into<String>) -> Self {
        self.id = id;
        self.failure_domain = failure_domain.into();
        self
    }

    /// Override the endpoint this server **registers** for discovery to `advertise`
    /// (host:port — a routable DNS service name or NAT-mapped address), decoupling
    /// the registration record from the bound socket address `bind` derived it from.
    /// This is what lets a server bound to a wildcard/loopback address (a
    /// containerized `--bind 0.0.0.0:PORT`) still publish an endpoint its consumers
    /// can actually dial, instead of the un-dialable wildcard/ephemeral bind value
    /// (the split-horizon advertisement gap this closes). Unset, the endpoint stays
    /// the bound-address value `bind` set (today's loopback behaviour, preserved).
    pub fn with_advertise_addr(mut self, advertise: impl Into<String>) -> Self {
        self.endpoint = format!("http://{}", advertise.into());
        self
    }

    /// Set the [`AdmissionControl`] posture this server's gRPC transport applies —
    /// the **server-wide** admission limit, request timeout, and the HTTP/2 / TCP
    /// tuning that make the request-admission path **fail closed under pressure**
    /// (architecture §8.9). Defaults to [`AdmissionControl::default`]; operators
    /// tune the limit to the backing device's useful queue depth.
    pub fn with_admission_control(mut self, admission: AdmissionControl) -> Self {
        self.admission = admission;
        self
    }

    /// The admission-control posture this server's gRPC transport applies.
    pub fn admission_control(&self) -> &AdmissionControl {
        &self.admission
    }

    /// Serve the `grpc.health.v1.Health` probe surface on the **stable,
    /// operator-configurable** address `health_bind` (proposal 0010 item 7) — the
    /// address a deployment supervisor (systemd / k8s / a load balancer) dials to ask
    /// "alive, and ready to serve?". It is deliberately a **separate** socket from the
    /// data-plane [`bind`](DServer::bind) address so the probe answers *outside* the
    /// admission layers (see [`serve`](DServer::serve), §"Overload policy"); the
    /// `wyrd d-server --health-bind ADDR` flag plumbs it, defaulting to the stable,
    /// non-ephemeral [`DEFAULT_HEALTH_BIND`] so the endpoint is discoverable rather than
    /// OS-assigned. Unset (the [`bind`](DServer::bind) default), **no probe surface is
    /// served** — so an in-process caller that spins several servers is not forced onto
    /// one fixed port. Bound at `serve` time (unlike the eagerly-bound data listener,
    /// because the probe surface is not registered for discovery, so it needs no
    /// pre-serve `local_addr`).
    pub fn with_health_bind(mut self, health_bind: SocketAddr) -> Self {
        // Refuse port zero AT CONFIGURATION TIME: the probe surface exists to give a
        // supervisor a KNOWN address, and `:0` asks the OS for a port nothing exposes —
        // `health_bind()` and the startup log would keep reporting the configured `:0`
        // while the real listener sits on a port no supervisor can discover (Codex P2 on
        // #587). The CLI refuses the same input with an `Err` (`cli.rs`
        // `resolve_health_bind`); this assert is the library-misuse backstop.
        assert!(
            health_bind.port() != 0,
            "health probe bind must use a concrete port: the probe exists to give \
             supervisors a KNOWN address, and an OS-assigned ephemeral port is exposed \
             nowhere"
        );
        self.health_bind = Some(health_bind);
        self
    }

    /// The address the `grpc.health.v1.Health` probe surface binds, or `None` when no
    /// probe surface is served — the configured value, for logging/introspection. When
    /// `Some`, it is the operator-facing, stable address, **not** an OS-assigned
    /// ephemeral read-back.
    pub fn health_bind(&self) -> Option<SocketAddr> {
        self.health_bind
    }

    /// Set the cadence at which the readiness-refresh task re-reads the store's
    /// `health()` (default [`DEFAULT_HEALTH_REFRESH_INTERVAL`]) — the operator-visible
    /// freshness bound the readiness status is refreshed against. A shorter interval
    /// tightens how quickly an unhealthy store's readiness flip becomes observable, at
    /// the cost of polling `health()` more often.
    pub fn with_health_refresh_interval(mut self, interval: Duration) -> Self {
        // Refuse zero AT CONFIGURATION TIME: `tokio::time::interval` panics on a zero
        // period, and that panic would land inside the spawned refresher task — killing
        // it silently while the data plane keeps serving, so readiness would sit at its
        // fail-closed initial NOT_SERVING forever with no diagnostic (Codex P2 on #587).
        // A loud panic at the builder is the misconfiguration surfacing where it was made.
        assert!(
            !interval.is_zero(),
            "health refresh interval must be non-zero: a zero period panics the readiness \
             refresher task and leaves the probe stuck at NOT_SERVING while the data plane \
             keeps serving"
        );
        self.health_refresh_interval = interval;
        self
    }

    /// Override the probe surface's concurrency bound (default
    /// [`HEALTH_MAX_CONCURRENT_REQUESTS`]) — the number of concurrent in-flight health
    /// RPCs, including held-open `Health/Watch` streams, admitted before shedding.
    pub fn with_health_max_concurrent_requests(mut self, limit: usize) -> Self {
        assert!(
            limit != 0,
            "health max concurrent requests must be non-zero: a zero bound sheds every \
             probe, so no supervisor could ever read readiness"
        );
        self.health_max_concurrent_requests = limit;
        self
    }

    /// The dialable endpoint this server advertises (e.g. `http://127.0.0.1:50051`).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// This server's stable D-server id.
    pub fn id(&self) -> DServerId {
        self.id
    }

    /// The registration record this server publishes (`{ id, endpoint, failure-domain
    /// label }`).
    pub fn registration(&self) -> DServerRegistration {
        DServerRegistration {
            id: self.id,
            endpoint: self.endpoint.clone(),
            failure_domain: self.failure_domain.clone(),
        }
    }

    /// Register this server's `{ id, endpoint, failure-domain label }` record under
    /// `group` with a lease of `ttl`. The returned [`Lease`] is renewed by
    /// [`serve`](DServer::serve); letting it lapse drops the server out of `discover`.
    pub async fn register(
        &self,
        coord: &impl Coordination,
        group: &str,
        ttl: Duration,
    ) -> Result<Lease> {
        let value = serde_json::to_vec(&self.registration())
            .map_err(|e| format!("encoding D-server registration: {e}"))?;
        coord.register(group, Bytes::from(value), ttl).await
    }

    /// Serve the gRPC `ChunkStore` (plus the standard `grpc.health.v1.Health` probe
    /// surface, proposal 0010 item 7) until `shutdown` resolves, renewing `lease`
    /// every `renew_interval` so the registration stays live. On a clean shutdown
    /// the lease is revoked so discovery converges promptly; if renewal ever
    /// fails (the lease was lost), the server stops serving.
    ///
    /// **The health probe surface** answers `grpc.health.v1.Health/Check` on the
    /// configured [`health_bind`](DServer::health_bind) address — a *separate* socket
    /// from the data listener. Its readiness status mirrors the backing store's own
    /// [`ChunkStore::health`]: `Healthy`/`Degraded` ⇒ SERVING (a degraded store still
    /// serves), `Unhealthy` **and** `Err(_)` ⇒ NOT_SERVING (fail closed — a store that
    /// cannot even report its health must not read as ready), refreshed every
    /// [`with_health_refresh_interval`](DServer::with_health_refresh_interval). The
    /// probe surface is served by its **own, separately bounded** transport, so it stays
    /// answerable even when the data plane is shedding at its admission bound (a probe
    /// shed as `RESOURCE_EXHAUSTED` would make supervisors restart an
    /// overloaded-but-healthy node). The store-derived status is published on **both**
    /// the overall empty-name service (what a plain `Health/Check` with no `service`
    /// field reads — the default for `grpcurl` and `grpc_health_probe`) and the
    /// `ChunkStoreServer`'s registered name, from one reading, so they can never
    /// disagree; liveness is simply the probe socket answering at all.
    pub async fn serve<Co>(
        self,
        coord: Arc<Co>,
        lease: Lease,
        renew_interval: Duration,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<()>
    where
        Co: Coordination + 'static,
    {
        // Share the store (`Arc`) rather than move it wholesale into `ChunkStoreService`,
        // because the readiness-refresh task below must poll the SAME store instance the
        // data plane serves — probing a different instance would defeat deriving readiness
        // from `health()`. `ChunkStoreService::from_arc` is the existing affordance for a
        // store already behind an `Arc` (`crates/chunkstore-grpc/src/server.rs:57-61`).
        let store = Arc::new(self.store);
        let service = ChunkStoreService::from_arc(Arc::clone(&store));
        let admission = self.admission;
        let health_max_concurrent_requests = self.health_max_concurrent_requests;

        // ---- the OPTIONAL health probe surface (observability floor, proposal 0010 item 7) ----
        //
        // Served only when a bind address is configured (`with_health_bind` / the
        // `wyrd d-server --health-bind ADDR` flag) — the library building block serves no
        // probe by default, so several in-process servers are not forced onto one fixed
        // port, while the deployable role always enables it on a stable, known address a
        // supervisor can dial. When served it is a SEPARATE transport with its OWN small
        // admission envelope (below),
        // so a probe answers *outside* the data-plane admission layers.
        let (health_surface, health_refresh_task) = match self.health_bind {
            Some(health_bind) => {
                // Bind the probe socket on the operator-configured, stable address — a
                // discoverable address a supervisor can dial, not an OS-assigned ephemeral
                // port. REVOKE THE LEASE on failure (e.g. the probe port is already in
                // use): the caller registered the data endpoint immediately before calling
                // `serve` (`cli.rs` `run_d_server`), so a bare `?` here would leave
                // discovery advertising a server whose listeners are dropped until the
                // lease expires — a phantom endpoint (Codex P1 on #587). The data listener
                // is already bound (in `bind`), so only this bind can still fail post-register.
                let health_listener = match TcpListener::bind(health_bind).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        let _ = coord.revoke(lease).await;
                        return Err(e.into());
                    }
                };
                // `health_reporter()` returns the write side (`HealthReporter`) linked to
                // the gRPC `HealthServer`. The store-derived readiness is published on BOTH
                // statuses a prober can ask for: the overall empty-name "" service — what
                // the protocol's plain `Health/Check` (grpcurl with no `service` field,
                // `grpc_health_probe` with no `-service`) reads, and what a generic
                // supervisor dials by default — and the `ChunkStoreServer`'s own registered
                // name, for per-service checks. Leaving "" at tonic-health's `Serving`
                // default would make the DOCUMENTED probe invocation report ready forever,
                // however unhealthy the store (Codex P1 on #587); liveness needs no status
                // of its own — the probe socket answering at all is the liveness signal.
                // Both are set `NotServing` here, before anything is served, so a probe
                // landing before the first `health()` read reads fail-closed NOT_SERVING
                // (not `NOT_FOUND`, what a never-registered name reads as; not the ""
                // default `Serving`, which would be a false ready).
                let (health_reporter, health_server) = tonic_health::server::health_reporter();
                health_reporter
                    .set_service_status("", ServingStatus::NotServing)
                    .await;
                health_reporter
                    .set_not_serving::<ChunkStoreServer<ChunkStoreService<S>>>()
                    .await;

                // Refresh the readiness status every `health_refresh_interval` (a bounded,
                // operator-visible cadence) by re-reading the store's own `health()`.
                // `Healthy`/`Degraded` read SERVING; `Unhealthy` and `Err(_)` both read
                // NOT_SERVING — the latter is the fail-closed case. `tokio::time::interval`'s
                // first tick fires immediately, so the first real reading happens promptly.
                let refresh_task = {
                    let store = Arc::clone(&store);
                    let reporter = health_reporter.clone();
                    let interval = self.health_refresh_interval;
                    tokio::spawn(async move {
                        // Publish one reading to BOTH statuses a prober can ask for — the
                        // empty-name overall service (the plain `Health/Check` a
                        // default-configured prober sends) and the named service — so
                        // they can never disagree.
                        let publish = |status: ServingStatus| {
                            let reporter = reporter.clone();
                            async move {
                                reporter.set_service_status("", status).await;
                                reporter
                                    .set_service_status(
                                        <ChunkStoreServer<ChunkStoreService<S>> as
                                            NamedService>::NAME,
                                        status,
                                    )
                                    .await;
                            }
                        };
                        let mut ticker = tokio::time::interval(interval);
                        loop {
                            ticker.tick().await;
                            // The probe read is BOUNDED by the refresh cadence: a
                            // `health()` that HANGS (a stalled mount blocking a
                            // filesystem metadata call) must fail closed on expiry, not
                            // pin this loop with the last SERVING on display (Codex P2
                            // on #587). And a timed-out probe is NOT abandoned for a
                            // fresh one: dropping the future leaves its `spawn_blocking`
                            // work running, so re-probing every interval would stack
                            // stuck blocking tasks until tokio's blocking pool starves
                            // the data plane's own filesystem calls (Codex P1 follow-on).
                            // AT MOST ONE underlying probe is ever in flight — on expiry
                            // the SAME future is kept and re-awaited, publishing
                            // NOT_SERVING each interval until it resolves.
                            let probe = store.health();
                            tokio::pin!(probe);
                            let status = loop {
                                match tokio::time::timeout(interval, &mut probe).await {
                                    Ok(Ok(Health::Healthy | Health::Degraded)) => {
                                        break ServingStatus::Serving;
                                    }
                                    Ok(Ok(Health::Unhealthy)) | Ok(Err(_)) => {
                                        break ServingStatus::NotServing;
                                    }
                                    Err(_still_in_flight) => {
                                        publish(ServingStatus::NotServing).await;
                                    }
                                }
                            };
                            publish(status).await;
                        }
                    })
                };
                (Some((health_listener, health_server)), Some(refresh_task))
            }
            None => (None, None),
        };

        // Fan the single `shutdown` future out so the data server and (when served) the
        // health probe stop on the same signal — each owns a `serve_with_incoming_shutdown`
        // call, and a bare `impl Future` can only be awaited once. A `watch` channel fired
        // exactly once gives every receiver the same one-shot signal.
        let (shutdown_tx, mut data_shutdown_rx) = tokio::sync::watch::channel(());
        let mut health_shutdown_rx = data_shutdown_rx.clone();
        tokio::spawn(async move {
            shutdown.await;
            let _ = shutdown_tx.send(());
        });
        let data_shutdown = async move {
            let _ = data_shutdown_rx.changed().await;
        };

        // The capacity plane (proposal 0010 item 5). Built once, outside the builder, so the
        // in-flight count is shared by BOTH observers and by every per-connection clone of
        // the layer stack — the gauge tracks the same server-wide population the admission
        // bound does.
        let plane = CapacityPlane::new(self.metrics);
        plane.preregister();
        let serve = Server::builder()
            // OBSERVE the shed (0010 item 5). Outermost, so it sees the `Overloaded`
            // rejection the load-shed layer below raises — the shed that until now was
            // visible ONLY as a client-side `RESOURCE_EXHAUSTED`. It forwards that error
            // untouched; nothing about what gets shed moves.
            .layer(ShedObserverLayer {
                plane: plane.clone(),
            })
            // Fail-closed admission, SERVER-WIDE (architecture §8.9). Applied via
            // `.layer()`, the layer stack is built once and *cloned* per connection,
            // so a `GlobalConcurrencyLimitLayer` (which holds one `Arc<Semaphore>`)
            // bounds the concurrent in-flight requests across the WHOLE server, not
            // per connection — aggregate in-flight can never exceed the limit no
            // matter how many connections pile on. The outer `LoadShedLayer` turns
            // the limit's backpressure into an immediate *shed*: an over-limit
            // request is rejected with `tower`'s `Overloaded`, which tonic maps to a
            // retryable `RESOURCE_EXHAUSTED` status (verified against tonic 0.14.6
            // `status.rs`: `Overloaded` -> `Status::resource_exhausted`), instead of
            // queuing without bound and contending for runtime threads.
            //
            // Order matters: the FIRST `.layer()` is the OUTERMOST, so load-shed
            // wraps the concurrency limit and sheds when the shared semaphore is
            // exhausted (verified against tower 0.5 `ServiceBuilder`/`Stack`).
            //
            // This layer stack — and everything else this ONE `Server::builder()` sets —
            // applies to every service `.add_service()`d to it. That is exactly why the
            // health service is NOT added here: the readiness probe must answer *through*
            // an overloaded data plane (a probe shed as `RESOURCE_EXHAUSTED` makes a
            // supervisor restart an overloaded-but-healthy node), so it is served by its
            // OWN, separately bounded `Server::builder()` on `health_listener` below — genuinely
            // "outside that stack" by construction, no per-service escape hatch needed.
            .layer(LoadShedLayer::new())
            .layer(GlobalConcurrencyLimitLayer::new(
                admission.max_concurrent_requests,
            ))
            // OBSERVE admission + the in-flight level (0010 item 5). INSIDE the concurrency
            // limit, so it is reached only by a request that already holds a permit — which
            // is what makes "admitted" mean admitted and keeps a shed request off the
            // in-flight gauge entirely.
            .layer(AdmissionObserverLayer {
                plane: plane.clone(),
                request_timeout: admission.request_timeout,
            })
            // Secondary, per-connection caps: a single connection cannot monopolise
            // the budget, and `load_shed` makes its over-limit excess shed too. The
            // server-wide layer above is the binding fail-closed bound; these only
            // shape a single client's fan-in.
            .concurrency_limit_per_connection(admission.max_concurrent_requests_per_connection)
            .load_shed(true)
            // Bound the work a single request can pin: a handler past this deadline
            // is cut loose rather than holding its admission slot forever.
            .timeout(admission.request_timeout)
            // Cap per-connection HTTP/2 stream fan-in (the implicit h2 default is
            // effectively unbounded) and tune the TCP/keepalive posture for small,
            // latency-sensitive gRPC frames.
            .max_concurrent_streams(Some(admission.max_concurrent_streams))
            .tcp_nodelay(admission.tcp_nodelay)
            .http2_keepalive_interval(admission.http2_keepalive_interval)
            .add_service(ChunkStoreServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(self.listener), data_shutdown);

        // Run the data server, and (when configured) the health probe, together. When the
        // probe is served it gets its OWN, separately bounded `Server::builder()` — no admission
        // layers, nothing shared with the data-plane builder above — on the configured
        // `health_bind` address, so a probe answered there is never contended by, or shed
        // behind, the data plane's admission bound.
        //
        // The probe surface's drain is BOUNDED, not joined unconditionally: the standard
        // streaming `grpc.health.v1.Health/Watch` RPC stays open until its CLIENT hangs
        // up, and tonic's graceful shutdown waits for in-flight RPCs — so a plain `join!`
        // would let one connected watcher pin this whole role past SIGTERM, never reaching
        // the lease revocation below (Codex P1 on #587). The data plane still drains
        // gracefully first (its in-flight work is bounded by the request timeout layer);
        // the health server then gets [`HEALTH_SHUTDOWN_GRACE`] to finish, after which it
        // is aborted — a probe stream carries no user data, and a supervisor that ordered
        // the shutdown is not owed a drained watch stream.
        let servers = async {
            match health_surface {
                Some((health_listener, health_server)) => {
                    // The probe surface's OWN, SMALL admission envelope — separate
                    // from (never contending with) the data plane's, but bounded: a
                    // publicly reachable probe bind must not be an unbounded DoS vector
                    // of long-lived Watch streams and sockets (Codex P1 on #587). Same
                    // fail-closed layering as the data plane, sized for supervisors.
                    let health_serve = Server::builder()
                        // Permit-holding concurrency bound: a permit is retained for
                        // each RPC's whole lifetime — including a long-lived Watch
                        // stream — so the surface is genuinely bounded, not just at the
                        // instant a stream opens (see `HealthConcurrencyLayer`). Its own
                        // semaphore, so the probe never contends with the data plane.
                        .layer(HealthConcurrencyLayer {
                            sema: Arc::new(tokio::sync::Semaphore::new(
                                health_max_concurrent_requests,
                            )),
                        })
                        .concurrency_limit_per_connection(health_max_concurrent_requests)
                        .max_concurrent_streams(Some(HEALTH_MAX_CONCURRENT_STREAMS))
                        .http2_keepalive_interval(Some(DEFAULT_HTTP2_KEEPALIVE_INTERVAL))
                        .add_service(health_server)
                        .serve_with_incoming_shutdown(
                            TcpListenerStream::new(health_listener),
                            async move {
                                let _ = health_shutdown_rx.changed().await;
                            },
                        );
                    // Abort-on-drop, NOT a bare spawn: the outer `select!` below drops
                    // this whole `servers` future when the lease-renewal arm wins (a
                    // lost lease), and a detached task would keep the probe listener
                    // alive indefinitely after `serve()` returned and revoked the lease.
                    // The guard ties the task's lifetime to this future: dropped for any
                    // reason ⇒ aborted.
                    struct AbortOnDrop(
                        tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>,
                    );
                    impl Drop for AbortOnDrop {
                        fn drop(&mut self) {
                            self.0.abort();
                        }
                    }
                    let mut health_task = AbortOnDrop(tokio::spawn(health_serve));
                    let data_res: Result<()> = serve.await.map_err(Into::into);
                    let health_res: Result<()> =
                        match tokio::time::timeout(HEALTH_SHUTDOWN_GRACE, &mut health_task.0).await
                        {
                            Ok(joined) => match joined {
                                Ok(served) => served.map_err(Into::into),
                                // The health task panicking is a real defect — surface it.
                                Err(join_err) => Err(join_err.into()),
                            },
                            Err(_grace_elapsed) => {
                                // A connected Watch stream is pinning the drain: cut it
                                // loose (the guard's abort). Deliberately Ok — an aborted
                                // probe stream at shutdown is the designed outcome, not a
                                // serve failure.
                                Ok(())
                            }
                        };
                    data_res?;
                    health_res
                }
                None => serve.await.map_err(Into::into),
            }
        };

        let renew = {
            let coord = Arc::clone(&coord);
            async move {
                let mut ticker = tokio::time::interval(renew_interval);
                ticker.tick().await; // the first tick fires immediately — skip it
                loop {
                    ticker.tick().await;
                    if coord.renew(lease).await.is_err() {
                        break; // lease lost; stop serving
                    }
                }
            }
        };

        let result = tokio::select! {
            res = servers => res,
            _ = renew => Err("d-server lease lost (renewal failed)".into()),
        };
        // The readiness refresher (if any) loops until cancelled, so it is aborted
        // explicitly rather than joined — it must not outlive `serve` (no leaked task
        // after shutdown, whichever `select!` arm won).
        if let Some(task) = health_refresh_task {
            task.abort();
        }
        // Best-effort: withdraw the registration so discovery converges promptly.
        let _ = coord.revoke(lease).await;
        result
    }
}

/// Resolve the dialable endpoints currently registered under `group`, decoding the
/// `{ id, endpoint, failure-domain label }` record (with a bare-endpoint fallback for
/// a pre-M3 registration).
pub async fn discover_endpoints(coord: &impl Coordination, group: &str) -> Result<Vec<String>> {
    let mut endpoints = Vec::new();
    for raw in coord.discover(group).await? {
        endpoints.push(DServerRegistration::decode(&raw)?.endpoint);
    }
    Ok(endpoints)
}

/// Build the failure-domain [`Topology`] the write selector places against from the
/// D servers currently registered under `group` (proposal 0005,
/// §"Failure-domain-aware placement", `0005:235-245`). Each registration contributes
/// its stable id and opaque failure-domain label; the selector then spreads a chunk's
/// fragments across distinct domains. This is the **production input** that retires
/// the domain-blind `index % n` route.
pub async fn discover_topology(coord: &impl Coordination, group: &str) -> Result<Topology> {
    let mut topology = Topology::default();
    for raw in coord.discover(group).await? {
        let reg = DServerRegistration::decode(&raw)?;
        topology.register(reg.id, reg.failure_domain);
    }
    Ok(topology)
}

/// Select `n` endpoints to fan a chunk's `n` fragments out to, **preferring
/// distinct D servers** and cycling when fewer than `n` are known.
///
/// This is best-effort selection, **not** a gated placement guarantee: with
/// fewer than `n` D servers some fragments necessarily share one, and even
/// endpoint-distinctness is not a DST invariant — failure-domain-aware placement
/// is L2 / custodian work (M3+, proposal 0004). Returns an empty vector when no
/// D server is known, so the caller fails closed rather than writing nowhere.
pub fn select_fanout(endpoints: &[String], n: usize) -> Vec<String> {
    if endpoints.is_empty() {
        return Vec::new();
    }
    (0..n)
        .map(|i| endpoints[i % endpoints.len()].clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wyrd_chunkstore_fs::FsChunkStore;

    /// `:117` `id -> Default::default()` — `id()` reports the configured stable id,
    /// not the `0` default. Bind a server, set an identity, and read it back.
    #[tokio::test]
    async fn id_reports_the_configured_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::open(dir.path()).unwrap();
        let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap()
            .with_identity(42, "rack-a");
        assert_eq!(
            server.id(),
            42,
            "id() returns the identity set via with_identity, not the default 0"
        );
    }
}
