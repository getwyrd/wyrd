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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::load_shed::LoadShedLayer;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService};
use wyrd_core::placement::Topology;
use wyrd_traits::{ChunkStore, Coordination, DServerId, Lease, Result};

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
}

impl<S: ChunkStore + 'static> DServer<S> {
    /// Bind the gRPC listener on `bind` (use port 0 for an ephemeral port) over
    /// `store`. The advertised endpoint is derived from the bound address; NAT /
    /// split-horizon advertisement is a later deployment concern. The server starts
    /// with a default stable id and the default failure domain; set them with
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
        })
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

    /// Serve the gRPC `ChunkStore` until `shutdown` resolves, renewing `lease`
    /// every `renew_interval` so the registration stays live. On a clean shutdown
    /// the lease is revoked so discovery converges promptly; if renewal ever
    /// fails (the lease was lost), the server stops serving.
    pub async fn serve<Co>(
        self,
        coord: Arc<Co>,
        lease: Lease,
        renew_interval: Duration,
        shutdown: impl Future<Output = ()>,
    ) -> Result<()>
    where
        Co: Coordination + 'static,
    {
        let service = ChunkStoreService::new(self.store);
        let admission = self.admission;
        let serve = Server::builder()
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
            .layer(LoadShedLayer::new())
            .layer(GlobalConcurrencyLimitLayer::new(
                admission.max_concurrent_requests,
            ))
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
            .serve_with_incoming_shutdown(TcpListenerStream::new(self.listener), shutdown);

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
            res = serve => res.map_err(Into::into),
            _ = renew => Err("d-server lease lost (renewal failed)".into()),
        };
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
