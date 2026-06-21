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
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService};
use wyrd_core::placement::Topology;
use wyrd_traits::{ChunkStore, Coordination, DServerId, Lease, Result};

/// The discovery group under which D servers register their gRPC endpoints.
pub const DSERVER_GROUP: &str = "chunkstore";

/// The default opaque failure-domain label a D server reports when none is
/// configured — a single-domain zone (the M2 best-effort posture). Real
/// deployments set a per-server rack / power / switch label (architecture §7.3).
pub const DEFAULT_FAILURE_DOMAIN: &str = "default";

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
        let serve = Server::builder()
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
