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
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService};
use wyrd_traits::{ChunkStore, Coordination, Lease, Result};

/// The discovery group under which D servers register their gRPC endpoints.
pub const DSERVER_GROUP: &str = "chunkstore";

/// A D server bound to a port but not yet serving. Binding first means the
/// listener is already accepting into the OS backlog and the advertised endpoint
/// is known, so the server can [`register`](DServer::register) for discovery
/// *before* the serve loop starts — a discoverer sees it the moment registration
/// returns, with no startup race.
pub struct DServer<S> {
    store: S,
    listener: TcpListener,
    endpoint: String,
}

impl<S: ChunkStore + 'static> DServer<S> {
    /// Bind the gRPC listener on `bind` (use port 0 for an ephemeral port) over
    /// `store`. The advertised endpoint is derived from the bound address; NAT /
    /// split-horizon advertisement is a later deployment concern.
    pub async fn bind(store: S, bind: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(bind).await?;
        let addr = listener.local_addr()?;
        Ok(Self {
            store,
            listener,
            endpoint: format!("http://{addr}"),
        })
    }

    /// The dialable endpoint this server advertises (e.g. `http://127.0.0.1:50051`).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Register this server's endpoint under `group` with a lease of `ttl`. The
    /// returned [`Lease`] is renewed by [`serve`](DServer::serve); letting it
    /// lapse drops the endpoint out of `discover`.
    pub async fn register(
        &self,
        coord: &impl Coordination,
        group: &str,
        ttl: Duration,
    ) -> Result<Lease> {
        coord
            .register(group, Bytes::from(self.endpoint.clone().into_bytes()), ttl)
            .await
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

/// Resolve the dialable endpoints currently registered under `group`. Endpoints
/// are stored as UTF-8 strings; a non-UTF-8 value is a contract violation.
pub async fn discover_endpoints(coord: &impl Coordination, group: &str) -> Result<Vec<String>> {
    let mut endpoints = Vec::new();
    for raw in coord.discover(group).await? {
        let endpoint = String::from_utf8(raw.to_vec())
            .map_err(|e| format!("non-utf8 endpoint in discovery group `{group}`: {e}"))?;
        endpoints.push(endpoint);
    }
    Ok(endpoints)
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
