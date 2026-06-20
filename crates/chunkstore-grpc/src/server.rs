//! [`ChunkStoreService`]: the D-server-side gRPC service, generic over the
//! [`ChunkStore`] it hosts.

use std::sync::Arc;

use bytes::Bytes;
use tonic::{Request, Response, Status};
use wyrd_proto::v0::chunk_store_server::ChunkStore as ChunkStoreRpc;
use wyrd_proto::v0::{
    FragmentGetRequest, FragmentGetResponse, FragmentPutRequest, FragmentPutResponse,
    HealthRequest, HealthResponse,
};
use wyrd_traits::ChunkStore;

use crate::conv;

/// Serves an injected `S: ChunkStore` over the gRPC `ChunkStore` contract.
///
/// `S` is held behind an [`Arc`] because tonic dispatches each request against
/// `&self` from its own task; the production injection is `FsChunkStore`, the
/// DST injection a fault-injecting fake. The service stays deliberately dumb —
/// it translates wire ⇆ trait types and delegates; **integrity verification is
/// the store's job** (the `ChunkStore` contract: implementations verify a
/// fragment's self-describing checksums on put and get), so a corrupt or
/// mis-filed fragment is rejected by `S` and surfaced here as an error status.
pub struct ChunkStoreService<S> {
    inner: Arc<S>,
}

impl<S> ChunkStoreService<S> {
    /// Wrap `store` as a gRPC service.
    pub fn new(store: S) -> Self {
        Self {
            inner: Arc::new(store),
        }
    }

    /// Wrap a store already behind an [`Arc`] (e.g. one shared with other roles
    /// in the same process).
    pub fn from_arc(store: Arc<S>) -> Self {
        Self { inner: store }
    }
}

#[tonic::async_trait]
impl<S: ChunkStore + 'static> ChunkStoreRpc for ChunkStoreService<S> {
    async fn put_fragment(
        &self,
        request: Request<FragmentPutRequest>,
    ) -> std::result::Result<Response<FragmentPutResponse>, Status> {
        let request = request.into_inner();
        let id = conv::from_wire_fragment_id(request.id)?;
        // The store verifies the fragment's checksums before acknowledging
        // (write step 2); a verification or I/O failure becomes an error status.
        self.inner
            .put_fragment(id, Bytes::from(request.fragment))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(FragmentPutResponse {}))
    }

    async fn get_fragment(
        &self,
        request: Request<FragmentGetRequest>,
    ) -> std::result::Result<Response<FragmentGetResponse>, Status> {
        let request = request.into_inner();
        let id = conv::from_wire_fragment_id(request.id)?;
        let fragment = self
            .inner
            .get_fragment(id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        // `None` travels as an absent field, not a NOT_FOUND status — the
        // client maps it back to the trait's `Ok(None)`.
        Ok(Response::new(FragmentGetResponse {
            fragment: fragment.map(|bytes| bytes.to_vec()),
        }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> std::result::Result<Response<HealthResponse>, Status> {
        let health = self
            .inner
            .health()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(HealthResponse {
            status: conv::to_wire_health(health) as i32,
        }))
    }
}
