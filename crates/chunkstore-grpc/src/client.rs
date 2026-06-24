//! [`GrpcChunkStore`]: a [`ChunkStore`] that lives on the *client* side of the
//! wire, dialing a D-server endpoint over tonic.

use async_trait::async_trait;
use bytes::Bytes;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};
use wyrd_proto::v0::chunk_store_client::ChunkStoreClient;
use wyrd_proto::v0::{
    FragmentDeleteRequest, FragmentGetRequest, FragmentListRequest, FragmentPutRequest,
    HealthRequest,
};
use wyrd_traits::{BoxError, ChunkStore, FragmentId, Health, IntegrityFault, Result};

use crate::conv;
use crate::error::TransportError;

/// Classify a `get_fragment` error status. A `DATA_LOSS` status is the wire form of a
/// stored-fragment **integrity** failure the D server detected on read (bit rot / a
/// misplaced fragment): surface it as the seam-level [`IntegrityFault`] so a consumer
/// (scrub, the read path) classifies it the *same* as a local store's corruption —
/// repair-and-continue, not retry — without depending on this crate's
/// [`TransportError`]. Every other status stays a [`TransportError`] (transient or
/// generic rpc), preserving the retry policy's existing classification.
fn classify_get_status(id: FragmentId, status: Status) -> BoxError {
    if status.code() == Code::DataLoss {
        Box::new(IntegrityFault {
            id,
            detail: status.message().to_string(),
        })
    } else {
        Box::new(TransportError::from(status))
    }
}

/// A [`ChunkStore`] implemented over a gRPC channel to one D server.
///
/// The trait's `&self` methods clone the inner tonic client per call — tonic
/// clients are cheap, reference-counted handles to a shared connection pool, so
/// one `GrpcChunkStore` serves concurrent fan-out calls (the M2.4/M2.5 read and
/// write paths) without external locking.
pub struct GrpcChunkStore {
    client: ChunkStoreClient<Channel>,
}

impl GrpcChunkStore {
    /// Dial `endpoint` (e.g. `"http://10.0.0.7:50051"`) and return a store that
    /// talks to the D server there.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let channel = Endpoint::try_from(endpoint.into())
            .map_err(TransportError::Connect)?
            .connect()
            .await
            .map_err(TransportError::Connect)?;
        Ok(Self::new(channel))
    }

    /// Wrap an already-built channel — the seam a host uses to inject a
    /// pre-configured (load-balanced, lazily-connected, or simulated) channel.
    pub fn new(channel: Channel) -> Self {
        Self {
            client: ChunkStoreClient::new(channel),
        }
    }
}

#[async_trait]
impl ChunkStore for GrpcChunkStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        let mut client = self.client.clone();
        let request = FragmentPutRequest {
            id: Some(conv::to_wire_fragment_id(id)),
            fragment: fragment.to_vec(),
        };
        client
            .put_fragment(Request::new(request))
            .await
            .map_err(TransportError::from)?;
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        let mut client = self.client.clone();
        let request = FragmentGetRequest {
            id: Some(conv::to_wire_fragment_id(id)),
        };
        let response = client
            .get_fragment(Request::new(request))
            .await
            .map_err(|status| classify_get_status(id, status))?;
        // Absent bytes preserve the trait's `Ok(None)` not-found contract — a
        // miss is not a transport error.
        Ok(response.into_inner().fragment.map(Bytes::from))
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        let mut client = self.client.clone();
        let response = client
            .list_fragments(Request::new(FragmentListRequest {}))
            .await
            .map_err(TransportError::from)?;
        response
            .into_inner()
            .ids
            .into_iter()
            .map(|wire| conv::from_wire_fragment_id(Some(wire)).map_err(Into::into))
            .collect()
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        let mut client = self.client.clone();
        let request = FragmentDeleteRequest {
            id: Some(conv::to_wire_fragment_id(id)),
        };
        client
            .delete_fragment(Request::new(request))
            .await
            .map_err(TransportError::from)?;
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        let mut client = self.client.clone();
        let response = client
            .health(Request::new(HealthRequest {}))
            .await
            .map_err(TransportError::from)?;
        Ok(conv::from_wire_health(response.into_inner().status)?)
    }
}
