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
use wyrd_traits::{
    BlockReadFault, BoxError, ChunkStore, FragmentId, Health, IntegrityFault, Result,
};

use crate::conv;
use crate::error::TransportError;

/// Classify a `get_fragment` error status into one of three mutually
/// distinguishable fault categories (the seam contract, `wyrd_traits` / ADR-0010):
///
/// * `DATA_LOSS` → [`IntegrityFault`]: stored-data corruption the D server
///   detected on read (bit rot / a misplaced fragment). Consumer: repair-and-
///   continue, emit a corruption finding (scrub `emit_corruption`).
///
/// * `FAILED_PRECONDITION` → [`BlockReadFault`]: the block device physically
///   could not return the bytes (`EIO` / dead sector). Consumer: read around it
///   (permanent, no retry), do NOT emit a corruption finding — the same branch
///   a local `EIO` takes at `scrub.rs:108` (`Err(e) => return Err(e)`).
///
/// * everything else → [`TransportError`]: transient or generic rpc fault.
///   Consumer: the retry policy decides.
fn classify_get_status(id: FragmentId, status: Status) -> BoxError {
    match status.code() {
        Code::DataLoss => Box::new(IntegrityFault {
            id,
            detail: status.message().to_string(),
        }),
        Code::FailedPrecondition => Box::new(BlockReadFault::new(id, status.message())),
        _ => Box::new(TransportError::from(status)),
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

    /// Like [`Self::connect`], but applies a per-request `timeout` (and an equal
    /// connect timeout) to the channel.
    ///
    /// Tonic's default channel has **no** request deadline: an RPC to a server that has
    /// stopped responding mid-call — a `docker pause`d node or an injected network
    /// partition that leaves the connection established but the peer silent — would hang
    /// the future indefinitely. With a timeout, such a request instead fails with a
    /// transient `DEADLINE_EXCEEDED` [`Status`] (classified as a retryable
    /// [`TransportError`], not an [`IntegrityFault`]), so a caller — e.g. the custodian
    /// reconstruction path driven by the Tier-1 consistency scenario — observes an
    /// *alive-but-unreachable* node and aborts the repair before commit rather than
    /// stalling.
    pub async fn connect_with_timeout(
        endpoint: impl Into<String>,
        timeout: std::time::Duration,
    ) -> Result<Self> {
        let channel = Endpoint::try_from(endpoint.into())
            .map_err(TransportError::Connect)?
            .timeout(timeout)
            .connect_timeout(timeout)
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
