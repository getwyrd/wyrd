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
    BlockReadFault, BoxError, ChunkStore, ErrorClass, FragmentId, Health, IntegrityFault, Result,
    TransientFault,
};

use crate::conv;
use crate::error::TransportError;

/// The seam [`ErrorClass`] a gRPC status `code` reconstructs to, client-side.
///
/// The class rides the **existing** status conventions — no proto change (proposal 0010
/// §"Scope boundary" item 6, §Backward compatibility) — so this is the one place that says
/// which codes the wire's vocabulary maps onto which seam class.
///
/// [`Transient`](ErrorClass::Transient) is exactly proposal 0010's trio — unreachable,
/// timed out, busy — spelled in the codes this stack **actually** produces, which is not
/// the same as the codes one would guess:
///
/// * `UNAVAILABLE` — *unreachable*. tonic maps a failure to connect to `UNAVAILABLE`
///   itself (`tonic-0.14.6/src/status.rs:652-656`, citing the gRPC spec: "most likely a
///   transient condition that can be corrected if retried with a backoff").
/// * `CANCELLED` — *timed out*, counter-intuitively. tonic renders an expired channel
///   deadline as `Status::cancelled("Timeout expired")`
///   (`tonic-0.14.6/src/status.rs:644-646`), **not** as `DEADLINE_EXCEEDED`, and the
///   d-server's admission-control request-timeout cut arrives as `CANCELLED` or
///   `DEADLINE_EXCEEDED` (`crates/server/tests/dserver.rs:381`). Excluding it on the
///   textbook reading of `CANCELLED` ("the caller gave up") would make the seam's
///   transient class miss the timeout case altogether — the very case proposal 0010 names.
/// * `DEADLINE_EXCEEDED` — *timed out*, the spelling a server-set deadline uses.
/// * `RESOURCE_EXHAUSTED` — *busy*: the D server's admission control sheds load with it
///   (`crates/server/tests/dserver.rs:319`).
///
/// `DATA_LOSS` → [`Integrity`](ErrorClass::Integrity), the precedent this generalizes.
/// Everything else → [`Terminal`](ErrorClass::Terminal), the **fail-safe** default —
/// including `ABORTED`, a concurrency conflict whose retry (if any) belongs to the layer
/// that owns the precondition, never to a transport retry loop.
fn class_of(code: Code) -> ErrorClass {
    match code {
        Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::ResourceExhausted => {
            ErrorClass::Transient
        }
        Code::DataLoss => ErrorClass::Integrity,
        _ => ErrorClass::Terminal,
    }
}

/// Box a wire [`Status`] as the seam's error, preserving **both** the transport detail and
/// the seam class.
///
/// A known-transient status is wrapped in a [`TransientFault`] — the seam type that makes
/// "try again" survive the wire, reconstructed client-side exactly as `DATA_LOSS` already
/// reconstructs an [`IntegrityFault`]. The [`TransportError`] becomes its
/// [`source`](std::error::Error::source), so it stays reachable by a chain-walking
/// downcast and nothing that the class costs is detail: the wire `Status`, its code and
/// its message all survive underneath.
///
/// Every other status keeps boxing a bare [`TransportError`] — unchanged behaviour, and it
/// classifies [`Terminal`](ErrorClass::Terminal) through `classify`'s fail-safe default.
fn transport_error(status: Status) -> BoxError {
    if class_of(status.code()).is_transient() {
        let detail = format!(
            "the D server answered {:?}: {}",
            status.code(),
            status.message()
        );
        Box::new(TransientFault::with_source(
            detail,
            TransportError::from(status),
        ))
    } else {
        Box::new(TransportError::from(status))
    }
}

/// A channel that could not be **dialed** is unreachable — the seam's transient class by
/// definition (proposal 0010: transient covers unreachable / timed out / busy). No status
/// crosses the wire to carry the class (no server ever answered), so the client names it
/// here instead, keeping the [`TransportError::Connect`] as the source.
///
/// This is the *dial* only. A **malformed endpoint** is rejected by `Endpoint::try_from`
/// through the same `tonic::transport::Error` type, and it is emphatically not transient:
/// it is invalid config, which proposal 0010 names terminal, and no amount of retrying
/// fixes a URI. That site keeps boxing a bare [`TransportError::Connect`] and takes the
/// fail-safe terminal default — which is why this helper is not simply applied to every
/// `transport::Error` the connect path can raise.
///
/// A **DNS resolution failure** sits between those two lines and lands, decidedly, on the
/// transient side (#582, settled at sign-off). A typo'd hostname (NXDOMAIN) *is* invalid
/// config, and classifying it transient licenses retries against a name that will never
/// resolve — but the same wire answer is produced by a resolver outage, stale negative
/// caching, or the rollout window in which an orchestrator has not yet published a
/// restarting peer's name, and those are exactly "unreachable, may be back a second
/// later". The retry policy consuming this class (#575) is *bounded*, so the typo costs a
/// few wasted redials before surfacing; the opposite misclassification would turn every
/// rollout-window blip into a false permanent failure. Telling the two apart would mean
/// matching resolver `io::Error` text inside tonic's opaque error chain — platform- and
/// version-fragile — to move only the least costly of the two mistakes. Pinned by
/// `tests/error_class.rs::a_dns_resolution_failure_classifies_transient_on_dial`.
fn dial_error(e: tonic::transport::Error) -> BoxError {
    Box::new(TransientFault::with_source(
        "the D-server endpoint could not be dialed",
        TransportError::Connect(e),
    ))
}

/// Classify a `get_fragment` error status into one of four mutually distinguishable fault
/// categories (the seam contract, `wyrd_traits` / ADR-0010):
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
/// * a known-transient status → [`TransientFault`] wrapping the [`TransportError`]: the D
///   server is unreachable, slow, or shedding load. Consumer: the retry policy may act on
///   it, because it is a *known*-transient signal rather than an unclassified one.
///
/// * everything else → [`TransportError`]: a generic rpc fault, which classifies
///   [`Terminal`](ErrorClass::Terminal) by the fail-safe default.
fn classify_get_status(id: FragmentId, status: Status) -> BoxError {
    match status.code() {
        Code::DataLoss => Box::new(IntegrityFault {
            id,
            detail: status.message().to_string(),
        }),
        Code::FailedPrecondition => Box::new(BlockReadFault::new(id, status.message())),
        _ => transport_error(status),
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
            .map_err(dial_error)?;
        Ok(Self::new(channel))
    }

    /// Like [`Self::connect`], but applies a per-request `timeout` (and an equal
    /// connect timeout) to the channel.
    ///
    /// Tonic's default channel has **no** request deadline: an RPC to a server that has
    /// stopped responding mid-call — a `docker pause`d node or an injected network
    /// partition that leaves the connection established but the peer silent — would hang
    /// the future indefinitely. With a timeout, such a request instead fails with a
    /// transient [`Status`] — `CANCELLED`, which is how tonic renders an expired channel
    /// deadline (`tonic-0.14.6/src/status.rs:644-646`; **not** `DEADLINE_EXCEEDED`, as
    /// this note claimed before #577 checked it) — classified as the seam's
    /// [`ErrorClass::Transient`] and never an [`IntegrityFault`]. So a caller — e.g. the
    /// custodian reconstruction path driven by the Tier-1 consistency scenario — observes
    /// an *alive-but-unreachable* node and aborts the repair before commit rather than
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
            .map_err(dial_error)?;
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
            .map_err(transport_error)?;
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
            .map_err(transport_error)?;
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
            .map_err(transport_error)?;
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        let mut client = self.client.clone();
        let response = client
            .health(Request::new(HealthRequest {}))
            .await
            .map_err(transport_error)?;
        Ok(conv::from_wire_health(response.into_inner().status)?)
    }
}
