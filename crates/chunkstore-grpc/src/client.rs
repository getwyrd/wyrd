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
    TransientFault, WriteDeadlineExpired, WriteEffect,
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

/// Classify a `put_fragment` error status (issue #638) into the D server's two **deadline
/// outcomes**, which a caller tells apart from a genuine backend fault by
/// `wyrd_traits::is_write_deadline_expired` — exactly the trick `classify_get_status` plays
/// for `get_fragment`'s fault categories:
///
/// * `FAILED_PRECONDITION` → [`WriteEffect::NotApplied`]: the D server refused a write whose
///   authorization deadline had elapsed *before* it published anything (proposal 0016
///   decision 5). Nothing landed, and that is definite.
/// * `ABORTED` → [`WriteEffect::Unknown`]: the D server began publishing in time but could
///   not verify the publication completed before the deadline. The bytes **may** be on that
///   server, so the caller must not count the write and must not record that nothing landed
///   either. Reconstructing this as the clean refusal would be the one wire lie that
///   matters — a caller would record "nothing landed" over possibly-durable bytes.
///
/// The outcome's `detail` is the **server's own** rendering (its deadline and its clock
/// reading), carried verbatim: this side never read that clock, so it must not fabricate the
/// readings — only restore the *class* and the *effect*.
///
/// `put_fragment`'s other special status, `INVALID_ARGUMENT` (a malformed fragment), keeps
/// surfacing through the wire `Status` code as it did before this issue, and everything else
/// falls to [`transport_error`]'s generic mapping (the transient trio plus the fail-safe
/// terminal default).
///
/// **Gated on `deadline_millis` — the request this reply answers must have carried one.**
/// Neither code is exclusive to this protocol: `:47` already documents `ABORTED` as a
/// concurrency conflict whose retry belongs to the layer owning the precondition, and
/// `FAILED_PRECONDITION` is `get_fragment`'s block-read fault. Wyrd's own D server emits
/// neither for any other reason on this RPC (`server.rs:118`), but a foreign implementation
/// of the same service, or a proxy in the path, can — and decoding on the code alone would
/// then manufacture a deadline verdict out of an unrelated fault. That is the one direction
/// that must not be got wrong: `NotApplied` tells the caller **nothing landed**, so a
/// fabricated one records "definitely not written" over bytes that may well be durable.
/// A request with no deadline has no deadline outcome by construction, so its statuses go
/// to [`transport_error`] exactly as they did before this issue.
fn classify_put_status(id: FragmentId, status: Status, deadline_millis: Option<u64>) -> BoxError {
    if deadline_millis.is_none() {
        return transport_error(status);
    }
    let effect = match status.code() {
        Code::FailedPrecondition => WriteEffect::NotApplied,
        Code::Aborted => WriteEffect::Unknown,
        _ => return transport_error(status),
    };
    Box::new(WriteDeadlineExpired {
        id,
        effect,
        detail: status.message().to_string(),
    })
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
    /// `deadline_millis` travels to the D server as `FragmentPutRequest.deadline_millis`
    /// (issue #638) and is enforced **there**, at the last instant before the D server's
    /// store publishes the write — with the accept queue behind it, which is why a write
    /// parked in that queue is refused rather than applied late — and *verified* once that
    /// publication has completed, so an `Ok(())` from this method means the D server
    /// published the fragment strictly before its deadline. This is the half of
    /// `W_write` a caller cannot provide for itself (proposal 0016 `:1557-1564`).
    /// The caller-side half — a bounded, fail-closed await — is the
    /// channel's own request timeout, wired at composition by
    /// [`GrpcChunkStore::connect_with_timeout`] (`crates/server/src/cli.rs:1441`); a
    /// second per-call bound here was reviewed and rejected as a duplicate
    /// (`results/issue_508/review-rejected.md:10`), and cancelling the await *at* the
    /// deadline would additionally destroy the verdict this field exists to deliver —
    /// the server's definite "not applied", which a client-side timeout can never
    /// distinguish from "may still land".
    ///
    /// **Mixed-version caveat:** an old D server ignores the field, so a new client
    /// talking to one degrades to today's unenforced behaviour rather than failing. There
    /// is no capability exchange yet — a mixed-version fleet does **not** get the
    /// guarantee.
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        let mut client = self.client.clone();
        let request = FragmentPutRequest {
            id: Some(conv::to_wire_fragment_id(id)),
            fragment: fragment.to_vec(),
            deadline_millis,
        };
        client
            .put_fragment(Request::new(request))
            .await
            .map_err(|status| classify_put_status(id, status, deadline_millis))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `:51` — every code of proposal 0010's transient trio must sit in [`class_of`]'s
    /// transient arm. The integration tests exercise `UNAVAILABLE` (dead server) and
    /// `CANCELLED` (expired channel deadline) through a real transport, but no test in the
    /// tree produces `DEADLINE_EXCEEDED` (a server-set deadline) or `RESOURCE_EXHAUSTED`
    /// (admission-control load shedding) on the wire — removing either from the match arm
    /// kept everything green (#581). This pins all four.
    #[test]
    fn the_transient_trio_codes_all_classify_transient() {
        for code in [
            Code::Unavailable,
            Code::Cancelled,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
        ] {
            assert_eq!(
                class_of(code),
                ErrorClass::Transient,
                "{code:?} is in proposal 0010's transient trio and must classify transient"
            );
        }
    }

    /// `:54`/`:55` — the two non-transient arms: `DATA_LOSS` reconstructs the integrity
    /// class, and an unlisted code falls through to the fail-safe terminal default
    /// (including `ABORTED`, whose retry belongs to the layer owning the precondition).
    #[test]
    fn data_loss_is_integrity_and_the_rest_fail_safe_to_terminal() {
        assert_eq!(class_of(Code::DataLoss), ErrorClass::Integrity);
        for code in [Code::Aborted, Code::Internal, Code::NotFound] {
            assert_eq!(
                class_of(code),
                ErrorClass::Terminal,
                "{code:?} is not in the transient or integrity sets — the default is terminal"
            );
        }
    }

    /// A write that carried NO deadline can have no deadline outcome, whatever status comes
    /// back. Neither code is exclusive to this protocol — `ABORTED` is the generic
    /// concurrency conflict (`:47`) and `FAILED_PRECONDITION` is `get_fragment`'s block-read
    /// fault — so a foreign server or a proxy in the path can emit either for its own
    /// reasons. Decoding on the code alone manufactured a deadline verdict out of that:
    /// `NotApplied` asserts **nothing landed**, which over possibly-durable bytes is the
    /// one wire lie that costs a caller correctness.
    ///
    /// Red without the gate: `classify_put_status` reads only `status.code()`, so both
    /// assertions below see a `WriteDeadlineExpired`.
    #[test]
    fn a_write_with_no_deadline_never_decodes_a_deadline_outcome() {
        let id = FragmentId {
            chunk: 0x638,
            index: 0,
        };
        for code in [Code::FailedPrecondition, Code::Aborted] {
            let err = classify_put_status(id, Status::new(code, "unrelated"), None);
            assert!(
                !wyrd_traits::is_write_deadline_expired(err.as_ref()),
                "{code:?} on a request with no deadline must fall through to the generic \
                 mapping, not reconstruct a deadline outcome"
            );
        }
    }

    /// The other half of the gate: with a deadline actually sent, both codes still decode
    /// into their outcomes — `FAILED_PRECONDITION` the definite refusal, `ABORTED` the
    /// indeterminate one. Pins that the fix narrowed the decode rather than removing it.
    #[test]
    fn a_write_with_a_deadline_still_decodes_both_outcomes() {
        let id = FragmentId {
            chunk: 0x638,
            index: 0,
        };
        for code in [Code::FailedPrecondition, Code::Aborted] {
            let err = classify_put_status(id, Status::new(code, "expired"), Some(1));
            assert!(
                wyrd_traits::is_write_deadline_expired(err.as_ref()),
                "{code:?} on a deadline-carrying request is a deadline outcome"
            );
        }
    }
}
