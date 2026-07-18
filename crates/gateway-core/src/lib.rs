//! The **shared gateway seam** — the protocol-agnostic object surface every
//! client-facing gateway front-door (S3 today; NFS / a future verb next) is generic
//! over, so a wire layer never names a concrete backend composition.
//!
//! # Why this crate exists (issue #364 carry-forward, T5-a crate boundary)
//! The S3 wire surface must **not calcify inside `crates/server`** (the composition
//! root). Extracting it to a dedicated [`wyrd-gateway-s3`] crate needs a seam both the
//! wire layer and the composing binary can name without either depending on the other's
//! internals: the wire crate is generic over [`ObjectGateway`] (defined here), and
//! `wyrd-server`'s `Gateway` *implements* it. Because the seam lives in this neutral
//! crate — not in `gateway-s3` and not in `server` — a second gateway front-door
//! (`gateway-nfs`, …) implements the *same* seam without depending on the S3 crate, and
//! the ADR-0010 rule holds: concretes are wired only at the composition root, never in a
//! wire layer.
//!
//! The seam is deliberately **narrow and neutral**: object PUT (streaming), object GET
//! (streaming), object DELETE (idempotent). No S3 vocabulary (SigV4, `aws-chunked`,
//! buckets) leaks in — those are the S3 crate's concern. The one cross-layer value the
//! seam carries is [`ContentHash`]: an *optional* integrity check a protocol may have
//! authenticated the body against, verified by the implementer **before commit** and
//! **after** the body has streamed (never buffered).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::Stream;
use wyrd_traits::Result;

/// A GET response body: a boxed byte stream. Boxed (not a concrete stream type) so the
/// seam names no runtime detail — an implementer over `tokio` channels and one over a
/// synchronous reader both fit, and the S3 layer feeds it straight into an HTTP body.
pub type ObjectStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// A GET response: the committed object's total byte length **and** its streamed body.
///
/// The length is carried alongside the stream so a wire layer can set an accurate framing
/// header (S3's `Content-Length`). Once the `200 OK` status line is on the wire the body
/// stream carries no in-band error channel, so a fault *mid-stream* (e.g. a fragment
/// reclaimed by a racing DELETE) can only end the body early. With an accurate declared
/// length a client **detects** that short read as a truncated response instead of mistaking
/// it for a complete object; without it, a single-chunk object silently truncates to zero
/// bytes and the client cannot tell (issue #364 carry-forward: streaming GET fault framing).
pub struct ObjectRead {
    /// The object's total length in bytes (its committed inode size).
    pub size: u64,
    /// The object body as a bounded, chunk-at-a-time byte stream.
    pub stream: ObjectStream,
    /// The object's content digest (opaque change-token), if recorded — the wire layer
    /// quotes it as S3's `ETag`. `None` for a record written before object metadata was
    /// modelled, in which case the wire layer omits the header (ADR-0047).
    pub etag: Option<String>,
    /// The `Content-Type` the writer declared, round-tripped verbatim. `None` falls back
    /// to `application/octet-stream` on the wire.
    pub content_type: Option<String>,
    /// Content-publication time (epoch millis); the wire layer renders it as an RFC-7231
    /// `Last-Modified`. `None` when unrecorded.
    pub modified: Option<u64>,
}

/// The payload-integrity instruction a gateway hands its object-store for a streaming PUT.
///
/// Neutral by design: a protocol that authenticated the body against a known hash (S3's
/// signed `x-amz-content-sha256`) asks the store to [`Expected`](ContentHash::Expected)
/// that hash; a protocol that already authenticated the body some other way (S3's
/// per-chunk `aws-chunked` signatures) or deliberately left it unsigned asks for
/// [`Unverified`](ContentHash::Unverified). The store verifies an `Expected` hash against
/// the streamed body **before** it commits, so a body that does not match the claim is
/// rejected before it is ever published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentHash {
    /// The streamed body must hash (SHA-256, lower-case hex) to this value; the write is
    /// rejected before commit if it does not.
    Expected(String),
    /// No content-hash check — the body's integrity is guaranteed elsewhere (unsigned,
    /// or already authenticated chunk-by-chunk) or deliberately unchecked.
    Unverified,
}

/// The neutral errors a gateway object-store surfaces through the boxed [`Result`], so a
/// wire layer can map them onto its protocol's status codes without depending on the
/// implementer's internals. A wire layer downcasts the boxed error to this.
#[derive(Debug, PartialEq, Eq)]
pub enum GatewayError {
    /// A concurrent writer won the commit; this write was rejected rather than allowed to
    /// corrupt the object.
    Conflict,
    /// A directory entry pointed at an object record the store does not hold.
    DanglingDirent,
    /// A streaming PUT's delivered bytes did not hash to the [`ContentHash::Expected`]
    /// value — rejected before the write was published.
    PayloadMismatch,
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GatewayError::Conflict => write!(f, "put rejected: a concurrent writer won the commit"),
            GatewayError::DanglingDirent => write!(f, "dangling directory entry: object missing"),
            GatewayError::PayloadMismatch => {
                write!(f, "payload does not match the expected content hash")
            }
        }
    }
}

impl std::error::Error for GatewayError {}

/// The object surface a client-facing gateway front-door drives — the seam a wire layer
/// (e.g. [`wyrd-gateway-s3`]) is generic over. An implementer (`wyrd-server`'s `Gateway`)
/// maps each method onto its composed write/read/delete paths; the wire layer never sees
/// a concrete backend.
///
/// Every method streams — a PUT source and a GET response are byte streams, never a
/// buffered whole object — so the "stream, don't buffer" invariant (0015:789 OOM cliff)
/// holds at the seam, not merely inside one implementation.
pub trait ObjectGateway: Send + Sync + 'static {
    /// Store the object whose bytes arrive over `source` under `key`, creating it or
    /// overwriting an existing one, without ever holding the whole object in memory.
    /// `expected` is the payload-integrity check (verified before commit); a body that
    /// fails it is rejected before publication. `content_type` is the writer's declared
    /// `Content-Type` (round-tripped verbatim, `None` if the client sent none). Returns
    /// the committed object's **ETag** — the content digest as an opaque change-token; the
    /// wire layer quotes it as S3's `ETag` header (ADR-0047). A concurrent writer loses
    /// with [`GatewayError::Conflict`] rather than corrupting the object.
    fn put_object_streaming<S>(
        &self,
        key: &str,
        source: S,
        expected: ContentHash,
        content_type: Option<String>,
    ) -> impl Future<Output = Result<String>> + Send
    where
        S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static;

    /// Read the object under `key` as its total length plus a bounded, chunk-at-a-time body
    /// stream (so a GET never materialises the whole object), or `None` if `key` has no
    /// committed object. The length lets the wire layer frame the response so a body truncated
    /// by a mid-stream fault is detectable (see [`ObjectRead`]).
    fn get_object_streaming(
        self: Arc<Self>,
        key: &str,
    ) -> impl Future<Output = Result<Option<ObjectRead>>> + Send;

    /// Remove the object under `key`. **Idempotent**: `Ok(true)` if an object was removed,
    /// `Ok(false)` if `key` was already absent — deleting a missing key is a success.
    fn delete_object(&self, key: &str) -> impl Future<Output = Result<bool>> + Send;
}
