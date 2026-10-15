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

#![forbid(unsafe_code)]

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

/// A **HEAD** response: the same header-bearing metadata [`ObjectRead`] carries, without a
/// body stream. Deliberately its own type rather than `ObjectRead` with the `stream` field
/// ignored — resolving one would force a metadata-only lookup to conjure a stream it never
/// plays out (or spawn a reader task solely to satisfy the type), which defeats the point of
/// a HEAD costing metadata round-trips, not data reads (issue #506).
pub struct ObjectMeta {
    /// The object's total length in bytes (its committed inode size) — the wire layer's
    /// `Content-Length`.
    pub size: u64,
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

/// A byte range a client asked for, in **object-byte terms** (issue #510) — the seam names
/// NO HTTP `Range` vocabulary (the suffix/open/closed forms below are byte-offset math, not
/// the `bytes=` header grammar). The wire layer parses the client's `Range:` header into one
/// of these and maps the [`RangeOutcome`] back onto 206/416/`Content-Range`.
///
/// The range is carried UNRESOLVED (not a pre-clamped `(offset, len)`): the implementer
/// resolves it against the object's own size *at read time*, so the span, the metadata, and
/// the streamed bytes all come from ONE inode resolve. A pre-resolved `(offset, len)` derived
/// from a *separate* metadata lookup would reopen a TOCTOU window — a racing overwrite between
/// that lookup and the body read could emit a `206` whose `Content-Range`/`ETag` name one
/// version while the bytes are another, poisoning an ETag-keyed cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=a-b` — the inclusive byte range `[a, b]` (`b >= a`; the wire layer rejects an
    /// inverted range as malformed before it reaches here).
    FromTo(u64, u64),
    /// `bytes=a-` — from byte `a` to the end of the object.
    From(u64),
    /// `bytes=-n` — the final `n` bytes of the object. `n == 0` resolves to
    /// [`RangeOutcome::Unsatisfiable`] (a grammatically valid but empty suffix → 416).
    Suffix(u64),
}

/// The result of a ranged read (issue #510): the object's metadata AND the range outcome,
/// both from a **single** inode resolve so a `206`'s headers and body can never mix object
/// versions. The outer `None` (on the seam's `Result<Option<RangeRead>>`) means no committed
/// object — the wire layer answers `404 NoSuchKey`.
pub struct RangeRead {
    /// The object's metadata, resolved from the SAME snapshot as [`Self::outcome`]'s stream,
    /// so the wire layer frames the `206`'s `Content-Range`/`ETag`/`Last-Modified` (and a
    /// `416`'s `bytes */{size}`) against the version the bytes came from.
    pub meta: ObjectMeta,
    /// Whether the range was satisfiable against this object's size, and — when it was — the
    /// covering-chunk stream.
    pub outcome: RangeOutcome,
}

/// Whether a [`ByteRange`] resolved to a satisfiable span (with its stream) or not.
pub enum RangeOutcome {
    /// A satisfiable span `[offset, offset + len)` with `len >= 1`, plus a stream over ONLY
    /// the chunks overlapping the span (never the whole object, discarded wire-side).
    Satisfiable {
        /// The absolute start byte of the span.
        offset: u64,
        /// The span length (`>= 1`) — the wire layer's `206` `Content-Length`.
        len: u64,
        /// The span's bytes, chunk-at-a-time.
        stream: ObjectStream,
    },
    /// Syntactically valid but not satisfiable against this object's size (start at/after the
    /// end, or any range against a zero-byte object) — the wire layer answers `416` with
    /// `Content-Range: bytes */{size}` (`size` from [`RangeRead::meta`]).
    Unsatisfiable,
}

/// Resolve a [`ByteRange`] against a known object `size` into a concrete `(offset, len)`
/// span, clamping to the object as RFC 9110 §14.1.2 requires (an end past the last byte is
/// truncated; a suffix larger than the object is the whole object). `None` when the range is
/// unsatisfiable (start at/after the end, a zero-length suffix, or any range against a
/// zero-byte object) — the caller answers `416`. Shared by the seam's default implementation
/// and every overriding gateway so the span math has ONE definition.
pub fn resolve_byte_range(range: ByteRange, size: u64) -> Option<(u64, u64)> {
    match range {
        ByteRange::FromTo(a, b) => {
            if a >= size || b < a {
                None
            } else {
                let end = b.min(size - 1); // clamp to the last byte
                Some((a, end - a + 1))
            }
        }
        ByteRange::From(a) => {
            if a >= size {
                None
            } else {
                Some((a, size - a))
            }
        }
        ByteRange::Suffix(n) => {
            // A zero-length suffix (`bytes=-0`) is unsatisfiable, as is any suffix against a
            // zero-byte object.
            if size == 0 || n == 0 {
                None
            } else {
                let n = n.min(size); // a suffix longer than the object is the whole object
                Some((size - n, n))
            }
        }
    }
}

/// Adapt a full-object body `stream` into the sub-span `[offset, offset + len)` by skipping
/// the leading `offset` bytes and yielding the next `len` — the wire-side slice the seam's
/// **default** [`ObjectGateway::get_object_range`] uses for a gateway with no chunk-aware
/// ranged read. A mid-stream error is forwarded once and ends the slice. This buffers no more
/// than one source chunk at a time (each `Bytes::slice` is a cheap refcount), so the "stream,
/// don't buffer" invariant holds even in the default.
fn slice_object_stream(inner: ObjectStream, offset: u64, len: u64) -> ObjectStream {
    use futures_util::StreamExt;
    Box::pin(futures_util::stream::unfold(
        (inner, offset, len),
        |(mut inner, mut skip, mut remaining)| async move {
            loop {
                if remaining == 0 {
                    return None;
                }
                match inner.next().await {
                    None => return None,
                    // Forward the error, then end the slice (state `remaining = 0`).
                    Some(Err(err)) => return Some((Err(err), (inner, 0, 0))),
                    Some(Ok(mut chunk)) => {
                        if skip > 0 {
                            let s = (skip as usize).min(chunk.len());
                            chunk = chunk.slice(s..);
                            skip -= s as u64;
                            if chunk.is_empty() {
                                continue; // this source chunk was entirely before the span
                            }
                        }
                        let take = (remaining as usize).min(chunk.len());
                        let out = chunk.slice(0..take);
                        remaining -= take as u64;
                        return Some((Ok(out), (inner, skip, remaining)));
                    }
                }
            }
        },
    ))
}

/// One object in a container listing (issue #507): the object's key **relative to the
/// container** (the container segment already stripped) plus the wire-relevant metadata a
/// listing row carries. The S3 wire layer groups these by delimiter, applies the combined
/// `max-keys` slice, and renders `<Contents>`; this type names **no** S3 vocabulary
/// (buckets, delimiters, tokens) — those stay in the S3 crate (ADR-0010, ADR-0046 dec. 6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    /// The object key, relative to the container (the container segment stripped).
    pub key: String,
    /// The object's total length in bytes (its committed inode size).
    pub size: u64,
    /// The object's content digest (opaque change-token), if recorded — the wire layer
    /// quotes it as S3's `ETag`. `None` for a record written before object metadata was
    /// modelled (ADR-0047).
    pub etag: Option<String>,
    /// Content-publication time (epoch millis); the wire layer renders it as an ISO-8601
    /// `<LastModified>`. `None` when unrecorded.
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

    /// Read the byte `range` of the object under `key` (issue #510): the object's metadata
    /// **and** the range outcome, resolved from ONE inode snapshot so a `206`'s headers and
    /// body can never mix versions. `None` if `key` has no committed object.
    ///
    /// The seam names no HTTP vocabulary: the wire layer parses the client's `Range:` header
    /// into a [`ByteRange`] and maps the returned [`RangeOutcome`] back onto 206/416. The
    /// implementer resolves the range against its OWN freshly-read size ([`resolve_byte_range`]),
    /// so the span, the `Content-Range` size, and the streamed bytes are all one version — a
    /// range pre-resolved against a *separate* metadata lookup would reopen the head-then-read
    /// TOCTOU (a racing overwrite could emit a version-mixed 206 that poisons an ETag-keyed
    /// cache). A satisfiable read streams **only the chunks overlapping the span**, never the
    /// whole object discarded wire-side (issue #510 anti-wire-side-discard oracle), and its
    /// `206` `Content-Length` is the SPAN length, so a body cut short by a mid-stream fault is
    /// still a detectable truncation of the requested span (issue #364, applied to the span).
    ///
    /// The **default** is correctness-preserving, not a landmine: it reads the whole object
    /// through [`get_object_streaming`](Self::get_object_streaming) (one resolve → meta + full
    /// stream) and slices the span off wire-side. That is exactly the whole-object read the
    /// chunk-aware override avoids for large objects — but as a default it is *correct* and
    /// *atomic* (meta and bytes from the one streaming resolve), so a gateway that does not
    /// override still answers a ranged GET of an existing object with a correct `206`/`416`
    /// rather than a `404` after the wire layer has advertised `Accept-Ranges: bytes`. The
    /// composition root's real gateway overrides it with a chunk-map walk that fetches only the
    /// covering chunks.
    fn get_object_range(
        self: Arc<Self>,
        key: &str,
        range: ByteRange,
    ) -> impl Future<Output = Result<Option<RangeRead>>> + Send {
        async move {
            let Some(read) = self.get_object_streaming(key).await? else {
                return Ok(None);
            };
            let ObjectRead {
                size,
                stream,
                etag,
                content_type,
                modified,
            } = read;
            let meta = ObjectMeta {
                size,
                etag,
                content_type,
                modified,
            };
            let outcome = match resolve_byte_range(range, size) {
                None => RangeOutcome::Unsatisfiable,
                Some((offset, len)) => RangeOutcome::Satisfiable {
                    offset,
                    len,
                    stream: slice_object_stream(stream, offset, len),
                },
            };
            Ok(Some(RangeRead { meta, outcome }))
        }
    }

    /// Resolve `key`'s metadata **only** — size, ETag, Content-Type, and publication time
    /// (ADR-0047) — without opening its fragment stream. `None` if `key` has no committed
    /// object. This is the seam a wire layer's **HEAD** answers from (issue #506): unlike
    /// [`get_object_streaming`](Self::get_object_streaming), resolving it costs metadata
    /// round-trips, not data reads, so a HEAD of a large object is cheap.
    fn head_object(&self, key: &str) -> impl Future<Output = Result<Option<ObjectMeta>>> + Send;

    /// Remove the object under `key`. **Idempotent**: `Ok(true)` if an object was removed,
    /// `Ok(false)` if `key` was already absent — deleting a missing key is a success.
    fn delete_object(&self, key: &str) -> impl Future<Output = Result<bool>> + Send;
}

/// The **container** side of the gateway seam — ADR-0046 decision 6: [`ObjectGateway`] stays
/// object-only and bucket-free, and container operations arrive as this narrow companion
/// trait speaking container vocabulary (list now; create/head/delete arrive with their
/// issues, #511). Implemented by `wyrd-server` at the composition root per ADR-0010; the S3
/// crate alone projects containers as buckets.
pub trait ContainerGateway: Send + Sync + 'static {
    /// List `container`'s **complete**, lexicographically-sorted object set — each key
    /// (relative to the container) with its size/etag/modified — as one materialized,
    /// `SCAN_CAP`-bounded [`Vec`] (issue #507, ADR-0046). Grouping (`delimiter`), the
    /// combined `max-keys` slice, and pagination all happen in ONE place — the wire layer —
    /// over this single sorted view, so `max-keys` (which counts `Contents` + `CommonPrefixes`
    /// combined) and cross-page common-prefix dedup stay correct (ADR-0046 seam decision).
    ///
    /// `None` when `container` has **no record** — the wire layer maps that to S3's
    /// `NoSuchBucket`. `Some(vec![])` is an existing but empty container, which lists as an
    /// empty `200`, not a `404`: the existence read (ADR-0046 decision 4) is the implementer's,
    /// so this seam names no bucket vocabulary and a non-container gateway simply has none.
    ///
    /// The dirent scan itself adds no new cost class over an ordinary `scan`: it is already
    /// materialized and `SCAN_CAP`-bounded, and a scan past the cap surfaces as `Err` (the
    /// complete-or-`Err` contract) rather than a silently truncated listing.
    ///
    /// **Bounded Alpha debt (explicit, not hidden):** the real implementation resolves each
    /// dirent's size/etag/modified with one *sequential* inode point-read, and — because the
    /// wire layer pages over the whole sorted view — the scan + N inode reads + sort re-run
    /// for **every** page of a paginated listing (`≤SCAN_CAP` reads/page). On an in-memory
    /// backend this is cheap; on a networked metadata backend it is up to N serial round-trips
    /// per page. Batching the inode reads and/or caching the sorted view across a listing's
    /// pages is deferred (tracked with the streaming-`scan` evolution, ADR-0046 consequences)
    /// — acceptable at Alpha under the `SCAN_CAP` ceiling, called out here so it is a known
    /// bound, not a surprise.
    ///
    /// A default of `Ok(None)` (no such container) keeps a gateway that has no container
    /// concept — and the wire crate's own object-focused test doubles — free of a bespoke
    /// impl; the composition root's real gateway overrides it.
    fn list_container(
        &self,
        container: &str,
    ) -> impl Future<Output = Result<Option<Vec<ListedObject>>>> + Send {
        let _ = container;
        std::future::ready(Ok(None))
    }
}

// ---------------------------------------------------------------------------------------------
// The multipart side of the seam (issue #666, the first slice of #508).
//
// Every answer below is a VALUE, never an error, and none names an HTTP status: the wire layer
// maps each arm onto its protocol's answer. The vocabulary mirrors `wyrd_core::multipart`'s
// typed outcomes arm for arm, with plain types in place of core's validated ones, so this crate
// stays free of a `wyrd-core` dependency (ADR-0010: concretes meet only at the composition root).
//
// No enum here is `#[non_exhaustive]`, deliberately, as in core: every consumer is in this
// workspace, and a new arm must break every wire mapping at compile time, never fall into a
// `_ =>` arm that answers it with a silently wrong status.
// ---------------------------------------------------------------------------------------------

/// Why a part a multipart Complete named is invalid. Mirrors core's
/// `multipart::InvalidPart` arm for arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultipartInvalidPart {
    /// The named part was never committed under this upload, or was superseded.
    Absent,
    /// The ETag the client named is not the one the part recorded.
    DigestMismatch,
    /// The named part numbers are not strictly ascending: out of order, or one named twice.
    OutOfOrder,
}

/// Which ceiling produced a [`MultipartRefusal::SlowDown`]: designed backpressure, not a
/// failure. Mirrors core's `multipart::Backpressure` arm for arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultipartBackpressure {
    /// The fleet's live-upload count is at its stored limit.
    SessionCap {
        /// The live upload count the ledger records.
        count: u64,
        /// The limit the ledger was checked against.
        max_sessions: u64,
    },
    /// Every in-flight part slot of this upload is taken.
    InflightParts {
        /// How many parts one upload may have in flight at once.
        max_inflight_parts: u32,
    },
    /// The create lost the serialized admission race this many times.
    AdmissionContention {
        /// Attempts spent.
        attempts: u32,
    },
}

/// A typed multipart answer other than success, shared by every [`MultipartGateway`] verb.
///
/// Mirrors core's `multipart::Refusal` arm for arm, plus two arms of the seam's own:
/// [`PartNumberOutOfRange`](Self::PartNumberOutOfRange) (core only ever sees a validated part
/// number, so it has no such arm) and [`NotImplemented`](Self::NotImplemented) (the answer of a
/// gateway that does not serve multipart).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartRefusal {
    /// No upload can serve this request: the upload id is unknown, does not parse as an upload
    /// id at all, names an upload that has left the states this verb acts on, or names one that
    /// already completed a **different** assembly.
    NoSuchUpload,
    /// The target container does not exist. Core's arm is `NoSuchBucket`; the seam speaks
    /// container vocabulary, and the S3 crate alone projects containers as buckets.
    NoSuchContainer,
    /// The upload is being completed by a publisher that still owns it; no client verb resumes
    /// it.
    OperationAborted,
    /// The named-part list of a Complete is invalid. The upload stays usable.
    InvalidPart {
        /// The offending part number.
        part_number: u32,
        /// What was wrong with it.
        reason: MultipartInvalidPart,
    },
    /// A part, or the upload's cumulative staged chunks, is past a ceiling the protocol
    /// enforces by refusal. The upload stays usable and abortable.
    EntityTooLarge {
        /// The chunk count the request would have installed.
        chunks: u64,
        /// The ceiling it crossed.
        limit: u64,
    },
    /// Designed backpressure.
    SlowDown {
        /// Which ceiling engaged.
        pressure: MultipartBackpressure,
    },
    /// This process's multipart budget disagrees with the one the fleet ledger records, so it
    /// refuses rather than defer to either. Core's arm carries both budgets; they are an
    /// operator's diagnosis (core raises the alarm), not anything a client is answered with, so
    /// the seam carries the arm without them.
    ProfileSkew,
    /// The upload has spent all its Complete attempts; its only exit is Abort.
    CompleteAttemptsExhausted,
    /// The seam's own arm: a part number outside the range a part may take (S3 numbers parts
    /// from 1), named by `upload_part` or in `complete_multipart_upload`'s list.
    PartNumberOutOfRange {
        /// The offending part number, as the caller sent it.
        part_number: u32,
    },
    /// The seam's own arm: this gateway does not serve multipart uploads. It is what every
    /// [`MultipartGateway`] default body answers.
    NotImplemented,
}

/// What a completed upload published.
///
/// Core's publication also names the inode and version it flipped; the seam names no inode
/// (as [`ObjectRead`] and [`ObjectMeta`] name none), so those stay behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPublication {
    /// The published object's ETag, its `-N` part-count suffix included. The wire layer quotes
    /// it as S3's `ETag`.
    pub etag: String,
    /// When the publication landed (epoch millis).
    pub modified: u64,
}

/// One part in a [`MultipartGateway::list_parts`] answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedPart {
    /// The part number.
    pub part_number: u32,
    /// The part's ETag (its content digest, lowercase hex), unquoted.
    pub etag: String,
    /// The part's length in bytes.
    pub size: u64,
    /// When the part was committed (epoch millis).
    pub modified: u64,
}

/// One in-progress upload in a [`MultipartGateway::list_multipart_uploads`] answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedUpload {
    /// The object key the upload targets, relative to the container.
    pub key: String,
    /// The upload's id.
    pub upload_id: String,
    /// When the upload was created (epoch millis).
    pub initiated: u64,
}

/// What [`MultipartGateway::create_multipart_upload`] answers. Mirrors core's
/// `multipart::CreateOutcome`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartCreateAnswer {
    /// The upload exists and is open for parts.
    Created {
        /// The upload's id, which every later verb on it names.
        upload_id: String,
    },
    /// A typed refusal.
    Refused(MultipartRefusal),
}

/// What [`MultipartGateway::upload_part`] answers. Mirrors core's
/// `multipart::UploadPartOutcome`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartUploadPartAnswer {
    /// The part is durable.
    Committed {
        /// The part number.
        part_number: u32,
        /// The part's ETag (its content digest, lowercase hex), unquoted.
        etag: String,
    },
    /// A typed refusal.
    Refused(MultipartRefusal),
}

/// What [`MultipartGateway::complete_multipart_upload`] answers. Mirrors core's
/// `multipart::CompleteOutcome`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartCompleteAnswer {
    /// The object is published by this call.
    Published(MultipartPublication),
    /// An identical earlier Complete already published it; this is that publication, as it was
    /// recorded.
    AlreadyCompleted(MultipartPublication),
    /// A typed refusal; nothing was published.
    Refused(MultipartRefusal),
}

/// What [`MultipartGateway::abort_multipart_upload`] answers. Mirrors core's
/// `multipart::AbortOutcome`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartAbortAnswer {
    /// The abort is accepted and durable. Reclaiming the parts' bytes happens afterwards, in
    /// the background; it is not part of this answer.
    Fenced,
    /// The upload was already being aborted: an idempotent success.
    AlreadyAborting,
    /// A typed refusal.
    Refused(MultipartRefusal),
}

/// What [`MultipartGateway::list_parts`] answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartListPartsAnswer {
    /// The upload's **complete** part set, ascending by part number.
    Listed(Vec<ListedPart>),
    /// A typed refusal.
    Refused(MultipartRefusal),
}

/// What [`MultipartGateway::list_multipart_uploads`] answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultipartListUploadsAnswer {
    /// The container's **complete** set of in-progress uploads, sorted by key, then upload id.
    Listed(Vec<ListedUpload>),
    /// A typed refusal.
    Refused(MultipartRefusal),
}

/// The parts a multipart Complete names: `(part number, ETag)` pairs in the order the client
/// sent them, each ETag with the wire's quoting removed.
///
/// **Never empty.** [`NamedParts::new`] refuses an empty list, so a Complete that names no
/// part (core refuses it as `RecordError::NoPartsNamed`) cannot reach the seam and needs no
/// [`MultipartRefusal`] arm. The wire layer finds it while decoding the request and answers it
/// as malformed there (S3: `MalformedXML`), as ADR-0045 places structural checks at decode.
/// Nothing else is checked here: the part numbers' range and order are the implementer's (see
/// [`MultipartGateway`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedParts(Vec<(u32, String)>);

impl NamedParts {
    /// The list, or `None` if it is empty.
    pub fn new(parts: Vec<(u32, String)>) -> Option<Self> {
        if parts.is_empty() {
            None
        } else {
            Some(Self(parts))
        }
    }

    /// The pairs, in the order the client sent them. Never empty.
    pub fn as_slice(&self) -> &[(u32, String)] {
        &self.0
    }

    /// The pairs by value, in the order the client sent them. Never empty.
    pub fn into_vec(self) -> Vec<(u32, String)> {
        self.0
    }
}

/// The **multipart** side of the gateway seam — ADR-0046 decision 6, as for
/// [`ContainerGateway`]: [`ObjectGateway`] stays object-only and bucket-free, and multipart
/// uploads arrive as this narrow companion trait in neutral container/object vocabulary.
/// Implemented by `wyrd-server` at the composition root per ADR-0010; the S3 crate alone
/// speaks S3's multipart wire forms.
///
/// Each verb names the container and the object key as **separate** strings: an upload
/// targets a container and an object in it.
///
/// **Answers are values.** Every protocol outcome, success or refusal, is an arm of the verb's
/// answer type. `Err` means what it means on
/// [`put_object_streaming`](ObjectGateway::put_object_streaming): the store failed, the body
/// stream failed, or (for [`upload_part`](Self::upload_part) only) the body failed its
/// [`ContentHash`] check as [`GatewayError::PayloadMismatch`]. It is never a protocol outcome.
///
/// **Arguments arrive unvalidated.** The upload id is a `&str` and the part number a `u32`;
/// validating them is the implementer's. An upload id that does not parse answers
/// [`MultipartRefusal::NoSuchUpload`] (it cannot name an upload), and a part number out of
/// range answers [`MultipartRefusal::PartNumberOutOfRange`]. When a request has more than one
/// fault, the part-number range check wins: it runs first, before the upload id is parsed or
/// anything is looked up (as S3 servers order it). [`upload_part`](Self::upload_part) with an
/// out-of-range part number therefore answers `PartNumberOutOfRange` whatever its upload id,
/// and in [`complete_multipart_upload`](Self::complete_multipart_upload) the first
/// out-of-range number in the list is the one carried. A Complete that names no part never
/// reaches the seam: [`NamedParts`] cannot be empty.
///
/// **Listings are complete.** As with [`ContainerGateway::list_container`], the listing verbs
/// return the whole set and the wire layer paginates.
///
/// Every method has a default body answering [`MultipartRefusal::NotImplemented`], so a
/// gateway with no multipart support (the wire crate's own object-focused test doubles) needs
/// only an empty `impl`.
pub trait MultipartGateway: Send + Sync + 'static {
    /// Open a new multipart upload of `key` in `container`. `content_type` is the writer's
    /// declared `Content-Type` for the object the upload will publish (`None` if the client
    /// sent none), round-tripped verbatim as on
    /// [`put_object_streaming`](ObjectGateway::put_object_streaming).
    fn create_multipart_upload(
        &self,
        container: &str,
        key: &str,
        content_type: Option<String>,
    ) -> impl Future<Output = Result<MultipartCreateAnswer>> + Send {
        let _ = (container, key, content_type);
        std::future::ready(Ok(MultipartCreateAnswer::Refused(
            MultipartRefusal::NotImplemented,
        )))
    }

    /// Store part `part_number` of upload `upload_id`, its bytes arriving over `body`, without
    /// ever holding the whole part in memory (the seam's streaming invariant, as on
    /// [`put_object_streaming`](ObjectGateway::put_object_streaming)). `expected` is the
    /// payload-integrity check, verified before the part commits; a body that fails it is
    /// `Err(`[`GatewayError::PayloadMismatch`]`)`.
    fn upload_part<S>(
        &self,
        container: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: S,
        expected: ContentHash,
    ) -> impl Future<Output = Result<MultipartUploadPartAnswer>> + Send
    where
        S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    {
        let _ = (container, key, upload_id, part_number, body, expected);
        std::future::ready(Ok(MultipartUploadPartAnswer::Refused(
            MultipartRefusal::NotImplemented,
        )))
    }

    /// Assemble upload `upload_id` into the object `key` from `named_parts`: the ordered,
    /// non-empty list of `(part number, ETag)` the client sent (see [`NamedParts`]).
    fn complete_multipart_upload(
        &self,
        container: &str,
        key: &str,
        upload_id: &str,
        named_parts: NamedParts,
    ) -> impl Future<Output = Result<MultipartCompleteAnswer>> + Send {
        let _ = (container, key, upload_id, named_parts);
        std::future::ready(Ok(MultipartCompleteAnswer::Refused(
            MultipartRefusal::NotImplemented,
        )))
    }

    /// Abort upload `upload_id`, so it publishes nothing and its parts are reclaimed.
    fn abort_multipart_upload(
        &self,
        container: &str,
        key: &str,
        upload_id: &str,
    ) -> impl Future<Output = Result<MultipartAbortAnswer>> + Send {
        let _ = (container, key, upload_id);
        std::future::ready(Ok(MultipartAbortAnswer::Refused(
            MultipartRefusal::NotImplemented,
        )))
    }

    /// List the parts committed under upload `upload_id`: the complete set, ascending by part
    /// number.
    fn list_parts(
        &self,
        container: &str,
        key: &str,
        upload_id: &str,
    ) -> impl Future<Output = Result<MultipartListPartsAnswer>> + Send {
        let _ = (container, key, upload_id);
        std::future::ready(Ok(MultipartListPartsAnswer::Refused(
            MultipartRefusal::NotImplemented,
        )))
    }

    /// List `container`'s in-progress uploads: the complete set, sorted by key, then upload id.
    fn list_multipart_uploads(
        &self,
        container: &str,
    ) -> impl Future<Output = Result<MultipartListUploadsAnswer>> + Send {
        let _ = container;
        std::future::ready(Ok(MultipartListUploadsAnswer::Refused(
            MultipartRefusal::NotImplemented,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::NamedParts;

    #[test]
    fn named_parts_refuses_an_empty_list() {
        assert_eq!(NamedParts::new(Vec::new()), None);
    }

    #[test]
    fn named_parts_keeps_every_pair_in_the_order_sent() {
        // Out of order on purpose: ordering is the implementer's check, never this type's.
        let sent = vec![(3, "c".to_string()), (1, "a".to_string())];
        let named = NamedParts::new(sent.clone()).expect("a non-empty list is accepted");
        assert_eq!(named.as_slice(), sent.as_slice());
        assert_eq!(named.into_vec(), sent);
    }
}
