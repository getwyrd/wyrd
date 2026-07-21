//! The gateway's **S3-compatible HTTP wire surface** — the "Stateless S3 front door"
//! that embeds the client library (m4-first-deployment-blueprint:59, §5 building-block
//! `05-building-block-view.md:132`, §7.5 `07-deployment-view.md:72`). It serves
//! **bucket-scoped object PUT / GET / DELETE** over HTTP, mapping each verb onto the
//! existing in-process client path ([`Gateway::put_object_streaming`] /
//! [`Gateway::get_object_streaming`] / [`Gateway::delete_object`]) so a wire round-trip is
//! byte-identical to the in-process one (blueprint:698-699). Every request is
//! **SigV4-verified before the body is read** ([`sigv4::verify`], §14 threat model
//! `14-threat-model.md:86`): there is no anonymous access, and an unsigned request never
//! forces the gateway to allocate for a body it will reject.
//!
//! # Streaming (invariant "stream, don't buffer", 0015:789)
//! Request and response bodies **stream**: a PUT is chunked + written as it arrives and a
//! GET reads chunk-by-chunk over a bounded channel, so the whole object is never resident
//! in the gateway heap. The signed `x-amz-content-sha256` is checked against the body's
//! running hash *after* it has streamed to the store (leased, uncommitted) and *before*
//! the commit, so a tampered body is rejected without ever being published.
//!
//! A **stock modern SDK** (boto3 / aws-sdk) sends an object PUT `aws-chunked`-framed with
//! `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`; that body is de-framed and
//! its per-chunk signatures verified as it streams ([`streaming::decode`]), so a real SDK
//! upload round-trips byte-identical rather than being refused (issue #364 carry-forward,
//! real-SDK streaming interop). A **default-configured** stock SDK actually goes further and
//! sends the checksum-**trailer** framing (`STREAMING-UNSIGNED-PAYLOAD-TRAILER` /
//! `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`, a declared `x-amz-checksum-*` trailer after
//! the terminating zero-length chunk) — also de-framed, trailer-signature-verified (signed
//! variant) and checksum-validated ([`checksum`]) as it streams, so that default upload is
//! accepted too rather than 403-ing (issue #505).
//!
//! # Crate boundary (issue #364 carry-forward, T5-a — extracted, ratified)
//! This wire surface is its **own crate** (`wyrd-gateway-s3`, architecture §5:132's named
//! `gateway-s3`), **not** a module inside `crates/server`. The earlier "keep it in server"
//! posture was rejected (iter-6): S3 is one of several planned gateways
//! (`14-threat-model.md` external principals, the §5 building-block view), so the wire layer
//! must not calcify inside the composition root. It is generic over the **shared gateway
//! seam** [`ObjectGateway`] (in the neutral `wyrd-gateway-core` crate) that every gateway
//! front-door implements, so a second front-door reuses the seam without depending on this
//! S3 crate, and `server` wires the concretes only at its composition root (ADR-0010).
//!
//! # Layering (ADR-0010)
//! [`S3Gateway`] is generic over `G: `[`ObjectGateway`] — it names **no** concrete backend.
//! Concretes are picked only at the composition root (`wyrd-server`'s `cli::cmd_s3`).
//!
//! # Scope boundary (brief §Out-of-scope)
//! The floor is object PUT/GET/DELETE with SigV4 header auth. Multipart, `ListObjectsV2`,
//! conditional requests, presigned URLs, and a full S3 error-code conformance sweep are
//! deferred (pre-M8). Bucket + key compose onto the M0 **flat namespace** (`ROOT`) as the
//! single object key `"{bucket}/{key}"` — no directory tree is invented.
//!
//! # TLS (§7.5 "TLS; S3 SigV4", two distinct identities)
//! The public S3 TLS identity ([`TlsIdentity`]) is modelled **separately** from the
//! internal step-ca mTLS fabric (blueprint:620-623, ADR-0036 req 5) so the two are never
//! conflated. Binding a real TLS listener needs a rustls crypto provider whose license is
//! outside the `deny.toml` allowlist — a **human-only** dependency/license decision
//! (INTEGRATION.md §4), pre-declared as NEEDS-HUMAN (build-notes item 5). Until that
//! decision the listener is exercised over loopback and an operator/#367 fronts it with
//! the public-TLS terminator ([`S3Gateway::serve`] takes an already-bound listener).

pub mod checksum;
pub mod crypto;
pub mod request_id;
pub mod sigv4;
pub mod streaming;

use std::sync::Arc;
use std::time::SystemTime;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::Response;
use axum::Router;
use futures_util::StreamExt;
use tracing::Instrument;
use wyrd_gateway_core::{
    resolve_byte_range, ByteRange, ContainerGateway, ContentHash, GatewayError, ListedObject,
    ObjectGateway, ObjectMeta, ObjectRead, ObjectStream, RangeOutcome, RangeRead,
};
use wyrd_traits::{BoxError, ErrorClass};

use crate::request_id::{RequestId, RequestIds};
use crate::sigv4::Credentials;

/// The public S3 TLS identity — deliberately a **distinct** type from any internal
/// service-to-service mTLS material (blueprint:620-623, ADR-0036 req 5). Carried in
/// [`S3Config`] as the seam a deployed host / the first-deployment gate (#367) binds to a
/// real public certificate once the rustls provider dependency is greenlit (NEEDS-HUMAN).
#[derive(Debug, Clone)]
pub struct TlsIdentity {
    /// Path to the PEM-encoded public S3 server certificate chain.
    pub cert_pem: std::path::PathBuf,
    /// Path to the PEM-encoded private key for [`cert_pem`](TlsIdentity::cert_pem).
    pub key_pem: std::path::PathBuf,
}

/// Configuration for the S3 wire surface: the accepted credentials, the SigV4 signing
/// scope (`region`/`service`), and the optional public-TLS identity.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// The credential set the gateway will accept (a single static credential at M4).
    pub credentials: Vec<Credentials>,
    /// The SigV4 region the credential scope must name (e.g. `us-east-1`).
    pub region: String,
    /// The SigV4 service the credential scope must name (`s3`).
    pub service: String,
    /// The public S3 TLS identity, separate from internal mTLS (bound by #367).
    pub tls: Option<TlsIdentity>,
}

impl S3Config {
    /// A config accepting exactly `credentials` in the default `s3` / `us-east-1` scope.
    pub fn new(credentials: Vec<Credentials>) -> Self {
        Self {
            credentials,
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            tls: None,
        }
    }
}

/// The S3 HTTP front door over any [`ObjectGateway`] `G` — the concrete backend
/// composition is `G`'s business, chosen at the composition root, never named here.
pub struct S3Gateway<G> {
    gateway: Arc<G>,
    config: Arc<S3Config>,
    metrics: Option<tracing::Dispatch>,
}

struct AppState<G> {
    gateway: Arc<G>,
    config: Arc<S3Config>,
    /// This process's request-id minter (#529). Shared, not per-request: the monotonic
    /// half of the id must be drawn from one counter.
    request_ids: Arc<RequestIds>,
    /// The `tracing` dispatch the **request plane's RED metrics** are emitted into
    /// (observability floor, proposal 0010 item 4) — see
    /// [`S3Gateway::with_metrics_dispatch`]. `None` ⇒ emit into the ambient subscriber,
    /// which is what a library caller and this crate's own tests get: the emission is
    /// unconditional, only the *sink* is a role's choice.
    metrics: Option<tracing::Dispatch>,
}

// `Arc` makes the state cheap to clone regardless of the gateway type, so avoid a
// derive that would spuriously demand `G: Clone`.
impl<G> Clone for AppState<G> {
    fn clone(&self) -> Self {
        Self {
            gateway: Arc::clone(&self.gateway),
            config: Arc::clone(&self.config),
            request_ids: Arc::clone(&self.request_ids),
            // `Dispatch` is itself a cheap handle (an `Arc` inside), so cloning the state
            // per request does not clone the subscriber.
            metrics: self.metrics.clone(),
        }
    }
}

impl<G> S3Gateway<G>
where
    G: ObjectGateway + ContainerGateway,
{
    /// Compose the front door over a gateway and its config.
    pub fn new(gateway: Arc<G>, config: S3Config) -> Self {
        Self {
            gateway,
            config: Arc::new(config),
            metrics: None,
        }
    }

    /// Emit this front door's **request-plane RED metrics** into `dispatch` (observability
    /// floor, proposal 0010 §"Scope boundary" item 4): a per-op latency histogram and an
    /// error counter keyed by op **and** the typed failure class.
    ///
    /// The dispatch is the role's metrics sink — `wyrd_telemetry::DurabilityTelemetry::
    /// metrics_dispatch()`, composed at the `wyrd s3` role entry (`cli::cmd_s3`) with the
    /// export surface chosen by `ExporterConfig`. This crate takes a plain
    /// [`tracing::Dispatch`] and never names a telemetry backend: the whole OpenTelemetry /
    /// Prometheus / OTLP stack stays behind the shared seam at the composition root
    /// (ADR-0012; 0010's "no concrete telemetry backend leaks into a leaf crate").
    ///
    /// It is carried rather than scoped because `axum::serve` spawns a task per connection,
    /// which does not inherit a scoped dispatch — see `DurabilityTelemetry::metrics_dispatch`.
    /// Unset, the metrics are emitted into the ambient subscriber (today's behaviour).
    pub fn with_metrics_dispatch(mut self, dispatch: tracing::Dispatch) -> Self {
        self.metrics = Some(dispatch);
        self
    }

    /// The axum router: every method/path routes through the `handle` dispatcher, which
    /// verifies the SigV4 signature first and then dispatches by verb.
    pub fn router(self) -> Router {
        // Mint every RED series this front door can ever report, at zero, before it serves
        // anything — see [`preregister_red`].
        preregister_red(self.metrics.as_ref());
        let state = AppState {
            gateway: self.gateway,
            config: self.config,
            request_ids: Arc::new(RequestIds::new()),
            metrics: self.metrics,
        };
        Router::new().fallback(handle::<G>).with_state(state)
    }

    /// Serve on an already-bound listener until it stops (loopback at Check; a deployed
    /// host wraps the listener in the public-TLS terminator, #367).
    pub async fn serve(self, listener: tokio::net::TcpListener) -> std::io::Result<()> {
        axum::serve(listener, self.router()).await
    }
}

// ---- the request plane (observability floor, proposal 0010 §"Scope boundary" item 4) ----
//
// RED for the S3 front door: a per-op latency histogram and an error counter keyed by op and
// by the typed failure class. Counters, not traces — the ratified floor shape (0010
// §Alternatives: "adequate, not elegant"); a span graph is deferred to its own ADR.
//
// Emitted as `tracing` metric events so the shared `MetricsLayer` bridge carries them to
// whichever export surface the role wired (ADR-0012 dual export, no backend hardcoded). That
// is the one instrumentation path this workspace has (ADR-0012): the custodian's durability
// plane raises `monotonic_counter.` / `gauge.` events the same way
// (`crates/core/src/read.rs:191-200`, `crates/custodian/src/rebalance.rs:320-326`), and
// minting OTel meters directly here would fork a second path around the seam.
//
// The **rate** half of RED is deliberately not a separate counter: the OTel→Prometheus
// exporter renders a histogram's own `_count` series, so `s3_request_duration_ms_count` is the
// per-op request rate and a parallel `s3_requests_total` could only drift from it.

/// Every op label the request plane can report. A **bounded** label space (0010 §Open
/// questions: op + class only — no per-key / per-tenant labels): the front door dispatches
/// PUT / GET / DELETE, answers HEAD body-lessly, and refuses everything else as `other`, so a
/// client sending `TRACE` mints no new series. Enumerated here so [`preregister_red`] can
/// raise the whole space up front.
const OPS: [&str; 5] = ["put", "get", "delete", "head", "other"];

/// The op label for `method` — the request plane's stable, low-cardinality key.
fn op_label(method: &Method, path: &str, query: &str) -> &'static str {
    match *method {
        Method::PUT => "put",
        Method::GET => "get",
        Method::DELETE => "delete",
        Method::HEAD => "head",
        // **Bulk `DeleteObjects` is a POST on the wire but a DELETE as an OPERATION.** Labelling
        // by method alone filed every bulk delete under `other` — pooled with unsupported methods
        // — so the `delete` RED series was blind to the one path that removes up to 1000 objects
        // per request, and its latency and failures never reached a delete dashboard or alert
        // (PR #612 review). The predicate mirrors the route's own interception exactly (see
        // `dispatch`), so this label can never name a request the bulk handler did not take.
        Method::POST if bucket_scoped_path(path).is_some() && is_delete_subresource(query) => {
            "delete"
        }
        _ => "other",
    }
}

/// Run `f` — which raises `tracing` metric events — against the role's metrics sink.
///
/// `Some` ⇒ the role configured a metrics dispatch ([`S3Gateway::with_metrics_dispatch`]);
/// enter it for the emission. `None` ⇒ emit into the ambient subscriber, which is what a
/// library caller gets. Entering a dispatch is sound from *any* task (it is a thread-local
/// set for the closure's duration), which is exactly why the sink is carried rather than
/// scoped around the serve future: `axum::serve` spawns a task per connection.
///
/// `f` must do nothing but emit — it runs with a metrics-only subscriber current, so an
/// ordinary log line raised inside it would land nowhere.
fn emit_into(dispatch: Option<&tracing::Dispatch>, f: impl FnOnce()) {
    match dispatch {
        Some(dispatch) => tracing::dispatcher::with_default(dispatch, f),
        None => f(),
    }
}

/// Raise every error **counter** series this front door can ever report, at **zero**, before
/// it serves a request.
///
/// A counter that only learns a label the first time that fault fires reports *nothing at
/// all* until something breaks — so a dashboard reads "no data" both when the gateway is
/// healthy and when it was never wired, and an alert on `rate(s3_request_errors[5m])` cannot
/// distinguish "no errors" from "no metric". #577 minted [`ErrorClass::ALL`] for exactly this
/// (its doc names this issue): a **closed** class set with a **stable** label form, so the
/// whole op × class space is enumerable up front rather than discovered during an incident.
///
/// **Only the counter.** The latency histogram is deliberately NOT pre-registered, and the
/// asymmetry is not an oversight: `add(0)` on a counter is value-neutral, but `record(0)` on
/// a histogram is a real observation — it would seed every op's distribution with a phantom
/// 0ms sample, dragging p50 toward zero and reporting a front door faster than it is. A
/// latency series that does not exist until the first request is served is the honest
/// alternative, and it costs nothing: unlike an error, a request arriving is the normal case,
/// so the series appears immediately in any deployment that is doing anything at all.
fn preregister_red(dispatch: Option<&tracing::Dispatch>) {
    emit_into(dispatch, || {
        for op in OPS {
            for class in ErrorClass::ALL {
                tracing::info!(
                    monotonic_counter.s3_request_errors = 0_u64,
                    op = op,
                    class = class.as_str(),
                );
            }
        }
    });
}

/// Raise one request's RED sample: always its latency, and — when the request failed — one
/// error keyed by op **and** the failure class.
///
/// The `class` label is #577's [`ErrorClass::as_str`], a value the seam **exports** with a
/// stable, bounded label form. It is *consumed*, never re-derived: `wyrd_traits::classify` is
/// the seam's one classifier (`gateway_error_response` runs it over the error's whole
/// `source()` chain and hands the verdict here through a response extension), so this counter
/// cannot drift from what the rest of the system calls the same fault — the whole point of
/// keying item 4's counter on item 6's typed class.
fn emit_request_red(
    dispatch: Option<&tracing::Dispatch>,
    op: &'static str,
    class: ErrorClass,
    duration_ms: u64,
    errored: bool,
) {
    emit_into(dispatch, || {
        tracing::info!(histogram.s3_request_duration_ms = duration_ms, op = op);
        if errored {
            tracing::info!(
                monotonic_counter.s3_request_errors = 1_u64,
                op = op,
                class = class.as_str(),
            );
        }
    });
}

/// The S3 subresource / multipart query keys this object-only floor does not implement.
/// A request carrying any of them is refused (501) rather than falling through to a plain
/// object PUT/GET/DELETE (or, on the bucket route, a listing), which would mishandle it —
/// destructively for `PUT ?partNumber` (UploadPart) and `DELETE ?tagging`, and silently for
/// `GET /bucket?acl` (would read a listing document). Benign params a normal SDK adds to
/// ordinary requests (e.g. `x-id=PutObject`) are not listed, so they still pass; this is a
/// denylist of unsupported operations, not a ban on all query strings.
const UNSUPPORTED_SUBRESOURCES: &[&str] = &[
    "uploads",
    "uploadId",
    "partNumber",
    "tagging",
    "acl",
    "versions",
    "versionId",
    "cors",
    "lifecycle",
    "policy",
    "website",
    "location",
    "delete",
    "restore",
    "select",
    "retention",
    "legal-hold",
    "torrent",
    "accelerate",
    "logging",
    "notification",
    "replication",
    "encryption",
    "requestPayment",
    "analytics",
    "inventory",
    "metrics",
    "object-lock",
    "publicAccessBlock",
    "attributes",
    // Bucket-level subresource GETs whose query key differs from the object-level spelling
    // above: without these a `GET /bucket?versioning` (and the rest) slips past the bucket
    // route's fence and is answered with a `<ListBucketResult>` instead of a clean `501`,
    // dying in the client's XML decoder (`expected VersioningConfiguration`) — the "bucket
    // subresource op silently answered with a listing" the routing decision forbids (issue
    // #507 adversary). `versions` (object versions) is spelled differently from `versioning`
    // (bucket versioning config), so both must be listed.
    "versioning",
    "intelligent-tiering",
    "ownershipControls",
    "policyStatus",
    "metadataTable",
];

/// The first [`UNSUPPORTED_SUBRESOURCES`] query key present in `query`, matching **raw**
/// (un-decoded) keys. Used on the OBJECT path (PUT/GET/DELETE), where a missed subresource
/// falls through to the plain verb rather than to a listing — so this is a raw match, not the
/// percent-decoding one the bucket route needs ([`unsupported_subresource_decoded`]).
///
/// The residual (adversary, not pressed): a client that percent-encodes a subresource key
/// (`?part%4Eumber`) dodges this raw match and the request executes as a plain object verb.
/// It is not a listing-disclosure like the bucket route's gap — only a fully-credentialed
/// client that deliberately encodes the key can reach it, and such a client can issue the
/// plain verb directly anyway, so a raw match is adequate HERE (unlike the bucket route).
/// Returns the first offending key found.
fn unsupported_subresource(query: &str) -> Option<&str> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').map(|(k, _)| k).unwrap_or(p))
        .find(|k| UNSUPPORTED_SUBRESOURCES.contains(k))
}

/// True if `query` names the **`delete`** subresource (`?delete`, `?delete=`, `?delete&x=1`) —
/// the marker of a bulk **DeleteObjects** POST (issue #509). A **bare-key** match, mirroring
/// [`unsupported_subresource`]'s split (:387-393): `?delete` has no `=`, so [`query_param`]
/// (:453), which requires `k=v`, would return `None` and miss it. Used on the bucket route to
/// intercept `POST /bucket?delete` BEFORE the subresource denylist (which lists `"delete"`, :344)
/// would refuse it — while `"delete"` stays on the OBJECT-path denylist so `DELETE /b/k?delete`
/// remains `501`.
fn is_delete_subresource(query: &str) -> bool {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').map(|(k, _)| k).unwrap_or(p))
        .any(|k| k == "delete")
}

/// The buffered-byte cap for a bulk **DeleteObjects** request body (issue #509). The body is
/// small (≤1000 keys) and SigV4-signed, so — unlike a streamed object PUT — it is buffered whole;
/// the cap is enforced BY CONSTRUCTION as the body is read (never by trusting `Content-Length`),
/// and an over-cap body is refused as `MalformedXML`.
///
/// The bound must sit in `[~6.2 MB, 9 MiB)`. FLOOR: a legal maximum request MUST fit **as it is
/// serialized on the wire — after XML escaping**. A 1024-byte key is legal, and one key character
/// can occupy up to 6 body bytes once escaped (`&quot;`/`&apos;`, or a `&#xNN;` character
/// reference), so the worst case is 1000 × 1024 × 6 ≈ 6.14 MB of key text plus the
/// `<Object><Key></Key></Object>` envelope (≈28 KB) ≈ 6.2 MB. Sizing this floor on the RAW key
/// bytes (~1.06 MB, the previous 2 MiB cap) fail-closed-rejected a request that satisfies both
/// documented limits — 1000 keys, each ≤1024 bytes — merely because its keys contained `&`
/// (PR #612 review). CEILING: the retained oversized-body test sends a 9 MiB body, so the cap MUST
/// be < 9 MiB to refuse it. 8 MiB sits in `[6.2 MB, 9 MiB)`.
///
/// A character reference may be padded with leading zeros (`&#x00000041;`) without bound, so no
/// finite cap accommodates EVERY escaping of a legal key; such a body is still refused. The floor
/// is sized for worst-case *standard* escaping, not adversarial padding.
const MAX_DELETE_BODY_BYTES: usize = 8 * 1024 * 1024;

/// How many per-key deletes a bulk **DeleteObjects** has in flight at once (issue #509).
///
/// The fan-out is what makes the bulk verb worth having: `delete_object` is a metadata operation
/// that can be a network round trip, so awaiting each key before starting the next made a 1000-key
/// batch cost the SUM of 1000 latencies — slower than the client just issuing 1000 parallel
/// single-object DELETE requests, which inverts the whole point of the batch (PR #612 review).
///
/// It stays BOUNDED so one request cannot open an unbounded number of concurrent metadata
/// operations against the backend, and the fan-out uses `buffered` — NOT `buffer_unordered` — so
/// results come back in request order and each `<Deleted>`/`<Error>` row still lines up with the
/// key that produced it. The response stays byte-for-byte deterministic.
const DELETE_FANOUT: usize = 16;
const _: () = assert!(
    MAX_DELETE_BODY_BYTES > 6_200_000 && MAX_DELETE_BODY_BYTES < 9 * 1024 * 1024,
    "MAX_DELETE_BODY_BYTES must exceed a legal 1000-key request AFTER XML escaping (~6.2 MB) and \
     stay under the 9 MiB oversized-body test's body",
);

/// Like [`unsupported_subresource`] but matches against **percent-decoded** query keys, so a
/// bucket subresource cannot slip past the listing route by percent-encoding its name
/// (`GET /bucket?%61cl`, `GET /bucket?upload%73`). The bucket route routes anything not on the
/// denylist to a listing, so an encoded `?acl` that dodged a raw match would be answered with
/// a listing document — precisely the "bucket subresource op silently answered with a listing"
/// the routing decision forbids (issue #507 adversarial finding). Returns the decoded key.
fn unsupported_subresource_decoded(query: &str) -> Option<String> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').map(|(k, _)| k).unwrap_or(p))
        .map(percent_decode_utf8)
        .find(|k| UNSUPPORTED_SUBRESOURCES.contains(&k.as_str()))
}

/// The denylisted subresource a bulk-delete query carries BESIDES its own `delete` marker, if any
/// — percent-decoded, exactly like [`unsupported_subresource_decoded`].
///
/// The bulk-delete route is intercepted BEFORE the denylist, so that `?delete` — which is itself on
/// the denylist — is not refused by it. That early return also skipped the denylist for every
/// OTHER key in the same query, making the one destructive route a hole in the fence every other
/// bucket verb passes through: `POST /bucket?delete&versionId=v` ran an ordinary UNVERSIONED bulk
/// delete while the client had explicitly asked for version semantics, destroying the current
/// object. That is the exact data-loss shape a `<VersionId>` in the BODY is refused for, reached by
/// the query spelling instead (PR #612 review).
///
/// Only the `delete` marker is exempt; every other denylisted key still refuses. Benign query
/// params are untouched, so this narrows nothing a client legitimately sends.
fn foreign_subresource_on_delete(query: &str) -> Option<String> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').map(|(k, _)| k).unwrap_or(p))
        .map(percent_decode_utf8)
        .find(|k| k != "delete" && UNSUPPORTED_SUBRESOURCES.contains(&k.as_str()))
}

/// Split a request path `/{bucket}/{key}` into `(bucket, key)`. `key` may itself contain
/// `/` (a nested key on the flat namespace). Returns `None` for a bucket-only or empty
/// path — bucket-level operations are out of scope.
fn split_bucket_key(path: &str) -> Option<(&str, &str)> {
    let (bucket, key) = path.trim_start_matches('/').split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket, key))
}

/// The bucket name for a **bucket-scoped** path (`/{bucket}` or `/{bucket}/`), or `None`
/// for an object path (`/{bucket}/{key}`, non-empty key) or the empty path (`/`). The
/// counterpart to [`split_bucket_key`]: that one names objects, this one names buckets, and
/// the two partition every path so the dispatcher routes a listing without touching the
/// object-path guard (issue #507). The returned name is still percent-encoded — the caller
/// decodes it, exactly as the object path is decoded.
fn bucket_scoped_path(path: &str) -> Option<&str> {
    // Strip EXACTLY the one leading `/` an HTTP request-target always carries, not every
    // leading slash: `trim_start_matches('/')` would fold `//bucket` down to `bucket` and
    // answer it as a listing, hiding the empty-bucket-segment forms this match rejects
    // (issue #507 sign-off adversary). With a single strip, `//bucket` keeps its empty first
    // segment and falls through to the object-path guard's error instead of a bogus 200.
    let trimmed = path.strip_prefix('/')?;
    match trimmed.split_once('/') {
        // `/{bucket}/{key}` with a non-empty key → an OBJECT path; not bucket-scoped. A
        // double-slash path `//{key}` lands here too — empty bucket segment, non-empty
        // remainder — so it is refused a listing and falls to the object-path guard's error.
        Some((_, key)) if !key.is_empty() => None,
        // `/{bucket}/` → bucket-scoped (a trailing slash, empty key).
        Some((bucket, _)) if !bucket.is_empty() => Some(bucket),
        // `//` — an empty bucket segment (and empty key) → neither. Reachable now that only
        // the single leading `/` is stripped (was dead under `trim_start_matches`).
        Some(_) => None,
        // `/{bucket}` (no slash at all) → bucket-scoped; `/` (empty) → neither.
        None => (!trimmed.is_empty()).then_some(trimmed),
    }
}

/// The percent-decoded value of query parameter `name` (first occurrence), or `None` if
/// absent or valueless. S3 percent-encodes `prefix`/`delimiter`/`continuation-token` values,
/// so they are decoded with the same [`percent_decode_utf8`] the object path uses (`+` is a
/// literal, not a space — an S3 key/token is a path value, not form data).
fn query_param(query: &str, name: &str) -> Option<String> {
    query
        .split('&')
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| percent_decode_utf8(v))
}

/// Render `epoch_millis` as an ISO-8601 / RFC-3339 UTC instant
/// (`2009-10-12T17:50:30.000Z`) — the shape S3 uses for a listing's `<LastModified>`
/// (distinct from the IMF-fixdate [`http_date`] uses for the `Last-Modified` *header*).
/// Shares [`civil_from_days`] with `http_date`, so no date dependency is pulled in.
///
/// `None` past year 9999 — the same bound [`http_date`] applies, for the same reason: the
/// stored `modified` is an unrestricted `u64`, and a five-digit year is not a valid RFC-3339
/// timestamp, so a strict SDK date parser would reject the whole listing document over one
/// pathological record. The caller omits the element instead (the ADR-0047 degradation).
fn iso8601(epoch_millis: u64) -> Option<String> {
    const SECS_PER_DAY: u64 = 86_400;
    let secs = epoch_millis / 1_000;
    let millis = epoch_millis % 1_000;
    let days = (secs / SECS_PER_DAY) as i64;
    let sod = secs % SECS_PER_DAY;
    let (hour, minute, second) = (sod / 3_600, (sod % 3_600) / 60, sod % 60);
    let (year, month, day) = civil_from_days(days);
    if year > 9_999 {
        return None;
    }
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
    ))
}

/// One computed listing page: the `<Contents>` rows, the `<CommonPrefixes>` (delimiter
/// rollups), whether the listing was truncated at `max-keys`, and TWO resume points that
/// coincide except at a delimiter rollup:
///
/// * `next_key` — the last underlying object key **consumed** (raw). This is the v2
///   continuation token's payload: because a common-prefix rollup is emitted for a WHOLE
///   contiguous group at once, resuming strictly after that group's last raw key skips the
///   entire rollup and can never re-emit it on the next page (ADR-0046 codex finding).
/// * `next_marker` — the last item **returned**: the common prefix when the last entry was a
///   delimiter rollup, else the last content key. This is v1's `<NextMarker>` — AWS emits the
///   rollup's prefix (`a/`), not its last raw key (`a/2`), and a client that stores and
///   resends that prefix as `marker` must skip the whole group, not re-receive it (issue #507
///   adversary). [`compute_page`]'s resume filter handles a marker naming a common prefix.
struct ListPage<'a> {
    contents: Vec<&'a ListedObject>,
    common_prefixes: Vec<String>,
    is_truncated: bool,
    next_key: Option<String>,
    next_marker: Option<String>,
}

/// Compute one listing page over the container's complete, already-sorted key set —
/// `prefix` filter, `delimiter` common-prefix rollups, the **combined** `max-keys` slice
/// (`Contents` + `CommonPrefixes` counted together), and the resume point. Pure over its
/// inputs so the wire behaviour is exercised by the SDK-driven integration test end to end.
fn compute_page<'a>(
    objects: &'a [ListedObject],
    prefix: &str,
    delimiter: Option<&str>,
    resume_after: Option<&str>,
    max_keys: usize,
) -> ListPage<'a> {
    // S3: `max-keys=0` is a valid request for zero keys — it returns an empty, **non-truncated**
    // page (`KeyCount=0`, `IsTruncated=false`, no continuation token). A truncated page MUST
    // always carry a resume point; a zero budget has none, so flagging it truncated would wedge
    // a conforming paginator that re-sends while `IsTruncated` with no token to advance
    // (issue #507 adversary). Handle it before the loop so the budget check never trips at 0.
    if max_keys == 0 {
        return ListPage {
            contents: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            next_key: None,
            next_marker: None,
        };
    }

    // `objects` arrives lexicographically sorted from the seam; keep that order (filtering only
    // by `prefix`). The resume skip is applied per emitted item/group INSIDE the loop rather
    // than as a pre-filter on raw keys, because a rollup is filtered on the COMMON PREFIX
    // itself, not on its surviving raw keys (see the group rule below) — a pre-filter of
    // `key > "a/"` would keep `a/1`, `a/2` and re-emit the `a/` rollup (issue #507 adversary).
    let filtered: Vec<&ListedObject> = objects
        .iter()
        .filter(|o| o.key.starts_with(prefix))
        .collect();

    let mut contents = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut count = 0usize;
    let mut next_key = None;
    let mut next_marker = None;
    let mut is_truncated = false;

    let mut i = 0;
    while i < filtered.len() {
        let obj = filtered[i];
        let rest = &obj.key[prefix.len()..];
        if let Some(delim) = delimiter {
            if let Some(idx) = rest.find(delim) {
                // Common prefix = everything up to and including the first delimiter after
                // `prefix`. All keys sharing it are contiguous (sorted).
                let cp = format!("{}{}", prefix, &rest[..idx + delim.len()]);
                // Delimit the group's extent [group_start, group_end) and its last raw key.
                let mut group_end = i;
                while group_end < filtered.len() && filtered[group_end].key.starts_with(&cp) {
                    group_end += 1;
                }
                let last_raw = filtered[group_end - 1].key.as_str();
                // A rollup survives the resume point iff the COMMON PREFIX itself is strictly
                // after it — AWS documents this rule verbatim for both listing forms:
                // "`CommonPrefixes` is filtered out from results if it is not lexicographically
                // greater than the `StartAfter` value" (ListObjectsV2, `delimiter`) and "… than
                // the key-marker" (ListObjects). One comparison covers every resume mechanism:
                // a client resume landing INSIDE the group (`start-after=a/1`) or naming the
                // prefix (`start-after=a/`) drops the `a/` rollup (`"a/" ≤ "a/1"`), the
                // server-issued v1 `NextMarker` naming the prefix collapses it the same way,
                // and the server-issued v2 `continuation-token` — always the group's LAST raw
                // key, because a rollup consumes its whole group below — likewise satisfies
                // `cp ≤ resume` exactly when the group was consumed. (Supersedes the
                // iteration-4 Delta 2 raw-keyspace-before-rollup reading, which contradicted
                // the documented rule.)
                let survives = match resume_after {
                    Some(r) => cp.as_str() > r,
                    None => true,
                };
                if !survives {
                    i = group_end;
                    continue;
                }
                // Budget checked only for an item we would actually emit: entering here with the
                // budget spent means a genuine unlisted entry remains, so the page is truncated.
                if count >= max_keys {
                    is_truncated = true;
                    break;
                }
                // Emit the group as ONE combined-count entry: the rollup is the last RETURNED
                // item (`next_marker`, v1 `NextMarker`); the group's last raw key is the last
                // CONSUMED key (`next_key`, the v2 token payload). The token payload stays the
                // last raw key even when a client resume landed inside the group, so the NEXT
                // page's v2 token (`>= last_raw`) collapses the whole group via `group_consumed`.
                common_prefixes.push(cp.clone());
                count += 1;
                next_marker = Some(cp.clone());
                next_key = Some(last_raw.to_string());
                i = group_end;
                continue;
            }
        }
        // A plain content key already consumed by the resume point is skipped (not counted).
        if resume_after.is_some_and(|r| obj.key.as_str() <= r) {
            i += 1;
            continue;
        }
        if count >= max_keys {
            is_truncated = true;
            break;
        }
        contents.push(obj);
        count += 1;
        // A content row is both the last consumed key and the last returned item.
        next_key = Some(obj.key.clone());
        next_marker = Some(obj.key.clone());
        i += 1;
    }

    ListPage {
        contents,
        common_prefixes,
        is_truncated,
        next_key: if is_truncated { next_key } else { None },
        next_marker: if is_truncated { next_marker } else { None },
    }
}

/// Handle a bucket-scoped listing GET — ListObjectsV2 (`?list-type=2`) or the v1 shim
/// (bare / `marker`). Parses the S3 query vocabulary, drives the neutral
/// [`ContainerGateway::list_container`] seam, then computes grouping, the combined `max-keys`
/// slice, and pagination wire-side (ADR-0046 seam decision) before emitting a
/// `<ListBucketResult>` by string building (no XML dependency — a human-gated decision).
async fn list_objects<G>(
    state: &AppState<G>,
    request_id: RequestId,
    bucket: &str,
    query: &str,
) -> Response
where
    G: ObjectGateway + ContainerGateway,
{
    // `encoding-type` (issue #507 Delta 1). botocore injects `encoding-type=url` into EVERY
    // ListObjects/V2 request (aws-cli, boto3, rclone), so it is load-bearing for the stock
    // clients this feature targets — not optional. `url` turns on render-time percent-encoding
    // of the returned `Key`/`Prefix`/`Delimiter`/`CommonPrefixes` and the resume echoes
    // (`StartAfter`/`Marker`/`NextMarker`); the opaque continuation tokens stay untouched.
    // Encoding is RENDER-TIME ONLY — filtering, grouping, resume comparison and token payloads
    // all operate on raw keys. Any OTHER `encoding-type` value is a client error: AWS answers
    // `400 InvalidArgument` (matching S3), never a silent degrade.
    let encode = match query_param(query, "encoding-type") {
        None => false,
        Some(v) if v == "url" => true,
        Some(_) => {
            return error_response(
                request_id,
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "the `encoding-type` parameter must be `url`",
            )
        }
    };

    // v2 iff `list-type=2`. `list-type` present with any OTHER value is a client error — AWS
    // answers `400 InvalidArgument` rather than silently degrading to the v1 shim (which would
    // hide a client bug). Absent `list-type` is the v1 shim (a bare `GET /bucket`, or `?marker`).
    match query_param(query, "list-type") {
        None => {}
        Some(v) if v == "2" => {}
        Some(_) => {
            return error_response(
                request_id,
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "the `list-type` parameter must be 2",
            )
        }
    }
    let is_v2 = query_param(query, "list-type").as_deref() == Some("2");
    let prefix = query_param(query, "prefix").unwrap_or_default();
    let delimiter = query_param(query, "delimiter").filter(|d| !d.is_empty());

    // `max-keys` defaults to 1000 and clamps to it; a non-numeric value is a client error.
    let max_keys = match query_param(query, "max-keys") {
        None => 1000usize,
        Some(v) => match v.parse::<usize>() {
            Ok(n) => n.min(1000),
            Err(_) => {
                return error_response(
                    request_id,
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "max-keys is not a valid non-negative integer",
                )
            }
        },
    };

    // The resume point. v2: the OPAQUE base64 `continuation-token` decodes to the last key of
    // the previous page; an undecodable token is a 400, never a silent restart. v1: the raw
    // `marker` IS a key, used verbatim.
    let request_token = if is_v2 {
        query_param(query, "continuation-token")
    } else {
        None
    };
    let request_marker = if is_v2 {
        None
    } else {
        query_param(query, "marker")
    };
    // The v2 `start-after` request value, echoed back in `<StartAfter>` (AWS echoes the request
    // parameter regardless of whether a `continuation-token` overrides it as the resume point).
    let request_start_after = if is_v2 {
        query_param(query, "start-after").filter(|s| !s.is_empty())
    } else {
        None
    };
    let resume_after: Option<String> = if is_v2 {
        match &request_token {
            Some(tok) => match decode_continuation_token(tok) {
                Some(key) => Some(key),
                None => {
                    return error_response(
                        request_id,
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "the continuation-token is not valid",
                    )
                }
            },
            // No `continuation-token`: `start-after` (v2) sets the resume point for the FIRST
            // page — the listing begins strictly AFTER this key. Not silently ignored: a
            // `start-after` client would otherwise re-receive keys it already consumed
            // (issue #507 carry-forward). An empty value is treated as absent. Once a
            // `continuation-token` is present it takes precedence, and `start-after` is ignored
            // as the resume point (AWS semantics — the stock paginator resends `StartAfter`
            // alongside every token, so this is the NORMAL flow, not an edge; a `max(token,
            // start-after)` blend would be wrong).
            None => request_start_after.clone(),
        }
    } else {
        request_marker.clone().filter(|m| !m.is_empty())
    };
    // Drive the neutral seam: the complete, sorted key set — or `None` (no bucket record).
    let objects = match state.gateway.list_container(bucket).await {
        Ok(Some(objects)) => objects,
        Ok(None) => {
            return error_response(
                request_id,
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "the specified bucket does not exist",
            )
        }
        Err(err) => return gateway_error_response(request_id, &err),
    };

    let page = compute_page(
        &objects,
        &prefix,
        delimiter.as_deref(),
        resume_after.as_deref(),
        max_keys,
    );

    let view = ListView {
        bucket,
        prefix: &prefix,
        delimiter: delimiter.as_deref(),
        max_keys,
        encode,
    };
    let xml = if is_v2 {
        render_list_v2(
            &view,
            request_token.as_deref(),
            request_start_after.as_deref(),
            &page,
        )
    } else {
        render_list_v1(&view, request_marker.as_deref().unwrap_or(""), &page)
    };
    list_response(xml)
}

/// Decode an opaque ListObjectsV2 continuation token back to the resume key: standard base64
/// of the key's UTF-8 bytes. `None` when the token is not well-formed base64 or not UTF-8 —
/// the caller answers `400 InvalidArgument` (never a silent restart from the top).
fn decode_continuation_token(token: &str) -> Option<String> {
    let bytes = crate::checksum::base64_decode(token)?;
    String::from_utf8(bytes).ok()
}

/// Encode a resume key as an opaque continuation token — standard base64 of its UTF-8 bytes.
fn encode_continuation_token(key: &str) -> String {
    crate::checksum::base64_encode(key.as_bytes())
}

/// Percent-encode a listing value for `encoding-type=url` (issue #507 Delta 1). RENDER-TIME
/// ONLY — filtering, grouping, resume comparison and token payloads all use raw keys; this
/// projects a value onto the wire. Only the unreserved set `A-Za-z0-9`, `-`, `_`, `.`, `~`
/// plus `/` (S3 keeps a key's path separators literal) pass through unescaped; every other
/// byte encodes as `%XX` (upper-hex) over its UTF-8 bytes — a space becomes `%20`, never `+`
/// (botocore URL-decodes these values, so `a&b/c d` must return as `a%26b/c%20d`).
fn url_encode_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(
                    char::from_digit(u32::from(b >> 4), 16)
                        .expect("nibble")
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit(u32::from(b & 0x0f), 16)
                        .expect("nibble")
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

/// Project a listing value onto the wire: under `encoding-type=url` percent-encode it
/// ([`url_encode_value`], Delta 1) THEN XML-escape; otherwise XML-escape only. URL-encoding
/// leaves no XML-special byte in practice, but the XML escape stays as the outer invariant
/// (ADR-0046 XML decision) so the output is well-formed either way.
fn project_value(s: &str, encode: bool) -> String {
    if encode {
        xml_escape(&url_encode_value(s))
    } else {
        xml_escape(s)
    }
}

/// Render one `<Contents>` row — key projected ([`project_value`]; URL-encoded under
/// `encoding-type=url`), size, the S3-quoted ETag (when recorded and well-formed), the
/// ISO-8601 `<LastModified>` (when recorded and renderable), and a `STANDARD` storage class.
/// An object whose metadata predates the timestamp model has `modified: None`; the element is
/// OMITTED then — like the ETag right below — never backfilled with the epoch: sync tools
/// (`aws s3 sync`, rclone) compare `LastModified` to decide whether to transfer, and a
/// fabricated `1970-01-01` makes every local copy look newer, silently leaving stale content
/// in place. This mirrors the GET/HEAD arm, which already omits the `Last-Modified` HEADER
/// for the same records (`modified.and_then(http_date)`), so a legacy object presents one
/// consistent "no recorded time" face on every surface; the stock SDKs model the field as
/// optional (a parse cannot fail on absence), and a consumer that then errors does so VISIBLY
/// — the fail-closed trade over silently corrupting sync decisions with invented data
/// (ADR-0047 stance).
///
/// The same degrade-by-omission covers the two ways a RECORDED value can still be
/// unpresentable, so one pathological record can never poison the whole listing document for
/// every other object in the bucket: a `modified` past year 9999 has no valid RFC-3339
/// rendering ([`iso8601`] returns `None`, the bound [`http_date`] already applies on GET),
/// and a stored ETag that is not a well-formed entity-tag (store corruption / out-of-band
/// edits — `xml_escape` cannot neutralise an XML-1.0-forbidden control byte) is omitted via
/// the SAME [`etag_header`] validation the GET path uses.
fn render_contents(out: &mut String, obj: &ListedObject, encode: bool) {
    out.push_str("<Contents><Key>");
    out.push_str(&project_value(&obj.key, encode));
    out.push_str("</Key>");
    if let Some(rendered) = obj.modified.and_then(iso8601) {
        out.push_str("<LastModified>");
        out.push_str(&rendered);
        out.push_str("</LastModified>");
    }
    if let Some(etag) = obj.etag.as_deref().filter(|e| etag_header(e).is_some()) {
        out.push_str("<ETag>");
        out.push_str(&xml_escape(&quote_etag(etag)));
        out.push_str("</ETag>");
    }
    out.push_str("<Size>");
    out.push_str(&obj.size.to_string());
    out.push_str("</Size><StorageClass>STANDARD</StorageClass></Contents>");
}

/// Render the `<CommonPrefixes>` rollups (each already delimiter-terminated); each prefix is
/// projected ([`project_value`]; URL-encoded under `encoding-type=url`).
fn render_common_prefixes(out: &mut String, prefixes: &[String], encode: bool) {
    for cp in prefixes {
        out.push_str("<CommonPrefixes><Prefix>");
        out.push_str(&project_value(cp, encode));
        out.push_str("</Prefix></CommonPrefixes>");
    }
}

/// The request-level fields both listing renderers echo back into `<ListBucketResult>` — the
/// bucket name, `prefix`, `delimiter`, effective `max-keys`, and whether `encoding-type=url`
/// is in effect. Bundled so each renderer keeps a small argument list (clippy too-many-args).
struct ListView<'a> {
    bucket: &'a str,
    prefix: &'a str,
    delimiter: Option<&'a str>,
    max_keys: usize,
    encode: bool,
}

/// Emit a ListObjectsV2 `<ListBucketResult>` (issue #507). `KeyCount` is the COMBINED count
/// of `Contents` + `CommonPrefixes` returned; `NextContinuationToken` is present only when
/// the listing was truncated, and is the opaque token for the next page. Under
/// `encoding-type=url` (`view.encode`) the `Key`/`Prefix`/`Delimiter`/`CommonPrefixes`/
/// `StartAfter` values are percent-encoded and an `<EncodingType>url</EncodingType>` element is
/// emitted; the opaque `ContinuationToken`/`NextContinuationToken` are left UNTOUCHED so a
/// returned token resumes verbatim (Delta 1).
fn render_list_v2(
    view: &ListView<'_>,
    request_token: Option<&str>,
    start_after: Option<&str>,
    page: &ListPage<'_>,
) -> String {
    let encode = view.encode;
    let key_count = page.contents.len() + page.common_prefixes.len();
    let mut out = String::with_capacity(256 + key_count * 128);
    out.push_str(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    out.push_str(&format!("<Name>{}</Name>", xml_escape(view.bucket)));
    out.push_str(&format!(
        "<Prefix>{}</Prefix>",
        project_value(view.prefix, encode)
    ));
    out.push_str(&format!("<KeyCount>{key_count}</KeyCount>"));
    out.push_str(&format!("<MaxKeys>{}</MaxKeys>", view.max_keys));
    if let Some(delim) = view.delimiter {
        out.push_str(&format!(
            "<Delimiter>{}</Delimiter>",
            project_value(delim, encode)
        ));
    }
    // The `<EncodingType>` element is echoed only when the client asked for `url`, matching AWS.
    if encode {
        out.push_str("<EncodingType>url</EncodingType>");
    }
    out.push_str(&format!("<IsTruncated>{}</IsTruncated>", page.is_truncated));
    // The opaque tokens are echoed VERBATIM (only XML-escaped, never URL-encoded) so a returned
    // token resumes the listing exactly (Delta 1).
    if let Some(tok) = request_token {
        out.push_str(&format!(
            "<ContinuationToken>{}</ContinuationToken>",
            xml_escape(tok)
        ));
    }
    if let Some(next) = &page.next_key {
        out.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(&encode_continuation_token(next))
        ));
    }
    // Echo `<StartAfter>` when the request carried it (URL-encoded under `encoding-type=url`).
    if let Some(sa) = start_after {
        out.push_str(&format!(
            "<StartAfter>{}</StartAfter>",
            project_value(sa, encode)
        ));
    }
    for obj in &page.contents {
        render_contents(&mut out, obj, encode);
    }
    render_common_prefixes(&mut out, &page.common_prefixes, encode);
    out.push_str("</ListBucketResult>");
    out
}

/// Emit a v1 ListObjects `<ListBucketResult>` — the thin compat shim (`GET /bucket`, no
/// `list-type`). `Marker`-based rather than token-based: it echoes the request `<Marker>`
/// and, when truncated with a delimiter, a `<NextMarker>` (the last RETURNED item — the
/// common prefix for a rollup, `page.next_marker`) the client resends as `marker`.
fn render_list_v1(view: &ListView<'_>, request_marker: &str, page: &ListPage<'_>) -> String {
    let encode = view.encode;
    let key_count = page.contents.len() + page.common_prefixes.len();
    let mut out = String::with_capacity(256 + key_count * 128);
    out.push_str(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    out.push_str(&format!("<Name>{}</Name>", xml_escape(view.bucket)));
    out.push_str(&format!(
        "<Prefix>{}</Prefix>",
        project_value(view.prefix, encode)
    ));
    out.push_str(&format!(
        "<Marker>{}</Marker>",
        project_value(request_marker, encode)
    ));
    out.push_str(&format!("<MaxKeys>{}</MaxKeys>", view.max_keys));
    if let Some(delim) = view.delimiter {
        out.push_str(&format!(
            "<Delimiter>{}</Delimiter>",
            project_value(delim, encode)
        ));
    }
    // The `<EncodingType>` element is echoed only when the client asked for `url`, matching AWS.
    if encode {
        out.push_str("<EncodingType>url</EncodingType>");
    }
    out.push_str(&format!("<IsTruncated>{}</IsTruncated>", page.is_truncated));
    // AWS emits `<NextMarker>` in a v1 listing ONLY when a `delimiter` is set; without one the
    // client resumes from the last `<Key>` of `<Contents>` itself. Emitting it unconditionally
    // is a non-AWS superset that a conforming client ignores — match AWS and gate on delimiter
    // (issue #507 conformance nit). The value is the last RETURNED item (`next_marker`): for a
    // delimiter rollup that is the common prefix (`a/`), NOT the group's last raw key (`a/2`) —
    // a client that stores `a/` and resends it as `marker` must skip the whole group, which
    // `compute_page`'s resume filter (a rollup survives only when `cp > marker`) guarantees.
    // Under `encoding-type=url` the marker echo is URL-encoded (Delta 1).
    if let Some(next) = &page.next_marker {
        if view.delimiter.is_some() {
            out.push_str(&format!(
                "<NextMarker>{}</NextMarker>",
                project_value(next, encode)
            ));
        }
    }
    for obj in &page.contents {
        render_contents(&mut out, obj, encode);
    }
    render_common_prefixes(&mut out, &page.common_prefixes, encode);
    out.push_str("</ListBucketResult>");
    out
}

/// A `200` XML listing response. Declares `content-length` so the access-log body wrapper
/// ([`AccessLogged`]) accounts the transfer as complete (see [`empty_response`]).
fn list_response(xml: String) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .header("content-length", xml.len().to_string())
        .body(Body::from(xml))
        .expect("static response is always valid")
}

/// Mint this request's id, run the dispatcher **inside a span carrying it**, and stamp the
/// id onto the response — every response, success or failure (#529).
///
/// The span is what makes the id a join key rather than a decoration: `tracing` attaches the
/// enclosing span's fields to every event emitted under it, so once the log subscriber (#527)
/// is installed, *everything* wyrd logs while serving this request — the gateway's own error
/// record, the read path's fragment faults — carries `request_id` automatically. A client
/// reporting a failure hands the operator a `jq` selector over the whole server-side trail.
/// The response body, wrapped so the **access row is written when the transfer actually ends**
/// (#529 review).
///
/// A GET's body is a `Body::from_stream(...)` that hyper polls *after* the handler returns, so an
/// access row emitted at head time claims `200` and a near-zero duration for a transfer that may
/// still truncate, error, or be abandoned by the client. That is worse than no row: it is a
/// confident, wrong answer, in the one place a field post-mortem starts.
///
/// So the row is written exactly once, from whichever of these happens first:
///
/// * the stream ends cleanly (`Ready(None)`) — `transfer = "complete"`;
/// * the stream yields an error — `transfer = "failed"`, and the object was truncated on the wire;
/// * the body is dropped before either (the client hung up, or hyper abandoned it) —
///   `transfer = "aborted"`. Without the `Drop` arm a disconnected client would leave NO row at
///   all, which is the same hole this whole change closes.
///
/// A non-streaming response (every error path, and PUT/DELETE) has a one-shot body, so it takes
/// the same path and its row is written the moment that single frame is consumed — same shape,
/// no special case.
/// Marks a response whose STATUS LINE is a success but which nevertheless carries backend
/// failures — today exactly one shape: a bulk `DeleteObjects` answering the protocol-required
/// `200` with per-key `<Error>` rows.
///
/// [`record_access`] derives `errored` from the status line and the stream outcome, so without
/// this marker a batch in which the backend failed to delete keys counted as a healthy request:
/// `s3_request_errors` stayed flat and a partially failed *destructive* batch was indistinguishable
/// from a clean one on the error dashboard (PR #612 review). It rides in the response extensions
/// beside the [`ErrorClass`] the same handler stamps, so the counter is keyed by the seam's own
/// typed class rather than the `Terminal` default.
#[derive(Clone, Copy)]
struct PartialFailure;

/// The single class a batch reports when several keys failed with DIFFERENT classes. One request
/// raises exactly one RED sample, so it must name one class — and it names the one carrying the
/// strongest operator obligation, so a batch mixing a routine transient fault with an
/// indeterminate commit is never filed under the routine one.
fn worst_class(a: ErrorClass, b: ErrorClass) -> ErrorClass {
    fn rank(class: ErrorClass) -> u8 {
        match class {
            // "may or may not have been applied" — on a DELETE the operator cannot even tell
            // whether the data is gone. Nothing else here demands attention sooner.
            ErrorClass::Indeterminate => 3,
            // Corruption: a durable repair obligation.
            ErrorClass::Integrity => 2,
            ErrorClass::Terminal => 1,
            ErrorClass::Transient => 0,
        }
    }
    if rank(b) > rank(a) {
        b
    } else {
        a
    }
}

struct AccessLogged<S> {
    inner: S,
    request_id: RequestId,
    started: SystemTime,
    status: u16,
    bytes: u64,
    /// The `content-length` the response DECLARED, when it declared one. A GET announces the
    /// object's exact size for precisely this reason — its own code says a body truncated by a
    /// mid-stream fault "is a detectable short read, not a silent complete 200". So a stream that
    /// ends cleanly but SHORT is a truncation, and the row must say so rather than call it
    /// complete just because the stream returned EOF. (Codex review of #532.)
    declared: Option<u64>,
    finished: bool,
    span: tracing::Span,
    /// The request plane's op label and failure class, carried to the completion point so
    /// the RED sample is raised from the same place — and at the same moment — as the
    /// access row (see [`record_access`]).
    op: &'static str,
    class: ErrorClass,
    /// Whether the head carried a [`PartialFailure`] marker — a success status hiding backend
    /// faults. Carried to the completion point exactly like `class`, so the RED sample raised
    /// there counts the request as errored.
    partial_failure: bool,
    metrics: Option<tracing::Dispatch>,
}

/// Write ONE access row **and raise this request's RED sample** — the single place both are
/// emitted, so the body-carrying and body-less paths cannot drift in what they record, and the
/// log row and the metric can never disagree about how an op ended.
///
/// `request_id` is a field of the EVENT, not merely inherited from `span`: a target-scoped
/// directive (`RUST_LOG=wyrd.gateway.s3.access=info` — the natural way to keep the access plane
/// and quieten the rest) enables this event without enabling the span, whose target is the module
/// path, so inheriting alone would lose the join key under exactly that directive.
///
/// **The RED sample rides here, and that placement is the whole point.** This function is called
/// when the transfer actually ENDS, not when the head is built (`finish_response`) — a GET's body
/// is polled after the handler returns, so a latency measured at head time would report a
/// near-zero duration for a transfer that may still run for seconds, truncate, or fail. The
/// access row already made that argument for `duration_ms`; the histogram is the same number, so
/// it belongs at the same point. It is also why a caller reading the metric back must first drain
/// the response body.
#[allow(clippy::too_many_arguments)]
fn record_access(
    span: &tracing::Span,
    request_id: RequestId,
    started: SystemTime,
    status: u16,
    bytes: u64,
    outcome: &'static str,
    op: &'static str,
    class: ErrorClass,
    partial_failure: bool,
    metrics: Option<&tracing::Dispatch>,
) {
    let duration_ms = started.elapsed().map(|d| d.as_millis()).unwrap_or(0);
    span.in_scope(|| {
        tracing::info!(
            target: "wyrd.gateway.s3.access",
            request_id = %request_id,
            http_status = status,
            bytes,
            duration_ms,
            transfer = outcome,
            "request served",
        );
    });
    // What counts as an ERROR for RED is the request's *observed* fate, not just its status
    // line. A 4xx/5xx is a failed request. So is a `200` head whose body then errored
    // (`failed`) or ended short of its declared length (`truncated`): the client did not get
    // the object, and counting it a success would be the "confident, wrong answer" this
    // module's own body-wrapping exists to prevent. `aborted` is NOT an error — that is a
    // client that hung up, which is not the gateway failing.
    //
    // `partial_failure` is the third way a request can have failed while LOOKING fine: a bulk
    // `DeleteObjects` must answer `200` with per-key `<Error>` rows even when the backend failed
    // to delete them (S3 says so), so neither the status nor the stream outcome can reveal it.
    // Counting that batch a success is exactly the "confident, wrong answer" this derivation
    // already refuses for a truncated body — and here the request is destructive.
    let errored = status >= 400 || matches!(outcome, "failed" | "truncated") || partial_failure;
    emit_request_red(metrics, op, class, duration_ms as u64, errored);
}

impl<S> AccessLogged<S> {
    /// Write the row, exactly once, with the class the **head** carried.
    ///
    /// Correct for every path whose failure was already decided when the head was built: the
    /// error response's own body cannot itself fail, so the verdict stamped on it by
    /// [`gateway_error_response`] is still the most specific statement about the request when
    /// that one-shot frame is consumed. The one path this is *not* true for takes
    /// [`record_classified`](Self::record_classified) instead.
    fn record(&mut self, outcome: &'static str) {
        let class = self.class;
        self.record_classified(outcome, class);
    }

    /// Write the row, exactly once, with `class` **in place of** the head-time verdict — for
    /// the one path whose failure is not knowable until mid-body (see the `Err` arm of
    /// [`poll_next`](futures_util::Stream::poll_next)).
    fn record_classified(&mut self, outcome: &'static str, class: ErrorClass) {
        if self.finished {
            return;
        }
        self.finished = true;
        record_access(
            &self.span,
            self.request_id,
            self.started,
            self.status,
            self.bytes,
            outcome,
            self.op,
            class,
            self.partial_failure,
            self.metrics.as_ref(),
        );
    }
}

impl<S> futures_util::Stream for AccessLogged<S>
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, axum::Error>> + Unpin,
{
    type Item = Result<bytes::Bytes, axum::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.bytes += chunk.len() as u64;
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                // The client got a 200 head and then a truncated body. The row must say so —
                // this is precisely the case a head-time log would have reported as "served".
                //
                // **And the CLASS comes from THIS error, not from the head.** A streaming GET's
                // head is built before a single chunk has been read, so it carries no seam error
                // and `handle`'s head-time read defaults to `Terminal` (its documented fail-safe).
                // But the fault that actually ended the transfer arrives *here* — a D-server dying
                // mid-read raises a `TransientFault` inside the body stream — and reporting it
                // `terminal` would tell an operator "retrying cannot help" about the one failure
                // shape where retrying is the whole remedy. That is the precise
                // transient-vs-terminal distinction the counter exists to carry, inverted.
                //
                // So the seam's own classifier runs over the error the stream yielded (#577's
                // contract: consume the class, never re-derive it locally). It walks `source()`,
                // which is what makes this work through the `axum::Error` the body machinery wraps
                // every stream error in (`axum-core`'s `StreamBody::poll_frame` → `Error::new`,
                // whose `source()` is the original `BoxError`) — so the wrapping is transparent to
                // the verdict, exactly as it is for a fault a backend wrapped in its own error.
                let class = wyrd_traits::classify(&e);
                self.record_classified("failed", class);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // EOF is not the same as "all of it". A stream that ends short of the declared
                // `content-length` truncated the object on the wire — the client sees a failed
                // transfer, and a row calling it `complete` would hide exactly the fault this
                // logging exists to diagnose.
                //
                // A truncation has no error to classify — the stream simply stopped — so this
                // keeps the head-time class, which for a streaming GET is `Terminal`. That is not
                // a gap left by the `Err` arm above: `Terminal` is the same answer `classify`
                // returns for anything it cannot name, and this is precisely the unnameable case.
                let short = self.declared.is_some_and(|declared| self.bytes < declared);
                self.record(if short { "truncated" } else { "complete" });
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for AccessLogged<S> {
    fn drop(&mut self) {
        // Dropped without EOF — and that is NOT automatically an abandoned transfer.
        //
        // Hyper's HTTP/1 encoder stops polling a `content-length`-delimited body the moment the
        // declared number of bytes has been written, and then drops it: a perfectly successful GET
        // never yields `Ready(None)`. Recording `aborted` here unconditionally would therefore
        // mislabel every ordinary download — the exact opposite of what this row is for.
        // (Codex review of #532.)
        //
        // So the verdict comes from the bytes actually sent: all of what was declared ⇒ the client
        // got the whole object, whoever stopped polling first. Anything less ⇒ the transfer really
        // was abandoned (a client that hung up, or hyper tearing the connection down).
        let delivered = self.declared.is_some_and(|declared| self.bytes >= declared);
        self.record(if delivered { "complete" } else { "aborted" });
    }
}

/// Arrange for the access row to be written when the transfer actually ends, and hand back the
/// response to serve.
///
/// Two shapes, and the distinction is load-bearing:
///
/// * **A response with a body** is wrapped in [`AccessLogged`], so the row carries the *observed*
///   outcome — bytes actually sent, real duration, and whether the transfer completed, failed
///   mid-stream, or was abandoned. A GET's body is polled only after the handler returns, so a
///   row written here would claim `200` and a near-zero duration for a transfer that may still
///   truncate.
/// * **A body-less response** (`204`/`304`/`1xx` — HTTP forbids a body, so hyper is *required* not
///   to poll one and drops it instead) is complete the moment its head is written. Wrapping it
///   would send every successful DELETE, which answers `204`, straight to the `Drop` arm and
///   record it as `aborted` — a systematic lie about the most ordinary success there is, and one
///   that would read as a fleet of clients hanging up mid-download. (Codex review of #532.)
#[allow(clippy::too_many_arguments)]
fn finish_response(
    response: Response,
    method: &Method,
    span: &tracing::Span,
    request_id: RequestId,
    started: SystemTime,
    op: &'static str,
    class: ErrorClass,
    partial_failure: bool,
    metrics: Option<tracing::Dispatch>,
) -> Response {
    let status = response.status();
    // Hyper never polls the body of a `204`/`304`/`1xx` (HTTP forbids one), and it SUPPRESSES the
    // body of any response to a `HEAD` — in both cases it drops the body instead. Wrapping those
    // would send them straight to the `Drop` arm as `aborted`: every successful DELETE, and every
    // HEAD probe (an S3 client's most routine call), recorded as a client that hung up.
    let bodyless =
        matches!(status.as_u16(), 204 | 304) || status.is_informational() || method == Method::HEAD;
    if bodyless {
        record_access(
            span,
            request_id,
            started,
            status.as_u16(),
            0,
            "complete",
            op,
            class,
            partial_failure,
            metrics.as_ref(),
        );
        return response;
    }
    let declared = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    response.map(|body| {
        Body::from_stream(AccessLogged {
            inner: body.into_data_stream(),
            request_id,
            started,
            status: status.as_u16(),
            bytes: 0,
            declared,
            finished: false,
            span: span.clone(),
            op,
            class,
            partial_failure,
            metrics,
        })
    })
}

async fn handle<G>(State(state): State<AppState<G>>, req: Request) -> Response
where
    G: ObjectGateway + ContainerGateway,
{
    let request_id = state.request_ids.mint();
    let started = SystemTime::now();
    let method = req.method().clone();
    // The request plane's op label + metrics sink, read before `state` is moved into the
    // dispatcher below.
    let op = op_label(
        &method,
        req.uri().path(),
        req.uri().query().unwrap_or_default(),
    );
    let metrics = state.metrics.clone();
    let span = tracing::info_span!(
        "s3.request",
        request_id = %request_id,
        method = %req.method(),
        path = %req.uri().path(),
    );
    let response = dispatch(state, req, request_id)
        .instrument(span.clone())
        .await;

    // **The failure class, as the seam classified it — not as this layer guesses it.**
    // `gateway_error_response` runs #577's `wyrd_traits::classify` over the error's whole
    // `source()` chain and stamps the verdict onto the response, so the RED counter keys on
    // the same typed class the retry policies and the audit log read (0010 item 4: "keyed by
    // the typed failure class from item 6").
    //
    // No extension ⇒ the response carries no seam error at all: the request was refused by
    // this layer before any backend was touched (an unsigned request's 403, an unparsable
    // path's 400, an unsupported subresource's 501, an absent key's 404, a bad verb's 405).
    // Those default to `Terminal`, which is not a fallback so much as the SAME answer
    // `classify` gives — its documented fail-safe default for anything it cannot otherwise
    // name — and it is correct here on the merits: retrying an unsigned request or a missing
    // key cannot help.
    //
    // **A streaming GET's `200` head also lands on that default, and for it the default is only
    // provisional.** Its body has not been read yet, so no seam error exists to stamp — but one
    // may arrive mid-transfer (a D-server dying mid-read). The body wrapper classifies *that*
    // error where it surfaces and overrides this verdict (`AccessLogged::poll_next`'s `Err`
    // arm), so a transient mid-stream fault is not reported `terminal`. Head time is simply too
    // early to know, which is the same reason the latency sample is not taken here either.
    let class = response
        .extensions()
        .get::<ErrorClass>()
        .copied()
        .unwrap_or(ErrorClass::Terminal);

    // A success status that nonetheless carried backend faults — see [`PartialFailure`]. Read
    // here, beside the class, because both must reach the completion point where the one RED
    // sample for this request is raised.
    let partial_failure = response.extensions().get::<PartialFailure>().is_some();

    // **The access line — without it the id is a promise this crate does not keep.**
    //
    // A span is not a log record: the `fmt` layer attaches a span's fields to the *events*
    // emitted under it, and does not write span lifecycle rows. So a request that fails before
    // the gateway is even reached — an unsigned or badly-signed request, an unparsable path, an
    // unsupported verb — emitted NOTHING, and the `x-amz-request-id` handed to the client
    // selected zero server-side rows. The id was findable only for requests that happened to log
    // something else, which is the opposite of the failures it exists for (#529 review).
    //
    // One row per request, whatever the verdict, is also the first question of any field
    // post-mortem — *did the request even reach us?* — and it is the one question no error log
    // can answer, because the failure mode may be that nothing arrived.
    //
    // `request_id` is recorded on the EVENT, not merely inherited from the span: a target-scoped
    // directive (`RUST_LOG=wyrd.gateway.s3.access=info` — the natural way to keep the access
    // plane and quieten the rest) enables this event without enabling the span, whose target is
    // the module path. Inheriting alone would lose the join key under exactly that directive.
    //
    // **It is emitted when the BODY finishes, not when the head is built.** A GET returns a
    // `Body::from_stream(...)` whose chunks are read only after this handler returns, so logging
    // here would record `200` and a near-zero duration for a transfer that may still truncate or
    // fail mid-stream — a row that is actively misleading in exactly the field-diagnosis case the
    // id exists for. The body is wrapped so the row carries the *observed* outcome: bytes
    // actually sent, the real duration, and whether the transfer completed (#529 review).
    let mut response = finish_response(
        response,
        &method,
        &span,
        request_id,
        started,
        op,
        class,
        partial_failure,
        metrics,
    );

    // Stamp it on the wire too. An SDK surfaces `x-amz-request-id` in its own error
    // reporting, so the tester who hits the failure can quote the id without any
    // client-side change.
    if let Ok(value) = HeaderValue::from_str(&request_id.to_string()) {
        response.headers_mut().insert(request_id::HEADER, value);
    }
    response
}

async fn dispatch<G>(state: AppState<G>, req: Request, request_id: RequestId) -> Response
where
    G: ObjectGateway + ContainerGateway,
{
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().unwrap_or("").to_string();

    // Fail-closed auth FIRST — before the body is touched (§14:86, carry-forward item 6):
    // an unsigned/bad-sig request is refused without materialising its body.
    let payload = match sigv4::verify(
        method.as_str(),
        &path,
        &query,
        &parts.headers,
        &state.config.credentials,
        &state.config.region,
        &state.config.service,
        SystemTime::now(),
    ) {
        Ok(payload) => payload,
        Err(err) => {
            // A rejected signature is worth a line of its own: in a field experiment a
            // misconfigured client (wrong region, skewed clock, stale key) presents as a wall
            // of 403s, and "which check failed" is the whole diagnosis. `warn`, not `error` —
            // the gateway is behaving correctly; the caller is not.
            tracing::warn!(
                target: "wyrd.gateway.s3.auth",
                // On the event, not inherited from the `info_span!` — a `warn`-only or
                // `error`-only filter drops the span and would strip the join key off the
                // very line the operator is grepping for (the same reason as the 500 path).
                request_id = %request_id,
                s3_code = err.s3_code(),
                reason = %err,
                "refused an unauthenticated request",
            );
            return error_response(
                request_id,
                StatusCode::FORBIDDEN,
                err.s3_code(),
                &err.to_string(),
            );
        }
    };

    // Bucket-scoped dispatch, split off BEFORE the object-path guard (issue #507, ADR-0046
    // routing decision). A path naming a bucket but no object (`/{bucket}` or `/{bucket}/`)
    // is a bucket-level request, not the malformed object path the guard below rejects —
    // `split_bucket_key` returns `None` for both, so without this split every bucket GET
    // would answer 400. `split_bucket_key` stays UNTOUCHED (it is load-bearing for object
    // verbs); the split routes bucket-scoped GET to the listing handler and leaves every
    // other bucket-scoped method on today's 400 (#511 / 509 extend the split later).
    if let Some(bucket) = bucket_scoped_path(&path) {
        // Bulk DeleteObjects (issue #509): `POST /bucket?delete` carries an XML `<Delete>` body
        // of keys and answers a `<DeleteResult>`. It is intercepted HERE, BEFORE the subresource
        // denylist below — which lists `"delete"` (:344) and would otherwise `501` it — so the
        // bulk handler runs. `"delete"` stays on the OBJECT-path denylist (:1513) so
        // `DELETE /b/k?delete` is still `501`. `?delete` is detected with a bare-key match
        // ([`is_delete_subresource`]); `query_param` would miss a valueless `?delete`.
        if method == Method::POST && is_delete_subresource(&query) {
            // This interception runs BEFORE the denylist below, so the denylist has to be applied
            // to every OTHER key here — otherwise the bulk-delete marker is a skeleton key that
            // walks any denylisted subresource past the fence and into a destructive handler
            // (`?delete&versionId=v` deleting the CURRENT object). Refuse exactly as the denylist
            // would have.
            if let Some(sub) = foreign_subresource_on_delete(&query) {
                return error_response(
                    request_id,
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    &format!("the `{sub}` S3 subresource/operation is not supported"),
                );
            }
            return delete_objects(
                &state,
                request_id,
                &percent_decode_utf8(bucket),
                payload,
                body,
            )
            .await;
        }
        // The bucket path MUST still consult the subresource denylist first: only a listing
        // form (`?list-type=2`, the v1 bare / `marker` / `prefix` / `delimiter` / `max-keys`
        // forms, and benign params) routes to listing. `GET /bucket?acl` / `?policy` /
        // `?versions` / `?location` / `?uploads` etc. stay `501 NotImplemented` so a bucket
        // subresource op is never silently answered with a listing document (which would let
        // `aws s3api get-bucket-acl` read a listing, and would destroy 508's
        // ListMultipartUploads red). The keys are DECODED before matching so a percent-encoded
        // subresource (`?%61cl`, `?upload%73`) cannot dodge the fence (issue #507 adversary).
        if let Some(sub) = unsupported_subresource_decoded(&query) {
            return error_response(
                request_id,
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                &format!("the `{sub}` S3 subresource/operation is not supported"),
            );
        }
        return match method {
            Method::GET => {
                list_objects(&state, request_id, &percent_decode_utf8(bucket), &query).await
            }
            // Other bucket-scoped methods keep today's behaviour (bucket ops are #511/509).
            _ => error_response(
                request_id,
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "expected a bucket-scoped object path /{bucket}/{key}",
            ),
        };
    }

    let Some((bucket, key)) = split_bucket_key(&path) else {
        return error_response(
            request_id,
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "expected a bucket-scoped object path /{bucket}/{key}",
        );
    };
    // The wire path is percent-encoded (the form the client signs, so SigV4 uses it
    // verbatim above). The stored object identity is the **decoded** key, so a real SDK
    // key like `my file.txt` — sent as `/bucket/my%20file.txt` — is stored under
    // `bucket/my file.txt`, not the literal `bucket/my%20file.txt` (issue #364
    // carry-forward, real-SDK break 1). Bucket + key compose onto the M0 flat namespace.
    let object_key = format!(
        "{}/{}",
        percent_decode_utf8(bucket),
        percent_decode_utf8(key)
    );

    // Subresource / multipart query forms (?uploadId, ?partNumber, ?tagging, ?acl, …) are
    // out of scope (brief §Out-of-scope: multipart upload, ACLs, the full S3 surface).
    // Dispatch below is by method only — the query is used solely for SigV4 — so without
    // this guard a `PUT /b/k?partNumber=1&uploadId=…` (UploadPart) would silently OVERWRITE
    // the whole object and a `DELETE /b/k?tagging` would DELETE the object itself, both
    // returning 2xx. Refuse a form we do not implement rather than mishandle it.
    if let Some(sub) = unsupported_subresource(&query) {
        return error_response(
            request_id,
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            &format!("the `{sub}` S3 subresource/operation is not supported"),
        );
    }

    match method {
        Method::PUT => {
            // `x-amz-copy-source` marks this PUT as a CopyObject request (issue #504), not an
            // ordinary object PUT. Dispatch below streams the request BODY into the
            // destination key — but a copy request's payload IS the copy-source reference,
            // so its body is empty; falling through here would silently overwrite the
            // destination with zero bytes and answer 200 (data loss). Refuse it before any
            // body byte is read, mirroring the subresource guard above (:548-561) and its
            // rationale — a form this floor does not implement is refused, never silently
            // mishandled. The header need not be part of the client's SigV4 signed-header
            // set for this guard to apply, so it is read directly off the request headers.
            // Server-side copy (resolving the source dirent/inode and aliasing its chunk
            // map, returning the source's ETag) is issue #504 step 2 — gated on #503's
            // metadata model — and out of scope here.
            if parts.headers.contains_key("x-amz-copy-source") {
                return error_response(
                    request_id,
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    "CopyObject (x-amz-copy-source) is not supported",
                );
            }
            // The client's declared `Content-Type`, round-tripped verbatim on GET (ADR-0047).
            // Read from the request head BEFORE the body stream is consumed. `None` if the
            // client sent none (or a non-ASCII value) — GET then falls back to
            // `application/octet-stream`.
            let content_type = parts
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string());
            // The raw request-body byte stream (never buffered whole).
            let raw = Box::pin(
                body.into_data_stream()
                    .map(|item| item.map_err(|e| Box::new(e) as BoxError)),
            );
            let result = match payload {
                // An `aws-chunked` streaming-signature upload — the form a stock SDK sends.
                // De-frame + verify each chunk as it streams (fail-closed) and feed the raw
                // object bytes to the write path. The per-chunk signatures already
                // authenticated the body, so there is no separate content-hash to re-check
                // post-stream — hence `Unverified` to the writer (issue #364 carry-forward,
                // real-SDK streaming interop).
                sigv4::PayloadHash::Streaming(ctx) => {
                    let decoded = streaming::decode(raw, ctx);
                    state
                        .gateway
                        .put_object_streaming(
                            &object_key,
                            decoded,
                            ContentHash::Unverified,
                            content_type,
                        )
                        .await
                }
                // A single-shot **signed** body: stream it straight in; the running hash is
                // checked against the signed digest before the commit.
                sigv4::PayloadHash::Signed(hex) => {
                    state
                        .gateway
                        .put_object_streaming(
                            &object_key,
                            raw,
                            ContentHash::Expected(hex),
                            content_type,
                        )
                        .await
                }
                // A deliberately-unsigned body: stream it in with no post-stream hash check.
                sigv4::PayloadHash::Unsigned => {
                    state
                        .gateway
                        .put_object_streaming(
                            &object_key,
                            raw,
                            ContentHash::Unverified,
                            content_type,
                        )
                        .await
                }
            };
            match result {
                // Answer with the committed object's ETag (ADR-0047): S3 quotes the value.
                Ok(etag) => put_object_response(&etag),
                Err(err) => gateway_error_response(request_id, &err),
            }
        }
        // GET honours a `Range` header (206 partial content / 416) and the conditional
        // preconditions (`If-Match`/`If-None-Match`/`If-Modified-Since`/`If-Unmodified-Since`
        // → 304/412), both evaluated BEFORE any body work, and advertises
        // `Accept-Ranges: bytes` (issue #510). Extracted so the dispatch stays a thin verb
        // table (peer `list_objects`).
        Method::GET => serve_get(&state, &parts.headers, &object_key, request_id).await,
        // HEAD answers exactly GET's metadata headers with no body (issue #506), now also
        // honouring the conditional preconditions and advertising `Accept-Ranges: bytes`
        // (issue #510). Resolved via the metadata-only `head_object` seam — not
        // `get_object_streaming` — so a HEAD of a large object costs a metadata round-trip,
        // never opens the fragment stream, and never spawns the chunk-reader task GET does.
        // `finish_response` classifies every HEAD response as body-less, so hyper suppresses
        // whatever body a builder sets and no access-log change is needed here.
        Method::HEAD => serve_head(&state, &parts.headers, &object_key, request_id).await,
        Method::DELETE => match state.gateway.delete_object(&object_key).await {
            // DELETE is idempotent: removing a present or an absent key both succeed.
            Ok(_) => empty_response(StatusCode::NO_CONTENT),
            Err(err) => gateway_error_response(request_id, &err),
        },
        _ => error_response(
            request_id,
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "only object PUT, GET, HEAD, and DELETE are supported",
        ),
    }
}

/// Why buffering a bulk-delete body was refused: over the byte cap, or a broken read.
enum BufferError {
    /// The accumulated bytes would exceed the cap — refused AS READ, by construction (never by
    /// trusting `Content-Length`), before the whole body is resident.
    TooLarge,
    /// The request-body stream yielded an error before it completed (a truncated / aborted body).
    Read(#[allow(dead_code)] BoxError),
}

/// Buffer `body` whole into a `Vec<u8>`, refusing the moment the accumulated bytes would exceed
/// `cap`. The cap is a hard bound on what is materialised — the read stops at the first chunk
/// that would breach it, so an oversized body is never fully resident (issue #509, DeleteObjects
/// buffering). Used ONLY for the small, signed bulk-delete body; the object PUT/GET paths stay
/// streaming.
async fn buffer_capped(body: Body, cap: usize) -> Result<Vec<u8>, BufferError> {
    let mut stream = body.into_data_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(frame) = stream.next().await {
        let chunk = frame.map_err(|e| BufferError::Read(Box::new(e) as BoxError))?;
        if buf.len() + chunk.len() > cap {
            return Err(BufferError::TooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// A parsed, validated bulk-delete request: the requested object keys (literal, already
/// XML-entity-decoded once by the parser) and whether the client asked for a `Quiet` result.
struct DeleteRequest {
    keys: Vec<String>,
    quiet: bool,
}

/// Why a `<Delete>` body was refused — and therefore which S3 error the request answers. **Both
/// variants are returned BEFORE any `delete_object` call**, so a refused request authorises no
/// deletion whatsoever; the distinction is only about telling the client the truth.
enum DeleteRequestError {
    /// Not a well-formed / semantically valid `<Delete>` document ⇒ `400 MalformedXML`.
    Malformed,
    /// An `<Object>` carried a child that decides **which object, or whether,** the key is
    /// deleted, and this gateway cannot honour it ⇒ `501 NotImplemented`. The document is
    /// perfectly well-formed, so reporting it as malformed would misdirect the client; what is
    /// missing is the FEATURE, exactly as `?versionId` is missing on the object route. Carries the
    /// field name so the refusal says which one.
    UnhonourableObjectField(&'static str),
}

/// The `<Object>` children that decide **which object, or whether,** a key is deleted — the ones
/// that must never be silently dropped (PR #612 review).
///
/// `<VersionId>` scopes the delete to one version; `<ETag>`, `<LastModifiedTime>` and `<Size>` are
/// S3's **conditional-delete** fields, a client's "only delete this if it still looks like this".
/// Ignoring any of them converts a guarded request into an unconditional destruction of the CURRENT
/// object — the caller's precondition silently discarded, which is the worst possible failure on a
/// destructive verb: it destroys precisely the object the client was trying to protect.
///
/// Every OTHER unknown child stays inert decoration and is ignored (real S3 is lenient about
/// extras); these four are refused whole.
const UNHONOURABLE_OBJECT_FIELDS: [&str; 4] = ["VersionId", "ETag", "LastModifiedTime", "Size"];

/// `char_data`'s fail-closed `Err(())` is a MALFORMED body — this lets the extraction contract
/// keep its minimal error type while `?` lifts it into [`DeleteRequestError`] inside the parser.
impl From<()> for DeleteRequestError {
    fn from((): ()) -> Self {
        Self::Malformed
    }
}

/// The `400 MalformedXML` a DeleteObjects request answers when its body is not a well-formed
/// `<Delete>` document (or violates a semantic bound / the fail-closed key-extraction contract).
/// **The load-bearing safety invariant:** this is returned BEFORE any `delete_object` call, so a
/// rejected request authorises no deletion. `MalformedXML` is S3's own code (issue #509).
fn malformed_xml(request_id: RequestId) -> Response {
    error_response(
        request_id,
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "the XML you provided was not well-formed or did not validate against the published schema",
    )
}

/// Bulk **DeleteObjects** (issue #509): `POST /bucket?delete` with an XML `<Delete>` body of
/// keys. Deletes each named key over the existing idempotent single-object seam and answers a
/// `200` S3 `<DeleteResult>` — a per-key `<Deleted>` (removed AND absent keys, S3 delete being
/// idempotent) or `<Error>` — honouring `<Quiet>`.
///
/// **The one destructive-path invariant** (the re-plan's whole point): any body that is not a
/// well-formed `<Delete>` document is `400 MalformedXML` and touches NO key. Well-formedness is
/// delegated WHOLE to [`roxmltree`] (a full XML-1.0 DOM validator that rejects DTDs by default —
/// no XXE), so this handler writes no hand-rolled grammar production and there is no next
/// production to miss (the five prior rejections were a hand-rolled tokenizer letting one more
/// XML production through each round).
async fn delete_objects<G>(
    state: &AppState<G>,
    request_id: RequestId,
    bucket: &str,
    payload: sigv4::PayloadHash,
    body: Body,
) -> Response
where
    G: ObjectGateway + ContainerGateway,
{
    // Buffer the signed body whole under the byte cap (refused as read; see [`buffer_capped`]).
    // An over-cap body is `MalformedXML` — S3 refuses an over-large request body, and this fires
    // before any key is touched.
    let bytes = match buffer_capped(body, MAX_DELETE_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return malformed_xml(request_id),
    };

    // **Body integrity is a PRECONDITION of the destructive fan-out**, not an optional extra: the
    // keys about to be deleted must be provably the keys the client sent. The match is EXHAUSTIVE
    // so a payload mode can never be added that silently skips the check (PR #612 review).
    match &payload {
        // Verify the signed digest EXACTLY as the PUT `Signed` path does: the buffered body is
        // checked against the signed `x-amz-content-sha256` before any key is touched.
        sigv4::PayloadHash::Signed(expected) => {
            let actual = crypto::hex(&crypto::sha256(&bytes));
            if !crypto::constant_time_eq(actual.as_bytes(), expected.as_bytes()) {
                return error_response(
                    request_id,
                    StatusCode::BAD_REQUEST,
                    "XAmzContentSHA256Mismatch",
                    "the delivered body does not match the signed x-amz-content-sha256",
                );
            }
        }
        // `UNSIGNED-PAYLOAD` puts the body deliberately OUTSIDE the signature, and this gateway
        // validates no `Content-MD5`/`x-amz-checksum-*` — so nothing whatsoever proves the key
        // list arrived as sent. On a read that is a caller's own risk; on a bulk DELETE it means
        // corruption or tampering between signing and receipt can substitute keys and destroy the
        // wrong objects, irrecoverably. Real S3 will not accept a multi-object delete without an
        // integrity header either. Refuse before parsing — never delete on an unverified body.
        //
        // `aws-chunked` streaming is refused by the same rule (previously it was only refused
        // *implicitly*, because raw chunk framing is not a well-formed `<Delete>` document —
        // a fail-closed accident rather than a stated precondition).
        sigv4::PayloadHash::Unsigned | sigv4::PayloadHash::Streaming(_) => {
            return error_response(
                request_id,
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "DeleteObjects requires an integrity-protected body: send the payload digest in \
                 a signed x-amz-content-sha256 (UNSIGNED-PAYLOAD and aws-chunked streaming are \
                 refused for bulk delete, which deletes nothing unverified)",
            );
        }
    }

    // `roxmltree::Document::parse` takes `&str`, so a non-UTF-8 body cannot be a well-formed XML
    // document — it is `MalformedXML`, no key touched.
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return malformed_xml(request_id);
    };

    // Parse + walk the validated tree. ANY parse error, semantic-bound violation, or fail-closed
    // key-extraction violation ⇒ `MalformedXML`, no key touched; a version-scoped or
    // conditional-delete entry ⇒ `501`, also with no key touched.
    let request = match parse_delete_request(text) {
        Ok(request) => request,
        Err(DeleteRequestError::Malformed) => return malformed_xml(request_id),
        // Refuse the WHOLE request rather than mishandle part of it — the same rule the object
        // route applies to `?versionId` (:1582). Deleting only the unversioned entries would be a
        // partial success on a destructive op whose refused half is exactly the dangerous one.
        Err(DeleteRequestError::UnhonourableObjectField(field)) => {
            return error_response(
                request_id,
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                &format!(
                    "version-scoped and conditional deletes are not supported: an <Object> \
                     carrying a <{field}> is refused, and no key in this request was deleted"
                ),
            );
        }
    };

    // Fan out over the existing idempotent single-object delete seam (the object DELETE arm,
    // :1755): `delete_object` returns `Ok(_)` for both a removed and an already-absent key (S3
    // delete is idempotent — both are `<Deleted>`), and a per-key gateway error becomes a per-key
    // `<Error>` via the shared [`classify`] rather than failing the whole batch.
    // Bounded-concurrency fan-out ([`DELETE_FANOUT`]): `buffered` keeps at most N deletes in
    // flight and yields their outcomes in REQUEST order, so the batch costs the slowest window
    // rather than the sum of every key's latency while each row stays paired with its own key.
    let outcomes = futures_util::stream::iter(request.keys.into_iter().map(|key| {
        let object_key = format!("{bucket}/{key}");
        async move {
            let outcome = state.gateway.delete_object(&object_key).await;
            (key, outcome)
        }
    }))
    .buffered(DELETE_FANOUT)
    .collect::<Vec<_>>()
    .await;

    let mut deleted: Vec<String> = Vec::new();
    let mut errors: Vec<(String, &'static str, String)> = Vec::new();
    // The class the RED sample keys on if any key failed — the most severe seen, so a mixed batch
    // is never filed under its mildest fault. `None` while the batch is still clean.
    let mut failure_class: Option<ErrorClass> = None;
    for (key, outcome) in outcomes {
        match outcome {
            Ok(_) => deleted.push(key),
            Err(err) => {
                let (status, code, message) = classify(&err);
                let class = wyrd_traits::classify(err.as_ref());
                failure_class = Some(match failure_class {
                    Some(seen) => worst_class(seen, class),
                    None => class,
                });
                // **Record the fault before the error is discarded.** The batch still answers
                // `200` with this key's `<Error>` row, so the request-level RED path sees a
                // success and reports nothing — this is the ONLY place the backend fault is
                // observable to an operator, and it carries the same request_id the client holds.
                //
                // The key is BUCKET-QUALIFIED, for the same reason `request_id` is a field on the
                // event rather than inherited from the span: `s3.request` is an `info_span!`, so
                // under an error-only filter it is never enabled and the path is not there to
                // disambiguate. Logging the bare XML key would make the same key in two buckets
                // indistinguishable in exactly the configuration an operator alerts on. Allocated
                // only on the failure arm.
                record_gateway_error(
                    request_id,
                    &err,
                    status,
                    code,
                    Some(&format!("{bucket}/{key}")),
                );
                errors.push((key, code, message));
            }
        }
    }

    let mut response = list_response(render_delete_result(&deleted, &errors, request.quiet));
    // **A batch that failed to delete keys is not a healthy request**, even though S3 requires it
    // to answer `200` with per-key `<Error>` rows. Neither the status line nor the stream outcome
    // can carry that, so the verdict rides in the extensions to the completion point: the marker
    // makes the RED sample count as errored, and the class keys it by the seam's own typed
    // classification instead of the `Terminal` default (PR #612 review).
    if let Some(class) = failure_class {
        response.extensions_mut().insert(PartialFailure);
        response.extensions_mut().insert(class);
    }
    response
}

/// Walk a buffered body as a well-formed S3 `<Delete>` document (issue #509), returning the
/// requested keys and the `Quiet` flag, or `Err(())` (⇒ `MalformedXML`, no key touched) on ANY
/// of:
///
///  * a [`roxmltree`] parse error — the WHOLE XML-1.0 grammar (exactly one root, matched/nested
///    tags, unique attribute names, valid char/entity references, no raw `<`/`&` in an attribute
///    value, comment/PI/CDATA grammar) is validated by construction, and DTDs are rejected (no
///    XXE); we write no XML validation of our own, so there is no next production to miss;
///  * a semantic bound: a root not locally named `Delete`, an empty key list, `>1000` `<Object>`,
///    an `<Object>` with zero or `>1` `<Key>`, or `>1` `<Quiet>`;
///  * the fail-closed key-extraction contract ([`char_data`]): a `<Key>` (or `<Quiet>`) whose
///    content is not a pure character-data run.
///
/// Elements are matched by LOCAL name, namespace-insensitively (`node.tag_name().name()`), so an
/// unqualified `<Delete>`, the S3-default-namespace form, and a prefixed `<s3:Delete>` are all
/// accepted (real S3's leniency / the stock SDK body). Unknown sibling elements are ignored ONLY
/// as children of `<Delete>` / `<Object>`.
///
/// [`UNHONOURABLE_OBJECT_FIELDS`] are the exception to that leniency: they are refused
/// ([`DeleteRequestError::UnhonourableObjectField`]), never ignored. Every other unknown element is
/// inert decoration, but those four decide WHICH object — or WHETHER — the key is deleted, so
/// dropping one silently converts a guarded request into an unconditional destruction of the
/// current object (PR #612 review).
fn parse_delete_request(text: &str) -> Result<DeleteRequest, DeleteRequestError> {
    let doc = roxmltree::Document::parse(text).map_err(|_| DeleteRequestError::Malformed)?;
    let root = doc.root_element();
    if root.tag_name().name() != "Delete" {
        return Err(DeleteRequestError::Malformed);
    }
    let mut keys: Vec<String> = Vec::new();
    let mut quiet = false;
    let mut quiet_seen = false;
    for child in root.children().filter(|n| n.is_element()) {
        match child.tag_name().name() {
            "Object" => {
                // **A child that decides WHICH object, or WHETHER, the key dies must never be
                // silently dropped** — see [`UNHONOURABLE_OBJECT_FIELDS`]. A `<VersionId>` turns
                // "delete this OLD version" into "delete the CURRENT object"; an `<ETag>` /
                // `<LastModifiedTime>` / `<Size>` turns "delete this only if it still looks like
                // this" into an unconditional delete, destroying exactly the object whose failed
                // precondition was supposed to save it. Both are irrecoverable, and `versionId` is
                // already on the object route's unsupported-subresource denylist, so honouring an
                // XML spelling while refusing the query spelling was self-inconsistent too
                // (PR #612 review).
                if let Some(field) = child.children().filter(|n| n.is_element()).find_map(|n| {
                    UNHONOURABLE_OBJECT_FIELDS
                        .iter()
                        .copied()
                        .find(|f| *f == n.tag_name().name())
                }) {
                    return Err(DeleteRequestError::UnhonourableObjectField(field));
                }
                let mut key_elems = child
                    .children()
                    .filter(|n| n.is_element() && n.tag_name().name() == "Key");
                let key_elem = key_elems.next().ok_or(DeleteRequestError::Malformed)?; // zero <Key>
                if key_elems.next().is_some() {
                    return Err(DeleteRequestError::Malformed); // >1 <Key> in one <Object>
                }
                let key = char_data(&key_elem)?;
                // An empty `<Key></Key>` is rejected — never a delete of `bucket/`.
                if key.is_empty() {
                    return Err(DeleteRequestError::Malformed);
                }
                keys.push(key);
            }
            "Quiet" => {
                if quiet_seen {
                    return Err(DeleteRequestError::Malformed); // >1 <Quiet> ⇒ malformed
                }
                quiet_seen = true;
                // `<Quiet>` content is subject to the same fail-closed extraction as `<Key>`, and
                // its VALUE is validated too. S3 types the field `xs:boolean`, whose lexical space
                // is exactly `true`/`false`/`1`/`0`.
                //
                // Comparing against the literal `true` alone was wrong in both directions: a
                // garbage value (`<Quiet>garbage</Quiet>`) silently read as "verbose" and
                // authorised the whole destructive fan-out, in a parser that otherwise refuses
                // every semantic violation before touching a key; and a perfectly valid `1`
                // answered a client's quiet request with a full listing (PR #612 review).
                // Refusing costs nothing here — no key has been touched yet.
                quiet = match char_data(&child)?.as_str() {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    _ => return Err(DeleteRequestError::Malformed),
                };
            }
            // Other unknown sibling elements are ignored — S3 is lenient about extras in a
            // well-formed `<Delete>`. `<VersionId>` is NOT among them: it changes WHICH object a
            // key names, so it is refused above rather than dropped.
            _ => {}
        }
    }
    if keys.is_empty() || keys.len() > 1000 {
        return Err(DeleteRequestError::Malformed);
    }
    Ok(DeleteRequest { keys, quiet })
}

/// The character-data value of `node`, FAIL-CLOSED (issue #509 Scope (6)): `Err(())` if ANY child
/// is not a text/CDATA node — a child element, COMMENT, or processing-instruction inside a
/// `<Key>` / `<Quiet>` is malformed, never ignored (this closes the parses-but-deletes-wrong
/// class in key EXTRACTION, downstream of roxmltree's grammar gate). The value is built by
/// CONCATENATING the text/CDATA child values in document order — NOT [`roxmltree::Node::text`],
/// which is first-text-node-only and TRUNCATES a run split by a comment/PI (`<Key>a<!--x-->c`
/// → `.text()` = `a`, which is exactly why such a `<Key>` is rejected here). The value roxmltree
/// yields is ALREADY XML-entity-decoded exactly once: it is the literal object key, used verbatim
/// — NOT re-decoded, NOT percent-decoded, NOT whitespace-trimmed.
fn char_data(node: &roxmltree::Node) -> Result<String, ()> {
    let mut value = String::new();
    for child in node.children() {
        if !child.is_text() {
            return Err(());
        }
        value.push_str(child.text().unwrap_or(""));
    }
    Ok(value)
}

/// Render an S3 `<DeleteResult>` by string building with [`xml_escape`], mirroring 507's
/// `render_list_v2` / `list_response` (:913, :1032; escape :1893) — the maintainer-blessed
/// string-built output (`roxmltree` is adopted for INPUT parsing only). `Quiet=true` omits the
/// `<Deleted>` entries (the objects are still gone); `<Error>` entries are always emitted.
fn render_delete_result(
    deleted: &[String],
    errors: &[(String, &str, String)],
    quiet: bool,
) -> String {
    let mut out = String::with_capacity(128 + (deleted.len() + errors.len()) * 64);
    out.push_str(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
    );
    if !quiet {
        for key in deleted {
            out.push_str(&format!(
                "<Deleted><Key>{}</Key></Deleted>",
                xml_escape(key)
            ));
        }
    }
    for (key, code, message) in errors {
        out.push_str(&format!(
            "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
            xml_escape(key),
            xml_escape(code),
            xml_escape(message),
        ));
    }
    out.push_str("</DeleteResult>");
    out
}

/// Serve an object `GET`, honouring the `Range` header (206 partial content, 416 unsatisfiable)
/// and the `If-Match`/`If-None-Match`/`If-Modified-Since`/`If-Unmodified-Since` preconditions
/// (304/412) — issue #510. The preconditions run BEFORE any body work, off a metadata-only
/// [`ObjectGateway::head_object`] resolve (no chunk read), so a 304/412 costs no data read at
/// all — and are skipped entirely when the request carries none, so a plain or purely-ranged GET
/// pays no extra metadata round-trip. A satisfiable ranged read then fetches its metadata AND its
/// bytes from ONE resolve inside [`ObjectGateway::get_object_range`], so the `206` (or the `416`)
/// is coherent — there is no head-then-read gap in which a racing overwrite could emit a
/// version-mixed 206 that poisons an ETag-keyed cache. A plain GET keeps the single-lookup
/// streaming path, now advertising `Accept-Ranges: bytes`.
async fn serve_get<G>(
    state: &AppState<G>,
    headers: &axum::http::HeaderMap,
    object_key: &str,
    request_id: RequestId,
) -> Response
where
    G: ObjectGateway,
{
    let cond = Conditionals::from_headers(headers);
    // A multi-range, non-`bytes`, or syntactically malformed `Range` parses to `None` and is
    // IGNORED — S3 serves the full 200 for anything it cannot honour (brief out-of-scope:
    // multi-range and malformed values answer the full 200 exactly as real S3 does).
    let range = honoured_range(headers);

    // Conditionals (RFC 9110 §13.2.2 precedence, S3 comparison semantics — see
    // `evaluate_conditionals`) are evaluated against the metadata of the SAME resolve that yields
    // the body, NEVER a separate `head_object` snapshot. A separate head-then-read would reopen a
    // check-then-act window: an `If-Match` could pass against a stale snapshot while a racing
    // overwrite's bytes went out under it — a self-coherent 206 of a version the precondition never
    // authorised (issue #510 carry-forward item 2). Binding both to the one resolve costs a body
    // resolve on a request that turns out to be 304/412 (the stream is then dropped unread), a
    // trade the sign-off judged worth making to keep the precondition fence intact.
    match range {
        // A ranged read: `get_object_range` resolves the object ONCE and returns its metadata AND
        // the covering-chunk stream from that one snapshot. The conditionals judge THAT metadata,
        // so the 206's `Content-Range`/`ETag`, its bytes, and the precondition verdict are all the
        // same version (no TOCTOU); an unsatisfiable range answers `416` with that snapshot's size,
        // never a whole-object read discarded wire-side.
        Some(range) => match Arc::clone(&state.gateway)
            .get_object_range(object_key, range)
            .await
        {
            Ok(Some(RangeRead { meta, outcome })) => {
                if let Some(verdict) = evaluate_conditionals(
                    &cond,
                    meta.etag.as_deref(),
                    meta.modified,
                    epoch_secs_now(),
                ) {
                    // A 304/412 short-circuits before the range is applied (§13.2); the
                    // (possibly-open) covering-chunk stream in `outcome` is dropped unread.
                    return precondition_response(
                        verdict,
                        meta.etag.as_deref(),
                        meta.modified,
                        request_id,
                    );
                }
                match outcome {
                    RangeOutcome::Satisfiable {
                        offset,
                        len,
                        stream,
                    } => partial_content_response(&meta, offset, len, stream),
                    RangeOutcome::Unsatisfiable => range_not_satisfiable(request_id, meta.size),
                }
            }
            // The object had no committed record (or vanished before the resolve): `NoSuchKey`.
            Ok(None) => no_such_key(request_id),
            Err(err) => gateway_error_response(request_id, &err),
        },
        // No honoured range: the full object (200), now advertising `Accept-Ranges: bytes`. The
        // full-object resolve carries the same metadata the conditionals judge, so a 304/412 and
        // the served body agree on version; a firing precondition drops the (unused) body stream.
        None => match Arc::clone(&state.gateway)
            .get_object_streaming(object_key)
            .await
        {
            Ok(Some(read)) => {
                if let Some(verdict) = evaluate_conditionals(
                    &cond,
                    read.etag.as_deref(),
                    read.modified,
                    epoch_secs_now(),
                ) {
                    return precondition_response(
                        verdict,
                        read.etag.as_deref(),
                        read.modified,
                        request_id,
                    );
                }
                full_object_response(read)
            }
            Ok(None) => no_such_key(request_id),
            Err(err) => gateway_error_response(request_id, &err),
        },
    }
}

/// Serve an object `HEAD` (issue #506), honouring the conditional preconditions (304/412),
/// advertising `Accept-Ranges: bytes`, and — now that it advertises range support — honouring
/// `Range` itself (issue #510 carry-forward item 3). Metadata-only throughout: it never opens the
/// fragment stream. A HEAD carries no body, so a satisfiable `Range` is resolved from the metadata
/// size alone (no chunk read) and answered with a body-less `206` mirroring GET's
/// `Content-Range`/SPAN-`Content-Length`; an unsatisfiable one answers `416`. Conditionals are
/// evaluated FIRST (§13.2), from the same `head_object` metadata.
async fn serve_head<G>(
    state: &AppState<G>,
    headers: &axum::http::HeaderMap,
    object_key: &str,
    request_id: RequestId,
) -> Response
where
    G: ObjectGateway,
{
    let cond = Conditionals::from_headers(headers);
    // A multi-range / non-`bytes` / malformed `Range` parses to `None` and is ignored (the full
    // metadata 200), exactly as on GET.
    let range = honoured_range(headers);
    match state.gateway.head_object(object_key).await {
        Ok(Some(meta)) => {
            // 1. Conditionals first (same precedence and S3 semantics as GET).
            if let Some(verdict) =
                evaluate_conditionals(&cond, meta.etag.as_deref(), meta.modified, epoch_secs_now())
            {
                return precondition_response(
                    verdict,
                    meta.etag.as_deref(),
                    meta.modified,
                    request_id,
                );
            }
            // 2. A satisfiable `Range` → a body-less `206` with `Content-Range` and the SPAN
            //    `Content-Length`; an unsatisfiable one → `416`. Resolved from the metadata size,
            //    so a ranged HEAD stays metadata-only (no chunk read for a body a HEAD never sends).
            if let Some(range) = range {
                return match resolve_byte_range(range, meta.size) {
                    Some((offset, len)) => head_partial_content_response(&meta, offset, len),
                    None => range_not_satisfiable(request_id, meta.size),
                };
            }
            // 3. Unranged HEAD: the full metadata 200 with the object's real size, not 0 — a
            //    HEAD's `Content-Length` must match what a follow-up GET would declare (issue #506).
            let builder = apply_object_headers(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-length", meta.size.to_string()),
                meta.content_type.as_deref(),
                meta.etag.as_deref(),
                meta.modified,
            );
            builder
                .body(Body::empty())
                .expect("static response is always valid")
        }
        Ok(None) => no_such_key(request_id),
        Err(err) => gateway_error_response(request_id, &err),
    }
}

/// Apply the object-metadata headers common to a `200`/`206` object response — `Content-Type`
/// (with the S3 default fallback), `Accept-Ranges: bytes`, `ETag`, and `Last-Modified` —
/// degrading an un-renderable stored value to no header rather than panicking the response build
/// (ADR-0047, symmetric with the PUT path). `Accept-Ranges: bytes` is advertised on every object
/// read so a client learns range support even from an unranged GET/HEAD (issue #510). The
/// `Content-Length` is set by the caller (the object size on a 200, the SPAN length on a 206), so
/// it is not touched here.
fn apply_object_headers(
    mut builder: axum::http::response::Builder,
    content_type: Option<&str>,
    etag: Option<&str>,
    modified: Option<u64>,
) -> axum::http::response::Builder {
    builder = builder
        // The stored content type, or the S3 default when none was recorded (an old record, or a
        // PUT that declared none) — and also when the stored value is not a valid HTTP header
        // value (ADR-0047): the seam commits the client's declared type verbatim, so an
        // un-renderable one degrades to the default rather than panicking the read.
        .header("content-type", content_type_header(content_type))
        .header("accept-ranges", "bytes");
    // ETag and Last-Modified are additive: a record predating the metadata model carries neither
    // (ADR-0047). A stored etag decoded liberally (ADR-0045) may carry a non-header byte;
    // `etag_header`/`http_date` degrade such a value to no header rather than panicking the read.
    if let Some(value) = etag.and_then(etag_header) {
        builder = builder.header("etag", value);
    }
    if let Some(value) = modified.and_then(http_date) {
        builder = builder.header("last-modified", value);
    }
    builder
}

/// A full-object `200` streaming response (the hot GET path), now advertising
/// `Accept-Ranges: bytes`. Declares the exact object length so a body truncated by a mid-stream
/// fault is a detectable short read, not a silent "complete" 200 (issue #364 carry-forward).
fn full_object_response(read: ObjectRead) -> Response {
    let ObjectRead {
        size,
        stream,
        etag,
        content_type,
        modified,
    } = read;
    apply_object_headers(
        Response::builder()
            .status(StatusCode::OK)
            .header("content-length", size.to_string()),
        content_type.as_deref(),
        etag.as_deref(),
        modified,
    )
    .body(Body::from_stream(stream))
    .expect("streaming response is always valid")
}

/// A `206 Partial Content` response over the resolved span `[offset, offset + len)`. Declares
/// `Content-Length = len` (the SPAN length, NOT the object size) and `Content-Range: bytes
/// {offset}-{offset+len-1}/{size}`, so the access-log body wrapper's declared==streamed
/// accounting holds for the SPAN — a 206 that declared the full size would be logged as truncated
/// on every ranged GET (issue #364 span invariant, brief note on `content-length`).
fn partial_content_response(
    meta: &ObjectMeta,
    offset: u64,
    len: u64,
    stream: ObjectStream,
) -> Response {
    // A satisfiable range always has `len >= 1`, so `offset + len - 1` is the inclusive last byte.
    let last = offset + len - 1;
    apply_object_headers(
        Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-length", len.to_string())
            .header(
                "content-range",
                format!("bytes {offset}-{last}/{}", meta.size),
            ),
        meta.content_type.as_deref(),
        meta.etag.as_deref(),
        meta.modified,
    )
    .body(Body::from_stream(stream))
    .expect("streaming response is always valid")
}

/// Map an [`evaluate_conditionals`] verdict to its wire response — a `304 Not Modified` carrying
/// the object's cache validators, or a `412 Precondition Failed`. Shared by the GET (ranged and
/// unranged) and HEAD arms so a precondition answers identically everywhere, judged against the
/// same `etag`/`modified` the arm resolved (issue #510 carry-forward item 2).
fn precondition_response(
    verdict: Precondition,
    etag: Option<&str>,
    modified: Option<u64>,
    request_id: RequestId,
) -> Response {
    match verdict {
        Precondition::NotModified => not_modified_response(etag, modified),
        Precondition::Failed => precondition_failed(request_id),
    }
}

/// A HEAD's `206 Partial Content` for a satisfiable `Range`: the same `Content-Range` and SPAN
/// `Content-Length` as GET's [`partial_content_response`], but body-less — a HEAD never carries a
/// body (RFC 9110 §9.3.2). The span is resolved from the metadata size alone, so a ranged HEAD
/// costs no chunk read (issue #510 carry-forward item 3).
fn head_partial_content_response(meta: &ObjectMeta, offset: u64, len: u64) -> Response {
    // A satisfiable range always has `len >= 1`, so `offset + len - 1` is the inclusive last byte.
    let last = offset + len - 1;
    apply_object_headers(
        Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-length", len.to_string())
            .header(
                "content-range",
                format!("bytes {offset}-{last}/{}", meta.size),
            ),
        meta.content_type.as_deref(),
        meta.etag.as_deref(),
        meta.modified,
    )
    .body(Body::empty())
    .expect("static response is always valid")
}

/// A `304 Not Modified` for a passing `If-None-Match`/`If-Modified-Since` gate. It carries the
/// cache validators (`ETag`, `Last-Modified`) and advertises range support, but NO body and no
/// `Content-Type`/`Content-Length` (RFC 9110 §15.4.5). `finish_response` records a 304 as a
/// complete body-less transfer, so it needs no declared length.
fn not_modified_response(etag: Option<&str>, modified: Option<u64>) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header("accept-ranges", "bytes");
    if let Some(value) = etag.and_then(etag_header) {
        builder = builder.header("etag", value);
    }
    if let Some(value) = modified.and_then(http_date) {
        builder = builder.header("last-modified", value);
    }
    builder
        .body(Body::empty())
        .expect("static response is always valid")
}

/// A `412 Precondition Failed` for a failing `If-Match`/`If-Unmodified-Since` gate.
fn precondition_failed(request_id: RequestId) -> Response {
    error_response(
        request_id,
        StatusCode::PRECONDITION_FAILED,
        "PreconditionFailed",
        "At least one of the preconditions you specified did not hold",
    )
}

/// A `416 Range Not Satisfiable` for an unsatisfiable `Range`, carrying `Content-Range: bytes
/// */{size}` so the client learns the object's true length (RFC 9110 §14.4 / §15.5.17; S3's
/// `InvalidRange`).
fn range_not_satisfiable(request_id: RequestId, size: u64) -> Response {
    let mut response = error_response(
        request_id,
        StatusCode::RANGE_NOT_SATISFIABLE,
        "InvalidRange",
        "The requested range is not satisfiable",
    );
    if let Ok(value) = HeaderValue::from_str(&format!("bytes */{size}")) {
        response.headers_mut().insert("content-range", value);
    }
    response
}

/// The `404 NoSuchKey` an absent object answers with, shared by the GET/HEAD paths.
fn no_such_key(request_id: RequestId) -> Response {
    error_response(
        request_id,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "the specified key does not exist",
    )
}

/// The request's honoured `Range`, or `None` when it cannot be honoured (→ the full 200).
///
/// Reads ALL `Range` field lines, not just the first: under HTTP field combination (RFC 9110
/// §5.2) repeated `Range` lines are semantically ONE comma-separated multi-range set — which
/// [`parse_range`] already refuses in its in-line form. `HeaderMap::get` observes only the
/// first stored value, so going through it would honour `Range: bytes=0-1` + `Range:
/// bytes=4-5` as a `206` of the first span while silently discarding the second — a
/// half-served request instead of the documented full-200 degradation (issue #510 review).
fn honoured_range(headers: &axum::http::HeaderMap) -> Option<ByteRange> {
    let mut values = headers.get_all("range").iter();
    let first = values.next()?;
    if values.next().is_some() {
        return None;
    }
    first.to_str().ok().and_then(parse_range)
}

/// Parse a `Range` header value into a single [`ByteRange`], or `None` when the value is a
/// multi-range (`bytes=a-b,c-d`), a non-`bytes` unit, or syntactically malformed — in every
/// `None` case the caller serves the full 200 exactly as real S3 does (brief: multi-range and
/// malformed values are out of scope and answer 200). Only the `bytes` unit is honoured; `If-Range`
/// is out of scope (Alpha).
///
/// The range-unit token is matched ASCII case-insensitively (`bytes` / `Bytes` / `BYTES`): RFC 9110
/// §14.1 range-unit names are case-insensitive, so a `Range: Bytes=0-15` must serve the requested
/// slice (206) rather than silently degrade to the full 200 (PR #611 review). Only the unit is
/// case-folded — the range-spec after `=` keeps its strict, no-tolerance parsing below.
fn parse_range(value: &str) -> Option<ByteRange> {
    let (unit, spec) = value.trim().split_once('=')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    // A comma is a multi-range set — ignore it (out of scope → full 200).
    if spec.contains(',') {
        return None;
    }
    let (start, end) = spec.split_once('-')?;
    // Each position must be EMPTY or all-ASCII-digits — no sign, no interior whitespace, no
    // other byte. `u64::from_str` itself tolerates a leading `+` (`"+8".parse()` == `Ok(8)`), so
    // a `bytes=+8-+15` would otherwise be honoured as a 206; real S3 treats any such non-digit
    // byte (or interior whitespace) as malformed and serves the full 200. Reject it here, before
    // parsing, with no trim tolerance — so `+`, a space, or garbage falls on the malformed→200
    // side, not the 206 side.
    match (digits_or_empty(start)?, digits_or_empty(end)?) {
        // `bytes=-n` — a suffix of the final `n` bytes. `n == 0` is grammatically VALID but
        // unsatisfiable (`resolve_byte_range` answers it with a 416), distinct from a *malformed*
        // value which parses to `None` → 200. Real S3 answers `InvalidRange` for `bytes=-0`, so
        // it must fall on the 416 side, not the ignore-and-200 side.
        (None, Some(n)) => Some(ByteRange::Suffix(n)),
        // `bytes=a-` — from `a` to the end.
        (Some(a), None) => Some(ByteRange::From(a)),
        // `bytes=a-b` — inclusive. `b < a` is malformed; ignore it (→ full 200).
        (Some(a), Some(b)) => (b >= a).then_some(ByteRange::FromTo(a, b)),
        // `bytes=-` — empty on both sides — malformed.
        (None, None) => None,
    }
}

/// One `Range` position: `Some(None)` when EMPTY (a suffix/open form's absent side),
/// `Some(Some(n))` when it is a run of ASCII digits, and `None` when it holds ANY other byte (a
/// sign, whitespace, or garbage) — which makes the whole range malformed (`?` → full 200). This
/// is deliberately STRICTER than `u64::from_str`, which accepts a leading `+`, so a `bytes=+8-…`
/// is rejected rather than silently honoured.
fn digits_or_empty(s: &str) -> Option<Option<u64>> {
    if s.is_empty() {
        return Some(None);
    }
    if s.bytes().all(|b| b.is_ascii_digit()) {
        // All ASCII digits, but still `None` if it overflows `u64` — an un-representable range is
        // ignored (→ full 200), the same safe degrade as any value we cannot honour.
        s.parse().ok().map(Some)
    } else {
        None
    }
}

/// The conditional-precondition headers a GET/HEAD may carry (RFC 9110 §13.1), read off the
/// request head. Borrowed from the header map — no allocation, and the `to_str` skips a
/// non-ASCII value (an unparsable header is simply absent, which the gates treat as "ignore").
struct Conditionals<'a> {
    if_match: Option<&'a str>,
    if_none_match: Option<&'a str>,
    if_modified_since: Option<&'a str>,
    if_unmodified_since: Option<&'a str>,
}

impl<'a> Conditionals<'a> {
    fn from_headers(headers: &'a axum::http::HeaderMap) -> Self {
        // Inlined rather than a closure so each borrow's lifetime ties cleanly to `headers` (a
        // closure returning a reference derived from a captured borrow trips lifetime inference).
        Self {
            if_match: headers.get("if-match").and_then(|v| v.to_str().ok()),
            if_none_match: headers.get("if-none-match").and_then(|v| v.to_str().ok()),
            if_modified_since: headers
                .get("if-modified-since")
                .and_then(|v| v.to_str().ok()),
            if_unmodified_since: headers
                .get("if-unmodified-since")
                .and_then(|v| v.to_str().ok()),
        }
    }
}

/// The precondition verdict the GET/HEAD gates can short-circuit with.
enum Precondition {
    /// `304 Not Modified` — `If-None-Match` matched, or `If-Modified-Since` was not modified.
    NotModified,
    /// `412 Precondition Failed` — `If-Match` did not match, or `If-Unmodified-Since` was
    /// modified.
    Failed,
}

/// Evaluate the conditional preconditions in RFC 9110 §13.2.2 PRECEDENCE (If-Match >
/// If-Unmodified-Since; If-None-Match > If-Modified-Since) but with **S3 comparison semantics**,
/// not full RFC (S3 itself deviates): exact opaque ETag equality plus `*`; weak comparators and
/// multi-ETag lists are out of scope (stock aws clients send a single value); date comparison
/// truncates the stored epoch-millis to SECONDS (IMF-fixdate has second resolution). A record
/// with no stored ETag (`etag == None`) fails a specific `If-Match` (no current entity-tag) and a
/// record with no stored `modified` ignores the date conditionals — degrade safely, never panic.
/// An unparsable date header is ignored (RFC 9110). `None` ⇒ no precondition fired; serve the
/// object.
fn evaluate_conditionals(
    cond: &Conditionals<'_>,
    etag: Option<&str>,
    modified: Option<u64>,
    now_secs: u64,
) -> Option<Precondition> {
    // 1. If-Match takes precedence over If-Unmodified-Since (only one of the two is consulted).
    if let Some(im) = cond.if_match {
        // Strong comparison (§13.1.1): a weak `W/`-tag never matches → 412.
        if !etag_matches(im, etag, true) {
            return Some(Precondition::Failed);
        }
    } else if let Some(ius) = cond.if_unmodified_since {
        // If-Unmodified-Since: FAIL if the object was modified strictly AFTER the given instant.
        if let (Some(m), Some(since)) = (modified, parse_http_date(ius, now_secs)) {
            if m / 1_000 > since {
                return Some(Precondition::Failed);
            }
        }
    }
    // 2. If-None-Match takes precedence over If-Modified-Since.
    if let Some(inm) = cond.if_none_match {
        // A listed entity-tag matched → the "none match" precondition is false → for a safe
        // method (GET/HEAD) that is `304 Not Modified`. Weak comparison (§13.1.2).
        if etag_matches(inm, etag, false) {
            return Some(Precondition::NotModified);
        }
    } else if let Some(ims) = cond.if_modified_since {
        // If-Modified-Since: `304` if the object was NOT modified after the given instant.
        if let (Some(m), Some(since)) = (modified, parse_http_date(ims, now_secs)) {
            if m / 1_000 <= since {
                return Some(Precondition::NotModified);
            }
        }
    }
    None
}

/// Whether an `If-Match`/`If-None-Match` header value matches the object's stored ETag under S3
/// comparison semantics: `*` matches any existing entity; otherwise an exact opaque-value match on
/// the quoted value.
///
/// The wildcard tests **existence of a current representation**, not possession of a persisted
/// entity-tag (RFC 9110 §13.1.1: `If-Match: *` is false only "if the origin server does not have
/// a current representation"). Every caller evaluates conditionals AFTER resolving the object, so
/// existence is already established here and `*` matches unconditionally — including a
/// pre-ADR-0047 record whose stored ETag is `None`. Gating `*` on `stored.is_some()` would 412 an
/// `If-Match: *` overwrite guard and serve a full 200 to an `If-None-Match: *` cache probe on
/// exactly those legacy records.
///
/// `strong` selects RFC 9110's comparison function. `If-Match` uses the **strong** comparison
/// (§13.1.1): a weak entity-tag (`W/`-prefixed) NEVER matches, so `If-Match: W/"<etag>"` fails →
/// 412 — silently accepting it as a match is exactly the precondition the RFC forbids weak
/// comparison on. `If-None-Match` uses the **weak** comparison (§13.1.2), where the `W/` indicator
/// is ignored before comparing the opaque value. Weak comparators are otherwise out of *support*
/// (stock aws clients send a single strong value); this narrow split only refuses to weak-match
/// where it would be wrong, it does not add weak-tag support. A record with no stored ETag never
/// matches a **specific** tag (no current entity-tag), so a specific `If-Match` fails on it — as
/// the design requires (degrade safely on a pre-ADR-0047 record).
fn etag_matches(header_value: &str, stored: Option<&str>, strong: bool) -> bool {
    let value = header_value.trim();
    if value == "*" {
        return true;
    }
    // Under strong comparison a weak entity-tag cannot match; under weak comparison the `W/`
    // indicator is stripped before the opaque values are compared.
    let inner = match value.strip_prefix("W/") {
        Some(_) if strong => return false,
        Some(weak) => weak,
        None => value,
    };
    let inner = inner.trim_matches('"');
    stored == Some(inner)
}

/// Parse an HTTP-date into epoch SECONDS — the inverse of the [`http_date`] emitter, sharing
/// [`days_from_civil`] so no date dependency is pulled in. RFC 9110 §5.6.7 defines THREE date
/// formats and requires a recipient to accept **all three**, even though only the preferred
/// IMF-fixdate is ever emitted:
///
/// * IMF-fixdate — `Sun, 06 Nov 1994 08:49:37 GMT` (preferred; the shape `http_date` and stock
///   SDKs send);
/// * RFC-850 (obsolete) — `Sunday, 06-Nov-94 08:49:37 GMT` (a full weekday name, a two-digit year);
/// * asctime (obsolete) — `Sun Nov  6 08:49:37 1994` (a space-padded day, a four-digit year).
///
/// Parsing only IMF-fixdate made a conditional carrying an obsolete date fail OPEN: an
/// `If-Unmodified-Since` with an RFC-850/asctime value went unparsed → ignored → served `200`
/// where `412` is conformant (issue #510 carry-forward item 1). A *genuinely* malformed value
/// (unknown format, impossible calendar date, out-of-range time) still returns `None` so the
/// caller ignores that conditional (RFC 9110 §13.1.3-4). Byte-indexing below assumes ASCII, so a
/// non-ASCII value is rejected up front.
fn parse_http_date(value: &str, now_secs: u64) -> Option<u64> {
    let value = value.trim();
    if !value.is_ascii() {
        return None;
    }
    parse_imf_fixdate(value)
        .or_else(|| parse_rfc850_date(value, now_secs))
        .or_else(|| parse_asctime_date(value))
}

/// The wall clock as epoch seconds — the `now` the RFC-850 two-digit-year rule is relative to
/// ([`parse_rfc850_date`]). Kept at the wire edge so the parsers stay pure over their inputs.
fn epoch_secs_now() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Parse a fixed-width HTTP-date numeric field that MUST be all ASCII digits, into `T`. Rust's
/// integer parser accepts a leading `+`/`-` (`"+8".parse::<u64>()` == `Ok(8)`, and an `i64` year
/// also takes `-`), even though an HTTP-date component is digit-only — so a signed field like the
/// `+8` in `… 1994 +8:49:37 GMT` would slip through as `08` and fire a spurious precondition
/// instead of the malformed value being IGNORED (RFC 9110 §13.1.4). Reject an empty slice or any
/// non-digit byte (a sign, whitespace, garbage) up front — the same strict-digit contract
/// [`digits_or_empty`] enforces on a `Range` position (PR #611 review). Callers pass an
/// already-`trim`med slice for the space-padded day columns.
fn parse_date_field<T: std::str::FromStr>(field: &str) -> Option<T> {
    if field.is_empty() || !field.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    field.parse().ok()
}

/// The preferred IMF-fixdate `Www, DD Mmm YYYY HH:MM:SS GMT` — exactly 29 ASCII bytes.
fn parse_imf_fixdate(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() != 29 {
        return None;
    }
    if &value[3..5] != ", " || bytes[7] != b' ' || bytes[11] != b' ' || bytes[16] != b' ' {
        return None;
    }
    if bytes[19] != b':' || bytes[22] != b':' || &value[25..] != " GMT" {
        return None;
    }
    if !is_weekday_abbrev(&value[0..3]) {
        return None;
    }
    let day: u32 = parse_date_field(value[5..7].trim())?;
    let month = month_index(&value[8..11])?;
    let year: i64 = parse_date_field(&value[12..16])?;
    let hour: u64 = parse_date_field(&value[17..19])?;
    let minute: u64 = parse_date_field(&value[20..22])?;
    let second: u64 = parse_date_field(&value[23..25])?;
    ymd_hms_to_epoch(year, month, day, hour, minute, second)
}

/// The obsolete RFC-850 date `Weekday, DD-Mmm-YY HH:MM:SS GMT` — a variable-length weekday name,
/// then a fixed 22-byte `DD-Mmm-YY HH:MM:SS GMT` with a two-digit year. The two-digit year is
/// disambiguated exactly as RFC 9110 §5.6.7 REQUIRES: interpret it in the current century
/// first, then move it back 100 years only when that lands more than 50 years in the future of
/// `now_secs` (compared at year granularity). A fixed pivot (the `httpdate` crate's 70-cutoff,
/// this parser's previous rule) violates that MUST and misreads e.g. `-75` as 1975 while the
/// clock says 2026 — turning an `If-Unmodified-Since` carrying 2075 into a spurious `412`
/// (issue #510 review). The clock dependency is injected (`now_secs`), so the parse stays a
/// pure, testable function of its inputs.
fn parse_rfc850_date(value: &str, now_secs: u64) -> Option<u64> {
    // Split the weekday name (`Monday`..`Sunday`, variable length) off at the ", " and validate it
    // is a recognized full weekday name — an unknown token (`Xxxday, …`) makes the value malformed.
    let (weekday, rest) = value.split_once(", ")?;
    if !is_weekday_full(weekday) {
        return None;
    }
    let bytes = rest.as_bytes();
    if bytes.len() != 22 {
        return None;
    }
    if bytes[2] != b'-' || bytes[6] != b'-' || bytes[9] != b' ' || &rest[18..] != " GMT" {
        return None;
    }
    if bytes[12] != b':' || bytes[15] != b':' {
        return None;
    }
    let day: u32 = parse_date_field(&rest[0..2])?;
    let month = month_index(&rest[3..6])?;
    let yy: i64 = parse_date_field(&rest[7..9])?;
    let hour: u64 = parse_date_field(&rest[10..12])?;
    let minute: u64 = parse_date_field(&rest[13..15])?;
    let second: u64 = parse_date_field(&rest[16..18])?;
    // RFC 9110 §5.6.7: current century first; "more than 50 years in the future" → last
    // century. The cutoff is applied at FULL TIMESTAMP precision, not year precision: at
    // `now = 2026-07-20 09:00`, a candidate `2076-07-20 10:00` is 50 years and one hour ahead
    // and must resolve to 1976, while `2076-07-19` (50 years minus a day) stays 2076. The
    // comparison shifts the CANDIDATE back 50 years and tuple-compares calendar fields
    // against `now` — exact calendar arithmetic with no averaged year length, and no shifted
    // date is ever constructed, so a Feb-29 candidate needs no leap-year special case.
    let (now_year, now_month, now_day) = civil_from_days((now_secs / 86_400) as i64);
    let now_sod = now_secs % 86_400;
    let mut year = now_year - now_year % 100 + yy;
    let candidate_sod = hour * 3_600 + minute * 60 + second;
    if (year - 50, month, day, candidate_sod) > (now_year, now_month, now_day, now_sod) {
        year -= 100;
    }
    ymd_hms_to_epoch(year, month, day, hour, minute, second)
}

/// The obsolete asctime date `Www Mmm _D HH:MM:SS YYYY` — exactly 24 ASCII bytes; the day is
/// space-padded to two columns (`%e`: ` 6` or `16`).
fn parse_asctime_date(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() != 24 {
        return None;
    }
    if bytes[3] != b' ' || bytes[7] != b' ' || bytes[10] != b' ' || bytes[19] != b' ' {
        return None;
    }
    if bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    if !is_weekday_abbrev(&value[0..3]) {
        return None;
    }
    let month = month_index(&value[4..7])?;
    let day: u32 = parse_date_field(value[8..10].trim())?;
    let hour: u64 = parse_date_field(&value[11..13])?;
    let minute: u64 = parse_date_field(&value[14..16])?;
    let second: u64 = parse_date_field(&value[17..19])?;
    let year: i64 = parse_date_field(&value[20..24])?;
    ymd_hms_to_epoch(year, month, day, hour, minute, second)
}

/// The 1-based month number for a three-letter English month abbreviation, or `None` for an
/// unknown one — shared by the three [`parse_http_date`] format parsers and symmetric with the
/// [`http_date`] emitter's month table.
fn month_index(name: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    MONTHS.iter().position(|m| *m == name).map(|i| i as u32 + 1)
}

/// Whether `name` is one of the seven English weekday ABBREVIATIONS (`Mon`..`Sun`) — the leading
/// token the IMF-fixdate and asctime formats carry, and the sibling of [`is_weekday_full`]. RFC
/// 9110 §5.6.7 dates open with a weekday token; a value whose weekday is not a recognized name is
/// malformed and must be rejected here, so an unparsable conditional is IGNORED (the object is
/// served) rather than fired as a spurious `304`/`412` (PR #611 review). The parsers otherwise
/// discarded the weekday bytes entirely, accepting `Xxx, 06 Nov 1994 …` as a valid date.
fn is_weekday_abbrev(name: &str) -> bool {
    const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    DAYS.contains(&name)
}

/// Whether `name` is one of the seven English weekday FULL names (`Monday`..`Sunday`) — the token
/// the obsolete RFC-850 format carries in place of [`is_weekday_abbrev`]'s three-letter form.
fn is_weekday_full(name: &str) -> bool {
    const DAYS: [&str; 7] = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ];
    DAYS.contains(&name)
}

/// Assemble a validated `(Y, M, D, h, m, s)` into epoch SECONDS, shared by the three
/// [`parse_http_date`] format parsers. Rejects an out-of-range time (`>= 24h`/`>= 60m`/`>= 60s`)
/// or an impossible calendar date (via [`days_from_civil`]). A pre-1970 date is well-formed
/// (RFC 9110 puts no floor on the date) and is CLAMPED to epoch 0 rather than failing the parse:
/// failing would make the caller IGNORE the conditional, and for `If-Unmodified-Since` that
/// INVERTS the answer — an object modified after a pre-epoch instant must FAIL (412), but an
/// ignored IUS serves the object (200). Clamped to 0, every real object (`modified > 0`) compares
/// as "modified after", the correct IUS (412) / IMS (200) verdict for any pre-1970 date.
fn ymd_hms_to_epoch(
    year: i64,
    month: u32,
    day: u32,
    hour: u64,
    minute: u64,
    second: u64,
) -> Option<u64> {
    if hour >= 24 || minute >= 60 || second >= 60 {
        return None;
    }
    let days = days_from_civil(year, month, day)?;
    let total = days
        .checked_mul(86_400)?
        .checked_add((hour * 3_600 + minute * 60 + second) as i64)?;
    Some(total.max(0) as u64)
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian `(year, month, day)` —
/// Howard Hinnant's `days_from_civil`, the exact inverse of [`civil_from_days`]. Returns `None`
/// for an out-of-range month **or a day past the month's real length** (leap-year February
/// included) so an impossible calendar date (`30 Feb`, `31 Apr`) is ignored rather than misparsed:
/// a bare `day <= 31` check silently rolled `30 Feb` over into early March, answering a
/// conditional as if the client had named a real (later) instant. RFC 9110 §13.1.4 requires an
/// invalid date be ignored, so it must fail parsing here, not resolve to a neighbouring day.
fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = i64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(day) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    Some(era * 146_097 + doe - 719_468)
}

/// The number of days in a proleptic-Gregorian month, so [`days_from_civil`] can reject a day
/// past the month's real end (`30 Feb`, `31 Sep`). February is 29 days in a leap year, 28
/// otherwise; `month` is a validated 1..=12 at every callsite.
fn days_in_month(year: i64, month: u32) -> u32 {
    const LEN: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if month == 2 && is_leap_year(year) {
        29
    } else {
        LEN[(month as usize - 1).min(11)]
    }
}

/// Whether `year` is a Gregorian leap year (divisible by 4, except centuries not divisible by
/// 400) — so leap-day (`29 Feb`) validates in a leap year and is rejected otherwise.
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn empty_response(status: StatusCode) -> Response {
    let mut builder = Response::builder().status(status);
    // **A body-less 2xx must still DECLARE its zero length.** A successful PUT answers `200` with
    // an empty body, and hyper may drop a zero-length body without ever polling it — which sends
    // the access wrapper straight to its `Drop` arm. With no declared length there is nothing to
    // compare the bytes sent against, so an ordinary successful PUT was recorded as
    // `transfer="aborted"`: a client that hung up, except no client hung up. Declaring `0` makes
    // the accounting exact (`bytes >= declared` ⇒ delivered), and it is what the response should
    // carry on the wire anyway. (Codex review, cross-vendor pass.)
    //
    // `204` is exempt: HTTP says a No Content response should not carry `Content-Length`, and it
    // never reaches the wrapper — `finish_response` records it as complete at head time.
    if status != StatusCode::NO_CONTENT {
        builder = builder.header("content-length", "0");
    }
    builder
        .body(Body::empty())
        .expect("static response is always valid")
}

/// The `200` a successful object PUT answers with (ADR-0047): a body-less response that
/// declares its zero length (see [`empty_response`] for why the length is load-bearing)
/// and carries the committed object's **ETag** — the content digest, S3-quoted — so a
/// client can validate integrity and cache the object without a follow-up GET.
///
/// The seam types the ETag as an opaque `String`, so a NON-hex-digest implementation of
/// [`ObjectGateway`] behind this wire layer could hand back a value that is not a valid
/// HTTP header value; render it through [`etag_header`] — omitting the header rather than
/// poisoning the builder and panicking the handler — symmetric with the GET arm.
fn put_object_response(etag: &str) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header("content-length", "0");
    if let Some(value) = etag_header(etag) {
        builder = builder.header("etag", value);
    }
    builder
        .body(Body::empty())
        .expect("static response is always valid")
}

/// S3 renders `ETag` as a **quoted** string (`"<hex>"`). The stored value is the opaque
/// change-token (lowercase-hex SHA-256, ADR-0047); quoting is a wire concern applied
/// identically on PUT and GET so the two agree.
fn quote_etag(etag: &str) -> String {
    format!("\"{etag}\"")
}

/// The `ETag` header value for a PUT or GET response, or `None` when the change-token
/// cannot be rendered as a valid HTTP header value. The stored `etag` is committed by the
/// write path (ADR-0047) and decoded **liberally** at the metadata boundary (ADR-0045), so
/// a corrupt record or an out-of-band edit can leave a value carrying a non-header byte
/// (e.g. CR/LF) — and on PUT, the seam types the committed ETag as an opaque `String` a
/// non-digest [`ObjectGateway`] implementation could fill arbitrarily. Quoting such a
/// value and setting it directly would make the whole response fail to build and panic the
/// `.expect(...)` — on GET, denying **every** read of the object. So an un-renderable etag
/// DEGRADES to no `ETag` header, symmetric with [`content_type_header`]; it never breaks
/// the request. A well-formed digest — the only value this path ever mints — always
/// renders, so this is a defence against stored corruption and foreign seam
/// implementations, not the happy path.
///
/// Validity is the RFC 7232 §2.3 entity-tag grammar (`etagc`: `%x21 / %x23-7E /
/// obs-text`), not merely "a valid header value": a stored tag containing `"` passes
/// `HeaderValue`'s byte check but quotes to the malformed `"abc"def"`, which caches
/// and strict clients may reject — a defeated degradation, sent on the wire instead
/// of omitted.
fn etag_header(etag: &str) -> Option<HeaderValue> {
    let etagc = |b: u8| b == 0x21 || (0x23..=0x7E).contains(&b) || b >= 0x80;
    if etag.bytes().all(etagc) {
        HeaderValue::from_str(&quote_etag(etag)).ok()
    } else {
        None
    }
}

/// The `Content-Type` header value for a GET response. The stored `content_type` is the
/// client's declared type committed **verbatim** at the seam (ADR-0047), so it may be any
/// string — including one that is not a valid HTTP header value (control bytes,
/// non-visible-ASCII). Rendering such a value directly would make the whole GET response
/// fail to build and panic the `.expect(...)` below, denying every read of the object. So
/// an absent OR un-renderable type falls back to the S3 default `application/octet-stream`,
/// exactly as an unrecorded type does — a malformed stored type degrades the content type,
/// it never breaks the read.
fn content_type_header(content_type: Option<&str>) -> HeaderValue {
    content_type
        .and_then(|value| HeaderValue::from_str(value).ok())
        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"))
}

/// Render `epoch_millis` as an RFC-7231 IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) for
/// the `Last-Modified` header. Implemented in-tree (Howard Hinnant's civil-from-days) so no
/// HTTP-date/chrono dependency is pulled in — a new dependency would be a human-only
/// sign-off item (ADR-0003 audit + `deny.toml`), and the formatter is small.
///
/// Returns `None` past year 9999: IMF-fixdate's year is exactly four digits, so a later
/// timestamp (metadata decoding accepts any `u64` — ADR-0045) has no valid rendering and
/// the caller omits the header, symmetric with `etag_header` on a malformed stored etag.
fn http_date(epoch_millis: u64) -> Option<String> {
    const SECS_PER_DAY: u64 = 86_400;
    let secs = epoch_millis / 1_000;
    let days = (secs / SECS_PER_DAY) as i64;
    let sod = secs % SECS_PER_DAY;
    let (hour, minute, second) = (sod / 3_600, (sod % 3_600) / 60, sod % 60);
    // 1970-01-01 was a Thursday; `days` is non-negative for any epoch time.
    let weekday = (((days % 7) + 4) % 7) as usize;
    let (year, month, day) = civil_from_days(days);
    if year > 9_999 {
        return None;
    }
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    Some(format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        WEEKDAYS[weekday],
        day,
        MONTHS[(month - 1) as usize],
        year,
        hour,
        minute,
        second,
    ))
}

/// Convert a day count since the Unix epoch (1970-01-01) into `(year, month, day)` —
/// Howard Hinnant's `civil_from_days`, exact for the whole `u64`-millis range the wire
/// carries. Used only by [`http_date`].
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

/// Percent-decode a path segment to its raw bytes and interpret them as UTF-8
/// (lossily — an object key is UTF-8 text). `%XX` escapes decode; a malformed escape
/// passes through literally. This recovers the client's true key from the encoded
/// request target (e.g. `my%20file.txt` → `my file.txt`), so the stored identity is the
/// key the client used, not its wire encoding (issue #364 carry-forward, real-SDK
/// break 1). `+` is **not** treated as a space — S3 keys are path segments, not form data.
fn percent_decode_utf8(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// XML-escape the five predefined entities so a value interpolated into an S3 error body
/// cannot inject markup. The error `<Message>` can echo attacker-influenced text — e.g. a
/// signed-header name from an unauthenticated request's `SignedHeaders` list — so escaping
/// it closes an XML/markup-injection vector (issue #364 carry-forward: XML error escaping).
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// An S3 error body. It carries the **`<RequestId>`** (#529): without it a client-reported
/// failure cannot be joined to the server's record of that request, and the operator is left
/// correlating by wall-clock timestamp across every node.
fn error_response(
    request_id: RequestId,
    status: StatusCode,
    code: &str,
    message: &str,
) -> Response {
    let code = xml_escape(code);
    let message = xml_escape(message);
    // `request_id` renders as hex, so it needs no escaping — but go through the same escape
    // as the rest rather than rely on that invariant holding if the rendering ever changes.
    let request_id = xml_escape(&request_id.to_string());
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{message}</Message>\
         <RequestId>{request_id}</RequestId></Error>"
    );
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(Body::from(xml))
        .expect("static response is always valid")
}

/// Renders an error's full `source()` chain — the detail the typed errors carry and that
/// nothing has ever read.
///
/// `metadata-fdb` classifies FoundationDB's error 1021 (`commit_unknown_result`) into a typed
/// value holding the native code and a `may_still_commit` discriminator; `chunkstore-grpc`
/// classifies a transport fault into `Unavailable`/`Timeout`/`Rpc`/`Connect` and keeps the
/// gRPC `Status` as its `source()`; `ReadError::InsufficientFragments` names the chunk and
/// how many fragments of how many it found. All of it is boxed into a `BoxError` on the way
/// up, and all of it was thrown away at the wire layer. Walking the chain recovers it.
struct CauseChain<'a>(&'a (dyn std::error::Error + 'static));

impl std::fmt::Display for CauseChain<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut source = self.0.source();
        let mut first = true;
        while let Some(cause) = source {
            if !first {
                write!(f, ": ")?;
            }
            write!(f, "{cause}")?;
            first = false;
            source = cause.source();
        }
        Ok(())
    }
}

/// Map a gateway error onto an S3-compatible HTTP response. A commit `Conflict`
/// (a concurrent writer won) is 409; a payload/hash mismatch is 400; a failed
/// `aws-chunked` chunk signature is 403 (fail-closed — the body was not signed by the
/// credential holder); malformed chunk framing is 400; anything else 500.
///
/// **And it records what happened.** This function took an `err: &BoxError` and, on every
/// path that was not one of the four recognised variants, never touched it: the error was
/// collapsed into one 500 and one 18-word string, and nothing was written anywhere. An FDB
/// `CommitUnknownResult` — *the write may or may not have landed*, the one condition where
/// the client's retry policy and the durability audit both depend on knowing — was reported
/// identically to a dangling dirent, a dead D-server, a scan-cap breach, an exhausted retry
/// budget, and an unreconstructable chunk. Five root causes, one indistinguishable message,
/// no server-side record (#528).
///
/// The log line carries the full `source()` chain, and rides in the `s3.request` span, so it
/// is joined to the client's `x-amz-request-id` (#529) without any further work.
///
/// The client-facing `<Message>` deliberately stays free of internal detail — **detail to the
/// log, request id to the client**. That is also why the 500 arm's message is unchanged: what
/// was missing was never a better string for the client, it was a record for the operator.
/// Record a backend fault on the error plane — the shared body of [`gateway_error_response`] and
/// the bulk-delete per-key path, so a fault is logged IDENTICALLY whether it fails the whole
/// request or only one key of a batch.
///
/// `key` names the object when the fault is scoped to ONE entry of a bulk operation. That case is
/// why this is split out: a bulk delete answers `200` with a per-key `<Error>` row, so the
/// request-level RED path sees a success and records nothing. Without this call the backend fault
/// reached the client as an XML row and left the operator NO request-id-linked diagnostic — no
/// cause chain, no `may_still_commit`, no typed class — for a fault that may have destroyed data
/// (PR #612 review).
fn record_gateway_error(
    request_id: RequestId,
    err: &BoxError,
    status: StatusCode,
    code: &str,
    key: Option<&str>,
) {
    if status.is_server_error() {
        // `may_still_commit` is a FIELD, not prose in the message: it is the one bit an operator
        // filters on. `true` means the batch may land AFTER this response, so a re-read that sees
        // nothing proves nothing — the single hardest state to reason about, and the reason the
        // client's generic 500 is not the whole story (#515).
        let may_still_commit = err
            .downcast_ref::<wyrd_traits::CommitUnknownResult>()
            .map(|u| u.may_still_commit);
        tracing::error!(
            target: "wyrd.gateway.s3.error",
            request_id = %request_id,
            s3_code = code,
            http_status = status.as_u16(),
            may_still_commit,
            object_key = key,
            error = %err,
            cause_chain = %CauseChain(err.as_ref()),
            "the gateway failed the request",
        );
    } else {
        tracing::warn!(
            target: "wyrd.gateway.s3.error",
            request_id = %request_id,
            s3_code = code,
            http_status = status.as_u16(),
            object_key = key,
            error = %err,
            cause_chain = %CauseChain(err.as_ref()),
            "the gateway refused the request",
        );
    }
}

fn gateway_error_response(request_id: RequestId, err: &BoxError) -> Response {
    let (status, code, message) = classify(err);

    // **The level follows the status, because the error plane is an alerting surface.**
    // A 4xx is the CLIENT being wrong — a bad payload hash, malformed `aws-chunked` framing,
    // a bad chunk signature, a concurrent writer losing a CAS (409, which a correct client
    // simply retries). Those are routine, they are the caller's fault, and firing them at
    // `error` means a wall of ordinary bad uploads drowns the plane an operator watches for
    // the gateway actually failing. It would also contradict this crate's own precedent: the
    // rejected-signature 403 is deliberately a `warn`, "the gateway is behaving correctly;
    // the caller is not". A 5xx is us. (Codex review of #533.)
    //
    // Both arms carry the identical fields — including `request_id` on the EVENT, not merely
    // inherited from the `s3.request` span: that span is an `info_span!`, so under an
    // error-only filter (`--log-level error`) it is never enabled and `with_current_span(true)`
    // has no fields to attach. This line, the one an operator reaches for at 3am, would be the
    // ONE that lost the `x-amz-request-id` the client is holding.
    record_gateway_error(request_id, err, status, code, None);
    let mut response = error_response(request_id, status, code, &message);
    // **Carry the typed failure class out to the request plane.** The class is derived HERE,
    // where the backend's error still exists, by the seam's own classifier (#577's
    // `wyrd_traits::classify`, which walks the `source()` chain so a wrapped fault is still
    // named) — and it is stamped on the response rather than re-derived from the status code
    // at the completion point. Re-deriving would be a second, divergent classifier: the S3
    // status mapping above deliberately answers `500 InternalError` for BOTH a transient
    // fault and a may-have-landed commit, so the wire status simply does not carry the
    // distinction the counter is supposed to report.
    response
        .extensions_mut()
        .insert(wyrd_traits::classify(err.as_ref()));
    response
}

/// The error → (status, S3 code, client message) mapping, split out so the *classification*
/// is one expression and [`gateway_error_response`] can record it before answering.
///
/// **The undetermined-commit class is recognised here, and deliberately NOT given a bespoke S3
/// code.** The gap this doc used to describe — "the typed errors live inside `metadata-fdb`, and
/// this crate must not name a concrete backend" — is closed: `wyrd_traits::CommitUnknownResult`
/// is now a seam type (#515), and `cmd_s3` resolves a real metadata backend by configuration
/// (#454), so a *"the write may or may not have landed"* really can arrive here.
///
/// It stays `500 InternalError` on the wire, and that is a decision rather than an oversight:
///
/// * **S3 has no code for "unknown".** AWS answers exactly this situation with `InternalError`,
///   and SDK retry policies are written against the standard codes. A bespoke code would be read
///   by a stock SDK as *non*-retryable, which is strictly worse than the truth.
/// * **The distinction does not change what a correct client does.** An S3 `PUT` (same key, same
///   bytes) and a `DELETE` are idempotent, so the safe action after either a clean transient
///   failure or a may-have-landed commit is the same: retry.
/// * **The party that needs the distinction is the OPERATOR**, and it is served — the error
///   record carries `may_still_commit` as a structured field, plus the whole cause chain, so
///   `may_still_commit=true` selects exactly the commits a re-read cannot settle.
///
/// Giving the client a distinct status/code is a product decision about the S3 contract, not a
/// classification bug, and it is not one to make silently inside a logging change.
fn classify(err: &BoxError) -> (StatusCode, &'static str, String) {
    if err
        .downcast_ref::<wyrd_traits::CommitUnknownResult>()
        .is_some()
    {
        // Recognised explicitly rather than falling through the `_` arm below — the class is now
        // nameable at the seam, and an error that reaches the client as a generic 500 should at
        // least have been *seen* by the classifier.
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            // The client-facing message stays free of internal detail, as every other arm does:
            // detail to the log, request id to the client.
            "the gateway could not complete the request".to_string(),
        );
    }
    if let Some(streaming) = err.downcast_ref::<streaming::StreamingError>() {
        return match streaming {
            streaming::StreamingError::ChunkSignature => (
                StatusCode::FORBIDDEN,
                "SignatureDoesNotMatch",
                "an aws-chunked chunk signature does not verify".to_string(),
            ),
            streaming::StreamingError::Framing(what) => (
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                format!("malformed aws-chunked streaming body: {what}"),
            ),
            // A declared `x-amz-checksum-*` streaming trailer that does not match the
            // bytes actually streamed (issue #505) — a content-integrity failure, S3's own
            // code for a checksum mismatch (distinct from `SignatureDoesNotMatch`: the
            // unsigned `-TRAILER` variant has no signature to fail, only a checksum to
            // mismatch). The object is never committed (`streaming::decode` aborts the
            // write on this `Err` before `put_object_streaming` ever sees a commit).
            streaming::StreamingError::ChecksumMismatch => (
                StatusCode::BAD_REQUEST,
                "BadDigest",
                "the declared x-amz-checksum-* trailer does not match the streamed body"
                    .to_string(),
            ),
        };
    }
    match err.downcast_ref::<GatewayError>() {
        Some(GatewayError::Conflict) => (
            StatusCode::CONFLICT,
            "OperationAborted",
            "a concurrent writer won the commit".to_string(),
        ),
        Some(GatewayError::PayloadMismatch) => (
            StatusCode::BAD_REQUEST,
            "XAmzContentSHA256Mismatch",
            "the delivered body does not match the signed x-amz-content-sha256".to_string(),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "the gateway could not complete the request".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An error whose real diagnosis lives in its `source()` chain — the shape every backend
    /// error arrives in. `metadata-fdb`'s `CommitUnknownResult` carries the FDB code and
    /// `may_still_commit` this way; `chunkstore-grpc`'s `TransportError` keeps the gRPC
    /// `Status` as its source; `ReadError::InsufficientFragments` names chunk/have/need.
    #[derive(Debug)]
    struct Backend(&'static str, Option<Box<Backend>>);

    impl std::fmt::Display for Backend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Backend {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1
                .as_deref()
                .map(|e| e as &(dyn std::error::Error + 'static))
        }
    }

    /// **The fix.** An unrecognised backend error must be RECORDED — with its whole cause
    /// chain — not silently collapsed into a bare 500.
    ///
    /// Pre-fix, `gateway_error_response`'s `_` arm returned the 500 while the `err` binding
    /// sat in scope, untouched: an FDB `commit_unknown_result` (the write MAY have landed),
    /// a scan-cap breach, an exhausted retry budget, a dangling dirent and an unreconstructable
    /// chunk all produced the same 18-word string and nothing else, anywhere. The capture
    /// buffer is EMPTY — RED.
    ///
    /// Mutation guard: delete the `tracing::error!` and this fails; the assertions cannot pass
    /// against an empty buffer.
    #[test]
    fn an_unrecognised_backend_error_is_recorded_with_its_whole_cause_chain() {
        let capture = Capture::default();
        let dispatch = {
            use tracing_subscriber::layer::SubscriberExt;
            tracing::Dispatch::new(
                tracing_subscriber::registry().with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(capture.clone()),
                ),
            )
        };

        // The FDB undetermined-commit shape: the class, its native code, and the caller's
        // remedy — every bit of it present in the error and, pre-fix, every bit discarded.
        let err: BoxError = Box::new(Backend(
            "commit outcome is undetermined; the batch may or may not have been applied",
            Some(Box::new(Backend(
                "foundationdb error 1021 (commit_unknown_result); may_still_commit=false",
                None,
            ))),
        ));

        let response = tracing::dispatcher::with_default(&dispatch, || {
            gateway_error_response(RequestIds::new().mint(), &err)
        });

        // The client contract is unchanged — the fix was never a better string for the client.
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let logged = capture.contents();
        assert!(
            logged.contains("commit outcome is undetermined"),
            "the error itself must be recorded; pre-fix the buffer is EMPTY. got: {logged}"
        );
        assert!(
            logged.contains("1021") && logged.contains("may_still_commit=false"),
            "the CAUSE CHAIN must survive — the FDB code and the caller's remedy live there, \
             and a 500 that omits them is indistinguishable from four other root causes. \
             got: {logged}"
        );
        assert!(
            logged.contains(r#""target":"wyrd.gateway.s3.error""#),
            "a collector must be able to select the gateway's error plane. got: {logged}"
        );
    }

    /// **The failure record keeps its join key under an ERROR-ONLY filter** (codex review of
    /// #533).
    ///
    /// The `x-amz-request-id` a client quotes is only useful if the server-side line it selects
    /// actually carries it. The request id lives on the `s3.request` span — but that is an
    /// `info_span!`, and a production server run with `--log-level error` (or `RUST_LOG=error`)
    /// never *enables* it, so `with_current_span(true)` has no fields to attach. Relying on the
    /// span alone meant the 500 — the one line an operator is grepping for — was exactly the
    /// line that lost the join key, and only under the filter a production deployment is most
    /// likely to be running.
    ///
    /// The subscriber here is the production one in miniature: an `EnvFilter` at `error`, plus a
    /// JSON `fmt` layer with `with_current_span(true)` — the same construction as
    /// `crates/server/src/logging.rs`. The event is emitted INSIDE an `info_span!` carrying the
    /// id, so a span-only implementation would look correct at `info` and go blank here.
    ///
    /// Mutation guard: drop `request_id = %request_id` from the `tracing::error!` and this
    /// fails, while the `info`-level test above keeps passing — which is precisely the trap.
    #[test]
    fn the_failure_record_carries_the_request_id_even_when_info_spans_are_filtered_out() {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::{fmt as tsfmt, EnvFilter, Layer};

        let capture = Capture::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tsfmt::layer()
                    .json()
                    .with_writer(capture.clone())
                    .with_current_span(true)
                    .with_span_list(false)
                    .with_filter(EnvFilter::new("error")),
            ),
        );

        let request_id = RequestIds::new().mint();
        let err: BoxError = Box::new(Backend("the backend fell over", None));

        tracing::dispatcher::with_default(&dispatch, || {
            // The real shape: the handler mints the id, opens the request span, and the error
            // is recorded from inside it. Under this filter the span is never enabled.
            let span = tracing::info_span!("s3.request", request_id = %request_id);
            let _guard = span.enter();
            gateway_error_response(request_id, &err);
        });

        let logged = capture.contents();
        assert!(
            !logged.is_empty(),
            "the error event must survive an error-only filter — it is `tracing::error!`",
        );
        assert!(
            logged.contains(&request_id.to_string()),
            "the failure record must carry the request id the CLIENT was handed, even with the \
             `info` span filtered out — otherwise the 500 has no join key under exactly the \
             log level a production gateway runs at. got: {logged}",
        );
    }

    /// **The ACCESS line keeps its join key under a target-scoped filter** (codex review of
    /// #533).
    ///
    /// `RUST_LOG=wyrd.gateway.s3.access=info` is the natural way to keep the access plane while
    /// quietening everything else — and it enables *this event* without enabling the
    /// `s3.request` span, whose target is the module path, not the event's. Inheriting the id
    /// from the span alone meant the access line — the one record that proves a request even
    /// REACHED the gateway, the first question of any field post-mortem — lost the join key
    /// under exactly the directive an operator would reach for.
    ///
    /// Driven through the REAL router with `oneshot`, not by re-emitting the event here: a test
    /// that logged its own line would only prove the test can log. The request is unsigned, so
    /// it is refused with a 403 — which is fine and is the point: the access line is emitted for
    /// every request that arrives, whatever the verdict.
    ///
    /// Mutation guard: drop `request_id = %request_id` from the access event and this fails.
    #[tokio::test]
    async fn the_access_line_carries_the_request_id_under_a_target_scoped_filter() {
        use tower::ServiceExt;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::{fmt as tsfmt, EnvFilter, Layer};

        struct NoGateway;
        // Object-focused double: the default `list_container` (`Ok(None)`) is exactly the
        // no-container answer these tests need.
        impl ContainerGateway for NoGateway {}
        impl ObjectGateway for NoGateway {
            async fn put_object_streaming<S>(
                &self,
                _key: &str,
                _source: S,
                _expected: ContentHash,
                _content_type: Option<String>,
            ) -> wyrd_traits::Result<String>
            where
                S: futures_util::Stream<Item = wyrd_traits::Result<bytes::Bytes>>
                    + Send
                    + Unpin
                    + 'static,
            {
                Ok(String::new())
            }

            async fn get_object_streaming(
                self: Arc<Self>,
                _key: &str,
            ) -> wyrd_traits::Result<Option<ObjectRead>> {
                Ok(None)
            }

            async fn head_object(&self, _key: &str) -> wyrd_traits::Result<Option<ObjectMeta>> {
                Ok(None)
            }

            async fn delete_object(&self, _key: &str) -> wyrd_traits::Result<bool> {
                Ok(false)
            }
        }

        let capture = Capture::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tsfmt::layer()
                    .json()
                    .with_writer(capture.clone())
                    .with_current_span(true)
                    .with_span_list(false)
                    // The directive in question: the access plane ON, everything else — the
                    // `s3.request` span included — OFF.
                    .with_filter(EnvFilter::new("wyrd.gateway.s3.access=info")),
            ),
        );

        let router = S3Gateway::new(
            Arc::new(NoGateway),
            S3Config::new(vec![Credentials {
                access_key_id: "AKIA".into(),
                secret_access_key: "secret".into(),
            }]),
        )
        .router();

        // A thread-scoped default, not `with_default`: the request is `await`ed, and a
        // sync-scoped dispatcher would not be installed across the await points. `#[tokio::test]`
        // runs a current-thread runtime, so the whole future stays on this thread.
        let _guard = tracing::dispatcher::set_default(&dispatch);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/bucket/key")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("the router answers");

        // Unsigned ⇒ refused. Immaterial to the property: the access line is emitted for every
        // request that arrives, and it is the *arrival* this line exists to record.
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // Read the id off the head BEFORE draining — the drain consumes the response.
        let served_id = response
            .headers()
            .get(request_id::HEADER)
            .expect("every response carries the id")
            .to_str()
            .expect("ascii")
            .to_string();

        // Drain the body, as a real client does: the row is written when the TRANSFER ends, not
        // when the head is built (#529), so nothing is logged until the body is consumed.
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("drain the body");

        let logged = capture.contents();
        assert!(
            logged.contains(r#""target":"wyrd.gateway.s3.access""#),
            "the access line must be emitted under its own target directive. got: {logged}",
        );
        assert!(
            logged.contains(&served_id),
            "the access line must carry the SAME id the client was handed in \
             `x-amz-request-id` — with the `s3.request` span filtered out, inheriting it from \
             the span records nothing, and the one line proving the request arrived has no join \
             key. got: {logged}",
        );
    }

    /// **A 4xx is the caller's fault and must not page the error plane** (codex review of
    /// #533).
    ///
    /// `wyrd.gateway.s3.error` is an alerting surface. A payload-hash mismatch, a bad chunk
    /// signature, malformed framing, or a `Conflict` (409 — a concurrent writer won, which a
    /// correct client just retries) are all *routine* and all the caller's doing. Firing them
    /// at `error` buries the gateway's own failures under a wall of ordinary bad uploads, and
    /// contradicts this crate's own precedent, where the rejected-signature 403 is deliberately
    /// a `warn`.
    ///
    /// The subscriber filters at `error`, so a `warn` is dropped: an empty buffer IS the
    /// assertion. The 500 case is covered by the tests above, so both directions are pinned —
    /// without that pairing, "log nothing at all" would pass this one.
    #[test]
    fn a_client_error_is_not_recorded_on_the_error_plane() {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::{fmt as tsfmt, EnvFilter, Layer};

        let capture = Capture::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tsfmt::layer()
                    .json()
                    .with_writer(capture.clone())
                    .with_filter(EnvFilter::new("error")),
            ),
        );

        // 409: a concurrent writer won the CAS. The client retries; nothing is broken.
        let conflict: BoxError = Box::new(GatewayError::Conflict);
        let response = tracing::dispatcher::with_default(&dispatch, || {
            gateway_error_response(RequestIds::new().mint(), &conflict)
        });

        // The client contract is untouched — only the log LEVEL moved.
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            capture.contents().is_empty(),
            "a 4xx must not reach the error plane at `error` level — it is the caller being \
             wrong, not the gateway failing, and it would drown the signal an operator alerts \
             on. got: {}",
            capture.contents(),
        );
    }

    /// …and it is still RECORDED, at `warn`, with its request id — refused, not silently
    /// dropped. (The pair to the test above: together they pin "warn, not error", where either
    /// alone would also pass for "not logged at all".)
    #[test]
    fn a_client_error_is_still_recorded_at_warn_with_its_request_id() {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::{fmt as tsfmt, EnvFilter, Layer};

        let capture = Capture::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tsfmt::layer()
                    .json()
                    .with_writer(capture.clone())
                    .with_filter(EnvFilter::new("warn")),
            ),
        );

        let request_id = RequestIds::new().mint();
        let conflict: BoxError = Box::new(GatewayError::Conflict);
        tracing::dispatcher::with_default(&dispatch, || {
            gateway_error_response(request_id, &conflict)
        });

        let logged = capture.contents();
        assert!(
            logged.contains(&request_id.to_string()),
            "a refused request must still carry its request id — the client was handed one and \
             will quote it. got: {logged}",
        );
        assert!(
            logged.contains(r#""level":"WARN""#),
            "a 4xx is a `warn`: the gateway is behaving correctly; the caller is not. \
             got: {logged}",
        );
    }

    /// **An undetermined commit is RECOGNISED, and the operator can filter on it** (codex
    /// cross-vendor review).
    ///
    /// `wyrd_traits::CommitUnknownResult` — *the write may or may not have landed* — used to fall
    /// through the classifier's `_` arm into a generic 500, indistinguishable from a dangling
    /// dirent or a dead D-server. It is now nameable at the seam (#515) and the S3 gateway
    /// resolves a real metadata backend (#454), so it genuinely arrives here.
    ///
    /// The wire contract is deliberately unchanged (`500 InternalError` — S3 has no code for
    /// "unknown", and a bespoke one reads as non-retryable to a stock SDK). What changes is that
    /// the class is *seen*: `may_still_commit` is a structured field on the error record, so
    /// `may_still_commit=true` selects exactly the commits a re-read cannot settle — the hardest
    /// state in the system to reason about, and the one an operator most needs to find.
    #[test]
    fn an_undetermined_commit_is_recorded_with_its_may_still_commit_flag() {
        use tracing_subscriber::layer::SubscriberExt;

        let capture = Capture::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(capture.clone()),
            ),
        );

        let err: BoxError = Box::new(wyrd_traits::CommitUnknownResult {
            backend: "foundationdb",
            code: Some(1031),
            detail: "FoundationDB error 1031".to_string(),
            may_still_commit: true,
        });

        let response = tracing::dispatcher::with_default(&dispatch, || {
            gateway_error_response(RequestIds::new().mint(), &err)
        });

        // The wire contract is unchanged — a stock SDK still sees a retryable 500.
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let logged = capture.contents();
        assert!(
            logged.contains(r#""may_still_commit":true"#),
            "the operator must be able to select the commits a re-read CANNOT settle — that is \
             the whole distinction the client's generic 500 cannot carry: {logged}",
        );
        assert!(
            logged.contains("1031"),
            "…and the cause chain must still name the backend's own code: {logged}",
        );
    }

    /// The recognised classes keep their S3 contract exactly — this change adds a record, it
    /// does not renegotiate any status code.
    #[test]
    fn the_recognised_error_classes_keep_their_s3_codes() {
        let conflict: BoxError = Box::new(GatewayError::Conflict);
        assert_eq!(classify(&conflict).0, StatusCode::CONFLICT);
        assert_eq!(classify(&conflict).1, "OperationAborted");

        let mismatch: BoxError = Box::new(GatewayError::PayloadMismatch);
        assert_eq!(classify(&mismatch).0, StatusCode::BAD_REQUEST);
        assert_eq!(classify(&mismatch).1, "XAmzContentSHA256Mismatch");

        let sig: BoxError = Box::new(streaming::StreamingError::ChunkSignature);
        assert_eq!(classify(&sig).0, StatusCode::FORBIDDEN);
        assert_eq!(classify(&sig).1, "SignatureDoesNotMatch");
    }

    use std::sync::{Arc as StdArc, Mutex};

    /// A `MakeWriter` that appends every line into a shared buffer, so a test can read back
    /// exactly what the subscriber emitted — the only way to prove a log row EXISTS.
    #[derive(Clone, Default)]
    struct Capture(StdArc<Mutex<Vec<u8>>>);

    impl Capture {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
        type Writer = Self;
        fn make_writer(&'w self) -> Self::Writer {
            self.clone()
        }
    }

    /// A gateway that is never reached: the request under test is refused at the signature
    /// check, which is the whole point — the id must be findable for a request that never got
    /// near a backend.
    struct NoGateway;

    // Object-focused double: the default `list_container` (`Ok(None)`) is exactly the
    // no-container answer these tests need.
    impl ContainerGateway for NoGateway {}

    impl ObjectGateway for NoGateway {
        async fn put_object_streaming<S>(
            &self,
            _key: &str,
            _source: S,
            _expected: ContentHash,
            _content_type: Option<String>,
        ) -> wyrd_traits::Result<String>
        where
            S: futures_util::Stream<Item = wyrd_traits::Result<bytes::Bytes>>
                + Send
                + Unpin
                + 'static,
        {
            Ok(String::new())
        }

        async fn get_object_streaming(
            self: Arc<Self>,
            _key: &str,
        ) -> wyrd_traits::Result<Option<ObjectRead>> {
            Ok(None)
        }

        async fn head_object(&self, _key: &str) -> wyrd_traits::Result<Option<ObjectMeta>> {
            Ok(None)
        }

        async fn delete_object(&self, _key: &str) -> wyrd_traits::Result<bool> {
            Ok(false)
        }
    }

    /// **The id the client is handed must select a server-side row** — including for a request
    /// that never reaches the gateway (#529 review).
    ///
    /// A span is not a log record. The `fmt` layer attaches a span's fields to the EVENTS under
    /// it and writes no span lifecycle rows, so before the access line this crate emitted
    /// *nothing at all*: an unsigned request got an `x-amz-request-id` in its 403 and that id
    /// selected zero rows on the server. The id was findable only for requests that happened to
    /// log something else — the opposite of the failures it exists for.
    ///
    /// Driven through the REAL router with `oneshot`, not by re-emitting the event here: a test
    /// that logged its own line would only prove the test can log. The request is unsigned, so
    /// it is refused at the signature check — exactly the "common client failure" the review
    /// named.
    ///
    /// Mutation guard: delete the `tracing::info!` access line and this fails — the buffer is
    /// empty.
    #[tokio::test]
    async fn the_id_handed_to_the_client_selects_a_server_side_row_even_when_refused() {
        use tower::ServiceExt;
        use tracing_subscriber::layer::SubscriberExt;

        let capture = Capture::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(capture.clone()),
            ),
        );

        let router = S3Gateway::new(
            Arc::new(NoGateway),
            S3Config::new(vec![crate::sigv4::Credentials {
                access_key_id: "AKIA".into(),
                secret_access_key: "secret".into(),
            }]),
        )
        .router();

        // A thread-scoped default, not `with_default`: the request is awaited, and a
        // sync-scoped dispatcher would not be installed across the await points.
        let _guard = tracing::dispatcher::set_default(&dispatch);
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/bucket/key")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("the router answers");

        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "an unsigned request is refused — and that is precisely the case whose id must still \
             be findable",
        );

        let served_id = response
            .headers()
            .get(request_id::HEADER)
            .expect("every response carries the id")
            .to_str()
            .expect("ascii")
            .to_string();

        // Drain the body, as a real client does. The access row is written when the TRANSFER
        // ends, not when the head is built — a head-time row would claim `200` for a stream
        // that later truncates. Nothing is logged until the body is consumed, which is the
        // property, so the test must consume it.
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("drain the body");

        let logged = capture.contents();
        assert!(
            !logged.is_empty(),
            "the request emitted NO server-side row — the `x-amz-request-id` handed to the \
             client selects nothing, which is the whole failure #529 exists to end",
        );
        assert!(
            logged.contains(&served_id),
            "the server-side row must carry the SAME id the client was handed ({served_id}), or \
             the join key joins to nothing. got: {logged}",
        );
        assert!(
            logged.contains(r#""target":"wyrd.gateway.s3.access""#),
            "the row must be on the access plane, so a collector can select it. got: {logged}",
        );
    }

    /// **The access row reports the transfer that actually happened** (#529 review).
    ///
    /// A GET's body is a `Body::from_stream(...)` that hyper polls *after* the handler returns.
    /// A row emitted at head time therefore claims `200` with a near-zero duration for a
    /// transfer that may still truncate, fail, or be abandoned — a confident, wrong answer in
    /// the one place a field post-mortem starts. The row is written from the body wrapper
    /// instead, so it carries the observed outcome.
    ///
    /// Driven directly against `AccessLogged` rather than through the router, because the
    /// failure mode is a *mid-stream* error and no in-process gateway stub can produce one on
    /// the wire. The three fates, each pinned:
    ///
    /// * a stream that ends cleanly → `transfer="complete"` with the true byte count;
    /// * a stream that errors after some bytes → `transfer="failed"` — the client got a 200 head
    ///   and a truncated body, and the row must NOT read "served";
    /// * a body dropped before either → `transfer="aborted"`, so a client that hangs up
    ///   mid-download still leaves exactly one row rather than none.
    #[tokio::test]
    async fn the_access_row_reports_the_observed_transfer_not_the_status_line() {
        use tracing_subscriber::layer::SubscriberExt;

        async fn drive(
            chunks: Vec<Result<bytes::Bytes, axum::Error>>,
            declared: Option<u64>,
            drain: bool,
        ) -> String {
            let capture = Capture::default();
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::registry().with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(capture.clone()),
                ),
            );
            let _guard = tracing::dispatcher::set_default(&dispatch);

            let logged = AccessLogged {
                inner: futures_util::stream::iter(chunks),
                request_id: RequestIds::new().mint(),
                started: SystemTime::now(),
                status: 200,
                bytes: 0,
                declared,
                finished: false,
                span: tracing::info_span!("s3.request"),
                // This test drives the ACCESS ROW, not the RED metrics: no sink, so the
                // metric event falls through to the ambient subscriber (#575).
                op: "get",
                class: ErrorClass::Terminal,
                // These cases drive an ordinary GET, not a partially-failed batch.
                partial_failure: false,
                metrics: None,
            };
            let body = Body::from_stream(logged);
            if drain {
                let _ = axum::body::to_bytes(body, usize::MAX).await;
            } else {
                drop(body); // the client hung up
            }
            capture.contents()
        }

        let complete = drive(vec![Ok(bytes::Bytes::from_static(b"hello"))], Some(5), true).await;
        assert!(
            complete.contains(r#""transfer":"complete""#) && complete.contains(r#""bytes":5"#),
            "a stream that ends cleanly is `complete`, with the bytes actually sent: {complete}",
        );

        let failed = drive(
            vec![
                Ok(bytes::Bytes::from_static(b"partial")),
                Err(axum::Error::new(std::io::Error::other("mid-stream fault"))),
            ],
            Some(99),
            true,
        )
        .await;
        assert!(
            failed.contains(r#""transfer":"failed""#),
            "a stream that errors mid-body must NOT be recorded as served — the client got a 200 \
             head and a truncated object, which is exactly what a head-time row would have \
             hidden: {failed}",
        );
        assert!(
            !failed.contains(r#""transfer":"complete""#),
            "and it must not ALSO claim completion — one row, one verdict: {failed}",
        );

        let aborted = drive(
            vec![Ok(bytes::Bytes::from_static(b"unread"))],
            Some(6),
            false,
        )
        .await;
        assert!(
            aborted.contains(r#""transfer":"aborted""#),
            "a body dropped before it finishes (the client hung up) must still leave ONE row — \
             without the Drop arm it would leave none: {aborted}",
        );

        // EOF is not the same as "all of it". A GET declares the object's exact `content-length`
        // so a short read is DETECTABLE — the response builder says so in as many words — and a
        // stream that ends cleanly but short truncated the object on the wire. Calling that
        // `complete` because the stream returned EOF hides the very fault this row exists for.
        // (Codex review of #532.)
        let truncated = drive(
            vec![Ok(bytes::Bytes::from_static(b"half"))],
            Some(1024),
            true,
        )
        .await;
        assert!(
            truncated.contains(r#""transfer":"truncated""#),
            "a stream that ends short of the declared content-length is a TRUNCATION, not a \
             completion — the client received a partial object: {truncated}",
        );
        assert!(
            !truncated.contains(r#""transfer":"complete""#),
            "…and it must not also claim completion — one row, one verdict: {truncated}",
        );

        // **Hyper drops a content-length body once it has written the declared bytes — it never
        // polls for EOF.** So an ORDINARY successful GET reaches the `Drop` arm, and a `Drop` that
        // says "aborted" unconditionally would mislabel every download in the fleet. The verdict
        // must come from the bytes actually sent. (Codex review of #532.)
        //
        // Simulated exactly: consume the declared 5 bytes, then drop without polling to EOF.
        let delivered_then_dropped = {
            let capture = Capture::default();
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::registry().with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(capture.clone()),
                ),
            );
            let _guard = tracing::dispatcher::set_default(&dispatch);

            let mut logged = AccessLogged {
                inner: futures_util::stream::iter(vec![Ok(bytes::Bytes::from_static(b"hello"))]),
                request_id: RequestIds::new().mint(),
                started: SystemTime::now(),
                status: 200,
                bytes: 0,
                declared: Some(5),
                finished: false,
                span: tracing::info_span!("s3.request"),
                op: "get",
                class: ErrorClass::Terminal,
                // These cases drive an ordinary GET, not a partially-failed batch.
                partial_failure: false,
                metrics: None,
            };
            let _ = futures_util::StreamExt::next(&mut logged).await; // the 5 declared bytes
            drop(logged); // hyper stops here: no EOF poll
            capture.contents()
        };
        assert!(
            delivered_then_dropped.contains(r#""transfer":"complete""#),
            "a body that delivered every declared byte and was then dropped unpolled is a \
             SUCCESSFUL download — that is what hyper does with every content-length response, \
             and calling it `aborted` would mislabel every ordinary GET: \
             {delivered_then_dropped}",
        );
        assert!(
            !delivered_then_dropped.contains(r#""transfer":"aborted""#),
            "…and it must not read as a client that hung up: {delivered_then_dropped}",
        );
    }

    /// **A body-less response is `complete`, not `aborted`** (codex review of #532).
    ///
    /// HTTP forbids a body on `204`/`304`/`1xx`, so hyper is *required* not to poll one — it drops
    /// the body instead. The first draft wrapped every response unconditionally, so the `Drop` arm
    /// fired and recorded `transfer="aborted"` for every successful DELETE (which answers `204`):
    /// a systematic lie about the most ordinary success there is, and one that would have read as
    /// a fleet of clients hanging up mid-download.
    ///
    /// Driven against `finish_response` — the production function `handle` calls — with the body
    /// left UNDRAINED, because that is exactly what hyper does with a `204` and what made the bug
    /// fire. The body-carrying case is included as the control: there, an undrained body genuinely
    /// IS an abandoned transfer, and must still say so.
    #[tokio::test]
    async fn a_bodyless_response_is_recorded_complete_not_aborted() {
        use tracing_subscriber::layer::SubscriberExt;

        async fn drive(response: Response, method: Method, drain: bool) -> String {
            let capture = Capture::default();
            let dispatch = tracing::Dispatch::new(
                tracing_subscriber::registry().with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_writer(capture.clone()),
                ),
            );
            let _guard = tracing::dispatcher::set_default(&dispatch);

            let span = tracing::info_span!("s3.request");
            let finished = finish_response(
                response,
                &method,
                &span,
                RequestIds::new().mint(),
                SystemTime::now(),
                // This case exercises response FINISHING, not routing: an ordinary object path
                // with no query keeps the label purely method-derived.
                op_label(&method, "/bucket/key", ""),
                ErrorClass::Terminal,
                false,
                None,
            );
            if drain {
                let _ = axum::body::to_bytes(finished.into_body(), usize::MAX).await;
            } else {
                drop(finished);
            }
            capture.contents()
        }

        // A successful DELETE: 204, no body, and hyper will never poll it.
        let logged = drive(
            empty_response(StatusCode::NO_CONTENT),
            Method::DELETE,
            false,
        )
        .await;
        assert!(
            logged.contains(r#""transfer":"complete""#),
            "a 204 carries no body and hyper never polls it — the row must say `complete`. \
             Wrapping it sent every successful DELETE to the Drop arm as `aborted`. got: {logged}",
        );
        assert!(
            !logged.contains(r#""transfer":"aborted""#),
            "…and it must NOT read as an abandoned transfer: there was no client to hang up. \
             got: {logged}",
        );

        // The control: a response that DOES carry a body, dropped without being read, is a
        // genuinely abandoned transfer and must still be recorded as one — otherwise the fix
        // above would have been "call everything complete", which reports nothing.
        let abandoned = drive(
            error_response(
                RequestIds::new().mint(),
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "unsigned",
            ),
            Method::GET,
            false,
        )
        .await;
        assert!(
            abandoned.contains(r#""transfer":"aborted""#),
            "a body that is dropped unread IS an abandoned transfer — the 204 fix must not \
             flatten every outcome to `complete`. got: {abandoned}",
        );

        // A successful PUT answers `200` with an EMPTY body, and hyper may drop a zero-length
        // body without ever polling it — straight to the `Drop` arm. With no declared length
        // there was nothing to compare the bytes sent against, so every ordinary successful PUT
        // was recorded as `aborted`: a client that hung up, except none did. `empty_response`
        // now declares `content-length: 0`, which makes the accounting exact.
        // (Codex review, cross-vendor pass.)
        let put_ok = drive(empty_response(StatusCode::OK), Method::PUT, false).await;
        assert!(
            put_ok.contains(r#""transfer":"complete""#)
                && !put_ok.contains(r#""transfer":"aborted""#),
            "a successful PUT (200, empty body, dropped unpolled by hyper) is a COMPLETED \
             request — recording it as an aborted transfer would mislabel every upload in the \
             fleet: {put_ok}",
        );

        // HEAD: hyper SUPPRESSES the body of any response to a HEAD and drops it unpolled, so a
        // wrapped HEAD lands in the `Drop` arm and reads as `aborted` — for an S3 client's most
        // routine probe. The response here carries a body (a 403 refusal); what makes it bodyless
        // on the wire is the METHOD, which is why the status alone was not enough.
        // (Codex review of #532.)
        let head = drive(
            error_response(
                RequestIds::new().mint(),
                StatusCode::FORBIDDEN,
                "AccessDenied",
                "unsigned",
            ),
            Method::HEAD,
            false,
        )
        .await;
        assert!(
            head.contains(r#""transfer":"complete""#) && !head.contains(r#""transfer":"aborted""#),
            "a HEAD response is body-less on the wire whatever its status — hyper never polls it, \
             so it must be recorded `complete`, not as a client that hung up: {head}",
        );
    }

    #[test]
    fn split_bucket_key_parses_object_paths() {
        assert_eq!(split_bucket_key("/bucket/key"), Some(("bucket", "key")));
        assert_eq!(
            split_bucket_key("/bucket/nested/key"),
            Some(("bucket", "nested/key"))
        );
        assert_eq!(split_bucket_key("/bucket/"), None);
        assert_eq!(split_bucket_key("/bucket"), None);
        assert_eq!(split_bucket_key("/"), None);
    }

    /// Bulk `DeleteObjects` must land in the `delete` RED series, not `other`. It is a POST on the
    /// wire, so a method-only label pooled it with unsupported methods and left delete dashboards
    /// and alerts blind to the one path that removes up to 1000 objects per request (PR #612
    /// review). The label must track the route's own interception predicate — bucket-scoped path
    /// AND a bare `?delete` — so it never claims a request the bulk handler did not take.
    #[test]
    fn op_label_attributes_bulk_delete_to_the_delete_series() {
        // The bulk-delete route: a bucket-scoped POST carrying a bare `?delete`.
        assert_eq!(op_label(&Method::POST, "/bucket", "delete"), "delete");
        assert_eq!(op_label(&Method::POST, "/bucket/", "delete"), "delete");
        // Ordinary methods keep their method-derived label.
        assert_eq!(op_label(&Method::GET, "/bucket/key", ""), "get");
        assert_eq!(op_label(&Method::DELETE, "/bucket/key", ""), "delete");
        assert_eq!(op_label(&Method::PUT, "/bucket/key", ""), "put");
        assert_eq!(op_label(&Method::HEAD, "/bucket/key", ""), "head");
        // A POST the bulk handler does NOT take stays `other`, so the delete series is never
        // credited with a request that deleted nothing: an OBJECT path (the route only fires on
        // a bucket-scoped path), and a bucket POST with no `?delete` marker.
        assert_eq!(op_label(&Method::POST, "/bucket/key", "delete"), "other");
        assert_eq!(op_label(&Method::POST, "/bucket", ""), "other");
        assert_eq!(op_label(&Method::POST, "/bucket", "uploads"), "other");
    }

    #[test]
    fn bucket_scoped_path_names_buckets_and_rejects_empty_segments() {
        // Bucket-scoped forms: `/{bucket}` and `/{bucket}/` name a bucket.
        assert_eq!(bucket_scoped_path("/bucket"), Some("bucket"));
        assert_eq!(bucket_scoped_path("/bucket/"), Some("bucket"));
        // Object paths are NOT bucket-scoped (they belong to `split_bucket_key`).
        assert_eq!(bucket_scoped_path("/bucket/key"), None);
        assert_eq!(bucket_scoped_path("/bucket/nested/key"), None);
        // The root path names no bucket.
        assert_eq!(bucket_scoped_path("/"), None);
        // Empty-bucket-segment forms must NOT be answered as a listing (sign-off #507
        // adversary): `trim_start_matches('/')` folded `//bucket` down to `bucket` and
        // returned a bogus 200; a single `strip_prefix('/')` keeps the empty first segment.
        assert_eq!(bucket_scoped_path("//bucket"), None);
        assert_eq!(bucket_scoped_path("//"), None);
        assert_eq!(bucket_scoped_path("///"), None);
    }

    #[test]
    fn unsupported_subresource_flags_multipart_and_subresource_forms() {
        // Destructive if mishandled: UploadPart would overwrite the whole object,
        // ?tagging DELETE would delete the object. These must be refused (501).
        assert_eq!(
            unsupported_subresource("partNumber=1&uploadId=abc"),
            Some("partNumber")
        );
        assert_eq!(unsupported_subresource("uploads"), Some("uploads"));
        assert_eq!(unsupported_subresource("tagging"), Some("tagging"));
        assert_eq!(unsupported_subresource("acl"), Some("acl"));
        assert_eq!(unsupported_subresource("versionId=3"), Some("versionId"));
        // Ordinary object requests pass: empty query, and the benign x-id a real SDK adds.
        assert_eq!(unsupported_subresource(""), None);
        assert_eq!(unsupported_subresource("x-id=PutObject"), None);
        assert_eq!(unsupported_subresource("x-id=GetObject"), None);
    }

    #[test]
    fn percent_decode_recovers_the_true_key() {
        // The real-SDK break: `my file.txt` arrives as `my%20file.txt` and must be stored
        // under the decoded key, not the literal `%20` form.
        assert_eq!(percent_decode_utf8("my%20file.txt"), "my file.txt");
        assert_eq!(percent_decode_utf8("a%2Fb"), "a/b");
        assert_eq!(percent_decode_utf8("plain-key"), "plain-key");
        // `+` is a literal in a path segment, not a space.
        assert_eq!(percent_decode_utf8("a+b"), "a+b");
        // A malformed escape passes through rather than erroring.
        assert_eq!(percent_decode_utf8("100%done"), "100%done");
    }

    #[test]
    fn xml_escape_neutralises_markup_injection() {
        // An attacker-influenced signed-header name cannot break out of <Message>.
        assert_eq!(
            xml_escape("signed header `<x>&\"'` absent"),
            "signed header `&lt;x&gt;&amp;&quot;&apos;` absent"
        );
        assert_eq!(xml_escape("NoSuchKey"), "NoSuchKey");
    }

    #[test]
    fn content_type_header_falls_back_when_the_stored_value_is_not_a_valid_header() {
        // A well-formed stored type is rendered verbatim.
        assert_eq!(
            content_type_header(Some("text/plain; charset=utf-8")),
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        // No stored type -> the S3 default (an old record, or a PUT that declared none).
        assert_eq!(
            content_type_header(None),
            HeaderValue::from_static("application/octet-stream"),
        );
        // A stored type that is NOT a valid HTTP header value (control bytes / CRLF, which
        // the seam commits verbatim and #504/#506 can pass through) degrades to the default
        // instead of yielding an error the GET builder would `.expect(...)`-panic on.
        assert_eq!(
            content_type_header(Some("text/plain\r\ninjected: header")),
            HeaderValue::from_static("application/octet-stream"),
        );
        assert_eq!(
            content_type_header(Some("bad\u{0}byte")),
            HeaderValue::from_static("application/octet-stream"),
        );
    }

    #[test]
    fn etag_header_renders_only_well_formed_entity_tags() {
        // The value this path actually mints — a lowercase-hex digest — renders quoted.
        assert_eq!(
            etag_header("0badc0de0badc0de"),
            Some(HeaderValue::from_static("\"0badc0de0badc0de\"")),
        );
        // Not a valid HTTP header value (CR/LF) — degrades to no header.
        assert_eq!(etag_header("0badc0de\r\ninjected: header"), None);
        // A VALID header value that is not a valid entity-tag (RFC 7232 §2.3): an
        // embedded `"` passes `HeaderValue`'s byte check but would quote to the
        // malformed `"abc"def"` — strict clients and caches may reject the response,
        // defeating the degradation. It must be omitted, not sent malformed.
        assert_eq!(etag_header("abc\"def"), None);
        // Spaces are header-valid but outside `etagc` too.
        assert_eq!(etag_header("abc def"), None);
    }

    #[test]
    fn a_contents_row_omits_last_modified_when_the_store_never_recorded_one() {
        // An object whose metadata predates the timestamp model (`modified: None`) must NOT
        // be rendered with a fabricated epoch `<LastModified>1970-01-01…`: sync tools compare
        // `LastModified` to decide whether to transfer, and a zero backfill makes every local
        // copy look newer, silently leaving stale content in place. The element is omitted —
        // the same degradation the absent ETag right beside it already uses.
        let mut out = String::new();
        render_contents(
            &mut out,
            &ListedObject {
                key: "old-object".to_string(),
                size: 3,
                etag: None,
                modified: None,
            },
            false,
        );
        assert!(
            !out.contains("<LastModified>"),
            "an unrecorded modified time must be omitted, never backfilled: {out}"
        );

        // A recorded timestamp still renders.
        let mut out = String::new();
        render_contents(
            &mut out,
            &ListedObject {
                key: "new-object".to_string(),
                size: 3,
                etag: None,
                modified: Some(1_700_000_000_000),
            },
            false,
        );
        assert!(
            out.contains(&format!(
                "<LastModified>{}</LastModified>",
                iso8601(1_700_000_000_000).expect("in-range instant renders")
            )),
            "a recorded modified time renders as ISO-8601: {out}"
        );
    }

    #[test]
    fn a_contents_row_omits_unpresentable_recorded_metadata() {
        // A RECORDED value can still be unpresentable; each degrades by omission so one
        // pathological record cannot poison the whole listing document (the GET path's
        // established behaviour, applied to the listing renderer).
        //
        // A `modified` past year 9999 has no valid RFC-3339 rendering (a five-digit year) —
        // the same bound `http_date` applies to the `Last-Modified` header on GET.
        let mut out = String::new();
        render_contents(
            &mut out,
            &ListedObject {
                key: "far-future".to_string(),
                size: 3,
                etag: None,
                modified: Some(253_402_300_800_000),
            },
            false,
        );
        assert!(
            !out.contains("<LastModified>"),
            "a year-10000+ modified time must be omitted, not rendered malformed: {out}"
        );

        // A stored ETag that is not a well-formed entity-tag (here: an XML-1.0-forbidden
        // control byte `xml_escape` cannot neutralise) is omitted via the same `etag_header`
        // validation the GET path uses — never emitted into the document.
        let mut out = String::new();
        render_contents(
            &mut out,
            &ListedObject {
                key: "corrupt-etag".to_string(),
                size: 3,
                etag: Some("abc\u{0008}def".to_string()),
                modified: None,
            },
            false,
        );
        assert!(
            !out.contains("<ETag>"),
            "a malformed stored ETag must be omitted from the listing: {out}"
        );

        // The well-formed baseline still renders quoted.
        let mut out = String::new();
        render_contents(
            &mut out,
            &ListedObject {
                key: "ok".to_string(),
                size: 3,
                etag: Some("0badc0de0badc0de".to_string()),
                modified: None,
            },
            false,
        );
        assert!(
            out.contains("<ETag>&quot;0badc0de0badc0de&quot;</ETag>"),
            "a well-formed stored ETag renders S3-quoted (then XML-escaped): {out}"
        );
    }

    /// A gateway whose stored object carries a caller-chosen `content_type` and `etag` — used
    /// to drive the real GET wire arm with a **malformed** stored value (ADR-0047: the seam
    /// commits the client's declared type verbatim, and the stored etag is decoded liberally
    /// per ADR-0045, so store corruption / out-of-band edits / #504/#506 can leave arbitrary
    /// strings on either field).
    struct StoredMetaGateway {
        content_type: Option<String>,
        etag: Option<String>,
        modified: Option<u64>,
    }

    impl StoredMetaGateway {
        /// The well-formed baseline — a valid stored etag, no content type, and an in-range
        /// modified time — that each test then perturbs on exactly the field it exercises.
        fn new() -> Self {
            Self {
                content_type: None,
                etag: Some("0badc0de0badc0de".to_string()),
                modified: Some(1_700_000_000_000),
            }
        }
    }

    impl ContainerGateway for StoredMetaGateway {}

    impl ObjectGateway for StoredMetaGateway {
        async fn put_object_streaming<S>(
            &self,
            _key: &str,
            _source: S,
            _expected: ContentHash,
            _content_type: Option<String>,
        ) -> wyrd_traits::Result<String>
        where
            S: futures_util::Stream<Item = wyrd_traits::Result<bytes::Bytes>>
                + Send
                + Unpin
                + 'static,
        {
            // The seam types the committed ETag as an opaque `String`; hand back whatever
            // the test configured, exactly as a foreign `ObjectGateway` implementation may.
            Ok(self.etag.clone().unwrap_or_default())
        }

        async fn get_object_streaming(
            self: Arc<Self>,
            _key: &str,
        ) -> wyrd_traits::Result<Option<ObjectRead>> {
            let body = bytes::Bytes::from_static(b"object-body");
            Ok(Some(ObjectRead {
                size: body.len() as u64,
                stream: Box::pin(futures_util::stream::once(async move { Ok(body) })),
                etag: self.etag.clone(),
                content_type: self.content_type.clone(),
                modified: self.modified,
            }))
        }

        async fn head_object(&self, _key: &str) -> wyrd_traits::Result<Option<ObjectMeta>> {
            Ok(Some(ObjectMeta {
                size: 11, // b"object-body".len()
                etag: self.etag.clone(),
                content_type: self.content_type.clone(),
                modified: Some(1_700_000_000_000),
            }))
        }

        async fn delete_object(&self, _key: &str) -> wyrd_traits::Result<bool> {
            Ok(false)
        }
    }

    /// Drive a signed `method` request with `body` at `/bucket/key` through the REAL router
    /// against `gateway`, returning the response. Shared by the malformed-metadata degradation
    /// tests so each exercises the production dispatch → wire arm → response builder, not a
    /// stand-in.
    async fn signed_through_router(
        gateway: Arc<StoredMetaGateway>,
        method: &str,
        body: &'static [u8],
    ) -> Response {
        use tower::ServiceExt;

        let creds = crate::sigv4::Credentials {
            access_key_id: "AKIA".to_string(),
            secret_access_key: "secret".to_string(),
        };
        let router = S3Gateway::new(gateway, S3Config::new(vec![creds.clone()])).router();

        let host = "example.com";
        let path = "/bucket/key";
        let amz_date = crate::sigv4::format_amz_date(SystemTime::now());
        let signed = crate::sigv4::sign(
            method,
            path,
            "",
            host,
            &amz_date,
            body,
            &creds,
            "us-east-1",
            "s3",
        );
        router
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(path)
                    .header("host", host)
                    .header("authorization", signed.authorization)
                    .header("x-amz-date", signed.amz_date)
                    .header("x-amz-content-sha256", signed.content_sha256)
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
            .expect("the router answers without panicking")
    }

    async fn signed_get_through_router(gateway: Arc<StoredMetaGateway>) -> Response {
        signed_through_router(gateway, "GET", b"").await
    }

    #[test]
    fn rfc850_two_digit_years_resolve_relative_to_the_clock() {
        // RFC 9110 §5.6.7 (a MUST): a two-digit RFC-850 year is interpreted in the CURRENT
        // century first, and moved back 100 years only when that lands more than 50 years in
        // the future. All expectations are independent epoch oracles (Python `calendar.timegm`);
        // `now` is injected, so the rule is exercised deterministically at a fixed 2026 clock.
        const NOW_2026: u64 = 1_767_225_600; // 2026-01-01T00:00:00Z
                                             // `-75` → 2075 (49 years ahead ≤ 50: stays in the current century). The previous fixed
                                             // pivot-at-70 read this as 1975, turning an If-Unmodified-Since carrying 2075 into a
                                             // spurious 412.
        assert_eq!(
            parse_rfc850_date("Sunday, 20-Jul-75 08:49:37 GMT", NOW_2026),
            Some(3_330_838_177),
        );
        // `-77` → 2077 would be 51 years ahead (> 50): moved back to 1977.
        assert_eq!(
            parse_rfc850_date("Wednesday, 20-Jul-77 08:49:37 GMT", NOW_2026),
            Some(238_236_577),
        );
        // The RFC's own example stays 1994 (2094 is far in the future), and `-69` resolves to
        // 2069 — the same answers the old fixed pivot gave, so existing wire vectors hold.
        assert_eq!(
            parse_rfc850_date("Sunday, 06-Nov-94 08:49:37 GMT", NOW_2026),
            Some(784_111_777),
        );
        assert_eq!(
            parse_rfc850_date("Wednesday, 06-Nov-69 08:49:37 GMT", NOW_2026),
            Some(3_150_953_377),
        );

        // The cutoff bites at FULL TIMESTAMP precision, not year precision: with the clock at
        // 2026-07-20T09:00, a candidate 2076-07-20T10:00 is 50 years and ONE HOUR ahead —
        // more than 50 years → 1976 — while 2076-07-20T08:00 (one hour short of the mark)
        // stays 2076. A year-granular comparison gets the first one wrong.
        const NOW_MID_2026: u64 = 1_784_538_000; // 2026-07-20T09:00:00Z
        assert_eq!(
            parse_rfc850_date("Monday, 20-Jul-76 10:00:00 GMT", NOW_MID_2026),
            Some(206_704_800), // 1976-07-20T10:00:00Z
        );
        assert_eq!(
            parse_rfc850_date("Monday, 20-Jul-76 08:00:00 GMT", NOW_MID_2026),
            Some(3_362_457_600), // 2076-07-20T08:00:00Z
        );
    }

    #[test]
    fn a_wildcard_validator_matches_object_existence_not_stored_etag_presence() {
        // RFC 9110 §13.1.1: `If-Match: *` is false only "if the origin server does not have a
        // current representation" — it tests EXISTENCE, not possession of a persisted
        // entity-tag. Every caller evaluates conditionals after resolving the object, so `*`
        // matches even a pre-ADR-0047 record whose stored ETag is `None`; gating on
        // `stored.is_some()` would 412 an `If-Match: *` overwrite guard and serve a full 200
        // to an `If-None-Match: *` cache probe on exactly those legacy records.
        assert!(
            etag_matches("*", None, true),
            "If-Match: * on a legacy record"
        );
        assert!(
            etag_matches("*", None, false),
            "If-None-Match: * on a legacy record"
        );
        assert!(etag_matches("*", Some("0badc0de"), true));
        assert!(etag_matches("*", Some("0badc0de"), false));
        // A SPECIFIC tag still never matches a record with no stored ETag.
        assert!(!etag_matches("\"0badc0de\"", None, true));
        assert!(!etag_matches("\"0badc0de\"", None, false));
    }

    /// The wildcard semantics end to end, on a legacy record with NO stored ETag: through the
    /// real signed router, `If-None-Match: *` must answer `304` (the object exists — the
    /// wildcard needs no persisted entity-tag), and `If-Match: *` must serve the `200`.
    #[tokio::test]
    async fn wildcard_conditionals_fire_on_etag_less_legacy_records() {
        let legacy = || {
            Arc::new(StoredMetaGateway {
                etag: None,
                ..StoredMetaGateway::new()
            })
        };
        let response = signed_get_with_header(legacy(), ("if-none-match", "*")).await;
        assert_eq!(
            response.status(),
            StatusCode::NOT_MODIFIED,
            "If-None-Match: * over an existing etag-less record revalidates (304)",
        );

        let response = signed_get_with_header(legacy(), ("if-match", "*")).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "If-Match: * over an existing etag-less record must NOT 412",
        );
    }

    /// [`signed_get_through_router`] plus one extra (unsigned) conditional header — the guard
    /// applies whether or not the client put the header in its SigV4 signed-header set.
    async fn signed_get_with_header(
        gateway: Arc<StoredMetaGateway>,
        (name, value): (&'static str, &'static str),
    ) -> Response {
        use tower::ServiceExt;

        let creds = crate::sigv4::Credentials {
            access_key_id: "AKIA".to_string(),
            secret_access_key: "secret".to_string(),
        };
        let router = S3Gateway::new(gateway, S3Config::new(vec![creds.clone()])).router();
        let host = "example.com";
        let path = "/bucket/key";
        let amz_date = crate::sigv4::format_amz_date(SystemTime::now());
        let signed = crate::sigv4::sign(
            "GET",
            path,
            "",
            host,
            &amz_date,
            b"",
            &creds,
            "us-east-1",
            "s3",
        );
        router
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri(path)
                    .header("host", host)
                    .header("authorization", signed.authorization)
                    .header("x-amz-date", signed.amz_date)
                    .header("x-amz-content-sha256", signed.content_sha256)
                    .header(name, value)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("the router answers without panicking")
    }

    /// A stored `content_type` that is NOT a valid HTTP header value must NOT panic a GET.
    ///
    /// Pre-hardening, the GET arm passed the stored string straight to
    /// `.header("content-type", <bad>)`, which records an invalid-`HeaderValue` error that
    /// only surfaces when the body is built — at `.expect("streaming response is always
    /// valid")` — so **every** read of such an object panics the handler (a 500 at best, a
    /// denied read for good). The seam commits the client's declared type verbatim
    /// (ADR-0047) and #504/#506 call it next with arbitrary strings, so this is reachable
    /// without any HTTP client (axum rejects a malformed *request* header, but a malformed
    /// *stored* value never passes through axum).
    ///
    /// Driven through the REAL signed router dispatch → GET arm → response builder, so it
    /// exercises the production `.expect(...)` path, not a stand-in. Post-hardening the GET
    /// degrades the content type to `application/octet-stream` and serves the body.
    #[tokio::test]
    async fn a_malformed_stored_content_type_degrades_the_get_instead_of_panicking() {
        let gateway = Arc::new(StoredMetaGateway {
            // CRLF is a valid Rust string but never a valid HTTP header value.
            content_type: Some("text/plain\r\ninjected: header".to_string()),
            ..StoredMetaGateway::new()
        });
        let response = signed_get_through_router(gateway).await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a malformed stored content type must still serve the object — not panic the GET",
        );
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .expect("a GET carries a content-type")
                .to_str()
                .expect("the served content type is ascii"),
            "application/octet-stream",
            "an un-renderable stored content type degrades to the S3 default",
        );
        // The ETag is still surfaced — hardening the content type does not drop other metadata.
        assert_eq!(
            response
                .headers()
                .get("etag")
                .expect("a GET carries the stored ETag")
                .to_str()
                .expect("ascii"),
            "\"0badc0de0badc0de\"",
        );
    }

    /// A stored `etag` that is NOT a valid HTTP header value must NOT panic a GET.
    ///
    /// Symmetric with the malformed-`content_type` case above and the residual gap it
    /// left: pre-hardening the GET arm passed the stored etag straight to
    /// `.header("etag", quote_etag(<bad>))`, which records an invalid-`HeaderValue` error
    /// that only surfaces when the body is built — at `.expect("streaming response is always
    /// valid")` — so **every** read of such an object panics the handler. The stored etag is
    /// committed by the write path and decoded **liberally** at the metadata boundary
    /// (ADR-0045), so store corruption or an out-of-band edit can leave a value carrying a
    /// non-header byte (e.g. CR/LF) — reachable without any HTTP client, since axum only
    /// screens malformed *request* headers, never a malformed *stored* value.
    ///
    /// Driven through the REAL signed router dispatch → GET arm → response builder, so it
    /// exercises the production `.expect(...)` path, not a stand-in. Post-hardening the GET
    /// **omits** the `ETag` header (degrade, never panic) and serves the body.
    #[tokio::test]
    async fn a_malformed_stored_etag_degrades_the_get_instead_of_panicking() {
        let gateway = Arc::new(StoredMetaGateway {
            // A CR/LF byte is a valid Rust string but never a valid HTTP header value; quoting
            // it does not make it one, so it would poison the response builder.
            etag: Some("0badc0de\r\ninjected: header".to_string()),
            ..StoredMetaGateway::new()
        });
        let response = signed_get_through_router(gateway).await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a malformed stored etag must still serve the object — not panic the GET",
        );
        // The ETag header is OMITTED (degraded), not rendered — the object is still readable.
        assert!(
            response.headers().get("etag").is_none(),
            "an un-renderable stored etag degrades to no ETag header, never a panic",
        );
        // The rest of the response is intact: the body still serves and the content type is
        // present — hardening the etag drops only the etag.
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .expect("a GET carries a content-type")
                .to_str()
                .expect("ascii"),
            "application/octet-stream",
            "the content type is unaffected — a well-formed (here: absent) type still renders",
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body serves without panicking");
        assert_eq!(
            &body[..],
            b"object-body",
            "the object body is served in full despite the malformed stored etag",
        );
    }

    /// A stored `modified` past year 9999 must degrade to NO `Last-Modified` header — never
    /// a malformed one.
    ///
    /// IMF-fixdate's year is exactly four digits, but metadata decoding accepts any `u64`
    /// (ADR-0045), so store corruption or an out-of-band edit can leave a timestamp whose
    /// year is 10000+. Unlike the malformed etag/content-type cases, such a value IS a
    /// valid HTTP header value — the failure is not a panic but a five-digit year on the
    /// wire, which is not a valid IMF-fixdate (RFC 7231 §7.1.1.1) and can misparse in
    /// caching clients. Post-hardening `http_date` declines the render and the GET omits
    /// the header, symmetric with the etag degradation above; everything else still serves.
    #[tokio::test]
    async fn an_unrenderable_stored_modified_omits_last_modified_instead_of_malforming_it() {
        // 253_402_300_800_000 ms is 10000-01-01T00:00:00Z — the first unrenderable instant.
        let gateway = Arc::new(StoredMetaGateway {
            modified: Some(253_402_300_800_000),
            ..StoredMetaGateway::new()
        });
        let response = signed_get_through_router(gateway).await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "an unrenderable stored modified time must still serve the object",
        );
        assert!(
            response.headers().get("last-modified").is_none(),
            "a year-10000+ stored modified time degrades to no Last-Modified header, \
             never a malformed IMF-fixdate",
        );
        // The rest of the metadata is intact — degrading the date drops only the date.
        assert_eq!(
            response
                .headers()
                .get("etag")
                .expect("a GET carries the stored ETag")
                .to_str()
                .expect("ascii"),
            "\"0badc0de0badc0de\"",
        );

        // And the boundary itself: the last renderable instant still renders (fixed 4-digit
        // year at its maximum), one millisecond later does not.
        assert_eq!(
            http_date(253_402_300_799_999).as_deref(),
            Some("Fri, 31 Dec 9999 23:59:59 GMT"),
        );
        assert_eq!(http_date(253_402_300_800_000), None);
        assert_eq!(http_date(u64::MAX), None);
    }

    /// A committed ETag that is NOT a valid HTTP header value must NOT panic a PUT.
    ///
    /// The seam types the committed ETag as an opaque `String`
    /// ([`ObjectGateway::put_object_streaming`]) — nothing in the contract requires it to be
    /// a hex digest, so a foreign gateway implementation behind this generic wire layer can
    /// hand back a value carrying a non-header byte (e.g. CR/LF). Pre-hardening,
    /// `put_object_response` passed it straight to `.header("etag", ...)`, poisoning the
    /// builder so the `.expect(...)` panicked the handler — every successful upload through
    /// such a gateway answered with a connection reset instead of its 200. Post-hardening
    /// the PUT omits the un-renderable `ETag` header and still answers 200, symmetric with
    /// the GET-side `etag_header` degradation above.
    #[tokio::test]
    async fn an_unrenderable_committed_etag_degrades_the_put_instead_of_panicking() {
        let gateway = Arc::new(StoredMetaGateway {
            etag: Some("0badc0de\r\ninjected: header".to_string()),
            ..StoredMetaGateway::new()
        });
        let response = signed_through_router(gateway, "PUT", b"object-body").await;

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "an un-renderable committed etag must still answer the PUT — not panic it",
        );
        assert!(
            response.headers().get("etag").is_none(),
            "an un-renderable committed etag degrades to no ETag header, never a panic",
        );

        // The same wire path with a well-formed committed etag still serves it — the guard
        // degrades only the un-renderable case, not the happy path.
        let response =
            signed_through_router(Arc::new(StoredMetaGateway::new()), "PUT", b"object-body").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("etag")
                .expect("a PUT answers with the committed ETag")
                .to_str()
                .expect("ascii"),
            "\"0badc0de0badc0de\"",
        );
    }

    // ------------------------------------------------------------------------------------------
    // Ranged-read seam correctness (issue #510 carry-forward items 3 & 4). These drive the REAL
    // router (`S3Gateway::router()` → `dispatch` → `serve_get`) against a hand-crafted
    // `ObjectGateway` double, so they exercise the production wiring — they reference the new
    // `ByteRange`/`RangeRead`/`RangeOutcome` symbols and therefore ship WITH the fix (they are not
    // the base red→green discriminator; `crates/server/tests/s3_range_conditional.rs` is).
    // ------------------------------------------------------------------------------------------

    /// Drive a signed request with arbitrary extra request headers (`range`, `if-*`) through the
    /// REAL router against `gateway`, returning the response — the generic sibling of
    /// `signed_through_router`, for the range-seam doubles below.
    async fn signed_ranged_request<G: ObjectGateway + ContainerGateway>(
        gateway: Arc<G>,
        method: &str,
        extra_headers: &[(&str, &str)],
        body: &'static [u8],
    ) -> Response {
        use tower::ServiceExt;

        let creds = crate::sigv4::Credentials {
            access_key_id: "AKIA".to_string(),
            secret_access_key: "secret".to_string(),
        };
        let router = S3Gateway::new(gateway, S3Config::new(vec![creds.clone()])).router();

        let host = "example.com";
        let path = "/bucket/key";
        let amz_date = crate::sigv4::format_amz_date(SystemTime::now());
        let signed = crate::sigv4::sign(
            method,
            path,
            "",
            host,
            &amz_date,
            body,
            &creds,
            "us-east-1",
            "s3",
        );
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("host", host)
            .header("authorization", signed.authorization)
            .header("x-amz-date", signed.amz_date)
            .header("x-amz-content-sha256", signed.content_sha256);
        for (name, value) in extra_headers {
            builder = builder.header(*name, *value);
        }
        router
            .oneshot(builder.body(Body::from(body)).expect("request"))
            .await
            .expect("the router answers without panicking")
    }

    /// A gateway that serves a fixed object body but does NOT override `get_object_range`, so a
    /// ranged GET through the router exercises the trait's **correctness-preserving default**
    /// (full-object read via `get_object_streaming` + wire-side slice, in `gateway-core`). The
    /// body is emitted in small source pieces so the default's slice crosses source-chunk
    /// boundaries.
    struct DefaultRangeSeamGateway {
        body: Vec<u8>,
    }

    impl ContainerGateway for DefaultRangeSeamGateway {}

    impl ObjectGateway for DefaultRangeSeamGateway {
        async fn put_object_streaming<S>(
            &self,
            _key: &str,
            _source: S,
            _expected: ContentHash,
            _content_type: Option<String>,
        ) -> wyrd_traits::Result<String>
        where
            S: futures_util::Stream<Item = wyrd_traits::Result<bytes::Bytes>>
                + Send
                + Unpin
                + 'static,
        {
            Ok(String::new())
        }

        async fn get_object_streaming(
            self: Arc<Self>,
            _key: &str,
        ) -> wyrd_traits::Result<Option<ObjectRead>> {
            let pieces: Vec<wyrd_traits::Result<bytes::Bytes>> = self
                .body
                .chunks(3)
                .map(|c| Ok(bytes::Bytes::copy_from_slice(c)))
                .collect();
            Ok(Some(ObjectRead {
                size: self.body.len() as u64,
                stream: Box::pin(futures_util::stream::iter(pieces)),
                etag: Some("seamdefault".to_string()),
                content_type: None,
                modified: None,
            }))
        }

        async fn head_object(&self, _key: &str) -> wyrd_traits::Result<Option<ObjectMeta>> {
            Ok(Some(ObjectMeta {
                size: self.body.len() as u64,
                etag: Some("seamdefault".to_string()),
                content_type: None,
                modified: None,
            }))
        }

        async fn delete_object(&self, _key: &str) -> wyrd_traits::Result<bool> {
            Ok(false)
        }
        // get_object_range: DELIBERATELY the trait DEFAULT (item 4).
    }

    /// **Item 4.** A gateway that does not override `get_object_range` must still answer a ranged
    /// GET of an EXISTING object with a correct `206` (and a `416` for an unsatisfiable range) —
    /// NOT the `Ok(None)` → `404 NoSuchKey` landmine the previous default was, which would 404 an
    /// object the wire layer just advertised `Accept-Ranges: bytes` for.
    #[tokio::test]
    async fn a_non_overriding_gateway_serves_ranges_via_the_correctness_preserving_default() {
        // 20-byte body: bytes 0..20.
        let body: Vec<u8> = (0u8..20).collect();
        let gateway = Arc::new(DefaultRangeSeamGateway { body: body.clone() });

        // A satisfiable range → 206 with the correct span, resolved by the DEFAULT (not a 404).
        let response =
            signed_ranged_request(Arc::clone(&gateway), "GET", &[("range", "bytes=8-15")], b"")
                .await;
        assert_eq!(
            response.status(),
            StatusCode::PARTIAL_CONTENT,
            "the default get_object_range must answer 206 for an existing object, never 404"
        );
        assert_eq!(
            response
                .headers()
                .get("content-range")
                .expect("a 206 carries Content-Range")
                .to_str()
                .expect("ascii"),
            "bytes 8-15/20",
        );
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .expect("a 206 carries Content-Length")
                .to_str()
                .expect("ascii"),
            "8",
        );
        let sliced = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(
            &sliced[..],
            &body[8..16],
            "the default must slice the CORRECT span out of the full object"
        );

        // An unsatisfiable range → 416 (the default resolves it against the object size too).
        let response =
            signed_ranged_request(Arc::clone(&gateway), "GET", &[("range", "bytes=999-")], b"")
                .await;
        assert_eq!(
            response.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "the default must answer 416 for an unsatisfiable range, not 404 or 200"
        );
    }

    /// A gateway whose `head_object` and `get_object_range` deliberately report DIFFERENT object
    /// versions — the deterministic stand-in for the race the atomic seam closes: a PUT landing
    /// between a metadata resolve and the body read. `head_object` reports the "stale" pre-write
    /// view (size 100, etag `stalehead`); `get_object_range` reports the "fresh" post-write view
    /// (size 40, etag `freshbody`) that the streamed bytes actually belong to.
    struct VersionSkewGateway;

    impl VersionSkewGateway {
        const FRESH_BODY: &'static [u8] = b"RANGEBYTES"; // 10 bytes, the served span
    }

    impl ContainerGateway for VersionSkewGateway {}

    impl ObjectGateway for VersionSkewGateway {
        async fn put_object_streaming<S>(
            &self,
            _key: &str,
            _source: S,
            _expected: ContentHash,
            _content_type: Option<String>,
        ) -> wyrd_traits::Result<String>
        where
            S: futures_util::Stream<Item = wyrd_traits::Result<bytes::Bytes>>
                + Send
                + Unpin
                + 'static,
        {
            Ok(String::new())
        }

        async fn get_object_streaming(
            self: Arc<Self>,
            _key: &str,
        ) -> wyrd_traits::Result<Option<ObjectRead>> {
            // The "fresh" full object — only reached by a non-ranged GET, unused by this test.
            Ok(Some(ObjectRead {
                size: 40,
                stream: Box::pin(futures_util::stream::once(async move {
                    Ok(bytes::Bytes::from_static(b"fresh"))
                })),
                etag: Some("freshbody".to_string()),
                content_type: None,
                modified: None,
            }))
        }

        async fn get_object_range(
            self: Arc<Self>,
            _key: &str,
            _range: ByteRange,
        ) -> wyrd_traits::Result<Option<RangeRead>> {
            // The FRESH view: the metadata and the bytes are one version (size 40, etag
            // `freshbody`). A racing overwrite is exactly what makes this differ from `head_object`
            // below; the wire layer must frame the 206 from THIS meta, not the stale head.
            let stream: ObjectStream = Box::pin(futures_util::stream::once(async move {
                Ok(bytes::Bytes::from_static(Self::FRESH_BODY))
            }));
            Ok(Some(RangeRead {
                meta: ObjectMeta {
                    size: 40,
                    etag: Some("freshbody".to_string()),
                    content_type: None,
                    modified: None,
                },
                outcome: RangeOutcome::Satisfiable {
                    offset: 8,
                    len: Self::FRESH_BODY.len() as u64,
                    stream,
                },
            }))
        }

        async fn head_object(&self, _key: &str) -> wyrd_traits::Result<Option<ObjectMeta>> {
            // The STALE view a separate metadata resolve would return under a race.
            Ok(Some(ObjectMeta {
                size: 100,
                etag: Some("stalehead".to_string()),
                content_type: None,
                modified: None,
            }))
        }

        async fn delete_object(&self, _key: &str) -> wyrd_traits::Result<bool> {
            Ok(false)
        }
    }

    /// **Item 3.** A ranged GET must frame its `206` entirely from `get_object_range`'s single
    /// resolve — the `Content-Range` total, `Content-Length`, `ETag`, and body all from the one
    /// snapshot the bytes came from — never mixing in a *separate* `head_object` resolve. With the
    /// two seams reporting different versions (the deterministic stand-in for a racing overwrite),
    /// the `206` must name the range seam's `freshbody`/size-40, not the stale head's
    /// `stalehead`/size-100: a version-mixed 206 (fresh bytes labelled with the stale size/etag)
    /// is precisely what poisons an ETag-keyed cache.
    #[tokio::test]
    async fn ranged_206_is_framed_from_the_range_seam_not_a_separate_head_resolve() {
        let response = signed_ranged_request(
            Arc::new(VersionSkewGateway),
            "GET",
            &[("range", "bytes=8-17")],
            b"",
        )
        .await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let last = 8 + VersionSkewGateway::FRESH_BODY.len() - 1;
        assert_eq!(
            response
                .headers()
                .get("content-range")
                .expect("206 Content-Range")
                .to_str()
                .expect("ascii"),
            format!("bytes 8-{last}/40"),
            "the Content-Range total must be the range seam's size (40), not the stale head's (100)"
        );
        assert_eq!(
            response
                .headers()
                .get("etag")
                .expect("206 ETag")
                .to_str()
                .expect("ascii"),
            "\"freshbody\"",
            "the 206 must carry the range seam's ETag, not the stale head's — no version mixing"
        );
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .expect("206 Content-Length")
                .to_str()
                .expect("ascii"),
            VersionSkewGateway::FRESH_BODY.len().to_string(),
        );
        let served = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(
            &served[..],
            VersionSkewGateway::FRESH_BODY,
            "the served bytes are the range seam's span"
        );
    }

    /// **Item 2 (carry-forward).** A conditional GET's preconditions must be judged against the
    /// SAME version its body is served from — the single `get_object_range`/`get_object_streaming`
    /// resolve — never a separate `head_object` snapshot. With the two seams reporting different
    /// versions (the deterministic stand-in for a racing overwrite between a metadata resolve and
    /// the body read), an `If-Match` naming the STALE head's ETag must FAIL with `412`: it does not
    /// match the version the served bytes belong to. Evaluating it against the stale head instead
    /// would let it pass and emit a self-coherent `206`/`200` of a version the client's precondition
    /// never authorised — the check-then-act window the atomic seam closes.
    #[tokio::test]
    async fn conditionals_are_judged_against_the_served_version_not_a_stale_head() {
        // Ranged path: If-Match on the STALE head's etag must 412 — the served version is
        // `freshbody`, which the stale `"stalehead"` never matches.
        let response = signed_ranged_request(
            Arc::new(VersionSkewGateway),
            "GET",
            &[("range", "bytes=8-17"), ("if-match", "\"stalehead\"")],
            b"",
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::PRECONDITION_FAILED,
            "a ranged If-Match against the stale head's etag must 412 — it judges the served \
             version (get_object_range's), not a separate head_object snapshot",
        );

        // Unranged path: the same window on the full-object resolve (`get_object_streaming`), which
        // also reports `freshbody` — a stale-head If-Match must 412 here too.
        let response = signed_ranged_request(
            Arc::new(VersionSkewGateway),
            "GET",
            &[("if-match", "\"stalehead\"")],
            b"",
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::PRECONDITION_FAILED,
            "an unranged If-Match must judge the served version (get_object_streaming's), not \
             the stale head_object snapshot",
        );

        // Positive control: If-Match on the SERVED version (`freshbody`) passes the fence → the
        // `206` is served — the fix rejects only the mismatched (stale) tag, not a genuine match.
        let response = signed_ranged_request(
            Arc::new(VersionSkewGateway),
            "GET",
            &[("range", "bytes=8-17"), ("if-match", "\"freshbody\"")],
            b"",
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::PARTIAL_CONTENT,
            "If-Match matching the served version passes → 206",
        );
    }
}
