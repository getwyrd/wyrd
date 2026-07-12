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
//! real-SDK streaming interop).
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
use wyrd_gateway_core::{ContentHash, GatewayError, ObjectGateway, ObjectRead};
use wyrd_traits::BoxError;

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
}

struct AppState<G> {
    gateway: Arc<G>,
    config: Arc<S3Config>,
    /// This process's request-id minter (#529). Shared, not per-request: the monotonic
    /// half of the id must be drawn from one counter.
    request_ids: Arc<RequestIds>,
}

// `Arc` makes the state cheap to clone regardless of the gateway type, so avoid a
// derive that would spuriously demand `G: Clone`.
impl<G> Clone for AppState<G> {
    fn clone(&self) -> Self {
        Self {
            gateway: Arc::clone(&self.gateway),
            config: Arc::clone(&self.config),
            request_ids: Arc::clone(&self.request_ids),
        }
    }
}

impl<G> S3Gateway<G>
where
    G: ObjectGateway,
{
    /// Compose the front door over a gateway and its config.
    pub fn new(gateway: Arc<G>, config: S3Config) -> Self {
        Self {
            gateway,
            config: Arc::new(config),
        }
    }

    /// The axum router: every method/path routes through the `handle` dispatcher, which
    /// verifies the SigV4 signature first and then dispatches by verb.
    pub fn router(self) -> Router {
        let state = AppState {
            gateway: self.gateway,
            config: self.config,
            request_ids: Arc::new(RequestIds::new()),
        };
        Router::new().fallback(handle::<G>).with_state(state)
    }

    /// Serve on an already-bound listener until it stops (loopback at Check; a deployed
    /// host wraps the listener in the public-TLS terminator, #367).
    pub async fn serve(self, listener: tokio::net::TcpListener) -> std::io::Result<()> {
        axum::serve(listener, self.router()).await
    }
}

/// The S3 subresource / multipart query keys this object-only floor does not implement.
/// A request carrying any of them is refused (501) rather than falling through to a plain
/// object PUT/GET/DELETE, which would mishandle it — destructively for `PUT ?partNumber`
/// (UploadPart) and `DELETE ?tagging`. Returns the first offending key found. Benign
/// params a normal SDK adds to ordinary object requests (e.g. `x-id=PutObject`) are not
/// listed, so they still pass; this is a denylist of unsupported operations, not a ban on
/// all query strings.
fn unsupported_subresource(query: &str) -> Option<&str> {
    const SUBRESOURCES: &[&str] = &[
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
    ];
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').map(|(k, _)| k).unwrap_or(p))
        .find(|k| SUBRESOURCES.contains(k))
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
}

/// Write ONE access row — the single place it is emitted, so the body-carrying and body-less
/// paths cannot drift in what they record.
///
/// `request_id` is a field of the EVENT, not merely inherited from `span`: a target-scoped
/// directive (`RUST_LOG=wyrd.gateway.s3.access=info` — the natural way to keep the access plane
/// and quieten the rest) enables this event without enabling the span, whose target is the module
/// path, so inheriting alone would lose the join key under exactly that directive.
fn record_access(
    span: &tracing::Span,
    request_id: RequestId,
    started: SystemTime,
    status: u16,
    bytes: u64,
    outcome: &'static str,
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
}

impl<S> AccessLogged<S> {
    /// Write the row, exactly once. `outcome` is the *observed* fate of the transfer, not the
    /// status line's optimism.
    fn record(&mut self, outcome: &'static str) {
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
                self.record("failed");
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // EOF is not the same as "all of it". A stream that ends short of the declared
                // `content-length` truncated the object on the wire — the client sees a failed
                // transfer, and a row calling it `complete` would hide exactly the fault this
                // logging exists to diagnose.
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
fn finish_response(
    response: Response,
    method: &Method,
    span: &tracing::Span,
    request_id: RequestId,
    started: SystemTime,
) -> Response {
    let status = response.status();
    // Hyper never polls the body of a `204`/`304`/`1xx` (HTTP forbids one), and it SUPPRESSES the
    // body of any response to a `HEAD` — in both cases it drops the body instead. Wrapping those
    // would send them straight to the `Drop` arm as `aborted`: every successful DELETE, and every
    // HEAD probe (an S3 client's most routine call), recorded as a client that hung up.
    let bodyless =
        matches!(status.as_u16(), 204 | 304) || status.is_informational() || method == Method::HEAD;
    if bodyless {
        record_access(span, request_id, started, status.as_u16(), 0, "complete");
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
        })
    })
}

async fn handle<G>(State(state): State<AppState<G>>, req: Request) -> Response
where
    G: ObjectGateway,
{
    let request_id = state.request_ids.mint();
    let started = SystemTime::now();
    let method = req.method().clone();
    let span = tracing::info_span!(
        "s3.request",
        request_id = %request_id,
        method = %req.method(),
        path = %req.uri().path(),
    );
    let response = dispatch(state, req, request_id)
        .instrument(span.clone())
        .await;

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
    let mut response = finish_response(response, &method, &span, request_id, started);

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
    G: ObjectGateway,
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
                        .put_object_streaming(&object_key, decoded, ContentHash::Unverified)
                        .await
                }
                // A single-shot **signed** body: stream it straight in; the running hash is
                // checked against the signed digest before the commit.
                sigv4::PayloadHash::Signed(hex) => {
                    state
                        .gateway
                        .put_object_streaming(&object_key, raw, ContentHash::Expected(hex))
                        .await
                }
                // A deliberately-unsigned body: stream it in with no post-stream hash check.
                sigv4::PayloadHash::Unsigned => {
                    state
                        .gateway
                        .put_object_streaming(&object_key, raw, ContentHash::Unverified)
                        .await
                }
            };
            match result {
                Ok(()) => empty_response(StatusCode::OK),
                Err(err) => gateway_error_response(request_id, &err),
            }
        }
        Method::GET => match Arc::clone(&state.gateway)
            .get_object_streaming(&object_key)
            .await
        {
            Ok(Some(ObjectRead { size, stream })) => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/octet-stream")
                // Declare the exact object length so a body truncated by a mid-stream fault
                // (e.g. a fragment reclaimed by a racing DELETE) is a detectable short read,
                // not a silent "complete" 200 (issue #364 carry-forward: GET fault framing).
                .header("content-length", size.to_string())
                .body(Body::from_stream(stream))
                .expect("streaming response is always valid"),
            Ok(None) => error_response(
                request_id,
                StatusCode::NOT_FOUND,
                "NoSuchKey",
                "the specified key does not exist",
            ),
            Err(err) => gateway_error_response(request_id, &err),
        },
        Method::DELETE => match state.gateway.delete_object(&object_key).await {
            // DELETE is idempotent: removing a present or an absent key both succeed.
            Ok(_) => empty_response(StatusCode::NO_CONTENT),
            Err(err) => gateway_error_response(request_id, &err),
        },
        _ => error_response(
            request_id,
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "only object PUT, GET, and DELETE are supported",
        ),
    }
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
            error = %err,
            cause_chain = %CauseChain(err.as_ref()),
            "the gateway refused the request",
        );
    }
    error_response(request_id, status, code, &message)
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
        impl ObjectGateway for NoGateway {
            async fn put_object_streaming<S>(
                &self,
                _key: &str,
                _source: S,
                _expected: ContentHash,
            ) -> wyrd_traits::Result<()>
            where
                S: futures_util::Stream<Item = wyrd_traits::Result<bytes::Bytes>>
                    + Send
                    + Unpin
                    + 'static,
            {
                Ok(())
            }

            async fn get_object_streaming(
                self: Arc<Self>,
                _key: &str,
            ) -> wyrd_traits::Result<Option<ObjectRead>> {
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

    impl ObjectGateway for NoGateway {
        async fn put_object_streaming<S>(
            &self,
            _key: &str,
            _source: S,
            _expected: ContentHash,
        ) -> wyrd_traits::Result<()>
        where
            S: futures_util::Stream<Item = wyrd_traits::Result<bytes::Bytes>>
                + Send
                + Unpin
                + 'static,
        {
            Ok(())
        }

        async fn get_object_streaming(
            self: Arc<Self>,
            _key: &str,
        ) -> wyrd_traits::Result<Option<ObjectRead>> {
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
}
