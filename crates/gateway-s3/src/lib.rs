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
async fn handle<G>(State(state): State<AppState<G>>, req: Request) -> Response
where
    G: ObjectGateway,
{
    let request_id = state.request_ids.mint();
    let span = tracing::info_span!(
        "s3.request",
        request_id = %request_id,
        method = %req.method(),
        path = %req.uri().path(),
    );
    let mut response = dispatch(state, req, request_id).instrument(span).await;
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
            return error_response(
                request_id,
                StatusCode::FORBIDDEN,
                err.s3_code(),
                &err.to_string(),
            )
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
    Response::builder()
        .status(status)
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

/// Map a gateway error onto an S3-compatible HTTP response. A commit `Conflict`
/// (a concurrent writer won) is 409; a payload/hash mismatch is 400; a failed
/// `aws-chunked` chunk signature is 403 (fail-closed — the body was not signed by the
/// credential holder); malformed chunk framing is 400; anything else 500.
fn gateway_error_response(request_id: RequestId, err: &BoxError) -> Response {
    if let Some(streaming) = err.downcast_ref::<streaming::StreamingError>() {
        return match streaming {
            streaming::StreamingError::ChunkSignature => error_response(
                request_id,
                StatusCode::FORBIDDEN,
                "SignatureDoesNotMatch",
                "an aws-chunked chunk signature does not verify",
            ),
            streaming::StreamingError::Framing(what) => error_response(
                request_id,
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &format!("malformed aws-chunked streaming body: {what}"),
            ),
        };
    }
    match err.downcast_ref::<GatewayError>() {
        Some(GatewayError::Conflict) => error_response(
            request_id,
            StatusCode::CONFLICT,
            "OperationAborted",
            "a concurrent writer won the commit",
        ),
        Some(GatewayError::PayloadMismatch) => error_response(
            request_id,
            StatusCode::BAD_REQUEST,
            "XAmzContentSHA256Mismatch",
            "the delivered body does not match the signed x-amz-content-sha256",
        ),
        _ => error_response(
            request_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "the gateway could not complete the request",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
