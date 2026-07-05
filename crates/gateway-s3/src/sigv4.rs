//! AWS Signature Version 4 (`AWS4-HMAC-SHA256`) **header-based** verification — the
//! gateway's fail-closed auth boundary (§7.5 "TLS; S3 SigV4", §14 threat model
//! `14-threat-model.md:86`). There is **no anonymous access**: a request with a
//! missing, malformed, expired, or mismatched signature is refused before any gateway
//! work runs — and, crucially, **before the request body is read** ([`verify`] needs no
//! body), so an unsigned request cannot force the gateway to allocate for a body it will
//! reject (issue #364 carry-forward item 6).
//!
//! Canonicalization is the real AWS algorithm, not a self-consistent shortcut
//! (carry-forward item 2): the canonical query string is **sorted and URI-encoded**
//! ([`canonical_query`]) and path/query encoding follows RFC 3986 ([`uri_encode`]). The
//! whole chain — canonical request → string-to-sign → signing key → HMAC — is pinned to
//! AWS's **published** worked example (the `sigv4_aws_docs_example` test), so a real
//! `aws-sdk`/`boto3` request with query parameters verifies rather than 403-ing on a
//! canonicalization divergence.
//!
//! Scope (a deliberate M4 floor; the fuller surface is the pre-declared "SigV4 scope"
//! NEEDS-HUMAN in the brief): the header-based `Authorization: AWS4-HMAC-SHA256 …`
//! variant with a static credential set, an `x-amz-date` freshness window (replay
//! bound), and a signed-payload integrity check. **Presigned-query** signing is out of
//! scope (brief §Scope).

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;

use crate::crypto;

/// The `x-amz-date` freshness window (S3's default 15 minutes): a request whose signed
/// timestamp is further than this from the gateway clock is refused as a replay/skew.
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(15 * 60);

/// A single static S3 credential the gateway will accept.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// The access key id (the public half, echoed in the `Credential=` scope).
    pub access_key_id: String,
    /// The secret access key (the signing secret — never sent on the wire).
    pub secret_access_key: String,
}

/// The payload-hash discipline a verified request declared, returned by [`verify`] so
/// the caller can check the streamed body against the **signed** hash after the body has
/// streamed to the store (never buffering it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadHash {
    /// `x-amz-content-sha256` was a hex digest the signature covers; the streamed body
    /// must hash to it (else the write is rejected before commit).
    Signed(String),
    /// `UNSIGNED-PAYLOAD`: the body is deliberately outside the signature (the client
    /// relies on the transport for body integrity); nothing to check post-stream.
    Unsigned,
    /// An `aws-chunked` streaming-signature upload (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
    /// and its `…-TRAILER` / `STREAMING-UNSIGNED-PAYLOAD-TRAILER` variants) — the form a
    /// **stock modern SDK** (boto3 / aws-sdk) sends a PUT in. The seed signature (the
    /// `Authorization` signature just verified) is the head of a per-chunk signature
    /// **chain**: the body is `aws-chunked`-framed (`<hex-len>;chunk-signature=…\r\n<data>\r\n`)
    /// and each chunk carries a signature derived from the previous one. The carried
    /// [`StreamingContext`] gives the decoder ([`super::streaming`]) everything it needs to
    /// strip the framing and **verify each chunk** as it streams — so a stock SDK upload
    /// round-trips byte-identical (issue #364 carry-forward: real-SDK interop / the
    /// `STREAMING-…-PAYLOAD` 501) while the body stays authenticated and never buffered.
    Streaming(StreamingContext),
}

/// The material the `aws-chunked` streaming decoder needs to verify a chunk-signed body:
/// the verified seed signature (head of the chunk chain), the SigV4 signing key, and the
/// timestamp + scope that go into each chunk's string-to-sign. Built by [`verify`] only
/// **after** the seed signature has been checked, so a request must first authenticate
/// before any chunk material is produced (fail-closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingContext {
    /// The verified seed signature — the `previous-signature` of the first chunk.
    pub seed_signature: String,
    /// The SigV4 signing key (`HMAC` ladder over the secret), reused per chunk.
    pub signing_key: [u8; 32],
    /// The request `x-amz-date` (`YYYYMMDDThhmmssZ`) — line 2 of the chunk string-to-sign.
    pub date_time: String,
    /// The credential scope `date/region/service/aws4_request` — line 3 of the string-to-sign.
    pub scope: String,
    /// Whether the chunks are **signed** (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`,
    /// each frame carries a `chunk-signature=`) or unsigned
    /// (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`, framing only). A signed stream is verified
    /// chunk-by-chunk; an unsigned one is de-framed but not chunk-authenticated (its
    /// integrity rides the seed signature + transport, the same posture as `UNSIGNED-PAYLOAD`).
    pub signed: bool,
}

/// Why a request's signature was refused. Every variant maps to HTTP 403; the
/// [`s3_code`](AuthError::s3_code) is the S3-compatible `<Error><Code>` string.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    /// No usable `Authorization` / `X-Amz-Date` header — an anonymous request.
    Missing,
    /// The `Authorization` header (or its credential scope) is not well-formed.
    Malformed(String),
    /// The credential's access key id is not one the gateway accepts.
    UnknownKey,
    /// The recomputed signature does not match the one presented.
    SignatureMismatch,
    /// The signed `x-amz-date` is outside the freshness window (replay / clock skew).
    Skewed,
}

impl AuthError {
    /// The S3-compatible error code for this refusal.
    pub fn s3_code(&self) -> &'static str {
        match self {
            AuthError::Missing => "AccessDenied",
            AuthError::Malformed(_) => "AuthorizationHeaderMalformed",
            AuthError::UnknownKey => "InvalidAccessKeyId",
            AuthError::SignatureMismatch => "SignatureDoesNotMatch",
            AuthError::Skewed => "RequestTimeTooSkewed",
        }
    }
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::Missing => write!(f, "request is not signed (no anonymous access)"),
            AuthError::Malformed(what) => write!(f, "malformed Authorization: {what}"),
            AuthError::UnknownKey => write!(f, "unknown access key id"),
            AuthError::SignatureMismatch => write!(f, "signature does not match"),
            AuthError::Skewed => write!(f, "x-amz-date is outside the allowed window"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Percent-encode `input` per RFC 3986 for SigV4 canonicalization: the unreserved set
/// `A-Za-z0-9-_.~` passes through, everything else becomes `%XX` (upper-case hex).
/// `/` is left literal when `encode_slash` is false (the canonical-URI path rule) and
/// encoded when true (the canonical-query key/value rule).
pub fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b'/' if !encode_slash => out.push('/'),
            _ => {
                out.push('%');
                out.push(
                    char::from_digit((b >> 4) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit((b & 0x0f) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

/// Percent-**decode** `s` to raw bytes (only `%XX`; `+` is NOT treated as space — SigV4
/// operates on the raw query, not form-encoding). Malformed `%` escapes are passed
/// through literally rather than erroring.
fn percent_decode(s: &str) -> Vec<u8> {
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
    out
}

/// The SigV4 "Trimall" normalization of a header value: strip leading/trailing whitespace
/// and collapse every internal run of whitespace to a single space — the rule AWS clients
/// apply when they canonicalize a header value, so a value signed with doubled internal
/// spaces still verifies here (issue #364 carry-forward). Whitespace **inside** a
/// double-quoted section is preserved verbatim, per the SigV4 spec.
fn trim_all(value: &str) -> String {
    let trimmed = value.trim();
    let mut out = String::with_capacity(trimmed.len());
    let mut in_quotes = false;
    let mut prev_space = false;
    for c in trimmed.chars() {
        if c == '"' {
            in_quotes = !in_quotes;
            out.push(c);
            prev_space = false;
        } else if !in_quotes && c.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    out
}

/// The SigV4 **canonical query string**: each parameter's name and value are
/// percent-decoded then re-URI-encoded (normalizing any client encoding), the params are
/// **sorted** by encoded name (then value), and rejoined as `name=value` pairs — a
/// value-less parameter becomes `name=`. This is the step that makes a real SDK request
/// (which sorts + encodes its query) verify instead of 403-ing (carry-forward item 2).
pub fn canonical_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut params: Vec<(String, String)> = raw
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            let key = uri_encode(&String::from_utf8_lossy(&percent_decode(k)), true);
            let val = uri_encode(&String::from_utf8_lossy(&percent_decode(v)), true);
            (key, val)
        })
        .collect();
    params.sort();
    params
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// The parsed fields of an `AWS4-HMAC-SHA256` `Authorization` header.
struct Authorization {
    access_key: String,
    date: String,
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
}

fn parse_authorization(value: &str) -> Result<Authorization, AuthError> {
    let rest = value
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(|| AuthError::Malformed("unsupported signing algorithm".into()))?;

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }

    let credential = credential.ok_or_else(|| AuthError::Malformed("missing Credential".into()))?;
    let signed_headers =
        signed_headers.ok_or_else(|| AuthError::Malformed("missing SignedHeaders".into()))?;
    let signature = signature.ok_or_else(|| AuthError::Malformed("missing Signature".into()))?;

    let mut scope = credential.split('/');
    let access_key = scope
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AuthError::Malformed("empty access key".into()))?;
    let date = scope
        .next()
        .ok_or_else(|| AuthError::Malformed("credential scope: missing date".into()))?;
    let region = scope
        .next()
        .ok_or_else(|| AuthError::Malformed("credential scope: missing region".into()))?;
    let service = scope
        .next()
        .ok_or_else(|| AuthError::Malformed("credential scope: missing service".into()))?;
    let terminator = scope
        .next()
        .ok_or_else(|| AuthError::Malformed("credential scope: missing terminator".into()))?;
    if terminator != "aws4_request" {
        return Err(AuthError::Malformed("credential scope terminator".into()));
    }

    Ok(Authorization {
        access_key: access_key.to_string(),
        date: date.to_string(),
        region: region.to_string(),
        service: service.to_string(),
        signed_headers: signed_headers
            .split(';')
            .map(|s| s.trim().to_ascii_lowercase())
            .collect(),
        signature: signature.to_string(),
    })
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// The SigV4 canonical request string (AWS SigV4 §"Create a canonical request").
/// `headers` must already be sorted ascending by name and their values trimmed;
/// `query` must already be canonicalized ([`canonical_query`]).
fn canonical_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &[(String, String)],
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    let mut canonical_headers = String::new();
    for (name, value) in headers {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(value);
        canonical_headers.push('\n');
    }
    format!("{method}\n{uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}")
}

/// The SigV4 signing key for a secret + scope — exposed so the streaming-chunk decoder and
/// test harnesses derive the exact key the chunk-signature chain uses. Thin public wrapper
/// over [`signing_key`].
pub fn signing_key_for(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    signing_key(secret, date, region, service)
}

/// The SigV4 signing key: `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region), service),
/// "aws4_request")` (AWS SigV4 §"Calculate the signature").
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = crypto::hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = crypto::hmac_sha256(&k_date, region.as_bytes());
    let k_service = crypto::hmac_sha256(&k_region, service.as_bytes());
    crypto::hmac_sha256(&k_service, b"aws4_request")
}

/// The hex signature for a canonical request (string-to-sign → signing-key → HMAC).
fn derive_signature(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
    amz_date: &str,
    canonical_request: &str,
) -> String {
    let hashed = crypto::hex(&crypto::sha256(canonical_request.as_bytes()));
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashed}");
    let key = signing_key(secret, date, region, service);
    crypto::hex(&crypto::hmac_sha256(&key, string_to_sign.as_bytes()))
}

/// Verify a request's `AWS4-HMAC-SHA256` header signature against `credentials`, and
/// bound its freshness against `now`. **Reads no body**: it authenticates from the
/// headers + the *claimed* `x-amz-content-sha256`, returning that claim as a
/// [`PayloadHash`] so the caller checks the streamed body against the **signed** hash
/// after the body has streamed (never buffering it).
///
/// Fail-closed: returns `Err` (→ HTTP 403) on any missing/malformed/mismatched/expired
/// input. `uri` is the request path exactly as received (already percent-encoded, the
/// S3 single-encoding canonical URI); `query` the raw query string.
#[allow(clippy::too_many_arguments)]
pub fn verify(
    method: &str,
    uri: &str,
    query: &str,
    headers: &HeaderMap,
    credentials: &[Credentials],
    region: &str,
    service: &str,
    now: SystemTime,
) -> Result<PayloadHash, AuthError> {
    let authz = parse_authorization(header(headers, "authorization").ok_or(AuthError::Missing)?)?;

    let cred = credentials
        .iter()
        .find(|c| c.access_key_id == authz.access_key)
        .ok_or(AuthError::UnknownKey)?;

    if authz.region != region || authz.service != service {
        return Err(AuthError::Malformed(
            "credential scope region/service".into(),
        ));
    }

    let amz_date = header(headers, "x-amz-date").ok_or(AuthError::Missing)?;
    if amz_date.len() < 8 || amz_date[..8] != authz.date {
        return Err(AuthError::Malformed(
            "x-amz-date does not match scope date".into(),
        ));
    }
    // Replay/skew bound: the signed timestamp must be within the window of `now`.
    let signed_at = parse_amz_date(amz_date).ok_or(AuthError::Skewed)?;
    if skew(signed_at, now) > MAX_CLOCK_SKEW {
        return Err(AuthError::Skewed);
    }

    // The signed-headers list exactly as the client declared it (already lower-cased in
    // `parse_authorization`). Fail closed on a downgrade: host and x-amz-date must be signed.
    let declared = &authz.signed_headers;
    if !declared.iter().any(|h| h == "host") || !declared.iter().any(|h| h == "x-amz-date") {
        return Err(AuthError::Malformed(
            "SignedHeaders must include host and x-amz-date".into(),
        ));
    }

    // The client signed the *claimed* content hash header; the body is checked against
    // it later (post-stream) by the caller. S3 requires x-amz-content-sha256.
    let claimed = header(headers, "x-amz-content-sha256")
        .ok_or_else(|| AuthError::Malformed("missing x-amz-content-sha256".into()))?;

    // Reconstruct the canonical header block: one line per signed header, each value passed
    // through the SigV4 "Trimall" rule (leading/trailing whitespace stripped, internal
    // whitespace runs collapsed to a single space — so a client that signed doubled internal
    // spaces still verifies), then sorted ascending by name (a canonical-request requirement).
    let mut canonical_headers = Vec::with_capacity(declared.len());
    for name in declared {
        let value = header(headers, name)
            .ok_or_else(|| AuthError::Malformed(format!("signed header `{name}` absent")))?;
        canonical_headers.push((name.clone(), trim_all(value)));
    }
    canonical_headers.sort_by(|a, b| a.0.cmp(&b.0));
    // The SignedHeaders *string* in the string-to-sign is the client's own list verbatim — a
    // client whose SignedHeaders is not lexically sorted signed *that* string, so honour it
    // rather than re-sorting and spuriously refusing (issue #364 carry-forward).
    let signed_headers_str = declared.join(";");

    let canonical = canonical_request(
        method,
        uri,
        &canonical_query(query),
        &canonical_headers,
        &signed_headers_str,
        claimed,
    );
    let expected = derive_signature(
        &cred.secret_access_key,
        &authz.date,
        region,
        service,
        amz_date,
        &canonical,
    );

    if !crypto::constant_time_eq(expected.as_bytes(), authz.signature.as_bytes()) {
        return Err(AuthError::SignatureMismatch);
    }

    // Signature verified. Classify the *claimed* content-hash sentinel against a **closed**
    // set (issue #364 carry-forward, iter-6 item 3 — "no half-accept"): the earlier open
    // `starts_with("STREAMING-")` accepted *any* streaming-looking sentinel, including
    // framings the `aws-chunked` decoder ([`super::streaming`]) cannot de-frame — a
    // half-accept that would misread a body it claimed to understand. Now every branch is
    // a form the pipeline actually handles; anything else is refused cleanly here. An
    // `aws-chunked` streaming upload carries the seed signature (head of the per-chunk chain)
    // and scope the decoder verifies each chunk with, so a stock SDK PUT round-trips instead
    // of 501-ing. Building the context only *after* the seed signature matches keeps the
    // boundary fail-closed.
    let payload = if claimed == "UNSIGNED-PAYLOAD" {
        PayloadHash::Unsigned
    } else if is_hex_sha256(claimed) {
        PayloadHash::Signed(claimed.to_string())
    } else if let Some(signed) = streaming_variant(claimed) {
        PayloadHash::Streaming(StreamingContext {
            seed_signature: expected,
            signing_key: signing_key(&cred.secret_access_key, &authz.date, region, service),
            date_time: amz_date.to_string(),
            scope: format!("{}/{region}/{service}/aws4_request", authz.date),
            signed,
        })
    } else {
        // Not UNSIGNED-PAYLOAD, not a hex digest, not a streaming sentinel the decoder
        // supports — refuse rather than half-accept a framing we cannot verify.
        return Err(AuthError::Malformed(format!(
            "unsupported x-amz-content-sha256 `{claimed}`"
        )));
    };
    Ok(payload)
}

/// The `aws-chunked` streaming sentinels the decoder ([`super::streaming`]) can actually
/// de-frame, each mapped to whether its **data chunks are per-chunk signed**. A **closed**
/// set: `Some(true)` — signed data chunks, each frame carries a chained `chunk-signature=`
/// the decoder verifies fail-closed; `Some(false)` — framing only (integrity rides the seed
/// signature + transport, as `UNSIGNED-PAYLOAD`); `None` — an unknown streaming sentinel
/// [`verify`] must refuse rather than half-accept (issue #364 carry-forward, iter-6 item 3).
///
/// The `-TRAILER` variants append trailer headers (and, for the signed variant, a trailer
/// signature) *after* the terminating zero-length chunk. The `aws-chunked` decoder
/// ([`super::streaming::Decoder::next_chunk`]) requires an immediate CRLF after that zero
/// chunk and cannot consume those trailer bytes, so admitting a `-TRAILER` sentinel is a
/// half-accept: a real checksum-trailer upload would authenticate and then fail mid-body as
/// malformed (issue #364 carry-forward, iter-6 item 3 — "no half-accept"). So they return
/// `None` here and are refused up front, before any body is read. The only fully-consumable
/// framing is the signed, no-trailer `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`. Trailer
/// consumption/verification stays deferred with the rest of the streaming-checksum surface.
fn streaming_variant(sentinel: &str) -> Option<bool> {
    match sentinel {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" => Some(true),
        _ => None,
    }
}

/// Whether `s` is a 64-character hex string — the shape of a literal SHA-256 payload hash.
/// Used to tell a real signed-payload digest from a sentinel, so a bogus non-hex claim is
/// refused by [`verify`] rather than silently treated as a (never-matching) signed hash.
fn is_hex_sha256(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The header values an S3 client sets to sign a request: the counterpart to
/// [`verify`]. Signs `host`, `x-amz-content-sha256`, and `x-amz-date` (the S3 floor set).
/// Exposed so a client — the `wyrd` CLI, a test harness, an operator's smoke check — can
/// drive the real wire path; correctness is anchored by the shared canonicalization and
/// the AWS published-example known-answer test rather than by this helper trusting itself.
#[derive(Debug, Clone)]
pub struct SignedHeaders {
    /// The `Authorization` header value.
    pub authorization: String,
    /// The `x-amz-date` header value (echoed back for convenience).
    pub amz_date: String,
    /// The `x-amz-content-sha256` header value (hex SHA-256 of the body).
    pub content_sha256: String,
}

/// Sign a request, returning the headers a client must send. `uri` is the request-target
/// path exactly as it goes on the wire (the S3 single-encoding canonical URI); `query`
/// the raw query string; `amz_date` an ISO-8601 basic timestamp (`YYYYMMDDThhmmssZ`);
/// `host` the exact `Host` header value.
#[allow(clippy::too_many_arguments)]
pub fn sign(
    method: &str,
    uri: &str,
    query: &str,
    host: &str,
    amz_date: &str,
    body: &[u8],
    credentials: &Credentials,
    region: &str,
    service: &str,
) -> SignedHeaders {
    let payload_hash = crypto::hex(&crypto::sha256(body));
    sign_with_payload_hash(
        method,
        uri,
        query,
        host,
        amz_date,
        &payload_hash,
        credentials,
        region,
        service,
    )
}

/// As [`sign`], but with an **explicit** `x-amz-content-sha256` value rather than the hash
/// of an in-hand body. This is what an `aws-chunked` streaming upload signs its **seed**
/// request with — the sentinel `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` — because the body is
/// not available whole at signing time (it streams). Exposed so a client / test harness can
/// drive the real streaming wire path; the seed it returns is the head of the per-chunk
/// signature chain (see [`super::streaming`]).
#[allow(clippy::too_many_arguments)]
pub fn sign_with_payload_hash(
    method: &str,
    uri: &str,
    query: &str,
    host: &str,
    amz_date: &str,
    payload_hash: &str,
    credentials: &Credentials,
    region: &str,
    service: &str,
) -> SignedHeaders {
    let date = &amz_date[..8];
    // Sorted ascending by header name (host < x-amz-content-sha256 < x-amz-date).
    let headers = [
        ("host".to_string(), host.trim().to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
        ("x-amz-date".to_string(), amz_date.to_string()),
    ];
    let signed_headers_str = "host;x-amz-content-sha256;x-amz-date";
    let canonical = canonical_request(
        method,
        uri,
        &canonical_query(query),
        &headers,
        signed_headers_str,
        payload_hash,
    );
    let signature = derive_signature(
        &credentials.secret_access_key,
        date,
        region,
        service,
        amz_date,
        &canonical,
    );
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers_str}, Signature={signature}",
        credentials.access_key_id
    );
    SignedHeaders {
        authorization,
        amz_date: amz_date.to_string(),
        content_sha256: payload_hash.to_string(),
    }
}

/// Format `time` as an `x-amz-date` basic-ISO-8601 UTC timestamp (`YYYYMMDDThhmmssZ`).
/// Exposed so a client (CLI / test / smoke check) stamps a *fresh* request that passes
/// the freshness window.
pub fn format_amz_date(time: SystemTime) -> String {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

/// Parse an `x-amz-date` (`YYYYMMDDThhmmssZ`) to seconds since the Unix epoch, or `None`
/// if it is not that exact shape.
fn parse_amz_date(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() != 16 || b[8] != b'T' || b[15] != b'Z' {
        return None;
    }
    let num = |range: std::ops::Range<usize>| s.get(range)?.parse::<i64>().ok();
    let year = num(0..4)?;
    let month = num(4..6)?;
    let day = num(6..8)?;
    let hour = num(9..11)?;
    let min = num(11..13)?;
    let sec = num(13..15)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days = days_from_civil(year, month as u32, day as u32);
    let total = days * 86_400 + hour * 3600 + min * 60 + sec;
    u64::try_from(total).ok()
}

/// Absolute difference (as a `Duration`) between a Unix-seconds instant and `now`.
fn skew(signed_secs: u64, now: SystemTime) -> Duration {
    let now_secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Duration::from_secs(now_secs.abs_diff(signed_secs))
}

/// Days from 1970-01-01 to `y-m-d` (Howard Hinnant's civil-calendar algorithm).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`]: `(year, month, day)` for a day count since epoch.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS SigV4 test suite, `get-vanilla`: a fixed request signs to a **published**
    /// signature. Pins the whole chain to AWS's reference answer.
    /// <https://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html>
    #[test]
    fn sigv4_get_vanilla_known_answer() {
        let empty_payload = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = [
            ("host".to_string(), "example.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let canonical =
            canonical_request("GET", "/", "", &headers, "host;x-amz-date", empty_payload);
        let signature = derive_signature(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "service",
            "20150830T123600Z",
            &canonical,
        );
        assert_eq!(
            signature,
            "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    /// AWS docs worked example (GET with a **query string**), the canonical independent
    /// oracle for canonicalization + sorting. The query is fed **out of order**
    /// (`Version=…&Action=…`); [`canonical_query`] must sort it to
    /// `Action=ListUsers&Version=2010-05-08` for the signature to match AWS's published
    /// value `5d672d79…`. This is what proves the verifier is AWS-correct — not merely
    /// self-consistent with `sign` — for real SDK requests that carry query parameters.
    /// <https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html>
    #[test]
    fn sigv4_aws_docs_example_sorts_query() {
        let sorted = canonical_query("Version=2010-05-08&Action=ListUsers");
        assert_eq!(sorted, "Action=ListUsers&Version=2010-05-08");

        let empty_payload = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = [
            (
                "content-type".to_string(),
                "application/x-www-form-urlencoded; charset=utf-8".to_string(),
            ),
            ("host".to_string(), "iam.amazonaws.com".to_string()),
            ("x-amz-date".to_string(), "20150830T123600Z".to_string()),
        ];
        let canonical = canonical_request(
            "GET",
            "/",
            &sorted,
            &headers,
            "content-type;host;x-amz-date",
            empty_payload,
        );
        let signature = derive_signature(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
            "20150830T123600Z",
            &canonical,
        );
        assert_eq!(
            signature,
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        );
    }

    #[test]
    fn trim_all_folds_internal_whitespace_but_keeps_quoted() {
        // Leading/trailing stripped; internal runs collapse to one space.
        assert_eq!(trim_all("  a   b\tc  "), "a b c");
        assert_eq!(trim_all("plain"), "plain");
        // Whitespace inside a quoted section is preserved verbatim (SigV4 rule).
        assert_eq!(trim_all(r#"a "x   y" b"#), r#"a "x   y" b"#);
    }

    #[test]
    fn uri_encode_follows_rfc3986() {
        assert_eq!(uri_encode("azAZ09-_.~", false), "azAZ09-_.~");
        assert_eq!(uri_encode("a b", false), "a%20b");
        assert_eq!(uri_encode("a/b", false), "a/b");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("k=v&x", true), "k%3Dv%26x");
    }

    #[test]
    fn canonical_query_encodes_and_sorts() {
        // Values needing encoding, fed out of order: normalized + sorted.
        assert_eq!(
            canonical_query("b=hello world&a=x/y"),
            "a=x%2Fy&b=hello%20world"
        );
        // A value-less parameter becomes `name=`.
        assert_eq!(canonical_query("prefix"), "prefix=");
        assert_eq!(canonical_query(""), "");
    }

    #[test]
    fn amz_date_round_trips_through_unix_seconds() {
        // 2015-08-30T12:36:00Z is 1440938160 seconds since the epoch.
        assert_eq!(parse_amz_date("20150830T123600Z"), Some(1_440_938_160));
        let t = UNIX_EPOCH + Duration::from_secs(1_440_938_160);
        assert_eq!(format_amz_date(t), "20150830T123600Z");
        assert_eq!(parse_amz_date("not-a-date"), None);
    }

    #[test]
    fn verify_rejects_a_stale_signature() {
        let creds = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let amz_date = "20150830T123600Z";
        let host = "example.amazonaws.com";
        let signed = sign(
            "GET",
            "/",
            "",
            host,
            amz_date,
            b"",
            &creds,
            "us-east-1",
            "s3",
        );
        let mut headers = HeaderMap::new();
        headers.insert("host", host.parse().unwrap());
        headers.insert("authorization", signed.authorization.parse().unwrap());
        headers.insert("x-amz-date", signed.amz_date.parse().unwrap());
        headers.insert(
            "x-amz-content-sha256",
            signed.content_sha256.parse().unwrap(),
        );

        // Fresh (clock == signing time): accepted.
        let fresh = UNIX_EPOCH + Duration::from_secs(1_440_938_160);
        assert!(verify(
            "GET",
            "/",
            "",
            &headers,
            std::slice::from_ref(&creds),
            "us-east-1",
            "s3",
            fresh,
        )
        .is_ok());

        // An hour later: outside the 15-minute window → refused as skew/replay.
        let stale = fresh + Duration::from_secs(3600);
        assert_eq!(
            verify("GET", "/", "", &headers, &[creds], "us-east-1", "s3", stale,),
            Err(AuthError::Skewed)
        );
    }

    #[test]
    fn parse_authorization_rejects_wrong_scheme() {
        assert!(matches!(
            parse_authorization("Basic abc"),
            Err(AuthError::Malformed(_))
        ));
    }

    /// An `aws-chunked` streaming upload authenticates on its seed signature (the canonical
    /// request uses the `STREAMING-…-PAYLOAD` literal as the payload hash) and must be
    /// classified as a **signed** [`PayloadHash::Streaming`] carrying the seed signature and
    /// scope the chunk decoder chains from, so the handler can decode + verify the chunked
    /// body a stock SDK sends rather than 501-ing (issue #364 carry-forward, real-SDK
    /// break 2 / streaming interop).
    #[test]
    fn verify_classifies_aws_chunked_streaming_payload() {
        let creds = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let streaming = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
        let amz_date = "20150830T123600Z";
        let host = "example.amazonaws.com";
        // Sign a canonical request whose payload hash is the streaming sentinel (the seed
        // signature AWS's SDK computes for a chunked upload).
        let headers_sig = [
            ("host".to_string(), host.to_string()),
            ("x-amz-content-sha256".to_string(), streaming.to_string()),
            ("x-amz-date".to_string(), amz_date.to_string()),
        ];
        let canonical = canonical_request(
            "PUT",
            "/bucket/key",
            "",
            &headers_sig,
            "host;x-amz-content-sha256;x-amz-date",
            streaming,
        );
        let signature = derive_signature(
            &creds.secret_access_key,
            "20150830",
            "us-east-1",
            "s3",
            amz_date,
            &canonical,
        );
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/20150830/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}",
            creds.access_key_id
        );
        let mut headers = HeaderMap::new();
        headers.insert("host", host.parse().unwrap());
        headers.insert("authorization", authorization.parse().unwrap());
        headers.insert("x-amz-date", amz_date.parse().unwrap());
        headers.insert("x-amz-content-sha256", streaming.parse().unwrap());

        let fresh = UNIX_EPOCH + Duration::from_secs(1_440_938_160);
        let payload = verify(
            "PUT",
            "/bucket/key",
            "",
            &headers,
            std::slice::from_ref(&creds),
            "us-east-1",
            "s3",
            fresh,
        )
        .expect("a well-formed streaming seed authenticates");
        let PayloadHash::Streaming(ctx) = payload else {
            panic!("streaming sentinel must classify as PayloadHash::Streaming");
        };
        // Signed variant, and the seed signature carried forward is the verified
        // Authorization signature (the head of the per-chunk chain).
        assert!(ctx.signed, "…-PAYLOAD (not -UNSIGNED-) chunks are signed");
        assert_eq!(ctx.seed_signature, signature);
        assert_eq!(ctx.scope, "20150830/us-east-1/s3/aws4_request");
        assert_eq!(ctx.date_time, amz_date);
        // The signing key matches the SigV4 ladder for these credentials/scope.
        assert_eq!(
            ctx.signing_key,
            signing_key(&creds.secret_access_key, "20150830", "us-east-1", "s3")
        );
    }

    /// Sign a request over an arbitrary `x-amz-content-sha256` claim and run it through
    /// [`verify`] with a fresh clock, returning the classification (or the refusal).
    fn verify_with_claim(claim: &str) -> Result<PayloadHash, AuthError> {
        let creds = Credentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let amz_date = "20150830T123600Z";
        let host = "example.amazonaws.com";
        let signed = sign_with_payload_hash(
            "PUT",
            "/bucket/key",
            "",
            host,
            amz_date,
            claim,
            &creds,
            "us-east-1",
            "s3",
        );
        let mut headers = HeaderMap::new();
        headers.insert("host", host.parse().unwrap());
        headers.insert("authorization", signed.authorization.parse().unwrap());
        headers.insert("x-amz-date", signed.amz_date.parse().unwrap());
        headers.insert("x-amz-content-sha256", claim.parse().unwrap());
        let fresh = UNIX_EPOCH + Duration::from_secs(1_440_938_160);
        verify(
            "PUT",
            "/bucket/key",
            "",
            &headers,
            std::slice::from_ref(&creds),
            "us-east-1",
            "s3",
            fresh,
        )
    }

    /// Issue #364 carry-forward, iter-6 item 3 ("no half-accept"): the classification is a
    /// CLOSED set. A well-signed request whose `x-amz-content-sha256` is a streaming-looking
    /// sentinel the decoder does not support — or a bogus non-hex claim — is refused
    /// **cleanly** (`Malformed`), not half-accepted as a framing the pipeline cannot verify.
    /// The three real `aws-chunked` sentinels and a literal hex digest still classify.
    #[test]
    fn verify_rejects_unsupported_content_sha256_sentinels() {
        // Unknown STREAMING-* framings: refused cleanly (were half-accepted before).
        assert!(matches!(
            verify_with_claim("STREAMING-AWS4-HMAC-SHA256-FUTURE"),
            Err(AuthError::Malformed(_))
        ));
        assert!(matches!(
            verify_with_claim("STREAMING-SOMETHING-ELSE"),
            Err(AuthError::Malformed(_))
        ));
        // A non-hex, non-sentinel claim is not silently treated as a signed hash.
        assert!(matches!(
            verify_with_claim("not-a-real-hash"),
            Err(AuthError::Malformed(_))
        ));

        // The supported set still classifies correctly.
        assert!(matches!(
            verify_with_claim("UNSIGNED-PAYLOAD"),
            Ok(PayloadHash::Unsigned)
        ));
        assert!(matches!(
            verify_with_claim("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
            Ok(PayloadHash::Streaming(StreamingContext {
                signed: true,
                ..
            }))
        ));
        // The `-TRAILER` variants are refused up front: the decoder cannot consume the
        // trailer bytes after the terminating zero chunk, so admitting them would be the
        // iter-6 half-accept (authenticate, then fail mid-body). Rejected cleanly here.
        assert!(matches!(
            verify_with_claim("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"),
            Err(AuthError::Malformed(_))
        ));
        assert!(matches!(
            verify_with_claim("STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            Err(AuthError::Malformed(_))
        ));
        // A literal 64-char hex digest is a signed single-shot payload.
        let hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert!(matches!(
            verify_with_claim(hex),
            Ok(PayloadHash::Signed(h)) if h == hex
        ));
    }
}
