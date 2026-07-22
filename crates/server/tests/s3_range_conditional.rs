//! Range and conditional requests on the S3 wire surface (RFC 9110 §13/§14, issue #510),
//! exercised over a real loopback listener — the same in-process redb + fs-tempdir stack
//! `s3_object_metadata.rs` drives. A stored multi-chunk object must answer:
//!
//! * a `GET` with `Range: bytes=a-b` → **206 Partial Content**, `Content-Range: bytes a-b/size`,
//!   `Content-Length: b-a+1`, and a body byte-identical to that slice — plus the suffix
//!   (`bytes=-N`), open (`bytes=a-`), and clamped (`bytes=a-huge`) forms;
//! * an unsatisfiable range → **416** with `Content-Range: bytes */size`;
//! * an unranged `GET` → `Accept-Ranges: bytes`;
//! * `If-None-Match` with the object's ETag → **304** on GET *and* HEAD;
//! * `If-Match` with a non-matching ETag → **412**;
//! * `If-Modified-Since` / `If-Unmodified-Since` → **304 / 412** (S3 second-resolution semantics),
//!   including a **pre-1970** `If-Unmodified-Since` (clamped to epoch 0, so it fires 412 rather
//!   than being silently ignored) and the two **obsolete HTTP-date formats** (RFC-850 / asctime,
//!   which must be honoured, not fail open — RFC 9110 §5.6.7);
//! * a HEAD carrying `Range` → **206** (satisfiable, span `Content-Length` + `Content-Range`, no
//!   body) / **416** (unsatisfiable), now that HEAD advertises `Accept-Ranges: bytes`;
//! * a multi-range (`bytes=a-b,c-d`), a `+`-signed spec (`bytes=+8-+15`), and an interior-space
//!   spec → the full **200** (out of scope / malformed — real S3 ignores what it cannot honour).
//!
//! # The anti-wire-side-discard oracle
//! The load-bearing assertion (adversarial finding): a narrow ranged GET of a many-chunk object
//! (`with_chunk_size(8)`) must fetch **only the covering chunks**. Without it, an implementation
//! that streams the WHOLE object and discards the out-of-range bytes wire-side passes every other
//! assertion byte-identically. This wraps the real [`FsChunkStore`] in a counting `ChunkStore`
//! that records every `get_fragment` chunk id (the `request_capacity_planes.rs:665` `GateStore`
//! pattern), and asserts a `bytes=8-15` GET of a 64-byte / 8-chunk object touches exactly ONE
//! chunk — not all eight.
//!
//! RED on the base: the GET arm ignores the `Range` header (always a full 200) and evaluates no
//! conditionals, so every 206/304/412/416 assertion fails by assertion — and the counting store
//! sees all eight chunks fetched for the narrow range. The test drives the wire ONLY (no new
//! production symbol imported), so the red leg fails on the assertions, not a compile error. The
//! seam-level correctness of the ranged read (the atomic conditional+ranged seam, item 3, and the
//! correctness-preserving trait default, item 4) is bound by unit tests in `wyrd-gateway-s3`,
//! which reference the new symbols and so ship with the fix.

// wall-clock exempt (test crate): SigV4 request dates / lease stamps against a
// live in-process server use real wall time; nothing here mixes clock sources
// within one asserted lifecycle (#619).
// File scope (not per-site) is deliberate here: a test crate never
// ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::EcScheme;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::{ChunkId, ChunkStore, FragmentId, Health, PlacementChunkStore, Result};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// The chunk size the test gateway uses so a modest object spans many chunks over the streaming
/// read path (matching the `s3_object_metadata.rs` harness). Combined with `EcScheme::None` (one
/// fragment per chunk) it makes the counting store's `get_fragment` call count equal to the
/// number of chunks read.
const CHUNK_SIZE: usize = 8;

type Backend = Gateway<RedbMetadataStore, CountingChunkStore, MemCoordination>;

/// Records the chunk id of every `get_fragment` call so a test can assert a ranged read fetched
/// only the covering chunks. Cheaply cloneable (shared handle) so the test keeps a reference
/// after the store is moved into the gateway.
#[derive(Clone, Default)]
struct FragmentCounter {
    chunks: Arc<Mutex<Vec<ChunkId>>>,
}

impl FragmentCounter {
    fn reset(&self) {
        self.chunks.lock().expect("counter poisoned").clear();
    }

    /// The number of DISTINCT chunks touched since the last [`reset`](Self::reset) — the oracle
    /// for "only the covering chunks were fetched", robust to the EC scheme's fan-out (all a
    /// chunk's fragments share its chunk id).
    fn distinct_chunks(&self) -> usize {
        let mut v = self.chunks.lock().expect("counter poisoned").clone();
        v.sort_unstable();
        v.dedup();
        v.len()
    }
}

/// A `ChunkStore` that counts `get_fragment` calls by chunk id, delegating every operation to a
/// wrapped [`FsChunkStore`]. `PlacementChunkStore`'s default `get_fragment_at` delegates to
/// `get_fragment`, so the read path's per-fragment fetch is counted here (the
/// `request_capacity_planes.rs:665` `GateStore` pattern).
struct CountingChunkStore {
    inner: FsChunkStore,
    counter: FragmentCounter,
}

#[async_trait]
impl ChunkStore for CountingChunkStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        self.counter
            .chunks
            .lock()
            .expect("counter poisoned")
            .push(id.chunk);
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        self.inner.health().await
    }
}

impl PlacementChunkStore for CountingChunkStore {}

/// A gateway over the counting store with a small chunk size and `EcScheme::None` (one fragment
/// per chunk), so a modest object spans many chunks and each chunk read is exactly one counted
/// `get_fragment`.
fn build_gateway(dir: &std::path::Path) -> (Arc<Backend>, FragmentCounter) {
    let counter = FragmentCounter::default();
    let store = CountingChunkStore {
        inner: FsChunkStore::open(dir).expect("fs store"),
        counter: counter.clone(),
    };
    let gateway = Arc::new(
        Gateway::new(
            RedbMetadataStore::in_memory().expect("redb"),
            store,
            MemCoordination::new(),
        )
        .with_chunk_size(CHUNK_SIZE)
        .with_durability(EcScheme::None),
    );
    (gateway, counter)
}

/// Start the S3 gateway on an ephemeral loopback port. Returns the counting handle and the
/// `TempDir` (kept alive by the caller so the chunk store outlives the test).
async fn start_gateway() -> (SocketAddr, tempfile::TempDir, FragmentCounter) {
    let dir = tempfile::tempdir().expect("temp dir");
    let (gateway, counter) = build_gateway(dir.path());
    let config = S3Config::new(vec![Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(gateway, config);
    tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    (addr, dir, counter)
}

/// The SigV4 headers for a request. Extra headers (`range`, `if-*`) ride UNSIGNED — S3 SigV4
/// verifies only the declared `SignedHeaders`, so the gateway reads them off the request head
/// exactly as a stock client sends them (the `content-type` precedent in `s3_object_metadata.rs`).
fn signed_headers(method: &str, path: &str, host: &str, body: &[u8]) -> Vec<(String, String)> {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let amz_date = format_amz_date(SystemTime::now());
    let signed = sign(
        method, path, "", host, &amz_date, body, &creds, REGION, "s3",
    );
    vec![
        ("authorization".to_string(), signed.authorization),
        ("x-amz-date".to_string(), signed.amz_date),
        ("x-amz-content-sha256".to_string(), signed.content_sha256),
    ]
}

/// Send one HTTP/1.1 request over a fresh connection, returning `(status, head, body)` — the raw
/// head block so the caller can assert on response headers (`Content-Range`, `Accept-Ranges`, …).
async fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> (u16, String, Vec<u8>) {
    let host = addr.to_string();
    let mut request = format!("{method} {path} HTTP/1.1\r\n");
    request.push_str(&format!("host: {host}\r\n"));
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!("content-length: {}\r\n", body.len()));
    request.push_str("connection: close\r\n\r\n");

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write head");
    stream.write_all(body).await.expect("write body");
    stream.flush().await.expect("flush");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let status_line = head.lines().next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    (status, head, raw[split + 4..].to_vec())
}

/// Pull a header value out of a raw HTTP/1.1 head block, case-insensitively.
fn header_value(head: &str, name: &str) -> Option<String> {
    head.lines()
        .find(|line| {
            line.to_ascii_lowercase()
                .starts_with(&format!("{}:", name.to_ascii_lowercase()))
        })
        .map(|line| line.split_once(':').expect("header").1.trim().to_string())
}

/// PUT a 64-byte object (bytes `0..64`) under `path`; with `CHUNK_SIZE == 8` it spans 8 chunks.
/// Returns the bytes and the ETag the server assigned (read back off a GET) for the conditional
/// oracles.
async fn put_object(addr: SocketAddr, path: &str) -> (Vec<u8>, String) {
    let host = addr.to_string();
    let object: Vec<u8> = (0u8..64).collect();
    let (status, _, _) = send(
        addr,
        "PUT",
        path,
        &signed_headers("PUT", path, &host, &object),
        &object,
    )
    .await;
    assert_eq!(status, 200, "signed PUT must be accepted");

    let (status, head, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200, "signed GET must be accepted");
    assert_eq!(
        body, object,
        "the round-tripped object must be byte-identical"
    );
    let etag = header_value(&head, "etag").expect("GET must carry an ETag (ADR-0047)");
    (object, etag)
}

/// A signed GET carrying an unsigned `Range` header.
async fn ranged_get(addr: SocketAddr, path: &str, range: &str) -> (u16, String, Vec<u8>) {
    let host = addr.to_string();
    let mut headers = signed_headers("GET", path, &host, b"");
    headers.push(("range".to_string(), range.to_string()));
    send(addr, "GET", path, &headers, b"").await
}

#[tokio::test]
async fn ranged_get_returns_206_with_content_range_and_sliced_body() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/ranged-object";
    let (object, _etag) = put_object(addr, path).await;
    let size = object.len();

    // Every form the success criterion names: (Range header, expected inclusive [first,last]).
    let cases: &[(&str, u64, u64)] = &[
        ("bytes=8-15", 8, 15),    // closed a-b, wholly within one chunk
        ("bytes=6-17", 6, 17),    // closed a-b, spanning three chunks (trims first & last)
        ("bytes=40-", 40, 63),    // open: a to end
        ("bytes=-10", 54, 63),    // suffix: final N bytes
        ("bytes=60-999", 60, 63), // end past the last byte clamps to the object
    ];

    for &(range, first, last) in cases {
        let (status, head, body) = ranged_get(addr, path, range).await;
        assert_eq!(
            status, 206,
            "{range}: a satisfiable range must answer 206 Partial Content"
        );
        assert_eq!(
            header_value(&head, "content-range").as_deref(),
            Some(format!("bytes {first}-{last}/{size}").as_str()),
            "{range}: Content-Range must name the resolved span and the total size"
        );
        let span_len = (last - first + 1).to_string();
        assert_eq!(
            header_value(&head, "content-length").as_deref(),
            Some(span_len.as_str()),
            "{range}: Content-Length must be the SPAN length (b-a+1), not the object size"
        );
        assert_eq!(
            body,
            object[first as usize..=last as usize],
            "{range}: the body must be byte-identical to that slice of the object"
        );
        assert_eq!(
            header_value(&head, "accept-ranges").as_deref(),
            Some("bytes"),
            "{range}: a 206 still advertises Accept-Ranges"
        );
    }
}

#[tokio::test]
async fn unsatisfiable_range_returns_416_with_content_range_star() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/unsatisfiable-object";
    let (object, _etag) = put_object(addr, path).await;
    let size = object.len();

    // Each is a *grammatically valid* but unsatisfiable range → 416 (never the ignore-and-200
    // path): a start at/after the object end, and — the carry-forward case — a zero-length suffix
    // `bytes=-0`, which real S3 answers with `InvalidRange` (RFC 9110 §14.1.1 / §15.5.17), NOT a
    // malformed value that would fall through to a full 200.
    for range in ["bytes=64-70", "bytes=100-200", "bytes=999-", "bytes=-0"] {
        let (status, head, _body) = ranged_get(addr, path, range).await;
        assert_eq!(
            status, 416,
            "{range}: a valid-but-unsatisfiable range must answer 416, not a full 200"
        );
        assert_eq!(
            header_value(&head, "content-range").as_deref(),
            Some(format!("bytes */{size}").as_str()),
            "{range}: a 416 must carry Content-Range: bytes */<size>"
        );
    }
}

#[tokio::test]
async fn out_of_scope_and_malformed_ranges_answer_full_200_with_accept_ranges() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/full-object";
    let (object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    // Unranged GET: a full 200 that now advertises range support.
    let (status, head, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, object);
    assert_eq!(
        header_value(&head, "accept-ranges").as_deref(),
        Some("bytes"),
        "an unranged GET must advertise Accept-Ranges: bytes"
    );

    // Out-of-scope / malformed `Range` forms are IGNORED — S3 serves the full 200 for anything it
    // cannot honour (brief open-question resolution: multi-range and syntactically malformed
    // values both answer 200, exactly as real S3 does). A `416` or a `206` here would be a
    // conformance regression. `bytes=+8-+15` and `bytes=8 -15` are the carry-forward item-2 cases:
    // `u64::from_str` tolerates a leading `+` and the previous attempt trimmed interior whitespace,
    // so both were wrongly honoured as a 206 — they must fall on the malformed→200 side.
    for range in [
        "bytes=0-3,8-11", // multi-range set
        "bytes=abc",      // non-numeric — malformed
        "bytes=10",       // no `-` — malformed
        "items=0-3",      // non-`bytes` unit
        "bytes=-",        // empty on both sides — malformed
        "bytes=+8-+15",   // `+`-signed positions — malformed (item 2)
        "bytes=8 -15",    // interior whitespace — malformed (item 2)
        "bytes=-+5",      // signed suffix — malformed (item 2)
    ] {
        let (status, head, body) = ranged_get(addr, path, range).await;
        assert_eq!(
            status, 200,
            "{range}: an out-of-scope/malformed Range is ignored and answers the full 200"
        );
        assert_eq!(
            body, object,
            "{range}: the ignored range serves the whole object"
        );
        assert!(
            header_value(&head, "content-range").is_none(),
            "{range}: an ignored range must NOT carry a Content-Range (it is a plain 200)"
        );
    }
}

/// The range-unit token is case-insensitive (RFC 9110 §14.1): `Range: Bytes=8-15` /
/// `Range: BYTES=8-15` must serve the requested slice as a `206`, exactly as the lowercase `bytes=`
/// form does. A case-sensitive prefix match silently degraded a case-variant unit to the full `200`
/// — defeating a client that varies the token's casing (PR #611 review).
#[tokio::test]
async fn range_unit_is_matched_case_insensitively() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/case-range-object";
    let (object, _etag) = put_object(addr, path).await;
    let size = object.len();

    for range in ["Bytes=8-15", "BYTES=8-15", "bYtEs=8-15"] {
        let (status, head, body) = ranged_get(addr, path, range).await;
        assert_eq!(
            status, 206,
            "{range}: a case-variant range unit must still answer 206 Partial Content"
        );
        assert_eq!(
            header_value(&head, "content-range").as_deref(),
            Some(format!("bytes 8-15/{size}").as_str()),
            "{range}: Content-Range names the resolved span in the canonical lowercase unit"
        );
        assert_eq!(
            body,
            object[8..=15],
            "{range}: the body is byte-identical to the requested slice"
        );
    }
}

#[tokio::test]
async fn if_none_match_with_the_objects_etag_returns_304_on_get_and_head() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/conditional-object";
    let (_object, etag) = put_object(addr, path).await;
    let host = addr.to_string();

    for method in ["GET", "HEAD"] {
        let mut headers = signed_headers(method, path, &host, b"");
        headers.push(("if-none-match".to_string(), etag.clone()));
        let (status, head, body) = send(addr, method, path, &headers, b"").await;
        assert_eq!(
            status, 304,
            "{method} If-None-Match with the object's own ETag must answer 304 Not Modified"
        );
        assert!(body.is_empty(), "{method} a 304 carries no body");
        // A 304 MUST carry the cache validators (`ETag`, `Last-Modified`) — a client's
        // revalidation loop reuses them on the next conditional GET (RFC 9110 §15.4.5). Dropping
        // them regresses every cache without changing the status, so assert them explicitly.
        assert_eq!(
            header_value(&head, "etag").as_deref(),
            Some(etag.as_str()),
            "{method} a 304 must echo the object's ETag validator"
        );
        assert!(
            header_value(&head, "last-modified").is_some(),
            "{method} a 304 must carry the Last-Modified validator"
        );
    }
}

#[tokio::test]
async fn if_match_with_a_non_matching_etag_returns_412() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/if-match-object";
    let (_object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    let mut headers = signed_headers("GET", path, &host, b"");
    headers.push(("if-match".to_string(), "\"0000000000000000\"".to_string()));
    let (status, _head, _body) = send(addr, "GET", path, &headers, b"").await;
    assert_eq!(
        status, 412,
        "If-Match with an ETag that does not match must answer 412 Precondition Failed"
    );
}

/// `If-Match` uses the STRONG comparison function (RFC 9110 §13.1.1): a weak entity-tag never
/// matches, so `If-Match: W/"<etag>"` — the weak form of the object's *own* ETag — must still 412,
/// not silently be accepted as a match. A positive control (the strong ETag → 200) guards the fix
/// against over-rejecting a genuine match. Weak comparators are otherwise out of *support*; this
/// only refuses to weak-match on the one precondition the RFC forbids it on.
#[tokio::test]
async fn if_match_uses_strong_comparison_weak_tag_rejected() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/if-match-strong-object";
    let (object, etag) = put_object(addr, path).await;
    let host = addr.to_string();

    // Positive control: the object's own STRONG ETag matches → the object is served (200).
    let mut ok = signed_headers("GET", path, &host, b"");
    ok.push(("if-match".to_string(), etag.clone()));
    let (status, _head, body) = send(addr, "GET", path, &ok, b"").await;
    assert_eq!(
        status, 200,
        "If-Match with the matching strong ETag must serve the object (200)"
    );
    assert_eq!(body, object, "the served body is the whole object");

    // The WEAK form of that same ETag must 412 under strong comparison.
    let mut weak = signed_headers("GET", path, &host, b"");
    weak.push(("if-match".to_string(), format!("W/{etag}")));
    let (status, _head, _body) = send(addr, "GET", path, &weak, b"").await;
    assert_eq!(
        status, 412,
        "If-Match with a weak entity-tag (W/) must answer 412 — strong comparison forbids the match"
    );
}

/// A syntactically well-formed but IMPOSSIBLE calendar date (`30 Feb`) in a conditional must be
/// IGNORED (RFC 9110 §13.1.4), answering the full 200 — not misparsed into a neighbouring day
/// that fires the precondition.
#[tokio::test]
async fn invalid_conditional_date_is_ignored_not_misparsed() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/invalid-date-object";
    let (object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    // `30 Feb 2026` — a valid IMF-fixdate shape, an impossible date (2026 is not a leap year, and
    // February never has 30 days). If-Unmodified-Since must ignore it → 200, never 412.
    let mut ius = signed_headers("GET", path, &host, b"");
    ius.push((
        "if-unmodified-since".to_string(),
        "Mon, 30 Feb 2026 00:00:00 GMT".to_string(),
    ));
    let (status, _head, body) = send(addr, "GET", path, &ius, b"").await;
    assert_eq!(
        status, 200,
        "an impossible calendar date (30 Feb) must be ignored → full 200, not a misparsed 412"
    );
    assert_eq!(
        body, object,
        "the ignored conditional serves the whole object"
    );
}

/// A conditional whose HTTP-date carries an UNRECOGNIZED weekday token is malformed and must be
/// IGNORED (RFC 9110 §13.1.4) — answering the full 200 — not parsed as a valid past instant that
/// fires the precondition. The parsers previously discarded the weekday bytes, so `Xxx, 06 Nov 1994
/// …` (and its RFC-850 / asctime forms) wrongly resolved to a real past date; a past
/// `If-Unmodified-Since` on a just-PUT object then 412'd (PR #611 review). Each value below shares
/// its date with a valid-weekday sibling elsewhere in this suite that DOES fire 412, so a 200 here
/// isolates the weekday token as the sole reason it is ignored.
#[tokio::test]
async fn invalid_weekday_token_conditional_is_ignored() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/invalid-weekday-object";
    let (object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    for bad in [
        "Xxx, 06 Nov 1994 08:49:37 GMT", // IMF-fixdate — unknown 3-letter weekday
        "Xxxday, 06-Nov-94 08:49:37 GMT", // RFC-850 — unknown full weekday name
        "Xxx Nov  6 08:49:37 1994",      // asctime — unknown 3-letter weekday
    ] {
        let mut ius = signed_headers("GET", path, &host, b"");
        ius.push(("if-unmodified-since".to_string(), bad.to_string()));
        let (status, _head, body) = send(addr, "GET", path, &ius, b"").await;
        assert_eq!(
            status, 200,
            "{bad}: an unrecognized weekday token makes the date malformed → ignored → full 200, not 412"
        );
        assert_eq!(
            body, object,
            "{bad}: the ignored conditional serves the whole object"
        );
    }
}

/// HTTP-date numeric fields are digit-only, but Rust's integer parser accepts a leading `+` (and
/// `-` for the year), so a signed field such as the `+8` in `… 1994 +8:49:37 GMT` would misparse as
/// `08` and fire a spurious precondition instead of the malformed value being IGNORED (RFC 9110
/// §13.1.4). Every numeric slice of all three date formats is now digit-validated, mirroring the
/// `Range` parser's `+8-` rejection (PR #611 review). Each value below is otherwise a PAST instant
/// whose well-formed sibling elsewhere in this suite fires 412, so a 200 here isolates the sign as
/// the sole reason it is ignored.
#[tokio::test]
async fn signed_http_date_fields_are_ignored_not_misparsed() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/signed-date-object";
    let (object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    for bad in [
        "Sun, 06 Nov 1994 +8:49:37 GMT",  // IMF-fixdate — signed hour
        "Sun, +6 Nov 1994 08:49:37 GMT",  // IMF-fixdate — signed day
        "Sunday, 06-Nov-94 +8:49:37 GMT", // RFC-850 — signed hour
        "Sun Nov  6 08:49:+7 1994",       // asctime — signed second
    ] {
        let mut ius = signed_headers("GET", path, &host, b"");
        ius.push(("if-unmodified-since".to_string(), bad.to_string()));
        let (status, _head, body) = send(addr, "GET", path, &ius, b"").await;
        assert_eq!(
            status, 200,
            "{bad}: a signed HTTP-date field makes the value malformed → ignored → full 200, not 412"
        );
        assert_eq!(
            body, object,
            "{bad}: the ignored conditional serves the whole object"
        );
    }
}

/// A **pre-1970** `If-Unmodified-Since` must FIRE 412, not be silently ignored (carry-forward
/// item 1). The object was PUT well after 1970, so it *was* modified after any pre-epoch instant →
/// the precondition fails → 412. The previous attempt failed the parse for a pre-epoch date
/// (`u64::try_from` on a negative epoch), which made the caller ignore the conditional and serve
/// 200 — and for If-Unmodified-Since "ignore" INVERTS the answer. The fix clamps a pre-1970
/// IMF-fixdate to epoch 0 in `parse_http_date` so the comparison fires.
#[tokio::test]
async fn pre_epoch_if_unmodified_since_fires_412_not_ignored() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/pre-epoch-object";
    let (_object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    // A well-formed IMF-fixdate strictly before 1970-01-01 (the epoch). The stored `modified` is
    // "now" (2020s), which is after it, so If-Unmodified-Since must fail with 412.
    let mut ius = signed_headers("GET", path, &host, b"");
    ius.push((
        "if-unmodified-since".to_string(),
        "Fri, 01 Jan 1960 00:00:00 GMT".to_string(),
    ));
    let (status, _head, _body) = send(addr, "GET", path, &ius, b"").await;
    assert_eq!(
        status, 412,
        "a pre-1970 If-Unmodified-Since must fire 412 (object modified after it), not be ignored"
    );
}

#[tokio::test]
async fn if_modified_since_and_if_unmodified_since_answer_304_and_412() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/date-conditional-object";
    let (_object, etag) = put_object(addr, path).await;
    let host = addr.to_string();

    // Read the object's own Last-Modified back — using it as `If-Modified-Since` means the object
    // was NOT modified after that instant (equal, at second resolution) → 304.
    let (_status, head, _body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    let last_modified = header_value(&head, "last-modified").expect("GET carries Last-Modified");

    let mut ims = signed_headers("GET", path, &host, b"");
    ims.push(("if-modified-since".to_string(), last_modified.clone()));
    let (status, head, body) = send(addr, "GET", path, &ims, b"").await;
    assert_eq!(
        status, 304,
        "If-Modified-Since at the object's own Last-Modified means not-modified → 304"
    );
    assert!(body.is_empty(), "a 304 carries no body");
    // The date-driven 304 must also carry the cache validators (RFC 9110 §15.4.5).
    assert_eq!(
        header_value(&head, "etag").as_deref(),
        Some(etag.as_str()),
        "a date-conditional 304 must echo the object's ETag validator"
    );
    assert_eq!(
        header_value(&head, "last-modified").as_deref(),
        Some(last_modified.as_str()),
        "a date-conditional 304 must carry the Last-Modified validator"
    );

    // A date long in the past as `If-Unmodified-Since`: the object was modified AFTER it → 412.
    let mut ius = signed_headers("GET", path, &host, b"");
    ius.push((
        "if-unmodified-since".to_string(),
        "Sun, 06 Nov 1994 08:49:37 GMT".to_string(),
    ));
    let (status, _head, _body) = send(addr, "GET", path, &ius, b"").await;
    assert_eq!(
        status, 412,
        "If-Unmodified-Since with a past date means the object WAS modified since → 412"
    );
}

/// The two OBSOLETE HTTP-date formats — RFC-850 and asctime — must be parsed on the conditional
/// headers, not fail OPEN (carry-forward item 3, RFC 9110 §5.6.7: a recipient MUST accept all
/// three date formats). Parsing only the preferred IMF-fixdate made an `If-Unmodified-Since`
/// carrying an obsolete date go unparsed → ignored → serve 200 where 412 is conformant — for
/// If-Unmodified-Since, "ignore" inverts the answer. The object is PUT "now", so a past instant
/// drives IUS to 412 (the object was modified after it) and a future instant drives IMS to 304
/// (the object was not modified after it).
#[tokio::test]
async fn obsolete_http_date_formats_are_honored_on_conditionals() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/obsolete-date-object";
    let (_object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    // (format label, a PAST instant, a FUTURE instant) for each obsolete date format.
    let cases: &[(&str, &str, &str)] = &[
        // RFC-850: full weekday name, two-digit year, resolved relative to the clock per
        // RFC 9110 §5.6.7 (current century first, minus 100 when >50 years ahead — `94` →
        // 1994, `69` → 2069 at any plausible test-run date).
        (
            "RFC-850",
            "Sunday, 06-Nov-94 08:49:37 GMT",
            "Saturday, 06-Nov-69 08:49:37 GMT",
        ),
        // asctime: space-padded day, four-digit year (an unambiguous far-future year).
        (
            "asctime",
            "Sun Nov  6 08:49:37 1994",
            "Sat Nov  6 08:49:37 2100",
        ),
    ];

    for &(label, past, future) in cases {
        // If-Unmodified-Since with a PAST obsolete-format date → 412 (a fail-open 200 is the bug).
        let mut ius = signed_headers("GET", path, &host, b"");
        ius.push(("if-unmodified-since".to_string(), past.to_string()));
        let (status, _head, _body) = send(addr, "GET", path, &ius, b"").await;
        assert_eq!(
            status, 412,
            "{label}: If-Unmodified-Since with a past {label} date must fire 412, not fail open to 200"
        );

        // If-Modified-Since with a FUTURE obsolete-format date → 304 (not modified after it).
        let mut ims = signed_headers("GET", path, &host, b"");
        ims.push(("if-modified-since".to_string(), future.to_string()));
        let (status, _head, body) = send(addr, "GET", path, &ims, b"").await;
        assert_eq!(
            status, 304,
            "{label}: If-Modified-Since with a future {label} date must answer 304 (not modified)"
        );
        assert!(body.is_empty(), "{label}: a 304 carries no body");
    }
}

/// Repeated `Range` FIELD LINES are, under HTTP field combination (RFC 9110 §5.2), one
/// comma-separated multi-range set — unsupported here, so they must degrade to the full 200
/// exactly like the in-line `bytes=a-b,c-d` form. A handler reading only the FIRST stored
/// value would honour `bytes=0-1` as a 206 and silently discard the second requested span —
/// a half-served request (issue #510 review).
#[tokio::test]
async fn repeated_range_field_lines_degrade_to_the_full_200() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/repeated-range-object";
    let (object, _etag) = put_object(addr, path).await;
    let host = addr.to_string();

    let mut headers = signed_headers("GET", path, &host, b"");
    headers.push(("range".to_string(), "bytes=0-1".to_string()));
    headers.push(("range".to_string(), "bytes=4-5".to_string()));
    let (status, head, body) = send(addr, "GET", path, &headers, b"").await;
    assert_eq!(
        status, 200,
        "two Range field lines are a multi-range set and must answer the full 200, \
         not a 206 of the first line only"
    );
    assert_eq!(body, object, "the full body is served");
    assert!(
        header_value(&head, "content-range").is_none(),
        "a full 200 carries no Content-Range"
    );
}

/// HEAD must HONOUR `Range` now that it advertises `Accept-Ranges: bytes` (carry-forward item 3):
/// a satisfiable range answers **206** with `Content-Range` and a SPAN `Content-Length` (no body —
/// a HEAD never carries one), mirroring GET; an unsatisfiable range answers **416**. The previous
/// attempt ignored `Range` on HEAD — a satisfiable `bytes=8-15` served 200 with the full size, and
/// an unsatisfiable `bytes=999-` served 200 too, both live deviations from real S3.
#[tokio::test]
async fn head_honors_range_with_206_and_416() {
    let (addr, _dir, _counter) = start_gateway().await;
    let path = "/wyrd-bucket/head-range-object";
    let (object, etag) = put_object(addr, path).await;
    let size = object.len();
    let host = addr.to_string();

    // Satisfiable: `bytes=8-15` → 206, `Content-Range: bytes 8-15/64`, `Content-Length: 8`, and —
    // because it is a HEAD — no body. The validators (ETag, Last-Modified) still ride along.
    let mut head = signed_headers("HEAD", path, &host, b"");
    head.push(("range".to_string(), "bytes=8-15".to_string()));
    let (status, head_block, body) = send(addr, "HEAD", path, &head, b"").await;
    assert_eq!(
        status, 206,
        "HEAD with a satisfiable Range must answer 206 Partial Content, not 200"
    );
    assert_eq!(
        header_value(&head_block, "content-range").as_deref(),
        Some(format!("bytes 8-15/{size}").as_str()),
        "HEAD 206 must carry Content-Range naming the span and the total size"
    );
    assert_eq!(
        header_value(&head_block, "content-length").as_deref(),
        Some("8"),
        "HEAD 206 Content-Length must be the SPAN length (8), not the object size"
    );
    assert!(body.is_empty(), "a HEAD never carries a body, even a 206");
    assert_eq!(
        header_value(&head_block, "etag").as_deref(),
        Some(etag.as_str()),
        "a HEAD 206 still carries the object's ETag validator"
    );

    // Unsatisfiable: `bytes=999-` → 416 with `Content-Range: bytes */64`.
    let mut head = signed_headers("HEAD", path, &host, b"");
    head.push(("range".to_string(), "bytes=999-".to_string()));
    let (status, head_block, _body) = send(addr, "HEAD", path, &head, b"").await;
    assert_eq!(
        status, 416,
        "HEAD with an unsatisfiable Range must answer 416, not 200"
    );
    assert_eq!(
        header_value(&head_block, "content-range").as_deref(),
        Some(format!("bytes */{size}").as_str()),
        "HEAD 416 must carry Content-Range: bytes */<size>"
    );
}

/// The anti-wire-side-discard oracle: a narrow ranged GET of a many-chunk object must fetch ONLY
/// the covering chunks. An implementation that streams the whole object and discards out-of-range
/// bytes wire-side passes every header/body assertion above byte-identically — this is the only
/// assertion that catches it, by counting the fragments the read path actually fetched.
#[tokio::test]
async fn narrow_range_fetches_only_the_covering_chunks() {
    let (addr, _dir, counter) = start_gateway().await;
    let path = "/wyrd-bucket/many-chunk-object";
    let (object, _etag) = put_object(addr, path).await;
    // 64 bytes over CHUNK_SIZE (8) → 8 chunks; chunk `i` holds bytes [8i, 8i+8).
    let total_chunks = object.len().div_ceil(CHUNK_SIZE);
    assert!(
        total_chunks >= 4,
        "the object must span several chunks for the oracle to bite"
    );

    // A whole-object GET must fetch EVERY chunk — the baseline the ranged read is measured against.
    counter.reset();
    let (status, _head, _body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &addr.to_string(), b""),
        b"",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        counter.distinct_chunks(),
        total_chunks,
        "a full GET reads all {total_chunks} chunks (the baseline)"
    );

    // `bytes=8-15` covers exactly chunk 1: a range-honouring read fetches ONE chunk, a
    // stream-then-discard read fetches all eight.
    counter.reset();
    let (status, head, body) = ranged_get(addr, path, "bytes=8-15").await;
    assert_eq!(status, 206, "the narrow range must answer 206");
    assert_eq!(body, object[8..16], "the body is the covered slice");
    assert_eq!(
        header_value(&head, "content-range").as_deref(),
        Some(format!("bytes 8-15/{}", object.len()).as_str())
    );
    assert_eq!(
        counter.distinct_chunks(),
        1,
        "a `bytes=8-15` GET must fetch ONLY the one covering chunk, not stream-then-discard the \
         whole {total_chunks}-chunk object"
    );
}

/// The conditional-short-circuit oracle (issue #510 review): a GET whose precondition fires
/// (304/412) must cost ZERO chunk reads. The conditionals are deliberately judged against the
/// SAME resolve that yields the body (no head-then-read TOCTOU window), and the wire layer then
/// drops the body stream unread — so the stream must be LAZY: an implementation that spawns its
/// reader eagerly at resolve time races ahead and fetches chunks (up to its channel bound) for a
/// body nobody will ever see, and every header/status assertion above passes byte-identically.
/// Only the fragment counter catches it. Both the unranged (`get_object_streaming`) and ranged
/// (`get_object_range`) paths are held to the bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fired_precondition_costs_zero_chunk_reads() {
    let (addr, _dir, counter) = start_gateway().await;
    let path = "/wyrd-bucket/revalidated-object";
    let (_object, etag) = put_object(addr, path).await;
    let host = addr.to_string();

    // 304 — the cache-revalidation loop's steady state: the client already holds the bytes, so
    // fetching even one chunk is pure waste, multiplied by every revalidation of every object.
    counter.reset();
    let mut headers = signed_headers("GET", path, &host, b"");
    headers.push(("if-none-match".to_string(), etag.clone()));
    let (status, _head, _body) = send(addr, "GET", path, &headers, b"").await;
    assert_eq!(status, 304);
    assert_eq!(
        counter.distinct_chunks(),
        0,
        "a 304 revalidation must read no chunk at all — the body stream is dropped unread and \
         must not have started"
    );

    // 412 — an If-Match overwrite guard that loses: same bound.
    counter.reset();
    let mut headers = signed_headers("GET", path, &host, b"");
    headers.push(("if-match".to_string(), "\"0badc0de0badc0de\"".to_string()));
    let (status, _head, _body) = send(addr, "GET", path, &headers, b"").await;
    assert_eq!(status, 412);
    assert_eq!(
        counter.distinct_chunks(),
        0,
        "a 412 precondition failure must read no chunk at all"
    );

    // The RANGED path's short-circuit: `get_object_range` resolves metadata and covering
    // chunks from one snapshot; a firing precondition drops that stream unread too.
    counter.reset();
    let mut headers = signed_headers("GET", path, &host, b"");
    headers.push(("if-none-match".to_string(), etag.clone()));
    headers.push(("range".to_string(), "bytes=8-15".to_string()));
    let (status, _head, _body) = send(addr, "GET", path, &headers, b"").await;
    assert_eq!(
        status, 304,
        "a conditional ranged GET short-circuits first (§13.2)"
    );
    assert_eq!(
        counter.distinct_chunks(),
        0,
        "a 304 on the ranged path must read no chunk at all"
    );
}
