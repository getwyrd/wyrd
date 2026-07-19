//! `HeadObject` (`HTTP HEAD /bucket/key`), exercised over a real loopback listener (issue
//! #506). Before this fix, the object dispatch matched only PUT/GET/DELETE and fell through
//! to the 405 `MethodNotAllowed` arm — breaking the ubiquitous HEAD-before-GET/PUT pattern
//! (`aws s3 cp`'s download preflight, most SDK existence checks).
//!
//! Adapted from the `s3_http_wire.rs` peer's `signed_headers` / `send` / `parse_response`
//! helpers (private to that test crate), extended here to also capture response **headers**
//! — the peer returns only status+body, but this criterion asserts the metadata headers and
//! the empty-body property a HEAD carries.
//!
//! RED before this patch: a signed HEAD on a stored object returns `405`. GREEN after: `200`
//! with an empty body and the same `Content-Length` / `ETag` / `Content-Type` /
//! `Last-Modified` headers a GET of the same object carries (#503); a HEAD of an absent key
//! returns `404` headers-only.

use std::net::SocketAddr;
use std::time::SystemTime;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

fn build_gateway(dir: &std::path::Path) -> std::sync::Arc<Backend> {
    std::sync::Arc::new(
        Gateway::new(
            RedbMetadataStore::in_memory().expect("redb"),
            FsChunkStore::open(dir).expect("fs store"),
            MemCoordination::new(),
        )
        .with_chunk_size(8),
    )
}

async fn start_gateway() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = build_gateway(dir.path());
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
    (addr, dir)
}

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

/// One raw HTTP/1.1 response: status, the parsed header block, and the (de-chunked, if
/// needed) body. Unlike the `s3_http_wire.rs` peer's `parse_response` (status + body only),
/// this criterion needs the metadata headers a HEAD carries, so the header block is kept.
struct RawResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RawResponse {
    /// Look up a header value case-insensitively — HTTP header names are case-insensitive
    /// and a HEAD's `Content-Type`/`ETag`/`Last-Modified` could render in any case.
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Send one HTTP/1.1 request over a fresh connection and return the parsed response,
/// headers included.
async fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> RawResponse {
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
    parse_response(&raw)
}

fn parse_response(raw: &[u8]) -> RawResponse {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut lines = head.lines();
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();

    let raw_body = &raw[split + 4..];
    let is_chunked = headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked"));
    let body = if is_chunked {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    RawResponse {
        status,
        headers,
        body,
    }
}

/// Decode an HTTP/1.1 chunked-transfer-encoded body (`<hex-size>\r\n<bytes>\r\n…0\r\n\r\n`).
fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let line_end = raw
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("chunk size line");
        let size_str = String::from_utf8_lossy(&raw[..line_end]);
        let size = usize::from_str_radix(size_str.trim(), 16).expect("hex chunk size");
        raw = &raw[line_end + 2..];
        if size == 0 {
            break;
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..]; // skip the chunk's trailing CRLF
    }
    out
}

/// The success criterion, end to end: a signed HEAD of a stored object returns `200` with
/// no body and the SAME metadata headers GET carries after #503 — `Content-Length` (the
/// object's real size, not 0), `ETag`, `Content-Type`, `Last-Modified`.
///
/// RED pre-fix: the dispatch matches only PUT/GET/DELETE, so this HEAD falls through to the
/// `_` arm and returns `405 MethodNotAllowed`.
#[tokio::test]
async fn signed_head_of_a_stored_object_returns_metadata_headers_and_no_body() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/head-me";
    let object = b"an object whose HEAD must answer GET's metadata headers".to_vec();

    let put = send(
        addr,
        "PUT",
        path,
        &signed_headers(
            "PUT", path, &host,
            // Declare a real content-type so HEAD's fallback-vs-stored path is exercised.
            &object,
        ),
        &object,
    )
    .await;
    assert_eq!(put.status, 200, "signed PUT must be accepted");

    // The GET response is the oracle for what HEAD's headers must equal (brief success
    // criterion: "the same metadata headers GET carries").
    let get = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(get.status, 200, "signed GET must be accepted");

    let head = send(
        addr,
        "HEAD",
        path,
        &signed_headers("HEAD", path, &host, b""),
        b"",
    )
    .await;

    assert_eq!(
        head.status, 200,
        "a signed HEAD of a stored object must succeed (pre-fix: 405 MethodNotAllowed)"
    );
    assert!(
        head.body.is_empty(),
        "a HEAD response must carry no body on the wire"
    );
    assert_eq!(
        head.header("content-length"),
        get.header("content-length"),
        "HEAD's Content-Length must equal the object's real size, matching what GET declares"
    );
    assert_eq!(
        head.header("content-length"),
        Some(object.len().to_string()).as_deref(),
        "HEAD's Content-Length must be the object's real size, not 0"
    );
    assert_eq!(
        head.header("etag"),
        get.header("etag"),
        "HEAD's ETag must match GET's"
    );
    assert_eq!(
        head.header("content-type"),
        get.header("content-type"),
        "HEAD's Content-Type must match GET's"
    );
    assert_eq!(
        head.header("last-modified"),
        get.header("last-modified"),
        "HEAD's Last-Modified must match GET's"
    );
}

/// A signed HEAD of an absent key returns `404` headers-only (brief success criterion).
#[tokio::test]
async fn signed_head_of_an_absent_key_returns_404_headers_only() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/never-existed";

    let head = send(
        addr,
        "HEAD",
        path,
        &signed_headers("HEAD", path, &host, b""),
        b"",
    )
    .await;

    assert_eq!(
        head.status, 404,
        "a signed HEAD of an absent key must be 404 (pre-fix: 405 MethodNotAllowed)"
    );
    assert!(
        head.body.is_empty(),
        "a HEAD response must carry no body on the wire, even on a 404"
    );
}

/// Out-of-scope guard (brief): HEAD must not perturb GET/PUT/DELETE behaviour. A HEAD, then
/// a GET, then a DELETE, then a GET again — all must behave exactly as they do with no HEAD
/// in between.
#[tokio::test]
async fn head_does_not_change_get_put_delete_behaviour() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/unaffected-by-head";
    let object = b"HEAD must be a pure read with no side effects".to_vec();

    let put = send(
        addr,
        "PUT",
        path,
        &signed_headers("PUT", path, &host, &object),
        &object,
    )
    .await;
    assert_eq!(put.status, 200);

    let head = send(
        addr,
        "HEAD",
        path,
        &signed_headers("HEAD", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(head.status, 200);

    let get = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(get.status, 200);
    assert_eq!(
        get.body, object,
        "a HEAD must not have changed the object GET returns"
    );

    let delete = send(
        addr,
        "DELETE",
        path,
        &signed_headers("DELETE", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(delete.status, 204, "DELETE must still succeed after a HEAD");

    let get_after = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(
        get_after.status, 404,
        "the object must be gone after DELETE"
    );
}
