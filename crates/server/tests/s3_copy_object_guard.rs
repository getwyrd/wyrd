//! CopyObject-as-PutObject data-loss guard (issue #504).
//!
//! `CopyObject` is a `PUT /dst-bucket/key` carrying an `x-amz-copy-source` header. Before
//! this fix, the gateway never read that header (zero mentions anywhere in
//! `crates/gateway-s3/`), so the PUT arm streamed the request **body** — which a real copy
//! request sends empty — into the destination key and answered `200`: a client asking for a
//! copy got a success response and a destroyed destination object.
//!
//! This test drives the real wire path over an in-process loopback TCP listener (the
//! `crates/server/tests/s3_http_wire.rs` pattern: redb in-memory metadata + an fs chunk
//! store in a tempdir, production `sigv4::sign`), duplicating that file's private harness
//! helpers (`start_gateway`, `signed_headers`, `send`, `parse_response`) since each
//! `tests/*.rs` file compiles as its own crate and cannot import them.
//!
//! RED on the base today: PUT an object, then a signed PUT of the same path carrying
//! `x-amz-copy-source` and an empty body returns `200`, and a subsequent GET returns zero
//! bytes (the destination was overwritten). GREEN once the guard refuses that form with
//! `501 NotImplemented` before touching the body, leaving the destination untouched, while
//! an ordinary PUT (no such header) still stores normally.

use std::net::SocketAddr;
use std::sync::Arc;

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

/// A gateway with a deliberately small chunk size, so a modest object still spans several
/// chunks over the streaming wire path (mirrors `s3_http_wire.rs::build_gateway`).
fn build_gateway(dir: &std::path::Path) -> Arc<Backend> {
    Arc::new(
        Gateway::new(
            RedbMetadataStore::in_memory().expect("redb"),
            FsChunkStore::open(dir).expect("fs store"),
            MemCoordination::new(),
        )
        .with_chunk_size(8),
    )
}

/// Start the S3 gateway on an ephemeral loopback port and return its address. The
/// `TempDir` is returned so the caller keeps the chunk store alive for the test.
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
    // Stamp a fresh timestamp so the request is inside the gateway's freshness window.
    let amz_date = format_amz_date(std::time::SystemTime::now());
    let signed = sign(
        method, path, "", host, &amz_date, body, &creds, REGION, "s3",
    );
    vec![
        ("authorization".to_string(), signed.authorization),
        ("x-amz-date".to_string(), signed.amz_date),
        ("x-amz-content-sha256".to_string(), signed.content_sha256),
    ]
}

/// Send one HTTP/1.1 request over a fresh connection and return `(status, body)`.
async fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> (u16, Vec<u8>) {
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

fn parse_response(raw: &[u8]) -> (u16, Vec<u8>) {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]);
    let status_line = head.lines().next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    let raw_body = &raw[split + 4..];
    let is_chunked = head.lines().any(|l| {
        l.to_ascii_lowercase().starts_with("transfer-encoding:")
            && l.to_ascii_lowercase().contains("chunked")
    });
    let body = if is_chunked {
        dechunk(raw_body)
    } else {
        raw_body.to_vec()
    };
    (status, body)
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

/// The core repro (brief §Repro instruction): a signed PUT carrying `x-amz-copy-source` and
/// an empty body must be REFUSED (501, S3 `NotImplemented`), and the destination object's
/// prior content must survive byte-identical — not the pre-fix `200` + destroyed object.
#[tokio::test]
async fn copy_source_put_is_refused_and_destination_survives() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/precious-object";
    let original = b"precious".to_vec();

    let (status, _) = send(
        addr,
        "PUT",
        path,
        &signed_headers("PUT", path, &host, &original),
        &original,
    )
    .await;
    assert_eq!(status, 200, "the initial signed PUT must be accepted");

    // A CopyObject request: same signed headers an ordinary empty-body PUT would carry,
    // plus `x-amz-copy-source` — which need not be in the signed-header set for the guard
    // to apply (brief: "detect it on the request headers regardless").
    let mut copy_headers = signed_headers("PUT", path, &host, b"");
    copy_headers.push((
        "x-amz-copy-source".to_string(),
        "/wyrd-bucket/other-key".to_string(),
    ));
    let (status, body) = send(addr, "PUT", path, &copy_headers, b"").await;
    assert_eq!(
        status, 501,
        "an x-amz-copy-source PUT must be refused (NotImplemented), not stored as an empty body"
    );
    let xml = String::from_utf8_lossy(&body);
    assert!(
        xml.contains("<Code>NotImplemented</Code>"),
        "the 501 body must carry the S3 NotImplemented error code, got: {xml}"
    );

    // The destination must be untouched by the refused request.
    let (status, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200, "the destination object must still exist");
    assert_eq!(
        body, original,
        "the destination's prior content must be byte-identical after the refused copy"
    );
}

/// An ordinary PUT (no `x-amz-copy-source` header) is unaffected by the guard and still
/// stores normally.
#[tokio::test]
async fn ordinary_put_without_copy_source_still_stores() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/ordinary-object";
    let object = b"an ordinary object".to_vec();

    let (status, _) = send(
        addr,
        "PUT",
        path,
        &signed_headers("PUT", path, &host, &object),
        &object,
    )
    .await;
    assert_eq!(status, 200, "an ordinary signed PUT must still be accepted");

    let (status, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, object, "GET must return the PUT bytes byte-identical");
}
