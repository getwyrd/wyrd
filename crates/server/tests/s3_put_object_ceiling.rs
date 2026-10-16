//! The single-PUT ceiling (#671). A single PUT is published as ONE flat chunk map, and a flat
//! map holds at most `MAX_MAP_CHUNKS` = 158 chunk refs, so the largest object one PUT can
//! publish is `158 × chunk_size`. Every gateway here is built with `.with_chunk_size(8)`, so
//! the ceiling is 158 × 8 = **1,264 bytes**. This drives the production wire path (the
//! loopback HTTP listener, `sigv4::verify`, `streaming::decode`, the S3 PUT arm and
//! `Gateway::put_object_streaming`) and the buffered `Gateway::put_object`, and asserts:
//!
//! 1. a PUT of exactly 1,264 bytes succeeds and round-trips byte for byte;
//! 2. a PUT of 1,265 bytes answers `400 EntityTooLarge`, naming its limit and not pointing at
//!    multipart, and stores nothing (HEAD `404`, no chunk on disk);
//! 3. a signed PUT declaring a 2 GiB `Content-Length` that never sends its body is refused
//!    `400 EntityTooLarge` within seconds — the refusal comes from the headers;
//! 4. a streamed `aws-chunked` PUT declaring `x-amz-decoded-content-length: 1265` is refused
//!    the same way, from the headers, with no body sent;
//! 5. a PUT that declares no object length — an `aws-chunked` upload (plain or `-TRAILER`)
//!    without `x-amz-decoded-content-length`, or a `Transfer-Encoding: chunked` body without
//!    `Content-Length` — is refused `411 MissingContentLength` and stores nothing;
//! 6. a streamed upload declaring 1,000 bytes that streams 1,265 is refused, publishes
//!    nothing, and writes at most the declared length's worth of chunks; one that streams
//!    fewer bytes than declared is refused too;
//!
//! 8a. the buffered `Gateway::put_object` is bounded the same way: 1,264 bytes is stored,
//!     1,265 bytes is `Err` with no chunk written and nothing readable.
//!
//! # Compiles on the base tree
//! The test uses only API that exists before the fix: the wire, `Gateway::put_object` /
//! `get_object`, and the public SigV4 client primitives (`signing_key_for`, `canonical_query`,
//! `format_amz_date`, `sign`, `crypto::{hex, hmac_sha256, sha256}`). It builds its own signed
//! requests and `aws-chunked` frames from those, so — with the production fix reverted — every
//! leg still reaches the gateway and fails on an assertion or a timeout, not on a compile
//! error. The gateway recomputes every signature itself, so this client-side signer is a peer
//! of the verifier, not a copy of it.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): SigV4 request dates against a live in-process server use
// real wall time; nothing here mixes clock sources within one asserted lifecycle (#619).
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::crypto::{hex, hmac_sha256, sha256};
use wyrd_gateway_s3::sigv4::{
    canonical_query, format_amz_date, sign, signing_key_for, Credentials,
};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// The chunk size every gateway in this file runs at.
const CHUNK_SIZE: usize = 8;
/// `MAX_MAP_CHUNKS × CHUNK_SIZE` = 158 × 8: the largest single PUT these gateways publish.
const CEILING: usize = 158 * CHUNK_SIZE;

/// How long a refusal "from the headers" may take. A gateway that waits for a body that is
/// never sent blows through this and the leg fails instead of hanging.
const HEADER_REFUSAL_BOUND: Duration = Duration::from_secs(10);

/// SHA-256 of the empty string — the fixed `Hash("")` line of every chunk string-to-sign.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// One in-process gateway over redb (in memory, no value limit — the backend that would
/// publish an over-budget map) and an fs chunk store whose chunks a test can count.
struct Fixture {
    addr: SocketAddr,
    gateway: Arc<Backend>,
    chunks: PathBuf,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let chunks = dir.path().join("chunks");
        let gateway = Arc::new(
            Gateway::new(
                RedbMetadataStore::in_memory().expect("redb"),
                FsChunkStore::open(&chunks).expect("fs store"),
                MemCoordination::new(),
            )
            .with_chunk_size(CHUNK_SIZE),
        );
        let config = S3Config::new(vec![Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
        }]);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = S3Gateway::new(Arc::clone(&gateway), config);
        tokio::spawn(async move {
            server.serve(listener).await.expect("serve");
        });
        Self {
            addr,
            gateway,
            chunks,
            _dir: dir,
        }
    }

    fn host(&self) -> String {
        self.addr.to_string()
    }

    /// Chunks on disk: directories under the fs store root named as a chunk id that hold at
    /// least one fragment file.
    fn chunk_count(&self) -> usize {
        count_chunk_dirs(&self.chunks)
    }

    /// HEAD `path` and return its status.
    async fn head_status(&self, path: &str) -> u16 {
        let headers = sign_request("HEAD", path, &self.host(), EMPTY_SHA256, &[]).0;
        send(self.addr, "HEAD", path, &headers, b"").await.0
    }
}

fn count_chunk_dirs(root: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            let is_chunk_id = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.len() == 32 && n.bytes().all(|b| b.is_ascii_hexdigit()));
            is_chunk_id
                && std::fs::read_dir(path).is_ok_and(|frags| {
                    frags
                        .flatten()
                        .any(|f| f.path().extension().and_then(|e| e.to_str()) == Some("frag"))
                })
        })
        .count()
}

/// Sign a request the way an SDK does, over `host`, `x-amz-content-sha256`, `x-amz-date` and
/// every `extra` header (an SDK signs the `x-amz-*` headers it sends, including
/// `x-amz-decoded-content-length`). Returns the headers to send (the extras included) and the
/// signature, which is the seed of an `aws-chunked` chunk chain.
fn sign_request(
    method: &str,
    path: &str,
    host: &str,
    payload_hash: &str,
    extra: &[(&str, &str)],
) -> (Vec<(String, String)>, ChunkSigner) {
    let amz_date = format_amz_date(SystemTime::now());
    let date = &amz_date[..8];
    let scope = format!("{date}/{REGION}/s3/aws4_request");
    let mut signed: Vec<(String, String)> = vec![
        ("host".to_string(), host.to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    for (name, value) in extra {
        signed.push((name.to_ascii_lowercase(), value.to_string()));
    }
    signed.sort_by(|a, b| a.0.cmp(&b.0));
    let signed_names = signed
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = signed.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
    let canonical = format!(
        "{method}\n{path}\n{}\n{canonical_headers}\n{signed_names}\n{payload_hash}",
        canonical_query("")
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&sha256(canonical.as_bytes()))
    );
    let key = signing_key_for(SECRET_KEY, date, REGION, "s3");
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, SignedHeaders={signed_names}, \
         Signature={signature}"
    );
    let mut headers = vec![
        ("authorization".to_string(), authorization),
        ("x-amz-date".to_string(), amz_date.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
    ];
    for (name, value) in extra {
        headers.push((name.to_ascii_lowercase(), value.to_string()));
    }
    (
        headers,
        ChunkSigner {
            seed: signature,
            key,
            date_time: amz_date,
            scope,
        },
    )
}

/// What an SDK holds to sign `aws-chunked` chunks: the seed signature and the signing scope.
struct ChunkSigner {
    seed: String,
    key: [u8; 32],
    date_time: String,
    scope: String,
}

impl ChunkSigner {
    /// The chained chunk signature (AWS "Transferring Payload in Multiple Chunks").
    fn sign_chunk(&self, previous: &str, data: &[u8]) -> String {
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{previous}\n{EMPTY_SHA256}\n{}",
            self.date_time,
            self.scope,
            hex(&sha256(data))
        );
        hex(&hmac_sha256(&self.key, string_to_sign.as_bytes()))
    }

    /// Frame `data` as signed `aws-chunked` chunks of `chunk_len` bytes plus the terminator.
    fn frame(&self, data: &[u8], chunk_len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        let mut previous = self.seed.clone();
        let mut pieces: Vec<&[u8]> = data.chunks(chunk_len).collect();
        pieces.push(&[]);
        for piece in pieces {
            let sig = self.sign_chunk(&previous, piece);
            out.extend_from_slice(
                format!("{:x};chunk-signature={sig}\r\n", piece.len()).as_bytes(),
            );
            out.extend_from_slice(piece);
            out.extend_from_slice(b"\r\n");
            previous = sig;
        }
        out
    }
}

/// The headers of a signed `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` upload, declaring
/// `decoded_len` (or no decoded length at all when `None`).
fn streaming_headers(
    path: &str,
    host: &str,
    decoded_len: Option<u64>,
) -> (Vec<(String, String)>, ChunkSigner) {
    let len = decoded_len.map(|n| n.to_string());
    let mut extra = vec![("content-encoding", "aws-chunked")];
    if let Some(len) = len.as_deref() {
        extra.push(("x-amz-decoded-content-length", len));
    }
    sign_request(
        "PUT",
        path,
        host,
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        &extra,
    )
}

/// Write `head` and `body` on a fresh connection and read the whole response, bounded by
/// [`HEADER_REFUSAL_BOUND`].
async fn exchange(addr: SocketAddr, head: String, body: &[u8]) -> (u16, Vec<u8>) {
    tokio::time::timeout(HEADER_REFUSAL_BOUND, async {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(head.as_bytes()).await.expect("write head");
        // A gateway that answered early may close before the whole body is written; the
        // response is what is asserted, so a write error after the head is not a failure.
        let _ = stream.write_all(body).await;
        let _ = stream.flush().await;
        let mut raw = Vec::new();
        let _ = stream.read_to_end(&mut raw).await;
        parse_response(&raw)
    })
    .await
    .expect("the gateway must answer within the bound")
}

fn request_head(
    method: &str,
    path: &str,
    host: &str,
    headers: &[(String, String)],
    content_length: Option<u64>,
) -> String {
    let mut head = format!("{method} {path} HTTP/1.1\r\nhost: {host}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(len) = content_length {
        head.push_str(&format!("content-length: {len}\r\n"));
    }
    head.push_str("connection: close\r\n\r\n");
    head
}

/// Send one request with an accurate `Content-Length` and return `(status, body)`.
async fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> (u16, Vec<u8>) {
    let head = request_head(
        method,
        path,
        &addr.to_string(),
        headers,
        Some(body.len() as u64),
    );
    exchange(addr, head, body).await
}

fn parse_response(raw: &[u8]) -> (u16, Vec<u8>) {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]);
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("numeric status");
    (status, raw[split + 4..].to_vec())
}

fn error_code(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    text.split("<Code>")
        .nth(1)
        .and_then(|rest| rest.split("</Code>").next())
        .unwrap_or("")
        .to_string()
}

fn object(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// (1) Exactly at the ceiling: 1,264 bytes is 158 chunks, one full flat map. Stored and
/// returned byte for byte.
#[tokio::test]
async fn a_put_at_the_ceiling_round_trips() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/at-the-ceiling";
    let body = object(CEILING);

    let headers = sign_request("PUT", path, &fx.host(), &hex(&sha256(&body)), &[]).0;
    let (status, _) = send(fx.addr, "PUT", path, &headers, &body).await;
    assert_eq!(
        status, 200,
        "a PUT of exactly {CEILING} bytes must be stored"
    );

    let headers = sign_request("GET", path, &fx.host(), EMPTY_SHA256, &[]).0;
    let (status, got) = send(fx.addr, "GET", path, &headers, b"").await;
    assert_eq!(status, 200);
    assert_eq!(
        got, body,
        "the object at the ceiling must round-trip byte for byte"
    );
}

/// (2) One byte past the ceiling: 159 chunks, one more than a flat map holds. Refused
/// `400 EntityTooLarge` naming the limit, without pointing at multipart, and nothing stored.
#[tokio::test]
async fn a_put_one_byte_past_the_ceiling_is_refused_and_stores_nothing() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/past-the-ceiling";
    let body = object(CEILING + 1);

    let headers = sign_request("PUT", path, &fx.host(), &hex(&sha256(&body)), &[]).0;
    let (status, response) = send(fx.addr, "PUT", path, &headers, &body).await;
    assert_eq!(
        status,
        400,
        "a PUT of {} bytes must be refused",
        CEILING + 1
    );
    assert_eq!(error_code(&response), "EntityTooLarge");
    let text = String::from_utf8_lossy(&response).to_ascii_lowercase();
    assert!(
        text.contains(&CEILING.to_string()),
        "the refusal must name the {CEILING}-byte limit: {text}"
    );
    assert!(
        !text.contains("multipart"),
        "the refusal must not point at multipart, which this gateway answers 501: {text}"
    );

    assert_eq!(fx.head_status(path).await, 404, "nothing may be published");
    assert_eq!(
        fx.chunk_count(),
        0,
        "no chunk may be written for a refused PUT"
    );
}

/// (3) The refusal comes from the headers: a signed PUT declaring 2 GiB that never sends a
/// byte of its body is answered `400 EntityTooLarge` within the bound.
#[tokio::test]
async fn a_declared_two_gib_put_is_refused_before_its_body_is_read() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/two-gib-never-sent";
    let headers = sign_request("PUT", path, &fx.host(), EMPTY_SHA256, &[]).0;
    let head = request_head("PUT", path, &fx.host(), &headers, Some(2_147_483_648));

    let (status, response) = exchange(fx.addr, head, b"").await;
    assert_eq!(
        status, 400,
        "a 2 GiB declared PUT must be refused from its headers"
    );
    assert_eq!(error_code(&response), "EntityTooLarge");
    assert_eq!(fx.head_status(path).await, 404);
    assert_eq!(fx.chunk_count(), 0);
}

/// (4) The same from an `aws-chunked` upload's declared object length: decoded length 1,265,
/// no body sent, refused `400 EntityTooLarge` at the header.
#[tokio::test]
async fn a_streamed_put_declaring_past_the_ceiling_is_refused_at_the_header() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/streamed-past-the-ceiling";
    let (headers, signer) = streaming_headers(path, &fx.host(), Some((CEILING + 1) as u64));
    let framed = signer.frame(&object(CEILING + 1), 128);
    // Declare the whole framed body, send none of it.
    let head = request_head("PUT", path, &fx.host(), &headers, Some(framed.len() as u64));

    let (status, response) = exchange(fx.addr, head, b"").await;
    assert_eq!(
        status,
        400,
        "a streamed PUT declaring {} bytes must be refused",
        CEILING + 1
    );
    assert_eq!(error_code(&response), "EntityTooLarge");
    assert_eq!(fx.head_status(path).await, 404);
    assert_eq!(fx.chunk_count(), 0);
}

/// (5a) An `aws-chunked` upload that declares no object length — the plain signed form, a
/// small 100-byte body — is refused `411 MissingContentLength`, and nothing is stored.
#[tokio::test]
async fn a_streamed_put_without_a_decoded_length_is_refused_411() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/streamed-no-length";
    let (headers, signer) = streaming_headers(path, &fx.host(), None);
    let framed = signer.frame(&object(100), 32);

    let (status, response) = send(fx.addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 411,
        "an aws-chunked PUT with no decoded length must be refused"
    );
    assert_eq!(error_code(&response), "MissingContentLength");
    assert_eq!(fx.head_status(path).await, 404);
    assert_eq!(fx.chunk_count(), 0);
}

/// (5b) The `-TRAILER` variants need the decoded length too: an unsigned checksum-trailer
/// upload with no `x-amz-decoded-content-length` is refused `411` from the headers.
#[tokio::test]
async fn a_trailer_put_without_a_decoded_length_is_refused_411() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/trailer-no-length";
    let headers = sign_request(
        "PUT",
        path,
        &fx.host(),
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        &[
            ("content-encoding", "aws-chunked"),
            ("x-amz-trailer", "x-amz-checksum-sha256"),
        ],
    )
    .0;
    let body = object(100);
    let checksum = base64(&sha256(&body));
    let mut framed = format!("{:x}\r\n", body.len()).into_bytes();
    framed.extend_from_slice(&body);
    framed
        .extend_from_slice(format!("\r\n0\r\nx-amz-checksum-sha256:{checksum}\r\n\r\n").as_bytes());

    let (status, response) = send(fx.addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 411,
        "a -TRAILER PUT with no decoded length must be refused"
    );
    assert_eq!(error_code(&response), "MissingContentLength");
    assert_eq!(fx.head_status(path).await, 404);
    assert_eq!(fx.chunk_count(), 0);
}

/// (5c) A `Transfer-Encoding: chunked` body with no `Content-Length` declares no length:
/// refused `411 MissingContentLength`, nothing stored.
#[tokio::test]
async fn a_transfer_encoding_chunked_put_without_content_length_is_refused_411() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/te-chunked-no-length";
    let body = object(100);
    let headers = sign_request("PUT", path, &fx.host(), &hex(&sha256(&body)), &[]).0;
    let mut head = request_head("PUT", path, &fx.host(), &headers, None);
    head.truncate(head.len() - 2);
    head.push_str("transfer-encoding: chunked\r\n\r\n");
    let mut chunked = format!("{:x}\r\n", body.len()).into_bytes();
    chunked.extend_from_slice(&body);
    chunked.extend_from_slice(b"\r\n0\r\n\r\n");

    let (status, response) = exchange(fx.addr, head, &chunked).await;
    assert_eq!(
        status, 411,
        "a chunked-transfer PUT with no Content-Length must be refused"
    );
    assert_eq!(error_code(&response), "MissingContentLength");
    assert_eq!(fx.head_status(path).await, 404);
    assert_eq!(fx.chunk_count(), 0);
}

/// (6) A streamed upload is held to its declared length while it streams: declared 1,000
/// (under the ceiling), streamed 1,265 — refused, nothing published, and at most the declared
/// 1,000 bytes' worth of chunks (125) written, not the whole stream's 159.
#[tokio::test]
async fn a_streamed_put_that_runs_past_its_declared_length_is_refused() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/streamed-runs-long";
    let declared = 1_000usize;
    let (headers, signer) = streaming_headers(path, &fx.host(), Some(declared as u64));
    let framed = signer.frame(&object(CEILING + 1), 64);

    let (status, _) = send(fx.addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "a stream longer than its declared length must be refused"
    );
    assert_eq!(fx.head_status(path).await, 404, "nothing may be published");
    let written = fx.chunk_count();
    assert!(
        written <= declared.div_ceil(CHUNK_SIZE),
        "the gateway must stop at the declared length: {written} chunks written, at most {} \
         allowed",
        declared.div_ceil(CHUNK_SIZE)
    );
}

/// (6, short) A stream that ends short of its declared length is refused too, and publishes
/// nothing.
#[tokio::test]
async fn a_streamed_put_shorter_than_its_declared_length_is_refused() {
    let fx = Fixture::start().await;
    let path = "/wyrd-bucket/streamed-runs-short";
    let (headers, signer) = streaming_headers(path, &fx.host(), Some(1_000));
    let framed = signer.frame(&object(900), 64);

    let (status, _) = send(fx.addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "a stream shorter than its declared length must be refused"
    );
    assert_eq!(fx.head_status(path).await, 404, "nothing may be published");
}

/// (8a) The buffered path is bounded too: `Gateway::put_object` stores 1,264 bytes and
/// refuses 1,265 with no chunk written and nothing readable. Only `Err` is asserted, not its
/// type; the wire legs pin the type by its `EntityTooLarge` mapping.
#[tokio::test]
async fn the_buffered_put_is_bounded_by_the_same_ceiling() {
    let fx = Fixture::start().await;

    let at = object(CEILING);
    fx.gateway
        .put_object("wyrd-bucket/buffered-at", &at)
        .await
        .expect("a buffered PUT at the ceiling must be stored");
    assert_eq!(
        fx.gateway
            .get_object("wyrd-bucket/buffered-at")
            .await
            .expect("get"),
        Some(at)
    );

    let before = fx.chunk_count();
    let refused = fx
        .gateway
        .put_object("wyrd-bucket/buffered-past", &object(CEILING + 1))
        .await;
    assert!(
        refused.is_err(),
        "a buffered PUT one byte past the ceiling must be refused"
    );
    assert_eq!(
        fx.chunk_count(),
        before,
        "a refused buffered PUT must write no chunk"
    );
    assert_eq!(
        fx.gateway
            .get_object("wyrd-bucket/buffered-past")
            .await
            .expect("get"),
        None,
        "a refused buffered PUT must publish nothing"
    );
}

/// Standard padded base64 (RFC 4648), for a test trailer value.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// The client signer above must agree with the production `sign` on a plain request, so a
/// green leg is not riding on a signer that only this file believes in.
#[test]
fn the_local_signer_matches_the_production_signer_without_extra_headers() {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let (headers, _) = sign_request("GET", "/b/k", "h:1", EMPTY_SHA256, &[]);
    let amz_date = &headers[1].1;
    let expected = sign(
        "GET", "/b/k", "", "h:1", amz_date, b"", &creds, REGION, "s3",
    );
    assert_eq!(headers[0].1, expected.authorization);
}
