//! `aws-chunked` checksum-**trailer** streaming PUT (issue #505). A **default-configured**
//! stock SDK (boto3 / aws-cli / modern aws-sdk) does not send the plain
//! `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` sentinel `s3_http_wire.rs`'s `streaming_interop`
//! module already covers — it defaults to the checksum-**trailer** framing
//! (`STREAMING-UNSIGNED-PAYLOAD-TRAILER` / `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`, a
//! declared `x-amz-checksum-*` trailer sent after the terminating zero-length chunk), which
//! `sigv4::verify`'s closed set refused outright before this fix — 403, before any body was
//! read (the iter-6 "no half-accept" set, `sigv4.rs:510-515` on `origin/main`). A
//! default-configured `aws s3 cp` upload therefore 403-ed.
//!
//! This drives the **production** wire path — the real loopback HTTP listener,
//! `sigv4::verify` + `streaming::decode` — with exactly the byte format a stock SDK sends,
//! over BOTH trailer sentinels, and asserts:
//!
//! * each round-trips **byte-identical** through GET, for each of the minimum accepted
//!   checksum algorithms (`crc32`, `crc32c`, `sha256`) — `unsigned_trailer_put_...`;
//! * the signed variant's per-chunk AND trailer signatures are genuinely verified —
//!   `signed_trailer_put_...` (valid) / `a_forged_trailer_signature_is_refused...` (invalid);
//! * a tampered declared checksum is refused and the object is never published —
//!   `a_tampered_trailer_checksum_...`;
//! * an unrecognised checksum algorithm (`x-amz-checksum-crc64nvme`, out of scope) is
//!   refused before admission — `an_unrecognised_checksum_algorithm_...`;
//! * a malformed trailer section — bad base64, an undeclared trailer name, garbage after
//!   the trailer block — is refused as a 400, never a silent accept; and
//! * a declared `x-amz-decoded-content-length` mismatch is refused.
//!
//! # The RED is a genuine 403 on the wire, not a compile error
//! Deliberately, this test constructs its signed / unsigned trailer bodies using ONLY the
//! public SigV4 **client** primitives that already exist on `origin/main` —
//! [`sign_with_payload_hash`], [`signing_key_for`], and `wyrd_gateway_s3::crypto`'s
//! `sha256` / `hmac_sha256` / `hex` — exactly as a real SDK's signer does. It never
//! references the fix's new production surface (`StreamingContext.trailer`,
//! `streaming::sign_trailer`, `checksum::ChecksumAlgorithm`, `TrailerDeclaration`), so it
//! **compiles unchanged against the base tree**. Therefore, with the production fix
//! reverted (the shape the `C4-verify` oracle re-runs), every trailer PUT here still
//! reaches the gateway and gets the base **403** — a *behavioural* red an assertion catches
//! (`assert_eq!(status, 200)` fails), not a compile-red. GREEN once the trailer sentinels
//! are admitted and the decoder consumes/validates the trailer section (`sigv4.rs` /
//! `streaming.rs`). The gateway independently recomputes every chunk/trailer signature and
//! checksum, so this client-side signing is a genuine peer, not a re-implementation of the
//! verifier it drives.
//!
//! The wire-level test harness (`send`/`parse_response`/`dechunk`/gateway bring-up,
//! `signed_headers`) is adapted/duplicated from `s3_http_wire.rs` per the brief's citation
//! — those helpers are PRIVATE to that test crate, not importable from here.

// wall-clock exempt (test crate): SigV4 request dates / lease stamps against a
// live in-process server use real wall time; nothing here mixes clock sources
// within one asserted lifecycle (#619).
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::crypto::{hex, hmac_sha256, sha256};
use wyrd_gateway_s3::sigv4::{
    format_amz_date, sign, sign_with_payload_hash, signing_key_for, Credentials,
};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// SHA-256 of the empty string — the fixed `Hash("")` line in every chunk string-to-sign
/// (mirrors `streaming.rs`'s own `EMPTY_SHA256`; kept local so this client-side signer needs
/// nothing from the fix's production surface).
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// The `x-amz-checksum-*` algorithms a stock SDK sends — a TEST-LOCAL enum (not the fix's
/// `wyrd_gateway_s3::checksum::ChecksumAlgorithm`), so this file compiles against the base
/// tree and the RED stays a wire-level 403 rather than a compile error.
#[derive(Debug, Clone, Copy)]
enum Algo {
    Crc32,
    Crc32c,
    Sha256,
}

/// A gateway with a deliberately small chunk size, so a modest object still spans several
/// store chunks over the streaming wire path (mirrors `s3_http_wire.rs::build_gateway`).
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

/// Start the S3 gateway on an ephemeral loopback port and return its address.
async fn start_gateway() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = build_gateway(dir.path());
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
        raw = &raw[size + 2..];
    }
    out
}

/// The `x-amz-checksum-<algo>` trailer header name for `algorithm`.
fn trailer_name(algorithm: Algo) -> &'static str {
    match algorithm {
        Algo::Crc32 => "x-amz-checksum-crc32",
        Algo::Crc32c => "x-amz-checksum-crc32c",
        Algo::Sha256 => "x-amz-checksum-sha256",
    }
}

/// CRC-32 (IEEE 802.3, reflected poly `0xEDB88320`), bit-by-bit — a DELIBERATELY different
/// implementation shape than the gateway's own table-driven one (`wyrd_gateway_s3::checksum`),
/// so a shared bug there could not hide behind a self-referential test. Independently
/// cross-checked against the standard `"123456789"` → `0xCBF43926` check value
/// (`crc_reference_check_values`).
fn crc32_ieee(data: &[u8]) -> u32 {
    crc32_reflected(0xEDB8_8320, data)
}

/// CRC-32C (Castagnoli, reflected poly `0x82F63B78`), bit-by-bit — kept IN-TREE in the test
/// itself rather than pulling the `crc32c` dev-crate, so this file depends on **nothing the
/// `C4-verify` oracle reverts with production**: the RED leg then reaches the gateway and
/// sees a real 403, instead of failing to compile on a missing dependency. Still an
/// INDEPENDENT oracle — the gateway computes CRC-32C via the `crc32c` crate, a different
/// implementation — cross-checked against `"123456789"` → `0xE3069283`
/// (`crc_reference_check_values`).
fn crc32c(data: &[u8]) -> u32 {
    crc32_reflected(0x82F6_3B78, data)
}

/// The shared reflected-CRC-32 inner loop for a given reflected polynomial.
fn crc32_reflected(poly: u32, data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (poly & mask);
        }
    }
    !crc
}

/// The base64-encoded `x-amz-checksum-<algo>` trailer value for `data` under `algorithm`.
/// Each checksum is computed by a test-local, independent implementation (`crc32_ieee` /
/// `crc32c` bit-by-bit, `wyrd_gateway_s3::crypto::sha256`) — distinct from the gateway's own
/// table-driven CRCs / `crc32c`-crate path — so a shared bug there could not manufacture a
/// false green.
fn checksum_trailer_value(algorithm: Algo, data: &[u8]) -> String {
    let raw: Vec<u8> = match algorithm {
        Algo::Crc32 => crc32_ieee(data).to_be_bytes().to_vec(),
        Algo::Crc32c => crc32c(data).to_be_bytes().to_vec(),
        Algo::Sha256 => sha256(data).to_vec(),
    };
    base64_encode(&raw)
}

/// The published check values for the two CRC-32 variants (`"123456789"` — the canonical
/// CRC test vector), so this file's independent implementations are anchored to a known
/// answer, not merely to agreement with the gateway they exercise.
#[test]
fn crc_reference_check_values() {
    assert_eq!(
        crc32_ieee(b"123456789"),
        0xCBF4_3926,
        "CRC-32/IEEE check value"
    );
    assert_eq!(
        crc32c(b"123456789"),
        0xE306_9283,
        "CRC-32C/Castagnoli check value"
    );
}

/// Standard base64 (RFC 4648, `+`/`/`, `=`-padded) — used only to build test wire bytes.
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
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

/// The material a real SDK's chunk/trailer signer holds — assembled here from PUBLIC base
/// primitives (`signing_key_for` + the seed request's own `Signature=`), NOT from the fix's
/// `StreamingContext`, so the file compiles against the base tree.
struct SeedCtx {
    /// The verified seed signature — the `previous-signature` of the first chunk.
    seed_signature: String,
    /// The SigV4 signing key (`HMAC` ladder over the secret), reused per chunk / trailer.
    signing_key: [u8; 32],
    /// The request `x-amz-date` (`YYYYMMDDThhmmssZ`) — line 2 of the chunk string-to-sign.
    date_time: String,
    /// The credential scope `date/region/service/aws4_request` — line 3 of the string-to-sign.
    scope: String,
    /// Whether the data chunks (and a trailer) are per-chunk signed.
    signed: bool,
}

/// The SigV4 **chunk signature** for `chunk_data`, chained from `previous_signature` — the
/// CLIENT side of AWS "Transferring Payload in Multiple Chunks". Byte-for-byte the
/// string-to-sign `streaming::sign_chunk` (the verifier) recomputes, but written here from
/// the public `crypto` primitives so the test needs nothing from the fix.
fn client_sign_chunk(ctx: &SeedCtx, previous_signature: &str, chunk_data: &[u8]) -> String {
    let data_hash = hex(&sha256(chunk_data));
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        ctx.date_time, ctx.scope, previous_signature, EMPTY_SHA256, data_hash
    );
    hex(&hmac_sha256(&ctx.signing_key, string_to_sign.as_bytes()))
}

/// The SigV4 **trailer signature** for the declared trailer headers, chained from the
/// terminating zero-length chunk's own signature — the CLIENT side of AWS "Signing the
/// trailing header". Byte-for-byte the string-to-sign `streaming::sign_trailer` recomputes.
fn client_sign_trailer(ctx: &SeedCtx, previous_signature: &str, canonical_headers: &str) -> String {
    let block_hash = hex(&sha256(canonical_headers.as_bytes()));
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
        ctx.date_time, ctx.scope, previous_signature, block_hash
    );
    hex(&hmac_sha256(&ctx.signing_key, string_to_sign.as_bytes()))
}

/// Sign the SEED request for a `-TRAILER` streaming PUT, exactly as a real SDK does: the
/// seed `Authorization` covers the sentinel (the body streams, not available whole at
/// signing time) and the request carries the trailer-declaration headers
/// (`x-amz-trailer`, optionally `x-amz-decoded-content-length`) — mirrors
/// `s3_http_wire.rs`'s `streaming_interop::streaming_seed`, extended with the trailer
/// declaration this brief (#505) adds.
fn streaming_seed(
    sentinel: &str,
    signed: bool,
    path: &str,
    host: &str,
    algorithm: Algo,
    decoded_content_length: Option<u64>,
) -> (Vec<(String, String)>, SeedCtx) {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let amz_date = format_amz_date(SystemTime::now());
    let seed = sign_with_payload_hash(
        "PUT", path, "", host, &amz_date, sentinel, &creds, REGION, "s3",
    );
    let seed_signature = seed
        .authorization
        .rsplit("Signature=")
        .next()
        .expect("authorization carries a Signature=")
        .to_string();
    let name = trailer_name(algorithm).to_string();
    let ctx = SeedCtx {
        seed_signature,
        signing_key: signing_key_for(SECRET_KEY, &amz_date[..8], REGION, "s3"),
        date_time: amz_date.clone(),
        scope: format!("{}/{REGION}/s3/aws4_request", &amz_date[..8]),
        signed,
    };
    let mut headers = vec![
        ("authorization".to_string(), seed.authorization),
        ("x-amz-date".to_string(), seed.amz_date),
        ("x-amz-content-sha256".to_string(), seed.content_sha256),
        ("content-encoding".to_string(), "aws-chunked".to_string()),
        ("x-amz-trailer".to_string(), name),
    ];
    if let Some(len) = decoded_content_length {
        headers.push(("x-amz-decoded-content-length".to_string(), len.to_string()));
    }
    (headers, ctx)
}

/// Frame `chunks` + a ONE-header checksum trailer as a real SDK checksum-trailer PUT does:
/// data chunks (signed per `ctx.signed`), the terminating zero-length chunk
/// (`0[;chunk-signature=…]\r\n`) directly followed by `<name>:<value>\r\n` and, for a
/// signed `ctx`, a computed `x-amz-trailer-signature:…\r\n`, closed by the blank-line
/// terminator — exactly the shape `streaming.rs`'s `Decoder::consume_trailer` expects.
fn frame_trailer_body(
    ctx: &SeedCtx,
    chunks: &[&[u8]],
    trailer_name: &str,
    trailer_value: &str,
) -> Vec<u8> {
    let mut out = Vec::new();
    let mut prev = ctx.seed_signature.clone();
    for data in chunks {
        if ctx.signed {
            let sig = client_sign_chunk(ctx, &prev, data);
            out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", data.len()).as_bytes());
            prev = sig;
        } else {
            out.extend_from_slice(format!("{:x}\r\n", data.len()).as_bytes());
        }
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
    }
    if ctx.signed {
        prev = client_sign_chunk(ctx, &prev, b"");
        out.extend_from_slice(format!("0;chunk-signature={prev}\r\n").as_bytes());
    } else {
        out.extend_from_slice(b"0\r\n");
    }
    out.extend_from_slice(format!("{trailer_name}:{trailer_value}\r\n").as_bytes());
    if ctx.signed {
        let canonical = format!("{trailer_name}:{trailer_value}\n");
        let trailer_sig = client_sign_trailer(ctx, &prev, &canonical);
        out.extend_from_slice(format!("x-amz-trailer-signature:{trailer_sig}\r\n").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// A default-configured stock SDK's actual default: `STREAMING-UNSIGNED-PAYLOAD-TRAILER`,
/// checksum-only integrity (no per-chunk signing), for EACH of the minimum accepted
/// algorithms (brief #505: "at minimum crc32, crc32c, and sha256"). Each round-trips
/// byte-identical through GET.
#[tokio::test]
async fn unsigned_trailer_put_accepts_the_minimum_checksum_algorithm_set() {
    for algorithm in [Algo::Crc32, Algo::Crc32c, Algo::Sha256] {
        let (addr, _dir) = start_gateway().await;
        let host = addr.to_string();
        let path = "/wyrd-bucket/unsigned-trailer-object";
        let object: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();

        let (headers, ctx) = streaming_seed(
            "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
            false,
            path,
            &host,
            algorithm,
            None,
        );
        let value = checksum_trailer_value(algorithm, &object);
        let framed = frame_trailer_body(
            &ctx,
            &[&object[..1000], &object[1000..2500], &object[2500..]],
            trailer_name(algorithm),
            &value,
        );

        let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
        assert_eq!(
            status, 200,
            "a default-configured STREAMING-UNSIGNED-PAYLOAD-TRAILER PUT ({algorithm:?}) \
             must be accepted (was 403 pre-#505)"
        );

        let (status, body) = send(
            addr,
            "GET",
            path,
            &signed_headers("GET", path, &host, b""),
            b"",
        )
        .await;
        assert_eq!(
            status, 200,
            "GET of the trailer-streamed object must succeed"
        );
        assert_eq!(
            body, object,
            "the trailer-streamed object ({algorithm:?}) must round-trip byte-identical"
        );
    }
}

/// The SIGNED trailer sentinel (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`): valid
/// chained per-chunk signatures AND a valid trailer signature, plus the declared checksum —
/// round-trips byte-identical.
#[tokio::test]
async fn signed_trailer_put_with_valid_chunk_and_trailer_signatures_round_trips() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/signed-trailer-object";
    let object: Vec<u8> = (0..3000u32).map(|i| (i % 197) as u8).collect();

    let (headers, ctx) = streaming_seed(
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        true,
        path,
        &host,
        Algo::Crc32,
        Some(object.len() as u64),
    );
    let value = checksum_trailer_value(Algo::Crc32, &object);
    let framed = frame_trailer_body(
        &ctx,
        &[&object[..1500], &object[1500..]],
        trailer_name(Algo::Crc32),
        &value,
    );

    let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 200,
        "a signed STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER PUT with valid chunk + \
         trailer signatures must be accepted (was 403 pre-#505)"
    );

    let (status, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, object, "must round-trip byte-identical");
}

/// Fail-closed edge: a forged trailer signature (the declared trailer was tampered without
/// re-signing) is refused — auth, not a checksum mismatch — and stores nothing.
#[tokio::test]
async fn a_forged_trailer_signature_is_refused_and_stores_nothing() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/forged-trailer-signature";
    let object = vec![b'q'; 500];

    let (headers, ctx) = streaming_seed(
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        true,
        path,
        &host,
        Algo::Crc32,
        None,
    );
    let value = checksum_trailer_value(Algo::Crc32, &object);
    let mut framed = frame_trailer_body(&ctx, &[&object], trailer_name(Algo::Crc32), &value);
    let needle = b"x-amz-trailer-signature:";
    let sig_at = framed
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("trailer signature line")
        + needle.len();
    framed[sig_at] = if framed[sig_at] == b'0' { b'1' } else { b'0' };

    let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 403,
        "a forged trailer signature must be refused (403 SignatureDoesNotMatch)"
    );

    let (status, _) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 404, "a refused PUT must store nothing");
}

/// Fail-closed edge (brief #505): a declared trailer checksum that does not match what was
/// ACTUALLY streamed is refused — never consumed-and-trusted — and stores nothing. The
/// unsigned variant isolates the checksum comparison specifically (no trailer signature to
/// catch it first). Distinguishes genuinely from the pre-fix blanket 403: this is a 400.
#[tokio::test]
async fn a_tampered_trailer_checksum_is_refused_and_stores_nothing() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/tampered-checksum";
    let object = vec![b'z'; 800];

    let (headers, ctx) = streaming_seed(
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        false,
        path,
        &host,
        Algo::Crc32,
        None,
    );
    // A checksum for bytes OTHER than what is actually streamed.
    let wrong_value = checksum_trailer_value(Algo::Crc32, b"not the real object");
    let framed = frame_trailer_body(&ctx, &[&object], trailer_name(Algo::Crc32), &wrong_value);

    let (status, body) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "a tampered/wrong declared checksum must be refused as 400 (was blanket 403 pre-#505)"
    );
    assert!(
        String::from_utf8_lossy(&body).contains("BadDigest"),
        "the S3 error code must name the checksum mismatch: {}",
        String::from_utf8_lossy(&body)
    );

    let (status, _) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 404, "a refused PUT must store nothing");
}

/// Fail-closed edge (brief #505 scope: `x-amz-checksum-crc64nvme` is out of scope): an
/// unrecognised checksum algorithm is refused BEFORE ADMISSION (before any body is read),
/// never half-accepted. Distinguishes genuinely from the pre-fix blanket refusal by the
/// error MESSAGE, since both are 403 `AuthorizationHeaderMalformed` — pre-fix the message
/// names the sentinel, never `crc64nvme` (the trailer header is never even parsed).
#[tokio::test]
async fn an_unrecognised_checksum_algorithm_is_refused_before_admission() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/unknown-checksum-algo";
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let amz_date = format_amz_date(SystemTime::now());
    let seed = sign_with_payload_hash(
        "PUT",
        path,
        "",
        &host,
        &amz_date,
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        &creds,
        REGION,
        "s3",
    );
    let headers = vec![
        ("authorization".to_string(), seed.authorization),
        ("x-amz-date".to_string(), seed.amz_date),
        ("x-amz-content-sha256".to_string(), seed.content_sha256),
        ("content-encoding".to_string(), "aws-chunked".to_string()),
        (
            "x-amz-trailer".to_string(),
            "x-amz-checksum-crc64nvme".to_string(),
        ),
    ];
    // Refused before any body is read — an arbitrary (never-consumed) body still refuses.
    let (status, body) = send(addr, "PUT", path, &headers, b"0\r\n\r\n").await;
    assert_eq!(status, 403);
    assert!(
        String::from_utf8_lossy(&body).contains("crc64nvme"),
        "the refusal must name the unrecognised algorithm (proves the NEW trailer-parsing \
         path ran, not the pre-#505 blanket sentinel refusal): {}",
        String::from_utf8_lossy(&body)
    );
}

/// Fail-closed edge (brief #505): a declared checksum value that is not valid base64 is
/// refused as 400, never half-parsed.
#[tokio::test]
async fn a_non_base64_trailer_checksum_is_refused_as_bad_request() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/bad-base64-checksum";
    let object = vec![b'z'; 200];

    let (headers, ctx) = streaming_seed(
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        false,
        path,
        &host,
        Algo::Crc32,
        None,
    );
    let framed = frame_trailer_body(
        &ctx,
        &[&object],
        trailer_name(Algo::Crc32),
        "not valid base64!!",
    );

    let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "a malformed base64 checksum value must be a 400 (was blanket 403 pre-#505)"
    );
}

/// A NON-canonical base64 for a 4-byte CRC-32 checksum: it decodes (under a naive decoder)
/// to the SAME bytes as the canonical form because the two dropped pad bits are set, so a
/// decoder that fails to enforce canonical padding would accept it as a valid checksum.
/// Used to prove the wire path refuses non-canonical trailer values (issue #505, C3/T5).
fn non_canonical_base64_crc32(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let raw = crc32_ieee(data).to_be_bytes();
    // 4 bytes -> group0 = raw[0..3] (4 chars), group1 = raw[3] alone (2 chars + "==").
    let n0 = (u32::from(raw[0]) << 16) | (u32::from(raw[1]) << 8) | u32::from(raw[2]);
    let c0 = ALPHABET[((n0 >> 18) & 0x3f) as usize] as char;
    let c1 = ALPHABET[((n0 >> 12) & 0x3f) as usize] as char;
    let c2 = ALPHABET[((n0 >> 6) & 0x3f) as usize] as char;
    let c3 = ALPHABET[(n0 & 0x3f) as usize] as char;
    let c4 = ALPHABET[(raw[3] >> 2) as usize] as char;
    // The last significant char carries raw[3]'s low 2 bits in its top-2 position; its low
    // 4 bits are pad bits that MUST be zero for a canonical encoding. Set one of them: the
    // decode is unchanged (same checksum bytes) but the encoding is now non-canonical.
    let non_canonical_v5 = ((raw[3] & 0x03) << 4) | 0x01;
    let c5 = ALPHABET[non_canonical_v5 as usize] as char;
    format!("{c0}{c1}{c2}{c3}{c4}{c5}==")
}

/// Fail-closed edge (brief #505): a checksum value that is well-formed base64 *arithmetic*
/// but NON-canonical (padding bits not zero) must be refused as 400 — never accepted by
/// decoding it as if canonical. Otherwise two distinct strings map to the same checksum and
/// a tampered/forged declared value could masquerade as valid (the silent-accept the
/// fail-closed contract forbids).
#[tokio::test]
async fn a_non_canonical_base64_trailer_checksum_is_refused_as_bad_request() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/non-canonical-base64-checksum";
    let object = vec![b'q'; 200];

    let (headers, ctx) = streaming_seed(
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        false,
        path,
        &host,
        Algo::Crc32,
        None,
    );
    let non_canonical = non_canonical_base64_crc32(&object);
    // Sanity: it differs textually from the canonical value it would decode to.
    assert_ne!(
        non_canonical,
        checksum_trailer_value(Algo::Crc32, &object),
        "the non-canonical value must differ from the canonical one for this test to bind"
    );
    let framed = frame_trailer_body(&ctx, &[&object], trailer_name(Algo::Crc32), &non_canonical);

    let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "a non-canonical base64 checksum value must be a 400, never decoded as if canonical"
    );
}

/// Fail-closed edge (brief #505): a trailer name on the wire other than the one declared in
/// `x-amz-trailer` is refused as 400 — never silently consumed under the declared name.
#[tokio::test]
async fn an_undeclared_trailer_name_is_refused_as_bad_request() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/undeclared-trailer-name";
    let object = vec![b'z'; 200];

    let (headers, ctx) = streaming_seed(
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        false,
        path,
        &host,
        Algo::Crc32,
        None,
    );
    // Declares crc32 but the wire sends crc32c instead.
    let value = checksum_trailer_value(Algo::Crc32c, &object);
    let framed = frame_trailer_body(&ctx, &[&object], "x-amz-checksum-crc32c", &value);

    let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "a trailer name not declared in x-amz-trailer must be a 400 (was blanket 403 pre-#505)"
    );
}

/// Fail-closed edge (brief #505): bytes left over after the trailer block's closing blank
/// line are refused as 400, never silently dropped.
#[tokio::test]
async fn garbage_after_the_trailer_block_is_refused_as_bad_request() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/trailer-garbage";
    let object = vec![b'z'; 200];

    let (headers, ctx) = streaming_seed(
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        false,
        path,
        &host,
        Algo::Crc32,
        None,
    );
    let value = checksum_trailer_value(Algo::Crc32, &object);
    let mut framed = frame_trailer_body(&ctx, &[&object], trailer_name(Algo::Crc32), &value);
    framed.extend_from_slice(b"GARBAGE-AFTER-THE-TRAILER-BLOCK");

    let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "bytes after the trailer block must be a 400, never a silent accept (was blanket \
         403 pre-#505)"
    );
}

/// Fail-closed edge (brief #505 scope item (b)): a declared `x-amz-decoded-content-length`
/// that does not match the actually-decoded byte count is refused.
#[tokio::test]
async fn a_decoded_content_length_mismatch_is_refused() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/decoded-length-mismatch";
    let object = vec![b'z'; 200];

    let (headers, ctx) = streaming_seed(
        "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
        false,
        path,
        &host,
        Algo::Crc32,
        Some(9999), // deliberately wrong
    );
    let value = checksum_trailer_value(Algo::Crc32, &object);
    let framed = frame_trailer_body(&ctx, &[&object], trailer_name(Algo::Crc32), &value);

    let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
    assert_eq!(
        status, 400,
        "an x-amz-decoded-content-length mismatch must be a 400 (was blanket 403 pre-#505)"
    );
}
