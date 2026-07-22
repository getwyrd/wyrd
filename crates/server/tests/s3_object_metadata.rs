//! Object metadata on the S3 wire surface (ADR-0047, issue #503), exercised over a real
//! loopback listener — the same in-process redb + fs-tempdir stack `s3_http_wire.rs`
//! drives. A signed `PutObject` that declares a `Content-Type` must:
//!
//! * answer with an **`ETag`** header (S3-quoted) — the content digest, so a client can
//!   validate integrity without a follow-up read;
//! * on a subsequent `GetObject`, return the **same** `ETag`, the **`Content-Type` the PUT
//!   declared** (not the hardcoded `application/octet-stream`), and a valid RFC-7231
//!   **`Last-Modified`**.
//!
//! The metadata round-trips through a real `MetadataStore` commit (it is stored on the
//! `InodeRecord`, not synthesized at the wire layer): the GET reads it back off the record
//! the PUT committed. The ETag is asserted to equal the **SHA-256 of the object bytes**
//! computed independently in this test, so the oracle is not self-referential — a wire
//! layer that echoed an arbitrary string would fail.
//!
//! RED on the base: the PUT response carries no `ETag` header and GET's `content-type` is
//! the hardcoded `application/octet-stream` regardless of what the PUT sent. GREEN with the
//! metadata model in place.

// wall-clock exempt (test crate): SigV4 request dates / lease stamps against a
// live in-process server use real wall time; nothing here mixes clock sources
// within one asserted lifecycle (#619).
// File scope (not per-site) is deliberate here: a test crate never
// ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use sha2::{Digest, Sha256};
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

/// A gateway with a small chunk size so a modest object still spans several chunks over
/// the streaming wire path (matching the `s3_http_wire.rs` harness).
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

/// Start the S3 gateway on an ephemeral loopback port. The `TempDir` is returned so the
/// caller keeps the chunk store alive for the test.
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

/// The SigV4 headers for a request. `content-type` is deliberately **not** signed (S3
/// SigV4 verifies only the declared `SignedHeaders`), so it rides as an ordinary header
/// the gateway reads off the request head — exactly as a stock client sends it.
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

/// Send one HTTP/1.1 request over a fresh connection and return `(status, head, body)` —
/// the raw head block so the caller can assert on response headers (`ETag`,
/// `Content-Type`, `Last-Modified`), which a status+body parse would discard.
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

/// The lowercase-hex SHA-256 of `bytes` — the independent oracle for the ETag (ADR-0047:
/// the ETag is the content digest as an opaque change-token).
fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// A strict RFC-7231 IMF-fixdate validator: `Sun, 06 Nov 1994 08:49:37 GMT`. Checks the
/// exact shape and field ranges rather than a substring, so a malformed or empty value
/// fails (a green must mean a genuinely well-formed `Last-Modified`).
fn is_imf_fixdate(value: &str) -> bool {
    const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    // "Www, DD Mmm YYYY HH:MM:SS GMT" — 29 characters.
    let bytes = value.as_bytes();
    if bytes.len() != 29 {
        return false;
    }
    let weekday = &value[0..3];
    if !WEEKDAYS.contains(&weekday) || &value[3..5] != ", " {
        return false;
    }
    let day = &value[5..7];
    if value.as_bytes()[7] != b' ' {
        return false;
    }
    let month = &value[8..11];
    if !MONTHS.contains(&month) || value.as_bytes()[11] != b' ' {
        return false;
    }
    let year = &value[12..16];
    let time = &value[17..25];
    if value.as_bytes()[16] != b' ' || &value[25..] != " GMT" {
        return false;
    }
    let day_ok = day.chars().all(|c| c.is_ascii_digit())
        && (1..=31).contains(&day.parse::<u32>().unwrap_or(0));
    let year_ok = year.chars().all(|c| c.is_ascii_digit());
    let (h, rest) = time.split_at(2);
    let colon1 = &rest[0..1];
    let (m, rest2) = rest[1..].split_at(2);
    let colon2 = &rest2[0..1];
    let s = &rest2[1..];
    let time_ok = colon1 == ":"
        && colon2 == ":"
        && h.parse::<u32>().map(|v| v < 24).unwrap_or(false)
        && m.parse::<u32>().map(|v| v < 60).unwrap_or(false)
        && s.parse::<u32>().map(|v| v < 60).unwrap_or(false);
    day_ok && year_ok && time_ok
}

/// PUT declaring a `Content-Type` answers with an `ETag`; the subsequent GET returns the
/// **same** `ETag`, that `Content-Type` verbatim, and a valid RFC-7231 `Last-Modified` —
/// all read back off the record the PUT committed to the real `MetadataStore` (ADR-0047).
#[tokio::test]
async fn put_answers_etag_and_get_round_trips_content_type_and_last_modified() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/metadata-object";
    let declared_content_type = "text/plain; charset=utf-8";
    // Longer than the 8-byte chunk size, so the object spans several chunks on the wire.
    let object = b"an S3 object that carries an ETag, a content type, and a modified time".to_vec();
    let expected_etag = format!("\"{}\"", sha256_hex(&object));

    // PUT with a client-declared Content-Type (unsigned header — SigV4 ignores it).
    let mut put_headers = signed_headers("PUT", path, &host, &object);
    put_headers.push((
        "content-type".to_string(),
        declared_content_type.to_string(),
    ));
    let (status, put_head, _) = send(addr, "PUT", path, &put_headers, &object).await;
    assert_eq!(status, 200, "signed PUT must be accepted");

    let put_etag = header_value(&put_head, "etag")
        .expect("a PutObject response must carry an ETag header (ADR-0047); pre-fix it has none");
    assert_eq!(
        put_etag, expected_etag,
        "the PUT ETag must be the quoted lowercase-hex SHA-256 of the object bytes"
    );

    // GET must return the SAME ETag, the declared content type, and a Last-Modified.
    let (status, get_head, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200, "signed GET must be accepted");
    assert_eq!(body, object, "GET must return the PUT bytes byte-identical");

    let get_etag = header_value(&get_head, "etag")
        .expect("a GetObject response must carry an ETag header (ADR-0047)");
    assert_eq!(
        get_etag, put_etag,
        "GET must return the SAME ETag the PUT committed (a stable content change-token)"
    );

    let get_content_type = header_value(&get_head, "content-type")
        .expect("a GetObject response must declare a content type");
    assert_eq!(
        get_content_type, declared_content_type,
        "GET must round-trip the Content-Type the PUT declared, not the hardcoded \
         application/octet-stream (pre-fix)"
    );

    let last_modified = header_value(&get_head, "last-modified")
        .expect("a GetObject response must carry Last-Modified (ADR-0047)");
    assert!(
        is_imf_fixdate(&last_modified),
        "Last-Modified must be a valid RFC-7231 IMF-fixdate, got {last_modified:?}"
    );
}

/// Overwrite **freshness** (ADR-0047): a second PUT of DIFFERENT content to the same key is
/// a fresh content publication — it must stamp a NEW `ETag` (the digest of the new bytes)
/// and round-trip the NEW `Content-Type`. A subsequent GET must serve the fresh metadata,
/// never the first version's stale `ETag`/content type for rewritten content.
///
/// This drives the production **overwrite** commit (`commit_chunk_map_superseding`, the
/// content-publication half of the "which commits set metadata" split) over the real wire
/// path — the single-PUT round-trip above cannot catch a stale-ETag regression (replacing
/// the overwrite's `meta.etag` with `prior.etag` would still pass one PUT + one GET). The
/// ETag oracle is an independent SHA-256 of each body, so an echo of the prior value fails.
///
/// RED on the base: the PUT response carries no `ETag` at all, and GET's content-type is the
/// hardcoded `application/octet-stream`. GREEN with the metadata model, and — crucially —
/// only GREEN if the overwrite RE-STAMPS rather than preserving the first publication.
#[tokio::test]
async fn a_second_put_of_new_content_stamps_a_fresh_etag_and_content_type() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/rewritten-object";

    // First publication: original bytes, one content type.
    let first = b"the original object body, version one, longer than one chunk".to_vec();
    let first_content_type = "text/plain; charset=utf-8";
    let expected_etag_1 = format!("\"{}\"", sha256_hex(&first));
    let mut put1_headers = signed_headers("PUT", path, &host, &first);
    put1_headers.push(("content-type".to_string(), first_content_type.to_string()));
    let (status, put1_head, _) = send(addr, "PUT", path, &put1_headers, &first).await;
    assert_eq!(status, 200, "the first PUT is accepted");
    let etag_1 = header_value(&put1_head, "etag")
        .expect("the first PutObject response must carry an ETag header (ADR-0047)");
    assert_eq!(
        etag_1, expected_etag_1,
        "the first ETag is the quoted SHA-256 of the original bytes"
    );

    // Overwrite: DIFFERENT bytes AND a DIFFERENT content type.
    let second =
        b"a completely different body, version two, also longer than a single 8-byte chunk"
            .to_vec();
    let second_content_type = "application/json";
    let expected_etag_2 = format!("\"{}\"", sha256_hex(&second));
    assert_ne!(
        expected_etag_1, expected_etag_2,
        "the two bodies differ, so their SHA-256 change-tokens must differ"
    );
    let mut put2_headers = signed_headers("PUT", path, &host, &second);
    put2_headers.push(("content-type".to_string(), second_content_type.to_string()));
    let (status, put2_head, _) = send(addr, "PUT", path, &put2_headers, &second).await;
    assert_eq!(status, 200, "the overwrite PUT is accepted");
    let etag_2 = header_value(&put2_head, "etag")
        .expect("the overwrite PutObject response must carry an ETag header (ADR-0047)");
    assert_eq!(
        etag_2, expected_etag_2,
        "the overwrite stamps a FRESH ETag — the digest of the NEW bytes, not the prior one"
    );
    assert_ne!(
        etag_2, etag_1,
        "rewritten content must not reuse the prior version's ETag (ADR-0047 freshness)"
    );

    // GET returns the OVERWRITE's content, ETag, and content type — never the stale first.
    let (status, get_head, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200, "the GET is accepted");
    assert_eq!(body, second, "GET returns the overwritten bytes");
    assert_eq!(
        header_value(&get_head, "etag").expect("GET must carry an ETag"),
        etag_2,
        "GET serves the FRESH ETag the overwrite committed, not the stale first one"
    );
    assert_eq!(
        header_value(&get_head, "content-type").expect("GET must declare a content type"),
        second_content_type,
        "GET round-trips the overwrite's Content-Type, not the first PUT's"
    );
    let last_modified =
        header_value(&get_head, "last-modified").expect("GET must carry Last-Modified");
    assert!(
        is_imf_fixdate(&last_modified),
        "the overwrite stamps a valid RFC-7231 Last-Modified, got {last_modified:?}"
    );
}
