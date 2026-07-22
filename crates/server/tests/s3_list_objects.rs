//! ListObjectsV2 (`GET /bucket?list-type=2`) and the v1 ListObjects compat shim
//! (`GET /bucket`) on the S3 wire surface (issue #507, ADR-0046), exercised over a real
//! loopback listener with a **stock `aws-sdk-s3` client** — the same in-process
//! redb + fs-tempdir + `MemCoordination` stack `s3_http_wire.rs` / `s3_object_metadata.rs`
//! drive, and the SDK's own `list_objects_v2` continuation-token chaining as the pagination
//! oracle (so a green here is not self-referential about paging).
//!
//! The success criterion (brief #507), all driven through the shipping HTTP surface:
//!
//! * a bucket-scoped `GET /bucket?list-type=2` returns a `<ListBucketResult>` whose
//!   `<Contents>` are exactly the bucket's keys in lexicographic order, with correct `Size`
//!   and `ETag` (the ETag asserted against an **independently-computed** SHA-256, so the
//!   oracle is not self-referential);
//! * `prefix` filters; `delimiter=/` rolls nested keys into `<CommonPrefixes>`;
//! * with `max-keys` below the key count, pages chain via
//!   `IsTruncated`/`NextContinuationToken`/`continuation-token` until every key is returned
//!   **exactly once**;
//! * a bucket with **no marker record** answers `404 NoSuchBucket`; an **empty** bucket WITH
//!   a marker lists as an empty `200` (not 404);
//! * an XML-special key (`&`, `<`, `>`, quotes) round-trips correctly escaped;
//! * an invalid `continuation-token` answers `400 InvalidArgument`;
//! * a v1 `GET /bucket` returns a `Marker`-based `<ListBucketResult>`;
//! * `encoding-type=url` (issue #507 Delta 1) URL-encodes the returned
//!   `Key`/`Prefix`/`Delimiter`/`CommonPrefixes` and the resume echoes
//!   (`StartAfter`/`Marker`/`NextMarker`), emits `<EncodingType>url</EncodingType>`, and
//!   leaves the opaque continuation tokens untouched (a returned token resumes verbatim); an
//!   `encoding-type` value other than `url` answers `400 InvalidArgument`;
//! * a rollup is filtered on the common prefix ITSELF: a `CommonPrefixes` entry not
//!   lexicographically greater than the client's `start-after` / v1 `marker` is dropped
//!   whole — the AWS-documented rule for both listing forms;
//! * a `continuation-token` overrides a co-sent `start-after` (AWS precedence).
//!
//! The encoding-type / v1 shim / malformed-param assertions drive **raw signed HTTP** (the SDK
//! paginator does not inject `encoding-type` by default, unlike botocore, and raw HTTP asserts
//! the exact wire bytes with no SDK decode layer in between). SigV4 trap: a listing request
//! always carries a query, so the canonical (sorted, encoded) query is signed — an EMPTY-query
//! signature (the peer object-metadata harness's default) would 403.
//!
//! The `bucket:{name}` existence marker (ADR-0046) is seeded **directly** on the
//! `MetadataStore` — the record CreateBucket (#511) will write, which this issue only reads
//! — committed BEFORE the store is moved into `Gateway::new` (`Gateway` owns it privately).
//! The marker is written as **raw** key/value bytes, so this test imports **no new production
//! symbol**: on the wave base the bucket-only GET is rejected with `400 InvalidRequest`
//! before any listing logic (`split_bucket_key` returns `None`), so every assertion here
//! fails by **assertion**, not a compile error (C4-verify red leg).

// wall-clock exempt (test crate): SigV4 request dates / lease stamps against a
// live in-process server use real wall time; nothing here mixes clock sources
// within one asserted lifecycle (#619).
// File scope (not per-site) is deliberate here: a test crate never
// ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use aws_sdk_s3::config::{Credentials as SdkCredentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials as GatewayCredentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::{MetadataStore, WriteBatch};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// Seed a `bucket:{name}` existence marker directly on the store (ADR-0046 decision 1). Raw
/// key/value bytes on purpose — listing only reads the marker for **presence** — so this test
/// imports no new production symbol and the C4-verify red leg fails by assertion. Committed
/// as a blind put BEFORE the store is moved into `Gateway::new`.
async fn seed_bucket(meta: &RedbMetadataStore, name: &str) {
    let key = format!("bucket:{name}").into_bytes();
    let value = format!("{{\"name\":\"{name}\",\"created_millis\":0}}").into_bytes();
    meta.commit(WriteBatch::new().put(key, value))
        .await
        .expect("commit bucket marker");
}

/// Start the S3 gateway on an ephemeral loopback port with each named bucket's marker
/// pre-seeded. The `TempDir` is returned so the caller keeps the chunk store alive.
async fn start_gateway(buckets: &[&str]) -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = RedbMetadataStore::in_memory().expect("redb");
    for b in buckets {
        seed_bucket(&meta, b).await;
    }
    let gateway = Arc::new(
        Gateway::new(
            meta,
            FsChunkStore::open(dir.path()).expect("fs store"),
            MemCoordination::new(),
        )
        .with_chunk_size(8),
    );
    let config = S3Config::new(vec![GatewayCredentials {
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

/// A stock `aws-sdk-s3` client pointed at the loopback gateway — custom endpoint, path-style,
/// static creds, plaintext, retries/stalled-stream protection off for determinism. Mirrors
/// `s3_gateway_cluster.rs::sdk_client`.
fn sdk_client(addr: SocketAddr) -> Client {
    let http_client = aws_smithy_http_client::Builder::new().build_http();
    let config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new(REGION))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(SdkCredentials::new(
            ACCESS_KEY, SECRET_KEY, None, None, "static",
        ))
        .http_client(http_client)
        .force_path_style(true)
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .stalled_stream_protection(aws_sdk_s3::config::StalledStreamProtectionConfig::disabled())
        .build();
    Client::from_conf(config)
}

/// The S3-quoted ETag an object of `body` bytes must carry — an independent lowercase-hex
/// SHA-256, so the listing's ETag oracle is not self-referential (matches
/// `s3_object_metadata.rs::sha256_hex`).
fn expected_etag(body: &[u8]) -> String {
    let digest = Sha256::digest(body);
    let mut hex = String::with_capacity(digest.len() * 2);
    for b in digest {
        hex.push_str(&format!("{b:02x}"));
    }
    format!("\"{hex}\"")
}

/// PUT an object over the wire.
async fn put(client: &Client, bucket: &str, key: &str, body: &[u8]) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body.to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("put {key}: {e:?}"));
}

/// SigV4 headers for a raw request carrying a query string. The peer object-metadata harness
/// signs an EMPTY query (`sign(method, path, "", …)`); a listing request always has a query, so
/// the canonical (sorted, encoded) query MUST be signed or the gateway 403s on signature
/// mismatch (issue #507 Delta 1 SigV4 trap). `sign` canonicalizes `query` internally, so pass
/// the same raw query that rides on the wire.
fn signed_headers_q(method: &str, path: &str, query: &str, host: &str) -> Vec<(String, String)> {
    let creds = GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let amz_date = format_amz_date(SystemTime::now());
    let signed = sign(
        method, path, query, host, &amz_date, b"", &creds, REGION, "s3",
    );
    vec![
        ("authorization".to_string(), signed.authorization),
        ("x-amz-date".to_string(), signed.amz_date),
        ("x-amz-content-sha256".to_string(), signed.content_sha256),
    ]
}

/// Send a raw signed `GET {path}?{query}` and return `(status, body_string)`. Handles both
/// content-length and chunked framing so the caller can assert on the exact response bytes.
async fn get_raw(addr: SocketAddr, path: &str, query: &str) -> (u16, String) {
    let host = addr.to_string();
    let headers = signed_headers_q("GET", path, query, &host);
    let target = format!("{path}?{query}");
    let mut request = format!("GET {target} HTTP/1.1\r\n");
    request.push_str(&format!("host: {host}\r\n"));
    for (name, value) in &headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("content-length: 0\r\n");
    request.push_str("connection: close\r\n\r\n");

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write head");
    stream.flush().await.expect("flush");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let status: u16 = head
        .lines()
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    let body_raw = &raw[split + 4..];
    let is_chunked = head.lines().any(|l| {
        l.to_ascii_lowercase().starts_with("transfer-encoding:")
            && l.to_ascii_lowercase().contains("chunked")
    });
    let body = if is_chunked {
        String::from_utf8_lossy(&dechunk(body_raw)).into_owned()
    } else {
        String::from_utf8_lossy(body_raw).into_owned()
    };
    (status, body)
}

/// Decode an HTTP/1.1 chunked-transfer-encoded body.
fn dechunk(mut raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(line_end) = raw.windows(2).position(|w| w == b"\r\n") {
        let size_str = String::from_utf8_lossy(&raw[..line_end]);
        let size = usize::from_str_radix(size_str.trim(), 16).unwrap_or(0);
        raw = &raw[line_end + 2..];
        if size == 0 || size > raw.len() {
            break;
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..];
    }
    out
}

/// The text between the first `<{tag}>` and `</{tag}>` in `body`, or `None`.
fn element(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].to_string())
}

/// Percent-encode a query-parameter VALUE for the wire (unreserved set literal; everything else
/// `%XX`). Used to carry an opaque continuation token (which may contain `+`/`/`/`=`) back in a
/// resume request without it being misread — the token bytes themselves are unchanged.
fn q_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_returns_sorted_contents_with_size_and_etag() {
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);

    // PUT in a deliberately UNSORTED order; the listing must come back lexicographic.
    let bodies: &[(&str, &[u8])] = &[
        ("gamma.txt", b"gamma-body-3"),
        ("alpha.txt", b"a"),
        ("beta.txt", b"beta-body-two"),
    ];
    for (k, b) in bodies {
        put(&client, "bucket", k, b).await;
    }

    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .send()
        .await
        .expect("list_objects_v2");

    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(
        keys,
        vec!["alpha.txt", "beta.txt", "gamma.txt"],
        "contents must be exactly the bucket's keys in lexicographic order"
    );
    assert_eq!(out.key_count(), Some(3));
    assert_eq!(out.is_truncated(), Some(false));

    // Size and ETag are correct per object (ETag against an independent SHA-256).
    for (k, b) in bodies {
        let obj = out
            .contents()
            .iter()
            .find(|o| o.key() == Some(*k))
            .unwrap_or_else(|| panic!("{k} listed"));
        assert_eq!(obj.size(), Some(b.len() as i64), "size of {k}");
        assert_eq!(obj.e_tag(), Some(expected_etag(b).as_str()), "etag of {k}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_prefix_filters() {
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["docs/a.txt", "docs/b.txt", "images/c.png", "top.txt"] {
        put(&client, "bucket", k, b"x").await;
    }

    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .prefix("docs/")
        .send()
        .await
        .expect("list");
    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["docs/a.txt", "docs/b.txt"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_delimiter_rolls_into_common_prefixes() {
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in [
        "root.txt",
        "photos/2023/a.jpg",
        "photos/2024/b.jpg",
        "photos/2024/c.jpg",
        "zed.txt",
    ] {
        put(&client, "bucket", k, b"x").await;
    }

    // Top-level delimiter: nested `photos/...` roll into the single `photos/` common prefix;
    // the two top-level keys stay as Contents.
    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .delimiter("/")
        .send()
        .await
        .expect("list");
    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["root.txt", "zed.txt"]);
    let cps: Vec<&str> = out
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(cps, vec!["photos/"]);
    // KeyCount is the COMBINED count of Contents + CommonPrefixes.
    assert_eq!(out.key_count(), Some(3));

    // One level deeper: prefix `photos/` + delimiter `/` groups by year.
    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .prefix("photos/")
        .delimiter("/")
        .send()
        .await
        .expect("list");
    assert!(out.contents().is_empty());
    let cps: Vec<&str> = out
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(cps, vec!["photos/2023/", "photos/2024/"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_pagination_chains_until_every_key_once() {
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    let expected: Vec<String> = (0..7).map(|i| format!("key-{i:02}")).collect();
    // PUT reversed so a naive unsorted listing would be wrong.
    for k in expected.iter().rev() {
        put(&client, "bucket", k, b"body").await;
    }

    let mut seen: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut req = client.list_objects_v2().bucket("bucket").max_keys(2);
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let out = req.send().await.expect("list page");
        pages += 1;
        assert!(pages <= 10, "pagination did not terminate");
        for o in out.contents() {
            seen.push(o.key().expect("key").to_string());
        }
        if out.is_truncated() == Some(true) {
            let next = out
                .next_continuation_token()
                .expect("truncated page carries a NextContinuationToken")
                .to_string();
            token = Some(next);
        } else {
            assert!(out.next_continuation_token().is_none());
            break;
        }
    }
    // Every key returned exactly once, in lexicographic order, across the chained pages.
    assert_eq!(seen, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_missing_bucket_is_404_no_such_bucket() {
    // Only "real" has a marker; "ghost" has none.
    let (addr, _dir) = start_gateway(&["real"]).await;
    let client = sdk_client(addr);

    let err = client
        .list_objects_v2()
        .bucket("ghost")
        .send()
        .await
        .expect_err("listing a bucket with no marker must fail");
    let svc = err.into_service_error();
    assert_eq!(
        svc.code(),
        Some("NoSuchBucket"),
        "an absent bucket answers NoSuchBucket, got {svc:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_bucket_with_marker_lists_empty_200() {
    let (addr, _dir) = start_gateway(&["empty"]).await;
    let client = sdk_client(addr);

    let out = client
        .list_objects_v2()
        .bucket("empty")
        .send()
        .await
        .expect("an existing empty bucket lists as an empty 200, not 404");
    assert!(out.contents().is_empty());
    assert_eq!(out.key_count(), Some(0));
    assert_eq!(out.is_truncated(), Some(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_invalid_continuation_token_is_400() {
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    put(&client, "bucket", "a.txt", b"x").await;

    let err = client
        .list_objects_v2()
        .bucket("bucket")
        .continuation_token("!!! not base64 !!!")
        .send()
        .await
        .expect_err("an undecodable continuation-token must be rejected, never restarted");
    let svc = err.into_service_error();
    assert_eq!(
        svc.code(),
        Some("InvalidArgument"),
        "an invalid continuation-token answers 400 InvalidArgument, got {svc:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_xml_special_key_round_trips() {
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    // `&`, `<`, `>`, and both quote kinds — the five predefined XML entities, plus a space.
    let key = "sp ci&al <k>\"q'k\".txt";
    put(&client, "bucket", key, b"payload").await;

    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .send()
        .await
        .expect("list");
    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(
        keys,
        vec![key],
        "an XML-special key must round-trip correctly escaped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v1_returns_marker_based_result() {
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    let expected: Vec<String> = (0..4).map(|i| format!("obj-{i}")).collect();
    for k in &expected {
        put(&client, "bucket", k, b"x").await;
    }

    // The v1 shim: `list_objects` (no `list-type`). A truncated page must carry a
    // `Marker`-based continuation (NextMarker), not a v2 continuation token.
    let first = client
        .list_objects()
        .bucket("bucket")
        .max_keys(2)
        .send()
        .await
        .expect("v1 list");
    let page1: Vec<&str> = first.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(page1, vec!["obj-0", "obj-1"]);
    assert_eq!(first.is_truncated(), Some(true));
    // AWS conformance: a v1 listing WITHOUT a delimiter emits NO `<NextMarker>`; the client
    // resumes from the last returned `<Key>`. (With a delimiter AWS does emit NextMarker — see
    // `list_v1_next_marker_is_common_prefix_and_resumes_without_double_emit`.)
    assert!(
        first.next_marker().is_none(),
        "v1 without a delimiter must not emit NextMarker (AWS resumes from the last Key)"
    );
    let marker = page1.last().expect("page 1 is non-empty").to_string();

    let second = client
        .list_objects()
        .bucket("bucket")
        .max_keys(2)
        .marker(marker)
        .send()
        .await
        .expect("v1 list page 2");
    let page2: Vec<&str> = second.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(page2, vec!["obj-2", "obj-3"]);
    assert_eq!(second.is_truncated(), Some(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v1_next_marker_is_common_prefix_and_resumes_without_double_emit() {
    // With a delimiter set, AWS emits `<NextMarker>` on a truncated v1 listing, and its VALUE
    // is the last RETURNED item — the common prefix `a/`, NOT the group's last raw key `a/2`.
    // A client that stores that `a/` and resends it as `marker` (the documented AWS v1 resume)
    // MUST skip the whole `a/…` group and continue at `b/`, never re-receiving `a/` (issue #507
    // adversary: the previous attempt emitted the raw key and re-emitted the group on resume).
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    // Two keys per group so a common-prefix marker is distinct from any single raw key.
    for k in ["a/1", "a/2", "b/1", "b/2", "c/1"] {
        put(&client, "bucket", k, b"x").await;
    }

    let first = client
        .list_objects()
        .bucket("bucket")
        .delimiter("/")
        .max_keys(1)
        .send()
        .await
        .expect("v1 list with delimiter");
    let cps: Vec<&str> = first
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(cps, vec!["a/"]);
    assert_eq!(first.is_truncated(), Some(true));
    // The VALUE, not merely is_some(): AWS's NextMarker for a rollup is the common prefix.
    assert_eq!(
        first.next_marker(),
        Some("a/"),
        "v1 NextMarker for a delimiter rollup is the common prefix `a/`, not the raw key `a/2`"
    );

    // Resume from the stored last-CommonPrefix `a/` (the AWS-documented v1 resume pattern):
    // the whole `a/…` group is skipped and the listing continues at `b/` — no double-emit.
    let second = client
        .list_objects()
        .bucket("bucket")
        .delimiter("/")
        .max_keys(1)
        .marker("a/")
        .send()
        .await
        .expect("v1 resume from a common-prefix marker");
    let cps2: Vec<&str> = second
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(
        cps2,
        vec!["b/"],
        "resuming from marker=`a/` must skip the a/ group entirely, never re-emitting it"
    );
    assert_eq!(second.next_marker(), Some("b/"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_max_keys_zero_is_empty_and_untruncated() {
    // S3: `max-keys=0` on a NON-empty bucket returns an empty, non-truncated page with NO
    // continuation token — never a truncated page a client cannot resume (issue #507 carry-fwd).
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["a.txt", "b.txt", "c.txt"] {
        put(&client, "bucket", k, b"x").await;
    }

    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .max_keys(0)
        .send()
        .await
        .expect("max-keys=0 is a valid request");
    assert!(out.contents().is_empty(), "max-keys=0 returns no keys");
    assert_eq!(out.key_count(), Some(0));
    assert_eq!(
        out.is_truncated(),
        Some(false),
        "max-keys=0 must NOT be truncated (S3), or a client with no token loops forever"
    );
    assert!(
        out.next_continuation_token().is_none(),
        "a non-truncated page carries no continuation token"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_delimiter_and_max_keys_chain_without_double_emitting_a_prefix() {
    // The centerpiece the previous suite left untested (adversary): the group-consume/resume
    // path UNDER truncation. delimiter=`/` + max-keys=1 over {a/1,a/2,b/1,b/2,c} must page the
    // common prefixes `a/`, `b/` and the key `c` — each exactly once, never re-emitting a
    // CommonPrefix on a later page (the codex finding the seam design cites).
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["a/1", "a/2", "b/1", "b/2", "c"] {
        put(&client, "bucket", k, b"x").await;
    }

    let mut seen_prefixes: Vec<String> = Vec::new();
    let mut seen_keys: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut req = client
            .list_objects_v2()
            .bucket("bucket")
            .delimiter("/")
            .max_keys(1);
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let out = req.send().await.expect("delimiter+max-keys page");
        pages += 1;
        assert!(pages <= 12, "pagination did not terminate");
        for cp in out.common_prefixes() {
            seen_prefixes.push(cp.prefix().expect("prefix").to_string());
        }
        for o in out.contents() {
            seen_keys.push(o.key().expect("key").to_string());
        }
        if out.is_truncated() == Some(true) {
            token = Some(
                out.next_continuation_token()
                    .expect("a truncated page carries a NextContinuationToken")
                    .to_string(),
            );
        } else {
            assert!(out.next_continuation_token().is_none());
            break;
        }
    }
    // Each common prefix appears exactly once across the chained pages — no double-emit.
    assert_eq!(
        seen_prefixes,
        vec!["a/", "b/"],
        "common prefixes must page exactly once each, never re-emitted across pages"
    );
    assert_eq!(seen_keys, vec!["c"], "the sole non-grouped key pages once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_start_after_skips_consumed_keys() {
    // `start-after` sets the first-page resume point: the listing begins strictly AFTER the
    // named key — it is NOT silently ignored (issue #507 carry-forward: a start-after client
    // must not re-receive keys it already consumed).
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    let expected: Vec<String> = (0..5).map(|i| format!("key-{i}")).collect();
    for k in &expected {
        put(&client, "bucket", k, b"x").await;
    }

    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .start_after("key-1")
        .send()
        .await
        .expect("start-after listing");
    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(
        keys,
        vec!["key-2", "key-3", "key-4"],
        "start-after=key-1 must begin strictly after key-1, never re-emitting it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_bucket_versioning_is_501_not_a_listing_document() {
    // A bucket subresource GET (`GET /bucket?versioning`) MUST NOT be silently answered with a
    // `<ListBucketResult>`: a stock client parsing the response expects a
    // `VersioningConfiguration` and dies in its XML decoder if handed a listing (issue #507
    // adversary — `versioning` was missing from the subresource denylist). It must answer a
    // clean `501 NotImplemented`. `get_bucket_versioning()` drives `GET /bucket?versioning`.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    put(&client, "bucket", "a.txt", b"x").await;

    let err = client
        .get_bucket_versioning()
        .bucket("bucket")
        .send()
        .await
        .expect_err("a bucket subresource GET must not be answered with a listing document");
    let svc = err.into_service_error();
    assert_eq!(
        svc.code(),
        Some("NotImplemented"),
        "GET /bucket?versioning answers 501 NotImplemented, got {svc:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_start_after_inside_delimiter_group_drops_the_rollup() {
    // Bucket {a/1, a/2, b} with delimiter=/ and a CLIENT-chosen `start-after=a/1` that lands
    // strictly INSIDE the `a/` group. AWS filters a rollup on the common prefix ITSELF:
    // "`CommonPrefixes` is filtered out from results if it is not lexicographically greater
    // than the `StartAfter` value" (ListObjectsV2 API reference, `delimiter`) — `"a/" ≤ "a/1"`,
    // so the `a/` entry a client explicitly resumed past is never re-delivered:
    // CommonPrefixes=[], Contents=[b].
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["a/1", "a/2", "b"] {
        put(&client, "bucket", k, b"x").await;
    }

    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .delimiter("/")
        .start_after("a/1")
        .send()
        .await
        .expect("start-after inside a delimiter group");
    let cps: Vec<&str> = out
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert!(
        cps.is_empty(),
        "a/ is not lexicographically greater than start-after=a/1, so the rollup is dropped; got {cps:?}"
    );
    assert_eq!(keys, vec!["b"], "only the top-level key b remains");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v1_marker_inside_delimiter_group_drops_the_rollup() {
    // Same rule via the v1 shim: `?delimiter=/&marker=a/1` over {a/1, a/2, b}. The v1 API
    // reference documents the identical filter — "`CommonPrefixes` is filtered out from
    // results if it is not lexicographically greater than the key-marker" — so the `a/`
    // rollup (`"a/" ≤ "a/1"`) is dropped whole: CommonPrefixes=[], Contents=[b].
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["a/1", "a/2", "b"] {
        put(&client, "bucket", k, b"x").await;
    }

    let out = client
        .list_objects()
        .bucket("bucket")
        .delimiter("/")
        .marker("a/1")
        .send()
        .await
        .expect("v1 marker inside a delimiter group");
    let cps: Vec<&str> = out
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert!(
        cps.is_empty(),
        "a/ is not lexicographically greater than marker=a/1, so the rollup is dropped; got {cps:?}"
    );
    assert_eq!(keys, vec!["b"], "only the top-level key b remains");
}

// ---------------------------------------------------------------------------------------------
// Delta 1 — `encoding-type=url` (raw signed HTTP; botocore always sends it, so the stock clients
// this feature targets need it). The SDK paginator does NOT inject `encoding-type` by default,
// so these drive the raw wire and assert the exact encoded response bytes with no SDK decode
// layer in between.
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_encoding_type_url_matches_botocore_oracle() {
    // The exact botocore oracle from the brief: a key `a&b/c d` returns as `a%26b/c%20d`
    // (`&`→`%26`, space→`%20`, `/` stays literal), and the response carries
    // `<EncodingType>url</EncodingType>`.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    put(&client, "bucket", "a&b/c d", b"payload").await;

    let (status, body) = get_raw(addr, "/bucket", "list-type=2&encoding-type=url").await;
    assert_eq!(status, 200, "encoding-type=url is a valid listing: {body}");
    assert!(
        body.contains("<EncodingType>url</EncodingType>"),
        "encoding-type=url must echo <EncodingType>url</EncodingType>; body: {body}"
    );
    assert!(
        body.contains("<Key>a%26b/c%20d</Key>"),
        "key `a&b/c d` must render URL-encoded as `a%26b/c%20d`; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_encoding_type_url_encodes_prefix_delimiter_and_common_prefixes() {
    // Every encoded v2 element with an encodable value in play: Prefix, Delimiter, Key,
    // CommonPrefixes→Prefix, and the <EncodingType> echo. prefix=`dir a/` (space), delimiter=`/`.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["dir a/leaf z", "dir a/sub b/one", "dir a/sub b/two"] {
        put(&client, "bucket", k, b"x").await;
    }

    let (status, body) = get_raw(
        addr,
        "/bucket",
        "list-type=2&encoding-type=url&prefix=dir%20a/&delimiter=/",
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert!(
        body.contains("<EncodingType>url</EncodingType>"),
        "must echo EncodingType; body: {body}"
    );
    assert!(
        body.contains("<Prefix>dir%20a/</Prefix>"),
        "the request Prefix must be URL-encoded in the echo; body: {body}"
    );
    assert!(
        body.contains("<Delimiter>/</Delimiter>"),
        "the Delimiter `/` stays literal under url-encoding; body: {body}"
    );
    assert!(
        body.contains("<Key>dir%20a/leaf%20z</Key>"),
        "the content Key must be URL-encoded; body: {body}"
    );
    assert!(
        body.contains("<CommonPrefixes><Prefix>dir%20a/sub%20b/</Prefix></CommonPrefixes>"),
        "the CommonPrefixes rollup must be URL-encoded; body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_encoding_type_url_start_after_echo_and_opaque_token_resume() {
    // Minor: <StartAfter> is echoed (URL-encoded). Delta 1: NextContinuationToken stays
    // raw/opaque (NOT URL-encoded) and resumes the encoded listing verbatim.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    // Resume key `p2 xy` is 5 bytes, so its base64 token carries `=` padding — a char url-encoding
    // WOULD change to `%3D`, so "no `%` in the token" is a real opacity check, not vacuous.
    for k in ["p1 x", "p2 xy", "p3 z", "p4 zz"] {
        put(&client, "bucket", k, b"x").await;
    }

    let (status, body) = get_raw(
        addr,
        "/bucket",
        "list-type=2&encoding-type=url&max-keys=1&start-after=p1%20x",
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert!(
        body.contains("<StartAfter>p1%20x</StartAfter>"),
        "start-after must be echoed URL-encoded; body: {body}"
    );
    assert!(
        body.contains("<Key>p2%20xy</Key>"),
        "the first returned key must be URL-encoded; body: {body}"
    );
    assert!(
        body.contains("<IsTruncated>true</IsTruncated>"),
        "max-keys=1 over 4 keys is truncated; body: {body}"
    );
    let token = element(&body, "NextContinuationToken")
        .unwrap_or_else(|| panic!("truncated page carries a NextContinuationToken; body: {body}"));
    assert!(
        !token.contains('%'),
        "the continuation token is opaque and must NOT be URL-encoded, got `{token}`"
    );

    // Resume with the returned token VERBATIM (percent-encoded only for wire transport): the
    // listing continues at `p3 z`, proving the token round-trips despite encoding-type=url.
    let page2_query = format!(
        "list-type=2&encoding-type=url&continuation-token={}",
        q_encode(&token)
    );
    let (status2, body2) = get_raw(addr, "/bucket", &page2_query).await;
    assert_eq!(status2, 200, "resume body: {body2}");
    assert!(
        body2.contains("<Key>p3%20z</Key>"),
        "resuming with the opaque token must continue at `p3 z`; body: {body2}"
    );
    assert!(
        !body2.contains("<Key>p2%20xy</Key>"),
        "the resumed page must not re-emit the already-consumed `p2 xy`; body: {body2}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_encoding_type_other_than_url_is_400_invalid_argument() {
    // An `encoding-type` value other than `url` is a client error (AWS answers 400
    // InvalidArgument). Asserted on the `<Code>InvalidArgument</Code>` BODY, not the status
    // alone: the base answers 400 (`InvalidRequest`) to EVERY bucket GET, so a status-only
    // assertion is vacuously green on the C4-verify red leg.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    put(&client, "bucket", "a.txt", b"x").await;

    let (status, body) = get_raw(addr, "/bucket", "list-type=2&encoding-type=broken").await;
    assert_eq!(status, 400, "a bad encoding-type is a 400; body: {body}");
    assert!(
        body.contains("<Code>InvalidArgument</Code>"),
        "a bad encoding-type answers 400 InvalidArgument (not the base's InvalidRequest); body: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v1_encoding_type_url_encodes_marker_and_next_marker() {
    // v1 shim under encoding-type=url: Marker echo, NextMarker (common prefix), CommonPrefixes,
    // and the <EncodingType> echo are all URL-encoded, and the encoded marker resumes correctly.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["g h/1", "g h/2", "i j/1"] {
        put(&client, "bucket", k, b"x").await;
    }

    // Page 1: delimiter rollup `g h/` — truncated, so NextMarker = the common prefix, encoded.
    let (status, body) = get_raw(addr, "/bucket", "delimiter=/&max-keys=1&encoding-type=url").await;
    assert_eq!(status, 200, "body: {body}");
    assert!(
        body.contains("<EncodingType>url</EncodingType>"),
        "v1 must echo EncodingType; body: {body}"
    );
    assert!(
        body.contains("<CommonPrefixes><Prefix>g%20h/</Prefix></CommonPrefixes>"),
        "v1 CommonPrefixes rollup must be URL-encoded; body: {body}"
    );
    assert!(
        body.contains("<NextMarker>g%20h/</NextMarker>"),
        "v1 NextMarker (the common prefix) must be URL-encoded; body: {body}"
    );

    // Page 2: resume with marker=`g h/` (the encoded common prefix) — echoed encoded, group
    // skipped, next rollup `i j/` returned.
    let (status2, body2) = get_raw(
        addr,
        "/bucket",
        "delimiter=/&max-keys=1&encoding-type=url&marker=g%20h/",
    )
    .await;
    assert_eq!(status2, 200, "body: {body2}");
    assert!(
        body2.contains("<Marker>g%20h/</Marker>"),
        "v1 Marker echo must be URL-encoded; body: {body2}"
    );
    assert!(
        body2.contains("<CommonPrefixes><Prefix>i%20j/</Prefix></CommonPrefixes>"),
        "resuming from marker=`g h/` must skip that group and roll up `i j/`; body: {body2}"
    );
    assert!(
        !body2.contains("g%20h/</Prefix></CommonPrefixes>"),
        "the consumed `g h/` group must not be re-emitted on resume; body: {body2}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_start_after_equal_to_common_prefix_drops_the_rollup() {
    // A CLIENT-chosen v2 `start-after` EXACTLY EQUAL to a common prefix (`start-after=a/`,
    // the folder-marker workflow) collapses the `a/` group: the rollup is filtered on the
    // common prefix itself, and `"a/"` is not lexicographically GREATER than `start-after=a/`
    // (the AWS-documented rule; equality is not enough). Bucket {a/1, a/2, b} with
    // `?list-type=2&delimiter=/&start-after=a/` returns CommonPrefixes=[], Contents=[b] —
    // the same collapse a v1 client gets resending the server-issued `NextMarker=a/`.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["a/1", "a/2", "b"] {
        put(&client, "bucket", k, b"x").await;
    }

    let out = client
        .list_objects_v2()
        .bucket("bucket")
        .delimiter("/")
        .start_after("a/")
        .send()
        .await
        .expect("start-after equal to a common prefix");
    let cps: Vec<&str> = out
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<&str> = out.contents().iter().filter_map(|o| o.key()).collect();
    assert!(
        cps.is_empty(),
        "a/ is not lexicographically greater than start-after=a/, so the rollup is dropped; got {cps:?}"
    );
    assert_eq!(keys, vec!["b"], "only the top-level key b remains");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_continuation_token_wins_over_start_after() {
    // Precedence (AWS semantics; the stock paginator resends StartAfter alongside every token,
    // so this is the NORMAL flow): when a `continuation-token` and `start-after` both arrive,
    // the token WINS and start-after is ignored — a `max(token, start-after)` blend would be
    // wrong. Keys {a,b,c,d}: page 1 start-after=a&max-keys=1 → token(b); page 2
    // token(b)&start-after=d must return `c`, not nothing.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    for k in ["a", "b", "c", "d"] {
        put(&client, "bucket", k, b"x").await;
    }

    let page1 = client
        .list_objects_v2()
        .bucket("bucket")
        .start_after("a")
        .max_keys(1)
        .send()
        .await
        .expect("page 1");
    let keys1: Vec<&str> = page1.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(
        keys1,
        vec!["b"],
        "page 1 (start-after=a, max-keys=1) returns b"
    );
    let token = page1
        .next_continuation_token()
        .expect("page 1 is truncated")
        .to_string();

    let page2 = client
        .list_objects_v2()
        .bucket("bucket")
        .continuation_token(token)
        .start_after("d") // a co-sent start-after that, if it won, would skip everything
        .max_keys(1)
        .send()
        .await
        .expect("page 2");
    let keys2: Vec<&str> = page2.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(
        keys2,
        vec!["c"],
        "the continuation-token must win over a co-sent start-after=d (which would return nothing)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_v2_double_slash_bucket_path_is_rejected_not_listed() {
    // Sign-off #507 adversary (§6 item 4): a signed `GET //bucket?list-type=2` carries an
    // EMPTY first path segment, so it names no bucket and MUST NOT be answered with a listing.
    // The previous attempt's `bucket_scoped_path` used `trim_start_matches('/')`, which folded
    // `//bucket` down to `bucket` and answered a bogus `200 <ListBucketResult>`; a single
    // `strip_prefix('/')` keeps the empty segment so the path falls through to the object-path
    // guard's client error. Asserted on the response SHAPE (no listing document + an error
    // status), not the status alone: the base answers 400 to every bucket GET, so a status-only
    // check would be vacuous, but a `<ListBucketResult>` body cleanly separates the buggy 200.
    let (addr, _dir) = start_gateway(&["bucket"]).await;
    let client = sdk_client(addr);
    put(&client, "bucket", "a.txt", b"x").await;

    let (status, body) = get_raw(addr, "//bucket", "list-type=2").await;
    assert!(
        !body.contains("<ListBucketResult"),
        "a `//bucket` path (empty first segment) must NOT be answered with a listing document; \
         got status {status}, body: {body}"
    );
    assert!(
        (400..500).contains(&status),
        "a `//bucket` path must answer a 4xx client error, not a listing; got status {status}, body: {body}"
    );
}
