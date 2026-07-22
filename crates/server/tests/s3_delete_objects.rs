//! Bulk **DeleteObjects** on the S3 wire surface (issue #509, ADR-0046 routing) — exercised
//! over a real loopback listener, the same in-process redb + fs-tempdir stack
//! `s3_object_metadata.rs` drives, and with a **stock `aws-sdk-s3` client** (mirroring the
//! `s3_gateway_cluster.rs::sdk_client` harness) so the SDK builds the signed
//! `POST /bucket?delete` + XML body for real.
//!
//! The load-bearing assertions (the brief's success criterion):
//!
//! * a `delete_objects()` naming N keys — some present, at least one absent — returns 200 with a
//!   `<DeleteResult>` naming each requested key exactly once as `<Deleted>` (removed AND absent
//!   keys, because S3 delete is idempotent), and subsequent GETs of the deleted keys answer
//!   `404 NoSuchKey`;
//! * with `Quiet=true` the result omits the `<Deleted>` entries (the objects are still gone);
//! * a malformed XML body answers `400 MalformedXML`;
//! * a document that is not well-formed XML — a SECOND `<Delete>` root, non-whitespace content
//!   trailing the closed root, junk after a tag name (`</Key garbage>`, `<Delete garbage>`), a
//!   DUPLICATE attribute name (`<Delete x='1' x='2'>`), a `<` / bare `&` inside an attribute
//!   value (`<Delete x='<'>`, `<Delete x='&'>`), or a malformed processing instruction
//!   (`<? ?>`) — is `400 MalformedXML` AND deletes nothing (a rejected body must never authorise
//!   a deletion: well-formedness is validated WHOLE by `roxmltree` and fails closed, it does not
//!   reduce a mangled tag to a plausible one);
//! * a request naming more than 1000 keys is refused;
//! * a body past the buffered-byte cap is refused before it is fully resident;
//! * an entity-escaped key (`a&amp;b`) deletes the right object (`bucket/a&b`);
//! * a `<Key>` carrying a literal percent-escape (`a%2Fb`) deletes the literal key, never a
//!   percent-decoded one (`a/b`);
//! * a `<Key>` whose entity nests (`a&amp;amp;b`, literal key `a&amp;b`) is XML-entity-decoded
//!   EXACTLY ONCE — a re-decode would delete the wrong object (`a&b`);
//! * a `<Key>` whose character data is split by a comment (`a<!--x-->c`) is `400 MalformedXML`
//!   and deletes nothing — never `.text()`-truncated to `a`, nor comment-ignored to `ac`.
//!
//! RED on the target base (`origin/main`): with 507's routing split on the base, a bucket-only
//! `POST /bucket?delete` hits the bucket-route subresource denylist (`"delete"` is listed) and
//! answers `501 NotImplemented`; it is not a `<DeleteResult>`, so every assertion below fails.
//! GREEN once the bulk handler intercepts `?delete` ahead of that denylist and answers. The test
//! drives the wire only (SDK + signed HTTP), importing no new production symbol, so the
//! C4-verify red leg fails by assertion, not compile error.

// wall-clock exempt (test crate): SigV4 request dates against a live
// in-process server use real wall time; nothing here mixes clock sources
// within one asserted lifecycle (#619).
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use aws_sdk_s3::config::{Credentials as SdkCredentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, sign_with_payload_hash, Credentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "bucket";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// A gateway with a small chunk size so a modest object still spans several chunks over the
/// streaming wire path (matching the `s3_object_metadata.rs` harness).
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

/// Start the S3 gateway on an ephemeral loopback port. The `TempDir` is returned so the caller
/// keeps the chunk store alive for the test.
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

/// An `aws-sdk-s3` client pointed at the loopback gateway — a real SDK configured the way any
/// S3-compatible endpoint is reached (custom endpoint, path-style, static creds, plaintext).
/// Mirrors `s3_gateway_cluster.rs::sdk_client`; retries and stalled-stream protection are off so
/// the test is deterministic.
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

/// PUT one object of a few bytes under `key` via the SDK.
async fn put_object(client: &Client, key: &str) {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(format!("body-of-{key}").into_bytes()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("PUT {key} failed: {e:?}"));
}

/// Assert that GET of `key` answers `404 NoSuchKey` — the object is gone.
async fn assert_absent(client: &Client, key: &str) {
    let err = client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .expect_err("a deleted key must GET as an error");
    let service = err.into_service_error();
    assert!(
        service.is_no_such_key(),
        "expected 404 NoSuchKey for {key}, got {service:?}",
    );
}

/// Assert that GET of `key` still succeeds — the object survived. Used to prove a REJECTED
/// bulk delete touched no keys (a malformed body must not delete anything).
async fn assert_present(client: &Client, key: &str) {
    client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("expected {key} to still exist, GET failed: {e:?}"));
}

/// Build a `Delete` payload from keys, optionally quiet.
fn delete_payload(keys: &[&str], quiet: bool) -> Delete {
    let mut builder = Delete::builder();
    for key in keys {
        builder = builder.objects(
            ObjectIdentifier::builder()
                .key(*key)
                .build()
                .expect("object identifier"),
        );
    }
    builder.quiet(quiet).build().expect("delete payload")
}

/// Send one HTTP/1.1 request over a fresh connection and return `(status, head, body)` — the raw
/// head so the caller can read the status line. Mirrors `s3_object_metadata.rs::send`.
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
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("numeric status");
    (status, head, raw[split + 4..].to_vec())
}

/// Sign and send a raw `POST /{bucket}?{query}` with `body`, driving the wire directly — used
/// for inputs the typed SDK will not produce (a malformed XML body, an over-limit key count, a
/// `<Key>` carrying exact bytes the SDK would escape differently). Signs exactly as
/// `s3_object_metadata.rs` does; the query is signed and sent verbatim so the gateway's SigV4
/// verify recomputes the same canonical form.
async fn post_delete_raw(addr: SocketAddr, query: &str, body: &[u8]) -> (u16, String, Vec<u8>) {
    let host = addr.to_string();
    let uri = format!("/{BUCKET}");
    let amz_date = format_amz_date(SystemTime::now());
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let signed = sign(
        "POST", &uri, query, &host, &amz_date, body, &creds, REGION, "s3",
    );
    let headers = vec![
        ("authorization".to_string(), signed.authorization),
        ("x-amz-date".to_string(), signed.amz_date),
        ("x-amz-content-sha256".to_string(), signed.content_sha256),
    ];
    let target = format!("{uri}?{query}");
    send(addr, "POST", &target, &headers, body).await
}

/// Drive a raw malformed-body attack: PUT `victim`, POST the (rejected) `body`, and assert
/// `400 MalformedXML` AND that `victim` survived. The load-bearing invariant across every
/// not-well-formed body: a rejected request authorises no deletion.
async fn assert_rejected_and_keeps_victim(body: &[u8]) {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);
    put_object(&client, "victim").await;

    let (status, head, resp) = post_delete_raw(addr, "delete", body).await;
    let resp = String::from_utf8_lossy(&resp);
    assert_eq!(
        status,
        400,
        "a not-well-formed DeleteObjects body must be 400: {head}\nbody sent: {}",
        String::from_utf8_lossy(body),
    );
    assert!(
        resp.contains("MalformedXML"),
        "expected S3 MalformedXML code, got body: {resp}",
    );
    // The load-bearing assertion: the rejected request removed nothing.
    assert_present(&client, "victim").await;
}

/// The bulk-delete route is intercepted BEFORE the subresource denylist, so that `?delete` — itself
/// a denylisted key — is not refused by it. That early return also skipped the denylist for every
/// OTHER key in the same query, making the one destructive route a hole in the fence: `POST
/// /bucket?delete&versionId=v` ran an ordinary UNVERSIONED bulk delete while the client had
/// explicitly asked for version semantics, destroying the current object. That is the same
/// data-loss shape a `<VersionId>` in the BODY is refused for, reached by the query spelling
/// instead (PR #612 review). The percent-encoded companion is covered too, since the fence decodes.
#[tokio::test]
async fn delete_objects_denylisted_query_companion_is_refused_and_deletes_nothing() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);
    let body = b"<Delete><Object><Key>victim</Key></Object></Delete>";

    for query in [
        "delete&versionId=v",   // the data-loss case: version semantics + a bulk delete
        "versionId=v&delete",   // order must not matter
        "delete&acl",           // any other denylisted subresource
        "delete&%76ersionId=v", // percent-encoded companion still decodes to versionId
    ] {
        put_object(&client, "victim").await;
        let (status, head, resp) = post_delete_raw(addr, query, body).await;
        let resp = String::from_utf8_lossy(&resp);
        assert_eq!(
            status, 501,
            "?{query}: a denylisted subresource riding alongside the delete marker must be \
             refused, not walked past the fence into the destructive handler: {head}",
        );
        assert!(
            resp.contains("NotImplemented"),
            "?{query}: expected S3 NotImplemented, got body: {resp}",
        );
        // The load-bearing assertion: the refused request deleted nothing.
        assert_present(&client, "victim").await;
    }
}

/// S3's **conditional-delete** fields — `<ETag>`, `<LastModifiedTime>`, `<Size>` — mean "delete
/// this only if it still looks like this". Ignoring them converts a guarded request into an
/// unconditional delete, destroying exactly the object whose failed precondition was supposed to
/// save it: the worst failure available on a destructive verb. They are refused whole, like
/// `<VersionId>` (PR #612 review).
#[tokio::test]
async fn delete_objects_conditional_delete_fields_are_refused_and_delete_nothing() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);

    for field in [
        "<ETag>\"d41d8cd98f00b204e9800998ecf8427e\"</ETag>",
        "<LastModifiedTime>2026-07-21T00:00:00.000Z</LastModifiedTime>",
        "<Size>42</Size>",
    ] {
        put_object(&client, "victim").await;
        let body =
            format!("<Delete><Object><Key>victim</Key>{field}</Object></Delete>").into_bytes();
        let (status, head, resp) = post_delete_raw(addr, "delete", &body).await;
        let resp = String::from_utf8_lossy(&resp);
        assert_eq!(
            status, 501,
            "{field}: a conditional-delete field this gateway cannot enforce must be refused, \
             never silently dropped into an unconditional delete: {head}",
        );
        assert!(
            resp.contains("NotImplemented"),
            "{field}: expected S3 NotImplemented, got body: {resp}",
        );
        // The load-bearing assertion: the object the precondition was protecting survives.
        assert_present(&client, "victim").await;
    }
}

/// A `<VersionId>` inside an `<Object>` must be REFUSED, never silently dropped. Ignoring it turns
/// "delete this OLD version" into "delete the CURRENT object" — irrecoverably destroying the live
/// object the client asked to keep. `versionId` is already on the object route's unsupported
/// denylist, so honouring the XML spelling while refusing the query spelling was self-inconsistent
/// too (PR #612 review).
#[tokio::test]
async fn delete_objects_version_scoped_entry_is_refused_and_deletes_nothing() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);
    put_object(&client, "victim").await;

    let body = b"<Delete><Object><Key>victim</Key><VersionId>abc123</VersionId></Object></Delete>";
    let (status, head, resp) = post_delete_raw(addr, "delete", body).await;
    let resp = String::from_utf8_lossy(&resp);
    assert_eq!(
        status, 501,
        "a version-scoped DeleteObjects entry must be refused as unsupported: {head}",
    );
    assert!(
        resp.contains("NotImplemented"),
        "expected S3 NotImplemented code, got body: {resp}",
    );
    // The load-bearing assertion: the LIVE object — which the client did not ask to delete — is
    // still there. A silently-ignored <VersionId> would have removed it.
    assert_present(&client, "victim").await;

    // A MIXED batch is refused WHOLE: the unversioned sibling is not deleted either, so the
    // destructive half never partially applies.
    put_object(&client, "sibling").await;
    let mixed = b"<Delete><Object><Key>sibling</Key></Object>\
                  <Object><Key>victim</Key><VersionId>v1</VersionId></Object></Delete>";
    let (status, head, _resp) = post_delete_raw(addr, "delete", mixed).await;
    assert_eq!(
        status, 501,
        "a batch mixing versioned and unversioned entries is refused whole: {head}",
    );
    assert_present(&client, "sibling").await;
    assert_present(&client, "victim").await;
}

/// A bulk delete carried on an UNVERIFIED body must be refused before any key is touched. Under
/// `UNSIGNED-PAYLOAD` the body sits deliberately outside the signature and this gateway validates
/// no `Content-MD5`/`x-amz-checksum-*`, so nothing proves the key list arrived as sent — corruption
/// or tampering in flight could substitute keys and destroy the wrong objects (PR #612 review).
/// The signature itself is VALID here: the caller is authenticated, only the body is unprotected.
#[tokio::test]
async fn delete_objects_unsigned_payload_is_refused_and_deletes_nothing() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);
    put_object(&client, "victim").await;

    let body = b"<Delete><Object><Key>victim</Key></Object></Delete>";
    let host = addr.to_string();
    let uri = format!("/{BUCKET}");
    let amz_date = format_amz_date(SystemTime::now());
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    let signed = sign_with_payload_hash(
        "POST",
        &uri,
        "delete",
        &host,
        &amz_date,
        "UNSIGNED-PAYLOAD",
        &creds,
        REGION,
        "s3",
    );
    let headers = vec![
        ("authorization".to_string(), signed.authorization),
        ("x-amz-date".to_string(), signed.amz_date),
        ("x-amz-content-sha256".to_string(), signed.content_sha256),
    ];
    let target = format!("{uri}?delete");
    let (status, head, resp) = send(addr, "POST", &target, &headers, body).await;
    let resp = String::from_utf8_lossy(&resp);
    assert_eq!(
        status, 400,
        "an UNSIGNED-PAYLOAD bulk delete must be refused, not executed on an unverified body: \
         {head}",
    );
    assert!(
        resp.contains("InvalidRequest"),
        "expected S3 InvalidRequest code, got body: {resp}",
    );
    // The load-bearing assertion: nothing was deleted on the unverified body.
    assert_present(&client, "victim").await;
}

#[tokio::test]
async fn delete_objects_removes_present_and_absent_keys_idempotently() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);

    // Three present objects; the delete also names one that was never stored.
    for key in ["k1", "k2", "nested/k3"] {
        put_object(&client, key).await;
    }
    let requested = ["k1", "k2", "nested/k3", "never-existed"];

    let out = client
        .delete_objects()
        .bucket(BUCKET)
        .delete(delete_payload(&requested, false))
        .send()
        .await
        .expect("delete_objects returns a well-formed DeleteResult (200)");

    assert!(
        out.errors().is_empty(),
        "no per-key errors expected, got {:?}",
        out.errors()
    );
    let mut deleted: Vec<String> = out
        .deleted()
        .iter()
        .map(|d| d.key().expect("a <Deleted> names its key").to_string())
        .collect();
    deleted.sort();
    let mut expected: Vec<String> = requested.iter().map(|k| k.to_string()).collect();
    expected.sort();
    // Each requested key named exactly once — removed AND absent keys are <Deleted> (idempotent).
    assert_eq!(
        deleted, expected,
        "every requested key must appear once as <Deleted>, present or absent",
    );

    // The present keys are actually gone now.
    for key in ["k1", "k2", "nested/k3"] {
        assert_absent(&client, key).await;
    }
}

/// `<Quiet>` is typed `xs:boolean` by S3, so its lexical space is exactly `true`/`false`/`1`/`0`.
/// Comparing against the literal `true` alone was wrong both ways: a garbage value read as
/// "verbose" and authorised the destructive fan-out anyway — in a parser that refuses every other
/// semantic violation before touching a key — and a valid `1` answered a quiet request with a full
/// listing (PR #612 review).
#[tokio::test]
async fn delete_objects_invalid_quiet_is_rejected_and_deletes_nothing() {
    for body in [
        &b"<Delete><Object><Key>victim</Key></Object><Quiet>garbage</Quiet></Delete>"[..],
        // An empty flag is not a boolean either.
        &b"<Delete><Object><Key>victim</Key></Object><Quiet></Quiet></Delete>"[..],
        // Case matters: `xs:boolean` is lower-case only.
        &b"<Delete><Object><Key>victim</Key></Object><Quiet>True</Quiet></Delete>"[..],
    ] {
        assert_rejected_and_keeps_victim(body).await;
    }
}

/// The other half of the `xs:boolean` lexical space: `1` and `0` are valid spellings of the flag
/// and must be honoured, not silently degraded to "verbose".
#[tokio::test]
async fn delete_objects_numeric_quiet_spellings_are_honoured() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);
    put_object(&client, "n1").await;
    put_object(&client, "n0").await;

    // `1` means quiet: the object is deleted and NO <Deleted> row is echoed.
    let (status, head, resp) = post_delete_raw(
        addr,
        "delete",
        b"<Delete><Object><Key>n1</Key></Object><Quiet>1</Quiet></Delete>",
    )
    .await;
    let resp = String::from_utf8_lossy(&resp);
    assert_eq!(status, 200, "<Quiet>1</Quiet> is a valid boolean: {head}");
    assert!(
        !resp.contains("<Deleted>"),
        "`1` means Quiet=true, so no <Deleted> row is echoed: {resp}"
    );
    assert_absent(&client, "n1").await;

    // `0` means verbose: the object is deleted and the <Deleted> row IS echoed.
    let (status, head, resp) = post_delete_raw(
        addr,
        "delete",
        b"<Delete><Object><Key>n0</Key></Object><Quiet>0</Quiet></Delete>",
    )
    .await;
    let resp = String::from_utf8_lossy(&resp);
    assert_eq!(status, 200, "<Quiet>0</Quiet> is a valid boolean: {head}");
    assert!(
        resp.contains("<Deleted>"),
        "`0` means Quiet=false, so the <Deleted> row is echoed: {resp}"
    );
    assert_absent(&client, "n0").await;
}

#[tokio::test]
async fn delete_objects_quiet_omits_deleted_entries_but_still_deletes() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);

    for key in ["q1", "q2"] {
        put_object(&client, key).await;
    }

    let out = client
        .delete_objects()
        .bucket(BUCKET)
        .delete(delete_payload(&["q1", "q2"], true))
        .send()
        .await
        .expect("quiet delete_objects still returns 200");

    assert!(
        out.deleted().is_empty(),
        "Quiet=true omits <Deleted> entries, got {:?}",
        out.deleted()
    );
    assert!(out.errors().is_empty(), "no errors expected");

    // Omitting the <Deleted> entries does not mean the objects survived.
    for key in ["q1", "q2"] {
        assert_absent(&client, key).await;
    }
}

#[tokio::test]
async fn delete_objects_entity_escaped_key_deletes_the_right_object() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);

    // A key whose bytes are XML-special: the SDK entity-escapes it in the body (`a&amp;b`), and
    // the gateway must unescape it to delete the stored `bucket/a&b`.
    let key = "a&b/weird<key>";
    put_object(&client, key).await;

    let out = client
        .delete_objects()
        .bucket(BUCKET)
        .delete(delete_payload(&[key], false))
        .send()
        .await
        .expect("delete of an entity-escaped key returns 200");

    let deleted: Vec<&str> = out.deleted().iter().filter_map(|d| d.key()).collect();
    assert_eq!(
        deleted,
        vec![key],
        "the entity-escaped key round-trips through the body and is reported deleted",
    );
    assert_absent(&client, key).await;
}

#[tokio::test]
async fn delete_objects_literal_percent_key_is_not_percent_decoded() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);

    // The `<Key>` body is XML-entity-decoded only, NEVER percent-decoded (percent_decode_utf8
    // applies to the URL path/query, not to a key carried in the body). So `<Key>a%2Fb</Key>`
    // names the LITERAL key `a%2Fb`, and `<Key>x%20y</Key>` names `x%20y` — not `a/b` / `x y`.
    put_object(&client, "a%2Fb").await;
    put_object(&client, "x%20y").await;
    // The percent-decoded forms are decoys a percent-decoding bug would delete instead — they
    // must SURVIVE, which is what discriminates "literal key" from "percent-decoded key".
    put_object(&client, "a/b").await;
    put_object(&client, "x y").await;

    let (status, head, body) = post_delete_raw(
        addr,
        "delete",
        b"<Delete><Object><Key>a%2Fb</Key></Object><Object><Key>x%20y</Key></Object></Delete>",
    )
    .await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(status, 200, "a literal-% key deletes a real object: {head}");
    assert!(
        body.contains("DeleteResult"),
        "expected a <DeleteResult>, got: {body}"
    );

    // The literal-% keys are gone; the percent-decoded decoys survive (no percent-decode).
    assert_absent(&client, "a%2Fb").await;
    assert_absent(&client, "x%20y").await;
    assert_present(&client, "a/b").await;
    assert_present(&client, "x y").await;
}

#[tokio::test]
async fn delete_objects_nested_entity_key_is_decoded_exactly_once() {
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);

    // `<Key>a&amp;amp;b</Key>` XML-entity-decodes EXACTLY ONCE to the literal key `a&amp;b`.
    // A handler that re-decoded it would reach `a&b` and delete the WRONG object. So the literal
    // `a&amp;b` victim must be deleted and the `a&b` decoy must survive.
    put_object(&client, "a&amp;b").await; // literal bytes: a & a m p ; b
    put_object(&client, "a&b").await; // the object a double-decode would hit

    let (status, head, body) = post_delete_raw(
        addr,
        "delete",
        b"<Delete><Object><Key>a&amp;amp;b</Key></Object></Delete>",
    )
    .await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(
        status, 200,
        "a nested-entity key returns a DeleteResult: {head}"
    );
    assert!(
        body.contains("DeleteResult"),
        "expected a <DeleteResult>, got: {body}"
    );

    assert_absent(&client, "a&amp;b").await; // decoded once → the literal key was deleted
    assert_present(&client, "a&b").await; // NOT re-decoded → the decoy survives
}

#[tokio::test]
async fn delete_objects_comment_split_key_is_rejected_and_deletes_nothing() {
    // The TRUE discriminator for the fail-closed key-extraction fix: a `<Key>` split by a comment
    // (`a<!--x-->c`) is NOT a clean character-data run, so the whole request is MalformedXML and
    // deletes nothing. It must NEVER be `Node::text()`-truncated to `a` (which would delete the
    // wrong object), nor comment-ignored and concatenated to `ac`. Both the truncation target
    // (`a`) and the concat target (`ac`) must survive.
    let (addr, _dir) = start_gateway().await;
    let client = sdk_client(addr);
    put_object(&client, "a").await;
    put_object(&client, "ac").await;

    let (status, head, resp) = post_delete_raw(
        addr,
        "delete",
        b"<Delete><Object><Key>a<!--x-->c</Key></Object></Delete>",
    )
    .await;
    let resp = String::from_utf8_lossy(&resp);
    assert_eq!(
        status, 400,
        "a <Key> split by a comment is not a clean char-data run: {head}",
    );
    assert!(
        resp.contains("MalformedXML"),
        "expected S3 MalformedXML code, got body: {resp}",
    );
    // A rejected request authorises no deletion — neither the `.text()` truncation target (`a`)
    // nor the concat target (`ac`) may be touched.
    assert_present(&client, "a").await;
    assert_present(&client, "ac").await;
}

#[tokio::test]
async fn delete_objects_malformed_xml_is_rejected() {
    let (addr, _dir) = start_gateway().await;
    let (status, head, body) = post_delete_raw(addr, "delete", b"this is not valid XML").await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(status, 400, "a malformed <Delete> body is a 400: {head}");
    assert!(
        body.contains("MalformedXML"),
        "expected S3 MalformedXML code, got body: {body}",
    );
}

#[tokio::test]
async fn delete_objects_multiple_roots_are_rejected_and_delete_nothing() {
    // A well-formed DeleteObjects body has EXACTLY ONE `<Delete>` root. A body with a SECOND
    // root must be MalformedXML — and, critically, must delete NOTHING (the iteration-4 class).
    assert_rejected_and_keeps_victim(
        b"<Delete></Delete><Delete><Object><Key>victim</Key></Object></Delete>",
    )
    .await;
}

#[tokio::test]
async fn delete_objects_trailing_content_after_root_is_rejected_and_deletes_nothing() {
    // Non-whitespace character data after the closed `<Delete>` root is MalformedXML — the
    // parser must reject it, not silently discard it (iteration-4 class).
    assert_rejected_and_keeps_victim(
        b"<Delete><Object><Key>victim</Key></Object></Delete>trailing garbage",
    )
    .await;
}

#[tokio::test]
async fn delete_objects_junk_after_a_tag_name_is_rejected_and_deletes_nothing() {
    // The iteration-5 destructive defect: a hand tokenizer reduced a tag to its first
    // whitespace-delimited token, so `</Key garbage>` was accepted as `</Key>` — a body that
    // must be rejected still authorised the key's deletion. Both a mangled END tag (the exact
    // reviewer case) and the start-tag half of the same class must be 400 AND delete NOTHING.
    for attack in [
        b"<Delete><Object><Key>victim</Key garbage></Object></Delete>".to_vec(),
        b"<Delete garbage><Object><Key>victim</Key></Object></Delete>".to_vec(),
    ] {
        assert_rejected_and_keeps_victim(&attack).await;
    }
}

#[tokio::test]
async fn delete_objects_duplicate_attribute_is_rejected_and_deletes_nothing() {
    // The iteration-6 destructive defect: attribute syntax was validated but names were not
    // tracked, so a duplicate attribute — `<Delete x='1' x='2'>` — was accepted (although XML's
    // Unique Att Spec makes the document malformed) and its `<Key>victim</Key>` deleted.
    for attack in [
        b"<Delete x='1' x='2'><Object><Key>victim</Key></Object></Delete>".to_vec(),
        b"<Delete><Object a=\"1\" a=\"2\"><Key>victim</Key></Object></Delete>".to_vec(),
    ] {
        assert_rejected_and_keeps_victim(&attack).await;
    }
}

#[tokio::test]
async fn delete_objects_malformed_attribute_value_is_rejected_and_deletes_nothing() {
    // The iteration-7 destructive defect: an attribute value's interior was located by finding
    // the closing quote but never validated, so a `<` or a bare `&` inside it (both forbidden by
    // the XML `AttValue` production) was accepted — `<Delete x='<'>` deleted `victim`. The
    // DOM parser now validates the value character by character.
    for attack in [
        b"<Delete x='<'><Object><Key>victim</Key></Object></Delete>".to_vec(),
        b"<Delete x=\"a<b\"><Object><Key>victim</Key></Object></Delete>".to_vec(),
        b"<Delete x='&'><Object><Key>victim</Key></Object></Delete>".to_vec(),
        b"<Delete x='a&b'><Object><Key>victim</Key></Object></Delete>".to_vec(),
    ] {
        assert_rejected_and_keeps_victim(&attack).await;
    }
}

#[tokio::test]
async fn delete_objects_malformed_processing_instruction_is_rejected_and_deletes_nothing() {
    // The iteration-8 destructive defect — the FIFTH rejected class: a malformed processing
    // instruction (`<? ?>`, a PI with no target) makes the document not well-formed. The
    // hand-rolled tokenizer let it through and still deleted the `<Key>victim</Key>` it named;
    // the DOM parser rejects the whole document, so it is 400 MalformedXML AND deletes nothing.
    // Both a leading and a trailing malformed PI are exercised.
    for attack in [
        b"<? ?><Delete><Object><Key>victim</Key></Object></Delete>".to_vec(),
        b"<Delete><Object><Key>victim</Key></Object></Delete><? ?>".to_vec(),
    ] {
        assert_rejected_and_keeps_victim(&attack).await;
    }
}

#[tokio::test]
async fn delete_objects_more_than_1000_keys_is_refused() {
    let (addr, _dir) = start_gateway().await;
    let mut xml = String::from("<Delete>");
    for i in 0..1001 {
        xml.push_str(&format!("<Object><Key>k{i}</Key></Object>"));
    }
    xml.push_str("</Delete>");

    let (status, head, body) = post_delete_raw(addr, "delete", xml.as_bytes()).await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(
        status, 400,
        "a DeleteObjects naming more than 1000 keys is refused: {head}",
    );
    // Assert the S3 code, not merely the 400 status: the base's keyless-path `501` is a
    // different failure, so a status-only check would not discriminate cleanly. Over-limit is
    // specifically `MalformedXML` (S3 refuses, never truncates to the first 1000).
    assert!(
        body.contains("MalformedXML"),
        "expected S3 MalformedXML code for an over-limit key count, got body: {body}",
    );
}

/// The body cap must be sized on the ESCAPED wire bytes, not the raw key bytes. A request at both
/// documented limits — 1000 keys, each a legal 1024-byte key — serializes to far more than 1024
/// bytes per key once escaped (`&` becomes `&amp;`), so a cap floored on the raw size
/// fail-closed-rejected a perfectly legal DeleteObjects with `400 MalformedXML` (PR #612 review).
/// This body is ≈5.1 MB escaped, comfortably past the previous 2 MiB cap, and must be ACCEPTED.
#[tokio::test]
async fn delete_objects_max_keys_with_escaped_content_is_accepted() {
    let (addr, _dir) = start_gateway().await;
    // 1000 keys × a 1024-byte key made entirely of `&` — each `&` costs 5 body bytes as `&amp;`.
    let escaped_key = "&amp;".repeat(1024);
    let mut xml = String::from("<Delete>");
    for _ in 0..1000 {
        xml.push_str("<Object><Key>");
        xml.push_str(&escaped_key);
        xml.push_str("</Key></Object>");
    }
    xml.push_str("</Delete>");
    assert!(
        xml.len() > 2 * 1024 * 1024,
        "the fixture must exceed the previous 2 MiB cap to be a regression guard, got {}",
        xml.len()
    );

    let (status, head, body) = post_delete_raw(addr, "delete", xml.as_bytes()).await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(
        status, 200,
        "a legal 1000-key request whose keys are XML-escaped must be accepted, not capped: {head}",
    );
    assert!(
        !body.contains("MalformedXML"),
        "a legal escaped-key request must not be refused as MalformedXML, got body: {body}",
    );
}

#[tokio::test]
async fn delete_objects_oversized_body_is_refused_before_it_is_resident() {
    // The bulk-delete body is buffered whole (unlike a streamed object PUT), so the handler
    // caps the buffered bytes BY CONSTRUCTION rather than trusting the declared Content-Length
    // (brief §Scope: "bound the buffered body bytes"). A body past the cap is refused as it is
    // read — before it is fully resident and before any key is touched. This drives the
    // production `buffer_capped` -> `BufferError::TooLarge` -> 400 branch over the wire; the
    // 1000-key semantic bound above cannot reach it (1001 small keys are well under the cap).
    let (addr, _dir) = start_gateway().await;
    // A single element whose text pads the body past the 8 MiB cap: well-formed XML, so a
    // *green* handler that ignored the cap would parse it and answer 200 — only the byte cap
    // makes this a 400.
    let mut xml = String::from("<Delete><Object><Key>");
    xml.push_str(&"a".repeat(9 * 1024 * 1024));
    xml.push_str("</Key></Object></Delete>");

    let (status, head, body) = post_delete_raw(addr, "delete", xml.as_bytes()).await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(
        status, 400,
        "a DeleteObjects body past the buffered-byte cap is refused: {head}",
    );
    assert!(
        body.contains("MalformedXML"),
        "expected S3 MalformedXML code for the oversized body, got body: {body}",
    );
}
