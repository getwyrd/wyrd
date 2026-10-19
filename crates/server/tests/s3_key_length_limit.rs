//! S3's 1,024-byte object-key limit on the wire surface (issue #620), exercised over a real
//! loopback listener on the in-process redb + fs-tempdir stack the peer S3 suites drive
//! (`s3_delete_objects.rs`, `s3_list_objects.rs`). Every request is signed SigV4 raw HTTP, so
//! the exact request path (percent-encoding, a `%2F` in the bucket segment) and the payload
//! form (single-shot signed, `aws-chunked`, `UNSIGNED-PAYLOAD`) are under the test's control.
//!
//! What it pins:
//!
//! 1. a PUT whose decoded key is 1,025 UTF-8 bytes is `400 KeyTooLongError` and stores nothing
//!    (HEAD `404`, absent from a listing that does show an in-limit control object);
//! 2. the limit counts UTF-8 bytes of the DECODED key alone: 512 × `é` (1,024 bytes) is accepted,
//!    342 × `€` (1,026 bytes) is refused, and a 1,024-byte key sent fully percent-encoded (a
//!    3,072-byte request path) is accepted. The limit is not on the wire form, nor on
//!    `bucket/key`;
//! 3. bulk DeleteObjects adds NO key-length refusal: one signed request naming an ordinary key,
//!    a 1,025-byte key and a 70,000-byte key (the last past hyper's 65,534-byte request-target
//!    ceiling, so single-object HEAD/DELETE answer `414` before the gateway runs — bulk delete is
//!    its only S3 exit) answers `200` with a `<Deleted>` row for each and no `<Error>` row, and
//!    all three are gone afterwards. The over-limit keys are seeded through `Gateway::put_object`,
//!    outside the S3 surface, exactly as a legacy or `wyrd put --key` object exists. A second
//!    request then deletes the LONGEST key bulk delete can carry — 8,388,563 bytes, the sole
//!    entry of a `<Delete>` document that fills the 8 MiB body cap exactly — so a length bound
//!    anywhere below the cap, not merely below 70,000 bytes, goes red;
//! 4. an over-limit object that already exists (seeded the same way) stays removable by
//!    single-object DELETE: HEAD `200` → DELETE `204` → HEAD `404`;
//! 5. `PUT /b%2Fa/<'x' × 1023>`, a 1,023-byte request key that would store the same object as
//!    `/b/a/<'x' × 1023>` (whose key under `b` is 1,025 bytes), is `400 InvalidBucketName` and
//!    stores nothing;
//! 6. both refusals hold for EVERY payload form the PUT arm dispatches, not only the single-shot
//!    signed body legs 1, 2 and 5 send: an `aws-chunked` upload with signed chunks
//!    (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, the form a stock SDK sends) and an
//!    `UNSIGNED-PAYLOAD` body. In each form a 1,024-byte key is stored (the framing itself is
//!    accepted), a 1,025-byte key is `400 KeyTooLongError` with nothing stored, and the
//!    `%2F`-bucket PUT is `400 InvalidBucketName` with nothing stored. A check placed inside the
//!    signed-payload arm passes legs 1 to 5 and still lets a stock SDK store a 1,025-byte key
//!    (#620 iteration-6 sign-off).
//!
//! RED on `origin/main`: legs 1, 2 (the `€` key), 5 and 6 answer `200` and store the object, so
//! each fails on an assertion. Legs 3 and 4 are green before and after: they pin that NO delete
//! path refuses a stored key for its length — leg 3 is the one that goes red against a
//! bulk-delete key-length bound (the design rejected at #620's iteration-4 sign-off). The test
//! uses only pre-existing API (the wire, `Gateway::put_object` / `get_object` and the metadata
//! store's `commit` for fixtures, and the SigV4 / streaming client primitives
//! `sign_with_payload_hash*`, `signing_key_for`, `StreamingContext`, `sign_chunk`), so the
//! reverted base compiles it.
//!
//! Every await on the socket or the backend runs under [`IO_BUDGET`], and the spawned server is
//! aborted when its [`Harness`] drops (including on an assertion failure), so a stalled server
//! fails the test instead of hanging it.

#![forbid(unsafe_code)]
// wall-clock exempt (test crate): SigV4 request dates against a live
// in-process server use real wall time; nothing here mixes clock sources
// within one asserted lifecycle (#619).
// File scope (not per-site) is deliberate here: a test crate never
// ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::{
    format_amz_date, sign, sign_with_payload_hash, sign_with_payload_hash_and_headers,
    signing_key_for, uri_encode, Credentials, StreamingContext,
};
use wyrd_gateway_s3::streaming::sign_chunk;
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::{MetadataStore, WriteBatch};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "b";

/// The bound on each socket exchange and backend call. A healthy one takes milliseconds on
/// loopback; the bound exists so a server that stalls a response, or never closes the
/// connection, fails the test rather than hanging it.
const IO_BUDGET: Duration = Duration::from_secs(30);

/// The bulk-delete request-body cap: `MAX_DELETE_BODY_BYTES` in `wyrd-gateway-s3`, restated
/// because that constant is private to its crate (and this file must compile against the
/// unpatched base, so it can use nothing the fix adds). The cap is the ONLY bound on one
/// `<Key>`'s length, so the longest key bulk delete can carry is the one that fills a one-key
/// document to exactly this many bytes. If the cap moves, this moves with it.
const DELETE_BODY_CAP: usize = 8 * 1024 * 1024;

/// A one-key `<Delete>` document with its key left out: the bytes of such a body that are not
/// the key.
const ONE_KEY_ENVELOPE: &str = "<Delete><Object><Key></Key></Object></Delete>";

/// The `x-amz-content-sha256` claim of an `aws-chunked` upload whose chunks are signed — the
/// seed of the per-chunk signature chain, and what a stock SDK sends a PUT as.
const STREAMING_SIGNED_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// The body every PUT in this file carries. Its content is irrelevant; the KEY is under test.
const PAYLOAD: &[u8] = b"payload";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// A running gateway: its address, a clone of the backend handle (so a fixture can write
/// outside the S3 surface), the server task, and the chunk-store tempdir.
struct Harness {
    addr: SocketAddr,
    backend: Arc<Backend>,
    server: JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for Harness {
    /// Cancel the server on every exit path, an assertion failure included, so it stops
    /// serving before its tempdir is removed rather than living on until runtime teardown.
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Await `fut` for at most [`IO_BUDGET`], failing the test with `what` if it does not finish.
async fn bounded<T>(what: &str, fut: impl Future<Output = T>) -> T {
    match tokio::time::timeout(IO_BUDGET, fut).await {
        Ok(value) => value,
        Err(_) => panic!("{what}: did not finish within {IO_BUDGET:?}"),
    }
}

fn credentials() -> Credentials {
    Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }
}

/// Start the S3 gateway on an ephemeral loopback port with bucket `b`'s existence marker
/// seeded (so listing answers), keeping an `Arc<Gateway>` clone before handing it over.
async fn start_gateway() -> Harness {
    let dir = tempfile::tempdir().expect("temp dir");
    let meta = RedbMetadataStore::in_memory().expect("redb");
    // Raw marker bytes, as `s3_list_objects.rs::seed_bucket` does: listing reads presence only.
    let marker = format!("{{\"name\":\"{BUCKET}\",\"created_millis\":0}}").into_bytes();
    bounded(
        "commit the bucket marker",
        meta.commit(WriteBatch::new().put(format!("bucket:{BUCKET}").into_bytes(), marker)),
    )
    .await
    .expect("commit bucket marker");
    let backend: Arc<Backend> = Arc::new(
        Gateway::new(
            meta,
            FsChunkStore::open(dir.path()).expect("fs store"),
            MemCoordination::new(),
        )
        .with_chunk_size(8),
    );
    let config = S3Config::new(vec![credentials()]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(Arc::clone(&backend), config);
    let server = tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    Harness {
        addr,
        backend,
        server,
        _dir: dir,
    }
}

/// Percent-encode EVERY byte, unreserved ones included: the longest legal spelling of `key` on
/// the wire. (The ordinary spelling is [`uri_encode`], used by [`object_path`].)
fn encode_all(key: &str) -> String {
    key.bytes().map(|b| format!("%{b:02X}")).collect()
}

/// The request head for `method target` with `headers` (name, value) and a `Content-Length`
/// of `content_length`, closing the connection after the response.
fn request_head(
    method: &str,
    target: &str,
    host: &str,
    headers: &[(&str, &str)],
    content_length: usize,
) -> String {
    let mut head = format!("{method} {target} HTTP/1.1\r\nhost: {host}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("content-length: {content_length}\r\n"));
    head.push_str("connection: close\r\n\r\n");
    head
}

/// Write `head` then `body` on a fresh connection and read the response to close, returning
/// `(status, body)`. The whole exchange runs under [`IO_BUDGET`], labelled `what`. A gateway
/// that refuses from the headers may answer and close before the body is written, so a write
/// error AFTER the head, or a reset after the response, is not a failure: the response is
/// what is asserted (as `s3_put_object_ceiling.rs::exchange` does).
async fn exchange(addr: SocketAddr, what: &str, head: &str, body: &[u8]) -> (u16, String) {
    let raw = bounded(what, async {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(head.as_bytes()).await.expect("write head");
        let _ = stream.write_all(body).await;
        let _ = stream.flush().await;
        let mut raw = Vec::new();
        let _ = stream.read_to_end(&mut raw).await;
        raw
    })
    .await;
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("{what}: response has no header terminator: {raw:?}"));
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("numeric status");
    (
        status,
        String::from_utf8_lossy(&raw[split + 4..]).into_owned(),
    )
}

/// Sign `method path?query` with `body` and send it over a fresh connection, returning
/// `(status, body)`. The path is signed and sent verbatim (the gateway verifies SigV4 over the
/// wire path as sent). Mirrors `s3_delete_objects.rs::send` / `post_delete_raw`.
async fn signed(
    addr: SocketAddr,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
) -> (u16, String) {
    let host = addr.to_string();
    let amz_date = format_amz_date(SystemTime::now());
    let s = sign(
        method,
        path,
        query,
        &host,
        &amz_date,
        body,
        &credentials(),
        REGION,
        "s3",
    );
    let target = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let head = request_head(
        method,
        &target,
        &host,
        &[
            ("authorization", &s.authorization),
            ("x-amz-date", &s.amz_date),
            ("x-amz-content-sha256", &s.content_sha256),
        ],
        body.len(),
    );
    exchange(addr, &format!("{method} {}", excerpt(&target)), &head, body).await
}

/// PUT [`PAYLOAD`] to `path` as `UNSIGNED-PAYLOAD`: the signature covers the headers and the
/// literal claim, and the body rides outside it.
async fn put_unsigned_payload(addr: SocketAddr, path: &str) -> (u16, String) {
    let host = addr.to_string();
    let amz_date = format_amz_date(SystemTime::now());
    let s = sign_with_payload_hash(
        "PUT",
        path,
        "",
        &host,
        &amz_date,
        "UNSIGNED-PAYLOAD",
        &credentials(),
        REGION,
        "s3",
    );
    let head = request_head(
        "PUT",
        path,
        &host,
        &[
            ("authorization", &s.authorization),
            ("x-amz-date", &s.amz_date),
            ("x-amz-content-sha256", &s.content_sha256),
        ],
        PAYLOAD.len(),
    );
    exchange(
        addr,
        &format!("PUT (UNSIGNED-PAYLOAD) {}", excerpt(path)),
        &head,
        PAYLOAD,
    )
    .await
}

/// Frame `object` as one signed `aws-chunked` chunk plus the terminating zero-length chunk,
/// chaining each chunk's signature off the seed with the crate's own [`sign_chunk`] — the same
/// function the decoder verifies with, so the two cannot drift.
fn frame_signed(ctx: &StreamingContext, object: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut previous = ctx.seed_signature.clone();
    for data in [object, &[][..]] {
        let sig = sign_chunk(ctx, &previous, data);
        out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", data.len()).as_bytes());
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n");
        previous = sig;
    }
    out
}

/// PUT [`PAYLOAD`] to `path` as an `aws-chunked` upload with signed chunks
/// (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`), the form a stock SDK sends: the seed request signs
/// the sentinel plus `content-encoding` and `x-amz-decoded-content-length`, and the body is
/// framed as a chunk chain seeded by that signature.
async fn put_aws_chunked(addr: SocketAddr, path: &str) -> (u16, String) {
    let host = addr.to_string();
    let amz_date = format_amz_date(SystemTime::now());
    let date = &amz_date[..8];
    let decoded_len = PAYLOAD.len().to_string();
    let extra = [
        ("content-encoding", "aws-chunked"),
        ("x-amz-decoded-content-length", decoded_len.as_str()),
    ];
    let s = sign_with_payload_hash_and_headers(
        "PUT",
        path,
        "",
        &host,
        &amz_date,
        STREAMING_SIGNED_PAYLOAD,
        &extra,
        &credentials(),
        REGION,
        "s3",
    );
    let (_, seed) = s
        .authorization
        .rsplit_once("Signature=")
        .expect("the Authorization header ends in its signature");
    let ctx = StreamingContext {
        seed_signature: seed.to_string(),
        signing_key: signing_key_for(SECRET_KEY, date, REGION, "s3"),
        date_time: amz_date.clone(),
        scope: format!("{date}/{REGION}/s3/aws4_request"),
        signed: true,
        trailer: None,
        decoded_content_length: Some(PAYLOAD.len() as u64),
    };
    let framed = frame_signed(&ctx, PAYLOAD);
    let head = request_head(
        "PUT",
        path,
        &host,
        &[
            ("authorization", &s.authorization),
            ("x-amz-date", &s.amz_date),
            ("x-amz-content-sha256", &s.content_sha256),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", &decoded_len),
        ],
        framed.len(),
    );
    exchange(
        addr,
        &format!("PUT (aws-chunked) {}", excerpt(path)),
        &head,
        &framed,
    )
    .await
}

/// The payload forms of a PUT other than the single-shot signed body [`put`] sends.
#[derive(Clone, Copy, Debug)]
enum PayloadForm {
    /// `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` with `content-encoding: aws-chunked`.
    AwsChunked,
    /// `UNSIGNED-PAYLOAD`.
    Unsigned,
}

async fn put_as(addr: SocketAddr, path: &str, form: PayloadForm) -> (u16, String) {
    match form {
        PayloadForm::AwsChunked => put_aws_chunked(addr, path).await,
        PayloadForm::Unsigned => put_unsigned_payload(addr, path).await,
    }
}

/// Object path for `key` under bucket `b`, percent-encoded the ordinary way: every byte outside
/// SigV4's unreserved set (`A-Z a-z 0-9 - . _ ~`) escaped, `/` included.
fn object_path(key: &str) -> String {
    format!("/{BUCKET}/{}", uri_encode(key, true))
}

/// PUT [`PAYLOAD`] to `path` as a single-shot signed body (`x-amz-content-sha256` = its hash).
async fn put(addr: SocketAddr, path: &str) -> (u16, String) {
    signed(addr, "PUT", path, "", PAYLOAD).await
}

async fn head_status(addr: SocketAddr, key: &str) -> u16 {
    signed(addr, "HEAD", &object_path(key), "", b"").await.0
}

/// POST a bulk DeleteObjects body to bucket `b`.
async fn delete_objects(addr: SocketAddr, xml: &str) -> (u16, String) {
    signed(
        addr,
        "POST",
        &format!("/{BUCKET}"),
        "delete",
        xml.as_bytes(),
    )
    .await
}

/// Seed `b/<key>` through the backend API, outside the S3 surface — how a legacy over-limit
/// object, or one written by `wyrd put --key`, comes to exist.
async fn seed_outside_s3(h: &Harness, key: &str) {
    bounded(
        &format!("seed a {}-byte key through the backend", key.len()),
        h.backend
            .put_object(&format!("{BUCKET}/{key}"), b"seeded outside S3"),
    )
    .await
    .expect("seed through the backend");
}

/// Assert `(status, body)` is a `400` carrying S3 error `code`.
fn assert_refused(what: &str, (status, body): &(u16, String), code: &str) {
    assert_eq!(status, &400, "{what}: expected 400 {code}, body: {body}");
    assert!(
        body.contains(&format!("<Code>{code}</Code>")),
        "{what}: expected S3 error code {code}, body: {body}",
    );
}

/// The first 600 bytes of a string (cut at a character boundary), for an assertion message: a
/// bulk-delete answer echoes every key it deleted, so a whole body can run to megabytes, and a
/// percent-encoded request path to kilobytes.
fn excerpt(text: &str) -> &str {
    let mut end = text.len().min(600);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// (1) A PUT of a 1,025-byte key is refused `KeyTooLongError` before the body is stored.
#[tokio::test]
async fn put_of_an_over_limit_key_is_refused_and_stores_nothing() {
    let h = start_gateway().await;
    // An in-limit control object, so the listing check below is not satisfied by an empty or
    // broken listing.
    let (status, body) = put(h.addr, &object_path("control")).await;
    assert_eq!(status, 200, "fixture PUT: {body}");

    let key = format!("k{}", "a".repeat(1024));
    assert_eq!(key.len(), 1025);
    let resp = put(h.addr, &object_path(&key)).await;
    assert_refused("PUT 1,025-byte key", &resp, "KeyTooLongError");

    assert_eq!(
        head_status(h.addr, &key).await,
        404,
        "a refused PUT must store nothing",
    );
    let (status, listing) = signed(h.addr, "GET", &format!("/{BUCKET}"), "list-type=2", b"").await;
    assert_eq!(status, 200, "listing must answer: {listing}");
    assert!(
        listing.contains("<Key>control</Key>"),
        "the listing must show the in-limit control object: {listing}",
    );
    assert!(
        !listing.contains(&key),
        "a refused PUT must not appear in the listing: {listing}",
    );
}

/// (2) The limit counts UTF-8 bytes of the decoded key alone: not characters, not the
/// percent-encoded wire form, not `bucket/key`.
#[tokio::test]
async fn limit_counts_utf8_bytes_of_the_decoded_key_alone() {
    let h = start_gateway().await;

    // 512 characters, 1,024 bytes: at the limit, accepted, and it round-trips.
    let e_acute = "é".repeat(512);
    assert_eq!((e_acute.chars().count(), e_acute.len()), (512, 1024));
    let (status, body) = put(h.addr, &object_path(&e_acute)).await;
    assert_eq!(status, 200, "a 1,024-byte key must be accepted: {body}");
    assert_eq!(head_status(h.addr, &e_acute).await, 200);

    // 342 characters, 1,026 bytes: over the limit though well under 1,024 characters.
    let euro = "€".repeat(342);
    assert_eq!((euro.chars().count(), euro.len()), (342, 1026));
    let resp = put(h.addr, &object_path(&euro)).await;
    assert_refused("PUT 342 × €", &resp, "KeyTooLongError");
    assert_eq!(head_status(h.addr, &euro).await, 404);

    // A 1,024-byte key sent with every byte percent-encoded: its request path is far past
    // 1,024 bytes, yet the decoded key is at the limit and is accepted. With bucket `b` the
    // flat name `b/<key>` is 1,026 bytes, so this also shows the bucket is not counted.
    let plain = "p".repeat(1024);
    let wire = format!("/{BUCKET}/{}", encode_all(&plain));
    assert!(wire.len() > 3000);
    let (status, body) = put(h.addr, &wire).await;
    assert_eq!(
        status, 200,
        "a 1,024-byte key must be accepted whatever its wire spelling: {body}",
    );
    assert_eq!(head_status(h.addr, &plain).await, 200);
}

/// (3) Bulk delete adds no key-length refusal: a key the existing request limits can carry is
/// deleted, however long. The 70,000-byte key sits past hyper's 65,534-byte request-target
/// ceiling, so its single-object HEAD and DELETE answer `414` before the gateway runs; bulk
/// delete is its only S3 exit, and the backend handle is the only way to read it. Green before
/// and after the fix; RED against a bulk-delete key-length bound (whole-request or per-key).
#[tokio::test]
async fn bulk_delete_removes_over_limit_keys_without_a_length_refusal() {
    let h = start_gateway().await;
    let over_limit = "a".repeat(1025);
    let past_414 = "c".repeat(70_000);
    seed_outside_s3(&h, &over_limit).await;
    seed_outside_s3(&h, &past_414).await;
    let (status, body) = put(h.addr, &object_path("ok")).await;
    assert_eq!(status, 200, "fixture PUT: {body}");

    // All three exist before the delete, so a `<Deleted>` row below is a real removal, not
    // S3's idempotent "deleted" for an absent key.
    assert_eq!(head_status(h.addr, "ok").await, 200);
    assert_eq!(head_status(h.addr, &over_limit).await, 200);
    let flat_past_414 = format!("{BUCKET}/{past_414}");
    assert!(
        bounded(
            "read the 70,000-byte key",
            h.backend.get_object(&flat_past_414)
        )
        .await
        .expect("backend read")
        .is_some(),
        "the 70,000-byte key must exist before the delete",
    );

    let xml = format!(
        "<Delete><Object><Key>ok</Key></Object><Object><Key>{over_limit}</Key></Object>\
         <Object><Key>{past_414}</Key></Object></Delete>"
    );
    let (status, body) = delete_objects(h.addr, &xml).await;
    assert_eq!(
        status,
        200,
        "a bulk delete naming over-limit keys must succeed: {}",
        excerpt(&body),
    );
    for key in ["ok", &over_limit, &past_414] {
        assert!(
            body.contains(&format!("<Deleted><Key>{key}</Key></Deleted>")),
            "the {}-byte key must be reported <Deleted>: {}",
            key.len(),
            excerpt(&body),
        );
    }
    assert!(
        !body.contains("<Error>"),
        "no key may be refused for its length: {}",
        excerpt(&body),
    );

    assert_eq!(head_status(h.addr, "ok").await, 404);
    assert_eq!(
        head_status(h.addr, &over_limit).await,
        404,
        "the 1,025-byte key must actually be gone",
    );
    assert!(
        bounded(
            "re-read the 70,000-byte key",
            h.backend.get_object(&flat_past_414)
        )
        .await
        .expect("backend read")
        .is_none(),
        "the 70,000-byte key must actually be gone",
    );
}

/// (3, at the cap) The LONGEST key bulk delete can carry — one that fills the 8 MiB body cap
/// exactly, as the sole entry of a `<Delete>` document — is deleted like any other: `200`, a
/// `<Deleted>` row, no `<Error>` row, gone afterwards. Every shorter key rides in a smaller
/// body, so a length bound anywhere under the cap goes red here. (Leg 3's 70,000-byte key let a
/// `key.len() > 100_000` check in the parser pass at the iteration-5 sign-off; such a check
/// strands every key between 100,001 and 131,072 bytes that `wyrd put --key` can write.) Seeded
/// through `Gateway::put_object` — no S3 write can produce it — and read back the same way.
#[tokio::test]
async fn bulk_delete_removes_the_longest_key_the_body_cap_can_carry() {
    let h = start_gateway().await;
    let at_cap = "d".repeat(DELETE_BODY_CAP - ONE_KEY_ENVELOPE.len());
    assert_eq!(at_cap.len(), 8_388_563);
    seed_outside_s3(&h, &at_cap).await;
    let flat_at_cap = format!("{BUCKET}/{at_cap}");
    assert!(
        bounded("read the at-cap key", h.backend.get_object(&flat_at_cap))
            .await
            .expect("backend read")
            .is_some(),
        "the at-cap key must exist before the delete",
    );

    let xml = format!("<Delete><Object><Key>{at_cap}</Key></Object></Delete>");
    assert_eq!(
        xml.len(),
        DELETE_BODY_CAP,
        "the one-key body must fill the cap exactly"
    );
    let (status, body) = delete_objects(h.addr, &xml).await;
    assert_eq!(
        status,
        200,
        "a bulk delete of the longest key the cap can carry must succeed: {}",
        excerpt(&body),
    );
    assert!(
        body.contains(&format!("<Deleted><Key>{at_cap}</Key></Deleted>")),
        "the {}-byte key must be reported <Deleted>: {}",
        at_cap.len(),
        excerpt(&body),
    );
    assert!(
        !body.contains("<Error>"),
        "no key may be refused for its length: {}",
        excerpt(&body),
    );
    assert!(
        bounded("re-read the at-cap key", h.backend.get_object(&flat_at_cap))
            .await
            .expect("backend read")
            .is_none(),
        "the at-cap key must actually be gone",
    );
}

/// (4) An over-limit object that already exists (stored outside the S3 surface) stays
/// removable by single-object DELETE. HEAD before and after: DELETE answers `204` for an
/// absent key too, so `204` alone proves nothing.
#[tokio::test]
async fn an_existing_over_limit_object_stays_deletable() {
    let h = start_gateway().await;
    let key = "a".repeat(1025);
    seed_outside_s3(&h, &key).await;

    assert_eq!(
        head_status(h.addr, &key).await,
        200,
        "the seeded object must exist"
    );
    let (status, body) = signed(h.addr, "DELETE", &object_path(&key), "", b"").await;
    assert_eq!(
        status, 204,
        "DELETE of an over-limit key must succeed: {body}"
    );
    assert_eq!(
        head_status(h.addr, &key).await,
        404,
        "the over-limit object must actually be gone",
    );
}

/// (5) A `%2F` in the bucket segment would move part of the key into the bucket and alias
/// `/b/a/<key>` with a shorter request key; a decoded bucket containing `/` is refused.
#[tokio::test]
async fn put_with_a_slash_in_the_decoded_bucket_is_refused() {
    let h = start_gateway().await;
    let tail = "x".repeat(1023);
    let resp = put(h.addr, &format!("/{BUCKET}%2Fa/{tail}")).await;
    assert_refused("PUT /b%2Fa/<1,023 bytes>", &resp, "InvalidBucketName");
    assert_eq!(
        head_status(h.addr, &format!("a/{tail}")).await,
        404,
        "a refused PUT must not store b/a/<key>",
    );
}

/// (6) Both refusals hold in the payload forms legs 1 to 5 do not send: an `aws-chunked`
/// upload with signed chunks (the stock-SDK form) and an `UNSIGNED-PAYLOAD` body. The PUT arm
/// dispatches on the payload form AFTER the key checks, so a check that had slipped inside the
/// signed-payload arm would pass every other leg here while a stock SDK stored a 1,025-byte
/// key (#620 iteration-6 sign-off). Per form: a 1,024-byte key is stored and found (so the
/// form itself is accepted, and a `400` below is the key refusal, not a framing or signature
/// failure), a 1,025-byte key is `400 KeyTooLongError` and absent, and the `%2F`-bucket PUT
/// is `400 InvalidBucketName` and stores nothing under `b/a/`.
#[tokio::test]
async fn refusals_hold_for_aws_chunked_and_unsigned_payload_puts() {
    let h = start_gateway().await;
    let tail = "y".repeat(1023);
    let aliased = format!("/{BUCKET}%2Fa/{tail}");

    for form in [PayloadForm::AwsChunked, PayloadForm::Unsigned] {
        let what = format!("{form:?} PUT");

        // Control: at the limit, stored and found through this form.
        let at_limit = format!("{form:?}-{}", "s".repeat(1024));
        let at_limit = &at_limit[..1024];
        assert_eq!(at_limit.len(), 1024);
        let (status, body) = put_as(h.addr, &object_path(at_limit), form).await;
        assert_eq!(
            status, 200,
            "{what} of a 1,024-byte key must be stored: {body}"
        );
        assert_eq!(
            head_status(h.addr, at_limit).await,
            200,
            "{what} of a 1,024-byte key must be found afterwards",
        );

        let over_limit = format!("{form:?}-{}", "t".repeat(1025));
        let over_limit = &over_limit[..1025];
        assert_eq!(over_limit.len(), 1025);
        let resp = put_as(h.addr, &object_path(over_limit), form).await;
        assert_refused(&format!("{what} 1,025-byte key"), &resp, "KeyTooLongError");
        assert_eq!(
            head_status(h.addr, over_limit).await,
            404,
            "{what} of a 1,025-byte key must store nothing",
        );

        let resp = put_as(h.addr, &aliased, form).await;
        assert_refused(
            &format!("{what} /b%2Fa/<1,023 bytes>"),
            &resp,
            "InvalidBucketName",
        );
        assert_eq!(
            head_status(h.addr, &format!("a/{tail}")).await,
            404,
            "{what} with a `/` in the decoded bucket must not store b/a/<key>",
        );
    }
}
