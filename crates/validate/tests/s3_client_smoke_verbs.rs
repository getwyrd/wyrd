//! Issues #955 and #956 (the two parts of #938, 743.2): the S3 client verbs `smoke` sends beyond
//! PUT, GET and DELETE of one object — HEAD, a ranged and a conditional GET, `ListObjectsV2` and
//! `ListObjects` (#955), bulk `DeleteObjects` and one probe of each operation the capability
//! matrix may declare unsupported (#956) — each reporting what the server sent: status, every
//! header field line, S3 code and message, body.
//!
//! The production [`S3Client`] runs against three kinds of endpoint, all on loopback:
//!
//! * a **real Wyrd S3 gateway** served in-process (redb + `MemCoordination` + a local
//!   `FsChunkStore`, the composition `s3_client_roundtrip.rs` uses), with the bucket's marker
//!   seeded on the store first so listings answer (ADR-0046 decision 1), or not seeded, for a
//!   bucket with no record;
//! * a **relay** in front of that gateway, which records every request's head and body and
//!   counts connections, where a case must show what reached the gateway;
//! * a **scripted endpoint** that reads one whole request and answers with exact bytes, for
//!   the answers no conforming gateway sends (the pattern of
//!   `s3_client_nonconforming_responses.rs`).
//!
//! #955's legs, as its brief names them:
//!
//! * **(A)** against the gateway: HEAD of a written key is `200` with its size and `ETag`, of
//!   a missing key `404` with no body; a ranged GET is `206` with the `Content-Range` as sent
//!   and exactly those bytes; a GET with `If-None-Match` of its `ETag` is `304` with no body,
//!   with another tag `200` and the whole object; both listings return every key under a
//!   prefix with its size across three pages.
//! * **(C)** a listing page answered `200` whose body then stalls past the body-idle
//!   deadline, trickles past the whole-body deadline, is cut mid-body, or is not a
//!   `<ListBucketResult>` is reported with the `200` and the body's failure beside it; one
//!   whose body is an S3 `<Error>` document, with the `200` and that document's code and
//!   message.
//! * **(D)** a listing page that is cut short, followed by junk or a second document,
//!   followed by bytes past its `Content-Length`, or past the buffered budget (with a
//!   `Content-Length` and chunked) is a typed refusal that keeps the `200`, never a partial
//!   listing; a page that fails after a good one discards the good one; a page of exactly the
//!   budget is read; a `<Size>` that is not plain decimal digits (a sign, a space, a
//!   fraction, past `u64`) is refused, as is a page whose `<IsTruncated>` is absent or not
//!   exactly `true` or `false`, or a field given twice or holding markup.
//! * **(G)** against the scripted endpoint: `ListObjects` follows a `<NextMarker>` that is not
//!   the last key, and the last key where the page names none; a truncated page that names
//!   nothing to continue from is refused; a listing still truncated after
//!   `MAX_LISTING_PAGES` pages stops there; a repeated header comes back line by line, one
//!   that is not text included; a `304` to a conditional GET that frames a body with any
//!   coding or declares a `Content-Length` is not refused, nor is a HEAD's answer with any
//!   coding, while framing no server may send on either stays refused.
//! * **(H)** an S3 error through every new verb keeps its status, every header line, its code
//!   and its message: the gateway's own `NoSuchBucket` and `NoSuchKey`, and a scripted `403
//!   AccessDenied`. An `<Error>` with no `<Code>`, a body that is not one whole `<Error>`, and
//!   a HEAD's bodiless `403` are each reported as what they are.
//!
//! Beside them: a request the SDK stops before it calls the connector is `RequestNotBuilt`
//! and reaches no endpoint, while one that reached the endpoint and got no answer stays
//! `NoResponse`; a GET's answer that carries an `x-amz-checksum-*` header is read as sent, and
//! its request asks for no checksum; a HEAD answered with a body keeps its `200` beside the
//! overrun; framing that cannot mark a body's end is refused beside the status, and a GET's 2xx
//! streams only under framing that marks the object's end.
//!
//! #956's legs, as its brief names them:
//!
//! * **(A)** against the gateway: `DeleteObjects` of four keys reports each under `<Deleted>`
//!   and no `<Error>`, and the keys are then gone. 1,000 keys of 32 bytes fit the budget; 1,000
//!   of 40 are refused beside the `200`, after the gateway has deleted them. With a store fault
//!   behind some of the keys, the gateway's own `<Error>` rows: 1,000 keys of 32 bytes fit with
//!   41 of them and are refused beside the `200` with 42. A key holding a carriage return is
//!   deleted, and its row comes back with a line feed in the carriage return's place (the
//!   gateway writes the key raw, and XML reads a raw carriage return as a line feed), so the
//!   requested key has no row and the row names no requested key: reported, not judged.
//! * **(B)** every probe reaches the gateway through the relay, on an IP-address and on a
//!   host-name (`localhost`) endpoint, as exactly one request of its own form (method, path, the
//!   operation parameters it alone carries, with their values, `CopyObject`'s
//!   `x-amz-copy-source`, and its body with the framing off: `CompleteMultipartUpload`'s names
//!   its one part, every other probe's is empty), and comes back with the gateway's status and
//!   S3 code. `CreateBucket` and `DeleteBucket` naming buckets S3's naming rules forbid reach it
//!   as one request with the name as given, one path segment percent-encoded where a byte is not
//!   unreserved, addressed to the endpoint and signed for `s3` in the configured region, on both
//!   endpoints. That includes the names the SDK's own endpoint rules would not send so: S3
//!   Express suffixes (one a host label could carry, one it could not), an S3 on Outposts
//!   access-point alias, and names starting `arn:`; and `.` and `..`, each sent as that segment.
//!   Against the scripted endpoint: an endpoint's path comes before the bucket, with or without
//!   its trailing slash; the region is signed as given. A request that fails before the connector is called (an empty,
//!   all-white-space or overlong name; an endpoint with a query or a scheme other than
//!   `http`/`https`) is `RequestNotBuilt`. One that reached the connector and got no answer is
//!   classified as the older verbs classify it: closed unanswered, reset, a refused connection
//!   and an `https` endpoint are `NoResponse`, the connect deadline `Timeout { Connect }`, the
//!   operation deadline `Timeout { Operation }`. A probe's key and upload id too long for a URI
//!   are `RequestNotBuilt` and send nothing, where the SDK would panic; just inside that limit
//!   they go on to the SDK.
//! * **(C)** a probe or a bulk delete answered `200` whose body then stalls past the body-idle
//!   deadline, trickles past the whole-body deadline, is cut mid-body, or is not the operation's
//!   success body, is reported with the `200` and the body's failure beside it; a `200` whose
//!   body is an S3 `<Error>` document is reported with its code and message beside the `200`.
//! * **(D)** a `<DeleteResult>` refused by the rules a listing page is, and one of exactly the
//!   budget read; a row that names no `<Key>` or an empty one, or gives a field twice or with
//!   markup, is refused, and so is anything the root holds beside its rows but white space and
//!   comments, a processing instruction included. A document probe's `200` whose body is one
//!   byte past the budget, with a `Content-Length` or chunked, is refused beside the `200`, and
//!   one of exactly the budget is its success.
//! * **(F)** a mixed `<DeleteResult>` (two `<Deleted>`, one `<Error>`) comes back row by row,
//!   the error row's key, code and message as sent, in document order. One that is empty, names
//!   only some of the requested keys, or names a key not requested comes back as a result that
//!   reports which keys have no row and which rows name no requested key, and judges neither;
//!   a row's key is read with XML's line endings (a raw carriage return as a line feed, `&#13;`
//!   as the carriage return) before it is compared.
//! * **(G)** a `200` (`204` where S3 answers so) carrying the operation's whole success document
//!   or, where the success has none, no body, is a success, once the request the endpoint read
//!   is shown to be the probe's own form; a `304` that frames a chunked body is refused only for
//!   a probe that is not a GET or a HEAD, and for a bulk delete, a POST.
//!
//! (E) of both briefs is the three existing client tests, run unmodified beside this one.
//!
//! RED before #955 and #956: none of these verbs exist, so the file does not compile.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::task::JoinSet;
use tokio::time::timeout;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::Credentials as GatewayCredentials;
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;
use wyrd_traits::{MetadataStore, WriteBatch};
use wyrd_validate::s3::{
    Answer, BodyError, BodyFailure, DeleteResult, DeleteRow, Fetched, KeyError, ListEntry, Listing,
    ListingError, NoResult, Outcome, Probe, S3Error, BUFFERED_BODY_BUDGET, MAX_LISTING_PAGES,
};
use wyrd_validate::{
    resolve_config, Deadlines, ErrorCode, ObjectBody, Phase, PutSource, ResolvedConfig, S3Client,
};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "validate";

/// The gateway's chunk size: small, so a range and an object span several chunks.
const GATEWAY_CHUNK: usize = 1024;
/// The length of the object the HEAD and GET cases write: five chunks.
const OBJECT_LEN: usize = 5000;

/// A bucket name S3's naming rules forbid (it ends in `-`).
const FORBIDDEN_BUCKET: &str = "wyrd-validate-probe-";
/// More names those rules forbid, each with the path segment it reaches the endpoint as:
/// upper case and an underscore, too short, an IP address, adjacent dots, a leading hyphen,
/// a space (percent-encoded); and the names the SDK's own endpoint rules would not send as
/// one path segment on the endpoint, which this client sends as every other:
///
/// * the S3 Express directory-bucket and access-point suffixes, `--x-s3` and `--xa-s3`, one
///   name a host label could carry and one it could not: the rules sign them for the
///   `s3express` service, send them virtual-host style on an endpoint that is a host name,
///   whatever path-style is set to, and refuse the one no host label can carry;
/// * an S3 on Outposts access-point alias, which the rules resolve to an `amazonaws.com`
///   https URL, whatever the endpoint;
/// * names that start with `arn:`, a well-formed ARN and one that is not, which the rules
///   read as an ARN and refuse, path-style, before anything is sent;
/// * the dot segments `.` and `..`, sent as they are: what they mean is the server's to decide.
const MORE_FORBIDDEN_BUCKETS: [(&str, &str); 17] = [
    ("Wyrd_Validate_Probe", "Wyrd_Validate_Probe"),
    ("ab", "ab"),
    ("192.168.5.4", "192.168.5.4"),
    ("wyrd..validate", "wyrd..validate"),
    ("-wyrd-validate", "-wyrd-validate"),
    ("wyrd validate", "wyrd%20validate"),
    // One segment, encoded once: a `/` cannot start another, and a `%` is no escape of its own.
    ("wyrd/validate", "wyrd%2Fvalidate"),
    ("wyrd%41validate", "wyrd%2541validate"),
    // Bytes, not characters: `é` is two.
    ("wyrd-é-validate", "wyrd-%C3%A9-validate"),
    ("wyrd-validate--x-s3", "wyrd-validate--x-s3"),
    ("Wyrd_Validate--x-s3", "Wyrd_Validate--x-s3"),
    ("wyrd-validate--xa-s3", "wyrd-validate--xa-s3"),
    (OUTPOSTS_ALIAS, OUTPOSTS_ALIAS),
    (
        "arn:aws:s3:::wyrd-validate-probe",
        "arn%3Aaws%3As3%3A%3A%3Awyrd-validate-probe",
    ),
    ("arn:wyrd-validate-probe", "arn%3Awyrd-validate-probe"),
    (".", "."),
    ("..", ".."),
];
/// An S3 on Outposts access-point alias, in the shape the SDK's endpoint tests give one
/// (`aws-sdk-s3` 1.155.0 `config/endpoint.rs:6438`).
const OUTPOSTS_ALIAS: &str = "test-accessp-o0b1d075431d83bebde8xz5w8ijx1qzlbp3i3kuse10--op-s3";

/// The test's own bound on any one call. Longer than every client deadline, so a production
/// deadline fires first where one applies.
const STALL: Duration = Duration::from_secs(30);
/// How long the scripted endpoint holds a connection open waiting for the client to close it.
const HOLD: Duration = Duration::from_secs(10);

/// Deadlines for the gateway cases: generous, so only a hang trips them.
fn deadlines() -> Deadlines {
    Deadlines {
        connect: Duration::from_secs(5),
        operation: Duration::from_secs(60),
        body_idle: Duration::from_secs(10),
        body: Duration::from_secs(60),
    }
}

/// The body deadlines the 2xx-head cases (C) expire: short, and the idle one shorter. The
/// trickle's gap ([`TRICKLE_EVERY`]) is a tenth of the idle deadline, so a loaded runner does
/// not turn the whole-body expiry into an idle one.
const SHORT_IDLE: Duration = Duration::from_millis(500);
const SHORT_BODY: Duration = Duration::from_millis(1500);
/// The gap between two trickled bytes.
const TRICKLE_EVERY: Duration = Duration::from_millis(50);

fn short_deadlines() -> Deadlines {
    Deadlines {
        connect: Duration::from_secs(5),
        operation: Duration::from_secs(30),
        body_idle: SHORT_IDLE,
        body: SHORT_BODY,
    }
}

// --- the in-process gateway ----------------------------------------------------------------

/// A Wyrd S3 gateway on an ephemeral loopback port. Field order is drop order: the serving
/// task is aborted before its chunk store's directory is removed.
struct GatewayFixture {
    _tasks: JoinSet<()>,
    addr: SocketAddr,
    _dir: tempfile::TempDir,
}

/// Seed a `bucket:{name}` existence marker directly on the store, as
/// `crates/server/tests/s3_list_objects.rs` does: a listing reads the marker for presence only.
async fn seed_bucket(meta: &RedbMetadataStore, name: &str) {
    let key = format!("bucket:{name}").into_bytes();
    let value = format!("{{\"name\":\"{name}\",\"created_millis\":0}}").into_bytes();
    meta.commit(WriteBatch::new().put(key, value))
        .await
        .expect("commit bucket marker");
}

/// As `s3_client_roundtrip.rs`'s `start_gateway_with_chunk`, with [`BUCKET`]'s marker seeded
/// before the store goes into the gateway.
async fn start_gateway() -> GatewayFixture {
    start_gateway_seeding(&[BUCKET]).await
}

/// As [`start_gateway`], with the markers of `buckets` seeded: none at all, for a gateway on
/// which [`BUCKET`] has no record.
async fn start_gateway_seeding(buckets: &[&str]) -> GatewayFixture {
    let meta = RedbMetadataStore::in_memory().expect("redb");
    for bucket in buckets {
        seed_bucket(&meta, bucket).await;
    }
    start_gateway_on(meta).await
}

/// As [`start_gateway`], on a store the test has already written to.
async fn start_gateway_on(meta: RedbMetadataStore) -> GatewayFixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Arc::new(
        Gateway::new(
            meta,
            FsChunkStore::open(dir.path()).expect("fs store"),
            MemCoordination::new(),
        )
        .with_chunk_size(GATEWAY_CHUNK),
    );
    let config = S3Config::new(vec![GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(gateway, config);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    GatewayFixture {
        _tasks: tasks,
        addr,
        _dir: dir,
    }
}

// --- the client, built the way the binary builds it ----------------------------------------

/// A resolved configuration for the `--endpoint` URL `endpoint` and the `--region` `region`,
/// through the production argument parser and credential resolution.
fn resolved(endpoint: &str, region: &str) -> ResolvedConfig {
    let pairs = [
        ("--endpoint", endpoint),
        ("--region", region),
        ("--bucket", BUCKET),
        ("--scenario", "smoke"),
        ("--duration", "1m"),
        ("--workers", "1"),
        ("--seed", "938"),
        ("--out", "unused"),
        ("--run-id", "s3-client-smoke-verbs"),
        ("--driver-placement", "loopback"),
    ];
    let args: Vec<String> = pairs
        .iter()
        .flat_map(|(flag, value)| [flag.to_string(), value.to_string()])
        .collect();
    let lookup = |name: &str| -> Option<OsString> {
        match name {
            "AWS_ACCESS_KEY_ID" => Some(ACCESS_KEY.into()),
            "AWS_SECRET_ACCESS_KEY" => Some(SECRET_KEY.into()),
            _ => None,
        }
    };
    resolve_config(&args, &lookup).expect("the configuration resolves")
}

fn client(endpoint: SocketAddr, deadlines: Deadlines) -> S3Client {
    client_at(&format!("http://{endpoint}"), deadlines)
}

/// A client for the `--endpoint` URL `endpoint`, as the operator would type it.
fn client_at(endpoint: &str, deadlines: Deadlines) -> S3Client {
    S3Client::with_deadlines(&resolved(endpoint, REGION), deadlines)
}

/// `f`, bounded by [`STALL`].
async fn bounded<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
    timeout(STALL, f)
        .await
        .unwrap_or_else(|_| panic!("{what}: no outcome within {STALL:?}"))
}

/// The bytes of a test object of `len` bytes, distinct per offset.
fn object(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// PUT `body` at `key`, returning the `ETag` the gateway gave it.
async fn put(client: &S3Client, key: &str, body: &[u8]) -> String {
    let piece = Bytes::copy_from_slice(body);
    let source = PutSource::new(
        body.len() as u64,
        stream::iter([Ok::<_, std::io::Error>(piece)]),
    );
    bounded(key, client.put_object(key, source))
        .await
        .unwrap_or_else(|e| panic!("PUT {key}: {e}"))
        .etag
        .unwrap_or_else(|| panic!("PUT {key}: the gateway returned no ETag"))
}

/// Every byte of a streamed body.
async fn read_all(body: &mut ObjectBody) -> Vec<u8> {
    let mut held = Vec::new();
    while let Some(piece) = bounded("a GET body", body.next_piece())
        .await
        .unwrap_or_else(|e| panic!("the GET body failed: {e}"))
    {
        held.extend_from_slice(&piece);
    }
    held
}

/// The id is the gateway's: 32 hex digits, the form it stamps on every response.
fn assert_gateway_answered(answer: &Answer, case: &str) {
    let id = answer
        .head
        .request_id()
        .unwrap_or_else(|| panic!("{case}: the answer carries no x-amz-request-id: {answer:?}"));
    assert!(
        id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
        "{case}: the gateway's request-id form (32 hex digits): {id:?}"
    );
}

// --- (A) the verbs against the real gateway ------------------------------------------------

#[tokio::test]
async fn head_reports_a_written_keys_size_and_etag_and_a_missing_keys_bare_404() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr, deadlines());
    let etag = put(&client, "head/written", &object(OBJECT_LEN)).await;

    let written = bounded("HEAD written", client.head_object("head/written"))
        .await
        .unwrap_or_else(|e| panic!("HEAD written: no answer: {e}"));
    assert_eq!(written.head.status, 200, "{written:?}");
    let size = OBJECT_LEN.to_string();
    assert_eq!(
        written.head.header("content-length"),
        Some(size.as_str()),
        "the size: {written:?}"
    );
    assert_eq!(
        written.head.header("etag"),
        Some(etag.as_str()),
        "the ETag the PUT returned: {written:?}"
    );
    assert_eq!(
        written.outcome,
        Outcome::Success { body: Bytes::new() },
        "a HEAD success has no body"
    );
    assert_gateway_answered(&written, "HEAD written");

    let missing = bounded("HEAD missing", client.head_object("head/missing"))
        .await
        .unwrap_or_else(|e| panic!("HEAD missing: no answer: {e}"));
    assert_eq!(missing.head.status, 404, "{missing:?}");
    assert_eq!(
        missing.outcome,
        Outcome::Error {
            code: ErrorCode::NoBody,
            message: None
        },
        "a HEAD error has no body, so no code: the SDK's made-up NotFound is not reported"
    );
    assert_gateway_answered(&missing, "HEAD missing");
}

#[tokio::test]
async fn a_ranged_get_is_206_with_its_content_range_and_exactly_those_bytes() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr, deadlines());
    let body = object(OBJECT_LEN);
    put(&client, "range/object", &body).await;
    // Across two chunk boundaries (1024 and 2048).
    let (first, last) = (1000u64, 2099u64);

    let fetched = bounded(
        "ranged GET",
        client.get_object_range("range/object", first, last),
    )
    .await
    .unwrap_or_else(|e| panic!("ranged GET: no answer: {e}"));
    let shown = format!("{fetched:?}");
    assert!(
        shown.starts_with("Object {") && shown.contains("streaming"),
        "a 2xx is shown as a streaming object: {shown}"
    );
    let Fetched::Object { head, body: got } = fetched else {
        panic!("ranged GET: not a 2xx: {fetched:?}");
    };
    assert_eq!(head.status, 206, "{head:?}");
    let range = format!("bytes {first}-{last}/{OBJECT_LEN}");
    assert_eq!(
        head.header("content-range"),
        Some(range.as_str()),
        "the Content-Range as sent: {head:?}"
    );
    let mut got = got.unwrap_or_else(|e| panic!("ranged GET: the body cannot be read: {e}"));
    assert_eq!(got.content_length(), Some(last - first + 1));
    assert!(
        read_all(&mut got).await == body[first as usize..=last as usize],
        "the ranged GET handed over exactly bytes {first}-{last}"
    );
}

#[tokio::test]
async fn a_conditional_get_is_304_on_its_etag_and_the_whole_object_on_another() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr, deadlines());
    let body = object(OBJECT_LEN);
    let etag = put(&client, "conditional/object", &body).await;

    let matched = bounded(
        "GET If-None-Match its ETag",
        client.get_object_if_none_match("conditional/object", &etag),
    )
    .await
    .unwrap_or_else(|e| panic!("GET If-None-Match its ETag: no answer: {e}"));
    let shown = format!("{matched:?}");
    assert!(shown.starts_with("Other("), "{shown}");
    let Fetched::Other(not_modified) = matched else {
        panic!("GET If-None-Match its ETag: a 2xx: {matched:?}");
    };
    assert_eq!(not_modified.head.status, 304, "{not_modified:?}");
    assert_eq!(
        not_modified.outcome,
        Outcome::Error {
            code: ErrorCode::NoBody,
            message: None
        },
        "a 304 has no body"
    );
    assert_gateway_answered(&not_modified, "GET If-None-Match its ETag");

    // A valid tag, but not this object's.
    let other = "\"0123456789abcdef0123456789abcdef\"";
    assert_ne!(etag, other);
    let fetched = bounded(
        "GET If-None-Match another ETag",
        client.get_object_if_none_match("conditional/object", other),
    )
    .await
    .unwrap_or_else(|e| panic!("GET If-None-Match another ETag: no answer: {e}"));
    let Fetched::Object { head, body: got } = fetched else {
        panic!("GET If-None-Match another ETag: not a 2xx: {fetched:?}");
    };
    assert_eq!(head.status, 200, "{head:?}");
    let mut got = got.unwrap_or_else(|e| panic!("the body cannot be read: {e}"));
    assert!(
        read_all(&mut got).await == body,
        "a GET that matched no tag handed over the whole object"
    );
}

/// The keys the listing cases write under `list/`, with their sizes, in the order S3 lists
/// them; and one outside the prefix.
const LISTED: [(&str, usize); 5] = [
    ("list/a", 0),
    ("list/b", 1),
    ("list/c", 10),
    ("list/d", 100),
    ("list/e", 3000),
];

fn expected_entries() -> Vec<(String, u64)> {
    LISTED
        .iter()
        .map(|&(key, size)| (key.to_string(), size as u64))
        .collect()
}

fn keys_and_sizes(listing: &Listing) -> Vec<(String, u64)> {
    listing
        .entries
        .iter()
        .map(|entry| (entry.key.clone(), entry.size))
        .collect()
}

#[tokio::test]
async fn both_listings_return_every_key_under_a_prefix_with_its_size_across_pages() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr, deadlines());
    for (key, size) in LISTED {
        put(&client, key, &object(size)).await;
    }
    put(&client, "listless/outside", b"not under the prefix").await;

    // Two keys a page: three pages, the client following each continuation.
    let v2 = bounded("ListObjectsV2", client.list_objects_v2("list/", 2))
        .await
        .unwrap_or_else(|e| panic!("ListObjectsV2: {e}"));
    assert_eq!(keys_and_sizes(&v2), expected_entries(), "ListObjectsV2");
    assert_eq!(v2.pages.len(), 3, "ListObjectsV2 read three pages");
    assert!(v2.pages.iter().all(|head| head.status == 200));

    let v1 = bounded("ListObjects", client.list_objects_v1("list/", 2))
        .await
        .unwrap_or_else(|e| panic!("ListObjects: {e}"));
    assert_eq!(keys_and_sizes(&v1), expected_entries(), "ListObjects");
    assert_eq!(v1.pages.len(), 3, "ListObjects read three pages");
    assert!(v1.pages.iter().all(|head| head.status == 200));
}

// --- #956 (A): bulk delete against the real gateway -----------------------------------------

#[tokio::test]
async fn delete_objects_reports_every_key_deleted_and_the_keys_are_gone() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr, deadlines());
    let keys = ["bulk/0", "bulk/1", "bulk/2", "bulk/3"];
    for key in keys {
        put(&client, key, key.as_bytes()).await;
    }

    let result = bounded("DeleteObjects", client.delete_objects(&keys))
        .await
        .unwrap_or_else(|e| panic!("DeleteObjects: {e}"));
    assert_eq!(result.head.status, 200, "{result:?}");
    let mut deleted: Vec<&str> = result.deleted().collect();
    deleted.sort_unstable();
    assert_eq!(deleted, keys, "every key under <Deleted>: {result:?}");
    assert_eq!(result.errors().count(), 0, "no <Error> row: {result:?}");
    assert_eq!(result.requested, keys, "{result:?}");
    assert_eq!(
        result.keys_without_a_row().count(),
        0,
        "every key has its row: {result:?}"
    );
    assert_eq!(
        result.rows_not_requested().count(),
        0,
        "every row names a requested key: {result:?}"
    );

    for key in keys {
        let head = bounded(key, client.head_object(key))
            .await
            .unwrap_or_else(|e| panic!("HEAD {key}: no answer: {e}"));
        assert_eq!(head.head.status, 404, "{key} is gone: {head:?}");
    }
}

/// Keys XML's line-ending rule rewrites, each with what a row naming it reads as: a raw carriage
/// return, alone or before a line feed, is one line feed (XML 1.0 §2.11). The gateway writes a
/// row's key into its `<DeleteResult>` raw (`crates/gateway-s3/src/lib.rs`, `xml_escape`
/// escapes the five predefined entities and nothing else), so the client reads these two back
/// with a line feed in the carriage return's place. Fixing the gateway to escape the carriage
/// return (`&#13;`) would make each row name its key, and this test would say so.
const CR_KEYS: [(&str, &str); 2] = [("cr\rkey", "cr\nkey"), ("crlf\r\nkey", "crlf\nkey")];

#[tokio::test]
async fn a_key_holding_a_carriage_return_is_deleted_but_its_row_comes_back_with_a_line_feed() {
    let gateway = start_gateway().await;
    let client = client(gateway.addr, deadlines());
    let keys: Vec<&str> = CR_KEYS.iter().map(|&(sent, _)| sent).collect();
    let mut read_as: Vec<&str> = CR_KEYS.iter().map(|&(_, read)| read).collect();
    read_as.sort_unstable();
    for key in &keys {
        put(&client, key, key.as_bytes()).await;
    }

    let result = bounded("DeleteObjects", client.delete_objects(&keys))
        .await
        .unwrap_or_else(|e| panic!("DeleteObjects: {e}"));
    assert_eq!(result.head.status, 200, "{result:?}");
    assert_eq!(result.requested, keys, "{result:?}");
    assert_eq!(result.errors().count(), 0, "no <Error> row: {result:?}");
    let mut deleted: Vec<&str> = result.deleted().collect();
    deleted.sort_unstable();
    assert_eq!(
        deleted, read_as,
        "each row names the key with a line feed where the carriage return was: {result:?}"
    );
    // What the comparison then finds, reported and not judged.
    assert_eq!(
        result.keys_without_a_row().collect::<Vec<_>>(),
        keys,
        "the keys as requested have no row: {result:?}"
    );
    let mut not_requested: Vec<&str> = result.rows_not_requested().map(DeleteRow::key).collect();
    not_requested.sort_unstable();
    assert_eq!(
        not_requested, read_as,
        "every row names a key that was not requested: {result:?}"
    );

    // The gateway deleted the objects all the same: the request named the keys with the
    // carriage return escaped, and only the answer lost it.
    for key in &keys {
        let head = bounded(key, client.head_object(key))
            .await
            .unwrap_or_else(|e| panic!("HEAD {key:?}: no answer: {e}"));
        assert_eq!(head.head.status, 404, "{key:?} is gone: {head:?}");
    }
}

/// `count` keys of exactly `len` bytes.
fn keys_of(count: usize, len: usize) -> Vec<String> {
    (0..count).map(|i| format!("{i:0len$}")).collect()
}

/// A result's size as the gateway declared it.
fn declared_length(result: &DeleteResult) -> Option<&str> {
    result.head.header("content-length")
}

/// A delete result refused for passing the budget, its `200` kept, and the size the gateway
/// declared for it. How many bytes were held when it was refused depends on how the response
/// split over TCP, so that is only checked to be past the budget.
fn assert_refused_over_budget_beside_its_200(
    case: &str,
    refused: &Result<DeleteResult, NoResult>,
    declared: &str,
) {
    match refused {
        Err(NoResult::Answered(Answer {
            head,
            outcome: Outcome::BodyFailed(BodyFailure::OverBudget { received }),
        })) if head.status == 200 && *received > BUFFERED_BODY_BUDGET => {
            assert_eq!(head.header("content-length"), Some(declared), "{case}");
        }
        other => panic!("{case}: not refused beside the 200: {other:?}"),
    }
}

#[tokio::test]
async fn a_bulk_delete_whose_result_passes_the_budget_keeps_its_200_after_the_gateway_deleted_the_keys(
) {
    let gateway = start_gateway().await;
    let client = client(gateway.addr, deadlines());

    // The sizes the docs give: 115 bytes, 30 a row, and the key as XML escapes it.
    let result = bounded("DeleteObjects", client.delete_objects(&["plain", "a&b"]))
        .await
        .unwrap_or_else(|e| panic!("two keys: {e}"));
    let escaped = 115 + 2 * 30 + "plain".len() + "a&amp;b".len();
    assert_eq!(
        declared_length(&result),
        Some(escaped.to_string().as_str()),
        "{result:?}"
    );

    // Only the result's size is at stake here: a key with no object has its row too.
    let fits = keys_of(1000, 32);
    for key in fits.iter().step_by(50) {
        put(&client, key, b"x").await;
    }
    let result = bounded("1,000 keys of 32 bytes", client.delete_objects(&fits))
        .await
        .unwrap_or_else(|e| panic!("1,000 keys of 32 bytes: {e}"));
    assert_eq!(declared_length(&result), Some("62115"), "{:?}", result.head);
    assert_eq!(result.deleted().count(), 1000);
    assert_eq!(result.errors().count(), 0);
    assert_eq!(result.keys_without_a_row().count(), 0);

    // Sampled as above: the size comes from the gateway's `Content-Length`, and a key with no
    // object has its row too. Every PUT and HEAD is a fresh connection, which the client closes,
    // so 1,000 of each would leave about 2,000 sockets in `TIME_WAIT` and can run the host out
    // of ephemeral ports when many runs share it.
    let over = keys_of(1000, 40);
    for key in over.iter().step_by(50) {
        put(&client, key, b"x").await;
    }
    let refused = bounded("1,000 keys of 40 bytes", client.delete_objects(&over)).await;
    assert_refused_over_budget_beside_its_200("1,000 keys of 40 bytes", &refused, "70115");
    for key in over.iter().step_by(50) {
        let head = bounded(key, client.head_object(key))
            .await
            .unwrap_or_else(|e| panic!("HEAD {key}: no answer: {e}"));
        assert_eq!(
            head.head.status, 404,
            "{key} is gone, every row lost: {head:?}"
        );
    }
}

/// The first [`DOOMED`] keys of the test below have a directory entry the store cannot decode.
const DOOMED: usize = 42;

/// What the gateway's own `<Error>` rows cost, from a real fault in the real store: the
/// gateway's `delete_object` cannot decode the entry, and its `classify` answers `InternalError`
/// in the row its `render_delete_result` writes.
#[tokio::test]
async fn a_bulk_delete_with_failed_keys_fits_the_budget_with_41_internal_errors_and_not_42() {
    let keys = keys_of(1001, 32);
    let meta = RedbMetadataStore::in_memory().expect("redb");
    seed_bucket(&meta, BUCKET).await;
    for key in &keys[..DOOMED] {
        // Inode 0 is the gateway's root (`ROOT`, `crates/server/src/lib.rs`).
        let entry = wyrd_core::metadata::dirent_key(0, &format!("{BUCKET}/{key}"));
        meta.commit(WriteBatch::new().put(entry, b"not a directory entry".to_vec()))
            .await
            .expect("commit the undecodable entry");
    }
    let gateway = start_gateway_on(meta).await;
    let client = client(gateway.addr, deadlines());

    // The sizes the docs give: 115 bytes, the key in each row, 30 for a `<Deleted>` row and
    // 113 for an `<Error>` row.
    let result = bounded(
        "one of each",
        client.delete_objects(&[&keys[1], &keys[DOOMED]]),
    )
    .await
    .unwrap_or_else(|e| panic!("one of each: {e}"));
    let one_of_each = 115 + 30 + 113 + 2 * 32;
    assert_eq!(
        declared_length(&result),
        Some(one_of_each.to_string().as_str()),
        "{result:?}"
    );
    assert_eq!(
        result.rows,
        [
            deleted_row(&keys[DOOMED]),
            DeleteRow::Error(KeyError {
                key: keys[1].clone(),
                code: ErrorCode::Code("InternalError".to_string()),
                message: Some("the gateway could not complete the request".to_string()),
            }),
        ]
    );

    // 41 doomed keys of the 1,000 requested: 62,115 + 41 x 83 bytes.
    let result = bounded("41 error rows", client.delete_objects(&keys[1..]))
        .await
        .unwrap_or_else(|e| panic!("41 error rows: {e}"));
    assert_eq!(result.head.status, 200);
    assert_eq!(declared_length(&result), Some("65518"), "{:?}", result.head);
    assert_eq!(result.deleted().count(), 959);
    assert_eq!(result.errors().count(), 41);
    assert_eq!(result.keys_without_a_row().count(), 0);
    assert!(
        result
            .errors()
            .all(|row| row.code == ErrorCode::Code("InternalError".to_string())),
        "{:?}",
        result.rows
    );

    // 42 of the 1,000: 62,115 + 42 x 83 bytes, one row more than fits, and all of them lost.
    let deleted_anyway = &keys[DOOMED];
    put(&client, deleted_anyway, b"x").await;
    let refused = bounded("42 error rows", client.delete_objects(&keys[..1000])).await;
    assert_refused_over_budget_beside_its_200("42 error rows", &refused, "65601");
    let head = bounded(deleted_anyway, client.head_object(deleted_anyway))
        .await
        .unwrap_or_else(|e| panic!("HEAD {deleted_anyway}: no answer: {e}"));
    assert_eq!(
        head.head.status, 404,
        "a key with no fault is gone, its row lost: {head:?}"
    );
}

// --- what a request carried ----------------------------------------------------------------

/// A request head's method, path and query.
fn request_line(head: &str) -> (&str, &str, &str) {
    let line = head.split("\r\n").next().unwrap_or_default();
    let mut parts = line.split(' ');
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    (method, path, query)
}

/// Whether a query names `name`, bare or with a value.
fn query_has(query: &str, name: &str) -> bool {
    query
        .split('&')
        .any(|pair| pair == name || pair.starts_with(&format!("{name}=")))
}

/// A request header's value, by name, from a recorded head.
fn request_header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.split("\r\n").skip(1).find_map(|line| {
        let (field, value) = line.split_once(':')?;
        field
            .trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim())
    })
}

// --- the relay (#956) -----------------------------------------------------------------------------

/// A loopback forwarder in front of the gateway that records every request it forwards (the
/// client opens one connection per request), and counts the connections it accepts.
struct Relay {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Request>>>,
    accepted: Arc<AtomicUsize>,
    _task: JoinSet<()>,
}

impl Relay {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("relay bind");
        let addr = listener.local_addr().expect("relay addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&accepted);
        let mut task = JoinSet::new();
        task.spawn(async move {
            // Owned by the accept loop, so every connection's task goes with the relay.
            let mut connections = JoinSet::new();
            while let Ok((client, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                connections.spawn(forward(client, upstream, Arc::clone(&recorded)));
            }
        });
        Self {
            addr,
            requests,
            accepted,
            _task: task,
        }
    }

    /// The requests recorded since the last call.
    fn take_requests(&self) -> Vec<Request> {
        std::mem::take(&mut *self.requests.lock().expect("relay lock"))
    }

    /// The heads of the requests recorded since the last call.
    fn take(&self) -> Vec<String> {
        self.take_requests()
            .into_iter()
            .map(|request| request.head)
            .collect()
    }

    /// How many connections the relay has accepted.
    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

/// Forward one connection, recording its request whole before any of it reaches the gateway.
async fn forward(mut client: TcpStream, upstream: SocketAddr, requests: Arc<Mutex<Vec<Request>>>) {
    let Ok(mut server) = TcpStream::connect(upstream).await else {
        return;
    };
    let Ok((request, bytes)) = read_request(&mut client).await else {
        return;
    };
    requests.lock().expect("relay lock").push(request);
    if server.write_all(&bytes).await.is_err() {
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
}

// --- #956 (B): every unsupported operation reaches the gateway, as its own request -----------

/// The query parameters that tell S3 operations apart. Each request carries exactly its own,
/// with the values given, and none of another's; other parameters the SDK adds (`x-id`) are not
/// the operation's, and are ignored.
const OPERATION_PARAMS: [&str; 5] = ["uploads", "uploadId", "partNumber", "location", "delete"];

/// The operation parameters `query` carries, each with its decoded value (a bare one has an empty
/// value), sorted; one given twice is there twice.
fn operation_params(query: &str) -> Vec<(String, String)> {
    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter_map(|pair| {
            let (field, value) = pair.split_once('=').unwrap_or((pair, ""));
            OPERATION_PARAMS
                .contains(&field)
                .then(|| (field.to_string(), percent_decoded(value)))
        })
        .collect();
    params.sort();
    params
}

/// What a probe's request body must be, the framing off.
#[derive(Clone, Copy)]
enum SentBody {
    /// Nothing. A `CreateBucket` with a `<CreateBucketConfiguration>`, or an `UploadPart` of a
    /// part with bytes in it, is another request than the probe's.
    Empty,
    /// A `<CompleteMultipartUpload>` naming the one empty part, and nothing else.
    TheEmptyPart,
}

impl SentBody {
    /// What is wrong with `payload` as this body, when anything is.
    fn fault(self, payload: &[u8]) -> Option<String> {
        let text = String::from_utf8_lossy(payload);
        match self {
            Self::Empty => {
                (!payload.is_empty()).then(|| format!("its body is {text:?}, not empty"))
            }
            Self::TheEmptyPart => {
                let names_the_part = (|| {
                    let document = roxmltree::Document::parse(&text).ok()?;
                    let root = document.root_element();
                    if root.tag_name().name() != "CompleteMultipartUpload" {
                        return Some(false);
                    }
                    let parts = elements(root);
                    let [part] = parts.as_slice() else {
                        return Some(false);
                    };
                    let mut fields: Vec<(&str, &str)> = elements(*part)
                        .iter()
                        .map(|field| (field.tag_name().name(), field.text().unwrap_or_default()))
                        .collect();
                    fields.sort_unstable();
                    Some(
                        part.tag_name().name() == "Part"
                            && fields == [("ETag", EMPTY_PART_ETAG), ("PartNumber", "1")],
                    )
                })();
                (names_the_part != Some(true))
                    .then(|| format!("its body is {text:?}, not the one empty part"))
            }
        }
    }
}

/// The child elements of `node`.
fn elements<'a, 'input>(node: roxmltree::Node<'a, 'input>) -> Vec<roxmltree::Node<'a, 'input>> {
    node.children()
        .filter(roxmltree::Node::is_element)
        .collect()
}

/// How a probe must reach the endpoint and what the gateway answers it.
struct ProbeCase {
    probe: Probe,
    method: &'static str,
    path: String,
    /// Every operation parameter the request must carry, with its value, sorted by name.
    params: Vec<(String, String)>,
    /// The `x-amz-copy-source` the request must carry; `None` for a request that carries none.
    copy_source: Option<&'static str>,
    /// What the request's body must be, the framing off.
    body: SentBody,
    status: u16,
    code: ErrorCode,
}

impl ProbeCase {
    /// What is wrong with `head` as the request of this probe, when anything is.
    fn request_fault(&self, head: &str) -> Option<String> {
        let (method, path, query) = request_line(head);
        if (method, path) != (self.method, self.path.as_str()) {
            return Some(format!(
                "sent as {method} {path}, not {} {}: {head}",
                self.method, self.path
            ));
        }
        let params = operation_params(query);
        if params != self.params {
            return Some(format!(
                "its operation parameters are {params:?}, not {:?}: {head}",
                self.params
            ));
        }
        let copy_source = request_header(head, "x-amz-copy-source");
        if copy_source != self.copy_source {
            return Some(format!(
                "its x-amz-copy-source is {copy_source:?}, not {:?}: {head}",
                self.copy_source
            ));
        }
        None
    }

    /// What is wrong with `request` as the request of this probe, its body included, when
    /// anything is.
    fn sent_fault(&self, request: &Request) -> Option<String> {
        self.request_fault(&request.head)
            .or_else(|| self.body.fault(&request.body))
    }
}

fn not_implemented() -> ErrorCode {
    ErrorCode::Code("NotImplemented".to_string())
}

fn invalid_request() -> ErrorCode {
    ErrorCode::Code("InvalidRequest".to_string())
}

/// The upload id every multipart probe names.
const UPLOAD_ID: &str = "wyrd-validate-probe-upload";

fn probe_cases() -> Vec<ProbeCase> {
    let key = "probe/object".to_string();
    let upload_id = UPLOAD_ID.to_string();
    let object_path = format!("/{BUCKET}/probe/object");
    // Path-style, a bucket-level request's path is the bucket and a slash.
    let bucket_path = format!("/{BUCKET}/");
    let forbidden_path = format!("/{FORBIDDEN_BUCKET}/");
    let case = |probe, method, path: &str, params: &[(&str, &str)], status, code| ProbeCase {
        probe,
        method,
        path: path.to_string(),
        params: params
            .iter()
            .map(|&(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        copy_source: None,
        body: SentBody::Empty,
        status,
        code,
    };
    let with_upload_id = [("uploadId", UPLOAD_ID)];
    vec![
        ProbeCase {
            copy_source: Some("validate/probe/source"),
            ..case(
                Probe::CopyObject {
                    key: key.clone(),
                    source_key: "probe/source".to_string(),
                },
                "PUT",
                &object_path,
                &[],
                501,
                not_implemented(),
            )
        },
        case(
            Probe::CreateMultipartUpload { key: key.clone() },
            "POST",
            &object_path,
            &[("uploads", "")],
            501,
            not_implemented(),
        ),
        case(
            Probe::UploadPart {
                key: key.clone(),
                upload_id: upload_id.clone(),
            },
            "PUT",
            &object_path,
            &[("partNumber", "1"), ("uploadId", UPLOAD_ID)],
            501,
            not_implemented(),
        ),
        ProbeCase {
            body: SentBody::TheEmptyPart,
            ..case(
                Probe::CompleteMultipartUpload {
                    key: key.clone(),
                    upload_id: upload_id.clone(),
                },
                "POST",
                &object_path,
                &with_upload_id,
                501,
                not_implemented(),
            )
        },
        case(
            Probe::AbortMultipartUpload {
                key: key.clone(),
                upload_id: upload_id.clone(),
            },
            "DELETE",
            &object_path,
            &with_upload_id,
            501,
            not_implemented(),
        ),
        case(
            Probe::ListParts { key, upload_id },
            "GET",
            &object_path,
            &with_upload_id,
            501,
            not_implemented(),
        ),
        case(
            Probe::ListMultipartUploads,
            "GET",
            &bucket_path,
            &[("uploads", "")],
            501,
            not_implemented(),
        ),
        case(
            Probe::GetBucketLocation,
            "GET",
            &bucket_path,
            &[("location", "")],
            501,
            not_implemented(),
        ),
        case(
            Probe::CreateBucket {
                bucket: FORBIDDEN_BUCKET.to_string(),
            },
            "PUT",
            &forbidden_path,
            &[],
            400,
            invalid_request(),
        ),
        case(
            Probe::DeleteBucket {
                bucket: FORBIDDEN_BUCKET.to_string(),
            },
            "DELETE",
            &forbidden_path,
            &[],
            400,
            invalid_request(),
        ),
        case(Probe::ListBuckets, "GET", "/", &[], 400, invalid_request()),
        case(
            Probe::HeadBucket,
            "HEAD",
            &bucket_path,
            &[],
            400,
            ErrorCode::NoBody,
        ),
    ]
}

#[tokio::test]
async fn every_unsupported_operation_reaches_the_gateway_and_comes_back_with_its_status_and_code() {
    let names: Vec<&str> = probe_cases()
        .iter()
        .map(|case| case.probe.operation())
        .collect();
    assert_eq!(
        names,
        [
            "CopyObject",
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
            "ListParts",
            "ListMultipartUploads",
            "GetBucketLocation",
            "CreateBucket",
            "DeleteBucket",
            "ListBuckets",
            "HeadBucket",
        ],
        "each probe names the S3 operation it sends"
    );
    let gateway = start_gateway().await;
    let relay = Relay::start(gateway.addr).await;
    for (endpoint, host) in endpoint_forms(relay.addr) {
        let client = client_at(&endpoint, deadlines());
        for case in probe_cases() {
            probe_reaches_the_gateway(&client, &relay, &endpoint, &host, case).await;
        }
    }
}

/// Send `case`'s probe through `relay` and check what reached the gateway and what came back.
async fn probe_reaches_the_gateway(
    client: &S3Client,
    relay: &Relay,
    endpoint: &str,
    host: &str,
    case: ProbeCase,
) {
    let name = format!("{} on {endpoint}", case.probe.operation());
    let name = name.as_str();
    let answer = bounded(name, client.probe(&case.probe))
        .await
        .unwrap_or_else(|e| panic!("{name}: no answer: {e}"));

    let requests = relay.take_requests();
    let [request] = requests.as_slice() else {
        panic!("{name}: one request reached the gateway, not {requests:?}");
    };
    if let Some(fault) = case.sent_fault(request) {
        panic!("{name}: not the operation's request: {fault}");
    }
    let head = &request.head;
    assert_eq!(
        request_header(head, "host"),
        Some(host),
        "{name}: addressed to the endpoint: {head}"
    );

    assert_eq!(answer.head.status, case.status, "{name}: {answer:?}");
    let message = match &answer.outcome {
        Outcome::Error { code, message } => {
            assert_eq!(code, &case.code, "{name}: the S3 code: {answer:?}");
            message.clone()
        }
        other => panic!("{name}: not an S3 error response: {other:?}"),
    };
    if case.code == ErrorCode::NoBody {
        assert_eq!(message, None, "{name}: no body, so no message");
    }
    assert_gateway_answered(&answer, name);
}

/// The forms an operator may type the endpoint `addr` in, each with the `Host` header a request
/// to it carries: its IP address, and the host name `localhost`, on which the SDK's own
/// endpoint rules would address an S3 Express bucket virtual-host style.
fn endpoint_forms(addr: SocketAddr) -> [(String, String); 2] {
    let port = addr.port();
    [
        (format!("http://{addr}"), addr.to_string()),
        (
            format!("http://localhost:{port}"),
            format!("localhost:{port}"),
        ),
    ]
}

/// A bucket-level request to a bucket the gateway does not have, as the bucket probes send:
/// `CreateBucket` and `DeleteBucket` naming `bucket`, each with its method.
fn bucket_probes(bucket: &str) -> [(Probe, &'static str); 2] {
    [
        (
            Probe::CreateBucket {
                bucket: bucket.to_string(),
            },
            "PUT",
        ),
        (
            Probe::DeleteBucket {
                bucket: bucket.to_string(),
            },
            "DELETE",
        ),
    ]
}

#[tokio::test]
async fn bucket_probes_send_every_name_as_one_path_segment_on_an_ip_or_a_host_name_endpoint() {
    let gateway = start_gateway().await;
    let relay = Relay::start(gateway.addr).await;
    let mut failures = Vec::new();
    for (endpoint, host) in endpoint_forms(relay.addr) {
        let client = client_at(&endpoint, deadlines());
        for (bucket, segment) in
            std::iter::once((FORBIDDEN_BUCKET, FORBIDDEN_BUCKET)).chain(MORE_FORBIDDEN_BUCKETS)
        {
            for (probe, method) in bucket_probes(bucket) {
                let case = format!("{} {bucket:?} on {endpoint}", probe.operation());
                let sent = bounded(&case, client.probe(&probe)).await;
                let heads = relay.take();
                let answer = match sent {
                    Ok(answer) => answer,
                    Err(e) => {
                        failures.push(format!(
                            "{case}: refused or unanswered: {e}; sent {heads:?}"
                        ));
                        continue;
                    }
                };
                // One request, the probe's own: never a `CreateSession` of the SDK's before it.
                let [head] = heads.as_slice() else {
                    failures.push(format!("{case}: not one request, but {heads:?}"));
                    continue;
                };
                let (sent_method, path, _) = request_line(head);
                let expected = format!("/{segment}/");
                if (sent_method, path) != (method, expected.as_str()) {
                    failures.push(format!(
                        "{case}: reached the gateway as {sent_method} {path}, not {method} \
                         {expected}"
                    ));
                }
                // Addressed to the endpoint itself, never to a host named after the bucket.
                if request_header(head, "host") != Some(host.as_str()) {
                    failures.push(format!("{case}: not addressed to {host}: {head}"));
                }
                if !request_header(head, "authorization")
                    .is_some_and(|auth| auth.contains(&format!("/{REGION}/s3/aws4_request")))
                {
                    failures.push(format!("{case}: not signed for s3 in {REGION}: {head}"));
                }
                // The gateway judged the operation: a signature it refused would be a 403.
                let refusal = Outcome::Error {
                    code: invalid_request(),
                    message: answer_message(&answer),
                };
                if answer.head.status != 400 || answer.outcome != refusal {
                    failures.push(format!("{case}: not the gateway's own refusal: {answer:?}"));
                }
                assert_gateway_answered(&answer, &case);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) not sent as one path segment:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A bare `400` with no body: what the scripted endpoint answers every request of the path
/// test with.
fn bare_400(request_id: &str) -> Script {
    Script::ended(head(
        "400 Bad Request",
        request_id,
        &[("Content-Length", "0".to_string())],
    ))
}

#[tokio::test]
async fn an_endpoint_path_comes_before_the_bucket_on_every_request() {
    for path in ["/s3-prefix", "/s3-prefix/"] {
        let mut endpoint = Endpoint::start((0..9).map(|_| bare_400("path-case")).collect()).await;
        let url = format!("http://localhost:{}{path}", endpoint.addr.port());
        let client = client_at(&url, deadlines());
        for probe in [
            Probe::CreateBucket {
                bucket: FORBIDDEN_BUCKET.to_string(),
            },
            Probe::ListBuckets,
            Probe::HeadBucket,
            // Dot segments, sent as they are: what they mean is the server's to decide.
            Probe::CreateBucket {
                bucket: ".".to_string(),
            },
            Probe::DeleteBucket {
                bucket: "..".to_string(),
            },
        ] {
            let case = format!("{} on {url}", probe.operation());
            let answer = bounded(&case, client.probe(&probe))
                .await
                .unwrap_or_else(|e| panic!("{case}: no answer: {e}"));
            assert_eq!(answer.head.status, 400, "{case}: {answer:?}");
        }
        let answer = bounded("HEAD", client.head_object("dir/key"))
            .await
            .unwrap_or_else(|e| panic!("HEAD on {url}: no answer: {e}"));
        assert_eq!(answer.head.status, 400, "HEAD on {url}: {answer:?}");
        // The older verbs go through the same resolution.
        let refused = |verb: &str, sent: Option<S3Error>| match sent {
            Some(S3Error::Service { status: 400, .. }) => {}
            other => panic!("{verb} on {url}: not the endpoint's 400: {other:?}"),
        };
        let source = PutSource::new(
            1,
            stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"a"))]),
        );
        refused(
            "PUT",
            bounded("PUT", client.put_object("dir/key", source))
                .await
                .err(),
        );
        refused(
            "GET",
            bounded("GET", client.get_object("dir/key")).await.err(),
        );
        refused(
            "DELETE",
            bounded("DELETE", client.delete_object("dir/key"))
                .await
                .err(),
        );
        drop(client);
        let lines: Vec<(String, String)> = endpoint
            .heads()
            .await
            .iter()
            .map(|head| {
                let (method, path, _) = request_line(head);
                (method.to_string(), path.to_string())
            })
            .collect();
        let expected: Vec<(String, String)> = [
            ("PUT", format!("/s3-prefix/{FORBIDDEN_BUCKET}/")),
            ("GET", "/s3-prefix/".to_string()),
            ("HEAD", format!("/s3-prefix/{BUCKET}/")),
            ("PUT", "/s3-prefix/./".to_string()),
            ("DELETE", "/s3-prefix/../".to_string()),
            ("HEAD", format!("/s3-prefix/{BUCKET}/dir/key")),
            ("PUT", format!("/s3-prefix/{BUCKET}/dir/key")),
            ("GET", format!("/s3-prefix/{BUCKET}/dir/key")),
            ("DELETE", format!("/s3-prefix/{BUCKET}/dir/key")),
        ]
        .into_iter()
        .map(|(method, path)| (method.to_string(), path))
        .collect();
        assert_eq!(
            lines, expected,
            "the endpoint {url}'s path, then the bucket"
        );
    }
}

/// The first 300 characters of `text`: a failure that quotes a 70,000-byte name stays readable.
fn first_chars(text: &str) -> String {
    text.chars().take(300).collect()
}

#[tokio::test]
async fn a_request_stopped_before_it_reaches_the_connector_is_request_not_built() {
    let gateway = start_gateway().await;
    let relay = Relay::start(gateway.addr).await;

    // Names the client cannot send. One the SDK reads as no name at all, empty or all white
    // space, which it refuses as a missing field; and one whose request URL would be longer
    // than an `http` URI may be (65,534 bytes).
    let long_name = "a".repeat(70_000);
    let client = client(relay.addr, deadlines());
    for (name, bucket, reason) in [
        ("an empty name", "", "bucket"),
        ("a name of white space", " \t", "bucket"),
        ("a 70,000-byte name", long_name.as_str(), "endpoint"),
    ] {
        for (probe, _) in bucket_probes(bucket) {
            let case = format!("{} of {name}", probe.operation());
            match bounded(&case, client.probe(&probe)).await {
                Err(S3Error::RequestNotBuilt { detail }) => assert!(
                    detail.contains(reason),
                    "{case}: the reason names the {reason}: {}",
                    first_chars(&detail)
                ),
                other => panic!(
                    "{case}: not refused as unsent: {}",
                    first_chars(&format!("{other:?}"))
                ),
            }
        }
    }

    // Endpoints the client's endpoint resolution refuses, as the SDK's endpoint rules refuse
    // them: one with a query, and one whose scheme is neither `http` nor `https`.
    for (endpoint, reason) in [
        (format!("http://{}/?x=1", relay.addr), "query"),
        (format!("ftp://{}", relay.addr), "scheme"),
    ] {
        let client = client_at(&endpoint, deadlines());
        let refused = |case: &str, e: &S3Error| match e {
            S3Error::RequestNotBuilt { detail } => assert!(
                detail.contains(reason),
                "{case} on {endpoint}: the reason names the {reason}: {detail}"
            ),
            other => panic!("{case} on {endpoint}: not refused as unsent: {other:?}"),
        };
        for probe in [
            Probe::ListBuckets,
            Probe::CreateBucket {
                bucket: FORBIDDEN_BUCKET.to_string(),
            },
        ] {
            let case = probe.operation();
            match bounded(case, client.probe(&probe)).await {
                Err(e) => refused(case, &e),
                Ok(answer) => panic!("{case} on {endpoint}: answered: {answer:?}"),
            }
        }
        match bounded("HEAD", client.head_object("dir/key")).await {
            Err(e) => refused("HEAD", &e),
            Ok(answer) => panic!("HEAD on {endpoint}: answered: {answer:?}"),
        }
        match bounded("ListObjectsV2", client.list_objects_v2("dir/", 10)).await {
            Err(ListingError::Page {
                page: 1,
                cause: NoResult::NoAnswer(e),
            }) => refused("ListObjectsV2", &e),
            other => panic!("ListObjectsV2 on {endpoint}: not refused as unsent: {other:?}"),
        }
        match bounded("DeleteObjects", client.delete_objects(&["dir/key"])).await {
            Err(NoResult::NoAnswer(e)) => refused("DeleteObjects", &e),
            other => panic!("DeleteObjects on {endpoint}: not refused as unsent: {other:?}"),
        }
    }
    assert_eq!(relay.accepted(), 0, "no connection was opened");
    assert_eq!(relay.take(), Vec::<String>::new(), "no request was sent");
}

/// The most percent-encoded bytes of key and upload id a probe may have: an `http` URI's 65,534,
/// less the 64 the client leaves for the rest of it.
const LONGEST_KEY_AND_UPLOAD_ID: usize = 65_534 - 64;

/// The probes that name both a key and an upload id.
fn probes_with_an_upload_id(key: &str, upload_id: &str) -> Vec<Probe> {
    let (key, upload_id) = (key.to_string(), upload_id.to_string());
    vec![
        Probe::UploadPart {
            key: key.clone(),
            upload_id: upload_id.clone(),
        },
        Probe::CompleteMultipartUpload {
            key: key.clone(),
            upload_id: upload_id.clone(),
        },
        Probe::AbortMultipartUpload {
            key: key.clone(),
            upload_id: upload_id.clone(),
        },
        Probe::ListParts { key, upload_id },
    ]
}

/// Every probe that names a key.
fn probes_with_a_key(key: &str, upload_id: &str) -> Vec<Probe> {
    let mut probes = vec![
        Probe::CopyObject {
            key: key.to_string(),
            source_key: "source".to_string(),
        },
        Probe::CreateMultipartUpload {
            key: key.to_string(),
        },
    ];
    probes.extend(probes_with_an_upload_id(key, upload_id));
    probes
}

#[tokio::test]
async fn a_probe_whose_key_or_upload_id_is_too_long_for_a_uri_is_request_not_built_and_sends_nothing(
) {
    let gateway = start_gateway().await;
    let relay = Relay::start(gateway.addr).await;
    let client = client(relay.addr, deadlines());
    let past = LONGEST_KEY_AND_UPLOAD_ID + 1;

    // What the SDK writes into the URI is what counts, not the key's own length: a raw length
    // would let the `&`s and the two-byte characters through, and the SDK would panic on them.
    let mut cases = Vec::new();
    for (name, key, encoded) in [
        ("70,000 bytes", "k".repeat(70_000), 70_000),
        ("one byte past the limit", "k".repeat(past), past),
        ("30,000 `&`", "&".repeat(30_000), 90_000),
        ("10,912 two-byte characters", "é".repeat(10_912), 65_472),
        ("a key of slashes", "/".repeat(past), past),
    ] {
        cases.push((name, probes_with_a_key(&key, ""), encoded));
    }
    for (name, upload_id, encoded) in [
        (
            "an upload id that reaches the limit beside a key of one byte",
            "u".repeat(past - 1),
            past,
        ),
        ("an upload id of 70,000 bytes", "u".repeat(70_000), 70_001),
        ("an upload id of 30,000 `&`", "&".repeat(30_000), 90_001),
        // A query value's `/` is encoded, where a key's is not.
        ("an upload id of 22,000 `/`", "/".repeat(22_000), 66_001),
    ] {
        cases.push((name, probes_with_an_upload_id("k", &upload_id), encoded));
    }

    for (name, probes, encoded) in cases {
        for probe in probes {
            let case = format!("{} with {name}", probe.operation());
            match bounded(&case, client.probe(&probe)).await {
                Err(S3Error::RequestNotBuilt { detail })
                    if detail
                        .contains(&format!("this {} take {encoded} bytes", probe.operation())) => {}
                other => panic!(
                    "{case}: not refused for its {encoded} bytes: {}",
                    first_chars(&format!("{other:?}"))
                ),
            }
        }
    }
    assert_eq!(relay.accepted(), 0, "no connection was opened");
}

#[tokio::test]
async fn a_probe_whose_key_and_upload_id_just_fit_goes_on_to_the_sdk() {
    let limit = LONGEST_KEY_AND_UPLOAD_ID;
    let plain = "k".repeat(limit);
    let slashed = "k/".repeat(limit / 2);
    let upload_id = "u".repeat(limit - 1);
    let copy_source = "s".repeat(70_000);

    // Sent, its URL under the limit with the endpoint's own bytes added: a key's `/` counts as
    // one byte, and a copy source is a header, which the limit does not reach.
    let sent = [
        (
            Probe::CreateMultipartUpload { key: plain.clone() },
            format!("/{BUCKET}/{plain}"),
            ("uploads", ""),
        ),
        (
            Probe::CreateMultipartUpload {
                key: slashed.clone(),
            },
            format!("/{BUCKET}/{slashed}"),
            ("uploads", ""),
        ),
        (
            Probe::ListParts {
                key: "k".to_string(),
                upload_id: upload_id.clone(),
            },
            format!("/{BUCKET}/k"),
            ("uploadId", upload_id.as_str()),
        ),
        (
            Probe::CopyObject {
                key: "k".to_string(),
                source_key: copy_source.clone(),
            },
            format!("/{BUCKET}/k"),
            ("", ""),
        ),
    ];
    for (probe, path, (param, value)) in sent {
        let name = probe.operation();
        let mut endpoint = Endpoint::start(vec![bare_400(name)]).await;
        let client = client(endpoint.addr, deadlines());
        let answer = bounded(name, client.probe(&probe)).await;
        drop(client);
        match answer {
            Ok(answer) => assert_eq!(answer.head.status, 400, "{name}"),
            Err(e) => panic!("{name}: not sent: {}", first_chars(&format!("{e:?}"))),
        }
        let heads = endpoint.heads().await;
        let [head] = heads.as_slice() else {
            panic!("{name}: not one request");
        };
        let (_, sent_path, query) = request_line(head);
        assert!(sent_path == path, "{name}: another path");
        if !param.is_empty() {
            assert!(query_has(query, param), "{name}: no {param}");
        }
        if !value.is_empty() {
            assert!(
                query_value(query, param).as_deref() == Some(value),
                "{name}: another {param}"
            );
        }
        if matches!(probe, Probe::CopyObject { .. }) {
            assert!(
                request_header(head, "x-amz-copy-source")
                    == Some(format!("{BUCKET}/{copy_source}").as_str()),
                "{name}: another copy source"
            );
        }
    }

    // The most the SDK writes around them. Too long once the endpoint is added, which the SDK
    // reports itself, in the same way, and with no panic.
    let client = client("127.0.0.1:9".parse().expect("an address"), deadlines());
    let probe = Probe::UploadPart {
        key: "k".to_string(),
        upload_id,
    };
    match bounded("UploadPart", client.probe(&probe)).await {
        Err(S3Error::RequestNotBuilt { detail }) if !detail.contains(" take ") => {}
        other => panic!(
            "UploadPart: not the SDK's own refusal: {}",
            first_chars(&format!("{other:?}"))
        ),
    }
}

#[tokio::test]
async fn a_probe_that_reached_the_connector_and_got_no_answer_is_no_response_never_unsent() {
    let probe = Probe::CreateBucket {
        bucket: FORBIDDEN_BUCKET.to_string(),
    };

    // Sent, read whole by the endpoint, and the connection closed, or reset, with no response.
    for (case, then) in [("closed unanswered", Then::Cut), ("reset", Then::Reset)] {
        let mut endpoint = Endpoint::start(vec![Script {
            response: Vec::new(),
            then,
        }])
        .await;
        // The `client` function, shadowed further down.
        let client = crate::client(endpoint.addr, deadlines());
        let lost = bounded(case, client.probe(&probe)).await;
        drop(client);
        let heads = endpoint.heads().await;
        assert_eq!(heads.len(), 1, "{case}: the probe reached the endpoint");
        assert!(
            matches!(lost, Err(S3Error::NoResponse { .. })),
            "{case}: {lost:?}"
        );
    }

    // No listener: the connection is refused, after the request was handed over. The port is
    // held by a socket that is bound but not listening, as `s3_client_roundtrip.rs` holds one:
    // the kernel refuses every connection to it at once, and no other test can take the port
    // meanwhile (a freed port can be taken by another test's listener, whose answer would
    // then reach this probe). Held to the end of the test: the relay below is pointed at it.
    let holder = TcpSocket::new_v4().expect("socket");
    holder
        .bind("127.0.0.1:0".parse().expect("loopback"))
        .expect("bind");
    let refused_addr = holder.local_addr().expect("addr");
    assert!(
        std::net::TcpListener::bind(refused_addr).is_err(),
        "the refused port is held, so no other listener can take it"
    );
    let refused_client = client(refused_addr, deadlines());
    let refused = bounded("refused", refused_client.probe(&probe)).await;
    assert!(
        matches!(refused, Err(S3Error::NoResponse { .. })),
        "a refused connection: {refused:?}"
    );

    // An `https` endpoint: the endpoint resolution accepts it, as the SDK's rules do, and the
    // plain-HTTP connector refuses it before it opens a connection. Nothing is sent, but the
    // connector was called, so the call is no response, as it is for the older verbs.
    let relay = Relay::start(refused_addr).await;
    let https_client = client_at(&format!("https://{}", relay.addr), deadlines());
    let https = bounded("https", https_client.probe(&probe)).await;
    assert!(
        matches!(&https, Err(S3Error::NoResponse { detail }) if detail.contains("plain HTTP")),
        "an https endpoint: {https:?}"
    );
    assert_eq!(relay.accepted(), 0, "no connection was opened");
}

#[tokio::test]
async fn the_region_is_signed_as_given_whether_or_not_it_could_name_a_host() {
    // `aws-global`, which the SDK's endpoint rules sign as `us-east-1`; and a region that is
    // not a DNS label, which they refuse.
    for region in ["aws-global", "us-east-2!"] {
        let mut endpoint =
            Endpoint::start(vec![bare_400("region-case"), bare_400("region-case")]).await;
        let config = resolved(&format!("http://{}", endpoint.addr), region);
        let client = S3Client::with_deadlines(&config, deadlines());
        let answer = bounded(region, client.probe(&Probe::ListBuckets))
            .await
            .unwrap_or_else(|e| panic!("{region}: not sent: {e}"));
        assert_eq!(answer.head.status, 400, "{region}: {answer:?}");
        // An older verb goes through the same resolution.
        let deleted = bounded(region, client.delete_object("dir/key")).await;
        assert!(
            matches!(deleted, Err(S3Error::Service { status: 400, .. })),
            "{region}: not sent: {deleted:?}"
        );
        drop(client);
        let heads = endpoint.heads().await;
        assert_eq!(heads.len(), 2, "{region}: {heads:?}");
        let scope = format!("/{region}/s3/aws4_request");
        for head in &heads {
            assert!(
                request_header(head, "authorization").is_some_and(|auth| auth.contains(&scope)),
                "{region}: signed for {scope}: {head}"
            );
        }
    }
}

/// A listener whose accept queue is full, as `s3_client_roundtrip.rs`'s: further connection
/// attempts hang in SYN-SENT. Returned with the queued connections so they stay open.
async fn saturated_listener() -> (SocketAddr, TcpListener, Vec<TcpStream>) {
    let socket = TcpSocket::new_v4().expect("socket");
    socket
        .bind("127.0.0.1:0".parse().expect("loopback"))
        .expect("bind");
    let listener = socket.listen(1).expect("listen");
    let addr = listener.local_addr().expect("addr");
    let mut queued = Vec::new();
    while let Ok(connected) = timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
        queued.push(connected.expect("a queued connection"));
        assert!(queued.len() < 64, "the accept queue never filled");
    }
    (addr, listener, queued)
}

/// Every probe, sent at once to `addr` under `limits`, and what each came back as.
async fn every_probe_under(
    addr: SocketAddr,
    limits: Deadlines,
) -> Vec<(String, Result<Answer, S3Error>)> {
    let mut sends = JoinSet::new();
    for case in probe_cases() {
        let client = client(addr, limits);
        sends.spawn(async move {
            let name = case.probe.operation().to_string();
            (name, bounded("a probe", client.probe(&case.probe)).await)
        });
    }
    sends.join_all().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_under_the_connect_deadline_expires_as_a_connect_timeout() {
    let (addr, _listener, _queued) = saturated_listener().await;
    let limits = Deadlines {
        connect: Duration::from_millis(500),
        ..deadlines()
    };
    let started = Instant::now();
    for (name, outcome) in every_probe_under(addr, limits).await {
        assert_eq!(
            outcome.err(),
            Some(S3Error::Timeout {
                phase: Phase::Connect,
                limit: limits.connect,
            }),
            "{name}"
        );
    }
    assert!(started.elapsed() >= limits.connect);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_under_the_operation_deadline_expires_as_an_operation_timeout() {
    // A peer that accepts and never answers.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let mut peer = JoinSet::new();
    peer.spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    let limits = Deadlines {
        operation: Duration::from_millis(500),
        ..deadlines()
    };
    let started = Instant::now();
    for (name, outcome) in every_probe_under(addr, limits).await {
        assert_eq!(
            outcome.err(),
            Some(S3Error::Timeout {
                phase: Phase::Operation,
                limit: limits.operation,
            }),
            "{name}"
        );
    }
    assert!(started.elapsed() >= limits.operation);
}

fn answer_message(answer: &Answer) -> Option<String> {
    match &answer.outcome {
        Outcome::Error { message, .. } => message.clone(),
        _ => None,
    }
}

// --- the scripted endpoint -----------------------------------------------------------------

/// What the endpoint does once it has written a response's bytes.
#[derive(Clone)]
enum Then {
    /// Close its side, then wait for the client to close its own.
    End,
    /// Write nothing more and keep the connection open until the client closes it.
    Hold,
    /// Close the connection at once.
    Cut,
    /// Reset the connection at once: a close that discards, and an RST on the wire.
    Reset,
    /// Write these bytes one at a time, `every` apart, then hold.
    Trickle { bytes: Vec<u8>, every: Duration },
}

#[derive(Clone)]
struct Script {
    response: Vec<u8>,
    then: Then,
}

impl Script {
    fn ended(response: Vec<u8>) -> Self {
        Self {
            response,
            then: Then::End,
        }
    }
}

/// A loopback endpoint that answers one connection per script, in order, each after reading
/// one whole request, and records each request's head. Owned and aborted on drop.
struct Endpoint {
    addr: SocketAddr,
    task: JoinSet<std::io::Result<Vec<String>>>,
}

impl Endpoint {
    async fn start(scripts: Vec<Script>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("endpoint bind");
        let addr = listener.local_addr().expect("endpoint addr");
        let mut task = JoinSet::new();
        task.spawn(async move {
            let mut heads = Vec::new();
            for script in scripts {
                let (mut stream, _) = listener.accept().await?;
                heads.push(read_request(&mut stream).await?.0.head);
                serve(&mut stream, script).await;
            }
            Ok(heads)
        });
        Self { addr, task }
    }

    /// The request heads the endpoint read, once it has served every script.
    async fn heads(&mut self) -> Vec<String> {
        timeout(2 * HOLD, self.task.join_next())
            .await
            .expect("the endpoint finished")
            .expect("the endpoint ran")
            .expect("the endpoint task did not panic")
            .expect("the endpoint read every request")
    }
}

async fn read_more(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<()> {
    if stream.read_buf(buf).await? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    Ok(())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// The index of the next CRLF at or after `from`, reading more as needed.
async fn line_end(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    from: usize,
) -> std::io::Result<usize> {
    loop {
        if let Some(i) = find(&buf[from..], b"\r\n") {
            return Ok(from + i);
        }
        read_more(stream, buf).await?;
    }
}

/// One request as it was read.
#[derive(Debug)]
struct Request {
    head: String,
    /// The body with its framing off (a chunked or an `aws-chunked` one's chunks put together),
    /// since the SDK may frame a body with checksums.
    body: Vec<u8>,
}

/// The size a chunk's size line gives.
fn chunk_size(line: &[u8]) -> usize {
    let line = String::from_utf8_lossy(line);
    let size = line.split(';').next().unwrap_or_default().trim();
    usize::from_str_radix(size, 16).expect("a hex chunk size")
}

/// `body`'s payload, by what `head` says frames it.
fn unframed(head: &str, body: &[u8]) -> Vec<u8> {
    let chunked = request_header(head, "transfer-encoding")
        .is_some_and(|te| te.eq_ignore_ascii_case("chunked"))
        || request_header(head, "content-encoding")
            .is_some_and(|ce| ce.eq_ignore_ascii_case("aws-chunked"));
    if !chunked {
        return body.to_vec();
    }
    let mut payload = Vec::new();
    let mut rest = body;
    loop {
        let end = find(rest, b"\r\n").expect("a chunk's size line");
        let size = chunk_size(&rest[..end]);
        if size == 0 {
            return payload;
        }
        payload.extend_from_slice(&rest[end + 2..end + 2 + size]);
        rest = &rest[end + 2 + size + 2..];
    }
}

/// Read one whole request, head and body, so the response is never written over unread
/// request bytes (`s3_client_nonconforming_responses.rs`'s `read_request`). Returns it, and
/// every byte of it as read.
async fn read_request(stream: &mut TcpStream) -> std::io::Result<(Request, Vec<u8>)> {
    let mut buf = Vec::with_capacity(8192);
    let head_end = loop {
        if let Some(i) = find(&buf, b"\r\n\r\n") {
            break i + 4;
        }
        read_more(stream, &mut buf).await?;
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    if let Some(len) = request_header(&head, "content-length") {
        let len: usize = len.parse().expect("a numeric request Content-Length");
        while buf.len() < head_end + len {
            read_more(stream, &mut buf).await?;
        }
    } else if request_header(&head, "transfer-encoding")
        .is_some_and(|te| te.eq_ignore_ascii_case("chunked"))
    {
        let mut pos = head_end;
        loop {
            let end = line_end(stream, &mut buf, pos).await?;
            let size = chunk_size(&buf[pos..end]);
            if size == 0 {
                let mut line = end + 2;
                loop {
                    let end = line_end(stream, &mut buf, line).await?;
                    if end == line {
                        break;
                    }
                    line = end + 2;
                }
                break;
            }
            let next = end + 2 + size + 2;
            while buf.len() < next {
                read_more(stream, &mut buf).await?;
            }
            pos = next;
        }
    }
    let body = unframed(&head, &buf[head_end..]);
    Ok((Request { head, body }, buf))
}

/// Write the script's response, then do what it says. A client that has closed the
/// connection ends the exchange: every case here has said what it is by then.
async fn serve(stream: &mut TcpStream, script: Script) {
    if stream.write_all(&script.response).await.is_err() {
        return;
    }
    match script.then {
        Then::End => {
            let _ = stream.shutdown().await;
            hold(stream).await;
        }
        Then::Hold => hold(stream).await,
        Then::Cut => {}
        Then::Reset => {
            let _ = stream.set_zero_linger();
        }
        Then::Trickle { bytes, every } => {
            for byte in bytes {
                tokio::time::sleep(every).await;
                if stream.write_all(&[byte]).await.is_err() {
                    return;
                }
            }
            hold(stream).await;
        }
    }
}

/// Wait, at most [`HOLD`], for the client to close the connection, reading whatever it sends.
async fn hold(stream: &mut TcpStream) {
    let mut sink = [0u8; 1024];
    let _ = timeout(HOLD, async {
        while matches!(stream.read(&mut sink).await, Ok(n) if n > 0) {}
    })
    .await;
}

const XML_DECL: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
const S3_NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// A response head: the status line, the `x-amz-request-id`, then `headers`.
fn head(status: &str, request_id: &str, headers: &[(&str, String)]) -> Vec<u8> {
    let mut text = format!("HTTP/1.1 {status}\r\nx-amz-request-id: {request_id}\r\n");
    for (name, value) in headers {
        let _ = write!(text, "{name}: {value}\r\n");
    }
    text.push_str("\r\n");
    text.into_bytes()
}

/// A `200` carrying exactly `body`, with a `Content-Length` of `declared`.
fn ok_with_length(request_id: &str, declared: usize, body: &[u8]) -> Vec<u8> {
    let mut response = head(
        "200 OK",
        request_id,
        &[
            ("Content-Type", "application/xml".to_string()),
            ("Content-Length", declared.to_string()),
        ],
    );
    response.extend_from_slice(body);
    response
}

/// A `200` carrying `body` chunked, in one chunk, with no `Content-Length`.
fn ok_chunked(request_id: &str, body: &[u8]) -> Vec<u8> {
    let mut response = head(
        "200 OK",
        request_id,
        &[
            ("Content-Type", "application/xml".to_string()),
            ("Transfer-Encoding", "chunked".to_string()),
        ],
    );
    response.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    response.extend_from_slice(body);
    response.extend_from_slice(b"\r\n0\r\n\r\n");
    response
}

// --- (C) a 2xx head survives its body, on a listing page -----------------------------------

/// A body that starts like a `ListObjectsV2` page and is cut short: not a whole
/// `<ListBucketResult>`, nor an `<Error>` document.
fn cut_listing() -> Vec<u8> {
    format!(
        "{XML_DECL}<ListBucketResult xmlns=\"{S3_NS}\"><Name>{BUCKET}</Name>\
         <IsTruncated>false</IsTruncated><Contents><Key>list/a</Key>"
    )
    .into_bytes()
}

/// An S3 `<Error>` document, as S3 sends one after a `200` head.
fn internal_error_document() -> String {
    format!(
        "{XML_DECL}<Error><Code>InternalError</Code>\
         <Message>We encountered an internal error. Please try again.</Message>\
         <RequestId>0123456789ABCDEF</RequestId></Error>"
    )
}

/// The ways a `200` head's body fails, and the endpoint script for each.
#[derive(Clone, Copy, Debug)]
enum BodyFault {
    /// Part of the body, then nothing, with the connection held open.
    Stall,
    /// One byte at a time, each inside the idle deadline, until the whole-body one.
    Trickle,
    /// Part of the body, then the connection closed.
    Cut,
    /// The whole declared body, which is not a listing page.
    NotADocument,
    /// The whole declared body, an S3 `<Error>` document in place of the listing.
    ErrorDocument,
}

const BODY_FAULTS: [BodyFault; 5] = [
    BodyFault::Stall,
    BodyFault::Trickle,
    BodyFault::Cut,
    BodyFault::NotADocument,
    BodyFault::ErrorDocument,
];

/// How much of the body a stalled or cut response sends before it stops.
const SENT_BEFORE_FAULT: usize = 10;

fn fault_script(document: &[u8], fault: BodyFault, request_id: &str) -> Script {
    // Room for more than the deadlines let through: a trickle of it takes several seconds.
    let declared = document.len() + 64;
    match fault {
        BodyFault::Stall => Script {
            response: ok_with_length(request_id, declared, &document[..SENT_BEFORE_FAULT]),
            then: Then::Hold,
        },
        BodyFault::Trickle => Script {
            response: ok_with_length(request_id, declared, b""),
            then: Then::Trickle {
                bytes: [document, &[b' '; 64]].concat(),
                every: TRICKLE_EVERY,
            },
        },
        BodyFault::Cut => Script {
            response: ok_with_length(request_id, declared, &document[..SENT_BEFORE_FAULT]),
            then: Then::Cut,
        },
        BodyFault::NotADocument => {
            Script::ended(ok_with_length(request_id, document.len(), document))
        }
        BodyFault::ErrorDocument => {
            let error = internal_error_document();
            Script::ended(ok_with_length(request_id, error.len(), error.as_bytes()))
        }
    }
}

/// What is wrong with `answer` as the report of a `200` from `request_id` whose body failed as
/// `fault` says, when anything is. `document` is the body a [`BodyFault::NotADocument`] sent, and
/// `refusal` words its refusal.
fn misreported_fault(
    answer: &Answer,
    fault: BodyFault,
    request_id: &str,
    document: &[u8],
    refusal: &str,
) -> Option<String> {
    if answer.head.status != 200 || answer.head.request_id() != Some(request_id) {
        return Some(format!("not the 200 the endpoint sent: {answer:?}"));
    }
    let sent = SENT_BEFORE_FAULT as u64;
    // The answer in words: its status and request id, then the failure.
    let shown = answer.to_string();
    let said = match fault {
        BodyFault::Stall => "a body that failed: the body-idle deadline",
        BodyFault::Trickle => "a body that failed: the body deadline",
        BodyFault::Cut => "a body that failed: reading the body failed",
        BodyFault::NotADocument => "an unreadable",
        BodyFault::ErrorDocument => "an S3 error",
    };
    if !shown.starts_with(&format!("HTTP 200, x-amz-request-id {request_id}: {said}")) {
        return Some(format!("shown as {shown:?}"));
    }
    let fits = match (&answer.outcome, fault) {
        (
            Outcome::BodyFailed(BodyFailure::Timeout {
                phase: Phase::BodyIdle,
                limit,
                received,
            }),
            BodyFault::Stall,
        ) => *limit == SHORT_IDLE && *received == sent,
        (
            Outcome::BodyFailed(BodyFailure::Timeout {
                phase: Phase::Body,
                limit,
                received,
            }),
            BodyFault::Trickle,
        ) => *limit == SHORT_BODY && *received > 0,
        (Outcome::BodyFailed(BodyFailure::Transport { received, detail }), BodyFault::Cut) => {
            *received <= sent && !detail.is_empty()
        }
        (Outcome::Unreadable { body, detail }, BodyFault::NotADocument) => {
            body[..] == *document && detail.contains(refusal)
        }
        (Outcome::Error { code, message }, BodyFault::ErrorDocument) => {
            *code == ErrorCode::Code("InternalError".to_string())
                && message.as_deref() == Some("We encountered an internal error. Please try again.")
        }
        _ => false,
    };
    (!fits).then(|| format!("the wrong failure beside the 200: {answer:?}"))
}

#[tokio::test]
async fn a_listing_page_answered_200_keeps_its_200_when_the_body_stalls_trickles_is_cut_or_is_unreadable(
) {
    let mut cases = JoinSet::new();
    for fault in BODY_FAULTS {
        cases.spawn(async move {
            let request_id = format!("ListObjectsV2-{fault:?}");
            let mut endpoint =
                Endpoint::start(vec![fault_script(&cut_listing(), fault, &request_id)]).await;
            let client = client(endpoint.addr, short_deadlines());
            let outcome = timeout(STALL, client.list_objects_v2("list/", 2)).await;
            drop(client);
            let heads = endpoint.heads().await;
            (fault, request_id, outcome, heads)
        });
    }
    let mut failures = Vec::new();
    while let Some(joined) = cases.join_next().await {
        let (fault, request_id, outcome, heads) = joined.expect("a case task");
        let case = format!("ListObjectsV2 {fault:?}");
        match heads.as_slice() {
            [request] if query_has(request_line(request).2, "list-type") => {}
            _ => {
                failures.push(format!("{case}: not one ListObjectsV2 request: {heads:?}"));
                continue;
            }
        }
        let answer = match outcome {
            Err(_) => {
                failures.push(format!("{case}: no outcome within {STALL:?}"));
                continue;
            }
            Ok(Err(ListingError::Page {
                page: 1,
                cause: NoResult::Answered(answer),
            })) => answer,
            Ok(other) => {
                failures.push(format!("{case}: the 200 head was lost: {other:?}"));
                continue;
            }
        };
        match misreported_fault(
            &answer,
            fault,
            &request_id,
            &cut_listing(),
            "the listing body",
        ) {
            None => eprintln!("GREEN {case}: {answer}"),
            Some(fault) => failures.push(format!("{case}: {fault}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) lost or misreported the 200:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// --- #956 (C): a 2xx head survives its body, on every probe that can carry one ---------------

/// A body that starts like the operation's success document and is cut short: not a readable
/// document, nor the empty body a headers-only success has.
fn cut_document(probe: &Probe) -> Vec<u8> {
    format!(
        "{XML_DECL}<{}Result><ETag>&quot;0123&quot;</ETag>",
        probe.operation()
    )
    .into_bytes()
}

/// Every probe whose answer can carry a body: a HEAD's never does.
fn bodied_probes() -> Vec<Probe> {
    probe_cases()
        .into_iter()
        .map(|case| case.probe)
        .filter(|probe| *probe != Probe::HeadBucket)
        .collect()
}

#[tokio::test]
async fn a_probe_answered_200_keeps_its_200_when_the_body_stalls_trickles_is_cut_or_is_unreadable()
{
    let mut cases = JoinSet::new();
    for probe in bodied_probes() {
        // `a_200_whose_body_is_an_s3_error_document…` is the fifth fault's, for a probe.
        for fault in BODY_FAULTS
            .into_iter()
            .filter(|fault| !matches!(fault, BodyFault::ErrorDocument))
        {
            let probe = probe.clone();
            cases.spawn(async move {
                let request_id = format!("{}-{fault:?}", probe.operation());
                let script = fault_script(&cut_document(&probe), fault, &request_id);
                let mut endpoint = Endpoint::start(vec![script]).await;
                let client = client(endpoint.addr, short_deadlines());
                let outcome = timeout(STALL, client.probe(&probe)).await;
                drop(client);
                let _ = endpoint.heads().await;
                (probe, fault, request_id, outcome)
            });
        }
    }
    let mut failures = Vec::new();
    while let Some(joined) = cases.join_next().await {
        let (probe, fault, request_id, outcome) = joined.expect("a case task");
        let case = format!("{} {fault:?}", probe.operation());
        let answer = match outcome {
            Err(_) => {
                failures.push(format!("{case}: no outcome within {STALL:?}"));
                continue;
            }
            Ok(Err(e)) => {
                failures.push(format!("{case}: the 200 head was lost: {e:?}"));
                continue;
            }
            Ok(Ok(answer)) => answer,
        };
        match misreported_fault(
            &answer,
            fault,
            &request_id,
            &cut_document(&probe),
            "success",
        ) {
            None => eprintln!("GREEN {case}: {answer}"),
            Some(fault) => failures.push(format!("{case}: {fault}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) lost or misreported the 200:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// --- #956 (C): a 2xx head survives its body, on a bulk delete --------------------------------

/// A `<DeleteResult>` cut short: not a readable document.
fn cut_delete_result() -> Vec<u8> {
    let document = delete_result(&["list/a", "list/b"]);
    document.as_bytes()[..document.len() - 20].to_vec()
}

#[tokio::test]
async fn a_delete_result_answered_200_keeps_its_200_when_the_body_stalls_trickles_is_cut_or_is_unreadable(
) {
    let mut cases = JoinSet::new();
    for fault in BODY_FAULTS {
        cases.spawn(async move {
            let request_id = format!("DeleteObjects-{fault:?}");
            let script = fault_script(&cut_delete_result(), fault, &request_id);
            let mut endpoint = Endpoint::start(vec![script]).await;
            let client = client(endpoint.addr, short_deadlines());
            let outcome = timeout(STALL, client.delete_objects(&["list/a", "list/b"])).await;
            drop(client);
            let heads = endpoint.heads().await;
            (fault, request_id, outcome, heads)
        });
    }
    let mut failures = Vec::new();
    while let Some(joined) = cases.join_next().await {
        let (fault, request_id, outcome, heads) = joined.expect("a case task");
        let case = format!("DeleteObjects {fault:?}");
        match heads.as_slice() {
            [request]
                if request_line(request).0 == "POST"
                    && query_has(request_line(request).2, "delete") => {}
            _ => {
                failures.push(format!("{case}: not one DeleteObjects request: {heads:?}"));
                continue;
            }
        }
        let answer = match outcome {
            Err(_) => {
                failures.push(format!("{case}: no outcome within {STALL:?}"));
                continue;
            }
            Ok(Err(NoResult::Answered(answer))) => answer,
            Ok(other) => {
                failures.push(format!("{case}: the 200 head was lost: {other:?}"));
                continue;
            }
        };
        match misreported_fault(
            &answer,
            fault,
            &request_id,
            &cut_delete_result(),
            "the delete result body",
        ) {
            None => eprintln!("GREEN {case}: {answer}"),
            Some(fault) => failures.push(format!("{case}: {fault}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) lost or misreported the 200:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// --- (D) unreadable listing pages (#955) and delete results (#956) are refused ----------------------------------------------

fn contents(key: &str, size: u64) -> String {
    format!(
        "<Contents><Key>{key}</Key><LastModified>2026-10-22T00:00:00.000Z</LastModified>\
         <ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag><Size>{size}</Size>\
         <StorageClass>STANDARD</StorageClass></Contents>"
    )
}

/// A whole `ListObjectsV2` page of `keys`; truncated, with `token`, when `token` is given.
fn listing_v2(keys: &[&str], token: Option<&str>) -> String {
    let entries: String = keys.iter().map(|key| contents(key, 7)).collect();
    let next = token.map_or(String::new(), |token| {
        format!("<NextContinuationToken>{token}</NextContinuationToken>")
    });
    format!(
        "{XML_DECL}<ListBucketResult xmlns=\"{S3_NS}\"><Name>{BUCKET}</Name>\
         <Prefix>list/</Prefix><KeyCount>{}</KeyCount><MaxKeys>2</MaxKeys>\
         <IsTruncated>{}</IsTruncated>{entries}{next}</ListBucketResult>",
        keys.len(),
        token.is_some()
    )
}

/// A whole `ListObjects` (version 1) page of `keys`, not truncated.
fn listing_v1(keys: &[&str]) -> String {
    listing_v1_page(keys, false, None)
}

/// A whole `ListObjects` (version 1) page of `keys`, truncated or not, with `next_marker` as
/// its `<NextMarker>` when given (an empty one for `Some("")`).
fn listing_v1_page(keys: &[&str], truncated: bool, next_marker: Option<&str>) -> String {
    let entries: String = keys.iter().map(|key| contents(key, 7)).collect();
    let next = next_marker.map_or(String::new(), |marker| {
        format!("<NextMarker>{marker}</NextMarker>")
    });
    format!(
        "{XML_DECL}<ListBucketResult xmlns=\"{S3_NS}\"><Name>{BUCKET}</Name>\
         <Prefix>list/</Prefix><Marker></Marker><MaxKeys>2</MaxKeys>\
         <IsTruncated>{truncated}</IsTruncated>{entries}{next}</ListBucketResult>"
    )
}

/// A whole `<DeleteResult>` reporting `keys` deleted.
fn delete_result(keys: &[&str]) -> String {
    let rows: String = keys
        .iter()
        .map(|key| format!("<Deleted><Key>{key}</Key></Deleted>"))
        .collect();
    format!("{XML_DECL}<DeleteResult xmlns=\"{S3_NS}\">{rows}</DeleteResult>")
}

/// `document` with a comment after its root that brings it to exactly `len` bytes.
fn padded(document: &str, len: usize) -> String {
    let fill = len
        .checked_sub(document.len() + "<!---->".len())
        .expect("room for the padding");
    let padded = format!("{document}<!--{}-->", "a".repeat(fill));
    assert_eq!(padded.len(), len);
    padded
}

/// Which verb a (D) case calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DocumentVerb {
    ListV2,
    ListV1,
    Delete,
}

const LISTING_VERBS: [DocumentVerb; 2] = [DocumentVerb::ListV2, DocumentVerb::ListV1];
const DOCUMENT_VERBS: [DocumentVerb; 3] = [
    DocumentVerb::ListV2,
    DocumentVerb::ListV1,
    DocumentVerb::Delete,
];

impl DocumentVerb {
    /// A whole, readable document of this verb's result.
    fn document(self) -> String {
        match self {
            Self::ListV2 => listing_v2(&["list/a", "list/b"], None),
            Self::ListV1 => listing_v1(&["list/a", "list/b"]),
            Self::Delete => delete_result(&["list/a", "list/b"]),
        }
    }

    /// What a refusal of this verb's document calls the body it refused.
    fn body_name(self) -> &'static str {
        match self {
            Self::ListV2 | Self::ListV1 => "the listing body",
            Self::Delete => "the delete result body",
        }
    }

    /// The method and a query parameter of the request this verb sends.
    fn request(self) -> (&'static str, &'static str) {
        match self {
            Self::ListV2 => ("GET", "list-type"),
            Self::ListV1 => ("GET", "prefix"),
            Self::Delete => ("POST", "delete"),
        }
    }
}

/// What a (D) verb returned: the entries or rows it read, or why it read none.
#[derive(Debug)]
enum Read {
    Listed(Vec<ListEntry>),
    Deleted(Vec<DeleteRow>),
    Listing(ListingError),
    Delete(NoResult),
}

async fn call_document_verb(verb: DocumentVerb, client: &S3Client) -> Read {
    match verb {
        DocumentVerb::ListV2 => match client.list_objects_v2("list/", 2).await {
            Ok(listing) => Read::Listed(listing.entries),
            Err(e) => Read::Listing(e),
        },
        DocumentVerb::ListV1 => match client.list_objects_v1("list/", 2).await {
            Ok(listing) => Read::Listed(listing.entries),
            Err(e) => Read::Listing(e),
        },
        DocumentVerb::Delete => match client.delete_objects(&["list/a", "list/b"]).await {
            Ok(result) => Read::Deleted(result.rows),
            Err(e) => Read::Delete(e),
        },
    }
}

/// The answer a (D) refusal kept, when it is one of page `page` (listings) or of the one request
/// (delete).
fn refused_answer(read: &Read, page: usize) -> Option<&Answer> {
    match read {
        Read::Listing(ListingError::Page {
            page: failed,
            cause: NoResult::Answered(answer),
        }) if *failed == page => Some(answer),
        Read::Delete(NoResult::Answered(answer)) => Some(answer),
        _ => None,
    }
}

/// How a (D) case's body must be refused.
#[derive(Clone, Copy, Debug)]
enum Refused {
    /// Read whole, and not one whole document of the verb's result.
    Unreadable,
    /// As [`Refused::Unreadable`], and the body as sent is empty: no `<Error>` was sent, so none
    /// is reported.
    UnreadableEmpty,
    /// Bytes past the end the framing marks.
    Overrun,
    /// Past the buffered budget.
    OverBudget,
}

fn refused_as(answer: &Answer, how: Refused, verb: DocumentVerb) -> bool {
    match (&answer.outcome, how) {
        (Outcome::Unreadable { detail, .. }, Refused::Unreadable) => {
            detail.contains(verb.body_name())
        }
        (Outcome::Unreadable { body, detail }, Refused::UnreadableEmpty) => {
            body.is_empty() && detail.contains(verb.body_name())
        }
        (Outcome::BodyFailed(BodyFailure::Overrun { after, .. }), Refused::Overrun) => *after > 0,
        (Outcome::BodyFailed(BodyFailure::OverBudget { received }), Refused::OverBudget) => {
            *received > BUFFERED_BODY_BUDGET
        }
        _ => false,
    }
}

#[tokio::test]
async fn listing_and_delete_result_documents_that_are_not_whole_are_refused_never_partial() {
    let budget = usize::try_from(BUFFERED_BODY_BUDGET).expect("the budget fits usize");
    let mut failures = Vec::new();
    for verb in DOCUMENT_VERBS {
        let doc = verb.document();
        let id = |case: &str| format!("{verb:?}-{case}");
        let over = padded(&doc, budget + 1);
        let cut = &doc[..doc.len() - 20];
        let junk = format!("{doc}junk");
        let twice = format!("{doc}{doc}");
        let cases: Vec<(&str, Vec<u8>, Refused)> = vec![
            (
                "empty",
                ok_with_length(&id("empty"), 0, b""),
                Refused::UnreadableEmpty,
            ),
            (
                "cut-short",
                ok_with_length(&id("cut-short"), cut.len(), cut.as_bytes()),
                Refused::Unreadable,
            ),
            (
                "followed-by-junk",
                ok_with_length(&id("followed-by-junk"), junk.len(), junk.as_bytes()),
                Refused::Unreadable,
            ),
            (
                "a-second-document",
                ok_with_length(&id("a-second-document"), twice.len(), twice.as_bytes()),
                Refused::Unreadable,
            ),
            (
                "bytes-after-content-length",
                ok_with_length(
                    &id("bytes-after-content-length"),
                    doc.len(),
                    junk.as_bytes(),
                ),
                Refused::Overrun,
            ),
            (
                "over-budget-with-length",
                ok_with_length(&id("over-budget-with-length"), over.len(), over.as_bytes()),
                Refused::OverBudget,
            ),
            (
                "over-budget-chunked",
                ok_chunked(&id("over-budget-chunked"), over.as_bytes()),
                Refused::OverBudget,
            ),
        ];
        for (case, response, how) in cases {
            let name = format!("{verb:?} {case}");
            let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
            let client = client(endpoint.addr, deadlines());
            let read = bounded(&name, call_document_verb(verb, &client)).await;
            drop(client);
            let heads = endpoint.heads().await;
            let (method, query_param) = verb.request();
            let [request] = heads.as_slice() else {
                failures.push(format!("{name}: expected one request, saw {heads:?}"));
                continue;
            };
            let (sent_method, _, query) = request_line(request);
            if sent_method != method || !query_has(query, query_param) {
                failures.push(format!("{name}: not the verb's request: {request}"));
                continue;
            }
            match refused_answer(&read, 1) {
                Some(answer)
                    if answer.head.status == 200
                        && answer.head.request_id() == Some(id(case).as_str())
                        && refused_as(answer, how, verb) =>
                {
                    eprintln!("GREEN {name}: {answer}");
                }
                _ => failures.push(format!("{name}: not refused as {how:?}: {read:?}")),
            }
        }

        // A document of exactly the budget is read.
        let exact = padded(&doc, budget);
        let mut endpoint = Endpoint::start(vec![Script::ended(ok_with_length(
            &id("exactly-the-budget"),
            exact.len(),
            exact.as_bytes(),
        ))])
        .await;
        let client = client(endpoint.addr, deadlines());
        let read = bounded("exactly the budget", call_document_verb(verb, &client)).await;
        drop(client);
        let _ = endpoint.heads().await;
        let expected = ["list/a", "list/b"].map(|key| ListEntry {
            key: key.to_string(),
            size: 7,
            etag: Some(SOME_ETAG.to_string()),
        });
        let rows = ["list/a", "list/b"].map(|key| DeleteRow::Deleted {
            key: key.to_string(),
        });
        match read {
            Read::Listed(entries) if entries == expected => {}
            Read::Deleted(got) if got == rows => {}
            other => failures.push(format!(
                "{verb:?} exactly the budget: not read whole: {other:?}"
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) not refused as they must be:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A page that reads as complete, and how each case spoils it: what is replaced with what, and
/// the words of the refusal. Each replacement alone is what stands between a refusal and a
/// partial listing reported as a whole one, so each must be refused for its own reason.
const SPOILED_PAGES: [(&str, &str, &str, &str); 19] = [
    (
        "no-is-truncated",
        "<IsTruncated>false</IsTruncated>",
        "",
        "names no <IsTruncated>",
    ),
    (
        "is-truncated-upper-case",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>TRUE</IsTruncated>",
        "<IsTruncated> is \"TRUE\"",
    ),
    (
        "is-truncated-empty",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated></IsTruncated>",
        "<IsTruncated> is \"\"",
    ),
    (
        "is-truncated-padded",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated> false </IsTruncated>",
        "<IsTruncated> is \" false \"",
    ),
    (
        "is-truncated-twice-last-false",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>true</IsTruncated><IsTruncated>false</IsTruncated>",
        "more than one <IsTruncated>",
    ),
    (
        "is-truncated-twice-first-false",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>false</IsTruncated><IsTruncated>true</IsTruncated>",
        "more than one <IsTruncated>",
    ),
    (
        "is-truncated-holds-a-comment",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>false<!-- true --></IsTruncated>",
        "<IsTruncated> of a <ListBucketResult> holds markup",
    ),
    (
        "is-truncated-holds-an-element",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>false<x>true</x></IsTruncated>",
        "<IsTruncated> of a <ListBucketResult> holds markup",
    ),
    (
        "token-twice",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>false</IsTruncated><NextContinuationToken>a</NextContinuationToken>\
         <NextContinuationToken>b</NextContinuationToken>",
        "more than one <NextContinuationToken>",
    ),
    (
        "token-holds-markup",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>false</IsTruncated><NextContinuationToken>a<x/></NextContinuationToken>",
        "<NextContinuationToken> of a <ListBucketResult> holds markup",
    ),
    (
        "marker-twice",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>false</IsTruncated><NextMarker>a</NextMarker><NextMarker>b</NextMarker>",
        "more than one <NextMarker>",
    ),
    (
        "marker-holds-markup",
        "<IsTruncated>false</IsTruncated>",
        "<IsTruncated>false</IsTruncated><NextMarker>a<x/></NextMarker>",
        "<NextMarker> of a <ListBucketResult> holds markup",
    ),
    (
        "key-twice",
        "<Key>list/a</Key>",
        "<Key>list/a</Key><Key>list/z</Key>",
        "more than one <Key>",
    ),
    (
        "key-holds-markup",
        "<Key>list/a</Key>",
        "<Key>list/a<x/>z</Key>",
        "<Key> of a <Contents> holds markup",
    ),
    (
        "no-key",
        "<Key>list/a</Key>",
        "",
        "a <Contents> of the listing names no <Key>",
    ),
    (
        "size-twice",
        "<Size>7</Size>",
        "<Size>7</Size><Size>8</Size>",
        "more than one <Size>",
    ),
    (
        "size-holds-markup",
        "<Size>7</Size>",
        "<Size>7<!-- 8 --></Size>",
        "<Size> of a <Contents> holds markup",
    ),
    ("no-size", "<Size>7</Size>", "", "names no <Size>"),
    (
        "etag-twice",
        "<ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag>",
        "<ETag>a</ETag><ETag>b</ETag>",
        "more than one <ETag>",
    ),
];

#[tokio::test]
async fn a_listing_page_whose_truncation_or_fields_cannot_be_read_is_refused_never_partial() {
    let mut failures = Vec::new();
    for verb in LISTING_VERBS {
        for (case, from, to, said) in SPOILED_PAGES {
            let name = format!("{verb:?} {case}");
            let document = verb.document();
            assert!(document.contains(from), "{name}: nothing to replace");
            let page = document.replacen(from, to, 1);
            let response = ok_with_length(case, page.len(), page.as_bytes());
            let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
            let client = client(endpoint.addr, deadlines());
            let read = bounded(&name, call_document_verb(verb, &client)).await;
            drop(client);
            let heads = endpoint.heads().await;
            if heads.len() != 1 {
                failures.push(format!("{name}: not one request: {heads:?}"));
                continue;
            }
            match refused_answer(&read, 1) {
                Some(Answer {
                    head,
                    outcome: Outcome::Unreadable { body, detail },
                }) if head.status == 200
                    && head.request_id() == Some(case)
                    && body[..] == *page.as_bytes()
                    && detail.contains(said) =>
                {
                    eprintln!("GREEN {name}: {detail}");
                }
                _ => failures.push(format!("{name}: not refused for {said:?}: {read:?}")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) not refused as they must be:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn a_listing_whose_later_page_is_unreadable_reports_no_listing_at_all() {
    let first = listing_v2(&["list/a", "list/b"], Some("page-two"));
    let second = listing_v2(&["list/c"], None);
    let cut = &second[..second.len() - 20];
    let mut endpoint = Endpoint::start(vec![
        Script::ended(ok_with_length("page-1", first.len(), first.as_bytes())),
        Script::ended(ok_with_length("page-2", cut.len(), cut.as_bytes())),
    ])
    .await;
    let client = client(endpoint.addr, deadlines());
    let listed = bounded("ListObjectsV2", client.list_objects_v2("list/", 2)).await;
    drop(client);
    let heads = endpoint.heads().await;
    assert_eq!(
        heads.len(),
        2,
        "the client asked for the second page: {heads:?}"
    );
    let shown = listed.as_ref().map_err(ToString::to_string).err();
    assert!(
        shown.as_deref().is_some_and(|shown| shown.starts_with(
            "listing page 2: an answer with no result: HTTP 200, x-amz-request-id page-2: \
             an unreadable"
        )),
        "the failure in words names the page and the answer: {shown:?}"
    );
    let (_, _, query) = request_line(&heads[1]);
    assert!(
        query_has(query, "continuation-token"),
        "the second request continues from the first page's token: {}",
        heads[1]
    );
    match listed {
        Err(ListingError::Page {
            page: 2,
            cause: NoResult::Answered(answer),
        }) => {
            assert_eq!(answer.head.status, 200, "{answer:?}");
            assert_eq!(answer.head.request_id(), Some("page-2"));
            assert!(
                matches!(answer.outcome, Outcome::Unreadable { .. }),
                "{answer:?}"
            );
        }
        other => panic!("a cut second page must fail the whole listing: {other:?}"),
    }
}

#[tokio::test]
async fn a_truncated_listing_page_that_names_nothing_to_continue_from_is_refused() {
    // Truncated, with keys, and no <NextContinuationToken>: V2 cannot continue.
    let page = listing_v2(&["list/a", "list/b"], Some("x"))
        .replace("<NextContinuationToken>x</NextContinuationToken>", "");
    let mut endpoint = Endpoint::start(vec![Script::ended(ok_with_length(
        "truncated",
        page.len(),
        page.as_bytes(),
    ))])
    .await;
    let client = client(endpoint.addr, deadlines());
    let listed = bounded("ListObjectsV2", client.list_objects_v2("list/", 2)).await;
    drop(client);
    let _ = endpoint.heads().await;
    match listed {
        Err(ListingError::Page {
            page: 1,
            cause: NoResult::Answered(answer),
        }) if answer.head.status == 200 => {
            assert!(
                matches!(&answer.outcome, Outcome::Unreadable { detail, .. }
                    if detail.contains("NextContinuationToken")),
                "{answer:?}"
            );
        }
        other => panic!("a truncated page with no continuation must be refused: {other:?}"),
    }

    // Truncated, with no <NextMarker> and no key to continue after: V1 cannot continue.
    let page = listing_v1_page(&[], true, None);
    let mut endpoint = Endpoint::start(vec![Script::ended(ok_with_length(
        "truncated-v1",
        page.len(),
        page.as_bytes(),
    ))])
    .await;
    // The `client` above shadows the function of that name.
    let v1_client = crate::client(endpoint.addr, deadlines());
    let listed = bounded("ListObjects", v1_client.list_objects_v1("list/", 2)).await;
    drop(v1_client);
    let _ = endpoint.heads().await;
    match listed {
        Err(ListingError::Page {
            page: 1,
            cause: NoResult::Answered(answer),
        }) if answer.head.status == 200 => {
            assert!(
                matches!(&answer.outcome, Outcome::Unreadable { detail, .. }
                    if detail.contains("NextMarker")),
                "{answer:?}"
            );
        }
        other => panic!("a truncated V1 page with nothing to continue after: {other:?}"),
    }
}

#[tokio::test]
async fn a_truncated_v1_page_whose_last_key_is_empty_is_refused_and_asks_for_no_next_page() {
    // A `marker=` of nothing would start the listing over, and a listing that then ends would be
    // reported whole. One script: a continuation request would reach no endpoint, so it could
    // not pass as a refusal of page 1.
    let cases: [(&str, &[&str], Option<&str>); 4] = [
        ("only-key-no-marker", &[""], None),
        ("only-key-empty-marker", &[""], Some("")),
        ("last-key-no-marker", &["list/a", ""], None),
        ("last-key-empty-marker", &["list/a", ""], Some("")),
    ];
    for (case, keys, next_marker) in cases {
        let page = listing_v1_page(keys, true, next_marker);
        let mut endpoint = Endpoint::start(vec![Script::ended(ok_with_length(
            case,
            page.len(),
            page.as_bytes(),
        ))])
        .await;
        let client = client(endpoint.addr, deadlines());
        let listed = bounded(case, client.list_objects_v1("list/", 2)).await;
        drop(client);
        let heads = endpoint.heads().await;
        assert_eq!(heads.len(), 1, "{case}: the one request: {heads:?}");
        match listed {
            Err(ListingError::Page {
                page: 1,
                cause: NoResult::Answered(answer),
            }) => {
                assert_eq!(answer.head.status, 200, "{case}: {answer:?}");
                assert_eq!(answer.head.request_id(), Some(case), "{answer:?}");
                assert!(
                    matches!(&answer.outcome, Outcome::Unreadable { body, detail }
                        if body == page.as_bytes() && detail.contains("NextMarker")),
                    "{case}: {answer:?}"
                );
            }
            other => panic!("{case}: page 1 must be refused: {other:?}"),
        }
    }
}

// --- #956 (F): a mixed <DeleteResult> is reported row by row ----------------------------------

#[tokio::test]
async fn a_mixed_delete_result_is_reported_row_by_row_in_document_order() {
    let document = format!(
        "{XML_DECL}<DeleteResult xmlns=\"{S3_NS}\">\
         <Deleted><Key>mixed/a</Key></Deleted>\
         <Error><Key>mixed/b</Key><Code>AccessDenied</Code><Message>Access Denied</Message>\
         </Error>\
         <Deleted><Key>mixed/c</Key></Deleted>\
         </DeleteResult>"
    );
    let mut endpoint = Endpoint::start(vec![Script::ended(ok_with_length(
        "mixed",
        document.len(),
        document.as_bytes(),
    ))])
    .await;
    let client = client(endpoint.addr, deadlines());
    let keys = ["mixed/a", "mixed/b", "mixed/c"];
    let result = bounded("DeleteObjects", client.delete_objects(&keys)).await;
    drop(client);
    let heads = endpoint.heads().await;
    let (method, path, query) = request_line(&heads[0]);
    assert_eq!((method, path), ("POST", format!("/{BUCKET}/").as_str()));
    assert_eq!(
        operation_params(query),
        [("delete".to_string(), String::new())],
        "{}",
        heads[0]
    );

    let result = result.unwrap_or_else(|e| panic!("DeleteObjects: {e}"));
    assert_eq!(result.head.status, 200);
    assert_eq!(result.head.request_id(), Some("mixed"));
    let denied = KeyError {
        key: "mixed/b".to_string(),
        code: ErrorCode::Code("AccessDenied".to_string()),
        message: Some("Access Denied".to_string()),
    };
    assert_eq!(
        result.rows,
        [
            DeleteRow::Deleted {
                key: "mixed/a".to_string()
            },
            DeleteRow::Error(denied.clone()),
            DeleteRow::Deleted {
                key: "mixed/c".to_string()
            },
        ],
        "every row, as sent, in document order"
    );
    assert_eq!(
        result.deleted().collect::<Vec<_>>(),
        ["mixed/a", "mixed/c"],
        "exactly the two deleted keys"
    );
    assert_eq!(
        result.errors().cloned().collect::<Vec<_>>(),
        [denied],
        "exactly the one error row, its key, code and message as sent"
    );
    assert_eq!(result.requested, keys);
    assert_eq!(result.keys_without_a_row().count(), 0, "{result:?}");
    assert_eq!(result.rows_not_requested().count(), 0, "{result:?}");
}

/// A `<Deleted>` row of `key`, as read.
fn deleted_row(key: &str) -> DeleteRow {
    DeleteRow::Deleted {
        key: key.to_string(),
    }
}

/// An `<Error>` row of `key` with code `AccessDenied` and no message, as read.
fn denied_row(key: &str) -> DeleteRow {
    DeleteRow::Error(KeyError {
        key: key.to_string(),
        code: ErrorCode::Code("AccessDenied".to_string()),
        message: None,
    })
}

#[tokio::test]
async fn a_delete_result_reports_requested_keys_with_no_row_and_rows_naming_no_requested_key() {
    struct Case {
        name: &'static str,
        keys: &'static [&'static str],
        rows: &'static str,
        expected_rows: Vec<DeleteRow>,
        without_a_row: &'static [&'static str],
        not_requested: Vec<DeleteRow>,
    }
    let cases = [
        Case {
            name: "empty",
            keys: &["a", "b"],
            rows: "",
            expected_rows: vec![],
            without_a_row: &["a", "b"],
            not_requested: vec![],
        },
        Case {
            name: "partial",
            keys: &["a", "b", "c"],
            rows: "<Deleted><Key>b</Key></Deleted>",
            expected_rows: vec![deleted_row("b")],
            without_a_row: &["a", "c"],
            not_requested: vec![],
        },
        Case {
            name: "unrequested-deleted",
            keys: &["a"],
            rows: "<Deleted><Key>c</Key></Deleted>",
            expected_rows: vec![deleted_row("c")],
            without_a_row: &["a"],
            not_requested: vec![deleted_row("c")],
        },
        Case {
            name: "unrequested-error",
            keys: &["a"],
            rows: "<Deleted><Key>a</Key></Deleted>\
                   <Error><Key>c</Key><Code>AccessDenied</Code></Error>",
            expected_rows: vec![deleted_row("a"), denied_row("c")],
            without_a_row: &[],
            not_requested: vec![denied_row("c")],
        },
        // An error row accounts for its key as a deleted row does: judging it is the caller's.
        Case {
            name: "all-errors",
            keys: &["a", "b"],
            rows: "<Error><Key>b</Key><Code>AccessDenied</Code></Error>\
                   <Error><Key>a</Key><Code>AccessDenied</Code></Error>",
            expected_rows: vec![denied_row("b"), denied_row("a")],
            without_a_row: &[],
            not_requested: vec![],
        },
        // Compared once the document's escapes are read, and exactly: `A` is not `a`, nor is
        // ` a` (S3 allows a key of white space, which is a key).
        Case {
            name: "as-sent",
            keys: &["x&y", "a", " "],
            rows: "<Deleted><Key>x&amp;y</Key></Deleted>\
                   <Deleted><Key>A</Key></Deleted>\
                   <Deleted><Key> </Key></Deleted>",
            expected_rows: vec![deleted_row("x&y"), deleted_row("A"), deleted_row(" ")],
            without_a_row: &["a"],
            not_requested: vec![deleted_row("A")],
        },
        // A key asked for twice and a key answered twice: every line as it came.
        Case {
            name: "repeated",
            keys: &["a", "a", "b", "b"],
            rows: "<Deleted><Key>b</Key></Deleted><Deleted><Key>b</Key></Deleted>",
            expected_rows: vec![deleted_row("b"), deleted_row("b")],
            without_a_row: &["a", "a"],
            not_requested: vec![],
        },
        // Compared with the document's line endings read as XML reads them (XML 1.0 §2.11): a
        // raw carriage return, alone or before a line feed, is one line feed, so a key that
        // holds one and is written raw comes back changed and names no requested key; one
        // escaped as `&#13;` is read as the carriage return and names its key.
        Case {
            name: "line-endings",
            keys: &["cr\rkey", "crlf\r\nkey", "escaped\rkey"],
            rows: "<Deleted><Key>cr\rkey</Key></Deleted>\
                   <Deleted><Key>crlf\r\nkey</Key></Deleted>\
                   <Deleted><Key>escaped&#13;key</Key></Deleted>",
            expected_rows: vec![
                deleted_row("cr\nkey"),
                deleted_row("crlf\nkey"),
                deleted_row("escaped\rkey"),
            ],
            without_a_row: &["cr\rkey", "crlf\r\nkey"],
            not_requested: vec![deleted_row("cr\nkey"), deleted_row("crlf\nkey")],
        },
    ];
    for case in cases {
        let name = case.name;
        let document = delete_result_of(case.rows);
        let response = ok_with_length(name, document.len(), document.as_bytes());
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let result = bounded(name, client.delete_objects(case.keys))
            .await
            .unwrap_or_else(|e| panic!("{name}: reported, not refused: {e}"));
        drop(client);
        let _ = endpoint.heads().await;
        assert_eq!(result.head.status, 200, "{name}: {result:?}");
        assert_eq!(result.requested, case.keys, "{name}: {result:?}");
        assert_eq!(result.rows, case.expected_rows, "{name}: every row as sent");
        assert_eq!(
            result.keys_without_a_row().collect::<Vec<_>>(),
            case.without_a_row,
            "{name}: the requested keys no row names, in the order given: {result:?}"
        );
        assert_eq!(
            result.rows_not_requested().cloned().collect::<Vec<_>>(),
            case.not_requested,
            "{name}: the rows that name no requested key, in document order: {result:?}"
        );
    }
}

/// A `<DeleteResult>` of `rows`.
fn delete_result_of(rows: &str) -> String {
    format!("{XML_DECL}<DeleteResult xmlns=\"{S3_NS}\">{rows}</DeleteResult>")
}

/// `<DeleteResult>` rows that hold no row the client can report, and the words of the refusal.
const SPOILED_ROWS: [(&str, &str, &str); 14] = [
    (
        "deleted-no-key",
        "<Deleted><VersionId>1</VersionId></Deleted>",
        "a <Deleted> row names no <Key>",
    ),
    (
        "error-no-key",
        "<Error><Code>AccessDenied</Code></Error>",
        "an <Error> row names no <Key>",
    ),
    (
        "deleted-key-twice",
        "<Deleted><Key>a</Key><Key>b</Key></Deleted>",
        "more than one <Key>",
    ),
    (
        "error-code-twice",
        "<Error><Key>a</Key><Code>x</Code><Code>y</Code></Error>",
        "more than one <Code>",
    ),
    (
        "deleted-key-markup",
        "<Deleted><Key><b>a</b></Key></Deleted>",
        "holds markup",
    ),
    (
        "error-message-markup",
        "<Error><Key>a</Key><Message><b>m</b></Message></Error>",
        "holds markup",
    ),
    // An empty `<Key>` names no key, as an empty `<Code>` names no code.
    (
        "deleted-self-closed-key",
        "<Deleted><Key/></Deleted>",
        "a <Deleted> row names no <Key>",
    ),
    (
        "deleted-empty-key",
        "<Deleted><Key></Key></Deleted>",
        "a <Deleted> row names no <Key>",
    ),
    (
        "error-empty-key",
        "<Error><Key></Key><Code>AccessDenied</Code></Error>",
        "an <Error> row names no <Key>",
    ),
    // S3 sends nothing there but rows, and a failed key must not vanish in a row left unread:
    // a row in the wrong case, an element of no kind, a row's field on its own, text, and a
    // processing instruction.
    (
        "error-row-in-the-wrong-case",
        "<error><Key>a</Key><Code>AccessDenied</Code></error>",
        "the <DeleteResult> holds a <error>",
    ),
    (
        "element-of-no-kind",
        "<Failed><Key>a</Key></Failed>",
        "the <DeleteResult> holds a <Failed>",
    ),
    (
        "bare-key",
        "<Key>a</Key>",
        "the <DeleteResult> holds a <Key>",
    ),
    (
        "text-outside-any-row",
        "and that is all",
        "the <DeleteResult> holds text outside any row",
    ),
    (
        "processing-instruction",
        "<?pi data?>",
        "the <DeleteResult> holds a processing instruction",
    ),
];

#[tokio::test]
async fn a_delete_result_row_that_cannot_be_read_is_refused_never_a_short_result() {
    let mut failures = Vec::new();
    for (case, row, said) in SPOILED_ROWS {
        // A good row first: the refusal discards it.
        let document = delete_result_of(&format!("<Deleted><Key>good</Key></Deleted>{row}"));
        let response = ok_with_length(case, document.len(), document.as_bytes());
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let result = bounded(case, client.delete_objects(&["good", "a"])).await;
        drop(client);
        let _ = endpoint.heads().await;
        match result {
            Err(NoResult::Answered(Answer {
                head,
                outcome: Outcome::Unreadable { body, detail },
            })) if head.status == 200
                && head.request_id() == Some(case)
                && body[..] == *document.as_bytes()
                && detail.contains(said) =>
            {
                eprintln!("GREEN {case}: {detail}");
            }
            other => failures.push(format!("{case}: not refused for {said:?}: {other:?}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) not refused as they must be:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn white_space_and_comments_beside_delete_result_rows_are_read_past() {
    let document =
        delete_result_of("\r\n\t <!-- a comment -->\n  <Deleted><Key>a</Key></Deleted>\n");
    let response = ok_with_length("laid-out", document.len(), document.as_bytes());
    let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
    let client = client(endpoint.addr, deadlines());
    let result = bounded("laid-out", client.delete_objects(&["a"]))
        .await
        .unwrap_or_else(|e| panic!("a laid-out document: {e}"));
    drop(client);
    let _ = endpoint.heads().await;
    assert_eq!(result.rows, [deleted_row("a")]);
}

#[tokio::test]
async fn a_delete_error_row_with_no_code_or_message_reports_none_and_a_rows_own_other_fields_are_passed_over(
) {
    // `<VersionId>`, `<DeleteMarker>` and `<DeleteMarkerVersionId>` are what S3 sends in a row
    // beside the fields the client reads.
    let document = delete_result_of(
        "<Error><Key>no-code</Key><Message>why</Message></Error>\
         <Error><Key>empty-code</Key><Code></Code><VersionId>1</VersionId></Error>\
         <Deleted><Key>versioned</Key><VersionId>2</VersionId><DeleteMarker>true</DeleteMarker>\
         <DeleteMarkerVersionId>3</DeleteMarkerVersionId></Deleted>",
    );
    let response = ok_with_length("sparse", document.len(), document.as_bytes());
    let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
    let client = client(endpoint.addr, deadlines());
    let result = bounded(
        "DeleteObjects",
        client.delete_objects(&["no-code", "empty-code", "versioned"]),
    )
    .await
    .unwrap_or_else(|e| panic!("DeleteObjects: {e}"));
    drop(client);
    let _ = endpoint.heads().await;
    assert_eq!(
        result.rows,
        [
            DeleteRow::Error(KeyError {
                key: "no-code".to_string(),
                code: ErrorCode::MissingInXml,
                message: Some("why".to_string()),
            }),
            DeleteRow::Error(KeyError {
                key: "empty-code".to_string(),
                code: ErrorCode::MissingInXml,
                message: None,
            }),
            deleted_row("versioned"),
        ],
        "nothing filled in on the server's behalf"
    );
}

// --- (G) on the scripted endpoint: header lines, listing continuation, the page cap -------

#[tokio::test]
async fn a_head_reports_every_header_line_a_repeated_one_and_one_that_is_not_text_included() {
    let etag = "\"0123456789abcdef0123456789abcdef\"";
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nx-amz-request-id: lines\r\nx-amz-meta-dup: first\r\n\
         ETag: {etag}\r\nX-Amz-Meta-Dup: second\r\nx-amz-meta-raw: value-"
    )
    .into_bytes();
    // A value that is not UTF-8 (Latin-1 `é`), which HTTP allows as obs-text.
    response.extend_from_slice(b"\xe9\r\nContent-Length: 0\r\n\r\n");
    let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
    let client = client(endpoint.addr, deadlines());
    let answer = bounded("HEAD", client.head_object("lines/object"))
        .await
        .unwrap_or_else(|e| panic!("HEAD: no answer: {e}"));
    drop(client);
    let heads = endpoint.heads().await;
    assert_eq!(request_line(&heads[0]).0, "HEAD", "{}", heads[0]);

    let head = &answer.head;
    assert_eq!(head.status, 200, "{answer:?}");
    assert_eq!(
        head.header_lines("X-Amz-Meta-Dup").collect::<Vec<_>>(),
        [&b"first"[..], &b"second"[..]],
        "every line of a repeated header, in the order sent, its name in any case"
    );
    assert_eq!(
        head.header("x-amz-meta-dup"),
        None,
        "a repeated header is never read as one of its lines"
    );
    assert_eq!(
        head.header_lines("x-amz-meta-raw").collect::<Vec<_>>(),
        [&b"value-\xe9"[..]],
        "a value that is not text, its bytes as sent"
    );
    assert_eq!(head.header("x-amz-meta-raw"), None, "not text");
    assert_eq!(head.header("etag"), Some(etag));
    assert_eq!(head.header_lines("x-amz-meta-absent").count(), 0);

    // Every line, each name lowercased; hyper groups a name's lines, so compare per name.
    let mut fields: Vec<(&str, &[u8])> = head.fields().collect();
    let mut sent: Vec<(&str, &[u8])> = vec![
        ("x-amz-request-id", b"lines"),
        ("x-amz-meta-dup", b"first"),
        ("etag", etag.as_bytes()),
        ("x-amz-meta-dup", b"second"),
        ("x-amz-meta-raw", b"value-\xe9"),
        ("content-length", b"0"),
    ];
    // Stable sorts: each name's lines keep their order.
    fields.sort_by_key(|&(name, _)| name);
    sent.sort_by_key(|&(name, _)| name);
    assert_eq!(fields, sent, "every header field line, as sent");
}

/// `value` with every `%XX` escape decoded (the test's own names are ASCII).
fn percent_decoded(value: &str) -> String {
    let mut out = Vec::new();
    let mut bytes = value.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let hex: String = bytes.by_ref().take(2).map(char::from).collect();
            out.push(u8::from_str_radix(&hex, 16).expect("a %XX escape"));
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).expect("UTF-8")
}

/// The decoded value of query parameter `name`, when the query names it.
fn query_value(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (field, value) = pair.split_once('=').unwrap_or((pair, ""));
        (field == name).then(|| percent_decoded(value))
    })
}

#[tokio::test]
async fn list_objects_v1_follows_a_next_marker_and_the_last_key_where_a_page_names_none() {
    let pages = [
        // A <NextMarker> that is not the page's last key: followed, not the key.
        listing_v1_page(&["list/a", "list/b"], true, Some("list/b-continued")),
        // No <NextMarker>: continue after the last key, S3's rule for version 1.
        listing_v1_page(&["list/c"], true, None),
        // An empty <NextMarker> names nothing: the last key again.
        listing_v1_page(&["list/d"], true, Some("")),
        listing_v1_page(&["list/e"], false, None),
    ];
    let scripts = pages
        .iter()
        .enumerate()
        .map(|(i, page)| {
            Script::ended(ok_with_length(
                &format!("v1-page-{}", i + 1),
                page.len(),
                page.as_bytes(),
            ))
        })
        .collect();
    let mut endpoint = Endpoint::start(scripts).await;
    let client = client(endpoint.addr, deadlines());
    let listed = bounded("ListObjects", client.list_objects_v1("list/", 2)).await;
    drop(client);
    let heads = endpoint.heads().await;

    let markers: Vec<Option<String>> = heads
        .iter()
        .map(|head| query_value(request_line(head).2, "marker"))
        .collect();
    assert_eq!(
        markers,
        [
            None,
            Some("list/b-continued".to_string()),
            Some("list/c".to_string()),
            Some("list/d".to_string()),
        ],
        "each request continues from what the page before it named: {heads:?}"
    );
    let listing = listed.unwrap_or_else(|e| panic!("ListObjects: {e}"));
    let keys: Vec<&str> = listing.entries.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, ["list/a", "list/b", "list/c", "list/d", "list/e"]);
    assert_eq!(listing.pages.len(), 4);
}

#[tokio::test]
async fn a_listing_still_truncated_after_max_listing_pages_pages_stops_there() {
    for verb in [DocumentVerb::ListV2, DocumentVerb::ListV1] {
        let scripts = (1..=MAX_LISTING_PAGES)
            .map(|page| {
                let key = format!("list/{page:03}");
                let body = match verb {
                    DocumentVerb::ListV2 => listing_v2(&[&key], Some(&format!("token-{page}"))),
                    _ => listing_v1_page(&[&key], true, None),
                };
                Script::ended(ok_with_length(
                    &format!("page-{page}"),
                    body.len(),
                    body.as_bytes(),
                ))
            })
            .collect();
        let mut endpoint = Endpoint::start(scripts).await;
        let client = client(endpoint.addr, deadlines());
        let read = bounded(
            "a listing that never ends",
            call_document_verb(verb, &client),
        )
        .await;
        drop(client);
        let heads = endpoint.heads().await;
        assert_eq!(
            heads.len(),
            MAX_LISTING_PAGES,
            "{verb:?}: one request a page, and none past the cap"
        );
        match read {
            Read::Listing(error @ ListingError::TooManyPages { pages }) => {
                assert_eq!(pages, MAX_LISTING_PAGES, "{verb:?}");
                assert_eq!(
                    error.to_string(),
                    format!("the listing was still truncated after {MAX_LISTING_PAGES} pages")
                );
            }
            other => panic!("{verb:?}: not stopped at the cap: {other:?}"),
        }
    }
}

// --- (G) a 304 to a conditional GET, and a HEAD's answer, may frame a body they do not have ---

/// The `Transfer-Encoding` field lines a response may carry where it has no body (RFC 9112 §6.1
/// allows any coding: the ones the server would have applied), each case's lines as sent.
const CODINGS_ON_NO_BODY: [&[&str]; 4] = [
    &["chunked"],
    &["gzip, chunked"],
    &["gzip"],
    &["gzip", "chunked"],
];

/// The head of `status` carrying `codings`, one field line each.
fn head_with_codings(status: &str, request_id: &str, codings: &[&str]) -> Vec<u8> {
    let headers: Vec<(&str, String)> = codings
        .iter()
        .map(|coding| ("Transfer-Encoding", (*coding).to_string()))
        .collect();
    head(status, request_id, &headers)
}

/// Whether `answer`'s head carries exactly the `Transfer-Encoding` lines of `codings`.
fn carries_codings(answer: &Answer, codings: &[&str]) -> bool {
    let sent: Vec<&[u8]> = answer.head.header_lines("transfer-encoding").collect();
    sent == codings.iter().map(|c| c.as_bytes()).collect::<Vec<_>>()
}

#[tokio::test]
async fn a_304_to_a_conditional_get_that_frames_a_body_with_any_coding_is_not_refused() {
    let etag = "\"0123456789abcdef0123456789abcdef\"";
    // RFC 9112 §6.1: a `Transfer-Encoding` on a `304` only in answer to a GET, which this is;
    // hyper gives a `304` no body whatever its headers say.
    for codings in CODINGS_ON_NO_BODY {
        let response = head_with_codings("304 Not Modified", "conditional-304", codings);
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let fetched = bounded(
            "GET If-None-Match",
            client.get_object_if_none_match("conditional/object", etag),
        )
        .await
        .unwrap_or_else(|e| panic!("GET If-None-Match {codings:?}: no answer: {e}"));
        drop(client);
        let heads = endpoint.heads().await;
        let [request] = heads.as_slice() else {
            panic!("expected one request, saw {heads:?}");
        };
        assert_eq!(request_line(request).0, "GET", "{request}");
        assert_eq!(
            request_header(request, "if-none-match"),
            Some(etag),
            "the tag as given: {request}"
        );

        let Fetched::Other(not_modified) = fetched else {
            panic!("a 304 is not a 2xx: {fetched:?}");
        };
        assert_eq!(not_modified.head.status, 304, "{not_modified:?}");
        assert_eq!(not_modified.head.request_id(), Some("conditional-304"));
        assert!(
            carries_codings(&not_modified, codings),
            "the framing header as sent, {codings:?}: {not_modified:?}"
        );
        assert_eq!(
            not_modified.outcome,
            Outcome::Error {
                code: ErrorCode::NoBody,
                message: None
            },
            "a GET's 304 that frames a body with {codings:?} is read as the bodiless 304 it is"
        );
    }
}

#[tokio::test]
async fn a_head_answer_that_frames_a_body_with_any_coding_is_read_as_the_bodiless_answer_it_is() {
    // RFC 9112 §6.1 allows the header on a response to a HEAD, whatever its status.
    let answers = [
        ("200 OK", 200, Outcome::Success { body: Bytes::new() }),
        (
            "404 Not Found",
            404,
            Outcome::Error {
                code: ErrorCode::NoBody,
                message: None,
            },
        ),
        (
            "304 Not Modified",
            304,
            Outcome::Error {
                code: ErrorCode::NoBody,
                message: None,
            },
        ),
    ];
    for (status, code, expected) in answers {
        for codings in CODINGS_ON_NO_BODY {
            let response = head_with_codings(status, "head-coding", codings);
            let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
            let client = client(endpoint.addr, deadlines());
            let answer = bounded("HEAD", client.head_object("head/object"))
                .await
                .unwrap_or_else(|e| panic!("HEAD {status} {codings:?}: no answer: {e}"));
            drop(client);
            let _ = endpoint.heads().await;
            assert_eq!(answer.head.status, code, "{answer:?}");
            assert_eq!(answer.head.request_id(), Some("head-coding"));
            assert!(
                carries_codings(&answer, codings),
                "the framing header as sent, {codings:?}: {answer:?}"
            );
            assert_eq!(answer.outcome, expected, "{status} {codings:?}");
        }
    }
}

#[tokio::test]
async fn bytes_after_a_bodiless_answer_that_frames_a_body_are_still_an_overrun() {
    let with_bytes = |status: &str| {
        let mut response = head_with_codings(status, "coding-bytes", &["gzip, chunked"]);
        response.extend_from_slice(b"hello");
        response
    };
    let mut endpoint = Endpoint::start(vec![Script::ended(with_bytes("304 Not Modified"))]).await;
    let client = crate::client(endpoint.addr, deadlines());
    let fetched = bounded(
        "GET If-None-Match",
        client.get_object_if_none_match("conditional/object", SOME_ETAG),
    )
    .await
    .unwrap_or_else(|e| panic!("GET If-None-Match: no answer: {e}"));
    drop(client);
    let _ = endpoint.heads().await;
    let Fetched::Other(not_modified) = fetched else {
        panic!("a 304 is not a 2xx: {fetched:?}");
    };
    assert_eq!(not_modified.head.status, 304, "{not_modified:?}");
    assert!(
        matches!(
            not_modified.outcome,
            Outcome::BodyFailed(BodyFailure::Overrun { received: 0, after }) if after > 0
        ),
        "{not_modified:?}"
    );

    let mut endpoint = Endpoint::start(vec![Script::ended(with_bytes("200 OK"))]).await;
    let client = crate::client(endpoint.addr, deadlines());
    let answer = bounded("HEAD", client.head_object("head/object"))
        .await
        .unwrap_or_else(|e| panic!("HEAD: no answer: {e}"));
    drop(client);
    let _ = endpoint.heads().await;
    assert_eq!(answer.head.status, 200, "{answer:?}");
    assert!(
        matches!(
            answer.outcome,
            Outcome::BodyFailed(BodyFailure::Overrun { received: 0, after }) if after > 0
        ),
        "{answer:?}"
    );
}

#[tokio::test]
async fn a_bodiless_answer_whose_framing_no_server_may_send_stays_refused() {
    let refused = |coding: Option<&str>, length: Option<&str>| {
        Outcome::BodyFailed(BodyFailure::Framing {
            transfer_encoding: coding.map(str::to_string),
            content_length: length.map(str::to_string),
        })
    };
    let both: [(&str, String); 2] = [
        ("Transfer-Encoding", "gzip, chunked".to_string()),
        ("Content-Length", "5".to_string()),
    ];
    let not_a_length: [(&str, String); 1] = [("Content-Length", "five".to_string())];
    let te_only: [(&str, String); 1] = [("Transfer-Encoding", "gzip, chunked".to_string())];
    let both_refused = refused(Some("gzip, chunked"), Some("5"));
    let length_refused = refused(None, Some("five"));
    // Both headers (RFC 9112 §6.2 forbids them in any message) and a length that is none.
    type Headers<'a> = &'a [(&'a str, String)];
    let conditional: [(Headers<'_>, &Outcome); 2] =
        [(&both, &both_refused), (&not_a_length, &length_refused)];
    for (headers, expected) in conditional {
        let response = head("304 Not Modified", "no-body-refused", headers);
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = crate::client(endpoint.addr, deadlines());
        let fetched = bounded(
            "GET If-None-Match",
            client.get_object_if_none_match("conditional/object", SOME_ETAG),
        )
        .await
        .unwrap_or_else(|e| panic!("GET If-None-Match: no answer: {e}"));
        drop(client);
        let _ = endpoint.heads().await;
        let Fetched::Other(answer) = fetched else {
            panic!("a 304 is not a 2xx: {fetched:?}");
        };
        assert_eq!(answer.head.status, 304, "{answer:?}");
        assert_eq!(&answer.outcome, expected, "{headers:?}");
    }
    // The same on a HEAD, and a `204` that frames a body, which RFC 9112 §6.1 forbids in answer
    // to anything.
    let te_refused = refused(Some("gzip, chunked"), None);
    let on_head: [(&str, u16, Headers<'_>, &Outcome); 3] = [
        ("200 OK", 200, &both, &both_refused),
        ("200 OK", 200, &not_a_length, &length_refused),
        ("204 No Content", 204, &te_only, &te_refused),
    ];
    for (status, code, headers, expected) in on_head {
        let response = head(status, "no-body-refused", headers);
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = crate::client(endpoint.addr, deadlines());
        let answer = bounded("HEAD", client.head_object("head/object"))
            .await
            .unwrap_or_else(|e| panic!("HEAD {status}: no answer: {e}"));
        drop(client);
        let _ = endpoint.heads().await;
        assert_eq!(answer.head.status, code, "{answer:?}");
        assert_eq!(&answer.outcome, expected, "{status} {headers:?}");
    }
}

#[tokio::test]
async fn a_304_to_a_conditional_get_that_declares_a_content_length_is_not_refused() {
    let etag = "\"0123456789abcdef0123456789abcdef\"";
    // RFC 9110 §8.6: a `304` to a conditional GET may carry the `Content-Length` a `200` would
    // have; hyper gives it no body whatever that length says.
    let response = head(
        "304 Not Modified",
        "conditional-304-length",
        &[
            ("ETag", etag.to_string()),
            ("Content-Length", OBJECT_LEN.to_string()),
        ],
    );
    let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
    let client = client(endpoint.addr, deadlines());
    let fetched = bounded(
        "GET If-None-Match",
        client.get_object_if_none_match("conditional/object", etag),
    )
    .await
    .unwrap_or_else(|e| panic!("GET If-None-Match: no answer: {e}"));
    drop(client);
    let _ = endpoint.heads().await;
    let Fetched::Other(not_modified) = fetched else {
        panic!("a 304 is not a 2xx: {fetched:?}");
    };
    assert_eq!(not_modified.head.status, 304, "{not_modified:?}");
    assert_eq!(
        not_modified.head.header("content-length"),
        Some(OBJECT_LEN.to_string().as_str()),
        "the length as sent: {not_modified:?}"
    );
    assert_eq!(
        not_modified.outcome,
        Outcome::Error {
            code: ErrorCode::NoBody,
            message: None
        },
        "a GET's 304 that declares a length is read as the bodiless 304 it is"
    );

    // Bytes after that head are still bytes after the end of the response.
    let mut response = head(
        "304 Not Modified",
        "conditional-304-bytes",
        &[("Content-Length", "5".to_string())],
    );
    response.extend_from_slice(b"hello");
    let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
    let client = crate::client(endpoint.addr, deadlines());
    let fetched = bounded(
        "GET If-None-Match",
        client.get_object_if_none_match("conditional/object", etag),
    )
    .await
    .unwrap_or_else(|e| panic!("GET If-None-Match: no answer: {e}"));
    drop(client);
    let _ = endpoint.heads().await;
    let Fetched::Other(overrun) = fetched else {
        panic!("a 304 is not a 2xx: {fetched:?}");
    };
    assert_eq!(overrun.head.status, 304, "{overrun:?}");
    assert!(
        matches!(
            overrun.outcome,
            Outcome::BodyFailed(BodyFailure::Overrun { received: 0, after }) if after > 0
        ),
        "{overrun:?}"
    );
}

// --- a HEAD's body: there is none, and bytes sent as one are refused beside the 200 --------

#[tokio::test]
async fn a_head_answered_with_a_body_keeps_its_200_and_reports_the_bytes_as_an_overrun() {
    let with_length = {
        let mut response = head(
            "200 OK",
            "head-body",
            &[("Content-Length", "5".to_string())],
        );
        response.extend_from_slice(b"hello");
        response
    };
    let with_no_length = {
        let mut response = head("200 OK", "head-body", &[]);
        response.extend_from_slice(b"hello");
        response
    };
    for (case, response) in [
        ("with a length", with_length),
        ("with no length", with_no_length),
    ] {
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let answer = bounded(case, client.head_object("head/object"))
            .await
            .unwrap_or_else(|e| panic!("HEAD {case}: no answer: {e}"));
        drop(client);
        let _ = endpoint.heads().await;
        assert_eq!(answer.head.status, 200, "{case}: {answer:?}");
        assert_eq!(answer.head.request_id(), Some("head-body"), "{answer:?}");
        assert!(
            matches!(
                answer.outcome,
                Outcome::BodyFailed(BodyFailure::Overrun { received: 0, after }) if after > 0
            ),
            "{case}: {answer:?}"
        );
    }
}

// --- framing that cannot mark where a body ends: refused, the status kept -------------------

#[tokio::test]
async fn an_answer_whose_framing_cannot_mark_its_end_is_refused_beside_its_status() {
    let error = b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
    let mut failures = Vec::new();
    for verb in READ_VERBS {
        // A listing page's 2xx is read whole here; a GET's streams, so only its error is.
        let listing = matches!(verb, ReadVerb::ListV2 | ReadVerb::ListV1);
        let page = listing_v2(&["list/a"], None);
        let answers: &[(&str, u16, &[u8])] = if listing {
            &[
                ("200 OK", 200, page.as_bytes()),
                ("403 Forbidden", 403, error),
            ]
        } else {
            &[("403 Forbidden", 403, error)]
        };
        for &(status, code, body) in answers {
            // Without the refusal each of these reads as a whole body: `gzip` to the
            // connection's close, and the chunked one to its terminal chunk.
            let unknown_coding = {
                let mut response = head(
                    status,
                    "framing",
                    &[("Transfer-Encoding", "gzip".to_string())],
                );
                response.extend_from_slice(body);
                response
            };
            let both = {
                let mut response = head(
                    status,
                    "framing",
                    &[
                        ("Transfer-Encoding", "chunked".to_string()),
                        ("Content-Length", body.len().to_string()),
                    ],
                );
                response.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
                response.extend_from_slice(body);
                response.extend_from_slice(b"\r\n0\r\n\r\n");
                response
            };
            let length = body.len().to_string();
            let framings = [
                ("gzip", unknown_coding, "gzip", None),
                ("both", both, "chunked", Some(length.as_str())),
            ];
            for (name, response, coding, declared) in framings {
                let case = format!("{verb:?} {status} {name}");
                let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
                let client = client(endpoint.addr, deadlines());
                let got = bounded(&case, verb.error_answer(&client, "list/")).await;
                drop(client);
                let _ = endpoint.heads().await;
                let expected = Outcome::BodyFailed(BodyFailure::Framing {
                    transfer_encoding: Some(coding.to_string()),
                    content_length: declared.map(str::to_string),
                });
                match got {
                    Ok(answer)
                        if answer.head.status == code
                            && answer.head.request_id() == Some("framing")
                            && answer.outcome == expected =>
                    {
                        eprintln!("GREEN {case}: {answer}");
                    }
                    other => {
                        failures.push(format!("{case}: expected {expected:?}, got {other:?}"));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) not refused as they must be:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// --- a ranged or conditional GET's 2xx: streamed only under framing that marks its end -------

/// `script`'s answer to `verb`, one of [`GET_VERBS`], with what the caller then hands to
/// [`assert_sent_once`] once it has dropped the body.
async fn fetch_scripted(
    verb: ReadVerb,
    script: Script,
    case: &str,
) -> (Fetched, Endpoint, S3Client) {
    let endpoint = Endpoint::start(vec![script]).await;
    let client = client(endpoint.addr, short_deadlines());
    let fetched = match verb {
        ReadVerb::Range => bounded(case, client.get_object_range("key", 0, 9)).await,
        _ => bounded(case, client.get_object_if_none_match("key", SOME_ETAG)).await,
    }
    .unwrap_or_else(|e| panic!("{case}: no answer: {e}"));
    (fetched, endpoint, client)
}

/// That the endpoint read one request, the verb's own.
async fn assert_sent_once(verb: ReadVerb, mut endpoint: Endpoint, client: S3Client, case: &str) {
    drop(client);
    let heads = endpoint.heads().await;
    assert!(
        matches!(heads.as_slice(), [request] if verb.sent(request)),
        "{case}: not the verb's request: {heads:?}"
    );
}

/// The 2xx each of [`GET_VERBS`] answers its own request with.
fn get_2xx(verb: ReadVerb) -> (&'static str, u16) {
    match verb {
        ReadVerb::Range => ("206 Partial Content", 206),
        _ => ("200 OK", 200),
    }
}

#[tokio::test]
async fn a_get_2xx_whose_framing_cannot_mark_the_objects_end_keeps_its_status_and_is_not_streamed()
{
    for verb in GET_VERBS {
        let (status, code) = get_2xx(verb);
        let sent = b"the bytes as sent";
        // Without the refusal each of these reads as a whole object, to the connection's close.
        let with_body = |headers: &[(&str, String)]| {
            let mut response = head(status, "get-framing", headers);
            response.extend_from_slice(sent);
            response
        };
        let cases: [(&str, Vec<u8>, BodyError); 3] = [
            ("no framing", with_body(&[]), BodyError::LengthUndeclared),
            (
                "an untrusted coding",
                with_body(&[("Transfer-Encoding", "gzip".to_string())]),
                BodyError::Framing {
                    transfer_encoding: Some("gzip".to_string()),
                    content_length: None,
                },
            ),
            (
                "both headers",
                with_body(&[
                    ("Transfer-Encoding", "chunked".to_string()),
                    ("Content-Length", sent.len().to_string()),
                ]),
                BodyError::Framing {
                    transfer_encoding: Some("chunked".to_string()),
                    content_length: Some(sent.len().to_string()),
                },
            ),
        ];
        for (name, response, expected) in cases {
            let case = format!("{verb:?} {status} {name}");
            let (fetched, endpoint, client) =
                fetch_scripted(verb, Script::ended(response), &case).await;
            let Fetched::Object { head, body } = fetched else {
                panic!("{case}: a 2xx is an object: {fetched:?}");
            };
            assert_eq!(head.status, code, "{case}");
            assert_eq!(head.request_id(), Some("get-framing"), "{case}");
            match body {
                Err(error) => assert_eq!(error, expected, "{case}"),
                Ok(body) => panic!(
                    "{case}: streamed, declaring {:?}, where a cut could read as the end",
                    body.content_length()
                ),
            }
            assert_sent_once(verb, endpoint, client, &case).await;
        }
    }
}

#[tokio::test]
async fn a_chunked_get_2xx_streams_to_its_terminal_chunk_and_a_cut_one_fails() {
    for verb in GET_VERBS {
        let (status, code) = get_2xx(verb);
        let sent = b"the bytes as sent";
        let mut response = head(
            status,
            "get-chunked",
            &[("Transfer-Encoding", "chunked".to_string())],
        );
        response.extend_from_slice(format!("{:x}\r\n", sent.len()).as_bytes());
        response.extend_from_slice(sent);
        let cut = response.clone();
        response.extend_from_slice(b"\r\n0\r\n\r\n");

        let case = format!("{verb:?} {status} chunked");
        let (fetched, endpoint, client) =
            fetch_scripted(verb, Script::ended(response), &case).await;
        let Fetched::Object {
            head: got,
            body: Ok(mut body),
        } = fetched
        else {
            panic!("{case}: a chunked 2xx streams: {fetched:?}");
        };
        assert_eq!(got.status, code, "{case}");
        assert_eq!(body.content_length(), None, "{case}: chunked declares none");
        assert_eq!(read_all(&mut body).await, sent, "{case}: the bytes as sent");
        drop(body);
        assert_sent_once(verb, endpoint, client, &case).await;

        // Cut before the terminal chunk: not the end of the object.
        let case = format!("{verb:?} {status} chunked, cut");
        let script = Script {
            response: cut,
            then: Then::Cut,
        };
        let (fetched, endpoint, client) = fetch_scripted(verb, script, &case).await;
        let Fetched::Object {
            head: got,
            body: Ok(mut body),
        } = fetched
        else {
            panic!("{case}: a chunked 2xx streams: {fetched:?}");
        };
        assert_eq!(got.status, code, "{case}");
        assert_body_cut(&mut body, None, &case).await;
        drop(body);
        assert_sent_once(verb, endpoint, client, &case).await;
    }
}

#[tokio::test]
async fn a_get_2xx_cut_short_of_its_content_length_fails_with_its_status_kept() {
    for verb in GET_VERBS {
        let (status, code) = get_2xx(verb);
        let sent = b"the first bytes";
        let declared = sent.len() as u64 + 100;
        let mut response = head(
            status,
            "get-cut",
            &[
                ("Content-Length", declared.to_string()),
                ("Content-Range", format!("bytes 0-{}/5000", declared - 1)),
            ],
        );
        response.extend_from_slice(sent);
        let case = format!("{verb:?} {status} cut");
        let script = Script {
            response,
            then: Then::Cut,
        };
        let (fetched, endpoint, client) = fetch_scripted(verb, script, &case).await;
        let Fetched::Object {
            head: got,
            body: Ok(mut body),
        } = fetched
        else {
            panic!("{case}: a framed 2xx streams: {fetched:?}");
        };
        assert_eq!(got.status, code, "{case}");
        assert_eq!(body.content_length(), Some(declared), "{case}");
        assert_body_cut(&mut body, Some(declared), &case).await;
        drop(body);
        assert_sent_once(verb, endpoint, client, &case).await;
    }
}

/// Read `body` to its failure, which must be a transport error under `declared`: a body that
/// ends cleanly would report a cut object as a whole one.
async fn assert_body_cut(body: &mut ObjectBody, declared: Option<u64>, case: &str) {
    let mut held = 0u64;
    loop {
        match bounded(case, body.next_piece()).await {
            Ok(Some(piece)) => held += piece.len() as u64,
            Ok(None) => panic!("{case}: a cut body ended cleanly after {held} bytes"),
            Err(S3Error::Body(BodyError::Transport {
                declared: reported,
                received,
                detail,
            })) => {
                assert_eq!(reported, declared, "{case}");
                assert_eq!(received, held, "{case}");
                assert!(!detail.is_empty(), "{case}");
                return;
            }
            Err(other) => panic!("{case}: not a transport error: {other:?}"),
        }
    }
}

// --- a GET's body as sent: the SDK's checksum validator never sits in front of it ----------

/// A checksum header no body here matches.
const CHECKSUM_HEADER: &str = "x-amz-checksum-crc32";
const WRONG_CHECKSUM: &str = "NhCmhg==";

/// The two verbs that go through the SDK's `GetObject`.
const GET_VERBS: [ReadVerb; 2] = [ReadVerb::Range, ReadVerb::IfNoneMatch];

/// A response of `status` carrying the checksum header and `body`, with a `Content-Length`,
/// or with no `Content-Length` when `body` is `None`.
fn with_checksum(status: &str, request_id: &str, body: Option<&[u8]>) -> Vec<u8> {
    let mut headers = vec![(CHECKSUM_HEADER, WRONG_CHECKSUM.to_string())];
    if let Some(body) = body {
        headers.push(("Content-Length", body.len().to_string()));
    }
    let mut response = head(status, request_id, &headers);
    response.extend_from_slice(body.unwrap_or_default());
    response
}

#[tokio::test]
async fn a_304_or_an_error_carrying_a_checksum_header_keeps_its_code_and_asks_for_no_checksum() {
    let denied = b"<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
    let gone = b"<Error><Code>NoSuchKey</Code><Message>gone</Message></Error>";
    let no_body = Outcome::Error {
        code: ErrorCode::NoBody,
        message: None,
    };
    let code = |code: &str, message: &str| Outcome::Error {
        code: ErrorCode::Code(code.to_string()),
        message: Some(message.to_string()),
    };
    let cases: [(&str, u16, Option<&[u8]>, Outcome); 4] = [
        ("304 Not Modified", 304, None, no_body.clone()),
        ("404 Not Found", 404, Some(gone), code("NoSuchKey", "gone")),
        (
            "403 Forbidden",
            403,
            Some(denied),
            code("AccessDenied", "Access Denied"),
        ),
        ("403 Forbidden", 403, Some(b""), no_body),
    ];
    let mut failures = Vec::new();
    for verb in GET_VERBS {
        for (status, code, body, expected) in &cases {
            let case = format!("{verb:?} {status} {:?} bytes", body.map(<[u8]>::len));
            let response = with_checksum(status, "checksum", *body);
            let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
            let client = client(endpoint.addr, deadlines());
            let got = bounded(&case, verb.error_answer(&client, "checksum/object")).await;
            drop(client);
            let heads = endpoint.heads().await;
            if !matches!(heads.as_slice(), [request] if verb.sent(request)
                && request_header(request, "x-amz-checksum-mode").is_none())
            {
                failures.push(format!("{case}: not the request asked for: {heads:?}"));
                continue;
            }
            match got {
                Ok(answer)
                    if answer.head.status == *code
                        && answer.head.header(CHECKSUM_HEADER) == Some(WRONG_CHECKSUM)
                        && answer.outcome == *expected =>
                {
                    eprintln!("GREEN {case}: {answer}");
                }
                other => failures.push(format!("{case}: expected {expected:?}, got {other:?}")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) did not read the answer as sent:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn a_get_2xx_carrying_a_checksum_header_that_does_not_match_streams_the_body_as_sent() {
    for verb in GET_VERBS {
        // The checksum is the caller's to read, in the head.
        let (status, id) = match verb {
            ReadVerb::Range => ("206 Partial Content", "checksum-206"),
            _ => ("200 OK", "checksum-200"),
        };
        let sent = b"the bytes as sent";
        let response = with_checksum(status, id, Some(sent));
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let fetched = match verb {
            ReadVerb::Range => bounded(id, client.get_object_range("checksum/object", 0, 16)).await,
            _ => {
                bounded(
                    id,
                    client.get_object_if_none_match("checksum/object", SOME_ETAG),
                )
                .await
            }
        };
        let read = match fetched {
            Ok(Fetched::Object {
                head,
                body: Ok(mut body),
            }) => {
                assert_eq!(head.header(CHECKSUM_HEADER), Some(WRONG_CHECKSUM));
                read_all(&mut body).await
            }
            other => panic!("{id}: not a streamed 2xx: {other:?}"),
        };
        drop(client);
        let heads = endpoint.heads().await;
        assert_eq!(read, sent, "{id}: the body as sent");
        assert!(
            heads
                .iter()
                .all(|head| request_header(head, "x-amz-checksum-mode").is_none()),
            "{id}: the client asked for no checksum: {heads:?}"
        );
    }
}

// --- (H) an S3 error through every new verb keeps its status, headers, code and message ----

/// The four new verbs that read an error's body (HEAD has none).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadVerb {
    Range,
    IfNoneMatch,
    ListV2,
    ListV1,
}

const READ_VERBS: [ReadVerb; 4] = [
    ReadVerb::Range,
    ReadVerb::IfNoneMatch,
    ReadVerb::ListV2,
    ReadVerb::ListV1,
];

/// The tag the (H) conditional GETs send: valid, and no object's.
const SOME_ETAG: &str = "\"0123456789abcdef0123456789abcdef\"";

impl ReadVerb {
    /// Call the verb on `key` (the GETs) or under the prefix `key` (the listings), and return
    /// the answer it reports for a status that is not 2xx, or what it returned instead.
    async fn error_answer(self, client: &S3Client, key: &str) -> Result<Answer, String> {
        let fetched = match self {
            Self::Range => client.get_object_range(key, 0, 9).await,
            Self::IfNoneMatch => client.get_object_if_none_match(key, SOME_ETAG).await,
            Self::ListV2 => return listing_error(client.list_objects_v2(key, 2).await),
            Self::ListV1 => return listing_error(client.list_objects_v1(key, 2).await),
        };
        match fetched {
            Ok(Fetched::Other(answer)) => Ok(answer),
            other => Err(format!("not an answer other than a 2xx: {other:?}")),
        }
    }

    /// Whether `request` is the request this verb sends.
    fn sent(self, request: &str) -> bool {
        let (method, _, query) = request_line(request);
        method == "GET"
            && match self {
                Self::Range => request_header(request, "range") == Some("bytes=0-9"),
                Self::IfNoneMatch => request_header(request, "if-none-match") == Some(SOME_ETAG),
                Self::ListV2 => query_has(query, "list-type"),
                Self::ListV1 => query_has(query, "prefix") && !query_has(query, "list-type"),
            }
    }
}

/// The answer a listing reports for its first page, when that page is an answer with no
/// listing.
fn listing_error(listed: Result<Listing, ListingError>) -> Result<Answer, String> {
    match listed {
        Err(ListingError::Page {
            page: 1,
            cause: NoResult::Answered(answer),
        }) => Ok(answer),
        other => Err(format!("not an answer on page 1: {other:?}")),
    }
}

/// The header lines every scripted (H) answer sends besides its `x-amz-request-id`: one
/// header sent twice, around another.
const FORBIDDEN_HEADERS: [(&str, &str); 3] = [
    ("x-amz-meta-twice", "first"),
    ("Content-Type", "application/xml"),
    ("x-amz-meta-twice", "second"),
];

/// A `403` carrying `body`, with [`FORBIDDEN_HEADERS`] and a `Content-Length` of its length.
fn forbidden(request_id: &str, body: &[u8]) -> Vec<u8> {
    let mut headers: Vec<(&str, String)> = FORBIDDEN_HEADERS
        .iter()
        .map(|&(name, value)| (name, value.to_string()))
        .collect();
    headers.push(("Content-Length", body.len().to_string()));
    let mut response = head("403 Forbidden", request_id, &headers);
    response.extend_from_slice(body);
    response
}

/// Whether `answer`'s head is every line [`forbidden`] sent, the repeated header as both
/// lines in the order sent, each name lowercased. hyper groups a name's lines, so the lines
/// are compared per name.
fn every_forbidden_line(answer: &Answer, request_id: &str, length: usize) -> bool {
    let length = length.to_string();
    let mut sent: Vec<(String, Vec<u8>)> = [("x-amz-request-id", request_id)]
        .into_iter()
        .chain(FORBIDDEN_HEADERS)
        .chain([("Content-Length", length.as_str())])
        .map(|(name, value)| (name.to_ascii_lowercase(), value.as_bytes().to_vec()))
        .collect();
    let mut got: Vec<(String, Vec<u8>)> = answer
        .head
        .fields()
        .map(|(name, value)| (name.to_string(), value.to_vec()))
        .collect();
    // Stable sorts: each name's lines keep their order.
    sent.sort_by(|a, b| a.0.cmp(&b.0));
    got.sort_by(|a, b| a.0.cmp(&b.0));
    got == sent
        && answer
            .head
            .header_lines("x-amz-meta-twice")
            .eq([&b"first"[..], &b"second"[..]])
}

/// One scripted `403` (H) case: its name, its body, and how it must be reported.
fn forbidden_cases() -> [(&'static str, String, Outcome); 4] {
    let cut = "<Error><Code>AccessDenied</Code><Message>Access";
    let other_root = "<Fault><Code>AccessDenied</Code><Message>Access Denied</Message></Fault>";
    [
        (
            "access-denied",
            "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>".to_string(),
            Outcome::Error {
                code: ErrorCode::Code("AccessDenied".to_string()),
                message: Some("Access Denied".to_string()),
            },
        ),
        (
            "no-code",
            "<Error><Message>Access Denied</Message></Error>".to_string(),
            Outcome::Error {
                code: ErrorCode::MissingInXml,
                message: Some("Access Denied".to_string()),
            },
        ),
        (
            "cut-short",
            cut.to_string(),
            Outcome::Unreadable {
                body: Bytes::from_static(cut.as_bytes()),
                detail: String::new(),
            },
        ),
        (
            "another-root",
            other_root.to_string(),
            Outcome::Unreadable {
                body: Bytes::from_static(other_root.as_bytes()),
                detail: String::new(),
            },
        ),
    ]
}

/// Whether `got` is `expected`: an `Unreadable` by its body as sent alone (its detail is the
/// client's words), anything else exactly.
fn reported_as(got: &Outcome, expected: &Outcome) -> bool {
    match (got, expected) {
        (
            Outcome::Unreadable { body, detail },
            Outcome::Unreadable {
                body: expected_body,
                ..
            },
        ) => body == expected_body && !detail.is_empty(),
        _ => got == expected,
    }
}

#[tokio::test]
async fn an_s3_error_through_every_new_read_verb_keeps_its_status_headers_code_and_message() {
    let mut failures = Vec::new();

    // The real gateway: a bucket with no record (`crates/gateway-s3/src/lib.rs:935-944`), and a
    // key that does not exist (`:2834-2841`).
    let no_bucket = start_gateway_seeding(&[]).await;
    let with_bucket = start_gateway().await;
    for verb in READ_VERBS {
        let (gateway, code, message) = match verb {
            ReadVerb::ListV2 | ReadVerb::ListV1 => (
                &no_bucket,
                "NoSuchBucket",
                "the specified bucket does not exist",
            ),
            ReadVerb::Range | ReadVerb::IfNoneMatch => (
                &with_bucket,
                "NoSuchKey",
                "the specified key does not exist",
            ),
        };
        let client = client(gateway.addr, deadlines());
        let case = format!("{verb:?} against the gateway");
        let expected = Outcome::Error {
            code: ErrorCode::Code(code.to_string()),
            message: Some(message.to_string()),
        };
        match bounded(&case, verb.error_answer(&client, "missing/")).await {
            Ok(answer) if answer.head.status == 404 && answer.outcome == expected => {
                assert_gateway_answered(&answer, &case);
                eprintln!("GREEN {case}: {answer}");
            }
            other => failures.push(format!("{case}: expected 404 {expected:?}, got {other:?}")),
        }
    }

    // A scripted `403`, each body through each verb.
    for verb in READ_VERBS {
        for (name, body, expected) in forbidden_cases() {
            let case = format!("{verb:?} 403 {name}");
            let request_id = format!("{verb:?}-{name}");
            let response = forbidden(&request_id, body.as_bytes());
            let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
            let client = client(endpoint.addr, deadlines());
            let got = bounded(&case, verb.error_answer(&client, "list/")).await;
            drop(client);
            let heads = endpoint.heads().await;
            if !matches!(heads.as_slice(), [request] if verb.sent(request)) {
                failures.push(format!("{case}: not the verb's one request: {heads:?}"));
                continue;
            }
            match got {
                Ok(answer)
                    if answer.head.status == 403
                        && answer.head.request_id() == Some(request_id.as_str())
                        && every_forbidden_line(&answer, &request_id, body.len())
                        && reported_as(&answer.outcome, &expected) =>
                {
                    eprintln!("GREEN {case}: {answer}");
                }
                other => failures.push(format!("{case}: expected 403 {expected:?}, got {other:?}")),
            }
        }
    }

    // HEAD: a `403` with no body, with the same header lines.
    let response = forbidden("head-403", b"");
    let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
    let client = client(endpoint.addr, deadlines());
    let got = bounded("HEAD 403", client.head_object("missing/object")).await;
    drop(client);
    let heads = endpoint.heads().await;
    let expected = Outcome::Error {
        code: ErrorCode::NoBody,
        message: None,
    };
    match got {
        Ok(answer)
            if matches!(heads.as_slice(), [request] if request_line(request).0 == "HEAD")
                && answer.head.status == 403
                && answer.head.request_id() == Some("head-403")
                && every_forbidden_line(&answer, "head-403", 0)
                && answer.outcome == expected =>
        {
            eprintln!("GREEN HEAD 403: {answer}");
        }
        other => failures.push(format!(
            "HEAD 403: expected {expected:?}, got {other:?}, requests {heads:?}"
        )),
    }

    assert!(
        failures.is_empty(),
        "{} case(s) lost the error's status, headers, code or message:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// --- no answer: stopped before the connector, or reached the endpoint and got none ---------

#[tokio::test]
async fn a_read_verb_stopped_before_the_connector_is_request_not_built_and_reaches_no_endpoint() {
    // A listener the client must not reach: the client's endpoint resolution refuses an endpoint
    // with a query, before the request is signed or the connector called.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let unsent = client_at(&format!("http://{addr}/?x=1"), deadlines());
    let head = bounded("HEAD", unsent.head_object("unsent/object")).await;
    let listed = bounded("ListObjectsV2", unsent.list_objects_v2("list/", 2)).await;
    drop(unsent);
    assert!(
        matches!(head, Err(S3Error::RequestNotBuilt { .. })),
        "HEAD: {head:?}"
    );
    assert!(
        matches!(
            listed,
            Err(ListingError::Page {
                page: 1,
                cause: NoResult::NoAnswer(S3Error::RequestNotBuilt { .. })
            })
        ),
        "ListObjectsV2: {listed:?}"
    );
    // A connection that was made stays in the backlog, closed or not, until it is accepted.
    assert!(
        timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err(),
        "nothing reached the endpoint"
    );

    // An endpoint that reads the request and closes without answering: sent, and no answer.
    let unanswered = || Script {
        response: Vec::new(),
        then: Then::Cut,
    };
    let mut endpoint = Endpoint::start(vec![unanswered(), unanswered()]).await;
    let client = client(endpoint.addr, deadlines());
    let head = bounded("HEAD", client.head_object("unanswered/object")).await;
    let listed = bounded("ListObjectsV2", client.list_objects_v2("list/", 2)).await;
    drop(client);
    let heads = endpoint.heads().await;
    assert_eq!(
        heads.len(),
        2,
        "both requests reached the endpoint: {heads:?}"
    );
    assert!(
        matches!(head, Err(S3Error::NoResponse { .. })),
        "HEAD: {head:?}"
    );
    assert!(
        matches!(
            listed,
            Err(ListingError::Page {
                page: 1,
                cause: NoResult::NoAnswer(S3Error::NoResponse { .. })
            })
        ),
        "ListObjectsV2: {listed:?}"
    );
}

// --- #956 (G): a whole success, and a 304's framing, on a probe -------------------------------

/// The `ETag` S3 gives an empty part.
const EMPTY_PART_ETAG: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";

/// A `200` (`204` where S3 answers so) carrying `probe`'s whole S3 success, and the body it
/// carries.
fn success_script(probe: &Probe, request_id: &str) -> (Script, u16, Bytes) {
    let document =
        |root: &str, inner: &str| format!("{XML_DECL}<{root} xmlns=\"{S3_NS}\">{inner}</{root}>");
    let etag = "<ETag>&quot;0123456789abcdef0123456789abcdef&quot;</ETag>";
    let body = match probe {
        Probe::CopyObject { .. } => document(
            "CopyObjectResult",
            &format!("<LastModified>2026-10-23T00:00:00.000Z</LastModified>{etag}"),
        ),
        Probe::CreateMultipartUpload { .. } => document(
            "InitiateMultipartUploadResult",
            "<Bucket>validate</Bucket><Key>probe/object</Key><UploadId>upload-1</UploadId>",
        ),
        Probe::CompleteMultipartUpload { .. } => document(
            "CompleteMultipartUploadResult",
            &format!(
                "<Location>http://127.0.0.1/validate/probe/object</Location>\
                 <Bucket>validate</Bucket><Key>probe/object</Key>{etag}"
            ),
        ),
        Probe::ListParts { .. } => document(
            "ListPartsResult",
            &format!(
                "<Bucket>validate</Bucket><Key>probe/object</Key>\
                 <UploadId>{UPLOAD_ID}</UploadId><IsTruncated>false</IsTruncated>"
            ),
        ),
        Probe::ListMultipartUploads => document(
            "ListMultipartUploadsResult",
            "<Bucket>validate</Bucket><IsTruncated>false</IsTruncated>",
        ),
        Probe::GetBucketLocation => document("LocationConstraint", "us-west-2"),
        Probe::ListBuckets => document(
            "ListAllMyBucketsResult",
            "<Owner><ID>wyrd</ID></Owner><Buckets><Bucket><Name>validate</Name>\
             <CreationDate>2026-10-23T00:00:00.000Z</CreationDate></Bucket></Buckets>",
        ),
        // Headers only: S3's success for these has no body.
        Probe::UploadPart { .. } => {
            let response = head(
                "200 OK",
                request_id,
                &[
                    ("ETag", EMPTY_PART_ETAG.to_string()),
                    ("Content-Length", "0".to_string()),
                ],
            );
            return (Script::ended(response), 200, Bytes::new());
        }
        Probe::CreateBucket { bucket } => {
            let response = head(
                "200 OK",
                request_id,
                &[
                    ("Location", format!("/{bucket}")),
                    ("Content-Length", "0".to_string()),
                ],
            );
            return (Script::ended(response), 200, Bytes::new());
        }
        Probe::HeadBucket => {
            let response = head(
                "200 OK",
                request_id,
                &[
                    ("x-amz-bucket-region", REGION.to_string()),
                    ("Content-Length", "0".to_string()),
                ],
            );
            return (Script::ended(response), 200, Bytes::new());
        }
        Probe::AbortMultipartUpload { .. } | Probe::DeleteBucket { .. } => {
            let response = head("204 No Content", request_id, &[]);
            return (Script::ended(response), 204, Bytes::new());
        }
    };
    let response = ok_with_length(request_id, body.len(), body.as_bytes());
    (Script::ended(response), 200, Bytes::from(body))
}

#[tokio::test]
async fn a_2xx_carrying_the_operations_whole_s3_success_is_a_success() {
    let mut failures = Vec::new();
    for case in probe_cases() {
        let name = case.probe.operation();
        let request_id = format!("{name}-success");
        let (script, status, body) = success_script(&case.probe, &request_id);
        let mut endpoint = Endpoint::start(vec![script]).await;
        let client = client(endpoint.addr, deadlines());
        let answer = bounded(name, client.probe(&case.probe)).await;
        drop(client);
        // The request first, so a success answered to the wrong request cannot pass.
        let heads = endpoint.heads().await;
        let [head] = heads.as_slice() else {
            failures.push(format!("{name}: not one request: {heads:?}"));
            continue;
        };
        if let Some(fault) = case.request_fault(head) {
            failures.push(format!("{name}: not the operation's request: {fault}"));
            continue;
        }
        match answer {
            Ok(answer)
                if answer.head.status == status
                    && answer.head.request_id() == Some(request_id.as_str())
                    && answer.outcome == (Outcome::Success { body: body.clone() }) =>
            {
                eprintln!("GREEN {name}: {answer}");
            }
            other => failures.push(format!("{name}: not its success: {other:?}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} success(es) not reported as one:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn a_2xx_whose_whole_document_is_under_another_root_is_no_success() {
    let document = format!("{XML_DECL}<SomeOtherResult xmlns=\"{S3_NS}\"></SomeOtherResult>");
    let mut failures = Vec::new();
    for probe in bodied_probes() {
        let name = probe.operation();
        let request_id = format!("{name}-another-root");
        let response = ok_with_length(&request_id, document.len(), document.as_bytes());
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let answer = bounded(name, client.probe(&probe)).await;
        drop(client);
        let _ = endpoint.heads().await;
        match answer {
            Ok(answer)
                if answer.head.status == 200
                    && matches!(&answer.outcome, Outcome::Unreadable { body, .. }
                        if body[..] == *document.as_bytes()) => {}
            other => failures.push(format!("{name}: {other:?}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) read as a success:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// --- #956 (D) for a probe: the budget caps its body as it caps a listing page's ----------------

/// How a sizing leg frames a probe's success document against the budget.
#[derive(Clone, Copy, Debug)]
enum Sizing {
    /// One byte past the budget, with a `Content-Length`.
    OverWithLength,
    /// One byte past the budget, chunked.
    OverChunked,
    /// Exactly the budget, with a `Content-Length`.
    Exact,
}

const SIZINGS: [Sizing; 3] = [Sizing::OverWithLength, Sizing::OverChunked, Sizing::Exact];

#[tokio::test]
async fn a_probe_body_past_the_budget_is_refused_beside_its_200_and_one_of_exactly_the_budget_is_read(
) {
    let budget = usize::try_from(BUFFERED_BODY_BUDGET).expect("the budget fits usize");
    let mut failures = Vec::new();
    let mut sized = Vec::new();
    for case in probe_cases() {
        let name = case.probe.operation();
        let (_, _, document) = success_script(&case.probe, "sizing");
        if document.is_empty() {
            // A headers-only success: a body of any size is not its success, and
            // `a_probe_answered_200_keeps_its_200…` has its body faults.
            continue;
        }
        sized.push(name);
        let document = std::str::from_utf8(&document).expect("the success document is text");
        let over = padded(document, budget + 1);
        let exact = padded(document, budget);
        for sizing in SIZINGS {
            let leg = format!("{name} {sizing:?}");
            let request_id = format!("{name}-{sizing:?}");
            let response = match sizing {
                Sizing::OverWithLength => ok_with_length(&request_id, over.len(), over.as_bytes()),
                Sizing::OverChunked => ok_chunked(&request_id, over.as_bytes()),
                Sizing::Exact => ok_with_length(&request_id, exact.len(), exact.as_bytes()),
            };
            let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
            let client = client(endpoint.addr, deadlines());
            let answer = bounded(&leg, client.probe(&case.probe)).await;
            drop(client);
            // The request first, as the success legs check it: a body answered to the wrong
            // request shows nothing about this probe.
            let heads = endpoint.heads().await;
            let [head] = heads.as_slice() else {
                failures.push(format!("{leg}: not one request: {heads:?}"));
                continue;
            };
            if let Some(fault) = case.request_fault(head) {
                failures.push(format!("{leg}: not the operation's request: {fault}"));
                continue;
            }
            let answer = match answer {
                Ok(answer)
                    if answer.head.status == 200
                        && answer.head.request_id() == Some(request_id.as_str()) =>
                {
                    answer
                }
                other => {
                    failures.push(format!("{leg}: the 200 head was lost: {other:?}"));
                    continue;
                }
            };
            let fits = match (&answer.outcome, sizing) {
                (
                    Outcome::BodyFailed(BodyFailure::OverBudget { received }),
                    Sizing::OverWithLength | Sizing::OverChunked,
                ) => *received > BUFFERED_BODY_BUDGET,
                (Outcome::Success { body }, Sizing::Exact) => body[..] == *exact.as_bytes(),
                _ => false,
            };
            if fits {
                eprintln!("GREEN {leg}: {answer}");
            } else {
                failures.push(format!(
                    "{leg}: the wrong outcome beside the 200: {answer:?}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} leg(s) not read against the budget as they must be:\n{}",
        failures.len(),
        failures.join("\n")
    );
    // The seven probes whose S3 success is a document, so the legs above cannot thin out
    // unnoticed should a success script change.
    assert_eq!(
        sized,
        [
            "CopyObject",
            "CreateMultipartUpload",
            "CompleteMultipartUpload",
            "ListParts",
            "ListMultipartUploads",
            "GetBucketLocation",
            "ListBuckets",
        ],
        "every document probe was sized against the budget"
    );
}

#[tokio::test]
async fn a_304_that_frames_a_chunked_body_is_refused_only_for_a_probe_that_is_not_a_get_or_head() {
    let cases = probe_cases();
    let mut failures = Vec::new();
    for case in &cases {
        let name = case.probe.operation();
        let request_id = format!("{name}-304");
        let response = head(
            "304 Not Modified",
            &request_id,
            &[("Transfer-Encoding", "chunked".to_string())],
        );
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let answer = bounded(name, client.probe(&case.probe)).await;
        drop(client);
        let heads = endpoint.heads().await;
        if let Some(fault) = heads.first().and_then(|head| case.request_fault(head)) {
            failures.push(format!("{name}: not the operation's request: {fault}"));
            continue;
        }
        // RFC 9112 §6.1: a `Transfer-Encoding` on a `304` only in answer to a GET (or a HEAD,
        // which is answered as the GET would be).
        let reads = matches!(case.method, "GET" | "HEAD");
        let expected = if reads {
            Outcome::Error {
                code: ErrorCode::NoBody,
                message: None,
            }
        } else {
            Outcome::BodyFailed(BodyFailure::Framing {
                transfer_encoding: Some("chunked".to_string()),
                content_length: None,
            })
        };
        match answer {
            Ok(answer) if answer.head.status == 304 && answer.outcome == expected => {
                eprintln!("GREEN {name} ({}): {answer}", case.method);
            }
            other => failures.push(format!(
                "{name} ({}): expected {expected:?}, got {other:?}",
                case.method
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} case(s) misread:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn a_304_that_frames_a_chunked_body_is_refused_for_a_bulk_delete_beside_its_status() {
    let response = head(
        "304 Not Modified",
        "DeleteObjects-304",
        &[("Transfer-Encoding", "chunked".to_string())],
    );
    let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
    let client = client(endpoint.addr, deadlines());
    let result = bounded("DeleteObjects", client.delete_objects(&["a"])).await;
    drop(client);
    let heads = endpoint.heads().await;
    assert_eq!(request_line(&heads[0]).0, "POST", "{heads:?}");
    // RFC 9112 §6.1: a `Transfer-Encoding` on a `304` only in answer to a GET or a HEAD.
    match result {
        Err(NoResult::Answered(Answer {
            head,
            outcome:
                Outcome::BodyFailed(BodyFailure::Framing {
                    transfer_encoding: Some(coding),
                    content_length: None,
                }),
        })) if head.status == 304 && coding == "chunked" => {}
        other => panic!("not refused beside the 304: {other:?}"),
    }
}

// --- #956 (C): a 2xx whose body is an S3 error document --------------------------------------

fn internal_error() -> Outcome {
    Outcome::Error {
        code: ErrorCode::Code("InternalError".to_string()),
        message: Some("We encountered an internal error. Please try again.".to_string()),
    }
}

#[tokio::test]
async fn a_200_whose_body_is_an_s3_error_document_keeps_the_200_and_reports_its_code_and_message() {
    let document = internal_error_document();
    let mut failures = Vec::new();
    for probe in bodied_probes() {
        let name = probe.operation();
        let request_id = format!("{name}-error-in-200");
        let response = ok_with_length(&request_id, document.len(), document.as_bytes());
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let answer = bounded(name, client.probe(&probe)).await;
        drop(client);
        let _ = endpoint.heads().await;
        match answer {
            Ok(answer)
                if answer.head.status == 200
                    && answer.head.request_id() == Some(request_id.as_str())
                    && answer.outcome == internal_error() =>
            {
                eprintln!("GREEN {name}: {answer}");
            }
            other => failures.push(format!("{name}: {other:?}")),
        }
    }

    // A listing page and a delete result: no result, the 200 and the error kept.
    for verb in [DocumentVerb::ListV2, DocumentVerb::Delete] {
        let request_id = format!("{verb:?}-error-in-200");
        let response = ok_with_length(&request_id, document.len(), document.as_bytes());
        let mut endpoint = Endpoint::start(vec![Script::ended(response)]).await;
        let client = client(endpoint.addr, deadlines());
        let read = bounded("a document verb", call_document_verb(verb, &client)).await;
        drop(client);
        let _ = endpoint.heads().await;
        match refused_answer(&read, 1) {
            Some(answer)
                if answer.head.status == 200
                    && answer.head.request_id() == Some(request_id.as_str())
                    && answer.outcome == internal_error() =>
            {
                eprintln!("GREEN {verb:?}: {answer}");
            }
            _ => failures.push(format!("{verb:?}: {read:?}")),
        }
    }

    // An empty body names no error: a 200 with none, where a document is due, is unreadable.
    let copy = Probe::CopyObject {
        key: "probe/object".to_string(),
        source_key: "probe/source".to_string(),
    };
    let mut endpoint =
        Endpoint::start(vec![Script::ended(ok_with_length("empty-200", 0, b""))]).await;
    let client = client(endpoint.addr, deadlines());
    let answer = bounded("CopyObject empty", client.probe(&copy)).await;
    drop(client);
    let _ = endpoint.heads().await;
    match answer {
        Ok(answer)
            if answer.head.status == 200
                && matches!(&answer.outcome, Outcome::Unreadable { body, .. } if body.is_empty()) =>
            {}
        other => failures.push(format!("CopyObject, an empty 200: {other:?}")),
    }

    assert!(
        failures.is_empty(),
        "{} case(s) lost the 200 or the error:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// --- (D) a listing <Size> must be plain decimal digits -----------------------------------------

#[tokio::test]
async fn a_listing_size_that_is_not_plain_decimal_digits_is_refused() {
    let whole = listing_v2(&["list/a"], None);
    assert!(whole.contains("<Size>7</Size>"));
    let page_with = |size: &str| whole.replace("<Size>7</Size>", &format!("<Size>{size}</Size>"));
    let mut failures = Vec::new();
    // A sign (`u64::from_str` takes a `+`), spaces, nothing, a fraction, hex, past `u64`.
    for size in [
        "+7",
        "-7",
        " 7",
        "7 ",
        "",
        "7.0",
        "0x7",
        "18446744073709551616",
    ] {
        let page = page_with(size);
        let mut endpoint = Endpoint::start(vec![Script::ended(ok_with_length(
            "size",
            page.len(),
            page.as_bytes(),
        ))])
        .await;
        let client = client(endpoint.addr, deadlines());
        let listed = bounded("ListObjectsV2", client.list_objects_v2("list/", 2)).await;
        drop(client);
        let _ = endpoint.heads().await;
        match listed {
            Err(ListingError::Page {
                page: 1,
                cause: NoResult::Answered(answer),
            }) if matches!(&answer.outcome, Outcome::Unreadable { detail, .. }
                    if detail.contains("<Size>")) => {}
            other => failures.push(format!("<Size>{size}</Size>: not refused: {other:?}")),
        }
    }

    // The largest size there is, read.
    let page = page_with("18446744073709551615");
    let mut endpoint = Endpoint::start(vec![Script::ended(ok_with_length(
        "size-max",
        page.len(),
        page.as_bytes(),
    ))])
    .await;
    let client = client(endpoint.addr, deadlines());
    let listed = bounded("ListObjectsV2", client.list_objects_v2("list/", 2)).await;
    drop(client);
    let _ = endpoint.heads().await;
    match listed {
        Ok(listing) if keys_and_sizes(&listing) == [("list/a".to_string(), u64::MAX)] => {}
        other => failures.push(format!("u64::MAX: not read: {other:?}")),
    }

    assert!(
        failures.is_empty(),
        "{} case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
