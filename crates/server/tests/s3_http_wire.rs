//! The S3 HTTP wire surface, exercised over a real loopback listener (issue #364,
//! m4-first-deployment-blueprint:698-699). An S3 client drives **PUT → GET → DELETE**
//! over the gateway's HTTP listener and asserts:
//!
//! * a **SigV4-signed** round-trip stores and returns the object **byte-identical**
//!   (a multi-chunk object, so the streaming wire path exercises the same chunking the
//!   in-process path does), and DELETE removes it (a later GET is `NoSuchKey`);
//! * an **unsigned** request is refused (403) — no anonymous access — and, crucially, an
//!   unsigned PUT is refused **before its body is read** (a huge declared body that is
//!   never sent still gets a prompt 403), proving auth precedes body materialisation;
//! * a **wrong-signature** request is refused (403) **and stores nothing**;
//! * concurrent **DELETE is idempotent** (both racers succeed; the object ends gone); and
//! * a **stock-SDK `aws-chunked` streaming PUT** (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, the
//!   default modern boto3 / aws-sdk wire form) round-trips **byte-identical** and a body
//!   whose chunk signature was tampered is **refused (403)** — the streaming path a real SDK
//!   actually uses, whose chunk-signing algorithm is pinned to AWS's *published* worked
//!   example in `s3::streaming` unit tests, so the interop claim is not self-referential.
//!
//! The request/response bytes are driven by hand over a `TcpStream` so the test controls
//! the exact signed headers (SigV4 is sensitive to header set/order); the signature is
//! produced by the production `sigv4::sign`, whose AWS-correctness — including the
//! **sorted/URI-encoded canonical query** a real SDK sends — is pinned independently by
//! the AWS published-example known-answer unit test (`sigv4::tests`), so a green here is
//! not self-referential. Timestamps are stamped **fresh** (`format_amz_date(now)`) so the
//! signature is inside the freshness/replay window the gateway enforces. RED before the
//! wire surface exists (no `s3` module to bind / dial); GREEN once it does.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_core::ObjectGateway;
use wyrd_gateway_s3::sigv4::{format_amz_date, sign, Credentials};
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// A gateway with a deliberately small chunk size, so a modest object still spans
/// several chunks over the streaming wire path.
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
    let (addr, dir, _gateway) = start_gateway_with_handle().await;
    (addr, dir)
}

/// As [`start_gateway`], but also returns the in-process gateway handle the listener
/// serves, so a test can assert on the *stored* state (e.g. the decoded object key).
async fn start_gateway_with_handle() -> (SocketAddr, tempfile::TempDir, Arc<Backend>) {
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
    (addr, dir, gateway)
}

fn signed_headers(method: &str, path: &str, host: &str, body: &[u8]) -> Vec<(String, String)> {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    // Stamp a fresh timestamp so the request is inside the gateway's freshness window.
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
    // A streaming GET now declares an accurate `Content-Length` (issue #364: truncation
    // detection), so with `connection: close` the body is the bytes up to EOF; a chunked
    // response (should the framing ever fall back) is de-framed instead.
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

#[tokio::test]
async fn signed_put_get_delete_round_trip_is_byte_identical() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/round-trip-object";
    // Longer than the 8-byte chunk size, so the object spans several chunks on the wire.
    let object = b"an S3 object that spans several chunks over the network".to_vec();

    let (status, _) = send(
        addr,
        "PUT",
        path,
        &signed_headers("PUT", path, &host, &object),
        &object,
    )
    .await;
    assert_eq!(status, 200, "signed PUT must be accepted");

    let (status, body) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 200, "signed GET must be accepted");
    assert_eq!(body, object, "GET must return the PUT bytes byte-identical");

    let (status, _) = send(
        addr,
        "DELETE",
        path,
        &signed_headers("DELETE", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 204, "signed DELETE must be accepted");

    let (status, _) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 404, "a deleted object must be gone");
}

#[tokio::test]
async fn unsigned_request_is_refused() {
    let (addr, _dir) = start_gateway().await;
    let path = "/wyrd-bucket/anon-object";
    let object = b"anonymous writers are refused".to_vec();

    // No Authorization header at all: anonymous access must be rejected, not stored.
    let (status, _) = send(addr, "PUT", path, &[], &object).await;
    assert_eq!(
        status, 403,
        "an unsigned PUT must be refused (no anonymous access)"
    );

    let host = addr.to_string();
    let (status, _) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 404, "the refused PUT must not have stored anything");
}

/// Auth is enforced **before** the body is materialised (carry-forward item 6): an
/// unsigned PUT that declares a huge `Content-Length` but never sends the body must still
/// be refused promptly — the gateway must not block reading (or allocating for) a body it
/// is going to reject. If auth ran after the body, this would hang until the read timed
/// out; the explicit timeout turns that regression into a failure, not a stall.
#[tokio::test]
async fn unsigned_put_is_refused_before_its_body_is_read() {
    let (addr, _dir) = start_gateway().await;
    let path = "/wyrd-bucket/never-sent";

    let host = addr.to_string();
    // Declare a 1 GiB body but send zero bytes of it, with no Authorization header.
    let head = format!(
        "PUT {path} HTTP/1.1\r\nhost: {host}\r\ncontent-length: 1073741824\r\nconnection: close\r\n\r\n"
    );

    let status = tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(head.as_bytes()).await.expect("write head");
        stream.flush().await.expect("flush");
        // Deliberately send NO body bytes.
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.expect("read response");
        parse_response(&raw).0
    })
    .await
    .expect("gateway must answer without waiting for the un-sent body");

    assert_eq!(
        status, 403,
        "an unsigned PUT must be refused before its (never-sent) body is read"
    );
}

#[tokio::test]
async fn wrong_signature_is_refused_and_stores_nothing() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/tampered-object";
    let object = b"a request whose signature has been tampered with".to_vec();

    // Sign correctly, then flip the last hex digit of the signature: fail-closed auth
    // must reject it (a valid-looking header whose signature no longer verifies).
    let mut headers = signed_headers("PUT", path, &host, &object);
    let authorization = &mut headers[0].1;
    let last = authorization.pop().expect("signature is non-empty");
    authorization.push(if last == '0' { '1' } else { '0' });

    let (status, _) = send(addr, "PUT", path, &headers, &object).await;
    assert_eq!(status, 403, "a wrong-signature PUT must be refused");

    let (status, _) = send(
        addr,
        "GET",
        path,
        &signed_headers("GET", path, &host, b""),
        b"",
    )
    .await;
    assert_eq!(status, 404, "the refused PUT must not have stored anything");
}

/// Concurrent DELETE of the same key is **idempotent** (carry-forward item 3): the CAS in
/// `metadata::unlink` lets only one racer win the removal, but the loser must observe the
/// key as already gone and report success too — S3 returns 204 for a DELETE of a missing
/// key (never 409). This drives the production `Gateway::delete_object` in-process
/// (load-light, no listener needed), which is exactly the path the wire DELETE calls.
///
/// The two deletes run as **separately-spawned tasks on a multi-thread runtime**, so they
/// genuinely race the read→commit window `unlink` opens (an in-process `join!` would poll
/// them one-at-a-time and never reach the CAS-conflict branch). Many rounds make the race
/// reliably occur; the invariant "**both racers succeed and the object ends gone**" holds
/// for *every* interleaving of the fixed code, so the test is deterministically green —
/// only a non-idempotent DELETE (409 on the losing CAS) turns it red.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_delete_is_idempotent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = build_gateway(dir.path());

    for round in 0..64 {
        let key = format!("wyrd-bucket/raced-delete-{round}");
        gateway
            .put_object(&key, b"an object two clients race to delete")
            .await
            .expect("put");

        let g1 = Arc::clone(&gateway);
        let g2 = Arc::clone(&gateway);
        let (k1, k2) = (key.clone(), key.clone());
        let t1 = tokio::spawn(async move { g1.delete_object(&k1).await });
        let t2 = tokio::spawn(async move { g2.delete_object(&k2).await });

        let a = t1
            .await
            .expect("task 1")
            .expect("first delete must succeed");
        let b = t2
            .await
            .expect("task 2")
            .expect("second delete must succeed (idempotent, never 409)");
        assert_eq!(
            [a, b].iter().filter(|removed| **removed).count(),
            1,
            "round {round}: exactly one racer removes the object; the other is a no-op"
        );
        assert!(
            gateway.get_object(&key).await.expect("get").is_none(),
            "round {round}: the object must be gone after the concurrent deletes"
        );
    }
}

/// A **GET resolved before a concurrent DELETE must not be truncated** (carry-forward,
/// GET-during-DELETE). A streaming GET resolves the object's chunk map up front and then reads
/// its fragments lazily, one chunk at a time. DELETE must therefore leave the fragments under
/// the reader-safe orphan grace window (reclaimed later by the custodian GC), **not** delete
/// them eagerly — otherwise the in-flight reader's `read_chunk_verified` would raise
/// `MissingFragment` mid-stream and the client would receive a **truncated** body it cannot
/// distinguish from success (single-chunk objects would truncate to zero bytes).
///
/// This drives the production streaming GET (`Gateway::get_object_streaming`) and
/// `Gateway::delete_object`: the stream is resolved, the DELETE lands, and the stream is then
/// drained — the full object must come back byte-identical. RED with an eager fragment reclaim
/// on the delete path (the fragments vanish → the drain errors/truncates); GREEN once DELETE
/// honours the grace window and leaves reclamation to GC.
#[tokio::test]
async fn get_streaming_resolved_before_delete_is_not_truncated() {
    use futures_util::StreamExt;

    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = build_gateway(dir.path()); // 8-byte chunks → a genuinely multi-chunk object
    let key = "wyrd-bucket/read-during-delete";
    let object = b"a multi-chunk object being read while it is concurrently deleted".to_vec();
    gateway.put_object(key, &object).await.expect("put");

    // Resolve the read stream up front (chunk map captured), exactly as a wire GET does before
    // it starts draining bytes to the socket.
    let mut stream = Arc::clone(&gateway)
        .get_object_streaming(key)
        .await
        .expect("get")
        .expect("the object is present")
        .stream;

    // The DELETE lands while that read is still in flight (not yet drained).
    assert!(gateway.delete_object(key).await.expect("delete"));

    // Draining the previously-resolved stream must still yield the whole object byte-identical:
    // the fragments survive under the grace window, so the reader is never torn.
    let mut got = Vec::new();
    while let Some(item) = stream.next().await {
        got.extend_from_slice(&item.expect("no chunk read may fail mid-stream after a delete"));
    }
    assert_eq!(
        got, object,
        "a GET resolved before a concurrent DELETE must read the full object, not a truncated body"
    );

    // …and the object is logically gone for a fresh reader (the DELETE still took effect).
    assert!(
        gateway.get_object(key).await.expect("get").is_none(),
        "the deleted object is gone for a GET started after the delete"
    );
}

/// Real-SDK break 1 (carry-forward): a client key with a space — `spaces in key.txt` —
/// arrives on the wire as `/bucket/spaces%20in%20key.txt` (the form the SDK signs). The
/// gateway must store it under the **decoded** identity `bucket/spaces in key.txt`, not the
/// literal `bucket/spaces%20in%20key.txt`. Proven by driving the wire PUT and then reading
/// the *production* in-process handle by the decoded key (and confirming the encoded key is
/// absent) — a plain wire round-trip alone would be self-consistent and hide the bug.
#[tokio::test]
async fn percent_encoded_key_is_stored_under_the_decoded_identity() {
    let (addr, _dir, gateway) = start_gateway_with_handle().await;
    let host = addr.to_string();
    // The request target is percent-encoded, exactly as a real SDK (boto3/aws-sdk) sends
    // and signs it; SigV4 verifies against this encoded target.
    let path = "/wyrd-bucket/spaces%20in%20key.txt";
    let object = b"an object whose key needs percent-encoding".to_vec();

    let (status, _) = send(
        addr,
        "PUT",
        path,
        &signed_headers("PUT", path, &host, &object),
        &object,
    )
    .await;
    assert_eq!(
        status, 200,
        "signed PUT of an encoded-key object must be accepted"
    );

    // The production store must hold it under the DECODED key…
    assert_eq!(
        gateway
            .get_object("wyrd-bucket/spaces in key.txt")
            .await
            .expect("get decoded"),
        Some(object),
        "the object must be stored under the decoded key, not its wire encoding"
    );
    // …and NOT under the raw, still-encoded key.
    assert_eq!(
        gateway
            .get_object("wyrd-bucket/spaces%20in%20key.txt")
            .await
            .expect("get encoded"),
        None,
        "the object must not be stored under the literal percent-encoded key"
    );
}

/// A signed GET must declare an **accurate `Content-Length`** (issue #364 durability
/// carry-forward: streaming-GET fault framing). Once the `200 OK` is on the wire the body
/// stream has no in-band error channel, so a chunk read that faults mid-stream (e.g. a
/// fragment reclaimed by a racing DELETE) can only end the body early. An accurate declared
/// length makes that a *detectable* short read for the client rather than a silent "complete"
/// object (a single-chunk object would otherwise truncate to zero bytes indistinguishably).
/// We read the raw response head and assert the header equals the object's true length.
#[tokio::test]
async fn get_declares_accurate_content_length_for_truncation_detection() {
    let (addr, _dir) = start_gateway().await;
    let host = addr.to_string();
    let path = "/wyrd-bucket/length-declared";
    let object = b"an object whose GET response declares its exact byte length".to_vec();

    let (status, _) = send(
        addr,
        "PUT",
        path,
        &signed_headers("PUT", path, &host, &object),
        &object,
    )
    .await;
    assert_eq!(status, 200, "signed PUT must be accepted");

    // Read the raw GET response so we can inspect its framing headers directly.
    let mut request = format!("GET {path} HTTP/1.1\r\nhost: {host}\r\n");
    for (name, value) in &signed_headers("GET", path, &host, b"") {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("connection: close\r\n\r\n");
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");

    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header terminator");
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let declared = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .map(|v| v.trim().parse::<usize>().expect("numeric content-length"));
    assert_eq!(
        declared,
        Some(object.len()),
        "GET must declare the object's exact length so a truncated body is detectable"
    );
}

/// Durability finding 1 (carry-forward), now cluster-safe (issue #477): a new-key PUT after a
/// restart must NOT reuse a committed inode id (a spurious conflict) or a committed chunk id
/// (clobbering the prior object's fragments). The gateway resumes inodes from the SHARED
/// persisted `meta:next_inode` allocator (seeded by `recover`) and mints coordination-free
/// chunk ids (a fresh random epoch, ADR-0019), so neither collides. A restart is modelled by
/// dropping the gateway and its store handles and reopening the SAME persisted redb +
/// filesystem state — the process-restart equivalent (mirrors `core/tests/placement_record.rs`).
#[tokio::test]
async fn restart_recovers_id_allocators_no_collision() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.redb");
    let frags = dir.path().join("frags");

    // Process 1: store object A (inode 1), then "crash" (drop the handles).
    {
        let g1 = Gateway::new(
            RedbMetadataStore::open(&db_path).expect("redb"),
            FsChunkStore::open(&frags).expect("fs store"),
            MemCoordination::new(),
        );
        g1.put_object("bucket/a", b"the first object's bytes")
            .await
            .expect("PUT A");
    }

    // Process 2: reopen the SAME persisted state and RECOVER the allocators before serving.
    let g2 = Gateway::new(
        RedbMetadataStore::open(&db_path).expect("redb reopen"),
        FsChunkStore::open(&frags).expect("fs reopen"),
        MemCoordination::new(),
    );
    g2.recover()
        .await
        .expect("recover allocators from persisted high-water marks");

    // A new-key PUT after recovery must NOT collide with A's committed inode/chunk ids.
    g2.put_object("bucket/b", b"the second object's bytes")
        .await
        .expect("a new-key PUT after recover must not collide with a committed id");

    // Both objects survive byte-identical: B's write did not clobber A's fragments, and A's
    // inode id was not re-minted.
    assert_eq!(
        g2.get_object("bucket/a").await.expect("get A").as_deref(),
        Some(&b"the first object's bytes"[..]),
        "object A survives the restart intact",
    );
    assert_eq!(
        g2.get_object("bucket/b").await.expect("get B").as_deref(),
        Some(&b"the second object's bytes"[..]),
        "object B stored under a fresh, non-colliding id",
    );
}

/// A restart is safe **without** `recover` — but *only because the prior process already
/// persisted the shared inode allocator* (issue #477, iteration-2 carry-forward: the earlier
/// blanket "safe by construction" claim did not hold for a legacy store, so this test is
/// narrowed to the case it actually proves). Under the cluster-safe scheme the gateway
/// allocates inodes from the durable `meta:next_inode` CAS counter and mints coordination-free
/// chunk ids (a fresh random epoch, ADR-0019). So when process 1 — running *this same code* —
/// stores object A it also advances the persisted counter past A; a restarted process 2 then
/// resumes strictly above A even if it never calls `recover`, and its fresh random chunk epoch
/// keeps chunk ids disjoint. Under the OLD per-process counters this identical
/// restart-without-recover replayed inode/chunk id 1 and corrupted the store (the finding-1
/// bug); the coordinated-inode + coordination-free-chunk scheme removes that failure mode for
/// this case outright.
///
/// The *migration* case — a store an older single-process gateway left with inodes on disk but
/// **no** persisted counter — is a different matter: there `recover` is load-bearing, pinned by
/// [`recover_seeds_the_allocator_over_a_legacy_store_without_meta_next_inode`].
#[tokio::test]
async fn restart_without_recover_is_safe_when_prior_process_persisted_the_allocator() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.redb");
    let frags = dir.path().join("frags");

    // Process 1 (this same coordinated code): storing A advances the persisted `meta:next_inode`
    // counter past A, so the on-disk store already carries the allocator state.
    {
        let g1 = Gateway::new(
            RedbMetadataStore::open(&db_path).expect("redb"),
            FsChunkStore::open(&frags).expect("fs store"),
            MemCoordination::new(),
        );
        g1.put_object("bucket/a", b"first").await.expect("PUT A");
    }

    // Restart and SKIP recover: the persisted `meta:next_inode` counter still resumes the inode
    // allocator above A, and a fresh random chunk epoch keeps the chunk ids disjoint — so the
    // new-key PUT commits, with no collision.
    let g2 = Gateway::new(
        RedbMetadataStore::open(&db_path).expect("redb reopen"),
        FsChunkStore::open(&frags).expect("fs reopen"),
        MemCoordination::new(),
    );
    g2.put_object("bucket/b", b"second").await.expect(
        "a new-key PUT after a restart must commit without recover — the prior process persisted \
         the allocator above A and the chunk epoch is fresh",
    );

    assert_eq!(
        g2.get_object("bucket/a").await.expect("get A").as_deref(),
        Some(&b"first"[..]),
        "object A survives the restart intact — B's write did not clobber its fragments",
    );
    assert_eq!(
        g2.get_object("bucket/b").await.expect("get B").as_deref(),
        Some(&b"second"[..]),
        "object B stored under fresh, non-colliding ids",
    );
}

/// `recover` is **load-bearing for the migration case it exists for** (issue #477, iteration-2
/// carry-forward). A store an *older single-process gateway* wrote carries committed objects
/// under `inode:` keys but **no** persisted `meta:next_inode` counter — that gateway minted
/// inodes from an in-process counter and never persisted one. A new gateway started over such a
/// legacy store:
///
/// * **WITHOUT `recover`** re-mints inode 1 for a new key and collides with the legacy inode
///   (`metadata::create` is `require_absent` on the inode key), so the new-key PUT is spuriously
///   rejected — exactly the corruption finding 1 closes, and precisely why the plain
///   restart-without-recover safety claim does *not* extend to a legacy store; but
/// * **WITH `recover`** (`seed_next_inode_floor` over `high_water_marks`) seeds the persisted
///   allocator strictly above every legacy inode, so the migrating PUT commits and both objects
///   round-trip byte-identical.
///
/// The legacy on-disk shape is reproduced faithfully by storing an object through the current
/// gateway (which writes `inode:1` **and** advances `meta:next_inode`) and then deleting the
/// `meta:next_inode` key — the one artificial step that leaves exactly what the old gateway
/// left: inodes present, allocator counter absent. This drives the production
/// `Gateway::{recover, put_object, get_object}`, so it proves `recover` is not a no-op for the
/// migration it targets.
#[tokio::test]
async fn recover_seeds_the_allocator_over_a_legacy_store_without_meta_next_inode() {
    use wyrd_traits::{MetadataStore, WriteBatch};

    // `cli::NEXT_INODE_KEY` is crate-private; the on-disk key it single-sources is
    // `meta:next_inode` (`crates/server/src/cli.rs:56`).
    const NEXT_INODE_KEY: &[u8] = b"meta:next_inode";

    // Reproduce the on-disk shape an older single-process gateway left: a committed object A
    // under `inode:` keys, but NO persisted allocator. Store A through the current gateway
    // (which writes inode:1 AND advances meta:next_inode), then DELETE meta:next_inode — the one
    // artificial step that models a legacy store (inodes present, counter absent).
    async fn seed_legacy_store(db_path: &std::path::Path, frags: &std::path::Path) {
        {
            let g = Gateway::new(
                RedbMetadataStore::open(db_path).expect("redb"),
                FsChunkStore::open(frags).expect("fs store"),
                MemCoordination::new(),
            );
            g.put_object("bucket/a", b"first").await.expect("PUT A");
        }
        let meta = RedbMetadataStore::open(db_path).expect("redb reopen to strip counter");
        assert!(
            meta.get(NEXT_INODE_KEY)
                .await
                .expect("get counter")
                .is_some(),
            "the current gateway must persist meta:next_inode, so stripping it models a legacy store",
        );
        meta.commit(WriteBatch::new().delete(NEXT_INODE_KEY.to_vec()))
            .await
            .expect("strip meta:next_inode to model a legacy single-process store");
    }

    // (1) Over a legacy store WITHOUT recover, the migrating new-key PUT collides — `recover` is
    //     load-bearing here (unlike the persisted-counter restart above).
    {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("meta.redb");
        let frags = dir.path().join("frags");
        seed_legacy_store(&db_path, &frags).await;

        let g = Gateway::new(
            RedbMetadataStore::open(&db_path).expect("redb reopen"),
            FsChunkStore::open(&frags).expect("fs reopen"),
            MemCoordination::new(),
        );
        let collided = g.put_object("bucket/b", b"second").await;
        assert!(
            collided.is_err(),
            "over a legacy store WITHOUT recover, a new-key PUT re-mints inode 1 and collides \
             with the legacy inode — recover is load-bearing for the migration finding 1 closes",
        );
    }

    // (2) Over the same legacy shape WITH recover, the allocator is seeded above the legacy
    //     inode, so the migrating PUT commits and both objects round-trip byte-identical.
    {
        let dir = tempfile::tempdir().expect("temp dir");
        let db_path = dir.path().join("meta.redb");
        let frags = dir.path().join("frags");
        seed_legacy_store(&db_path, &frags).await;

        let g = Gateway::new(
            RedbMetadataStore::open(&db_path).expect("redb reopen"),
            FsChunkStore::open(&frags).expect("fs reopen"),
            MemCoordination::new(),
        );
        g.recover()
            .await
            .expect("recover seeds meta:next_inode above the legacy inodes");
        g.put_object("bucket/b", b"second").await.expect(
            "after recover, a new-key PUT over the legacy store must commit — the allocator is \
             seeded above the legacy inode",
        );
        assert_eq!(
            g.get_object("bucket/a").await.expect("get A").as_deref(),
            Some(&b"first"[..]),
            "legacy object A survives the migration byte-identical",
        );
        assert_eq!(
            g.get_object("bucket/b").await.expect("get B").as_deref(),
            Some(&b"second"[..]),
            "object B stored under a fresh, non-colliding inode after recover",
        );
    }
}

/// Durability finding (iter-8 carry-forward), now cluster-safe (issue #477): a new object
/// minted after `PUT → DELETE → restart` must not collide with the **orphan ledger**. After a
/// DELETE the deleted object's inode key is gone and its chunk was already committed (so no
/// `pending:` entry survives), yet its fragments live on under a grace record
/// (`orphan:<ds>:<chunk>:<index>`, `crates/core/src/metadata.rs:60`) until the custodian GC's
/// reader-safe window elapses (`crates/custodian/src/gc.rs:134-141`). If the next object
/// re-minted that chunk id, the stale orphan record would reclaim a fragment the new object
/// just wrote — permanent data loss.
///
/// This drives the **production** paths (`put_object` / `delete_object` / `recover` /
/// `get_object`) and then performs exactly the reclaim the orphan ledger authorises — GC
/// deletes each fragment the ledger names once its grace elapses (`gc.rs:136,152`) — by
/// deleting those very fragments through the same `ChunkStore`. Because chunk ids are now
/// coordination-free (a fresh random epoch per process, ADR-0019), B's id is disjoint from the
/// deleted object's orphaned id **by construction**, so reclaiming the old fragments leaves B
/// byte-identical — no orphan-ledger scan of a per-process counter is needed to keep them apart.
#[tokio::test]
async fn restart_recovers_id_allocators_over_orphan_ledger_no_reclaim_loss() {
    use wyrd_core::metadata::{parse_orphan_key, ORPHAN_PREFIX};
    use wyrd_traits::{ChunkStore, MetadataStore};

    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.redb");
    let frags = dir.path().join("frags");
    let object_b = b"object B must survive an orphan-ledger reclaim of the deleted object A";

    // Process 1: store object A (inode 1), then DELETE it — its fragments are now orphaned
    // under the grace ledger (still on disk), its inode key removed, its pending ledger already
    // cleared at commit. Then "crash" (drop the handles).
    {
        let g1 = Gateway::new(
            RedbMetadataStore::open(&db_path).expect("redb"),
            FsChunkStore::open(&frags).expect("fs store"),
            MemCoordination::new(),
        );
        g1.put_object("bucket/a", b"the deleted object's bytes")
            .await
            .expect("PUT A");
        let removed = g1.delete_object("bucket/a").await.expect("DELETE A");
        assert!(removed, "A was present, so DELETE removes it");
    }

    // Process 2: reopen the SAME persisted state and RECOVER, then store a NEW-key object B.
    {
        let g2 = Gateway::new(
            RedbMetadataStore::open(&db_path).expect("redb reopen"),
            FsChunkStore::open(&frags).expect("fs reopen"),
            MemCoordination::new(),
        );
        g2.recover()
            .await
            .expect("recover allocators from persisted high-water marks");
        g2.put_object("bucket/b", object_b)
            .await
            .expect("a new-key PUT after recover");
    }

    // Now perform exactly the reclaim the orphan ledger authorises: once the grace window
    // elapses, GC deletes each fragment the `orphan:` records name (`gc.rs:152`). Read those
    // records back and delete precisely those fragments through the same ChunkStore.
    {
        let meta = RedbMetadataStore::open(&db_path).expect("redb reopen for reclaim");
        let store = FsChunkStore::open(&frags).expect("fs reopen for reclaim");
        let orphans = meta.scan(ORPHAN_PREFIX).await.expect("scan orphan ledger");
        assert!(
            !orphans.is_empty(),
            "DELETE must have left A's fragments under the orphan grace ledger",
        );
        for (key, _) in &orphans {
            let (_dserver, frag) = parse_orphan_key(key).expect("well-formed orphan key");
            store
                .delete_fragment(frag)
                .await
                .expect("reclaim orphan fragment");
        }
    }

    // Object B must still round-trip byte-identical: its fragments were minted under a chunk
    // id DISJOINT from the reclaimed orphan chunk (a fresh random epoch, ADR-0019), so the
    // reclaim never touched them. Were the gateway to re-mint the orphaned chunk id, the
    // reclaim above would delete B's own fragments — this GET would then fail.
    let g3 = Gateway::new(
        RedbMetadataStore::open(&db_path).expect("redb reopen for read"),
        FsChunkStore::open(&frags).expect("fs reopen for read"),
        MemCoordination::new(),
    );
    g3.recover().await.expect("recover before read");
    assert_eq!(
        g3.get_object("bucket/b")
            .await
            .expect("GET B must succeed — the orphan reclaim must not have deleted B's fragments")
            .as_deref(),
        Some(&object_b[..]),
        "object B survives the orphan-ledger reclaim byte-identical: recover minted it a chunk \
         id disjoint from the deleted object's still-live orphan record",
    );
}

/// The "stream, don't buffer" invariant, proven **behaviourally** (carry-forward): drive
/// the production streaming PUT with a body source that reveals how many pieces have been
/// pulled, and a chunk store that records the pull-count at the moment the first fragment
/// is written. A streaming writer writes chunk 1 after pulling only enough pieces to fill
/// it (so the recorded count is far below the total); a buffering writer would drain the
/// whole source *first*, recording the total. Asserting the first write happens well before
/// the source is exhausted fails for a buffering implementation and passes for this one.
mod streaming_behaviour {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use bytes::Bytes;
    use wyrd_gateway_core::ContentHash;
    use wyrd_metadata_redb::RedbMetadataStore;
    use wyrd_traits::{ChunkStore, FragmentId, Health, PlacementChunkStore, Result};

    /// A chunk store that actually holds fragments (so the write's commit is real) and
    /// records how many source pieces had been pulled when the **first** fragment landed.
    struct RecordingChunkStore {
        fragments: Mutex<HashMap<FragmentId, Bytes>>,
        pieces_pulled: Arc<AtomicUsize>,
        /// Shared with the test so it can read the recorded count without an accessor on
        /// the gateway (which owns the store by value).
        first_write_at: Arc<Mutex<Option<usize>>>,
    }

    #[async_trait]
    impl ChunkStore for RecordingChunkStore {
        async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
            let mut first = self.first_write_at.lock().unwrap();
            if first.is_none() {
                *first = Some(self.pieces_pulled.load(Ordering::SeqCst));
            }
            drop(first);
            self.fragments.lock().unwrap().insert(id, fragment);
            Ok(())
        }
        async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
            Ok(self.fragments.lock().unwrap().get(&id).cloned())
        }
        async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
            Ok(self.fragments.lock().unwrap().keys().copied().collect())
        }
        async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
            self.fragments.lock().unwrap().remove(&id);
            Ok(())
        }
        async fn health(&self) -> Result<Health> {
            Ok(Health::Healthy)
        }
    }

    impl PlacementChunkStore for RecordingChunkStore {}

    #[tokio::test]
    async fn streaming_put_writes_chunks_as_they_arrive_not_after_buffering() {
        const CHUNK: usize = 4;
        const PIECES: usize = 16; // 16 pieces of one chunk each → 16 chunks.

        let pieces_pulled = Arc::new(AtomicUsize::new(0));
        let first_write_at = Arc::new(Mutex::new(None));
        let store = RecordingChunkStore {
            fragments: Mutex::new(HashMap::new()),
            pieces_pulled: Arc::clone(&pieces_pulled),
            first_write_at: Arc::clone(&first_write_at),
        };
        let gateway = Arc::new(
            Gateway::new(
                RedbMetadataStore::in_memory().expect("redb"),
                store,
                MemCoordination::new(),
            )
            .with_chunk_size(CHUNK),
        );

        // A lazy body source: each pull bumps `pieces_pulled`, so the chunk store can see
        // how far the source has been drained when it writes.
        let counter = Arc::clone(&pieces_pulled);
        let source = futures_util::stream::unfold(0usize, move |i| {
            let counter = Arc::clone(&counter);
            async move {
                if i == PIECES {
                    return None;
                }
                counter.fetch_add(1, Ordering::SeqCst);
                let piece = Bytes::from(vec![b'x'; CHUNK]);
                Some((Ok(piece), i + 1))
            }
        });

        gateway
            .put_object_streaming(
                "wyrd-bucket/streamed",
                Box::pin(source),
                ContentHash::Unverified,
            )
            .await
            .expect("streaming put");

        let first = first_write_at
            .lock()
            .unwrap()
            .expect("at least one fragment was written");
        assert!(
            first < PIECES,
            "streaming: the first chunk must be written before the whole {PIECES}-piece \
             source is drained (first write saw {first} pieces pulled); a buffering \
             implementation would only write after draining all {PIECES}"
        );
        // Sanity: a small bounded look-ahead, not the whole object.
        assert!(
            first <= 2,
            "streaming: the first write should follow within a chunk or two of the first \
             piece, not after buffering (saw {first})"
        );
    }
}

/// Real-SDK streaming interop (issue #364 carry-forward, the recurring "STREAMING-…-PAYLOAD
/// 501"): a **stock modern SDK** (boto3 / aws-sdk) sends an object PUT `aws-chunked`-framed
/// with `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and a per-chunk signature
/// chain. This drives the **production** wire path with exactly that byte format — a seed
/// request signed over the streaming sentinel, then chunks framed
/// `<hex-len>;chunk-signature=…\r\n<data>\r\n` with signatures chained off the seed — and
/// asserts the object round-trips byte-identical, and that a body whose chunk was tampered is
/// refused 403 (fail-closed). The chunk-signing algorithm itself is pinned to AWS's
/// **published** streaming example in `s3::streaming` unit tests, so a green here proves
/// AWS-format compatibility, not self-consistency.
///
/// RED before the streaming decoder exists (the gateway 501-ed a chunked upload / stored the
/// framing as object bytes); GREEN once it de-frames + verifies the chunks.
mod streaming_interop {
    use super::*;
    use wyrd_gateway_s3::sigv4::{signing_key_for, StreamingContext};
    use wyrd_gateway_s3::streaming::sign_chunk;

    const STREAMING_SENTINEL: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

    /// The seed headers + the context to chain chunk signatures from, exactly as a real SDK
    /// derives them: the seed request is signed over the `STREAMING-…-PAYLOAD` sentinel (the
    /// body is not available whole at signing time).
    fn streaming_seed(path: &str, host: &str) -> (Vec<(String, String)>, StreamingContext) {
        let creds = Credentials {
            access_key_id: ACCESS_KEY.to_string(),
            secret_access_key: SECRET_KEY.to_string(),
        };
        let amz_date = format_amz_date(SystemTime::now());
        let signed = wyrd_gateway_s3::sigv4::sign_with_payload_hash(
            "PUT",
            path,
            "",
            host,
            &amz_date,
            STREAMING_SENTINEL,
            &creds,
            REGION,
            "s3",
        );
        // The seed signature is the `Signature=` value of the Authorization header — the head
        // of the per-chunk signature chain.
        let seed_signature = signed
            .authorization
            .rsplit("Signature=")
            .next()
            .expect("authorization carries a Signature=")
            .to_string();
        let ctx = StreamingContext {
            seed_signature,
            signing_key: signing_key_for(SECRET_KEY, &amz_date[..8], REGION, "s3"),
            date_time: amz_date.clone(),
            scope: format!("{}/{REGION}/s3/aws4_request", &amz_date[..8]),
            signed: true,
            // The plain, no-trailer sentinel this module drives — no trailer declaration.
            trailer: None,
        };
        let headers = vec![
            ("authorization".to_string(), signed.authorization),
            ("x-amz-date".to_string(), signed.amz_date),
            ("x-amz-content-sha256".to_string(), signed.content_sha256),
            ("content-encoding".to_string(), "aws-chunked".to_string()),
        ];
        (headers, ctx)
    }

    /// Frame `data` split into `chunk_len`-byte `aws-chunked` chunks + a 0-byte terminator,
    /// each carrying its chained chunk-signature — the exact bytes a real SDK writes.
    fn frame(ctx: &StreamingContext, data: &[u8], chunk_len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        let mut prev = ctx.seed_signature.clone();
        let mut pieces: Vec<&[u8]> = data.chunks(chunk_len.max(1)).collect();
        pieces.push(&[]); // the terminating zero-length chunk
        for piece in pieces {
            let sig = sign_chunk(ctx, &prev, piece);
            out.extend_from_slice(
                format!("{:x};chunk-signature={sig}\r\n", piece.len()).as_bytes(),
            );
            out.extend_from_slice(piece);
            out.extend_from_slice(b"\r\n");
            prev = sig;
        }
        out
    }

    #[tokio::test]
    async fn stock_sdk_chunked_put_round_trips_byte_identical() {
        let (addr, _dir) = start_gateway().await;
        let host = addr.to_string();
        let path = "/wyrd-bucket/streamed-by-a-real-sdk";
        // Larger than the 8-byte chunk size AND spanning several aws-chunked frames, so the
        // decode + re-chunk onto the store is genuinely exercised.
        let object: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();

        let (headers, ctx) = streaming_seed(path, &host);
        let framed = frame(&ctx, &object, 512);
        let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
        assert_eq!(
            status, 200,
            "a stock-SDK aws-chunked streaming PUT must be accepted (not 501)"
        );

        let (status, body) = send(
            addr,
            "GET",
            path,
            &signed_headers("GET", path, &host, b""),
            b"",
        )
        .await;
        assert_eq!(status, 200, "GET of the streamed object must succeed");
        assert_eq!(
            body, object,
            "the streamed object must round-trip byte-identical (chunk framing stripped, \
             not stored as object data)"
        );
    }

    #[tokio::test]
    async fn tampered_chunk_body_is_refused_and_stores_nothing() {
        let (addr, _dir) = start_gateway().await;
        let host = addr.to_string();
        let path = "/wyrd-bucket/streamed-tampered";
        let object = vec![b'q'; 2000];

        let (headers, ctx) = streaming_seed(path, &host);
        let mut framed = frame(&ctx, &object, 512);
        // Flip a byte inside the first chunk's DATA (past its header line): the chunk
        // signature no longer matches, so fail-closed auth must refuse the upload.
        let first_data = framed
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("first chunk header")
            + 2;
        framed[first_data] ^= 0xff;

        let (status, _) = send(addr, "PUT", path, &headers, &framed).await;
        assert_eq!(
            status, 403,
            "a streaming body whose chunk signature does not verify must be refused"
        );

        // Nothing must have been committed.
        let (status, _) = send(
            addr,
            "GET",
            path,
            &signed_headers("GET", path, &host, b""),
            b"",
        )
        .await;
        assert_eq!(status, 404, "a refused streaming PUT must store nothing");
    }
}

/// **Real-SDK interop** (issue #364 carry-forward, iter-5 BLOCKING 3): the recurring reviewer
/// finding was that every wire round-trip here signs with the gateway's *own* `sigv4::sign` and
/// frames with its own streaming helper — a self-referential oracle that hides any divergence
/// from what a genuine AWS client emits. This module closes that gap with the **real Rust
/// `aws-sdk-s3`** as the independent oracle: the SDK's own signer, canonicalizer, and
/// `aws-chunked` framer drive `put_object` → `get_object` → `delete_object` against the
/// gateway's loopback listener, and the object must round-trip **byte-identical**. Nothing in
/// this path calls `crates/server`'s `sigv4`/`streaming` code to build the request, so a green
/// proves AWS-format compatibility (canonicalization, header set, streaming payload framing),
/// not self-consistency.
///
/// It runs under plain `cargo test` (no container): the SDK is pointed at the loopback endpoint
/// with **path-style** addressing, **static** credentials, and a **plaintext** hyper client
/// (the public-TLS terminator is deferred to #367, so the at-Check listener is plaintext
/// loopback). A live boto3 / aws-cli leg stays a pre-declared DEFERRED backstop, not the
/// at-Check bar.
///
/// RED before the wire surface / SigV4 canonicalization is AWS-correct (a real SDK request
/// 403s or its streamed body is stored as framing); GREEN once a stock SDK round-trips.
mod real_sdk_interop {
    use super::*;
    use aws_sdk_s3::config::{Credentials, Region};
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::Client;

    /// Build an `aws-sdk-s3` client pointed at the loopback gateway: a real SDK, configured
    /// only the way any S3-compatible endpoint is reached (custom endpoint, path-style, static
    /// creds, plaintext client). Retries and stalled-stream protection are disabled so the test
    /// is deterministic and needs no async sleep timer.
    fn sdk_client(addr: SocketAddr) -> Client {
        let http_client = aws_smithy_http_client::Builder::new().build_http();
        let config =
            aws_sdk_s3::Config::builder()
                .behavior_version_latest()
                .region(Region::new(REGION))
                .endpoint_url(format!("http://{addr}"))
                .credentials_provider(Credentials::new(
                    ACCESS_KEY, SECRET_KEY, None, None, "static",
                ))
                .http_client(http_client)
                .force_path_style(true)
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                .stalled_stream_protection(
                    aws_sdk_s3::config::StalledStreamProtectionConfig::disabled(),
                )
                .build();
        Client::from_conf(config)
    }

    #[tokio::test]
    async fn real_aws_sdk_put_get_delete_round_trips_byte_identical() {
        let (addr, _dir) = start_gateway().await;
        let client = sdk_client(addr);
        let bucket = "wyrd-bucket";
        let key = "real-sdk/round-trip-object";
        // Larger than the 8-byte chunk size so the object genuinely spans several chunks over
        // whatever wire form (single-shot or aws-chunked) the SDK chooses.
        let object: Vec<u8> = (0..9000u32).map(|i| (i % 251) as u8).collect();

        // PUT — the SDK signs + frames this itself (its own SigV4 + aws-chunked codec).
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(object.clone()))
            .send()
            .await
            .expect("a stock aws-sdk-s3 PUT must be accepted by the gateway");

        // GET — the object must come back byte-identical.
        let got = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("a stock aws-sdk-s3 GET must succeed");
        let bytes = got
            .body
            .collect()
            .await
            .expect("collect the GET body")
            .into_bytes();
        assert_eq!(
            bytes.as_ref(),
            object.as_slice(),
            "a real aws-sdk-s3 upload must round-trip byte-identical (framing stripped, not \
             stored as object data)"
        );

        // DELETE — then a GET must 404 (NoSuchKey).
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("a stock aws-sdk-s3 DELETE must succeed");
        let after = client.get_object().bucket(bucket).key(key).send().await;
        assert!(
            after.is_err(),
            "a GET of the SDK-deleted object must fail (NoSuchKey), not return stale bytes"
        );
    }

    #[tokio::test]
    async fn real_aws_sdk_unsigned_client_is_refused() {
        // A client with WRONG credentials the gateway does not accept: the SDK still signs, but
        // the signature verifies against a secret the gateway never issued → fail-closed 403.
        let (addr, _dir) = start_gateway().await;
        let http_client = aws_smithy_http_client::Builder::new().build_http();
        let config =
            aws_sdk_s3::Config::builder()
                .behavior_version_latest()
                .region(Region::new(REGION))
                .endpoint_url(format!("http://{addr}"))
                .credentials_provider(Credentials::new(
                    "AKIAINTRUDER0000000",
                    "an-attacker-secret-the-gateway-never-issued",
                    None,
                    None,
                    "static",
                ))
                .http_client(http_client)
                .force_path_style(true)
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                .stalled_stream_protection(
                    aws_sdk_s3::config::StalledStreamProtectionConfig::disabled(),
                )
                .build();
        let client = Client::from_conf(config);

        let err = client
            .put_object()
            .bucket("wyrd-bucket")
            .key("real-sdk/forbidden")
            .body(ByteStream::from(b"should never be stored".to_vec()))
            .send()
            .await
            .expect_err("a request signed with an unknown credential must be refused");
        let service_err = err.into_service_error();
        assert_eq!(
            service_err.meta().code(),
            Some("InvalidAccessKeyId"),
            "the gateway must fail closed on an unknown access key (no anonymous / forged access)"
        );
    }
}

// ---- the request id: the join key between a client failure and the server's record (#529) ----

/// Like [`send`], but also returns the response's header block, so a test can assert on
/// `x-amz-request-id` — which [`parse_response`] discards.
async fn send_with_headers(
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
    let (status, body) = parse_response(&raw);
    (status, head, body)
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

/// **Every** response carries `x-amz-request-id` — success as well as failure.
///
/// A field tester who reports "it failed at 14:32" gives you a timestamp and nothing else.
/// The id is what turns that into a `jq` selector over the server's own record. It has to be
/// on the success path too: a corruption noticed *later* is traced back through the PUT that
/// wrote it, and that PUT returned 200.
///
/// Pre-fix the header does not exist on any response — RED.
#[tokio::test]
async fn every_response_carries_a_request_id_header() {
    let (addr, _dir) = start_gateway().await;
    let body = b"trace me";

    let headers = signed_headers("PUT", "/b/traced", &addr.to_string(), body);
    let (status, head, _) = send_with_headers(addr, "PUT", "/b/traced", &headers, body).await;
    assert_eq!(status, 200, "the PUT succeeds");
    let put_id = header_value(&head, "x-amz-request-id")
        .expect("a 200 PUT must carry x-amz-request-id; pre-fix no response carries one");
    assert_eq!(
        put_id.len(),
        32,
        "the canonical 32-char hex rendering: {put_id}"
    );

    let headers = signed_headers("GET", "/b/traced", &addr.to_string(), b"");
    let (status, head, _) = send_with_headers(addr, "GET", "/b/traced", &headers, b"").await;
    assert_eq!(status, 200);
    let get_id = header_value(&head, "x-amz-request-id").expect("a 200 GET must carry one too");

    assert_ne!(
        put_id, get_id,
        "two requests must not share an id, or a log search returns the wrong one"
    );
}

/// On a failure, the id is in **both** places a client can see it: the `x-amz-request-id`
/// header (which an SDK surfaces in its own error reporting) and `<RequestId>` in the S3
/// error body (which a `curl`ing operator reads). They must be the SAME id — two different
/// ids would be worse than none, sending the operator to search for a request that never was.
///
/// Pre-fix the error XML is `<Error><Code/><Message/></Error>` with no `<RequestId>` at all,
/// and no header exists — RED on both assertions.
#[tokio::test]
async fn a_failure_reports_the_same_request_id_in_the_header_and_the_error_body() {
    let (addr, _dir) = start_gateway().await;

    // An unsigned request: refused at the SigV4 gate, the earliest error path there is. If
    // the id survives to *this* response it survives to all of them — it is minted before
    // any dispatch, so no failure mode can outrun it.
    let (status, head, body) = send_with_headers(addr, "GET", "/b/missing", &[], b"").await;
    assert_eq!(status, 403, "unsigned is refused");

    let header_id = header_value(&head, "x-amz-request-id")
        .expect("an error response must carry x-amz-request-id; pre-fix it does not");
    let xml = String::from_utf8_lossy(&body);
    let body_id = xml
        .split_once("<RequestId>")
        .and_then(|(_, rest)| rest.split_once("</RequestId>"))
        .map(|(id, _)| id.to_string())
        .expect("the S3 error body must carry <RequestId>; pre-fix the element is absent");

    assert_eq!(
        header_id, body_id,
        "the header and the body must name the SAME request, or the operator searches for a \
         request that never existed"
    );
}
