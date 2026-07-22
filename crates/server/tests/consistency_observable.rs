//! Consistency-checker observable client (issue #405, ADR-0041 decision 1, #329 slice 2):
//! drives an overwriting PUT v1 -> v2 -> v3 workload with interleaved GET/DELETE against a
//! real, in-process loopback S3 gateway (mirrors `s3_http_wire.rs::start_gateway`) through
//! [`wyrd_server::consistency_observable::ObservableS3Client`], and asserts the recorded
//! history is **non-vacuous** and **well-formed**: every op carries a real `start <= end`
//! timestamp span, and the register's observed versions never regress (no stale/torn
//! reads) — the register-model history #329's downstream checker needs (ADR-0041
//! §Decision 1). The linearizability verdict itself and the real-cluster partition-nemesis
//! run are a separate, later #329 slice and are NOT exercised here.
//!
//! RED before the observable exists (no client type to construct, so no history can be
//! recorded); GREEN once it drives real PUT/GET/DELETE over the wire and records a
//! well-formed, non-vacuous history.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::Credentials;
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::consistency_observable::{ObservableS3Client, OpKind, INDETERMINATE_STATUS};
use wyrd_server::consistency_workload::{is_indeterminate, register_completion_keyword};
use wyrd_server::Gateway;

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const BUCKET: &str = "wyrd-bucket";

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// Start the S3 gateway on an ephemeral loopback port (mirrors
/// `s3_http_wire.rs::start_gateway_with_handle`) — the same in-process loopback gateway
/// (redb + fs + mem backends behind the HTTP listener) the brief's Falsifiability section
/// names as what fully exhibits the register.
async fn start_gateway() -> (SocketAddr, tempfile::TempDir) {
    let (addr, dir, _handle) = start_gateway_with_handle().await;
    (addr, dir)
}

/// As [`start_gateway`], but hands back the serving task's handle so a test can **abort** it —
/// dropping the listener and making the port refuse connections. That is the load-light,
/// headless stand-in for the transport failure a #407 nemesis leg induces on a real cluster: it
/// is the *client's* recording behaviour under a dead peer that is under test here, and that
/// behaviour is identical whether the peer died from `iptables -j DROP` or from a dropped
/// listener.
async fn start_gateway_with_handle() -> (SocketAddr, tempfile::TempDir, tokio::task::JoinHandle<()>)
{
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway: Arc<Backend> = Arc::new(Gateway::new(
        RedbMetadataStore::in_memory().expect("redb"),
        FsChunkStore::open(dir.path()).expect("fs store"),
        MemCoordination::new(),
    ));
    let config = S3Config::new(vec![Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }]);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = S3Gateway::new(Arc::clone(&gateway), config);
    let handle = tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    (addr, dir, handle)
}

/// Abort the serving task and block until `addr` genuinely refuses connections, so a test that
/// wants a transport failure gets one deterministically rather than racing the runtime.
async fn stop_gateway(handle: tokio::task::JoinHandle<()>, addr: SocketAddr) {
    handle.abort();
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_err() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the aborted gateway kept accepting connections at {addr}");
}

fn client(addr: SocketAddr) -> ObservableS3Client {
    let creds = Credentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    };
    ObservableS3Client::new(addr, BUCKET, creds, REGION)
}

#[tokio::test]
async fn observable_records_a_nonvacuous_wellformed_register_history() {
    let (addr, _dir) = start_gateway().await;
    let mut c = client(addr);
    let key = "register-object";

    // Overwrite v1 -> v2 -> v3 with an interleaved read after each commit, then delete and
    // confirm a post-delete read observes nothing — the register workload ADR-0041 decision
    // 1 models (an overwrite is a new inode version, bumped at the commit-point CAS).
    c.put(key, 1).await.expect("put v1");
    let v1 = c.get(key).await.expect("get v1").version;
    c.put(key, 2).await.expect("put v2");
    let v2 = c.get(key).await.expect("get v2").version;
    c.put(key, 3).await.expect("put v3");
    let v3 = c.get(key).await.expect("get v3").version;
    c.delete(key).await.expect("delete");
    let after_delete = c.get(key).await.expect("get after delete").version;

    assert_eq!(
        v1,
        Some(1),
        "a GET right after PUT v1 must read-after-commit v1"
    );
    assert_eq!(
        v2,
        Some(2),
        "a GET right after PUT v2 must read-after-commit v2, not a stale v1"
    );
    assert_eq!(
        v3,
        Some(3),
        "a GET right after PUT v3 must read-after-commit v3, not a stale v2"
    );
    assert_eq!(
        after_delete, None,
        "a GET after DELETE must observe no value"
    );

    let history = c.into_history();

    // Non-vacuous: all 8 driven ops (3x PUT, 4x GET, 1x DELETE) are recorded — not the
    // empty/single-op history the #250 iterations produced over the immutable data path.
    assert_eq!(history.ops().len(), 8, "every driven op must be recorded");
    let puts = history
        .ops()
        .iter()
        .filter(|o| o.kind == OpKind::Put)
        .count();
    let gets = history
        .ops()
        .iter()
        .filter(|o| o.kind == OpKind::Get)
        .count();
    let deletes = history
        .ops()
        .iter()
        .filter(|o| o.kind == OpKind::Delete)
        .count();
    assert_eq!(
        (puts, gets, deletes),
        (3, 4, 1),
        "the recorded history must be genuinely the PUT/GET/DELETE workload driven"
    );

    // The recorded per-op version must match what was actually written/observed — pins the
    // history entries themselves (not just the client's return values above), so a broken
    // recorder that drops/mis-tags the observed version is caught here even if the client's
    // return value happens to still be correct.
    let recorded: Vec<Option<u64>> = history.ops().iter().map(|o| o.version).collect();
    assert_eq!(
        recorded,
        vec![
            Some(1), // PUT v1
            Some(1), // GET -> v1
            Some(2), // PUT v2
            Some(2), // GET -> v2
            Some(3), // PUT v3
            Some(3), // GET -> v3
            None,    // DELETE
            None,    // GET after DELETE -> absent
        ],
        "each history entry must carry the register version actually written/observed"
    );

    // Well-formed: every op carries a real client-observed start<=end span (a non-empty
    // history of individually sane timestamps).
    assert!(
        history.well_formed(),
        "every recorded op must have a non-reversed start<=end real-time span"
    );

    // The register model itself (ADR-0041 decision 1): no stale/torn reads — the observed
    // versions never regress in real-time order.
    assert!(
        history.versions_monotone_per_key(),
        "the register's observed versions must be monotone per key (no stale/torn read)"
    );
}

// ─── #408: an op whose transport failed is an OBSERVATION, never a gap ────────────────
//
// The checked run (#408) drives this client under a real #407 nemesis leg, so ops genuinely
// fail: a partitioned coordinator refuses the connection. Two fabrications the recorder must
// not commit, both exercised here against the production client over a real (then killed)
// loopback listener:
//
//   1. **A dropped op.** An errored PUT/GET/DELETE that is never pushed leaves the checker a
//      history with a hole exactly where the fault bit — the ops that raced the nemesis are the
//      ones that vanish, and the remaining history reads as an unremarkable clean run.
//   2. **A stale status.** With the op missing, a caller reading "the status of the op I just
//      drove" off the tail (`history().ops().last()`) inherits the PREVIOUS op's status — so an
//      indeterminate op serializes as a definite `:ok` of a neighbour's 200 (INV-1's exact
//      prohibition: no fabricated certainty).

#[tokio::test]
async fn a_transport_failure_records_the_op_as_indeterminate_rather_than_omitting_it() {
    let (addr, _dir, handle) = start_gateway_with_handle().await;
    let mut c = client(addr);
    let key = "register-object";

    // One real, determinate 200 op — this is the op a stale-status read would later inherit.
    let committed = c
        .put(key, 1)
        .await
        .expect("put v1 against the live gateway");
    assert_eq!(committed.status, 200, "the live PUT commits determinately");

    // The fault: the peer goes away mid-run, exactly as a partitioned coordinator does.
    stop_gateway(handle, addr).await;

    let put_err = c.put(key, 2).await;
    let get_err = c.get(key).await;
    let delete_err = c.delete(key).await;
    assert!(
        put_err.is_err() && get_err.is_err() && delete_err.is_err(),
        "the ops must genuinely fail at the transport against a dead peer — otherwise this test \
         is not exercising the indeterminate path at all"
    );

    let history = c.into_history();

    // (1) Never omitted: all four invoked ops are in the history, not just the one that got a
    // status back.
    assert_eq!(
        history.ops().len(),
        4,
        "every invoked op must be recorded — an op whose transport failed is an indeterminate \
         observation, not a hole in the history: {:?}",
        history.ops(),
    );

    // (2) Never fabricated: each errored op is stamped indeterminate, so INV-1's completion-type
    // arm serializes it `:info` — never a definite `:ok`/`:fail`.
    for op in &history.ops()[1..] {
        assert_eq!(
            op.status, INDETERMINATE_STATUS,
            "an op whose round trip produced no status must be stamped indeterminate: {op:?}",
        );
        assert!(
            is_indeterminate(op.status),
            "the stamped status must satisfy the substrate's INV-1 indeterminacy predicate",
        );
        assert_eq!(
            register_completion_keyword(op.kind, op.status),
            "info",
            "an indeterminate op must serialize as :info (INV-1), never a definite completion",
        );
    }

    // (3) The stale-status fabrication itself: the tail of the history must be the op that was
    // actually just driven, NOT the earlier determinate 200 inherited from a neighbour.
    let last = history.ops().last().expect("a recorded op");
    assert_eq!(last.kind, OpKind::Delete, "the tail is the op last driven");
    assert_ne!(
        last.status, 200,
        "the errored op must never inherit the previous op's determinate 200 status",
    );

    // (4) The indeterminate PUT still carries the version it ATTEMPTED — that is the write the
    // checker's `:invoke` micro-op states (`[:w key 2]`); a versionless write has no
    // representable rw-register encoding at all.
    let indeterminate_put = &history.ops()[1];
    assert_eq!(indeterminate_put.kind, OpKind::Put);
    assert_eq!(
        indeterminate_put.version,
        Some(2),
        "the indeterminate PUT records the version it attempted",
    );
}

// The companion soundness property — that an indeterminate write is never counted as an observed
// version, so a correct later read of the earlier version is not fabricated into a stale-read
// violation — is unit-tested at `consistency_observable::tests::
// an_indeterminate_write_is_not_counted_as_an_observed_version`. It lives there because exhibiting
// it needs a determinate read AFTER an indeterminate write, which a dead-peer loopback cannot
// stage (once the peer is gone, no read completes determinately); constructing the history
// directly drives the same production predicate without inventing a fake peer that comes back.

#[tokio::test]
async fn a_peer_that_closes_without_a_complete_response_records_the_op_as_indeterminate() {
    // The OTHER transport fault mode a nemesis induces: the peer ACCEPTS the connection (so
    // connect/write succeed) and then closes without a complete HTTP response, so `read_to_end`
    // returns successfully with empty bytes. Response parsing must surface that as `Err` down the
    // same recorded-`:info` path as a refused connection — a panic here would abort the workload
    // task and drop the invoked op from the history (both INV-1 prohibitions at once).
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let accept_then_close = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await; // consume the request's first bytes, answer nothing
        drop(stream);
    });

    let mut c = client(addr);
    let put_err = c.put("register-object", 7).await;
    accept_then_close.await.expect("fake peer task");

    let failed = put_err
        .expect_err("a torn (empty) response must be an OpFailed, not a success and never a panic");
    assert_eq!(failed.record.status, INDETERMINATE_STATUS);
    assert_eq!(
        failed.record.version,
        Some(7),
        "the indeterminate PUT records the version it attempted",
    );

    let history = c.into_history();
    assert_eq!(
        history.ops().len(),
        1,
        "the invoked op is recorded, not dropped by a parse panic: {:?}",
        history.ops(),
    );
}

#[tokio::test]
async fn a_body_cut_mid_transfer_records_an_indeterminate_read_not_a_prefix_version() {
    // The fabrication mode: the peer answers a well-formed 200 header declaring the full
    // body length, sends only a PREFIX of the register value, and resets. Accepting the
    // prefix would record a DETERMINATE read of the wrong version (`42` torn to `4`) — a
    // wrong observation in the history handed to Elle, strictly worse than a lost one.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let torn_peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await;
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n4")
            .await; // one byte of the two-byte register value `42`, then close
        drop(stream);
    });

    let mut c = client(addr);
    let get_err = c.get("register-object").await;
    torn_peer.await.expect("fake peer task");

    let failed = get_err.expect_err("a mid-body reset must be an OpFailed, never a prefix read");
    assert_eq!(failed.record.status, INDETERMINATE_STATUS);
    assert_eq!(
        failed.record.version, None,
        "a torn read observed nothing — recording version 4 would fabricate an observation",
    );
}
