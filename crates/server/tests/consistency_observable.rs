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

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_gateway_s3::sigv4::Credentials;
use wyrd_gateway_s3::{S3Config, S3Gateway};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::consistency_observable::{ObservableS3Client, OpKind};
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
    tokio::spawn(async move {
        server.serve(listener).await.expect("serve");
    });
    (addr, dir)
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
    let v1 = c.get(key).await.expect("get v1");
    c.put(key, 2).await.expect("put v2");
    let v2 = c.get(key).await.expect("get v2");
    c.put(key, 3).await.expect("put v3");
    let v3 = c.get(key).await.expect("get v3");
    c.delete(key).await.expect("delete");
    let after_delete = c.get(key).await.expect("get after delete");

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
