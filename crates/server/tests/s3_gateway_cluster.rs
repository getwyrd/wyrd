//! Issue #454 definition of done: the **`wyrd s3` gateway composed over the cluster
//! backends by configuration**. The gateway must select its backends the same way every
//! other cluster-facing role does (`resolve_backend` for put/get/custodian #255,
//! `resolve_coordination_backend` for d-server #449) — not hardcode a private redb + a
//! local `FsChunkStore` + `MemCoordination`. A fleet of gateways must then be ONE pool
//! over the shared cluster state, not three single-node islands.
//!
//! This is the in-process loopback proof of that composition (the Check-exercised
//! redb + `MemCoordination` + `connect_fanout(--endpoints)` arm; the live TiKV + etcd +
//! 9-D-server demonstration is #455, off-Check). Like `gateway_cluster.rs` it stands up
//! real gRPC D servers over loopback — real tonic transport, real HTTP/2 framing, no
//! double — and, like `s3_http_wire.rs`'s `real_sdk_interop`, drives the S3 wire surface
//! with a **stock `aws-sdk-s3` client**. Crucially it drives the SAME production
//! composition core the CLI runs — `wyrd_server::cli::serve_s3_role` (which `cmd_s3`
//! calls) — over the cluster backends, so the test exercises the shipping wiring, not a
//! stand-in.
//!
//! The load-bearing assertions (the brief's success criterion): an S3 PUT's fragments
//! must **land on the loopback D servers** over gRPC (their on-disk stores fill), the
//! local `data-dir/chunks` directory must stay **empty** (nothing was written to a local
//! `FsChunkStore` — the pre-fix hardcoded behaviour is gone), and a subsequent GET must
//! read the object back **byte-identical**, reconstructed from the D-server fragments.
//!
//! RED before the fix: `serve_s3_role` (the config-selection composition core the test
//! drives) does not exist, so the test does not compile against the shipped surface.
//! GREEN once `cmd_s3` composes its backends by configuration.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::Path;

use aws_sdk_s3::config::{Credentials as SdkCredentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService};
use wyrd_gateway_s3::sigv4::Credentials as GatewayCredentials;
use wyrd_server::cli::{serve_s3_role, CoordinationBackend, MetadataBackend};

const ACCESS_KEY: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// Stand up one D-server service over a fresh `FsChunkStore`, bound to an ephemeral
/// loopback port, and return its dialable endpoint. Mirrors `gateway_cluster.rs`'s
/// `spawn_dserver`; the temp dir is returned so the test can later count the fragment
/// files that landed on this D server's on-disk store.
async fn spawn_dserver() -> (String, tempfile::TempDir, tokio::task::JoinHandle<()>) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    let service = ChunkStoreService::new(store);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChunkStoreServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });

    (format!("http://{addr}"), dir, server)
}

/// Count `.frag` fragment files under a chunk-store root
/// (`<root>/<32-hex chunk>/<05-index>.frag`, `chunkstore-fs`). A non-zero count means
/// fragments actually landed on that store; zero means nothing was written there.
fn count_fragments(root: &Path) -> usize {
    let mut n = 0;
    let Ok(chunk_dirs) = std::fs::read_dir(root) else {
        return 0;
    };
    for chunk_dir in chunk_dirs.flatten() {
        let path = chunk_dir.path();
        if !path.is_dir() {
            continue;
        }
        if let Ok(frags) = std::fs::read_dir(&path) {
            for frag in frags.flatten() {
                if frag.path().extension().and_then(|e| e.to_str()) == Some("frag") {
                    n += 1;
                }
            }
        }
    }
    n
}

/// Build an `aws-sdk-s3` client pointed at the loopback gateway — a real SDK configured
/// the way any S3-compatible endpoint is reached (custom endpoint, path-style, static
/// creds, plaintext client). Mirrors `s3_http_wire.rs::real_sdk_interop::sdk_client`;
/// retries and stalled-stream protection are disabled so the test is deterministic.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_gateway_composed_over_cluster_backends_lands_fragments_on_dservers() {
    // A four-server cluster of real, networked gRPC D servers over loopback. Four, not
    // three: rs(6,3) places 9 fragments per chunk needing any k=6, so fragments fan
    // across several servers (matching the documented compose / `gateway_cluster.rs`).
    let mut endpoints = Vec::new();
    let mut dirs = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..4 {
        let (endpoint, dir, server) = spawn_dserver().await;
        endpoints.push(endpoint);
        dirs.push(dir);
        servers.push(server);
    }

    // The gateway's data dir holds ONLY metadata (redb) in the cluster composition: the
    // fan-out chunk plane writes NO local chunk store, so `data-dir/chunks` must stay
    // empty. A tempdir keeps it isolated and alive for the whole test.
    let data_dir = tempfile::tempdir().expect("data dir");
    let data_dir_path = data_dir.path().to_str().unwrap().to_string();

    // Serve the S3 front door composed BY CONFIGURATION over the cluster backends — redb
    // metadata + `MemCoordination` + `connect_fanout(--endpoints)` chunk plane — exactly
    // the composition `cmd_s3` selects when `--endpoints` is given. Driven through the
    // SAME production `serve_s3_role` the CLI runs (not a re-implementation).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind gateway");
    let gateway_addr = listener.local_addr().expect("gateway addr");
    let credentials = vec![GatewayCredentials {
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
    }];
    let serve_endpoints = endpoints.clone();
    let serve_data_dir = data_dir_path.clone();
    let gateway_task = tokio::spawn(async move {
        serve_s3_role(
            MetadataBackend::Redb,
            CoordinationBackend::Mem,
            &serve_data_dir,
            Some(&serve_endpoints),
            credentials,
            REGION.to_string(),
            listener,
            // This test asserts the CLUSTER composition, not the request plane: no metrics
            // sink, so the RED events go to the ambient subscriber as before (#575).
            None,
        )
        .await
    });

    // A stock S3 client PUTs an object, then GETs it back over the gateway's listener.
    let client = sdk_client(gateway_addr);
    let bucket = "wyrd-bucket";
    let key = "cluster/round-trip-object";
    let object: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(object.clone()))
        .send()
        .await
        .expect("an S3 PUT through the cluster-composed gateway must be accepted");

    let got = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("an S3 GET of the stored object must succeed");
    let bytes = got
        .body
        .collect()
        .await
        .expect("collect the GET body")
        .into_bytes();

    // (1) Byte-identical round trip, reconstructed from the D-server fragments.
    assert_eq!(
        bytes.as_ref(),
        object.as_slice(),
        "the S3 object must round-trip byte-identical, reconstructed from the D-server fragments",
    );

    // (2) The fragments actually FANNED OUT onto the loopback D servers over gRPC — the
    // cluster pool, not a private local disk.
    let per_server: Vec<usize> = dirs.iter().map(|d| count_fragments(d.path())).collect();
    let total_fragments: usize = per_server.iter().sum();
    assert!(
        total_fragments > 0,
        "the object's fragments must have fanned out onto the loopback D servers over gRPC \
         (per-server fragment counts: {per_server:?})",
    );
    let servers_with_fragments = per_server.iter().filter(|&&c| c > 0).count();
    assert!(
        servers_with_fragments >= 2,
        "rs(6,3) fans 9 fragments across the D servers — more than one server must hold \
         fragments (per-server counts: {per_server:?})",
    );

    // (3) The local `data-dir/chunks` `FsChunkStore` stays EMPTY: nothing was written to a
    // local disk chunk store. This is the invariant #454 restores — the pre-fix
    // `FsChunkStore::open(dir.join("chunks"))` hardcode is gone.
    let local_chunks = data_dir.path().join("chunks");
    assert_eq!(
        count_fragments(&local_chunks),
        0,
        "no fragment may be written to a local FsChunkStore: the gateway composes over the \
         cluster D servers, not a private local disk ({}/chunks must stay empty)",
        data_dir_path,
    );

    gateway_task.abort();
    for server in servers {
        server.abort();
    }
}
