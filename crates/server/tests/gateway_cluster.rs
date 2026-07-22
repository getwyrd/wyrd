//! M2.8 definition of done (issue #155): the **static-endpoints gateway client
//! mode** — `wyrd put`/`get --endpoints …` — fans an object's erasure-coded
//! fragments across a `FanoutChunkStore<GrpcChunkStore>` built from a configured
//! endpoint list, holding metadata (and the persisted inode allocator) locally,
//! and round-trips objects byte-identically across a real, networked cluster.
//!
//! This is the in-process loopback proof of that composition (the C4-verify
//! criterion in the brief; the containerized `docker compose up` + `wyrd
//! put/get` flow is the supplementary manual / nightly tier). Like
//! `chunkstore-grpc/tests/round_trip.rs` and the Tier-2 test, it stands up real
//! gRPC D servers over loopback — real tonic transport, real HTTP/2 framing —
//! rather than an in-memory fake, then drives the *same* `cluster_store_put` /
//! `cluster_store_get` functions the CLI's `--endpoints` path uses, so the test
//! exercises the shipping client mode, not a stand-in.
//!
//! The load-bearing case (issue #155 iteration 2): storing **two distinct keys
//! across two separate gateway compositions** over one `--data-dir`. A
//! per-process inode counter (reset every invocation) would re-allocate inode 1
//! on the second PUT and fail it as a bogus `Conflict`, and would reuse chunk id
//! 1 — clobbering the first object's fragments on the shared chunk store. Routing
//! the cluster path through the persisted `meta:next_inode` allocator (and
//! inode-derived chunk ids), exactly as the local-disk path does, is what makes
//! both objects survive and round-trip byte-identically.

#![forbid(unsafe_code)]

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService};
use wyrd_server::cli::{cluster_store_get, cluster_store_put, connect_fanout, open_cluster_meta};
use wyrd_server::DEFAULT_DURABILITY;
use wyrd_traits::CommitOutcome;

/// Stand up one D-server service over a fresh `FsChunkStore`, bound to an
/// ephemeral loopback port, and return its dialable endpoint. The listener is
/// bound (accepting into the OS backlog) before we hand back the endpoint, so a
/// client can dial with no startup race. The temp dir and serve task are kept
/// alive for the test's duration.
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

/// A deterministic payload that spans several chunks at the test's chunk size,
/// so each chunk fans its own fragments out across the cluster. `seed` makes the
/// two objects' bytes distinct.
fn payload(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_gateway_round_trips_distinct_objects_across_separate_compositions() {
    // A four-server cluster of real, networked gRPC D servers over loopback. Four,
    // not three: the default rs(6,3) places 9 fragments per chunk needing any k=6,
    // so a single-server loss must stay above k (matching the documented compose).
    let mut endpoints = Vec::new();
    let mut dirs = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..4 {
        let (endpoint, dir, server) = spawn_dserver().await;
        endpoints.push(endpoint);
        dirs.push(dir);
        servers.push(server);
    }

    let data_dir = tempfile::tempdir().expect("data dir");
    let data_dir_path = data_dir.path().to_str().unwrap();

    // A small chunk size so each object spans several chunks under rs(6,3): every
    // chunk's 9 fragments fan out across the four D servers.
    let chunk_size = 8 * 1024;
    let one = payload(7, 40 * 1024 + 777);
    let two = payload(200, 33 * 1024 + 13);

    // Composition A: a FRESH gateway composition over the data dir stores the
    // first object, then is dropped (releasing the redb file lock) — modelling a
    // first `wyrd put --endpoints …` *process*.
    {
        let meta = open_cluster_meta(data_dir_path).expect("open meta A");
        let fanout = connect_fanout(&endpoints).await.expect("connect fanout A");
        let outcome = cluster_store_put(
            &meta,
            &fanout,
            "obj/one",
            &one,
            chunk_size,
            DEFAULT_DURABILITY,
        )
        .await
        .expect("PUT obj/one fans fragments out over gRPC");
        assert_eq!(
            outcome,
            CommitOutcome::Committed,
            "first object must commit"
        );
    }

    // Composition B: a SECOND fresh composition over the SAME data dir stores a
    // DISTINCT key. With a per-process id counter this PUT would re-allocate inode
    // 1 and fail as a bogus `Conflict` (and reuse chunk id 1, clobbering obj/one's
    // fragments). The persisted allocator keeps the ids distinct.
    let meta = open_cluster_meta(data_dir_path).expect("open meta B");
    let fanout = connect_fanout(&endpoints).await.expect("connect fanout B");
    let outcome = cluster_store_put(
        &meta,
        &fanout,
        "obj/two",
        &two,
        chunk_size,
        DEFAULT_DURABILITY,
    )
    .await
    .expect("PUT obj/two");
    assert_eq!(
        outcome,
        CommitOutcome::Committed,
        "a distinct second key across a separate composition must commit, not conflict",
    );

    // BOTH objects round-trip byte-identically over the networked cluster. Reading
    // obj/one back proves obj/two's write did not clobber its fragments (no chunk-id
    // collision) — the corruption a per-process counter would have caused.
    let got_one = cluster_store_get(&meta, &fanout, "obj/one")
        .await
        .expect("GET obj/one reconstructs from gRPC fragments");
    assert_eq!(
        got_one.as_deref(),
        Some(&one[..]),
        "obj/one must survive obj/two's PUT byte-identically",
    );
    let got_two = cluster_store_get(&meta, &fanout, "obj/two")
        .await
        .expect("GET obj/two reconstructs from gRPC fragments");
    assert_eq!(
        got_two.as_deref(),
        Some(&two[..]),
        "obj/two read back over the networked gRPC cluster must be byte-identical",
    );

    // A missing key reports not-found, not an error, through the same path.
    assert!(
        cluster_store_get(&meta, &fanout, "obj/absent")
            .await
            .expect("a miss is Ok(None), not a transport error")
            .is_none(),
        "an unknown key returns None",
    );

    for server in servers {
        server.abort();
    }
}
