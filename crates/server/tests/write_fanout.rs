//! M2.4 (issue #114), over the wire: an `rs(6,3)` write fans its 9 fragments out
//! to multiple networked D servers in parallel, the commit stays atomic, and a
//! partial fan-out (a downed D server) fails closed — no half-committed object,
//! only leased garbage.
//!
//! The `FanoutChunkStore` over per-endpoint `GrpcChunkStore` clients is exactly
//! the networked-profile composition (`C: ChunkStore`), so `core`'s write/read
//! paths run unchanged against it (ADR-0010).

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{FanoutChunkStore, GrpcChunkStore};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::dserver::{DServer, DSERVER_GROUP};
use wyrd_traits::{ChunkStore, CommitOutcome, FragmentId, MetadataStore, Result};

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const CHUNK: usize = 1 << 16; // one chunk per payload
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

fn ids_from(base: u128) -> impl FnMut() -> wyrd_traits::ChunkId {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// A running cluster of networked D servers, in placement order.
struct Cluster {
    endpoints: Vec<String>,
    shutdowns: Vec<Option<oneshot::Sender<()>>>,
    handles: Vec<JoinHandle<Result<()>>>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Cluster {
    /// Bind, register, and serve `n` D servers over loopback gRPC.
    async fn start(n: usize) -> Self {
        let coord = Arc::new(MemCoordination::new());
        let mut endpoints = Vec::new();
        let mut shutdowns = Vec::new();
        let mut handles = Vec::new();
        let mut dirs = Vec::new();
        for _ in 0..n {
            let dir = tempfile::tempdir().expect("temp dir");
            let store = FsChunkStore::open(dir.path()).expect("fs store");
            let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
                .await
                .expect("bind");
            let endpoint = server.endpoint().to_string();
            let lease = server
                .register(&*coord, DSERVER_GROUP, Duration::from_secs(3600))
                .await
                .expect("register");
            let (tx, rx) = oneshot::channel();
            let handle = tokio::spawn(server.serve(
                coord.clone(),
                lease,
                Duration::from_secs(1),
                async move {
                    let _ = rx.await;
                },
            ));
            endpoints.push(endpoint);
            shutdowns.push(Some(tx));
            handles.push(handle);
            dirs.push(dir);
        }
        Cluster {
            endpoints,
            shutdowns,
            handles,
            _dirs: dirs,
        }
    }

    /// A `GrpcChunkStore` per endpoint, in placement order, behind one fan-out.
    async fn fanout(&self) -> FanoutChunkStore<GrpcChunkStore> {
        let mut clients = Vec::new();
        for endpoint in &self.endpoints {
            clients.push(GrpcChunkStore::connect(endpoint.clone()).await.unwrap());
        }
        FanoutChunkStore::new(clients)
    }

    /// Stop D server `i` and wait for it to wind down (its port closes), so a
    /// later put to it fails.
    async fn stop(&mut self, i: usize) {
        if let Some(tx) = self.shutdowns[i].take() {
            let _ = tx.send(());
        }
        let _ = (&mut self.handles[i]).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rs_write_fans_out_to_distinct_d_servers_and_commits() {
    let cluster = Cluster::start(9).await;
    let fanout = cluster.fanout().await;
    let meta = RedbMetadataStore::in_memory().unwrap();
    let data = b"erasure-coded across nine networked D servers, in parallel";

    // The four-phase write runs unchanged against the networked fan-out store.
    let outcome = write::write_new_object(
        &meta,
        &fanout,
        ROOT,
        "obj",
        1,
        data,
        CHUNK,
        RS,
        || NOW,
        TTL,
        ids_from(0x10),
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed, "the commit is atomic");

    let inode = read::read_inode(&meta, 1).await.unwrap().unwrap();
    let chunk_id = inode.chunk_map[0].id;

    // Each of the 9 fragments landed on its own distinct D server: a direct client
    // to endpoint i holds index i and nothing else.
    for i in 0..9u16 {
        let direct = GrpcChunkStore::connect(cluster.endpoints[i as usize].clone())
            .await
            .unwrap();
        assert!(
            direct
                .get_fragment(FragmentId {
                    chunk: chunk_id,
                    index: i,
                })
                .await
                .unwrap()
                .is_some(),
            "D server {i} holds its placed fragment {i}",
        );
        let other = (i + 1) % 9;
        assert!(
            direct
                .get_fragment(FragmentId {
                    chunk: chunk_id,
                    index: other,
                })
                .await
                .unwrap()
                .is_none(),
            "D server {i} holds only its own fragment, not {other}",
        );
    }

    // The object reconstructs byte-identical through the read path over the fan-out.
    assert_eq!(read::read_object_from(&fanout, &inode).await.unwrap(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_fan_out_fails_closed_over_the_wire() {
    let mut cluster = Cluster::start(9).await;
    let fanout = cluster.fanout().await;
    let meta = RedbMetadataStore::in_memory().unwrap();

    // A first write commits cleanly while all nine are up.
    write::write_new_object(
        &meta,
        &fanout,
        ROOT,
        "ok",
        1,
        b"first object",
        CHUNK,
        RS,
        || NOW,
        TTL,
        ids_from(0x10),
    )
    .await
    .unwrap();
    // Its pending-ledger entries were released on the winning commit.
    assert!(
        meta.scan(b"pending:").await.unwrap().is_empty(),
        "a committed write leaves no pending entries",
    );

    // One D server goes down; the next write's fan-out loses fragment 4.
    cluster.stop(4).await;
    let result = write::write_new_object(
        &meta,
        &fanout,
        ROOT,
        "bad",
        2,
        b"second object",
        CHUNK,
        RS,
        || NOW,
        TTL,
        ids_from(0x20),
    )
    .await;

    assert!(
        result.is_err(),
        "a put to a downed D server fails the write"
    );
    // Fail-closed: the object was never committed...
    assert!(
        read::resolve(&meta, ROOT, "bad").await.unwrap().is_none(),
        "no half-committed object exists",
    );
    // ...and only leased garbage remains, for the pending-ledger sweep to reclaim.
    assert!(
        !meta.scan(b"pending:").await.unwrap().is_empty(),
        "the aborted write left leased garbage in the pending ledger",
    );

    // Tidy: stop the rest.
    for i in [0, 1, 2, 3, 5, 6, 7, 8] {
        cluster.stop(i).await;
    }
}
