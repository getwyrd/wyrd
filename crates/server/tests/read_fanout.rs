//! M2.5 (issue #115), over the wire: the read-side mirror of `write_fanout.rs`.
//! An `rs(6,3)` GET fires `get_fragment` at all 9 networked D servers in
//! parallel and reconstructs byte-identical from whichever 6 verify their
//! checksums **first** — so the slow/hung `m` D servers are read *around*, never
//! waited on, and below `k` readable fragments the read returns a clean typed
//! error (proposal 0004, "Read — any-*k*-arrive-first" / "Error taxonomy";
//! architecture §6.2, §6.6).
//!
//! A D server is made **slow/hanging** (not cleanly down — a quick error already
//! drops through the serial loop) by an injected fault store whose `get_fragment`
//! never resolves. The discriminating property: the in-order serial fetch awaits
//! the first hung index and the read never completes; the parallel any-*k* read
//! reconstructs from the 6 live fragments and abandons the hung fetches. The
//! `FanoutChunkStore` over per-endpoint `GrpcChunkStore` clients is exactly the
//! networked-profile composition (`C: ChunkStore`), so `core`'s read path runs
//! unchanged against it (ADR-0010).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{FanoutChunkStore, GrpcChunkStore};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::EcScheme;
use wyrd_core::read::ReadError;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::dserver::{DServer, DSERVER_GROUP};
use wyrd_traits::{ChunkStore, CommitOutcome, FragmentId, Health, Result};

const ROOT: u64 = 0;
const NOW: u64 = 1_000;
const TTL: u64 = 5_000;
const CHUNK: usize = 1 << 16; // one chunk per payload
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };
const READ_BUDGET: Duration = Duration::from_secs(10);

fn ids_from(base: u128) -> impl FnMut() -> wyrd_traits::ChunkId {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// A `ChunkStore` over a filesystem store that can **hang** its reads: `put` and
/// `health` always delegate (so a write fan-out still commits and discovery still
/// sees the server as live), but when `hang_get` is set every `get_fragment`
/// never resolves — modelling a D server that accepts the request and then stalls
/// indefinitely (slow / hung, not cleanly down).
struct FaultStore {
    inner: FsChunkStore,
    hang_get: bool,
}

#[async_trait]
impl ChunkStore for FaultStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        self.inner.put_fragment(id, fragment).await
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        if self.hang_get {
            // Stall forever: the request is accepted but never answered.
            std::future::pending::<()>().await;
        }
        self.inner.get_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        self.inner.health().await
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
    /// Bind, register, and serve one D server per entry of `hang_gets` over
    /// loopback gRPC. The D server at index `i` hangs its reads iff `hang_gets[i]`.
    async fn start(hang_gets: &[bool]) -> Self {
        let coord = Arc::new(MemCoordination::new());
        let mut endpoints = Vec::new();
        let mut shutdowns = Vec::new();
        let mut handles = Vec::new();
        let mut dirs = Vec::new();
        for &hang_get in hang_gets {
            let dir = tempfile::tempdir().expect("temp dir");
            let inner = FsChunkStore::open(dir.path()).expect("fs store");
            let store = FaultStore { inner, hang_get };
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
    /// later fetch to it fails quickly (cleanly down, not hung).
    async fn stop(&mut self, i: usize) {
        if let Some(tx) = self.shutdowns[i].take() {
            let _ = tx.send(());
        }
        let _ = (&mut self.handles[i]).await;
    }

    /// Stop every still-running D server (test teardown).
    async fn stop_all(&mut self) {
        for i in 0..self.handles.len() {
            self.stop(i).await;
        }
    }
}

/// Write `data` under `rs(6,3)` across the cluster while all 9 D servers serve
/// reads, then return the committed inode. The write fan-out commits even when
/// some servers will later hang reads — `put` always lands.
async fn write_rs(
    meta: &RedbMetadataStore,
    fanout: &FanoutChunkStore<GrpcChunkStore>,
    name: &str,
    data: &[u8],
) -> wyrd_core::metadata::InodeRecord {
    let outcome = write::write_new_object(
        meta,
        fanout,
        ROOT,
        name,
        1,
        data,
        CHUNK,
        RS,
        NOW,
        TTL,
        ids_from(0x10),
    )
    .await
    .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed, "the commit is atomic");
    read::read_inode(meta, 1).await.unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rs_read_reconstructs_from_the_first_k_and_abandons_hung_d_servers() {
    // 6 D servers serve reads; 3 (indices 0,1,2) accept the request and hang.
    let hang = [true, true, true, false, false, false, false, false, false];
    let mut cluster = Cluster::start(&hang).await;
    let fanout = cluster.fanout().await;
    let meta = RedbMetadataStore::in_memory().unwrap();
    let data = b"any-k-arrive-first: read around the slow tail, never wait on it";

    let inode = write_rs(&meta, &fanout, "obj", data).await;

    // The serial read awaits index 0 — a hung D server — and never completes; the
    // parallel any-k read reconstructs from the 6 that arrive first and abandons
    // the 3 hung fetches. A wall-clock budget turns the pre-fix stall into a clean
    // red (the live fan-out answers in milliseconds, far inside the budget).
    let got = tokio::time::timeout(READ_BUDGET, read::read_object_from(&fanout, &inode))
        .await
        .expect("the any-k read must not stall on the hung D servers")
        .expect("reconstructs from the 6 fragments that arrived first");
    assert_eq!(got, data, "reconstructed bytes are byte-identical");

    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn below_k_readable_fragments_is_a_clean_typed_error() {
    // All 9 serve reads at write time so the write commits...
    let hang = [false; 9];
    let mut cluster = Cluster::start(&hang).await;
    let fanout = cluster.fanout().await;
    let meta = RedbMetadataStore::in_memory().unwrap();

    let inode = write_rs(&meta, &fanout, "obj", b"only five of nine survive").await;

    // ...then 4 D servers go cleanly down, leaving only 5 < k=6 readable.
    for i in [0, 1, 2, 3] {
        cluster.stop(i).await;
    }

    let err = tokio::time::timeout(READ_BUDGET, read::read_object_from(&fanout, &inode))
        .await
        .expect("below-k read must terminate, not hang")
        .expect_err("fewer than k readable fragments cannot reconstruct");
    // A clean, typed error — no panic, no corrupt bytes.
    let read_err = err
        .downcast_ref::<ReadError>()
        .expect("the read surfaces a typed ReadError");
    assert!(
        matches!(
            read_err,
            ReadError::InsufficientFragments {
                have: 5,
                need: 6,
                ..
            }
        ),
        "expected InsufficientFragments {{ have: 5, need: 6 }}, got {read_err:?}",
    );

    for i in [4, 5, 6, 7, 8] {
        cluster.stop(i).await;
    }
}
