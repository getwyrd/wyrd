//! Aggregate write/read throughput across D-server counts (M2.7, proposal 0004
//! PR step 7, issue #117).
//!
//! Proposal 0004 § "Benchmarks": the §10 Q6 throughput-scaling claim ("scales
//! close to linearly with D-server count, divided by EC amplification") becomes
//! **first measurable** at M2. This bench measures the M2 data path — the
//! parallel fan-out write and the any-k-arrive-first read — over **real tonic**
//! gRPC D servers, sweeping the number of distinct D servers a chunk's `rs(6,3)`
//! fragments spread across. The servers run in-process over loopback (the same
//! real wire stack as `tests/round_trip.rs`): that is what is reproducible on a
//! laptop and what compiles in CI. The container cluster is exercised by the
//! Tier-2 integration test; the absolute number "lands on real hardware".
//!
//! M2's in-CI obligation is only that the data path builds **no shared
//! bottleneck** that would preclude Q6 — the bytes cross no shared component, so
//! widening the D-server count must not serialize. Run via `cargo xtask bench`;
//! CI tracks the numbers (regression visibility), it does not gate on them —
//! wall-clock is noisy (mirrors the M1.7 EC bench, issue #99).

#![forbid(unsafe_code)]

use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, FanoutChunkStore, GrpcChunkStore};
use wyrd_core::metadata::{EcScheme, InodeRecord, InodeState};
use wyrd_core::{read, write};

/// D-server counts swept. A chunk has 9 (`k+m`) fragments under rs(6,3); the
/// fan-out routes fragment `i` to server `i % n`, so at n=9 every fragment lands
/// on its own server and at n=1 they all share one — the bottleneck baseline.
const COUNTS: &[usize] = &[1, 3, 9];
/// Logical object size, reported as the throughput denominator.
const SIZE: usize = 256 * 1024;
/// Chunk size, so the object spans several chunks (each a 9-way fan-out).
const CHUNK_SIZE: usize = 64 * 1024;
/// The gateway-default durability (`server::DEFAULT_DURABILITY`).
const SCHEME: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

/// A deterministic payload (no RNG → reproducible inputs).
fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect()
}

/// A running cluster of `n` real, in-process gRPC D servers over loopback, with a
/// fan-out client store spread across them. Servers and temp dirs are held so they
/// outlive the benchmark. Dropping the cluster *detaches* the server tasks (a
/// dropped `JoinHandle` detaches, it does not abort) and removes the temp dirs;
/// the detached servers are reclaimed when the bench process exits.
struct Cluster {
    store: FanoutChunkStore<GrpcChunkStore>,
    _servers: Vec<JoinHandle<()>>,
    _dirs: Vec<tempfile::TempDir>,
}

/// Stand up `n` D servers (each over its own `FsChunkStore`) bound to ephemeral
/// loopback ports, and connect a `GrpcChunkStore` client to each.
async fn start_cluster(n: usize) -> Cluster {
    let mut clients = Vec::with_capacity(n);
    let mut servers = Vec::with_capacity(n);
    let mut dirs = Vec::with_capacity(n);
    for _ in 0..n {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = FsChunkStore::open(dir.path()).expect("open fs store");
        let service = ChunkStoreService::new(store);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        servers.push(tokio::spawn(async move {
            Server::builder()
                .add_service(ChunkStoreServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve");
        }));
        clients.push(
            GrpcChunkStore::connect(format!("http://{addr}"))
                .await
                .expect("connect"),
        );
        dirs.push(dir);
    }
    Cluster {
        store: FanoutChunkStore::new(clients),
        _servers: servers,
        _dirs: dirs,
    }
}

fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let data = payload(SIZE);
    let mut next: u128 = 0;
    let plan = write::plan_write(&data, CHUNK_SIZE, SCHEME, || {
        let id = next;
        next += 1;
        id
    })
    .expect("plan the write");
    let inode = InodeRecord {
        size: plan.size,
        chunk_map: plan.chunk_refs(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };

    let mut wgroup = c.benchmark_group("dserver_write_throughput");
    wgroup.throughput(Throughput::Bytes(SIZE as u64));
    for &n in COUNTS {
        let cluster = rt.block_on(start_cluster(n));
        wgroup.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                rt.block_on(write::write_fragments(&cluster.store, &plan))
                    .expect("fan-out write");
            });
        });
    }
    wgroup.finish();

    let mut rgroup = c.benchmark_group("dserver_read_throughput");
    rgroup.throughput(Throughput::Bytes(SIZE as u64));
    for &n in COUNTS {
        let cluster = rt.block_on(start_cluster(n));
        rt.block_on(write::write_fragments(&cluster.store, &plan))
            .expect("seed the read");
        rgroup.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                rt.block_on(read::read_object_from(&cluster.store, &inode))
                    .expect("any-k read");
            });
        });
    }
    rgroup.finish();
}

// CI-bounded sampling, like the EC bench: the numbers are tracked, not gated.
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = bench_throughput
}
criterion_main!(benches);
