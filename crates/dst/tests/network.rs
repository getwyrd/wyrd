//! Tier-1 network DST: the **real** gRPC `ChunkStore` wire code run on madsim's
//! simulated network under seed-reproducible faults (proposal 0004, "DST and
//! integration tests (the heart of M2)"; ADR-0009). M2.1–M2.5 built the proto
//! service, the `GrpcChunkStore` client + D-server service, the parallel fan-out
//! write, and the any-`k` read — but exercised them only over an in-process tonic
//! loopback. This campaign drives the same code over `madsim-tonic` (cfg-aliased
//! as `tonic` under `--cfg madsim`), so every put/get is a simulated gRPC
//! round-trip the simulator can drop, partition, delay, or corrupt — and replay
//! from its seed.
//!
//! The five Tier-1 properties asserted (proposal 0004 §"Tier-1"):
//!   1. parallel-write durability — all `n` fragments readable on their distinct
//!      D servers after a fan-out commit;
//!   2. `k`-of-`n` over the network with drops — byte-identical reconstruction
//!      when up to `m` fragment fetches are dropped (clogged links);
//!   3. re-read-on-corruption — a checksum-failing fragment is treated as absent
//!      and read around; the read still succeeds;
//!   4. fail-closed partial write — an injected partition/timeout aborts the
//!      write **before commit**, leaving only leased garbage, never a
//!      half-committed chunk;
//!   5. commit suite over the network — concurrent-writer-one-wins re-runs
//!      unchanged with the gRPC `ChunkStore`, proving the trait seam is real.
//!
//! Determinism holds despite the parallel fan-out: `try_join_all` /
//! `FuturesUnordered` poll cooperatively on one task, so completion ordering is
//! decided by madsim's seed-driven scheduler, not the wall clock.
//!
//! Requires `--cfg madsim` (set by `cargo xtask dst`, which sweeps 50 seeds); a
//! normal `cargo test` neither builds nor runs this file.
#![cfg(madsim)]

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use madsim::net::NetSim;
use madsim::runtime::Handle;
use madsim::task::NodeId;
use madsim::time::{sleep, timeout};
use tonic::transport::Server;
use wyrd_chunk_format::decode;
use wyrd_chunkstore_grpc::{ChunkStoreServer, ChunkStoreService, FanoutChunkStore, GrpcChunkStore};
use wyrd_core::metadata::EcScheme;
use wyrd_core::{read, write};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::{NetFault, SeededNetFaults};
use wyrd_traits::{ChunkStore, FragmentId, Health, Result};

/// RS(6,3): `k = 6` data + `m = 3` parity = `n = 9` fragments per chunk — the
/// default erasure-coded data path (proposal 0004 graduation criteria).
const RS: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };
const K: usize = 6;
const M: usize = 3;
const N: usize = K + M;
/// One chunk per object keeps the placement 1:1 — fragment index `i` lands on D
/// server `i`, so a clogged/corrupt server maps to exactly one missing fragment.
const CHUNK: usize = 1 << 16;
const PORT: u16 = 50_051;
const LEASE_EXPIRY: u64 = 6_000;

/// A unique, deterministic chunk-id generator starting just above `base`.
fn ids_from(base: u128) -> impl FnMut() -> u128 {
    let mut n = base;
    move || {
        n += 1;
        n
    }
}

/// Per-D-server fault behaviour injected behind the gRPC service — the
/// "fault-injecting fake under DST" the service is generic over (proposal 0004
/// §"D server").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreFault {
    /// A well-behaved D server.
    None,
    /// Returns corrupted bytes on `get`, so the fragment fails its client-side
    /// checksum and is treated as absent (property 3).
    CorruptGet,
}

/// An in-memory `ChunkStore` standing in for a D server's `FsChunkStore` under
/// simulation. It honours the contract the service relies on — **verify on
/// put** (a non-fragment is rejected) — and can be told to corrupt its `get`
/// responses to model on-the-wire corruption the client must read around.
struct DStore {
    fragments: Mutex<HashMap<(u128, u16), Vec<u8>>>,
    fault: StoreFault,
}

impl DStore {
    fn new(fault: StoreFault) -> Self {
        Self {
            fragments: Mutex::new(HashMap::new()),
            fault,
        }
    }
}

#[async_trait]
impl ChunkStore for DStore {
    async fn put_fragment(&self, id: FragmentId, fragment: Bytes) -> Result<()> {
        // Verify integrity before acknowledging (the D-server contract); a
        // non-fragment is rejected, exactly as `FsChunkStore` would.
        decode(&fragment).map_err(|e| Box::new(e) as wyrd_traits::BoxError)?;
        self.fragments
            .lock()
            .unwrap()
            .insert((id.chunk, id.index), fragment.to_vec());
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        let stored = self
            .fragments
            .lock()
            .unwrap()
            .get(&(id.chunk, id.index))
            .cloned();
        Ok(match (self.fault, stored) {
            (StoreFault::CorruptGet, Some(mut bytes)) => {
                // Flip a byte so the stored payload checksum no longer matches —
                // the client's `decode` rejects it and reads around.
                if let Some(last) = bytes.last_mut() {
                    *last ^= 0xff;
                }
                Some(Bytes::from(bytes))
            }
            (_, stored) => stored.map(Bytes::from),
        })
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

/// A running simulated cluster: `N` D-server nodes, each serving the real gRPC
/// `ChunkStore` over madsim's network, plus a client node from which the data
/// path runs.
struct Cluster {
    handle: Handle,
    server_ids: Vec<NodeId>,
    client_id: NodeId,
    endpoints: Vec<String>,
}

impl Cluster {
    /// Stand up `N` D servers (D server `i` applies `faults[i]`) and a client
    /// node. Returns once every server is bound and accepting.
    async fn start(faults: [StoreFault; N]) -> Self {
        let handle = Handle::current();
        let mut server_ids = Vec::with_capacity(N);
        let mut endpoints = Vec::with_capacity(N);

        for (i, fault) in faults.into_iter().enumerate() {
            let ip: IpAddr = format!("10.0.0.{}", i + 2).parse().unwrap();
            let node = handle.create_node().name(format!("d{i}")).ip(ip).build();
            let store = Arc::new(DStore::new(fault));
            let addr = format!("{ip}:{PORT}").parse().unwrap();
            node.spawn(async move {
                Server::builder()
                    .add_service(ChunkStoreServer::new(ChunkStoreService::from_arc(store)))
                    .serve(addr)
                    .await
                    .expect("d-server serve");
            });
            server_ids.push(node.id());
            endpoints.push(format!("http://{ip}:{PORT}"));
        }

        let client_ip: IpAddr = "10.0.0.1".parse().unwrap();
        let client = handle.create_node().name("client").ip(client_ip).build();
        let client_id = client.id();

        // Let every server bind before the client dials (deterministic in sim time).
        sleep(Duration::from_secs(1)).await;

        Self {
            handle,
            server_ids,
            client_id,
            endpoints,
        }
    }
}

/// Run `f` on the client node and await its result, surfacing a failed
/// assertion (a panic) as a test failure.
async fn on_client<F, Fut, T>(cluster: &Cluster, f: F) -> T
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send,
    T: Send + 'static,
{
    let node = cluster
        .handle
        .get_node(cluster.client_id)
        .expect("client node");
    node.spawn(async move { f().await })
        .await
        .expect("client task")
}

/// Property 1 — parallel-write durability. After a fan-out commit, every one of
/// the `n` fragments is readable on its own D server over the network.
#[madsim::test]
async fn parallel_write_durability_over_network() {
    let cluster = Cluster::start([StoreFault::None; N]).await;
    let endpoints = cluster.endpoints.clone();

    on_client(&cluster, move || async move {
        let meta = RedbMetadataStore::in_memory().expect("redb");
        let mut clients = Vec::new();
        for e in &endpoints {
            clients.push(GrpcChunkStore::connect(e.clone()).await.expect("connect"));
        }
        let chunks = FanoutChunkStore::new(clients);
        let payload = b"all n fragments must survive on distinct D servers";

        let plan = write::plan_write(payload, CHUNK, RS, ids_from(1)).unwrap();
        let chunk_id = plan.chunks[0].id;
        write::intent(&meta, &plan, LEASE_EXPIRY).await.unwrap();
        write::write_fragments(&chunks, &plan).await.unwrap();
        write::commit_create(&meta, 0, "obj", 1, &plan)
            .await
            .unwrap();
        write::release(&meta, &plan).await.unwrap();

        // Each of the n fragments is individually present on its placed D server.
        for index in 0..N as u16 {
            let got = chunks
                .get_fragment(FragmentId {
                    chunk: chunk_id,
                    index,
                })
                .await
                .unwrap();
            assert!(
                got.is_some(),
                "fragment {index} must be durable on its D server"
            );
        }

        let bytes = read::read_path(&meta, &chunks, 0, "obj").await.unwrap();
        assert_eq!(bytes.as_deref(), Some(&payload[..]));
    })
    .await;
}

/// Property 2 — `k`-of-`n` with drops. Clog up to `m` D-server links (chosen
/// from the seed) *after* a clean write; the any-`k` read reconstructs
/// byte-identical from the `k` survivors, never waiting on the dropped `m`.
#[madsim::test]
async fn k_of_n_read_survives_dropped_fetches() {
    let cluster = Cluster::start([StoreFault::None; N]).await;
    let endpoints = cluster.endpoints.clone();
    let server_ids = cluster.server_ids.clone();

    // Seed-reproducible choice of which (at most m) D servers to partition.
    let mut rng = rand_seed();
    let plan = SeededNetFaults::pick(&mut rng, N, M, NetFault::Drop);
    let clogged: Vec<NodeId> = plan.faults().keys().map(|&i| server_ids[i]).collect();

    on_client(&cluster, move || async move {
        let meta = RedbMetadataStore::in_memory().expect("redb");
        let mut clients = Vec::new();
        for e in &endpoints {
            clients.push(GrpcChunkStore::connect(e.clone()).await.expect("connect"));
        }
        let chunks = FanoutChunkStore::new(clients);
        let payload = b"reconstruct from whichever k arrive first";

        write::write_new_object(
            &meta,
            &chunks,
            0,
            "obj",
            1,
            payload,
            CHUNK,
            RS,
            0,
            LEASE_EXPIRY,
            ids_from(1),
        )
        .await
        .unwrap();

        // Drop up to m fragment fetches by partitioning their D servers.
        let net = NetSim::current();
        for &id in &clogged {
            net.clog_node(id);
        }

        let bytes = read::read_path(&meta, &chunks, 0, "obj").await.unwrap();
        assert_eq!(
            bytes.as_deref(),
            Some(&payload[..]),
            "read must reconstruct from the k survivors despite {} dropped fetches",
            clogged.len()
        );
    })
    .await;
}

/// Property 3 — re-read-on-corruption. Up to `m` D servers (chosen from the
/// seed) corrupt their `get` responses; each corrupt fragment fails its checksum,
/// is treated as absent, and is read around — the read still succeeds.
#[madsim::test]
async fn corrupt_fragment_is_read_around() {
    let mut rng = rand_seed();
    let faulted = SeededNetFaults::pick(&mut rng, N, M, NetFault::Corrupt);
    let mut faults = [StoreFault::None; N];
    for &i in faulted.faults().keys() {
        faults[i] = StoreFault::CorruptGet;
    }

    let cluster = Cluster::start(faults).await;
    let endpoints = cluster.endpoints.clone();

    on_client(&cluster, move || async move {
        let meta = RedbMetadataStore::in_memory().expect("redb");
        let mut clients = Vec::new();
        for e in &endpoints {
            clients.push(GrpcChunkStore::connect(e.clone()).await.expect("connect"));
        }
        let chunks = FanoutChunkStore::new(clients);
        let payload = b"a corrupt shard is never handed to the decoder";

        // Writes succeed (corruption is on get only); the read reads around.
        write::write_new_object(
            &meta,
            &chunks,
            0,
            "obj",
            1,
            payload,
            CHUNK,
            RS,
            0,
            LEASE_EXPIRY,
            ids_from(1),
        )
        .await
        .unwrap();

        let bytes = read::read_path(&meta, &chunks, 0, "obj").await.unwrap();
        assert_eq!(
            bytes.as_deref(),
            Some(&payload[..]),
            "read must succeed by reading around corrupt fragments"
        );
    })
    .await;
}

/// Property 4 — fail-closed partial write. A partitioned D server makes one
/// fan-out put hang; the write times out and aborts **before commit**, so the
/// object never exists and only leased garbage remains (reclaimed by the sweep).
#[madsim::test]
async fn partial_fanout_fails_closed() {
    let cluster = Cluster::start([StoreFault::None; N]).await;
    let endpoints = cluster.endpoints.clone();
    let server_ids = cluster.server_ids.clone();

    // Seed-reproducible choice of the single D server to partition mid-write.
    let mut rng = rand_seed();
    let victim = server_ids[(rng_u64(&mut rng) as usize) % N];

    on_client(&cluster, move || async move {
        let meta = RedbMetadataStore::in_memory().expect("redb");
        let mut clients = Vec::new();
        for e in &endpoints {
            clients.push(GrpcChunkStore::connect(e.clone()).await.expect("connect"));
        }
        let chunks = FanoutChunkStore::new(clients);
        let payload = b"never a silent half-write";

        // Partition one D server, then attempt the fan-out write under a deadline.
        NetSim::current().clog_node(victim);

        let plan = write::plan_write(payload, CHUNK, RS, ids_from(1)).unwrap();
        write::intent(&meta, &plan, LEASE_EXPIRY).await.unwrap();

        let result = timeout(
            Duration::from_secs(5),
            write::write_fragments(&chunks, &plan),
        )
        .await;
        let aborted = match result {
            Err(_elapsed) => true,             // the partitioned put never returned
            Ok(Err(_transport_error)) => true, // or surfaced a transport error
            Ok(Ok(())) => false,
        };
        assert!(
            aborted,
            "a partial fan-out must not complete — the write fails closed"
        );

        // The protocol aborted *before* commit: the object does not exist.
        assert!(
            read::read_object(&meta, &chunks, 1)
                .await
                .unwrap()
                .is_none(),
            "a failed-closed write must never produce a committed chunk"
        );

        // What landed is harmless leased garbage the pending-ledger sweep reclaims.
        let reclaimed = write::sweep_expired_leases(&meta, LEASE_EXPIRY + 1)
            .await
            .unwrap();
        assert!(
            !reclaimed.is_empty(),
            "the aborted write must leave leased garbage to reclaim"
        );
    })
    .await;
}

/// Property 5 — the M0/M1 commit suite, re-run over the gRPC `ChunkStore`.
/// Concurrent writers each fan their fragments out over the network, then race
/// the metadata commit; the version compare-and-set still admits exactly one
/// winner. The commit point is unchanged — proving the trait seam is real.
#[madsim::test]
async fn exactly_one_concurrent_writer_wins_over_network() {
    let cluster = Cluster::start([StoreFault::None; N]).await;
    let endpoints = cluster.endpoints.clone();

    on_client(&cluster, move || async move {
        let meta = Arc::new(RedbMetadataStore::in_memory().expect("redb"));
        let mut clients = Vec::new();
        for e in &endpoints {
            clients.push(GrpcChunkStore::connect(e.clone()).await.expect("connect"));
        }
        let chunks = Arc::new(FanoutChunkStore::new(clients));

        // An existing object at version 1, written over the network.
        let v0 = write::plan_write(b"v0", 4, RS, ids_from(1)).unwrap();
        write::intent(&*meta, &v0, LEASE_EXPIRY).await.unwrap();
        write::write_fragments(&*chunks, &v0).await.unwrap();
        write::commit_create(&*meta, 0, "obj", 1, &v0)
            .await
            .unwrap();
        write::release(&*meta, &v0).await.unwrap();
        let prior = read::read_inode(&*meta, 1).await.unwrap().unwrap();

        // Four writers stage independently over gRPC, then race to commit; madsim
        // schedules their interleaving from the seed.
        let mut handles = Vec::new();
        for i in 0..4u128 {
            let meta = Arc::clone(&meta);
            let chunks = Arc::clone(&chunks);
            let prior = prior.clone();
            handles.push(madsim::task::spawn(async move {
                let plan =
                    write::plan_write(b"contended", 4, RS, ids_from(0x1000 * (i + 1))).unwrap();
                write::intent(&*meta, &plan, LEASE_EXPIRY).await.unwrap();
                write::write_fragments(&*chunks, &plan).await.unwrap();
                let outcome = write::commit_overwrite(&*meta, 1, &prior, &plan)
                    .await
                    .unwrap();
                if outcome == wyrd_traits::CommitOutcome::Committed {
                    write::release(&*meta, &plan).await.unwrap();
                }
                outcome
            }));
        }

        let mut winners = 0;
        for handle in handles {
            if handle.await.unwrap() == wyrd_traits::CommitOutcome::Committed {
                winners += 1;
            }
        }

        assert_eq!(
            winners, 1,
            "exactly one concurrent writer must win the commit"
        );
        let after = read::read_inode(&*meta, 1).await.unwrap().unwrap();
        assert_eq!(
            after.version,
            prior.version + 1,
            "version bumped exactly once"
        );

        let bytes = read::read_path(&*meta, &*chunks, 0, "obj").await.unwrap();
        assert_eq!(bytes.as_deref(), Some(&b"contended"[..]));
    })
    .await;
}

/// A seed-derived RNG for the per-test fault selection, drawn from madsim's
/// seeded global RNG so the whole campaign — *which* links are faulted included —
/// reproduces from the run seed (ADR-0009).
fn rand_seed() -> rand_chacha::ChaCha8Rng {
    use rand::SeedableRng;
    rand_chacha::ChaCha8Rng::seed_from_u64(madsim::runtime::Handle::current().seed())
}

/// One `u64` from a `ChaCha8Rng`, without pulling the `rand::Rng` trait into
/// scope at every call site.
fn rng_u64(rng: &mut rand_chacha::ChaCha8Rng) -> u64 {
    use rand::RngCore;
    rng.next_u64()
}
