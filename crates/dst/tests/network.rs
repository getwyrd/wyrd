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

#![forbid(unsafe_code)]
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
use wyrd_chunk_format::{decode, encode, FragmentHeader};
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

    /// The simulated wall clock this D server judges deadlines against — madsim
    /// virtualises `SystemTime`, so this is the *simulator's* clock and stays
    /// seed-deterministic (ADR-0009; the wall-clock exemption the rubric grants exactly
    /// this case, #619). It is the same source the production `FsChunkStore` reads
    /// through its `SystemClock` when it runs under the simulator (property 6 below).
    fn now_millis() -> u64 {
        #[allow(clippy::disallowed_methods)]
        let now = std::time::SystemTime::now();
        now.duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[async_trait]
impl ChunkStore for DStore {
    /// This store's publication *is* the map insert, and it mirrors the production seam's
    /// **three-phase shape** exactly (the rubric's test-fidelity rule: a sim model must not
    /// be a stronger or weaker variant of the adapter it mirrors — issue #638 batch review).
    /// The publication lock is taken **first**, so the verdict is rendered at the publication
    /// point with nothing schedulable between judging and publishing; the insert follows;
    /// and the clock is read once more afterwards, so this store — like `FsChunkStore` — can
    /// never acknowledge a write it did not verify landed in window. A model that judged
    /// before contending for the lock, and never re-read, would be *weaker* than production
    /// and would keep acknowledging a write delayed past its deadline inside the store.
    ///
    /// It is deliberately **not** a stand-in for `FsChunkStore`'s enforcement: both the
    /// parked-write scenario (0016's failure-mode row) and the in-store elapsing are driven
    /// against the *real* `FsChunkStore` in properties 6 and 7 below, so nothing here
    /// substitutes for production enforcement.
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        // Verify integrity before acknowledging (the D-server contract); a
        // non-fragment is rejected, exactly as `FsChunkStore` would.
        decode(&fragment).map_err(|e| Box::new(e) as wyrd_traits::BoxError)?;
        let mut fragments = self.fragments.lock().unwrap();
        if let Some(deadline_millis) = deadline_millis {
            if let Some(refusal) = wyrd_traits::WriteDeadlineExpired::if_elapsed(
                id,
                deadline_millis,
                Self::now_millis(),
            ) {
                // Nothing was inserted, so the store is already in its pre-write state —
                // the `WriteEffect::NotApplied` postcondition, trivially, for a store whose
                // publication is a single map insert.
                return Err(Box::new(refusal));
            }
        }
        fragments.insert((id.chunk, id.index), fragment.to_vec());
        if let Some(deadline_millis) = deadline_millis {
            if let Some(unverified) = wyrd_traits::WriteDeadlineExpired::if_publication_unverified(
                id,
                deadline_millis,
                Self::now_millis(),
            ) {
                return Err(Box::new(unverified));
            }
        }
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

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self
            .fragments
            .lock()
            .unwrap()
            .keys()
            .map(|&(chunk, index)| FragmentId { chunk, index })
            .collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.fragments.lock().unwrap().remove(&(id.chunk, id.index));
        Ok(())
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
        write::commit_create(&meta, 0, "obj", 1, &plan, 0)
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
            || 0,
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
            || 0,
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
        write::commit_create(&*meta, 0, "obj", 1, &v0, 0)
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
                let outcome = write::commit_overwrite(&*meta, 1, &prior, &plan, 0)
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

/// A D server whose **accept queue** is modelled: it takes a `put_fragment`, holds it for
/// `park` of simulated time, and only then applies it — against the **real, production**
/// [`FsChunkStore`] behind it (issue #638; 0016's failure-mode row, `0016:1784`).
///
/// Two properties make it a faithful model rather than a convenient one:
///
/// * **The application outlives the request.** The parked write runs on a spawned task,
///   so dropping the request future — a client that reset the stream or gave up on its
///   own await — does **not** cancel it. That mirrors production exactly: the D server's
///   store hands its work to `spawn_blocking` (`crates/chunkstore-fs/src/lib.rs:216`),
///   and a cancelled tonic handler leaves that closure running on to its publish point.
///   A model that parked *inside* the request future could not reproduce 0016's scenario
///   at all — the caller "has long since timed out" there, so the write under test is
///   precisely one that is no longer attached to a caller.
/// * **It enforces nothing itself.** It only delays and delegates; every deadline
///   judgment below is made by the production `FsChunkStore`, reading the simulator's
///   clock through its own `SystemClock` (madsim virtualises `SystemTime`, ADR-0009).
struct ParkedFsDServer {
    inner: Arc<wyrd_chunkstore_fs::FsChunkStore>,
    park: Duration,
}

#[async_trait]
impl ChunkStore for ParkedFsDServer {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        deadline_millis: Option<u64>,
    ) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let park = self.park;
        // Detached on purpose (see the type's doc): the write is already accepted, and
        // from here on nothing the caller does can stop it landing — which is the whole
        // hazard `W_write` exists to bound.
        let applied = madsim::task::spawn(async move {
            sleep(park).await;
            inner.put_fragment(id, fragment, deadline_millis).await
        });
        // The store's own error object travels back unchanged (`BoxError` is `Send +
        // Sync`), so a refusal still reaches the service as the typed
        // `WriteDeadlineExpired` and is mapped to its own status — nothing here
        // re-classifies anything.
        applied.await.expect("the parked write task must not panic")
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        self.inner.get_fragment(id).await
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        self.inner.list_fragments().await
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.inner.delete_fragment(id).await
    }

    async fn health(&self) -> Result<Health> {
        self.inner.health().await
    }
}

/// Property 6 (issue #638) — 0016's own failure-mode row (`0016:1784`): "authorize a
/// fragment write, park it in the D server's accept queue past `W_write` (the caller has
/// long since timed out), fence and reap the session, and let the parked write proceed. The
/// D server MUST refuse it as past its deadline."
///
/// Staged exactly that way, and against the **real** `FsChunkStore` (the D server's
/// production store, reading the simulator's clock): the caller authorizes a write with
/// 200 ms of headroom, gives up on its await after 500 ms — so it is gone, learning
/// nothing, long before anything is applied — and the D server applies the parked write two
/// simulated seconds later. No caller-side bound can produce the outcome asserted here: the
/// caller has already stopped waiting, and its own timeout cannot reach a write the server
/// has accepted (the status quo 0016 rejects at `:1557-1564`), which would land after its
/// authorization elapsed — outcome (a), the unreferenced *and* unevidenced fragment.
///
/// The **control** is the same run's second write: identical park, identical abandoned
/// caller, no deadline. It lands. That is what makes the first write's absence attributable
/// to the deadline rather than to the caller's disappearance — without it, a D server that
/// simply dropped abandoned writes would pass.
#[madsim::test]
async fn a_write_parked_past_its_deadline_is_refused_by_the_real_d_server() {
    let handle = Handle::current();
    let ip: IpAddr = "10.0.0.2".parse().unwrap();
    let dserver = handle.create_node().name("d-parked").ip(ip).build();
    let addr = format!("{ip}:{PORT}").parse().unwrap();
    dserver.spawn(async move {
        // A real on-disk store, as a D server runs it; the tempdir is held for the
        // lifetime of the server task.
        let dir = tempfile::tempdir().expect("temp dir");
        let store = Arc::new(ParkedFsDServer {
            inner: Arc::new(wyrd_chunkstore_fs::FsChunkStore::open(dir.path()).expect("fs store")),
            park: Duration::from_secs(2),
        });
        Server::builder()
            .add_service(ChunkStoreServer::new(ChunkStoreService::from_arc(store)))
            .serve(addr)
            .await
            .expect("d-server serve");
    });

    let client_ip: IpAddr = "10.0.0.1".parse().unwrap();
    let client_node = handle.create_node().name("client").ip(client_ip).build();
    // Let the server bind before the client dials (deterministic in sim time).
    sleep(Duration::from_secs(1)).await;

    let endpoint = format!("http://{ip}:{PORT}");
    client_node
        .spawn(async move {
            // Bounded, fail-closed dial (the rubric's await discipline,
            // `AGENTS.md:181-183`) — generous enough that it can never be what produces
            // the refusal this property is about (it is 300× the deadline's headroom).
            let client = GrpcChunkStore::connect_with_timeout(endpoint, Duration::from_secs(60))
                .await
                .expect("connect");
            let parked = FragmentId {
                chunk: 0x0016_0638,
                index: 0,
            };
            let control = FragmentId {
                chunk: 0x0016_0639,
                index: 0,
            };
            let body = |id: FragmentId| {
                let mut header = FragmentHeader::new_v1(id.chunk, 4);
                header.ec_fragment_index = id.index;
                Bytes::from(encode(&header, b"park"))
            };

            // Authorized now with 200 ms of headroom: live when the D server accepts the
            // write, long elapsed by the time it applies it. madsim virtualises
            // `SystemTime`, so this reads the simulator's clock — the same one the D
            // server's `FsChunkStore` judges against (ADR-0009, #619).
            #[allow(clippy::disallowed_methods)]
            let now = std::time::SystemTime::now();
            let deadline_millis = now
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 200;

            // The caller gives up after 500 ms — a quarter of the park. From here it is
            // not waiting for anything, exactly as 0016 stages it.
            assert!(
                timeout(
                    Duration::from_millis(500),
                    client.put_fragment(parked, body(parked), Some(deadline_millis)),
                )
                .await
                .is_err(),
                "the caller must still be parked when it gives up — if the D server \
                 answered inside 500 ms the scenario did not happen"
            );
            assert!(
                timeout(
                    Duration::from_millis(500),
                    client.put_fragment(control, body(control), None),
                )
                .await
                .is_err(),
                "the control write is parked the same way and abandoned the same way"
            );

            // Let both parked writes proceed to their application point, then look at
            // what the D server actually holds. Bounded, fail-closed reads (the rubric's
            // await discipline, `AGENTS.md:181-183`).
            sleep(Duration::from_secs(4)).await;

            let got = timeout(Duration::from_secs(60), client.get_fragment(parked))
                .await
                .expect("the read-back must be answered")
                .unwrap();
            assert!(
                got.is_none(),
                "the parked write landed after its authorization deadline and MUST have \
                 been refused by the D server — a caller-side bound cannot produce this: \
                 the caller stopped waiting 1.5 s before the write was applied"
            );

            let control_got = timeout(Duration::from_secs(60), client.get_fragment(control))
                .await
                .expect("the read-back must be answered")
                .unwrap();
            assert_eq!(
                control_got.as_deref(),
                Some(body(control).as_ref()),
                "the control — same park, same abandoned caller, no deadline — must land, \
                 which is what makes the refusal above the deadline's doing rather than \
                 the caller's disappearance"
            );
        })
        .await
        .expect("client task");
}

/// A [`wyrd_testkit::Clock`] anchored to the **real** `FsChunkStore`'s own on-disk
/// progress (issue #638): it answers `live` until the fragment's bytes appear under the
/// chunk directory — as the store's private scratch (`<index>.<seq>.tmp`) or, for a build
/// that judged too late, as the published `<index>.frag` — and `late` from that moment on,
/// i.e. from the instant the data write completed and the store is at its publication
/// point.
///
/// Property 6 parks the write in the D server's **accept queue**, so the store sees an
/// already-expired deadline the moment it is handed the write. This clock parks the
/// elapsing one layer deeper — *inside* the store, between its data write and its
/// publishing rename — which is the interval a D server that judged the deadline only on
/// entry would sail straight through.
struct AtStorePublicationPoint {
    chunk_dir: std::path::PathBuf,
    live: u64,
    late: u64,
}

impl wyrd_testkit::Clock for AtStorePublicationPoint {
    fn now_millis(&self) -> u64 {
        let bytes_on_disk = std::fs::read_dir(&self.chunk_dir)
            .map(|entries| {
                entries.flatten().any(|entry| {
                    matches!(
                        entry.path().extension().and_then(|e| e.to_str()),
                        Some("tmp") | Some("frag")
                    )
                })
            })
            .unwrap_or(false);
        if bytes_on_disk {
            self.late
        } else {
            self.live
        }
    }
}

/// Property 7 (issue #638) — the enforcement point, one layer below property 6: a write
/// whose deadline elapses **inside the D server's store**, after its bytes are on disk and
/// before it is published, must still be refused, and must leave nothing behind.
///
/// Property 6 stages 0016's own failure-mode row (the accept queue). This stages the
/// interval that row cannot reach: the store has already taken the write, has already
/// written its bytes, and only then discovers it is out of window. A D server whose guard
/// sits on entry — the shape a "check it when you accept it" implementation naturally
/// takes — publishes this write, and the read-back below finds it.
///
/// Everything is real: madsim's simulated network, the generated tonic client and service,
/// and the production `FsChunkStore` behind them. Only the store's clock is injected, and
/// it decides nothing about the write — it reports what the store's own directory shows.
#[madsim::test]
async fn a_write_that_expires_inside_the_d_servers_store_is_refused_before_publication() {
    let handle = Handle::current();
    let ip: IpAddr = "10.0.0.4".parse().unwrap();
    let dserver = handle.create_node().name("d-publish-point").ip(ip).build();
    let addr = format!("{ip}:{PORT}").parse().unwrap();

    let expiring = FragmentId {
        chunk: 0x0016_0640,
        index: 0,
    };
    let control = FragmentId {
        chunk: 0x0016_0641,
        index: 0,
    };

    dserver.spawn(async move {
        let dir = tempfile::tempdir().expect("temp dir");
        // `live` (9_500) and `late` (10_500) straddle the 10_000 deadline the client
        // sends below. The chunk directory watched is the one the expiring write uses;
        // the control write has no deadline, so it never consults the clock at all.
        let clock = AtStorePublicationPoint {
            chunk_dir: dir.path().join(format!("{:032x}", expiring.chunk)),
            live: 9_500,
            late: 10_500,
        };
        let store = Arc::new(
            wyrd_chunkstore_fs::FsChunkStore::open_with_clock(dir.path(), clock).expect("fs store"),
        );
        Server::builder()
            .add_service(ChunkStoreServer::new(ChunkStoreService::from_arc(store)))
            .serve(addr)
            .await
            .expect("d-server serve");
    });

    let client_ip: IpAddr = "10.0.0.3".parse().unwrap();
    let client_node = handle.create_node().name("client").ip(client_ip).build();
    // Let the server bind before the client dials (deterministic in sim time).
    sleep(Duration::from_secs(1)).await;

    let endpoint = format!("http://{ip}:{PORT}");
    client_node
        .spawn(async move {
            // Bounded, fail-closed dial (the rubric's await discipline,
            // `AGENTS.md:181-183`); generous, so nothing client-side can be what produces
            // the refusal.
            let client = GrpcChunkStore::connect_with_timeout(endpoint, Duration::from_secs(60))
                .await
                .expect("connect");
            let body = |id: FragmentId| {
                let mut header = FragmentHeader::new_v1(id.chunk, 4);
                header.ec_fragment_index = id.index;
                Bytes::from(encode(&header, b"pubp"))
            };

            // Bounded, fail-closed awaits (the rubric's await discipline,
            // `AGENTS.md:181-183`) — and generous, so nothing client-side can be what
            // produces the refusal.
            let err = timeout(
                Duration::from_secs(60),
                client.put_fragment(expiring, body(expiring), Some(10_000)),
            )
            .await
            .expect("the D server must answer")
            .expect_err(
                "a write whose deadline elapses between the store's data write and its \
                 publication must be refused at the publication point",
            );
            assert!(
                wyrd_traits::is_write_deadline_expired(err.as_ref()),
                "the refusal must cross the wire as the typed deadline class: {err}"
            );
            assert!(
                timeout(Duration::from_secs(60), client.get_fragment(expiring))
                    .await
                    .expect("the read-back must be answered")
                    .unwrap()
                    .is_none(),
                "and nothing may be left stored — a store that judged the deadline only \
                 when it accepted the write would have published these bytes"
            );

            // The control: same server, same store, no deadline — it lands. Without it a
            // D server that refused everything would pass.
            timeout(
                Duration::from_secs(60),
                client.put_fragment(control, body(control), None),
            )
            .await
            .expect("the D server must answer")
            .expect("a deadline-less write must store exactly as before");
            assert_eq!(
                timeout(Duration::from_secs(60), client.get_fragment(control))
                    .await
                    .expect("the read-back must be answered")
                    .unwrap()
                    .as_deref(),
                Some(body(control).as_ref()),
                "the control write is unaffected"
            );
        })
        .await
        .expect("client task");
}

/// A [`wyrd_testkit::Clock`] that steps past the deadline exactly when the store's
/// **publication completes** — when `<index>.frag` appears (issue #638).
///
/// Where [`AtStorePublicationPoint`] elapses between the data write and the rename, this
/// one elapses *across the rename itself*: the interval no pre-publication check can cover,
/// because `rename(2)` admits no predicate, cannot be cancelled, and on a hung device
/// returns whenever it returns.
struct AtStorePublicationCompletion {
    fragment_path: std::path::PathBuf,
    live: u64,
    late: u64,
}

impl wyrd_testkit::Clock for AtStorePublicationCompletion {
    fn now_millis(&self) -> u64 {
        if self.fragment_path.exists() {
            self.late
        } else {
            self.live
        }
    }
}

/// Property 8 (issue #638) — **a D server never acknowledges a publication it could not
/// verify landed in window**, asserted end to end over the simulated network.
///
/// Properties 6 and 7 cover writes the D server *refuses*. This one covers the case it
/// cannot refuse, because it only finds out afterwards: the first clock reading it can take
/// once publication returns is already past the deadline. The distinction is what makes
/// `Ok(())` mean anything — a D server that acknowledges here is telling its caller the
/// fragment landed inside `W_write` when it never checked, which is the same "bounds
/// acceptance, not effect" defect 0016 rejects for caller-side timeouts (`:1557-1564`), one
/// layer down and now on the server's side of the wire.
///
/// Three things must survive the wire, and only an end-to-end run proves it: the store's
/// verdict, its **effect** (`Unknown` — the bytes may be there, so the client must not hear
/// the clean refusal's definite "nothing landed"), and its class (`Indeterminate`).
/// Everything here is production but the injected clock: madsim's network, the generated
/// tonic client and service, and the real `FsChunkStore`.
#[madsim::test]
async fn a_publication_the_d_server_could_not_verify_is_reported_not_acknowledged() {
    let handle = Handle::current();
    let ip: IpAddr = "10.0.0.6".parse().unwrap();
    let dserver = handle.create_node().name("d-late-publish").ip(ip).build();
    let addr = format!("{ip}:{PORT}").parse().unwrap();

    let straggler = FragmentId {
        chunk: 0x0016_0642,
        index: 0,
    };
    let control = FragmentId {
        chunk: 0x0016_0643,
        index: 0,
    };

    dserver.spawn(async move {
        let dir = tempfile::tempdir().expect("temp dir");
        // `live` (9_500) and `late` (10_500) straddle the 10_000 deadline the client sends;
        // the step happens when the straggler's `.frag` appears, i.e. across its rename.
        let clock = AtStorePublicationCompletion {
            fragment_path: wyrd_chunkstore_fs::fragment_path(dir.path(), straggler),
            live: 9_500,
            late: 10_500,
        };
        let store = Arc::new(
            wyrd_chunkstore_fs::FsChunkStore::open_with_clock(dir.path(), clock).expect("fs store"),
        );
        Server::builder()
            .add_service(ChunkStoreServer::new(ChunkStoreService::from_arc(store)))
            .serve(addr)
            .await
            .expect("d-server serve");
    });

    let client_ip: IpAddr = "10.0.0.5".parse().unwrap();
    let client_node = handle.create_node().name("client").ip(client_ip).build();
    // Let the server bind before the client dials (deterministic in sim time).
    sleep(Duration::from_secs(1)).await;

    let endpoint = format!("http://{ip}:{PORT}");
    client_node
        .spawn(async move {
            // Bounded, fail-closed dial (the rubric's await discipline,
            // `AGENTS.md:181-183`); generous, so nothing client-side produces the outcome.
            let client = GrpcChunkStore::connect_with_timeout(endpoint, Duration::from_secs(60))
                .await
                .expect("connect");
            let body = |id: FragmentId| {
                let mut header = FragmentHeader::new_v1(id.chunk, 4);
                header.ec_fragment_index = id.index;
                Bytes::from(encode(&header, b"late"))
            };

            let err = timeout(
                Duration::from_secs(60),
                client.put_fragment(straggler, body(straggler), Some(10_000)),
            )
            .await
            .expect("the D server must answer")
            .expect_err(
                "the D server could not verify the publication landed in window, so it \
                 must not acknowledge the write",
            );
            let outcome = wyrd_traits::write_deadline_outcome(err.as_ref())
                .expect("the outcome must cross the wire as the typed deadline class");
            assert_eq!(
                outcome.effect,
                wyrd_traits::WriteEffect::Unknown,
                "and with its effect intact: that D server may hold the bytes, so telling \
                 the caller 'nothing landed' would be the one wire lie that matters: {err}"
            );
            assert_eq!(
                wyrd_traits::classify(err.as_ref()),
                wyrd_traits::ErrorClass::Indeterminate,
                "durable state changed, so the class must not read as terminal: {err}"
            );

            // The report is true on the far side of the wire as well.
            assert_eq!(
                timeout(Duration::from_secs(60), client.get_fragment(straggler))
                    .await
                    .expect("the read-back must be answered")
                    .unwrap()
                    .as_deref(),
                Some(body(straggler).as_ref()),
                "the report is honest in both directions: `Unknown` does not claim the \
                 bytes are absent, and here they are in fact present"
            );

            // The control: same server, same store, no deadline — acknowledged.
            timeout(
                Duration::from_secs(60),
                client.put_fragment(control, body(control), None),
            )
            .await
            .expect("the D server must answer")
            .expect("a deadline-less write must store exactly as before");
        })
        .await
        .expect("client task");
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
    use rand::Rng;
    rng.next_u64()
}
