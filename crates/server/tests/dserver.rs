//! M2.3 definition of done (issue #113): the `d-server` role hosts an injected
//! `ChunkStore` over gRPC and registers its endpoint for discovery through the
//! L5 `Coordination` seam.
//!
//! Two tiers, matching how the rest of the system is proven:
//! - a **deterministic** lease test (a `ManualClock`-driven coordinator) shows a
//!   registration renews and a lapsed one drops out of discovery — no wall-clock;
//! - an **in-process integration** test stands up two real D servers over
//!   loopback gRPC, discovers them, resolves the fan-out endpoint set, and proves
//!   a *discovered* endpoint actually serves a fragment round-trip.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_chunkstore_grpc::GrpcChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_server::dserver::{discover_endpoints, select_fanout, DServer, DSERVER_GROUP};
use wyrd_testkit::ManualClock;
use wyrd_traits::{ChunkId, ChunkStore, Coordination, FragmentId};

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// A valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

fn fs_store() -> (FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    (store, dir)
}

/// DoD: leased registration renews; an expired (stale) registration drops out of
/// discovery. Driven by a `ManualClock` so the lifecycle is deterministic — no
/// real time, no flakiness.
#[tokio::test]
async fn lease_renews_and_lapses_deterministically() {
    let clock = ManualClock::new(0);
    let coord = MemCoordination::with_clock(clock.clone());
    let (store, _dir) = fs_store();

    let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let ttl = Duration::from_secs(30);
    let lease = server.register(&coord, DSERVER_GROUP, ttl).await.unwrap();

    // Registered now → discoverable.
    assert_eq!(
        discover_endpoints(&coord, DSERVER_GROUP).await.unwrap(),
        vec![server.endpoint().to_string()],
    );

    // Renew before expiry (at t=20s), then advance to t=40s: re-stamped to expire
    // at t=50s, so still discoverable.
    clock.advance(20_000);
    coord.renew(lease).await.unwrap();
    clock.advance(20_000);
    assert_eq!(
        discover_endpoints(&coord, DSERVER_GROUP)
            .await
            .unwrap()
            .len(),
        1,
        "a renewed registration stays discoverable past its original TTL",
    );

    // Advance past the renewed expiry without renewing → drops out.
    clock.advance(20_000);
    assert!(
        discover_endpoints(&coord, DSERVER_GROUP)
            .await
            .unwrap()
            .is_empty(),
        "a lapsed registration drops out of discovery",
    );
}

/// DoD: `wyrd d-server` hosts `FsChunkStore` over gRPC; a registered D server is
/// discovered via `Coordination::discover`, and the gateway resolves the set of
/// endpoints a chunk's `n` fragments fan out to. Proven in-process over real
/// loopback gRPC with two D servers sharing one coordinator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d_servers_register_serve_and_are_discovered() {
    let coord = Arc::new(MemCoordination::new());
    let ttl = Duration::from_secs(3600);
    let renew = Duration::from_secs(1);

    // Two D servers, each over its own filesystem store, bound to ephemeral ports.
    let (store0, _d0) = fs_store();
    let (store1, _d1) = fs_store();
    let s0 = DServer::bind(store0, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let s1 = DServer::bind(store1, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    // Register before serving, so discovery is race-free.
    let l0 = s0.register(&*coord, DSERVER_GROUP, ttl).await.unwrap();
    let l1 = s1.register(&*coord, DSERVER_GROUP, ttl).await.unwrap();

    // Discovery returns both distinct endpoints.
    let mut endpoints = discover_endpoints(&*coord, DSERVER_GROUP).await.unwrap();
    endpoints.sort();
    assert_eq!(endpoints.len(), 2, "both D servers are discovered");
    assert_ne!(endpoints[0], endpoints[1], "endpoints are distinct");

    // The gateway resolves the endpoint set for an rs(6,3) chunk's 9 fragments —
    // best-effort distinct, cycling over the two known D servers.
    let fanout = select_fanout(&endpoints, 9);
    assert_eq!(fanout.len(), 9, "one endpoint chosen per fragment");
    assert!(
        fanout.iter().all(|e| endpoints.contains(e)),
        "every chosen endpoint is a discovered one",
    );
    assert!(
        endpoints.iter().all(|e| fanout.contains(e)),
        "fan-out spreads across the distinct D servers",
    );

    // Start serving both.
    let (tx0, rx0) = oneshot::channel();
    let (tx1, rx1) = oneshot::channel();
    let h0 = tokio::spawn(s0.serve(coord.clone(), l0, renew, async move {
        let _ = rx0.await;
    }));
    let h1 = tokio::spawn(s1.serve(coord.clone(), l1, renew, async move {
        let _ = rx1.await;
    }));

    // A client dialing a *discovered* endpoint round-trips a fragment.
    let client = GrpcChunkStore::connect(endpoints[0].clone()).await.unwrap();
    let id = fid(0xabc_def, 4);
    let frag = fragment(id, b"a fragment to a discovered D server");
    client.put_fragment(id, frag.clone()).await.unwrap();
    assert_eq!(
        client.get_fragment(id).await.unwrap().as_deref(),
        Some(frag.as_ref()),
        "the discovered endpoint serves the fragment byte-identical",
    );

    // Clean shutdown revokes the leases, so discovery converges to empty.
    tx0.send(()).unwrap();
    tx1.send(()).unwrap();
    h0.await.unwrap().unwrap();
    h1.await.unwrap().unwrap();
    assert!(
        discover_endpoints(&*coord, DSERVER_GROUP)
            .await
            .unwrap()
            .is_empty(),
        "a cleanly stopped D server withdraws its registration",
    );
}
