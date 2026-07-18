//! Tier-2 integration test (Milestone 2 PR step 7, issue #117) — born at M2 per
//! proposal 0004 § "DST and integration tests (the heart of M2)" → "Tier-2 —
//! integration against real backends" and § "Benchmarks".
//!
//! Tiers 0/1 prove the data path in *simulation* (in-process tonic, madsim). This
//! test proves it against **reality**: it drives an end-to-end S3-style
//! write → read under `rs(6,3)` across **multiple real, networked gRPC D servers
//! running in containers**, asserting the read is byte-identical to the write.
//! What it exercises that no in-process test can — real tonic transport over real
//! HTTP/2 framing, real prost (de)serialization of the fragment-addressed wire
//! messages, real connection lifecycle and backpressure — is exactly what
//! "Tier-2 validates that the abstractions Tier-1 simulates match reality" means
//! (proposal 0004, ADR-0009).
//!
//! It is **gated**: the default `cargo test` (and the docs-skippable `cargo xtask
//! ci` lane) never needs Docker, so the test is `#[ignore]`d and additionally
//! no-ops when its endpoint list is absent. It is run by the heavier-runner
//! `cargo xtask integration` job, which stands up the container D servers, exports
//! their dialable endpoints in `WYRD_DSERVER_ENDPOINTS`, runs the test with
//! `--ignored`, and tears the containers down. The data path is unchanged from
//! steps 1–6; this is the wiring that makes the §10 Q6 throughput claim first
//! measurable on real hardware.

use std::time::Duration;

use wyrd_chunkstore_grpc::{FanoutChunkStore, GrpcChunkStore};
use wyrd_core::metadata::{EcScheme, InodeRecord, InodeState};
use wyrd_core::{read, write};
use wyrd_traits::{ChunkStore, Health};

/// The comma-separated list of dialable D-server endpoints (e.g.
/// `http://127.0.0.1:32768,http://127.0.0.1:32769,…`) that `cargo xtask
/// integration` exports after standing up the container cluster.
const ENDPOINTS_ENV: &str = "WYRD_DSERVER_ENDPOINTS";

/// Reed-Solomon RS(6,3) — the gateway default (`server::DEFAULT_DURABILITY`):
/// 6 data + 3 parity = 9 fragments per chunk, fanned out across the D servers.
const SCHEME: EcScheme = EcScheme::ReedSolomon { k: 6, m: 3 };

/// Dial a D server, retrying briefly so a just-launched container that has not
/// finished binding its listener is waited on rather than failing the test.
async fn connect(endpoint: &str) -> GrpcChunkStore {
    let mut last_err = None;
    for _ in 0..50 {
        match GrpcChunkStore::connect(endpoint.to_string()).await {
            Ok(client) => return client,
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    panic!("could not connect to D server `{endpoint}`: {last_err:?}");
}

/// A deterministic payload spanning several chunks, so each chunk fans its own
/// 9 fragments out across the cluster (not just one chunk's worth).
fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Tier-2: needs real containerized gRPC D servers — run via `cargo xtask integration`"]
async fn write_read_byte_identical_over_real_networked_dservers() {
    let raw = match std::env::var(ENDPOINTS_ENV) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "tier2_integration: {ENDPOINTS_ENV} unset — skipping. \
                 Run `cargo xtask integration` to stand up the container D servers."
            );
            return;
        }
    };
    let endpoints: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    assert!(
        endpoints.len() >= 2,
        "Tier-2 needs multiple D servers (got {}): {endpoints:?}",
        endpoints.len()
    );

    // One real gRPC client per containerized D server, composed into the M2
    // fan-out placement primitive: fragment index `i` lands on D server `i % n`,
    // so a chunk's 9 fragments prefer 9 distinct networked servers.
    let mut clients = Vec::with_capacity(endpoints.len());
    for endpoint in &endpoints {
        clients.push(connect(endpoint).await);
    }
    let store = FanoutChunkStore::new(clients);

    // Liveness over the wire: every D server answers Health.
    assert_eq!(
        store.health().await.expect("aggregate health over gRPC"),
        Health::Healthy,
        "every containerized D server must report Healthy"
    );

    // An S3-style object spanning several chunks under rs(6,3).
    let data = payload(40 * 1024 + 777);
    let chunk_size = 8 * 1024;
    let mut next: u128 = 0;
    let plan = write::plan_write(&data, chunk_size, SCHEME, || {
        let id = next;
        next += 1;
        id
    })
    .expect("plan the write");
    assert!(
        plan.chunks.len() >= 2,
        "the object should span multiple chunks"
    );

    // Phase 2 write — fan out every chunk's 9 fragments **in parallel to the real
    // networked D servers** over tonic.
    write::write_fragments(&store, &plan)
        .await
        .expect("fan-out write over real gRPC D servers");

    // Read back via the any-k-arrive-first path over the same real transport, and
    // assert byte-identical (the Tier-2 success criterion).
    let inode = InodeRecord {
        size: plan.size,
        chunk_map: plan.chunk_refs(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    let got = read::read_object_from(&store, &inode)
        .await
        .expect("any-k read over real gRPC D servers");

    assert_eq!(
        got, data,
        "object read back over real networked gRPC D servers must be byte-identical"
    );
}
