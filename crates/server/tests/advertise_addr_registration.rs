//! Issue #458: a D server bound to a wildcard/loopback address but given a
//! distinct **advertise** address registers *that* advertised endpoint into L5
//! coordination — not the bound (`0.0.0.0`/ephemeral) address. Closes the
//! deferral `DServer::bind`'s doc comment carried ("NAT / split-horizon
//! advertisement is a later deployment concern", pre-#458 `dserver.rs:182`): a
//! server behind NAT or in a container can now publish a routable endpoint
//! distinct from its listen socket (docs/design/proposals/0005 §"The placement
//! record").
//!
//! Mirrors `failure_domain_registration.rs`'s bind→register→discover→decode
//! harness over an in-process `MemCoordination` — no etcd, no Docker.

use std::time::Duration;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_server::dserver::{DServer, DServerRegistration, DSERVER_GROUP};
use wyrd_traits::Coordination;

/// A D server bound on `127.0.0.1:0` but given `--advertise-addr dserver-x:50051`
/// registers `http://dserver-x:50051` for discovery — NOT the bound loopback
/// address `DServer::bind` derived.
#[tokio::test]
async fn advertise_addr_overrides_the_registered_endpoint() {
    let coord = MemCoordination::new();
    let ttl = Duration::from_secs(60);
    let dir = tempfile::tempdir().unwrap();

    let store = FsChunkStore::open(dir.path().join("frags")).unwrap();
    let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap()
        .with_identity(1, "fd0")
        .with_advertise_addr("dserver-x:50051");
    server.register(&coord, DSERVER_GROUP, ttl).await.unwrap();

    let raw = coord.discover(DSERVER_GROUP).await.unwrap();
    assert_eq!(raw.len(), 1);
    let decoded = DServerRegistration::decode(&raw[0]).unwrap();
    assert_eq!(
        decoded.endpoint, "http://dserver-x:50051",
        "discovery must decode the ADVERTISED endpoint, not the bound loopback address"
    );
}

/// With no advertise address set, the registered endpoint remains the
/// bound-address value exactly as today (loopback behaviour preserved).
#[tokio::test]
async fn no_advertise_addr_keeps_the_bound_address_endpoint() {
    let coord = MemCoordination::new();
    let ttl = Duration::from_secs(60);
    let dir = tempfile::tempdir().unwrap();

    let store = FsChunkStore::open(dir.path().join("frags")).unwrap();
    let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap()
        .with_identity(2, "fd1");
    let bound_endpoint = server.endpoint().to_string();
    assert!(bound_endpoint.starts_with("http://127.0.0.1:"));
    server.register(&coord, DSERVER_GROUP, ttl).await.unwrap();

    let raw = coord.discover(DSERVER_GROUP).await.unwrap();
    assert_eq!(raw.len(), 1);
    let decoded = DServerRegistration::decode(&raw[0]).unwrap();
    assert_eq!(
        decoded.endpoint, bound_endpoint,
        "unset --advertise-addr must preserve today's bound-address registration"
    );
}
