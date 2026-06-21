//! M3.3 (issue #141, proposal 0005 §"Failure-domain-aware placement",
//! `0005:235-245`; PR-sequence slice-1 leftover absorbed into Option B,
//! `0005:510-513`): a D server's registration carries an **opaque failure-domain
//! label**, and the write path builds a [`Topology`] from discovery that the
//! selector spreads a chunk's fragments across.
//!
//! This is the **production input surface** that retires the domain-blind `index % n`
//! route: discovery yields `{ id, endpoint, failure-domain label }` per D server, the
//! topology composes them, and the selector then places `n` fragments across `n`
//! distinct domains.

use std::time::Duration;

use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::placement::select_distinct_domains;
use wyrd_server::dserver::{
    discover_endpoints, discover_topology, DServer, DServerRegistration, DSERVER_GROUP,
};
use wyrd_traits::Coordination;

/// A D server registers `{ id, endpoint, failure-domain label }`; discovery decodes
/// the label, the topology composes the fleet, and the selector spreads a chunk's
/// fragments across the distinct domains.
#[tokio::test]
async fn registration_carries_label_and_topology_spreads_distinct_domains() {
    let coord = MemCoordination::new();
    let ttl = Duration::from_secs(60);

    // Three D servers in three distinct failure domains (rack-a / rack-b / rack-c).
    let dir = tempfile::tempdir().unwrap();
    let labels = [(10u64, "rack-a"), (20, "rack-b"), (30, "rack-c")];
    let mut servers = Vec::new();
    for (id, domain) in labels {
        let store = FsChunkStore::open(dir.path().join(format!("frags-{id}"))).unwrap();
        let server = DServer::bind(store, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap()
            .with_identity(id, domain);
        server.register(&coord, DSERVER_GROUP, ttl).await.unwrap();
        servers.push(server);
    }

    // The registration record round-trips the opaque label through coordination.
    let raw = coord.discover(DSERVER_GROUP).await.unwrap();
    let mut decoded: Vec<DServerRegistration> = raw
        .iter()
        .map(|b| DServerRegistration::decode(b).unwrap())
        .collect();
    decoded.sort_by_key(|r| r.id);
    assert_eq!(decoded[0].failure_domain, "rack-a");
    assert_eq!(decoded[1].failure_domain, "rack-b");
    assert_eq!(decoded[2].failure_domain, "rack-c");

    // Discovery still yields the bare endpoints (back-compatible decode).
    assert_eq!(
        discover_endpoints(&coord, DSERVER_GROUP)
            .await
            .unwrap()
            .len(),
        3
    );

    // The topology built from discovery offers three distinct domains, so a 3-wide
    // distinct-domain placement is selectable; the chosen ids are the registered ones.
    let topo = discover_topology(&coord, DSERVER_GROUP).await.unwrap();
    let placement = select_distinct_domains(&topo, 3).unwrap();
    assert_eq!(placement.len(), 3);
    let mut chosen = placement.clone();
    chosen.sort();
    assert_eq!(
        chosen,
        vec![10, 20, 30],
        "one fragment per registered D server"
    );

    // A 4-wide placement is refused — only three domains exist.
    assert!(
        select_distinct_domains(&topo, 4).is_err(),
        "the selector refuses when domains < n"
    );
}
