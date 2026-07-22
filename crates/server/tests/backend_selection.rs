//! M4.4 — backend-selection composition regression (proposal 0015 slice 4,
//! §"Composition, not refactor"; issue #255). Two load-bearing assertions that
//! pin this slice's Check-verifiable end result — "the parameterized `server`
//! compiles and its redb path passes a red→green regression":
//!
//! (a) the **redb** backend is selected by config, and a roundtrip succeeds
//!     through the now-generic `cli.rs` helper (`alloc_inode`) on that backend;
//! (b) `alloc_inode` against a `MetadataStore` that always returns `Conflict`
//!     returns a **bounded** error rather than the old unbounded `loop` spin
//!     (`cli.rs` pre-fix `alloc_inode`).
//!
//! The mock store below is only constructible because `alloc_inode` is now
//! generic over `M: MetadataStore` — so this test also load-bears the
//! parameterization seam: revert the production change and it no longer compiles
//! (the C4-verify red), fix it and both assertions pass (green).

#![forbid(unsafe_code)]

use std::time::Duration;

use bytes::Bytes;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::cli::{alloc_inode, CoordinationBackend, MetadataBackend};
use wyrd_traits::{CommitOutcome, MetadataStore, Result, WriteBatch};

/// #365 — the L5 `Coordination` backend is selected by config exactly like the
/// metadata backend (ADR-0006, ADR-0008/0016): the in-memory dev default with no
/// config, and an unknown name is a config error. `etcd` is only *namable* in a
/// build with `--features etcd` (in the default Check build it is rejected with a
/// build-hint), so selecting it is a composition swap, not a caller edit.
#[test]
fn coordination_backend_selects_by_config() {
    assert_eq!(
        CoordinationBackend::from_config(None).unwrap(),
        CoordinationBackend::Mem,
        "no config selects the in-memory dev default"
    );
    assert_eq!(
        CoordinationBackend::from_config(Some("mem")).unwrap(),
        CoordinationBackend::Mem,
    );
    assert!(
        CoordinationBackend::from_config(Some("nonsense")).is_err(),
        "an unknown coordination backend name is a config error"
    );
    // `etcd` is accepted only under `--features etcd`; without it, it is rejected
    // with a build hint rather than silently falling back (an invalid selection must
    // fail loud). Under the feature the same value selects the etcd concrete.
    #[cfg(not(feature = "etcd"))]
    assert!(
        CoordinationBackend::from_config(Some("etcd")).is_err(),
        "etcd requires --features etcd; unavailable builds reject it, not fall back"
    );
    #[cfg(feature = "etcd")]
    assert_eq!(
        CoordinationBackend::from_config(Some("etcd")).unwrap(),
        CoordinationBackend::Etcd,
    );
}

/// (a) redb is the config default (and the explicit `redb` value), an unknown
/// name is rejected, and the generic `alloc_inode` drives the redb backend:
/// consecutive allocations are monotonic and persisted through its get+commit.
#[tokio::test]
async fn redb_backend_selected_by_config_and_alloc_inode_roundtrips() {
    assert_eq!(
        MetadataBackend::from_config(None).unwrap(),
        MetadataBackend::Redb,
        "no config selects the redb dev default (ADR-0014)"
    );
    assert_eq!(
        MetadataBackend::from_config(Some("redb")).unwrap(),
        MetadataBackend::Redb,
    );
    assert!(
        MetadataBackend::from_config(Some("nonsense")).is_err(),
        "an unknown backend name is a config error"
    );

    let meta = RedbMetadataStore::in_memory().expect("in-memory redb");
    let first = alloc_inode(&meta).await.expect("first inode");
    let second = alloc_inode(&meta).await.expect("second inode");
    assert_eq!(
        (first, second),
        (1, 2),
        "the generic helper allocates monotonic, persisted inodes on the redb backend"
    );
}

/// A `MetadataStore` whose every `commit` is a `Conflict` — no allocation can
/// ever land. Constructible only because the helper is generic over `M`.
struct AlwaysConflict;

#[async_trait::async_trait]
impl MetadataStore for AlwaysConflict {
    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn scan(&self, _prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(Vec::new())
    }

    async fn commit(&self, _batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(CommitOutcome::Conflict)
    }
}

/// (b) Against a perpetual-`Conflict` store the old `loop` spun forever; the
/// budget-bounded retry-with-backoff must instead return an `Err` once the
/// wall-clock budget is spent. Runs on tokio's virtual clock (`start_paused`) so the
/// backoff sleeps auto-advance instantly — fast even with a multi-second budget. The
/// outer `timeout` is a safety net: a regression to an *unbounded* backoff spin trips
/// the (virtual) 5s timeout and fails loudly rather than hanging the whole suite.
#[tokio::test(start_paused = true)]
async fn alloc_inode_is_bounded_against_a_perpetual_conflict_store() {
    let store = AlwaysConflict;
    let outcome = tokio::time::timeout(Duration::from_secs(5), alloc_inode(&store)).await;
    let result = outcome.expect("alloc_inode hung: unbounded Conflict spin (the pre-fix bug)");
    assert!(
        result.is_err(),
        "a perpetual Conflict must surface a bounded exhaustion error, not a value"
    );
}
