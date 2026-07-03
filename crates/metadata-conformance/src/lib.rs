//! The **shared** `MetadataStore` trait-contract suite.
//!
//! These assertions are written against the [`MetadataStore`] *trait* surface
//! (never a concrete backend, ADR-0016), so **one** suite pins the contract for
//! **every** implementation instead of each backend forking its own copy — the
//! discipline that "a trait's semantics are pinned by two implementations"
//! (ADR-0006; proposal 0007 §"DST and tests"). They were lifted verbatim out of
//! `crates/metadata-redb/tests/conformance.rs`, whose own header noted they
//! "lift to a shared suite when a second backend (TiKV) arrives" — that arrival
//! is M4.1.
//!
//! Each function takes `&impl MetadataStore` and asserts one contract clause. A
//! backend's test target supplies a **fresh, empty store per function** (so the
//! functions never collide on keys) and drives them under whatever executor that
//! backend needs — `pollster::block_on` for the synchronous redb store, a
//! `tokio` runtime for the networked TiKV store.

#![forbid(unsafe_code)]

use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

/// `commit` lands every put atomically and `get` reads them back; a missing key
/// reads as `None`.
pub async fn contract_commit_and_get(store: &impl MetadataStore) {
    let outcome = store
        .commit(
            WriteBatch::new()
                .put(b"a".to_vec(), "1")
                .put(b"b".to_vec(), "2"),
        )
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);
    assert_eq!(store.get(b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(store.get(b"b").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(store.get(b"missing").await.unwrap(), None);
}

/// `scan(prefix)` returns exactly the pairs whose key begins with `prefix`
/// (order is unspecified, so the caller sorts before asserting).
pub async fn contract_scan_by_prefix(store: &impl MetadataStore) {
    store
        .commit(
            WriteBatch::new()
                .put(b"p:1".to_vec(), "x")
                .put(b"p:2".to_vec(), "y")
                .put(b"q:1".to_vec(), "z"),
        )
        .await
        .unwrap();
    let mut hits = store.scan(b"p:").await.unwrap();
    hits.sort();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, b"p:1");
    assert_eq!(hits[1].0, b"p:2");
}

/// A `require_absent` precondition rejects when the key exists, and the whole
/// batch is atomic — no side-effect put lands on the conflict path.
pub async fn contract_require_absent_gates(store: &impl MetadataStore) {
    store
        .commit(WriteBatch::new().put(b"k".to_vec(), "v"))
        .await
        .unwrap();
    // The key now exists, so require_absent must reject — and write nothing.
    let outcome = store
        .commit(
            WriteBatch::new()
                .require_absent(b"k".to_vec())
                .put(b"side".to_vec(), "effect"),
        )
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Conflict);
    assert_eq!(store.get(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(
        store.get(b"side").await.unwrap(),
        None,
        "batch must be atomic"
    );
}

/// A `require(key, value)` precondition is value-equality CAS: a stale expected
/// value conflicts and writes nothing; the fresh value commits.
pub async fn contract_require_value_gates(store: &impl MetadataStore) {
    store
        .commit(WriteBatch::new().put(b"k".to_vec(), "v"))
        .await
        .unwrap();
    let stale = store
        .commit(
            WriteBatch::new()
                .require(b"k".to_vec(), "WRONG")
                .put(b"k".to_vec(), "v2"),
        )
        .await
        .unwrap();
    assert_eq!(stale, CommitOutcome::Conflict);
    assert_eq!(store.get(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));

    let fresh = store
        .commit(
            WriteBatch::new()
                .require(b"k".to_vec(), "v")
                .put(b"k".to_vec(), "v2"),
        )
        .await
        .unwrap();
    assert_eq!(fresh, CommitOutcome::Committed);
    assert_eq!(store.get(b"k").await.unwrap().as_deref(), Some(&b"v2"[..]));
}
