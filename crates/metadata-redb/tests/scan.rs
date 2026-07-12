//! `scan` completeness-or-fail-loud on the embedded backend (#516; #262, ADR-0011).
//!
//! The contract (`wyrd_traits::MetadataStore`, clause 5) says a `scan` returns the
//! complete matching set or `Err` — never a silently truncated `Vec`, because a short
//! `inode:` listing shrinks GC's never-reclaim safety set (data loss). The two
//! distributed backends enforced it with a shared `SCAN_CAP`; redb enforced *nothing*
//! and could materialize an unbounded `Vec` (#516). These pin the fix.
//!
//! Shaped after `crates/metadata-fdb/tests/scan.rs`, which lowers the store's cap with
//! `with_scan_cap` so the fail-loud arm is reachable without writing 2^20 keys — the
//! production `scan` path is driven either way, only the ceiling moves.

use pollster::block_on;
use wyrd_metadata_redb::{RedbMetadataStore, ScanCapExceeded, SCAN_CAP};
use wyrd_traits::{MetadataStore, WriteBatch};

const LOWERED_CAP: usize = 8;

fn store_with_cap(cap: usize) -> RedbMetadataStore {
    RedbMetadataStore::in_memory()
        .expect("in-memory redb")
        .with_scan_cap(cap)
}

/// Seed `n` keys under `p:`.
fn seed(store: &RedbMetadataStore, n: usize) {
    let mut batch = WriteBatch::new();
    for i in 0..n {
        batch = batch.put(format!("p:{i:04}").into_bytes(), format!("v{i}"));
    }
    assert!(block_on(store.commit(batch)).is_ok());
}

#[test]
fn a_scan_past_the_cap_fails_loud_and_returns_no_partial_results() {
    let store = store_with_cap(LOWERED_CAP);
    seed(&store, LOWERED_CAP + 1);

    let err = block_on(store.scan(b"p:"))
        .expect_err("a scan past the cap must fail loud, never return a truncated Vec (#262)");

    // Typed, and the SEAM type — so a caller classifies "too big, failed loud" the same
    // way whichever backend it holds (#516's whole point: this used to be a different
    // per-crate type on each backend, and absent entirely on redb).
    let cap_err = err
        .downcast_ref::<ScanCapExceeded>()
        .unwrap_or_else(|| panic!("an over-cap scan must be a typed ScanCapExceeded, got: {err}"));
    assert_eq!(cap_err.cap, LOWERED_CAP);
    assert_eq!(cap_err.prefix, b"p:".to_vec());
}

#[test]
fn a_scan_of_exactly_the_cap_is_a_legal_complete_result() {
    // `total > cap`, not `>=` — the boundary the other two backends already agreed on
    // (`metadata-tikv`'s `after_page`). Exactly `cap` keys is complete, not a breach.
    let store = store_with_cap(LOWERED_CAP);
    seed(&store, LOWERED_CAP);

    let hits = block_on(store.scan(b"p:")).expect("exactly `cap` keys is a complete result");
    assert_eq!(hits.len(), LOWERED_CAP);
}

#[test]
fn a_scan_under_the_cap_still_returns_the_complete_set() {
    // The cap must not change what an ordinary listing returns.
    let store = store_with_cap(LOWERED_CAP);
    seed(&store, LOWERED_CAP - 3);

    let hits = block_on(store.scan(b"p:")).expect("an under-cap scan succeeds");
    assert_eq!(hits.len(), LOWERED_CAP - 3);
}

#[test]
fn the_cap_cannot_be_raised() {
    // The cap is a correctness constraint (#262), not a knob a caller may loosen, so
    // `with_scan_cap` clamps to SCAN_CAP — exactly as FdbMetadataStore's does. Asserted
    // on the effective cap directly: proving the ceiling by writing 2^20 + 1 keys would be
    // absurd, but a test that merely scanned a handful of keys through an "unclamped"
    // store would pass whether or not the clamp existed, and would guard nothing.
    assert_eq!(
        store_with_cap(usize::MAX).scan_cap(),
        SCAN_CAP,
        "a cap above SCAN_CAP must clamp back to it"
    );
    // Lowering is allowed — that is what makes the fail-loud arm testable at all.
    assert_eq!(store_with_cap(LOWERED_CAP).scan_cap(), LOWERED_CAP);
    // And the default is the shared seam cap, not something redb-specific.
    assert_eq!(
        RedbMetadataStore::in_memory()
            .expect("in-memory redb")
            .scan_cap(),
        SCAN_CAP
    );
}
