//! `scan_page` on the embedded backend: the **cap escape** and the **page bound**
//! (#634; proposal 0016 "What the implementing slices change", `0016:2645-2672`).
//!
//! Two things this file exists to prove, on a real backend rather than a double:
//!
//! 1. **The primitive escapes the bound it exists to escape.** `scan` is
//!    complete-or-fail-loud (`wyrd_traits::MetadataStore` clause 5; #262, ADR-0011),
//!    so a namespace past the cap cannot be read by it at *any* size — and today two
//!    multipart-era populations cross it (the unbounded `retire:` drain; GC's
//!    `orphan:` ledger, ~1.78 M marks from one maximum segmented-object retirement
//!    against a cap of 1,048,576). Both halves are asserted **in the same test**: the
//!    `scan` that fails loud, and the `scan_page` walk of the *same* store that
//!    returns every key exactly once. This is the leg that fails against any
//!    `scan()`-backed `scan_page`, which would inherit the cap and report the same
//!    fail-loud error.
//! 2. **`limit` is a page bound and non-progress is impossible**: a page never
//!    exceeds `min(limit, cap)`, a limit above the cap is *clamped* rather than
//!    refused, and a page bound that resolves to **zero** — `limit == 0`, or a store
//!    configured `with_scan_cap(0)` — is refused with the seam's typed
//!    `ZeroPageLimit` rather than answered. An empty page would be either a false
//!    "exhausted" or a non-terminal answer with no progress; a page returned in
//!    spite of the bound is unbounded, which is the failure iteration 1 shipped and
//!    `a_store_whose_cap_is_zero_refuses_every_page_and_never_reads_unbounded` pins.
//! 3. **A cursor from outside the prefix has a page, never an error.** Below the
//!    prefix it widens to the prefix's own start (never an empty terminal page for a
//!    prefix that is not exhausted); at or past the prefix's exclusive upper bound it
//!    is an empty *terminal* page (never an inverted range read, which is how the
//!    same input fails on the distributed backends).
//!
//! The cap is lowered with `with_scan_cap`, the established idiom of
//! `crates/metadata-redb/tests/scan.rs:9-11,75-89`: the *production* paths are driven
//! either way, only the ceiling moves — proving the ceiling by writing 2^20 keys
//! would be absurd.
//!
//! The shared contract clauses themselves are **not** restated here; they are called
//! directly from `wyrd-metadata-conformance`, so this target exercises the normative
//! semantics (order, exclusive cursor, termination, no-skip) against redb as well as
//! the backend-specific legs above.

#![forbid(unsafe_code)]

use bytes::Bytes;
use pollster::block_on;
use wyrd_metadata_conformance as conformance;
use wyrd_metadata_redb::{RedbMetadataStore, ScanCapExceeded, ZeroPageLimit, SCAN_CAP};
use wyrd_traits::{MetadataStore, WriteBatch};

/// Small on purpose: the production `scan`/`scan_page` code paths are identical at
/// any ceiling, and 2^20 keys per test run is not a test.
const LOWERED_CAP: usize = 8;

fn store() -> RedbMetadataStore {
    RedbMetadataStore::in_memory().expect("in-memory redb")
}

fn capped_store() -> RedbMetadataStore {
    store().with_scan_cap(LOWERED_CAP)
}

/// Seed `n` keys under `p:`, each with its own value, returning the pairs in byte
/// order. Distinct values because a walk owes its caller the bytes it committed:
/// the `retire:` drain and GC's `orphan:` ledger both DECODE what they read, so a
/// page of correct keys carrying stale or empty values is as unusable as a skip.
fn seed(store: &RedbMetadataStore, n: usize) -> Vec<(Vec<u8>, Bytes)> {
    let mut batch = WriteBatch::new();
    let mut pairs = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("p:{i:06}").into_bytes();
        let value = Bytes::from(format!("v{i}"));
        batch = batch.put(key.clone(), value.clone());
        pairs.push((key, value));
    }
    assert!(block_on(store.commit(batch)).is_ok());
    pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
    pairs
}

/// Walk the whole prefix with `limit`-sized pages, asserting termination.
fn walk(store: &RedbMetadataStore, prefix: &[u8], limit: usize) -> Vec<(Vec<u8>, Bytes)> {
    let mut pairs: Vec<(Vec<u8>, Bytes)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    for lap in 1..=64 {
        let (items, next) = block_on(store.scan_page(prefix, after.as_deref(), limit))
            .expect("a page of a walk must not fail");
        pairs.extend(items);
        match next {
            Some(cursor) => after = Some(cursor),
            None => return pairs,
        }
        assert!(lap < 64, "the walk did not terminate");
    }
    unreachable!("the lap budget above returns or panics")
}

// ---- Leg B: the cap escape, both halves in one test ------------------------

#[test]
fn a_population_scan_refuses_whole_is_still_walkable_page_by_page() {
    // `cap × k + r`: past the cap, and deliberately not a multiple of it, so the
    // walk has to handle the ragged final page as well as the full ones.
    let store = capped_store();
    let expected = seed(&store, LOWERED_CAP * 3 + 1);

    // Half 1 — `scan` cannot read this population at all. It fails loud with the
    // SEAM's typed error, returning no partial `Vec` (#262, ADR-0011).
    let err = block_on(store.scan(b"p:"))
        .expect_err("a scan past the cap must fail loud, never truncate (#262)");
    let cap_err = err
        .downcast_ref::<ScanCapExceeded>()
        .unwrap_or_else(|| panic!("an over-cap scan must be a typed ScanCapExceeded, got: {err}"));
    assert_eq!(cap_err.cap, LOWERED_CAP);

    // Half 2 — the SAME store, walked page by page, returns every key exactly once
    // and in byte order. A `scan_page` implemented over `scan` fails right here: it
    // inherits the cap and raises the error half 1 just observed.
    assert_eq!(
        walk(&store, b"p:", LOWERED_CAP),
        expected,
        "a population `scan` refuses whole must still be enumerable page by page, \
         every key exactly once and carrying the value it was committed with — that \
         is the entire reason this primitive exists (0016:2645-2652)"
    );
}

#[test]
fn every_page_of_an_over_cap_walk_stays_inside_the_cap() {
    // Even for a caller asking for far more than the cap: the page is bounded by
    // BOTH the caller's limit and the store's own ceiling, since no page may exceed
    // `SCAN_CAP` (0016:2647-2650).
    let store = capped_store();
    let expected = seed(&store, LOWERED_CAP * 3 + 1);

    let mut pairs: Vec<(Vec<u8>, Bytes)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (items, next) = block_on(store.scan_page(b"p:", after.as_deref(), usize::MAX))
            .expect("a limit above the cap is clamped, never refused");
        assert!(
            items.len() <= LOWERED_CAP,
            "a page must never exceed the store's cap ({LOWERED_CAP}), even when the \
             caller asks for usize::MAX — an unbounded page is the heap growth the cap \
             exists to stop"
        );
        pairs.extend(items);
        match next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    assert_eq!(pairs, expected);
}

// ---- Leg C: the page bound, and why non-progress is impossible -------------

#[test]
fn a_page_is_bounded_by_the_callers_limit() {
    let store = store();
    let expected = seed(&store, 7);

    let (page, next) = block_on(store.scan_page(b"p:", None, 3)).expect("a bounded page");
    assert_eq!(page.len(), 3, "a limit of 3 bounds the page to 3");
    assert_eq!(
        page,
        expected[..3],
        "the page carries the first three pairs — each key with the value it was \
         committed with, not a neighbour's"
    );
    assert_eq!(
        next.as_deref(),
        Some(expected[2].0.as_slice()),
        "a full page carries the last key returned as the cursor (0016:2657-2658)"
    );
    assert_eq!(walk(&store, b"p:", 3), expected);
}

#[test]
fn a_limit_above_the_cap_is_clamped_never_refused() {
    // The cap refuses to be RAISED (`with_scan_cap` clamps, `scan.rs:75-89`), so a
    // caller asking for a bigger page must not be failed for it: it gets a smaller
    // page plus a cursor, which is a complete answer. Observable here precisely
    // because redb's cap knob makes the effective cap explicit.
    let store = capped_store();
    seed(&store, LOWERED_CAP + 3);

    let (page, next) = block_on(store.scan_page(b"p:", None, usize::MAX))
        .expect("a limit above the cap must be clamped to the cap, not an Err");
    assert_eq!(
        page.len(),
        LOWERED_CAP,
        "the page is clamped to the store's cap"
    );
    assert!(next.is_some(), "and still carries a cursor to resume from");
}

#[test]
fn a_zero_limit_is_refused_with_the_seam_error() {
    // Not answered with an empty page: `next: Some(_)` would be a successful,
    // non-terminal response carrying no progress — the shape that makes a drain loop
    // forever — and `next: None` would falsely report the prefix exhausted.
    let store = store();
    seed(&store, 3);

    let err = block_on(store.scan_page(b"p:", None, 0))
        .expect_err("a page of 0 keys must be refused, not answered");
    let zero = err
        .downcast_ref::<ZeroPageLimit>()
        .unwrap_or_else(|| panic!("a zero limit must be a typed ZeroPageLimit, got: {err}"));
    assert_eq!(zero.prefix, b"p:".to_vec());
    assert_eq!((zero.limit, zero.cap), (0, SCAN_CAP));
}

#[test]
fn a_store_whose_cap_is_zero_refuses_every_page_and_never_reads_unbounded() {
    // The regression, on the real backend: `with_scan_cap(0)` is an accepted
    // configuration (the knob clamps only from above — the cap may be lowered, never
    // raised, #262), so a `min(limit, cap)` page bound would be 0 for every caller —
    // and a loop that stops at `len == limit` never stops, returning the WHOLE prefix
    // as one page. That inverts the bound in the exact direction the cap exists to
    // stop: 25 keys seeded, `scan_page(b"p:", None, 5)` answered with all 25.
    //
    // Refusal is the only correct answer (`items.len() <= min(limit, cap)` is 0 here),
    // and it is the seam's typed one so a caller classifies it identically whichever
    // store it holds.
    let store = store().with_scan_cap(0);
    let seeded = seed(&store, 25);
    assert_eq!(store.scan_cap(), 0, "the knob does not floor a zero cap");

    for limit in [1usize, 5, usize::MAX] {
        match block_on(store.scan_page(b"p:", None, limit)) {
            Err(err) => {
                let zero = err.downcast_ref::<ZeroPageLimit>().unwrap_or_else(|| {
                    panic!("a zero effective cap must be a typed ZeroPageLimit, got: {err}")
                });
                assert_eq!((zero.limit, zero.cap), (limit, 0));
            }
            Ok((items, next)) => panic!(
                "a store with an effective cap of 0 answered scan_page(limit = {limit}) \
                 with {} of {} seeded keys and next = {next:?}: a page bound of zero must \
                 be refused, never answered — least of all with an unbounded page",
                items.len(),
                seeded.len()
            ),
        }
    }
}

#[test]
fn the_cursor_advances_on_every_non_terminal_page() {
    // A page that comes back with `next <= after` would make the next lap re-read
    // exactly what it just read, forever.
    let store = store();
    seed(&store, 9);

    let mut after: Option<Vec<u8>> = None;
    loop {
        let (_, next) = block_on(store.scan_page(b"p:", after.as_deref(), 2)).expect("a page");
        let Some(cursor) = next else { break };
        if let Some(previous) = &after {
            assert!(
                cursor > *previous,
                "the cursor must advance strictly: {cursor:?} is not past {previous:?}"
            );
        }
        after = Some(cursor);
    }
}

#[test]
fn a_cursor_below_the_prefix_starts_the_page_at_the_prefix() {
    // The guard `page_start` makes shared: a cursor from *outside* this prefix
    // (a persisted drain cursor, an empty `Some(b"")`) must not be handed to the range
    // read as-is. redb's range would then open below `p:`, meet the `o:` key first, and
    // stop — an empty page with `next: None`, telling the caller a prefix whose every
    // key is still there is exhausted. Driven on the real backend, with a real
    // neighbouring key present, because without one the bug hides.
    let store = store();
    let expected = seed(&store, 4);
    block_on(store.commit(WriteBatch::new().put(b"o:decoy".to_vec(), "elsewhere")))
        .expect("seed the neighbouring prefix");

    for below in [&b"o:"[..], &b"o:decoy"[..], &b"p"[..], &b""[..]] {
        let (page, _) = block_on(store.scan_page(b"p:", Some(below), 16)).expect("a page");
        assert_eq!(
            page, expected,
            "a cursor below the prefix ({below:?}) must start the page at the prefix, \
             not answer an empty terminal page for a prefix that is not exhausted"
        );
    }
}

#[test]
fn a_cursor_past_the_prefix_is_an_empty_terminal_page_never_an_error() {
    // The other half of the same seam decision, and the arm that fails loudest
    // elsewhere: `p;` is the exclusive end of the `p:` range, and `q:`/`\xff` are past
    // it. Nothing under `p:` can follow such a cursor, so the answer is a page — empty,
    // and terminal so the caller stops rather than lapping forever. redb survives this
    // input even without the guard (its range is `Unbounded` on the right and stops at
    // the first foreign key), which is exactly why the assertion belongs in the SHARED
    // clause as well: the two distributed backends build `[cursor, upper_bound)`, and
    // an inverted range there is a client-side panic (tikv-client's transaction
    // buffer) or a silently-empty read (FDB) — one contract, two substrate accidents.
    // Kept here too so the per-fix gate, which runs only this target and the
    // demonstrated-red one, sees the property on a real backend.
    let store = store();
    seed(&store, 4);
    block_on(store.commit(WriteBatch::new().put(b"q:decoy".to_vec(), "past the prefix")))
        .expect("seed the following prefix");

    for past in [&b"p;"[..], &b"q:"[..], &b"q:decoy"[..], &b"\xff"[..]] {
        let (page, next) = block_on(store.scan_page(b"p:", Some(past), 16)).unwrap_or_else(|err| {
            panic!("a cursor past the prefix ({past:?}) must be answered, not failed: {err}")
        });
        assert!(
            page.is_empty() && next.is_none(),
            "a cursor at or past the prefix's upper bound ({past:?}) must answer an \
             empty TERMINAL page; got {} item(s) and next = {next:?}",
            page.len()
        );
    }
}

// ---- The shared contract clauses, driven against redb ----------------------
//
// Called directly (rather than only through `run_all`, which
// `crates/metadata-redb/tests/conformance.rs` already drives) so that a run of THIS
// target alone — which is what the per-fix verification gate invokes — exercises the
// normative clauses, not just the backend-specific legs above. Each clause gets a
// fresh store, the isolation every clause assumes.

#[test]
fn redb_honours_the_shared_scan_page_clauses() {
    block_on(async {
        conformance::contract_scan_page_orders_by_raw_bytes(&store()).await;
        conformance::contract_scan_page_cursor_is_exclusive(&store()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(&store()).await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&store()).await;
        conformance::contract_scan_page_limit_bounds_the_page(&store()).await;
        conformance::contract_scan_page_escapes_the_scan_cap(
            &store().with_scan_cap(conformance::LOWERED_SCAN_CAP),
            conformance::LOWERED_SCAN_CAP,
        )
        .await;
        conformance::contract_scan_page_refuses_a_zero_page_bound(&store().with_scan_cap(0), 0)
            .await;
    });
}
