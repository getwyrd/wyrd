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

use bytes::Bytes;
use wyrd_traits::{
    CommitOutcome, MetadataStore, ScanCapExceeded, ScanPage, WriteBatch, ZeroPageLimit,
};

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

// ---- Read-consistency (#261 decision; #419) --------------------------------
//
// The three properties below pin the *snapshot/temporal* dimension of the read
// contract (`#261`'s decided read-consistency level: a fresh-TSO snapshot per
// op, one snapshot held across all internal pages of a single `scan()`) that
// the four sequential `contract_*` functions above do not touch: ADR-0015
// clause 3 ("Per-session read-your-writes and monotonic reads",
// `../wyrd/docs/design/adr/0015-consistency-contract.md:24`) and proposal
// 0015's "Read consistency to document" open question
// (`../wyrd/docs/design/proposals/accepted/0015-milestone-4-production-metadata-backend-revised.md:780-785`).
// Each is demonstrated non-vacuous against a deliberately-violating store in
// `crates/metadata-conformance/tests/demonstrated_red.rs` (build-notes records
// which sequential `contract_*` each violating store still passes, proving
// these three catch something the existing suite does not).

/// A `get` observes the most recently committed value for a key across a
/// **sequence** of overwrites — not merely the single commit-then-read
/// [`contract_commit_and_get`] already pins (`:24-37`, one commit, one read
/// per key). This is the read-your-writes / anti-stale-read dimension #261
/// decided (ADR-0015 clause 3): a `get` must never serve a value older than
/// the most recently committed one for that key, which is exactly the failure
/// mode a nearest-replica / bounded-staleness read (ADR-0015's rejected
/// Option B) would exhibit, and what a fresh-TSO snapshot-per-op read forbids.
pub async fn contract_read_after_commit(store: &impl MetadataStore) {
    let key = b"read-after-commit".to_vec();
    for i in 1..=4u8 {
        let value = format!("v{i}");
        let outcome = store
            .commit(WriteBatch::new().put(key.clone(), value.clone()))
            .await
            .unwrap();
        assert_eq!(outcome, CommitOutcome::Committed, "overwrite {i} commits");
        assert_eq!(
            store.get(&key).await.unwrap().as_deref(),
            Some(value.as_bytes()),
            "get after commit {i} must observe THAT commit's write, not an earlier \
             one (read-your-writes, ADR-0015 clause 3) — a store that only \
             invalidates a cached read on the very next commit would pass a single \
             commit-then-get but fail this repeated overwrite"
        );
    }
}

/// A mutation that lands **between** a read-then-commit's read and its own
/// commit must yield [`CommitOutcome::Conflict`] — never a torn or duplicated
/// binding. This models the `rename` pattern in `crates/core/src/metadata.rs:276`
/// (`get(&old_key)` at `:284`, then `.require(old_key, current)` at `:288`):
/// safety rests on that `require` re-check under proposal 0015's locking-read
/// rule (ADR-0015 clause 3), **not** on read freshness. Unlike the sequential
/// [`contract_require_value_gates`] (`:83-111`, a single `put` gated by one
/// stale `require`, no `delete`), this drives the exact multi-precondition +
/// `delete` + `put` shape `rename` issues, and — critically — the
/// *interleaved* case: another writer's mutation commits strictly between the
/// racer's `get` and its own `commit` call.
pub async fn contract_rename_race_yields_conflict(store: &impl MetadataStore) {
    let old_key = b"race:old".to_vec();
    let winner_key = b"race:winner".to_vec();
    let loser_key = b"race:loser".to_vec();

    store
        .commit(WriteBatch::new().put(old_key.clone(), "binding"))
        .await
        .unwrap();

    // The racer's read (mirrors `rename`'s pre-commit `get`, metadata.rs:284).
    let read = store.get(&old_key).await.unwrap().expect("binding exists");

    // A concurrent mutation lands strictly between that read and the racer's
    // commit below — another writer wins the move first.
    let winner = store
        .commit(
            WriteBatch::new()
                .require(old_key.clone(), read.clone())
                .require_absent(winner_key.clone())
                .delete(old_key.clone())
                .put(winner_key.clone(), read.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        winner,
        CommitOutcome::Committed,
        "the concurrent mutation wins"
    );

    // The racer now commits against its now-stale read (mirrors metadata.rs:288's
    // `require(old_key, current)`) — it must lose, and must not tear the binding.
    let racer = store
        .commit(
            WriteBatch::new()
                .require(old_key.clone(), read.clone())
                .require_absent(loser_key.clone())
                .delete(old_key.clone())
                .put(loser_key.clone(), read),
        )
        .await
        .unwrap();
    assert_eq!(
        racer,
        CommitOutcome::Conflict,
        "a stale read-then-commit must lose to the interleaved mutation"
    );

    // Exactly one binding exists post-race: the winner's, never both (a
    // duplicated binding) and never neither (a lost/torn binding).
    assert_eq!(store.get(&old_key).await.unwrap(), None, "source is gone");
    assert_eq!(
        store.get(&winner_key).await.unwrap().as_deref(),
        Some(&b"binding"[..]),
        "the winner's binding must have landed"
    );
    assert_eq!(
        store.get(&loser_key).await.unwrap(),
        None,
        "the loser's commit must not have written anything (atomic conflict, no \
         torn binding)"
    );
}

/// A single `scan()` observes one consistent cut: a concurrent rename that
/// moves a binding from one key to another under the **same prefix** appears
/// in exactly one of the two positions — never both (a duplicated/torn view)
/// and never neither (a lost view). Unlike [`contract_scan_by_prefix`]
/// (`:41-56`, which never mutates between commits and never re-scans), this
/// scans **before and after** a rename-shaped mutation and pins the *count*
/// and *identity* of what a listing observes across it — the discriminator
/// #254's TiKV paged-scan swap must preserve. Note (Difficulty, #419 brief):
/// redb's `scan` is a single atomic local read, so this necessarily passes
/// trivially here; its value is the documented, TiKV-inherited pin plus the
/// demonstrated-red counter-store below, which shows the property is not a
/// tautology even though redb cannot make it bite.
pub async fn contract_scan_is_consistent_cut(store: &impl MetadataStore) {
    let prefix = b"cut:".to_vec();
    let old_key = b"cut:old".to_vec();
    let new_key = b"cut:new".to_vec();

    store
        .commit(WriteBatch::new().put(old_key.clone(), "binding"))
        .await
        .unwrap();
    let before = store.scan(&prefix).await.unwrap();
    assert_eq!(before.len(), 1, "one binding exists before the rename");
    assert_eq!(before[0].0, old_key);

    let outcome = store
        .commit(
            WriteBatch::new()
                .require(old_key.clone(), "binding")
                .require_absent(new_key.clone())
                .delete(old_key.clone())
                .put(new_key.clone(), "binding"),
        )
        .await
        .unwrap();
    assert_eq!(outcome, CommitOutcome::Committed);

    let after = store.scan(&prefix).await.unwrap();
    assert_eq!(
        after.len(),
        1,
        "the rename must appear in exactly one scan position, never both (torn) \
         nor neither (lost)"
    );
    assert_eq!(
        after[0].0, new_key,
        "the surviving position is the rename's target"
    );
}

// ---- The paginated range read (#634; proposal 0016:2645-2672) ---------------
//
// `scan_page` exists because `scan` is complete-or-fail-loud: a namespace past
// `SCAN_CAP` cannot be enumerated by it at any size, and two multipart-era
// populations cross it (the unbounded `retire:` drain; GC's `orphan:` ledger,
// where one maximum segmented-object retirement installs ~1.78 M marks against a
// cap of 1,048,576). The signature alone is not the contract — a continuation
// that silently SKIPS a key would leave a `retire:` obligation holding its bytes
// and its records forever — so the four normative clauses at `0016:2653-2666`
// are asserted here, on every backend, rather than left to each implementation.
//
// Each is demonstrated non-vacuous against a deliberately-violating store in
// `crates/metadata-conformance/tests/scan_page_demonstrated_red.rs`.

/// The lowered per-scan cap the cap-escape clause runs at, so every driver
/// lowers to the **same** value and the clause's population sizing is one
/// decision. Small on purpose: proving the ceiling by writing 2^20 keys would be
/// absurd, and the production code path is driven either way — only the ceiling
/// moves (`crates/metadata-redb/tests/scan.rs:75-89`).
pub const LOWERED_SCAN_CAP: usize = 8;

/// Lap ceiling for every paged walk in this suite: a **non-termination detector**,
/// deliberately far larger than any clause's population needs. It is not a
/// page-count assertion — the contract lets a store answer a short non-final page,
/// so the number of laps a walk takes is not fixed — but a walk that cannot end is a
/// defect, not a slow test, and something has to stop it.
const LAP_BUDGET: usize = 64;

/// Read one page and assert the **per-page** shape every clause below depends
/// on, whatever that clause is doing around it (#634 legs A and C):
///
/// * no more than `limit` pairs, all under `prefix`;
/// * raw byte-lexicographic order *within* the page;
/// * every key strictly after `after` — the cursor is exclusive;
/// * `next` is the last key returned, or `None`; never `Some` on an **empty**
///   page, and never a cursor that failed to advance past `after`. Both are
///   successful non-terminal answers carrying no progress, which is exactly the
///   shape that makes a drain loop forever.
async fn checked_page(
    store: &impl MetadataStore,
    prefix: &[u8],
    after: Option<&[u8]>,
    limit: usize,
) -> ScanPage {
    let (items, next) = store
        .scan_page(prefix, after, limit)
        .await
        .unwrap_or_else(|e| panic!("scan_page({prefix:?}, {after:?}, {limit}) failed: {e}"));

    assert!(
        items.len() <= limit,
        "a page must not exceed the caller's limit: asked for {limit}, got {}",
        items.len()
    );
    for (key, _) in &items {
        assert!(
            key.starts_with(prefix),
            "scan_page must return only keys under the prefix {prefix:?}, got {key:?}"
        );
    }
    for pair in items.windows(2) {
        assert!(
            pair[0].0 < pair[1].0,
            "scan_page must order by raw byte-lexicographic key — {:?} came back \
             before {:?}. A continuation over an order the caller cannot predict \
             is a continuation that can skip a key, and a skipped `retire:` \
             obligation retains its bytes forever (0016:2653-2656)",
            pair[0].0,
            pair[1].0
        );
    }
    if let (Some(cursor), Some((first, _))) = (after, items.first()) {
        assert!(
            first.as_slice() > cursor,
            "a page must start strictly after the cursor {cursor:?}, but it began \
             at {first:?} — an inclusive cursor re-yields the boundary key on \
             every lap, so the walk never ends (0016:2656)"
        );
    }
    match (&next, items.last()) {
        (Some(returned), Some((last, _))) => {
            assert_eq!(
                returned, last,
                "`next` must be the last key returned, so the caller can resume \
                 from it (0016:2657-2658)"
            );
            if let Some(cursor) = after {
                assert!(
                    returned.as_slice() > cursor,
                    "the cursor must advance: a non-terminal page came back with \
                     `next` = {returned:?}, which is not past the {cursor:?} it \
                     was called with — the next lap would re-read this page forever"
                );
            }
        }
        (Some(returned), None) => panic!(
            "an empty page must never carry a cursor, but this one returned \
             `next` = {returned:?}: a successful, non-terminal answer that made no \
             progress is what makes a drain loop forever (#634 leg C)"
        ),
        (None, _) => {}
    }
    (items, next)
}

/// Walk the whole `prefix` with pages of `limit`, returning the `(key, value)`
/// pairs in the order they came back. The lap budget is the termination proof: a
/// walk that does not end is a defect, not a slow test.
///
/// Pairs, not keys: a walk that hands back the right keys carrying the wrong bytes
/// is not a walk the retirement drain can use, and dropping the values here would
/// make every clause below blind to that (#634, iteration-3 review).
async fn walk(
    store: &impl MetadataStore,
    prefix: &[u8],
    limit: usize,
    laps: usize,
) -> Vec<(Vec<u8>, Bytes)> {
    let mut pairs: Vec<(Vec<u8>, Bytes)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    for lap in 1..=laps {
        let (items, next) = checked_page(store, prefix, after.as_deref(), limit).await;
        pairs.extend(items);
        match next {
            Some(cursor) => after = Some(cursor),
            None => return pairs,
        }
        assert!(
            lap < laps,
            "the walk over {prefix:?} did not terminate within {laps} pages of \
             {limit} — a paginated walk that cannot end is the same unusable \
             primitive as one that fails whole"
        );
    }
    unreachable!("the lap budget above returns or panics")
}

/// `p:\x80`-style rendering for an assertion message. A *lossy* decode would map
/// both `0x80` and `0xff` to U+FFFD — indistinguishable in the very clause whose
/// fixture seeds both — so the escape is per byte and reversible.
fn escaped(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &byte in bytes {
        if byte == b'\\' {
            // The escape character itself, or the rendering is not reversible after
            // all: a literal backslash is `is_ascii_graphic`, so passing it through
            // would render the four bytes `\x80` identically to the single byte 0x80
            // — a collision in exactly the clause whose fixture seeds high bytes.
            out.push_str("\\\\");
        } else if byte.is_ascii_graphic() || byte == b' ' {
            out.push(byte as char);
        } else {
            out.push_str(&format!("\\x{byte:02x}"));
        }
    }
    out
}

/// The pairs as `key=value` strings, for a readable failure. Message-only: every
/// assertion below compares the **raw** pairs, so a rendering collision can never
/// weaken one.
fn rendered(pairs: &[(Vec<u8>, Bytes)]) -> Vec<String> {
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", escaped(key), escaped(value)))
        .collect()
}

/// Assert an exact `(key, value)` sequence — **values included**.
///
/// The value half is not decoration. A paged read that returns the right keys and
/// stale, swapped or empty bytes passes every key-and-cursor assertion and still
/// hands its caller the wrong record: for the `retire:` drain that is an obligation
/// discharged against bytes that are not its own, and no clause asserted on keys
/// alone can see it (#634, iteration-3 review finding).
fn assert_pairs_eq(actual: &[(Vec<u8>, Bytes)], expected: &[(Vec<u8>, Bytes)], what: &str) {
    assert!(
        actual == expected,
        "{what}\n  Each key must come back with the value it was committed with — a \
         page whose keys and cursors are right but whose bytes are stale, swapped or \
         empty is a caller decoding the wrong record.\n  expected: {:?}\n       got: \
         {:?}",
        rendered(expected),
        rendered(actual)
    );
}

/// Assert one page against the exact pairs the contract says remain at that
/// cursor, **without assuming the page filled its `limit`**.
///
/// The contract bounds a page from above (`items.len() <= min(limit, cap)`) and
/// constrains `next`, never `items.len()` from below: a store may answer a **short
/// non-terminal** page — a range read that stopped at a transaction boundary, a
/// page truncated by a byte budget — provided it carries the cursor to resume from.
/// So a fixture that asserted `page.len() == limit` would fail a *conforming* store,
/// which is the suite's bug and not the store's (#634, iteration-3 review finding).
///
/// What the contract does fix, and what this asserts: the page is the next
/// `page.len()` pairs of `expected` in order, values included; it may stop short
/// only by carrying a cursor; and `next: None` means the prefix is exhausted, so
/// the page must have returned all of them.
fn assert_page_is_next_of(
    page: &[(Vec<u8>, Bytes)],
    next: Option<&[u8]>,
    expected: &[(Vec<u8>, Bytes)],
    what: &str,
) {
    assert!(
        page.len() <= expected.len(),
        "{what}\n  the page returned {} pair(s) but only {} remain under the prefix \
         at this cursor: {:?}",
        page.len(),
        expected.len(),
        rendered(page)
    );
    assert_pairs_eq(page, &expected[..page.len()], what);
    if next.is_none() {
        assert!(
            page.len() == expected.len(),
            "{what}\n  the page reported `next: None`, which means the prefix is \
             exhausted at that instant — but {} of the {} pair(s) under it were never \
             returned, starting at {:?}. A short page MAY stop early; it may not \
             claim the prefix is done while doing so, because the caller then stops \
             walking and those keys are never seen again (0016:2657-2658)",
            expected.len() - page.len(),
            expected.len(),
            rendered(&expected[page.len()..])
        );
    }
}

/// **Clause (a) — order.** Results come back ordered by **raw byte-lexicographic
/// key**, identically on every backend (`0016:2655-2656`).
///
/// The keys are chosen so byte order and *decoded-string* order genuinely
/// disagree: `0x80` and `0xff` are not valid UTF-8 and a lossy decode maps both
/// to U+FFFD, which sorts *after* `é` (U+00E9) — so a backend that sorts decoded
/// strings returns `é` before `0x80`, and a backend that returns insertion order
/// fails on the shuffled seeding. One key is also a strict **prefix** of another
/// (`p:a` before `p:a0`), the boundary a length-first or padded comparison gets
/// wrong.
///
/// Order is the load-bearing half of pagination: the cursor is a *key*, so a
/// continuation over an order the caller cannot predict is a continuation that
/// can silently skip. Which is why the population is read back three ways — one
/// page, pages of two, and **one key per page**: the last puts a page boundary on
/// every key, so the strict-prefix pair is crossed by a *continuation* and not
/// merely sorted inside one page.
pub async fn contract_scan_page_orders_by_raw_bytes(store: &impl MetadataStore) {
    let suffixes: [&[u8]; 6] = [b"a", b"a0", b"\x7f", b"\x80", "é".as_bytes(), b"\xff"];
    let key = |suffix: &[u8]| {
        let mut k = b"p:".to_vec();
        k.extend_from_slice(suffix);
        k
    };
    // Distinct values, so the page has to carry each key's OWN bytes: a read that
    // returns the right keys with the wrong values hands the caller the wrong record.
    let value = |i: usize| Bytes::from(format!("v{i}"));
    // Seeded in a shuffled order, so insertion order is not byte order either.
    let mut batch = WriteBatch::new();
    for i in [3usize, 0, 5, 2, 4, 1] {
        batch = batch.put(key(suffixes[i]), value(i));
    }
    store.commit(batch).await.unwrap();

    let expected: Vec<(Vec<u8>, Bytes)> = {
        let mut pairs: Vec<(Vec<u8>, Bytes)> = suffixes
            .iter()
            .enumerate()
            .map(|(i, s)| (key(s), value(i)))
            .collect();
        pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
        pairs
    };
    // Byte order and decoded-string order must actually differ here, or this
    // clause would pass against the very backend it exists to reject.
    let lossy_order: Vec<Vec<u8>> = {
        let mut keys: Vec<Vec<u8>> = suffixes.iter().map(|s| key(s)).collect();
        keys.sort_by_key(|k| String::from_utf8_lossy(k).to_string());
        keys
    };
    assert_ne!(
        expected.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        lossy_order,
        "the fixture is broken: these keys sort identically by bytes and by \
         decoded string, so the clause could not catch a string-ordering backend"
    );

    let (whole, next) = checked_page(store, b"p:", None, 16).await;
    assert_page_is_next_of(
        &whole,
        next.as_deref(),
        &expected,
        "one page of the whole prefix, asked for with a limit above its size, must \
         come back in raw byte order",
    );

    // The same order across a multi-page walk: the pages stitch into one
    // ascending sequence, not just each page sorted within itself.
    assert_pairs_eq(
        &walk(store, b"p:", 2, LAP_BUDGET).await,
        &expected,
        "a paged walk must stitch into the same raw byte order",
    );
    // …and again one key per page, which is what puts a page boundary between EVERY
    // adjacent pair — including `p:a` / `p:a0`, where the second key extends the
    // first. Order alone does not settle that boundary: an implementation that
    // resumes at an arithmetic successor of the cursor (its last byte incremented,
    // `p:a` -> `p:b`) returns these keys in perfect byte order inside every page and
    // still drops `p:a0` from the walk entirely, because it is the only key the
    // cursor is a prefix of. At `limit` 2 and 16 the boundary falls elsewhere and the
    // walk is complete, so the strict-prefix pair proves ordering *within* a page and
    // nothing about continuation *across* one — the blind spot this lap closes.
    assert_pairs_eq(
        &walk(store, b"p:", 1, LAP_BUDGET).await,
        &expected,
        "a walk one key per page must still return every key exactly once: with a page \
         boundary on every key, a resume point computed from the cursor rather than \
         taken as `the next key strictly after it` silently skips whatever extends the \
         cursor",
    );
}

/// **Clause (b) — the cursor is exclusive.** A page starts strictly *after*
/// `after`, whether or not that key exists in the range (`0016:2656`).
///
/// Four shapes are asserted, because they fail differently: an `after` that
/// **exists** *and is a strict prefix of the key that follows it* catches both the
/// inclusive-cursor implementation, which re-yields the boundary key on every lap
/// and never terminates, and the successor-arithmetic one, which resumes at the
/// cursor with its last byte incremented and so steps over every key that extends
/// the cursor — strictly after it, and still a silent skip; an `after` that does
/// **not** exist (a key synthesized *between* two stored keys) catches an
/// implementation that resumes by looking the cursor up and starts over when it
/// misses; and the two **degenerate** cursors, which fail in opposite directions
/// and are the two this suite has to drive because no ordinary walk produces them:
///
/// * `after` lexicographically **below the prefix** catches the implementation
///   that feeds the cursor straight into its range read — the range opens on an
///   earlier namespace, stops at the first key not carrying the prefix, and answers
///   an **empty terminal page** for a prefix that is not exhausted: a false
///   "nothing left", the silent skip clause (c) forbids, arriving through clause
///   (b)'s input.
/// * `after` **at or past the prefix's exclusive upper bound** (`p;`, `q:`, `\xff`
///   for a `p:` walk) catches the one that hands it to a *bounded* range read —
///   `[cursor, upper_bound(prefix))` is then a `begin > end` range, and what a
///   substrate does with that is *not* uniform: tikv-client resolves it against its
///   transaction buffer with `BTreeMap::range` and **panics** client-side, while
///   FoundationDB's key selectors tolerate it and read nothing. The contract's
///   answer is neither: a page, empty and terminal. Both distributed backends
///   shipped this arm as a plain lower bound in this slice's second iteration, and
///   the suite missed it because its "past the end" cursor (`p:99`) was still
///   *inside* the prefix's range — the exact blind spot a clause asserted on every
///   backend exists to close.
///
/// The page is the intersection of "after the cursor" and "under the prefix" —
/// which is why neither degenerate cursor may be an error, and why they get
/// opposite answers.
pub async fn contract_scan_page_cursor_is_exclusive(store: &impl MetadataStore) {
    // A distinct value per key: resuming after a cursor must hand back each key's own
    // bytes, not a neighbour's — the off-by-one an implementation that pairs a keys-only
    // range read with a separately-fetched value block makes.
    //
    // `p:200` is the load-bearing member: it is a strict **extension** of `p:20`, so it
    // is the key that must come back FIRST when the walk resumes after `p:20`. It is
    // what separates "strictly after the cursor" from the arithmetic successor an
    // implementation reaches for when its range API offers only an *inclusive* lower
    // bound — increment the cursor's last byte (`p:20` -> `p:21`) and open the range
    // there. That page also starts strictly after the cursor, and it silently steps
    // over every key the cursor is a prefix of, on every lap, forever: the exact skip
    // this primitive exists to prevent, invisible to a fixture whose keys are all the
    // same length. Not hypothetical either — TiKV is the backend that resolves the
    // cursor by successor arithmetic (`paging::next_page_start`, which appends `0x00`,
    // the smallest strict extension, and is right today), and TiKV is also the backend
    // whose conformance run is off-Check.
    let seeded: Vec<(Vec<u8>, Bytes)> = [&b"p:10"[..], b"p:20", b"p:200", b"p:30", b"p:40"]
        .iter()
        .map(|k| (k.to_vec(), Bytes::from(format!("v-{}", escaped(&k[..])))))
        .collect();
    let mut batch = WriteBatch::new();
    for (key, value) in &seeded {
        batch = batch.put(key.clone(), value.clone());
    }
    // A decoy under an EARLIER prefix, so a range read opened below `p:` meets a
    // key that is not under `p:` before it meets one that is. Without it, a
    // below-the-prefix cursor would accidentally work on an ordered store and case
    // (iv) below would pass against an implementation missing the guard.
    batch = batch.put(b"o:decoy".to_vec(), "not-under-the-prefix");
    // …and one under a LATER prefix, so case (v)'s beyond-the-range cursors have a
    // real key sitting past the boundary: an implementation that answered "the rest
    // of the keyspace after the cursor" would leak it into the page.
    batch = batch.put(b"q:decoy".to_vec(), "past-the-prefix");
    store.commit(batch).await.unwrap();

    // Every case below asserts against the exact pairs that remain at its cursor, and
    // tolerates a SHORT page that carries a cursor — the contract bounds a page from
    // above and constrains `next`, never `items.len()` from below.

    // (i) A cursor that EXISTS in the range, and whose immediate successor EXTENDS it
    // (`p:20` -> `p:200`). Two bugs answer this one input: an inclusive cursor returns
    // `p:20` again, and a successor-arithmetic cursor (`p:21`) skips `p:200` outright.
    let (page, next) = checked_page(store, b"p:", Some(b"p:20"), 16).await;
    assert_page_is_next_of(
        &page,
        next.as_deref(),
        &seeded[2..],
        "a page must resume strictly after an existing cursor, at the IMMEDIATE next \
         key — the cursor key itself must never come back, since an inclusive cursor \
         duplicates the boundary key on every lap and the walk never ends; and a key \
         that merely EXTENDS the cursor (`p:200` after `p:20`) must come back first, \
         since a resume point computed by incrementing the cursor's last byte steps \
         over every such key silently, which is the skip a paginated walk exists to \
         prevent",
    );

    // (ii) A cursor that does NOT exist, lying between two stored keys.
    let (page, next) = checked_page(store, b"p:", Some(b"p:25"), 16).await;
    assert_page_is_next_of(
        &page,
        next.as_deref(),
        &seeded[3..],
        "a cursor that is not itself a stored key still resumes from the next key \
         after it — the caller may hold a key that has since been deleted",
    );

    // (iii) A cursor below the range's first key (but still under the prefix)
    // yields everything; one past the last key but still inside the prefix's range
    // yields an exhausted page. Neither may walk outside the prefix.
    let (page, next) = checked_page(store, b"p:", Some(b"p:0"), 16).await;
    assert_page_is_next_of(
        &page,
        next.as_deref(),
        &seeded,
        "a cursor below the range skips nothing",
    );
    let (page, next) = checked_page(store, b"p:", Some(b"p:99"), 16).await;
    assert!(
        page.is_empty() && next.is_none(),
        "a cursor past the last key is an exhausted, terminal page"
    );

    // (iv) A cursor lexicographically BELOW THE PREFIX ITSELF — the caller resumed
    // a walk with a cursor from another namespace, or with an empty one. The page is
    // the intersection of "strictly after the cursor" and "under the prefix", so it
    // starts at the prefix and skips nothing. An implementation that hands the raw
    // cursor to its range read answers an empty page here, with `next: None`, which
    // tells the caller the prefix is exhausted while every key is still there.
    for below in [&b"o:"[..], &b"o:decoy"[..], &b"p"[..], &b""[..]] {
        let (page, next) = checked_page(store, b"p:", Some(below), 16).await;
        assert_page_is_next_of(
            &page,
            next.as_deref(),
            &seeded,
            &format!(
                "a cursor below the prefix ({below:?}) must start the page at the \
                 prefix and return the whole range — answering an empty page reports \
                 the prefix exhausted when nothing has been read, which is the silent \
                 skip a paginated walk exists to prevent. The page is `after the \
                 cursor` AND `under the prefix`, never neither"
            ),
        );
    }
    // …and the neighbouring prefix's key is never leaked into the page, whatever the
    // cursor: the guard must widen the page to the prefix, not past it.
    for cursor in [None, Some(&b"o:"[..]), Some(&b""[..])] {
        let (page, _) = checked_page(store, b"p:", cursor, 16).await;
        assert!(
            page.iter().all(|(key, _)| key.starts_with(b"p:")),
            "a page must never carry a key from outside the prefix (cursor {cursor:?})"
        );
    }

    // (v) A cursor AT OR PAST THE PREFIX'S EXCLUSIVE UPPER BOUND — the mirror image
    // of (iv), and the arm a *bounded* range read gets catastrophically wrong. `p;`
    // is exactly that upper bound (`b':'` + 1); `q:`, `q:decoy` and `\xff` are past
    // it, with a real key (`q:decoy`) sitting there so an implementation answering
    // "everything after the cursor" would leak it.
    //
    // Nothing under `p:` can follow such a cursor, so the contract's answer is a
    // page — empty and terminal — never an error. An implementation that builds
    // `[cursor, upper_bound(prefix))` from it produces `begin > end`, which its
    // substrate answers however it likes (tikv-client panics inside its
    // transaction-buffer range lookup; FoundationDB tolerates it), and "however it
    // likes" is what a shared clause exists to forbid. A caller reaches this arm by
    // resuming a walk with a cursor persisted under an earlier namespace, or by
    // sharing one cursor column across prefixes — the same way it reaches (iv).
    for past in [&b"p;"[..], &b"q:"[..], &b"q:decoy"[..], &b"\xff"[..]] {
        let (page, next) = store
            .scan_page(b"p:", Some(past), 16)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "a cursor at or past the prefix's upper bound ({past:?}) must be \
                     answered with an empty terminal page, not an error: {err}. The page \
                     is the intersection of `after the cursor` and `under the prefix`, so \
                     an empty intersection is an ordinary terminal answer — a backend that \
                     turns it into a bounded range read inverts that range (begin > end) \
                     and fails, or panics on, a walk whose cursor merely outran the prefix"
                )
            });
        assert!(
            page.is_empty() && next.is_none(),
            "a cursor at or past the prefix's upper bound ({past:?}) must answer an \
             empty TERMINAL page; got {} item(s) and next = {next:?}. Any key here \
             came from outside the prefix, and any cursor here makes the caller lap \
             forever",
            page.len()
        );
    }
}

/// **Clause (c) — termination.** `next` is `Some(last key returned)` while more
/// may remain and `None` **only** when the prefix is exhausted at that instant,
/// so a whole-population walk terminates *and* returns every key exactly once
/// (`0016:2657-2658`).
///
/// Both boundary shapes are driven, because the off-by-one lives between them: a
/// population that is an **exact multiple** of `limit` (a walk that answers
/// `None` on the last full page skips nothing, but one that answers `Some` must
/// terminate on the following, empty page) and one that is not.
pub async fn contract_scan_page_walk_terminates_and_is_complete(store: &impl MetadataStore) {
    const LIMIT: usize = 3;
    for (prefix, count, exact_multiple) in [
        (&b"exact:"[..], LIMIT * 3, true),
        (&b"ragged:"[..], LIMIT * 2 + 1, false),
    ] {
        // The fixture's own invariant, asserted rather than assumed: this clause is
        // *about* the two boundary shapes, so a population that quietly stopped being
        // an exact multiple (or started being one) would leave the off-by-one untested
        // while the clause still passed.
        assert_eq!(
            count.is_multiple_of(LIMIT),
            exact_multiple,
            "the {prefix:?} fixture must{} be an exact multiple of the page limit",
            if exact_multiple { "" } else { " NOT" }
        );
        // …and it must span MORE than two pages, so the walk actually contains a
        // non-final full page, a boundary page and a terminal one. A population that
        // fits in two pages would keep the "exact multiple" property while testing
        // neither the off-by-one nor a mid-walk cursor.
        assert!(
            count > LIMIT * 2,
            "the {prefix:?} fixture must span more than two pages of {LIMIT} to reach \
             the boundary this clause is about; got {count}"
        );
        let mut expected: Vec<(Vec<u8>, Bytes)> = Vec::new();
        let mut batch = WriteBatch::new();
        for i in 0..count {
            let mut key = prefix.to_vec();
            key.extend_from_slice(format!("{i:04}").as_bytes());
            let value = Bytes::from(format!("v{i}"));
            batch = batch.put(key.clone(), value.clone());
            expected.push((key, value));
        }
        store.commit(batch).await.unwrap();
        expected.sort_by(|(a, _), (b, _)| a.cmp(b));

        // A fixed, generous lap budget — far more pages than these populations need
        // (including the empty terminal page an exact multiple costs). It is a
        // non-termination detector, not a page-count assertion: the contract permits a
        // short non-final page, so the exact number of laps is not the store's to fix.
        let walked = walk(store, prefix, LIMIT, LAP_BUDGET).await;
        assert_pairs_eq(
            &walked,
            &expected,
            &format!(
                "the walk over {prefix:?} ended early or repeated itself: {count} \
                 keys seeded, {} returned. A walk must return every key present \
                 throughout it exactly once, with its own value, and then stop — \
                 `next: None` before the prefix is exhausted is the silent skip this \
                 primitive exists to prevent",
                walked.len()
            ),
        );
    }
}

/// **Clause (d) — no-skip for stable keys under concurrent mutation.** A key
/// present **throughout** the walk and not lexicographically before the cursor
/// comes back **exactly once** (`0016:2658-2660`).
///
/// That guarantee, and deliberately no more. `0016` states the rest as
/// unconstrained, and this clause holds the line: a key **inserted behind** the
/// cursor after it passed may be missed; a key **deleted ahead** of the cursor
/// may be missed or returned; and a key **inserted ahead** of the cursor is *not
/// covered at all* — it was not present throughout the walk — so either outcome
/// is accepted. Requiring it would reject a conforming backend that reads a
/// fresh snapshot per page, a shape 0016 permits on purpose: **no snapshot
/// isolation is required of any backend**, which is what keeps the primitive
/// implementable on redb, FoundationDB and TiKV alike.
///
/// The asymmetry is what the retirement drain needs: it is idempotent and
/// re-entrant per obligation, so a duplicate is a no-op while a skip is
/// unbounded retention.
///
/// Every mutation key is **derived from the cursor the walk actually reached**,
/// never from where a full page *would* have left it. The contract permits a short
/// non-terminal page, so "the cursor is at `c1` after page one" is the suite's
/// assumption and not the store's obligation — a fixture built on it fails a
/// conforming store (#634, iteration-3 review finding). Derivation also makes each
/// mutation's side of the cursor provable rather than eyeballed.
pub async fn contract_scan_page_no_skip_for_stable_keys(store: &impl MetadataStore) {
    const PREFIX: &[u8] = b"walk:";
    const LIMIT: usize = 2;
    // A distinct value per control key: the clause asserts what came back as well as
    // that it came back, so a store returning the right stable keys carrying another
    // key's bytes is caught here too.
    let control: Vec<(Vec<u8>, Bytes)> = (0..8)
        .map(|i| {
            (
                format!("walk:c{i}").into_bytes(),
                Bytes::from(format!("control-{i}")),
            )
        })
        .collect::<Vec<_>>();

    let mut batch = WriteBatch::new();
    for (key, value) in &control {
        batch = batch.put(key.clone(), value.clone());
    }
    store.commit(batch).await.unwrap();

    // Mutations applied mid-walk, once each, at the lap that puts them on the
    // right side of the cursor — each key derived from that lap's actual progress.
    let mut behind_cursor: Option<Vec<u8>> = None;
    let mut deleted_ahead: Option<Vec<u8>> = None;
    let mut ahead_of_cursor: Option<Vec<u8>> = None;

    let mut returned: Vec<(Vec<u8>, Bytes)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    // Pages that opened at or before the cursor they were given, `(cursor, key)` —
    // collected during the walk, judged after the stable-key assertions below.
    let mut opened_before_cursor: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut lap = 0usize;
    loop {
        lap += 1;
        assert!(
            lap <= LAP_BUDGET,
            "the walk did not terminate under concurrent mutation — a mutation may \
             cost a key, never the end of the walk"
        );
        // Deliberately the LIGHT per-page checks rather than the shared `checked_page`:
        // this clause's subject is the end-to-end outcome over the whole walk, and a
        // per-page shape assertion — which clauses (a)–(c) already drive, on static
        // populations where a store cannot blame the mutation — firing first would
        // attribute a duplicated or skipped stable key to the page's shape instead of
        // to the mutation that caused it, hiding the outcome this clause exists to
        // judge. (`tests/scan_page_demonstrated_red.rs` shows the difference: the
        // offset-paging double is caught here by "returned exactly once", the
        // assertion that IS clause (d).)
        let (items, next) = store
            .scan_page(PREFIX, after.as_deref(), LIMIT)
            .await
            .unwrap();
        assert!(
            items.len() <= LIMIT,
            "a page must not exceed the caller's limit, mutation or no mutation: \
             asked for {LIMIT}, got {}",
            items.len()
        );
        match (&next, items.last()) {
            (Some(cursor), Some((last, _))) => assert_eq!(cursor, last),
            (Some(_), None) => panic!("an empty page must never carry a cursor"),
            (None, _) => {}
        }
        // Clause (b) — the exclusive cursor — holds under mutation too, and nothing
        // else here would see it broken: a store that leaks the lap-1 behind-the-cursor
        // INSERT is returning a key at or before the cursor it was handed, and every
        // assertion below either excludes that key by name or says nothing about it.
        // (0016's "may be missed or duplicated" licence does not reach it: a key
        // inserted behind the cursor cannot be *duplicated*, never having been returned
        // before, so returning it is a (b) violation and not a permitted outcome.)
        //
        // RECORDED here and asserted after the stable-key checks below, not asserted
        // in place: a per-page assertion that fires first would attribute a skip or a
        // duplicate to the page's shape instead of to the mutation, which is exactly
        // what this clause avoids by using the light checks rather than `checked_page`.
        // The LIMIT/OFFSET double proves the ordering matters — it breaks both rules,
        // and the one it must be caught by here is "returned exactly once".
        if let Some(cursor) = after.as_deref() {
            for (key, _) in &items {
                if key.as_slice() <= cursor {
                    opened_before_cursor.push((cursor.to_vec(), key.clone()));
                }
            }
        }
        returned.extend(items);
        // How far the walk has actually got. Each mutation below is derived from it
        // and asserted against it, so it lands on the side of the cursor this clause
        // claims — a "behind the cursor" insert that in fact landed ahead of it is a
        // key the contract does not cover, so the clause would be asserting nothing
        // about that mutation.
        let progress = returned.last().map(|(key, _)| key.clone());

        if lap == 1 {
            let progress = progress
                .clone()
                .expect("the first page of a seeded prefix returns keys");
            // BEHIND the cursor: the last key returned with its final byte dropped is
            // a strict prefix of it, so it sorts strictly before it — whatever that
            // key turned out to be, and whether the page held one pair or two.
            let mut behind = progress.clone();
            behind.pop();
            assert!(
                behind.starts_with(PREFIX) && behind.len() > PREFIX.len(),
                "the derived behind-the-cursor key ({}) must still be under the walk's \
                 own prefix, or the insert is not in the range being walked at all",
                escaped(&behind)
            );
            assert!(
                behind.as_slice() < progress.as_slice(),
                "the behind-the-cursor insert ({}) must sort BEFORE the last key \
                 returned so far ({}), or it is not behind the cursor",
                escaped(&behind),
                escaped(&progress)
            );
            assert!(
                store.get(&behind).await.unwrap().is_none(),
                "the behind-the-cursor key ({}) must be an INSERT — it collided with a \
                 key the fixture already seeded, so the walk would have returned it \
                 anyway and the mutation would prove nothing",
                escaped(&behind)
            );
            // AHEAD of the cursor: the greatest control key there is. Stated as an
            // assertion rather than searched for with a comparison, so the fixture
            // cannot quietly settle on a key the walk has ALREADY returned — a delete
            // behind the cursor is a mutation the contract says nothing about, and
            // clause (d) would then be asserting nothing about this one.
            let (ahead, _) = control
                .last()
                .expect("the fixture seeds a control set")
                .clone();
            assert!(
                ahead.as_slice() > progress.as_slice(),
                "the first page ({}) already reached the last control key ({}), so no \
                 delete can land ahead of the cursor — the fixture needs a population \
                 larger than one page",
                escaped(&progress),
                escaped(&ahead)
            );
            store
                .commit(
                    WriteBatch::new()
                        .put(behind.clone(), "inserted-behind")
                        .delete(ahead.clone()),
                )
                .await
                .unwrap();
            behind_cursor = Some(behind);
            deleted_ahead = Some(ahead);
        }
        if lap == 2 {
            let progress = progress
                .clone()
                .expect("the second page of this fixture returns keys");
            // AHEAD of the cursor: appending to the last key returned always sorts
            // after it, so this insert lands ahead of the cursor whatever the pages
            // were — and the contract covers neither outcome for it.
            let mut ahead = progress.clone();
            ahead.extend_from_slice(b"-inserted");
            assert!(
                ahead.as_slice() > progress.as_slice() && ahead.starts_with(PREFIX),
                "the ahead-of-the-cursor insert ({}) must sort AFTER the last key \
                 returned so far ({}) and stay under the prefix — behind it, the \
                 contract WOULD cover it and the clause would be accepting an outcome \
                 it must forbid",
                escaped(&ahead),
                escaped(&progress)
            );
            store
                .commit(WriteBatch::new().put(ahead.clone(), "inserted-ahead"))
                .await
                .unwrap();
            ahead_of_cursor = Some(ahead);
        }

        match next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }

    let behind_cursor = behind_cursor.expect("the lap-1 insert must have been applied");
    let deleted_ahead = deleted_ahead.expect("the lap-1 delete must have been applied");
    let ahead_of_cursor = ahead_of_cursor.expect("the lap-2 insert must have been applied");

    // The stable set: seeded before the walk, never touched during it. The deleted
    // key is excluded — it was NOT present throughout, and the contract permits
    // either outcome for it.
    for (key, value) in control.iter().filter(|(k, _)| *k != deleted_ahead) {
        let seen: Vec<&Bytes> = returned
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v)
            .collect();
        assert_eq!(
            seen.len(),
            1,
            "{} was present throughout the walk and never mutated, so it must be \
             returned exactly once — it came back {} times. A skip here is the \
             failure the paginated walk exists to prevent (a skipped `retire:` \
             obligation retains its bytes and its records forever); a duplicate \
             means the cursor did not advance past it",
            escaped(key),
            seen.len()
        );
        assert_eq!(
            seen[0],
            value,
            "{} came back carrying {} instead of {}: a stable key must come back \
             with the value it was committed with, and a walk under mutation must \
             not shuffle values between its stable keys either",
            escaped(key),
            escaped(seen[0]),
            escaped(value)
        );
    }
    // The exclusive cursor, judged only now that the clause's own subject — a stable
    // key skipped or duplicated — has been judged, so a store that breaks both is
    // reported as the skip it is (the LIMIT/OFFSET double is exactly that store).
    if let Some((cursor, key)) = opened_before_cursor.first() {
        panic!(
            "a page must start strictly AFTER the cursor it was given, under concurrent \
             mutation as much as without it: asked for the page after {}, got {} at or \
             before it{}. A key inserted BEHIND the cursor may be missed — it may not be \
             served by a later page, which is a page that opened before its own cursor \
             ({} such page(s) did)",
            escaped(cursor),
            escaped(key),
            if key == &behind_cursor {
                " — the key this clause inserted behind the cursor mid-walk"
            } else {
                ""
            },
            opened_before_cursor.len()
        );
    }

    // `behind_cursor`, `ahead_of_cursor` and `deleted_ahead` are asserted **not at all**
    // beyond that, and that is the clause, not an omission: `0016:2659-2661` says such
    // keys "may be missed or duplicated". An assertion here — even a mild "at most
    // once" — would reject a conforming backend that reads a fresh snapshot per page,
    // the shape 0016 permits on purpose. If the stronger rule is ever wanted it is an
    // amendment to 0016, not a clause tightened here. What the check above forbids is
    // narrower and belongs to clause (b): a page *opening* at or before its own cursor,
    // whoever wrote the key. "Missed" stays available for all three.

    // What IS asserted about them: that the mutations happened at all, and mid-walk.
    // Without this the clause could pass vacuously — a store whose `commit` quietly
    // dropped these three batches would be walking a static population, which is
    // clause (c)'s job, not (d)'s. (The mutations are *observed* through `get`, not
    // through the walk, so this constrains the fixture, never the paging.)
    assert!(
        lap >= 3,
        "the walk ended in {lap} page(s), so the mutations at laps 1 and 2 were not \
         mid-walk — the clause would be asserting nothing about concurrency"
    );
    assert!(
        store.get(&behind_cursor).await.unwrap().is_some(),
        "the behind-the-cursor insert must actually be in the store"
    );
    assert!(
        store.get(&ahead_of_cursor).await.unwrap().is_some(),
        "the ahead-of-the-cursor insert must actually be in the store"
    );
    assert!(
        store.get(&deleted_ahead).await.unwrap().is_none(),
        "the ahead-of-the-cursor delete must actually have removed the key"
    );
}

/// **`limit` is a page bound, and non-progress is impossible** (#634 leg C).
///
/// Three rules, all of which a drain loop depends on:
///
/// 1. `items.len() <= min(limit, the store's effective cap)` — a page is bounded
///    by the caller's limit *and* by the store's own ceiling, since no page may
///    exceed `SCAN_CAP` (`0016:2647-2650`).
/// 2. A `limit` **above** the store's cap is **clamped**, never an `Err`: the cap
///    refuses to be raised, and a caller asking for more must not be failed for
///    it. (The clamp itself is observable only where the cap is lowered — see
///    [`contract_scan_page_escapes_the_scan_cap`].)
/// 3. `limit == 0` is **rejected** with the seam's [`ZeroPageLimit`], not
///    answered with an empty page: an empty page carrying `next: Some(_)` is a
///    successful non-terminal response with no progress — the shape that makes a
///    drain loop forever — and `next: None` would falsely report the prefix
///    exhausted.
///
/// Plus **cursor progress**: every non-terminal page's `next` is strictly greater
/// than the `after` it was called with — asserted for every page this suite reads,
/// by the shared `checked_page` helper.
pub async fn contract_scan_page_limit_bounds_the_page(store: &impl MetadataStore) {
    let mut expected: Vec<(Vec<u8>, Bytes)> = Vec::new();
    let mut batch = WriteBatch::new();
    for i in 0..5 {
        let (key, value) = (format!("p:{i}").into_bytes(), Bytes::from(format!("v{i}")));
        batch = batch.put(key.clone(), value.clone());
        expected.push((key, value));
    }
    store.commit(batch).await.unwrap();
    expected.sort_by(|(a, _), (b, _)| a.cmp(b));

    // 3. A zero limit is a typed refusal, on every backend.
    let err = store
        .scan_page(b"p:", None, 0)
        .await
        .expect_err("a scan_page of 0 keys must be rejected, not answered with an empty page");
    assert!(
        err.downcast_ref::<ZeroPageLimit>().is_some(),
        "a zero limit must raise the SEAM's ZeroPageLimit, so a caller classifies \
         it identically whichever store it holds (#516's rule); got: {err}"
    );

    // 1. The page is bounded by the limit (`checked_page` asserts that on every page
    //    this suite reads), it carries the pairs that actually remain at its cursor,
    //    and the walk still returns everything. What is NOT asserted is that the page
    //    is FULL: the contract bounds a page from above and constrains `next`, so a
    //    short page carrying a cursor is conforming and a `page.len() == limit`
    //    assertion here would fail a conforming store.
    let (page, next) = checked_page(store, b"p:", None, 2).await;
    assert_page_is_next_of(
        &page,
        next.as_deref(),
        &expected,
        "the first page of a limit-2 walk",
    );
    assert_pairs_eq(
        &walk(store, b"p:", 2, LAP_BUDGET).await,
        &expected,
        "the bounded pages still enumerate the whole prefix",
    );

    // 2. A limit far above any store's cap is clamped, never an error: the answer is
    //    a page (however large the store chose to make it) plus, if it stopped short,
    //    a cursor — and the walk from it still returns everything.
    let (page, next) = checked_page(store, b"p:", None, usize::MAX).await;
    assert_page_is_next_of(
        &page,
        next.as_deref(),
        &expected,
        "a limit above the store's cap must be clamped to the cap and ANSWERED, never \
         refused — the cap is a ceiling on the page, not a bound on what a caller may \
         ask for",
    );
    assert_pairs_eq(
        &walk(store, b"p:", usize::MAX, LAP_BUDGET).await,
        &expected,
        "a walk asking for more than the cap enumerates the whole prefix too",
    );
}

/// **The primitive escapes the bound it exists to escape** (#634 legs B and E) —
/// asserted against a store whose effective cap the *driver* has lowered to
/// `cap`, because a store's cap knob is a per-backend inherent method that this
/// suite cannot reach through the trait.
///
/// Both halves, in one clause, because either alone proves nothing:
///
/// 1. `scan(prefix)` over this population fails loud with [`ScanCapExceeded`] at
///    `cap` — so the bound is genuinely in force and the walk below is escaping
///    something. Without this half a cap-lowering hook that silently lowered
///    nothing would leave the clause vacuous.
/// 2. A `scan_page` walk of the **same** store returns **every** key exactly
///    once, in byte order, with every page inside `cap` even though the caller
///    asked for `usize::MAX`.
///
/// This is the leg that fails against any `scan()`-backed `scan_page`: such a
/// body inherits the cap and reports the same fail-loud error the walk is
/// supposed to make survivable.
pub async fn contract_scan_page_escapes_the_scan_cap(store: &impl MetadataStore, cap: usize) {
    assert!(
        cap >= 2,
        "the cap-escape clause needs a cap of at least 2 to page over; got {cap}"
    );
    const PREFIX: &[u8] = b"cap:";
    // Deliberately not a multiple of the cap: the walk must handle the ragged
    // final page as well as the full ones. Both properties asserted, not assumed —
    // a population that stopped exceeding the cap would leave the *escape* untested
    // while this clause still passed.
    let count = cap * 3 + 1;
    assert!(
        count > cap * 3 && !count.is_multiple_of(cap),
        "the cap-escape fixture must exceed THREE full pages of the cap ({cap}) — so the \
         walk spans several capped pages plus a ragged one, not merely one key past the \
         cap — and must not be a multiple of it; got {count}"
    );
    let mut expected: Vec<(Vec<u8>, Bytes)> = Vec::new();
    let mut batch = WriteBatch::new();
    for i in 0..count {
        let key = format!("cap:{i:06}").into_bytes();
        let value = Bytes::from(format!("v{i}"));
        batch = batch.put(key.clone(), value.clone());
        expected.push((key, value));
    }
    store.commit(batch).await.unwrap();
    expected.sort_by(|(a, _), (b, _)| a.cmp(b));

    // 1. `scan` cannot read this population at all.
    let err = store.scan(PREFIX).await.expect_err(
        "with a lowered cap this population is past it, so `scan` must fail loud — \
         if it succeeds, the cap-lowering hook did not lower anything and the walk \
         below would prove nothing",
    );
    let cap_err = err
        .downcast_ref::<ScanCapExceeded>()
        .unwrap_or_else(|| panic!("an over-cap scan must be a typed ScanCapExceeded, got: {err}"));
    assert_eq!(cap_err.cap, cap, "the breach reports the store's own cap");

    // 2. `scan_page` walks the same population to completion anyway. The caller
    //    asks for far more than the cap; each page is clamped to it.
    let mut pairs: Vec<(Vec<u8>, Bytes)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    for lap in 1..=LAP_BUDGET {
        let (items, next) = checked_page(store, PREFIX, after.as_deref(), usize::MAX).await;
        assert!(
            items.len() <= cap,
            "no page may exceed the store's cap ({cap}), even for a caller asking \
             for usize::MAX — a page past the cap is the unbounded materialization \
             the cap exists to stop (0016:2647-2650)"
        );
        pairs.extend(items);
        match next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
        assert!(lap < LAP_BUDGET, "the over-cap walk did not terminate");
    }
    assert_pairs_eq(
        &pairs,
        &expected,
        "a population `scan` refuses whole must still be enumerable page by page, \
         every key exactly once, with its own value and in byte order — that is the \
         entire reason this primitive exists (0016:2645-2652). A `scan_page` built \
         on `scan` fails here, because it inherits the very cap it must escape",
    );
}

/// **A store whose effective cap is `0` refuses every page — it never answers an
/// unbounded one** (#634, iteration-1 finding).
///
/// The degenerate configuration every cap knob admits: `with_scan_cap(0)` is
/// accepted (the knobs clamp only from *above*, since the cap may be lowered but
/// never raised, #262), so `min(limit, cap)` is `0` for **every** caller. A backend
/// that resolved its page bound with that `min` and then stopped its read at
/// `len >= limit` would never stop at all — the first key already exceeds the
/// bound — and would return the **whole prefix** as one page: the exact inversion of
/// the bound, and the unbounded materialization the cap exists to prevent. It is
/// reachable through a public method on a production type, so it is a contract
/// question, not a test-knob curiosity.
///
/// The contract's answer is the same one `limit == 0` gets, and for the same reason:
/// no page can carry a key, every possible answer is wrong (an empty page with a
/// cursor makes a drain loop forever; without one it falsely reports the prefix
/// exhausted; a page in spite of the bound is unbounded), so the call is **refused**
/// with the seam's [`ZeroPageLimit`]. Driven with a seeded population, because
/// against an empty store the wrong implementation would answer an empty page and
/// look correct.
pub async fn contract_scan_page_refuses_a_zero_page_bound(store: &impl MetadataStore, cap: usize) {
    assert_eq!(
        cap, 0,
        "this clause is about a store whose effective cap is zero; got {cap}"
    );
    const PREFIX: &[u8] = b"zerocap:";
    let mut batch = WriteBatch::new();
    for i in 0..5 {
        batch = batch.put(format!("zerocap:{i}").into_bytes(), format!("v{i}"));
    }
    store.commit(batch).await.unwrap();

    for limit in [1usize, 3, usize::MAX] {
        match store.scan_page(PREFIX, None, limit).await {
            Err(err) => assert!(
                err.downcast_ref::<ZeroPageLimit>().is_some(),
                "a page bound of zero must raise the SEAM's ZeroPageLimit, so a caller \
                 classifies it identically whichever store it holds (#516's rule); \
                 got: {err}"
            ),
            Ok((items, next)) => panic!(
                "a store whose effective cap is 0 answered scan_page(limit = {limit}) \
                 with {} item(s) and next = {next:?} instead of refusing it. \
                 `items.len() <= min(limit, cap)` is 0 here, so ANY page breaks the \
                 bound — and a page loop that returns the whole prefix has inverted \
                 the cap it was meant to respect",
                items.len()
            ),
        }
    }
}

// ---- The commit partition's third clause (#437) -----------------------------

/// A **blind** batch — one carrying no preconditions — never yields
/// [`CommitOutcome::Conflict`]: it either commits, or fails with `Err`.
///
/// The contract point the FoundationDB port made load-bearing, and the one the
/// suite did not pin (`wyrd_traits::CommitOutcome`, clause 3). `Conflict` is the
/// answer to "your precondition lost"; a batch that asserted nothing about prior
/// state has nothing to lose, so a backend that gives up on one owes the caller an
/// `Err`. It is not a stylistic preference: blind writers across the codebase
/// (`core::repair::enqueue_repair`, the custodian's desired-state writes) `?` the
/// call and ignore the returned [`CommitOutcome`] — a `Conflict` handed to them
/// reads as success while the write silently vanished. The pressure to get this
/// wrong is real and backend-shaped: an optimistic backend receives ONE lost-race
/// error code for both batch shapes (FoundationDB's `1020 not_committed`) and must
/// route it by shape, and a pessimistic one must not let a lock loss on a blind
/// batch fall through the same path as a failed precondition.
///
/// Two halves, because the sequential half cannot reach the race:
///
/// - **Sequential**: blind overwrites and blind deletes of keys that already exist
///   commit — including on a key a conditional writer just conflicted on.
/// - **Concurrent**: two blind batches racing on the SAME key. Neither may come
///   back `Conflict`; each must be `Committed` or `Err`. Which of the two, and
///   whether either errors, is deliberately NOT asserted — that is backend
///   latitude (an optimistic backend retries both to `Committed`; a pessimistic one
///   may report the loser's lock loss as `Err`). The clause forbids exactly one
///   answer: `Ok(Conflict)`.
///
/// A backend whose futures do not actually overlap (redb, whose write
/// transactions serialize) passes the concurrent half trivially — as with
/// [`contract_scan_is_consistent_cut`], the property's teeth are shown against a
/// deliberately-violating store in `tests/demonstrated_red.rs`, not against the
/// backend that cannot make it bite.
pub async fn contract_blind_batch_is_never_conflict(store: &impl MetadataStore) {
    let key = b"blind:k".to_vec();

    // Seed, then blind-overwrite the now-existing key: no preconditions, so the
    // overwrite must land — a store that reports "someone else already wrote this"
    // as a Conflict is swallowing the write.
    store
        .commit(WriteBatch::new().put(key.clone(), "v1"))
        .await
        .unwrap();
    let overwrite = store
        .commit(WriteBatch::new().put(key.clone(), "v2"))
        .await
        .unwrap();
    assert_eq!(
        overwrite,
        CommitOutcome::Committed,
        "a blind overwrite of an existing key must commit — it asserted nothing about \
         the prior value, so there is nothing for it to lose"
    );

    // A conditional writer loses on this key…
    let doomed = store
        .commit(
            WriteBatch::new()
                .require(key.clone(), "STALE")
                .put(key.clone(), "v3"),
        )
        .await
        .unwrap();
    assert_eq!(doomed, CommitOutcome::Conflict, "the CAS writer loses");

    // …and a blind batch on the very same key still commits: the Conflict belonged
    // to the precondition, not to the key.
    let blind_after_conflict = store
        .commit(WriteBatch::new().put(key.clone(), "v4"))
        .await
        .unwrap();
    assert_eq!(
        blind_after_conflict,
        CommitOutcome::Committed,
        "a blind batch must commit even on a key a conditional writer just lost on"
    );

    // A blind delete of an existing key is the same rule on the other verb.
    let blind_delete = store
        .commit(WriteBatch::new().delete(key.clone()))
        .await
        .unwrap();
    assert_eq!(
        blind_delete,
        CommitOutcome::Committed,
        "a blind delete of an existing key must commit"
    );
    assert_eq!(store.get(&key).await.unwrap(), None, "the delete landed");

    // The race: two blind batches on ONE key, driven concurrently. Neither may be
    // reported as a Conflict — that is the whole clause; anything else is latitude.
    let racer = b"blind:race".to_vec();
    let (left, right) = futures_util::future::join(
        store.commit(WriteBatch::new().put(racer.clone(), "left")),
        store.commit(WriteBatch::new().put(racer.clone(), "right")),
    )
    .await;
    let mut anyone_committed = false;
    for (side, result) in [("left", left), ("right", right)] {
        match result {
            Ok(CommitOutcome::Committed) => anyone_committed = true,
            Ok(CommitOutcome::Conflict) => panic!(
                "the {side} blind racer came back Conflict — a batch with no preconditions \
                 must never conflict. Callers that `?` the commit and ignore the \
                 CommitOutcome would read this as success while their write was dropped; \
                 a backend that cannot apply a blind batch owes them an Err"
            ),
            Err(_) => {} // Backend latitude: a lost race on a blind batch may be an Err.
        }
    }

    // `Committed` is a claim about the world, and the clause holds the backend to it: if
    // EITHER racer said Committed, the key must EXIST and hold one of the two racers'
    // values. Absence is legal only when BOTH racers errored — the one case in which
    // nothing was ever claimed to have landed.
    //
    // Asserting only "if a value is present it is one of the two" would let the very bug
    // this clause exists to catch walk straight through: a backend that returns
    // `Ok(Committed)` for a raced blind batch and then drops the write leaves the key
    // absent, so a presence-conditional assertion simply skips and the clause passes.
    // `Conflict` is not the only way to swallow a blind write — lying about `Committed` is
    // the other.
    match store.get(&racer).await.unwrap() {
        Some(value) => assert!(
            value.as_ref() == b"left" || value.as_ref() == b"right",
            "the surviving value must be one of the two racers', not a torn write"
        ),
        None => assert!(
            !anyone_committed,
            "a blind racer returned Committed, but the key is absent — the write was \
             dropped while the caller was told it landed. `Committed` means the batch was \
             applied; a backend that cannot apply a blind batch must return Err"
        ),
    }
}

/// Drive **every** contract in this suite against a fresh store per clause.
///
/// A backend runs the whole contract by calling this ONE function, so there is no
/// per-driver list to drift out of sync: a new `contract_*` added here is picked up by
/// **both** backends automatically. (This is the seam that let the read-consistency
/// clauses run against redb but skip TiKV — the very backend those snapshot properties
/// exist to protect.) `make_store(tag)` yields a fresh, isolated store for each clause —
/// redb hands back a new in-memory db, TiKV a connection scoped to a per-`tag` namespace —
/// the fresh-store-per-clause isolation every clause assumes.
///
/// **This is not the whole suite.** The clauses that need a store whose per-scan cap the
/// driver has lowered cannot be reached through the trait, so they live in
/// [`run_all_cap_scoped`] and a driver calls **both** — that second call is the one a new
/// backend's driver can forget while this one still passes green, so wire them together
/// (every current driver does: redb, both DST sim stores, FDB, TiKV).
pub async fn run_all<S, F, Fut>(mut make_store: F)
where
    S: MetadataStore,
    F: FnMut(&'static str) -> Fut,
    Fut: core::future::Future<Output = S>,
{
    contract_commit_and_get(&make_store("commit_and_get").await).await;
    contract_scan_by_prefix(&make_store("scan_by_prefix").await).await;
    contract_require_absent_gates(&make_store("require_absent").await).await;
    contract_require_value_gates(&make_store("require_value").await).await;
    contract_read_after_commit(&make_store("read_after_commit").await).await;
    contract_rename_race_yields_conflict(&make_store("rename_race").await).await;
    contract_scan_is_consistent_cut(&make_store("scan_consistent_cut").await).await;
    contract_blind_batch_is_never_conflict(&make_store("blind_never_conflict").await).await;
    contract_scan_page_orders_by_raw_bytes(&make_store("scan_page_order").await).await;
    contract_scan_page_cursor_is_exclusive(&make_store("scan_page_cursor").await).await;
    contract_scan_page_walk_terminates_and_is_complete(&make_store("scan_page_terminates").await)
        .await;
    contract_scan_page_no_skip_for_stable_keys(&make_store("scan_page_no_skip").await).await;
    contract_scan_page_limit_bounds_the_page(&make_store("scan_page_limit").await).await;
}

/// Drive every clause that needs a store whose per-scan **cap** the driver has
/// lowered — today the cap-escape clause (#634 legs B and E) and the zero-cap
/// refusal.
///
/// A separate runner, not a fourth argument to [`run_all`], because the cap knob is
/// a per-backend *inherent* method (`RedbMetadataStore::with_scan_cap`,
/// `FdbMetadataStore::with_scan_cap`, …): this suite cannot reach it through the
/// trait seam, so the driver must hand back an already-lowered store. `make_store`
/// therefore takes the cap **the suite asks for** rather than one the driver
/// chooses: the caps are the suite's decision (one clause needs a small positive
/// cap, another needs exactly `0`), so a new cap-scoped clause added *here* is
/// picked up by every driver with no per-driver list — and no per-driver cap — to
/// drift. Same discipline as [`run_all`] otherwise.
pub async fn run_all_cap_scoped<S, F, Fut>(mut make_store: F)
where
    S: MetadataStore,
    F: FnMut(&'static str, usize) -> Fut,
    Fut: core::future::Future<Output = S>,
{
    contract_scan_page_escapes_the_scan_cap(
        &make_store("scan_page_cap_escape", LOWERED_SCAN_CAP).await,
        LOWERED_SCAN_CAP,
    )
    .await;
    contract_scan_page_refuses_a_zero_page_bound(&make_store("scan_page_zero_cap", 0).await, 0)
        .await;
}

/// The suite's own rendering, pinned. Every `scan_page` clause reports a failure
/// through [`escaped`], and the keys it reports on are chosen precisely because they
/// are **not** text (`0x7f`, `0x80`, `0xff`, a multi-byte UTF-8 sequence) — so a
/// renderer that collapsed them would leave a backend author staring at a diff of
/// identical-looking keys in the one clause that seeds both `0x80` and `0xff`. The
/// property is one line of code and is therefore exactly the kind that rots
/// unnoticed: it is asserted here rather than trusted.
#[cfg(test)]
mod rendering_tests {
    use super::{escaped, rendered};
    use bytes::Bytes;

    #[test]
    fn every_byte_is_rendered_reversibly_and_distinctly() {
        // Printable ASCII (and the space) survive as themselves…
        assert_eq!(escaped(b"p:a0 z"), "p:a0 z");
        // …and everything else becomes a two-digit hex escape, never a replacement
        // character: a lossy decode maps `0x80` and `0xff` to the SAME U+FFFD, which
        // is indistinguishable in the clause whose fixture seeds both.
        assert_eq!(escaped(b"p:\x80"), "p:\\x80");
        assert_eq!(escaped(b"p:\xff"), "p:\\xff");
        assert_ne!(escaped(b"p:\x80"), escaped(b"p:\xff"));
        // The control and boundary bytes an ordering fixture actually carries.
        assert_eq!(escaped(b"\x00\x09\x0a\x7f"), "\\x00\\x09\\x0a\\x7f");
        // A multi-byte UTF-8 sequence is rendered per BYTE, so `é` and any other
        // encoding of the same glyph stay distinguishable.
        assert_eq!(escaped("é".as_bytes()), "\\xc3\\xa9");
        assert_eq!(escaped(b""), "");
        // The escape character itself is escaped, or the rendering is not reversible
        // after all: a literal backslash is `is_ascii_graphic`, so passing it through
        // would render the four bytes `\x80` exactly as the single byte 0x80 — a
        // collision inside the escape that the per-byte rule is supposed to prevent.
        // Its absence from the sample set below is how it survived the first time.
        assert_eq!(escaped(b"\\"), "\\\\");
        assert_eq!(escaped(b"p:\\x80"), "p:\\\\x80");
        assert_ne!(escaped(b"p:\\x80"), escaped(b"p:\x80"));
        // Distinct byte strings must render distinctly — the whole point of the
        // escape. A per-byte, prefix-free rendering gives that only once the escape
        // character is itself escaped.
        let samples: [&[u8]; 8] = [
            b"p:a", b"p:a0", b"p:\x7f", b"p:\x80", b"p:\xff", b"p: ", b"p:\\", b"p:\\x80",
        ];
        for (i, left) in samples.iter().enumerate() {
            for right in &samples[i + 1..] {
                assert_ne!(escaped(left), escaped(right), "{left:?} vs {right:?}");
            }
        }
    }

    #[test]
    fn pairs_render_as_key_equals_value() {
        assert_eq!(
            rendered(&[
                (b"p:1".to_vec(), Bytes::from_static(b"v1")),
                (b"p:\x80".to_vec(), Bytes::from_static(b"\xff")),
            ]),
            vec!["p:1=v1".to_string(), "p:\\x80=\\xff".to_string()],
            "a failure message must show the VALUE beside its key — the clauses assert \
             on pairs, so a rendering that dropped the value would report a value \
             mismatch as an unexplained key list"
        );
    }
}
