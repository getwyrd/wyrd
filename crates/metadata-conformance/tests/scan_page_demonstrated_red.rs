//! Demonstrated-red for the normative `scan_page` clauses (#634;
//! `0016:2653-2666`).
//!
//! The clauses run against backends that are *already* correct, where they
//! necessarily pass — so passing proves nothing on its own. Each must be shown to
//! **catch** a deliberately-violating `MetadataStore`, which is what turns a
//! compile-shaped red into evidence: with the violating store in place the matching
//! clause fails **by assertion**, in the same `cargo xtask ci` run, which a build
//! error can never be. Exactly the pattern `tests/demonstrated_red.rs:1-24`
//! established for #419.
//!
//! The violations are the ways the contract's clauses are actually broken, and each
//! is a real implementation shape rather than a strawman:
//!
//! | Violating store | The bug | Caught by |
//! |---|---|---|
//! | [`StringOrderedStore`] | pages ordered by the *decoded* string | clause (a), order |
//! | [`KeysOnlyStore`] | right keys, right cursors, **wrong values** | clause (a), the value half |
//! | [`InclusiveCursorStore`] | `after` treated as inclusive | clause (b), exclusive cursor |
//! | [`BadSuccessorStore`] | resume point *computed* from the cursor (last byte + 1) | clause (b), immediate next key; clause (a), one key per page |
//! | [`NaiveLowerBoundStore`] | a below-the-prefix cursor fed straight to the range read | clause (b), cursor floor |
//! | [`InvertedRangeStore`] | a past-the-prefix cursor fed to a *bounded* range read | clause (b), cursor past the range |
//! | [`EarlyNoneStore`] | `next: None` on a full page | clause (c), termination |
//! | [`OffsetPagingStore`] | LIMIT/OFFSET paging (ignores the key cursor) | clause (d), no-skip |
//! | [`ZeroCapUnboundedStore`] | `min(limit, cap)` with no refusal, so a cap of `0` reads unbounded | the zero-page-bound clause |
//! | [`ScanBackedStore`] | `scan_page` paged out of `scan` — the rejected shim shape | the cap-escape clause |
//! | [`StoppedFillStore`] | the fill loop stops SHORT of its bound, so the honest `next: None` reports a prefix that is not exhausted | clause (c), termination; the page bound; the cap escape |
//!
//! Each `#[should_panic]` test asserts the matching clause goes red against its
//! store; each sibling `*_passes_existing_sequential_contracts` test asserts the
//! SAME store still passes the four pre-existing sequential clauses
//! (`crates/metadata-conformance/src/lib.rs:25-112`) unmodified — together they show
//! the new clauses add discriminating power the old suite lacked, not a
//! differently-shaped restatement of it. Two *conforming* stores close the loop from
//! the other side, so no clause is red-by-construction and none rejects a legal
//! implementation: [`FaithfulPagedStore`] (full pages) and [`ShortPagedStore`] (one
//! pair per page, whatever the caller asked for — the short non-terminal page the
//! contract permits and a fixture is apt to forbid by accident) each pass every new
//! clause.
//!
//! [`ScanBackedStore`] does double duty. It is the shim the trait's required-method
//! decision exists to forbid (#508's 4th attempt), *and* it is the shape all ~34
//! in-workspace test doubles legitimately use — `wyrd_testkit::test_double_scan_page`
//! — so driving the whole clause set through it is how that shared helper's body gets
//! executed rather than merely asserted about.
//!
//! Every store below is dev/test-scope only (`tests/`, never compiled into the
//! library, never shipped, never a real backend) — the same placement discipline
//! `tests/demonstrated_red.rs:11-14` records.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use pollster::block_on;
use wyrd_metadata_conformance as conformance;
use wyrd_traits::{
    page_cursor, page_limit, page_start, BoxError, CommitOutcome, MetadataStore, PageStart, Result,
    ScanCapExceeded, ScanPage, WriteBatch, SCAN_CAP,
};

// ---- The correct core every store below shares -----------------------------
//
// Only `scan_page` differs between the doubles: `get`, `scan` and `commit` are the
// same correct bodies, so a clause that goes red below is reacting to the paging
// bug and to nothing else.

struct Truth {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
    scan_cap: usize,
}

impl Default for Truth {
    fn default() -> Self {
        Self {
            kv: Mutex::new(BTreeMap::new()),
            scan_cap: SCAN_CAP,
        }
    }
}

impl Truth {
    fn with_cap(cap: usize) -> Self {
        Self {
            scan_cap: cap,
            ..Self::default()
        }
    }

    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.kv.lock().unwrap().get(key).cloned()
    }

    /// The ordered range under `prefix` — what a real backend's cursored range read
    /// hands back, and the input every `scan_page` below works from.
    fn range(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Bytes)> {
        self.range_from(Bound::Included(prefix.to_vec()), prefix)
    }

    /// The ordered range from an explicit lower bound, stopping at the first key
    /// that does not carry `prefix` — a faithful model of every backend's range
    /// read, including the way it *ends*. This is what makes the cursor-floor bug
    /// ([`NaiveLowerBoundStore`]) reproducible rather than hypothetical: opened
    /// below the prefix, the range meets a foreign key first and stops at once.
    fn range_from(&self, lower: Bound<Vec<u8>>, prefix: &[u8]) -> Vec<(Vec<u8>, Bytes)> {
        self.kv
            .lock()
            .unwrap()
            .range((lower, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// A **bounded** range read `[lower, upper)` — the shape both *distributed*
    /// backends use, **including the way it refuses an inverted one**. An inverted
    /// range is an `Err` here; the substrates spell it differently and one of them
    /// spells it worse: tikv-client resolves the range against its transaction
    /// buffer with `BTreeMap::range`
    /// (`tikv-client-0.4.0/src/transaction/buffer.rs:129`) and **panics** client-side
    /// on a start past its end, while FoundationDB's key selectors tolerate it. `Err`
    /// is modelled rather than the panic on purpose: it lets the *clause's own
    /// assertion* be what catches this store (a panicking store would abort first,
    /// and the red would prove nothing about the clause) — and a substrate that
    /// panics is caught by the same assertion, only louder.
    ///
    /// The bounded shape is why an unbounded `BTreeMap` range cannot reproduce this
    /// defect at all: redb's range runs to `Unbounded` and stops at the first foreign
    /// key, so it survives a cursor a bounded range read cannot express.
    fn bounded_range(
        &self,
        lower: Bound<Vec<u8>>,
        upper: Option<Vec<u8>>,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let begin = match &lower {
            Bound::Included(key) | Bound::Excluded(key) => key.clone(),
            Bound::Unbounded => Vec::new(),
        };
        if let Some(end) = &upper {
            if begin >= *end {
                return Err(BoxError::from(format!(
                    "inverted range: begin {begin:?} is not below end {end:?} — a bounded \
                     range read cannot express this cursor (tikv-client panics on it; \
                     FoundationDB tolerates it and reads nothing)"
                )));
            }
        }
        let kv = self.kv.lock().unwrap();
        let end_bound = upper.map_or(Bound::Unbounded, Bound::Excluded);
        Ok(kv
            .range((lower, end_bound))
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    /// Complete-or-fail-loud, like every production backend (#262, ADR-0011).
    fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let hits = self.range(prefix);
        if hits.len() > self.scan_cap {
            return Err(BoxError::from(ScanCapExceeded {
                cap: self.scan_cap,
                prefix: prefix.to_vec(),
            }));
        }
        Ok(hits)
    }

    fn commit(&self, batch: &WriteBatch) -> CommitOutcome {
        let mut kv = self.kv.lock().unwrap();
        if !batch
            .preconditions
            .iter()
            .all(|pre| kv.get(&pre.key).cloned() == pre.expected)
        {
            return CommitOutcome::Conflict;
        }
        for key in &batch.deletes {
            kv.remove(key);
        }
        for (key, value) in &batch.puts {
            kv.insert(key.clone(), value.clone());
        }
        CommitOutcome::Committed
    }
}

/// Where every faithful page below starts: strictly after `after`, never before the
/// prefix's own start, and terminal past the prefix's exclusive end — the seam's
/// shared rule (`wyrd_traits::page_start`), so a *faithful* double is faithful for
/// the same reason a backend is. `None` is the terminal arm: nothing under the
/// prefix can follow the cursor, so the caller returns an empty terminal page.
fn faithful_lower_bound(prefix: &[u8], after: Option<&[u8]>) -> Option<Bound<Vec<u8>>> {
    match page_start(prefix, after) {
        PageStart::After(cursor) => Some(Bound::Excluded(cursor.to_vec())),
        PageStart::Prefix => Some(Bound::Included(prefix.to_vec())),
        PageStart::PastPrefix => None,
    }
}

/// The exclusive end of `prefix`'s range — the bound both distributed backends
/// build their range read from (`keyspace::prefix_upper_bound` in
/// `metadata-{fdb,tikv}`), reproduced here so [`InvertedRangeStore`] can be the
/// *bounded* range read they are. `None` when the prefix runs to the end of the
/// keyspace (empty, or all `0xff`).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 0xff {
            *last += 1;
            return Some(end);
        }
        end.pop();
    }
    None
}

// ---- The conforming store: the clauses are satisfiable, not red-by-construction ----

/// A faithful paged store: ordered range, exclusive cursor floored at the prefix,
/// short-page termination, page clamped to `min(limit, cap)` with a bound of zero
/// refused. Every new clause passes against it.
#[derive(Default)]
struct FaithfulPagedStore {
    truth: Truth,
}

impl FaithfulPagedStore {
    fn with_cap(cap: usize) -> Self {
        Self {
            truth: Truth::with_cap(cap),
        }
    }
}

#[async_trait]
impl MetadataStore for FaithfulPagedStore {
    // `get`/`scan`/`commit` are the shared correct core in every store below; only
    // `scan_page` differs, so a clause that goes red is reacting to the paging bug.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        // The terminal arm: a cursor past the prefix's exclusive end is an empty
        // page with no cursor, never a range read.
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let mut items = self.truth.range_from(lower, prefix);
        items.truncate(limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
fn a_faithful_paged_store_passes_every_new_clause() {
    block_on(async {
        conformance::contract_scan_page_orders_by_raw_bytes(&FaithfulPagedStore::default()).await;
        conformance::contract_scan_page_cursor_is_exclusive(&FaithfulPagedStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &FaithfulPagedStore::default(),
        )
        .await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&FaithfulPagedStore::default())
            .await;
        conformance::contract_scan_page_limit_bounds_the_page(&FaithfulPagedStore::default()).await;
        // And the cap-scoped clauses against a second, non-redb implementation: a
        // population its own `scan` refuses whole, walked to completion; and a store
        // whose effective cap is zero, which refuses every page.
        conformance::contract_scan_page_escapes_the_scan_cap(
            &FaithfulPagedStore::with_cap(conformance::LOWERED_SCAN_CAP),
            conformance::LOWERED_SCAN_CAP,
        )
        .await;
        conformance::contract_scan_page_refuses_a_zero_page_bound(
            &FaithfulPagedStore::with_cap(0),
            0,
        )
        .await;
    });
}

// ---- The other conforming store: SHORT pages, and why they are legal --------
//
// A page bound is a bound: the contract caps `items.len()` from above and constrains
// `next`, and it never obliges a store to FILL the page it was asked for. A real one
// stops short for its own reasons — a range read that hit a transaction's byte
// budget, an FDB `more()` that came back after fewer pairs than the limit, a region
// boundary — and answers the short page with the cursor to resume from, which is a
// complete answer.
//
// This store is that behaviour taken to its extreme: **one pair per page**, whatever
// the caller asked for, terminal only when the prefix really is exhausted. It is
// conforming in every clause, so every clause must pass against it — and measured
// against this slice's third iteration, **four** of the seven did not: order, the
// exclusive cursor, no-skip and the page bound all read the whole population out of
// one page or demanded `page.len() == limit`. That is the suite rejecting a legal
// store, which is worse than a missing assertion: it tells a backend author the
// contract says something it does not (#634, iteration-3 review finding).

#[derive(Default)]
struct ShortPagedStore {
    truth: Truth,
}

impl ShortPagedStore {
    fn with_cap(cap: usize) -> Self {
        Self {
            truth: Truth::with_cap(cap),
        }
    }
}

#[async_trait]
impl MetadataStore for ShortPagedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        // The bound is still resolved by the seam, and its refusal still honoured: a
        // short page is a choice about how MUCH to return, never a licence to answer a
        // page bound of zero. The resolved value itself is unused — this store returns
        // one pair, which is inside `min(limit, cap)` for every limit it accepts.
        page_limit(limit, self.truth.scan_cap, prefix)?;
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let mut items = self.truth.range_from(lower, prefix);
        // …and `next` is computed against what this page ACTUALLY returned, so the
        // page is terminal only when the prefix is genuinely exhausted. That is the
        // one thing a short page still owes the caller.
        let exhausted = items.len() <= 1;
        items.truncate(1);
        let next = if exhausted {
            None
        } else {
            items.last().map(|(key, _)| key.clone())
        };
        Ok((items, next))
    }
}

#[test]
fn a_short_paging_store_passes_every_new_clause_too() {
    // The regression this test exists for: a clause whose fixture demanded a full
    // page fails HERE, against a store that has broken nothing. Four of the seven did
    // before the fourth iteration, each clause driven separately to measure it — order
    // (read the whole prefix out of one page), the exclusive cursor (fixed-page
    // equality on the resumed page), no-skip (mutations placed where a full first page
    // would have left the cursor) and the page bound (`len == limit`, twice). The
    // termination, cap-escape and zero-bound clauses already tolerated it: they walk.
    block_on(async {
        conformance::contract_scan_page_orders_by_raw_bytes(&ShortPagedStore::default()).await;
        conformance::contract_scan_page_cursor_is_exclusive(&ShortPagedStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(&ShortPagedStore::default())
            .await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&ShortPagedStore::default()).await;
        conformance::contract_scan_page_limit_bounds_the_page(&ShortPagedStore::default()).await;
        conformance::contract_scan_page_escapes_the_scan_cap(
            &ShortPagedStore::with_cap(conformance::LOWERED_SCAN_CAP),
            conformance::LOWERED_SCAN_CAP,
        )
        .await;
        conformance::contract_scan_page_refuses_a_zero_page_bound(&ShortPagedStore::with_cap(0), 0)
            .await;
    });
}

// ---- Violating store 1: pages ordered by the DECODED string ----------------
//
// The everyday shape of this bug: keys are rendered as text somewhere on the way
// out (a `String`-keyed map, a `sort_by_key(|k| String::from_utf8_lossy(k))`, an
// index that stores decoded names). It is invisible while every key is ASCII —
// which is why the clause seeds keys that are not: `0x80` and `0xff` are not valid
// UTF-8 and a lossy decode maps BOTH to U+FFFD, which sorts *after* `é`.
//
// Order is not cosmetic for a paginated read: the cursor is a *key*, so a page
// ordered differently from the way the cursor compares can leave a key on the wrong
// side of the boundary forever — the silent skip the whole primitive exists to
// prevent.

#[derive(Default)]
struct StringOrderedStore {
    truth: Truth,
}

#[async_trait]
impl MetadataStore for StringOrderedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let mut items = self.truth.range_from(lower, prefix);
        // THE BUG: the page is ordered by the decoded string, not by raw bytes.
        items.sort_by_key(|(key, _)| String::from_utf8_lossy(key).to_string());
        items.truncate(limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "raw byte-lexicographic key")]
fn string_ordered_store_fails_the_order_clause() {
    block_on(conformance::contract_scan_page_orders_by_raw_bytes(
        &StringOrderedStore::default(),
    ));
}

#[test]
fn string_ordered_store_passes_existing_sequential_contracts() {
    // The four pre-existing clauses never call `scan_page`, and `contract_scan_by_prefix`
    // sorts before asserting because `scan`'s order is unspecified — so none of them can
    // observe an ordering bug in the paged read.
    block_on(async {
        conformance::contract_commit_and_get(&StringOrderedStore::default()).await;
        conformance::contract_scan_by_prefix(&StringOrderedStore::default()).await;
        conformance::contract_require_absent_gates(&StringOrderedStore::default()).await;
        conformance::contract_require_value_gates(&StringOrderedStore::default()).await;
    });
}

// ---- Violating store 2: the right keys carrying the WRONG VALUES ------------
//
// The half a paging clause forgets, because a cursor is a *key* and everything the
// walk's machinery touches is a key: this store's order, cursors, page bounds and
// termination are all exactly correct, and every pair it hands back carries bytes
// that are not the ones committed.
//
// The shape is ordinary, not a strawman: a substrate's range read has a **keys-only**
// mode (`tikv_client::Transaction::scan_keys` is precisely that), it is the cheaper
// call, and it is the natural one to reach for when what you are building is a
// *cursor*. The values then have to come from somewhere — here, the empty default;
// elsewhere a stale secondary index, or a zip against a separately-fetched value
// block that is off by one. All three fail the same assertion, which is why one
// double demonstrates the class.
//
// Why it matters as much as a skipped key: the paginated walk exists for the
// `retire:` drain and GC's `orphan:` ledger, and both DECODE what they read. An
// obligation record read as empty bytes is an obligation discharged against nothing
// — the same unbounded retention a skip causes, arriving through the value.

#[derive(Default)]
struct KeysOnlyStore {
    truth: Truth,
}

impl KeysOnlyStore {
    fn with_cap(cap: usize) -> Self {
        Self {
            truth: Truth::with_cap(cap),
        }
    }
}

#[async_trait]
impl MetadataStore for KeysOnlyStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let mut items = self.truth.range_from(lower, prefix);
        items.truncate(limit);
        // THE BUG: the range read asked for keys only, so the values are filled in
        // rather than read. Every key-shaped property of the page survives it.
        let items: Vec<(Vec<u8>, Bytes)> = items
            .into_iter()
            .map(|(key, _)| (key, Bytes::new()))
            .collect();
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "with the value it was committed with")]
fn keys_only_store_fails_the_order_clause_on_its_values() {
    // The clause reads a page whose keys and cursor are beyond reproach; only the
    // pairs' second half is wrong. Before this slice's third iteration the whole
    // `scan_page` clause set discarded values, and this store passed every one of
    // them.
    block_on(conformance::contract_scan_page_orders_by_raw_bytes(
        &KeysOnlyStore::default(),
    ));
}

#[test]
fn keys_only_store_passes_every_clause_that_never_reads_a_value() {
    // The discriminating half. The four pre-existing sequential clauses read through
    // `scan`/`get`, which this store answers correctly, and the zero-page-bound clause
    // asserts a refusal — no page, so no value. Everything else about this store is
    // the faithful body with the values dropped, so ONLY a value assertion can see
    // it: exactly the gap the iteration-3 review found.
    block_on(async {
        conformance::contract_commit_and_get(&KeysOnlyStore::default()).await;
        conformance::contract_scan_by_prefix(&KeysOnlyStore::default()).await;
        conformance::contract_require_absent_gates(&KeysOnlyStore::default()).await;
        conformance::contract_require_value_gates(&KeysOnlyStore::default()).await;
        conformance::contract_scan_page_refuses_a_zero_page_bound(&KeysOnlyStore::with_cap(0), 0)
            .await;
    });
}

#[test]
#[should_panic(expected = "with the value it was committed with")]
fn keys_only_store_fails_the_no_skip_clause_on_its_values() {
    // …and the mutation clause catches it too, through its own stable-set assertion:
    // "returned exactly once" is only half of what a stable key owes the caller, the
    // other half being the bytes it was committed with.
    block_on(conformance::contract_scan_page_no_skip_for_stable_keys(
        &KeysOnlyStore::default(),
    ));
}

// ---- Violating store 3: an INCLUSIVE cursor --------------------------------
//
// The off-by-one a range API invites: `range(after..)` instead of
// `range((Excluded(after), Unbounded))`, or a `>=` where the contract says `>`. The
// boundary key comes back on every lap, so a drain that deletes what it reads makes
// progress by accident and one that does not never terminates at all.

#[derive(Default)]
struct InclusiveCursorStore {
    truth: Truth,
}

#[async_trait]
impl MetadataStore for InclusiveCursorStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        // THE BUG: `Included` where the contract says `Excluded` — the page starts AT
        // the cursor.
        let lower = match page_start(prefix, after) {
            PageStart::After(cursor) => Bound::Included(cursor.to_vec()),
            PageStart::Prefix => Bound::Included(prefix.to_vec()),
            PageStart::PastPrefix => return Ok((Vec::new(), None)),
        };
        let mut items = self.truth.range_from(lower, prefix);
        items.truncate(limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "must start strictly after the cursor")]
fn inclusive_cursor_store_fails_the_exclusive_cursor_clause() {
    block_on(conformance::contract_scan_page_cursor_is_exclusive(
        &InclusiveCursorStore::default(),
    ));
}

#[test]
fn inclusive_cursor_store_passes_existing_sequential_contracts() {
    // No pre-existing clause passes a cursor to anything — `scan` has none.
    block_on(async {
        conformance::contract_commit_and_get(&InclusiveCursorStore::default()).await;
        conformance::contract_scan_by_prefix(&InclusiveCursorStore::default()).await;
        conformance::contract_require_absent_gates(&InclusiveCursorStore::default()).await;
        conformance::contract_require_value_gates(&InclusiveCursorStore::default()).await;
    });
}

// ---- Violating store 4: the resume point is COMPUTED from the cursor --------
//
// The subtlest cursor bug of the set, and the only one that is *strictly after* the
// cursor and still skips: instead of "the first key after `after`", the page opens at
// the cursor's arithmetic successor — its last byte incremented. Every key that has
// the cursor as a **prefix** sorts between the two, so every one of them is stepped
// over, silently, on every lap, forever.
//
// It is the natural implementation whenever the range API in reach offers only an
// *inclusive* lower bound (`range(start..)`, a `seek(key)` cursor, a REST `?from=`
// parameter): you cannot say "excluded", so you compute the next key yourself — and
// incrementing the last byte is the obvious wrong answer. The right one appends a
// `0x00`, the smallest strict extension, which is exactly what TiKV's
// `paging::next_page_start` does (`crates/metadata-tikv/src/lib.rs:435`) — the one
// backend here that resolves the cursor by arithmetic, and the one whose conformance
// run is off-Check.
//
// Nothing in the suite saw it before this clause pair was strengthened: it returns
// perfectly ordered pages, terminates, never duplicates, never exceeds the limit, and
// is *correct* for any population whose keys are all the same length — which is what
// every fixture built from `format!("{i:04}")` produces. It shows up only where a page
// boundary lands on a key that is a strict prefix of the next one.

#[derive(Default)]
struct BadSuccessorStore {
    truth: Truth,
}

/// The cursor's arithmetic successor as this store computes it: increment the last
/// byte (carrying by truncation, the way a prefix upper bound is built). Strictly
/// greater than the cursor, and **not** the next key — `p:a` -> `p:b` steps over
/// `p:a0`, `p:a00`, and everything else the cursor is a prefix of.
fn bad_successor(cursor: &[u8]) -> Vec<u8> {
    let mut next = cursor.to_vec();
    while let Some(last) = next.last_mut() {
        if *last < 0xff {
            *last += 1;
            return next;
        }
        next.pop();
    }
    next
}

#[async_trait]
impl MetadataStore for BadSuccessorStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        // THE BUG: the resume point is computed from the cursor instead of being the
        // first key strictly after it. `Included` is not the error here — an exclusive
        // bound on the same wrong key skips exactly as much.
        let lower = match page_start(prefix, after) {
            PageStart::After(cursor) => Bound::Included(bad_successor(cursor)),
            PageStart::Prefix => Bound::Included(prefix.to_vec()),
            PageStart::PastPrefix => return Ok((Vec::new(), None)),
        };
        let mut items = self.truth.range_from(lower, prefix);
        items.truncate(limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "at the IMMEDIATE next key")]
fn bad_successor_store_fails_the_exclusive_cursor_clause() {
    // Clause (b)'s existing-cursor case: `p:200` extends `p:20`, so resuming at `p:21`
    // returns a page that is strictly after the cursor and missing its first key.
    block_on(conformance::contract_scan_page_cursor_is_exclusive(
        &BadSuccessorStore::default(),
    ));
}

#[test]
#[should_panic(expected = "one key per page")]
fn bad_successor_store_fails_the_order_clause_one_key_per_page() {
    // Clause (a)'s one-key-per-page lap: `p:a` and `p:a0` are adjacent, so at a limit
    // of 1 the boundary lands exactly on the pair — and `p:a0` never comes back. The
    // same store passes the SAME clause's limit-2 and limit-16 reads, which is why the
    // extra lap had to exist.
    block_on(conformance::contract_scan_page_orders_by_raw_bytes(
        &BadSuccessorStore::default(),
    ));
}

#[test]
fn bad_successor_store_passes_every_clause_whose_boundary_never_lands_on_a_prefix() {
    // The discriminating half, and the measurement that says the two clauses above are
    // the ONLY things standing between this store and a clean conformance run: every
    // other clause's SEEDED population is fixed-width (`{i:04}`, `{i:06}`, `walk:c{i}`,
    // `p:{i}`), so no seeded key is a strict prefix of another, the bad successor lands
    // on exactly the right key every time, and the walks are complete. (Clause (d) does
    // create one strict extension mid-walk — its "inserted ahead of the cursor" key —
    // and this store does step over it; that key is the one the contract explicitly
    // leaves unconstrained, so the clause tolerates either outcome, which is why it
    // passes here too.) A suite whose fixtures all looked like that would ship this
    // backend.
    block_on(async {
        conformance::contract_commit_and_get(&BadSuccessorStore::default()).await;
        conformance::contract_scan_by_prefix(&BadSuccessorStore::default()).await;
        conformance::contract_require_absent_gates(&BadSuccessorStore::default()).await;
        conformance::contract_require_value_gates(&BadSuccessorStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &BadSuccessorStore::default(),
        )
        .await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&BadSuccessorStore::default())
            .await;
        conformance::contract_scan_page_limit_bounds_the_page(&BadSuccessorStore::default()).await;
        conformance::contract_scan_page_escapes_the_scan_cap(
            &BadSuccessorStore {
                truth: Truth::with_cap(conformance::LOWERED_SCAN_CAP),
            },
            conformance::LOWERED_SCAN_CAP,
        )
        .await;
        conformance::contract_scan_page_refuses_a_zero_page_bound(
            &BadSuccessorStore {
                truth: Truth::with_cap(0),
            },
            0,
        )
        .await;
    });
}

// ---- Violating store 5: the cursor is NOT floored at the prefix -------------
//
// The most invisible of the set, and the one iteration 1 shipped unverified: the
// exclusive cursor is correct for every cursor *inside* the prefix, and the range
// read is correct too — but a cursor lexicographically BELOW the prefix opens the
// range on an earlier namespace, where the first key does not carry the prefix and
// the range read stops immediately. The answer is an empty page with `next: None`:
// a successful, terminal "the prefix is exhausted" for a prefix whose every key is
// still there. That is the silent skip in the shape a caller cannot detect — no
// error, no duplicate, no partial page.
//
// Reachable whenever a caller resumes a walk with a cursor it did not get from this
// prefix: the drain's persisted cursor after a namespace rename, an empty cursor
// spelled `Some(b"")`, a shared cursor column across namespaces.

#[derive(Default)]
struct NaiveLowerBoundStore {
    truth: Truth,
}

#[async_trait]
impl MetadataStore for NaiveLowerBoundStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        // THE BUG: the cursor goes straight into the range read, with no floor at the
        // prefix — `page_start`'s `Prefix` arm is the decision being skipped here.
        let lower = match after {
            Some(cursor) => Bound::Excluded(cursor.to_vec()),
            None => Bound::Included(prefix.to_vec()),
        };
        let mut items = self.truth.range_from(lower, prefix);
        items.truncate(limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "a cursor below the prefix")]
fn naive_lower_bound_store_fails_the_exclusive_cursor_clause() {
    block_on(conformance::contract_scan_page_cursor_is_exclusive(
        &NaiveLowerBoundStore::default(),
    ));
}

#[test]
fn naive_lower_bound_store_passes_every_clause_that_never_passes_a_low_cursor() {
    // The discriminating half: this store is correct for every cursor at or above the
    // prefix, so order, termination, no-skip and the page bound all pass — and so do
    // the four pre-existing sequential clauses. Only an `after` below the prefix sees
    // it, which is why clause (b) has to drive that input explicitly.
    block_on(async {
        conformance::contract_commit_and_get(&NaiveLowerBoundStore::default()).await;
        conformance::contract_scan_by_prefix(&NaiveLowerBoundStore::default()).await;
        conformance::contract_require_absent_gates(&NaiveLowerBoundStore::default()).await;
        conformance::contract_require_value_gates(&NaiveLowerBoundStore::default()).await;
        conformance::contract_scan_page_orders_by_raw_bytes(&NaiveLowerBoundStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &NaiveLowerBoundStore::default(),
        )
        .await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&NaiveLowerBoundStore::default())
            .await;
        conformance::contract_scan_page_limit_bounds_the_page(&NaiveLowerBoundStore::default())
            .await;
    });
}

// ---- Violating store 6: a past-the-prefix cursor into a BOUNDED range read ---
//
// The mirror image of store 3, and the defect this slice's own second iteration
// shipped on BOTH distributed backends (caught in review, not by the suite — whose
// "past the end" cursor was still inside the prefix's range). The exclusive cursor
// is right, the floor at the prefix is right, and every ordinary walk is right —
// but a cursor at or past the prefix's exclusive upper bound is still treated as a
// lower bound, so the bounded range becomes `[cursor, upper_bound(prefix))` with
// `begin > end`.
//
// A bounded range read is what a *distributed* backend has, and what its substrate
// does with an inverted one is not uniform: tikv-client panics inside its
// transaction-buffer lookup (`BTreeMap::range`, client-side, before any RPC), while
// FoundationDB tolerates it and reads nothing. Leaving the contract to that
// difference is the defect — a store either answers the clause or it does not — and
// on the panicking side a drain that resumed from a cursor persisted under an
// earlier namespace takes the process down every lap, forever.

#[derive(Default)]
struct InvertedRangeStore {
    truth: Truth,
}

impl InvertedRangeStore {
    fn with_cap(cap: usize) -> Self {
        Self {
            truth: Truth::with_cap(cap),
        }
    }
}

#[async_trait]
impl MetadataStore for InvertedRangeStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        // THE BUG, verbatim: the two-way rule iteration 2 shipped — "at or above the
        // prefix ⇒ an exclusive lower bound" — which conflates a cursor INSIDE the
        // range with one PAST its end. `PageStart::PastPrefix` is precisely the arm
        // missing here, and a bounded range read is where the conflation bites.
        let lower = match after.filter(|cursor| *cursor >= prefix) {
            Some(cursor) => Bound::Excluded(cursor.to_vec()),
            None => Bound::Included(prefix.to_vec()),
        };
        let mut items = self
            .truth
            .bounded_range(lower, prefix_upper_bound(prefix), prefix)?;
        items.truncate(limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "must be answered with an empty terminal page, not an error")]
fn inverted_range_store_fails_the_exclusive_cursor_clause() {
    block_on(conformance::contract_scan_page_cursor_is_exclusive(
        &InvertedRangeStore::default(),
    ));
}

#[test]
fn inverted_range_store_passes_every_clause_that_never_passes_a_high_cursor() {
    // The discriminating half: this store is correct for every cursor a walk of its
    // own prefix ever produces — order, termination, no-skip, the page bound and the
    // cap escape all pass, as do the four pre-existing sequential clauses. Only a
    // cursor at or past the prefix's upper bound sees it, which is why clause (b)
    // has to drive that input explicitly rather than trust a walk to produce it.
    block_on(async {
        conformance::contract_commit_and_get(&InvertedRangeStore::default()).await;
        conformance::contract_scan_by_prefix(&InvertedRangeStore::default()).await;
        conformance::contract_require_absent_gates(&InvertedRangeStore::default()).await;
        conformance::contract_require_value_gates(&InvertedRangeStore::default()).await;
        conformance::contract_scan_page_orders_by_raw_bytes(&InvertedRangeStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &InvertedRangeStore::default(),
        )
        .await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&InvertedRangeStore::default())
            .await;
        conformance::contract_scan_page_limit_bounds_the_page(&InvertedRangeStore::default()).await;
        // Including the cap escape: this store is emphatically NOT a `scan()`-backed
        // shim — it reads its own cursored range and walks a population its `scan`
        // refuses whole. The one thing it gets wrong is the cursor past the prefix.
        conformance::contract_scan_page_escapes_the_scan_cap(
            &InvertedRangeStore::with_cap(conformance::LOWERED_SCAN_CAP),
            conformance::LOWERED_SCAN_CAP,
        )
        .await;
    });
}

// ---- Violating store 7: `next: None` on a FULL page ------------------------
//
// "The page is full, so I have nothing more to say" — the shape of an
// implementation that reports the cursor only when it can *prove* more remains, and
// gets the proof wrong at the boundary. Every page it returns is well-formed and
// every key in it is correct; the walk simply stops early and the caller is told the
// prefix is exhausted. That is the silent skip in its purest form: no error, no
// duplicate, just keys that are never seen again.

#[derive(Default)]
struct EarlyNoneStore {
    truth: Truth,
}

#[async_trait]
impl MetadataStore for EarlyNoneStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let mut items = self.truth.range_from(lower, prefix);
        items.truncate(limit);
        // THE BUG: the page is complete and correct, but reports the prefix
        // exhausted — `None` is answered where the contract says `Some(last key)`.
        Ok((items, None))
    }
}

#[test]
#[should_panic(expected = "ended early or repeated itself")]
fn early_none_store_fails_the_termination_clause() {
    block_on(
        conformance::contract_scan_page_walk_terminates_and_is_complete(&EarlyNoneStore::default()),
    );
}

#[test]
fn early_none_store_passes_existing_sequential_contracts() {
    // `scan` returns the whole set in one call, so "the walk stopped early" has no
    // analogue any pre-existing clause could observe.
    block_on(async {
        conformance::contract_commit_and_get(&EarlyNoneStore::default()).await;
        conformance::contract_scan_by_prefix(&EarlyNoneStore::default()).await;
        conformance::contract_require_absent_gates(&EarlyNoneStore::default()).await;
        conformance::contract_require_value_gates(&EarlyNoneStore::default()).await;
    });
}

// ---- Violating store 8: LIMIT/OFFSET paging --------------------------------
//
// The classic: pages counted by *position* rather than keyed by the cursor. It is
// exactly correct on a static population — indistinguishable from a conforming
// implementation — and wrong the moment the set changes under the walk, which is
// precisely the case clause (d) exists for. A key inserted behind the cursor shifts
// the array right and the walk RE-YIELDS a stable key; a key deleted ahead of it
// shifts the array left and the walk SKIPS one. Both break "a key present
// throughout the walk is returned exactly once", and neither is visible without
// mutating mid-walk.

#[derive(Default)]
struct OffsetPagingStore {
    truth: Truth,
    /// How far the current walk has counted — the offset a keyed cursor makes
    /// unnecessary, and the whole bug.
    walked: Mutex<usize>,
}

#[async_trait]
impl MetadataStore for OffsetPagingStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        let all = self.truth.range(prefix);
        let mut walked = self.walked.lock().unwrap();
        if after.is_none() {
            *walked = 0; // a fresh walk restarts the count
        }
        // THE BUG: the cursor's VALUE is ignored; the page is `all[walked..]`.
        let items: Vec<(Vec<u8>, Bytes)> = all.into_iter().skip(*walked).take(limit).collect();
        *walked += items.len();
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "returned exactly once")]
fn offset_paging_store_fails_the_no_skip_clause() {
    block_on(conformance::contract_scan_page_no_skip_for_stable_keys(
        &OffsetPagingStore::default(),
    ));
}

#[test]
fn offset_paging_store_passes_existing_sequential_contracts() {
    block_on(async {
        conformance::contract_commit_and_get(&OffsetPagingStore::default()).await;
        conformance::contract_scan_by_prefix(&OffsetPagingStore::default()).await;
        conformance::contract_require_absent_gates(&OffsetPagingStore::default()).await;
        conformance::contract_require_value_gates(&OffsetPagingStore::default()).await;
    });
}

#[test]
fn offset_paging_store_passes_the_static_population_clauses() {
    // The discriminating half of this double: with nothing mutating under it, offset
    // paging is indistinguishable from a conforming implementation — it walks a
    // static prefix completely, in order, and terminates. Only the mutation clause
    // (d) can see the bug, which is why (d) is not a restatement of (a)–(c).
    block_on(async {
        conformance::contract_scan_page_orders_by_raw_bytes(&OffsetPagingStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &OffsetPagingStore::default(),
        )
        .await;
    });
}

// ---- Violating store 9: `min(limit, cap)` with no refusal -------------------
//
// The defect this slice's first iteration shipped, kept here as a permanent
// regression: resolve the page bound as `min(limit, cap)` and stop the read at
// `items.len() == limit`. Correct for every positive cap — and for a cap of `0`
// (which every cap knob accepts, since a cap may be lowered but never raised) the
// break can never fire, so the page drains the WHOLE prefix. The bound is not merely
// ignored, it is inverted: the one input that should refuse hardest returns the
// largest possible page, which is precisely the unbounded materialization the cap
// exists to stop.

struct ZeroCapUnboundedStore {
    truth: Truth,
}

impl ZeroCapUnboundedStore {
    fn with_cap(cap: usize) -> Self {
        Self {
            truth: Truth::with_cap(cap),
        }
    }
}

#[async_trait]
impl MetadataStore for ZeroCapUnboundedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        // THE BUG: the clamp with no refusal — `page_limit` is the decision skipped.
        let limit = limit.min(self.truth.scan_cap);
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let all = self.truth.range_from(lower, prefix);
        let mut items: Vec<(Vec<u8>, Bytes)> = Vec::new();
        for pair in all {
            items.push(pair);
            // …and the `==` guard that a bound of zero walks straight past.
            if items.len() == limit {
                break;
            }
        }
        let next = if items.len() == limit {
            items.last().map(|(key, _)| key.clone())
        } else {
            None
        };
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "instead of refusing it")]
fn zero_cap_unbounded_store_fails_the_zero_page_bound_clause() {
    block_on(conformance::contract_scan_page_refuses_a_zero_page_bound(
        &ZeroCapUnboundedStore::with_cap(0),
        0,
    ));
}

#[test]
fn zero_cap_unbounded_store_passes_every_clause_at_a_positive_cap() {
    // The discriminating half: at any positive cap this store is a conforming
    // implementation — it passes the four pre-existing clauses, the four ordinary
    // `scan_page` clauses, and the cap-escape clause. (The page-bound clause is the
    // one exception, and not because of the cap: it drives `limit == 0`, which this
    // store answers instead of refusing — the same missing refusal, from the
    // caller's side rather than the store's.) Only a page bound of zero sees the
    // bug, which is why the zero-bound clause is not a restatement of the others.
    block_on(async {
        conformance::contract_commit_and_get(&ZeroCapUnboundedStore::with_cap(SCAN_CAP)).await;
        conformance::contract_scan_by_prefix(&ZeroCapUnboundedStore::with_cap(SCAN_CAP)).await;
        conformance::contract_require_absent_gates(&ZeroCapUnboundedStore::with_cap(SCAN_CAP))
            .await;
        conformance::contract_require_value_gates(&ZeroCapUnboundedStore::with_cap(SCAN_CAP)).await;
        conformance::contract_scan_page_orders_by_raw_bytes(&ZeroCapUnboundedStore::with_cap(
            SCAN_CAP,
        ))
        .await;
        conformance::contract_scan_page_cursor_is_exclusive(&ZeroCapUnboundedStore::with_cap(
            SCAN_CAP,
        ))
        .await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &ZeroCapUnboundedStore::with_cap(SCAN_CAP),
        )
        .await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&ZeroCapUnboundedStore::with_cap(
            SCAN_CAP,
        ))
        .await;
        conformance::contract_scan_page_escapes_the_scan_cap(
            &ZeroCapUnboundedStore::with_cap(conformance::LOWERED_SCAN_CAP),
            conformance::LOWERED_SCAN_CAP,
        )
        .await;
    });
}

// ---- Violating store 10: `scan_page` paged out of `scan` --------------------
//
// The rejected shim (#508's 4th attempt), and the reason `scan_page` is a REQUIRED
// trait method with no default body: a `scan()`-backed page inherits `SCAN_CAP`, so
// it cannot enumerate the namespaces the primitive exists for — past the cap the
// underlying `scan` fails loud and the walk fails whole, exactly as before.
//
// This store is deliberately built on `wyrd_testkit::test_double_scan_page`, the
// shared body all ~34 in-workspace test doubles delegate to. Two things follow, and
// both are the point:
//
// * the cap-escape clause CATCHES the shim shape — so leg B is not a claim about
//   `scan()`-backed bodies, it is a demonstration; and
// * every other clause PASSES, which executes that shared helper's whole contract
//   (byte order out of an unordered `scan`, exclusive cursor floored at the prefix,
//   short-page termination, zero-bound refusal) instead of merely asserting it in a
//   doc comment. A `HashMap` truth makes the ordering half real: this store's `scan`
//   hands back an arbitrary order, so the helper must sort.

#[derive(Default)]
struct ScanBackedStore {
    kv: Mutex<HashMap<Vec<u8>, Bytes>>,
    scan_cap: Option<usize>,
}

impl ScanBackedStore {
    fn with_cap(cap: usize) -> Self {
        Self {
            kv: Mutex::new(HashMap::new()),
            scan_cap: Some(cap),
        }
    }
}

#[async_trait]
impl MetadataStore for ScanBackedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let kv = self.kv.lock().unwrap();
        // `HashMap` iteration order: arbitrary, as a real `scan`'s is (the trait
        // leaves it unspecified) — so the paged read has to impose the order itself.
        let hits: Vec<(Vec<u8>, Bytes)> = kv
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if let Some(cap) = self.scan_cap {
            if hits.len() > cap {
                return Err(BoxError::from(ScanCapExceeded {
                    cap,
                    prefix: prefix.to_vec(),
                }));
            }
        }
        Ok(hits)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        if !batch
            .preconditions
            .iter()
            .all(|pre| kv.get(&pre.key).cloned() == pre.expected)
        {
            return Ok(CommitOutcome::Conflict);
        }
        for key in &batch.deletes {
            kv.remove(key);
        }
        for (key, value) in &batch.puts {
            kv.insert(key.clone(), value.clone());
        }
        Ok(CommitOutcome::Committed)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        // THE SHAPE THE TRAIT FORBIDS OF A BACKEND — and the one every test double
        // legitimately uses, which is why the helper lives in the dev-only testkit
        // crate where no backend can name it.
        wyrd_testkit::test_double_scan_page(self, prefix, after, limit).await
    }
}

#[test]
#[should_panic(expected = "failed: metadata scan exceeded")]
fn a_scan_backed_page_fails_the_cap_escape_clause() {
    // The whole reason `scan_page` has no default body over `scan`: this store's
    // pages are contract-correct in every other respect, and it *still* cannot
    // enumerate a population past the cap — the underlying `scan` fails loud and the
    // walk fails whole, which is the failure mode the primitive was added to remove.
    block_on(conformance::contract_scan_page_escapes_the_scan_cap(
        &ScanBackedStore::with_cap(conformance::LOWERED_SCAN_CAP),
        conformance::LOWERED_SCAN_CAP,
    ));
}

#[test]
fn the_shared_test_double_body_satisfies_every_other_clause() {
    // Executes `wyrd_testkit::test_double_scan_page` — the body behind all ~34
    // delegating doubles — against the whole clause set, so its documented contract
    // is checked rather than asserted. Without this, a `>` slipping to `>=` in that
    // one helper would silently give every double an inclusive cursor.
    block_on(async {
        conformance::contract_commit_and_get(&ScanBackedStore::default()).await;
        conformance::contract_scan_by_prefix(&ScanBackedStore::default()).await;
        conformance::contract_require_absent_gates(&ScanBackedStore::default()).await;
        conformance::contract_require_value_gates(&ScanBackedStore::default()).await;
        conformance::contract_scan_page_orders_by_raw_bytes(&ScanBackedStore::default()).await;
        conformance::contract_scan_page_cursor_is_exclusive(&ScanBackedStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(&ScanBackedStore::default())
            .await;
        conformance::contract_scan_page_no_skip_for_stable_keys(&ScanBackedStore::default()).await;
        conformance::contract_scan_page_limit_bounds_the_page(&ScanBackedStore::default()).await;
    });
}

// ---- Violating store 11: a fill loop that STOPS SHORT of its own bound ------
//
// The production shape behind `wyrd_traits::page_is_full`, and the one this suite
// could not previously be shown to catch on any backend. A page is assembled by a
// loop over the substrate's chunks — FDB's `more()` replies, TiKV's `PAGE_SIZE`
// reads — and the loop stops on its own comparison. Get that comparison wrong by
// one, or stop at the first chunk, and the page comes back SHORT: not malformed, not
// out of order, every key and value correct. The seam then reads "short" as "the
// prefix is exhausted at this instant" and answers `next: None`, so the caller stops
// walking a prefix that is not exhausted and never sees the rest.
//
// It is not hypothetical: with `>=` flipped to `<` inside `FdbMetadataStore::scan_page_once`
// a live 600-key range answered 138 pairs with `next: None`, and the entire
// maintainer conformance leg stayed green — every clause population fits one FDB
// chunk, so nothing at any tier noticed (#634, iteration-5 adversarial review).
//
// Distinct from [`EarlyNoneStore`], which returns a FULL page and lies about the
// cursor; this one never lies — it derives `next` from the page it actually built,
// exactly as a correct backend does. The bug is upstream of the cursor, in where the
// fill stopped, which is why the two decisions are now ONE function
// (`wyrd_traits::page_is_full`): a loop that stops on the seam's rule cannot produce
// this store's answer at all, and one that stops on its own is caught here.
//
// The other half of the demonstration is [`ShortPagedStore`] two hundred lines up: a
// store that also returns one pair per page and is CONFORMING, because it carries the
// cursor. The suite must catch this one and pass that one — the difference between
// them is the whole contract of clause (c).

#[derive(Default)]
struct StoppedFillStore {
    truth: Truth,
}

impl StoppedFillStore {
    fn with_cap(cap: usize) -> Self {
        Self {
            truth: Truth::with_cap(cap),
        }
    }
}

#[async_trait]
impl MetadataStore for StoppedFillStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.truth.commit(&batch))
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let mut items = self.truth.range_from(lower, prefix);
        items.truncate(limit);
        // THE BUG: the fill stops one pair before the bound — an off-by-one in the
        // loop's own fullness test, or a chunk boundary treated as the range's end.
        if items.len() >= limit {
            items.pop();
        }
        // …and `next` is then derived correctly from what the page holds, which is
        // exactly what makes the defect invisible without the population to expose it:
        // a short page is terminal, so this answers `None` in perfect good faith.
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "ended early or repeated itself")]
fn stopped_fill_store_fails_the_termination_clause() {
    block_on(
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &StoppedFillStore::default(),
        ),
    );
}

#[test]
#[should_panic(expected = "never returned")]
fn stopped_fill_store_fails_the_page_bound_clause() {
    // The other side of the same defect: a page that stops short while claiming the
    // prefix is exhausted is caught by `assert_page_is_next_of`, which is what lets a
    // clause tolerate a legal short page (`ShortPagedStore`) without tolerating this
    // one. Two clauses catch it, from the page and from the walk.
    block_on(conformance::contract_scan_page_limit_bounds_the_page(
        &StoppedFillStore::default(),
    ));
}

#[test]
#[should_panic(expected = "must still be enumerable page by page")]
fn stopped_fill_store_fails_the_cap_escape_clause() {
    // And at the cap: a walk that stops short of the bound on every page still makes
    // progress, so it terminates — it just never returns the whole population. The
    // clause that exists for "a namespace past the cap is enumerable at all" is the
    // one that states that loss in those words.
    block_on(conformance::contract_scan_page_escapes_the_scan_cap(
        &StoppedFillStore::with_cap(conformance::LOWERED_SCAN_CAP),
        conformance::LOWERED_SCAN_CAP,
    ));
}

#[test]
fn stopped_fill_store_passes_existing_sequential_contracts() {
    // `scan` returns the whole set in one call, so "the page stopped filling early"
    // has no analogue the pre-existing clauses could observe — the same blind spot the
    // live backends had before the at-scale legs, in miniature.
    block_on(async {
        conformance::contract_commit_and_get(&StoppedFillStore::default()).await;
        conformance::contract_scan_by_prefix(&StoppedFillStore::default()).await;
        conformance::contract_require_absent_gates(&StoppedFillStore::default()).await;
        conformance::contract_require_value_gates(&StoppedFillStore::default()).await;
        conformance::contract_scan_page_refuses_a_zero_page_bound(
            &StoppedFillStore::with_cap(0),
            0,
        )
        .await;
    });
}

// ---- Violating store 12: a recent-write buffer that ignores the cursor ------
//
// The only store here whose defect a *static* population cannot see. Every clause
// that walks an unchanging prefix passes, because the buffer it leaks from is empty
// by the second page; it fails only under concurrent mutation. That is clause (d)'s
// subject — and clause (d) could not see it either until the walk loop asserted the
// exclusive cursor per page, which is the gap this store exists to hold open.
//
// The shape is ordinary: serve a page as "the ordered range from the cursor" UNION
// "whatever sits in the unflushed write buffer under this prefix", so a write that
// has not reached the sorted structure yet is not missed. The union is what a
// memtable-backed store must do; forgetting to filter it by the cursor is the bug.
// What it leaks is exactly the key the contract says may be MISSED — one inserted
// behind the cursor after the cursor passed — and returning it means the page opened
// before the cursor it was handed, which clause (b) forbids of every page, mutation
// or no mutation.
//
// For the retirement drain the leak is not the unbounded retention a skip is, but it
// is not free either: the obligation is re-delivered on a later page, so a drain that
// budgets one page per pass stops making forward progress while a hot key keeps
// landing behind its cursor.

#[derive(Default)]
struct RecentWriteLeakStore {
    truth: Truth,
    /// Keys written since the current walk began — the model's unflushed buffer.
    recent: Mutex<Vec<Vec<u8>>>,
    /// Whether a walk is in flight. A write that arrives before the first page is
    /// already in the sorted structure by the time anyone reads it, so it never sits
    /// in the buffer — which is why every fixture that seeds and *then* walks sees a
    /// faithful store.
    walking: Mutex<bool>,
}

#[async_trait]
impl MetadataStore for RecentWriteLeakStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.truth.get(key))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.truth.scan(prefix)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let outcome = self.truth.commit(&batch);
        if matches!(outcome, CommitOutcome::Committed) && *self.walking.lock().unwrap() {
            self.recent
                .lock()
                .unwrap()
                .extend(batch.puts.iter().map(|(key, _)| key.clone()));
        }
        Ok(outcome)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, self.truth.scan_cap, prefix)?;
        // A walk that opens with no cursor starts a fresh buffer. From here on, writes
        // are buffered — so this store is only ever wrong about writes that arrive
        // DURING a walk, which is what keeps every static-population clause green and
        // makes the mutation clause the only one that can catch it.
        if after.is_none() {
            self.recent.lock().unwrap().clear();
        }
        *self.walking.lock().unwrap() = true;
        let Some(lower) = faithful_lower_bound(prefix, after) else {
            return Ok((Vec::new(), None));
        };
        let mut items = self.truth.range_from(lower, prefix);
        // THE BUG: the buffer is unioned in unfiltered — the cursor is applied to the
        // sorted range and to nothing else. Draining it models the flush that follows:
        // a buffered write is served once and then lives in the sorted structure, so
        // the walk still terminates and the leak is a single page's defect, not a
        // walk-length one. (Leaking on every page would trip the clause's lap budget
        // first and report a non-terminating walk instead of the cursor violation.)
        for key in self.recent.lock().unwrap().drain(..) {
            if !key.starts_with(prefix) || items.iter().any(|(seen, _)| *seen == key) {
                continue;
            }
            if let Some(value) = self.truth.get(&key) {
                items.push((key, value));
            }
        }
        items.sort_by(|(a, _), (b, _)| a.cmp(b));
        items.truncate(limit);
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }
}

#[test]
#[should_panic(expected = "must start strictly AFTER the cursor it was given")]
fn recent_write_leak_store_fails_the_no_skip_clause() {
    // The clause's own mid-walk insert is the write that lands in the buffer, so the
    // fixture needs no cooperation: the next page opens with a key behind its cursor.
    block_on(conformance::contract_scan_page_no_skip_for_stable_keys(
        &RecentWriteLeakStore::default(),
    ));
}

#[test]
fn recent_write_leak_store_passes_every_clause_on_a_static_population() {
    // The discriminating half, and the reason the gap survived review: nothing that
    // walks an unchanging prefix can see this store's defect. Order, termination,
    // completeness and the page bound all pass, as do the four pre-existing
    // sequential clauses — only a write DURING the walk exposes it.
    block_on(async {
        conformance::contract_commit_and_get(&RecentWriteLeakStore::default()).await;
        conformance::contract_scan_by_prefix(&RecentWriteLeakStore::default()).await;
        conformance::contract_require_absent_gates(&RecentWriteLeakStore::default()).await;
        conformance::contract_require_value_gates(&RecentWriteLeakStore::default()).await;
        conformance::contract_scan_page_orders_by_raw_bytes(&RecentWriteLeakStore::default()).await;
        conformance::contract_scan_page_cursor_is_exclusive(&RecentWriteLeakStore::default()).await;
        conformance::contract_scan_page_walk_terminates_and_is_complete(
            &RecentWriteLeakStore::default(),
        )
        .await;
        conformance::contract_scan_page_limit_bounds_the_page(&RecentWriteLeakStore::default())
            .await;
    });
}
