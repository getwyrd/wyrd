//! TiKV-backed [`MetadataStore`](wyrd_traits::MetadataStore): the distributed,
//! **production** metadata backend (ADR-0008), behind the *unchanged*
//! `MetadataStore` trait. Choosing it over embedded redb is composition in
//! `server` (ADR-0010), not a refactor here — the milestone's whole thesis
//! (proposal 0007).
//!
//! The basic `get` / `scan` / `commit` shapes over TiKV's transactional API, so
//! the **shared** conformance suite that redb passes also passes against a real
//! TiKV, landed in the **M4.1 skeleton** (proposal 0007 §"Suggested PR sequence"
//! item 1). The rigorous atomic-commit conflict semantics (`get_for_update`
//! locking discipline hardening, write-conflict → `Conflict` classification,
//! version-CAS-under-contention) are **M4.2** (#253). The **native, internally
//! paged** prefix scan — one consistent snapshot across all pages, fail-loud on an
//! interim cap rather than truncate — plus the documented `get`/`scan`
//! read-consistency contract are **M4.3** (#254; proposal 0015 §"Native prefix
//! scan", #261/#262). The paging/cap **decision** logic is factored into the
//! dependency-free [`paging`] module; the read-consistency contract is documented
//! on the `store` module.
//!
//! The real backend is compiled only under the `tikv` feature. It is **off by
//! default** so a machine with no TiKV (a laptop or a PDCA worktree) builds this
//! crate as an empty skeleton, never pulls in the pre-1.0 `tikv-client` tree, and
//! keeps `cargo xtask ci` green; the dedicated `xtask tikv-conformance` job turns
//! it on and drives the shared suite against the throwaway `deploy/` TiKV.

#![forbid(unsafe_code)]

/// Keyspace math shared by the store's read/scan/commit paths — kept free of any
/// `tikv-client` dependency so it compiles and is unit-tested on every machine,
/// TiKV or not (the load-light production unit).
pub mod keyspace {
    /// Prefix a logical key with the store's `namespace`, yielding the physical
    /// key actually stored in TiKV. An empty namespace is the identity (the
    /// production default); a per-test namespace gives the isolated "fresh store"
    /// each shared-suite clause expects.
    pub fn physical(namespace: &[u8], key: &[u8]) -> Vec<u8> {
        let mut physical = Vec::with_capacity(namespace.len() + key.len());
        physical.extend_from_slice(namespace);
        physical.extend_from_slice(key);
        physical
    }

    /// Strip the `namespace` back off a physical key read from TiKV, recovering
    /// the logical key the trait exposes. Returns `None` if the physical key is
    /// not under this namespace (so a foreign key is never misattributed).
    pub fn logical(namespace: &[u8], physical: &[u8]) -> Option<Vec<u8>> {
        physical.strip_prefix(namespace).map(<[u8]>::to_vec)
    }

    /// The **exclusive** upper bound of the half-open range that covers every key
    /// beginning with `prefix`: the prefix with its last non-`0xff` byte
    /// incremented and the trailing `0xff`s dropped. `None` means "no upper bound"
    /// — an empty prefix, or an all-`0xff` prefix, scans to the end of the
    /// keyspace. This turns `scan(prefix)` into a bounded TiKV range
    /// `[prefix, upper)` instead of a whole-table filter.
    pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
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

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn physical_and_logical_round_trip() {
            let ns = b"conformance/42/".as_slice();
            let phys = physical(ns, b"inode:7");
            assert!(phys.starts_with(ns));
            assert_eq!(logical(ns, &phys).as_deref(), Some(&b"inode:7"[..]));
            // The empty namespace is the identity used in production.
            assert_eq!(physical(b"", b"k"), b"k");
            assert_eq!(logical(b"", b"k").as_deref(), Some(&b"k"[..]));
        }

        #[test]
        fn logical_rejects_a_foreign_key() {
            // A key not under this namespace must not be misattributed to it.
            assert_eq!(logical(b"ns-a/", b"ns-b/inode:1"), None);
        }

        #[test]
        fn prefix_upper_bound_is_the_exclusive_end_of_the_prefix_range() {
            // Ordinary prefix: last byte incremented.
            assert_eq!(prefix_upper_bound(b"p:").as_deref(), Some(&b"p;"[..]));
            // `b':'` (0x3a) -> `b';'` (0x3b), so [b"p:", b"p;") covers p:1, p:2, …
            assert!(b"p:1".as_slice() >= b"p:".as_slice());
            assert!(b"p:1".as_slice() < b"p;".as_slice());
            assert!(b"q:1".as_slice() >= b"p;".as_slice());
        }

        #[test]
        fn prefix_upper_bound_carries_over_trailing_0xff() {
            // Trailing 0xff bytes are dropped and the prior byte is bumped.
            assert_eq!(
                prefix_upper_bound(&[0x01, 0xff]).as_deref(),
                Some(&[0x02][..])
            );
            // Empty prefix and all-0xff prefix scan to the end of the keyspace.
            assert_eq!(prefix_upper_bound(b""), None);
            assert_eq!(prefix_upper_bound(&[0xff, 0xff]), None);
        }
    }
}

/// Internal paging + fail-loud completeness for the native prefix `scan` (proposal
/// 0015 §"Native prefix scan", §"Suggested PR sequence" item 3, Open questions
/// "Large-directory `scan` buffering"). Kept free of any `tikv-client` dependency —
/// exactly like [`keyspace`] — so the paging/cap **decisions** (cursor advance,
/// short-page termination, cap-breach → error) compile and are unit-tested on every
/// machine, TiKV or not (the load-light production unit the store's `scan` drives).
///
/// The store's `scan` holds **one** transaction — a single consistent read
/// timestamp, the #261 consistent cut — across **every** internal page and drives
/// the loop with [`after_page`]. The invariant is **completeness-or-fail-loud**
/// (#262 / ADR-0011): a `scan(prefix)` returns the *complete* matching set observed
/// at one snapshot, or `Err` — it **never** returns a silently truncated `Vec` (a
/// truncated `inode:` scan corrupts GC's never-reclaim safety set — data loss).
pub mod paging {
    use std::fmt;

    /// Maximum keys pulled per internal range read — a bounded network round-trip's
    /// worth of dirents. The whole prefix is assembled by *looping* these pages
    /// under one snapshot, not by one unbounded read (proposal 0015 Open questions
    /// "Large-directory `scan` buffering"). Interim (#262): the paging mechanism and
    /// value are Do's call *within* the completeness-or-fail-loud invariant.
    pub const PAGE_SIZE: u32 = 1024;

    /// Interim ceiling on the **total** materialized results of a single `scan`. On
    /// breach the call fails loud (`Err`, via [`ScanCapExceeded`]) and returns **no**
    /// partial `Vec`: a silently truncated `inode:` scan corrupts GC's never-reclaim
    /// safety set (data loss), so this is a **correctness constraint, not a tuning
    /// knob** (#262). 2^20 dirents is far past any legitimate single directory yet
    /// bounds the gateway heap against a pathological prefix; it is revisited if a
    /// paginated/streaming trait method is measured in (out of M4's unchanged-trait
    /// scope). A product-facing "max dirents per listing" is the human's to confirm
    /// (INTEGRATION §4 / #262).
    pub const SCAN_CAP: usize = 1 << 20;

    /// The interim per-`scan` cap was exceeded. Returned as the store's `Err` so the
    /// scan **fails loud instead of truncating** (#262); the operator-visible
    /// ADR-0011 audit signal is surfaced by the caller (GC/custodian), which already
    /// owns the telemetry path — `metadata-tikv` carries no tracing dependency today,
    /// so pushing the emit into the store would be a new-dependency ADR-0003 review.
    /// A descriptive typed error keeps the audit signal caller-side and lets a caller
    /// downcast to distinguish "too big, fail loud" from a genuine backend fault.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ScanCapExceeded {
        /// The interim cap that was breached.
        pub cap: usize,
        /// The logical prefix whose scan overflowed (lossy-rendered for operators).
        pub prefix: Vec<u8>,
    }

    impl fmt::Display for ScanCapExceeded {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "metadata scan exceeded the interim per-listing cap of {} keys for \
                 prefix {:?}: failing loud rather than returning a truncated result set \
                 (a silently truncated scan is data loss — #262, ADR-0011)",
                self.cap,
                String::from_utf8_lossy(&self.prefix),
            )
        }
    }

    impl std::error::Error for ScanCapExceeded {}

    /// The next page's **inclusive** start key: the last key of the page just read
    /// with a `0x00` byte appended — the smallest key strictly greater than
    /// `last_key`. So the next page never re-yields `last_key` and never skips a key
    /// between them, because no key sorts strictly between `k` and `k || 0x00`.
    #[must_use]
    pub fn next_page_start(last_key: &[u8]) -> Vec<u8> {
        let mut next = Vec::with_capacity(last_key.len() + 1);
        next.extend_from_slice(last_key);
        next.push(0x00);
        next
    }

    /// What the paged `scan` loop does after reading one page.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PageStep {
        /// The cap was breached — fail loud, return **no** partial `Vec` (#262).
        CapExceeded,
        /// The bounded range is exhausted — the accumulated set is complete.
        Done,
        /// Continue the scan from this next (inclusive) start key.
        Continue(Vec<u8>),
    }

    /// Decide whether a paged `scan` continues, is complete, or must fail loud, given
    /// the running `total` materialized so far, the just-read page's length
    /// `page_len`, and its physical `last_key`.
    ///
    /// Order matters: the cap is checked **first**, so an over-cap scan fails loud
    /// even on what would otherwise be its final (short) page — an over-cap result
    /// set can never slip through as a "complete" short page. A **short** page (fewer
    /// than `page_size`, including empty) means the range is exhausted → `Done`; a
    /// **full** page means there may be more → `Continue` past its last key.
    #[must_use]
    pub fn after_page(
        total: usize,
        page_len: usize,
        last_key: Option<&[u8]>,
        page_size: u32,
        cap: usize,
    ) -> PageStep {
        if total > cap {
            return PageStep::CapExceeded;
        }
        match last_key {
            Some(k) if page_len >= page_size as usize => PageStep::Continue(next_page_start(k)),
            _ => PageStep::Done,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn next_page_start_is_the_smallest_key_strictly_after_the_last() {
            let last = b"inode:42";
            let next = next_page_start(last);
            // Strictly greater than the key already returned (never re-yielded)...
            assert!(next.as_slice() > last.as_slice());
            // ...and nothing sorts between them, so the paging skips no key.
            assert_eq!(next, b"inode:42\x00");
        }

        #[test]
        fn a_full_page_continues_past_its_last_key() {
            assert_eq!(
                after_page(1024, 1024, Some(b"p:last"), 1024, SCAN_CAP),
                PageStep::Continue(b"p:last\x00".to_vec())
            );
        }

        #[test]
        fn a_short_page_ends_the_scan_complete() {
            // Fewer than a full page => the bounded range is exhausted.
            assert_eq!(
                after_page(10, 10, Some(b"p:z"), 1024, SCAN_CAP),
                PageStep::Done
            );
            // An empty page (no last key) likewise terminates cleanly.
            assert_eq!(after_page(0, 0, None, 1024, SCAN_CAP), PageStep::Done);
        }

        #[test]
        fn a_breach_of_the_cap_fails_loud_never_truncates() {
            // total past the cap => CapExceeded regardless of page fullness — the
            // caller must turn this into `Err`, never a partial Vec (#262).
            assert_eq!(after_page(6, 5, Some(b"k"), 5, 5), PageStep::CapExceeded);
            // Exactly at the cap is allowed — a full page there still continues, so a
            // set of size == cap that ends on a page boundary is NOT a false breach.
            assert_eq!(
                after_page(5, 5, Some(b"k"), 5, 5),
                PageStep::Continue(b"k\x00".to_vec())
            );
        }

        #[test]
        fn cap_is_checked_before_termination() {
            // Even a short (would-be-final) page fails loud once it pushes total over
            // the cap: an over-cap scan can never slip through as a "complete" short
            // page and silently truncate.
            assert_eq!(after_page(7, 2, Some(b"k"), 1024, 5), PageStep::CapExceeded);
        }

        #[test]
        fn scan_cap_exceeded_error_is_operator_visible() {
            let err = ScanCapExceeded {
                cap: SCAN_CAP,
                prefix: b"inode:".to_vec(),
            };
            let msg = err.to_string();
            assert!(
                msg.contains("inode:"),
                "names the overflowing prefix: {msg}"
            );
            assert!(
                msg.contains("truncated"),
                "states it refused to truncate: {msg}"
            );
        }
    }
}

#[cfg(feature = "tikv")]
mod store {
    //! # Read-consistency contract (#261, ADR-0015 — the M4.3 decision)
    //!
    //! The `MetadataStore` trait promises nothing about `get`/`scan` snapshot
    //! semantics (proposal 0015 Open questions "Read consistency to document"); this
    //! backend pins it explicitly:
    //!
    //! - **Fresh-TSO snapshot per call.** Each `get` and each `scan` opens **one**
    //!   `begin_pessimistic` transaction, which reads at a single PD-assigned start
    //!   timestamp (TiKV snapshot isolation, ADR-0015). Reads are always current as
    //!   of the call — **no** stale / follower / cached-timestamp reads. That
    //!   relaxation is a *future* cross-zone behaviour behind the `meta:version`
    //!   fence (ADR-0015 Option C); read-your-writes (ADR-0015 clause 3) likewise
    //!   only becomes cross-zone behaviour later. M4 rejects them.
    //! - **One consistent cut across all pages of a `scan`.** Because `scan` pages
    //!   the range internally, all pages are read inside that *one* transaction, at
    //!   the *one* start timestamp — so a `scan` observes a single consistent cut,
    //!   never a torn read stitched across timestamps (#261). This is *required*
    //!   precisely because the read is now paged.
    //! - **Completeness or fail-loud, never truncation.** A `scan` returns the
    //!   complete matching set or `Err` (see [`crate::paging`]); it never returns a
    //!   silently truncated `Vec` (#262, ADR-0011).
    //! - **`rename`'s read-then-commit is safe by re-check, not by read freshness.**
    //!   `rename` reads *outside* the commit txn; its correctness rests on the
    //!   commit's precondition re-check under the **locking rule** — every
    //!   precondition key is re-read with `get_for_update` inside the one commit txn
    //!   (see [`TikvMetadataStore::commit`]; proposal 0015 §"the mandatory rule",
    //!   ADR-0015) — not on the freshness of the earlier read. So this read-snapshot
    //!   contract does **not** alter `commit`; it documents why the split read/commit
    //!   pattern is already sound.

    use async_trait::async_trait;
    use bytes::Bytes;
    use tikv_client::{BoundRange, Transaction, TransactionClient};
    use wyrd_traits::{BoxError, CommitOutcome, MetadataStore, Result, WriteBatch};

    use crate::keyspace;
    use crate::paging::{self, PageStep, ScanCapExceeded};

    /// Best-effort-roll back a still-active transaction before surfacing `err`.
    ///
    /// tikv-client 0.4.0 drops an unfinished `Transaction` with the default
    /// `CheckLevel::Panic`, so returning a backend error with a bare `?` — while
    /// the pessimistic txn is still open — would abort the process instead of
    /// yielding `Err`. Finishing the txn with a best-effort rollback (a secondary
    /// rollback error is deliberately ignored so it can't mask the original error)
    /// both preserves the caller's error and leaves no active txn to panic on drop.
    /// The terminal `commit()` needs no such guard: it finalizes the txn whether it
    /// succeeds or fails, so its drop is already safe.
    async fn rollback_then(txn: &mut Transaction, err: tikv_client::Error) -> BoxError {
        let _ = txn.rollback().await;
        err.into()
    }

    /// Is `err` a TiKV **write-write conflict** — a lost race — rather than a
    /// genuine fault?
    ///
    /// The load-bearing partition of the commit contract (`crates/traits/src/lib.rs`
    /// `CommitOutcome`; proposal 0007 §"The semantic translation — two conflict
    /// signals, one outcome, faults stay faults"): a losing writer is
    /// `Ok(Conflict)`, everything else (network, region-unavailable, PD-timeout,
    /// lock-resolution, deadlock, undetermined) stays `Err`. The single TiKV signal
    /// for a write-write race is `Error::KeyError` carrying a `conflict:
    /// Some(WriteConflict)` (kvrpcpb `KeyError.conflict`, tikv-client 0.4.0). Under
    /// a pessimistic txn that key error can arrive wrapped: `commit()` prewrite
    /// batches surface it as `ExtractedErrors`/`MultipleKeyErrors`, and a
    /// `get_for_update` lock failure as `PessimisticLockError`. We recurse into the
    /// wrappers — **any** wrapped write-conflict makes the whole error a conflict —
    /// but we deliberately do **not** treat any other `KeyError` (`locked`,
    /// `deadlock`, `abort`, …) as a conflict: only `conflict.is_some()`.
    fn is_write_conflict(err: &tikv_client::Error) -> bool {
        use tikv_client::Error;
        match err {
            Error::KeyError(ke) => ke.conflict.is_some(),
            Error::MultipleKeyErrors(errs) | Error::ExtractedErrors(errs) => {
                errs.iter().any(is_write_conflict)
            }
            Error::PessimisticLockError { inner, .. } => is_write_conflict(inner),
            _ => false,
        }
    }

    /// Finish `txn` and classify `err` as either a lost race (`Ok(Conflict)`) or a
    /// fault (`Err`).
    ///
    /// A write-write conflict becomes `Ok(Conflict)` **only for a `conditional` batch**
    /// — one carrying preconditions, i.e. a CAS the caller re-reads and retries on
    /// `Conflict`. A precondition-**free** (blind) batch has no precondition to have
    /// failed, and `CommitOutcome::Conflict` is *defined* as "a precondition did not
    /// hold" (`crates/traits/src/lib.rs`); reporting `Conflict` there would let the many
    /// blind writers that use `?` and ignore the `CommitOutcome` (e.g.
    /// `core::repair::enqueue_repair`, custodian desired-state set/clear) read success
    /// while their write was silently dropped. So an unconditional conflict stays `Err`
    /// — the caller *sees* the lost race. Non-conflict faults are always `Err`.
    ///
    /// Best-effort roll back first so no active txn is left to panic on drop and any
    /// prewrite locks release promptly (a secondary rollback error is ignored so it
    /// can't mask the original). Drop-safety holds on every arm it guards (proposal
    /// 0007 / verified backend facts): after a `get_for_update`/`put`/`delete` error the
    /// txn is still `Active`, so the rollback is what makes it drop-safe; after a failed
    /// `commit()` the txn is already past `Active` (it moves to `StartedCommit` before
    /// its RPC), so it is drop-safe regardless and `rollback` accepts `StartedCommit`,
    /// releasing the prewrite locks a losing writer left.
    async fn conflict_or_err(
        txn: &mut Transaction,
        err: tikv_client::Error,
        conditional: bool,
    ) -> Result<CommitOutcome> {
        let _ = txn.rollback().await;
        if conditional && is_write_conflict(&err) {
            Ok(CommitOutcome::Conflict)
        } else {
            Err(err.into())
        }
    }

    /// A [`MetadataStore`] backed by a TiKV cluster, reached through its
    /// transactional API (Percolator 2PC coordinated by PD). Metadata keys/values
    /// are stored **byte-identically** — TiKV never interprets a key or value —
    /// so the application-level version CAS (a full-value precondition) is exact.
    pub struct TikvMetadataStore {
        client: TransactionClient,
        /// Prepended to every key. Empty in production; a per-test value gives the
        /// isolated keyspace each shared-suite clause needs.
        namespace: Vec<u8>,
    }

    impl TikvMetadataStore {
        /// Connect to TiKV via its PD (Placement Driver) endpoints.
        ///
        /// NOTE (proposal 0007 Open questions; research issue #260): the exact
        /// `tikv-client` 0.4.x entry points (`TransactionClient::new`,
        /// `begin_pessimistic`, `get_for_update`, the `commit` write-conflict
        /// error path) are reconfirmed at build time against the pinned version —
        /// this crate compiles only under `--features tikv`.
        pub async fn connect(pd_endpoints: Vec<String>) -> Result<Self> {
            let client = TransactionClient::new(pd_endpoints).await?;
            Ok(Self {
                client,
                namespace: Vec::new(),
            })
        }

        /// Scope this store to an isolated `namespace` (used by the conformance
        /// suite to give each clause a fresh keyspace against one shared cluster).
        #[must_use]
        pub fn with_namespace(mut self, namespace: impl Into<Vec<u8>>) -> Self {
            self.namespace = namespace.into();
            self
        }

        fn physical(&self, key: &[u8]) -> Vec<u8> {
            keyspace::physical(&self.namespace, key)
        }
    }

    #[async_trait]
    impl MetadataStore for TikvMetadataStore {
        /// Read `key` at a **fresh TSO snapshot** — one `begin_pessimistic` txn per
        /// call reads at a single PD-assigned timestamp, so the value is current as
        /// of the call (no stale/follower/cached-ts read). See the module-level
        /// read-consistency contract (#261, ADR-0015).
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            let mut txn = self.client.begin_pessimistic().await?;
            let value = match txn.get(self.physical(key)).await {
                Ok(value) => value,
                Err(e) => return Err(rollback_then(&mut txn, e).await),
            };
            txn.rollback().await?;
            Ok(value.map(Bytes::from))
        }

        /// Native, **internally paged** prefix scan (proposal 0015 §"Native prefix
        /// scan", §"Suggested PR sequence" item 3).
        ///
        /// The bounded range `[prefix, prefix_upper)` is read in `PAGE_SIZE`-key
        /// pages **inside one `begin_pessimistic` transaction** — one fixed read
        /// timestamp across every page, so the whole materialized set is a single
        /// consistent cut (#261, ADR-0015; see the module-level contract). The loop
        /// advances the cursor strictly past the last key of each full page until a
        /// short page ends the range.
        ///
        /// **Completeness or fail-loud (#262, ADR-0011):** the full matching set is
        /// returned, or — if the interim [`paging::SCAN_CAP`] is breached — `Err`
        /// ([`ScanCapExceeded`]). It **never** returns a silently truncated `Vec`; a
        /// truncated `inode:` scan would corrupt GC's never-reclaim safety set (data
        /// loss). Order stays unspecified (callers collect into a set/map).
        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            let start = self.physical(prefix);
            let upper = keyspace::prefix_upper_bound(&start);
            // ONE transaction (one start timestamp) held across every page — the
            // #261 consistent cut. `begin_pessimistic` reads at a single TSO, so
            // keeping this one txn across all pages IS the snapshot guarantee.
            let mut txn = self.client.begin_pessimistic().await?;
            let mut out: Vec<(Vec<u8>, Bytes)> = Vec::new();
            let mut cursor = start;
            loop {
                let range: BoundRange = match &upper {
                    Some(end) => (cursor.clone()..end.clone()).into(),
                    None => (cursor.clone()..).into(),
                };
                let page: Vec<_> = match txn.scan(range, paging::PAGE_SIZE).await {
                    Ok(page) => page.collect(),
                    Err(e) => return Err(rollback_then(&mut txn, e).await),
                };
                let page_len = page.len();
                let mut last_physical: Option<Vec<u8>> = None;
                for pair in page {
                    let physical: Vec<u8> = pair.0.into();
                    if let Some(logical) = keyspace::logical(&self.namespace, &physical) {
                        out.push((logical, Bytes::from(pair.1)));
                    }
                    last_physical = Some(physical);
                }
                match paging::after_page(
                    out.len(),
                    page_len,
                    last_physical.as_deref(),
                    paging::PAGE_SIZE,
                    paging::SCAN_CAP,
                ) {
                    PageStep::CapExceeded => {
                        // Fail loud — return NO partial Vec (#262). Roll back first
                        // so no active txn is left to panic on drop (see
                        // `rollback_then`); a secondary rollback error is ignored so
                        // it cannot mask the cap-breach error the caller must see.
                        let _ = txn.rollback().await;
                        return Err(BoxError::from(ScanCapExceeded {
                            cap: paging::SCAN_CAP,
                            prefix: prefix.to_vec(),
                        }));
                    }
                    PageStep::Continue(next) => cursor = next,
                    PageStep::Done => break,
                }
            }
            txn.rollback().await?;
            Ok(out)
        }

        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            // A batch WITH preconditions is a CAS: a write-write conflict is a lost race
            // the caller re-reads and retries (`Ok(Conflict)`). A precondition-FREE
            // (blind) batch has no precondition to fail, so a conflict must NOT surface
            // as `Conflict` — the blind writers that use `?` and ignore `CommitOutcome`
            // would silently drop the write; it stays `Err` (see `conflict_or_err`).
            let conditional = !batch.preconditions.is_empty();
            let mut txn = self.client.begin_pessimistic().await?;

            // Read + byte-compare every precondition INSIDE the one transaction, so
            // preconditions and mutations are all-or-nothing across keys.
            // `get_for_update` is a LOCKING read, so a precondition on a key the
            // batch only reads is still conflict-checked (the proposal's locking
            // rule); the rigorous write-conflict CLASSIFICATION under contention is
            // hardened in M4.2 (#253).
            for pc in &batch.preconditions {
                // A `get_for_update` lock failure can itself be a write-write race
                // (the concurrent winner already holds/moved the lock), so classify
                // it — a losing writer here is a `Conflict`, not a fault. (`conditional`
                // is necessarily true in this loop.)
                let current = match txn.get_for_update(self.physical(&pc.key)).await {
                    Ok(current) => current,
                    Err(e) => return conflict_or_err(&mut txn, e, conditional).await,
                };
                let holds = match &pc.expected {
                    Some(expected) => current.as_deref() == Some(expected.as_ref()),
                    None => current.is_none(),
                };
                if !holds {
                    // Best-effort cleanup: a rollback fault here must not mask the
                    // legitimate precondition-miss `Conflict` we are returning.
                    let _ = txn.rollback().await;
                    return Ok(CommitOutcome::Conflict);
                }
            }

            // A pessimistic `put`/`delete` does NOT merely buffer: it eagerly acquires
            // the pessimistic lock (an RPC) before buffering. So a mutated key that no
            // precondition already locked can lose a write-write race right here. For a
            // CAS batch (`conditional`) that is the trait's `Conflict`; for a blind batch
            // it stays `Err` (a blind write must never be silently reported `Conflict`).
            // A genuine backend fault always falls through to `Err`.
            for (key, value) in &batch.puts {
                if let Err(e) = txn.put(self.physical(key), value.to_vec()).await {
                    return conflict_or_err(&mut txn, e, conditional).await;
                }
            }
            for key in &batch.deletes {
                if let Err(e) = txn.delete(self.physical(key)).await {
                    return conflict_or_err(&mut txn, e, conditional).await;
                }
            }

            // Prewrite can lose a write-write race to a concurrent committer; for a CAS
            // batch that is the trait's `Conflict`, for a blind batch it stays `Err`. Any
            // other commit error stays `Err` (faults, undetermined) — the delicate
            // partition proposal 0007 calls out.
            if let Err(e) = txn.commit().await {
                return conflict_or_err(&mut txn, e, conditional).await;
            }
            Ok(CommitOutcome::Committed)
        }
    }
}

#[cfg(feature = "tikv")]
pub use store::TikvMetadataStore;
