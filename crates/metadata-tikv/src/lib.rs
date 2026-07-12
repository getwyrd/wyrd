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

/// The **operation deadline** that makes "every operation terminates" true on this
/// backend (#517) — the trait's liveness clause (`wyrd_traits::MetadataStore`,
/// "Operational envelope"). Dependency-free like [`keyspace`], so the decisions are
/// unit-tested on every machine, TiKV or not.
///
/// **Why the driver must supply one.** tikv-client 0.4.0 does bound each *RPC
/// attempt*: `Config::timeout` (default **2 s**) is written into the `grpc-timeout`
/// header and enforced client-side by tonic, and the region/store backoffs are finite
/// (`Backoff::no_jitter_backoff(_, _, 10)`), so a blackholed TiKV *store* on an
/// established connection fails in ≈25 s. But two paths escape that bound entirely, and
/// both are on wyrd's hot path:
///
/// 1. **Connection establishment is unbounded.** `SecurityManager::endpoint` never calls
///    tonic's `connect_timeout`, and PD's connect explicitly *discards* the configured
///    timeout (`pd/cluster.rs`'s `connect(&self, addr, _timeout: Duration)`). A connect
///    to a blackholed node is bounded only by the OS TCP handshake (~2 min on Linux
///    defaults), and the PD retry loop multiplies that.
/// 2. **The TSO stream has no deadline at all.** `TimestampOracle::get_timestamp` parks
///    on a `oneshot` with no timeout, awaiting a long-lived bidirectional stream on which
///    no per-RPC timeout is (or can be) set. tikv-client sets no HTTP/2 keepalive
///    interval, so under a blackhole the stream does not break at the application layer —
///    only TCP keepalive eventually tears it down, on the order of **10+ minutes**. And
///    *every* `get`/`scan`/`commit` takes a TSO first (`begin_pessimistic`).
///
/// So an operation that the client believes is bounded can block for ten minutes or more,
/// which for a custodian loop or an S3 request is indistinguishable from a hang. A
/// deadline the driver owns is the only bound available to us; setting `Config::timeout`
/// alone would not close either gap.
pub mod deadline {
    use std::fmt;

    /// Override the operation deadline (milliseconds).
    pub const OPERATION_TIMEOUT_ENV: &str = "WYRD_TIKV_OPERATION_TIMEOUT_MS";

    /// What tikv-client's *own* bounded machinery costs in the worst case it does handle:
    /// ≤11 attempts × the 2 s per-RPC `grpc-timeout`, plus a sub-3 s total backoff. Our
    /// deadline must sit **above** this, or it would cut off the client's legitimate
    /// retries and turn a recoverable region move into a spurious failure.
    pub const CLIENT_BOUNDED_PATH_MS: u64 = 25_000;

    /// The default deadline: twice the client's own bounded path, so its retries have room
    /// while the unbounded connect/TSO paths above are still cut off long before the OS
    /// would notice. (The same shape as the FDB driver's deadline, which is twice
    /// FoundationDB's own 5 s transaction envelope.)
    pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 2 * CLIENT_BOUNDED_PATH_MS;

    /// The deadline must leave the client's own retry machinery intact.
    const _: () = assert!(DEFAULT_OPERATION_TIMEOUT_MS > CLIENT_BOUNDED_PATH_MS);

    /// Resolve the deadline from a raw [`OPERATION_TIMEOUT_ENV`] value. Unset, blank,
    /// unparsable or non-positive falls back to [`DEFAULT_OPERATION_TIMEOUT_MS`].
    #[must_use]
    pub fn operation_timeout_ms(raw: Option<String>) -> u64 {
        raw.and_then(|v| v.trim().parse::<i64>().ok())
            .map(sanitize_operation_timeout_ms)
            .unwrap_or(DEFAULT_OPERATION_TIMEOUT_MS)
    }

    /// Coerce a requested deadline into a *bounded* one: `0` (which would read as "no
    /// deadline") and negatives fall back to the default.
    ///
    /// The deadline is a **liveness constraint, not a tuning knob a caller may switch
    /// off** — the same register as `SCAN_CAP`, which refuses to be raised. A caller who
    /// wants a longer deadline may have one; a caller who wants *none* gets the default.
    #[must_use]
    pub fn sanitize_operation_timeout_ms(ms: i64) -> u64 {
        if ms > 0 {
            #[allow(clippy::cast_sign_loss)] // guarded positive
            {
                ms as u64
            }
        } else {
            DEFAULT_OPERATION_TIMEOUT_MS
        }
    }

    /// A `MetadataStore` operation exceeded the driver's deadline.
    ///
    /// Raised for `get` / `scan` / `connect`, which mutate nothing, so a deadline is a
    /// definite failure and nothing is in doubt. A timed-out **`commit`** is NOT this: it
    /// is by construction an *unknown result* (the commit may still land), so it surfaces
    /// as `wyrd_traits::CommitUnknownResult` instead — the same call the FDB driver makes
    /// for its `1031 transaction_timed_out`.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OperationTimedOut {
        /// The operation that ran out of time (`"get"`, `"scan"`, `"connect"`).
        pub op: &'static str,
        /// The deadline it exceeded.
        pub after_ms: u64,
    }

    impl fmt::Display for OperationTimedOut {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "metadata `{}` exceeded the {} ms operation deadline and was abandoned: \
                 tikv-client bounds each RPC attempt but neither connection establishment \
                 nor the TSO stream, so without this deadline the call could block for \
                 many minutes against an unreachable PD (#517). Nothing was written.",
                self.op, self.after_ms,
            )
        }
    }

    impl std::error::Error for OperationTimedOut {}

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn an_explicit_deadline_wins() {
            assert_eq!(operation_timeout_ms(Some("1500".into())), 1500);
            assert_eq!(operation_timeout_ms(Some("  1500 \n".into())), 1500);
        }

        #[test]
        fn an_absent_blank_or_unparsable_value_falls_back_to_the_default() {
            assert_eq!(operation_timeout_ms(None), DEFAULT_OPERATION_TIMEOUT_MS);
            assert_eq!(
                operation_timeout_ms(Some("  ".into())),
                DEFAULT_OPERATION_TIMEOUT_MS
            );
            assert_eq!(
                operation_timeout_ms(Some("soon".into())),
                DEFAULT_OPERATION_TIMEOUT_MS
            );
        }

        #[test]
        fn the_deadline_cannot_be_switched_off() {
            // `0` would read as "wait forever", which is the very hang this exists to
            // prevent — so it is not an available choice, and neither is a negative.
            assert_eq!(
                operation_timeout_ms(Some("0".into())),
                DEFAULT_OPERATION_TIMEOUT_MS
            );
            assert_eq!(
                operation_timeout_ms(Some("-1".into())),
                DEFAULT_OPERATION_TIMEOUT_MS
            );
            assert_eq!(
                sanitize_operation_timeout_ms(0),
                DEFAULT_OPERATION_TIMEOUT_MS
            );
            assert_eq!(
                sanitize_operation_timeout_ms(-5),
                DEFAULT_OPERATION_TIMEOUT_MS
            );
        }

        // That the default leaves tikv-client's own ≈25 s bounded path room — below it, our
        // deadline would abort legitimate region-move retries and manufacture failures — is
        // pinned at COMPILE time by the `const _: () = assert!(…)` above, which is stricter
        // than a test: it cannot be skipped, and a bad edit does not build.

        #[test]
        fn the_timeout_error_tells_an_operator_nothing_was_written() {
            let msg = OperationTimedOut {
                op: "get",
                after_ms: 50_000,
            }
            .to_string();
            assert!(msg.contains("get"), "names the operation: {msg}");
            assert!(
                msg.contains("Nothing was written"),
                "a read deadline is a definite failure: {msg}"
            );
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
    /// The shared per-`scan` ceiling and its fail-loud error, re-exported from the seam
    /// crate (`wyrd_traits`) where they now live (#516).
    ///
    /// They were defined *here*, and independently in `metadata-fdb`, with identical
    /// values, fields and `Display` — each crate's comment asserting the other's had to
    /// match. Two backends of the same trait must not disagree about how large a listing
    /// may be, and a caller must not have to know which backend it holds to downcast the
    /// error, so the cap and the type are one definition in the seam and every backend
    /// (redb included) raises it. Re-exported under the old paths so callers that name
    /// `wyrd_metadata_tikv::paging::ScanCapExceeded` keep compiling. [`PAGE_SIZE`] and
    /// [`after_page`] stay here: the *cursor* mechanics are this backend's own.
    pub use wyrd_traits::{ScanCapExceeded, SCAN_CAP};

    /// Maximum keys pulled per internal range read — a bounded network round-trip's
    /// worth of dirents. The whole prefix is assembled by *looping* these pages
    /// under one snapshot, not by one unbounded read (proposal 0015 Open questions
    /// "Large-directory `scan` buffering"). Interim (#262): the paging mechanism and
    /// value are Do's call *within* the completeness-or-fail-loud invariant.
    pub const PAGE_SIZE: u32 = 1024;

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

    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use tikv_client::{BoundRange, CheckLevel, Transaction, TransactionClient, TransactionOptions};
    use wyrd_traits::{
        BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, Result, WriteBatch,
    };

    use crate::deadline::{self, OperationTimedOut};
    use crate::keyspace;
    use crate::paging::{self, PageStep, ScanCapExceeded};

    /// Best-effort-roll back a still-active transaction before surfacing `err`.
    ///
    /// tikv-client 0.4.0 drops an unfinished `Transaction` with the default
    /// `CheckLevel::Panic`, so returning a backend error with a bare `?` — while
    /// the pessimistic txn is still open — would abort the process instead of
    /// yielding `Err`. Finishing the txn with a best-effort rollback (a secondary
    /// rollback error is deliberately ignored so it can't mask the original error)
    /// both preserves the caller's error and releases the txn's locks promptly. The
    /// terminal `commit()` needs no such guard: it finalizes the txn whether it
    /// succeeds or fails, so its drop is already safe.
    ///
    /// Since #517 this store opens its transactions with `drop_check(CheckLevel::Warn)`
    /// (see `TikvMetadataStore::begin`), because the operation deadline can **cancel** an
    /// operation's future and drop its txn at any await — a path no rollback can guard,
    /// the future being already gone. So a dropped active txn no longer aborts the process.
    /// This rollback stays, and is still right on every *reachable* error path: it releases
    /// the locks immediately instead of leaving them to expire on their TTL. What changed is
    /// that it is now a promptness measure rather than a crash guard.
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

    /// Whether `err`, returned by **`Transaction::commit`**, leaves the batch's fate
    /// **undetermined** — it may or may not have been applied (#515).
    ///
    /// Percolator commits the transaction the moment the *primary key's* commit record
    /// lands; whether the client learns of it is a separate matter. So a commit-phase
    /// failure where the request may have reached TiKV is possibly-committed — and the
    /// client cannot tell us which phase failed:
    ///
    /// * `tikv_client::Error::UndeterminedError` exists, but the crate raises it **only**
    ///   for `Error::Grpc` — a *connection-establishment* failure, i.e. a definite NON-
    ///   commit — while every dispatched RPC failure, a timed-out commit included, is built
    ///   as `Error::GrpcAPI` (`store/request.rs`'s `impl_request!` does
    ///   `map_err(Error::GrpcAPI)` unconditionally). The one signal the crate offers for
    ///   this class fires on the wrong case and stays silent on the right one. We do not
    ///   rely on it — we treat it as undetermined anyway, which costs a re-read and never a
    ///   wrong answer.
    /// * A prewrite-phase transport error is a definite non-commit; a commit-phase one is
    ///   possibly-committed. **Both surface as the same variants** (`GrpcAPI`,
    ///   `RegionError`), so we cannot separate them from outside and are deliberately
    ///   **conservative**: the whole transport/region class is undetermined. Calling a
    ///   definite non-commit "unknown" costs the caller one re-read; calling a possibly-
    ///   committed batch "definitely not committed" corrupts state. The asymmetry decides
    ///   it.
    ///
    /// A `KeyError` is **not** undetermined: it arrived *inside a response we received*, so
    /// the server reached a decision and told us — a definite rejection (write conflict,
    /// `locked`, `abort`, a rollback record).
    ///
    /// Only a `commit()` error may reach here. A failing `get_for_update` read or eager
    /// pessimistic-lock RPC is always a definite non-commit — no prewrite has happened, so
    /// nothing can have been applied — which is why those sites keep the definite
    /// classifier below.
    /// **The rule is per-LEAF, deliberately.** A multi-region commit reports its per-region
    /// failures wrapped (`ExtractedErrors` / `MultipleKeyErrors`), so ONE aggregate can
    /// carry a write conflict from one region *and* a transport error from another. Asking
    /// "is this whole error a write conflict?" first, as a short-circuit, would answer
    /// **yes** on that mixed aggregate — `is_write_conflict` recurses too — and classify the
    /// batch as a definite `Conflict`, discarding the nested transport error whose region
    /// may well have committed. That is backwards: **unknown outranks conflict**, because
    /// `Conflict` promises *nothing was written* and the mixed case cannot promise it. So a
    /// conflict short-circuits nothing. Each leaf is judged on its own, and any undetermined
    /// leaf makes the whole commit undetermined.
    fn is_undetermined_commit(err: &tikv_client::Error) -> bool {
        use tikv_client::Error;
        match err {
            Error::UndeterminedError(_)
            | Error::Grpc(_)
            | Error::GrpcAPI(_)
            | Error::RegionError(_) => true,
            Error::MultipleKeyErrors(errs) | Error::ExtractedErrors(errs) => {
                errs.iter().any(is_undetermined_commit)
            }
            Error::PessimisticLockError { inner, .. } => is_undetermined_commit(inner),
            // A `KeyError` LEAF — a write conflict, `locked`, `abort`, a rollback record —
            // arrived inside a response we RECEIVED: the server decided and told us.
            // Definite, never unknown.
            _ => false,
        }
    }

    /// The seam error ([`CommitUnknownResult`]) for an undetermined TiKV commit.
    ///
    /// `may_still_commit` is **always true**: the client retries the commit RPC up to 11
    /// times and then gives up, but TiKV may still apply an in-flight commit afterwards, so
    /// a re-read that observes nothing does not prove nothing will land. FoundationDB's
    /// 1021 — "the transaction is already out of flight" — is the stronger guarantee, and
    /// TiKV has no analogue of it. `code` is `None`: the tikv-client error carries no code
    /// for this class.
    fn tikv_unknown_result(err: &tikv_client::Error) -> CommitUnknownResult {
        CommitUnknownResult {
            backend: "tikv",
            code: None,
            detail: err.to_string(),
            may_still_commit: true,
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

    /// Drive `fut` under the store's operation deadline (#517).
    ///
    /// For a **read** (`get`, `scan`) and for `connect`, a deadline is a definite failure —
    /// nothing was written, nothing is in doubt — so it surfaces as the typed
    /// [`OperationTimedOut`]. `commit` does NOT use this: a timed-out commit is by
    /// construction an *unknown result*, so it has its own arm below.
    async fn under_deadline<T>(
        op: &'static str,
        deadline: Duration,
        fut: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        match tokio::time::timeout(deadline, fut).await {
            Ok(result) => result,
            Err(_elapsed) => Err(BoxError::from(OperationTimedOut {
                op,
                #[allow(clippy::cast_possible_truncation)] // the deadline is milliseconds
                after_ms: deadline.as_millis() as u64,
            })),
        }
    }

    /// A **commit** that exceeded the operation deadline is an *unknown result*, never a
    /// definite failure (#517 meeting #515).
    ///
    /// We abandoned the await; TiKV did not abandon the commit. The RPC may already have
    /// been sent, and Percolator commits the moment the primary key's commit record lands
    /// — so the batch may land *after* we gave up. That is precisely `may_still_commit`,
    /// and it is the same call the FDB driver makes for `1031 transaction_timed_out`
    /// (which is likewise "the commit may still be applied"), rather than for 1021.
    ///
    /// Reporting a timed-out commit as a plain fault would be the dangerous lie: a caller
    /// would take "it failed" at face value and never re-read.
    fn timed_out_commit(deadline: Duration) -> CommitUnknownResult {
        CommitUnknownResult {
            backend: "tikv",
            code: None,
            detail: format!(
                "commit exceeded the {} ms operation deadline and the await was abandoned; \
                 the commit RPC may already have been sent",
                deadline.as_millis()
            ),
            may_still_commit: true,
        }
    }

    /// Classify an error returned by **`Transaction::commit`** — the one site where a
    /// failure may leave the batch applied (#515).
    ///
    /// Three outcomes, in this order:
    ///
    /// 1. **Undetermined** ([`is_undetermined_commit`]) → `Err(CommitUnknownResult)`, the
    ///    seam type FoundationDB also raises, so a caller downcasts *once* whatever backend
    ///    it holds. Checked **first**, and for *every* batch shape: an undetermined commit
    ///    is not a lost race, so reporting it as `Conflict` would tell a CAS caller
    ///    "nothing was written" when something may have been.
    /// 2. **A lost race on a conditional batch** → `Ok(Conflict)`, unchanged.
    /// 3. Anything else → `Err`, unchanged.
    ///
    /// **The undetermined arm does not roll back.** `conflict_or_err` rolls back to release
    /// prewrite locks promptly, which is right for a definite failure — but a rollback here
    /// would be an attempt to erase a transaction that may already be committed, and its
    /// result is discarded anyway (`let _ =`), so it could not even inform the answer. We
    /// leave the transaction alone and let TiKV's own lock resolution settle it against the
    /// primary. This is drop-safe: `Transaction::drop` panics only while the status is
    /// `Active` (tikv-client 0.4.0 `transaction.rs`'s `Drop`), and a failed `commit()` has
    /// already moved it to `StartedCommit`.
    ///
    /// The batch is **never retried** — a `WriteBatch` is not guaranteed idempotent, so
    /// re-applying one whose fate is unknown could double-apply it. The caller re-reads.
    async fn commit_outcome_from_error(
        txn: &mut Transaction,
        err: tikv_client::Error,
        conditional: bool,
    ) -> Result<CommitOutcome> {
        if is_undetermined_commit(&err) {
            return Err(BoxError::from(tikv_unknown_result(&err)));
        }
        conflict_or_err(txn, err, conditional).await
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
        /// The deadline every operation runs under (#517) — the trait's "every
        /// operation terminates" clause, which tikv-client cannot supply for us.
        deadline: Duration,
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
            let deadline = Duration::from_millis(deadline::operation_timeout_ms(
                std::env::var(deadline::OPERATION_TIMEOUT_ENV).ok(),
            ));
            // Bounded like every other operation: `TransactionClient::new` connects to PD,
            // and tikv-client sets no connect timeout at all — an unreachable PD would
            // otherwise hang this for as long as the OS takes to give up on the handshake.
            let client = under_deadline("connect", deadline, async {
                TransactionClient::new(pd_endpoints)
                    .await
                    .map_err(BoxError::from)
            })
            .await?;
            Ok(Self {
                client,
                namespace: Vec::new(),
                deadline,
            })
        }

        /// Override the operation deadline (#517). It cannot be switched off — `0` and
        /// negatives fall back to the default, since the deadline is a liveness
        /// constraint, not a knob (see [`deadline::sanitize_operation_timeout_ms`]).
        #[must_use]
        pub fn with_deadline_ms(mut self, ms: i64) -> Self {
            self.deadline = Duration::from_millis(deadline::sanitize_operation_timeout_ms(ms));
            self
        }

        /// This store's effective operation deadline — so the clamp above is observable.
        #[must_use]
        pub fn deadline(&self) -> Duration {
            self.deadline
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

        /// Open the pessimistic transaction every operation runs in — and make it **safe to
        /// drop** (#517, review of #521).
        ///
        /// `begin_pessimistic()` takes `TransactionOptions::new_pessimistic()`, whose
        /// `check_level` is `CheckLevel::Panic` (tikv-client 0.4.0 `transaction.rs`), and
        /// `Transaction::drop` **panics the process** when a still-`Active` transaction is
        /// dropped. That is survivable only while every path either commits or rolls back
        /// explicitly — which is exactly what the operation deadline broke: an elapsed
        /// `tokio::time::timeout` CANCELS the operation's future, and cancelling drops the
        /// `Transaction` wherever the await happened to be. A slow multi-page `scan`, a slow
        /// `get_for_update`, a large conditional batch mid-`put` — any of them, on deadline,
        /// would abort the process instead of returning the timeout error the deadline exists
        /// to produce. A liveness guard that turns a hang into a crash is not a fix.
        ///
        /// So every transaction is opened with `drop_check(CheckLevel::Warn)`: a cancelled
        /// operation drops its transaction with a log line, not a panic. The explicit
        /// rollbacks on the non-cancelled paths are unchanged — they remain the normal route
        /// and still release locks promptly. What is given up on the cancellation path is only
        /// the *eager* release: the locks are left to TiKV's own resolution (a pessimistic
        /// lock carries a TTL, and its heartbeat stops the moment the dropped transaction
        /// stops renewing it). That is the only trade available — rolling back before the drop
        /// would mean issuing an RPC to the very cluster whose unreachability caused the
        /// deadline, which cannot be done from `Drop` (not async) and cannot be awaited (the
        /// future has already been cancelled).
        async fn begin(&self) -> Result<Transaction> {
            self.client
                .begin_with_options(
                    TransactionOptions::new_pessimistic().drop_check(CheckLevel::Warn),
                )
                .await
                .map_err(BoxError::from)
        }
    }

    #[async_trait]
    impl MetadataStore for TikvMetadataStore {
        /// Read `key` at a **fresh TSO snapshot** — one `begin_pessimistic` txn per
        /// call reads at a single PD-assigned timestamp, so the value is current as
        /// of the call (no stale/follower/cached-ts read). See the module-level
        /// read-consistency contract (#261, ADR-0015).
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            under_deadline("get", self.deadline, async {
                let mut txn = self.begin().await?;
                let value = match txn.get(self.physical(key)).await {
                    Ok(value) => value,
                    Err(e) => return Err(rollback_then(&mut txn, e).await),
                };
                txn.rollback().await?;
                Ok(value.map(Bytes::from))
            })
            .await
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
            under_deadline("scan", self.deadline, async {
                let start = self.physical(prefix);
                let upper = keyspace::prefix_upper_bound(&start);
                // ONE transaction (one start timestamp) held across every page — the
                // #261 consistent cut. `begin_pessimistic` reads at a single TSO, so
                // keeping this one txn across all pages IS the snapshot guarantee.
                let mut txn = self.begin().await?;
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
            })
            .await
        }

        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            // The deadline (#517) — but NOT via `under_deadline`, whose error says "nothing
            // was written". A commit we stopped awaiting may still land, so an elapsed
            // deadline here is an UNKNOWN RESULT, exactly as FDB's 1031 is (#515).
            match tokio::time::timeout(self.deadline, self.commit_inner(batch)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(BoxError::from(timed_out_commit(self.deadline))),
            }
        }
    }

    impl TikvMetadataStore {
        async fn commit_inner(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            // A batch WITH preconditions is a CAS: a write-write conflict is a lost race
            // the caller re-reads and retries (`Ok(Conflict)`). A precondition-FREE
            // (blind) batch has no precondition to fail, so a conflict must NOT surface
            // as `Conflict` — the blind writers that use `?` and ignore `CommitOutcome`
            // would silently drop the write; it stays `Err` (see `conflict_or_err`).
            let conditional = !batch.preconditions.is_empty();
            let mut txn = self.begin().await?;

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
                // The ONE site where a failure may leave the batch applied — every other
                // arm above failed before prewrite, so nothing can have landed (#515).
                return commit_outcome_from_error(&mut txn, e, conditional).await;
            }
            Ok(CommitOutcome::Committed)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::time::Duration;

        use tikv_client::{Error, ProtoKeyError, ProtoRegionError};
        use wyrd_traits::{BoxError, CommitUnknownResult};

        use super::{is_undetermined_commit, is_write_conflict, tikv_unknown_result};
        use crate::deadline::OperationTimedOut;

        /// A `KeyError` carrying `conflict` — TiKV's single write-conflict signal.
        fn write_conflict() -> Error {
            let mut ke = ProtoKeyError::default();
            ke.conflict = Some(Default::default());
            Error::KeyError(Box::new(ke))
        }

        /// A `KeyError` that is NOT a conflict (a `locked` key, say) — still a decision the
        /// server sent us inside a response.
        fn other_key_error() -> Error {
            Error::KeyError(Box::new(ProtoKeyError::default()))
        }

        fn region_error() -> Error {
            Error::RegionError(Box::new(ProtoRegionError::default()))
        }

        /// The deadline fires on a read that never completes — the hang this exists to
        /// prevent (an unreachable PD parks the TSO oneshot for 10+ minutes), reproduced
        /// deterministically with a future that simply never resolves. No cluster, no
        /// timing luck: `pending()` cannot finish, so if `under_deadline` did not bound it
        /// this test would hang rather than fail — which is itself the signal.
        #[tokio::test]
        async fn a_read_that_never_completes_is_cut_off_by_the_deadline() {
            let deadline = Duration::from_millis(20);
            let err = super::under_deadline("get", deadline, async {
                std::future::pending::<std::result::Result<(), BoxError>>().await
            })
            .await
            .expect_err("a never-completing read must be cut off, not awaited forever");

            let timed_out = err
                .downcast_ref::<OperationTimedOut>()
                .unwrap_or_else(|| panic!("a deadline must be a typed OperationTimedOut: {err}"));
            assert_eq!(timed_out.op, "get");
            assert_eq!(timed_out.after_ms, 20);
            // A read mutates nothing, so the deadline is a DEFINITE failure…
            assert!(
                err.downcast_ref::<CommitUnknownResult>().is_none(),
                "a read deadline is not an unknown result — nothing was written"
            );
        }

        /// …but a COMMIT deadline is not. We stopped awaiting; TiKV did not stop
        /// committing.
        #[test]
        fn a_commit_deadline_is_an_unknown_result_not_a_definite_failure() {
            let unknown = super::timed_out_commit(Duration::from_millis(50_000));
            assert_eq!(unknown.backend, "tikv");
            assert!(
                unknown.may_still_commit,
                "the commit RPC may already be in flight and may land after we gave up, so \
                 a re-read that sees nothing does not prove nothing will land"
            );
            let msg = unknown.to_string();
            assert!(msg.contains("50000 ms"), "names the deadline: {msg}");
            assert!(
                msg.contains("not retried"),
                "a non-idempotent batch is never replayed on our own timeout: {msg}"
            );
        }

        #[test]
        fn a_transport_or_region_failure_at_commit_is_undetermined() {
            // The commit RPC may have reached TiKV and been applied while the response was
            // lost — and the client cannot tell us whether it failed in prewrite (a definite
            // non-commit) or in the commit phase (possibly committed). Conservative: unknown.
            assert!(is_undetermined_commit(&region_error()));
            assert!(is_undetermined_commit(&Error::UndeterminedError(Box::new(
                region_error()
            ))));
        }

        #[test]
        fn a_server_decision_is_never_undetermined() {
            // A KeyError arrived INSIDE a response we received: the server decided and told
            // us. Treating these as unknown would send every ordinary CAS loser off to
            // re-read for nothing.
            assert!(!is_undetermined_commit(&write_conflict()));
            assert!(!is_undetermined_commit(&other_key_error()));
        }

        #[test]
        fn a_write_conflict_stays_a_conflict_even_when_wrapped() {
            // The conflict classification must survive the wrappers a pessimistic commit
            // puts around it, and must still NOT be read as undetermined — otherwise every
            // lost race would degrade from `Ok(Conflict)` into an unknown-result `Err`,
            // which is the regression this ordering guards.
            let wrapped = Error::ExtractedErrors(vec![write_conflict()]);
            assert!(is_write_conflict(&wrapped));
            assert!(!is_undetermined_commit(&wrapped));
        }

        #[test]
        fn a_transport_error_nested_in_the_wrappers_is_still_undetermined() {
            // A prewrite batch surfaces its per-region failures wrapped; an undetermined one
            // must not be lost in the wrapper.
            let wrapped = Error::MultipleKeyErrors(vec![region_error()]);
            assert!(is_undetermined_commit(&wrapped));
        }

        #[test]
        fn a_mixed_aggregate_is_undetermined_conflict_must_not_mask_it() {
            // A multi-region commit can fail with ONE aggregate carrying a write conflict
            // from one region and a transport error from another. `is_write_conflict`
            // recurses, so it answers `true` here — and an earlier draft short-circuited on
            // that and reported `Ok(Conflict)`, throwing away a nested error whose region
            // may well have committed. `Conflict` promises nothing was written; this
            // aggregate cannot promise it. Unknown outranks conflict.
            let mixed = Error::ExtractedErrors(vec![write_conflict(), region_error()]);
            assert!(
                is_write_conflict(&mixed),
                "the conflict predicate does see the conflict leaf — which is exactly why \
                 it must not be allowed to short-circuit the undetermined check"
            );
            assert!(
                is_undetermined_commit(&mixed),
                "a conflict leaf must NOT mask an undetermined leaf: a caller told \
                 `Conflict` retries a CAS whose fate is still ambiguous"
            );
            // Order-independent: the classifier must not depend on which leaf comes first.
            let mixed_reversed = Error::MultipleKeyErrors(vec![region_error(), write_conflict()]);
            assert!(is_undetermined_commit(&mixed_reversed));
        }

        #[test]
        fn the_unknown_result_error_is_the_seam_type_and_says_a_re_read_is_not_conclusive() {
            let unknown = tikv_unknown_result(&region_error());
            assert_eq!(unknown.backend, "tikv");
            assert_eq!(
                unknown.code, None,
                "the tikv-client error carries no code for this class"
            );
            assert!(
                unknown.may_still_commit,
                "TiKV may apply a commit RPC the client gave up on, so a re-read that sees \
                 nothing does not prove nothing will land — unlike FoundationDB's 1021"
            );
            let msg = unknown.to_string();
            assert!(
                msg.contains("not retried"),
                "states it refused to retry: {msg}"
            );
            assert!(
                msg.contains("may still be applied"),
                "states the weaker guarantee a re-read cannot settle: {msg}"
            );
        }
    }
}

#[cfg(feature = "tikv")]
pub use store::TikvMetadataStore;
