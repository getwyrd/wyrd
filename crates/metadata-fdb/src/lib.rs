//! FoundationDB-backed [`MetadataStore`](wyrd_traits::MetadataStore): the production
//! distributed metadata backend (**ADR-0042**, superseding ADR-0008), behind the
//! *unchanged* `MetadataStore` trait. Selecting it over embedded redb is composition in
//! `server` (ADR-0010), not a refactor here — so this crate adds an implementation and
//! changes no trait (`crates/traits/src/lib.rs:338`).
//!
//! The `get` / `scan` / `commit` shapes over FDB's transactional API, so the **shared**
//! conformance suite redb and TiKV pass (`crates/metadata-conformance/src/lib.rs:291`,
//! `run_all`) also passes against a real `fdbserver`. Nothing in the live path constructs
//! this store yet — backend *selection* in `crates/server/src/cli.rs:133-140` is a later,
//! explicitly-blocked issue — so the contract is held by the three cluster-file-gated test
//! binaries, which drive this real driver against a real cluster over the shared suite.
//!
//! # The commit contract, mapped onto FDB
//!
//! The trait's normative rule (`crates/traits/src/lib.rs:346-350`): `commit` returns
//! `Ok(CommitOutcome::Conflict)` **exactly when** a precondition did not hold, `Err` for a
//! backend fault, and **never** `Conflict` for a batch carrying no preconditions — a blind
//! write must not be silently swallowed by the many callers that `?` the result and ignore
//! the `CommitOutcome`. FDB gives two distinct ways for a precondition to fail:
//!
//! 1. **Observed miss.** The precondition key is read *inside* the commit transaction and
//!    its value does not match. Returned as `Ok(Conflict)` directly.
//! 2. **Lost race.** The precondition *held* at the transaction's read version, but a
//!    concurrent writer committed to that key first. Because the precondition read is a
//!    **non-snapshot** read it entered the transaction's *read-conflict set*, so FDB's
//!    resolver rejects the commit with error **1020 `not_committed`** — which
//!    [`classify::classify_commit_error`] maps to `Ok(Conflict)`, but **only** for a
//!    conditional batch.
//!
//! ## What actually keeps a **blind** batch out of `Conflict`
//!
//! Be precise here, because the obvious answer is wrong. The `conditional` argument
//! threaded into [`classify::classify_commit_error`] looks like the whole of the
//! invariant's third clause, and it is **not**: on the blind commit path
//! `classify_commit_error(1020, false)` is **structurally unreachable**. FDB reports 1020
//! as `retryable_not_committed`, so `store`'s blind retry gate claims it *before* any
//! classification happens. The guard there is **defence-in-depth** — it costs nothing, it
//! makes the rule legible at the one place a reader looks for it, and it would catch a
//! future refactor that routed 1020 to the classifier — but no test can pin it, because
//! no production input reaches it. Claiming otherwise would be the exact vacuity this
//! crate's tests exist to avoid.
//!
//! Three *reachable* mechanisms hold the clause, each with a test that goes red without it:
//!
//! 1. **The routing rule.** `commit` sends a precondition-free batch down the blind path
//!    (`store::commit_path`, the rule TiKV states as `let conditional =
//!    !batch.preconditions.is_empty()`, `crates/metadata-tikv/src/lib.rs:546`). Force it
//!    to the conditional path and a blind batch inherits CAS classification *and* loses
//!    its bounded retry of `1007 transaction_too_old` / `1009 future_version`. Pinned by
//!    `store::tests::a_blind_batch_routes_to_the_blind_path` through the production
//!    `store::route_commit`.
//! 2. **The retry gate.** `store::blind_commit_step` re-applies only
//!    `retryable_not_committed` errors, so `1021 commit_unknown_result` — the one that may
//!    already have landed — is never re-applied. Pinned by `store::tests` on the
//!    production `store::blind_commit_loop`.
//! 3. **The exhaustion error.** A blind batch that loses `MAX_ATTEMPTS` races in a row
//!    surfaces `RetryBudgetExhausted` (an `Err`, carrying the last `FdbError` as `source`)
//!    — never `Ok(Conflict)`. Pinned by
//!    `store::tests::a_blind_commit_that_keeps_losing_is_err_never_conflict`.
//!
//! Underneath all three, FoundationDB itself makes the clause hard to breach: a write-only
//! transaction has an empty read-conflict set, so the resolver cannot reject it with 1020
//! at all. `crates/metadata-fdb/tests/contention.rs` grounds that against the live server
//! rather than trusting it.
//!
//! **1021 `commit_unknown_result` is never retried and never `Conflict`.** A `WriteBatch`
//! is not guaranteed idempotent, so a commit whose outcome the client cannot determine is
//! surfaced as the distinguishable [`classify::CommitUnknownResult`] error. This is
//! exactly why the `foundationdb` crate's `Database::run` closure-retry is **not** used
//! here: it re-runs the closure on 1021. A healthy `fdbserver` cannot be made to emit 1021
//! on demand, so this rule is pinned by the blind-commit-loop unit tests in `store`, which
//! drive the *production* retry loop and its retry gate with a real `FdbError`, rather than
//! by a live test that could only re-assert the pure classifier.
//!
//! # Every operation terminates
//!
//! FoundationDB's C client has **no** timeout by default and never gives up on a cluster it
//! cannot reach — it retries the connection forever, so an awaited `get`, `get_range` or
//! `commit` future simply never resolves. A wrong cluster file or an FDB outage would hang a
//! metadata operation rather than fail it. So `store`'s `trx` — the one place a transaction
//! is created — puts a deadline on every transaction
//! ([`config::DEFAULT_TRANSACTION_TIMEOUT_MS`], overridable via
//! [`config::TRANSACTION_TIMEOUT_ENV`]); `crates/metadata-fdb/tests/timeout.rs` drives the
//! production `get` and `commit` against an unreachable cluster and fails if either hangs.
//!
//! The deadline sits **above** FoundationDB's own five-second transaction envelope, on
//! purpose. A transaction that reached the cluster dies at five seconds with `1007
//! transaction_too_old` — retryable, and definitively *not* committed. The deadline instead
//! yields `1031 transaction_timed_out` ([`classify::TRANSACTION_TIMED_OUT`]), which FDB's own
//! guide says carries *no* guarantee: the commit may have been sent and may land later. Ours
//! is the worse error, so FDB's is given every chance to win the race, and 1031 fires only
//! when the client cannot reach the cluster at all. When it does fire on a commit it joins
//! 1021 in [`classify::CommitUnknownResult`] — never retried, never `Conflict` — and
//! [`classify::CommitUnknownResult::may_still_commit`] tells the two apart, because after
//! 1021 a re-read settles the outcome and after 1031 it does not.
//!
//! # `scan`: completeness or fail loud
//!
//! The prefix `scan` reads its bounded range inside one transaction, following FDB's
//! `more()` paging, and is bounded by [`paging::SCAN_CAP`]. The invariant is
//! **completeness-or-fail-loud** (#262, ADR-0011), reproduced from the sibling backend
//! (`crates/metadata-tikv/src/lib.rs:136-145`): a `scan` returns the *complete* matching
//! set observed at one read version, or `Err` ([`paging::ScanCapExceeded`]) — it **never**
//! returns a silently truncated `Vec`, because a truncated `inode:` scan corrupts GC's
//! never-reclaim safety set (data loss). The shared suite's scan clause fits in a single
//! page and stores three keys, so it can see neither the paging loop nor the cap; both are
//! pinned by `crates/metadata-fdb/tests/scan.rs` against the live server.
//!
//! # Errors a caller can tell apart
//!
//! Every `Err` this store returns is downcastable to something more specific than a
//! string: a bare `foundationdb::FdbError` for a single backend fault,
//! [`classify::CommitUnknownResult`] for 1021, [`paging::ScanCapExceeded`] for a cap
//! breach, and `RetryBudgetExhausted` — whose `source()` is the last `FdbError` — when a
//! bounded retry ran out. No path returns a `String`-backed error, so a caller can always
//! tell a transient `transaction_too_old` from a permanent `value_too_large`.
//!
//! The real backend is compiled only under the `fdb` feature. It is **off by default**, so
//! a machine with no FoundationDB (a laptop or a PDCA worktree) builds this crate as a
//! dependency-free skeleton — [`classify`], [`config`], [`keyspace`] and [`paging`] still
//! compile and their unit tests still run — never links `libfdb_c`, and keeps `cargo xtask
//! ci` green; the dedicated `xtask fdb-conformance` job turns it on and drives the shared
//! suite against the throwaway `deploy/fdb-single-node` cluster.

// NOT `forbid`: booting FoundationDB's client network thread is an `unsafe fn` in the
// `foundationdb` crate (there is no safe alternative — `api::NetworkBuilder::boot` is
// `unsafe` too), so exactly one `unsafe` block exists in this crate, in `store`. `deny`
// keeps every other line unsafe-free while letting that one site opt in explicitly.
#![deny(unsafe_code)]

/// Classification of an FDB commit error into the trait's `CommitOutcome` partition — the
/// load-bearing rule of this crate, kept free of any `foundationdb` dependency so it
/// compiles and is unit-tested on **every** machine, FDB or not (the load-light production
/// unit the store's `commit` drives). Mirrors the `keyspace`/`paging` precedent in
/// `crates/metadata-tikv/src/lib.rs:31,126`.
///
/// The rule reproduced here is TiKV's (`crates/metadata-tikv/src/lib.rs:542-546`): `let
/// conditional = !batch.preconditions.is_empty();` and a lost race is `Conflict` **only**
/// for a conditional batch.
pub mod classify {
    use std::fmt;

    /// FDB error `1020 not_committed`: "Transaction not committed due to conflict with
    /// another transaction." The single FDB signal for a lost read-write race — the
    /// resolver found a key in this transaction's *read-conflict set* that a concurrent
    /// transaction wrote after this one's read version.
    pub const NOT_COMMITTED: i32 = 1020;

    /// FDB error `1021 commit_unknown_result`: the commit may or may not have succeeded and
    /// the client cannot tell. **Never** retried by this driver (a `WriteBatch` is not
    /// guaranteed idempotent, so a blind retry can double-apply) and **never** `Conflict`.
    pub const COMMIT_UNKNOWN_RESULT: i32 = 1021;

    /// FDB error `1031 transaction_timed_out`: the transaction outlived the deadline this
    /// driver sets on every transaction it creates
    /// ([`crate::config::DEFAULT_TRANSACTION_TIMEOUT_MS`]).
    ///
    /// On the **commit** path this is an undeterminable outcome, exactly like
    /// [`COMMIT_UNKNOWN_RESULT`] — and **strictly weaker**. FoundationDB's own guide is
    /// explicit that a timed-out transaction lacks the one guarantee `commit_unknown_result`
    /// gives: *"if the commit has already been sent to the database, the transaction could
    /// get committed at a later point in time"*
    /// (<https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results>).
    /// Where 1021 promises the transaction is out of flight, 1031 promises nothing. So it is
    /// **never** retried and **never** `Conflict`.
    ///
    /// It is only reachable because this driver bounds every transaction in time. Without a
    /// timeout FoundationDB's C client waits for an unreachable cluster forever, so a
    /// metadata `get` / `scan` / `commit` would hang rather than return `Err` — the deadline
    /// buys termination, and this error code is its price.
    pub const TRANSACTION_TIMED_OUT: i32 = 1031;

    /// What a failed FDB commit means in terms of the `MetadataStore` commit contract
    /// (`crates/traits/src/lib.rs:346-350`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CommitClass {
        /// A precondition of a **conditional** batch lost a race:
        /// `Ok(CommitOutcome::Conflict)`.
        Conflict,
        /// The commit's outcome is undeterminable: `Err(CommitUnknownResult)`.
        /// Distinguishable from a plain fault, and never retried.
        UnknownResult,
        /// A backend fault: `Err`.
        Fault,
    }

    /// Map an FDB commit-error `code` onto the commit contract, given whether the batch
    /// carried preconditions (`conditional`).
    ///
    /// Order is load-bearing:
    ///
    /// 1. **The undeterminable outcomes first.** `1021 commit_unknown_result` and `1031
    ///    transaction_timed_out` are `UnknownResult` for *every* batch, conditional or not.
    ///    Neither is a lost race — the write may well have landed — so reporting either as
    ///    `Conflict` would tell a CAS caller "nothing was written" when something may have
    ///    been, and retrying it could double-apply a non-idempotent batch.
    /// 2. **1020 only when `conditional`.** A conditional batch that loses a race on a
    ///    `require`d key is the trait's `Conflict`; the caller re-reads and retries (e.g.
    ///    `alloc_inode`'s budgeted backoff loop, `crates/server/src/cli.rs:1027-1049`). A
    ///    **blind** batch has no precondition to have failed, so `Conflict` is not even a
    ///    meaningful answer: it stays `Fault` (`Err`), and the caller *sees* the loss
    ///    instead of a `?`-only caller silently dropping the write.
    /// 3. Everything else is a `Fault`.
    ///
    /// Only *commit* errors reach here (the single callsite is `store`'s
    /// `outcome_from_commit_error`, fed by `Transaction::commit`). That is what makes clause
    /// 1 correct for 1031: a timeout raised while **reading** a precondition, before any
    /// commit was sent, is a definite non-commit and is surfaced by `?` as a plain
    /// `FdbError` without ever passing through this function.
    #[must_use]
    pub fn classify_commit_error(code: i32, conditional: bool) -> CommitClass {
        if code == COMMIT_UNKNOWN_RESULT || code == TRANSACTION_TIMED_OUT {
            return CommitClass::UnknownResult;
        }
        if conditional && code == NOT_COMMITTED {
            return CommitClass::Conflict;
        }
        CommitClass::Fault
    }

    /// A commit whose outcome FoundationDB could not determine ([`COMMIT_UNKNOWN_RESULT`] or
    /// [`TRANSACTION_TIMED_OUT`]): the batch may or may not have been applied.
    ///
    /// Surfaced as the store's `Err` — a **distinguishable** typed error a caller can
    /// downcast to — rather than retried, because a `WriteBatch` is not guaranteed
    /// idempotent (re-applying a blind `put` is harmless, but re-applying a batch that
    /// bumps a version counter is not, and the trait admits both).
    ///
    /// The two codes are not equally bad, so [`code`](Self::code) is carried rather than
    /// discarded and [`may_still_commit`](Self::may_still_commit) reads the difference off
    /// it: 1021 guarantees the transaction is no longer in flight, 1031 guarantees nothing.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CommitUnknownResult {
        /// The FDB error code that reported the undeterminable outcome.
        pub code: i32,
    }

    impl CommitUnknownResult {
        /// Whether the cluster may still apply this batch **after** the error was returned.
        ///
        /// `false` for [`COMMIT_UNKNOWN_RESULT`], whose guarantee is that the transaction is
        /// already out of flight — so a re-read establishes the outcome once and for all.
        /// `true` for [`TRANSACTION_TIMED_OUT`], where the commit may have been sent and may
        /// land later: a re-read that sees nothing does **not** prove nothing will land.
        #[must_use]
        pub fn may_still_commit(self) -> bool {
            self.code == TRANSACTION_TIMED_OUT
        }
    }

    impl fmt::Display for CommitUnknownResult {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "metadata commit returned an unknown result (FoundationDB error {}): the \
                 batch may or may not have been applied. It is not retried — a WriteBatch \
                 is not guaranteed idempotent — and it is not a Conflict; the caller must \
                 re-read to establish what happened.",
                self.code,
            )?;
            if self.may_still_commit() {
                write!(
                    f,
                    " The commit timed out rather than reporting an unknown result, so it \
                     may still be applied AFTER this error: a re-read that observes nothing \
                     does not prove the batch will never land.",
                )?;
            }
            Ok(())
        }
    }

    impl std::error::Error for CommitUnknownResult {}

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_conditional_batch_that_loses_a_race_is_a_conflict() {
            assert_eq!(
                classify_commit_error(NOT_COMMITTED, true),
                CommitClass::Conflict
            );
        }

        #[test]
        fn a_blind_batch_is_never_a_conflict() {
            // The invariant's third clause: a precondition-free batch has no precondition
            // to have failed, so 1020 must NOT be laundered into `Ok(Conflict)` — a
            // `?`-only caller would read that as success and drop the write.
            assert_eq!(
                classify_commit_error(NOT_COMMITTED, false),
                CommitClass::Fault
            );
        }

        #[test]
        fn commit_unknown_result_is_distinguishable_for_every_batch() {
            // 1021 outranks the conflict rule: it is never `Conflict`, conditional or not,
            // and its own class keeps it out of every retry path.
            assert_eq!(
                classify_commit_error(COMMIT_UNKNOWN_RESULT, true),
                CommitClass::UnknownResult
            );
            assert_eq!(
                classify_commit_error(COMMIT_UNKNOWN_RESULT, false),
                CommitClass::UnknownResult
            );
        }

        #[test]
        fn a_timed_out_commit_is_an_unknown_result_for_every_batch() {
            // The driver bounds every transaction in time, so a commit can time out. FDB
            // gives a timed-out transaction *weaker* guarantees than 1021 — it may still
            // land later — so it must never be `Conflict` (a CAS caller would retry a batch
            // that may already have been applied) and never a bare `Fault` (which reads as
            // a definite non-commit, like `2103 value_too_large`).
            assert_eq!(
                classify_commit_error(TRANSACTION_TIMED_OUT, true),
                CommitClass::UnknownResult
            );
            assert_eq!(
                classify_commit_error(TRANSACTION_TIMED_OUT, false),
                CommitClass::UnknownResult
            );
        }

        #[test]
        fn any_other_error_is_a_fault_for_every_batch() {
            // 1007 transaction_too_old, 1038 database_locked, 2103 value_too_large: faults,
            // never conflicts, whatever the batch shape. Each is *definitively* not
            // committed, which is what separates them from 1021 / 1031 above.
            for code in [1007, 1038, 2103] {
                assert_eq!(classify_commit_error(code, true), CommitClass::Fault);
                assert_eq!(classify_commit_error(code, false), CommitClass::Fault);
            }
        }

        #[test]
        fn unknown_result_error_is_operator_visible() {
            let msg = CommitUnknownResult {
                code: COMMIT_UNKNOWN_RESULT,
            }
            .to_string();
            assert!(msg.contains("1021"), "names the error code: {msg}");
            assert!(
                msg.contains("not retried"),
                "states it refused to retry: {msg}"
            );
        }

        #[test]
        fn only_a_timeout_may_still_commit_after_the_error() {
            // The distinction a caller acts on: after 1021 a re-read settles the question;
            // after 1031 it does not, because the commit may still be in flight.
            let unknown = CommitUnknownResult {
                code: COMMIT_UNKNOWN_RESULT,
            };
            let timed_out = CommitUnknownResult {
                code: TRANSACTION_TIMED_OUT,
            };
            assert!(!unknown.may_still_commit());
            assert!(timed_out.may_still_commit());

            let msg = timed_out.to_string();
            assert!(msg.contains("1031"), "names the error code: {msg}");
            assert!(
                msg.contains("may still be applied"),
                "states the weaker guarantee a re-read cannot settle: {msg}"
            );
            assert!(
                !unknown.to_string().contains("may still be applied"),
                "1021 keeps its stronger guarantee: {unknown}"
            );
        }
    }
}

/// Cluster-file and transaction-deadline configuration, owned by this driver's own
/// constructor in this slice (no `server`-side selection arm exists yet). Pure input →
/// output, so it is unit-tested with no FDB present.
pub mod config {
    /// The environment variable naming the FoundationDB cluster file.
    pub const CLUSTER_FILE_ENV: &str = "WYRD_FDB_CLUSTER_FILE";

    /// Where FoundationDB's own packages install the cluster file; used when
    /// [`CLUSTER_FILE_ENV`] is unset or blank.
    pub const DEFAULT_CLUSTER_FILE: &str = "/etc/foundationdb/fdb.cluster";

    /// The environment variable naming a **multi-version client** external-client
    /// directory (#441; `docs/design/architecture/07-deployment-view.md` §7.6): a directory
    /// of additional `libfdb_c` versions FoundationDB's own `ExternalClientDirectory`
    /// network option loads, letting one client bridge a lockstep cluster upgrade. Unset
    /// means today's behaviour — `store::ensure_network` boots with no such option set,
    /// byte-identical to before this env var existed.
    pub const EXTERNAL_CLIENT_DIR_ENV: &str = "WYRD_FDB_EXTERNAL_CLIENT_DIR";

    /// The environment variable bounding how long one FoundationDB transaction may run,
    /// in milliseconds. Unset, unparsable, or non-positive falls back to
    /// [`DEFAULT_TRANSACTION_TIMEOUT_MS`].
    pub const TRANSACTION_TIMEOUT_ENV: &str = "WYRD_FDB_TRANSACTION_TIMEOUT_MS";

    /// FoundationDB's own transaction envelope: a transaction that has taken a read version
    /// may not use it for longer than **five seconds**, after which the cluster fails it
    /// with `1007 transaction_too_old` (the same envelope this crate's `with_scan_cap` docs
    /// invoke). Not a knob — FDB's number, recorded here so
    /// [`DEFAULT_TRANSACTION_TIMEOUT_MS`] can be justified against it.
    pub const FDB_TRANSACTION_ENVELOPE_MS: i32 = 5_000;

    /// The default deadline this driver puts on every transaction: **twice**
    /// [`FDB_TRANSACTION_ENVELOPE_MS`].
    ///
    /// Deliberately above FDB's own envelope, and this is the whole of the reasoning. A
    /// transaction that has *reached* the cluster dies at five seconds with `1007
    /// transaction_too_old` — retryable, and definitively **not** committed. Our deadline
    /// produces `1031 transaction_timed_out`
    /// ([`crate::classify::TRANSACTION_TIMED_OUT`]), which promises nothing about whether
    /// the batch landed. Setting the deadline *below* the envelope would race FDB's safe,
    /// informative error and let the ambiguous one win; setting it above means 1031 fires
    /// only when the client cannot reach the cluster at all — the hang this deadline exists
    /// to end, and the one case where no better answer exists.
    pub const DEFAULT_TRANSACTION_TIMEOUT_MS: i32 = 2 * FDB_TRANSACTION_ENVELOPE_MS;

    /// The rule above, enforced at **compile time** rather than asserted in a test that a
    /// future edit could delete: put the deadline under FDB's envelope and a slow — but
    /// reachable — cluster starts reporting undeterminable commits (1031) where it used to
    /// report retryable, definitively-not-committed ones (1007).
    const _: () = assert!(DEFAULT_TRANSACTION_TIMEOUT_MS > FDB_TRANSACTION_ENVELOPE_MS);

    /// Resolve the cluster-file path from a raw [`CLUSTER_FILE_ENV`] value. An unset or
    /// whitespace-only value falls back to [`DEFAULT_CLUSTER_FILE`]; surrounding whitespace
    /// is trimmed, so a value that picked up a trailing newline still resolves.
    #[must_use]
    pub fn cluster_file(raw: Option<String>) -> String {
        match raw {
            Some(path) if !path.trim().is_empty() => path.trim().to_string(),
            _ => DEFAULT_CLUSTER_FILE.to_string(),
        }
    }

    /// Resolve [`EXTERNAL_CLIENT_DIR_ENV`] into an optional external-client-directory path.
    /// An unset or whitespace-only value is `None` — no `ExternalClientDirectory` network
    /// option is set, and the network boots exactly as it did before this env var existed.
    #[must_use]
    pub fn external_client_dir(raw: Option<String>) -> Option<String> {
        raw.map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
    }

    /// Resolve the per-transaction deadline from a raw [`TRANSACTION_TIMEOUT_ENV`] value.
    /// Unset, blank, unparsable, or non-positive falls back to
    /// [`DEFAULT_TRANSACTION_TIMEOUT_MS`] via [`sanitize_transaction_timeout_ms`].
    #[must_use]
    pub fn transaction_timeout_ms(raw: Option<String>) -> i32 {
        raw.and_then(|v| v.trim().parse::<i32>().ok())
            .map(sanitize_transaction_timeout_ms)
            .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT_MS)
    }

    /// Coerce a requested deadline into a *bounded* one.
    ///
    /// FoundationDB reads `timeout = 0` as **"disable all timeouts"**, and a negative value
    /// is rejected outright. Both would restore the unbounded wait — a `get` against an
    /// unreachable cluster never returning — so neither is an available choice here: the
    /// deadline is a liveness constraint, not a tuning knob a caller may switch off. Same
    /// register as `with_scan_cap`, which refuses to *raise* the scan cap. A caller who
    /// wants a longer deadline may have one; a caller who wants none gets the default.
    #[must_use]
    pub fn sanitize_transaction_timeout_ms(ms: i32) -> i32 {
        if ms > 0 {
            ms
        } else {
            DEFAULT_TRANSACTION_TIMEOUT_MS
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn an_explicit_cluster_file_wins() {
            assert_eq!(
                cluster_file(Some("/tmp/fdb.cluster".into())),
                "/tmp/fdb.cluster"
            );
            assert_eq!(
                cluster_file(Some("  /tmp/fdb.cluster \n".into())),
                "/tmp/fdb.cluster"
            );
        }

        #[test]
        fn an_absent_or_blank_value_falls_back_to_the_package_default() {
            assert_eq!(cluster_file(None), DEFAULT_CLUSTER_FILE);
            assert_eq!(cluster_file(Some("   ".into())), DEFAULT_CLUSTER_FILE);
        }

        #[test]
        fn an_explicit_external_client_dir_wins() {
            assert_eq!(
                external_client_dir(Some("/opt/fdb/multiversion".into())),
                Some("/opt/fdb/multiversion".to_string())
            );
            assert_eq!(
                external_client_dir(Some("  /opt/fdb/multiversion \n".into())),
                Some("/opt/fdb/multiversion".to_string())
            );
        }

        #[test]
        fn an_absent_or_blank_external_client_dir_is_none() {
            assert_eq!(external_client_dir(None), None);
            assert_eq!(external_client_dir(Some("   ".into())), None);
        }

        #[test]
        fn an_explicit_transaction_timeout_wins() {
            assert_eq!(transaction_timeout_ms(Some("250".into())), 250);
            assert_eq!(transaction_timeout_ms(Some(" 60000 \n".into())), 60_000);
        }

        #[test]
        fn an_absent_or_unparsable_timeout_falls_back_to_the_default() {
            for raw in [
                None,
                Some(String::new()),
                Some("   ".into()),
                Some("soon".into()),
            ] {
                assert_eq!(transaction_timeout_ms(raw), DEFAULT_TRANSACTION_TIMEOUT_MS);
            }
        }

        #[test]
        fn zero_does_not_disable_the_timeout() {
            // FDB reads `timeout = 0` as "disable all timeouts". Honouring it would hand
            // back the unbounded wait: an operation against an unreachable cluster would
            // hang forever instead of returning `Err`.
            assert_eq!(
                transaction_timeout_ms(Some("0".into())),
                DEFAULT_TRANSACTION_TIMEOUT_MS
            );
            assert_eq!(
                sanitize_transaction_timeout_ms(0),
                DEFAULT_TRANSACTION_TIMEOUT_MS
            );
        }

        #[test]
        fn a_negative_timeout_does_not_disable_the_timeout() {
            assert_eq!(
                transaction_timeout_ms(Some("-1".into())),
                DEFAULT_TRANSACTION_TIMEOUT_MS
            );
            assert_eq!(
                sanitize_transaction_timeout_ms(i32::MIN),
                DEFAULT_TRANSACTION_TIMEOUT_MS
            );
        }
    }
}

/// Keyspace math shared by the store's read/scan/commit paths — kept free of any
/// `foundationdb` dependency so it compiles and is unit-tested on every machine.
///
/// Deliberately duplicated from `crates/metadata-tikv/src/lib.rs:31-111` rather than
/// shared: ADR-0010's dependency rule forbids a concrete backend depending on a sibling
/// concrete, and `traits` is the seam they share, not a utility library.
pub mod keyspace {
    /// Prefix a logical key with the store's `prefix`, yielding the physical key actually
    /// stored in FoundationDB. An empty prefix is the identity (the production default); a
    /// per-instance prefix gives the isolated "fresh store" each shared-suite clause
    /// expects from `make_store(tag)`.
    #[must_use]
    pub fn physical(prefix: &[u8], key: &[u8]) -> Vec<u8> {
        let mut physical = Vec::with_capacity(prefix.len() + key.len());
        physical.extend_from_slice(prefix);
        physical.extend_from_slice(key);
        physical
    }

    /// Strip the store's `prefix` back off a physical key read from FoundationDB,
    /// recovering the logical key the trait exposes. Returns `None` if the physical key is
    /// not under this prefix (so a foreign key is never misattributed).
    #[must_use]
    pub fn logical(prefix: &[u8], physical: &[u8]) -> Option<Vec<u8>> {
        physical.strip_prefix(prefix).map(<[u8]>::to_vec)
    }

    /// The **exclusive** upper bound of the half-open range covering every key beginning
    /// with `prefix`: the prefix with its last non-`0xff` byte incremented and the trailing
    /// `0xff`s dropped. `None` means "no upper bound" — an empty prefix, or an all-`0xff`
    /// prefix, scans to the end of the keyspace. This turns `scan(prefix)` into a bounded
    /// FDB range read `[prefix, upper)` rather than a whole-keyspace filter.
    #[must_use]
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

    /// The exclusive end of FoundationDB's **user** keyspace, used when
    /// [`prefix_upper_bound`] returns `None`. FDB's system keys live at and above `\xff`,
    /// so this both means "scan to the end" and keeps an unbounded scan out of the system
    /// keyspace.
    pub const KEYSPACE_END: &[u8] = b"\xff";

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn physical_and_logical_round_trip() {
            let prefix = b"conformance/42/".as_slice();
            let phys = physical(prefix, b"inode:7");
            assert!(phys.starts_with(prefix));
            assert_eq!(logical(prefix, &phys).as_deref(), Some(&b"inode:7"[..]));
            // The empty prefix is the identity used in production.
            assert_eq!(physical(b"", b"k"), b"k");
            assert_eq!(logical(b"", b"k").as_deref(), Some(&b"k"[..]));
        }

        #[test]
        fn logical_rejects_a_foreign_key() {
            assert_eq!(logical(b"ns-a/", b"ns-b/inode:1"), None);
        }

        #[test]
        fn prefix_upper_bound_is_the_exclusive_end_of_the_prefix_range() {
            assert_eq!(prefix_upper_bound(b"p:").as_deref(), Some(&b"p;"[..]));
            assert!(b"p:1".as_slice() >= b"p:".as_slice());
            assert!(b"p:1".as_slice() < b"p;".as_slice());
            assert!(b"q:1".as_slice() >= b"p;".as_slice());
        }

        #[test]
        fn prefix_upper_bound_carries_over_trailing_0xff() {
            assert_eq!(
                prefix_upper_bound(&[0x01, 0xff]).as_deref(),
                Some(&[0x02][..])
            );
            assert_eq!(prefix_upper_bound(b""), None);
            assert_eq!(prefix_upper_bound(&[0xff, 0xff]), None);
        }

        #[test]
        fn the_unbounded_scan_stops_before_the_system_keyspace() {
            assert_eq!(KEYSPACE_END, b"\xff");
        }
    }
}

/// The bound on a single `scan`, and the typed error a breach raises — kept free of any
/// `foundationdb` dependency so it compiles and is unit-tested on every machine.
///
/// The store's `scan` holds **one** transaction — a single read version, the #261
/// consistent cut — across every internal FDB page. The invariant is
/// **completeness-or-fail-loud** (#262 / ADR-0011), reproduced verbatim in substance from
/// the sibling backend (`crates/metadata-tikv/src/lib.rs:120-145`): a `scan(prefix)`
/// returns the *complete* matching set observed at one read version, or `Err` — it
/// **never** returns a silently truncated `Vec` (a truncated `inode:` scan corrupts GC's
/// never-reclaim safety set — data loss).
///
/// FDB does the page-cursor arithmetic itself (`RangeOption::next_range`), so unlike TiKV
/// this module carries no `next_page_start` / `PAGE_SIZE`: the only decision left to the
/// driver is the one the peer calls out as a **correctness constraint, not a tuning
/// knob** — the total-results ceiling.
pub mod paging {
    use std::fmt;

    /// Interim ceiling on the **total** materialized results of a single `scan`. On breach
    /// the call fails loud (`Err`, via [`ScanCapExceeded`]) and returns **no** partial
    /// `Vec`: a silently truncated `inode:` scan corrupts GC's never-reclaim safety set
    /// (data loss), so this is a **correctness constraint, not a tuning knob** (#262).
    ///
    /// The value is the sibling backend's, deliberately (`crates/metadata-tikv/src/lib.rs:145`):
    /// 2^20 dirents is far past any legitimate single directory yet bounds the gateway heap
    /// against a pathological prefix. Two backends of the same trait must not disagree about
    /// how large a listing may be. Revisited if a paginated/streaming trait method is
    /// measured in (out of M4's unchanged-trait scope); a product-facing "max dirents per
    /// listing" is the human's to confirm (INTEGRATION §4 / #262).
    ///
    /// Without it, an FDB `scan` of a pathological prefix grows the gateway heap unbounded
    /// until FDB's own 5 s transaction limit trips `1007 transaction_too_old`, whereupon the
    /// driver restarts the *whole* scan and finally reports a retry-budget exhaustion — an
    /// opaque, expensive, timing-dependent way to say "too big".
    pub const SCAN_CAP: usize = 1 << 20;

    /// The interim per-`scan` cap was exceeded. Returned as the store's `Err` so the scan
    /// **fails loud instead of truncating** (#262); the operator-visible ADR-0011 audit
    /// signal is surfaced by the caller (GC/custodian), which already owns the telemetry
    /// path. A descriptive typed error keeps the audit signal caller-side and lets a caller
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

    /// What the paged `scan` loop does after materializing one FDB page.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PageStep {
        /// The cap was breached — fail loud, return **no** partial `Vec` (#262).
        CapExceeded,
        /// Under the cap: consult FDB's own cursor (`next_range`) for whether more pages
        /// remain.
        Continue,
    }

    /// Decide whether a paged `scan` may continue, given the running `total` materialized
    /// so far and the `cap`.
    ///
    /// Checked **after each page and before FDB's cursor is consulted**, so an over-cap scan
    /// fails loud even on what would otherwise be its final page — an over-cap result set
    /// can never slip through as a "complete" last page. `total > cap` (not `>=`) so a scan
    /// returning exactly `cap` keys is a legal complete result, matching
    /// `crates/metadata-tikv/src/lib.rs:217`.
    #[must_use]
    pub fn after_page(total: usize, cap: usize) -> PageStep {
        if total > cap {
            return PageStep::CapExceeded;
        }
        PageStep::Continue
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_page_under_the_cap_continues() {
            assert_eq!(after_page(0, SCAN_CAP), PageStep::Continue);
            assert_eq!(after_page(1024, SCAN_CAP), PageStep::Continue);
        }

        #[test]
        fn exactly_the_cap_is_a_legal_complete_result() {
            // `>` not `>=`: a scan that materializes exactly `cap` keys is complete, not a
            // breach. Same boundary as `crates/metadata-tikv/src/lib.rs:217`.
            assert_eq!(after_page(5, 5), PageStep::Continue);
            assert_eq!(after_page(SCAN_CAP, SCAN_CAP), PageStep::Continue);
        }

        #[test]
        fn one_key_past_the_cap_fails_loud() {
            assert_eq!(after_page(6, 5), PageStep::CapExceeded);
            assert_eq!(after_page(SCAN_CAP + 1, SCAN_CAP), PageStep::CapExceeded);
        }

        #[test]
        fn the_cap_matches_the_sibling_backend() {
            // Two backends of the same trait must not disagree about how large a listing
            // may be: `crates/metadata-tikv/src/lib.rs:145` is `1 << 20` too.
            assert_eq!(SCAN_CAP, 1 << 20);
        }

        #[test]
        fn the_cap_error_is_operator_visible() {
            let err = ScanCapExceeded {
                cap: SCAN_CAP,
                prefix: b"inode:".to_vec(),
            }
            .to_string();
            assert!(err.contains("1048576"), "names the cap: {err}");
            assert!(err.contains("inode:"), "names the prefix: {err}");
            assert!(
                err.contains("truncated"),
                "states it refused to truncate: {err}",
            );
        }
    }
}

/// Fail-closed, **non-feature-gated** readiness classification for the FDB client's
/// connection to its cluster (#441) — a third pure sibling to [`classify`] and [`config`]:
/// pure input → output, no `foundationdb` type in any signature, so it compiles and its
/// unit tests run on **every** machine, FDB or not, in the default `cargo xtask ci`.
///
/// # Why this exists
///
/// The `foundationdb` C client binds **exactly** to its cluster's wire protocol: a client
/// built against one FDB version cannot talk to a cluster running another, at all, ever.
/// Before this module, a version-mismatched client hit the same bounded-but-anonymous `1031
/// transaction_timed_out` ([`classify::TRANSACTION_TIMED_OUT`]) a genuinely unreachable
/// cluster produces — an operator who mismatched their client saw the same error as one
/// whose cluster was simply down. [`verdict`] turns `Database::get_client_status()`'s JSON —
/// reduced to [`ClientStatus`] by the feature-gated `store` module below; this module never
/// touches FDB types — into a diagnosis the matching [`message`] can act on.
///
/// # The discrimination rule
///
/// Established empirically at Plan against a live `libfdb_c` 7.3.77, not guessed: a
/// connection whose `Status` reports `"connected"` but whose `Compatible` reports `false` is
/// version skew — **not** "zero reachable coordinators" (under skew the `Coordinators` list
/// stays populated) and **not** `Healthy == false` alone (that is false in the unreachable
/// case too). An unreachable cluster's connection instead reports `Status == "failed"` and
/// carries **no** `ProtocolVersion` at all. [`ClientStatus`] carries the already-reduced
/// shape — [`ClientStatus::coordinators_reachable`] is `Status == "connected"`;
/// [`ClientStatus::cluster_protocol`] is `Some` only when the connection reported a protocol
/// version for a connection that turned out incompatible — so [`verdict`] itself never
/// inspects raw JSON.
///
/// **Fail-honest, always.** An absent, late, unparsable, or novel status — anything
/// [`verdict`] cannot positively identify as skew within the caller's deadline — degrades to
/// [`Verdict::Unreachable`] with a version-coupling hint in [`message`], never a guessed
/// [`Verdict::VersionSkew`].
pub mod preflight {
    use std::time::Duration;

    /// The client-side status this module classifies, already reduced from
    /// `Database::get_client_status()`'s raw JSON by the feature-gated `store` module
    /// (`store::client_status`) — never constructed here, so this module never depends on
    /// `foundationdb` or a JSON crate.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ClientStatus {
        /// The status JSON's top-level `Healthy`.
        pub healthy: bool,
        /// Whether the (first) connection reported `Status == "connected"` — a
        /// network-level signal, deliberately **not** derived from the `Coordinators` list,
        /// which the skew fixture shows stays populated even when the protocol is
        /// incompatible.
        pub coordinators_reachable: bool,
        /// This client's own version, for the operator-facing message. Not the exact
        /// `fdb_get_client_version()` string — `foundationdb` 0.10's safe API does not
        /// expose it — but the API version `get_max_api_version()` returns, plus this
        /// crate's `fdb-7_3` pin.
        pub client_version: String,
        /// The **cluster's** reported protocol version, present only when the connection is
        /// `"connected"` **and** incompatible — i.e. only in the version-skew shape. `None`
        /// for both the unreachable shape (no protocol exchange happens at all) and the
        /// healthy shape (nothing to report — client and cluster already agree).
        pub cluster_protocol: Option<String>,
    }

    /// What [`verdict`] concluded.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Verdict {
        /// The store may proceed: the client reached the cluster and the protocol versions
        /// agree.
        Ready,
        /// The client and cluster disagree on protocol version. `client` is this client's
        /// own version; `cluster` is the cluster's.
        VersionSkew {
            /// This client's own version (API version + the crate's `fdb-7_3` pin).
            client: String,
            /// The cluster's reported protocol version.
            cluster: Option<String>,
        },
        /// The cluster could not be confirmed reachable within the caller's deadline — the
        /// honest fallback for everything [`verdict`] cannot positively call skew, including
        /// a missing, unparsable, or late-arriving status.
        Unreachable {
            /// How long the caller waited before giving up.
            waited: Duration,
        },
    }

    /// Classify a client-status probe. Pure: no I/O, no `foundationdb` type — the
    /// non-feature-gated seam that makes this decision unit-testable on any machine.
    ///
    /// `status` is `None` when the probe itself produced nothing to classify (the
    /// `Database::get_client_status()` call errored, or never returned before `deadline`).
    /// Only a status that arrives **strictly within** `deadline` is trusted for a positive
    /// [`Verdict::Ready`] or [`Verdict::VersionSkew`] call — a bounded probe that overran its
    /// own bound is not "just in time", it is indistinguishable from luck, so it degrades to
    /// [`Verdict::Unreachable`] like a missing status would. This is what makes `deadline` a
    /// real input rather than a pass-through: the fail-honest rule (module docs) applies to
    /// **lateness**, not only to absence.
    #[must_use]
    pub fn verdict(
        status: Option<&ClientStatus>,
        elapsed: Duration,
        deadline: Duration,
    ) -> Verdict {
        match status {
            Some(status)
                if elapsed < deadline && status.healthy && status.coordinators_reachable =>
            {
                Verdict::Ready
            }
            Some(status)
                if elapsed < deadline
                    && status.coordinators_reachable
                    && status.cluster_protocol.is_some() =>
            {
                Verdict::VersionSkew {
                    client: status.client_version.clone(),
                    cluster: status.cluster_protocol.clone(),
                }
            }
            _ => Verdict::Unreachable { waited: elapsed },
        }
    }

    /// Render an operator-facing message for `v`. Mainly useful for [`Verdict::VersionSkew`]
    /// and [`Verdict::Unreachable`] — `FdbMetadataStore::connect()` turns either into its
    /// `Err`; [`Verdict::Ready`] callers have nothing to report.
    #[must_use]
    pub fn message(v: &Verdict) -> String {
        match v {
            Verdict::Ready => "FoundationDB metadata store: ready.".to_string(),
            Verdict::VersionSkew { client, cluster } => {
                let cluster = cluster.as_deref().unwrap_or("<not reported>");
                format!(
                    "FoundationDB metadata store: client/cluster protocol version mismatch \
                     — this client is {client}, the cluster reports protocol version \
                     {cluster}. A FoundationDB client cannot talk to a cluster running a \
                     different protocol version, ever: load the cluster's `libfdb_c` into a \
                     multi-version external-client directory and point \
                     WYRD_FDB_EXTERNAL_CLIENT_DIR at it, then upgrade the cluster and drop \
                     the old library once every client has the new one — see the \
                     multi-version client upgrade procedure in \
                     docs/design/architecture/07-deployment-view.md.",
                )
            }
            Verdict::Unreachable { waited } => format!(
                "FoundationDB metadata store: cluster unreachable after waiting {waited:?} \
                 for a client-status response. The client could not even determine the \
                 cluster's protocol version, so this is reported as unreachable rather than \
                 a guessed version skew. Check that fdbserver is running and that \
                 WYRD_FDB_CLUSTER_FILE points at a reachable cluster file; if this follows a \
                 FoundationDB upgrade, an unmigrated client is also worth ruling out — see \
                 the multi-version client upgrade procedure in \
                 docs/design/architecture/07-deployment-view.md.",
            ),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn status(
            healthy: bool,
            coordinators_reachable: bool,
            cluster_protocol: Option<&str>,
        ) -> ClientStatus {
            ClientStatus {
                healthy,
                coordinators_reachable,
                client_version: "api 730 (fdb-7_3 pin)".to_string(),
                cluster_protocol: cluster_protocol.map(str::to_string),
            }
        }

        #[test]
        fn a_healthy_connected_client_is_ready() {
            let s = status(true, true, None);
            assert_eq!(
                verdict(Some(&s), Duration::from_millis(50), Duration::from_secs(5)),
                Verdict::Ready
            );
        }

        #[test]
        fn no_status_at_all_is_unreachable_not_a_guess() {
            assert_eq!(
                verdict(None, Duration::from_secs(5), Duration::from_secs(5)),
                Verdict::Unreachable {
                    waited: Duration::from_secs(5)
                }
            );
        }

        #[test]
        fn a_status_that_only_arrives_at_the_deadline_is_not_trusted() {
            // Even a status that LOOKS healthy is not believed if it took the entire
            // budget to arrive: a bounded probe that overran its bound is indistinguishable
            // from luck, not a confirmed Ready.
            let s = status(true, true, None);
            assert_eq!(
                verdict(Some(&s), Duration::from_secs(5), Duration::from_secs(5)),
                Verdict::Unreachable {
                    waited: Duration::from_secs(5)
                }
            );
        }

        #[test]
        fn ready_message_names_no_version_at_all() {
            let msg = message(&Verdict::Ready);
            assert!(!msg.contains("mismatch"));
        }
    }
}

#[cfg(feature = "fdb")]
mod store {
    //! The live driver. Compiled only under `--features fdb`, which links `libfdb_c`.

    use std::fmt;
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use bytes::Bytes;
    use foundationdb::api::{FdbApiBuilder, NetworkAutoStop};
    use foundationdb::options::{NetworkOption, StreamingMode, TransactionOption};
    use foundationdb::{Database, FdbError, RangeOption, Transaction, TransactionCommitError};
    use wyrd_traits::{BoxError, CommitOutcome, MetadataStore, Result, WriteBatch};

    use crate::classify::{self, CommitClass, CommitUnknownResult};
    use crate::paging::{self, PageStep, ScanCapExceeded};
    use crate::preflight;
    use crate::{config, keyspace};

    /// How many attempts a **read** or a **blind** (precondition-free) commit makes on a
    /// retryable FDB error before it is surfaced as `Err`.
    ///
    /// A **conditional** batch is never retried here: the trait hands a lost race back to
    /// the caller as `Ok(Conflict)` so the *caller* decides (e.g. `alloc_inode`'s budgeted
    /// backoff loop, `crates/server/src/cli.rs:1027-1049`), and a driver-side retry would
    /// re-read the precondition against a newer read version, silently turning the caller's
    /// CAS into a last-writer-wins overwrite.
    const MAX_ATTEMPTS: u32 = 5;

    /// A bounded retry budget ran out: `attempts` attempts at `op` all failed with a
    /// *retryable* FoundationDB error, the last of which is carried as this error's
    /// [`source`](std::error::Error::source).
    ///
    /// Keeping the last [`FdbError`] reachable is the point. This crate promises a caller
    /// can tell a transient `1007 transaction_too_old` from a permanent `2103
    /// value_too_large` by downcasting; a `String`-backed "exhausted 5 attempts" error
    /// silently destroys exactly that distinction, on exactly the paths where the cause
    /// matters most. `Err(e)` here is never `Ok(Conflict)`: a blind batch that loses
    /// `MAX_ATTEMPTS` races in a row surfaces the loss rather than laundering it into an
    /// outcome a `?`-only caller would read as success.
    #[derive(Debug, Clone, Copy)]
    pub struct RetryBudgetExhausted {
        /// The store operation that ran out of retries (`"get"`, `"scan"`, `"blind commit"`).
        pub op: &'static str,
        /// How many attempts were made.
        pub attempts: u32,
        /// The retryable error the final attempt failed with — also this error's `source()`.
        pub last: FdbError,
    }

    impl fmt::Display for RetryBudgetExhausted {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "metadata {} exhausted {} attempts against FoundationDB; the last attempt \
                 failed with FDB error {} ({}). The underlying FdbError is this error's \
                 `source()`.",
                self.op,
                self.attempts,
                self.last.code(),
                self.last,
            )
        }
    }

    impl std::error::Error for RetryBudgetExhausted {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.last)
        }
    }

    impl RetryBudgetExhausted {
        fn new(op: &'static str, last: FdbError) -> Self {
            Self {
                op,
                attempts: MAX_ATTEMPTS,
                last,
            }
        }
    }

    /// The FDB client permits exactly **one** network thread per process, while `run_all`
    /// (`crates/metadata-conformance/src/lib.rs:291`) constructs **seven** stores in one
    /// test process. So the network is booted once, lazily, behind a `OnceLock`, and every
    /// `FdbMetadataStore` in the process shares it.
    ///
    /// The guard is deliberately never dropped: a `static` is not dropped at process exit,
    /// and `NetworkAutoStop::drop` stops the run loop — which must not happen while any
    /// `Database` is still alive. Process teardown stops the thread.
    static NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();

    /// Boot the FDB client network thread exactly once per process.
    ///
    /// Takes the same [`FdbApiBuilder`] → `NetworkBuilder` → `.boot()` path
    /// `foundationdb::boot()` takes internally, but stops short of that top-level
    /// convenience function so [`config::EXTERNAL_CLIENT_DIR_ENV`] can set
    /// `NetworkOption::ExternalClientDirectory` first — the **multi-version client** #441's
    /// lockstep-upgrade dance depends on
    /// (`docs/design/architecture/07-deployment-view.md` §7.6). When the env var is unset,
    /// the network boots exactly as `foundationdb::boot()` would have: no network option is
    /// set, byte-identical to this function's behaviour before this env var existed.
    fn ensure_network() -> &'static NetworkAutoStop {
        NETWORK.get_or_init(|| {
            let builder = FdbApiBuilder::default()
                .build()
                .expect("fdb api initialized");
            let builder = match config::external_client_dir(
                std::env::var(config::EXTERNAL_CLIENT_DIR_ENV).ok(),
            ) {
                Some(dir) => builder
                    .set_option(NetworkOption::ExternalClientDirectory(dir))
                    .expect(
                        "WYRD_FDB_EXTERNAL_CLIENT_DIR names a directory the FDB client accepts",
                    ),
                None => builder,
            };
            // SAFETY: `NetworkBuilder::boot` selects the API version, starts the client run
            // loop on a dedicated thread, and returns the guard that stops it. Its two
            // documented requirements are (1) it is called at most once per process and (2)
            // the returned guard outlives every `Database`. `OnceLock::get_or_init` gives
            // (1) — the initializer runs exactly once, even under the repeated `make_store`
            // calls of the conformance suite — and storing the guard in a `static` that is
            // never dropped gives (2). There is no safe alternative in the `foundationdb`
            // crate: `api::NetworkBuilder::boot` is `unsafe` as well — the same requirement
            // `foundationdb::boot()` carried before this function inlined its body to reach
            // the builder's `set_option`.
            #[allow(unsafe_code)]
            unsafe {
                builder.boot().expect("fdb network running")
            }
        })
    }

    /// How long [`FdbMetadataStore::preflight`] pauses between `get_client_status()` polls
    /// while a connection is still `"connecting"`. Verified live against `libfdb_c` 7.3.77:
    /// a fresh `Database`'s connection settles (to `"connected"` or `"failed"`) within
    /// ~0.2–2s, so a poll interval well under that — and well under the deadline — costs at
    /// most one wasted round-trip's worth of latency without turning the probe into a busy
    /// loop.
    const STATUS_POLL_INTERVAL: Duration = Duration::from_millis(100);

    /// Reduce `Database::get_client_status()`'s raw client-status JSON into the pure
    /// [`preflight::ClientStatus`] shape, using only the fields #441's design proposal's
    /// fixture table showed distinguish skew, unreachable, and healthy: `Healthy`,
    /// `Connections[0].Status`, `Connections[0].Compatible`, `Connections[0].ProtocolVersion`.
    ///
    /// **`None` means "not yet actionable", not only "unparsable".** A connection whose
    /// `Status` is still `"connecting"` — the shape every fresh `Database` reports for a
    /// beat before it settles (see [`STATUS_POLL_INTERVAL`]) — is `None` too, so
    /// [`FdbMetadataStore::preflight`]'s poll loop keeps waiting instead of treating an
    /// in-flight dial as a settled verdict. Anything else that fails to parse — a missing
    /// field, a shape this probe has never seen — is also `None`, which [`preflight::verdict`]
    /// degrades to `Unreachable` rather than a guessed `VersionSkew` (fail-honest, the
    /// `preflight` module docs).
    fn client_status(bytes: &[u8]) -> Option<preflight::ClientStatus> {
        let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
        let healthy = value.get("Healthy")?.as_bool()?;
        let connection = value
            .get("Connections")
            .and_then(serde_json::Value::as_array)
            .and_then(|conns| conns.first());
        let status_str = connection
            .and_then(|c| c.get("Status"))
            .and_then(serde_json::Value::as_str);
        if status_str == Some("connecting") {
            // Not yet settled: let the caller poll again rather than judge an in-flight
            // dial as Unreachable.
            return None;
        }
        let coordinators_reachable = status_str.is_some_and(|s| s == "connected");
        // `Compatible` defaults to `true` (never absent-and-incompatible) so an
        // unparsable connection never manufactures a version-skew claim.
        let compatible = connection
            .and_then(|c| c.get("Compatible"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let cluster_protocol = if compatible {
            None
        } else {
            connection
                .and_then(|c| c.get("ProtocolVersion"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        Some(preflight::ClientStatus {
            healthy,
            coordinators_reachable,
            client_version: format!(
                "api {} (fdb-7_3 pin)",
                foundationdb::api::get_max_api_version()
            ),
            cluster_protocol,
        })
    }

    /// A [`MetadataStore`] backed by a FoundationDB cluster (ADR-0042). Metadata keys and
    /// values are stored **byte-identically** — FDB never interprets a key or a value — so
    /// the application-level version CAS (a full-value precondition) is exact.
    pub struct FdbMetadataStore {
        db: Database,
        /// Prepended to every key. Empty in production; a per-instance value gives the
        /// isolated keyspace each shared-suite clause needs from `make_store(tag)`.
        prefix: Vec<u8>,
        /// Ceiling on the total results of one `scan`; see [`paging::SCAN_CAP`].
        scan_cap: usize,
        /// Deadline applied to every transaction this store creates, in milliseconds; see
        /// [`config::DEFAULT_TRANSACTION_TIMEOUT_MS`] and [`Self::trx`].
        timeout_ms: i32,
    }

    impl FdbMetadataStore {
        /// Open the store against the cluster file named by
        /// [`config::CLUSTER_FILE_ENV`](crate::config::CLUSTER_FILE_ENV), falling back to
        /// [`config::DEFAULT_CLUSTER_FILE`](crate::config::DEFAULT_CLUSTER_FILE), with the
        /// per-transaction deadline named by
        /// [`config::TRANSACTION_TIMEOUT_ENV`](crate::config::TRANSACTION_TIMEOUT_ENV).
        ///
        /// Configuration is owned here, by the driver's own constructor: no `server`-side
        /// selection arm exists yet (that is a later, blocked issue), so there is nowhere
        /// else for it to live.
        ///
        /// Unlike [`Self::open`], `connect()` performs a bounded, fail-closed readiness
        /// probe before returning `Ok` (#441) — see [`Self::preflight`]. This is the
        /// operator path reached from `crates/server/src/cli.rs:175` (`open_fdb_meta`);
        /// [`Self::open`] stays probe-free so the conformance/scan/timeout test harnesses,
        /// which deliberately point at unreachable clusters, are unaffected.
        ///
        /// **`async`, and that is load-bearing.** The probe awaits
        /// `Database::get_client_status()`, a `foundationdb` future that only resolves on a
        /// running reactor. Every caller of this constructor is already inside a Tokio
        /// runtime — `open_fdb_meta` (`crates/server/src/cli.rs:175`) is reached from seven
        /// call sites, four directly inside `runtime.block_on(async { … })` and three inside
        /// an `async fn` — so a synchronous `connect()` could only drive that future by
        /// building its **own** runtime and calling `Runtime::block_on` on it, which Tokio
        /// panics on ("Cannot start a runtime from within a runtime"). Awaiting on the
        /// caller's runtime is the only shape that works, and it is the shape the TiKV peer
        /// already uses (`open_tikv_meta`, `crates/server/src/cli.rs:147`).
        pub async fn connect() -> Result<Self> {
            let store = Self::open(&config::cluster_file(
                std::env::var(config::CLUSTER_FILE_ENV).ok(),
            ))?;
            let store = store.with_transaction_timeout_ms(config::transaction_timeout_ms(
                std::env::var(config::TRANSACTION_TIMEOUT_ENV).ok(),
            ));
            store.preflight().await?;
            Ok(store)
        }

        /// The bounded, fail-closed readiness check [`Self::connect`] performs before
        /// returning `Ok` (#441). Feeds `Database::get_client_status()`'s JSON (via
        /// [`client_status`]) into the pure [`preflight::verdict`] and returns
        /// `Err(preflight::message(..))` on anything but `Ready`, so a client/cluster
        /// protocol mismatch is reported as *itself* — naming the cluster's protocol version
        /// and the multi-version upgrade procedure — instead of the anonymous `1031
        /// transaction_timed_out` the first real transaction would otherwise hit (see this
        /// crate's `preflight` module docs).
        ///
        /// Bounded by this store's own transaction deadline, the same one every transaction
        /// it creates carries, so a genuinely unreachable cluster fails this probe no more
        /// slowly than it would have failed the first `get`/`commit`.
        ///
        /// **Runs on the caller's runtime.** It owns no runtime and calls no `block_on`; it
        /// is an `async fn` awaited by [`Self::connect`], which is awaited by
        /// `open_fdb_meta`. See [`Self::connect`] for why any other shape panics.
        ///
        /// **Polls, rather than calling `get_client_status()` once.** Verified live against
        /// `libfdb_c` 7.3.77 (`deploy/fdb-single-node`): immediately after `Database::new`
        /// the JSON's `Connections[0].Status` is `"connecting"` — settling to `"connected"`
        /// or `"failed"` only after (observed) ~0.2–2s. [`client_status`] treats an unsettled
        /// `"connecting"` connection the same as an unparsable one (`None`), so a single call
        /// would misclassify *every* fresh, perfectly healthy connect as `Unreachable`. This
        /// loop instead re-polls at [`STATUS_POLL_INTERVAL`] until a settled status is parsed
        /// or the deadline elapses — still a single bounded probe from the caller's
        /// perspective, never slower than that deadline.
        ///
        /// `pub` so the cluster-file-free regression case in `tests/timeout.rs` can drive
        /// this exact production probe against an unreachable coordinator from inside a
        /// Tokio runtime.
        pub async fn preflight(&self) -> Result<()> {
            let deadline = Duration::from_millis(u64::try_from(self.timeout_ms).unwrap_or(0));
            let started = Instant::now();
            let status = loop {
                let remaining = deadline.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    break None;
                }
                match tokio::time::timeout(remaining, self.db.get_client_status()).await {
                    Ok(Ok(bytes)) => {
                        if let Some(status) = client_status(&bytes) {
                            break Some(status);
                        }
                        // Unsettled ("connecting") or unparsable this round: a short pause,
                        // still bounded by `remaining` on the next lap.
                        let pause =
                            STATUS_POLL_INTERVAL.min(deadline.saturating_sub(started.elapsed()));
                        if pause.is_zero() {
                            break None;
                        }
                        tokio::time::sleep(pause).await;
                    }
                    Ok(Err(_)) | Err(_) => break None,
                }
            };
            let elapsed = started.elapsed();

            match preflight::verdict(status.as_ref(), elapsed, deadline) {
                preflight::Verdict::Ready => Ok(()),
                other => Err(preflight::message(&other).into()),
            }
        }

        /// Open the store against an explicit `cluster_file` path, with the default
        /// per-transaction deadline ([`config::DEFAULT_TRANSACTION_TIMEOUT_MS`]).
        pub fn open(cluster_file: &str) -> Result<Self> {
            ensure_network();
            let db = Database::new(Some(cluster_file))?;
            Ok(Self {
                db,
                prefix: Vec::new(),
                scan_cap: paging::SCAN_CAP,
                timeout_ms: config::DEFAULT_TRANSACTION_TIMEOUT_MS,
            })
        }

        /// Scope this store to an isolated key `prefix`, giving each clause of the shared
        /// suite a fresh keyspace against one shared cluster
        /// (`crates/metadata-tikv/tests/conformance.rs:51-59` does the same for TiKV).
        #[must_use]
        pub fn with_prefix(mut self, prefix: impl Into<Vec<u8>>) -> Self {
            self.prefix = prefix.into();
            self
        }

        /// **Lower** this store's per-`scan` result ceiling below the default
        /// [`paging::SCAN_CAP`]. Values above the default are clamped to it: the cap is a
        /// correctness constraint (#262), not a knob a caller may loosen — raising it is how
        /// an unbounded listing gets back in.
        ///
        /// Lowering it is what makes the fail-loud path *reachable by a test*. Proving
        /// completeness-or-fail-loud against the real 2^20 default would mean writing a
        /// million keys into a throwaway `fdbserver` on every run — and FDB's 5 s / 10 MB
        /// transaction envelope would trip first, so the test would witness `1007
        /// transaction_too_old`, never the cap. With a lowered cap
        /// `crates/metadata-fdb/tests/scan.rs` drives the *production* `scan` loop into the
        /// *production* [`ScanCapExceeded`] arm and asserts it returns **no** partial `Vec`.
        #[must_use]
        pub fn with_scan_cap(mut self, cap: usize) -> Self {
            self.scan_cap = cap.min(paging::SCAN_CAP);
            self
        }

        /// Set the deadline applied to every transaction this store creates.
        ///
        /// Non-positive values do **not** disable the deadline — FDB would read `0` as
        /// "disable all timeouts", restoring the unbounded wait — they resolve to
        /// [`config::DEFAULT_TRANSACTION_TIMEOUT_MS`]. See
        /// [`config::sanitize_transaction_timeout_ms`].
        ///
        /// Lowering it is what makes the deadline *reachable by a test*: with a short
        /// deadline `crates/metadata-fdb/tests/timeout.rs` drives the production `get` and
        /// `commit` against an unreachable cluster and shows they return `Err` rather than
        /// hanging.
        #[must_use]
        pub fn with_transaction_timeout_ms(mut self, timeout_ms: i32) -> Self {
            self.timeout_ms = config::sanitize_transaction_timeout_ms(timeout_ms);
            self
        }

        fn physical(&self, key: &[u8]) -> Vec<u8> {
            keyspace::physical(&self.prefix, key)
        }

        /// Begin a transaction against the cluster, **bounded in time**.
        ///
        /// Every `get`, `scan` and `commit` starts here, and the deadline is what makes each
        /// of them terminate. FoundationDB's C client sets no timeout by default (`timeout`
        /// defaults to `0`, "disable all timeouts") and does not give up on an unreachable
        /// cluster: it retries the connection indefinitely, so an awaited `get`/`get_range`/
        /// `commit` future simply never resolves. A wrong cluster file, a DNS failure or an
        /// FDB outage would therefore hang a metadata operation forever rather than
        /// surfacing `Err`. With the deadline set, the same operations fail with `1031
        /// transaction_timed_out` ([`classify::TRANSACTION_TIMED_OUT`]).
        ///
        /// The option is `persistent` in FDB's own option table and is not cleared by
        /// `on_error` at API version ≥ 610 (this crate binds 7.3), so it still bounds the
        /// transaction across the `on_error` resets in `get`, `scan` and
        /// [`blind_commit_loop`]. Worst case those loops run [`MAX_ATTEMPTS`] deadlines;
        /// either way they terminate.
        fn trx(&self) -> Result<Transaction> {
            let trx = self.db.create_trx()?;
            trx.set_option(TransactionOption::Timeout(self.timeout_ms))?;
            Ok(trx)
        }

        /// Read one bounded range to exhaustion inside a single `trx`, following FDB's
        /// `more()` paging. Every page shares `trx`'s one read version — the consistent cut.
        ///
        /// Bounded by [`Self::scan_cap`](FdbMetadataStore): once the accumulated set passes
        /// the cap the scan abandons `out` and reports [`ScanFailure::CapExceeded`], so **no
        /// partial `Vec` can escape** (#262, ADR-0011).
        async fn scan_once(
            &self,
            trx: &Transaction,
            start: Vec<u8>,
            end: Vec<u8>,
        ) -> std::result::Result<Vec<(Vec<u8>, Bytes)>, ScanFailure> {
            let mut range = RangeOption::from((start, end));
            range.mode = StreamingMode::WantAll;

            let mut out: Vec<(Vec<u8>, Bytes)> = Vec::new();
            let mut iteration = 1;
            loop {
                let values = trx
                    .get_range(&range, iteration, false)
                    .await
                    .map_err(ScanFailure::Fdb)?;
                for kv in values.iter() {
                    if let Some(logical) = keyspace::logical(&self.prefix, kv.key()) {
                        out.push((logical, Bytes::copy_from_slice(kv.value())));
                    }
                }
                // Checked after each page and BEFORE FDB's cursor is consulted, so an
                // over-cap set can never slip through as a "complete" final page.
                if paging::after_page(out.len(), self.scan_cap) == PageStep::CapExceeded {
                    return Err(ScanFailure::CapExceeded);
                }
                match range.next_range(&values) {
                    Some(next) => range = next,
                    None => return Ok(out),
                }
                iteration += 1;
            }
        }

        /// The CAS path. Preconditions are read **non-snapshot** inside the commit
        /// transaction, which is what puts them in the read-conflict set and makes a lost
        /// race surface as 1020 rather than as a silent last-writer-wins overwrite.
        async fn commit_conditional(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            let trx = self.trx()?;

            for pc in &batch.preconditions {
                // `snapshot = false` is load-bearing: a snapshot read would NOT join the
                // read-conflict set, so two racers would both observe their precondition
                // holding and both commit — the lost race would vanish.
                let current = trx.get(&self.physical(&pc.key), false).await?;
                let holds = match &pc.expected {
                    Some(expected) => current.as_deref() == Some(expected.as_ref()),
                    None => current.is_none(),
                };
                if !holds {
                    // Observed miss. Dropping `trx` cancels it; nothing was written.
                    return Ok(CommitOutcome::Conflict);
                }
            }

            self.stage(&trx, &batch);

            match trx.commit().await {
                Ok(_) => Ok(CommitOutcome::Committed),
                // NOT retried: a lost race is the caller's to retry (`Ok(Conflict)`), and
                // any other error is a fault the caller must see.
                Err(err) => outcome_from_commit_error(*err, true),
            }
        }

        /// The blind path. No preconditions ⇒ nothing joins the read-conflict set ⇒ FDB
        /// cannot reject this with 1020 at all. A retryable, *definitively not committed*
        /// error is retried (bounded by [`MAX_ATTEMPTS`]); `1021 commit_unknown_result` is
        /// **not** retryable-not-committed, so [`blind_commit_step`] never lets it into the
        /// retry arm and it is surfaced as the distinguishable [`CommitUnknownResult`].
        ///
        /// The bounded retry itself lives in [`blind_commit_loop`], driving the
        /// [`TrxBlindCommit`] seam: the loop's load-bearing rule is about an error a
        /// *healthy* cluster cannot be made to emit on demand, so it must be reachable by a
        /// test without a fabricated cluster.
        async fn commit_blind(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            let mut target = TrxBlindCommit {
                store: self,
                batch: &batch,
                trx: Some(self.trx()?),
                failed: None,
            };
            blind_commit_loop(&mut target).await
        }

        /// Buffer the batch's puts and deletes into `trx`. Neither `set` nor `clear` is a
        /// read, so neither adds anything to the read-conflict set.
        fn stage(&self, trx: &Transaction, batch: &WriteBatch) {
            for (key, value) in &batch.puts {
                trx.set(&self.physical(key), value);
            }
            for key in &batch.deletes {
                trx.clear(&self.physical(key));
            }
        }
    }

    /// Why one `scan_once` attempt did not produce a complete result set.
    ///
    /// The two are handled differently by [`MetadataStore::scan`]'s retry loop, which is the
    /// reason they are distinguished rather than both being an `FdbError`: an [`Self::Fdb`]
    /// error may be transient (retry the whole scan on a fresh read version), while
    /// [`Self::CapExceeded`] is a *deterministic* property of the data — retrying it would
    /// re-read the same over-cap range four more times before failing anyway.
    #[derive(Debug, Clone, Copy)]
    enum ScanFailure {
        /// FoundationDB reported an error reading a page.
        Fdb(FdbError),
        /// The accumulated set passed the store's cap; the partial `Vec` was dropped.
        CapExceeded,
    }

    /// What the **blind** path does with a failed commit attempt.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum BlindStep {
        /// Reset the transaction (FDB's own `on_error` backoff) and re-apply the batch.
        Retry,
        /// Hand the error to [`outcome_from_commit_error`]. Never retried.
        Surface,
    }

    /// The **single** place the driver decides whether it may re-apply a precondition-free
    /// batch, and therefore the whole of the "1021 is never blind-retried" rule.
    ///
    /// The predicate is FoundationDB's own `retryable_not_committed` — *definitively not
    /// committed*, so re-applying the batch cannot double-apply it. `1021
    /// commit_unknown_result` is `maybe_committed`, so the predicate is **false** for it and
    /// it can never be [`BlindStep::Retry`]: a `WriteBatch` is not guaranteed idempotent, so
    /// re-applying one that may already have landed is a corruption, not a retry. (This is
    /// exactly why the `foundationdb` crate's `Database::run` closure-retry is unusable
    /// here — it re-runs the closure on 1021. Widen this predicate to `is_retryable()` and
    /// 1021 becomes a silent double-apply.)
    fn blind_commit_step(err: FdbError) -> BlindStep {
        if err.is_retryable_not_committed() {
            BlindStep::Retry
        } else {
            BlindStep::Surface
        }
    }

    /// One blind-commit attempt, plus the reset that follows a retryable failure — the seam
    /// [`blind_commit_loop`] drives.
    ///
    /// Production is [`TrxBlindCommit`]: a real `Transaction` committing to a real cluster.
    /// The seam exists because the loop's load-bearing rule concerns `1021
    /// commit_unknown_result`, which a healthy `fdbserver` cannot be made to emit on demand
    /// (it means "the client lost contact after sending the commit"). The unit tests drive
    /// **this same loop** and **this same [`blind_commit_step`]** with real
    /// [`FdbError`]s — whose retryability comes from `libfdb_c` itself, not from a
    /// hand-written table — so the rule is bound rather than merely asserted.
    #[async_trait]
    trait BlindCommit {
        /// Apply the batch once. `Ok(())` means the commit was acknowledged.
        async fn attempt(&mut self) -> std::result::Result<(), FdbError>;
        /// Recover from the attempt that just failed, ready for another `attempt`.
        async fn reset(&mut self) -> std::result::Result<(), FdbError>;
    }

    /// The bounded retry loop for a **blind** batch. A conditional batch never comes here:
    /// the trait hands its lost race back to the caller as `Ok(Conflict)` so the *caller*
    /// decides (`crates/server/src/cli.rs:1027-1049`).
    ///
    /// Exhaustion is an `Err([RetryBudgetExhausted])` carrying the last [`FdbError`] as its
    /// `source()` — **never** `Ok(Conflict)`. That is one of the three reachable mechanisms
    /// holding the invariant's third clause (see this crate's module docs): a blind batch
    /// that keeps losing surfaces the loss instead of laundering it into an outcome a
    /// `?`-only caller reads as success.
    ///
    /// The last failed attempt is **not** followed by a `reset`: FDB's `on_error` sleeps out
    /// its backoff, and charging the caller a backoff it can never spend is pure latency.
    async fn blind_commit_loop(target: &mut impl BlindCommit) -> Result<CommitOutcome> {
        let mut last: Option<FdbError> = None;
        for attempt in 1..=MAX_ATTEMPTS {
            match target.attempt().await {
                Ok(()) => return Ok(CommitOutcome::Committed),
                Err(err) => match blind_commit_step(err) {
                    BlindStep::Surface => return outcome_from_commit_error(err, false),
                    BlindStep::Retry => {
                        last = Some(err);
                        if attempt == MAX_ATTEMPTS {
                            break;
                        }
                        target.reset().await?;
                    }
                },
            }
        }
        // `last` is `Some` on every path that breaks out: the loop only leaves the `Retry`
        // arm by exhausting the budget, and that arm always records the error first.
        let last = last.expect("the retry arm records the error before exhausting the budget");
        Err(BoxError::from(RetryBudgetExhausted::new(
            "blind commit",
            last,
        )))
    }

    /// The production [`BlindCommit`]: commits `batch` on a real `Transaction`, and resets
    /// it through FDB's `on_error` (which owns the backoff and hands back a fresh
    /// transaction).
    struct TrxBlindCommit<'a> {
        store: &'a FdbMetadataStore,
        batch: &'a WriteBatch,
        /// The transaction the next attempt commits. `None` only between a failed attempt
        /// and its `reset`.
        trx: Option<Transaction>,
        /// The commit that just failed, kept so `reset` can hand it to FDB's `on_error`.
        failed: Option<TransactionCommitError>,
    }

    #[async_trait]
    impl BlindCommit for TrxBlindCommit<'_> {
        async fn attempt(&mut self) -> std::result::Result<(), FdbError> {
            let trx = self
                .trx
                .take()
                .expect("blind_commit_loop resets before every attempt after the first");
            self.store.stage(&trx, self.batch);
            match trx.commit().await {
                Ok(_) => Ok(()),
                Err(err) => {
                    // `TransactionCommitError` derefs to the `FdbError` the loop classifies;
                    // the error itself is kept because `on_error` consumes it.
                    let code = *err;
                    self.failed = Some(err);
                    Err(code)
                }
            }
        }

        async fn reset(&mut self) -> std::result::Result<(), FdbError> {
            let failed = self
                .failed
                .take()
                .expect("blind_commit_loop only resets after a failed attempt");
            // Safe here precisely because the batch is blind AND `blind_commit_step` proved
            // the error definitively-not-committed: there is no precondition whose truth a
            // fresh read version could change, and nothing may already have been applied.
            self.trx = Some(failed.on_error().await?);
            Ok(())
        }
    }

    /// Turn a failed FDB commit into the trait's `CommitOutcome` partition — the **single**
    /// classification site, shared by the conditional and blind commit paths so the two can
    /// never drift.
    ///
    /// `conditional` is `!batch.preconditions.is_empty()` (the TiKV rule,
    /// `crates/metadata-tikv/src/lib.rs:546`). See [`classify::classify_commit_error`].
    ///
    /// # The `conditional = false` callsite is defence-in-depth, not the invariant
    ///
    /// Called from [`blind_commit_loop`] with `conditional = false`, this can never see
    /// `1020 not_committed`: FDB reports 1020 as `retryable_not_committed`, so
    /// [`blind_commit_step`] routes it to [`BlindStep::Retry`] before any classification
    /// happens. Flipping that `false` to `true` is therefore a **semantically inert**
    /// mutation, and no test can kill it — say so plainly rather than claim coverage that
    /// does not exist. The argument stays because it is free, because it keeps the rule
    /// legible where a reader looks for it, and because it is what catches a future refactor
    /// that stopped routing 1020 through the retry gate. What actually holds the invariant's
    /// third clause on the blind path is [`commit_path`], [`blind_commit_step`] and
    /// [`RetryBudgetExhausted`] — each with a test in `store::tests` that goes red without
    /// it (see this crate's module docs).
    fn outcome_from_commit_error(err: FdbError, conditional: bool) -> Result<CommitOutcome> {
        match classify::classify_commit_error(err.code(), conditional) {
            CommitClass::Conflict => Ok(CommitOutcome::Conflict),
            CommitClass::UnknownResult => {
                Err(BoxError::from(CommitUnknownResult { code: err.code() }))
            }
            CommitClass::Fault => Err(BoxError::from(err)),
        }
    }

    /// Which of the two commit paths a batch takes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CommitPath {
        /// The batch carries preconditions: a CAS, whose lost race is `Ok(Conflict)` and
        /// which the driver never retries on the caller's behalf.
        Conditional,
        /// The batch carries no preconditions: a blind write, which is never `Conflict` and
        /// which the driver may safely retry on a definitively-not-committed error.
        Blind,
    }

    /// **The routing rule**, and the first of the three mechanisms that keep a blind batch
    /// out of `Conflict`: `let conditional = !batch.preconditions.is_empty()`
    /// (`crates/metadata-tikv/src/lib.rs:546`).
    ///
    /// Break it — route a precondition-free batch to [`CommitPath::Conditional`] — and two
    /// things go wrong at once. The batch inherits CAS classification, so a commit error
    /// that a conditional batch would legitimately call `Conflict` is now reported as
    /// `Conflict` for a write that has no precondition to have failed. And it silently loses
    /// its bounded retry of `1007 transaction_too_old` / `1009 future_version`, because
    /// [`FdbMetadataStore::commit_conditional`] deliberately does not retry.
    ///
    /// Pinned by `store::tests::a_blind_batch_routes_to_the_blind_path` and
    /// `…::a_conditional_batch_routes_to_the_conditional_path`, which drive the production
    /// [`route_commit`] below.
    fn commit_path(batch: &WriteBatch) -> CommitPath {
        if batch.preconditions.is_empty() {
            CommitPath::Blind
        } else {
            CommitPath::Conditional
        }
    }

    /// The two commit paths [`route_commit`] dispatches between — the seam that lets the
    /// routing rule be tested without a cluster.
    ///
    /// Production is [`FdbMetadataStore`]. Exactly as with [`BlindCommit`], only the
    /// *destination* is scripted in tests; [`commit_path`] and [`route_commit`] are the
    /// production functions `MetadataStore::commit` calls.
    #[async_trait]
    trait CommitPaths {
        /// Apply a batch that carries preconditions (CAS).
        async fn conditional(&self, batch: WriteBatch) -> Result<CommitOutcome>;
        /// Apply a batch that carries none (blind write).
        async fn blind(&self, batch: WriteBatch) -> Result<CommitOutcome>;
    }

    /// Dispatch `batch` to the path [`commit_path`] chooses. This *is* the body of
    /// `MetadataStore::commit`; there is no second routing decision anywhere.
    async fn route_commit(target: &impl CommitPaths, batch: WriteBatch) -> Result<CommitOutcome> {
        match commit_path(&batch) {
            CommitPath::Conditional => target.conditional(batch).await,
            CommitPath::Blind => target.blind(batch).await,
        }
    }

    #[async_trait]
    impl CommitPaths for FdbMetadataStore {
        async fn conditional(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            self.commit_conditional(batch).await
        }

        async fn blind(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            self.commit_blind(batch).await
        }
    }

    #[async_trait]
    impl MetadataStore for FdbMetadataStore {
        /// Read `key` inside a fresh transaction — one cluster-assigned read version per
        /// call, so the value is current as of the call (no stale/cached read).
        ///
        /// A read is idempotent, so a retryable error is retried (bounded by
        /// `MAX_ATTEMPTS`) using FDB's own `on_error` backoff. Exhausting the budget is
        /// `Err(`[`RetryBudgetExhausted`]`)`, whose `source()` is the last [`FdbError`] — so
        /// a caller can still tell *why* the read never landed. The final failed attempt is
        /// not followed by an `on_error` backoff the caller can never spend.
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            let physical = self.physical(key);
            let mut trx = self.trx()?;
            let mut last: Option<FdbError> = None;
            for attempt in 1..=MAX_ATTEMPTS {
                match trx.get(&physical, false).await {
                    Ok(value) => return Ok(value.map(|v| Bytes::copy_from_slice(&v))),
                    Err(err) if err.is_retryable() => {
                        last = Some(err);
                        if attempt == MAX_ATTEMPTS {
                            break;
                        }
                        trx = trx.on_error(err).await?;
                    }
                    Err(err) => return Err(BoxError::from(err)),
                }
            }
            let last = last.expect("the retry arm records the error before exhausting the budget");
            Err(BoxError::from(RetryBudgetExhausted::new("get", last)))
        }

        /// Native prefix scan over the bounded range `[prefix, upper)`, read inside **one**
        /// transaction — a single read version across every internal page, so the whole
        /// materialized set is one consistent cut (the property
        /// `contract_scan_is_consistent_cut` pins,
        /// `crates/metadata-conformance/src/lib.rs:244`).
        ///
        /// **Completeness or fail-loud (#262, ADR-0011):** the full matching set is
        /// returned, or — if [`paging::SCAN_CAP`] is breached — `Err([ScanCapExceeded])`. It
        /// **never** returns a silently truncated `Vec`; a truncated `inode:` scan would
        /// corrupt GC's never-reclaim safety set (data loss). The cap breach is deterministic
        /// in the data, so it is not retried: it is returned on the first page that crosses
        /// it, before the heap grows further.
        ///
        /// A retryable failure restarts the whole scan on a fresh transaction — it never
        /// stitches pages across read versions, which would tear the cut. Order stays
        /// unspecified (callers sort or collect into a set).
        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            let start = self.physical(prefix);
            let end = keyspace::prefix_upper_bound(&start)
                .unwrap_or_else(|| keyspace::KEYSPACE_END.to_vec());

            let mut trx = self.trx()?;
            let mut last: Option<FdbError> = None;
            for attempt in 1..=MAX_ATTEMPTS {
                match self.scan_once(&trx, start.clone(), end.clone()).await {
                    Ok(out) => return Ok(out),
                    // Fail loud — return NO partial Vec (#262). Deterministic in the data:
                    // retrying would re-read the same over-cap range and fail again.
                    Err(ScanFailure::CapExceeded) => {
                        return Err(BoxError::from(ScanCapExceeded {
                            cap: self.scan_cap,
                            prefix: prefix.to_vec(),
                        }));
                    }
                    // A retryable error invalidates the whole cut: start over on the
                    // transaction `on_error` hands back (reset, with backoff applied).
                    Err(ScanFailure::Fdb(err)) if err.is_retryable() => {
                        last = Some(err);
                        if attempt == MAX_ATTEMPTS {
                            break;
                        }
                        trx = trx.on_error(err).await?;
                    }
                    Err(ScanFailure::Fdb(err)) => return Err(BoxError::from(err)),
                }
            }
            let last = last.expect("the retry arm records the error before exhausting the budget");
            Err(BoxError::from(RetryBudgetExhausted::new("scan", last)))
        }

        /// Apply `batch` atomically — the commit point.
        ///
        /// A batch **with** preconditions is a CAS: every precondition key is read
        /// non-snapshot inside the one transaction, so it joins the read-conflict set and a
        /// concurrent winner makes the commit fail with 1020 → `Ok(Conflict)`. A batch
        /// **without** preconditions (blind) is never `Conflict` — see the crate docs — and,
        /// because it is precondition-free, it is safe to retry on a definitively-not-
        /// committed retryable error. A conditional batch is **never** retried here: the
        /// caller owns that decision (`crates/server/src/cli.rs:1027-1049`).
        ///
        /// The routing rule itself is `commit_path`; this body is exactly `route_commit`, so
        /// there is no second, drifting copy of the decision.
        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            route_commit(self, batch).await
        }
    }

    /// Tests for the two rules of this driver a live `fdbserver` cannot be made to exhibit:
    /// the **commit routing rule** and the **blind commit retry policy**.
    ///
    /// Neither is reachable from a live test. `1021 commit_unknown_result` means the client
    /// lost contact after sending the commit, which no healthy cluster produces on demand.
    /// And the routing rule is invisible from outside: a blind batch sent down the
    /// conditional path still commits, because a write-only transaction has an empty
    /// read-conflict set — it merely loses its bounded retry, silently.
    /// (`tests/contention.rs` covers everything that *can* be provoked for real: the 1020
    /// lost race and a real blind-commit fault. `tests/scan.rs` covers paging and the cap.)
    ///
    /// These drive the **production** [`route_commit`], [`commit_path`],
    /// [`blind_commit_loop`] and [`blind_commit_step`] — not copies — with real
    /// [`FdbError`]s built by `FdbError::from_code`, so `is_retryable_not_committed()` is
    /// answered by `libfdb_c` itself. Only the commit *destination* and the *source* of the
    /// error are scripted; the routing, the loop, the gate and the classification are
    /// production.
    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::classify::{COMMIT_UNKNOWN_RESULT, NOT_COMMITTED};

        /// FDB error `2103 value_too_large`: a permanent, non-retryable blind-commit fault.
        const VALUE_TOO_LARGE: i32 = 2103;

        /// FDB error `1007 transaction_too_old`: retryable-not-committed. The error a blind
        /// batch loses its bounded retry of when the routing rule is broken.
        const TRANSACTION_TOO_OLD: i32 = 1007;

        /// A [`BlindCommit`] whose attempts fail with a scripted sequence of FDB error
        /// codes and then commit. Counts attempts and resets so the test can assert the
        /// batch was applied **at most once** when the outcome is unknown.
        struct ScriptedCommit {
            failures: Vec<i32>,
            attempts: usize,
            resets: usize,
        }

        impl ScriptedCommit {
            fn failing_with(failures: &[i32]) -> Self {
                Self {
                    failures: failures.to_vec(),
                    attempts: 0,
                    resets: 0,
                }
            }
        }

        #[async_trait]
        impl BlindCommit for ScriptedCommit {
            async fn attempt(&mut self) -> std::result::Result<(), FdbError> {
                let scripted = self.failures.get(self.attempts).copied();
                self.attempts += 1;
                match scripted {
                    Some(code) => Err(FdbError::from_code(code)),
                    None => Ok(()),
                }
            }

            async fn reset(&mut self) -> std::result::Result<(), FdbError> {
                self.resets += 1;
                Ok(())
            }
        }

        /// **The headline constraint.** A blind commit that returns `1021
        /// commit_unknown_result` is applied **once**, never reset-and-retried, and reaches
        /// the caller as the distinguishable [`CommitUnknownResult`] — never `Ok(Conflict)`,
        /// never a silent second application of a non-idempotent `WriteBatch`.
        ///
        /// Widen [`blind_commit_step`]'s predicate from `is_retryable_not_committed()` to
        /// `is_retryable()` — 1021 *is* retryable — and this fails on all three counts.
        #[tokio::test]
        async fn a_blind_commit_never_retries_commit_unknown_result() {
            let mut target = ScriptedCommit::failing_with(&[COMMIT_UNKNOWN_RESULT]);
            let outcome = blind_commit_loop(&mut target).await;

            assert_eq!(
                target.attempts, 1,
                "a batch whose commit outcome is unknown must be applied at most once",
            );
            assert_eq!(
                target.resets, 0,
                "1021 commit_unknown_result must never enter the retry arm",
            );
            let err = outcome.expect_err(
                "1021 must surface as Err — never Ok(Committed) (a lie) and never \
                 Ok(Conflict) (a `?`-only caller would drop the write)",
            );
            assert!(
                err.downcast_ref::<CommitUnknownResult>().is_some(),
                "1021 must be distinguishable by downcast, not an opaque fault: {err}",
            );
        }

        /// The retry arm exists and is used: a *definitively not committed* error (`1020
        /// not_committed`, which `libfdb_c` reports as `retryable_not_committed`) is reset
        /// and re-applied, bounded. Delete the arm and the first failure surfaces as `Err`.
        #[tokio::test]
        async fn a_blind_commit_retries_a_definitively_not_committed_error() {
            let mut target = ScriptedCommit::failing_with(&[NOT_COMMITTED, NOT_COMMITTED]);
            let outcome = blind_commit_loop(&mut target).await;

            assert_eq!(
                outcome.expect("a retried blind commit that finally lands is Committed"),
                CommitOutcome::Committed,
            );
            assert_eq!(target.attempts, 3, "two failures, then the commit lands");
            assert_eq!(target.resets, 2, "each retry goes through FDB's `on_error`");
        }

        /// A permanent fault is surfaced at once — never retried, and never `Conflict` (the
        /// batch is blind: it has no precondition that could have failed).
        #[tokio::test]
        async fn a_blind_commit_surfaces_a_non_retryable_fault_at_once() {
            let mut target = ScriptedCommit::failing_with(&[VALUE_TOO_LARGE]);
            let outcome = blind_commit_loop(&mut target).await;

            assert_eq!(target.attempts, 1, "a permanent fault is not retried");
            assert_eq!(target.resets, 0);
            let err = outcome.expect_err("2103 value_too_large must surface as Err");
            assert!(
                err.downcast_ref::<CommitUnknownResult>().is_none(),
                "a plain fault must not masquerade as an unknown result: {err}",
            );
        }

        /// The retry is **bounded**: a blind batch that keeps losing gives up after
        /// [`MAX_ATTEMPTS`] and surfaces the exhaustion, rather than retrying forever.
        ///
        /// It also spends no backoff it cannot use: the *last* failed attempt is not
        /// followed by a `reset` (FDB's `on_error` sleeps out a real backoff), so
        /// `MAX_ATTEMPTS` attempts cost `MAX_ATTEMPTS - 1` resets.
        #[tokio::test]
        async fn a_blind_commit_gives_up_after_max_attempts() {
            let failures = vec![NOT_COMMITTED; MAX_ATTEMPTS as usize];
            let mut target = ScriptedCommit::failing_with(&failures);
            let outcome = blind_commit_loop(&mut target).await;

            assert_eq!(target.attempts, MAX_ATTEMPTS as usize);
            assert_eq!(
                target.resets,
                MAX_ATTEMPTS as usize - 1,
                "the final failed attempt must not be charged a backoff it can never spend",
            );
            let err = outcome.expect_err("an exhausted blind commit is Err");
            assert!(
                err.to_string().contains("exhausted"),
                "the caller is told the retry budget ran out: {err}",
            );
        }

        /// **The third clause, on the reachable blind path.** A blind batch that loses
        /// `MAX_ATTEMPTS` races in a row is an `Err` — never `Ok(Conflict)`. It has no
        /// precondition to have failed, so `Conflict` is not a meaningful answer, and a
        /// `?`-only caller would read it as success and drop the write.
        ///
        /// This is the assertion that actually binds the clause on the blind path.
        /// `classify_commit_error(1020, false)` cannot: [`blind_commit_step`] claims 1020 for
        /// the retry arm long before any classifier sees it.
        #[tokio::test]
        async fn a_blind_commit_that_keeps_losing_is_err_never_conflict() {
            let failures = vec![NOT_COMMITTED; MAX_ATTEMPTS as usize];
            let mut target = ScriptedCommit::failing_with(&failures);

            match blind_commit_loop(&mut target).await {
                Ok(CommitOutcome::Conflict) => panic!(
                    "a precondition-free batch was reported Conflict after exhausting its \
                     retries — a `?`-only caller would read that as success and drop the write"
                ),
                Ok(other) => panic!("an exhausted blind commit is not Ok({other:?})"),
                Err(_) => {}
            }
        }

        /// The exhaustion error keeps the **cause** reachable. A `String`-backed
        /// "exhausted 5 attempts" would destroy the very distinction this crate promises its
        /// callers: transient `1007 transaction_too_old` vs permanent `2103 value_too_large`.
        #[tokio::test]
        async fn an_exhausted_retry_budget_carries_the_last_fdb_error_as_its_source() {
            use std::error::Error as _;

            let failures = vec![TRANSACTION_TOO_OLD; MAX_ATTEMPTS as usize];
            let mut target = ScriptedCommit::failing_with(&failures);
            let err = blind_commit_loop(&mut target)
                .await
                .expect_err("an exhausted blind commit is Err");

            let exhausted = err
                .downcast_ref::<RetryBudgetExhausted>()
                .expect("exhaustion must be a typed error, not a formatted String");
            assert_eq!(exhausted.op, "blind commit");
            assert_eq!(exhausted.attempts, MAX_ATTEMPTS);
            assert_eq!(exhausted.last.code(), TRANSACTION_TOO_OLD);

            let source = exhausted
                .source()
                .expect("the last FdbError is the exhaustion error's source");
            assert_eq!(
                source
                    .downcast_ref::<FdbError>()
                    .expect("the source downcasts to the FdbError that caused it")
                    .code(),
                TRANSACTION_TOO_OLD,
                "a caller must be able to tell a transient 1007 from a permanent 2103",
            );
        }

        /// A [`CommitPaths`] that records which path [`route_commit`] dispatched to, so the
        /// routing rule can be asserted without a cluster. Neither arm touches FDB — the
        /// *destination* is scripted; [`commit_path`] and [`route_commit`] are production.
        #[derive(Default)]
        struct RecordingPaths {
            took: std::sync::Mutex<Option<CommitPath>>,
        }

        impl RecordingPaths {
            fn took(&self) -> Option<CommitPath> {
                *self.took.lock().expect("uncontended")
            }
        }

        #[async_trait]
        impl CommitPaths for RecordingPaths {
            async fn conditional(&self, _batch: WriteBatch) -> Result<CommitOutcome> {
                *self.took.lock().expect("uncontended") = Some(CommitPath::Conditional);
                Ok(CommitOutcome::Committed)
            }

            async fn blind(&self, _batch: WriteBatch) -> Result<CommitOutcome> {
                *self.took.lock().expect("uncontended") = Some(CommitPath::Blind);
                Ok(CommitOutcome::Committed)
            }
        }

        /// **The routing rule.** A precondition-free batch must reach the *blind* path.
        ///
        /// Force `commit_path` to `Conditional` — the `let conditional = true` mutation — and
        /// this goes red. Nothing else catches it: a blind batch on the conditional path
        /// still commits against a live cluster (a write-only transaction has an empty
        /// read-conflict set, so the resolver never rejects it), it merely loses the bounded
        /// retry asserted by `a_blind_commit_retries_a_definitively_not_committed_error`,
        /// and inherits CAS classification of its commit errors.
        #[tokio::test]
        async fn a_blind_batch_routes_to_the_blind_path() {
            let target = RecordingPaths::default();
            let batch = WriteBatch::new().put(b"k".to_vec(), b"v".to_vec());

            route_commit(&target, batch).await.expect("committed");

            assert_eq!(
                target.took(),
                Some(CommitPath::Blind),
                "a batch with no preconditions is a blind write: it must take the blind \
                 path, which is what gives it a bounded retry and keeps it out of Conflict",
            );
        }

        /// The converse: any precondition — including a `require_absent`, whose `expected`
        /// is `None` — makes the batch a CAS and must reach the *conditional* path, where
        /// preconditions are read into the read-conflict set. Break this and the CAS clauses
        /// of the shared conformance suite fail, because no precondition is ever checked.
        #[tokio::test]
        async fn a_conditional_batch_routes_to_the_conditional_path() {
            for batch in [
                WriteBatch::new()
                    .require(b"k".to_vec(), b"v0".to_vec())
                    .put(b"k".to_vec(), b"v1".to_vec()),
                WriteBatch::new()
                    .require_absent(b"k".to_vec())
                    .put(b"k".to_vec(), b"v1".to_vec()),
            ] {
                let target = RecordingPaths::default();
                route_commit(&target, batch).await.expect("committed");
                assert_eq!(
                    target.took(),
                    Some(CommitPath::Conditional),
                    "a batch carrying ANY precondition is a CAS and must take the \
                     conditional path — `require_absent` included",
                );
            }
        }

        /// The routing rule is exactly `!batch.preconditions.is_empty()`, stated on the
        /// production [`commit_path`] itself. An empty batch is blind: it has nothing to
        /// check, so there is nothing to conflict with.
        #[test]
        fn commit_path_is_decided_solely_by_the_presence_of_preconditions() {
            assert_eq!(commit_path(&WriteBatch::new()), CommitPath::Blind);
            assert_eq!(
                commit_path(&WriteBatch::new().put(b"k".to_vec(), b"v".to_vec())),
                CommitPath::Blind,
            );
            assert_eq!(
                commit_path(&WriteBatch::new().require_absent(b"k".to_vec())),
                CommitPath::Conditional,
            );
        }
    }
}

#[cfg(feature = "fdb")]
pub use store::{FdbMetadataStore, RetryBudgetExhausted};

/// The `foundationdb` client crate, re-exported so a caller can name the error type this
/// store surfaces.
///
/// `commit` / `get` / `scan` return a backend fault as `Err(BoxError)` wrapping a
/// [`foundationdb::FdbError`]; a caller that wants to distinguish, say, a transient
/// `transaction_too_old` from a permanent `value_too_large` must be able to
/// `downcast_ref::<foundationdb::FdbError>()`, which requires naming the type. Exporting it
/// here means consumers do not have to add — and version-match — their own `foundationdb`
/// dependency just to read an error this crate handed them.
#[cfg(feature = "fdb")]
pub use foundationdb;
