//! **Every metadata operation terminates**, even when the FoundationDB cluster cannot be
//! reached (ADR-0042, issue #438).
//!
//! FoundationDB's C client sets no timeout by default — its `timeout` transaction option
//! defaults to `0`, which FDB's own option table documents as *"disable all timeouts"* — and
//! it does not give up on a cluster it cannot reach: it retries the connection indefinitely,
//! so an awaited `get` / `get_range` / `commit` future never resolves. A wrong cluster file,
//! a DNS failure or an FDB outage would therefore **hang** a metadata operation instead of
//! failing it. `store`'s `trx` puts a deadline on every transaction it creates; these tests
//! are what hold that line.
//!
//! Unlike the other three binaries this one needs **no `fdbserver`** — an unreachable
//! coordinator *is* the fixture. It points a well-formed cluster file at `192.0.2.1`
//! (RFC 5737 TEST-NET-1: routable-looking, guaranteed to answer nothing) and asserts the
//! production `get` and `commit` return `Err` promptly. Every case is wrapped in
//! [`WALL_CLOCK_GUARD`], so a regression that drops the deadline **fails the test** rather
//! than hanging the run: without `trx`'s `set_option` these tests do not merely fail an
//! assertion, they never return at all.
//!
//! What each case pins, beyond "it returns":
//!
//! * `get` surfaces the real `1031 transaction_timed_out` from the client, not some
//!   substitute error of ours.
//! * A **blind** commit that times out is [`CommitUnknownResult`], never `Ok(Committed)` and
//!   never a bare fault. FDB's guide is explicit that a timed-out transaction lacks the one
//!   guarantee `1021 commit_unknown_result` gives — *"if the commit has already been sent to
//!   the database, the transaction could get committed at a later point in time"* — so a
//!   timed-out commit is the *more* dangerous unknown, and `may_still_commit()` says so.
//! * A **conditional** commit that times out is never `Ok(Conflict)`. That is the clause with
//!   teeth: `alloc_inode` (`crates/server/src/cli.rs:1027-1049`) re-reads and retries on
//!   `Conflict`, so laundering a timeout into one would spin a CAS against a cluster that may
//!   yet apply the batch it already sent.
//!
//! Under `cargo xtask ci` the `fdb` feature is off, this binary compiles to skip notices, and
//! nothing links `libfdb_c`. `cargo xtask fdb-conformance` runs it for real.

/// How long a case may take before the harness calls it hung. Generously above the
/// [`STORE_TIMEOUT_MS`] deadline (and above the driver's `MAX_ATTEMPTS` × deadline worst
/// case), so this bound is only ever reached by an operation that is not coming back.
#[cfg(feature = "fdb")]
const WALL_CLOCK_GUARD: std::time::Duration = std::time::Duration::from_secs(30);

/// The deadline the store under test runs with. Short, so the suite is fast; the *default*
/// (`config::DEFAULT_TRANSACTION_TIMEOUT_MS`) is deliberately long enough that FDB's own
/// five-second envelope wins the race on a reachable cluster, which is a property of the
/// constant and is pinned in `config::tests`, not here.
#[cfg(feature = "fdb")]
const STORE_TIMEOUT_MS: i32 = 500;

/// A well-formed cluster file naming a coordinator that answers nothing.
///
/// RFC 5737 reserves `192.0.2.0/24` (TEST-NET-1) for documentation; it is never routed to a
/// host that responds. That models the case the deadline exists for — the client can neither
/// connect nor conclude the cluster is gone — far more faithfully than a closed local port,
/// which answers `ECONNREFUSED` immediately.
#[cfg(feature = "fdb")]
const UNREACHABLE_CLUSTER_FILE_CONTENTS: &str = "wyrdtimeout:wyrdtimeout@192.0.2.1:4500\n";

/// `get` against an unreachable cluster returns `Err`, and does so because the *client*
/// timed the transaction out — not because this driver invented an error of its own.
#[test]
fn get_against_an_unreachable_cluster_fails_rather_than_hanging() {
    run_get_times_out();
}

/// A **blind** commit that times out is an undeterminable outcome, never a silent success
/// and never a bare fault: the batch may already have been sent, and — unlike `1021` — may
/// still be applied after the error is returned.
#[test]
fn a_blind_commit_that_times_out_is_an_unknown_result() {
    run_blind_commit_times_out();
}

/// A **conditional** commit that times out is never `Ok(Conflict)`. A caller that retries on
/// `Conflict` must not be told a timeout was a lost race.
#[test]
fn a_conditional_commit_that_times_out_is_never_a_conflict() {
    run_conditional_commit_times_out();
}

/// The #441 readiness probe, driven **from inside a Tokio runtime** — the shape every
/// production caller has (`open_fdb_meta`, `crates/server/src/cli.rs:175`, is awaited from
/// seven call sites that are all already on a runtime).
///
/// Two properties, and the first is why this case exists at all:
///
/// 1. **It returns.** `preflight` owns no runtime and calls no `block_on`; it awaits on the
///    caller's. A version of it that built its own runtime and blocked on it would panic
///    here with Tokio's *"Cannot start a runtime from within a runtime"* — silently, for
///    every `wyrd … --metadata-backend fdb` invocation, since no gate compiles the `fdb`
///    feature. `guarded` drives it on a real multi-thread runtime, so that panic is a test
///    failure rather than a production one.
/// 2. **It fails honest.** An unreachable coordinator is `Unreachable`, never a guessed
///    `VersionSkew`: the client never exchanged a protocol version with anything, so it has
///    no basis to claim a mismatch (`preflight`'s fail-honest rule).
#[test]
fn preflight_against_an_unreachable_cluster_is_err_not_a_panic() {
    run_preflight_times_out();
}

#[cfg(feature = "fdb")]
fn run_get_times_out() {
    use wyrd_traits::MetadataStore;

    let store = unreachable_store("get");
    let outcome = guarded(async move { store.get(b"timeout:key").await });

    let err = outcome.expect_err("a get against an unreachable cluster must not succeed");
    assert_eq!(
        fdb_code(err.as_ref()),
        Some(wyrd_metadata_fdb::classify::TRANSACTION_TIMED_OUT),
        "the error must be FDB's own 1031 transaction_timed_out, so the deadline — not some \
         substitute of ours — is what ended the wait: {err}",
    );
}

#[cfg(feature = "fdb")]
fn run_blind_commit_times_out() {
    use wyrd_metadata_fdb::classify::{CommitUnknownResult, TRANSACTION_TIMED_OUT};
    use wyrd_traits::{MetadataStore, WriteBatch};

    let store = unreachable_store("blind_commit");
    let batch = WriteBatch::new().put(b"timeout:blind".to_vec(), b"v".to_vec());
    let outcome = guarded(async move { store.commit(batch).await });

    let err = outcome.expect_err("a blind commit against an unreachable cluster must not succeed");
    let unknown = err
        .downcast_ref::<CommitUnknownResult>()
        .unwrap_or_else(|| panic!("a timed-out commit must be a CommitUnknownResult: {err}"));

    assert_eq!(unknown.code, TRANSACTION_TIMED_OUT);
    assert!(
        unknown.may_still_commit(),
        "a timed-out commit may still be applied after the error, unlike 1021: {unknown}",
    );
}

#[cfg(feature = "fdb")]
fn run_conditional_commit_times_out() {
    use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

    let store = unreachable_store("conditional_commit");
    let batch = WriteBatch::new()
        .require_absent(b"timeout:cas".to_vec())
        .put(b"timeout:cas".to_vec(), b"v".to_vec());
    let outcome = guarded(async move { store.commit(batch).await });

    match outcome {
        Ok(CommitOutcome::Conflict) => panic!(
            "a timed-out CAS was reported as Conflict: a caller that retries on Conflict \
             (alloc_inode) would spin against a cluster that may yet apply the batch",
        ),
        Ok(CommitOutcome::Committed) => {
            panic!("a commit against an unreachable cluster cannot have committed")
        }
        Err(err) => assert!(
            !err.to_string().is_empty(),
            "the timeout must surface as an Err the caller cannot ignore",
        ),
    }
}

#[cfg(feature = "fdb")]
fn run_preflight_times_out() {
    let store = unreachable_store("preflight");
    // The production probe, on the caller's runtime — exactly as `connect()` awaits it.
    let outcome = guarded(async move { store.preflight().await });

    let err = outcome
        .expect_err("a readiness probe against an unreachable cluster must not report Ready");
    let msg = err.to_string();
    assert!(
        msg.contains("unreachable"),
        "an unreachable coordinator must be reported as unreachable: {msg}",
    );
    assert!(
        !msg.contains("mismatch"),
        "the client never exchanged a protocol version with anything, so it must not claim \
         a version mismatch — that is the misdiagnosis #441 exists to prevent: {msg}",
    );
}

/// Drive `fut` on a fresh runtime, failing the test if it does not finish within
/// [`WALL_CLOCK_GUARD`]. This is the assertion that a missing deadline is a *test failure*
/// and not a hung CI job.
#[cfg(feature = "fdb")]
fn guarded<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            match tokio::time::timeout(WALL_CLOCK_GUARD, fut).await {
                Ok(value) => value,
                Err(_elapsed) => panic!(
                    "the operation did not return within {WALL_CLOCK_GUARD:?} against an \
                     unreachable cluster — FoundationDB was left without a transaction \
                     deadline, so it waited for a cluster that never answers",
                ),
            }
        })
}

/// A store pointed at a coordinator that answers nothing, with a short deadline.
#[cfg(feature = "fdb")]
fn unreachable_store(tag: &str) -> wyrd_metadata_fdb::FdbMetadataStore {
    let path = unreachable_cluster_file(tag);
    wyrd_metadata_fdb::FdbMetadataStore::open(&path)
        .expect("opening a Database is lazy and must not depend on reaching the cluster")
        .with_transaction_timeout_ms(STORE_TIMEOUT_MS)
}

/// Write a private, well-formed cluster file naming the unreachable coordinator. FDB may
/// rewrite a cluster file it is given, so each case gets its own.
#[cfg(feature = "fdb")]
fn unreachable_cluster_file(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("wyrd-fdb-timeout/{}/{tag}", std::process::id(),));
    std::fs::create_dir_all(&dir).expect("create the throwaway cluster-file directory");
    let path = dir.join("fdb.cluster");
    std::fs::write(&path, UNREACHABLE_CLUSTER_FILE_CONTENTS).expect("write the cluster file");
    path.to_str().expect("a UTF-8 temp path").to_string()
}

/// The FDB error code carried by `err`, following `source()` so a driver error that *wraps*
/// an `FdbError` (`RetryBudgetExhausted`) is read as faithfully as a bare one.
#[cfg(feature = "fdb")]
fn fdb_code(err: &(dyn std::error::Error + 'static)) -> Option<i32> {
    let mut cursor = Some(err);
    while let Some(current) = cursor {
        if let Some(fdb) = current.downcast_ref::<wyrd_metadata_fdb::foundationdb::FdbError>() {
            return Some(fdb.code());
        }
        if let Some(unknown) =
            current.downcast_ref::<wyrd_metadata_fdb::classify::CommitUnknownResult>()
        {
            return Some(unknown.code);
        }
        cursor = current.source();
    }
    None
}

#[cfg(not(feature = "fdb"))]
fn run_get_times_out() {
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn run_blind_commit_times_out() {
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn run_conditional_commit_times_out() {
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn run_preflight_times_out() {
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn feature_off() {
    eprintln!(
        "wyrd-metadata-fdb: built without `--features fdb` — skipping the FoundationDB \
         transaction-deadline run (clean skip; the gate stays green without an FDB client)."
    );
}
