//! The atomic-commit **conflict semantics under contention** for FoundationDB (ADR-0042).
//! Where `tests/conformance.rs` exercises the *sequential* CAS clauses of the shared
//! suite, this suite forces a real server-side **lost race** over a live `fdbserver` and
//! pins the load-bearing partition of the commit contract
//! (`crates/traits/src/lib.rs:346-350`; the rule TiKV reproduces at
//! `crates/metadata-tikv/src/lib.rs:542-546`):
//!
//! * a **conditional** batch (one carrying preconditions) that loses a race on a
//!   `require`d key is `Ok(CommitOutcome::Conflict)`, never `Err` — FDB signals it with
//!   error **1020 `not_committed`**, and the caller (e.g. `alloc_inode`'s budgeted backoff
//!   loop, `crates/server/src/cli.rs:1027-1049`) decides whether to retry;
//! * a **blind** batch (no preconditions) is **never** `Conflict`. It has no precondition
//!   to have failed, so a commit failure surfaces as `Err` — the many callers that `?` the
//!   result and ignore the `CommitOutcome` would otherwise read `Conflict` as success and
//!   silently drop the write.
//!
//! **This binary is the live witness for the first clause only.** The 1020 → `Conflict`
//! classification is genuinely exercised here, on a real error from a real server. The
//! second clause — "a blind batch is never `Conflict`" — cannot be witnessed live, and this
//! file does not pretend otherwise: FoundationDB never rejects a write-only transaction
//! (empty read-conflict set), so no blind commit error ever reaches the classifier. What
//! this suite contributes to that clause is its *premise*, grounded against the server
//! (`blind_write_race_never_reports_conflict`), plus a real blind commit **fault** surfacing
//! as `Err` (`blind_batch_commit_error_surfaces_as_err_never_conflict`).
//!
//! The clause itself, and the `1021 commit_unknown_result` rule (which means the client lost
//! contact after sending the commit, and which no healthy cluster emits on demand), are
//! pinned in `crates/metadata-fdb/src/lib.rs` (`store::tests`) on the production routing
//! rule, retry gate, and exhaustion error. `xtask fdb-conformance` runs them as its `--lib`
//! leg. A live test could only have re-asserted the pure classifier — green whatever the
//! blind path actually does with the error.
//!
//! The run is **cluster-file-gated**, identical to `tests/conformance.rs`: with no
//! `WYRD_FDB_CLUSTER_FILE` set it **skips cleanly** so `cargo xtask ci` stays green;
//! `cargo xtask fdb-conformance` brings up the throwaway `deploy/fdb-single-node` cluster,
//! sets the cluster file, rebuilds with `--features fdb`, and runs it for real.

// wall-clock exempt (test crate): fresh-namespace uniqueness must hold across
// RUNS against a live, persistent external cluster — a pid+counter scheme
// collides with leftovers from earlier runs; real time is the tool (#619).
// File scope (not per-site) is deliberate here: a test crate never
// ships, so no production lifecycle can acquire a mixed clock from it.
#![allow(clippy::disallowed_methods)]

/// The FoundationDB cluster file, or `None` when FDB is not configured.
fn cluster_file() -> Option<String> {
    match std::env::var("WYRD_FDB_CLUSTER_FILE") {
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => None,
    }
}

/// The skip notice shared by every gate below.
fn skip() {
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the FoundationDB \
         contention run (clean skip; the gate stays green without an FDB)."
    );
}

/// N concurrent `commit(require(k, v0).put(k, "w{i}"))` over one shared key that starts at
/// `v0`: exactly one `Committed`, the rest `Ok(Conflict)`, **zero `Err`**, and the final
/// stored value equals the winner's write.
///
/// Every racer's precondition *holds* at its own read version, so no racer takes the
/// "observed miss" path; the losers are rejected by FDB's resolver with **1020**, which is
/// precisely the classification this test binds. Negate that classification (return `Err`
/// for 1020) and this test fails.
#[test]
fn conditional_race_loser_yields_conflict() {
    let Some(cluster_file) = cluster_file() else {
        skip();
        return;
    };
    run_conditional_race(cluster_file);
}

/// N concurrent **blind** `commit(put(k, "w{i}"))` — **no precondition** on the key.
///
/// **What this test proves, precisely.** It *grounds*, against the live server, the FDB
/// property the whole never-`Conflict` rule rests on: a write-only transaction contributes
/// nothing to the read-conflict set, so the resolver cannot reject it. All N writers commit,
/// none is reported `Conflict`, none faults, and the key ends up holding one writer's bytes.
///
/// **What it does not prove.** Because the resolver never rejects a write-only transaction,
/// no error reaches the driver's classification at all — so this test cannot exercise the
/// blind path's `Conflict` arm, and its `panic!` on `Ok(Conflict)` is (on a healthy cluster)
/// unreachable. The clause "a blind batch is never `Conflict`" is bound where it *is*
/// reachable: `store::tests` in `crates/metadata-fdb/src/lib.rs`, which drives the
/// production routing rule, the retry gate and the exhaustion error. Read this test as
/// evidence for the *premise*, not for the rule.
#[test]
fn blind_write_race_never_reports_conflict() {
    let Some(cluster_file) = cluster_file() else {
        skip();
        return;
    };
    run_blind_write_race(cluster_file);
}

/// A **blind** batch whose commit genuinely fails surfaces that failure as `Err` and never
/// as `Ok(Conflict)`.
///
/// Three steps, each against the live cluster:
///
/// 1. **Ground the constant.** Drive the raw FDB client through a deterministic lost
///    conditional race (read `k`, let another transaction commit `k`, then commit) and
///    assert the error this *actual server* returns is exactly `classify::NOT_COMMITTED`.
///    Without this, step 2 would be asserting about a number someone typed rather than the
///    code FoundationDB emits.
/// 2. **Pin the production classifier on that real code, in the direction production
///    reaches.** `commit_conditional` hands a failed CAS commit to `classify_commit_error`
///    with `conditional = true`, so `classify_commit_error(1020, true) == Conflict` is a
///    genuine assertion about a production-reachable state — and it is the assertion
///    `conditional_race_loser_yields_conflict` depends on above.
///
///    The mirror-image assertion, `classify_commit_error(1020, false) == Fault`, is
///    **deliberately not made here**. It is unreachable from production: the blind path's
///    retry gate claims 1020 (`retryable_not_committed`) before any classification happens,
///    so asserting it would pin nothing while *looking* like it pinned the never-`Conflict`
///    clause. That clause is bound in `store::tests` on the production routing rule, retry
///    gate and exhaustion error. See the `outcome_from_commit_error` docs.
/// 3. **Drive a real blind batch that faults.** A value past FDB's value-size limit makes a
///    genuine, server-reported blind commit failure (`2103 value_too_large`) reach the
///    production `commit`, which must return `Err`, not `Ok(Conflict)`. This is the one
///    live, end-to-end witness that a failed blind commit is not laundered into an outcome.
#[test]
fn blind_batch_commit_error_surfaces_as_err_never_conflict() {
    let Some(cluster_file) = cluster_file() else {
        skip();
        return;
    };
    run_blind_commit_error(cluster_file);
}

/// How many writers race for the one key. >1 so there is always a set of losers to
/// classify; small enough to stay fast against the single-node `deploy/` cluster.
#[cfg(feature = "fdb")]
const WRITERS: usize = 8;

/// FoundationDB's hard value-size limit is 100_000 bytes; this is comfortably past it, so
/// the commit fails with `2103 value_too_large` — a real, server-reported blind-commit
/// fault, deterministic and needing no contention. (This is a *fixture* that provokes a
/// fault, not the transaction-envelope **check**, which is out of scope here and lands with
/// the #437 consolidation.)
#[cfg(feature = "fdb")]
const OVERSIZED_VALUE_BYTES: usize = 200_000;

/// FDB error `2103 value_too_large`: what the server reports for [`OVERSIZED_VALUE_BYTES`].
/// Asserted, not assumed — a blind commit that failed for some *other* reason would
/// otherwise satisfy "surfaces as `Err`" without exercising the path this test claims to.
#[cfg(feature = "fdb")]
const VALUE_TOO_LARGE: i32 = 2103;

#[cfg(feature = "fdb")]
fn run_conditional_race(cluster_file: String) {
    use futures_util::future::join_all;
    use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

    let key = b"race:key".to_vec();
    let v0 = b"v0".to_vec();

    fdb_runtime().block_on(async move {
        let prefix = fresh_prefix("conditional_race");

        // Seed the contended key to `v0` so every racer's `require(k, v0)` holds at its own
        // read version — the winner is then decided by the server-side resolver, not by a
        // precondition that was already false for the losers.
        let seed = connect(&cluster_file, &prefix);
        assert_eq!(
            seed.commit(
                WriteBatch::new()
                    .require_absent(key.clone())
                    .put(key.clone(), v0.clone()),
            )
            .await
            .expect("seeding the contended key must not fault"),
            CommitOutcome::Committed,
            "seeding the contended key must succeed",
        );

        // Each writer gets its OWN store handle sharing the prefix, so the race is real
        // cross-transaction contention at the cluster, not a same-transaction artifact.
        let stores: Vec<_> = (0..WRITERS)
            .map(|_| connect(&cluster_file, &prefix))
            .collect();

        let outcomes: Vec<_> = join_all(stores.iter().enumerate().map(|(i, store)| {
            let batch = WriteBatch::new()
                .require(key.clone(), v0.clone())
                .put(key.clone(), writer_value(i));
            async move { store.commit(batch).await }
        }))
        .await;

        let mut winner = None;
        let mut committed = 0usize;
        let mut conflicts = 0usize;
        for (i, outcome) in outcomes.into_iter().enumerate() {
            match outcome {
                Ok(CommitOutcome::Committed) => {
                    committed += 1;
                    winner = Some(i);
                }
                Ok(CommitOutcome::Conflict) => conflicts += 1,
                // The whole point: a lost race is a Conflict, not a fault. If the 1020
                // classification is negated, every loser lands here and this panics.
                Err(e) => panic!("writer {i} surfaced a fault instead of a Conflict: {e}"),
            }
        }
        assert_eq!(committed, 1, "exactly one writer must win the race");
        assert_eq!(
            conflicts,
            WRITERS - 1,
            "every losing writer must be a Conflict, not an Err",
        );

        // The batch landed atomically and byte-identically (no FDB-side normalization).
        let winner = winner.expect("a winner");
        let reader = connect(&cluster_file, &prefix);
        assert_eq!(
            reader
                .get(&key)
                .await
                .expect("reading back the contended key must not fault")
                .as_deref(),
            Some(writer_value(winner).as_slice()),
            "the final stored value must equal the winner's write",
        );
    });
}

#[cfg(feature = "fdb")]
fn run_blind_write_race(cluster_file: String) {
    use futures_util::future::join_all;
    use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

    let key = b"blind:key".to_vec();

    fdb_runtime().block_on(async move {
        // Fresh prefix ⇒ the key is absent; the batches carry NO precondition, so nothing
        // enters the read-conflict set and the resolver has nothing to reject.
        let prefix = fresh_prefix("blind_write_race");

        let stores: Vec<_> = (0..WRITERS)
            .map(|_| connect(&cluster_file, &prefix))
            .collect();
        let outcomes: Vec<_> = join_all(stores.iter().enumerate().map(|(i, store)| {
            let batch = WriteBatch::new().put(key.clone(), writer_value(i));
            async move { store.commit(batch).await }
        }))
        .await;

        let mut committed = 0usize;
        for (i, outcome) in outcomes.into_iter().enumerate() {
            match outcome {
                Ok(CommitOutcome::Committed) => committed += 1,
                // A precondition-free write must not masquerade as `Conflict`: a `?`-only
                // caller would mistake it for success and drop the write.
                Ok(CommitOutcome::Conflict) => panic!(
                    "blind writer {i} was reported Conflict — a precondition-free write \
                     must not masquerade as Conflict (a `?`-only caller would drop it)"
                ),
                // NOT swallowed. A blind write to an uncontended-for-reads key on a healthy
                // cluster has no reason to fault, and the driver already retried anything
                // transient (`blind_commit_loop`). Surfacing it here is the difference
                // between a test that asserts something and one that cannot fail.
                Err(e) => panic!("blind writer {i} faulted: {e}"),
            }
        }

        // Every blind writer commits. This is the grounded FDB property the whole
        // never-Conflict rule rests on: a write-only transaction contributes nothing to the
        // read-conflict set, so the resolver has nothing to reject and cannot produce 1020.
        // (Contrast `conditional_race_loser_yields_conflict`, where exactly one wins.)
        assert_eq!(
            committed, WRITERS,
            "every blind writer must commit: a write-only FDB transaction has an empty \
             read-conflict set, so the resolver cannot reject it",
        );

        // Last-writer-wins, and the winner's bytes are stored intact. Reading back is what
        // stops this test from passing on a driver whose blind `commit` returned
        // `Ok(Committed)` without ever staging the batch.
        let stored = connect(&cluster_file, &prefix)
            .get(&key)
            .await
            .expect("reading back the blind-written key must not fault")
            .expect("a committed blind write must be readable");
        let expected: Vec<Vec<u8>> = (0..WRITERS).map(writer_value).collect();
        assert!(
            expected.iter().any(|v| v.as_slice() == stored.as_ref()),
            "the stored value must be some writer's write, got {stored:?}",
        );
    });
}

#[cfg(feature = "fdb")]
fn run_blind_commit_error(cluster_file: String) {
    use wyrd_metadata_fdb::classify::{self, CommitClass};
    use wyrd_metadata_fdb::foundationdb::{Database, FdbError};
    use wyrd_traits::{MetadataStore, WriteBatch};

    fdb_runtime().block_on(async move {
        let prefix = fresh_prefix("blind_commit_error");
        // Opening the store boots the process-wide FDB network thread that the raw
        // `Database` below also needs.
        let store = connect(&cluster_file, &prefix);

        // --- 1. Ground `NOT_COMMITTED` against THIS server -----------------------------
        //
        // A deterministic lost conditional race with the raw client: `loser` takes a read
        // version and reads `k` (a non-snapshot read, so `k` joins its read-conflict set);
        // `winner` then commits a write to `k`; `loser`'s commit must be rejected. Whatever
        // code the server returns here IS the code a lost race produces — assert it is the
        // constant the driver classifies on.
        let db = Database::from_path(&cluster_file).expect("open a raw FDB database");
        let k = [prefix.as_slice(), b"ground:1020"].concat();

        let seed = db.create_trx().expect("create seed txn");
        seed.set(&k, b"v0");
        seed.commit().await.expect("seed commit");

        let loser = db.create_trx().expect("create loser txn");
        let _ = loser.get(&k, false).await.expect("loser read");

        let winner = db.create_trx().expect("create winner txn");
        winner.set(&k, b"winner");
        winner.commit().await.expect("winner commit");

        loser.set(&k, b"loser");
        let err = match loser.commit().await {
            Ok(_) => panic!("the loser of a read-write race must not commit"),
            Err(err) => err,
        };
        assert_eq!(
            err.code(),
            classify::NOT_COMMITTED,
            "a lost conditional race on this server must report FDB error 1020 \
             not_committed — the code the driver classifies on",
        );

        // --- 2. Pin the PRODUCTION classifier on that real code ------------------------
        //
        // `commit_conditional` hands a failed CAS commit to `classify_commit_error` with
        // `conditional = true`, so this asserts a production-reachable state on the code the
        // server just produced. It is the rule `conditional_race_loser_yields_conflict`
        // above depends on.
        assert_eq!(
            classify::classify_commit_error(err.code(), true),
            CommitClass::Conflict,
            "a CONDITIONAL batch that loses a race is Ok(Conflict)",
        );
        // NOT asserted here: `classify_commit_error(err.code(), false) == Fault`. It would
        // look like it pinned the never-Conflict clause and would pin nothing — the blind
        // path's retry gate claims 1020 (`retryable_not_committed`) before the classifier is
        // ever consulted, so no production input reaches that arm. The clause is bound in
        // `store::tests` (routing rule, retry gate, exhaustion error) instead. Step 3 below
        // is this file's real, end-to-end contribution to it.

        // --- 3. Drive a real blind batch that faults through PRODUCTION `commit` --------
        //
        // A value past FDB's value-size limit makes the commit fail for real, server-side.
        let outcome = store
            .commit(WriteBatch::new().put(
                b"blind:oversized".to_vec(),
                vec![0x7u8; OVERSIZED_VALUE_BYTES],
            ))
            .await;
        let err = match outcome {
            Err(err) => err,
            Ok(other) => panic!(
                "a blind batch whose commit failed must surface Err, got Ok({other:?}) — \
                 and `Conflict` in particular is forbidden for a precondition-free batch"
            ),
        };

        // The fault is the one we provoked, and it reaches the caller as a *downcastable*
        // `FdbError` — not a formatted string. A caller that must tell a permanent `2103
        // value_too_large` from a transient `1007 transaction_too_old` can still do so.
        let fdb_err = err.downcast_ref::<FdbError>().unwrap_or_else(|| {
            panic!("a blind commit fault must surface the FdbError itself, got: {err}")
        });
        assert_eq!(
            fdb_err.code(),
            VALUE_TOO_LARGE,
            "the surfaced fault is the oversized-value error this test provoked, not some \
             other failure that happens to be an Err: {err}",
        );

        // And nothing was written: an atomic batch that failed to commit leaves no key.
        assert!(
            store
                .get(b"blind:oversized")
                .await
                .expect("reading back after a failed commit must not fault")
                .is_none(),
            "a blind batch whose commit faulted must not have applied any of its puts",
        );
    });
}

/// The value writer `i` attempts to store — distinct per writer so the final read
/// identifies exactly which racer won.
#[cfg(feature = "fdb")]
fn writer_value(i: usize) -> Vec<u8> {
    format!("w{i}").into_bytes()
}

/// A fresh, isolated key prefix per test (pid + tag + nanosecond stamp) so repeated runs
/// and the separate tests never collide over one shared cluster — the same fresh-store
/// isolation the conformance suite uses.
#[cfg(feature = "fdb")]
fn fresh_prefix(tag: &str) -> Vec<u8> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("wyrd-fdb-contention/{}/{tag}/{nanos}/", std::process::id()).into_bytes()
}

/// Open a store scoped to `prefix`.
#[cfg(feature = "fdb")]
fn connect(cluster_file: &str, prefix: &[u8]) -> wyrd_metadata_fdb::FdbMetadataStore {
    wyrd_metadata_fdb::FdbMetadataStore::open(cluster_file)
        .expect("open the FoundationDB metadata store")
        .with_prefix(prefix.to_vec())
}

/// A multi-thread runtime so the racers make real concurrent progress against the cluster
/// (a genuine race, not a cooperatively-serialized one).
#[cfg(feature = "fdb")]
fn fdb_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WRITERS)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

#[cfg(not(feature = "fdb"))]
fn run_conditional_race(cluster_file: String) {
    let _ = cluster_file;
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn run_blind_write_race(cluster_file: String) {
    let _ = cluster_file;
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn run_blind_commit_error(cluster_file: String) {
    let _ = cluster_file;
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn feature_off() {
    eprintln!(
        "wyrd-metadata-fdb: the crate was built without `--features fdb` — skipping. \
         Run it via `cargo xtask fdb-conformance`."
    );
}
