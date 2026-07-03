//! The atomic-commit **conflict semantics under contention** (M4.2, #253;
//! proposal 0007 / draft 0015 §"Suggested PR sequence" item 2). Where
//! `tests/conformance.rs` exercises the *sequential* CAS clauses, this suite
//! forces a real server-side **write-write race** over a live TiKV and pins the
//! contract's load-bearing partition (`crates/traits/src/lib.rs` `CommitOutcome`;
//! proposal 0007 §"The semantic translation — two conflict signals, one outcome"):
//! a **losing writer is `Ok(Conflict)`, never `Err`**. Exactly one racer commits,
//! the rest conflict, zero faults, and the final stored value is the winner's.
//!
//! The run is **endpoint-gated**, identical to `tests/conformance.rs`: with no
//! `WYRD_TIKV_PD_ENDPOINTS` set (a laptop or a PDCA worktree with no TiKV) it
//! **skips cleanly** so `cargo xtask ci` stays green; `cargo xtask tikv-conformance`
//! brings up the throwaway `deploy/` TiKV, sets the endpoint, rebuilds with
//! `--features tikv`, and runs it for real.

/// The PD (Placement Driver) endpoints, or `None` when TiKV is not configured.
fn pd_endpoints() -> Option<Vec<String>> {
    match std::env::var("WYRD_TIKV_PD_ENDPOINTS") {
        Ok(raw) if !raw.trim().is_empty() => Some(
            raw.split(',')
                .map(|e| e.trim().to_string())
                .filter(|e| !e.is_empty())
                .collect(),
        ),
        _ => None,
    }
}

/// N concurrent `commit(require(k, v0).put(k, "w{i}"))` over one shared key that
/// starts at `v0`: exactly one `Committed`, the rest `Conflict`, **zero `Err`**,
/// and the final stored value equals the winner's write.
#[test]
fn write_write_race_exactly_one_winner() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the TiKV \
             contention run (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run_write_write_race(endpoints);
}

/// N concurrent `commit(require_absent(k).put(k, "w{i}"))` over one **absent** key:
/// exactly one `Committed`, the rest `Conflict`, **zero `Err`**, and the final
/// stored value equals the winner's write.
#[test]
fn require_absent_race() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the TiKV \
             contention run (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run_require_absent_race(endpoints);
}

/// N concurrent **blind** `commit(put(k, "w{i}"))` — **no precondition** on the
/// contended key — so a loser can only be rejected by the pessimistic lock that
/// `put` eagerly acquires, not by a `get_for_update` precondition. Regression for
/// the mutator-lock gap (Codex P2 on #422): a lost race in the put/delete loop must
/// be `Ok(Conflict)`, not `Err`. Exactly one `Committed`, the rest `Conflict`, zero
/// `Err`, final value = the winner's.
#[test]
fn put_only_write_race() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the TiKV \
             contention run (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run_put_only_write_race(endpoints);
}

/// How many writers race for the one key. >1 so there is always a set of losers to
/// classify; small enough to stay fast against the single-node `deploy/` TiKV.
#[cfg(feature = "tikv")]
const WRITERS: usize = 8;

#[cfg(feature = "tikv")]
fn run_write_write_race(endpoints: Vec<String>) {
    use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

    let key = b"race:key".to_vec();
    let v0 = b"v0".to_vec();

    tikv_runtime().block_on(async move {
        let namespace = fresh_namespace("write_write_race");

        // Seed the contended key to `v0` so every racer's `require(k, v0)` holds at
        // the outset — the winner is then decided by the server-side prewrite race,
        // not by a precondition that was already false for the losers.
        let seed = connect(&endpoints, &namespace).await;
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

        let winner = drive_race(&endpoints, &namespace, |i| {
            WriteBatch::new()
                .require(key.clone(), v0.clone())
                .put(key.clone(), writer_value(i))
        })
        .await;

        assert_final_value(&endpoints, &namespace, &key, writer_value(winner)).await;
    });
}

#[cfg(feature = "tikv")]
fn run_require_absent_race(endpoints: Vec<String>) {
    use wyrd_traits::WriteBatch;

    let key = b"absent:key".to_vec();

    tikv_runtime().block_on(async move {
        // Fresh namespace ⇒ the key is absent, so every racer's `require_absent(k)`
        // holds until the server-side race picks the single first committer.
        let namespace = fresh_namespace("require_absent_race");

        let winner = drive_race(&endpoints, &namespace, |i| {
            WriteBatch::new()
                .require_absent(key.clone())
                .put(key.clone(), writer_value(i))
        })
        .await;

        assert_final_value(&endpoints, &namespace, &key, writer_value(winner)).await;
    });
}

#[cfg(feature = "tikv")]
fn run_put_only_write_race(endpoints: Vec<String>) {
    use wyrd_traits::WriteBatch;

    let key = b"put-only:key".to_vec();

    tikv_runtime().block_on(async move {
        // Fresh namespace ⇒ the key is absent, and the batch carries NO precondition
        // on it — so the ONLY place a loser can be rejected is the pessimistic lock
        // `put` takes. Before the fix a loser fell through the put loop's `Err` path;
        // it must now be `Ok(Conflict)`.
        let namespace = fresh_namespace("put_only_race");

        let winner = drive_race(&endpoints, &namespace, |i| {
            WriteBatch::new().put(key.clone(), writer_value(i))
        })
        .await;

        assert_final_value(&endpoints, &namespace, &key, writer_value(winner)).await;
    });
}

/// Fan [`WRITERS`] `commit`s — one per **independent** store connection, all sharing
/// `namespace` — at the cluster concurrently via `join_all`, and assert the
/// conflict partition: exactly one `Ok(Committed)`, the rest `Ok(Conflict)`, and
/// **zero `Err`** (a fault fails the test loudly). Returns the winner's index.
#[cfg(feature = "tikv")]
async fn drive_race<F>(endpoints: &[String], namespace: &[u8], make_batch: F) -> usize
where
    F: Fn(usize) -> wyrd_traits::WriteBatch,
{
    use futures_util::future::join_all;
    use wyrd_traits::{CommitOutcome, MetadataStore};

    // Each writer gets its OWN connection sharing the namespace, so the race is real
    // cross-connection contention at the cluster, not a same-transaction artifact.
    let mut stores = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        stores.push(connect(endpoints, namespace).await);
    }
    let batches: Vec<_> = (0..WRITERS).map(&make_batch).collect();

    let outcomes: Vec<_> = join_all(
        stores
            .iter()
            .zip(batches)
            .map(|(store, batch)| async move { store.commit(batch).await }),
    )
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
            // The whole point of #253: a lost race is a Conflict, not a fault.
            Err(e) => panic!("writer {i} surfaced a fault instead of a Conflict: {e}"),
        }
    }
    assert_eq!(committed, 1, "exactly one writer must win the race");
    assert_eq!(
        conflicts,
        WRITERS - 1,
        "every losing writer must be a Conflict, not an Err",
    );
    winner.expect("a winner")
}

/// Read the key back and assert the final stored value is the winner's write —
/// the batch landed atomically and byte-identically (no TiKV-side normalization).
#[cfg(feature = "tikv")]
async fn assert_final_value(endpoints: &[String], namespace: &[u8], key: &[u8], expected: Vec<u8>) {
    use wyrd_traits::MetadataStore;

    let reader = connect(endpoints, namespace).await;
    let stored = reader
        .get(key)
        .await
        .expect("reading back the contended key must not fault");
    assert_eq!(
        stored.as_deref(),
        Some(expected.as_slice()),
        "the final stored value must equal the winner's write",
    );
}

/// The value writer `i` attempts to store — distinct per writer so the final read
/// identifies exactly which racer won.
#[cfg(feature = "tikv")]
fn writer_value(i: usize) -> Vec<u8> {
    format!("w{i}").into_bytes()
}

/// A fresh, isolated keyspace per test (pid + tag + nanosecond stamp) so repeated
/// runs and the two tests never collide over one shared cluster — the same
/// fresh-store isolation the conformance suite uses.
#[cfg(feature = "tikv")]
fn fresh_namespace(tag: &str) -> Vec<u8> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("wyrd-contention/{}/{tag}/{nanos}/", std::process::id()).into_bytes()
}

/// Connect a store scoped to `namespace`.
#[cfg(feature = "tikv")]
async fn connect(endpoints: &[String], namespace: &[u8]) -> wyrd_metadata_tikv::TikvMetadataStore {
    wyrd_metadata_tikv::TikvMetadataStore::connect(endpoints.to_vec())
        .await
        .expect("connect to TiKV")
        .with_namespace(namespace.to_vec())
}

/// A multi-thread runtime so the racers make real concurrent progress against the
/// cluster (a genuine race, not a cooperatively-serialized one).
#[cfg(feature = "tikv")]
fn tikv_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WRITERS)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

#[cfg(not(feature = "tikv"))]
fn run_write_write_race(endpoints: Vec<String>) {
    let _ = endpoints;
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via `cargo xtask tikv-conformance`."
    );
}

#[cfg(not(feature = "tikv"))]
fn run_require_absent_race(endpoints: Vec<String>) {
    let _ = endpoints;
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via `cargo xtask tikv-conformance`."
    );
}

#[cfg(not(feature = "tikv"))]
fn run_put_only_write_race(endpoints: Vec<String>) {
    let _ = endpoints;
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via `cargo xtask tikv-conformance`."
    );
}
