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

use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

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
    for (side, result) in [("left", left), ("right", right)] {
        match result {
            Ok(CommitOutcome::Committed) => {}
            Ok(CommitOutcome::Conflict) => panic!(
                "the {side} blind racer came back Conflict — a batch with no preconditions \
                 must never conflict. Callers that `?` the commit and ignore the \
                 CommitOutcome would read this as success while their write was dropped; \
                 a backend that cannot apply a blind batch owes them an Err"
            ),
            Err(_) => {} // Backend latitude: a lost race on a blind batch may be an Err.
        }
    }
    // Whatever the race decided, the key holds one of the two racers' values — the
    // loser was never silently dropped in favour of nothing.
    if let Some(value) = store.get(&racer).await.unwrap() {
        assert!(
            value.as_ref() == b"left" || value.as_ref() == b"right",
            "the surviving value must be one of the two racers', not a torn write"
        );
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
}
