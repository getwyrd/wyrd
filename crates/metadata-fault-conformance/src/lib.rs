//! The **shared** `MetadataStore` fault-conformance suite — the contract under a real cluster
//! fault and under write-write contention (#257 for TiKV, #442 for FoundationDB).
//!
//! Sibling of [`wyrd_metadata_conformance`], and the same discipline (ADR-0016: one suite pins
//! the contract for every implementation). That suite pins the contract on a **quiet** store;
//! this one pins it while a node of a real ≥3-process cluster is **symmetrically isolated**,
//! with concurrent writers racing the same compare-and-swap. Like it, this crate depends on
//! the trait surface **only** and names no concrete backend — which is exactly what lets one
//! scenario judge both TiKV and FoundationDB by identical invariants.
//!
//! It needs a Tier-1 fixture (a real multi-replica cluster + an `iptables` agent, ADR-0043)
//! and is driven by `cargo xtask metadata-tier1` / `fdb-metadata-tier1`. That is a property of
//! the *fixture it requires*, not of what it is, so it is not in the name.
//!
//! One scenario, two backends. #257's Tier-1 leg proved the **TiKV** commit path upholds the
//! ADR-0015 single-zone contract across a real cluster fault — but it was written against the
//! concrete `TikvMetadataStore`, and its fault-effect oracle asked **PD** for a store
//! heartbeat, a concept FoundationDB does not have. So #442's go/no-go could not reuse it.
//!
//! It is lifted here, generic over [`MetadataStore`], with the backend-specific parts moved
//! behind one seam ([`ClusterFault`]): *how* you cut a node, and *how you ask its peers
//! whether the cut bit*. Everything that decides PASS or FAIL — the workload, the
//! invariants, the signal arithmetic — is now **the same code for both backends**, which is
//! the only basis on which their verdicts can be compared. It is the discipline of the shared
//! `metadata-conformance` suite (ADR-0016: one suite pins the contract for every
//! implementation), applied to the fault battery.
//!
//! # What the consistency scenario proves
//!
//! That the **production commit path**, behind the unchanged trait, upholds the ADR-0015
//! single-zone contract while a node of a real ≥3-process cluster is **symmetrically
//! isolated** mid-scenario. It carries the Tier-1 *integration* leg (multi-key atomic
//! create / rename / delete, all-or-nothing) and the Tier-1 *consistency* leg
//! (read-after-commit, exactly-once convergence, and no-lost-update-under-contention as
//! **INDEPENDENT** signals, asserted **across the heal**, gated by the Invariant-B
//! fault-effect oracle).
//!
//! # Teeth
//!
//! Two properties, inherited from #257's iteration-12 amendment, are what stop this being a
//! hollow green:
//!
//! * **Contention.** The defect class this guards — a missing or mis-ordered commit-point
//!   re-check of a precondition — only fires under concurrent write-write contention. So ≥2
//!   writers, each on its **own connection**, barrier-released together, race the SAME
//!   compare-and-swap on a version cell across the fault window. Exactly one may win
//!   ([`no_lost_update`]); a reported `Conflict` must not be visible afterwards; and a
//!   deliberately **stale** CAS probe must be rejected. Weaken the re-check and a stale
//!   precondition is admitted — two winners, a lost update, red.
//! * **A fault that provably bit.** [`ConsistencySignals::fault_materialized`] is an
//!   independent signal, and the verdict cannot pass without it. A cut that the cluster never
//!   noticed proves nothing, so the oracle asks the node's **PEERS** whether they lost it —
//!   never the cut node itself, and never by probing the dropped port (which would only prove
//!   our own packets are dropped).
//!
//! # What is NOT here
//!
//! The mechanism of a cut is backend-shaped and lives in each backend's test: TiKV resolves
//! the Raft **leader** from PD and cuts it (a minority-follower cut cannot change a
//! linearizable outcome); FoundationDB cuts a **coordinator** and reads reachability from a
//! survivor's `status json`. Both implement [`ClusterFault`]; both drive the code below.

#![forbid(unsafe_code)]

use std::time::Duration;

use wyrd_testkit::{
    consistency_passes, converged_exactly_once, heal_is_complete, no_lost_update,
    partition_materialized, partition_took_effect, ConsistencySignals,
};
use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

/// A **real, symmetric cluster fault** the scenario applies mid-flight, and the peer-side
/// oracle that says whether it actually bit (Invariant B).
///
/// The seam that makes one battery judge two backends. Everything here is backend-shaped:
/// TiKV cuts the Raft leader resolved from PD and asks PD for the store's heartbeat
/// freshness; FoundationDB cuts a coordinator and asks a *surviving* process whether it can
/// still reach it (`status json` → `coordinators[].reachable`). Both answer the same
/// questions, so the scenario above them is identical.
///
/// The methods are synchronous on purpose: an implementation shells out to `docker` /
/// `iptables`, which is blocking work with no async story, and the scenario's own concurrency
/// is in the *contenders*, not in the fault.
pub trait ClusterFault {
    /// Block until the cluster is ready to take the workload (all nodes up and serving).
    fn wait_cluster_ready(&self);

    /// `(total_replicas, isolated)` — fed to [`partition_materialized`], so a cut that would
    /// destroy quorum is never mistaken for a valid minority partition.
    fn topology(&self) -> (usize, usize);

    /// Do the target's **peers** currently see it as live? Sampled BEFORE the cut, and it
    /// must be `true` or the fault-effect signal cannot pass — that is what stops a broken
    /// oracle (one that always says "not live") from manufacturing a fault that never
    /// happened.
    fn peers_see_target_live(&self) -> bool;

    /// Apply the symmetric, bidirectional isolation.
    fn apply(&self) -> Result<(), String>;

    /// Poll the peers' view for up to `timeout`, returning whether they **still** see the
    /// target live. `true` after a full timeout means the cut was a no-op — the scenario
    /// records the fault as NOT materialized and the verdict fails, honestly.
    fn peers_still_see_target_live_after(&self, timeout: Duration) -> bool;

    /// Remove every isolation rule, returning the identifiers actually removed (for
    /// [`heal_is_complete`] — a partial heal must not read as healed).
    fn heal(&self) -> Result<Vec<String>, String>;

    /// Poll for up to `timeout` until the peers see the target live again — the peer-side
    /// confirmation that the heal took.
    fn wait_peers_see_target_live(&self, timeout: Duration) -> bool;

    /// The isolation rules that were applied, for the heal-completeness check.
    fn applied_rules(&self) -> Vec<String>;
}

/// The version cell — a per-key monotonic counter, so exactly-once convergence is observable
/// as an **arithmetic delta** ([`converged_exactly_once`]) independent of any value read.
const VKEY: &[u8] = b"dir/version";
const KEY_A: &[u8] = b"dir/a";
const KEY_B: &[u8] = b"dir/b";

/// The marker key contender `i` writes iff its CAS wins. `usize::MAX` is the stale-CAS
/// probe's marker.
#[must_use]
pub fn contender_key(i: usize) -> Vec<u8> {
    if i == usize::MAX {
        b"contend/stale-probe".to_vec()
    } else {
        format!("contend/{i}").into_bytes()
    }
}

async fn read_version(store: &impl MetadataStore, vkey: &[u8]) -> u64 {
    let bytes = store
        .get(vkey)
        .await
        .expect("read version cell")
        .expect("version cell present");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(buf)
}

/// Drive the Tier-1 consistency-under-fault scenario against **any** [`MetadataStore`].
///
/// `make_store` yields a **fresh connection** each call — the contenders must be genuinely
/// separate clients, or the "race" is only a race against one client's internal mutex.
/// `fault` is `None` for a negative-control run with no cluster fault, in which case
/// `fault_materialized` is `false` and the consistency verdict **cannot pass** — honest by
/// design, and the reason a misconfigured runner fails loudly instead of reporting a green
/// battery it never ran.
///
/// Panics with the failing [`ConsistencySignals`] on any violation; that is the Tier-1 red.
pub async fn run_consistency_under_fault<S, F, Fut>(
    make_store: F,
    fault: Option<&dyn ClusterFault>,
    n_contenders: usize,
) where
    S: MetadataStore + 'static,
    F: Fn() -> Fut,
    Fut: core::future::Future<Output = S>,
{
    let n_contenders = n_contenders.max(2); // one writer cannot contend; an uncontended leg has no teeth
    let store = make_store().await;

    let vkey = VKEY.to_vec();
    let a = KEY_A.to_vec();
    let b = KEY_B.to_vec();

    // ── Tier-1 integration: multi-key atomic CREATE (all-or-nothing) ──
    assert_eq!(
        store
            .commit(
                WriteBatch::new()
                    .require_absent(a.clone())
                    .require_absent(vkey.clone())
                    .put(a.clone(), b"payload-0".to_vec())
                    .put(vkey.clone(), 0u64.to_be_bytes().to_vec()),
            )
            .await
            .expect("atomic create must not fault"),
        CommitOutcome::Committed,
        "multi-key atomic create must commit all-or-nothing",
    );
    let version_before = read_version(&store, &vkey).await;
    let read_after_create =
        store.get(&a).await.expect("get a").as_deref() == Some(b"payload-0".as_slice());

    // ── Prepare the contenders BEFORE the cut (own connections, caches warm) ──
    // Warmed with a read so the cut lands on their COMMITS, not on connection setup.
    let mut contenders = Vec::with_capacity(n_contenders);
    for _ in 0..n_contenders {
        let c = make_store().await;
        let _ = c.get(&vkey).await.expect("warm the contender's client");
        contenders.push(c);
    }

    // ── Apply the symmetric, bidirectional cut, and confirm from the PEERS' side ──
    let fault_materialized = match fault {
        Some(f) => {
            f.wait_cluster_ready();
            let live_before = f.peers_see_target_live();
            f.apply().expect("apply the symmetric cluster fault");
            let live_during = f.peers_still_see_target_live_after(Duration::from_secs(45));
            let (total, isolated) = f.topology();
            partition_materialized(total, isolated)
                && partition_took_effect(live_before, live_during)
        }
        None => false,
    };

    // ── Tier-1 integration under the fault: multi-key atomic RENAME a→b + version bump ──
    // Delete-old + put-new in ONE batch, guarded on the version cell so the commit point is a
    // single CAS. The majority side keeps quorum, so this must commit.
    assert_eq!(
        store
            .commit(
                WriteBatch::new()
                    .require(vkey.clone(), version_before.to_be_bytes().to_vec())
                    .require(a.clone(), b"payload-0".to_vec())
                    .delete(a.clone())
                    .put(b.clone(), b"payload-0".to_vec())
                    .put(vkey.clone(), (version_before + 1).to_be_bytes().to_vec()),
            )
            .await
            .expect("atomic rename must not fault on the quorum-holding side"),
        CommitOutcome::Committed,
        "the rename must commit on the quorum-holding majority side across the fault",
    );
    let version_mid = version_before + 1;

    // ── THE TEETH: n concurrent writers race the SAME CAS across the fault window ──
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(n_contenders));
    let mut tasks = Vec::with_capacity(n_contenders);
    for (i, c) in contenders.into_iter().enumerate() {
        let barrier = barrier.clone();
        let vkey = vkey.clone();
        let ckey = contender_key(i);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            c.commit(
                WriteBatch::new()
                    .require(vkey.clone(), version_mid.to_be_bytes().to_vec())
                    .put(ckey, format!("contender-{i}").into_bytes())
                    .put(vkey, (version_mid + 1).to_be_bytes().to_vec()),
            )
            .await
            .expect("a contended commit must resolve to Committed or Conflict, not fault")
        }));
    }
    let mut outcomes = Vec::with_capacity(n_contenders);
    for t in tasks {
        outcomes.push(t.await.expect("contender task must not panic"));
    }
    let committed_contenders = outcomes
        .iter()
        .filter(|o| **o == CommitOutcome::Committed)
        .count();

    // ── Stale-CAS probe: a precondition the cell has ALREADY LEFT must be rejected ──
    // Deliberately does NOT touch vkey, so it flips only the no_lost_update / marker signals
    // and never the convergence arithmetic — the clauses stay independent.
    let stale_key = contender_key(usize::MAX);
    let stale_probe_committed = store
        .commit(
            WriteBatch::new()
                .require(vkey.clone(), version_before.to_be_bytes().to_vec())
                .put(stale_key.clone(), b"stale-probe".to_vec()),
        )
        .await
        .expect("the stale probe must resolve, not fault")
        == CommitOutcome::Committed;

    // ── Heal AFTER the assertions run under the fault (heal ACROSS, not before) ──
    let heal_ok = match fault {
        Some(f) => {
            let healed = f.heal().expect("heal: remove every isolation rule");
            let live_after = f.wait_peers_see_target_live(Duration::from_secs(60));
            heal_is_complete(&f.applied_rules(), &healed, live_after)
        }
        None => true,
    };
    assert!(
        heal_ok || fault.is_none(),
        "the fault must heal completely: every isolation rule removed AND the peers see the \
         isolated node again (Invariant B — no leaked host firewall state)",
    );

    // ── Consistency signals, INDEPENDENT, asserted ACROSS the heal ──
    let version_after = read_version(&store, &vkey).await;
    // A contender's marker may be visible IFF its commit reported Committed — a visible
    // "Conflict" write is a torn commit; an invisible "Committed" write is a lost one.
    let mut markers_match_outcomes = true;
    for (i, outcome) in outcomes.iter().enumerate() {
        let present = store
            .get(&contender_key(i))
            .await
            .expect("read contender marker")
            .is_some();
        markers_match_outcomes &= present == (*outcome == CommitOutcome::Committed);
    }
    let stale_marker_absent = store
        .get(&stale_key)
        .await
        .expect("read stale-probe marker")
        .is_none()
        || stale_probe_committed;

    let signals = ConsistencySignals {
        read_after_commit: read_after_create
            && store.get(&b).await.expect("get b").as_deref() == Some(b"payload-0".as_slice())
            && store.get(&a).await.expect("get a").is_none()
            && markers_match_outcomes
            && stale_marker_absent,
        converged_once: converged_exactly_once(version_mid, version_after),
        fault_materialized,
        no_lost_update: no_lost_update(committed_contenders, stale_probe_committed),
    };
    assert!(
        consistency_passes(&signals),
        "the ADR-0015 single-zone contract must hold across the fault+heal (independent \
         signals; {n_contenders} contenders, {committed_contenders} committed): {signals:?}",
    );

    // ── Tier-1 integration: multi-key atomic DELETE (cleanup, all-or-nothing) ──
    let mut cleanup = WriteBatch::new()
        .require(b.clone(), b"payload-0".to_vec())
        .delete(b.clone())
        .delete(vkey.clone())
        .delete(stale_key);
    for i in 0..n_contenders {
        cleanup = cleanup.delete(contender_key(i));
    }
    assert_eq!(
        store
            .commit(cleanup)
            .await
            .expect("atomic delete must not fault"),
        CommitOutcome::Committed,
        "multi-key atomic delete must commit all-or-nothing",
    );
}

// ─── The contention battery (#442 scope item 1) ──────────────────────────────────────────
//
// The workloads that caught the tikv-client sharp edges pre-merge, now run against whichever
// backend is wired in. All three assert the SAME property, which is the one the whole
// metadata layer rests on: **a lost race is a `Conflict`, never data loss, never a phantom
// success, never a misclassified `Err`.**
//
// They need no cluster fault — contention alone is the adversary — so they run against any
// live cluster, and are the cheap half of the battery.

/// **Rename races** (#442): N clients race the SAME rename `a → b_i`, each guarded on the
/// binding it read. Exactly one may win; the losers must be `Conflict`, and the binding must
/// end up in exactly ONE place — never duplicated (two winners), never lost (none).
///
/// This is the multi-key shape `core::metadata::rename` actually issues (a `require` on the
/// source binding + `require_absent` on the target + `delete` + `put`), which the shared
/// conformance clause `contract_rename_race_yields_conflict` drives only *sequentially*. Here
/// it is driven **concurrently against a real cluster**, which is the only place the commit
/// point's re-check is genuinely tested.
pub async fn contention_rename_races<S, F, Fut>(make_store: F, racers: usize)
where
    S: MetadataStore + 'static,
    F: Fn() -> Fut,
    Fut: core::future::Future<Output = S>,
{
    let racers = racers.max(2);
    let seed = make_store().await;
    let src = b"race/src".to_vec();

    seed.commit(WriteBatch::new().put(src.clone(), b"binding".to_vec()))
        .await
        .expect("seed the source binding")
        .eq(&CommitOutcome::Committed)
        .then_some(())
        .expect("the seed must commit");

    // Every racer reads the SAME binding, then races to move it to its own target.
    let mut clients = Vec::with_capacity(racers);
    for _ in 0..racers {
        clients.push(make_store().await);
    }
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(racers));
    let mut tasks = Vec::with_capacity(racers);
    for (i, c) in clients.into_iter().enumerate() {
        let barrier = barrier.clone();
        let src = src.clone();
        tasks.push(tokio::spawn(async move {
            let read = c
                .get(&src)
                .await
                .expect("read the binding")
                .expect("binding exists");
            let dst = format!("race/dst-{i}").into_bytes();
            barrier.wait().await;
            let outcome = c
                .commit(
                    WriteBatch::new()
                        .require(src.clone(), read.clone())
                        .require_absent(dst.clone())
                        .delete(src)
                        .put(dst, read),
                )
                .await
                .expect("a raced rename must resolve to Committed or Conflict, never fault");
            (i, outcome)
        }));
    }
    let mut winners = Vec::new();
    for t in tasks {
        let (i, outcome) = t.await.expect("racer task must not panic");
        if outcome == CommitOutcome::Committed {
            winners.push(i);
        }
    }

    assert_eq!(
        winners.len(),
        1,
        "exactly one rename may win the race — {} won, which is a lost update (two winners) \
         or a total loss (none). Every loser must be Conflict.",
        winners.len(),
    );

    // The binding lives in exactly ONE place: the winner's target, and nowhere else.
    assert!(
        seed.get(&src).await.expect("read src").is_none(),
        "the source binding must be gone after the winning rename",
    );
    for i in 0..racers {
        let dst = format!("race/dst-{i}").into_bytes();
        let present = seed.get(&dst).await.expect("read dst").is_some();
        assert_eq!(
            present,
            winners.contains(&i),
            "target dst-{i} is {} but racer {i} {} — a rename's binding must be visible IFF \
             its commit reported Committed",
            if present { "present" } else { "absent" },
            if winners.contains(&i) { "won" } else { "lost" },
        );
    }

    // Cleanup.
    let mut cleanup = WriteBatch::new();
    for i in 0..racers {
        cleanup = cleanup.delete(format!("race/dst-{i}").into_bytes());
    }
    seed.commit(cleanup.delete(src))
        .await
        .expect("cleanup must not fault");
}

/// **The inode-allocator hot path** (#442): N clients hammer the SAME allocator cell with a
/// compare-and-swap loop, exactly as `alloc_inode` does (`crates/server/src/cli.rs`'s budgeted
/// backoff loop). Each client must eventually allocate, and **every allocated id must be
/// unique** — a duplicate id is two files sharing an inode, the single worst outcome the
/// metadata layer can produce.
///
/// The unsharded path is what is under test here. #429's sharded/batched allocator reduces the
/// pressure, but the unsharded path must still be *correct* under it, and a CAS allocator is
/// exactly where a mis-classified `Conflict` (a lost race reported as success) hands two
/// clients the same id.
pub async fn contention_inode_allocator_hot_path<S, F, Fut>(
    make_store: F,
    clients: usize,
    allocations_each: usize,
) where
    S: MetadataStore + 'static,
    F: Fn() -> Fut,
    Fut: core::future::Future<Output = S>,
{
    let clients = clients.max(2);
    let seed = make_store().await;
    let cell = b"alloc/next-inode".to_vec();
    seed.commit(
        WriteBatch::new()
            .require_absent(cell.clone())
            .put(cell.clone(), 0u64.to_be_bytes().to_vec()),
    )
    .await
    .expect("seed the allocator cell");

    let mut handles = Vec::with_capacity(clients);
    for _ in 0..clients {
        handles.push(make_store().await);
    }
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(clients));
    let mut tasks = Vec::with_capacity(clients);
    for c in handles {
        let barrier = barrier.clone();
        let cell = cell.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut mine = Vec::with_capacity(allocations_each);
            for _ in 0..allocations_each {
                // The `alloc_inode` shape: read, CAS, retry on Conflict. Unbounded here only
                // in the sense that the test would hang rather than silently under-allocate —
                // a hang is a louder failure than a skipped assertion.
                loop {
                    let current = c
                        .get(&cell)
                        .await
                        .expect("read the allocator cell")
                        .expect("allocator cell present");
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&current[..8]);
                    let next = u64::from_be_bytes(buf);
                    let outcome = c
                        .commit(
                            WriteBatch::new()
                                .require(cell.clone(), current.clone())
                                .put(cell.clone(), (next + 1).to_be_bytes().to_vec()),
                        )
                        .await
                        .expect("an allocator CAS must resolve to Committed or Conflict");
                    if outcome == CommitOutcome::Committed {
                        mine.push(next);
                        break;
                    }
                    // Conflict: someone else took this id. Re-read and try again — which is
                    // exactly what a Conflict is FOR.
                    tokio::task::yield_now().await;
                }
            }
            mine
        }));
    }

    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.expect("allocator client must not panic"));
    }

    // THE invariant: every id handed out is unique.
    let mut sorted = all.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        all.len(),
        "the allocator handed out a DUPLICATE inode id under contention ({} ids, {} distinct) \
         — two files would share an inode. A lost CAS race must be a Conflict the caller \
         retries, never a silent success.",
        all.len(),
        sorted.len(),
    );
    assert_eq!(
        all.len(),
        clients * allocations_each,
        "every client must complete every allocation",
    );
    // And the cell's final value accounts for exactly the ids handed out — no gaps, no
    // double-bumps (a double-bump would mean a commit landed that reported Conflict).
    let final_value = read_version(&seed, &cell).await;
    assert_eq!(
        final_value as usize,
        clients * allocations_each,
        "the allocator cell must have advanced by EXACTLY the number of ids handed out",
    );

    seed.commit(WriteBatch::new().delete(cell))
        .await
        .expect("cleanup must not fault");
}

/// **Blind-batch storms** (#442): N clients fire precondition-free batches at overlapping keys
/// as fast as they can. A blind batch asserted nothing about prior state, so it must NEVER
/// come back `Conflict` — it commits, or it is an `Err` the caller sees.
///
/// This is the contract clause the FDB port made load-bearing (#437, `CommitOutcome` clause 3),
/// driven here at real concurrency against a real cluster rather than in the shared suite's
/// two-future race. The failure it hunts: blind writers across the codebase (`enqueue_repair`,
/// the custodian's desired-state writes) `?` the commit and IGNORE the outcome, so a `Conflict`
/// handed to them reads as success while the write vanishes.
pub async fn contention_blind_batch_storm<S, F, Fut>(
    make_store: F,
    clients: usize,
    writes_each: usize,
) where
    S: MetadataStore + 'static,
    F: Fn() -> Fut,
    Fut: core::future::Future<Output = S>,
{
    let clients = clients.max(2);
    let seed = make_store().await;

    let mut handles = Vec::with_capacity(clients);
    for _ in 0..clients {
        handles.push(make_store().await);
    }
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(clients));
    let mut tasks = Vec::with_capacity(clients);
    for (c_idx, c) in handles.into_iter().enumerate() {
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut conflicts = 0usize;
            let mut errors = 0usize;
            // The keys THIS client was told committed. Recorded per write, not counted in
            // aggregate — see the assertion below for why that distinction is the whole test.
            let mut committed_keys: Vec<Vec<u8>> = Vec::new();
            for w in 0..writes_each {
                // Deliberately OVERLAPPING keys: every client writes the same `storm/hot`
                // cell, plus a private one. The hot cell is where a backend is tempted to
                // report a lost race — and where a blind batch must not.
                let private = format!("storm/{c_idx}/{w}").into_bytes();
                let batch = WriteBatch::new()
                    .put(b"storm/hot".to_vec(), format!("c{c_idx}-w{w}").into_bytes())
                    .put(private.clone(), b"private".to_vec());
                match c.commit(batch).await {
                    Ok(CommitOutcome::Committed) => committed_keys.push(private),
                    Ok(CommitOutcome::Conflict) => conflicts += 1,
                    Err(_) => errors += 1,
                }
            }
            (conflicts, errors, committed_keys)
        }));
    }

    let mut total_conflicts = 0usize;
    let mut total_errors = 0usize;
    let mut committed_keys: Vec<Vec<u8>> = Vec::new();
    for t in tasks {
        let (c, e, keys) = t.await.expect("storm client must not panic");
        total_conflicts += c;
        total_errors += e;
        committed_keys.extend(keys);
    }

    assert_eq!(
        total_conflicts, 0,
        "a blind batch came back Conflict {total_conflicts} times under the storm — a batch \
         with NO preconditions has nothing to lose, so a backend that cannot apply one owes \
         the caller an Err. The many blind writers that `?` the commit and ignore the \
         CommitOutcome would read this as success while their write was dropped (#437).",
    );

    // **Every key whose commit reported `Committed` must be present — checked KEY BY KEY.**
    //
    // The first draft compared aggregates: `missing.len() <= total_errors`. That let one
    // write's error EXCUSE a different write's disappearance. Concretely: client A's commit
    // errors but its key lands anyway (a legal unknown result), while client B is told
    // `Committed` and its key vanishes. Missing = 1, errors = 1, and the storm passed —
    // through the exact data-loss scenario it exists to catch. (Codex's review of #535; my own
    // comment there even conceded "we cannot attribute errors to keys", which was simply
    // false — the client knows which key it was told about.)
    //
    // A key whose commit ERRORED is not checked either way: it may be absent (the batch never
    // landed) or present (an unknown result that did land). Both are legal, and that is
    // precisely why the errored writes cannot be allowed to launder a missing successful one.
    let mut missing = Vec::new();
    for key in &committed_keys {
        if seed.get(key).await.expect("read storm key").is_none() {
            missing.push(String::from_utf8_lossy(key).into_owned());
        }
    }
    assert!(
        missing.is_empty(),
        "{} blind write(s) VANISHED after their commit reported `Committed` — the caller was \
         told the write landed and it did not. ({total_errors} other commit(s) errored; those \
         are legal and are not what this asserts.) Missing: {missing:?}",
        missing.len(),
    );

    let mut cleanup = WriteBatch::new().delete(b"storm/hot".to_vec());
    for c_idx in 0..clients {
        for w in 0..writes_each {
            cleanup = cleanup.delete(format!("storm/{c_idx}/{w}").into_bytes());
        }
    }
    seed.commit(cleanup).await.expect("cleanup must not fault");
}
