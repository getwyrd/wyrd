//! **Killing an `fdbserver` mid-commit** — #442 scope item 2, and the only place a real
//! `1021 commit_unknown_result` can be induced.
//!
//! This is the leg the rest of the battery cannot reach. The driver's unknown-result rules are
//! pinned by unit tests over the classifier, and by `tests/timeout.rs` against an unreachable
//! address — but a *healthy* cluster never emits 1021, so until now nothing exercised the real
//! thing: a commit whose fate the cluster genuinely could not report, because the process
//! handling it died with the RPC in flight.
//!
//! # The workload, and the invariant it makes checkable
//!
//! A single writer runs a **CAS chain**: batch `k` is
//! `require(version == k) + put(marker/k) + put(version = k+1)`. One writer, so there is no
//! contention — which is exactly the point. It makes three properties observable that a
//! contended workload would muddy:
//!
//! 1. **No phantom `Conflict`.** With no competing writer, *nothing* can legitimately lose a
//!    race. So `Ok(Conflict)` here is never correct: it would mean the driver mapped a fault,
//!    or an unknown result, onto "a stale writer was rejected" — telling the caller *nothing
//!    was written* when something may well have been. This is the misclassification #442 exists
//!    to hunt, and it is unobservable under contention (where a `Conflict` is legal).
//! 2. **Atomicity across the kill.** Each batch writes its marker AND bumps the version, so a
//!    torn commit is directly visible: after the dust settles, the markers present must be
//!    EXACTLY `{0 .. final_version}`. A marker without its bump — or a bump without its marker
//!    — is a batch that half-landed.
//! 3. **No double-apply.** A `WriteBatch` is not guaranteed idempotent, so a driver that
//!    silently retried an unknown-result commit could apply one twice. The CAS chain makes that
//!    self-evident: a re-applied batch `k` would have to pass `require(version == k)` a second
//!    time, which the first application already falsified.
//!
//! Every unknown result is **accounted for** (#442's acceptance criterion: "every 1021
//! occurrence accounted for"). When a commit returns `Err`, the writer does the one thing the
//! contract says it may do — it **re-reads** — and records whether the batch landed. The run
//! then asserts the chain is coherent with those observations.
//!
//! # The fault
//!
//! `docker kill --signal=KILL` on one `fdbserver` container while commits are in flight, then
//! `docker start` to let the cluster recover. `double` redundancy across 3 processes tolerates
//! one loss, so the database stays available and the chain can continue — which is what makes
//! the *post-kill* assertions meaningful rather than a study of a dead cluster.
//!
//! Runs only under `cargo xtask fdb-metadata-tier1`; skips cleanly otherwise.

#[cfg(feature = "fdb")]
use std::time::Duration;

#[cfg(feature = "fdb")]
mod support;

fn cluster_file() -> Option<String> {
    match std::env::var("WYRD_FDB_CLUSTER_FILE") {
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => None,
    }
}

#[test]
#[ignore = "privileged Tier-1: needs a live 3-process FDB cluster (cargo xtask fdb-metadata-tier1)"]
fn a_commit_interrupted_by_a_killed_fdbserver_is_never_a_phantom_conflict_and_never_tears() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!(
            "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the mid-commit kill leg \
             (clean skip; the gate stays green without an FDB)."
        );
        return;
    };
    run(cluster_file);
}

#[cfg(feature = "fdb")]
fn run(cluster_file: String) {
    use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

    // Kill the process holding the **commit_proxy** role — the process a commit RPC is
    // actually SENT to. This is the whole recipe for `1021 commit_unknown_result`: the client
    // submitted the commit, the proxy died before it could answer, and now nobody can say
    // whether the transaction landed.
    //
    // The first draft of this leg killed the *master* at a round boundary, and its own honesty
    // check caught the problem: "the kill perturbed no commit — 40 committed, 0 unknown
    // results". Killing between commits interrupts nothing (the commits are milliseconds apart
    // and the cluster simply recovers), and the master is not the process holding the RPC. Two
    // corrections, both load-bearing: target the commit proxy, and fire the kill from a
    // BACKGROUND thread while commits are in flight, not at a round boundary.
    let all = support::processes().expect(
        "WYRD_TIER1_NETNS_MAP is unset — the kill leg cannot run without a topology, and \
         silently skipping it would report a battery that never happened",
    );
    let victim = support::resolve_role_holder(&all, "commit_proxy", Duration::from_secs(90))
        .or_else(|| {
            eprintln!(
                "wyrd-tier1-fdb: no commit_proxy in status json — falling back to the master."
            );
            support::resolve_role_holder(&all, "master", Duration::from_secs(30))
        })
        .expect("could not resolve a commit-path process — refusing to kill a guess");
    eprintln!(
        "wyrd-tier1-fdb: the victim is the commit-path process at {} (container {})",
        victim.addr, victim.container,
    );
    let victim = victim.container;

    // No explicit `foundationdb::boot()`: `open` boots the process-wide network itself
    // (`ensure_network`); selecting the API version twice panics the process.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let prefix = format!("wyrd-tier1-fdb-kill/{}/", std::process::id()).into_bytes();
        let store = wyrd_metadata_fdb::FdbMetadataStore::open(&cluster_file)
            .expect("open the FoundationDB metadata store")
            .with_prefix(prefix);

        let vkey = b"kill/version".to_vec();
        store
            .commit(
                WriteBatch::new()
                    .require_absent(vkey.clone())
                    .put(vkey.clone(), 0u64.to_be_bytes().to_vec()),
            )
            .await
            .expect("seed the version cell");

        const ROUNDS: u64 = 400;

        let mut unknown_results = 0usize;
        let mut faults = 0usize;
        let mut phantom_conflicts = 0usize;
        // The batches the writer BELIEVES landed, plus the ones it could not tell about and had
        // to settle by re-reading. Every one is accounted for — nothing is shrugged off.
        let mut believed_committed: Vec<u64> = Vec::new();
        let mut settled_by_reread: Vec<u64> = Vec::new();

        // The kill fires from a BACKGROUND thread, asynchronously to the commit loop, so it
        // lands *inside* a commit RPC rather than in the gap between two. (The first draft
        // killed at a round boundary and perturbed nothing — its own honesty check said so.)
        // It runs a short kill→restart cycle a few times, because a single kill may still miss
        // the window on a fast local cluster.
        let killer_victim = victim.clone();
        let killer = std::thread::spawn(move || {
            let mut cycles = 0;
            for _ in 0..3 {
                std::thread::sleep(Duration::from_millis(250));
                eprintln!("wyrd-tier1-fdb: KILLING {killer_victim} (mid-flight)");
                if support::docker(&["kill", "--signal=KILL", &killer_victim]).is_err() {
                    break; // already dead / gone — nothing to do
                }
                cycles += 1;
                std::thread::sleep(Duration::from_millis(1500));
                eprintln!("wyrd-tier1-fdb: restarting {killer_victim}");
                let _ = support::docker(&["start", &killer_victim]);
                std::thread::sleep(Duration::from_millis(1500));
            }
            cycles
        });

        let mut k: u64 = 0;
        for round in 0..ROUNDS {
            let batch = WriteBatch::new()
                .require(vkey.clone(), k.to_be_bytes().to_vec())
                .put(format!("kill/marker/{k}").into_bytes(), b"landed".to_vec())
                .put(vkey.clone(), (k + 1).to_be_bytes().to_vec());

            match store.commit(batch).await {
                Ok(CommitOutcome::Committed) => {
                    believed_committed.push(k);
                    k += 1;
                }
                Ok(CommitOutcome::Conflict) => {
                    // THE misclassification this leg hunts. There is no other writer, so no
                    // precondition can legitimately have lost a race — a `Conflict` here is a
                    // fault or an unknown result wearing the wrong hat, and it tells the caller
                    // "nothing was written" when something may have been.
                    phantom_conflicts += 1;
                    // Re-sync `k` so the chain can continue and the run still reports the count.
                    k = read_version(&store, &vkey).await;
                }
                Err(err) => {
                    // The contract's one remedy: RE-READ to establish what happened. This is
                    // the caller obligation #437 wrote down, executed for real.
                    let unknown = err
                        .downcast_ref::<wyrd_traits::CommitUnknownResult>()
                        .is_some();
                    if unknown {
                        unknown_results += 1;
                        eprintln!(
                            "wyrd-tier1-fdb: round {round} — unknown-result commit (accounted \
                             for by re-read): {err}"
                        );
                    } else {
                        faults += 1;
                    }
                    // Settle it. The cluster may be mid-recovery, so poll rather than assume.
                    let settled = wait_for_version(&store, &vkey, Duration::from_secs(90)).await;
                    if settled == k + 1 {
                        // It DID land, despite the error. Perfectly legal — that is what an
                        // unknown result means — and the re-read is what makes it accounted for.
                        settled_by_reread.push(k);
                        k += 1;
                    } else {
                        // It did not land; the chain is unchanged and the writer retries `k`.
                        k = settled;
                    }
                }
            }
        }

        let kill_cycles = killer.join().expect("the killer thread must not panic");
        // Make sure the victim is back before the post-mortem reads, so a "cluster unreadable"
        // failure below means a REAL unrecovered cluster, not a process we left dead.
        let _ = support::docker(&["start", &victim]);

        // ── The verdict ──────────────────────────────────────────────────────────────────

        assert!(
            kill_cycles > 0,
            "the leg killed nothing — no fault was injected, so a green here would be a \
             battery that never ran",
        );

        assert_eq!(
            phantom_conflicts, 0,
            "a commit came back Ok(Conflict) {phantom_conflicts} time(s) with NO competing \
             writer — nothing could have lost a race, so the driver mapped a fault or an \
             unknown result onto `Conflict`, telling the caller nothing was written when \
             something may have been (#442, #437)",
        );

        // Atomicity across the kill: the markers present must be EXACTLY {0..final_version}.
        // A marker without its version bump, or a bump without its marker, is a torn batch.
        let final_version = wait_for_version(&store, &vkey, Duration::from_secs(120)).await;
        for i in 0..final_version {
            assert!(
                store
                    .get(&format!("kill/marker/{i}").into_bytes())
                    .await
                    .expect("read marker")
                    .is_some(),
                "version reached {final_version} but marker {i} is MISSING — batch {i} bumped \
                 the version without writing its marker: a torn commit across the kill",
            );
        }
        // …and nothing beyond it: a marker past the version cell would be a batch that wrote
        // its marker without its bump — the other half of a torn commit, and the shape a
        // silently-retried unknown-result batch would leave.
        for i in final_version..final_version + 3 {
            assert!(
                store
                    .get(&format!("kill/marker/{i}").into_bytes())
                    .await
                    .expect("read marker")
                    .is_none(),
                "marker {i} exists but the version cell is only at {final_version} — a batch \
                 wrote its marker without its version bump (a torn commit, or a double-applied \
                 batch)",
            );
        }

        // Every batch is accounted for: the chain length equals what the writer observed —
        // the ones it was told committed, plus the ones it had to settle by re-reading.
        assert_eq!(
            final_version as usize,
            believed_committed.len() + settled_by_reread.len(),
            "the chain reached {final_version} but the writer accounts for {} committed + {} \
             settled-by-re-read batches — some batch landed that no caller was told about, or \
             was told about twice",
            believed_committed.len(),
            settled_by_reread.len(),
        );

        eprintln!(
            "wyrd-tier1-fdb: mid-commit kill leg — {} committed, {} settled by re-read, \
             {unknown_results} unknown-result commit(s), {faults} plain fault(s), \
             {phantom_conflicts} phantom conflict(s); final version {final_version}",
            believed_committed.len(),
            settled_by_reread.len(),
        );
        // The leg is only meaningful if the kill actually perturbed the commit path. A run in
        // which every single commit sailed through untouched proves nothing about the
        // unknown-result rules — say so rather than bank a hollow green.
        if unknown_results == 0 && faults == 0 && settled_by_reread.is_empty() {
            eprintln!(
                "wyrd-tier1-fdb: NOTE — the kill perturbed no commit (no unknown results, no \
                 faults). The atomicity and no-phantom-conflict invariants still held, but this \
                 run did not exercise the unknown-result path. Recorded as such in the verdict."
            );
        }
    });
}

#[cfg(feature = "fdb")]
async fn read_version(store: &wyrd_metadata_fdb::FdbMetadataStore, vkey: &[u8]) -> u64 {
    use wyrd_traits::MetadataStore;
    let bytes = store
        .get(vkey)
        .await
        .expect("read the version cell")
        .expect("version cell present");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(buf)
}

/// Read the version cell, retrying while the cluster is mid-recovery (a read during an FDB
/// recovery can legitimately fail; a *permanent* failure still fails the leg via the timeout).
#[cfg(feature = "fdb")]
async fn wait_for_version(
    store: &wyrd_metadata_fdb::FdbMetadataStore,
    vkey: &[u8],
    timeout: Duration,
) -> u64 {
    use wyrd_traits::MetadataStore;
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(Some(bytes)) = store.get(vkey).await {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[..8]);
            return u64::from_be_bytes(buf);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the version cell was unreadable for {timeout:?} — the cluster never recovered from \
             the mid-commit kill, which is itself a no-go finding",
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(not(feature = "fdb"))]
fn run(cluster_file: String) {
    let _ = cluster_file;
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE is set but the crate was built without \
         `--features fdb` — skipping. Run it via `cargo xtask fdb-metadata-tier1`."
    );
}
