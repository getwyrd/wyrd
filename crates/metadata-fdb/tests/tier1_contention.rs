//! **The contention battery against a real FoundationDB cluster** — #442 scope item 1.
//!
//! The workloads that caught the tikv-client sharp edges before the TiKV merge, now aimed at
//! FDB's mapping layer: precondition → read-conflict set, `1020 not_committed` → `Conflict`,
//! the blind-batch rule. All three assert the property the whole metadata layer rests on:
//! **a lost race is a `Conflict`, never data loss, never a phantom success, never a
//! misclassified `Err`.**
//!
//! The workloads themselves live in the SHARED `wyrd-metadata-fault-conformance` crate — the same code
//! TiKV can be judged by — so a green here means the same thing a green there does. Only the
//! store construction is FDB's.
//!
//! Why these three, specifically (from #442's scope):
//!
//! * **Rename races** — the multi-key shape `core::metadata::rename` actually issues
//!   (`require` the source binding + `require_absent` the target + `delete` + `put`). The
//!   shared conformance clause drives it *sequentially*; only a real concurrent race against a
//!   real cluster tests the commit point's re-check.
//! * **The inode-allocator hot path** — a CAS loop hammered by N clients. This is where a
//!   misclassified `Conflict` (a lost race reported as success) hands **two files the same
//!   inode**, the single worst outcome the metadata layer can produce. #429's sharded
//!   allocator reduces the pressure; the unsharded path must still be correct under it.
//! * **Blind-batch storms** — precondition-free batches at overlapping keys. A blind batch has
//!   nothing to lose, so `Conflict` is not a meaningful answer (#437); and the blind writers
//!   across the codebase `?` the commit and *ignore* the outcome, so a `Conflict` handed to
//!   them reads as success while the write vanishes.
//!
//! Cluster-file-gated: skips cleanly with no FDB, so `cargo xtask ci` stays green. Driven for
//! real by `cargo xtask fdb-metadata-tier1` (against the 3-process `deploy/fdb-multi-replica`)
//! — contention needs no fault, so a single-node cluster would also do, but the battery runs
//! it against the multi-process cluster the verdict is about.

#![forbid(unsafe_code)]

/// The FDB cluster file, or `None` when FDB is not configured (clean-skip gate).
fn cluster_file() -> Option<String> {
    match std::env::var("WYRD_FDB_CLUSTER_FILE") {
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => None,
    }
}

#[cfg(feature = "fdb")]
fn clients() -> usize {
    std::env::var("WYRD_TIER1_CONTENDERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4usize)
        .max(2)
}

#[test]
#[ignore = "privileged Tier-1: needs a live FDB cluster (cargo xtask fdb-metadata-tier1)"]
fn contention_battery_against_fdb() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!(
            "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the contention battery \
             (clean skip; the gate stays green without an FDB)."
        );
        return;
    };
    run(cluster_file);
}

#[cfg(feature = "fdb")]
fn run(cluster_file: String) {
    // No explicit `foundationdb::boot()`: `open` boots the process-wide network itself
    // (`ensure_network`); selecting the API version twice panics the process.

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let n = clients();

        // Each workload gets its OWN key prefix, so a failure names one workload and the
        // three cannot collide on keys.
        let make = |leg: &'static str| {
            let cluster_file = cluster_file.clone();
            move || {
                let cluster_file = cluster_file.clone();
                let prefix =
                    format!("wyrd-tier1-fdb-contention/{}/{leg}/", std::process::id()).into_bytes();
                async move {
                    wyrd_metadata_fdb::FdbMetadataStore::open(&cluster_file)
                        .expect("open the FoundationDB metadata store")
                        .with_prefix(prefix)
                }
            }
        };

        eprintln!("wyrd-tier1-fdb: contention leg 1/3 — rename races ({n} racers)");
        wyrd_metadata_fault_conformance::contention_rename_races(make("rename"), n).await;

        eprintln!("wyrd-tier1-fdb: contention leg 2/3 — inode-allocator hot path ({n} clients)");
        wyrd_metadata_fault_conformance::contention_inode_allocator_hot_path(make("alloc"), n, 8)
            .await;

        eprintln!("wyrd-tier1-fdb: contention leg 3/3 — blind-batch storm ({n} clients)");
        wyrd_metadata_fault_conformance::contention_blind_batch_storm(make("storm"), n, 8).await;
    });
}

#[cfg(not(feature = "fdb"))]
fn run(cluster_file: String) {
    let _ = cluster_file;
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE is set but the crate was built without \
         `--features fdb` — skipping. Run it via `cargo xtask fdb-metadata-tier1`."
    );
}
