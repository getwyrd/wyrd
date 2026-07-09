//! Drives the **shared** `MetadataStore` trait-contract suite
//! (`wyrd-metadata-conformance`) — the identical assertions redb and TiKV pass — against
//! a real `FdbMetadataStore` (ADR-0042: FoundationDB is the production distributed
//! metadata backend).
//!
//! All **seven** clauses run through the one shared `run_all` runner
//! (`crates/metadata-conformance/src/lib.rs:291`), so FDB drives the identical clause set
//! with no per-driver list to drift: a new `contract_*` added there is picked up here
//! automatically. The suite is **shared, not forked** — weakening it to make FDB pass
//! would violate the very invariant it exists to enforce.
//!
//! The run is **cluster-file-gated**, exactly like `crates/metadata-tikv/tests/conformance.rs:11-34`:
//! with no `WYRD_FDB_CLUSTER_FILE` set (a laptop or a PDCA worktree with no FDB) it
//! **skips cleanly** so `cargo xtask ci` stays green; the `xtask fdb-conformance` job
//! brings up the throwaway `deploy/fdb-single-node` cluster, writes the cluster file,
//! rebuilds with `--features fdb`, and runs it for real.

/// The FoundationDB cluster file, or `None` when FDB is not configured.
fn cluster_file() -> Option<String> {
    match std::env::var("WYRD_FDB_CLUSTER_FILE") {
        Ok(raw) if !raw.trim().is_empty() => Some(raw.trim().to_string()),
        _ => None,
    }
}

#[test]
fn trait_contract_against_fdb() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!(
            "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the FoundationDB \
             conformance run (clean skip; the gate stays green without an FDB)."
        );
        return;
    };
    run(cluster_file);
}

#[cfg(feature = "fdb")]
fn run(cluster_file: String) {
    use wyrd_metadata_conformance as conformance;
    use wyrd_metadata_fdb::FdbMetadataStore;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // The whole shared contract via the single `run_all` runner. `make_store(tag)` hands
    // each of the seven clauses a store scoped to a fresh, isolated per-`tag` key prefix
    // against the one shared cluster — without that isolation the clauses corrupt each
    // other's keys.
    //
    // The prefix carries the pid AND a nanosecond stamp, matching `tests/contention.rs:334`
    // and `tests/scan.rs`: the pid alone separates concurrent runs, but a *repeat* run on a
    // cluster that was not wiped would inherit the previous run's keys — and then, say,
    // `contract_put_then_get`'s key is already present. `xtask fdb-conformance` does
    // `compose down -v` between runs, so today only the pid is load-bearing; a test's
    // isolation should not depend on its harness remembering to wipe the disk.
    //
    // Seven stores are constructed in this one process, while the FDB client permits
    // exactly one network thread per process: `FdbMetadataStore::open` boots that network
    // once behind a `OnceLock` and every store shares it.
    let run_stamp = nanos();
    runtime.block_on(conformance::run_all(|tag| {
        let cluster_file = cluster_file.clone();
        let prefix = format!(
            "wyrd-fdb-conformance/{}/{tag}/{run_stamp}/",
            std::process::id()
        )
        .into_bytes();
        async move {
            FdbMetadataStore::open(&cluster_file)
                .expect("open the FoundationDB metadata store")
                .with_prefix(prefix)
        }
    }));
}

/// Nanoseconds since the epoch — the per-run component of the isolation prefix. Taken
/// **once** per process, so all seven clauses of one `run_all` share a run stamp and are
/// separated from each other by `tag` alone, exactly as the shared suite intends.
#[cfg(feature = "fdb")]
fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}

#[cfg(not(feature = "fdb"))]
fn run(cluster_file: String) {
    let _ = cluster_file;
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE is set but the crate was built without \
         `--features fdb` — skipping. Run it via `cargo xtask fdb-conformance`."
    );
}
