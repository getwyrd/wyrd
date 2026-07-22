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

/// The **production** constructor, driven the way production drives it: `connect()` awaited
/// from inside a Tokio runtime, resolving its own cluster file from `WYRD_FDB_CLUSTER_FILE`
/// (#441).
///
/// `open()` — what the suite above uses — is deliberately probe-free. `connect()` is the
/// operator path (`open_fdb_meta`, `crates/server/src/cli.rs:175`, awaited from seven call
/// sites), and it is the one that runs the version-skew readiness probe. This case is the
/// only place the *whole* production entry point runs: env resolution → `Database::new` →
/// `preflight().await` → `Ok`.
///
/// It pins two things a pure unit test cannot:
///
/// 1. **No nested runtime.** Every caller of `connect()` is already on a runtime, so a
///    `connect()` that drove its probe on a runtime of its own would panic here with
///    *"Cannot start a runtime from within a runtime"* — the exact way a `wyrd
///    --metadata-backend fdb` invocation would die, in code no `cargo xtask ci` job
///    compiles.
/// 2. **`Ready` against a real, matched cluster.** `store::client_status` parses the real
///    `get_client_status()` JSON of the live `libfdb_c`, and `preflight::verdict` calls it
///    `Ready` — so the probe now standing in front of every production connect does not
///    reject the healthy case it must let through.
#[test]
fn connect_probes_the_real_cluster_from_inside_a_runtime() {
    let Some(cluster_file) = cluster_file() else {
        eprintln!(
            "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the FoundationDB \
             connect probe (clean skip; the gate stays green without an FDB)."
        );
        return;
    };
    run_connect(cluster_file);
}

/// `connect()` reads `WYRD_FDB_CLUSTER_FILE` itself — the value `xtask fdb-conformance`
/// exported. `cluster_file` is re-read here only so the skip gate and the failure message
/// name the same cluster; nothing mutates the environment.
#[cfg(feature = "fdb")]
fn run_connect(cluster_file: String) {
    use wyrd_metadata_fdb::FdbMetadataStore;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let store = runtime.block_on(async { FdbMetadataStore::connect().await });

    assert!(
        store.is_ok(),
        "connect() must reach the matched cluster at {cluster_file} and pass its readiness \
         probe; it returned: {:?}",
        store.err().map(|e| e.to_string()),
    );
}

#[cfg(not(feature = "fdb"))]
fn run_connect(cluster_file: String) {
    let _ = cluster_file;
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE is set but the crate was built without \
         `--features fdb` — skipping. Run it via `cargo xtask fdb-conformance`."
    );
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
