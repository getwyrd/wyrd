//! Drives the **shared** `MetadataStore` trait-contract suite
//! (`wyrd-metadata-conformance`) — the identical assertions redb passes — against
//! a real `TikvMetadataStore` (proposal 0007, M4.1 DoD: "TiKV passes the shared,
//! not forked, conformance suite for the basic operations").
//!
//! The run is **endpoint-gated**: with no `WYRD_TIKV_PD_ENDPOINTS` set (a laptop
//! or a PDCA worktree with no TiKV) it **skips cleanly** so `cargo xtask ci` stays
//! green; the `xtask tikv-conformance` job brings up the throwaway `deploy/` TiKV,
//! sets the endpoint, rebuilds with `--features tikv`, and runs it for real.

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

#[test]
fn trait_contract_against_tikv() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the TiKV \
             conformance run (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run(endpoints);
}

#[cfg(feature = "tikv")]
fn run(endpoints: Vec<String>) {
    use wyrd_metadata_conformance as conformance;
    use wyrd_metadata_tikv::TikvMetadataStore;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // The whole shared contract via the single `run_all` runner, so TiKV drives the
    // identical clause set redb does with no per-driver list to drift — the
    // read-consistency clauses (#419) now run here too. `make_store(tag)` hands each
    // clause a connection scoped to a fresh, isolated per-`tag` keyspace against the one
    // shared cluster (the pid keeps concurrent CI runs from colliding).
    runtime.block_on(conformance::run_all(|tag| {
        let endpoints = endpoints.clone();
        let namespace = format!("wyrd-conformance/{}/{tag}/", std::process::id()).into_bytes();
        async move {
            TikvMetadataStore::connect(endpoints)
                .await
                .expect("connect to TiKV")
                .with_namespace(namespace)
        }
    }));
}

#[cfg(not(feature = "tikv"))]
fn run(endpoints: Vec<String>) {
    let _ = endpoints;
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via `cargo xtask tikv-conformance`."
    );
}
