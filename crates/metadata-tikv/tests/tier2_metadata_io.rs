//! **Tier-2 single-machine metadata I/O scenario** (M4.6, #257; proposal 0015 §"DST and
//! tests", PR-sequence item 6 — the Tier-2 "one owned machine" leg).
//!
//! What this proves that neither DST nor a fault-injection tier can: the production
//! `TikvMetadataStore` path behind the unchanged trait is green on **one real machine** with
//! **real `fsync`, real NVMe, a real OS** — honest single-node durable I/O, no simulation and
//! no injected fault. It drives a durable **create → read-after-commit → overwrite (CAS) →
//! delete** cycle and asserts each step against a real TiKV, so the realism-ladder Tier-2
//! rung is exercised for the metadata backend exactly as `tier2_kill_reconstruct.rs` is for
//! the chunk plane.
//!
//! # Gating
//!
//! `#[ignore]`d and **endpoint-gated** (`WYRD_TIKV_PD_ENDPOINTS`): a clean skip on a laptop
//! or a PDCA worktree. The `run()` body below is behind `#[cfg(feature = "tikv")]`, so the
//! default `cargo test --workspace` (tikv OFF) compiles only the skeleton — the privileged
//! Tier CI job type-checks the real body in the whole-tree gate via `cargo xtask ci`'s
//! dedicated `cargo check -p wyrd-metadata-tikv --features tikv --tests` step
//! (`xtask::feature_gated_checks`), **gated on `WYRD_TIKV_TOOLCHAIN`** so the default offline
//! gate never compiles the pre-1.0 `tikv-client` tree. The live run happens only in the privileged off-Check
//! Tier-2 job (`WYRD_TIER2=1`), against a real single-node TiKV on one machine, routed via
//! `xtask::metadata_faults::metadata_scenario_args`.

#![forbid(unsafe_code)]

/// The PD endpoints, or `None` when TiKV is not configured (clean-skip gate).
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
#[ignore = "privileged Tier-2: needs a real single-node TiKV on one machine (WYRD_TIER2 job)"]
fn tier2_metadata_real_single_node_io() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the Tier-2 metadata \
             I/O scenario (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run(endpoints);
}

#[cfg(feature = "tikv")]
fn run(endpoints: Vec<String>) {
    use wyrd_testkit::converged_exactly_once;
    use wyrd_traits::{CommitOutcome, MetadataStore, WriteBatch};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let ns = format!("wyrd-tier2-io/{}/", std::process::id()).into_bytes();
        let store = wyrd_metadata_tikv::TikvMetadataStore::connect(endpoints)
            .await
            .expect("connect to TiKV")
            .with_namespace(ns);

        let key = b"file/inode".to_vec();
        let vkey = b"file/version".to_vec();

        // create (durable, real fsync/NVMe under the hood)
        assert_eq!(
            store
                .commit(
                    WriteBatch::new()
                        .require_absent(key.clone())
                        .put(key.clone(), b"v0".to_vec())
                        .put(vkey.clone(), 0u64.to_be_bytes().to_vec()),
                )
                .await
                .expect("create must not fault"),
            CommitOutcome::Committed,
        );

        // read-after-commit
        assert_eq!(
            store.get(&key).await.expect("get").as_deref(),
            Some(b"v0".as_slice()),
            "the committed value must be durably readable on the real node",
        );

        // overwrite via CAS on the version cell (exactly-once convergence)
        assert_eq!(
            store
                .commit(
                    WriteBatch::new()
                        .require(vkey.clone(), 0u64.to_be_bytes().to_vec())
                        .put(key.clone(), b"v1".to_vec())
                        .put(vkey.clone(), 1u64.to_be_bytes().to_vec()),
                )
                .await
                .expect("overwrite must not fault"),
            CommitOutcome::Committed,
        );
        let v = {
            let bytes = store.get(&vkey).await.expect("get v").expect("version");
            let mut b = [0u8; 8];
            b.copy_from_slice(&bytes[..8]);
            u64::from_be_bytes(b)
        };
        assert!(
            converged_exactly_once(0, v),
            "the CAS overwrite must advance the version by exactly one (got {v})",
        );
        assert_eq!(
            store.get(&key).await.expect("get").as_deref(),
            Some(b"v1".as_slice()),
            "read-after-commit must reflect the overwrite",
        );

        // delete (all-or-nothing cleanup)
        assert_eq!(
            store
                .commit(
                    WriteBatch::new()
                        .require(key.clone(), b"v1".to_vec())
                        .delete(key.clone())
                        .delete(vkey.clone()),
                )
                .await
                .expect("delete must not fault"),
            CommitOutcome::Committed,
        );
        assert!(
            store.get(&key).await.expect("get").is_none(),
            "the deleted key must be absent after commit",
        );
    });
}

#[cfg(not(feature = "tikv"))]
fn run(endpoints: Vec<String>) {
    let _ = endpoints;
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via the privileged Tier-2 job."
    );
}
