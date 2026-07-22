//! At-scale proof of the **native, internally paged** prefix scan (M4.3, #254;
//! proposal 0015 §"Native prefix scan"). Inserts **more than one internal page**
//! of dirents under a single prefix in a fresh namespace, then asserts `scan`
//! returns the **complete** set observed as one consistent cut — never a silently
//! truncated subset (#262) — exercising the cursor-advance / short-page-termination
//! paging that the single-page shared conformance clause (`contract_scan_by_prefix`)
//! cannot reach.
//!
//! **Endpoint-gated**, exactly like `tests/conformance.rs`: with no
//! `WYRD_TIKV_PD_ENDPOINTS` set (a laptop or a PDCA worktree with no TiKV) it
//! **skips cleanly** so `cargo xtask ci` stays green; `cargo xtask tikv-conformance`
//! brings up the throwaway `deploy/` TiKV, sets the endpoint, rebuilds with
//! `--features tikv`, and runs it for real. The paging/cap **decision** logic that
//! IS observable without a TiKV lives in the `paging` unit tests in `src/lib.rs`.

#![forbid(unsafe_code)]

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
fn paged_prefix_scan_returns_the_complete_set_at_scale() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the at-scale \
             paged-scan run (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run(endpoints);
}

#[cfg(feature = "tikv")]
fn run(endpoints: Vec<String>) {
    use wyrd_metadata_tikv::paging::PAGE_SIZE;
    use wyrd_metadata_tikv::TikvMetadataStore;
    use wyrd_traits::{MetadataStore, WriteBatch};

    // Enough dirents to span MORE than one internal page — the single-shot skeleton
    // and any off-by-one in the cursor advance would drop or duplicate keys here.
    let count: usize = PAGE_SIZE as usize + PAGE_SIZE as usize / 2 + 7;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async {
        let namespace = format!("wyrd-scan/{}/paged/", std::process::id()).into_bytes();
        let store = TikvMetadataStore::connect(endpoints)
            .await
            .expect("connect to TiKV")
            .with_namespace(namespace);

        // Insert `count` dirents under `dir:` plus a decoy under a NEIGHBOURING
        // prefix that must NOT appear in the scan (bounded-range correctness).
        let mut batch = WriteBatch::new();
        for i in 0..count {
            // Zero-padded so the physical key order is well-defined; the scan
            // contract is order-UNSPECIFIED, so the assertion is set-based.
            batch = batch.put(format!("dir:{i:08}").into_bytes(), format!("v{i}"));
        }
        // `dir;` sorts immediately after the `[dir:, dir;)` range's upper bound.
        batch = batch.put(b"dir;decoy".to_vec(), "nope");
        store.commit(batch).await.expect("bulk commit");

        let hits = store.scan(b"dir:").await.expect("paged scan");

        // COMPLETENESS: exactly the inserted set, nothing truncated, no decoy, no dup.
        assert_eq!(
            hits.len(),
            count,
            "paged scan must return the COMPLETE set ({count}), never a truncated subset"
        );
        let mut keys: Vec<Vec<u8>> = hits.iter().map(|(k, _)| k.clone()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(
            keys.len(),
            count,
            "no key dropped or duplicated across pages"
        );

        // CONSISTENT CUT: every key present with its committed value, read at one
        // snapshot. (Values kept as `Vec<u8>` so the test needn't name the optional
        // `bytes` dependency — the scan yields `Bytes`, which derefs to `[u8]`.)
        let seen: std::collections::HashMap<Vec<u8>, Vec<u8>> = hits
            .into_iter()
            .map(|(k, v)| (k, v.as_ref().to_vec()))
            .collect();
        for i in 0..count {
            let key = format!("dir:{i:08}").into_bytes();
            assert_eq!(
                seen.get(&key).map(Vec::as_slice),
                Some(format!("v{i}").as_bytes()),
                "key {i} missing or wrong value in the paged scan"
            );
        }
        assert!(
            !seen.contains_key(b"dir;decoy".as_slice()),
            "the neighbouring-prefix decoy must be outside the bounded range"
        );
    });
}

#[cfg(not(feature = "tikv"))]
fn run(endpoints: Vec<String>) {
    let _ = endpoints;
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via `cargo xtask tikv-conformance`."
    );
}
