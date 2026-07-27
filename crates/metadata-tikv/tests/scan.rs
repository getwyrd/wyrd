//! At-scale proof of the **native, internally paged** prefix scan (M4.3, #254;
//! proposal 0015 §"Native prefix scan"). Inserts **more than one internal page**
//! of dirents under a single prefix in a fresh namespace, then asserts `scan`
//! returns the **complete** set observed as one consistent cut — never a silently
//! truncated subset (#262) — exercising the cursor-advance / short-page-termination
//! paging that the single-page shared conformance clause (`contract_scan_by_prefix`)
//! cannot reach.
//!
//! The **paginated** read (`scan_page`, #634) fills one page from the same
//! `PAGE_SIZE`-bounded reads and has the same blind spot — a handful of keys is one
//! chunk, so the shared clauses never advance the cursor *within* a page — so the
//! `scan_page` binary below drives the identical at-scale fixture: a page bounded at the
//! whole population must be FILLED across chunks and carry its resume cursor, and the walk
//! over the same range must return every key exactly once in byte order.
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

/// **One `scan_page` page spans several internal `PAGE_SIZE` chunks** (#634).
///
/// The paginated read fills a page with [`PAGE_SIZE`](wyrd_metadata_tikv::paging::PAGE_SIZE)
/// -bounded reads inside one transaction, for the heap reason `scan` already pages:
/// tikv-client carries a request's `limit` unchanged into every region's shard, so one read
/// for a whole page materializes up to `regions × limit` pairs client-side. That fill loop
/// is reached by nothing else — the shared conformance clauses store a handful of keys, so
/// their pages are satisfied by the first chunk and the cursor never advances *within* a
/// page. This binary is what makes a broken advance fail: it asks for one page bounded at
/// a population larger than `PAGE_SIZE` and asserts the page is **filled** and carries its
/// resume cursor, then walks the same range and asserts every key comes back exactly once
/// in byte order.
#[test]
fn a_scan_page_that_spans_several_internal_chunks_fills_and_resumes() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the at-scale \
             scan_page run (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run_scan_page(endpoints);
}

#[cfg(feature = "tikv")]
fn run_scan_page(endpoints: Vec<String>) {
    use std::collections::HashMap;

    use wyrd_metadata_tikv::paging::PAGE_SIZE;
    use wyrd_metadata_tikv::TikvMetadataStore;
    use wyrd_traits::{MetadataStore, WriteBatch};

    // Past one internal chunk, and deliberately not a multiple of it: the fill loop
    // must handle the ragged last chunk as well as the full ones.
    let count: usize = PAGE_SIZE as usize + PAGE_SIZE as usize / 2 + 7;
    // The fixture's own invariant, asserted rather than assumed: a page bounded at
    // `count` cannot be answered in one internal read, because `paging::chunk_size`
    // caps every read at PAGE_SIZE. A `count` that quietly dropped to one chunk would
    // leave the fill loop untested while this binary still passed.
    assert!(
        count > PAGE_SIZE as usize && !count.is_multiple_of(PAGE_SIZE as usize),
        "the fixture must span more than one {PAGE_SIZE}-key chunk and end raggedly; \
         got {count}"
    );
    // A page bound smaller than the population but larger than one chunk, so the WALK
    // spans pages and each PAGE spans chunks.
    let walk_limit: usize = PAGE_SIZE as usize + 11;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async {
        let namespace = format!("wyrd-scan/{}/scan-page/", std::process::id()).into_bytes();
        let store = TikvMetadataStore::connect(endpoints)
            .await
            .expect("connect to TiKV")
            .with_namespace(namespace);

        let mut batch = WriteBatch::new();
        for i in 0..count {
            batch = batch.put(format!("dir:{i:08}").into_bytes(), format!("v{i}"));
        }
        batch = batch.put(b"dir;decoy".to_vec(), "nope");
        store.commit(batch).await.expect("bulk commit");

        // ONE page, bounded at the whole population: it must fill across chunks.
        let (items, next) = store
            .scan_page(b"dir:", None, count)
            .await
            .expect("a page bounded at the whole population");
        assert_eq!(
            items.len(),
            count,
            "one page bounded at {count} must be FILLED to {count} across the internal \
             {PAGE_SIZE}-key chunks — a fill loop that stops at the first chunk returns a \
             short page, which the seam then labels `next: None`, and the caller stops \
             walking a prefix that is not exhausted"
        );
        assert_eq!(
            next.as_deref(),
            items.last().map(|(k, _)| k.as_slice()),
            "a page at its bound carries its last key as the resume cursor (0016:2657-2658)"
        );

        // …and the whole walk: every key exactly once, in byte order, with its value.
        let mut seen: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut order: Vec<Vec<u8>> = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        let mut pages = 0usize;
        loop {
            let (page, next) = store
                .scan_page(b"dir:", after.as_deref(), walk_limit)
                .await
                .expect("one page of the walk");
            assert!(
                page.len() <= walk_limit,
                "a page must not exceed the caller's limit: asked {walk_limit}, got {}",
                page.len()
            );
            for (key, value) in page {
                assert!(
                    seen.insert(key.clone(), value.as_ref().to_vec()).is_none(),
                    "key {key:?} came back twice in one walk"
                );
                order.push(key);
            }
            pages += 1;
            assert!(pages <= 16, "the walk over {count} keys did not terminate");
            match next {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        assert!(
            pages > 1,
            "the walk finished in one page — raise `count` or lower `walk_limit`, or it no \
             longer exercises the cursor across pages"
        );
        assert_eq!(
            seen.len(),
            count,
            "the walk must return the COMPLETE set ({count}) — a skipped key is an \
             obligation retained forever (0016:2653-2660)"
        );
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(
            order, sorted,
            "the walk is ordered by raw byte-lexicographic key"
        );
        for i in 0..count {
            let key = format!("dir:{i:08}").into_bytes();
            assert_eq!(
                seen.get(&key).map(Vec::as_slice),
                Some(format!("v{i}").as_bytes()),
                "key {i} missing or carrying the wrong value in the paged walk"
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
    feature_off();
}

#[cfg(not(feature = "tikv"))]
fn run_scan_page(endpoints: Vec<String>) {
    let _ = endpoints;
    feature_off();
}

#[cfg(not(feature = "tikv"))]
fn feature_off() {
    eprintln!(
        "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS is set but the crate was built without \
         `--features tikv` — skipping. Run it via `cargo xtask tikv-conformance`."
    );
}
