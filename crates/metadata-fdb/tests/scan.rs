//! At-scale proof of the **internally paged** prefix scan against a live `fdbserver`
//! (ADR-0042, issue #438), mirroring the TiKV peer `crates/metadata-tikv/tests/scan.rs`
//! (M4.3, #254).
//!
//! `FdbMetadataStore::scan` reads its bounded range `[prefix, upper)` inside **one**
//! transaction, following FoundationDB's `more()` paging until the range is exhausted. The
//! shared conformance clause `contract_scan_by_prefix`
//! (`crates/metadata-conformance/src/lib.rs`) only ever stores a handful of keys, so it
//! fits in FDB's **first** page and never advances the cursor: replace the paging loop's
//! `next_range` with `return Ok(out)` — a scan that silently returns its first page — and
//! the shared suite stays green. This binary is what makes that truncation fail.
//!
//! It inserts **more than one internal page** of dirents under a single prefix in a fresh
//! namespace, then asserts `scan` returns the **complete** set as one consistent cut, never
//! a truncated subset. The fixture is *grounded* first, the way `tests/contention.rs`
//! grounds FDB error 1020: before trusting the constants below to span a page boundary, the
//! test drives the raw FDB client over the same physical range and asserts the server really
//! does report `more()` — so a future client/server knob change that made the whole set fit
//! in one page fails loudly here instead of quietly disarming the test.
//!
//! The second binary-level property is the other half of the same invariant
//! (**completeness or fail loud**, #262 / ADR-0011): when the result set passes the store's
//! [`paging::SCAN_CAP`](wyrd_metadata_fdb::paging::SCAN_CAP), `scan` returns
//! `Err(ScanCapExceeded)` and **no partial `Vec`** — because a silently truncated `inode:`
//! scan corrupts GC's never-reclaim safety set (data loss). Proving that against the real
//! 2^20 cap would mean writing a million keys per run, and FDB's 5 s transaction envelope
//! would trip `1007 transaction_too_old` long before the cap did — so the test lowers the
//! store's cap with `with_scan_cap` and drives the *production* `scan` loop into the
//! *production* fail-loud arm.
//!
//! **Cluster-file-gated**, exactly like `tests/conformance.rs` and `tests/contention.rs`:
//! with no `WYRD_FDB_CLUSTER_FILE` set (a laptop or a PDCA worktree with no FDB) it **skips
//! cleanly** so `cargo xtask ci` stays green; `cargo xtask fdb-conformance` brings up the
//! throwaway `deploy/fdb-single-node` cluster, sets the cluster file, rebuilds with
//! `--features fdb`, and runs it for real.

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

/// The skip notice shared by every gate below.
fn skip(what: &str) {
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE not set — skipping the {what} run \
         (clean skip; the gate stays green without an FDB)."
    );
}

#[test]
fn paged_prefix_scan_returns_the_complete_set_at_scale() {
    let Some(cluster_file) = cluster_file() else {
        skip("at-scale paged-scan");
        return;
    };
    run(cluster_file);
}

/// **Completeness or fail loud (#262, ADR-0011).** A `scan` whose result set passes the
/// store's cap returns `Err(ScanCapExceeded)` and **no partial `Vec`**.
///
/// The cap is lowered with `with_scan_cap` so the fail-loud arm is reachable in a test (the
/// real 2^20 default would need a million keys, and FDB's 5 s transaction limit would trip
/// first). Everything else is production: the same `scan`, the same paging loop, the same
/// `paging::after_page` decision, the same `ScanCapExceeded` error.
///
/// Delete the cap check from `scan_once`, or make the breach return the partial `Vec` it has
/// accumulated instead of `Err`, and this test fails. Nothing else catches either mutation:
/// the shared conformance suite stores three keys per scan.
#[test]
fn a_scan_past_the_cap_fails_loud_and_returns_no_partial_results() {
    let Some(cluster_file) = cluster_file() else {
        skip("scan-cap fail-loud");
        return;
    };
    run_scan_cap(cluster_file);
}

/// How many dirents to store under the scanned prefix. With [`VALUE_BYTES`] this is far
/// past FoundationDB's per-page reply budget, so the range read spans several pages —
/// `assert_the_range_really_pages` proves that against the live server rather than assuming
/// it.
#[cfg(feature = "fdb")]
const DIRENTS: usize = 600;

/// The value size that, at [`DIRENTS`] keys, pushes the range well past one page (~300 KB
/// total — comfortably inside FDB's 10 MB transaction and 100 KB value limits, so the
/// fixture provokes paging and nothing else).
#[cfg(feature = "fdb")]
const VALUE_BYTES: usize = 512;

/// The scanned prefix, and the decoy that sorts immediately after its exclusive upper
/// bound (`dir;` follows `dir:`): a bounded-range scan must not return it.
#[cfg(feature = "fdb")]
const DIR_PREFIX: &[u8] = b"dir:";
#[cfg(feature = "fdb")]
const DECOY_KEY: &[u8] = b"dir;decoy";

#[cfg(feature = "fdb")]
fn run(cluster_file: String) {
    use std::collections::HashMap;

    use wyrd_traits::{MetadataStore, WriteBatch};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let prefix = fresh_prefix("paged");
        let store = wyrd_metadata_fdb::FdbMetadataStore::open(&cluster_file)
            .expect("open the FoundationDB metadata store")
            .with_prefix(prefix.clone());

        // Insert `DIRENTS` dirents under `dir:` plus a decoy under the NEIGHBOURING prefix
        // that must not appear in the scan (bounded-range correctness).
        let mut batch = WriteBatch::new();
        for i in 0..DIRENTS {
            batch = batch.put(dirent_key(i), value(i));
        }
        batch = batch.put(DECOY_KEY.to_vec(), b"nope".to_vec());
        store.commit(batch).await.expect("bulk commit");

        // Ground the fixture: this range genuinely spans more than one FDB page.
        assert_the_range_really_pages(&cluster_file, &prefix).await;

        let hits = store.scan(DIR_PREFIX).await.expect("paged scan");

        // COMPLETENESS: exactly the inserted set — nothing truncated at a page boundary,
        // no decoy, no duplicate across the cursor advance.
        assert_eq!(
            hits.len(),
            DIRENTS,
            "paged scan must return the COMPLETE set ({DIRENTS}), never a truncated subset \
             — a scan that stops after its first page lands here",
        );
        let mut keys: Vec<Vec<u8>> = hits.iter().map(|(k, _)| k.clone()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(
            keys.len(),
            DIRENTS,
            "no key dropped or duplicated across pages",
        );

        // CONSISTENT CUT: every key present with its committed value, read at one read
        // version. (Values kept as `Vec<u8>` so the test needn't name the optional `bytes`
        // dependency — the scan yields `Bytes`, which derefs to `[u8]`.)
        let seen: HashMap<Vec<u8>, Vec<u8>> = hits
            .into_iter()
            .map(|(k, v)| (k, v.as_ref().to_vec()))
            .collect();
        for i in 0..DIRENTS {
            assert_eq!(
                seen.get(&dirent_key(i)).map(Vec::as_slice),
                Some(value(i).as_slice()),
                "dirent {i} missing or wrong value in the paged scan",
            );
        }
        assert!(
            !seen.contains_key(DECOY_KEY),
            "the neighbouring-prefix decoy must be outside the bounded range",
        );
    });
}

/// The lowered cap this test scans against. Comfortably below [`DIRENTS`], so the fixture
/// genuinely crosses it; and small enough that the breach happens on the **first** page,
/// which is what proves the check runs before FDB's cursor is consulted.
#[cfg(feature = "fdb")]
const LOWERED_CAP: usize = 100;

#[cfg(feature = "fdb")]
fn run_scan_cap(cluster_file: String) {
    use wyrd_metadata_fdb::paging::ScanCapExceeded;
    use wyrd_traits::{MetadataStore, WriteBatch};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async move {
        let prefix = fresh_prefix("cap");

        // Seed with the DEFAULT cap: the write path is unaffected, and this store's own
        // `scan` would happily return all DIRENTS keys.
        let seeder = wyrd_metadata_fdb::FdbMetadataStore::open(&cluster_file)
            .expect("open the FoundationDB metadata store")
            .with_prefix(prefix.clone());

        let mut batch = WriteBatch::new();
        for i in 0..DIRENTS {
            batch = batch.put(dirent_key(i), value(i));
        }
        seeder.commit(batch).await.expect("bulk commit");

        // Ground the fixture: the uncapped store really does see all DIRENTS keys, so the
        // capped store below fails for the CAP, not because the data is missing.
        assert_eq!(
            seeder.scan(DIR_PREFIX).await.expect("uncapped scan").len(),
            DIRENTS,
            "the uncapped store must see the whole set — otherwise the cap assertion \
             below would pass for the wrong reason",
        );
        // The fixture must genuinely cross the cap, or the assertion below passes vacuously.
        const { assert!(DIRENTS > LOWERED_CAP) };

        // Now the same data through a store whose cap the set exceeds.
        let capped = wyrd_metadata_fdb::FdbMetadataStore::open(&cluster_file)
            .expect("open the FoundationDB metadata store")
            .with_prefix(prefix.clone())
            .with_scan_cap(LOWERED_CAP);

        let err = match capped.scan(DIR_PREFIX).await {
            // The whole point: NO partial Vec. A scan that returns what it managed to
            // accumulate before the cap is data loss dressed up as success — GC would treat
            // the missing `inode:` keys as unreachable and reclaim live chunks.
            Ok(partial) => panic!(
                "an over-cap scan returned Ok({} keys) — it must fail loud with \
                 ScanCapExceeded and return NO partial result set (#262, ADR-0011)",
                partial.len(),
            ),
            Err(err) => err,
        };

        let cap_err = err.downcast_ref::<ScanCapExceeded>().unwrap_or_else(|| {
            panic!(
                "an over-cap scan must be a typed ScanCapExceeded, so a \
                 caller can tell it from a backend fault; got: {err}"
            )
        });
        assert_eq!(cap_err.cap, LOWERED_CAP, "the error names the cap breached");
        assert_eq!(
            cap_err.prefix,
            DIR_PREFIX.to_vec(),
            "the error names the LOGICAL prefix the caller asked for, not the physical one",
        );

        // The cap never raises: asking for more than SCAN_CAP is clamped back to it, so a
        // caller cannot loosen a correctness constraint into a tuning knob.
        let unclampable = wyrd_metadata_fdb::FdbMetadataStore::open(&cluster_file)
            .expect("open the FoundationDB metadata store")
            .with_prefix(prefix)
            .with_scan_cap(usize::MAX);
        assert_eq!(
            unclampable.scan(DIR_PREFIX).await.expect("scan").len(),
            DIRENTS,
            "a cap above SCAN_CAP is clamped to SCAN_CAP, which {DIRENTS} keys are under",
        );
    });
}

/// Assert, against the live server, that the physical range this test scans really does
/// span **more than one** page — i.e. that FDB reports `more()` on the first `WantAll`
/// range read, exactly as `FdbMetadataStore::scan_once`'s loop sees it.
///
/// Without this, a client or server knob change that raised the per-page budget above the
/// fixture's ~300 KB would silently reduce the completeness assertion above to a
/// single-page read — the test would still pass, while no longer guarding paging at all.
#[cfg(feature = "fdb")]
async fn assert_the_range_really_pages(cluster_file: &str, prefix: &[u8]) {
    use wyrd_metadata_fdb::foundationdb::options::StreamingMode;
    use wyrd_metadata_fdb::foundationdb::{Database, RangeOption};

    let db = Database::from_path(cluster_file).expect("open a raw FDB database");
    let trx = db.create_trx().expect("create a read txn");

    let start = [prefix, DIR_PREFIX].concat();
    let mut end = start.clone();
    let last = end.last_mut().expect("a non-empty prefix");
    *last += 1; // `dir:` -> `dir;`, the exclusive upper bound.

    let mut range = RangeOption::from((start, end));
    range.mode = StreamingMode::WantAll;

    let first_page = trx
        .get_range(&range, 1, false)
        .await
        .expect("first page of the range read");
    assert!(
        first_page.more(),
        "fixture is not exercising paging: FoundationDB returned all {DIRENTS} dirents \
         ({} of them) in ONE page. Raise DIRENTS/VALUE_BYTES until the server reports \
         `more()`, or this test no longer guards the paging loop.",
        first_page.len(),
    );
}

/// The `i`-th dirent's key. Zero-padded so the physical key order is well-defined; the
/// scan contract leaves order unspecified, so the assertions above are set-based.
#[cfg(feature = "fdb")]
fn dirent_key(i: usize) -> Vec<u8> {
    format!("dir:{i:08}").into_bytes()
}

/// The `i`-th dirent's value: [`VALUE_BYTES`] long and distinct per key, so a page-boundary
/// mix-up is caught by value as well as by key.
#[cfg(feature = "fdb")]
fn value(i: usize) -> Vec<u8> {
    let mut v = format!("v{i}:").into_bytes();
    v.resize(VALUE_BYTES, b'x');
    v
}

/// A fresh, isolated key prefix per run (pid + tag + nanosecond stamp) so repeated runs
/// never collide over one shared cluster — the same fresh-store isolation the conformance
/// suite gets from `make_store(tag)`.
#[cfg(feature = "fdb")]
fn fresh_prefix(tag: &str) -> Vec<u8> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("wyrd-fdb-scan/{}/{tag}/{nanos}/", std::process::id()).into_bytes()
}

#[cfg(not(feature = "fdb"))]
fn run(cluster_file: String) {
    let _ = cluster_file;
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn run_scan_cap(cluster_file: String) {
    let _ = cluster_file;
    feature_off();
}

#[cfg(not(feature = "fdb"))]
fn feature_off() {
    eprintln!(
        "wyrd-metadata-fdb: WYRD_FDB_CLUSTER_FILE is set but the crate was built without \
         `--features fdb` — skipping. Run it via `cargo xtask fdb-conformance`."
    );
}
