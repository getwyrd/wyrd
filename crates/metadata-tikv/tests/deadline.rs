//! The operation deadline is **survivable** — it returns an error, it does not abort the
//! process (#517; the drop-safety review of #521).
//!
//! The deadline is implemented with `tokio::time::timeout`, and a timeout does not politely
//! ask a future to stop: it **cancels** it, dropping it wherever the await happened to be.
//! Every `get`/`scan`/`commit` here owns a live `tikv_client::Transaction` across those
//! awaits, and tikv-client's `Transaction::drop` **panics the process** when a still-`Active`
//! transaction is dropped (`CheckLevel::Panic` — the level `begin_pessimistic` takes). So the
//! first draft of the deadline turned a hang into a crash: a slow multi-page `scan`, a slow
//! `get_for_update`, a large conditional batch mid-`put` — cancel any of them on the deadline
//! and the process aborts instead of returning `OperationTimedOut`. A liveness guard that
//! turns a hang into a crash is not a fix.
//!
//! The fix is that this driver opens every transaction with `drop_check(CheckLevel::Warn)`
//! (`store::begin`). These tests make that claim falsifiable: they drive real operations
//! against a real TiKV under deadlines short enough to fire mid-flight, and a regression
//! would not merely fail an assertion — it would **abort the test process**, which is the
//! loudest red available.
//!
//! **Endpoint-gated**, like `tests/conformance.rs` and `tests/scan.rs`: with no
//! `WYRD_TIKV_PD_ENDPOINTS` it skips cleanly, so `cargo xtask ci` stays green on a machine
//! with no TiKV; `cargo xtask tikv-conformance` runs it for real.

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
fn a_deadline_that_fires_mid_operation_returns_an_error_and_does_not_abort_the_process() {
    let Some(endpoints) = pd_endpoints() else {
        eprintln!(
            "wyrd-metadata-tikv: WYRD_TIKV_PD_ENDPOINTS not set — skipping the deadline \
             drop-safety run (clean skip; the gate stays green without a TiKV)."
        );
        return;
    };
    run(endpoints);
}

#[cfg(feature = "tikv")]
fn run(endpoints: Vec<String>) {
    use wyrd_metadata_tikv::deadline::OperationTimedOut;
    use wyrd_metadata_tikv::TikvMetadataStore;
    use wyrd_traits::{CommitUnknownResult, MetadataStore, WriteBatch};

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        // Sweep the deadline across the risky window rather than guessing one value. The
        // cancellations that matter are the ones landing AFTER `begin()` hands back a live
        // Transaction — during the TSO, a `get_for_update`, a page of the scan, the buffered
        // puts. One fixed deadline could sit entirely before or entirely after that window on
        // a given machine; a sweep cannot.
        for deadline_ms in [1_i64, 2, 3, 5, 8, 13, 21, 34] {
            let store = TikvMetadataStore::connect(endpoints.clone())
                .await
                .expect("connect to TiKV")
                .with_namespace(format!("deadline/{deadline_ms}/"))
                .with_deadline_ms(deadline_ms);
            assert_eq!(
                store.deadline(),
                std::time::Duration::from_millis(u64::try_from(deadline_ms).expect("positive"),)
            );

            // Each operation owns a Transaction across its awaits, so each is a distinct
            // chance to drop an Active one. Whatever comes back, it must be a VALUE — the
            // process surviving this loop at all is the assertion the review is about.
            //
            // A read cut off by the deadline is a DEFINITE failure (it mutated nothing), so
            // it must never be reported as an unknown result.
            if let Err(err) = store.get(b"k").await {
                assert!(
                    err.downcast_ref::<CommitUnknownResult>().is_none(),
                    "a `get` cut off by the {deadline_ms} ms deadline mutated nothing and \
                     must not be an unknown result: {err}"
                );
                if let Some(timed_out) = err.downcast_ref::<OperationTimedOut>() {
                    assert_eq!(timed_out.op, "get");
                }
            }

            if let Err(err) = store.scan(b"k").await {
                assert!(
                    err.downcast_ref::<CommitUnknownResult>().is_none(),
                    "a `scan` cut off by the {deadline_ms} ms deadline mutated nothing and \
                     must not be an unknown result: {err}"
                );
                if let Some(timed_out) = err.downcast_ref::<OperationTimedOut>() {
                    assert_eq!(timed_out.op, "scan");
                }
            }

            // A conditional batch spends the longest in `Active` — a `get_for_update` per
            // precondition, then the eager pessimistic put — so it is the likeliest to be
            // cancelled BEFORE `commit()` moves the txn out of `Active`, which is the exact
            // window the review flagged.
            let committed = store
                .commit(
                    WriteBatch::new()
                        .require_absent(b"k".to_vec())
                        .put(b"k".to_vec(), "v"),
                )
                .await;
            if let Err(err) = committed {
                // A cancelled commit is UNDETERMINED, never a bare timeout: we stopped
                // awaiting, TiKV did not stop committing (#515). A backend fault raised
                // before the commit was ever sent is also legal here — what is NOT legal is
                // an `OperationTimedOut`, which would claim nothing was written.
                assert!(
                    err.downcast_ref::<OperationTimedOut>().is_none(),
                    "a `commit` cut off by the {deadline_ms} ms deadline must NOT claim \
                     nothing was written — the RPC may already be in flight: {err}"
                );
            }
        }
    });
}

#[cfg(not(feature = "tikv"))]
fn run(_endpoints: Vec<String>) {
    eprintln!(
        "wyrd-metadata-tikv: built without `--features tikv` — the deadline drop-safety run \
         needs the real backend."
    );
}
