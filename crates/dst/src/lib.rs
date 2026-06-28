//! Deterministic-simulation tests for the commit protocol on madsim (ADR-0009).
//!
//! This crate holds no production code — the tests live in `tests/` and run on
//! madsim's single-threaded, seed-reproducible runtime, which requires building
//! with `--cfg madsim`. Run them with `cargo xtask dst`, which sets that flag.
//!
//! ## The DST determinism barrier (ADR-0035)
//!
//! madsim virtualises the *runtime* but not the *process*: anything in a `static`
//! lives outside the simulated world and is shared across the parallel OS threads
//! `cargo test` runs the campaign's `#[madsim::test]` functions on. `tracing`'s
//! per-callsite interest cache is exactly such a process-global — the first thread
//! to touch a callsite latches its interest from the *global* default, so a callsite
//! first reached under `NoSubscriber` caches `never` and short-circuits every later
//! scoped capture, an order-dependent (not seed-dependent) outcome that defeats the
//! seed-determinism ADR-0009 rests on.
//!
//! [`install_dst_barrier`] neutralises that global once, before any campaign runs,
//! and [`dst_campaign_test`] makes installing it **unbypassable**: a campaign property
//! declared through the macro cannot be written without the barrier, so the fix is a
//! substrate property rather than a per-test convention (the gap ADR-0035 closes; the
//! superseded per-test `install_metric_dispatch()` is gone). The install is
//! **fail-loud** — a foreign global default already in place is a compromised barrier
//! and panics rather than silently dropping the guarantee (#243).

#![forbid(unsafe_code)]

use std::sync::Once;

static DST_BARRIER: Once = Once::new();

/// Install the permissive global `tracing` default exactly once, before any campaign
/// callsite is hit (ADR-0035 §2). A bare `tracing_subscriber::registry()` is interested
/// in every callsite, so a callsite's interest can never latch `never` from a
/// `NoSubscriber` first touch; scoped `with_subscriber(...)` captures still override it
/// for routing, so per-property capture stays correct and deterministic.
///
/// Prefer declaring campaign tests through [`dst_campaign_test`], which calls this for
/// you — a property cannot then be written without the barrier. Calling it directly is
/// only for tests that need the barrier outside that macro.
///
/// **Fail-loud (#243):** `set_global_default` errors if a default is already installed.
/// A foreign default means the barrier is not the process's tracing authority and
/// determinism is no longer guaranteed, so this panics rather than discarding the error.
/// Idempotent via a single shared [`Once`]: the one install runs before any campaign
/// thread observes a callsite, and later calls are no-ops.
pub fn install_dst_barrier() {
    DST_BARRIER.call_once(|| {
        tracing::subscriber::set_global_default(tracing_subscriber::registry()).expect(
            "DST determinism barrier (ADR-0035): a global tracing default was already \
             installed before the campaign barrier — seed-determinism cannot be guaranteed",
        );
    });
}

/// Declare a DST campaign property test with the [`install_dst_barrier`] preamble baked
/// in (ADR-0035 §2). This is the **only** sanctioned way to declare a custodian-campaign
/// `#[madsim::test]`: the barrier is part of the generated function, so "forgetting" it
/// is unrepresentable rather than merely discouraged.
///
/// ```ignore
/// dst_campaign_test! {
///     async fn my_property() {
///         prop_my_property(&mut rand_seed()).await;
///     }
/// }
/// ```
#[macro_export]
macro_rules! dst_campaign_test {
    ($(#[$meta:meta])* async fn $name:ident() $body:block) => {
        $(#[$meta])*
        #[::madsim::test]
        async fn $name() {
            $crate::install_dst_barrier();
            $body
        }
    };
}
