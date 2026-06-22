//! The **scrub custodian loop** (proposal 0005 §"The four custodian loops" / Scrub,
//! `0005:262-267`; §6.3 step 1 read-vs-scrub mirror `0005:264-266`; the durability
//! metrics scrub coverage + scrub-detected corruption rate `0005:331-332`;
//! PR-sequence slice 5 `0005:528-530`).
//!
//! Scrub catches **bit rot before the data is needed** — the proactive mirror of the
//! read path's read-time checksum verification (`0005:262-266`, the read path in
//! `crates/core/src/read.rs`). One pass walks each D server
//! ([`ChunkStore::list_fragments`]) and, for each fragment a **committed** chunk map
//! references, fetches its bytes and **verifies its self-describing checksum against
//! the chunk map** ([`wyrd_core::repair::fragment_intact`]). On a mismatch the
//! fragment is treated as lost — **excluded** (never fed to a decoder) — and its
//! chunk is **enqueued for reconstruction** on the one shared, durable repair queue
//! ([`wyrd_core::repair::enqueue_repair`]) that the read path also feeds
//! (`0005:174-176`). The load-bearing invariant, whose violation is **silent
//! corruption**: a checksum-failing fragment is **never absorbed silently** — it
//! always becomes a durable repair obligation (`0005:262-267`).
//!
//! Scope: scrub only **produces** repair obligations. It never dequeues, rebuilds, or
//! deletes — gathering any-`k`, recomputing, re-placing, and the version-conditional
//! commit are the reconstruction custodian (slice 6, `0005:531-536`). Reclaiming the
//! displaced bytes is GC's (slice 4). So scrub does **not** call `delete_fragment`.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): the loop stays over the
//! `traits` / `core` seams plus `tracing` — the checksum verify is borrowed from
//! `core` (which owns the on-disk-format reader), so `custodian` gains no
//! chunk-format dependency and no new on-disk-format knowledge.

use wyrd_core::repair;
use wyrd_traits::{ChunkStore, DServerId, FragmentId, MetadataStore, Result};

use crate::gc::referenced_fragments;
use crate::reconciliation::Reconciled;

/// What the scrub reconciler reads over: the authoritative metadata store (committed
/// chunk maps + the shared repair queue) and the **fleet** of D servers, each a
/// [`ChunkStore`] keyed by its stable [`DServerId`] — the same shape GC takes
/// ([`crate::GcContext`]).
///
/// This is the input the running control point hands scrub; it is **not** a deployed
/// custodian process (Option A, `0005:524-527`). The loop is correct over these
/// abstractions and reachable through the real [`crate::reconcile_step`].
pub struct ScrubContext<'a> {
    /// The authoritative metadata store (chunk maps + the repair queue).
    pub meta: &'a dyn MetadataStore,
    /// The fleet of D servers to scrub, each addressed by its stable id.
    pub fleet: &'a [(DServerId, &'a dyn ChunkStore)],
}

/// One scrub reconciliation pass over `ctx`. Dispatched only from
/// [`crate::reconcile_step`] (the fenced control point) — never a parallel entry.
/// Returns [`Reconciled::Changed`] if any chunk was enqueued for reconstruction,
/// [`Reconciled::Satisfied`] otherwise.
pub(crate) async fn reconcile(ctx: &ScrubContext<'_>, _now_millis: u64) -> Result<Reconciled> {
    // The reference set: every fragment a *committed* chunk map points at. Scrub
    // verifies exactly these — an orphan / pending-garbage fragment is GC's concern,
    // not a corruption finding (the same set GC uses as its safety gate).
    let referenced = referenced_fragments(ctx.meta).await?;

    let mut changed = false;
    for &(dserver, store) in ctx.fleet {
        for frag in store.list_fragments().await? {
            // Only fragments a committed chunk map references are scrubbed.
            if !referenced.contains(&(dserver, frag)) {
                continue;
            }
            // Fetch the bytes named by the chunk map. A fragment that vanished between
            // the walk and the fetch is a loss for GC/reconstruction to notice, not a
            // checksum finding — skip it rather than raise a false positive.
            let Some(bytes) = store.get_fragment(frag).await? else {
                continue;
            };

            // COVERAGE: a referenced fragment scrub walked and verified.
            emit_scrubbed(dserver, frag);

            // VERIFY the self-describing checksum against the committed chunk map.
            // `frag.chunk` is the id the chunk map references it under.
            if !repair::fragment_intact(&bytes, frag.chunk) {
                // CORRUPTION: exclude the failing fragment (never decode it) and
                // enqueue its chunk on the shared repair queue the read path feeds.
                emit_corruption(dserver, frag);
                repair::enqueue_repair(ctx.meta, frag.chunk, "scrub").await?;
                changed = true;
            }
        }
    }

    Ok(if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
}

/// Emit **scrub coverage** on the durability-plane seam (ADR-0011 / ADR-0012,
/// `0005:331`): one increment per referenced fragment scrub walked + verified, the
/// metric the `DurabilityTelemetry` `tracing`→OTel bridge counts, plus an
/// append-only audit event (`0005:336-340`).
fn emit_scrubbed(dserver: DServerId, frag: FragmentId) {
    tracing::info!(monotonic_counter.scrub_coverage = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.scrub.audit",
        action = "verify",
        dserver,
        chunk = %frag.chunk,
        index = frag.index,
        "scrub verified a referenced fragment's checksum against the chunk map",
    );
}

/// Emit **scrub-detected corruption** on the same seam (`0005:332`): a referenced
/// fragment that failed its checksum, now excluded and enqueued for reconstruction.
fn emit_corruption(dserver: DServerId, frag: FragmentId) {
    tracing::info!(monotonic_counter.scrub_corruption_detected = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.scrub.audit",
        action = "corruption",
        dserver,
        chunk = %frag.chunk,
        index = frag.index,
        "scrub detected bit rot: fragment excluded, chunk enqueued for reconstruction",
    );
}
