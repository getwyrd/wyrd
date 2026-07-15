//! The **scrub custodian loop** (proposal 0005 §"The four custodian loops" / Scrub,
//! `0005:262-267`; §6.3 step 1 read-vs-scrub mirror `0005:264-266`; the durability
//! metrics scrub coverage + scrub-detected corruption rate `0005:331-332`;
//! PR-sequence slice 5 `0005:528-530`).
//!
//! Scrub catches **bit rot before the data is needed** — the proactive mirror of the
//! read path's read-time checksum verification (`0005:262-266`, the read path in
//! `crates/core/src/read.rs`). One pass walks the reference set — every
//! `(dserver, fragment)` a **committed** chunk map's placement record names
//! (`referenced_fragments`) — and, for each one, fetches its bytes **directly from
//! its placed D server** ([`ChunkStore::get_fragment`]) rather than only whatever that
//! server's own listing happens to return (issue #330: a fragment that is simply
//! *absent* from the store is otherwise never observed, because nothing ever asks the
//! store for exactly that id). A fetched fragment's self-describing checksum is
//! verified against the chunk map ([`wyrd_core::repair::fragment_intact`]); a fetch
//! that instead comes back empty means the placed D server holds **no bytes at all**
//! for a fragment the chunk map places there. Both a checksum mismatch and a placed-
//! but-absent fragment are treated as **lost** — excluded (never fed to a decoder) —
//! and the chunk is **enqueued for reconstruction** on the one shared, durable repair
//! queue ([`wyrd_core::repair::enqueue_repair`]) that the read path also feeds
//! (`0005:174-176`). The load-bearing invariant, whose violation is **silent
//! corruption**: for every referenced fragment, corruption AND absence are **never
//! absorbed silently** — each always becomes a durable repair obligation
//! (`0005:262-267`; issue #330's invariant: a committed reference is either
//! present-and-intact or a durable repair obligation, with no third, silent outcome).
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

use std::collections::HashMap;

use wyrd_core::metadata::EcScheme;
use wyrd_core::repair;
use wyrd_traits::{ChunkId, ChunkStore, DServerId, FragmentId, MetadataStore, Result};

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
    // The reference set: every fragment a *committed* chunk map points at, keyed by
    // the D server its placement record names. This is the SAME set GC uses as its
    // safety gate (`crate::gc::referenced_fragments`) — an orphan / pending-garbage
    // fragment is never in it, so it can never be a scrub finding. Nor is a fragment
    // belonging to an in-flight (not-yet-committed) write: the four-phase write
    // protocol commits the chunk map only after *every* fragment has acked
    // (`crates/core/src/write.rs:220`), so a committed reference's bytes are always
    // supposed to already exist — a fragment in this set genuinely missing from its
    // placed D server is a loss, never a benign race.
    let referenced = referenced_fragments(ctx.meta).await?;

    // Malformed committed placement (ADR-0040 decision 4, "strict maintenance"): a
    // non-empty, wrong-length vector can only be truncation/corruption. Scrub FAILS SAFE
    // — it does NOT fabricate the missing identity tail and enqueue phantom repair
    // obligations for it (the valid reference set below excludes these chunks entirely) —
    // and surfaces each one as an operator signal on the durability seam instead.
    for (&chunk, m) in &referenced.malformed {
        emit_malformed(chunk, m.expected, m.actual);
    }

    // Group the reference set by placed D server so the pass is driven by WHAT IS
    // REFERENCED, not by what a store's own `list_fragments()` happens to enumerate
    // (issue #330). Walking `list_fragments()` alone can only ever find a
    // present-but-corrupt fragment — a fragment simply ABSENT from the store never
    // appears in its listing, so it was never visited at all and the missing-fragment
    // case silently fell through the `Ok(None)` "vanished between the walk and the
    // fetch" arm. Asking each placed D server directly for exactly the fragments the
    // chunk map says it holds closes that gap: a `get_fragment` that comes back
    // `Ok(None)` now means exactly what it says — no bytes for a fragment placed here.
    // Carry each referenced fragment's committed scheme alongside it, so the verify
    // below checks the header's FULL identity (index + EC tuple) against the chunk map,
    // not the `chunk_id` alone (`repair::fragment_intact`, `0005:262-267`).
    let mut by_dserver: HashMap<DServerId, Vec<(FragmentId, EcScheme)>> = HashMap::new();
    for &(dserver, frag) in &referenced.placed {
        if let Some(&scheme) = referenced.schemes.get(&frag.chunk) {
            by_dserver.entry(dserver).or_default().push((frag, scheme));
        }
    }

    let mut changed = false;
    for &(dserver, store) in ctx.fleet {
        let Some(frags) = by_dserver.get(&dserver) else {
            continue;
        };
        for &(frag, scheme) in frags {
            // Fetch the bytes named by the chunk map, then decide what the fetch told
            // us. A backend that does not verify on read (an in-memory fake) hands
            // back the raw bytes for scrub's own `fragment_intact` to check; a
            // verifying backend (the on-disk / networked D server) instead *rejects*
            // a corrupt fragment with an `IntegrityFault` rather than returning bytes
            // that already failed the very same check — so corruption must be handled
            // on BOTH arms, and a single rotten fragment must never abort the pass.
            match store.get_fragment(frag).await {
                Ok(Some(bytes)) => {
                    // COVERAGE: a referenced fragment scrub walked and verified.
                    emit_scrubbed(dserver, frag);

                    // VERIFY the self-describing checksum against the committed chunk
                    // map — the FULL identity `frag` (chunk id + index) and `scheme`
                    // (the EC tuple) name, not the `chunk_id` alone.
                    if !repair::fragment_intact(&bytes, frag, scheme) {
                        // CORRUPTION: exclude the failing fragment (never decode it)
                        // and enqueue its chunk on the shared repair queue.
                        emit_corruption(dserver, frag);
                        repair::enqueue_repair(ctx.meta, frag.chunk, "scrub").await?;
                        changed = true;
                    }
                }
                // MISSING (issue #330): the placed D server holds NO bytes for a
                // fragment the committed chunk map references there. This is not a
                // checksum finding (there is nothing to check) but it is the same
                // durable-loss category as corruption — the Invariant to restore is
                // that a referenced fragment is either present-and-intact or a durable
                // repair obligation, never silently absorbed either way. False
                // positives are guarded structurally: `referenced` only holds
                // COMMITTED chunk-map placements (an in-flight write's provisional map
                // is excluded), and GC's own safety gate never reclaims anything in
                // this same set — so an `Ok(None)` here can only mean genuine loss,
                // not a pending-GC or in-flight-write race.
                Ok(None) => {
                    emit_missing(dserver, frag);
                    repair::enqueue_repair(ctx.meta, frag.chunk, "scrub").await?;
                    changed = true;
                }
                // The store REJECTED the fetch. Distinguish a **corruption** fault
                // (the bytes failed their self-describing integrity check — a
                // verifying backend's way of reporting bit rot / a misplaced
                // fragment, locally or across the gRPC seam) from a **transient** one
                // (unreachable / timed out / busy). Corruption is the same durable
                // repair obligation as the mismatch above, and scrub must record it
                // and CONTINUE past it — never abort the whole pass over one rotten
                // fragment. A transient fault carries no such signal: propagate it so
                // the retry policy, not scrub, decides. (A wholly unreachable /
                // partitioned D server — every fragment on it faulting transiently —
                // is deliberately out of scope here: that needs desired-state /
                // topology awareness, a separate detector.)
                Err(e) if wyrd_traits::is_integrity_fault(e.as_ref()) => {
                    emit_scrubbed(dserver, frag);
                    emit_corruption(dserver, frag);
                    repair::enqueue_repair(ctx.meta, frag.chunk, "scrub").await?;
                    changed = true;
                }
                Err(e) => return Err(e),
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
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
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
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "scrub detected bit rot: fragment excluded, chunk enqueued for reconstruction",
    );
}

/// Emit a **malformed committed placement** signal on the durability-plane seam
/// (ADR-0011 / ADR-0012, ADR-0040 decision 4): a committed chunk whose `placement` vector
/// is non-empty but of the wrong length — truncation / corruption. Scrub fails safe (it
/// never fabricates the missing tail into phantom repair obligations); this is the
/// operator signal that a corrupt placement is no longer masked as a silent resolution.
fn emit_malformed(chunk: ChunkId, expected: u16, actual: usize) {
    tracing::warn!(monotonic_counter.scrub_malformed_placement = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.scrub.audit",
        action = "malformed-placement",
        chunk = %wyrd_traits::chunk_hex(chunk),
        expected,
        actual,
        "scrub found a committed placement of the wrong length (truncation/corruption); chunk treated as fully referenced, no phantom repair enqueued — operator signal",
    );
}

/// Emit **scrub-detected absence** (issue #330) on the same durability-plane seam: a
/// referenced fragment whose placed D server holds no bytes for it at all, now
/// enqueued for reconstruction — the same durable obligation corruption produces, for
/// the "placed but simply missing" loss category.
fn emit_missing(dserver: DServerId, frag: FragmentId) {
    tracing::info!(monotonic_counter.scrub_missing_detected = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.scrub.audit",
        action = "missing",
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "scrub detected a placed fragment absent from its D server: chunk enqueued for reconstruction",
    );
}
