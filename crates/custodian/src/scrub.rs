//! The **scrub custodian loop** (proposal 0005 §"The four custodian loops" / Scrub,
//! `0005:262-267`; §6.3 step 1 read-vs-scrub mirror `0005:264-266`; the durability
//! metrics scrub coverage + scrub-detected corruption rate `0005:331-332`;
//! PR-sequence slice 5 `0005:528-530`).
//!
//! Scrub catches **bit rot before the data is needed** — the proactive mirror of the
//! read path's read-time checksum verification (`0005:262-266`, the read path in
//! `crates/core/src/read.rs`). One pass walks the reference set — every
//! `(dserver, fragment)` a **committed** chunk map's placement record names
//! (`referenced_fragments`), PLUS every one a multipart upload's own COMMITTED `part:`
//! record names (`crate::gc::staged_committed_parts`; proposal 0016 decision 2,
//! `0016:824-825`, split from #663) — and, for each one, fetches its bytes **directly
//! from its placed D server** ([`ChunkStore::get_fragment`]) rather than only whatever
//! that server's own listing happens to return (issue #330: a fragment that is simply
//! *absent* from the store is otherwise never observed, because nothing ever asks the
//! store for exactly that id). A fetched fragment's self-describing checksum is
//! verified against whichever record placed it ([`wyrd_core::repair::fragment_intact`]),
//! with the EC scheme THAT record carries — a part record's own [`EcScheme`], never a
//! committed chunk map's, for a chunk that is only ever staged; a fetch that instead
//! comes back empty means the placed D server holds **no bytes at all** for a fragment
//! that record places there. Both a checksum mismatch and a placed-but-absent fragment
//! are treated as **lost** — excluded (never fed to a decoder) — and the chunk is
//! **enqueued for reconstruction** on the one shared, durable repair queue
//! ([`wyrd_core::repair::enqueue_repair`]) that the read path also feeds
//! (`0005:174-176`). The load-bearing invariant, whose violation is **silent
//! corruption**: for every referenced fragment, corruption AND absence are **never
//! absorbed silently** — each always becomes a durable repair obligation
//! (`0005:262-267`; issue #330's invariant: a committed reference is either
//! present-and-intact or a durable repair obligation, with no third, silent outcome).
//!
//! A part record's placement is checked only while no committed chunk map names its chunk.
//! Once a publication has handed the chunk to a committed object, that object's placement is
//! the one reads use and reconstruction repairs, and the part record — left until its
//! retirement drain, never updated when reconstruction moves a fragment — may name a position
//! that no longer holds anything. Checking it there would have scrub re-enqueue a chunk that
//! reconstruction finds whole and drains, pass after pass.
//!
//! **One re-queue case is bounded, not closed.** If the published object is deleted or
//! overwritten before the upload's `part:` records are retired, and reconstruction had moved one
//! of the chunk's fragments, no committed map names the chunk any more and the leftover part
//! record — its only reference again — names the empty old position. Scrub then re-queues the
//! chunk every pass, and reconstruction keeps the obligation and answers `Blocked` every pass,
//! until the `retire:records:` drain deletes the part record: that drain is the bound. No
//! writer of `part:` records exists yet, so this is forward-looking.
//!
//! Scrub never reads a session's in-flight `sidx:` owned entries (leg B,
//! `crates/custodian/tests/staged_scrub.rs`): checking a fragment needs the COMMITTED
//! scheme a part record carries, and an owned entry's placement is only planned, not
//! yet committed (`0016:776-781`) — there is nothing yet for scrub to hold a corrupt or
//! missing fragment against.
//!
//! **The two classes are read source-first, `part:` before `inode:`** (normative,
//! `0016:782-800`; GC's own order over the same two, `crate::gc::reconcile`). A
//! publication moves a chunk's protection from its `part:` record to a committed inode
//! and deletes the part record only in a LATER batch, so a pass that read `inode:`
//! first can see a chunk in neither class and fetch none of its fragments — rot or loss
//! it then reports as `Satisfied`. Reading the source first sees the chunk in at least
//! one class whichever way a concurrent publication interleaves.
//!
//! Scope: scrub only **produces** repair obligations. It never dequeues, rebuilds, or
//! deletes — gathering any-`k`, recomputing, re-placing, and the version-conditional
//! commit are the reconstruction custodian (slice 6, `0005:531-536`), which is also
//! where a STAGED chunk's own re-place lands (#814, split from #663; out of scope
//! here). Reclaiming the displaced bytes is GC's (slice 4). So scrub does **not** call
//! `delete_fragment`.
//!
//! Both classes this pass walks are only as complete as the build that produced them
//! (proposal 0016 decision 7(e) for the committed reference set; ADR-0045 decision 3 for
//! the staged one): a committed object whose chunk map could not be read, or a committed
//! part record that could not be read, contributes no fragments to its class, so scrub
//! never fetched them and cannot say one word about their integrity. It therefore
//! answers [`Reconciled::Blocked`] rather than certify a store it could only partly
//! read — the identical condition GC answers the identical way over the committed set
//! (`crate::gc::reconcile`) and over its own OWNED-plus-committed staged reading, because
//! two passes disagreeing about one set is a state an operator cannot resolve from
//! outside. A part record whose placement is malformed leaves the same hole for a chunk no
//! committed map names: nothing is fetched at a placement that cannot be trusted, so that
//! chunk was not checked, and the pass answers `Blocked` too and names the record.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): the loop stays over the
//! `traits` / `core` seams plus `tracing` — the checksum verify is borrowed from
//! `core` (which owns the on-disk-format reader), so `custodian` gains no
//! chunk-format dependency and no new on-disk-format knowledge. The staged read
//! reuses `crate::gc`'s own multipart key/record parsing rather than duplicating it.

use std::collections::HashMap;

use wyrd_core::metadata::EcScheme;
use wyrd_core::repair;
use wyrd_traits::{ChunkId, ChunkStore, DServerId, FragmentId, MetadataStore, Result};

use crate::gc::{referenced_fragments, staged_committed_parts};
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
///
/// Returns [`Reconciled::Blocked`] if any committed object's chunk map, OR any committed
/// `part:` record, could not be read (either class is then **incomplete**: this pass verified
/// everything it could reach and certifies nothing over the store — see [`emit_unscrubbable`] /
/// [`emit_unscrubbable_staged`]) or a chunk no committed map names has a malformed part
/// placement (so it was never checked — see [`emit_malformed_staged`]), [`Reconciled::Changed`]
/// if any chunk was enqueued for reconstruction, and [`Reconciled::Satisfied`] otherwise. GC
/// answers an incomplete set the identical way ([`crate::gc::reconcile`]) — one incomplete set,
/// one rule, read twice.
pub(crate) async fn reconcile(ctx: &ScrubContext<'_>, _now_millis: u64) -> Result<Reconciled> {
    // The staged class scrub checks (proposal 0016 decision 2, `0016:824-825`, split from
    // #663): every fragment a session's COMMITTED `part:` record places, with the scheme that
    // record's own `ChunkRef` carries — never an in-flight `sidx:` entry (leg B,
    // `crates/custodian/tests/staged_scrub.rs`): checking a fragment needs the COMMITTED
    // scheme a part record carries, and an owned entry's is only planned, not yet committed
    // (`0016:776-781`).
    //
    // **Source before destination: this class FIRST, the committed one below second**
    // (normative, `0016:782-800`; the identical order GC reads the same two classes in,
    // `crate::gc::reconcile`). A publication hands one chunk's protection from a `part:`
    // record to a committed inode and then, in a LATER batch, deletes the part record it
    // replaced (`0016:793-800`) — so a pass that read `inode:` first can see the chunk in
    // NEITHER class: absent from a committed scan taken before the flip, absent from a
    // `part:` read taken after the drain. That pass fetches none of its fragments, and a
    // fragment that is never fetched is one whose rot or loss never becomes a repair
    // obligation while the pass answers `Satisfied` — the exact loss this loop exists to
    // prevent, and the reason the order is the protection here as much as it is in GC.
    // Reading the source first sees the chunk staged, or committed by the `inode:` scan
    // below, or both — never neither (the leg this guards,
    // `crates/custodian/tests/staged_scrub.rs`'s (C') publication race).
    let staged = staged_committed_parts(ctx.meta).await?;

    // Malformed committed PART placement (ADR-0040 decision 4, "strict maintenance", by way
    // of `crate::gc::staged_placement`'s exact-length rule): a placement that is not one D
    // server per fragment can only be truncation/corruption. Scrub FAILS SAFE — it does NOT
    // fabricate the missing tail and enqueue phantom repair obligations against servers no
    // record ever named — and surfaces each one as an operator signal on the durability seam
    // instead, naming the `part:` RECORD (there may be no committed object to look for).
    // Emitted the moment the staged reading returns, before the committed read's own `?` can
    // carry the names away with it.
    for (&chunk, records) in &staged.malformed {
        for (record, m) in records {
            emit_malformed_staged(&crate::gc::object_name(record), chunk, m.expected, m.actual);
        }
    }

    // **An INCOMPLETE staged reading is not a scrubbable store either** — `emit_unscrubbable`'s
    // rule, for a committed part record instead of a committed chunk map. Emitted here, beside
    // the malformed names above and ahead of the committed read, for the same reason.
    for (record, fault) in &staged.unresolvable {
        emit_unscrubbable_staged(&crate::gc::object_name(record), fault);
    }

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

    // **An INCOMPLETE reference set is not a scrubbable store.** An object whose chunk map
    // could not be read contributes no fragments to the set
    // (`crate::gc::ReferenceSet::unresolvable`), so this pass cannot ask any D server for
    // them and cannot say one word about their integrity — and a fragment that is never
    // fetched is one whose corruption or absence never becomes a repair obligation, the
    // exact loss this loop exists to prevent. Said out loud here, per object, and it is what
    // stops the pass from answering `Satisfied` below: "every referenced fragment was
    // verified" over a set missing an object's references is a clean bill for part of the
    // store wearing the name of one for all of it (the rubric's *Absent or unsupported
    // entries*: never silent success, never a silent skip).
    //
    // Emitted BEFORE the fleet walk, so a transient store fault later in the pass cannot
    // cost the operator the attribution — and the walk still runs, over every fragment that
    // IS in the set, because containment scopes the fault to the object that has it and to
    // nothing else.
    for (object, fault) in &referenced.unresolvable {
        emit_unscrubbable(&crate::gc::object_name(object), fault);
    }

    // Group BOTH classes by placed D server so the pass is driven by WHAT IS REFERENCED, not
    // by what a store's own `list_fragments()` happens to enumerate (issue #330). Walking
    // `list_fragments()` alone can only ever find a present-but-corrupt fragment — a fragment
    // simply ABSENT from the store never appears in its listing, so it was never visited at
    // all and the missing-fragment case silently fell through the `Ok(None)` "vanished
    // between the walk and the fetch" arm. Asking each placed D server directly for exactly
    // the fragments a record says it holds closes that gap: a `get_fragment` that comes back
    // `Ok(None)` now means exactly what it says — no bytes for a fragment placed here. Carry
    // each fragment's committed scheme alongside it (from whichever record placed it), so the
    // verify below checks the header's FULL identity (index + EC tuple), not the `chunk_id`
    // alone (`repair::fragment_intact`, `0005:262-267`).
    let mut by_dserver: HashMap<DServerId, Vec<(FragmentId, EcScheme)>> = HashMap::new();
    for &(dserver, frag) in &referenced.placed {
        if let Some(&scheme) = referenced.schemes.get(&frag.chunk) {
            by_dserver.entry(dserver).or_default().push((frag, scheme));
        }
    }
    // **A committed chunk map supersedes a part record for the chunk it names.** Once a
    // publication has handed a chunk to a committed object, reads use that object's placement
    // and reconstruction repairs against it (`crate::reconstruction`'s `assess` consults the
    // staged reading only for a chunk no committed map names). The part record is a leftover
    // until its retirement drain deletes it, and nothing updates it when reconstruction moves a
    // lost fragment of the published chunk — so it goes on naming the old, now empty, position.
    // Checking it there would enqueue the chunk every pass while reconstruction found the
    // committed chunk whole and drained it every pass: the two loops undoing each other for as
    // long as the part record lives. So a part record's placement is checked only for a chunk
    // no committed map names (validly placed or malformed), where it is the only record of
    // where the bytes are. The source-first order above still holds: a chunk a publication
    // moves mid-pass is named by at least one of the two reads, and is checked at the
    // placement the committed one gives if it gives one, at the part record's otherwise. The
    // skip also means no `(dserver, fragment)` is pushed twice: a chunk is walked from one
    // class or the other, never both.
    let committed = |chunk: &ChunkId| {
        referenced.schemes.contains_key(chunk) || referenced.malformed.contains_key(chunk)
    };
    for (&(dserver, frag), &scheme) in &staged.placed {
        if committed(&frag.chunk) {
            continue;
        }
        by_dserver.entry(dserver).or_default().push((frag, scheme));
    }
    // **A staged chunk whose part placement could not be used was not checked.** No committed
    // map names it, so its part records are its only reference, and at least one of them places
    // it malformed: none of its fragments was fetched at that placement (the emit above names
    // the record). `Satisfied` claims every referenced fragment was checked
    // (`crate::reconciliation`), so this pass refuses to certify. A chunk a committed map DOES
    // name is walked or reported by the committed map's own rule above, whatever a leftover
    // part record holds, and its damaged part record does not block the pass.
    let staged_unchecked = staged.malformed.keys().any(|chunk| !committed(chunk));

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
                // positives are guarded structurally: `by_dserver` only holds COMMITTED
                // placements — a committed chunk map's, or a committed part record's for a
                // chunk no committed map names (an in-flight write's provisional map, an
                // owned `sidx:` entry's planned one, and a part placement a committed map
                // has superseded are all excluded) — and GC's own safety gate never
                // reclaims anything either class names, so an `Ok(None)` here can only mean
                // genuine loss, not a pending-GC or in-flight-write race.
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

    Ok(
        if !referenced.unresolvable.is_empty()
            || !staged.unresolvable.is_empty()
            || staged_unchecked
        {
            // Refuse to certify — whatever else this pass did. The enqueues above are durable in
            // the shared repair queue either way, so nothing is lost by reporting the blocker
            // instead of the convergence; what reporting `Satisfied` / `Changed` would destroy
            // is the only signal that this pass verified only PART of the store.
            Reconciled::Blocked
        } else if changed {
            Reconciled::Changed
        } else {
            Reconciled::Satisfied
        },
    )
}

/// Emit a committed object scrub could **not** verify, on the durability-plane seam
/// (ADR-0011 / ADR-0012): its chunk map could not be read, so none of its fragments is in
/// the reference set this pass walks, and none of them was fetched, checksummed or enqueued.
///
/// Emitted from the scrub loop, never from the shared reference build: that build also backs
/// GC, restore and the drain-status surface, and a `scrub_` counter ticked inside it would
/// report a blocked scrub for a pass scrub never ran.
///
/// The counterpart of `crate::gc::ReferenceSet::protects` on the read side of the same
/// incomplete set: GC's answer is to reclaim nothing, scrub's is to certify nothing — and
/// both NAME the object, so the gap is repairable rather than merely known to be somewhere.
fn emit_unscrubbable(object: &str, fault: &str) {
    tracing::warn!(monotonic_counter.scrub_unresolvable_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.scrub.audit",
        action = "unresolvable-chunk-map",
        inode = %object,
        fault = %fault,
        "a committed object's chunk map could not be read, so none of its fragments was verified this pass; scrub verifies every other object as usual and refuses to certify the store — operator signal",
    );
}

/// Emit a committed **part** record scrub could **not** verify, on the same seam:
/// [`emit_unscrubbable`]'s twin for the staged half of what scrub checks. Its scheme and
/// placement are unknown, so none of its chunks' fragments was fetched, checksummed or
/// enqueued this pass. Named as a RECORD (a `part:` key), never as an `inode:` key — an
/// unreadable part record names no committed object at all — the same distinction GC's
/// `emit_unresolvable_staged` draws for its own staged reading.
fn emit_unscrubbable_staged(record: &str, fault: &str) {
    tracing::warn!(monotonic_counter.scrub_unresolvable_staged_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.scrub.audit",
        action = "unresolvable-staged-record",
        record = %record,
        fault = %fault,
        "a committed part record could not be read, so none of its chunks' fragments was verified this pass; scrub verifies every other record as usual and refuses to certify the store — operator signal",
    );
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

/// Emit a **malformed staged placement** on the same seam: [`emit_malformed`]'s twin for a
/// committed `part:` record, named as that RECORD (its `part:` key) — as
/// [`emit_unscrubbable_staged`] names one — because while no publication has handed the chunk
/// to a committed object there is no `inode:` key to look for. A part record's placement must
/// be exact: every staged record is born with a full one (`0016:828`), so an empty one is
/// damage, never the pre-M3 identity fallback a committed chunk map's empty vector is
/// ([`crate::gc::staged_placement`]). Nothing is fabricated or enqueued; the chunk was not
/// checked at this placement, and unless a committed map names it the pass does not certify.
fn emit_malformed_staged(record: &str, chunk: ChunkId, expected: u16, actual: usize) {
    tracing::warn!(monotonic_counter.scrub_malformed_staged_placement = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.scrub.audit",
        action = "malformed-staged-placement",
        record = %record,
        chunk = %wyrd_traits::chunk_hex(chunk),
        expected,
        actual,
        "scrub found a staged part record whose placement is the wrong length (truncation/corruption); the chunk was not checked at it and no phantom repair was enqueued; unless a committed map names the chunk, scrub refuses to certify the store — operator signal",
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
