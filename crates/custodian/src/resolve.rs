//! The maintenance plane's **one** way to read a committed object's chunk list
//! (proposal 0016 decision 7(e), `0016:2393-2399`).
//!
//! Every custodian loop that used to iterate `record.chunk_map` directly goes through
//! here, so a **segmented** map (decision 7(a)) is resolved by *all* of them or by none:
//! the recorded #508-attempt-4 failure was a resolver wired into the read path alone
//! while GC and restore still walked the field, after which a restore pass stranded a
//! live segmented object and a later GC pass deleted its fragments.
//!
//! The other half of the rule is what to do when the generation a pass is holding was
//! **retired under it** — the root now names a different group, or is gone (decision
//! 7(h)). "Drop the stale resolution" (`0016:2463-2469`) means drop *that resolution*,
//! **not the object**: a pass that answered "this object owns no fragments" would take
//! a live generation out of GC's reference set, restore would then manufacture `orphan:`
//! evidence for its fragments (`restore.rs:239`,`:282`) and desired-state would certify a
//! drain over bytes the object still owns (`desired_state.rs:174-183`). So the shared
//! entry re-reads the root and resolves the generation that **replaced** it
//! ([`metadata::resolve_live_chunk_map`]), and fails closed if that keeps happening.
//!
//! `None` therefore means one thing only: there is **no live committed generation** —
//! the root is absent (the object was deleted) or is not `Committed`. That is the
//! condition every one of these loops already skipped on before segmentation existed,
//! and a deleted object's bytes are reclaimed through the deletion's own evidence, never
//! through a consumer's silence. The restart is never silent either: it is emitted on the
//! durability seam, so an operator sees a pass that re-derived rather than inferring it.
//!
//! ## Why these calls read the root, when the caller has already decoded one
//!
//! Every pass here starts from a `scan("inode:")`, and a scan is a **snapshot**: over a
//! large store the pass reaches its last record minutes after it read its first. Resolving
//! from that snapshot answers about the generation the object *had*. For a segmented map
//! the resolver notices (the root re-read that settles decision 7(h) restarts onto the
//! live generation); for a **flat** map it cannot — the snapshot record *is* the map, so a
//! superseded object's old chunk list comes back looking perfectly current, and the pass
//! enters it in GC's reference set while the live generation's fragments are in nobody's.
//! That is the same shape as the recorded #508-attempt-4 failure, one layer up: protection
//! computed over a map that is not the object's.
//!
//! So the maintenance plane resolves through [`metadata::resolve_current_chunk_map`] /
//! [`metadata::resolve_current_chunk_homes`], which read the root themselves — one `get`
//! per committed object per pass, in a pass that already lists every fragment on every D
//! server — and the guarantee is then uniform across both shapes. The read paths keep the
//! snapshot entry ([`metadata::resolve_live_chunk_map`]): they read the root and resolve it
//! in the same breath, so a re-read there would cost a round trip per GET and buy nothing.

//! ## Containment: one object's fault is one object's fault
//!
//! Every pass below reaches this module from a `scan("inode:")` loop, so the *other*
//! half of decision 7(e) is what a pass does when **one** object's map cannot be read at
//! all. Propagating is what the pre-segmentation code did (`metadata::decode(&value)?`),
//! and over a fleet-wide loop it converts a per-object fault into a fleet-wide outage:
//! the reference build stops, the drain surface blanks, evacuation planning halts for
//! every draining server, and a queued repair for a healthy chunk that happens to sort
//! after the damaged one is never assessed.
//!
//! So the classification lives HERE, once ([`contain`]), and every pass shares it:
//! a [`ChunkMapError`] is **the object's own** fault — contained, attributed on the audit
//! seam, and the walk continues — while anything else (a store error, an undecodable
//! record that is not a chunk-map fault) still propagates, because a pass that cannot read
//! the metadata store has no view of the store at all. What each pass then does with the
//! blocker is its own containment obligation, and every one of them refuses to certify:
//! GC's reference set becomes incomplete and reclaims nothing
//! (`gc.rs:253-273`), the drain surface answers `PendingUnresolvable`
//! (`desired_state.rs:184-195`), reconstruction leaves the repair obligation **queued**
//! (`reconstruction.rs:325-333`), restore reports the object in
//! `RestoreReport::unresolvable`, and backfill raises the population after it has drained
//! everything it could. None of them deletes, moves, or rewrites a byte on the strength of
//! a map it could not read.
//!
//! The *scope* of that refusal is decided here too ([`classify_root`]): every one of these
//! passes covers **committed** records only, so an unreadable root whose still-readable
//! bytes say unambiguously that it is uncommitted was never in the population and blocks
//! nothing. Recording it anyway is a second way to turn one object's fault into a
//! fleet-wide one — this time over an object that was authorizing nothing at all.

use std::borrow::Cow;

use wyrd_core::metadata::{self, ChunkMapError, ChunkRef, HomedChunk, InodeRecord, InodeState};
use wyrd_traits::{BoxError, MetadataStore, Result};

/// One committed inode's chunk list and the generation it came from.
///
/// The **record travels with the chunks**, and it is the live one: when the caller's own
/// snapshot was superseded mid-resolve, `record` is the root that replaced it, not the
/// stale one. A pass that resolved through here and then re-read its own `record` for a
/// *decision* — what shape the map has, which bytes to compare-and-swap against — would be
/// judging the generation it just found was gone. That is not hypothetical: backfill
/// decides whether to rewrite a record from its shape, and a stale segmented snapshot
/// whose live root is flat would make it decline (and report unfillable) a map that is
/// perfectly fillable, while the CAS it skipped would have been against bytes no longer in
/// the store.
pub(crate) struct LiveMap<'a> {
    /// The generation the chunks were resolved from: the root as the store holds it,
    /// which is the caller's own record whenever its snapshot is still current.
    pub(crate) record: Cow<'a, InodeRecord>,
    /// That generation's ordered chunk list.
    pub(crate) chunks: Cow<'a, [ChunkRef]>,
}

/// Resolve one committed inode's chunk list **against the live root**, or `None` if the
/// object no longer has a live committed generation (see the module docs).
///
/// `record` is the pass's own snapshot: it is not what gets resolved (the root is re-read),
/// it is what the live root is *compared against*, so a pass that was working from a
/// superseded generation says so on the seam instead of swapping silently.
pub(crate) async fn chunks_of<'a>(
    meta: &dyn MetadataStore,
    inode_key: &[u8],
    record: &'a InodeRecord,
) -> Result<Option<LiveMap<'a>>> {
    let Some(current) = metadata::resolve_current_chunk_map(meta, inode_key).await? else {
        emit_no_live_generation(inode_key);
        return Ok(None);
    };
    note_currency(inode_key, record, &current.record);
    Ok(Some(LiveMap {
        record: Cow::Owned(current.record),
        chunks: Cow::Owned(current.chunks),
    }))
}

/// [`chunks_of`] for the repoint consumers: each chunk carries the record that holds it,
/// and the root a repoint must compare-and-swap against — the **live** one, read here
/// rather than taken from the pass's snapshot, so the plan is re-derived rather than
/// dropped and the CAS is against bytes the store actually holds.
pub(crate) async fn homes_of(
    meta: &dyn MetadataStore,
    inode_key: &[u8],
    record: &InodeRecord,
) -> Result<Option<(InodeRecord, Vec<HomedChunk>)>> {
    let Some(current) = metadata::resolve_current_chunk_homes(meta, inode_key).await? else {
        emit_no_live_generation(inode_key);
        return Ok(None);
    };
    note_currency(inode_key, record, &current.record);
    Ok(Some((current.record, current.chunks)))
}

/// The maintenance pass that met a fault, for the audit seam — so an operator reading
/// `unresolvable-chunk-map` sees *which* loop is degraded by it, not just that one is.
pub(crate) const GC: &str = "gc";
/// See [`GC`].
pub(crate) const BACKFILL: &str = "backfill";
/// See [`GC`].
pub(crate) const REBALANCE: &str = "rebalance";
/// See [`GC`].
pub(crate) const RECONSTRUCTION: &str = "reconstruction";
/// See [`GC`].
pub(crate) const RESTORE: &str = "restore";

/// One object's chunk map could not be read: the fault is **the object's**, and it is
/// contained here rather than propagated to the pass.
///
/// Carrying the error rather than a message keeps the operator signal specific — the
/// audit event renders the fault that actually happened, not a category — and carrying the
/// key means attribution cannot silently name a different object than the one that failed.
/// Whether the fault is a **blocker** is a separate question, settled by
/// [`classify_root`] and expressed in which of the two `attribute*` methods a pass may
/// call.
pub(crate) struct ChunkMapFault {
    /// The inode key as the store spells it (`inode:<id>`), rendered rather than parsed:
    /// this is attribution, and a key that did not parse would otherwise drop a blocker on
    /// the floor.
    object: String,
    err: BoxError,
}

impl ChunkMapFault {
    /// Attribute the fault on the durability-plane audit seam (ADR-0011 / ADR-0012) and
    /// return the blocker — the inode key — for the pass to record.
    ///
    /// Consuming `self` is the point: a fault is attributed **exactly once**, by the pass
    /// that contained it, so a blocker can neither be recorded without an operator signal
    /// (a silent skip, which is how damage stays invisible until the object is needed)
    /// nor emitted twice for one object.
    pub(crate) fn attribute(self, pass: &'static str) -> String {
        emit_unresolvable(pass, &self.object, &self.err);
        self.object
    }

    /// Attribute an **uncommitted** record whose bytes did not decode — and return
    /// nothing, because it is not a blocker.
    ///
    /// The two halves are the containment rule's precision, and both matter. Not a
    /// blocker: an uncommitted map is outside every one of these passes' populations by
    /// definition (each skips `state != Committed`), so a pass that recorded it would be
    /// degraded by an object that authorizes nothing — GC would stop reclaiming
    /// fleet-wide ([`crate::gc::ReferenceSet::protects`]), backfill would fail every pass,
    /// restore would never report clean, and a queued repair for an absent chunk would
    /// never drain. Still attributed: a record the store cannot read is an operator's
    /// business whatever its state, and a silent skip is how damage stays invisible until
    /// the object it belongs to is needed (the rubric's *Absent or unsupported entries*,
    /// `AGENTS.md:175-177`).
    ///
    /// The return type is the difference that cannot be misused: [`attribute`] hands back
    /// the blocker a pass must record, this one hands back nothing to record.
    ///
    /// [`attribute`]: ChunkMapFault::attribute
    pub(crate) fn attribute_uncommitted(self, pass: &'static str) {
        emit_uncommitted_unreadable(pass, &self.object, &self.err);
    }
}

/// One `inode:` value from a maintenance walk's `scan`, classified.
pub(crate) enum Root {
    /// The value decoded; the pass applies its own `state` filter to it.
    Decoded(InodeRecord),
    /// The value did not decode, and the bytes that are still readable say
    /// **unambiguously** that the record is not committed: attributed, never recorded as
    /// a blocker ([`ChunkMapFault::attribute_uncommitted`]).
    UncommittedUnreadable(ChunkMapFault),
    /// The value did not decode and may well be committed: the object's own fault,
    /// contained, and a blocker for whatever this pass certifies.
    Unresolvable(ChunkMapFault),
}

/// **The decode arm of every `scan("inode:")` loop**, state ordering included — the one
/// place that decides whether an unreadable root is a blocker at all.
///
/// Every pass here walks committed records and skips the rest, so the question an
/// undecodable root raises is not "can it be read" but *"would this record have been
/// skipped anyway?"*. A **pending** inode's map is outside the committed population by
/// definition, and recording it as a blocker is not merely imprecise: it is an object
/// that authorizes nothing stopping a pass that has nothing to do with it — reclamation
/// frozen fleet-wide, every backfill pass failing, a restore that can never report clean,
/// a repair obligation that never drains. So the state is taken from the bytes that are
/// still readable ([`metadata::inode_state_hint`]) **before** the fault is classified.
///
/// The direction of the doubt is deliberate: only an *unambiguous* non-committed state
/// (`Some(state)` that is not `Committed`) is treated as uncommitted. `None` — bytes that
/// are not JSON, a duplicate `state` field, an unknown state string — is treated as
/// possibly committed, because reading a committed record as pending would drop a live
/// object's chunks out of the population, which is the #508-attempt-4 data loss this
/// containment exists to prevent.
///
/// **Why this is a function and not a rule each pass follows.** The ordering was written
/// out once, in GC, and the next five walks (backfill's pass and its telemetry rescan,
/// rebalance's evacuation planning, reconstruction's lookup, restore's audit) each
/// re-spelled the arm without it — one defect, five sites, found a round later. The same
/// shape as [`contain`] being the crate's only `downcast_ref::<ChunkMapError>()`: a
/// decision that must be identical everywhere is taken in one place, so a seventh walk
/// inherits it and cannot spell it differently.
pub(crate) fn classify_root(key: &[u8], value: &[u8]) -> Result<Root> {
    match contain(key, metadata::decode::<InodeRecord>(value))? {
        Contained::Resolved(record) => Ok(Root::Decoded(record)),
        Contained::Unresolvable(fault) => Ok(
            if metadata::inode_state_hint(value).is_some_and(|state| state != InodeState::Committed)
            {
                Root::UncommittedUnreadable(fault)
            } else {
                Root::Unresolvable(fault)
            },
        ),
    }
}

/// One object-scoped resolve, classified.
pub(crate) enum Contained<T> {
    /// The object's map was read.
    Resolved(T),
    /// The object's map could not be read; the pass continues without it.
    Unresolvable(ChunkMapFault),
}

/// **The containment boundary.** Classify one object-scoped outcome: a [`ChunkMapError`]
/// — from the root's own decode or from resolving its `seg:` range — is that object's
/// fault and is contained; anything else propagates.
///
/// This is deliberately the crate's **only** `downcast_ref::<ChunkMapError>()`: the
/// recorded failure of the round that added the first containment was a second call site
/// spelling the same test slightly differently, so a malformed record was contained in one
/// pass and fleet-fatal in the next. One classifier, one meaning, every consumer.
pub(crate) fn contain<T>(key: &[u8], outcome: Result<T>) -> Result<Contained<T>> {
    match outcome {
        Ok(value) => Ok(Contained::Resolved(value)),
        Err(err) if err.downcast_ref::<ChunkMapError>().is_some() => {
            Ok(Contained::Unresolvable(ChunkMapFault {
                object: String::from_utf8_lossy(key).into_owned(),
                err,
            }))
        }
        Err(err) => Err(err),
    }
}

/// Emit a committed object whose chunk map could not be read, on the durability-plane
/// seam (ADR-0011 / ADR-0012) — the operator signal naming the object to repair and the
/// pass that is degraded until it is.
///
/// Reported, not raised: the pass keeps going and acts on nothing it could not read, so
/// the fault stays scoped to the object that has it while every other object keeps its
/// availability and its protection. A silent skip would be the opposite of both — the
/// damaged object's fragments would look unreferenced.
fn emit_unresolvable(pass: &'static str, object: &str, err: &BoxError) {
    tracing::warn!(
        monotonic_counter.custodian_unresolvable_chunk_map = 1_u64,
        pass
    );
    tracing::warn!(
        target: "wyrd.custodian.audit",
        action = "unresolvable-chunk-map",
        pass,
        inode = %object,
        error = %err,
        "a committed object's chunk map could not be read; this pass acts on nothing of that object and certifies nothing over it, and every other object is unaffected — operator signal",
    );
}

/// Emit an **uncommitted** record whose bytes did not decode, on the same seam and with
/// the same `pass` attribution as [`emit_unresolvable`] — the operator signal for damage
/// that is real but is blocking nothing.
///
/// A distinct action string, because the two say different things to whoever is on call:
/// `unresolvable-chunk-map` names a pass that is degraded until the record is repaired,
/// this one names a record to repair while every pass keeps its full reach.
fn emit_uncommitted_unreadable(pass: &'static str, object: &str, err: &BoxError) {
    tracing::warn!(
        monotonic_counter.custodian_unreadable_uncommitted_record = 1_u64,
        pass
    );
    tracing::warn!(
        target: "wyrd.custodian.audit",
        action = "unreadable-uncommitted-record",
        pass,
        inode = %object,
        error = %err,
        "an UNCOMMITTED inode record could not be read; it is outside this pass's committed population either way, so the pass keeps its full reach and certifies over every other object as usual — operator signal",
    );
}

/// Say so on the seam when the live root is **not** the generation this pass scanned.
///
/// One place, for both entries: the comparison and what it means are the same fact, and a
/// second copy of it is a second thing to get wrong. Silence means the snapshot was still
/// current — a pass that quietly swapped generations would leave an operator reading a
/// report about an object that no longer exists in that shape.
fn note_currency(inode_key: &[u8], snapshot: &InodeRecord, current: &InodeRecord) {
    if current != snapshot {
        emit_restarted(inode_key);
    }
}

/// Emit a resolve whose live root is **not** the generation the pass's own scan snapshot
/// carried, on the durability-plane seam (ADR-0011 / ADR-0012): the observable record that
/// the object was superseded (or its segmented generation retired) between the scan and
/// this resolve, and that the pass acted on the live one instead.
fn emit_restarted(inode_key: &[u8]) {
    tracing::info!(monotonic_counter.custodian_retired_generation_restarted = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.audit",
        action = "retired-generation",
        inode = %String::from_utf8_lossy(inode_key),
        "this inode's live root is not the generation this pass scanned; the pass resolved the generation that replaced it rather than the snapshot's (proposal 0016 decision 7(h))",
    );
}

/// Emit an object that has no live committed generation — deleted, or not committed —
/// so a pass that considered it and moved on is observable rather than inferred.
fn emit_no_live_generation(inode_key: &[u8]) {
    tracing::info!(monotonic_counter.custodian_absent_generation_skipped = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.audit",
        action = "absent-generation",
        inode = %String::from_utf8_lossy(inode_key),
        "no live committed generation for this inode while the pass resolved it; its bytes are reclaimed through the deletion's own evidence, never through this pass",
    );
}
