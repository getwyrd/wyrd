//! The **backfill custodian pass** (issue #350; ADR-0040 decision 6, steps 1–2 of the
//! identity-placement-fallback removal path).
//!
//! The identity-placement fallback (the empty-`placement` branch of
//! [`ChunkRef::placed_dserver`](wyrd_core::metadata::ChunkRef::placed_dserver),
//! `core/src/metadata.rs:119-124`) exists solely for pre-M3 / mixed-era committed
//! records; M3+ writes always record a full-length vector (`core/src/write.rs:271`),
//! and reconstruction/rebalance already materialize full placement when they *touch*
//! a chunk (ADR-0040 decision 5). What never drains on its own is the long tail of
//! committed records no other loop ever touches — this pass closes it:
//!
//! ```text
//! scan:    every COMMITTED inode record's chunk map
//! classify: reuse the single-source classifier (#348 — `ChunkRef::checked_fragments`
//!           / `placement_is_valid`, `core/src/metadata.rs:159-185`, ADR-0040 decision 4)
//!   empty      -> materialize (0..fragment_count()).map(u64::from) as the explicit
//!                 identity vector
//!   full-length -> untouched (idempotent: already explicit)
//!   malformed  -> untouched, surfaced (#348's strict-maintenance posture: audit,
//!                 NEVER silently rewritten)
//! commit:  ONE version-conditional MetadataStore::commit per touched record — the
//!          same require(prior)/put(next) CAS the custodians already race through
//!          (`0005:200-203`, ADR-0015; `rebalance.rs:evacuate_chunk` :276-294 /
//!          `core/src/metadata.rs:commit_chunk_map` :299-317) — so a racing
//!          writer/custodian wins and this pass's fill is simply retried later.
//! observe: emit the empty-placement population REMAINING on the durability-plane
//!          seam every pass (ADR-0011/ADR-0012), so an operator can watch it drain
//!          to zero (ADR-0040 decision 6's first precondition).
//! ```
//!
//! No fragment moves — this rewrites metadata only, so the semantic resolution of
//! every fragment is unchanged (identity in, explicit identity out).
//!
//! **Scope note (issue #350):** step 3 of the removal path — converting the
//! empty-vector branch into a defensive error — is explicitly OUT of scope here
//! (tracked by follow-up #363); the read path (`placed_dserver`) is unchanged.
//! Rewriting a **malformed** vector is also out of scope (#348's strict-maintenance
//! concern: operator signal, never silent rewrite).
//!
//! **Hosting note:** the issue #350 design proposal marks wiring this pass into
//! [`crate::reconcile_step`] alongside GC/scrub/reconstruction/rebalance
//! ILLUSTRATIVE, not binding — [`reconcile`] is a public, directly-callable entry
//! (unlike its siblings' `pub(crate) reconcile`, reachable only through
//! `reconcile_step`) until a later slice threads it through the fenced control point.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): this pass stays over the `traits` /
//! `core` seams plus `tracing` — no D-server fleet, no failure-domain topology, no
//! concrete backend.

use std::collections::BTreeSet;

use wyrd_core::metadata::{self, InodeId, InodeRecord, InodeState};
use wyrd_traits::{ChunkId, CommitOutcome, MetadataStore, Result, WriteBatch};

use crate::reconciliation::Reconciled;

/// What the backfill reconciler reads and rewrites over: the authoritative metadata
/// store alone. Unlike GC/reconstruction/rebalance this pass touches no D-server
/// fleet and no failure-domain topology — it materializes an already-implied
/// placement into the record, it never moves a fragment byte.
pub struct BackfillContext<'a> {
    /// The authoritative metadata store.
    pub meta: &'a dyn MetadataStore,
}

fn parse_inode_key(key: &[u8]) -> Option<InodeId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("inode:")?
        .parse()
        .ok()
}

/// One backfill reconciliation pass over `ctx`. Returns [`Reconciled::Changed`] if
/// any committed record's placement was backfilled, [`Reconciled::Satisfied`]
/// otherwise. Always emits the empty-placement-remaining gauge (issue #350 step 2),
/// even on a `Satisfied` pass, so the drain is observable at every cadence.
///
/// A committed **segmented** record carrying an empty placement fails the pass with
/// [`SegmentedPlacementUnfillable`] — see the skip below. The failure is raised at the
/// **end**, after every record this pass *can* drain has been drained and after the
/// gauge has been emitted: halting on the first one would suppress the very observability
/// an operator needs to see the population, and would let one undrainable record stop the
/// rest of the store from draining. A **store** error still pre-empts it — every `?` in
/// the loop propagates at once — so an indeterminate commit is never masked by this
/// diagnostic, which is the ordering the aggregate-error rule requires.
///
/// A record whose map cannot be **read at all** is contained on the same terms and with
/// the same ordering ([`UnresolvableChunkMaps`], the containment boundary
/// [`crate::resolve::contain`]): the damaged object is attributed and skipped, every other
/// record is classified and filled, the gauge is emitted, and the population is raised
/// afterwards. Propagating from inside the loop is what this replaces — one damaged object
/// then stopped the drain of every healthy record in the store *and* suppressed the gauge
/// an operator watches it by, which is a fleet-wide outage bought for a per-object fault.
pub async fn reconcile(ctx: &BackfillContext<'_>) -> Result<Reconciled> {
    let mut changed = false;
    let mut unfillable: Vec<(InodeId, usize)> = Vec::new();
    // The objects this pass could not read, in the store's own key spelling. Attributed as
    // they are met (on the audit seam) and raised once at the end, so the pass reports the
    // POPULATION rather than only the first member — an operator repairing one record at a
    // time would otherwise need a pass per damaged object to discover the next. Ordered and
    // deduplicated (the same `BTreeSet` shape [`crate::gc::ReferenceSet::unresolvable`]
    // uses): `MetadataStore::scan` leaves order unspecified, and a diagnostic that named a
    // different record each pass over an unchanged store is one an operator cannot act on.
    let mut unresolvable: BTreeSet<String> = BTreeSet::new();

    for (key, value) in ctx.meta.scan(b"inode:").await? {
        let record: InodeRecord = match crate::resolve::classify_root(&key, &value)? {
            crate::resolve::Root::Decoded(record) => record,
            // Attributed, and deliberately NOT a member of the population: this pass fills
            // committed records only (the skip below), so an uncommitted one it could not
            // read was never fillable — raising [`UnresolvableChunkMaps`] over it would
            // fail every pass in the store forever over a record that has nothing to fill.
            // The ordering itself lives in `classify_root`, once, for all six walks.
            crate::resolve::Root::UncommittedUnreadable(fault) => {
                fault.attribute_uncommitted(crate::resolve::BACKFILL);
                continue;
            }
            crate::resolve::Root::Unresolvable(fault) => {
                unresolvable.insert(fault.attribute(crate::resolve::BACKFILL));
                continue;
            }
        };
        if record.state != InodeState::Committed {
            continue;
        }
        let Some(inode_id) = parse_inode_key(&key) else {
            continue;
        };

        // Resolve the map through the shared maintenance resolver (proposal 0016 decision
        // 7(e)) — a segmented map included, so this pass sees the same chunks every other
        // consumer sees.
        //
        // **Every decision below is taken on `live.record`, never on the snapshot this
        // scan decoded.** A generation retired mid-resolve is re-resolved against the root
        // that replaced it, and that root can have a different SHAPE: a stale segmented
        // snapshot whose live root is flat would otherwise take the segmented skip below
        // and be reported unfillable, while the fill it declined was available on the
        // generation whose chunks it is holding. The record that carries the chunks is the
        // record that gets classified, skipped, and compare-and-swapped.
        //
        // A map this pass cannot read is contained here rather than propagated: it is one
        // object's fault (`crate::resolve::contain`), and the records after it in the scan
        // are none of its business.
        let live = match crate::resolve::contain(
            &key,
            crate::resolve::chunks_of(ctx.meta, &key, &record).await,
        )? {
            crate::resolve::Contained::Resolved(live) => live,
            crate::resolve::Contained::Unresolvable(fault) => {
                unresolvable.insert(fault.attribute(crate::resolve::BACKFILL));
                continue;
            }
        };
        let Some(live) = live else {
            continue;
        };
        let record = live.record;
        let chunks = live.chunks;

        // Classify BEFORE acting (ADR-0040 decision 4, reusing #348's single-source
        // classifier): collect the indices of chunks whose committed placement is
        // EMPTY, surfacing any MALFORMED one as an operator signal along the way.
        // Neither classification mutates `record.chunk_map` — a read-only pass.
        //
        // This runs for a segmented map TOO, and it runs BEFORE the skip below: the
        // strict maintenance-time placement validation is the operator's only signal that
        // a placement is corrupt, and a shape this pass declines to rewrite must not
        // silently downgrade that signal to nothing.
        let mut to_fill = Vec::new();
        for (index, chunk) in chunks.iter().enumerate() {
            match chunk.checked_fragments() {
                Ok(_) if chunk.placement.is_empty() => to_fill.push(index),
                // Ok(_) and non-empty: already an explicit full-length vector —
                // idempotent, left untouched.
                Ok(_) => {}
                // Err: malformed (non-empty, wrong length) — NEVER rewritten (#348).
                Err(m) => emit_malformed(chunk.id, m.expected, m.actual),
            }
        }

        // A **segmented** map (proposal 0016 decision 7) is resolved and classified like
        // every other consumer's, then deliberately NOT rewritten — the stated decision
        // this pass takes, not an oversight (the unlisted-consumer hazard the re-plan
        // flags). Two reasons, both structural:
        //
        //   * the fill this pass performs is an inode CAS, and a segmented chunk's
        //     `ChunkRef` does not live in the inode — it lives in a `seg:` record, whose
        //     rewrite is the segment repoint of decision 7(f)
        //     (`metadata::repoint_chunk`), not this pass's shape; and
        //   * nothing it would fill can exist there: an empty `placement` is a pre-M3 /
        //     mixed-era artefact, and the only writer of a segmented chunk **refuses** one
        //     — the publication's write boundary rejects an empty placement before any
        //     record is durable (`ChunkMapError::WritePlacementEmpty`,
        //     `crates/core/src/metadata.rs:3348-3365`), and a repoint's replacement vector
        //     must be exactly `fragment_count()` long (`metadata.rs:1499-1513`). So the
        //     premise below is true by construction, not by a claim about a producer.
        //
        // So the record is left BYTE-IDENTICAL and the skip is surfaced **with the count
        // of chunks it declined to fill**, rather than the map being mangled into an
        // inode-shaped rewrite.
        //
        // And when there IS something it declined to fill, the pass **fails closed**. A
        // segmented map with an empty placement is a record no pass in the system drains:
        // this one cannot (the `ChunkRef` is not in the inode) and no other one is looking.
        // Telemetry alone would leave the shape as a silent skip that a `Satisfied` pass
        // then certifies as "nothing left to do" — precisely the *Absent or unsupported
        // entries* rule's forbidden move (`AGENTS.md:175-177`), and precisely the
        // count-based reassurance that lets an undrainable population sit unnoticed. It is
        // also structurally impossible (see the second bullet — no writer in the workspace
        // can produce it), so an explicit error is the maintenance-path strictness ADR-0045
        // asks for over corruption, not a new failure mode a healthy store can meet.
        if record.chunk_map.is_segmented() {
            emit_segmented_skip(inode_id, chunks.len(), to_fill.len());
            if !to_fill.is_empty() {
                unfillable.push((inode_id, to_fill.len()));
            }
            continue;
        }

        if to_fill.is_empty() {
            continue;
        }

        // Materialize the explicit full-length identity vector for each empty chunk
        // — the same resolution `placed_dserver` already applies implicitly, now
        // made durable (`core/src/metadata.rs:119-124`).
        let mut next_chunk_map = chunks.into_owned();
        for &index in &to_fill {
            let n = next_chunk_map[index].fragment_count();
            next_chunk_map[index].placement = (0..n).map(u64::from).collect();
        }

        // THE binding commit: version-conditional on the prior record, exactly the
        // second fence writers and custodians already race through (`0005:200-203`,
        // ADR-0015; the same require(prior)/put(next) shape as
        // `rebalance.rs:evacuate_chunk` :276-294). A racing writer/custodian wins the
        // CAS; this record is simply re-examined on a later pass, never clobbered.
        let next = InodeRecord {
            chunk_map: next_chunk_map.into(),
            version: record.version + 1,
            // Everything else is carried over UNCHANGED, and only the map and the version
            // are named: backfill re-commits the SAME content (a repair), so it preserves
            // the object metadata (ADR-0047) — a placement-maintenance commit must not
            // move `Last-Modified` or drop the content type — and equally the `size` and
            // the `Committed` state the scan already filtered on. Restating a field the
            // update syntax supplies identically would be dead code dressed as intent.
            ..record.as_ref().clone()
        };
        let inode_key = metadata::inode_key(inode_id);
        let batch = WriteBatch::new()
            .require(inode_key.clone(), metadata::encode(record.as_ref()))
            .put(inode_key, metadata::encode(&next));

        match ctx.meta.commit(batch).await? {
            CommitOutcome::Committed => {
                emit_backfilled(inode_id, to_fill.len());
                changed = true;
            }
            CommitOutcome::Conflict => emit_conflict(inode_id),
        }
    }

    emit_remaining(ctx.meta).await?;

    // The unreadable population outranks the unfillable one: an object whose map this pass
    // could not read is a record whose empty-placement count is UNKNOWN, so it is also a
    // potential member of the diagnostic below. Reporting the fillable diagnostic first
    // would answer a question the pass could not fully ask.
    if let Some(first) = unresolvable.first() {
        return Err(UnresolvableChunkMaps {
            first: first.clone(),
            records: unresolvable.len(),
        }
        .into());
    }

    if let Some(&(inode, unfilled)) = unfillable.first() {
        return Err(SegmentedPlacementUnfillable {
            inode,
            unfilled,
            records: unfillable.len(),
        }
        .into());
    }

    Ok(if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
}

/// Emit the **empty-placement population remaining** on the durability-plane seam
/// (ADR-0011/ADR-0012, issue #350 step 2): a gauge sample of how many committed
/// chunk records still carry an empty `placement` after this pass, so an operator
/// can watch the pre-M3 / mixed-era population drain to zero (ADR-0040 decision 6's
/// first precondition — the removal gate itself, step 3, stays out of scope, tracked
/// by #363).
/// A record this walk cannot read is **counted, not propagated** — and counted on its own
/// gauge, beside the population. A damaged object's empty-placement count is unknown, so
/// the remaining-population gauge alone would be a number an operator reads as complete
/// while it silently excludes a whole record; the two levels together say "this much
/// remains, over this store minus these unreadable records". Emitting one without the
/// other is the count-based reassurance the rubric's *Absent or unsupported entries* rule
/// forbids (`AGENTS.md:175-177`).
async fn emit_remaining(meta: &dyn MetadataStore) -> Result<()> {
    let mut remaining: u64 = 0;
    let mut unreadable: u64 = 0;
    for (key, value) in meta.scan(b"inode:").await? {
        let record: InodeRecord = match crate::resolve::classify_root(&key, &value)? {
            crate::resolve::Root::Decoded(record) => record,
            // NOT counted: this gauge is the qualifier on the *remaining* population, and
            // that population is committed records only. An unreadable UNCOMMITTED record
            // excludes nothing from it, so counting one would leave a permanently non-zero
            // level saying the store's drain figure is incomplete when it is exact.
            // Dropped rather than attributed for the same reason the arm below is: the
            // pass loop over this very scan already named it on the audit seam.
            crate::resolve::Root::UncommittedUnreadable(_) => continue,
            // Counted, and deliberately not attributed a second time: this walk runs
            // immediately after the pass loop over the same scan, and that loop already
            // named every object it could not read on the audit seam and will raise the
            // population as [`UnresolvableChunkMaps`]. What this walk owes is the
            // qualifier on its own gauge, not a duplicate operator signal per pass.
            crate::resolve::Root::Unresolvable(_) => {
                unreadable += 1;
                continue;
            }
        };
        if record.state != InodeState::Committed {
            continue;
        }
        // Resolved, not walked: a segmented map's chunks are counted honestly here even
        // though the pass does not rewrite them, so the drain gauge cannot silently
        // exclude a whole class of committed record.
        let live = match crate::resolve::contain(
            &key,
            crate::resolve::chunks_of(meta, &key, &record).await,
        )? {
            crate::resolve::Contained::Resolved(live) => live,
            // Same rule as the decode arm above: counted here, attributed there.
            crate::resolve::Contained::Unresolvable(_) => {
                unreadable += 1;
                continue;
            }
        };
        let Some(live) = live else {
            continue;
        };
        remaining += live
            .chunks
            .iter()
            .filter(|chunk| chunk.placement.is_empty())
            .count() as u64;
    }
    tracing::info!(gauge.backfill_placement_remaining = remaining);
    // A LEVEL, emitted every pass (0 included) so it rises while records are damaged and
    // returns to zero when they are repaired — the same gauge discipline the population
    // above uses, and the qualifier that keeps that population honest.
    tracing::info!(gauge.backfill_unreadable_records = unreadable);
    Ok(())
}

/// Emit a backfilled record on the durability-plane seam: the metric the
/// `tracing`→OTel bridge counts plus an append-only audit event
/// (`0005:336-340`-style).
fn emit_backfilled(inode_id: InodeId, filled: usize) {
    tracing::info!(monotonic_counter.backfill_chunks_filled = filled as u64);
    tracing::info!(
        target: "wyrd.custodian.backfill.audit",
        action = "backfill",
        inode = inode_id,
        filled,
        "backfill materialized the full-length identity placement for an empty-placement committed chunk",
    );
}

/// Emit the deliberate **skip of a segmented map** on the durability-plane seam: this
/// pass resolved the record and classified its placements (so a malformed one still
/// raises the operator signal above), then left it byte-identical, because materializing
/// a placement into a `seg:` record is the segment repoint of proposal 0016 decision
/// 7(f), not an inode CAS.
///
/// `unfilled` is the count of chunks whose placement is empty and which this pass
/// therefore did **not** fill. It is expected to be 0 — a segmented map is produced only
/// by a multipart Complete (`0016:2287-2312`), which always records a full-length
/// placement — so a non-zero value is a population no pass is currently draining. It is
/// emitted here for the audit trail and then **raised** as
/// [`SegmentedPlacementUnfillable`]: the emission is the operator's record of what was
/// seen, the error is what stops the pass from reporting a clean sweep over it.
fn emit_segmented_skip(inode_id: InodeId, chunks: usize, unfilled: usize) {
    tracing::info!(monotonic_counter.backfill_segmented_skipped = 1_u64);
    tracing::info!(
        gauge.backfill_segmented_empty_placement_remaining = unfilled as u64,
        inode = inode_id,
    );
    tracing::info!(
        target: "wyrd.custodian.backfill.audit",
        action = "skip-segmented",
        inode = inode_id,
        chunks,
        unfilled,
        "backfill resolved a segmented chunk map and left it untouched: its chunks live in `seg:` records (proposal 0016 decision 7), which no inode CAS may rewrite; any empty placement it carries is reported, never filled here",
    );
}

/// A committed **segmented** record carrying a chunk whose `placement` is empty.
///
/// This pass cannot fill it — the `ChunkRef` lives in a `seg:` record, whose rewrite is
/// the segment repoint of proposal 0016 decision 7(f), not an inode compare-and-swap —
/// and no other pass is draining that population. So it is raised rather than skipped:
/// an entry a maintenance pass cannot act on is an explicit error or a queued repair
/// obligation, never a silent skip under a `Satisfied` result (`AGENTS.md:175-177`,
/// ADR-0045's strict-in-maintenance boundary).
///
/// It is also structurally impossible for any writer in this workspace to produce: the
/// staged publication refuses a chunk with an empty placement before it makes anything
/// durable (`ChunkMapError::WritePlacementEmpty`, `crates/core/src/metadata.rs:3348-3365`)
/// and a segment repoint's replacement vector must be exactly `fragment_count()` long
/// (`crates/core/src/metadata.rs:1499-1513`). So this fires on corruption alone — the case
/// where halting the pass is the safe answer, and the case ADR-0045 asks a maintenance path
/// to be strict about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentedPlacementUnfillable {
    /// The first such inode the pass met.
    pub inode: InodeId,
    /// How many of *that* inode's chunks have an empty placement.
    pub unfilled: usize,
    /// How many committed segmented records the pass met carrying one, so the operator
    /// sees the size of the population and not just its first member.
    pub records: usize,
}

impl std::fmt::Display for SegmentedPlacementUnfillable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "inode {} has a segmented chunk map with {} empty-placement chunk(s), one of {} such record(s) this pass: backfill cannot fill a `seg:` record's chunk, and no other pass drains them",
            self.inode, self.unfilled, self.records,
        )
    }
}

impl std::error::Error for SegmentedPlacementUnfillable {}

/// Committed objects whose chunk map this pass could not **read** — a segmented root whose
/// generation is incomplete, or a record whose structure is corrupt.
///
/// Raised **after** the pass has drained every record it could and emitted its gauges, and
/// never from inside the scan: the fault belongs to the objects it names, and the rest of
/// the store has a drain to make progress on ([`crate::resolve`]'s containment boundary).
/// It is raised at all — rather than left to the audit seam — because backfill's only other
/// report is a *count*, and a population number that silently excludes an unreadable record
/// is the count-based reassurance the strictness rule forbids (`AGENTS.md:175-177`,
/// ADR-0045's strict-in-maintenance boundary). Nothing of the named objects was rewritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvableChunkMaps {
    /// The lowest-keyed such object, as the store spells its key (`inode:<id>`). Keyed
    /// order, not scan order: `MetadataStore::scan` leaves order unspecified, and a
    /// diagnostic that named a different record on each pass over an unchanged store is
    /// one an operator cannot act on.
    pub first: String,
    /// How many committed objects the pass could not read, so the operator sees the size
    /// of the population and not just its first member.
    pub records: usize,
}

impl std::fmt::Display for UnresolvableChunkMaps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} committed object(s) have a chunk map backfill could not read, starting at {}: each was left untouched and every other record was drained normally",
            self.records, self.first,
        )
    }
}

impl std::error::Error for UnresolvableChunkMaps {}

/// Emit a **NEEDS-HUMAN** signal for a malformed committed placement encountered
/// during a backfill scan (ADR-0040 decisions 3–4, #348's posture): NEVER rewritten.
fn emit_malformed(chunk: ChunkId, expected: u16, actual: usize) {
    tracing::warn!(monotonic_counter.backfill_malformed_placement = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.backfill.audit",
        action = "needs-human",
        chunk = %wyrd_traits::chunk_hex(chunk),
        expected,
        actual,
        "backfill found a committed placement of the wrong length (truncation/corruption); left untouched, NEVER rewritten — operator signal",
    );
}

/// Emit a lost-CAS conflict on the same seam: a racing writer/custodian won the
/// version-conditional commit; this record's identity fill is retried on a later
/// pass rather than clobbering the winner.
fn emit_conflict(inode_id: InodeId) {
    tracing::info!(monotonic_counter.backfill_conflict = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.backfill.audit",
        action = "conflict",
        inode = inode_id,
        "backfill lost the version-conditional commit; retried on a later pass",
    );
}
