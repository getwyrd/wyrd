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
//! scan:    every COMMITTED inode record, read through the ONE resolver every consumer
//!          shares (`metadata::resolve_chunk_map`, proposal 0016 decision 7(e)) — so a
//!          segmented object is judged like any other, and one this pass cannot read is
//!          CONTAINED to itself: named on the audit seam, the walk going on
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
//!          to zero (ADR-0040 decision 6's first precondition) — counted in the SAME
//!          walk that fills, and published beside the number of committed objects this
//!          pass could not stand it behind.
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

use wyrd_core::metadata::{self, ChunkMapError, InodeId, InodeRecord, InodeState};
use wyrd_traits::{ChunkId, CommitOutcome, MetadataStore, Result, WriteBatch};

use crate::gc::object_name;
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

/// One backfill reconciliation pass over `ctx`. Returns [`Reconciled::Changed`] if any
/// committed record's placement was backfilled, [`Reconciled::Blocked`] if some committed
/// object could not be read or carries a fill this pass may not perform, and
/// [`Reconciled::Satisfied`] otherwise. Always emits the empty-placement-remaining gauge
/// (issue #350 step 2), even on a `Satisfied` pass, so the drain is observable at every
/// cadence.
///
/// Every committed record is read through the ONE resolver every consumer shares
/// ([`metadata::resolve_chunk_map`], proposal 0016 decision 7(e)), so a **segmented**
/// object's chunks are judged here like any other. An object this pass cannot read — its
/// own bytes will not decode, or the resolver refuses the generation the root names — is
/// CONTAINED: named on the audit seam and stepped over, the walk going on, by exactly the
/// rule [`crate::gc::referenced_fragments`] contains by (`gc.rs:360-416`) and no other. A
/// fault that is **not** one object's map still propagates: a walk that cannot reach the
/// metadata store has no answer for any object, not one unreadable object. Before this,
/// ONE segmented object ended the fill for every object in the store.
///
/// A committed object whose fill this pass may not perform is **declined**, not written:
/// its chunks live in `seg:` records and rewriting one is the segmented write path's, so
/// the record is left BYTE-IDENTICAL, its empty placements stay on the gauge, and the pass
/// refuses to certify a drain it did not complete. A segmented object that needs no fill
/// is ordinary and healthy — it declines nothing and blocks nothing.
///
/// **What this pass may write is decided from the generation the scan returned**, never
/// from what a resolve answered after restarting onto a newer root — and that holds here by
/// construction rather than by a comparison of its own. The fill below is built from
/// `record.chunk_map.as_flat()`, the scanned record's OWN chunk list, and conditioned on
/// the scanned record's own bytes; a flat map resolves to a borrow of that very list and
/// reads nothing (`core/src/metadata.rs:2585`), so it can never be superseded and never
/// restarts. Only a segmented snapshot can (`:2629`), and a segmented snapshot is one this
/// pass declines — so the restart path reaches no write at all. A flat scan that went stale
/// meanwhile is settled where it always was: by the version-conditional commit below, which
/// loses the CAS rather than clobbering the newer generation.
///
/// deferred: #682 — the segmented write path (`repoint_chunk`, the record ceilings) is that
/// slice's, and the decline here is what leaves room for it. deferred: #699 — telling a
/// restarted resolution apart from the scanned generation is that slice's; this pass needs
/// no such comparison, per the paragraph above.
pub async fn reconcile(ctx: &BackfillContext<'_>) -> Result<Reconciled> {
    let mut changed = false;
    // The empty-placement population still owed when this pass ends, counted in the SAME
    // walk that fills — never by a second reading of the namespace, which would re-read
    // (and, over segmented objects, re-resolve) every record for a number already in hand.
    let mut remaining: u64 = 0;
    // The hole in this pass's reading: committed objects it could not read at all, or read
    // and may not fill. Each such object adds exactly one; while the total is non-zero this
    // pass has answered over less than the committed store and certifies nothing.
    let mut incomplete: u64 = 0;

    for (key, value) in ctx.meta.scan(b"inode:").await? {
        // The record's own bytes are already in hand, so a decode failure is THIS object's
        // fault and no store's — contained exactly as `gc.rs:366-384` sets out, and
        // conservatively WITHOUT first asking whether it was committed: reading `state` out
        // of bytes that will not decode needs a lenient peek, and this pass holds the
        // ADR-0010 boundary of `traits` / `core` / `tracing` (module docs above), so it
        // owns no decoder to do it with. Blocking until the record is repaired is the
        // fail-closed direction.
        let record: InodeRecord = match metadata::decode(&value) {
            Ok(record) => record,
            Err(fault) => {
                emit_unresolvable(&object_name(&key), &fault.to_string());
                incomplete += 1;
                continue;
            }
        };
        if record.state != InodeState::Committed {
            continue;
        }
        let Some(inode_id) = parse_inode_key(&key) else {
            continue;
        };

        // Read the object's chunks through the shared resolver, so a segmented map is READ
        // rather than refused. The network bound on this await is the `MetadataStore`
        // IMPLEMENTATION's, not this caller's (#508/#636) — the same rule the
        // `meta.scan(b"inode:")` above has always followed, and the rule `gc.rs:394-401`
        // states in full for this same call. It is fail-closed either way: an error here
        // either propagates or contains the object, never "this object owns no chunks".
        let resolved = match metadata::resolve_chunk_map(ctx.meta, &key, &record).await {
            Ok(Some(resolved)) => resolved,
            // No live committed generation is left under this key (deleted or retired
            // since the scan read it): there is nothing left to fill, so it is skipped
            // exactly as an uncommitted record is above — and exactly as both merged peers
            // skip it (`gc.rs:404`, `restore.rs:646`).
            Ok(None) => continue,
            Err(err) => match err.downcast::<ChunkMapError>() {
                // The resolver's own typed verdict that THIS generation cannot be read,
                // recovered by downcast because the trait seam boxes every error.
                // Contained, by exactly `gc.rs:402-416`'s rule and no other.
                Ok(fault) => {
                    emit_unresolvable(&object_name(&key), &fault.to_string());
                    incomplete += 1;
                    continue;
                }
                // Not a chunk-map anomaly but a store fault under the read: not this
                // object's own, and folding it into "this object is unreadable" would be
                // the wrong answer for every object in the store.
                Err(err) => return Err(err),
            },
        };

        // Classify BEFORE acting (ADR-0040 decision 4, reusing #348's single-source
        // classifier): collect the indices of chunks whose committed placement is
        // EMPTY, surfacing any MALFORMED one as an operator signal along the way.
        // Neither classification mutates the record — a read-only pass.
        let mut to_fill = Vec::new();
        for (index, chunk) in resolved.chunks.iter().enumerate() {
            match chunk.checked_fragments() {
                Ok(_) if chunk.placement.is_empty() => to_fill.push(index),
                // Ok(_) and non-empty: already an explicit full-length vector —
                // idempotent, left untouched.
                Ok(_) => {}
                // Err: malformed (non-empty, wrong length) — NEVER rewritten (#348).
                Err(m) => emit_malformed(chunk.id, m.expected, m.actual),
            }
        }

        if to_fill.is_empty() {
            continue;
        }

        // Every empty placement this pass READ is owed from here on, added at ONE site: a
        // record left unfilled for any reason below stays on the number an operator watches
        // drain, and only a fill known to have COMMITTED takes it off again.
        remaining += to_fill.len() as u64;

        // What this pass may WRITE, taken from the generation the SCAN returned and from
        // nothing else: the scanned record's own inline chunk list, which for a flat map is
        // the very slice `resolved.chunks` borrows (`core/src/metadata.rs:2585`) — so the
        // indices classified above address it directly, and the CAS below conditions on the
        // bytes those chunks arrived in.
        //
        // A segmented generation has no such list: its chunks live in `seg:` records, and
        // rewriting one is the segmented write path's, so the object is left BYTE-IDENTICAL
        // — root and `seg:` records alike — the empty placements this pass read stay on the
        // gauge, and a declined fill is work this pass may not certify away. A segmented
        // object that needs NO fill never reaches here: it is ordinary and healthy, blocks
        // nothing, and the pass may still answer `Satisfied` over it.
        //
        // deferred: #682 — that write path, and the seeded Tier-0 DST case belonging to it,
        // land together. This branch performs no write at all.
        let Some(scanned_chunks) = record.chunk_map.as_flat() else {
            emit_declined(&object_name(&key), to_fill.len());
            incomplete += 1;
            continue;
        };

        // Materialize the explicit full-length identity vector for each empty chunk
        // — the same resolution `placed_dserver` already applies implicitly, now
        // made durable (`core/src/metadata.rs:119-124`).
        let mut next_chunk_map = scanned_chunks.to_vec();
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
            size: record.size,
            chunk_map: next_chunk_map.into(),
            state: InodeState::Committed,
            version: record.version + 1,
            // Backfill re-commits the SAME content (a repair), so it PRESERVES the object
            // metadata (ADR-0047): a placement-maintenance commit must not move
            // `Last-Modified` or drop the content type.
            ..record.clone()
        };
        let inode_key = metadata::inode_key(inode_id);
        let batch = WriteBatch::new()
            .require(inode_key.clone(), metadata::encode(&record))
            .put(inode_key, metadata::encode(&next));

        match ctx.meta.commit(batch).await? {
            CommitOutcome::Committed => {
                // Materialized, so these leave the population this pass reports — the ONE
                // way anything leaves it. The `+=` above ran on this same iteration for
                // this same length, so the subtraction cannot go below zero.
                remaining -= to_fill.len() as u64;
                emit_backfilled(inode_id, to_fill.len());
                changed = true;
            }
            // A racing writer/custodian won the version-conditional commit: ordinary
            // second-fence racing, and the pass still answers over what it did
            // (`tests/backfill.rs:278-325` pins that answer as `Satisfied` — "declined work
            // ⇒ Blocked" is about work this pass may not PERFORM, never about a CAS it
            // merely lost). Its empty placements STAY on `remaining`, deliberately: this
            // pass never read the winner's bytes, so only a fill IT committed is evidence
            // one landed, and a drain signal may only ever err toward still-owed work.
            //
            // deferred: #699 — reading the winner back to settle what its placements now
            // are is a comparison of two generations, which is that slice's; re-reading it
            // here would also spend a second reading of a record this pass already read.
            // The residue is accounting only, and self-corrects: the next pass scans the
            // winner's own generation and reports the population from THAT reading.
            CommitOutcome::Conflict => emit_conflict(inode_id),
        }
    }

    emit_remaining(remaining, incomplete);

    Ok(if incomplete > 0 {
        // A pass that could not read part of the committed namespace, or declined a fill it
        // may not perform, has answered over less than the store: `Satisfied` would tell an
        // operator the drain converged and `Changed` would claim it converged what it
        // declined. The same refusal `gc.rs:234-246` makes over an incomplete reference
        // set, for the same reason (`docs/principles.md` §5 C-1) — an operator reading
        // `Satisfied` acts on it: decommissions the server, closes the ticket.
        Reconciled::Blocked
    } else if changed {
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
/// by #363). Counted in the pass's OWN walk — never by a second resolving reading, which
/// over segmented objects would spend every `seg:` range read twice for a number the first
/// reading already held.
///
/// It is a **sample over what this pass read**, never a proof the store is drained: a scan
/// hands back a snapshot, so a record another writer changes behind the walk is counted as
/// the walk saw it, and only a fill this pass itself committed comes off. `incomplete` is
/// what bounds it — the committed objects this pass could not read, or read and declined —
/// and while that is non-zero the true population can only be LARGER than this sample.
///
/// Both ride the SAME event, each as its own `gauge.`-prefixed instrument: an unprefixed
/// integer beside a gauge reaches the `tracing`→OTel bridge as an ATTRIBUTE on every metric
/// in the event, which would split the series an operator watches.
fn emit_remaining(remaining: u64, incomplete: u64) {
    tracing::info!(
        gauge.backfill_placement_remaining = remaining,
        gauge.backfill_placement_incomplete = incomplete,
    );
}

/// Emit a committed object whose chunk map this pass could **not read** on the
/// durability-plane seam (ADR-0011 / ADR-0012): its own bytes will not decode, or the
/// shared resolver refused the generation its root names.
///
/// The **same `action` string** GC and restore already publish for the same condition
/// (`gc.rs:563-573`, `restore.rs:825-835`), so one grep finds every loop that met the
/// record. Named by the store's own key through [`object_name`], which escapes rather than
/// replaces, so two damaged records never arrive under one name (`gc.rs:470-480`).
///
/// Emitted the moment the walk meets the record, ahead of every store read that follows —
/// `gc.rs:155-166`'s placement, for `gc.rs:159-160`'s reason: a store fault a `?` later
/// ends the pass with an `Err`, and a name this pass ALREADY HELD must not go down with it.
fn emit_unresolvable(object: &str, fault: &str) {
    tracing::warn!(monotonic_counter.backfill_unresolvable_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.backfill.audit",
        action = "unresolvable-chunk-map",
        inode = %object,
        fault = %fault,
        "backfill could not read a committed object's chunk map; the object is left untouched and every other record is still filled, but this pass certifies NOTHING until it is repaired — operator signal",
    );
}

/// Emit a fill this pass **may not perform** on the same seam: the generation the scan
/// returned keeps its chunks in `seg:` records, and rewriting one is the segmented write
/// path's (#682).
///
/// A decline, under its own `action` so a reader tells it apart from an unreadable record:
/// this object was read perfectly well, so the operator's move is to wait for that write
/// path, not to go repair a record nothing is wrong with. Nothing at all is written for it,
/// the empty placements this pass read stay on the remaining gauge, and the pass does not
/// certify.
///
/// deferred: #699 — `placements` is what the shared resolver answered, which for a generation
/// superseded mid-resolve is the live root's chunk list rather than the scanned one. Every word
/// of the decline still holds there (this pass wrote nothing, and a fill it read is still owed,
/// which is exactly what the gauge is a sample of); telling those two readings apart is a
/// generation comparison, and that is #699's slice, not this one's.
fn emit_declined(object: &str, placements: usize) {
    tracing::warn!(monotonic_counter.backfill_declined_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.backfill.audit",
        action = "declined-segmented",
        inode = %object,
        placements,
        "backfill DECLINED the fill for a committed object whose scanned generation keeps its chunks in `seg:` records (the segmented write path is #682); nothing at all was written for it, the empty placements it read stay on the remaining gauge, and this pass certifies nothing",
    );
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
