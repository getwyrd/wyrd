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
pub async fn reconcile(ctx: &BackfillContext<'_>) -> Result<Reconciled> {
    let mut changed = false;

    for (key, value) in ctx.meta.scan(b"inode:").await? {
        let record: InodeRecord = metadata::decode(&value)?;
        if record.state != InodeState::Committed {
            continue;
        }
        let Some(inode_id) = parse_inode_key(&key) else {
            continue;
        };

        // Classify BEFORE acting (ADR-0040 decision 4, reusing #348's single-source
        // classifier): collect the indices of chunks whose committed placement is
        // EMPTY, surfacing any MALFORMED one as an operator signal along the way.
        // Neither classification mutates `record.chunk_map` — a read-only pass.
        let mut to_fill = Vec::new();
        for (index, chunk) in record.chunk_map.iter().enumerate() {
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

        // Materialize the explicit full-length identity vector for each empty chunk
        // — the same resolution `placed_dserver` already applies implicitly, now
        // made durable (`core/src/metadata.rs:119-124`).
        let mut next_chunk_map = record.chunk_map.clone();
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
            chunk_map: next_chunk_map,
            state: InodeState::Committed,
            version: record.version + 1,
        };
        let inode_key = metadata::inode_key(inode_id);
        let batch = WriteBatch::new()
            .require(inode_key.clone(), metadata::encode(&record))
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
async fn emit_remaining(meta: &dyn MetadataStore) -> Result<()> {
    let mut remaining: u64 = 0;
    for (_key, value) in meta.scan(b"inode:").await? {
        let record: InodeRecord = metadata::decode(&value)?;
        if record.state != InodeState::Committed {
            continue;
        }
        remaining += record
            .chunk_map
            .iter()
            .filter(|chunk| chunk.placement.is_empty())
            .count() as u64;
    }
    tracing::info!(gauge.backfill_placement_remaining = remaining);
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
