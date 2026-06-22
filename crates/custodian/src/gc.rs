//! The **GC custodian loop** (proposal 0005 §"The four custodian loops" / GC,
//! `0005:288-295`; the GC step of the reconstruction pipeline `0005:279`; the
//! correctness argument Q3 `0005:394-397`; the graduation invariant `0005:486-488`;
//! PR-sequence slice 4 `0005:524-527`).
//!
//! GC promotes the test-invoked stand-in (`core::sweep_expired_leases`,
//! `crates/core/src/write.rs:332`, which removed only the `pending:` ledger entry and
//! explicitly **deferred the fragment-byte reclaim**, `write.rs:330-331`) into a
//! running reconciliation loop dispatched from the fenced control point
//! ([`crate::reconcile_step`]). It reclaims the **two** GC inputs (`0005:288-291`):
//!
//! 1. the **bytes behind an expired pending-ledger lease** — the leased garbage a
//!    crashed write/repair fan-out leaves (`0005:289-290`); and
//! 2. an **orphaned fragment** — present in a D server's
//!    [`ChunkStore::list_fragments`] but referenced by **no** committed chunk map
//!    (from deletes and completed reconstructions, `0005:290-291`).
//!
//! Bytes are reclaimed via [`ChunkStore::delete_fragment`] **only after a reader-safe
//! grace window** — long enough that an in-flight reader holding the prior version is
//! never torn (`0005:291-294`; the pending-ledger sweep pattern of architecture §5).
//!
//! The loop's load-bearing invariant, whose violation is **silent corruption**:
//! **never reclaim a referenced fragment** — a fragment a committed chunk map's
//! placement record points at is **never** passed to `delete_fragment`
//! (`0005:294-295`, Q3 `0005:394-397`, graduation invariant `0005:488`).
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): this loop stays over the
//! `traits` / `core` seams plus `tracing` — **no** concrete backend.

use std::collections::{HashMap, HashSet};

use wyrd_core::metadata::{self, InodeRecord, InodeState, PendingEntry};
use wyrd_traits::{ChunkId, ChunkStore, DServerId, FragmentId, MetadataStore, Result, WriteBatch};

use crate::reconciliation::Reconciled;

/// Key prefix for the **orphan ledger** — the reader-safe grace record an orphaning
/// operation (a delete or a completed reconstruction) writes when it strands a
/// fragment, mirroring the pending-ledger sweep pattern (architecture §5; the grace
/// window of `0005:291-294`). The value is the logical-millis instant the fragment
/// became orphaned; GC reclaims it only once the grace window has elapsed past that
/// instant.
const ORPHAN_PREFIX: &[u8] = b"orphan:";

fn orphan_key(dserver: DServerId, frag: FragmentId) -> Vec<u8> {
    format!("orphan:{dserver}:{}:{}", frag.chunk, frag.index).into_bytes()
}

fn parse_orphan_key(key: &[u8]) -> Option<(DServerId, FragmentId)> {
    let rest = std::str::from_utf8(key).ok()?.strip_prefix("orphan:")?;
    let mut parts = rest.splitn(3, ':');
    let dserver = parts.next()?.parse().ok()?;
    let chunk = parts.next()?.parse().ok()?;
    let index = parts.next()?.parse().ok()?;
    Some((dserver, FragmentId { chunk, index }))
}

fn parse_pending_chunk(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("pending:")?
        .parse()
        .ok()
}

/// What the GC reconciler reads and reclaims over: the authoritative metadata store
/// (committed chunk maps + the pending / orphan ledgers) and the **fleet** of D
/// servers, each a [`ChunkStore`] keyed by its stable [`DServerId`]. The
/// `grace_window_millis` is the reader-safe window an **orphaned** fragment must
/// outlive before reclamation — **derived** from reader version-hold / lease
/// semantics by the caller, not a magic constant baked into GC (`0005:585-586`).
///
/// This is the input the running control point hands GC; it is **not** a deployed
/// custodian process (Option A, `0005:524-527`) — standing up the host that drives
/// the loop against live stores is a later slice. The loop is correct over these
/// abstractions and reachable through the real [`crate::reconcile_step`].
pub struct GcContext<'a> {
    /// The authoritative metadata store.
    pub meta: &'a dyn MetadataStore,
    /// The fleet of D servers to sweep, each addressed by its stable id.
    pub fleet: &'a [(DServerId, &'a dyn ChunkStore)],
    /// The reader-safe grace window (logical millis) an orphan must outlive.
    pub grace_window_millis: u64,
}

/// Record that `frag` on `dserver` became **orphaned** at `orphaned_at_millis` — the
/// grace-record an orphaning operation (delete / completed reconstruction, later
/// slices) writes so GC can honour the reader-safe window before reclaiming the
/// bytes. Idempotent at the metadata layer (a plain put).
pub async fn mark_orphaned(
    meta: &impl MetadataStore,
    dserver: DServerId,
    frag: FragmentId,
    orphaned_at_millis: u64,
) -> Result<()> {
    meta.commit(WriteBatch::new().put(
        orphan_key(dserver, frag),
        orphaned_at_millis.to_string().into_bytes(),
    ))
    .await?;
    Ok(())
}

/// One GC reconciliation pass over `ctx` at logical time `now_millis`. Dispatched
/// only from [`crate::reconcile_step`] (the fenced control point) — never a parallel
/// entry. Returns [`Reconciled::Changed`] if any fragment bytes were reclaimed,
/// [`Reconciled::Satisfied`] otherwise.
pub(crate) async fn reconcile(ctx: &GcContext<'_>, now_millis: u64) -> Result<Reconciled> {
    // The reference set is the safety gate: every fragment a *committed* chunk map's
    // placement record points at. A fragment in this set is NEVER reclaimed
    // (`0005:294-295`, Q3 `0005:394-397`) — its violation is silent corruption.
    let referenced = referenced_fragments(ctx.meta).await?;
    // Input (1): chunks whose pending lease has expired — their fan-out garbage is
    // collectable (the lease TTL already encodes the crashed-write grace).
    let expired_pending = expired_pending_chunks(ctx.meta, now_millis).await?;
    // Input (2): orphaned fragments and the instant each was stranded.
    let orphaned_at = orphan_leases(ctx.meta).await?;

    let mut changed = false;
    let mut cleanup = WriteBatch::new();
    let mut swept_pending: HashSet<ChunkId> = HashSet::new();

    for &(dserver, store) in ctx.fleet {
        for frag in store.list_fragments().await? {
            // SAFETY GATE — never reclaim a referenced fragment.
            if referenced.contains(&(dserver, frag)) {
                emit_skip(dserver, frag, "referenced");
                continue;
            }

            let reason = if let Some(&since) = orphaned_at.get(&(dserver, frag)) {
                // Orphan input: reclaim ONLY after the reader-safe grace window.
                if now_millis >= since.saturating_add(ctx.grace_window_millis) {
                    Some("orphan")
                } else {
                    emit_skip(dserver, frag, "within-grace");
                    None
                }
            } else if expired_pending.contains(&frag.chunk) {
                // Expired pending-lease input: the lease TTL is its grace.
                Some("expired-lease")
            } else {
                // No evidence the grace window elapsed — conservatively keep it
                // (reader-safe: a fragment is never reclaimed without a deadline).
                None
            };

            if let Some(reason) = reason {
                store.delete_fragment(frag).await?;
                emit_reclaim(dserver, frag, reason);
                cleanup = cleanup.delete(orphan_key(dserver, frag));
                if reason == "expired-lease" {
                    swept_pending.insert(frag.chunk);
                }
                changed = true;
            }
        }
    }

    // Retire the swept pending-ledger entries (the byte reclaim the stand-in
    // deferred, `write.rs:330-331`) and the consumed orphan grace records.
    for chunk in swept_pending {
        cleanup = cleanup.delete(metadata::pending_key(chunk));
    }
    if changed {
        ctx.meta.commit(cleanup).await?;
    }

    Ok(if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
}

/// Every `(dserver, fragment)` a **committed** chunk map references through its
/// placement record (`core::metadata::ChunkRef.placement`). A pending (uncommitted)
/// inode's provisional map is excluded — only a committed reference protects bytes.
async fn referenced_fragments(
    meta: &dyn MetadataStore,
) -> Result<HashSet<(DServerId, FragmentId)>> {
    let mut set = HashSet::new();
    for (_key, value) in meta.scan(b"inode:").await? {
        let record: InodeRecord = metadata::decode(&value)?;
        if record.state != InodeState::Committed {
            continue;
        }
        for chunk in &record.chunk_map {
            for (index, dserver) in chunk.placement.iter().enumerate() {
                set.insert((
                    *dserver,
                    FragmentId {
                        chunk: chunk.id,
                        index: index as u16,
                    },
                ));
            }
        }
    }
    Ok(set)
}

/// The chunk ids whose pending-ledger lease has expired as of `now_millis`.
async fn expired_pending_chunks(
    meta: &dyn MetadataStore,
    now_millis: u64,
) -> Result<HashSet<ChunkId>> {
    let mut set = HashSet::new();
    for (key, value) in meta.scan(b"pending:").await? {
        let entry: PendingEntry = metadata::decode(&value)?;
        if entry.lease_expiry_millis <= now_millis {
            if let Some(chunk) = parse_pending_chunk(&key) {
                set.insert(chunk);
            }
        }
    }
    Ok(set)
}

/// The orphan ledger: each stranded `(dserver, fragment)` and the instant it became
/// orphaned.
async fn orphan_leases(meta: &dyn MetadataStore) -> Result<HashMap<(DServerId, FragmentId), u64>> {
    let mut map = HashMap::new();
    for (key, value) in meta.scan(ORPHAN_PREFIX).await? {
        if let Some(slot) = parse_orphan_key(&key) {
            if let Some(at) = std::str::from_utf8(&value)
                .ok()
                .and_then(|s| s.parse().ok())
            {
                map.insert(slot, at);
            }
        }
    }
    Ok(map)
}

/// Emit a reclamation on the durability-plane seam (ADR-0011 / ADR-0012): a metric
/// the `DurabilityTelemetry` `tracing`→OTel bridge counts, plus an append-only audit
/// event (`0005:336-340`).
fn emit_reclaim(dserver: DServerId, frag: FragmentId, reason: &str) {
    tracing::info!(monotonic_counter.gc_fragments_reclaimed = 1_u64, reason);
    tracing::info!(
        target: "wyrd.custodian.gc.audit",
        action = "reclaim",
        reason,
        dserver,
        chunk = %frag.chunk,
        index = frag.index,
        "gc reclaimed collectable fragment bytes after the grace window",
    );
}

/// Emit a skip (a still-referenced or within-grace fragment) on the same seam — the
/// observable record that GC *considered* and *declined* a fragment.
fn emit_skip(dserver: DServerId, frag: FragmentId, reason: &str) {
    tracing::info!(monotonic_counter.gc_fragments_skipped = 1_u64, reason);
    tracing::info!(
        target: "wyrd.custodian.gc.audit",
        action = "skip",
        reason,
        dserver,
        chunk = %frag.chunk,
        index = frag.index,
        "gc declined a fragment (still referenced, or within its grace window)",
    );
}
