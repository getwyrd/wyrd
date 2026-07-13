//! **Post-restore reconciliation** (#551) — the pass that puts the fragment tier and the
//! metadata tier back on the same page after the metadata has been restored from a backup.
//!
//! # Why this exists
//!
//! Backup is asymmetric by tier (architecture §8.2): the **metadata is backed up, the
//! fragments are not** — EC plus custodian reconstruction *is* the fragments' durability.
//! So a restore moves the metadata back to some version *V* while the D servers stay at
//! "now", and the two tiers land at **different points in time**. "Restore the map and let
//! the custodian sort it out" is exactly what an operator expects to be true, and it is
//! **not** — for two reasons, both of which this pass exists to answer.
//!
//! ## 1. Stranded fragments leak forever
//!
//! [`crate::gc`] never reclaims a fragment on suspicion. It reclaims on **evidence** that a
//! reader-safe grace deadline has elapsed: an `orphan:` record, or an expired `pending:`
//! lease. Absent either, its final branch is *"no evidence the grace window elapsed —
//! conservatively keep it"*. That conservatism is correct — it is what makes it impossible
//! for GC to race a reader — but it has a sharp consequence after a restore:
//!
//! A file created **after** *V* loses its chunk map in the restore, so its fragments are
//! unreferenced. But its `orphan:` / `pending:` records lived **in the metadata**, so the
//! restore erased those too. The fragments are therefore unreferenced *and* evidence-free:
//! GC keeps them, forever, and the space leaks with no mechanism to reclaim it.
//!
//! This pass supplies the missing evidence. It marks every unreferenced fragment as an
//! orphan (the same record [`crate::mark_orphaned`] writes), which hands it to the *existing*
//! GC on its *existing* grace window. It deletes nothing itself.
//!
//! ## 2. Files deleted after *V* come back unreadable
//!
//! The mirror image. A file that existed at *V* and was **deleted** after it has its chunk
//! map *resurrected* by the restore — while its fragments were reclaimed at delete time.
//! Whether that file is readable depends on how far the GC got before the restore:
//!
//! - inside the grace window, nothing reclaimed → all fragments present → **readable**;
//! - fewer than `m` fragments reclaimed → **reconstructible**, and the repair loop handles it;
//! - more than `m` gone → fewer than `k` remain → a **dangling map**: the file is back in the
//!   namespace, unreadable, and unreconstructible — there is nothing left to rebuild from.
//!
//! Nothing detects the third case today; an operator meets it as a failed read. This pass
//! enumerates them and surfaces each on the durability seam, so a restore's true cost is
//! *known* rather than discovered.
//!
//! # The safety gate, unchanged
//!
//! Marking is the front half of a deletion, so the invariant [`crate::gc`] is built around
//! holds here identically and is enforced twice: **a fragment referenced by a committed chunk
//! map is never marked** (and, even if it somehow were, GC's own gate would still refuse to
//! reclaim it). A chunk with a *malformed* placement is treated as fully referenced — fail
//! safe — exactly as GC treats it.
//!
//! # Idempotent, and running it twice is not a way to lose data
//!
//! A fragment that **already** carries an `orphan:` record is left alone rather than
//! re-marked: re-stamping would reset its grace clock and *delay* reclamation. Re-running the
//! pass is therefore free, and never resets a deadline.
//!
//! # Explicit, never automatic
//!
//! This is an operator command, not a loop step. Marking leads to deletion, and "the metadata
//! version went backwards, so mark everything unreferenced" is a rule that would fire on a
//! *misconfigured* cluster (an empty or wrong metadata store) and cheerfully mark the entire
//! fleet's fragments as orphans. The blast radius of a false positive is the whole cluster, so
//! the trigger is a human who knows a restore happened — and who has stopped the writers, as
//! the runbook says.

use std::collections::{HashMap, HashSet};

use wyrd_core::metadata::{self, InodeRecord, InodeState};
use wyrd_traits::{ChunkId, DServerId, FragmentId, MetadataStore, Result, WriteBatch};

use crate::gc::{orphan_key, orphan_leases, referenced_fragments, GcContext};

/// How many orphan marks to commit at once.
///
/// NOT one fleet-sized batch: FoundationDB — the backend whose restore this pass exists to
/// clean up after — caps transaction size and age, so a large restore delta would exceed the
/// limit, fail, and record no evidence at all, leaving a command that can never make progress.
/// Bounded batches make partial progress durable, which is safe precisely because the pass is
/// idempotent (an already-marked fragment is skipped, its original grace clock intact).
const MARK_BATCH: usize = 1_000;

/// What one [`reconcile_after_restore`] pass found and did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RestoreReport {
    /// Unreferenced fragments newly marked `orphan:` — the evidence GC needs to reclaim
    /// them on its normal grace window. **This pass deletes nothing**; these become
    /// collectable, not collected.
    pub stranded_marked: usize,
    /// Unreferenced fragments that already carried an `orphan:` record. Left untouched —
    /// re-stamping would reset the grace clock and delay their reclamation.
    pub already_marked: usize,
    /// Unreferenced fragments left alone because their chunk still holds a `pending:`
    /// lease — an in-flight write, whose lease TTL is already its grace. GC owns them.
    pub pending_skipped: usize,
    /// Fragments the restored map still needs but whose bytes have MOVED — a repair or
    /// rebalance after the restore point wrote them to a new D server and repointed the
    /// placement, and the restore rewound the map but not the bytes. These are the **only
    /// surviving copy**, so they are never marked: deleting them would turn a stale placement
    /// (repairable) into real data loss. The chunk is fine; the map is out of date, and the
    /// repair loop repoints it.
    pub displaced_kept: usize,
    /// Committed chunks with **fewer than `k` fragments present**: unreadable, and
    /// unreconstructible. A restore resurrected the map after the bytes were reclaimed.
    /// **These files are lost** — the pass reports them, it cannot recover them.
    pub dangling: Vec<ChunkId>,
    /// Committed chunks missing fragments but still holding **at least `k`**: readable, and
    /// the reconstruction loop will rebuild them. Reported for visibility, not for action.
    pub under_replicated: Vec<ChunkId>,
}

impl RestoreReport {
    /// Did the pass find anything an operator must act on or absorb?
    pub fn is_clean(&self) -> bool {
        self.stranded_marked == 0 && self.dangling.is_empty() && self.under_replicated.is_empty()
    }
}

/// Reconcile the fragment tier against a **restored** metadata store, at logical time
/// `now_millis`.
///
/// Two halves, in one pass over the fleet:
///
/// 1. every fragment **no committed chunk map references** is marked `orphan:` (unless it is
///    already marked, or its chunk still holds a pending lease), which is the evidence
///    [`crate::gc`] requires before it will ever reclaim bytes; and
/// 2. every **committed chunk** is checked against the fragments actually present, and those
///    that can no longer be read *or rebuilt* are reported as [`RestoreReport::dangling`].
///
/// Deletes nothing. Marks only. Run it with **writers stopped**, after a restore.
///
/// # The fleet must be COMPLETE
///
/// `ctx.fleet` must contain **every** D server, not the reachable subset. Both halves of the
/// pass read absence as meaning something, and a missing server makes absence a lie:
///
/// - a fragment on an unreachable server is not in `list_fragments`, so its chunk looks short
///   and could be reported [`RestoreReport::dangling`] — **live data declared lost**; and
/// - that server's own strays are never marked, so the leak persists on exactly the box nobody
///   looked at.
///
/// A partial view cannot tell *"the fragment is gone"* from *"the server is down"*, and telling
/// those apart is this pass's entire job. Callers that assemble a fleet with degraded-start
/// semantics (as `connect_fleet` does, deliberately, for the repair loop) **must** refuse to
/// run this pass on the survivors.
pub async fn reconcile_after_restore(
    ctx: &GcContext<'_>,
    now_millis: u64,
) -> Result<RestoreReport> {
    let referenced = referenced_fragments(ctx.meta).await?;
    let already = orphan_leases(ctx.meta).await?;
    let pending = pending_chunks(ctx.meta).await?;

    let mut report = RestoreReport::default();
    let mut marks = WriteBatch::new();
    // The fragments queued in the CURRENT batch, held back until it commits. Counting or
    // auditing a mark before its transaction lands would let a failed commit (an FDB
    // transaction error, say) leave a permanent, append-only audit trail and a monotonic
    // counter both claiming evidence that was never written — the report would overstate the
    // reconciliation, and the next operator to read it would believe fragments are collectable
    // that GC will never touch. Evidence is claimed only once it is durable.
    let mut batched: Vec<(DServerId, FragmentId)> = Vec::new();

    // Pass 1 — WHAT IS ACTUALLY ON DISK, before deciding anything. The whole fleet's view has
    // to exist before a single mark is written, because the question "may I mark this copy?"
    // cannot be answered from one D server alone (see the displaced case below).
    let mut present: HashSet<(DServerId, FragmentId)> = HashSet::new();
    let mut on_disk: Vec<(DServerId, FragmentId)> = Vec::new();
    for &(dserver, store) in ctx.fleet {
        for frag in store.list_fragments().await? {
            present.insert((dserver, frag));
            on_disk.push((dserver, frag));
        }
    }

    // Where the RESTORED map says each fragment lives. A restore rewinds the placement record
    // along with everything else, so this is the map's opinion — which the bytes may have moved
    // on from (below).
    let mut canonical: HashMap<FragmentId, Vec<DServerId>> = HashMap::new();
    for &(dserver, frag) in &referenced.placed {
        canonical.entry(frag).or_default().push(dserver);
    }

    // Pass 2 — decide, with the full picture.
    for (dserver, frag) in on_disk {
        // SAFETY GATE, identical to GC's: never mark a fragment the restored map points at —
        // nor any fragment of a malformed-placement chunk, whose true placement cannot be
        // trusted (fail safe).
        if referenced.protects(dserver, frag) {
            continue;
        }

        // THE DISPLACED CASE, and it is a data-loss trap.
        //
        // A repair or rebalance that landed AFTER the restore point moved this fragment: it
        // wrote the bytes to a new D server and repointed `placement[index]` at it
        // (`reconstruction.rs` / `rebalance.rs`: `new_placement[index] = target`). The restore
        // rewinds the map to the OLD server — while the bytes sit here, on the new one.
        //
        // So the map references this (chunk, index) but not at THIS server, and the naive
        // (dserver, fragment) check calls the bytes unreferenced. Mark them and GC deletes the
        // ONLY SURVIVING COPY of a fragment the map still needs. That is not a leak; it is
        // destroying live data, and it is the one outcome this pass must never produce.
        if let Some(holders) = canonical.get(&frag) {
            let canonical_copy_exists = holders.iter().any(|&d| present.contains(&(d, frag)));
            if !canonical_copy_exists {
                // The map's server does NOT have it; this is the last copy. Never mark it.
                // The chunk is not lost — the bytes are right here — the PLACEMENT is stale.
                // Repair repoints it; deleting it would make the loss real.
                report.displaced_kept += 1;
                emit_displaced(dserver, frag, holders);
                continue;
            }
            // The map's server DOES have it, so this copy is the stale duplicate a completed
            // move left behind — the copy whose `orphan:` record the restore erased. Marking it
            // is exactly right, and is the leak this pass exists to close.
        }

        if already.contains_key(&(dserver, frag)) {
            report.already_marked += 1;
            continue;
        }
        // An in-flight write's fragments are not orphans: the pending lease is already their
        // grace, and GC sweeps them when it expires. (With writers stopped, as the runbook
        // requires, this should be empty — but running the pass against a live cluster must not
        // steal fragments out from under a committing writer.)
        if pending.contains(&frag.chunk) {
            report.pending_skipped += 1;
            continue;
        }

        marks = marks.put(
            orphan_key(dserver, frag),
            now_millis.to_string().into_bytes(),
        );
        batched.push((dserver, frag));

        // Commit in BOUNDED batches. One fleet-sized WriteBatch would be the obvious shape, and
        // it breaks on the backend this pass exists for: FoundationDB caps a transaction's size
        // (and its age), so a restore that stranded enough fragments would blow the limit, fail
        // the commit, and record NO evidence at all — and every re-run would fail identically,
        // leaving the operator with a command that can never make progress on precisely the
        // large restore that needs it most.
        //
        // Partial progress is safe here *because* the pass is idempotent: a fragment marked by
        // an earlier batch is skipped (`already`) on the next run, with its original grace clock
        // intact. So a batch that lands is durable progress, and one that fails costs only the
        // work since the last commit.
        if batched.len() >= MARK_BATCH {
            ctx.meta.commit(std::mem::take(&mut marks)).await?;
            // Durable now — and only now is a mark real.
            for &(d, f) in &batched {
                emit_strand(d, f);
            }
            report.stranded_marked += batched.len();
            batched.clear();
        }
    }

    // The tail of the final batch, on the same terms.
    if !batched.is_empty() {
        ctx.meta.commit(std::mem::take(&mut marks)).await?;
        for &(d, f) in &batched {
            emit_strand(d, f);
        }
        report.stranded_marked += batched.len();
    }

    // The set of fragments whose bytes exist SOMEWHERE, regardless of which server holds them.
    let present_anywhere: HashSet<FragmentId> = present.iter().map(|&(_d, f)| f).collect();

    // Pass 3 — the metadata's view: what the map points at that is no longer anywhere.
    for (chunk, expected) in committed_chunks(ctx.meta).await? {
        // Count a fragment as AVAILABLE if its bytes exist ANYWHERE in the fleet — not only at
        // the D server the restored (and possibly stale) placement names. A fragment a repair
        // moved after the restore point sits at the new server while the rewound map still
        // names the old one; counting strictly by placement would call those bytes missing and
        // report a perfectly recoverable chunk as DANGLING — data lost. A false "your data is
        // gone" is the worst thing this command can say, so availability is counted generously
        // and staleness is reported separately (`displaced_kept`).
        let available = expected
            .frags
            .iter()
            .filter(|&&(_dserver, frag)| present_anywhere.contains(&frag))
            .count();
        if available < usize::from(expected.k) {
            report.dangling.push(chunk);
            emit_dangling(chunk, available, expected.k, expected.frags.len());
        } else if available < expected.frags.len() {
            report.under_replicated.push(chunk);
        }
    }

    emit_summary(&report);
    Ok(report)
}

/// A committed chunk's reconstruction threshold and where its fragments are meant to live.
struct Expected {
    /// Fragments needed to reconstruct (`k`); `EcScheme::None` is a single fragment, k = 1.
    k: u16,
    /// Every `(dserver, fragment)` the committed placement points at.
    frags: Vec<(DServerId, FragmentId)>,
}

/// Every **committed** chunk, with its `k` and its placement. Skips malformed placements —
/// GC treats them as fully referenced (fail safe) and so does this pass; a chunk whose
/// placement cannot be trusted is not one to declare dangling.
async fn committed_chunks(meta: &dyn MetadataStore) -> Result<Vec<(ChunkId, Expected)>> {
    let mut out = Vec::new();
    for (_key, value) in meta.scan(b"inode:").await? {
        let record: InodeRecord = metadata::decode(&value)?;
        if record.state != InodeState::Committed {
            continue;
        }
        for chunk in &record.chunk_map {
            let Ok(frags) = chunk.checked_fragments() else {
                continue;
            };
            let frags: Vec<(DServerId, FragmentId)> = frags
                .map(|(index, dserver)| {
                    (
                        dserver,
                        FragmentId {
                            chunk: chunk.id,
                            index,
                        },
                    )
                })
                .collect();
            out.push((
                chunk.id,
                Expected {
                    k: reconstruction_threshold(chunk),
                    frags,
                },
            ));
        }
    }
    Ok(out)
}

/// How many fragments must survive for this chunk to be rebuildable: `k` under
/// Reed-Solomon, and 1 under `EcScheme::None` (the lone fragment *is* the data).
fn reconstruction_threshold(chunk: &wyrd_core::metadata::ChunkRef) -> u16 {
    match chunk.scheme {
        wyrd_core::metadata::EcScheme::None => 1,
        wyrd_core::metadata::EcScheme::ReedSolomon { k, .. } => u16::from(k),
    }
}

/// Chunk ids that still hold a `pending:` lease — an in-flight write, GC's business.
async fn pending_chunks(meta: &dyn MetadataStore) -> Result<HashSet<ChunkId>> {
    let mut out = HashSet::new();
    for (key, _value) in meta.scan(b"pending:").await? {
        if let Some(chunk) = std::str::from_utf8(&key)
            .ok()
            .and_then(|k| k.strip_prefix("pending:"))
            .and_then(|c| c.parse().ok())
        {
            out.insert(chunk);
        }
    }
    Ok(out)
}

/// A fragment nothing references and nothing accounted for — the leak this pass closes.
/// Marked collectable; **not** deleted.
fn emit_strand(dserver: DServerId, frag: FragmentId) {
    tracing::info!(monotonic_counter.restore_fragments_marked = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.restore.audit",
        action = "mark-stranded",
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        "post-restore: fragment referenced by no committed chunk map and carrying no grace record; marked orphan so GC can reclaim it after the grace window",
    );
}

/// A committed chunk that can no longer be read **or rebuilt** — the restore resurrected its
/// map after GC had already reclaimed its bytes. The file is lost; this is the operator
/// signal that says so, instead of leaving it to be found by a failed read.
fn emit_dangling(chunk: ChunkId, available: usize, k: u16, n: usize) {
    tracing::error!(monotonic_counter.restore_dangling_chunks = 1_u64);
    tracing::error!(
        target: "wyrd.custodian.restore.audit",
        action = "dangling",
        chunk = %wyrd_traits::chunk_hex(chunk),
        available,
        required = k,
        total = n,
        "post-restore: committed chunk has fewer than k fragments present — UNREADABLE and UNRECONSTRUCTIBLE. The restore resurrected a map whose bytes were already reclaimed; this data is lost",
    );
}

/// A fragment the restored map still needs, found somewhere the map does not name — and found
/// NOWHERE the map does name. The bytes moved after the restore point (a repair/rebalance
/// repointed `placement[index]`), and the restore rewound the map beneath them. Never marked:
/// this is the last copy, and marking it would hand the only surviving bytes to GC.
fn emit_displaced(dserver: DServerId, frag: FragmentId, expected_on: &[DServerId]) {
    tracing::warn!(monotonic_counter.restore_fragments_displaced = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "displaced-kept",
        dserver,
        chunk = %wyrd_traits::chunk_hex(frag.chunk),
        index = frag.index,
        expected_on = ?expected_on,
        "post-restore: the restored placement names a D server that does not hold this fragment, \
         while THIS server does — a repair moved the bytes after the restore point. Kept (never \
         marked): it is the only surviving copy. The placement is stale, not the data; repair \
         repoints it",
    );
}

/// The pass's own verdict, so a restore's true cost lands in one line an operator can read.
fn emit_summary(report: &RestoreReport) {
    tracing::info!(
        target: "wyrd.custodian.restore.audit",
        action = "summary",
        stranded_marked = report.stranded_marked,
        already_marked = report.already_marked,
        pending_skipped = report.pending_skipped,
        displaced_kept = report.displaced_kept,
        dangling = report.dangling.len(),
        under_replicated = report.under_replicated.len(),
        "post-restore reconciliation complete",
    );
}
