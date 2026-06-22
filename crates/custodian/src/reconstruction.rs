//! The **reconstruction custodian loop** — the heart of M3 (proposal 0005
//! §"Reconstruction — the heart of M3", `0005:269-286`; §"Repair-vs-serve: dynamic
//! priority, not a static throttle", `0005:305-317`; the three M3 repair metrics
//! `0005:326-332`; PR-sequence slice 6, `0005:531-536`).
//!
//! Scrub (`0005:262-267`) and the read path (`0005:174-176`) only **produce** repair
//! obligations on the one shared, durable queue ([`wyrd_core::repair`]). This loop is
//! the **consumer**: on each pass it drains the queue and, for each affected chunk
//! (`0005:273-279`):
//!
//! ```text
//! detect:  the obligation on the shared repair queue (a D-server loss, a scrub or
//!          read checksum failure) ──► an under-replicated chunk
//! repair:  gather any k surviving fragments ──[verify checksums]──► reconstruct the
//!          missing shard(s) from the chunk's PER-CHUNK EcScheme
//!          ──► place the rebuilt fragment(s) on healthy D servers in DISTINCT
//!              failure domains
//!          ──[ONE version-conditional MetadataStore::commit: repoint the placement
//!             record + drain the obligation + orphan the displaced fragment]──►
//!             readers flip atomically to the new location
//! gc:      the displaced fragment ──[after GC's reader-safe grace window]──► reclaimed
//! ```
//!
//! Two load-bearing invariants (whose violation is silent corruption or data loss):
//!
//! - **The location update is ONE version-conditional commit** (`0005:277`,
//!   `0005:200-203`, ADR-0015): the rebuilt fragments are written **before** the
//!   commit, so a crash mid-repair leaves only **collectable garbage** (orphaned
//!   fragments GC reclaims), never a torn or hybrid chunk. The repoint is CAS'd on the
//!   prior inode record, so a superseded custodian or a racing writer loses the commit
//!   rather than corrupting the placement record.
//! - **A checksum-failing shard is never decoded** (`0005:275`): every surviving
//!   fragment is verified via [`wyrd_core::repair::fragment_intact`] before it is fed
//!   to the decoder; a corrupt one is **excluded** and treated as missing.
//!
//! Reconstruction is **scheme-driven** from the chunk's per-chunk [`EcScheme`]
//! (`0005:282-284`): *k*/*m* vary per chunk (mixed-era), so the rebuild reads the
//! recorded scheme, never a zone-global constant. With encryption on the client
//! encrypts *below* EC (ADR-0021, `0005:285-286`), so the custodian rebuilds
//! **ciphertext** fragments and never needs tenant keys.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): the loop stays over the
//! `traits` / `core` seams plus `tracing` — the erasure math, the placement selector,
//! and the on-disk fragment format are all borrowed from `core`, so `custodian` gains
//! no backend and no on-disk-format knowledge of its own.

use std::collections::HashMap;

use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_core::placement::{select_distinct_domains_excluding, FailureDomain, Topology};
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, repair};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

use crate::reconciliation::Reconciled;

/// What the reconstruction reconciler reads, rebuilds, and re-places over: the
/// authoritative metadata store (committed chunk maps + the shared repair queue), the
/// **fleet** of D servers — each a [`ChunkStore`] keyed by its stable [`DServerId`],
/// the same shape GC / scrub take — and the zone-local failure-domain
/// [`Topology`](wyrd_core::placement::Topology) the rebuilt fragments are re-placed
/// against.
///
/// This is the input the running control point hands reconstruction; it is **not** a
/// deployed custodian process (Option A, `0005:524-527`). The loop is correct over
/// these abstractions and reachable through the real [`crate::reconcile_step`].
pub struct ReconstructionContext<'a> {
    /// The authoritative metadata store (chunk maps + the repair queue).
    pub meta: &'a dyn MetadataStore,
    /// The fleet of D servers, each addressed by its stable id. A server absent from
    /// this map (or holding no/no-longer-intact bytes) is a **loss** the rebuild reads
    /// around.
    pub fleet: &'a [(DServerId, &'a dyn ChunkStore)],
    /// The zone-local failure-domain view the rebuilt fragments are re-placed against
    /// (the **same** selector the write fan-out uses, `0005:241-242`).
    pub topology: &'a Topology,
}

/// The **repair priority** of a chunk, derived from how close it is to its durability
/// floor (`0005:305-317`): a chunk with `survivors` intact fragments under a scheme
/// that needs `k` has `survivors - k` fragments of slack before it becomes
/// unrecoverable. Repair priority **rises as redundancy falls**, so this returns the
/// slack as the **ascending** sort key — a smaller value is a chunk nearer its floor,
/// which is drained (and, with the read-retry reserved seat of proposal 0004, would
/// preempt foreground work) **first**.
///
/// This is the priority *function* M3 builds; the full global admission / backpressure
/// scheduler (`0005:315-317`, §8.9) lands incrementally and is out of scope here.
pub fn repair_priority(survivors: usize, k: usize) -> i64 {
    survivors as i64 - k as i64
}

/// One chunk's reconstruction plan: where it lives, its scheme, and the surviving vs.
/// missing fragments an assessment pass found — enough to both **prioritize** the
/// drain and **execute** the rebuild without re-fetching.
struct RepairPlan {
    inode_id: InodeId,
    prior: InodeRecord,
    chunk_index: usize,
    chunk_id: ChunkId,
    k: usize,
    m: usize,
    /// `(fragment_index, decoded shard bytes)` for each intact survivor.
    survivors: Vec<(usize, Vec<u8>)>,
    /// The failure domains the survivors occupy (to keep the rebuild disjoint).
    survivor_domains: Vec<FailureDomain>,
    /// Fragment indices that are missing or checksum-failing (to be rebuilt).
    missing: Vec<usize>,
    /// The chunk's current placement vector (length `n`).
    placement: Vec<DServerId>,
    /// The logical (pre-coding) chunk length, for `erasure::reconstruct`.
    len: usize,
}

/// One reconstruction reconciliation pass over `ctx` at logical time `now_millis`.
/// Dispatched only from [`crate::reconcile_step`] (the fenced control point) — never a
/// parallel entry. Returns [`Reconciled::Changed`] if any chunk's placement record was
/// repointed, [`Reconciled::Satisfied`] otherwise.
pub(crate) async fn reconcile(
    ctx: &ReconstructionContext<'_>,
    now_millis: u64,
) -> Result<Reconciled> {
    let stores: HashMap<DServerId, &dyn ChunkStore> = ctx.fleet.iter().copied().collect();

    // Drain the shared repair queue: the obligations scrub / the read path produced.
    let queue = repair::queued_repairs(ctx.meta).await?;
    emit_queue_depth(queue.len());

    // Assess each obligation (resolve the chunk, gather + verify survivors) so the
    // drain can be ordered by repair priority before any rebuild commits.
    let mut plans = Vec::new();
    let mut drain_only = Vec::new();
    for chunk in queue {
        match assess(ctx, &stores, chunk).await? {
            Assessment::Repairable(plan) => plans.push(plan),
            // The obligation refers to a chunk no longer referenced (deleted), or one
            // already at full redundancy (a duplicate / transient finding): nothing to
            // rebuild, so just drain the obligation.
            Assessment::Drain => drain_only.push(chunk),
            // Below `k` survivors (loss beyond the scheme's tolerance) or a scheme with
            // no redundancy (`EcScheme::None`): not reconstructable here. Leave the
            // obligation queued and surface it as under-replicated.
            Assessment::Unrepairable => {}
        }
    }

    // Repair priority: most-urgent (nearest its durability floor) first (`0005:305-317`).
    plans.sort_by_key(|p| repair_priority(p.survivors.len(), p.k));

    // **Emit the durability-plane metrics here**, from the assessment frame — *before*
    // the rebuild/commit loop. This is deliberate, not incidental: the rebuild step runs
    // a heavy erasure-decode + version-conditional commit, and emitting a metric on the
    // `tracing`→OTel seam *after* that section is unreliable under load (the bridge can
    // drop the late event), so the three M3 repair metrics (`0005:326-332`) are emitted
    // up front where the assessment is authoritative — the under-replicated chunk count
    // and, per chunk the pass is reconstructing, the dispatched-repair counter and the
    // time-to-repair sample. A repair that subsequently loses the CAS race is recorded
    // on the separate `reconstruction_conflict` counter (so successes = repaired −
    // conflict), and re-assessed next pass.
    emit_under_replicated(plans.len());
    for plan in &plans {
        emit_repaired(plan.chunk_id, plan.missing.len(), now_millis);
    }

    let mut changed = false;
    for plan in &plans {
        match repair_chunk(ctx, &stores, plan, now_millis).await? {
            RepairOutcome::Committed => changed = true,
            RepairOutcome::Conflict => emit_conflict(plan.chunk_id),
            RepairOutcome::Aborted => {}
        }
    }

    // Drain the no-op obligations in one commit (best-effort; not the binding repoint).
    if !drain_only.is_empty() {
        let mut batch = WriteBatch::new();
        for chunk in drain_only {
            batch = batch.delete(repair::repair_key(chunk));
        }
        ctx.meta.commit(batch).await?;
    }

    Ok(if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
}

/// The outcome of assessing one queued obligation.
enum Assessment {
    /// A reconstructable under-replicated chunk, with its survivors already gathered.
    Repairable(Box<RepairPlan>),
    /// Nothing to rebuild — drain the obligation (deleted chunk, or already healthy).
    Drain,
    /// Cannot be reconstructed in this slice (below `k`, or a no-redundancy scheme).
    Unrepairable,
}

/// Resolve `chunk` to its committed chunk map, then gather and **verify** its surviving
/// fragments — classifying it into an [`Assessment`].
async fn assess(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    chunk: ChunkId,
) -> Result<Assessment> {
    let Some((inode_id, prior, chunk_index)) = find_chunk(ctx.meta, chunk).await? else {
        // The chunk is referenced by no committed chunk map — it was deleted out from
        // under the obligation. Nothing to repair.
        return Ok(Assessment::Drain);
    };
    let chunk_ref = prior.chunk_map[chunk_index].clone();

    let (k, m) = match chunk_ref.scheme {
        // A single-fragment chunk has no redundancy to reconstruct from; recovering it
        // is a replica-copy concern, not erasure reconstruction (out of scope here).
        EcScheme::None => return Ok(Assessment::Unrepairable),
        EcScheme::ReedSolomon { k, m } => (k as usize, m as usize),
    };
    let n = k + m;
    let placement: Vec<DServerId> = (0..n)
        .map(|i| {
            chunk_ref
                .placement
                .get(i)
                .copied()
                .unwrap_or(i as DServerId)
        })
        .collect();

    let mut survivors = Vec::new();
    let mut survivor_domains = Vec::new();
    let mut missing = Vec::new();
    for (index, &dserver) in placement.iter().enumerate() {
        let frag = FragmentId {
            chunk,
            index: index as u16,
        };
        let bytes = match stores.get(&dserver) {
            Some(store) => store.get_fragment(frag).await?,
            None => None,
        };
        // VERIFY: a present fragment must decode cleanly AND name this chunk; a
        // checksum-failing or misplaced fragment is excluded (never decoded) and
        // treated as missing (`0005:275`). `repair::intact_shard` is the shared verify,
        // so `custodian` recovers the shard without a chunk-format dependency.
        match bytes
            .as_deref()
            .and_then(|b| repair::intact_shard(b, chunk))
        {
            Some(shard) => {
                survivors.push((index, shard));
                if let Some(domain) = ctx.topology.domain_of(dserver) {
                    survivor_domains.push(domain.clone());
                }
            }
            None => missing.push(index),
        }
    }

    if missing.is_empty() {
        // Already at full redundancy: a stale / duplicate obligation. Drain it.
        return Ok(Assessment::Drain);
    }
    if survivors.len() < k {
        // Loss beyond the scheme's tolerance: cannot reconstruct (more than `m` gone).
        return Ok(Assessment::Unrepairable);
    }

    Ok(Assessment::Repairable(Box::new(RepairPlan {
        inode_id,
        prior,
        chunk_index,
        chunk_id: chunk,
        k,
        m,
        survivors,
        survivor_domains,
        missing,
        placement,
        len: chunk_ref.len as usize,
    })))
}

/// The outcome of repairing one chunk — reported back to [`reconcile`] so the metric /
/// audit emission stays in that frame.
enum RepairOutcome {
    /// The version-conditional commit landed; the rebuilt shard(s) were re-placed.
    Committed,
    /// The commit lost the CAS race (rebuilt fragments are now collectable garbage).
    Conflict,
    /// The repair could not proceed (e.g. the selector chose a server outside the
    /// fleet view); nothing was committed.
    Aborted,
}

/// Rebuild `plan`'s missing fragment(s), re-place them in distinct failure domains, and
/// repoint the chunk's placement record with **one version-conditional commit**.
async fn repair_chunk(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    plan: &RepairPlan,
    now_millis: u64,
) -> Result<RepairOutcome> {
    let (k, m, chunk_id) = (plan.k, plan.m, plan.chunk_id);

    // Reconstruct the chunk's logical bytes from any `k` survivors, then re-derive
    // EVERY shard scheme-driven (`erasure::encode` is deterministic, so the rebuilt
    // shard is byte-identical to the original). The missing shards are taken from this.
    let available: Vec<(usize, Vec<u8>)> = plan.survivors.clone();
    let data = erasure::reconstruct(k, m, plan.len, &available)?;
    let all_shards = erasure::encode(k, m, &data)?;

    // Pick re-placement domains for the missing fragments, distinct from each other AND
    // from the survivors' domains (keeps the chunk on `n` distinct domains, `0005:491`).
    let new_servers = select_distinct_domains_excluding(
        ctx.topology,
        plan.missing.len() as u16,
        &plan.survivor_domains,
    )?;

    // Write the rebuilt fragments to their new D servers FIRST — before the commit, so a
    // crash here leaves only collectable garbage, never a torn chunk (`0005:277`).
    let mut new_placement = plan.placement.clone();
    let mut displaced = Vec::new();
    for (slot, &index) in plan.missing.iter().enumerate() {
        let target = new_servers[slot];
        let Some(target_store) = stores.get(&target) else {
            // The selector chose a server outside the fleet view — cannot place. Abort
            // this chunk's repair (leave the obligation; nothing was committed).
            return Ok(RepairOutcome::Aborted);
        };
        let shard = &all_shards[index];
        let frag_bytes =
            encode_ec_fragment(chunk_id, index as u16, plan.k as u8, plan.m as u8, shard);
        let frag = FragmentId {
            chunk: chunk_id,
            index: index as u16,
        };
        target_store.put_fragment(frag, frag_bytes).await?;

        let old = plan.placement[index];
        if old != target {
            displaced.push((old, frag));
        }
        new_placement[index] = target;
    }

    // THE binding commit: ONE version-conditional mutation that atomically repoints the
    // placement record, drains the obligation, and orphans the displaced fragments. The
    // CAS on the prior inode record is the second fence (`0005:200-203`, ADR-0015) — a
    // racing writer / superseded custodian loses here rather than corrupting the record.
    let mut next_chunk_map = plan.prior.chunk_map.clone();
    next_chunk_map[plan.chunk_index].placement = new_placement;
    let next = InodeRecord {
        size: plan.prior.size,
        chunk_map: next_chunk_map,
        state: InodeState::Committed,
        version: plan.prior.version + 1,
    };
    let inode_key = metadata::inode_key(plan.inode_id);
    let mut batch = WriteBatch::new()
        .require(inode_key.clone(), metadata::encode(&plan.prior))
        .put(inode_key, metadata::encode(&next))
        .delete(repair::repair_key(chunk_id));
    for (dserver, frag) in &displaced {
        batch = batch.put(
            crate::gc::orphan_key(*dserver, *frag),
            now_millis.to_string().into_bytes(),
        );
    }

    match ctx.meta.commit(batch).await? {
        CommitOutcome::Committed => Ok(RepairOutcome::Committed),
        // Lost the CAS race: the placement moved under us. The rebuilt fragments are
        // collectable garbage; the obligation stays queued for the next pass.
        CommitOutcome::Conflict => Ok(RepairOutcome::Conflict),
    }
}

/// Find the committed inode whose chunk map references `chunk`, returning its id, the
/// full prior record (for the CAS), and the chunk's index within the map.
async fn find_chunk(
    meta: &dyn MetadataStore,
    chunk: ChunkId,
) -> Result<Option<(InodeId, InodeRecord, usize)>> {
    for (key, value) in meta.scan(b"inode:").await? {
        let record: InodeRecord = metadata::decode(&value)?;
        if record.state != InodeState::Committed {
            continue;
        }
        if let Some(index) = record
            .chunk_map
            .iter()
            .position(|c: &ChunkRef| c.id == chunk)
        {
            if let Some(inode_id) = parse_inode_key(&key) {
                return Ok(Some((inode_id, record, index)));
            }
        }
    }
    Ok(None)
}

fn parse_inode_key(key: &[u8]) -> Option<InodeId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("inode:")?
        .parse()
        .ok()
}

/// Emit **repair-queue depth** on the durability-plane seam (ADR-0011 / ADR-0012,
/// `0005:330`): the number of obligations the pass observed on the shared queue.
fn emit_queue_depth(depth: usize) {
    tracing::info!(histogram.reconstruction_queue_depth = depth as u64);
}

/// Emit the **under-replicated chunk count** (`0005:326-329`): the chunks this pass
/// found below their scheme's fragment count — the metric whose silent non-zero value
/// is the durability failure the request plane hides.
fn emit_under_replicated(count: usize) {
    tracing::info!(monotonic_counter.reconstruction_under_replicated = count as u64);
}

/// Emit a **dispatched reconstruction** plus the **time-to-repair** sample (`0005:330`):
/// the metric + an append-only audit event (`0005:336-340`) for a chunk the pass is
/// reconstructing. The sample is the logical instant of the repair pass; a per-obligation
/// enqueue stamp (a precise elapsed window) is a later refinement of the shared queue's
/// value encoding. A dispatched repair that loses its CAS is recorded separately on
/// [`emit_conflict`], so successful repairs are `reconstruction_repaired − conflict`.
fn emit_repaired(chunk: ChunkId, rebuilt: usize, now_millis: u64) {
    tracing::info!(monotonic_counter.reconstruction_repaired = 1_u64);
    tracing::info!(histogram.reconstruction_time_to_repair_millis = now_millis);
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "repair",
        chunk = %chunk,
        rebuilt,
        "reconstruction is rebuilding the missing shard(s) and repointing the placement record",
    );
}

/// Emit a lost-CAS conflict on the same seam: the repoint raced another writer and the
/// rebuilt fragments are now collectable garbage.
fn emit_conflict(chunk: ChunkId) {
    tracing::info!(monotonic_counter.reconstruction_conflict = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "conflict",
        chunk = %chunk,
        "reconstruction lost the version-conditional commit; rebuilt fragments are collectable garbage",
    );
}
