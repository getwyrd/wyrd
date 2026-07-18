//! The **rebalance custodian loop** — drain / decommission evacuation (proposal 0005
//! §"The four custodian loops" / Rebalance, `0005:297-303`; the shared
//! commit-point-atomic re-place §"Repair-vs-serve" `0005:305-317` and the atomicity
//! graduation line `0005:486`; §"Declarative management hook" `0005:346-356`;
//! PR-sequence slice 7, `0005:537-540`).
//!
//! Rebalance proactively moves fragments **off** D servers the operator has marked
//! draining / decommissioning (the [`crate::desired_state`] hook), **preserving the
//! failure-domain distinctness invariant** (`0005:298`, architecture §6.3 step 3). One
//! pass reads the desired state, finds every committed chunk with a fragment on a
//! draining server, and evacuates it (`0005:297-303`):
//!
//! ```text
//! desired: operator marks a D server draining/decommissioning (desired_state hook)
//! detect:  a committed chunk whose placement record points at the draining server
//! move:    pick a healthy NON-draining D server in a failure domain DISTINCT from the
//!          chunk's surviving fragments ──► copy the intact fragment bytes there FIRST
//!          ──[ONE version-conditional MetadataStore::commit: repoint the placement
//!             record + orphan the displaced fragment on the draining server]──►
//!             readers flip atomically to the new location
//! gc:      the displaced fragment ──[after GC's reader-safe grace window]──► reclaimed
//! ```
//!
//! Each move is **the same commit-point-atomic re-place as a reconstruction**
//! (`0005:298-299`, `0005:486`): the fragment is written to its new home **before** the
//! commit, so a crash mid-move leaves only **collectable garbage** (an orphaned
//! fragment GC reclaims), never a torn / hybrid chunk; the repoint is CAS'd on the
//! prior inode record, so a superseded custodian or racing writer loses the commit
//! rather than corrupting the placement record. Unlike reconstruction it needs **no**
//! erasure rebuild — the fragment is intact on the (alive, draining) server, so it is
//! **copied**, not reconstructed; the shared piece is the failure-domain selector and
//! the atomic repoint, not the decode.
//!
//! Two load-bearing invariants:
//!
//! - **Spread wins** (`0005:302-303`, durability is gate-zero): where a move cannot keep
//!   the chunk on `n` distinct domains (no free distinct domain remains off the draining
//!   servers), the selector **refuses** and the move is **aborted** — the fragment stays
//!   put rather than collapse the chunk's spread.
//! - **Never propagate corruption**: a fragment that is missing or checksum-failing on
//!   the draining server is **not** moved (that is a loss for the reconstruction loop,
//!   not a clean drain move) — only an intact fragment is copied.
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): the loop stays over the
//! `traits` / `core` seams plus `tracing` — the placement selector and the fragment
//! verify are borrowed from `core`, so `custodian` gains no backend and no
//! on-disk-format knowledge of its own.

use std::collections::{BTreeSet, HashMap};

use wyrd_core::metadata::{self, InodeId, InodeRecord, InodeState};
use wyrd_core::placement::{select_distinct_domains_excluding, FailureDomain, Topology};
use wyrd_core::repair;
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

use crate::desired_state;
use crate::reconciliation::Reconciled;

/// What the rebalance reconciler reads and re-places over: the authoritative metadata
/// store (committed chunk maps + the desired-state ledger), the **fleet** of D servers
/// — each a [`ChunkStore`] keyed by its stable [`DServerId`], the same shape GC / scrub
/// / reconstruction take — and the zone-local failure-domain
/// [`Topology`](wyrd_core::placement::Topology) the evacuated fragments are re-placed
/// against.
///
/// This is the input the running control point hands rebalance; it is **not** a
/// deployed custodian process (Option A, `0005:519-523`). The loop is correct over
/// these abstractions and reachable through the real [`crate::reconcile_step`].
pub struct RebalanceContext<'a> {
    /// The authoritative metadata store (chunk maps + the desired-state ledger).
    pub meta: &'a dyn MetadataStore,
    /// The fleet of D servers, each addressed by its stable id.
    pub fleet: &'a [(DServerId, &'a dyn ChunkStore)],
    /// The zone-local failure-domain view the evacuated fragments are re-placed against
    /// (the **same** selector the write fan-out uses, `0005:241-242`).
    pub topology: &'a Topology,
}

/// One chunk's evacuation plan: which fragment(s) sit on a draining server, where the
/// chunk lives (for the CAS), and the failure domains its surviving fragments occupy
/// (to keep the move disjoint).
struct EvacPlan {
    inode_id: InodeId,
    prior: InodeRecord,
    chunk_index: usize,
    chunk_id: ChunkId,
    /// The chunk's FULL fragment placement (length `n` == `fragment_count()`),
    /// resolved through the same authoritative identity-placement fallback the read
    /// path, GC, scrub, and reconstruction use (`ChunkRef::placed_dserver`,
    /// `core/src/metadata.rs:119`) — never the raw, possibly-empty or short
    /// `ChunkRef::placement` field. This is what gets cloned, indexed, and committed
    /// back by [`evacuate_chunk`], so it must already be full-length here.
    placement: Vec<DServerId>,
    /// Fragment indices on a draining server (to be evacuated).
    evac: Vec<usize>,
    /// The failure domains the fragments that **stay** occupy (excluded from the move).
    survivor_domains: Vec<FailureDomain>,
}

/// One rebalance reconciliation pass over `ctx` at logical time `now_millis`.
/// Dispatched only from [`crate::reconcile_step`] (the fenced control point) — never a
/// parallel entry. Returns [`Reconciled::Changed`] if any chunk's placement record was
/// repointed, [`Reconciled::Satisfied`] otherwise.
pub(crate) async fn reconcile(ctx: &RebalanceContext<'_>, now_millis: u64) -> Result<Reconciled> {
    let stores: HashMap<DServerId, &dyn ChunkStore> = ctx.fleet.iter().copied().collect();

    // **Capacity plane**: emit per-failure-domain utilization every pass — the
    // by-product of the domain model the durability plane publishes (`0005:341-343`).
    emit_domain_utilization(ctx.topology);

    // Read the operator's desired state: which D servers are draining/decommissioning.
    let draining = desired_state::draining_servers(ctx.meta).await?;
    if draining.is_empty() {
        return Ok(Reconciled::Satisfied);
    }
    let draining_set: BTreeSet<DServerId> = draining.keys().copied().collect();

    // Plan an evacuation for each committed chunk with a fragment on a draining server.
    let plans = plan_evacuations(ctx.meta, ctx.topology, &draining_set).await?;

    let mut changed = false;
    for plan in &plans {
        match evacuate_chunk(ctx, &stores, plan, &draining_set, now_millis).await? {
            EvacOutcome::Committed => changed = true,
            EvacOutcome::Conflict => emit_conflict(plan.chunk_id),
            EvacOutcome::Aborted => {}
        }
    }

    Ok(if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
}

/// Scan the committed chunk maps for fragments sitting on a draining server, building
/// one [`EvacPlan`] per affected chunk.
async fn plan_evacuations(
    meta: &dyn MetadataStore,
    topology: &Topology,
    draining: &BTreeSet<DServerId>,
) -> Result<Vec<EvacPlan>> {
    let mut plans = Vec::new();
    for (key, value) in meta.scan(b"inode:").await? {
        let record: InodeRecord = metadata::decode(&value)?;
        if record.state != InodeState::Committed {
            continue;
        }
        let Some(inode_id) = parse_inode_key(&key) else {
            continue;
        };
        for (chunk_index, chunk) in record.chunk_map.iter().enumerate() {
            // Resolve the FULL `0..fragment_count()` index space through the shared
            // STRICT companion (`ChunkRef::checked_fragments`, `core/src/metadata.rs`,
            // ADR-0040 decision 4) — classify the committed placement BEFORE expanding it,
            // NEVER the raw `placement` vector. A valid (empty / full-length) vector
            // resolves through the same authoritative identity-placement fallback the read
            // path, GC, scrub, and reconstruction use: a pre-M3 / mixed-era chunk decodes
            // with `placement: vec![]` (`#[serde(default)]`, `core/src/metadata.rs:93`)
            // and expands full-length, so a live fragment on a draining server is no longer
            // silently skipped (#346). A MALFORMED vector (non-empty, wrong length) is
            // rejected here — the chunk is skipped and flagged NEEDS-HUMAN rather than
            // evacuated over (and committed back with) a fabricated identity tail.
            let placement: Vec<DServerId> = match chunk.checked_fragments() {
                Ok(frags) => frags.map(|(_, dserver)| dserver).collect(),
                Err(_) => {
                    emit_needs_human(chunk.id);
                    continue;
                }
            };
            let evac: Vec<usize> = placement
                .iter()
                .enumerate()
                .filter(|(_, server)| draining.contains(server))
                .map(|(index, _)| index)
                .collect();
            if evac.is_empty() {
                continue;
            }
            // The domains the fragments that STAY occupy — resolved through the same
            // fallback as `placement` above, so a mixed-era chunk's spread is computed
            // over its FULL fragment set (not just whatever the raw vector happened to
            // carry) — the move must avoid them so the chunk keeps `n` distinct domains
            // (`0005:298`, the invariant).
            let survivor_domains: Vec<FailureDomain> = placement
                .iter()
                .enumerate()
                .filter(|(index, _)| !evac.contains(index))
                .filter_map(|(_, server)| topology.domain_of(*server).cloned())
                .collect();
            plans.push(EvacPlan {
                inode_id,
                prior: record.clone(),
                chunk_index,
                chunk_id: chunk.id,
                placement,
                evac,
                survivor_domains,
            });
        }
    }
    Ok(plans)
}

/// The outcome of evacuating one chunk.
enum EvacOutcome {
    /// The version-conditional commit landed; the fragment(s) were re-placed.
    Committed,
    /// The commit lost the CAS race (the copied fragments are now collectable garbage).
    Conflict,
    /// The move could not proceed — spread could not be preserved (no free distinct
    /// domain), or a fragment was missing / corrupt / off-fleet; nothing was committed.
    Aborted,
}

/// Evacuate `plan`'s fragment(s) off the draining server(s): copy each to a healthy
/// non-draining D server in a distinct failure domain, then repoint the chunk's
/// placement record with **one version-conditional commit**.
async fn evacuate_chunk(
    ctx: &RebalanceContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    plan: &EvacPlan,
    draining: &BTreeSet<DServerId>,
    now_millis: u64,
) -> Result<EvacOutcome> {
    // Select re-placement servers from the NON-draining pool, in domains distinct from
    // the survivors — so an evacuation never lands back on a draining server and never
    // collapses the chunk's spread. **Spread wins**: if no free distinct domain remains,
    // the selector refuses and the move is aborted (`0005:302-303`).
    let pool = ctx.topology.excluding(draining);
    let new_servers = match select_distinct_domains_excluding(
        &pool,
        plan.evac.len() as u16,
        &plan.survivor_domains,
    ) {
        Ok(servers) => servers,
        Err(_) => return Ok(EvacOutcome::Aborted),
    };

    // Copy each evacuated fragment to its new home FIRST — before the commit, so a crash
    // here leaves only collectable garbage, never a torn chunk (`0005:298-299`).
    let mut new_placement = plan.placement.clone();
    let mut displaced = Vec::new();
    for (slot, &index) in plan.evac.iter().enumerate() {
        let source = plan.placement[index];
        let target = new_servers[slot];
        let frag = FragmentId {
            chunk: plan.chunk_id,
            index: index as u16,
        };
        let (Some(source_store), Some(target_store)) = (stores.get(&source), stores.get(&target))
        else {
            // The source or selector target is outside the fleet view — cannot move.
            return Ok(EvacOutcome::Aborted);
        };
        // Only an INTACT fragment is moved. A missing / checksum-failing / misplaced /
        // misencoded fragment is a loss for the reconstruction loop, not a clean drain
        // move — never propagate it. Verify the FULL identity (chunk id + index + the
        // committed EC tuple) against the chunk map, not the `chunk_id` alone.
        let Some(bytes) = source_store.get_fragment(frag).await? else {
            return Ok(EvacOutcome::Aborted);
        };
        if !repair::fragment_intact(&bytes, frag, plan.prior.chunk_map[plan.chunk_index].scheme) {
            return Ok(EvacOutcome::Aborted);
        }
        target_store.put_fragment(frag, bytes).await?;
        displaced.push((source, frag));
        new_placement[index] = target;
    }

    // THE binding commit: ONE version-conditional mutation that atomically repoints the
    // placement record and orphans the displaced fragments on the draining server. The
    // CAS on the prior inode record is the second fence (`0005:200-203`, ADR-0015) — a
    // racing writer / superseded custodian loses here rather than corrupting the record.
    let mut next_chunk_map = plan.prior.chunk_map.clone();
    next_chunk_map[plan.chunk_index].placement = new_placement;
    let next = InodeRecord {
        size: plan.prior.size,
        chunk_map: next_chunk_map,
        state: InodeState::Committed,
        version: plan.prior.version + 1,
        // A rebalance re-places the SAME content, so it PRESERVES the object metadata
        // (ADR-0047): a placement-maintenance commit must not move `Last-Modified` or drop
        // the content type.
        ..plan.prior.clone()
    };
    let inode_key = metadata::inode_key(plan.inode_id);
    let mut batch = WriteBatch::new()
        .require(inode_key.clone(), metadata::encode(&plan.prior))
        .put(inode_key, metadata::encode(&next));
    for (dserver, frag) in &displaced {
        batch = batch.put(
            crate::gc::orphan_key(*dserver, *frag),
            now_millis.to_string().into_bytes(),
        );
    }

    match ctx.meta.commit(batch).await? {
        CommitOutcome::Committed => {
            emit_evacuated(plan.chunk_id, displaced.len());
            Ok(EvacOutcome::Committed)
        }
        // Lost the CAS race: the placement moved under us. The copied fragments are now
        // collectable garbage; the drain is re-assessed next pass.
        CommitOutcome::Conflict => Ok(EvacOutcome::Conflict),
    }
}

fn parse_inode_key(key: &[u8]) -> Option<InodeId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("inode:")?
        .parse()
        .ok()
}

/// Emit the **capacity plane's per-failure-domain utilization** on the durability-plane
/// seam (ADR-0011 / ADR-0012, `0005:341-343`): one gauge sample per failure domain, the
/// `DurabilityTelemetry` `tracing`→OTel bridge fans out (the `domain` label carries the
/// opaque domain id).
fn emit_domain_utilization(topology: &Topology) {
    for (domain, used) in topology.domain_utilization() {
        tracing::info!(
            gauge.capacity_domain_utilization = used,
            domain = domain.0.as_str(),
        );
    }
}

/// Emit an **evacuation** on the durability-plane seam (`0005:336-340`): the metric the
/// `tracing`→OTel bridge counts plus an append-only audit event for a chunk the pass
/// drained off a draining server.
fn emit_evacuated(chunk: ChunkId, moved: usize) {
    tracing::info!(monotonic_counter.rebalance_fragments_evacuated = moved as u64);
    tracing::info!(
        target: "wyrd.custodian.rebalance.audit",
        action = "evacuate",
        chunk = %wyrd_traits::chunk_hex(chunk),
        moved,
        "rebalance evacuated fragment(s) off a draining server and repointed the placement record",
    );
}

/// Emit a **NEEDS-HUMAN** signal on the durability-plane seam (ADR-0011 / ADR-0012,
/// ADR-0040 decision 4): rebalance found a committed chunk whose `placement` vector is
/// non-empty but of the wrong length — truncation / corruption. It is NOT evacuated
/// (moving over a fabricated identity tail would then commit the malformed record back);
/// the chunk is skipped and left for a human to resolve.
fn emit_needs_human(chunk: ChunkId) {
    tracing::warn!(monotonic_counter.rebalance_malformed_placement = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.rebalance.audit",
        action = "needs-human",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "rebalance skipped a chunk with a malformed committed placement (wrong length); NEEDS-HUMAN, fragment left in place",
    );
}

/// Emit a lost-CAS conflict on the same seam: the repoint raced another writer and the
/// copied fragments are now collectable garbage.
fn emit_conflict(chunk: ChunkId) {
    tracing::info!(monotonic_counter.rebalance_conflict = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.rebalance.audit",
        action = "conflict",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "rebalance lost the version-conditional commit; copied fragments are collectable garbage",
    );
}
