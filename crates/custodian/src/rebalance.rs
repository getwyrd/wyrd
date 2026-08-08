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
//! Three load-bearing invariants:
//!
//! - **Spread wins** (`0005:302-303`, durability is gate-zero): where a move cannot keep
//!   the chunk on `n` distinct domains (no free distinct domain remains off the draining
//!   servers), the selector **refuses** and the move is **aborted** — the fragment stays
//!   put rather than collapse the chunk's spread.
//! - **Never propagate corruption**: a fragment that is missing or checksum-failing on
//!   the draining server is **not** moved (that is a loss for the reconstruction loop,
//!   not a clean drain move) — only an intact fragment is copied.
//! - **One damaged object never stops the drain, and an incomplete pass never certifies
//!   one** (#696): every committed record is read through the ONE resolver every other
//!   consumer shares ([`metadata::resolve_chunk_map`], proposal 0016 decision 7(e)), a
//!   fault it meets is contained to the object that owns it — named on the durability
//!   seam and skipped, with the walk going on — and an evacuation this pass may not
//!   perform is **refused**, which writes nothing at all. Either way the pass answers
//!   [`Reconciled::Blocked`] rather than report a drain satisfied over work it did not do:
//!   an operator reading `Satisfied` is being told the server is safe to decommission, and
//!   will act on it (`docs/principles.md` §5 C-1).
//!
//! Dependency boundary (ADR-0010, `0005:421-422`): the loop stays over the
//! `traits` / `core` seams plus `tracing` — the placement selector and the fragment
//! verify are borrowed from `core`, so `custodian` gains no backend and no
//! on-disk-format knowledge of its own.

use std::collections::{BTreeSet, HashMap};

use wyrd_core::metadata::{self, ChunkMapError, ChunkRef, InodeId, InodeRecord, InodeState};
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
    /// The **scanned** generation's flat chunk list — the bytes the repoint is built from
    /// and conditioned on, carried from the scan rather than read again at commit time.
    ///
    /// Only a **flat** scanned record ever produces a plan (a segmented one is refused in
    /// [`plan_evacuations`]), so this list always exists and [`evacuate_chunk`] needs no
    /// second reading of the map's shape — which is what removes the pass's second
    /// "a segmented map ends everything" site rather than guarding it.
    prior_chunks: Vec<ChunkRef>,
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
/// parallel entry. Returns [`Reconciled::Blocked`] if the pass met a committed object it
/// could not read or an evacuation it may not perform ([`EvacScan::withheld`]),
/// [`Reconciled::Changed`] if any chunk's placement record was repointed, and
/// [`Reconciled::Satisfied`] otherwise.
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
    let scan = plan_evacuations(ctx.meta, ctx.topology, &draining_set).await?;

    let mut changed = false;
    for plan in &scan.plans {
        match evacuate_chunk(ctx, &stores, plan, &draining_set, now_millis).await? {
            EvacOutcome::Committed => changed = true,
            EvacOutcome::Conflict => emit_conflict(plan.chunk_id),
            // deferred: #682 — an ordinary abort (no free distinct domain, an off-fleet /
            // missing / checksum-failing fragment) is swallowed here exactly as it is on
            // the base: this child makes only the refusal IT introduces non-certifying,
            // and whether an abort should also withhold certification is decided with the
            // segmented write path, whose refusals it would then have to answer for.
            EvacOutcome::Aborted => {}
        }
    }

    Ok(if scan.withheld {
        // Refuse to certify. Whatever was evacuated above is durable either way — every
        // plan was built from a record this pass READ, and a refusal wrote nothing. What
        // answering `Changed` / `Satisfied` would destroy is the only signal that this pass
        // could not finish the drain: an operator reading either is being told the
        // evacuation is converging, and a decommission acts on that (`docs/principles.md`
        // §5 C-1). The same shape GC answers an incomplete reference set with.
        Reconciled::Blocked
    } else if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
}

/// What one scan of the committed namespace produced: the evacuations this pass may
/// perform, and whether it met anything it must not certify over.
struct EvacScan {
    /// One plan per chunk this pass may evacuate.
    plans: Vec<EvacPlan>,
    /// Whether the scan met something that **withholds the drain's certification**: a
    /// committed object it could not read at all (contained and named, exactly as GC
    /// contains one — `crate::gc::reconcile`), or an evacuation it may not perform (a chunk
    /// whose bytes live in a `seg:` record, refused and never written). Either way this
    /// pass has no picture of the whole store, so it answers [`Reconciled::Blocked`].
    ///
    /// Deliberately **not** set by two conditions this slice leaves exactly as the base
    /// answers them:
    ///
    /// * a **malformed** committed placement — skipped + NEEDS-HUMAN ([`emit_needs_human`],
    ///   ADR-0040 decision 4), and already blocked **cluster-wide** at the operator's own
    ///   drain query by [`crate::desired_state::ReconciliationStatus::PendingMalformed`],
    ///   which is a different surface from one loop's convergence answer;
    /// * an ordinary [`EvacOutcome::Aborted`] — deferred: #682, see the work loop above.
    withheld: bool,
}

/// Scan the committed chunk maps for fragments sitting on a draining server, building
/// one [`EvacPlan`] per affected chunk.
///
/// Every committed record is read through the ONE resolver every other consumer shares
/// ([`metadata::resolve_chunk_map`], proposal 0016 decision 7(e)) — the same reading GC
/// (`crate::gc::referenced_fragments`) and restore (`crate::restore`) already do — so a
/// **segmented** object's chunks are judged here like any other's instead of ending the
/// pass, and a record that will not decode, or a generation the resolver cannot read, is
/// CONTAINED: named on the durability seam and skipped, with the walk going on. A fault
/// that is **not** this object's own (a store failing underneath the read) still
/// propagates, by exactly the downcast rule GC uses: a walk that cannot reach the metadata
/// store has no answer for any object, not one unreadable object.
///
/// **Whether this pass may write for an object is decided from the generation the SCAN
/// returned** — this record's own `chunk_map` shape, already in hand — never from the shape
/// of whatever a resolve answered after restarting onto a newer root. That needs no
/// machinery of its own: a flat snapshot resolves to a borrow of the record and reads
/// nothing, so it can never be superseded and never restarts
/// (`crates/core/src/metadata.rs:2585`, `:2629`); only a segmented snapshot can, and a
/// segmented snapshot is one this pass refuses. So the restart path reaches no write at
/// all, by construction — no generation comparison and no counter of its own (the
/// fleet-wide version of that question is deferred: #699).
///
/// Attribution is emitted **per object, where the object is read** — and therefore before
/// the caller's work loop, mirroring `crate::gc::reconcile` — so a later transient store
/// fault cannot cost the operator the name of the record to repair.
async fn plan_evacuations(
    meta: &dyn MetadataStore,
    topology: &Topology,
    draining: &BTreeSet<DServerId>,
) -> Result<EvacScan> {
    let mut plans = Vec::new();
    let mut withheld = false;
    for (key, value) in meta.scan(b"inode:").await? {
        // The record's own bytes are in hand, so a decode failure is THIS object's fault
        // and no store's — contained, and conservatively without first asking whether the
        // record was committed (reading `state` out of bytes that will not decode needs a
        // lenient peek this crate owns no decoder for; blocking until the record is
        // repaired is the fail-closed direction).
        let record: InodeRecord = match metadata::decode(&value) {
            Ok(record) => record,
            Err(fault) => {
                emit_unresolvable(&crate::gc::object_name(&key), &fault.to_string());
                withheld = true;
                continue;
            }
        };
        if record.state != InodeState::Committed {
            continue;
        }
        let Some(inode_id) = parse_inode_key(&key) else {
            continue;
        };
        // `Ok(None)` is no live committed generation under this key (deleted or retired
        // since the scan read it): there is nothing left to evacuate, so it is skipped
        // exactly as an uncommitted record is above — and exactly as both merged peers skip
        // it (`crate::gc`, `crate::restore`).
        //
        // The network bound on this await is the `MetadataStore` IMPLEMENTATION's, not this
        // caller's (#508/#636) — the same rule `crate::gc` and `crate::restore` follow for
        // the same call, and the same rule the `meta.scan(b"inode:")` above has always
        // followed. It is fail-closed either way: an error here either propagates or
        // contains the object, and is never read as "this object owns no chunks".
        let resolved = match metadata::resolve_chunk_map(meta, &key, &record).await {
            Ok(Some(resolved)) => resolved,
            Ok(None) => continue,
            Err(err) => match err.downcast::<ChunkMapError>() {
                // The resolver's own typed verdict that THIS generation cannot be read —
                // recovered by downcast because the trait seam boxes every error. Contained.
                Ok(fault) => {
                    emit_unresolvable(&crate::gc::object_name(&key), &fault.to_string());
                    withheld = true;
                    continue;
                }
                // Not a chunk-map anomaly: a store fault under the read. Not this object's
                // fault, so it is not folded into "this object is unreadable".
                Err(err) => return Err(err),
            },
        };
        // The eligibility decision, read off the scanned generation's own shape (above).
        // For a flat record this IS `resolved.chunks` — a flat map resolves to a borrow of
        // exactly this list (`crates/core/src/metadata.rs:2585`), so `chunk_index` indexes
        // both the same way.
        let scanned_flat = record.chunk_map.as_flat();
        // One refusal per OBJECT, not one per chunk: a multipart object owing a thousand
        // evacuations is one blocker to repair, not a thousand lines on the seam.
        let mut refused = false;
        for (chunk_index, chunk) in resolved.chunks.iter().enumerate() {
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
            // An evacuation this pass may NOT perform: the chunk's bytes live in a `seg:`
            // record, and the evacuation write path for one is #682's. REFUSED — nothing at
            // all is written for it, the record and its `seg:` records are left
            // byte-identical, and the drain is not certified below. Refusing (rather than
            // aborting the whole pass, or silently dropping the chunk) is what keeps every
            // OTHER object in the store draining while this one waits for #682.
            let Some(prior_chunks) = scanned_flat else {
                refused = true;
                continue;
            };
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
                prior_chunks: prior_chunks.to_vec(),
                chunk_index,
                chunk_id: chunk.id,
                placement,
                evac,
                survivor_domains,
            });
        }
        if refused {
            emit_refused(&crate::gc::object_name(&key));
            withheld = true;
        }
    }
    Ok(EvacScan { plans, withheld })
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

    // The chunk list this commit is built from and conditioned on is the one the SCAN read
    // ([`EvacPlan::prior_chunks`]) — the map's shape is never read a second time here, so a
    // segmented map has no site left in this function to end the pass from. Only a flat
    // scanned record produces a plan at all; a segmented one is refused in
    // [`plan_evacuations`] and never reaches this far.
    let prior_chunk_map = &plan.prior_chunks;

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
        if !repair::fragment_intact(&bytes, frag, prior_chunk_map[plan.chunk_index].scheme) {
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
    let mut next_chunk_map = prior_chunk_map.to_vec();
    next_chunk_map[plan.chunk_index].placement = new_placement;
    let next = InodeRecord {
        size: plan.prior.size,
        chunk_map: next_chunk_map.into(),
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

/// Emit a committed object whose chunk map this pass could **not read** on the
/// durability-plane seam (ADR-0011 / ADR-0012): the record's own bytes will not decode, or
/// the resolver refused the generation it names. The object is CONTAINED — every other
/// object in the store is planned and evacuated exactly as usual — and NAMED by the store's
/// own key ([`crate::gc::object_name`], which escapes rather than replaces, so two damaged
/// records never arrive under one name and a repair guided by it fixes the right one).
///
/// The **same** `action` string GC, restore and scrub already publish for the same
/// condition (`crate::gc::emit_unresolvable`), each with its own
/// `<loop>_unresolvable_records` counter — so one grep over the durability plane finds
/// every loop blocked on one record.
fn emit_unresolvable(object: &str, fault: &str) {
    tracing::warn!(monotonic_counter.rebalance_unresolvable_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.rebalance.audit",
        action = "unresolvable-chunk-map",
        inode = %object,
        fault = %fault,
        "rebalance could not read a committed object's chunk map; the object is skipped, the rest of the store still drains, and this pass certifies NOTHING until the record is repaired — operator signal",
    );
}

/// Emit an evacuation this pass **may not perform** on the same seam: the chunk's bytes
/// live in a `seg:` record, whose evacuation write path is not built yet (deferred: #682).
///
/// A refusal writes **nothing at all** — the root and its `seg:` records are left
/// byte-identical — and it withholds the drain's certification, because an operator reading
/// a satisfied drain is being told the server is safe to decommission and will act on it.
///
/// Once per **object**, not once per chunk: the operator's unit of repair is the object, and
/// a line per chunk floods the seam for exactly the multipart objects this names.
fn emit_refused(object: &str) {
    tracing::warn!(monotonic_counter.rebalance_refused_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.rebalance.audit",
        action = "refused-segmented",
        inode = %object,
        reason = "segmented-chunk-map",
        "rebalance refused an evacuation it may not perform: this object's chunks live in seg: records and the write path for one is not built yet (#682); nothing was written and the drain is NOT certified",
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
