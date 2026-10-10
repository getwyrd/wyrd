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
//! prior inode record — and, for a segmented object, on the `seg:` record that holds the
//! chunk ([`metadata::repoint_chunk`]) — so a superseded custodian or racing writer loses
//! the commit rather than corrupting the placement record. Unlike reconstruction it needs **no**
//! erasure rebuild — the fragment is intact on the (alive, draining) server, so it is
//! **copied**, not reconstructed; the shared piece is the failure-domain selector and
//! the atomic repoint, not the decode.
//!
//! Five load-bearing invariants:
//!
//! - **Spread wins** (`0005:302-303`, durability is gate-zero): where a move cannot keep
//!   the chunk on `n` distinct domains (no free distinct domain remains off the draining
//!   servers), the selector **refuses** and the move is **aborted** — the fragment stays
//!   put rather than collapse the chunk's spread.
//! - **Never propagate corruption**: a fragment that is missing or checksum-failing on
//!   the draining server is **not** moved (that is a loss for the reconstruction loop,
//!   not a clean drain move) — only an intact fragment is copied.
//! - **A move that did not persist neither certifies nor counts** (#710): a repoint whose
//!   re-encoded record would cross the backend value ceiling
//!   ([`metadata::flat_value_ceiling_crossed`]) is **refused before anything is written** —
//!   committing one would leave an object whose placement can never be repaired again — and
//!   a pass that refused, aborted or lost the CAS on a planned move answers
//!   [`Reconciled::Blocked`], never `Satisfied`: the fragment is still on the draining
//!   server, and an operator reading a satisfied drain pulls the box.
//! - **One damaged object never stops the drain, and an incomplete pass never certifies
//!   one** (#696): every committed record is read through the ONE resolver every other
//!   consumer shares ([`metadata::resolve_chunk_map`], proposal 0016 decision 7(e)), and a
//!   fault it meets — or the move's own re-read meets — is contained to the object that owns
//!   it: named ONCE on the durability seam and skipped, with the walk going on. The pass
//!   then answers [`Reconciled::Blocked`] rather than report a drain satisfied over a store
//!   it could only partly read: an operator reading `Satisfied` is being told the server is
//!   safe to decommission, and will act on it (`docs/principles.md` §5 C-1).
//! - **Every shape of committed map drains** (#722): the fragment is moved in whichever
//!   record holds its `ChunkRef` — the flat inode record or one `seg:` record — through the
//!   one placement move reconstruction also uses ([`metadata::repoint_chunk`]), so a
//!   decommission converges over a multipart object instead of waiting on it forever.
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
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result,
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

/// One committed object this pass owes an evacuation inside, as the resolve answered it —
/// held ONCE per object and shared by every plan in it (an index into [`EvacScan::objects`],
/// never a copy), so a multipart object owing many moves is one snapshot of its root rather
/// than one deep copy per plan.
struct EvacObject {
    inode_id: InodeId,
    /// The root record **the resolve answered from** — this scan's own snapshot, or the live
    /// one a retired resolution restarted onto ([`metadata::ResolvedChunkMap::record`]). It is
    /// the generation the move pins and, for a flat object, the record it re-encodes.
    prior: InodeRecord,
}

/// One chunk's evacuation plan: which fragment(s) sit on a draining server, where the
/// chunk lives (for the CAS), and the failure domains its surviving fragments occupy
/// (to keep the move disjoint).
struct EvacPlan {
    /// Index into [`EvacScan::objects`] — the ONE snapshot of the object holding this chunk.
    object: usize,
    /// The first object byte this chunk covers: the address [`metadata::repoint_chunk`]
    /// finds it by, in whichever record holds its `ChunkRef`. A flat map and a segmented
    /// one are both byte tilings, so the scan never branches on the map's shape.
    byte_offset: u64,
    /// The committed reference this plan was built from: the scheme the intact-fragment
    /// check verifies against, and the reference the move requires to be unchanged.
    prior: ChunkRef,
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
/// parallel entry. Returns [`Reconciled::Blocked`] if the pass contained a committed object
/// it could not read or rewrite ([`EvacScan::contained`]), or a planned move did **not**
/// persist ([`EvacOutcome::persisted`]); [`Reconciled::Changed`] if
/// every planned move landed and at least one placement record was repointed; and
/// [`Reconciled::Satisfied`] only where reality already matched the desired state.
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
    let mut scan = plan_evacuations(ctx.meta, ctx.topology, &draining_set).await?;

    let mut changed = false;
    // Set by any planned move that did NOT persist — the drain's one certification rule,
    // which #696 deliberately left to this slice. It is the whole rule: a fragment still
    // sitting on the draining server is still sitting on the draining server, whether the
    // move was aborted for want of a free distinct domain, refused for crossing the value
    // ceiling, or lost its CAS. The pass names each of them differently below — it may
    // certify over none of them.
    let mut unmoved = false;
    for plan in &scan.plans {
        let object = &scan.objects[plan.object];
        let outcome = evacuate_chunk(ctx, &stores, object, plan, &draining_set, now_millis).await?;
        // Apply the ONE certification rule to the outcome itself, and apply it FIRST — ahead
        // of the arms that merely name it, so the drain's answer neither depends on an arm nor
        // can be dropped by one. An arm forgetting to withhold certification is the whole
        // defect this closes (`EvacOutcome::Aborted => {}` certified a move that never
        // happened), so the rule is read off the outcome exactly once, before any of them.
        unmoved |= !outcome.persisted();
        // Then name what it was on the durability seam.
        match outcome {
            EvacOutcome::Committed => changed = true,
            EvacOutcome::Conflict => emit_conflict(plan.chunk_id),
            EvacOutcome::Refused { bytes, ceiling } => {
                emit_ceiling_refused(plan.chunk_id, bytes, ceiling)
            }
            // Named once per OBJECT, however many of its chunks meet the same fault.
            EvacOutcome::Contained { fault } => contain(
                &mut scan.contained,
                &metadata::inode_key(object.inode_id),
                &fault,
            ),
            // An ordinary abort (no free distinct domain, an off-fleet / missing /
            // checksum-failing fragment) keeps the base's silence here — the selector's own
            // refusal is already the operator's signal for it, and it is transient — but it
            // no longer certifies the drain: the rule above already withheld that, whatever
            // this arm does or does not say. The `deferred: #682` marker #696 left on
            // this arm is DISCHARGED, not dropped: the refusal this slice adds lands in this
            // same `match`, so leaving the arm silent would have re-created the very defect
            // it records for the new outcome on the day that outcome was born.
            EvacOutcome::Aborted => {}
        }
    }

    Ok(if !scan.contained.is_empty() || unmoved {
        // Refuse to certify. Whatever was evacuated above is durable either way — every
        // plan was built from a record this pass READ, and a refusal wrote nothing. What
        // answering `Changed` / `Satisfied` would destroy is the only signal that this pass
        // could not finish the drain: an operator reading either is being told the
        // evacuation is converging, and a decommission acts on that (`docs/principles.md`
        // §5 C-1). The same shape GC answers an incomplete reference set with.
        //
        // `Blocked` outranks `Changed` ([`Reconciled::least_certified`]), so a pass that
        // moved one chunk and could not move another still reports the weaker — and true —
        // claim: this drain has not finished. The operator's per-server query
        // ([`crate::desired_state::reconciliation_status`]) stays the authority on *which*
        // server is still referenced; this is one loop's answer about its own pass.
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
    /// One snapshot per committed object this pass owes an evacuation inside, shared by
    /// every plan in it.
    objects: Vec<EvacObject>,
    /// One plan per chunk this pass may evacuate.
    plans: Vec<EvacPlan>,
    /// Every committed object this pass **contained**, by the store's own key bytes — one it
    /// could not read at all, or could read but the move cannot address (a segmented row
    /// under a non-canonical key, chunk lengths past `u64`), or whose records the move's own
    /// re-read found unusable ([`EvacOutcome::Contained`]). Each is named exactly once
    /// ([`contain`]), as GC contains one (`crate::gc::reconcile`). Non-empty withholds the
    /// drain's certification: this pass then has no picture of the whole store, so it
    /// answers [`Reconciled::Blocked`].
    ///
    /// Deliberately **not** added to by two conditions this slice leaves exactly as the base
    /// answers them:
    ///
    /// * a **malformed** committed placement — skipped + NEEDS-HUMAN ([`emit_needs_human`],
    ///   ADR-0040 decision 4), and already blocked **cluster-wide** at the operator's own
    ///   drain query by [`crate::desired_state::ReconciliationStatus::PendingMalformed`],
    ///   which is a different surface from one loop's convergence answer.
    ///
    /// A move that did not persist ([`EvacOutcome::persisted`]) is **not** folded in here
    /// either — that is a property of one *move*, which the work loop above tracks itself,
    /// not of this scan of the namespace. Both withhold the same certification.
    contained: BTreeSet<Vec<u8>>,
}

/// Contain one committed object: name it on the durability seam the FIRST time this pass
/// meets it — a line per chunk floods the seam for exactly the multipart objects it names —
/// and record that the pass may not certify.
fn contain(contained: &mut BTreeSet<Vec<u8>>, key: &[u8], fault: &str) {
    if contained.insert(key.to_vec()) {
        emit_unresolvable(&crate::gc::object_name(key), fault);
    }
}

/// Scan the committed chunk maps for fragments sitting on a draining server, building
/// one [`EvacPlan`] per affected chunk.
///
/// **Staged multipart bytes are out of this scan by construction, and deliberately so**
/// (proposal 0016 decision 2, the rebalance row `0016:881`). The walk is a `scan(b"inode:")`
/// of the committed namespace: an upload's `mpu:` / `part:` / `sidx:` records are in other
/// namespaces, so no plan is ever built for a staged chunk and no `part:` record is ever
/// rewritten here. That is the design, not an omission — a staged fragment empties itself
/// within the session's own window (published, aborted, or reaped), and repointing a part
/// record from outside the session fence buys no durability while racing the upload that
/// owns it. Reconstruction is where a staged chunk's placement is rewritten, under the
/// session precondition (`0016:875`).
///
/// So for a draining server holding **only** staged fragments this pass plans nothing and
/// answers [`Reconciled::Satisfied`] — the honest answer to *its own* question, "is there
/// committed content left to evacuate". It is **not** the operator's drain verdict: that is
/// [`crate::desired_state::reconciliation_status`], which counts the staged class too and
/// answers `Pending` for exactly that server. The two are consistent only because the staged
/// class is disjoint from the committed reference set rather than merged into it
/// (`0016:767-782`, `:881`); merging them would have made this pass plan a move it must not
/// perform.
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
/// **Every move is conditioned on the generation the resolve ANSWERED FROM** — the record
/// this scan read, or the live one a retired resolution restarted onto — so the chunk a plan
/// is built from and the root its CAS pins come from the same generation. A generation that
/// moves on after that costs the move its CAS and nothing else (the fleet-wide version of
/// that question is deferred: #699).
///
/// Planning stays per *(object, chunk)*: two objects naming one `ChunkId` get one plan
/// each, so EVERY committed reference to a drained fragment is repointed. Deduplicating by
/// `ChunkId` (first reference wins) would leave the second object naming a server the drain
/// is emptying.
///
/// Attribution is emitted **per object, where the object is read** — and therefore before
/// the caller's work loop, mirroring `crate::gc::reconcile` — so a later transient store
/// fault cannot cost the operator the name of the record to repair.
async fn plan_evacuations(
    meta: &dyn MetadataStore,
    topology: &Topology,
    draining: &BTreeSet<DServerId>,
) -> Result<EvacScan> {
    let mut objects: Vec<EvacObject> = Vec::new();
    let mut plans = Vec::new();
    let mut contained = BTreeSet::new();
    for (key, value) in meta.scan(b"inode:").await? {
        // The record's own bytes are in hand, so a decode failure is THIS object's fault
        // and no store's — contained, and conservatively without first asking whether the
        // record was committed (reading `state` out of bytes that will not decode needs a
        // lenient peek this crate owns no decoder for; blocking until the record is
        // repaired is the fail-closed direction).
        let record: InodeRecord = match metadata::decode(&value) {
            Ok(record) => record,
            Err(fault) => {
                contain(&mut contained, &key, &fault.to_string());
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
                    contain(&mut contained, &key, &fault.to_string());
                    continue;
                }
                // Not a chunk-map anomaly: a store fault under the read. Not this object's
                // fault, so it is not folded into "this object is unreadable".
                Err(err) => return Err(err),
            },
        };
        let metadata::ResolvedChunkMap {
            record: prior,
            chunks,
        } = resolved;
        // A SEGMENTED record under a non-canonical spelling of its id (`inode:03`): the move
        // pins the root at the key re-derived from the id, which is not the row this scan
        // read, so every pass would copy the fragment, lose that CAS and never name the
        // object. Contained on the first chunk it owes instead, exactly as the reconstruction
        // pass contains it. The guard is this shape's alone: a flat record under such a key
        // is #698's, as it is there.
        let canonical =
            record.chunk_map.as_flat().is_some() || key == metadata::inode_key(inode_id);
        // The ONE snapshot of this object, taken on the first chunk it owes a move on and
        // shared by every later one; an object owing none is never copied.
        let mut object = None;
        // The chunk's address in the object's bytes, summed over the resolved list — the same
        // tiling the move walks. `None` once the lengths leave `u64`, which only a flat map
        // can reach (a segmented one is checked at decode).
        let mut next_offset = Some(0u64);
        for chunk in chunks.iter() {
            let at = next_offset;
            next_offset = at.and_then(|at| at.checked_add(chunk.len));
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
            if !canonical {
                let fault = "the row's key is not the canonical `inode:<id>` key";
                contain(&mut contained, &key, fault);
                break;
            }
            let Some(byte_offset) = at else {
                // No address the move could find this chunk by: contained and named, never
                // a conflict every pass with the drain stuck behind it.
                let fault = "the chunk lengths overflow the object's byte range";
                contain(&mut contained, &key, fault);
                break;
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
            let object = *object.get_or_insert_with(|| {
                objects.push(EvacObject {
                    inode_id,
                    prior: prior.as_ref().clone(),
                });
                objects.len() - 1
            });
            plans.push(EvacPlan {
                object,
                byte_offset,
                prior: chunk.clone(),
                chunk_id: chunk.id,
                placement,
                evac,
                survivor_domains,
            });
        }
    }
    Ok(EvacScan {
        objects,
        plans,
        contained,
    })
}

/// The outcome of evacuating one chunk.
enum EvacOutcome {
    /// The version-conditional commit landed; the fragment(s) were re-placed.
    Committed,
    /// The move lost its race: the record holding the chunk had already moved when the move
    /// was prepared (nothing was written, not even a fragment copy), or the commit itself
    /// lost the CAS (the copied fragments are left in place; nothing is retracted). Either
    /// way the fragment is still on the draining server and the next pass re-plans.
    Conflict,
    /// The move could not proceed — spread could not be preserved (no free distinct
    /// domain), or a fragment was missing / corrupt / off-fleet; nothing was committed.
    /// Every cause here is **transient**: it clears when a domain frees up, a server
    /// returns to the fleet view, or the fragment is reconstructed.
    Aborted,
    /// The repoint would have crossed the backend value ceiling
    /// ([`metadata::flat_value_ceiling_crossed`]), so it was refused before anything at all
    /// was written — no fragment copy, no record. Distinct from [`Self::Conflict`] and
    /// [`Self::Aborted`] precisely because those are transient: this shape fails again every
    /// pass until the record shrinks, so it is the object's own defect and an operator
    /// signal ([`emit_ceiling_refused`]).
    Refused {
        /// The re-encoded record's own length.
        bytes: usize,
        /// The ceiling it crossed.
        ceiling: usize,
    },
    /// The move found this committed object's own records unusable — a `seg:` record that
    /// is absent, torn, or disagrees with a root that still names it, or a flat record whose
    /// `version` cannot advance — so nothing was written. Not a race: the object is contained
    /// as one the scan could not read would be, and named once however many plans meet it
    /// ([`contain`]).
    Contained {
        /// What the move found.
        fault: String,
    },
}

impl EvacOutcome {
    /// Whether the move **persisted**. Only a landed commit did; a lost CAS, a ceiling
    /// refusal and an abort all left the fragment exactly where the drain found it.
    ///
    /// ONE rule, asked of the outcome itself rather than re-decided in each arm of the work
    /// loop, because the defect this closes is precisely a per-arm decision going missing: a
    /// pass that answered [`Reconciled::Satisfied`] over moves that never happened told an
    /// operator the box was safe to remove (`docs/principles.md` §5 C-1). A variant added
    /// later is non-certifying until it says otherwise here, instead of certifying silently
    /// by falling through — which is how [`Self::Aborted`] came to certify at all.
    fn persisted(&self) -> bool {
        matches!(self, Self::Committed)
    }
}

/// Evacuate `plan`'s fragment(s) off the draining server(s): copy each to a healthy
/// non-draining D server in a distinct failure domain, then repoint the chunk's
/// placement record with **one version-conditional commit**.
async fn evacuate_chunk(
    ctx: &RebalanceContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    object: &EvacObject,
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

    // Resolve every fragment this move would copy — its source and target stores, and its
    // intact bytes — WITHOUT writing any of them yet.
    //
    // Every failure in this loop is TRANSIENT (a server outside this pass's fleet view, a
    // fragment missing or checksum-failing on the draining server): the move aborts and is
    // re-assessed next pass. They are resolved BEFORE the ceiling refusal below so that a
    // move which could not have proceeded anyway is never reported as the permanent
    // "this record must shrink" defect — a compound failure is named by the recoverable
    // cause, not by the one that pages a human. The refusal still runs before any write.
    let mut new_placement = plan.placement.clone();
    let mut copies = Vec::new();
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
        if !repair::fragment_intact(&bytes, frag, plan.prior.scheme) {
            return Ok(EvacOutcome::Aborted);
        }
        copies.push((source, *target_store, frag, bytes));
        new_placement[index] = target;
    }

    // THE placement move the binding commit below is built from, prepared from the
    // selector's answer alone with no fragment written yet, so the move is judged before
    // anything is written. It addresses whichever record holds the chunk's `ChunkRef` — the
    // flat inode record or one `seg:` record — and pins three things: the root generation
    // the scan resolved, the holding `seg:` record as the move itself re-reads it (not the
    // bytes the scan's resolve saw), and the chunk's own reference. The orphan marks join
    // that same batch below, so ONE version-conditional mutation repoints the placement and
    // orphans the displaced fragments (`0005:298-299`, `0005:200-203`, ADR-0015): a racing
    // writer or superseded custodian loses the CAS rather than corrupting the record. That
    // re-read is bounded by the `MetadataStore` implementation (#508/#636), fail-closed.
    //
    // Every answer but `Prepared` WRITES NOTHING AT ALL, and each is judged ahead of the
    // fragment copies below. The value-ceiling refusal is weighed inside the move, on the
    // very bytes it would commit (`metadata::flat_value_ceiling_crossed`); see
    // `crates/core/src/metadata.rs:333-341` for what committing a record past it costs.
    let move_ = metadata::repoint_chunk(
        ctx.meta,
        object.inode_id,
        &object.prior,
        plan.byte_offset,
        &plan.prior,
        new_placement,
    )
    .await;
    let mut batch = match move_ {
        Ok(metadata::Repoint::Prepared(batch)) => batch,
        // The two terminal verdicts are weighed on the PLANNED generation alone — they carry
        // no batch, so no root pin tested it. On a generation the root has since left they are
        // a stale plan, the same retry a lost CAS is; only while it is still live are they the
        // object's own defect.
        Ok(metadata::Repoint::Refused { .. } | metadata::Repoint::VersionExhausted { .. })
            if !still_current(ctx.meta, object).await? =>
        {
            return Ok(EvacOutcome::Conflict)
        }
        Ok(metadata::Repoint::Refused { bytes, ceiling }) => {
            return Ok(EvacOutcome::Refused { bytes, ceiling })
        }
        Ok(metadata::Repoint::VersionExhausted { version }) => {
            return Ok(EvacOutcome::Contained {
                fault: format!("the record's version {version} cannot be advanced"),
            })
        }
        Ok(metadata::Repoint::Conflict) => return Ok(EvacOutcome::Conflict),
        // The move's typed verdict that THIS object's records cannot be rewritten, recovered
        // by downcast as the scan recovers the resolver's. Anything else — a store fault
        // under the read — is not this object's, and ends the pass.
        Err(err) => match err.downcast::<ChunkMapError>() {
            Ok(fault) => {
                return Ok(EvacOutcome::Contained {
                    fault: fault.to_string(),
                })
            }
            Err(err) => return Err(err),
        },
    };

    // Copy each evacuated fragment to its new home FIRST — before the commit, so a crash
    // here leaves only collectable garbage, never a torn chunk (`0005:298-299`).
    let mut displaced = Vec::new();
    for (source, target_store, frag, bytes) in copies {
        target_store.put_fragment(frag, bytes, None).await?;
        displaced.push((source, frag));
    }

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

/// Whether the object's root still holds exactly the generation the move was planned from —
/// the bytes [`metadata::repoint_chunk`]'s root pin would require, read fresh. A store fault
/// is the pass's, not the object's, and ends it; the read is bounded by the `MetadataStore`
/// implementation (#508/#636).
async fn still_current(meta: &dyn MetadataStore, object: &EvacObject) -> Result<bool> {
    let current = meta.get(&metadata::inode_key(object.inode_id)).await?;
    Ok(current.as_deref() == Some(&metadata::encode(&object.prior)[..]))
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

/// Emit a move **refused** because the repointed record would cross the backend value
/// ceiling ([`metadata::flat_value_ceiling_crossed`]) on the same seam: nothing at all was
/// written — not even a fragment copy — and the drain is not certified.
///
/// Distinct from [`emit_conflict`] and from a plain abort, which are transient and worth
/// retrying next pass: this chunk's object will refuse every move until its record shrinks,
/// so a drain waiting on it never converges on its own. That is the operator's signal — the
/// box cannot be emptied by waiting (`crates/core/src/metadata.rs:333-341`).
fn emit_ceiling_refused(chunk: ChunkId, bytes: usize, ceiling: usize) {
    tracing::warn!(monotonic_counter.rebalance_ceiling_refused = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.rebalance.audit",
        action = "refused-ceiling",
        chunk = %wyrd_traits::chunk_hex(chunk),
        bytes,
        ceiling,
        "rebalance refused a move whose repointed record would cross the backend value ceiling; NOTHING was written and this pass does not certify the drain — NEEDS-HUMAN: the object's record must shrink before the fragment can leave the draining server",
    );
}
