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
use wyrd_core::placement::{FailureDomain, Topology};
use wyrd_core::repair;
use wyrd_traits::{ChunkId, ChunkStore, DServerId, FragmentId, MetadataStore, Result};

use crate::desired_state;
use crate::premark;
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
    /// The [`wyrd_testkit::Clock`] seam (ADR-0024) every evacuation reads its whole lifecycle's
    /// time through (`crate::premark`): the stamp its destination pre-mark carries — read as
    /// the pre-mark batch is built, never the pass's start — the `W_repoint` gate it checks
    /// once before it authorizes all of a move's destination writes together, and the stamp of
    /// the mark it leaves on each position it vacates. The write's deadline is fixed from the
    /// pre-mark's own stamp ([`Self::write_window_millis`]), and the D server enforces it on the
    /// deployment clock this one reads.
    ///
    /// **One source with the pass's `now_millis`** (ADR-0009: one clock per correctness
    /// lifecycle), exactly as [`crate::ReconstructionContext::clock`]. Rebalance reads no other
    /// time — the `now_millis` [`crate::reconcile_step`] is handed is not consulted by this loop
    /// — so whoever builds the context backs it with the clock that reading comes from, and a
    /// test that passes a fixed instant puts a `wyrd_testkit::ManualClock` reading that instant
    /// here, never the wall clock beside it.
    pub clock: &'a (dyn wyrd_testkit::Clock + Sync),
    /// How long after an evacuation's pre-mark its destination write may still land — proposal
    /// 0016's `W_write` (`0016:1551-1576`), the quantity
    /// [`crate::ReconstructionContext::staged_write_window_millis`] carries for reconstruction,
    /// fed from this crate's own [`crate::gc::W_WRITE_MILLIS`] rather than a second definition.
    /// The evacuation sends every write with the deadline `pre-mark stamp + this`, which the D
    /// server refuses at or after (`wyrd_traits::WriteDeadlineExpired`). 0016 requires
    /// `G_orphan > W_repoint + W_write + δ_clock` (`0016:1348`), and #800's late-write deadline
    /// (`crate::gc::LATE_WRITE_DEADLINE_MILLIS`) must not be sized below it.
    pub write_window_millis: u64,
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

/// One rebalance reconciliation pass over `ctx`, its moves timed by the context's clock
/// ([`RebalanceContext::clock`]). Dispatched only from [`crate::reconcile_step`] (the fenced
/// control point) — never a parallel entry. Returns [`Reconciled::Blocked`] if the pass
/// contained a committed object it could not read or rewrite ([`EvacScan::contained`]), or a
/// planned move did **not** persist ([`EvacOutcome::persisted`]); [`Reconciled::Changed`] if
/// every planned move landed and at least one placement record was repointed; and
/// [`Reconciled::Satisfied`] only where reality already matched the desired state.
pub(crate) async fn reconcile(ctx: &RebalanceContext<'_>) -> Result<Reconciled> {
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
        let outcome = evacuate_chunk(ctx, &stores, object, plan, &draining_set).await?;
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
/// placement record with **one version-conditional commit**, by the destination-pre-mark rule
/// every committed move keeps ([`crate::premark`]). Its time is read from the context's clock
/// ([`RebalanceContext::clock`]).
async fn evacuate_chunk(
    ctx: &RebalanceContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    object: &EvacObject,
    plan: &EvacPlan,
    draining: &BTreeSet<DServerId>,
) -> Result<EvacOutcome> {
    // Select re-placement servers from the NON-draining pool, in domains distinct from
    // the survivors — so an evacuation never lands back on a draining server and never
    // collapses the chunk's spread. **Spread wins**: if no free distinct domain remains,
    // the selector refuses and the move is aborted (`0005:302-303`). A position whose mark is
    // already `reclaiming`, or is none of the three shapes, is passed over and named, and another
    // is chosen — the server may still take another of the fragments — with the same refusal
    // when no assignment is left ([`premark::choose`]).
    let pool = ctx.topology.excluding(draining);
    let admission =
        premark::Committed::new(ctx.meta, &pool, plan.chunk_id, emit_destination_passed_over);
    let Some(chosen) =
        premark::choose(&pool, &plan.survivor_domains, &plan.evac, &admission).await?
    else {
        return Ok(EvacOutcome::Aborted);
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
    let mut destinations = Vec::with_capacity(chosen.len());
    let mut copies = Vec::with_capacity(chosen.len());
    for (index, target, mark) in chosen {
        let source = plan.placement[index];
        let frag = FragmentId {
            chunk: plan.chunk_id,
            index: index as u16,
        };
        let (Some(source_store), Some(&target_store)) = (stores.get(&source), stores.get(&target))
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
        copies.push((target_store, frag, bytes));
        destinations.push(premark::Destination {
            dserver: target,
            frag,
            mark,
            vacated: source,
        });
        new_placement[index] = target;
    }

    // THE placement move the binding commit below is built from, prepared from the
    // selector's answer alone with no fragment written yet, so the move is judged before
    // anything is written. It addresses whichever record holds the chunk's `ChunkRef` — the
    // flat inode record or one `seg:` record — and pins three things: the root generation
    // the scan resolved, the holding `seg:` record as the move itself re-reads it (not the
    // bytes the scan's resolve saw), and the chunk's own reference. The adoption of each
    // pre-mark and the orphan marks join that same batch, so ONE version-conditional mutation
    // repoints the placement and orphans the displaced fragments (`0005:298-299`,
    // `0005:200-203`, ADR-0015): a racing writer or superseded custodian loses the CAS rather
    // than corrupting the record. That re-read is bounded by the `MetadataStore`
    // implementation (#508/#636), fail-closed.
    //
    // Every answer but `Prepared` WRITES NOTHING AT ALL, and each is judged ahead of the
    // pre-mark below. The value-ceiling refusal is weighed inside the move, on the
    // very bytes it would commit (`metadata::flat_value_ceiling_crossed`); see
    // `crates/core/src/metadata.rs:333-341` for what committing a record past it costs.
    let move_ = metadata::repoint_chunk(
        ctx.meta,
        object.inode_id,
        &object.prior,
        plan.byte_offset,
        &plan.prior,
        new_placement.clone(),
    )
    .await;
    let batch = match move_ {
        Ok(metadata::Repoint::Prepared(batch)) => batch,
        // The two terminal verdicts are weighed on the PLANNED generation alone — they carry
        // no batch, so no root pin tested it. On a generation the root has since left they are
        // a stale plan, the same retry a lost CAS is; only while it is still live are they the
        // object's own defect.
        Ok(metadata::Repoint::VersionExhausted { .. })
            if !still_current(ctx.meta, object).await? =>
        {
            return Ok(EvacOutcome::Conflict)
        }
        // A ceiling refusal is also weighed on the record the move READ, which for a
        // segmented object is a `seg:` record no root check covers.
        Ok(metadata::Repoint::Refused { bytes, ceiling }) => {
            return confirm_refusal(ctx, object, plan, new_placement, bytes, ceiling).await
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

    // The rest is the destination-pre-mark rule ([`premark`]): pre-mark, authorize, copy, and
    // adopt in the commit that repoints the placement. A crash at any point leaves only
    // collectable garbage — each copy under its pre-mark — never a torn chunk (`0005:298-299`).
    //
    // deferred: #682 — the seeded Tier-0 DST property for this move's interleavings (pre-mark,
    // copy and adoption against GC and a supersede); until it lands they are scripted in this
    // module's tests.
    //
    // PRE-MARK every destination before anything is copied. A lost precondition is a lost race
    // with nothing written: the drain is re-assessed next pass.
    let Some(premarked) =
        premark::premark(ctx.meta, ctx.clock, plan.chunk_id, &batch, &destinations).await?
    else {
        return Ok(EvacOutcome::Conflict);
    };
    // AUTHORIZE every copy at once under the `W_repoint` gate, then SEND them together, each
    // with the deadline the D server enforces. The fan-out is the write path's own
    // (`wyrd_core::write::write_fragments`): the futures are polled on this task, no task is
    // spawned, and every copy has answered before the move goes on. Each await is bounded as
    // every fragment write's is, by the D-server client's own request timeout set at composition
    // — the caller-side half of `W_write` (`crates/chunkstore-grpc/src/client.rs:264-271`).
    let Some(deadline) = premarked.authorize(ctx.clock, ctx.write_window_millis) else {
        emit_move_aborted(plan.chunk_id, "pre-mark-stale");
        return Ok(EvacOutcome::Aborted);
    };
    let sent = copies
        .into_iter()
        .map(|(store, frag, bytes)| store.put_fragment(frag, bytes, Some(deadline)));
    if let Some(reason) = premark::refusal(futures_util::future::join_all(sent).await)? {
        emit_move_aborted(plan.chunk_id, reason);
        return Ok(EvacOutcome::Aborted);
    }
    // ADOPT. Lost: the placement moved under us, and each copy stays under its pre-mark, which
    // GC reclaims after grace; the drain is re-assessed next pass.
    if !premarked
        .adopt(ctx.meta, ctx.clock, batch, &destinations)
        .await?
    {
        return Ok(EvacOutcome::Conflict);
    }
    emit_evacuated(plan.chunk_id, destinations.len());
    Ok(EvacOutcome::Committed)
}

/// Whether a ceiling refusal still stands, so it may be escalated as the object's own defect
/// ([`EvacOutcome::Refused`]) rather than retried as a stale plan ([`EvacOutcome::Conflict`]).
///
/// The refusal was weighed on the record the move read and carries no batch, so nothing pinned
/// that record. For a flat object that record IS the root, and [`still_current`] settles it.
/// For a segmented object it is a `seg:` record, which placement maintenance rewrites WITHOUT
/// moving the root: a sibling edit that shrank the segment after the move's read leaves the
/// root byte-identical, so the root check alone would page an operator over a record that
/// already fits. The move is therefore prepared again — a fresh read of the `seg:` record —
/// and only a second refusal, on a root still current after it, is escalated (with the second
/// read's numbers). Anything else is a stale plan: nothing has been written, and the next pass
/// re-plans from what is there. A store fault under either read is the pass's, and ends it.
async fn confirm_refusal(
    ctx: &RebalanceContext<'_>,
    object: &EvacObject,
    plan: &EvacPlan,
    placement: Vec<DServerId>,
    bytes: usize,
    ceiling: usize,
) -> Result<EvacOutcome> {
    let (bytes, ceiling) = if object.prior.chunk_map.as_flat().is_some() {
        (bytes, ceiling)
    } else {
        let again = metadata::repoint_chunk(
            ctx.meta,
            object.inode_id,
            &object.prior,
            plan.byte_offset,
            &plan.prior,
            placement,
        )
        .await;
        match again {
            Ok(metadata::Repoint::Refused { bytes, ceiling }) => (bytes, ceiling),
            Ok(_) => return Ok(EvacOutcome::Conflict),
            // The record changed into one the move cannot address: not the refusal this pass
            // saw. The next pass's scan reads it and contains it if it stays that way.
            Err(err) => match err.downcast::<ChunkMapError>() {
                Ok(_) => return Ok(EvacOutcome::Conflict),
                Err(err) => return Err(err),
            },
        }
    };
    if !still_current(ctx.meta, object).await? {
        return Ok(EvacOutcome::Conflict);
    }
    Ok(EvacOutcome::Refused { bytes, ceiling })
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
/// copied fragments are now collectable garbage, each under the pre-mark that evidences it — or
/// the move lost before it copied anything.
fn emit_conflict(chunk: ChunkId) {
    tracing::info!(monotonic_counter.rebalance_conflict = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.rebalance.audit",
        action = "conflict",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "rebalance lost the version-conditional commit; copied fragments are collectable garbage",
    );
}

/// Emit a destination position an evacuation will not write ([`premark::choose`]) on the same
/// seam: its `orphan:` mark is already `reclaiming` — GC is deleting whatever the position holds
/// — or is none of the three mark shapes, which NO writer overwrites (ADR-0045) and which is left
/// for a human byte for byte: NEEDS-HUMAN. Another position is chosen — the server may still take
/// another fragment — or, with none left, the move is aborted and the drain does not certify. The
/// same action strings reconstruction publishes for the same positions, so one grep finds both.
fn emit_destination_passed_over(
    dserver: DServerId,
    frag: FragmentId,
    unusable: &premark::Unusable,
) {
    let mark = crate::gc::object_name(&crate::gc::orphan_key(dserver, frag));
    match unusable {
        premark::Unusable::Reclaiming => tracing::info!(
            target: "wyrd.custodian.rebalance.audit",
            action = "destination-passed-over",
            reason = "reclaiming",
            subject = %mark,
            chunk = %wyrd_traits::chunk_hex(frag.chunk),
            "an evacuation passed over a destination GC is reclaiming; another is chosen, or the move waits for the next pass",
        ),
        premark::Unusable::Unreadable(fault) => {
            tracing::warn!(monotonic_counter.rebalance_unreadable_destination_marks = 1_u64);
            tracing::warn!(
                target: "wyrd.custodian.rebalance.audit",
                action = "unreadable-destination-mark",
                chunk = %wyrd_traits::chunk_hex(frag.chunk),
                mark = %mark,
                fault = %fault,
                "an evacuation passed over a destination whose orphan mark it cannot read; the mark is left in place and another position is chosen, or the move waits — NEEDS-HUMAN",
            );
        }
    }
}

/// Emit why an evacuation stopped after pre-marking its destinations and before adopting them
/// on the same seam: its pre-mark aged past `W_repoint` before the
/// copies were authorized (`pre-mark-stale`), or the D server refused a copy past its deadline
/// (`write-deadline-expired`) or could not certify it landed in time (`write-unverified`).
/// Every copy it wrote stays under its pre-mark, and the drain does not certify.
fn emit_move_aborted(chunk: ChunkId, reason: &'static str) {
    tracing::info!(
        target: "wyrd.custodian.rebalance.audit",
        action = "move-aborted",
        reason,
        chunk = %wyrd_traits::chunk_hex(chunk),
        "an evacuation stopped before adopting anything; every copy it wrote stays under its pre-mark, and the fragment stays on the draining server",
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

/// Issue #1084 (proposal 0016 X47, `0016:2587`; the pre-mark rule, `0016:1287-1312`; the write
/// gate, `0016:1341-1360`; the per-move nonce, `0016:1179-1189`) — **rebalance's committed
/// evacuation pre-marks its destination, gates its copy, and adopts the pre-mark in the commit
/// that repoints the chunk** ([`crate::premark`]).
///
/// The same legs as reconstruction's committed repair (`crates/custodian/tests/
/// move_destination_marks.rs`), driven the same way — through [`crate::reconcile_step`], over
/// `RedbMetadataStore::in_memory()` behind a wrapper that can hold or race a commit, and a
/// placement-aware chunk-store double that enforces `put_fragment`'s deadline on the test's own
/// clock, as a D server does — with a **library-level** [`RebalanceContext`]: the deployed loop
/// passes `rebalance: None` (`crates/server/src/custodian.rs`), and wiring it is not this
/// change's. One `ManualClock` backs every context's clock and every pass's `now_millis`. They
/// live here rather than beside the repair legs because they build a `RebalanceContext`, whose
/// clock and write-window fields this change adds: no added test file could hold them and still
/// compile on the base.
///
/// **Topology.** Four D servers, 2–5, in four distinct failure domains; the object — published by
/// a production multipart Complete — holds one RS(2,1) chunk on servers 3, 4 and 5 (fragment 2 on
/// server 5), flat or segmented; server 2 is free. Evacuation excludes the draining server and
/// the survivors' domains, so server 2 is the only legal destination for server 5's fragment.
///
/// Each leg runs on a flat and on a segmented object: **(D1)** a stale destination mark is gone
/// once the move commits, and the vacated position is marked as it always was; **(D2)** a
/// fragment drained 5 → 2 and, within grace, back 2 → 5 carries no mark on server 5; **(D3)**
/// after either, a production Complete's supersede leaves no fragment for GC to reclaim before
/// the retirement drain marks it, and all are gone after those marks' grace; **(D4)** a
/// `reclaiming` or unreadable destination is never written, and is named, and a vacated position
/// whose mark is `reclaiming` or unreadable keeps those exact bytes through the adoption; **(D5)**
/// GC's reclaim intent on the destination mid-move fails the adoption, released while the key
/// still holds `reclaiming` — GC held after its intent commit, or after its fragment delete;
/// **(D6)** a move that loses its commit leaves its copy reclaimable; **(D7a)** a pre-mark past
/// `W_repoint` authorizes no copy; **(D7b)** a copy held past its deadline is refused by the D
/// server; **(D7c)** a copy whose deadline passes inside the pre-mark's grace is refused and
/// adopts nothing, one a millisecond earlier is adopted; **(D7d)** a pre-mark exactly `W_repoint`
/// old authorizes no copy, one a millisecond younger does; **(D8)** two moves that pre-mark in one
/// millisecond never satisfy each other's adoption; and a copy whose landing the D server cannot
/// certify is never adopted. **(Swap)**, over five servers: a position a mark rules out rules out
/// only itself, so a chunk draining two fragments is moved in one pass by the two free servers
/// swapping them. These legs cannot compile without this change, so their reds are mutations of
/// the rule ([`crate::premark`]): each leg fails with the one check it pins removed or weakened.
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::fmt::Debug;
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::Notify;
    use tracing::field::{Field, Visit};
    use tracing::instrument::WithSubscriber;
    use tracing::Dispatch;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use wyrd_coordination_mem::MemCoordination;
    use wyrd_core::metadata::{
        self as md, decode_orphan_mark, encode_orphan_mark, orphan_key, ChunkRef, EcScheme,
        InodeId, InodeRecord, OrphanMark, SegmentNonce,
    };
    use wyrd_core::multipart::complete::{
        complete_upload, CompleteRequest, NamedParts, PublishRequest,
    };
    use wyrd_core::multipart::create::{create_session, CreateRequest, CreateRuntime};
    use wyrd_core::multipart::{
        decode_part_record, decode_part_summary, decode_retire_obligation, part_key, psum_key,
        Budget, CompleteOutcome, CreateOutcome, Digest, PartNumber, PartSummary, UploadId,
    };
    use wyrd_core::placement::Topology;
    use wyrd_core::write::encode_ec_fragment;
    use wyrd_core::{erasure, repair};
    use wyrd_metadata_redb::RedbMetadataStore;
    use wyrd_testkit::{Clock, ManualClock};
    use wyrd_traits::{
        ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, Health, MetadataStore,
        Result as StoreResult, WriteBatch, WriteDeadlineExpired,
    };

    use super::RebalanceContext;
    use crate::desired_state::{clear_lifecycle, set_lifecycle, DServerLifecycle};
    use crate::gc::{W_REPOINT_MILLIS, W_WRITE_MILLIS};
    use crate::{
        reconcile_step, Custodian, ExpiredPendingPolicy, FencedZone, GcContext, Reconciled,
        ReconstructionContext, Tombstone,
    };

    // ─────────────────────────────── fixtures ───────────────────────────────

    /// Where the clock starts.
    const T0: u64 = 10_000_000;
    /// The orphan grace every GC pass here honours.
    const GRACE: u64 = 60_000;
    /// `W_repoint` (`crate::gc::W_REPOINT_MILLIS`), stated here.
    const W_REPOINT: u64 = 10_000;
    const _: () = assert!(W_REPOINT == W_REPOINT_MILLIS);
    // A clock moved past the grace is past `W_repoint` and the write window too.
    const _: () = assert!(GRACE > W_REPOINT + W_WRITE_MILLIS);
    // The gate closes inside the write window, so a move held exactly at the gate would still
    // have its copy taken by the D server (D7d).
    const _: () = assert!(W_REPOINT < W_WRITE_MILLIS);
    const BUCKET: &str = "photos";
    const OBJECT: &str = "photos/moved";
    const ROOT: InodeId = 0;
    /// The inode the object is published under.
    const INODE: InodeId = 40;
    const MARKER: &[u8] = br#"{"name":"photos","created_at_millis":1}"#;
    const COMMITTED_AT: u64 = 1_500;
    const RS21: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };
    /// The chunk every leg moves.
    const CHUNK: ChunkId = 0xC0FF_EE00;
    const CHUNK_LEN: u64 = 64;
    /// The fragment index on [`SOURCE`].
    const INDEX: u16 = 2;
    /// The four D servers, each in its own failure domain.
    const SERVERS: [DServerId; 4] = [2, 3, 4, 5];
    /// The swap leg's five D servers: [`SERVERS`] and server 6, in a fifth domain.
    const WIDE: [DServerId; 5] = [2, 3, 4, 5, 6];
    /// The server the operator drains first.
    const SOURCE: DServerId = 5;
    /// The one server in a free domain: the only legal destination for [`SOURCE`]'s fragment.
    const FREE: DServerId = 2;
    /// A stale legacy mark, stamped long before the clock starts.
    const STALE: &[u8] = b"1000";
    /// A value that is none of the three mark shapes.
    const UNREADABLE: &[u8] = b"not a mark";
    /// First chunk id of a segmented object's filler chunks.
    const FILLER: ChunkId = 0x00F1_0000;
    /// The chunk the superseding Complete publishes.
    const REPLACEMENT: ChunkId = 0xBEEF_0000;
    /// The audit target the evacuation names what it did on.
    const AUDIT: &str = "wyrd.custodian.rebalance.audit";

    /// The committed object's shape.
    #[derive(Clone, Copy, Debug)]
    enum Shape {
        /// One part holding the moved chunk: a flat chunk map.
        Flat,
        /// Two parts of 100 chunks, past the flat map's 158: a segmented chunk map, the moved
        /// chunk first in segment 0. The filler chunks are single-copy chunks on server 3, which
        /// no drain moves.
        Segmented,
    }

    impl Shape {
        fn parts(self) -> Vec<(u32, Vec<ChunkRef>)> {
            match self {
                Shape::Flat => vec![(1, vec![moved_chunk()])],
                Shape::Segmented => {
                    let mut first = vec![moved_chunk()];
                    first.extend((0..99).map(filler));
                    vec![(1, first), (2, (99..199).map(filler).collect())]
                }
            }
        }
    }

    fn moved_chunk() -> ChunkRef {
        ChunkRef {
            id: CHUNK,
            scheme: RS21,
            len: CHUNK_LEN,
            placement: vec![3, 4, SOURCE],
        }
    }

    fn filler(n: u64) -> ChunkRef {
        ChunkRef {
            id: FILLER + ChunkId::from(n),
            scheme: EcScheme::None,
            len: CHUNK_LEN,
            placement: vec![3],
        }
    }

    /// The moved fragment's identity.
    fn moved() -> FragmentId {
        FragmentId {
            chunk: CHUNK,
            index: INDEX,
        }
    }

    /// Fragment `index` of the moved chunk, as the write path encodes it.
    fn fragment(index: u16) -> Bytes {
        let data: Vec<u8> = (0..CHUNK_LEN as u8).collect();
        let shards = erasure::encode(2, 1, &data).unwrap();
        encode_ec_fragment(CHUNK, index, 2, 1, &shards[usize::from(index)])
    }

    fn legacy(at: u64) -> Bytes {
        encode_orphan_mark(&OrphanMark::legacy(at))
    }

    fn reclaiming() -> Bytes {
        encode_orphan_mark(&OrphanMark::legacy(1_000).into_reclaiming())
    }

    fn name(key: &[u8]) -> String {
        String::from_utf8(key.to_vec()).unwrap()
    }

    fn upload(n: u64) -> UploadId {
        UploadId::new(format!("{n:032x}")).unwrap()
    }

    fn part(n: u32) -> PartNumber {
        PartNumber::new(n).unwrap()
    }

    fn digest_of(n: u32) -> Digest {
        let mut bytes = [0x3c; 32];
        bytes[..4].copy_from_slice(&n.to_be_bytes());
        Digest::from_bytes(bytes)
    }

    /// The puts planting part `n` of `id` over `chunks`: its `part:` record and `psum:` summary,
    /// each in its codec's own spelling.
    fn plant_part(id: &UploadId, n: u32, chunks: &[ChunkRef]) -> Vec<(Vec<u8>, Bytes)> {
        let digest = digest_of(n);
        let len: u64 = chunks.iter().map(|c| c.len).sum();
        let record = format!(
            "{{\"chunks\":{},\"len\":{len},\"digest\":\"{digest}\",\
             \"committed_at_millis\":{COMMITTED_AT},\"session_epoch\":0}}",
            String::from_utf8(md::encode(&chunks).to_vec()).unwrap()
        );
        let decoded = decode_part_record(record.as_bytes()).expect("a canonical part");
        assert_eq!(md::encode(&decoded), record.as_bytes());
        let summary = PartSummary {
            chunks: chunks.len() as u32,
            len,
            digest,
            committed_at_millis: COMMITTED_AT,
        };
        let psum = md::encode(&summary);
        assert_eq!(decode_part_summary(&psum).unwrap(), summary);
        vec![
            (part_key(id, part(n)), Bytes::from(record)),
            (psum_key(id, part(n)), psum),
        ]
    }

    /// Create's runtime: one fixed identity, no jitter, no wait.
    struct Fixed(Option<(UploadId, SegmentNonce)>);

    impl CreateRuntime for Fixed {
        fn mint(&mut self) -> (UploadId, SegmentNonce) {
            self.0.take().expect("one identity per create")
        }

        fn jitter(&mut self) -> u64 {
            0
        }

        fn wait(&mut self, _millis: u64) -> impl Future<Output = ()> + Send {
            std::future::ready(())
        }
    }

    /// A profile admitting sixteen sessions; Complete reads no other cap of it.
    fn profile() -> Budget {
        let json = "{\"w_ref\":16,\"max_part_chunks\":1,\"max_parts_per_session\":1,\
                    \"max_inflight_parts\":1,\"max_staged_chunks\":1}";
        md::decode(json.as_bytes()).expect("a legal profile")
    }

    // ─────────────────────────── one move's interleavings ───────────────────────────

    /// A point a move is stopped at until the test releases it.
    #[derive(Clone, Default)]
    struct Hold {
        reached: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Hold {
        async fn wait(&self) {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }

    /// Run `pass` until it reaches `hold`, then `meanwhile`, then release it; `true` if the hold
    /// was reached at all. A pass that never reaches it simply finishes, so a move that skips the
    /// step the hold sits on fails its leg instead of hanging it.
    async fn held<T>(
        hold: &Hold,
        pass: impl Future<Output = T>,
        meanwhile: impl Future<Output = ()>,
    ) -> (T, bool) {
        let done = Notify::new();
        let pass = async {
            let out = pass.await;
            done.notify_one();
            out
        };
        let orchestrate = async {
            tokio::select! {
                () = hold.reached.notified() => {
                    meanwhile.await;
                    hold.release.notify_one();
                    true
                }
                () = done.notified() => false,
            }
        };
        tokio::join!(pass, orchestrate)
    }

    /// A pass run alongside the leg's script, which can wait for it to finish.
    struct Running {
        done: Notify,
    }

    impl Running {
        fn new() -> Self {
            Self {
                done: Notify::new(),
            }
        }

        /// `pass`, telling [`Self::finished`] when it ends.
        async fn run<T>(&self, pass: impl Future<Output = T>) -> T {
            let out = pass.await;
            self.done.notify_one();
            out
        }

        /// Wait for `hold`, failing the leg if the pass ends without reaching it.
        async fn reaches(&self, hold: &Hold, what: &str) {
            tokio::select! {
                () = hold.reached.notified() => {}
                () = self.done.notified() => panic!("{what}"),
            }
        }

        async fn finished(&self) {
            self.done.notified().await;
        }
    }

    /// What the metadata wrapper does with a commit a hook matches.
    enum Act {
        /// Hold the commit before it is applied.
        Before(Hold),
        /// Hold the commit once it has been applied.
        After(Hold),
        /// Apply these puts first — another writer that got there first — then the commit.
        PlantFirst(Vec<(Vec<u8>, Bytes)>),
    }

    struct Hook {
        when: fn(&WriteBatch) -> bool,
        act: Act,
    }

    /// The commit that rewrites the object's placement record — the move's last.
    fn repoints(batch: &WriteBatch) -> bool {
        batch
            .puts
            .iter()
            .any(|(key, _)| key.starts_with(b"inode:") || key.starts_with(b"seg:"))
    }

    /// Whether `batch` puts at the moved fragment's position on [`FREE`] a mark `matches`
    /// accepts.
    fn marks_free(batch: &WriteBatch, matches: fn(&OrphanMark) -> bool) -> bool {
        let position = orphan_key(FREE, moved());
        batch.puts.iter().any(|(key, value)| {
            *key == position && decode_orphan_mark(value).is_ok_and(|mark| matches(&mark))
        })
    }

    /// A commit that pre-marks the free server's position and rewrites no placement record: a
    /// destination pre-mark, a structured mark naming a move.
    fn premarks_free(batch: &WriteBatch) -> bool {
        !repoints(batch)
            && marks_free(batch, |mark| {
                !mark.is_reclaiming() && mark.event().is_some_and(|e| e.starts_with("repoint:"))
            })
    }

    /// GC's reclaim-intent commit on the free server's position: its mark swapped to
    /// `reclaiming`.
    fn reclaims_free(batch: &WriteBatch) -> bool {
        marks_free(batch, OrphanMark::is_reclaiming)
    }

    /// `RedbMetadataStore` behind a wrapper that acts once on the first commit each armed hook
    /// matches, in the order they were armed.
    struct Store {
        inner: RedbMetadataStore,
        hooks: Mutex<Vec<Hook>>,
    }

    impl Store {
        fn new() -> Self {
            Self {
                inner: RedbMetadataStore::in_memory().unwrap(),
                hooks: Mutex::new(Vec::new()),
            }
        }

        fn arm(&self, when: fn(&WriteBatch) -> bool, act: Act) {
            self.hooks.lock().unwrap().push(Hook { when, act });
        }

        /// Fixture writes, straight to the inner store.
        async fn plant(&self, puts: Vec<(Vec<u8>, Bytes)>) {
            for chunk in puts.chunks(400) {
                let mut batch = WriteBatch::new();
                batch.puts.extend(chunk.iter().cloned());
                assert_eq!(
                    self.inner.commit(batch).await.unwrap(),
                    CommitOutcome::Committed
                );
            }
        }

        async fn value(&self, key: &[u8]) -> Option<Bytes> {
            self.inner.get(key).await.unwrap()
        }
    }

    #[async_trait]
    impl MetadataStore for Store {
        async fn get(&self, key: &[u8]) -> StoreResult<Option<Bytes>> {
            self.inner.get(key).await
        }

        async fn scan(&self, prefix: &[u8]) -> StoreResult<Vec<(Vec<u8>, Bytes)>> {
            self.inner.scan(prefix).await
        }

        async fn scan_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> StoreResult<wyrd_traits::ScanPage> {
            self.inner.scan_page(prefix, after, limit).await
        }

        async fn commit(&self, batch: WriteBatch) -> StoreResult<CommitOutcome> {
            let act = {
                let mut hooks = self.hooks.lock().unwrap();
                let at = hooks.iter().position(|hook| (hook.when)(&batch));
                at.map(|at| hooks.remove(at).act)
            };
            match act {
                None => self.inner.commit(batch).await,
                Some(Act::Before(hold)) => {
                    hold.wait().await;
                    self.inner.commit(batch).await
                }
                Some(Act::After(hold)) => {
                    let landed = self.inner.commit(batch).await;
                    hold.wait().await;
                    landed
                }
                Some(Act::PlantFirst(puts)) => {
                    self.plant(puts).await;
                    self.inner.commit(batch).await
                }
            }
        }
    }

    // ─────────────────────────────── the D servers ───────────────────────────────

    #[derive(Default)]
    struct FleetState {
        held: BTreeMap<DServerId, HashMap<FragmentId, Bytes>>,
        /// Every fragment write a D server was sent, accepted or refused.
        sent: Vec<(DServerId, FragmentId)>,
        /// One write to hold in flight, before the D server judges its deadline.
        hold: Option<((DServerId, FragmentId), Hold)>,
        /// One write to hold once its bytes are published, before the D server reads its clock
        /// again to certify the landing.
        hold_published: Option<((DServerId, FragmentId), Hold)>,
        /// One delete to hold once its bytes are gone.
        hold_deleted: Option<((DServerId, FragmentId), Hold)>,
    }

    /// In-process D servers addressed by id. Each judges a write's deadline on the test's clock,
    /// as a D server judges it on its own: immediately before it stores the bytes
    /// (`WriteDeadlineExpired::if_elapsed`), and once more right after, refusing to certify a
    /// landing its clock then reads at or past the deadline
    /// (`WriteDeadlineExpired::if_publication_unverified`, the bytes left where they landed).
    #[derive(Clone)]
    struct Fleet {
        state: Arc<Mutex<FleetState>>,
        clock: ManualClock,
    }

    impl Fleet {
        fn new(clock: ManualClock) -> Self {
            let mut state = FleetState::default();
            for id in SERVERS {
                state.held.insert(id, HashMap::new());
            }
            Self {
                state: Arc::new(Mutex::new(state)),
                clock,
            }
        }

        fn put(&self, dserver: DServerId, frag: FragmentId, bytes: Bytes) {
            let mut state = self.state.lock().unwrap();
            state.held.entry(dserver).or_default().insert(frag, bytes);
        }

        fn take(&self, dserver: DServerId, frag: FragmentId) -> Option<Bytes> {
            let mut state = self.state.lock().unwrap();
            state.held.get_mut(&dserver)?.remove(&frag)
        }

        fn holds(&self, dserver: DServerId, frag: FragmentId) -> Option<Bytes> {
            let state = self.state.lock().unwrap();
            state.held.get(&dserver)?.get(&frag).cloned()
        }

        fn listed(&self, dserver: DServerId) -> Vec<FragmentId> {
            let state = self.state.lock().unwrap();
            let held = state.held.get(&dserver);
            held.map(|held| held.keys().copied().collect())
                .unwrap_or_default()
        }

        /// The writes `dserver` was sent so far.
        fn sent_to(&self, dserver: DServerId) -> Vec<FragmentId> {
            let state = self.state.lock().unwrap();
            state
                .sent
                .iter()
                .filter(|(to, _)| *to == dserver)
                .map(|(_, frag)| *frag)
                .collect()
        }

        /// Hold the next write of `frag` to `dserver` in flight.
        fn hold_write(&self, dserver: DServerId, frag: FragmentId) -> Hold {
            let hold = Hold::default();
            self.state.lock().unwrap().hold = Some(((dserver, frag), hold.clone()));
            hold
        }

        /// Hold the next write of `frag` to `dserver` once its bytes are published.
        fn hold_published(&self, dserver: DServerId, frag: FragmentId) -> Hold {
            let hold = Hold::default();
            self.state.lock().unwrap().hold_published = Some(((dserver, frag), hold.clone()));
            hold
        }

        /// Hold the next delete of `frag` on `dserver` once its bytes are gone.
        fn hold_delete(&self, dserver: DServerId, frag: FragmentId) -> Hold {
            let hold = Hold::default();
            self.state.lock().unwrap().hold_deleted = Some(((dserver, frag), hold.clone()));
            hold
        }
    }

    /// Take `slot`'s hold if it is armed at `at`.
    fn take_hold(
        slot: &mut Option<((DServerId, FragmentId), Hold)>,
        at: (DServerId, FragmentId),
    ) -> Option<Hold> {
        match slot.as_ref() {
            Some((held, _)) if *held == at => slot.take().map(|(_, hold)| hold),
            _ => None,
        }
    }

    /// One D server of a [`Fleet`].
    struct Server {
        id: DServerId,
        fleet: Fleet,
    }

    #[async_trait]
    impl ChunkStore for Server {
        async fn put_fragment(
            &self,
            id: FragmentId,
            bytes: Bytes,
            deadline_millis: Option<u64>,
        ) -> StoreResult<()> {
            let (hold, published) = {
                let mut state = self.fleet.state.lock().unwrap();
                state.sent.push((self.id, id));
                let at = (self.id, id);
                (
                    take_hold(&mut state.hold, at),
                    take_hold(&mut state.hold_published, at),
                )
            };
            if let Some(hold) = hold {
                hold.wait().await;
            }
            if let Some(deadline) = deadline_millis {
                let now = self.fleet.clock.now_millis();
                if let Some(refusal) = WriteDeadlineExpired::if_elapsed(id, deadline, now) {
                    return Err(Box::new(refusal));
                }
            }
            self.fleet.put(self.id, id, bytes);
            if let Some(hold) = published {
                hold.wait().await;
            }
            if let Some(deadline) = deadline_millis {
                let now = self.fleet.clock.now_millis();
                if let Some(unverified) =
                    WriteDeadlineExpired::if_publication_unverified(id, deadline, now)
                {
                    return Err(Box::new(unverified));
                }
            }
            Ok(())
        }

        async fn get_fragment(&self, id: FragmentId) -> StoreResult<Option<Bytes>> {
            Ok(self.fleet.holds(self.id, id))
        }

        async fn list_fragments(&self) -> StoreResult<Vec<FragmentId>> {
            Ok(self.fleet.listed(self.id))
        }

        async fn delete_fragment(&self, id: FragmentId) -> StoreResult<()> {
            self.fleet.take(self.id, id);
            let hold = take_hold(
                &mut self.fleet.state.lock().unwrap().hold_deleted,
                (self.id, id),
            );
            if let Some(hold) = hold {
                hold.wait().await;
            }
            Ok(())
        }

        async fn health(&self) -> StoreResult<Health> {
            Ok(Health::Healthy)
        }
    }

    // ─────────────────────────────── the audit seam ───────────────────────────────

    /// The evacuation's audit events, captured by a subscriber attached to each pass's future.
    #[derive(Clone, Default)]
    struct Audit(Arc<Mutex<Vec<HashMap<String, String>>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Audit {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() == AUDIT {
                let mut fields = FieldText(HashMap::new());
                event.record(&mut fields);
                self.0.lock().unwrap().push(fields.0);
            }
        }
    }

    struct FieldText(HashMap<String, String>);

    impl Visit for FieldText {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    impl Audit {
        fn dispatch(&self) -> Dispatch {
            Dispatch::new(tracing_subscriber::registry().with(self.clone()))
        }

        /// Whether an event of `action` names `subject` under `field`.
        fn names(&self, action: &str, field: &str, subject: &str) -> bool {
            self.0.lock().unwrap().iter().any(|event| {
                event.get("action").map(String::as_str) == Some(action)
                    && event.get(field).map(String::as_str) == Some(subject)
            })
        }
    }

    // ─────────────────────────────── the rig ───────────────────────────────

    /// One store, one fleet, one clock — every pass's one time source — and the audit seam.
    struct Rig {
        store: Store,
        fleet: Fleet,
        clock: ManualClock,
        audit: Audit,
        /// The D servers every pass runs over, each in its own failure domain.
        servers: &'static [DServerId],
    }

    async fn elect() -> (FencedZone, Custodian) {
        let coord = MemCoordination::new();
        let custodian = Custodian::elect(&coord, "zone-evacuation-destination-marks")
            .await
            .expect("leader election over the in-memory coordination seam");
        let mut zone = FencedZone::new();
        zone.install(custodian.leadership());
        (zone, custodian)
    }

    fn topology(servers: &[DServerId]) -> Topology {
        let mut topology = Topology::default();
        for &id in servers {
            topology.register(id, format!("d{id}"));
        }
        topology
    }

    impl Rig {
        /// The object of `shape` published by a production Complete, its moved chunk's
        /// fragments on servers 3, 4 and 5 and its filler chunks' on server 3, and the
        /// session's own records drained, so the committed map alone names the chunk.
        async fn new(shape: Shape) -> Self {
            Self::over(shape, &SERVERS).await
        }

        /// [`Self::new`] with every pass over `servers`.
        async fn over(shape: Shape, servers: &'static [DServerId]) -> Self {
            let clock = ManualClock::new(T0);
            let rig = Self {
                store: Store::new(),
                fleet: Fleet::new(clock.clone()),
                clock,
                audit: Audit::default(),
                servers,
            };
            rig.store
                .plant(vec![(md::bucket_key(BUCKET), Bytes::from_static(MARKER))])
                .await;
            let parts = shape.parts();
            let id = rig.session(1).await;
            let mut puts = Vec::new();
            for (n, chunks) in &parts {
                puts.extend(plant_part(&id, *n, chunks));
                for chunk in chunks {
                    for (index, dserver) in chunk.fragments() {
                        let frag = FragmentId {
                            chunk: chunk.id,
                            index,
                        };
                        let bytes = if chunk.id == CHUNK {
                            fragment(index)
                        } else {
                            Bytes::from(format!("{frag:?}"))
                        };
                        rig.fleet.put(dserver, frag, bytes);
                    }
                }
            }
            rig.store.plant(puts).await;
            let named: Vec<u32> = parts.iter().map(|(n, _)| *n).collect();
            rig.complete(&id, &named, INODE).await;
            let flat = rig.root().await.chunk_map.as_flat().is_some();
            assert_eq!(flat, matches!(shape, Shape::Flat), "the object's shape");
            rig.settle().await;
            rig
        }

        /// A session made by Create, numbered `n`, targeting the object.
        async fn session(&self, n: u64) -> UploadId {
            let request = CreateRequest {
                bucket: BUCKET,
                parent: ROOT,
                object: OBJECT,
                content_type: None,
                created_at_millis: 1_000,
                clock_source: "wall",
            };
            let nonce = SegmentNonce::new(format!("{:032x}", 0x5e60_0000 + n)).unwrap();
            let created = create_session(
                &self.store.inner,
                &profile(),
                &request,
                &mut Fixed(Some((upload(n), nonce))),
            )
            .await
            .unwrap();
            let CreateOutcome::Created { upload_id } = created else {
                panic!("expected Created, got {created:?}");
            };
            upload_id
        }

        /// A production Complete of `id` naming `named`, publishing under `inode` when the name
        /// is free and superseding the object when it is not.
        async fn complete(&self, id: &UploadId, named: &[u32], inode: InodeId) {
            let list = NamedParts::new(named.iter().map(|&n| (part(n), digest_of(n))).collect());
            let request = CompleteRequest {
                bucket: BUCKET,
                parent: ROOT,
                object: OBJECT,
                parts: &list.unwrap(),
                fenced_at_millis: 2_000,
                clock_source: "wall",
            };
            let publish = PublishRequest {
                fresh_inode: inode,
                completed_at_millis: 3_000,
            };
            let outcome = complete_upload(&self.store.inner, id, &request, &publish).await;
            assert!(
                matches!(outcome, Ok(CompleteOutcome::Published(_))),
                "{outcome:?}"
            );
        }

        /// Drain passes until the `retire:` ledger is empty.
        async fn settle(&self) {
            for _ in 0..8 {
                if self.store.inner.scan(b"retire:").await.unwrap().is_empty() {
                    return;
                }
                self.drain().await;
            }
            panic!("the retire: ledger did not drain");
        }

        fn servers(&self) -> Vec<Server> {
            self.servers
                .iter()
                .map(|&id| Server {
                    id,
                    fleet: self.fleet.clone(),
                })
                .collect()
        }

        /// One fenced rebalance pass over every D server, with a library-level
        /// [`RebalanceContext`] whose clock is the pass's `now_millis`'s own source and whose
        /// write window is the deployed one.
        async fn evacuate(&self) -> Reconciled {
            let (zone, custodian) = elect().await;
            let servers = self.servers();
            let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
                .iter()
                .map(|s| (s.id, s as &dyn ChunkStore))
                .collect();
            let topology = topology(self.servers);
            let ctx = RebalanceContext {
                meta: &self.store,
                fleet: &fleet,
                topology: &topology,
                clock: &self.clock,
                write_window_millis: W_WRITE_MILLIS,
            };
            let now = self.clock.now_millis();
            reconcile_step(&zone, &custodian, None, None, None, Some(&ctx), None, now)
                .with_subscriber(self.audit.dispatch())
                .await
                .expect("the rebalance pass never fails")
        }

        /// One fenced reconstruction pass — the retirement leg runs on it — over every D server,
        /// its context built as the deployed loop builds it. Nothing is owed a repair here.
        async fn drain(&self) -> Reconciled {
            let (zone, custodian) = elect().await;
            let servers = self.servers();
            let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
                .iter()
                .map(|s| (s.id, s as &dyn ChunkStore))
                .collect();
            let topology = topology(self.servers);
            let ctx = ReconstructionContext {
                meta: &self.store,
                fleet: &fleet,
                topology: &topology,
                unreachable: &[],
                clock: &self.clock,
                staged_write_window_millis: W_WRITE_MILLIS,
                operator_fleet_size: fleet.len(),
                tombstone: Tombstone::Retain,
            };
            let now = self.clock.now_millis();
            reconcile_step(&zone, &custodian, None, None, Some(&ctx), None, None, now)
                .await
                .expect("the reconstruction pass never fails")
        }

        /// One fenced GC pass over every D server at the clock's reading.
        async fn gc(&self) -> Reconciled {
            let (zone, custodian) = elect().await;
            let servers = self.servers();
            let fleet: Vec<(DServerId, &dyn ChunkStore)> = servers
                .iter()
                .map(|s| (s.id, s as &dyn ChunkStore))
                .collect();
            let ctx = GcContext {
                meta: &self.store,
                fleet: &fleet,
                grace_window_millis: GRACE,
                expired_pending: ExpiredPendingPolicy::Defer,
            };
            let now = self.clock.now_millis();
            reconcile_step(&zone, &custodian, Some(&ctx), None, None, None, None, now)
                .await
                .expect("the GC pass never fails")
        }

        /// GC passes until one changes nothing.
        async fn gc_until_quiet(&self) {
            for _ in 0..6 {
                if self.gc().await != Reconciled::Changed {
                    return;
                }
            }
            panic!("GC never settled");
        }

        async fn drain_server(&self, dserver: DServerId) {
            set_lifecycle(&self.store, dserver, DServerLifecycle::Draining)
                .await
                .unwrap();
        }

        async fn undrain_server(&self, dserver: DServerId) {
            clear_lifecycle(&self.store, dserver).await.unwrap();
        }

        async fn root(&self) -> InodeRecord {
            let bytes = self.store.value(&md::inode_key(INODE)).await.unwrap();
            md::decode(&bytes).unwrap()
        }

        /// The live generation's chunks, through the one resolver every consumer reads it by.
        async fn chunks(&self) -> Vec<ChunkRef> {
            let root = self.root().await;
            let key = md::inode_key(INODE);
            let resolved = md::resolve_chunk_map(&self.store.inner, &key, &root)
                .await
                .unwrap()
                .expect("the object is live");
            resolved.chunks.into_owned()
        }

        /// The moved chunk's placement, as the live generation names it.
        async fn placement(&self) -> Vec<DServerId> {
            let chunks = self.chunks().await;
            let chunk = chunks.iter().find(|c| c.id == CHUNK).expect("the chunk");
            chunk.placement.clone()
        }

        /// Every position the live generation names.
        async fn positions(&self) -> Vec<(DServerId, FragmentId)> {
            let mut positions = Vec::new();
            for chunk in self.chunks().await {
                for (index, dserver) in chunk.fragments() {
                    let frag = FragmentId {
                        chunk: chunk.id,
                        index,
                    };
                    positions.push((dserver, frag));
                }
            }
            positions
        }

        /// The mark at the moved fragment's position on `dserver`.
        async fn mark(&self, dserver: DServerId) -> Option<Bytes> {
            self.store.value(&orphan_key(dserver, moved())).await
        }

        async fn plant_mark(&self, dserver: DServerId, value: &[u8]) {
            self.plant_mark_at(dserver, moved(), value).await;
        }

        async fn plant_mark_at(&self, dserver: DServerId, frag: FragmentId, value: &[u8]) {
            let key = orphan_key(dserver, frag);
            self.store
                .plant(vec![(key, Bytes::copy_from_slice(value))])
                .await;
        }

        /// The stamp of a move's own pre-mark at the moved fragment's position on `dserver`: a
        /// structured, non-`reclaiming` mark whose event names a move of the chunk.
        async fn premark_stamp(&self, dserver: DServerId) -> u64 {
            let stored = self.mark(dserver).await.expect("the move's pre-mark");
            let mark = decode_orphan_mark(&stored).expect("one of the three shapes");
            let prefix = format!("repoint:{}:", wyrd_traits::chunk_hex(CHUNK));
            assert!(
                mark.event().is_some_and(|event| event.starts_with(&prefix))
                    && !mark.is_reclaiming(),
                "not a move's own pre-mark: {mark:?}"
            );
            mark.orphaned_at_millis()
        }

        /// No position the live generation names carries a mark.
        async fn assert_no_mark_on_placement(&self) {
            for (dserver, frag) in self.positions().await {
                let mark = self.store.value(&orphan_key(dserver, frag)).await;
                assert!(
                    mark.is_none(),
                    "{frag:?} on {dserver} is named by the placement and marked {mark:?}"
                );
            }
        }

        /// Every position the live generation names holds its fragment, and the moved chunk's
        /// hold intact ones.
        async fn assert_complete(&self) {
            for (dserver, frag) in self.positions().await {
                let held = self.fleet.holds(dserver, frag);
                let held =
                    held.unwrap_or_else(|| panic!("the placement names {frag:?} on {dserver}"));
                if frag.chunk == CHUNK {
                    assert!(repair::fragment_intact(&held, frag, RS21), "{frag:?}");
                }
            }
        }

        /// No fragment on `dserver` lacks both a reference and a mark.
        async fn assert_no_stray(&self, dserver: DServerId) {
            let named = self.positions().await;
            for frag in self.fleet.listed(dserver) {
                let marked = self.store.value(&orphan_key(dserver, frag)).await.is_some();
                assert!(
                    marked || named.contains(&(dserver, frag)),
                    "{frag:?} on {dserver} is neither referenced nor marked: GC keeps it for ever"
                );
            }
        }

        /// The move did not adopt: the placement still names the draining server, and every
        /// position it names holds its fragment.
        async fn assert_not_adopted(&self) {
            assert_eq!(
                self.placement().await[usize::from(INDEX)],
                SOURCE,
                "the move adopted a destination it may not publish"
            );
            self.assert_complete().await;
        }

        /// A later pass moves the chunk onto server 2, whole and unmarked.
        async fn assert_moved_later(&self) {
            assert_eq!(self.evacuate().await, Reconciled::Changed);
            assert_eq!(self.placement().await, vec![3, 4, FREE]);
            self.assert_no_mark_on_placement().await;
            self.assert_complete().await;
        }

        /// A sibling's repoint in the segmented object's first `seg:` record, the one holding
        /// the moved chunk: the record's key, and its bytes with the first filler chunk moved
        /// from server 3 to server 4 — where this also moves its fragment. The root is not
        /// touched.
        async fn sibling_repoint(&self) -> (Vec<u8>, Bytes) {
            let root = self.root().await;
            let map = root.chunk_map.segmented().expect("a segmented object");
            let key = md::seg_key(map.group(), map.segments()[0].index).unwrap();
            let stored = self.store.value(&key).await.expect("the `seg:` record");
            let record: md::SegmentRecord = md::decode(&stored).unwrap();
            let offset = record.byte_offset();
            let mut chunks = record.into_chunks();
            assert_eq!((chunks[0].id, &chunks[1].placement), (CHUNK, &vec![3]));
            chunks[1].placement = vec![4];
            let sibling = FragmentId {
                chunk: chunks[1].id,
                index: 0,
            };
            let bytes = self.fleet.take(3, sibling).expect("the sibling's fragment");
            self.fleet.put(4, sibling, bytes);
            let rewritten = md::SegmentRecord::new(chunks, offset).unwrap();
            (key, md::encode(&rewritten))
        }

        /// A second session's Complete over the same name supersedes the object: the key of the
        /// `retire:bytes:` obligation it installs for the superseded generation, and that
        /// obligation's event.
        async fn supersede(&self) -> (Vec<u8>, String) {
            let id = self.session(2).await;
            let replacement = ChunkRef {
                id: REPLACEMENT,
                scheme: RS21,
                len: CHUNK_LEN,
                placement: vec![2, 3, 4],
            };
            for (index, dserver) in replacement.fragments() {
                let frag = FragmentId {
                    chunk: REPLACEMENT,
                    index,
                };
                self.fleet
                    .put(dserver, frag, Bytes::from(format!("{frag:?}")));
            }
            self.store
                .plant(plant_part(&id, 1, std::slice::from_ref(&replacement)))
                .await;
            self.complete(&id, &[1], INODE + 1).await;
            let generations = self.store.inner.scan(b"retire:bytes:g:").await.unwrap();
            assert_eq!(
                generations.len(),
                1,
                "the superseded generation's obligation"
            );
            let (key, value) = generations.into_iter().next().unwrap();
            let (_, token, _) = decode_retire_obligation(&key, &value).unwrap();
            (key, token.to_string())
        }
    }

    /// One leg as two tests, on a flat and on a segmented object.
    macro_rules! on_both_shapes {
        ($leg:ident => $flat:ident, $segmented:ident) => {
            #[tokio::test]
            async fn $flat() {
                $leg(Shape::Flat).await;
            }

            #[tokio::test]
            async fn $segmented() {
                $leg(Shape::Segmented).await;
            }
        };
    }

    // ─────────────────────────────── (D1) ───────────────────────────────

    /// The (D1) move: server 2 carries a stale legacy mark at the moved fragment's position, and
    /// draining server 5 evacuates the fragment onto server 2 — the move alone, so (D3) can run
    /// after it. The rig, and the instant the move ran at.
    async fn d1_moved(shape: Shape) -> (Rig, u64) {
        let rig = Rig::new(shape).await;
        rig.plant_mark(FREE, STALE).await;
        rig.drain_server(SOURCE).await;
        let at = rig.clock.now_millis();
        assert_eq!(rig.evacuate().await, Reconciled::Changed);
        assert_eq!(
            rig.placement().await,
            vec![3, 4, FREE],
            "moved onto server 2"
        );
        (rig, at)
    }

    /// **(D1)** After the (D1) move no position the new placement names carries a mark, and the
    /// vacated position on server 5 carries the legacy mark stamped at the move, as it always
    /// has.
    async fn d1(shape: Shape) {
        let (rig, at) = d1_moved(shape).await;

        rig.assert_no_mark_on_placement().await;
        assert_eq!(
            rig.mark(SOURCE).await,
            Some(legacy(at)),
            "the vacated position carries its mark as it always has"
        );
        rig.assert_complete().await;
    }

    on_both_shapes!(d1 =>
        d1_a_stale_destination_mark_is_gone_once_a_flat_evacuation_commits,
        d1_a_stale_destination_mark_is_gone_once_a_segmented_evacuation_commits);

    // ─────────────────────────────── (D2) ───────────────────────────────

    /// The (D2) moves, the round trip: draining server 5 moves the fragment onto server 2; within
    /// grace the operator drains server 2 and returns server 5, which moves it back — the moves
    /// alone, so (D3) can run after them. The rig, and the instants the two moves ran at.
    async fn d2_moved(shape: Shape) -> (Rig, u64, u64) {
        let rig = Rig::new(shape).await;
        rig.drain_server(SOURCE).await;
        let first = rig.clock.now_millis();
        assert_eq!(rig.evacuate().await, Reconciled::Changed);
        assert_eq!(rig.placement().await, vec![3, 4, FREE]);
        assert_eq!(
            rig.mark(SOURCE).await,
            Some(legacy(first)),
            "the vacated mark"
        );

        rig.clock.advance(1_000);
        rig.drain_server(FREE).await;
        rig.undrain_server(SOURCE).await;
        let second = rig.clock.now_millis();
        assert_eq!(rig.evacuate().await, Reconciled::Changed);
        assert_eq!(
            rig.placement().await,
            vec![3, 4, SOURCE],
            "back on server 5"
        );
        (rig, first, second)
    }

    /// **(D2)** After the round trip no mark exists at server 5's position — the first move's
    /// mark on it is gone — and server 2's vacated position carries one.
    async fn d2(shape: Shape) {
        let (rig, _, second) = d2_moved(shape).await;

        assert_eq!(
            rig.mark(SOURCE).await,
            None,
            "the first move's mark outlived it on a position the placement names"
        );
        rig.assert_no_mark_on_placement().await;
        assert_eq!(
            rig.mark(FREE).await,
            Some(legacy(second)),
            "the vacated mark"
        );
        rig.assert_complete().await;
    }

    on_both_shapes!(d2 =>
        d2_a_flat_fragment_drained_back_within_grace_carries_no_mark,
        d2_a_segmented_fragment_drained_back_within_grace_carries_no_mark);

    // ─────────────────────────────── (D3) ───────────────────────────────

    /// A production Complete supersedes the object, then GC-only passes run with the clock past
    /// `old`'s grace — `old` the stamp of the mark the base leaves on a position the moved
    /// placement names: they reclaim none of the superseded generation's fragments, which no mark
    /// covers yet. Drain passes then mark every one under the obligation's event, and past those
    /// marks' grace GC reclaims them all.
    async fn supersede_then_reclaim(rig: &Rig, old: u64) {
        let positions = rig.positions().await;
        let (obligation, event) = rig.supersede().await;
        let past = old + GRACE + 1;
        if rig.clock.now_millis() < past {
            rig.clock.set(past);
        }

        for _ in 0..2 {
            rig.gc().await;
        }
        for &(dserver, frag) in &positions {
            assert!(
                rig.fleet.holds(dserver, frag).is_some(),
                "GC reclaimed {frag:?} on {dserver} before the drain marked it"
            );
        }

        for _ in 0..8 {
            if rig.store.value(&obligation).await.is_none() {
                break;
            }
            rig.drain().await;
        }
        assert!(rig.store.value(&obligation).await.is_none(), "drained");
        for &(dserver, frag) in &positions {
            let stored = rig.store.value(&orphan_key(dserver, frag)).await;
            let stored = stored.unwrap_or_else(|| panic!("{frag:?} on {dserver} is unmarked"));
            let mark = decode_orphan_mark(&stored).unwrap();
            assert_eq!(mark.event(), Some(event.as_str()), "{frag:?} on {dserver}");
        }

        rig.clock.advance(GRACE + 1);
        rig.gc_until_quiet().await;
        for &(dserver, frag) in &positions {
            assert!(
                rig.fleet.holds(dserver, frag).is_none(),
                "{frag:?} on {dserver} survived its mark's grace"
            );
        }
    }

    /// **(D3, after D1)** The stale mark the base leaves at the destination is stamped 1000.
    async fn d3_after_d1(shape: Shape) {
        let (rig, _) = d1_moved(shape).await;
        supersede_then_reclaim(&rig, 1_000).await;
    }

    on_both_shapes!(d3_after_d1 =>
        d3_a_superseded_flat_object_keeps_its_evacuated_fragment_until_the_drain_marks_it,
        d3_a_superseded_segmented_object_keeps_its_evacuated_fragment_until_the_drain_marks_it);

    /// **(D3, after D2)** The mark the base leaves on server 5's position is the first move's.
    async fn d3_after_d2(shape: Shape) {
        let (rig, first, _) = d2_moved(shape).await;
        supersede_then_reclaim(&rig, first).await;
    }

    on_both_shapes!(d3_after_d2 =>
        d3_a_superseded_flat_object_keeps_its_round_tripped_fragment_until_the_drain_marks_it,
        d3_a_superseded_segmented_object_keeps_its_round_tripped_fragment_until_the_drain_marks_it);

    // ─────────────────────────────── (D4) ───────────────────────────────

    /// **(D4)** Server 2's position carries `mark`, a value no writer may replace, and server 5
    /// drains with server 2 the only free domain. The move aborts — the drain does not certify —
    /// sends server 2 no write, leaves the placement and the mark byte-identical, and the audit
    /// seam names the position under `action`.
    async fn refuses(shape: Shape, mark: &[u8], action: &str, field: &str) -> Rig {
        let rig = Rig::new(shape).await;
        rig.plant_mark(FREE, mark).await;
        rig.drain_server(SOURCE).await;

        assert_eq!(rig.evacuate().await, Reconciled::Blocked);

        assert!(
            rig.fleet.sent_to(FREE).is_empty(),
            "the evacuation wrote to a destination it may not write"
        );
        rig.assert_not_adopted().await;
        assert_eq!(
            rig.mark(FREE).await.as_deref(),
            Some(mark),
            "the mark moved"
        );
        let position = name(&orphan_key(FREE, moved()));
        assert!(
            rig.audit.names(action, field, &position),
            "the audit seam does not name {position}"
        );
        rig
    }

    /// **(D4, `reclaiming`)** …and once GC has finished the reclaim — its fragment-less sweep
    /// deletes the mark, nothing beneath it — a later pass moves the chunk onto server 2.
    async fn d4_reclaiming(shape: Shape) {
        let rig = refuses(shape, &reclaiming(), "destination-passed-over", "subject").await;

        rig.gc_until_quiet().await;
        assert_eq!(rig.mark(FREE).await, None, "GC finished the reclaim");
        rig.assert_moved_later().await;
    }

    on_both_shapes!(d4_reclaiming =>
        d4_a_reclaiming_destination_is_never_written_by_a_flat_evacuation,
        d4_a_reclaiming_destination_is_never_written_by_a_segmented_evacuation);

    /// **(D4, unreadable)** A mark that is none of the three shapes is NEEDS-HUMAN: never
    /// overwritten.
    async fn d4_unreadable(shape: Shape) {
        refuses(shape, UNREADABLE, "unreadable-destination-mark", "mark").await;
    }

    on_both_shapes!(d4_unreadable =>
        d4_an_unreadable_destination_mark_is_never_overwritten_by_a_flat_evacuation,
        d4_an_unreadable_destination_mark_is_never_overwritten_by_a_segmented_evacuation);

    /// **(D4, the vacated position)** The mark on the draining server's position — the one the
    /// evacuation vacates — is written under a precondition on what the adoption read there,
    /// never blind: a `reclaiming` or unreadable mark still holds those exact bytes after the
    /// adoption, and a stale legacy one is re-stamped at the move.
    async fn d4_vacated(shape: Shape) {
        let cases = [
            (reclaiming(), false),
            (Bytes::from_static(UNREADABLE), false),
            (Bytes::from_static(STALE), true),
        ];
        for (planted, restamped) in cases {
            let rig = Rig::new(shape).await;
            rig.plant_mark(SOURCE, &planted).await;
            rig.drain_server(SOURCE).await;
            let at = rig.clock.now_millis();

            assert_eq!(rig.evacuate().await, Reconciled::Changed);

            assert_eq!(rig.placement().await, vec![3, 4, FREE], "{planted:?}");
            let expected = if restamped {
                legacy(at)
            } else {
                planted.clone()
            };
            assert_eq!(
                rig.mark(SOURCE).await,
                Some(expected),
                "the vacated position's mark {planted:?} was written over"
            );
            rig.assert_no_mark_on_placement().await;
            rig.assert_complete().await;
        }
    }

    on_both_shapes!(d4_vacated =>
        d4_a_flat_evacuation_writes_the_vacated_mark_under_a_precondition,
        d4_a_segmented_evacuation_writes_the_vacated_mark_under_a_precondition);

    /// **(D4, the vacated position, raced)** The vacated position's mark changes between the
    /// adoption's read of it and the adoption's commit — the wrapper lands the other writer's
    /// put just before that commit: an absent mark, or a stale legacy one, becomes `reclaiming`;
    /// an unreadable one is replaced by a legacy mark. The adoption loses on what it read: the
    /// placement is unchanged, the other writer's bytes are still there, the copy stays under
    /// its pre-mark, and the drain does not certify. A later pass moves the chunk, and leaves a
    /// `reclaiming` mark as it finds it.
    async fn d4_vacated_raced(shape: Shape) {
        let cases: [(Option<&[u8]>, Bytes); 3] = [
            (None, reclaiming()),
            (Some(STALE), reclaiming()),
            (Some(UNREADABLE), Bytes::from_static(STALE)),
        ];
        for (before, raced) in cases {
            let rig = Rig::new(shape).await;
            if let Some(before) = before {
                rig.plant_mark(SOURCE, before).await;
            }
            rig.drain_server(SOURCE).await;
            let racer = vec![(orphan_key(SOURCE, moved()), raced.clone())];
            rig.store.arm(repoints, Act::PlantFirst(racer));

            let outcome = rig.evacuate().await;

            assert_eq!(
                rig.mark(SOURCE).await,
                Some(raced.clone()),
                "{before:?}: the adoption wrote over a vacated mark that changed after its read"
            );
            rig.assert_not_adopted().await;
            assert_eq!(outcome, Reconciled::Blocked, "{before:?}: a lost adoption");
            assert!(
                rig.fleet.holds(FREE, moved()).is_some(),
                "{before:?}: the move wrote its copy"
            );
            rig.premark_stamp(FREE).await;
            rig.assert_no_stray(FREE).await;

            rig.clock.advance(1_000);
            let later = rig.clock.now_millis();
            rig.assert_moved_later().await;
            let kept = decode_orphan_mark(&raced).is_ok_and(|mark| mark.is_reclaiming());
            let expected = if kept { raced } else { legacy(later) };
            assert_eq!(rig.mark(SOURCE).await, Some(expected), "{before:?}");
        }
    }

    on_both_shapes!(d4_vacated_raced =>
        d4_a_flat_evacuations_adoption_loses_to_a_vacated_mark_changed_after_its_read,
        d4_a_segmented_evacuations_adoption_loses_to_a_vacated_mark_changed_after_its_read);

    // ─────────────────────────────── (D5) ───────────────────────────────

    /// Where GC is held while the move's adoption is released.
    #[derive(Clone, Copy, Debug)]
    enum GcHeld {
        /// Right after its reclaim-intent commit: the key holds `reclaiming`, the copy is still
        /// there.
        AfterIntent,
        /// Right after it deleted the copy, before its key cleanup: the key still holds
        /// `reclaiming`, the copy is gone.
        AfterDelete,
    }

    /// **(D5)** Server 2's position carries a stale legacy mark; server 5 drains. The move is
    /// held just before its adoption — after the copy — while the clock passes the grace and a GC
    /// pass records its reclaim intent on the destination's mark; GC is held at `gc_held`, and
    /// the adoption is released while the key still holds `reclaiming`. The adoption fails: no
    /// placement names the reclaimed position, and a later pass moves the chunk.
    async fn d5(shape: Shape, gc_held: GcHeld) {
        let rig = Rig::new(shape).await;
        rig.plant_mark(FREE, STALE).await;
        rig.drain_server(SOURCE).await;
        let adopt = Hold::default();
        rig.store.arm(repoints, Act::Before(adopt.clone()));
        let gc_hold = match gc_held {
            GcHeld::AfterIntent => {
                let hold = Hold::default();
                rig.store.arm(reclaims_free, Act::After(hold.clone()));
                hold
            }
            GcHeld::AfterDelete => rig.fleet.hold_delete(FREE, moved()),
        };

        let evacuation = Running::new();
        let script = async {
            evacuation
                .reaches(&adopt, "the move never reached its adoption")
                .await;
            assert!(
                rig.fleet.holds(FREE, moved()).is_some(),
                "the move wrote its copy"
            );
            rig.clock.advance(GRACE + 1);
            let gc = Running::new();
            let meanwhile = async {
                gc.reaches(&gc_hold, "GC never reached its hold").await;
                let stored = rig.mark(FREE).await.expect("the destination's mark");
                assert!(
                    decode_orphan_mark(&stored).is_ok_and(|mark| mark.is_reclaiming()),
                    "{gc_held:?}: the key does not hold `reclaiming` as the adoption is released"
                );
                adopt.release.notify_one();
                evacuation.finished().await;
                gc_hold.release.notify_one();
            };
            tokio::join!(gc.run(rig.gc()), meanwhile);
        };
        let (outcome, ()) = tokio::join!(evacuation.run(rig.evacuate()), script);

        assert_eq!(outcome, Reconciled::Blocked, "a lost move certified");
        assert!(
            rig.fleet.holds(FREE, moved()).is_none(),
            "GC did not reclaim the destination in flight"
        );
        rig.assert_not_adopted().await;
        rig.gc_until_quiet().await;
        rig.assert_moved_later().await;
    }

    async fn d5_after_intent(shape: Shape) {
        d5(shape, GcHeld::AfterIntent).await;
    }

    async fn d5_after_delete(shape: Shape) {
        d5(shape, GcHeld::AfterDelete).await;
    }

    on_both_shapes!(d5_after_intent =>
        d5_a_flat_evacuations_adoption_fails_while_gc_holds_its_intent,
        d5_a_segmented_evacuations_adoption_fails_while_gc_holds_its_intent);

    on_both_shapes!(d5_after_delete =>
        d5_a_flat_evacuations_adoption_fails_after_gc_deleted_its_copy,
        d5_a_segmented_evacuations_adoption_fails_after_gc_deleted_its_copy);

    // ─────────────────────────────── (D6) ───────────────────────────────

    /// **(D6)** Server 5 drains; just before the move's adoption another writer rewrites the root
    /// (its version bumped, its chunks unchanged), so the adoption loses. The copy the move wrote
    /// on server 2 is unreferenced, and GC reclaims it once the grace has passed.
    async fn d6(shape: Shape) {
        let rig = Rig::new(shape).await;
        rig.drain_server(SOURCE).await;
        let mut root = rig.root().await;
        root.version += 1;
        let rewritten = vec![(md::inode_key(INODE), md::encode(&root))];
        rig.store.arm(repoints, Act::PlantFirst(rewritten));

        assert_eq!(rig.evacuate().await, Reconciled::Blocked);

        assert_eq!(rig.root().await.version, root.version, "the racer landed");
        assert_eq!(rig.placement().await[usize::from(INDEX)], SOURCE);
        assert!(
            rig.fleet.holds(FREE, moved()).is_some(),
            "the move wrote its copy"
        );

        rig.clock.advance(GRACE + 1);
        rig.gc_until_quiet().await;
        assert!(
            rig.fleet.holds(FREE, moved()).is_none(),
            "the lost move's copy is kept for ever"
        );
    }

    on_both_shapes!(d6 =>
        d6_a_flat_evacuation_that_loses_its_commit_leaves_its_copy_reclaimable,
        d6_a_segmented_evacuation_that_loses_its_commit_leaves_its_copy_reclaimable);

    /// **(D6, the `seg:` pin)** The pre-mark is pinned to the `seg:` record the repoint read, not
    /// to the root alone. Server 5 drains; a sibling chunk's repoint rewrites the segment record
    /// holding the moved chunk — the root byte-identical — just before the move's pre-mark
    /// commit. The pre-mark loses on that record: server 2 is sent no write and its position
    /// carries no mark, and the pass does not certify. A later pass re-reads the record and moves
    /// the chunk, the sibling's edit kept.
    #[tokio::test]
    async fn d6_a_segmented_evacuations_pre_mark_loses_to_a_seg_record_rewrite() {
        let rig = Rig::new(Shape::Segmented).await;
        rig.drain_server(SOURCE).await;
        let root = rig.store.value(&md::inode_key(INODE)).await;
        let (key, rewritten) = rig.sibling_repoint().await;
        let racer = vec![(key.clone(), rewritten.clone())];
        rig.store.arm(premarks_free, Act::PlantFirst(racer));

        let outcome = rig.evacuate().await;

        assert!(
            rig.store.value(&key).await == Some(rewritten),
            "the `seg:` record does not hold the sibling's repoint"
        );
        assert_eq!(
            rig.store.value(&md::inode_key(INODE)).await,
            root,
            "the root moved"
        );
        assert!(
            rig.fleet.sent_to(FREE).is_empty(),
            "a move whose `seg:` record moved before its pre-mark sent a copy"
        );
        assert_eq!(
            rig.mark(FREE).await,
            None,
            "a pre-mark was committed over a `seg:` record that had moved"
        );
        rig.assert_not_adopted().await;
        assert_eq!(outcome, Reconciled::Blocked, "a pre-mark lost certified");
        rig.assert_moved_later().await;
        assert_eq!(
            rig.chunks().await[1].placement,
            vec![4],
            "the later move dropped the sibling's edit"
        );
    }

    // ─────────────────────────────── (D7) ───────────────────────────────

    /// Server 2 holds a copy of the fragment no record names, under the aged pre-mark of an
    /// earlier move that lost its commit; server 5 drains.
    async fn leftover(shape: Shape) -> Rig {
        let rig = Rig::new(shape).await;
        rig.fleet.put(FREE, moved(), fragment(INDEX));
        let earlier = T0 - 1_000;
        let event = format!("repoint:{}:earlier-move", wyrd_traits::chunk_hex(CHUNK));
        let mark = encode_orphan_mark(&OrphanMark::structured(earlier, event).unwrap());
        rig.plant_mark(FREE, &mark).await;
        rig.drain_server(SOURCE).await;
        rig
    }

    /// GC, with the clock past `W_repoint` and the pre-mark's grace: it reclaims server 2's old
    /// copy and deletes the mark over it.
    async fn reclaim_the_leftover(rig: &Rig) {
        rig.clock.advance(GRACE + 1);
        rig.gc_until_quiet().await;
        assert!(
            rig.fleet.holds(FREE, moved()).is_none(),
            "GC kept the old copy"
        );
        assert_eq!(rig.mark(FREE).await, None, "GC kept the mark");
    }

    /// What every (D7) leg's refused move leaves: the drain not certified, the abort named under
    /// `reason`, nothing on server 2 that lacks both a reference and a mark, the placement still
    /// naming the draining server and no missing fragment, and a later pass moves the chunk.
    async fn refused_then_moved(rig: &Rig, outcome: Reconciled, reason: &str) {
        assert_eq!(outcome, Reconciled::Blocked, "an aborted move certified");
        assert!(
            rig.audit.names("move-aborted", "reason", reason),
            "the audit seam does not name the abort as {reason}"
        );
        rig.assert_no_stray(FREE).await;
        rig.assert_not_adopted().await;
        rig.assert_moved_later().await;
    }

    /// **(D7a)** The move is held right after its pre-mark commit lands; GC reclaims the old copy
    /// and the pre-mark's key meanwhile. Released, the move sends server 2 no copy at all.
    async fn d7a(shape: Shape) {
        let rig = leftover(shape).await;
        let hold = Hold::default();
        rig.store.arm(premarks_free, Act::After(hold.clone()));

        let (outcome, reached) = held(&hold, rig.evacuate(), reclaim_the_leftover(&rig)).await;

        assert!(reached, "no pre-mark was committed before the copy");
        assert!(
            rig.fleet.sent_to(FREE).is_empty(),
            "a copy was authorized on a pre-mark past W_repoint"
        );
        refused_then_moved(&rig, outcome, "pre-mark-stale").await;
    }

    on_both_shapes!(d7a =>
        d7_a_a_flat_evacuations_stale_pre_mark_authorizes_no_copy,
        d7_a_a_segmented_evacuations_stale_pre_mark_authorizes_no_copy);

    /// **(D7b)** The destination copy is held in flight; GC reclaims the old copy and the
    /// pre-mark meanwhile. Released, the D server refuses the copy, its deadline passed, and
    /// nothing lands.
    async fn d7b(shape: Shape) {
        let rig = leftover(shape).await;
        let hold = rig.fleet.hold_write(FREE, moved());

        let (outcome, reached) = held(&hold, rig.evacuate(), reclaim_the_leftover(&rig)).await;

        assert!(reached, "the move never sent its copy");
        assert!(
            rig.fleet.holds(FREE, moved()).is_none(),
            "the D server stored a copy past its pre-mark's evidence"
        );
        refused_then_moved(&rig, outcome, "write-deadline-expired").await;
    }

    on_both_shapes!(d7b =>
        d7_b_a_flat_evacuations_copy_past_its_deadline_is_refused,
        d7_b_a_segmented_evacuations_copy_past_its_deadline_is_refused);

    /// What a (D7c)/(D7d) twin that stayed inside its bound leaves: adopted.
    async fn adopted_inside_grace(rig: &Rig, outcome: Reconciled) {
        assert_eq!(
            outcome,
            Reconciled::Changed,
            "a move inside its bounds was refused"
        );
        assert_eq!(rig.placement().await, vec![3, 4, FREE]);
        rig.assert_no_mark_on_placement().await;
        rig.assert_complete().await;
    }

    /// Server 5 drains, and the evacuation runs with its copy to server 2 held — before the D
    /// server judges the deadline, or once the bytes are published — while the clock moves to
    /// the pre-mark's stamp plus `after`. No GC pass runs: the pre-mark stays inside its grace.
    async fn hold_copy_until(shape: Shape, published: bool, after: u64) -> (Rig, Reconciled) {
        let rig = Rig::new(shape).await;
        rig.drain_server(SOURCE).await;
        let hold = if published {
            rig.fleet.hold_published(FREE, moved())
        } else {
            rig.fleet.hold_write(FREE, moved())
        };
        let (outcome, reached) = held(&hold, rig.evacuate(), async {
            let stamp = rig.premark_stamp(FREE).await;
            rig.clock.set(stamp + after);
        })
        .await;
        assert!(reached, "the move never sent its copy");
        (rig, outcome)
    }

    /// **(D7c)** The deadline passes while the pre-mark still stands — the clock at exactly the
    /// pre-mark's stamp plus the write window, inside its grace — and the destination holds
    /// nothing: the D server refuses the copy, nothing is adopted, and the pre-mark stands over
    /// the empty position. A millisecond earlier the copy lands and is adopted.
    async fn d7c(shape: Shape) {
        let (rig, outcome) = hold_copy_until(shape, false, W_WRITE_MILLIS).await;
        assert!(
            rig.fleet.holds(FREE, moved()).is_none(),
            "the D server stored a copy at its deadline"
        );
        rig.premark_stamp(FREE).await;
        refused_then_moved(&rig, outcome, "write-deadline-expired").await;

        let (rig, outcome) = hold_copy_until(shape, false, W_WRITE_MILLIS - 1).await;
        adopted_inside_grace(&rig, outcome).await;
    }

    on_both_shapes!(d7c =>
        d7_c_a_flat_evacuations_copy_at_its_deadline_is_refused_and_never_adopted,
        d7_c_a_segmented_evacuations_copy_at_its_deadline_is_refused_and_never_adopted);

    /// Server 5 drains, and the evacuation is held right after its pre-mark lands while the
    /// clock moves to exactly the pre-mark's stamp — the clock's reading the batch was built at
    /// — plus `after`. No GC pass runs.
    async fn hold_premark_until(shape: Shape, after: u64) -> (Rig, Reconciled) {
        let rig = Rig::new(shape).await;
        rig.drain_server(SOURCE).await;
        let hold = Hold::default();
        rig.store.arm(premarks_free, Act::After(hold.clone()));
        let (outcome, reached) = held(&hold, rig.evacuate(), async {
            let stamp = rig.premark_stamp(FREE).await;
            rig.clock.set(stamp + after);
        })
        .await;
        assert!(reached, "no pre-mark was committed before the copy");
        (rig, outcome)
    }

    /// **(D7d)** The gate's exact boundary: a pre-mark exactly `W_repoint` old — still inside the
    /// write window, so the D server would take the copy — authorizes no copy, the move aborts
    /// and the placement still names the draining server; one a millisecond younger authorizes
    /// the copy, which lands and is adopted.
    async fn d7d(shape: Shape) {
        let (rig, outcome) = hold_premark_until(shape, W_REPOINT).await;
        assert!(
            rig.fleet.sent_to(FREE).is_empty(),
            "a copy was authorized on a pre-mark W_repoint old"
        );
        refused_then_moved(&rig, outcome, "pre-mark-stale").await;

        let (rig, outcome) = hold_premark_until(shape, W_REPOINT - 1).await;
        assert_eq!(rig.fleet.sent_to(FREE), vec![moved()], "the copy");
        adopted_inside_grace(&rig, outcome).await;
    }

    on_both_shapes!(d7d =>
        d7_d_a_flat_evacuations_copy_gate_closes_at_exactly_w_repoint,
        d7_d_a_segmented_evacuations_copy_gate_closes_at_exactly_w_repoint);

    /// **(Landing unverified)** A copy whose landing the D server cannot certify is never
    /// adopted: held once its bytes are published while the clock reaches its deadline, the D
    /// server answers that it cannot say whether it landed in time. Whatever landed is under the
    /// pre-mark.
    async fn unverified(shape: Shape) {
        let (rig, outcome) = hold_copy_until(shape, true, W_WRITE_MILLIS).await;
        assert!(
            rig.fleet.holds(FREE, moved()).is_some(),
            "the uncertified copy's bytes are where they landed"
        );
        rig.premark_stamp(FREE).await;
        refused_then_moved(&rig, outcome, "write-unverified").await;
    }

    on_both_shapes!(unverified =>
        a_flat_evacuations_uncertified_copy_is_never_adopted,
        a_segmented_evacuations_uncertified_copy_is_never_adopted);

    // ─────────────────────────────── (D8) ───────────────────────────────

    /// **(D8)** Each move's identity is its own. Two evacuations of the chunk, B and A, pre-mark
    /// in the same millisecond (one clock reading): B is held before its pre-mark commit; A
    /// pre-marks, copies, and is held before its adoption; the clock passes the grace and GC
    /// reclaims A's copy and its mark; B is released, and its pre-mark re-creates the now-empty
    /// key; A is released. A's adoption fails — the key holds B's pre-mark, not its own — and no
    /// placement names the copy GC deleted.
    async fn d8(shape: Shape) {
        let rig = Rig::new(shape).await;
        rig.drain_server(SOURCE).await;
        let b_premark = Hold::default();
        rig.store.arm(premarks_free, Act::Before(b_premark.clone()));
        let a_adopt = Hold::default();

        let b = Running::new();
        let script = async {
            b.reaches(&b_premark, "B never reached its pre-mark commit")
                .await;
            rig.store.arm(repoints, Act::Before(a_adopt.clone()));
            let a = Running::new();
            let meanwhile = async {
                a.reaches(&a_adopt, "A never reached its adoption").await;
                let a_value = rig.mark(FREE).await.expect("A's pre-mark");
                rig.premark_stamp(FREE).await;
                assert!(rig.fleet.holds(FREE, moved()).is_some(), "A's copy");

                reclaim_the_leftover(&rig).await;

                b_premark.release.notify_one();
                b.finished().await;
                let b_value = rig.mark(FREE).await.expect("B's pre-mark");
                assert_eq!(
                    decode_orphan_mark(&b_value).unwrap().orphaned_at_millis(),
                    decode_orphan_mark(&a_value).unwrap().orphaned_at_millis(),
                    "the two pre-marks were stamped in one millisecond"
                );
                assert_ne!(b_value, a_value, "two moves' pre-marks are the same bytes");
                a_adopt.release.notify_one();
            };
            let (a_outcome, ()) = tokio::join!(a.run(rig.evacuate()), meanwhile);
            a_outcome
        };
        let (b_outcome, a_outcome) = tokio::join!(b.run(rig.evacuate()), script);

        assert_eq!(
            b_outcome,
            Reconciled::Blocked,
            "B's stale pre-mark certified"
        );
        assert_eq!(
            a_outcome,
            Reconciled::Blocked,
            "A's lost adoption certified"
        );
        assert!(rig.fleet.holds(FREE, moved()).is_none(), "GC's reclaim");
        rig.assert_not_adopted().await;
        rig.assert_no_stray(FREE).await;
        rig.assert_moved_later().await;
    }

    on_both_shapes!(d8 =>
        d8_a_flat_evacuations_adoption_is_never_satisfied_by_another_moves_pre_mark,
        d8_a_segmented_evacuations_adoption_is_never_satisfied_by_another_moves_pre_mark);

    // ─────────────────────────────── the swap ───────────────────────────────

    fn index(index: u16) -> FragmentId {
        FragmentId {
            chunk: CHUNK,
            index,
        }
    }

    /// **(Swap)** A position a mark rules out rules out only itself, never its server. Over five
    /// servers, 4 and 5 drain together: fragments 1 and 2 must leave, servers 2 and 6 hold the two
    /// free domains, and the selector's own pick gives fragment 1 to server 2, whose position for
    /// fragment 1 carries a mark no writer may replace. One pass moves both — fragment 1 onto
    /// server 6, fragment 2 onto server 2 — never writes the ruled-out position, leaves its mark
    /// byte-identical and names it on the audit seam. Ruling out server 2 whole would leave one
    /// server for two fragments, and an unreadable mark would then hold the drain for good.
    async fn swap(shape: Shape) {
        let bad = [
            (reclaiming(), "destination-passed-over", "subject"),
            (
                Bytes::from_static(UNREADABLE),
                "unreadable-destination-mark",
                "mark",
            ),
        ];
        for (mark, action, field) in bad {
            let rig = Rig::over(shape, &WIDE).await;
            rig.plant_mark_at(FREE, index(1), &mark).await;
            rig.drain_server(4).await;
            rig.drain_server(SOURCE).await;
            let at = rig.clock.now_millis();

            assert_eq!(
                rig.evacuate().await,
                Reconciled::Changed,
                "{action}: one pass moves both draining fragments"
            );

            assert_eq!(
                rig.placement().await,
                vec![3, 6, FREE],
                "{action}: the free servers swap fragments"
            );
            assert_eq!(
                rig.fleet.sent_to(FREE),
                vec![index(2)],
                "{action}: the ruled-out position was written"
            );
            assert_eq!(
                rig.store.value(&orphan_key(FREE, index(1))).await,
                Some(mark.clone()),
                "{action}: the ruled-out position's mark moved"
            );
            let position = name(&orphan_key(FREE, index(1)));
            assert!(
                rig.audit.names(action, field, &position),
                "{action}: the audit seam does not name {position}"
            );
            rig.assert_no_mark_on_placement().await;
            for (vacated, frag) in [(4, index(1)), (SOURCE, index(2))] {
                assert_eq!(
                    rig.store.value(&orphan_key(vacated, frag)).await,
                    Some(legacy(at)),
                    "{action}: the vacated position on {vacated}"
                );
            }
            rig.assert_complete().await;
        }
    }

    on_both_shapes!(swap =>
        a_ruled_out_position_rules_out_only_itself_in_a_flat_evacuation,
        a_ruled_out_position_rules_out_only_itself_in_a_segmented_evacuation);

    /// **(Mixed answers)** A move's copies are judged together. Over five servers, 4 and 5 drain
    /// together, and the move sends both copies at once: fragment 1 to server 2, which lands, and
    /// fragment 2 to server 6, held in flight until its deadline and refused. One refused copy
    /// refuses the whole move: nothing is adopted, the copy that landed stays under its pre-mark,
    /// and the pass does not certify. A later pass moves both.
    async fn mixed(shape: Shape) {
        let rig = Rig::over(shape, &WIDE).await;
        rig.drain_server(4).await;
        rig.drain_server(SOURCE).await;
        let hold = rig.fleet.hold_write(6, moved());

        let (outcome, reached) = held(&hold, rig.evacuate(), async {
            let stamp = rig.premark_stamp(6).await;
            rig.clock.set(stamp + W_WRITE_MILLIS);
        })
        .await;

        assert!(reached, "the move never sent fragment 2's copy");
        assert!(
            rig.fleet.holds(FREE, index(1)).is_some(),
            "fragment 1's copy did not land"
        );
        assert!(
            rig.fleet.holds(6, moved()).is_none(),
            "the D server stored a copy at its deadline"
        );
        assert_eq!(
            rig.placement().await,
            vec![3, 4, SOURCE],
            "a move with one refused copy adopted"
        );
        assert_eq!(
            outcome,
            Reconciled::Blocked,
            "a move with one refused copy certified"
        );
        assert!(
            rig.audit
                .names("move-aborted", "reason", "write-deadline-expired"),
            "the audit seam does not name the refused copy"
        );
        assert!(
            rig.store.value(&orphan_key(FREE, index(1))).await.is_some(),
            "the copy that landed is not under its pre-mark"
        );
        rig.assert_no_stray(FREE).await;

        assert_eq!(rig.evacuate().await, Reconciled::Changed);
        assert_eq!(rig.placement().await, vec![3, FREE, 6]);
        rig.assert_no_mark_on_placement().await;
        rig.assert_complete().await;
    }

    on_both_shapes!(mixed =>
        one_refused_copy_refuses_a_whole_flat_evacuation,
        one_refused_copy_refuses_a_whole_segmented_evacuation);
}
