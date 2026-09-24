//! Reconstruction's **staged** half (proposal 0016 decision 2's reconstruction row, `0016:825`;
//! its failure rows `0016:885-889`; the pre-mark and write-deadline rules, `0016:1285-1358`):
//! rebuilding a chunk that no committed chunk map names but a multipart upload's committed part
//! (`part:`) does, while the upload is still `Open`, by a fenced **re-place**.
//!
//! Scrub verifies a committed part's fragments exactly as it verifies a committed map's
//! (`crate::scrub`) and queues the ordinary obligation, which the pass used to keep and never
//! discharge: the part stayed a fragment short until it was published, or for good if it never
//! was. Here the chunk is rebuilt and moved by the destination-pre-mark rule every move that
//! adopts a pre-written fragment follows:
//!
//! ```text
//! choose:   per missing fragment, a server in a free failure domain, in this pass's fleet, with
//!           no desired:dserver:<S_new> record, at a position whose mark is absent or can be
//!           re-stamped (a mark that rules out one position never rules out its server)
//! pre-mark: require(mpu == Open@E) ∧ require(part == prior) ∧ each P_new's mark as read
//!           ∧ require_absent(desired:dserver:<S_new>)
//!           ──► orphan:<P_new> = { t, event }        t: the context clock, read now; FRESH
//! gate:     the move's writes are authorized together, once, only while the clock reads
//!           earlier than t + W_repoint
//! write:    every put_fragment(P_new, rebuilt shard, deadline = t + W_write) sent at once;
//!           the D server refuses each at or after its deadline
//! adopt:    require(mpu == Open@E) ∧ require(part == prior) ∧ require(orphan:<P_new> == pre-mark)
//!           ∧ require_absent(desired:dserver:<S_new>) ∧ each vacated P_old's mark as read
//!           ──► part: P_old → P_new, delete orphan:<P_new>, orphan:<P_old> = t',
//!               delete repair:<chunk>
//! ```
//!
//! **No outcome strands a fragment.** Every fragment the re-place writes is, at every instant,
//! either named by the part record or covered by its pre-mark: the pre-mark is durable before the
//! write is sent, and only the batch that adopts the fragment deletes it. So:
//!
//! - a fence before the pre-mark — a Complete, an Abort or a reaper moving the session out of
//!   `Open@E`, or a writer replacing the part record — makes the pre-mark batch lose, and nothing
//!   is written at all;
//! - a fence after it makes the adoption lose (X29, `0016:888`): the part record is left
//!   byte-identical, the pre-mark stands over whatever was written, so GC reclaims it after
//!   grace, and the obligation stays queued. A repair blocked by a Complete is retried once the
//!   chunk is published (`0016:825`);
//! - GC recording its decision to reclaim a pre-mark (`reclaiming`, `crate::gc`,
//!   `0016:1312-1336`) makes the adoption lose on the pre-mark's exact bytes, whatever the
//!   adoption's latency;
//! - a write the D server refuses, or whose landing it cannot certify, adopts nothing, and
//!   whatever landed is under the pre-mark;
//! - and the adoption is the one batch that removes the obligation, so the obligation goes only
//!   once the repair is durable.
//!
//! **One fact for the drain fence** (`0016:885`: the fence that closes an intent's placement
//! closes a re-place's destination too). A destination is fenced by the *presence* of a
//! `desired:dserver:<S>` record, whatever its value — the fact `require_absent` tests. The
//! choice of destination reads exactly that fact and both batches pin it, so a server with any
//! desired-state record is passed over when destinations are chosen, never chosen, written to and
//! then refused at the adoption on every pass. A drain recorded after the choice makes the
//! pre-mark lose (nothing written) or the adoption lose (the pre-mark stands over what was).
//!
//! **One clock for the move's lifecycle** (ADR-0009). The pre-mark and each vacated position's
//! mark are stamped from [`ReconstructionContext::clock`] as their batches are built, the
//! `W_repoint` gate reads the same clock, and the destination write's deadline is fixed from the
//! pre-mark's own stamp: `t + W_write` ([`ReconstructionContext::staged_write_window_millis`]).
//! The D server enforces that deadline on its own clock, within `δ_clock` of this one
//! (`ChunkStore::put_fragment`, #638), and GC ages the marks on the same deployment clock. So:
//!
//! - however long the pass spent reading and assessing before it reached this repair, the stamp
//!   is read when the pre-mark is built, not when the pass began, so no write is ever sent with a
//!   deadline that had already passed when it was authorized;
//! - a destination's existing mark, whatever event wrote it and however young it is, is
//!   re-stamped fresh under a precondition on its exact bytes, so an earlier move's pre-mark is
//!   never reused as this one's;
//! - no write is authorized on a pre-mark `W_repoint` old or older (`0016:1339-1349`); and a
//!   worker that stalls after authorizing one delivers a write whose deadline is already fixed,
//!   which the D server refuses once it passes. A fragment this re-place writes therefore lands
//!   before `t + W_write` on the acceptor's clock, strictly inside its pre-mark's grace
//!   (`G_orphan > W_repoint + W_write + δ_clock`, `0016:1348`): no fragment is written after its
//!   evidence may have been reclaimed;
//! - and the writes of one move are authorized together, under the one pre-mark they all sit
//!   under, then sent together. One write's latency therefore never ages the pre-mark past the
//!   gate for the next: each write has the whole of `W_write` from the pre-mark to land in,
//!   however long the others take, so a chunk missing several fragments is repaired in one pass
//!   whenever every write is legal.
//!
//! **What it cannot read, it does not rewrite** (ADR-0045). A session record that will not
//! decode, or a vacated position whose `orphan:` value is none of the three mark shapes, withholds
//! the whole repair before anything is written — named NEEDS-HUMAN, the obligation kept, the pass
//! not certified ([`Assessment::Withheld`]). A destination position whose mark will not decode,
//! or is already `reclaiming`, is never written: another position is chosen.

use std::collections::{BTreeSet, HashMap, HashSet};

use wyrd_core::metadata::{
    self, decode_orphan_mark, encode_orphan_mark, orphan_key, EcScheme, OrphanMark,
};
use wyrd_core::multipart::{decode_part_record, decode_session_record, PartRecord, SessionState};
use wyrd_core::placement::{select_distinct_domains_excluding, FailureDomain};
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, repair};
use wyrd_traits::{
    write_deadline_outcome, ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId,
    MetadataStore, Result, WriteBatch,
};

use super::{
    emit_staged, gather, Assessment, ReconstructionContext, RepairOutcome, RepairPlan, Target,
};
use crate::desired_state::desired_key;
use crate::gc::{object_name, staged_fragments_observing, StagedSet, W_REPOINT_MILLIS};

/// This pass's reading of the staged classes: the protection class every staged reader shares,
/// and, for each chunk the pass owes a repair on, the committed part record that names it — both
/// from one walk ([`read`]).
#[derive(Default)]
pub(super) struct StagedReading {
    /// The staged protection class ([`StagedSet`]), exactly as GC, restore and the drain-status
    /// query build it.
    pub(super) set: StagedSet,
    /// One entry per committed part this pass owes a repair inside, held once however many of
    /// its chunks are owed — and only once a repair is owed inside it.
    parts: Vec<StagedPart>,
    /// For each owed chunk a committed part names, the first such part in the walk's key order —
    /// the first-reference rule the committed reading applies (`super::read_committed`).
    sites: HashMap<ChunkId, StagedSite>,
}

/// One committed part this pass owes a repair inside, as the walk read it: the snapshot every
/// repair inside it is built from and pinned to.
struct StagedPart {
    /// The owning session's `mpu:` key and its value exactly as the session listing read it. Both
    /// batches of a re-place require these bytes — `require(mpu == Open@E)` — and every session
    /// transition rewrites them, bumping the epoch, so any fence fails the re-place.
    session_key: Vec<u8>,
    session: Vec<u8>,
    /// The part record's key and its value exactly as read — the other pin of both batches,
    /// `require(part == prior)`.
    key: Vec<u8>,
    prior: Vec<u8>,
    /// Those bytes, decoded.
    record: PartRecord,
}

/// Where an owed chunk sits in [`StagedReading::parts`].
struct StagedSite {
    part: usize,
    /// The chunk's index within that part's own chunk list.
    index: usize,
}

/// What a staged [`RepairPlan`] repoints and where its rebuilt fragments go, fixed before
/// anything is written.
pub(super) struct StagedTarget {
    /// Index into [`StagedReading::parts`].
    part: usize,
    /// One per missing fragment, in the plan's `missing` order.
    destinations: Vec<Destination>,
    /// The part record the adoption writes ([`repointed_part`]).
    next: Vec<u8>,
}

/// Where one rebuilt fragment goes, and the two marks the move pins: the destination position's,
/// which the pre-mark replaces, and the vacated position's, which the adoption replaces.
struct Destination {
    /// The fragment index it rebuilds — one of the plan's `missing`.
    index: usize,
    dserver: DServerId,
    /// The mark at the destination position, as the choice of destination read it.
    mark: DestinationMark,
    /// The mark at the position the move vacates — the part record's current placement for
    /// this index — as the assessment read it.
    vacated: VacatedMark,
}

/// The `orphan:` mark at a destination position, as read.
#[derive(Clone)]
enum DestinationMark {
    /// No mark: the pre-mark is written under `require_absent`.
    Absent,
    /// A legacy or structured mark from another unreference event — an earlier move's pre-mark
    /// included. Re-stamped fresh, never reused with its old stamp, under a precondition on these
    /// exact bytes.
    Stamped(Vec<u8>),
}

/// The `orphan:` mark at a position the move vacates, as read.
enum VacatedMark {
    /// No mark: the adoption writes one, under `require_absent`.
    Absent,
    /// A legacy or structured mark from another event: re-stamped with the adoption's own
    /// instant, under a precondition on these exact bytes.
    Stamped(Vec<u8>),
    /// GC has already recorded its decision to reclaim the position. No writer replaces such a
    /// mark (`metadata::OrphanMark`), and none needs to: it already evidences the vacated bytes,
    /// and GC finishes the reclaim.
    Reclaiming,
}

/// A server a rebuilt fragment of the chunk may go to ([`candidate`]).
struct Candidate {
    dserver: DServerId,
    domain: FailureDomain,
}

/// Read this pass's [`StagedReading`] for `queue`, through the one staged walk
/// ([`staged_fragments_observing`]): owned entries, then committed parts, session by session in
/// bounded pages, before the committed namespace (`super::reconcile` says why). A part is held
/// only when it names a chunk this pass owes a repair on and no earlier part in key order already
/// does.
pub(super) async fn read(meta: &dyn MetadataStore, queue: &[ChunkId]) -> Result<StagedReading> {
    let owed: HashSet<ChunkId> = queue.iter().copied().collect();
    let mut parts = Vec::new();
    let mut sites = HashMap::new();
    let set = staged_fragments_observing(meta, |read| {
        let mut held = None;
        for (index, chunk) in read.record.chunks().iter().enumerate() {
            if !owed.contains(&chunk.id) || sites.contains_key(&chunk.id) {
                continue;
            }
            let part = *held.get_or_insert_with(|| {
                parts.push(StagedPart {
                    session_key: read.session_key.to_vec(),
                    session: read.session.to_vec(),
                    key: read.key.to_vec(),
                    prior: read.value.to_vec(),
                    record: read.record.clone(),
                });
                parts.len() - 1
            });
            sites.insert(chunk.id, StagedSite { part, index });
        }
    })
    .await?;
    Ok(StagedReading { set, parts, sites })
}

/// Assess an owed chunk that no committed chunk map names, against the committed part that names
/// it — `None` when no committed part does (the caller then keeps an obligation an owned entry
/// still names, and drains one nothing names).
///
/// A chunk whose upload has left `Open` is kept: every other state has already fenced a
/// re-place, and the repair runs once the chunk is published (`0016:825`). An `Open` upload's
/// chunk is gathered and verified exactly as a committed one is, against the scheme its part
/// record carries, and settles the same way ([`super::Gathered::settle`]): at full redundancy
/// its obligation drains as a duplicate finding, and below `k` it is `Unreachable` or
/// `Unrepairable`. Otherwise its destinations are chosen here, before anything is written, and one
/// no usable destination can take is `Blocked`, off the repairable-backlog gauge like a committed
/// chunk with no free domain.
pub(super) async fn assess(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    staged: &StagedReading,
    chunk: ChunkId,
) -> Result<Option<Assessment>> {
    let Some(site) = staged.sites.get(&chunk) else {
        return Ok(None);
    };
    let part = &staged.parts[site.part];
    // Only an `Open` session's part may be repointed: the re-place is fenced exactly like a part
    // upload, and every other state has fenced it already. Both batches pin these same bytes, so
    // a session fenced after this read fails them too.
    match decode_session_record(&part.session) {
        Ok(session) if matches!(session.state(), SessionState::Open {}) => {}
        Ok(_) => {
            emit_staged(chunk, "session-not-open");
            return Ok(Some(Assessment::Staged));
        }
        Err(fault) => {
            emit_withheld(chunk, &object_name(&part.session_key), &fault.to_string());
            return Ok(Some(Assessment::Withheld));
        }
    }
    let chunk_ref = &part.record.chunks()[site.index];
    // A staged placement is trusted only at exactly one D server per fragment
    // (`crate::gc::StagedSet`'s rule), and the staged class holds a chunk whose record breaks it,
    // which the caller keeps before asking here — so this holds by construction. Checked all the
    // same: a rebuild indexes the placement by fragment.
    if chunk_ref.placement.len() != usize::from(chunk_ref.fragment_count()) {
        emit_staged(chunk, "untrusted-staged-record");
        return Ok(Some(Assessment::Staged));
    }
    let (k, m) = match chunk_ref.scheme {
        // A single-copy chunk has no second source to rebuild from: classified exactly as the
        // committed assessment classifies one (`super::assess`), before any fragment is fetched,
        // so a staged and a committed single-copy chunk raise the same signal for the same
        // obligation. Recovering a single copy is a replica-copy concern on both paths, not
        // erasure reconstruction's.
        EcScheme::None => return Ok(Some(Assessment::Unrepairable)),
        EcScheme::ReedSolomon { k, m } => (usize::from(k), usize::from(m)),
    };
    let gathered = gather(ctx, stores, chunk, &chunk_ref.placement, chunk_ref.scheme).await?;
    if let Some(settled) = gathered.settle(k) {
        return Ok(Some(settled));
    }

    // The mark at each position the move would vacate, read now, so that one no writer can parse
    // withholds the repair before anything is written (ADR-0045) — and so the adoption can pin the
    // bytes it replaces.
    let mut vacated = Vec::with_capacity(gathered.missing.len());
    for &index in &gathered.missing {
        let key = orphan_key(chunk_ref.placement[index], fragment(chunk, index));
        vacated.push(match ctx.meta.get(&key).await? {
            None => VacatedMark::Absent,
            Some(current) => match decode_orphan_mark(&current) {
                Ok(mark) if mark.is_reclaiming() => VacatedMark::Reclaiming,
                Ok(_) => VacatedMark::Stamped(current.to_vec()),
                Err(fault) => {
                    emit_withheld(chunk, &object_name(&key), &fault.to_string());
                    return Ok(Some(Assessment::Withheld));
                }
            },
        });
    }
    let Some(chosen) = choose_destinations(
        ctx,
        stores,
        chunk,
        &gathered.missing,
        &gathered.survivor_domains,
    )
    .await?
    else {
        return Ok(Some(Assessment::Blocked));
    };
    // `chosen` is in `missing` order, as `vacated` is.
    let destinations: Vec<Destination> = chosen
        .into_iter()
        .zip(vacated)
        .map(|((index, dserver, mark), vacated)| Destination {
            index,
            dserver,
            mark,
            vacated,
        })
        .collect();
    let Some(next) = repointed_part(part, site.index, &destinations) else {
        emit_withheld(
            chunk,
            &object_name(&part.key),
            "the repointed part record could not be derived from the stored bytes",
        );
        return Ok(Some(Assessment::Withheld));
    };
    Ok(Some(Assessment::Repairable(Box::new(RepairPlan {
        target: Target::Staged(StagedTarget {
            part: site.part,
            destinations,
            next,
        }),
        chunk_index: site.index,
        chunk_id: chunk,
        k,
        m,
        survivors: gathered.survivors,
        survivor_domains: gathered.survivor_domains,
        missing: gathered.missing,
        placement: chunk_ref.placement.clone(),
        len: chunk_ref.len as usize,
    }))))
}

/// The positions a choice of destinations has read, keyed by `(server, fragment index)`:
/// `Some(mark)` for one it may use, `None` for one it may not.
type Positions = HashMap<(DServerId, usize), Option<DestinationMark>>;

/// Choose a destination for each `missing` fragment — `(fragment index, server, the mark at that
/// position)`, in `missing` order: servers in failure domains distinct from each other and from
/// `survivor_domains` (the committed path's selector), each at a position it may take — or `None`
/// when no such choice exists.
///
/// **A position is ruled out, not its server.** Whether a server may hold any fragment of this
/// chunk is one fact ([`candidate`]: in this pass's fleet, fenced by no desired-state record);
/// whether it may hold fragment `i` is another ([`position`]: the mark at that position). A mark
/// that forbids one position leaves the server free to take another of the chunk's missing
/// fragments, so a chunk missing several fragments is never blocked by one bad position while a
/// valid assignment exists: the servers swap fragments instead.
///
/// The servers considered start as the selector's own pick for all the missing fragments at once
/// — the committed path's choice, and the answer whenever every position is usable — and grow one
/// server at a time, in the selector's order, only while no assignment exists over them.
/// [`assign`] finds an assignment over the positions read usable and those not read yet; each
/// unread position it relies on is then read, and one ruled out sends it round again. Every round
/// either considers a new server or reads a new position, of finitely many, so the choice ends.
///
/// Each read's await is bounded as every custodian read is, by the `MetadataStore`
/// implementation's own network bound (#508/#636); a fault fails the pass before anything is
/// written.
async fn choose_destinations(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    chunk: ChunkId,
    missing: &[usize],
    survivor_domains: &[FailureDomain],
) -> Result<Option<Vec<(usize, DServerId, DestinationMark)>>> {
    let mut considered = BTreeSet::new();
    let mut candidates: Vec<Candidate> = Vec::new();
    let mut positions = Positions::new();
    loop {
        let Some(assignment) = assign(missing, &candidates, &positions) else {
            // No assignment even with every unread position usable: consider more servers — the
            // selector's pick for every missing fragment first, then one at a time.
            let want = if considered.is_empty() {
                missing.len()
            } else {
                1
            };
            let view = ctx.topology.excluding(&considered);
            let Ok(picked) =
                select_distinct_domains_excluding(&view, want as u16, survivor_domains)
            else {
                return Ok(None);
            };
            for dserver in picked {
                considered.insert(dserver);
                if let Some(usable) = candidate(ctx, stores, chunk, dserver).await? {
                    candidates.push(usable);
                }
            }
            continue;
        };
        let mut chosen = Vec::with_capacity(missing.len());
        for (&index, &at) in missing.iter().zip(&assignment) {
            let dserver = candidates[at].dserver;
            let mark = match positions.get(&(dserver, index)) {
                Some(read) => read.clone(),
                None => {
                    let read = position(ctx, chunk, dserver, index).await?;
                    positions.insert((dserver, index), read.clone());
                    read
                }
            };
            if let Some(mark) = mark {
                chosen.push((index, dserver, mark));
            }
        }
        // Complete only if no position read just now ruled itself out; otherwise choose again
        // without it.
        if chosen.len() == missing.len() {
            return Ok(Some(chosen));
        }
    }
}

/// An assignment of each `missing` fragment, by its slot, to one of `candidates`, by its index: in
/// failure domains distinct from each other, at no position `positions` has ruled out — or `None`
/// when none exists. Slot by slot, a free domain is taken in the order the candidates were
/// considered, so while every position is usable fragment `i` goes to the selector's `i`-th pick,
/// as on the committed path; a domain another slot holds is taken only if that slot can move
/// (an augmenting path — Kuhn's bipartite matching, slots against domains).
fn assign(
    missing: &[usize],
    candidates: &[Candidate],
    positions: &Positions,
) -> Option<Vec<usize>> {
    let mut held: HashMap<&FailureDomain, (usize, usize)> = HashMap::new();
    for slot in 0..missing.len() {
        let mut visited = HashSet::new();
        if !augment(
            slot,
            missing,
            candidates,
            positions,
            &mut held,
            &mut visited,
        ) {
            return None;
        }
    }
    let mut assignment = vec![0; missing.len()];
    for &(slot, at) in held.values() {
        assignment[slot] = at;
    }
    Some(assignment)
}

/// Give `slot` a domain: a free one if a candidate offers it at a usable position, else one whose
/// holder can move to another (see [`assign`]).
fn augment<'c>(
    slot: usize,
    missing: &[usize],
    candidates: &'c [Candidate],
    positions: &Positions,
    held: &mut HashMap<&'c FailureDomain, (usize, usize)>,
    visited: &mut HashSet<&'c FailureDomain>,
) -> bool {
    for displace in [false, true] {
        for (at, candidate) in candidates.iter().enumerate() {
            let ruled_out = matches!(
                positions.get(&(candidate.dserver, missing[slot])),
                Some(None)
            );
            let holder = held.get(&candidate.domain).map(|&(holder, _)| holder);
            if ruled_out || holder.is_some() != displace || !visited.insert(&candidate.domain) {
                continue;
            }
            if holder
                .is_none_or(|holder| augment(holder, missing, candidates, positions, held, visited))
            {
                held.insert(&candidate.domain, (slot, at));
                return true;
            }
        }
    }
    false
}

/// `dserver` as a server a rebuilt fragment of `chunk` may go to, or `None` when it may take none:
/// outside this pass's fleet view, or fenced by a `desired:dserver:<S>` record — present at all,
/// whatever its value, the fact both batches pin with `require_absent`. Each is named on the audit
/// seam.
async fn candidate(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    chunk: ChunkId,
    dserver: DServerId,
) -> Result<Option<Candidate>> {
    let domain = match ctx.topology.domain_of(dserver) {
        Some(domain) if stores.contains_key(&dserver) => domain,
        _ => {
            emit_passed_over(chunk, &format!("dserver {dserver}"), "outside-fleet");
            return Ok(None);
        }
    };
    if ctx.meta.get(&desired_key(dserver)).await?.is_some() {
        emit_passed_over(chunk, &format!("dserver {dserver}"), "desired-state");
        return Ok(None);
    }
    Ok(Some(Candidate {
        dserver,
        domain: domain.clone(),
    }))
}

/// The mark at the position fragment `index` of `chunk` would take on `dserver`, or `None` when
/// that position may not be written: its mark is already `reclaiming` (a fragment written there
/// could be deleted by the reclaim in progress, and no writer replaces such a mark), or no writer
/// can parse it (never overwritten, ADR-0045). Each is named on the audit seam; the server may
/// still take another fragment.
async fn position(
    ctx: &ReconstructionContext<'_>,
    chunk: ChunkId,
    dserver: DServerId,
    index: usize,
) -> Result<Option<DestinationMark>> {
    let key = orphan_key(dserver, fragment(chunk, index));
    Ok(match ctx.meta.get(&key).await? {
        None => Some(DestinationMark::Absent),
        Some(current) => match decode_orphan_mark(&current) {
            Ok(mark) if mark.is_reclaiming() => {
                emit_passed_over(chunk, &object_name(&key), "reclaiming");
                None
            }
            Ok(_) => Some(DestinationMark::Stamped(current.to_vec())),
            Err(fault) => {
                emit_unreadable_destination(chunk, &object_name(&key), &fault.to_string());
                None
            }
        },
    })
}

/// Re-place `plan`'s missing fragment(s) of a committed part's chunk: rebuild, pre-mark each
/// destination, authorize every write at once under the `W_repoint` gate and send them together
/// under the deadline, and adopt in one commit pinned to the session, the part record, each
/// pre-mark and each destination's drain key — the sequence, and why no outcome strands a
/// fragment, is this module's own doc.
///
/// [`RepairOutcome::Aborted`] when the move stopped before it could adopt anything and is simply
/// started again next pass — a pre-mark that lost its precondition, a pre-mark `W_repoint` old
/// before a write was authorized, a write the D server refused past its deadline or could not
/// certify; [`RepairOutcome::Conflict`] when the adoption lost; [`RepairOutcome::Refused`] when
/// the repointed part record would cross the value ceiling. A store fault fails the pass, as on
/// the committed path; every pre-mark already written covers whatever landed.
pub(super) async fn repair(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    staged: &StagedReading,
    target: &StagedTarget,
    plan: &RepairPlan,
) -> Result<RepairOutcome> {
    let part = &staged.parts[target.part];
    let (k, m, chunk) = (plan.k, plan.m, plan.chunk_id);

    // REFUSE, AND WRITE NOTHING AT ALL, a repoint whose record would cross the value ceiling —
    // the committed path's rule (`super::repair_chunk`), judged on the very bytes the adoption
    // writes.
    if let Some(ceiling) = metadata::flat_value_ceiling_crossed(&target.next) {
        return Ok(RepairOutcome::Refused {
            bytes: target.next.len(),
            ceiling,
        });
    }

    // Rebuild the chunk from any `k` survivors and re-derive every shard (`erasure::encode` is
    // deterministic, so a rebuilt shard is byte-identical to the original), then resolve each
    // destination's store — all before anything is written.
    let data = erasure::reconstruct(k, m, plan.len, &plan.survivors)?;
    let shards = erasure::encode(k, m, &data)?;
    let mut writes = Vec::with_capacity(target.destinations.len());
    for dest in &target.destinations {
        // Chosen from this pass's fleet (`candidate`), so always found; fail safe regardless.
        let Some(&store) = stores.get(&dest.dserver) else {
            emit_staged_aborted(chunk, "destination-outside-fleet");
            return Ok(RepairOutcome::Aborted);
        };
        let bytes = encode_ec_fragment(
            chunk,
            dest.index as u16,
            k as u8,
            m as u8,
            &shards[dest.index],
        );
        writes.push((store, fragment(chunk, dest.index), bytes));
    }

    // PRE-MARK every destination, durably, before any write is sent: from here on a fragment the
    // move writes is covered by evidence GC acts on, whichever way the adoption goes. Stamped from
    // the context's clock as the batch is built, under this move's own event, FRESH whatever mark
    // was there before — and pinned to the session, the part record and each destination's drain
    // key, so a fence or a drain that already landed writes nothing at all.
    let stamp = ctx.clock.now_millis();
    let premark = encode_orphan_mark(&OrphanMark::structured(stamp, move_event(chunk, stamp))?);
    let mut batch = WriteBatch::new()
        .require(part.session_key.clone(), part.session.clone())
        .require(part.key.clone(), part.prior.clone());
    for dest in &target.destinations {
        let key = orphan_key(dest.dserver, fragment(chunk, dest.index));
        batch = match &dest.mark {
            DestinationMark::Absent => batch.require_absent(key.clone()),
            DestinationMark::Stamped(current) => batch.require(key.clone(), current.clone()),
        }
        .put(key, premark.clone())
        .require_absent(desired_key(dest.dserver));
    }
    if ctx.meta.commit(batch).await? == CommitOutcome::Conflict {
        emit_staged_aborted(chunk, "pre-mark-lost");
        return Ok(RepairOutcome::Aborted);
    }

    // deferred: #825 — settling this move's pre-marks. Every abort from here on leaves each
    // pre-mark standing, which is what keeps whatever landed beneath it evidenced; two residuals
    // of that are the follow-up's. A pre-mark whose write never landed (the gate below refused
    // it, or the D server did) stays in the `orphan:` ledger, since GC's sweep keeps every
    // structured mark (`crate::gc::LATE_WRITE_DEADLINE_MILLIS`) — a mark kept too long, never a
    // fragment left without one. And a write the D server answered `WriteEffect::Unknown` may land
    // after GC has reclaimed its pre-mark, which can leave bytes behind only on a destination that
    // already held them from an earlier aborted attempt (`crates/traits/src/lib.rs:862-902`).
    //
    // AUTHORIZE every write at once, only while the pre-mark is younger than `W_repoint`
    // (`0016:1339-1349`: a caller-side deadline gating a caller-side action), then SEND them
    // together, each carrying the deadline fixed at the pre-mark's own stamp, which the D server
    // refuses at or after. One gate for the move, never one per write: the writes sit under the
    // one pre-mark that authorizes them, so a slow but legal write never ages it past the gate
    // for the next one — which would refuse that write on every pass while each pass rewrote the
    // first, leaving the chunk degraded for good. Sent together, each write has the whole window
    // from the pre-mark to land in, however long the others take. The fan-out is the write path's
    // own (`wyrd_core::write::write_fragments`): the futures are polled on this task, no task is
    // spawned, and every write has answered before the move goes on, so nothing outlives the pass.
    // Each await is bounded as every fragment write's is, by the D-server client's own request
    // timeout set at composition — the caller-side half of `W_write`
    // (`crates/chunkstore-grpc/src/client.rs:264-271`).
    if ctx.clock.now_millis() >= stamp.saturating_add(W_REPOINT_MILLIS) {
        emit_staged_aborted(chunk, "pre-mark-stale");
        return Ok(RepairOutcome::Aborted);
    }
    let deadline = stamp.saturating_add(ctx.staged_write_window_millis);
    let sent = writes
        .into_iter()
        .map(|(store, frag, bytes)| store.put_fragment(frag, bytes, Some(deadline)));
    // A write the D server refused, or one whose landing it could not certify, adopts nothing:
    // whatever landed is under its pre-mark. Any other fault fails the pass, as on the committed
    // path.
    let mut refused = None;
    for answered in futures_util::future::join_all(sent).await {
        let Err(fault) = answered else {
            continue;
        };
        let Some(refusal) = write_deadline_outcome(fault.as_ref()) else {
            return Err(fault);
        };
        if refusal.effect.may_have_landed() {
            refused = Some("write-unverified");
        } else {
            refused.get_or_insert("write-deadline-expired");
        }
    }
    if let Some(reason) = refused {
        emit_staged_aborted(chunk, reason);
        return Ok(RepairOutcome::Aborted);
    }

    // ADOPT, in ONE commit: the part record repointed, each pre-mark consumed, each vacated
    // position marked, the obligation drained. It lands only while the session is still `Open`
    // at the epoch this pass read, the part record is still the one it read, every pre-mark still
    // holds the exact bytes written above (GC swaps a mark to `reclaiming` before it deletes
    // anything), and no drain has been recorded on a destination since it was chosen.
    //
    // The vacated position is marked when it stops being named, stamped from the same clock. The
    // mark is the legacy shape the committed path's repair leaves on a vacated position
    // (`super::repair_chunk`): it is written AFTER its fragment — nothing this move does writes
    // under it — which is the shape GC's sweep of marks with no fragment beneath them retires
    // (`crate::gc`).
    let vacated_mark = encode_orphan_mark(&OrphanMark::legacy(ctx.clock.now_millis()));
    let mut adopt = WriteBatch::new()
        .require(part.session_key.clone(), part.session.clone())
        .require(part.key.clone(), part.prior.clone())
        .put(part.key.clone(), target.next.clone())
        .delete(repair::repair_key(chunk));
    for dest in &target.destinations {
        let key = orphan_key(dest.dserver, fragment(chunk, dest.index));
        adopt = adopt
            .require(key.clone(), premark.clone())
            .delete(key)
            .require_absent(desired_key(dest.dserver));
        let vacated = plan.placement[dest.index];
        if vacated == dest.dserver {
            // Rebuilt in place: the position stays named, so nothing is vacated.
            continue;
        }
        let key = orphan_key(vacated, fragment(chunk, dest.index));
        adopt = match &dest.vacated {
            VacatedMark::Absent => adopt
                .require_absent(key.clone())
                .put(key, vacated_mark.clone()),
            VacatedMark::Stamped(current) => adopt
                .require(key.clone(), current.clone())
                .put(key, vacated_mark.clone()),
            VacatedMark::Reclaiming => adopt,
        };
    }
    Ok(match ctx.meta.commit(adopt).await? {
        CommitOutcome::Committed => RepairOutcome::Committed,
        // Lost: nothing was adopted and the obligation stays queued. Each written fragment stays
        // under its pre-mark, which GC reclaims after grace.
        CommitOutcome::Conflict => RepairOutcome::Conflict,
    })
}

/// The bytes the adoption writes under the part record's key: the stored value with the chunk at
/// `chunk_index` placed on `destinations`, and nothing else changed — or `None` if they cannot be
/// derived.
///
/// A [`PartRecord`] has no writer-side constructor (`crates/core/src/multipart.rs:2491`), so the
/// new value is derived from the stored bytes themselves. The one decoder accepts exactly the
/// canonical encoding ([`decode_part_record`]'s canonical-bytes gate, `multipart.rs:1927-1937`),
/// which spells the chunk list as `metadata::encode` spells it, so that list is found in the
/// stored bytes and replaced by the repointed one, and every other byte is kept. The result is
/// kept only if the same decoder reads it back as exactly the record intended — the repointed
/// chunks and every other field as stored — so this never writes a part record the decoder would
/// refuse, nor one that differs from the stored record in anything but the placement it moves.
fn repointed_part(
    part: &StagedPart,
    chunk_index: usize,
    destinations: &[Destination],
) -> Option<Vec<u8>> {
    let mut chunks = part.record.chunks().to_vec();
    for dest in destinations {
        chunks[chunk_index].placement[dest.index] = dest.dserver;
    }
    let stored = metadata::encode(&part.record.chunks());
    let at = part
        .prior
        .windows(stored.len())
        .position(|window| window == &stored[..])?;
    let mut next = Vec::with_capacity(part.prior.len());
    next.extend_from_slice(&part.prior[..at]);
    next.extend_from_slice(&metadata::encode(&chunks));
    next.extend_from_slice(&part.prior[at + stored.len()..]);
    let read_back = decode_part_record(&next).ok()?;
    (read_back.chunks() == chunks.as_slice()
        && read_back.len() == part.record.len()
        && read_back.digest() == part.record.digest()
        && read_back.committed_at_millis() == part.record.committed_at_millis()
        && read_back.session_epoch() == part.record.session_epoch())
    .then_some(next)
}

/// This move's identity on its pre-marks — 0016's per-move nonce (`0016:1174-1187`), minted when
/// the move begins: the chunk and the pre-mark's own stamp, never a coordinate successive moves
/// share (the session epoch, the part record). This move never takes the same-identity arm (it
/// re-stamps every mark it meets, under a precondition on the mark's exact bytes), so two moves of
/// one chunk minting the same identity in one millisecond cost nothing but the identity.
fn move_event(chunk: ChunkId, stamp: u64) -> String {
    format!("replace:{}:{stamp}", wyrd_traits::chunk_hex(chunk))
}

fn fragment(chunk: ChunkId, index: usize) -> FragmentId {
    FragmentId {
        chunk,
        index: index as u16,
    }
}

/// Emit a staged repair **withheld** because a record it would have to rewrite or pin cannot be
/// read — the upload's session record, the `orphan:` mark at a position the move would vacate,
/// or the part record's own bytes — on the durability-plane seam. NEEDS-HUMAN: nothing was
/// written, the record is left byte for byte as it is, the obligation stays queued, and the pass
/// does not certify until the record is repaired.
fn emit_withheld(chunk: ChunkId, record: &str, fault: &str) {
    tracing::warn!(monotonic_counter.reconstruction_withheld_staged_repairs = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "withheld-staged-repair",
        chunk = %wyrd_traits::chunk_hex(chunk),
        record = %record,
        fault = %fault,
        "reconstruction withheld a staged repair: a record it would have to rewrite cannot be read, and it never overwrites what it cannot parse; NOTHING was written and the obligation stays queued — NEEDS-HUMAN",
    );
}

/// Emit why a staged re-place stopped before it could adopt anything, on the same seam (the
/// `reconstruction_aborted` offset itself is `super::emit_aborted`'s).
fn emit_staged_aborted(chunk: ChunkId, reason: &'static str) {
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "staged-aborted",
        reason,
        chunk = %wyrd_traits::chunk_hex(chunk),
        "a staged re-place stopped before adopting anything; every fragment it wrote stays under its pre-mark, and the obligation stays queued",
    );
}

/// Emit a server (`outside-fleet`, `desired-state`) or a position (`reclaiming`) a staged
/// re-place of `chunk` will not place a fragment on, on the same seam. Another is chosen.
fn emit_passed_over(chunk: ChunkId, subject: &str, reason: &'static str) {
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "destination-passed-over",
        reason,
        subject = %subject,
        chunk = %wyrd_traits::chunk_hex(chunk),
        "a staged re-place passed over a destination it may not use; another is chosen",
    );
}

/// Emit a destination position whose `orphan:` mark is none of the three mark shapes, on the same
/// seam: NEEDS-HUMAN — the mark is never overwritten (ADR-0045) and is left for a human byte for
/// byte, as GC leaves one; the re-place chooses another position.
fn emit_unreadable_destination(chunk: ChunkId, mark: &str, fault: &str) {
    tracing::warn!(monotonic_counter.reconstruction_unreadable_destination_marks = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "unreadable-destination-mark",
        chunk = %wyrd_traits::chunk_hex(chunk),
        mark = %mark,
        fault = %fault,
        "a staged re-place passed over a destination whose orphan mark it cannot read; the mark is left in place and another position is chosen — NEEDS-HUMAN",
    );
}
