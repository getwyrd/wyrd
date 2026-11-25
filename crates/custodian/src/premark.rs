//! The **destination-pre-mark rule** every committed move keeps: reconstruction's repair of a
//! committed chunk (`crate::reconstruction`, `repair_chunk`) and rebalance's evacuation of one
//! (`crate::rebalance`, `evacuate_chunk`). Proposal 0016 X47 (`0016:2587`), the rule that a
//! pre-mark must still stand when the move relying on it commits (`0016:1287-1312`), and the
//! write gate (`0016:1341-1360`):
//!
//! ```text
//! choose:   per moved fragment, a server in a free failure domain, at a position whose mark is
//!           absent or can be re-stamped — a mark that rules out one position never rules out its
//!           server ([`choose`], the one choice the staged re-place makes too)
//! pre-mark: the repoint's own pins (the root generation, and the `seg:` record for a segmented
//!           map) ∧ each P_new's mark as read
//!           ──► orphan:<P_new> = { t, event }        t: the context clock, read now; FRESH
//!               — except a P_new the placement already names (a fragment rebuilt in place):
//!               delete orphan:<P_new>, and mark nothing there
//! gate:     the move's writes are authorized together, once, only while the clock reads
//!           earlier than t + W_repoint
//! write:    every put_fragment(P_new, bytes, deadline = t + W_write) sent at once;
//!           the D server refuses each at or after its deadline
//! adopt:    the repoint ∧ require(orphan:<P_new> == pre-mark)   (none for a P_new in place)
//!           ∧ orphan:<P_old> as read just before (absent, or its exact bytes)
//!           ──► placement P_old → P_new, delete orphan:<P_new>,
//!               orphan:<P_old> = t' — unless it is `reclaiming` or unreadable: left as it is
//! ```
//!
//! **No mark outlives its event on a position the new placement names.** The pre-mark replaces
//! whatever mark the destination position held — a vacated position's mark from an earlier move
//! the fragment is now coming back to, or the expired leftover of a move that lost its commit —
//! and only the adoption that publishes the placement deletes it. Without that, a stale mark sat
//! on a referenced position, and the first unreference that writes no fresh mark of its own (a
//! multipart Complete's supersede) let GC reclaim the fragment on the old stamp before the
//! retirement drain marked it (C-1, `docs/principles.md` §5).
//!
//! **A position the placement already names is never marked.** A fragment rebuilt in place — on
//! the server the placement already names for it — is written to a position that never stops
//! being referenced, so there is nothing for a pre-mark to evidence: GC reclaims no referenced
//! position, and an unreference that reads its stamp after the pre-mark batch marks the position,
//! inline or through its drain, no earlier than the batch (#1100's exception, below). A pre-mark
//! there would be the very defect this rule removes: a crash, a lost adoption or a refused write
//! after the write landed would leave it standing on a referenced position, the next pass would
//! find the chunk whole and drain its obligation without touching the mark, and a later supersede
//! would let GC reclaim the fragment on that stamp before the drain marked it. So the pre-mark
//! batch deletes whatever mark the position holds instead — under its exact bytes and the
//! repoint's pins, so only while the position is still named — and writes none. Nothing marks a
//! named position without moving a record those pins cover (a supersede rewrites the root, a
//! move the record it repoints), so the position is still unmarked when the adoption lands. The
//! batch is committed all the same, before any write, which is what anchors the write gate: at its
//! stamp the generation was still current, so an unreference stamped after it marks the position
//! no earlier, and a write that lands before `t + W_write` lands inside that mark's grace.
//!
//! deferred: #1100 — the exception is any unreference whose mark is stamped before the in-place
//! batch and commits after it: a gateway unlink or a flat overwrite that read its stamp before the
//! batch, or a competing move of the same fragment whose adoption was built before it. Such a
//! mark carries the stamp its writer read as it built the commit (no `MetadataStore` commit has a
//! stamp of its own, and `0016:1304-1312` rules out a caller-side time for when a commit applies),
//! so GC may retire it before this move's write lands, and the fragment written in place is then
//! left unreferenced and unmarked: bytes GC keeps for ever, never bytes lost. Closing it needs a
//! fence proposal 0016 does not define.
//!
//! **No outcome strands a fragment, and none publishes over one GC has decided to reclaim.**
//! Every fragment a move writes is, at every instant, either named by the record it repoints or
//! covered by its pre-mark — a fragment rebuilt in place under an unreference stamped before its
//! batch and committed after it aside (deferred: #1100, above): the pre-mark is durable before
//! any write is sent, and only the adoption deletes it. So:
//!
//! - a supersede, a racing move or a changed destination mark that landed before the pre-mark
//!   makes the pre-mark lose, and nothing is written;
//! - one that lands after it makes the adoption lose: the pre-mark stands over whatever was
//!   written, and GC reclaims it after grace (`0016:2587`, the losing branch) — while a fragment
//!   rebuilt in place stays named, or is marked by the unreference that beat it (stamped too
//!   early only in #1100's case);
//! - GC recording its decision to reclaim the pre-mark (`reclaiming`, `crate::gc`) makes the
//!   adoption lose on the pre-mark's exact bytes, whatever the adoption's latency, so no
//!   placement is published over bytes GC is deleting (`0016:1314-1339`, outcome (c));
//! - a write the D server refuses, or whose landing it cannot certify, adopts nothing, and
//!   whatever landed is under the pre-mark, or on a position the placement still names.
//!
//! **Each move's pre-mark is its own.** The pre-mark's event carries a fresh per-move nonce
//! (`0016:1179-1189`, [`move_event`]), never a coordinate two moves share. So when another move
//! of the same chunk re-creates the key after GC reclaimed this move's destination and deleted its
//! pre-mark — the two stamped in one millisecond, say — the bytes differ, and this move's
//! adoption, which requires its own pre-mark's exact bytes, fails rather than publish a placement
//! over the fragment GC deleted.
//!
//! **The vacated position's mark is written under a precondition on what the adoption read
//! there, never blind** (`0016:670`, `:1224-1226`): absent, under `require_absent`; a legacy or
//! structured mark of another event, re-stamped under its exact bytes; a mark GC has already
//! decided to reclaim (`reclaiming`), or one that is none of the three shapes, left byte for byte
//! and pinned (`wyrd_core::metadata::OrphanMark`, ADR-0045 decision 3) — the first already
//! evidences the vacated bytes and GC finishes the reclaim, the second is a human's.
//!
//! **No fragment is written after its evidence may have been reclaimed** (`0016:1341-1360`). The
//! writes are authorized together, once, and only while the pre-mark is younger than
//! [`W_REPOINT_MILLIS`]; each carries the deadline `t + W_write`, fixed from the pre-mark's own
//! stamp, which the D server refuses at or after (`ChunkStore::put_fragment`, #638). A write
//! this rule permits therefore lands strictly inside its pre-mark's grace
//! (`G_orphan > W_repoint + W_write + δ_clock`, `0016:1348`).
//!
//! **One clock for the move** (ADR-0009). The pre-mark's stamp, the `W_repoint` gate and the
//! vacated position's mark are all read from the move's context clock as each step is built —
//! [`crate::ReconstructionContext::clock`] or [`crate::RebalanceContext::clock`], whose builder
//! backs it with the clock the pass's `now_millis` is read from — and the write window is the
//! context's too. GC ages the marks, and the D server judges the deadline, on the same
//! deployment clock, within `δ_clock` of it.
//!
//! The staged re-place (`crate::reconstruction`'s `staged` module) makes the same choice of
//! destinations ([`choose`], through its own [`Admission`]) and keeps the same rule with more
//! pins of its own — the session fence, the part record, each destination's drain key — so its
//! batches are its own.
//!
//! deferred: #825 — settling an aborted move's pre-marks. Every abort after the pre-mark leaves
//! it standing, which is what keeps whatever landed beneath it evidenced; a pre-mark whose write
//! never landed stays in the `orphan:` ledger (GC's sweep keeps every structured mark,
//! `crate::gc::LATE_WRITE_DEADLINE_MILLIS`), and a write the D server answered
//! `WriteEffect::Unknown` may land after GC reclaimed its pre-mark. Both are the staged
//! re-place's residuals too, and #825's.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::atomic::{AtomicBool, Ordering};

use wyrd_core::metadata::{decode_orphan_mark, encode_orphan_mark, orphan_key, OrphanMark};
use wyrd_core::placement::{select_distinct_domains_excluding, FailureDomain, Topology};
use wyrd_traits::{
    write_deadline_outcome, ChunkId, CommitOutcome, DServerId, FragmentId, MetadataStore, Result,
    WriteBatch,
};

use crate::gc::W_REPOINT_MILLIS;

/// The `orphan:` mark at a destination position, as the choice read it.
#[derive(Clone)]
pub(crate) enum DestinationMark {
    /// No mark: the pre-mark is written under `require_absent`.
    Absent,
    /// A legacy or structured mark from another event — a vacated position's mark, an earlier
    /// move's pre-mark. Re-stamped fresh, never reused with its old stamp, under a precondition
    /// on these exact bytes — or, on a position the placement already names, deleted under them.
    Stamped(Vec<u8>),
}

/// Why a destination position may not be written.
pub(crate) enum Unusable {
    /// GC has already recorded its decision to reclaim the position: a fragment written there
    /// could be deleted by the reclaim in progress, and no writer replaces such a mark
    /// (`wyrd_core::metadata::OrphanMark`).
    Reclaiming,
    /// The mark is none of the three shapes, so no writer may overwrite it (ADR-0045): it is
    /// left for a human byte for byte, as GC leaves one. The decoder's own words.
    Unreadable(String),
}

/// The mark at `frag`'s position on `dserver`, as a destination: `Ok` with the mark a pre-mark
/// may replace, or `Err` with why the position may not be written — the one classification every
/// mover's [`Admission::position`] makes. The read's await is bounded as every custodian read is,
/// by the `MetadataStore` implementation's own network bound (#508/#636); a fault fails the pass
/// before anything is written.
pub(crate) async fn destination_mark(
    meta: &dyn MetadataStore,
    dserver: DServerId,
    frag: FragmentId,
) -> Result<std::result::Result<DestinationMark, Unusable>> {
    Ok(match meta.get(&orphan_key(dserver, frag)).await? {
        None => Ok(DestinationMark::Absent),
        Some(current) => match decode_orphan_mark(&current) {
            Ok(mark) if mark.is_reclaiming() => Err(Unusable::Reclaiming),
            Ok(_) => Ok(DestinationMark::Stamped(current.to_vec())),
            Err(fault) => Err(Unusable::Unreadable(fault.to_string())),
        },
    })
}

/// A server the choice may give one of the chunk's fragments to, and its failure domain.
pub(crate) struct Candidate {
    pub(crate) dserver: DServerId,
    pub(crate) domain: FailureDomain,
}

/// What one mover admits as a destination, for [`choose`]: a server, for any of the chunk's
/// fragments ([`Self::candidate`]), and a position — that server for one fragment index
/// ([`Self::position`]). Each answers `None` for what it rules out, and names it on the mover's
/// own audit seam. Every read either makes is bounded as every custodian read is, by the
/// `MetadataStore` implementation's own network bound (#508/#636); a fault fails the pass before
/// anything is written.
pub(crate) trait Admission {
    /// `dserver` as a server any of the chunk's fragments may go to, or `None`.
    fn candidate(
        &self,
        dserver: DServerId,
    ) -> impl Future<Output = Result<Option<Candidate>>> + Send;

    /// The mark at the position fragment `index` would take on `dserver`, or `None` when that
    /// position may not be written.
    fn position(
        &self,
        dserver: DServerId,
        index: usize,
    ) -> impl Future<Output = Result<Option<DestinationMark>>> + Send;
}

/// The positions a choice of destinations has read, keyed by `(server, fragment index)`:
/// `Some(mark)` for one it may use, `None` for one it may not.
type Positions = HashMap<(DServerId, usize), Option<DestinationMark>>;

/// Choose a destination for each `missing` fragment — `(fragment index, server, the mark at that
/// position)`, in `missing` order: servers of `pool` in failure domains distinct from each other
/// and from `occupied` (the shared selector), each a server `admission` admits at a position it
/// admits — or `None` when no such choice exists. The one choice every move of a chunk's
/// fragments makes: the staged re-place's and both committed movers'.
///
/// **A position is ruled out, not its server.** Whether a server may hold any fragment of this
/// chunk is one fact ([`Admission::candidate`]); whether it may hold fragment `i` is another
/// ([`Admission::position`]: the mark at that position). A mark that forbids one position leaves
/// the server free to take another of the chunk's fragments, so a chunk moving several fragments
/// is never blocked by one bad position while a valid assignment exists: the servers swap
/// fragments instead.
///
/// The servers considered start as the selector's own pick for all the fragments at once — the
/// answer whenever every position is usable — and grow one server at a time, in the selector's
/// order, only while no assignment exists over them. [`assign`] finds an assignment over the
/// positions read usable and those not read yet; each unread position it relies on is then read,
/// and one ruled out sends it round again. Every round either considers a new server or reads a
/// new position, of finitely many, so the choice ends. With no free domain left for even the
/// first pick, the answer is `None` before any position is read.
pub(crate) async fn choose(
    pool: &Topology,
    occupied: &[FailureDomain],
    missing: &[usize],
    admission: &impl Admission,
) -> Result<Option<Vec<(usize, DServerId, DestinationMark)>>> {
    let mut considered = BTreeSet::new();
    let mut candidates: Vec<Candidate> = Vec::new();
    let mut positions = Positions::new();
    loop {
        let Some(assignment) = assign(missing, &candidates, &positions) else {
            // No assignment even with every unread position usable: consider more servers — the
            // selector's pick for every fragment first, then one at a time.
            let want = if considered.is_empty() {
                missing.len()
            } else {
                1
            };
            let view = pool.excluding(&considered);
            let Ok(picked) = select_distinct_domains_excluding(&view, want as u16, occupied) else {
                return Ok(None);
            };
            for dserver in picked {
                considered.insert(dserver);
                if let Some(usable) = admission.candidate(dserver).await? {
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
                    let read = admission.position(dserver, index).await?;
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
/// considered, so while every position is usable fragment `i` goes to the selector's `i`-th pick;
/// a domain another slot holds is taken only if that slot can move (an augmenting path — Kuhn's
/// bipartite matching, slots against domains).
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

/// The committed movers' [`Admission`]: every server of `pool`, at a position whose mark
/// [`destination_mark`] does not rule out. Each position ruled out is named through
/// `passed_over`. Whether a chosen server is in the pass's fleet view is asked where its store is
/// resolved, as it always was: a server outside it aborts the move there.
pub(crate) struct Committed<'a, F> {
    meta: &'a dyn MetadataStore,
    pool: &'a Topology,
    chunk: ChunkId,
    passed_over: F,
    /// Whether any position was ruled out by its mark ([`Self::ruled_out`]). Instance state of
    /// one choice, never shared beyond it (ADR-0035).
    ruled_out: AtomicBool,
}

impl<'a, F> Committed<'a, F> {
    /// The admission of one move of `chunk`'s fragments over `pool`.
    pub(crate) fn new(
        meta: &'a dyn MetadataStore,
        pool: &'a Topology,
        chunk: ChunkId,
        passed_over: F,
    ) -> Self {
        Self {
            meta,
            pool,
            chunk,
            passed_over,
            ruled_out: AtomicBool::new(false),
        }
    }

    /// Whether the choice ruled out a position by its mark. When [`choose`] then finds no
    /// assignment, the move was refused by a mark — `reclaiming`, or one no writer may replace —
    /// and not only for want of a free failure domain: a repair still owed, which the pass may not
    /// certify over.
    pub(crate) fn ruled_out(&self) -> bool {
        self.ruled_out.load(Ordering::Relaxed)
    }
}

impl<F> Admission for Committed<'_, F>
where
    F: Fn(DServerId, FragmentId, &Unusable) + Sync,
{
    fn candidate(
        &self,
        dserver: DServerId,
    ) -> impl Future<Output = Result<Option<Candidate>>> + Send {
        let candidate = self.pool.domain_of(dserver).map(|domain| Candidate {
            dserver,
            domain: domain.clone(),
        });
        std::future::ready(Ok(candidate))
    }

    async fn position(&self, dserver: DServerId, index: usize) -> Result<Option<DestinationMark>> {
        let frag = FragmentId {
            chunk: self.chunk,
            index: index as u16,
        };
        Ok(match destination_mark(self.meta, dserver, frag).await? {
            Ok(mark) => Some(mark),
            Err(unusable) => {
                self.ruled_out.store(true, Ordering::Relaxed);
                (self.passed_over)(dserver, frag, &unusable);
                None
            }
        })
    }
}

/// One position a move writes a fragment to: the destination, the mark it held when the move
/// chose it, and the server the fragment leaves.
pub(crate) struct Destination {
    pub(crate) dserver: DServerId,
    pub(crate) frag: FragmentId,
    pub(crate) mark: DestinationMark,
    /// The server the chunk's placement names for this fragment before the move. Equal to
    /// [`Self::dserver`] for a fragment rebuilt in place, whose position stays named and so is
    /// neither vacated nor ever marked.
    pub(crate) vacated: DServerId,
}

impl Destination {
    /// Whether the fragment is rebuilt on the server the placement already names for it.
    fn in_place(&self) -> bool {
        self.dserver == self.vacated
    }
}

/// A move's pre-mark, durable at every destination: what authorizes the move's writes
/// ([`Self::authorize`]) and what its adoption consumes ([`Self::adopt`]).
pub(crate) struct PreMarked {
    /// The pre-mark's stamp, read from the move's clock as the batch was built.
    stamp: u64,
    /// The pre-mark's stored bytes: what every destination the placement does not already name
    /// holds, and what the adoption requires there.
    value: Vec<u8>,
}

/// PRE-MARK every destination, durably, before any write is sent — from then on a fragment the
/// move writes is covered by evidence GC acts on, or sits on a position the placement still
/// names, whichever way the adoption goes — or `None` when the batch lost its precondition, and
/// nothing was written.
///
/// Stamped from `clock` as the batch is built, never the pass's start, under this move's own
/// event — a nonce minted here, once per move ([`move_event`]) — FRESH whatever mark was there
/// before (each destination's mark as the choice read it is the batch's precondition). A
/// destination the placement already names gets no pre-mark: the batch deletes whatever mark it
/// holds and writes none (the module doc says why). `repoint`'s
/// own preconditions — the root generation and, for a segmented map, the `seg:` record
/// ([`wyrd_core::metadata::repoint_chunk`]) — pin it too, so a move whose record has already moved
/// writes nothing at all, and the stamp is one at which every position the placement names was
/// still named. A store fault fails the pass.
pub(crate) async fn premark(
    meta: &dyn MetadataStore,
    clock: &(dyn wyrd_testkit::Clock + Sync),
    chunk: ChunkId,
    repoint: &WriteBatch,
    destinations: &[Destination],
) -> Result<Option<PreMarked>> {
    let stamp = clock.now_millis();
    let value = encode_orphan_mark(&OrphanMark::structured(stamp, move_event(chunk))?);
    let pins = repoint
        .preconditions
        .iter()
        .fold(WriteBatch::new(), |batch, pin| match &pin.expected {
            Some(expected) => batch.require(pin.key.clone(), expected.clone()),
            None => batch.require_absent(pin.key.clone()),
        });
    let batch = destinations.iter().fold(pins, |batch, dest| {
        let key = orphan_key(dest.dserver, dest.frag);
        let batch = match &dest.mark {
            DestinationMark::Absent => batch.require_absent(key.clone()),
            DestinationMark::Stamped(current) => batch.require(key.clone(), current.clone()),
        };
        match (&dest.mark, dest.in_place()) {
            (_, false) => batch.put(key, value.clone()),
            // Rebuilt in place: the position stays named, so a mark there is stale — deleted,
            // and none is written. deferred: #1100 — an unreference whose mark is stamped before
            // this batch and commits after it can leave the write unmarked (the module doc).
            (DestinationMark::Stamped(_), true) => batch.delete(key),
            (DestinationMark::Absent, true) => batch,
        }
    });
    Ok(match meta.commit(batch).await? {
        CommitOutcome::Committed => Some(PreMarked {
            stamp,
            value: value.to_vec(),
        }),
        CommitOutcome::Conflict => None,
    })
}

impl PreMarked {
    /// AUTHORIZE the move's writes, all at once: the deadline every one of them carries —
    /// `stamp + write_window_millis`, fixed at the pre-mark's own stamp, which the D server
    /// refuses at or after — or `None` once the pre-mark is [`W_REPOINT_MILLIS`] old, when no
    /// write may be sent at all (`0016:1341-1351`: a caller-side deadline gating a caller-side
    /// action). One gate for the move, never one per write, so a slow but legal write never ages
    /// the pre-mark past the gate for the next one. `clock` is the one the pre-mark was stamped
    /// from.
    pub(crate) fn authorize(
        &self,
        clock: &(dyn wyrd_testkit::Clock + Sync),
        write_window_millis: u64,
    ) -> Option<u64> {
        (clock.now_millis() < self.stamp.saturating_add(W_REPOINT_MILLIS))
            .then(|| self.stamp.saturating_add(write_window_millis))
    }

    /// ADOPT, in ONE commit: `repoint` — the placement move with whatever evidence of its own the
    /// caller added — each pre-mark consumed, and each vacated position marked. `true` when it
    /// landed; `false` when it lost, and every fragment the move wrote to a new position stays
    /// under its pre-mark, which GC reclaims after grace — or, rebuilt in place, on a position the
    /// placement still names.
    ///
    /// It lands only while every destination still holds this pre-mark's exact bytes — GC swaps a
    /// mark to `reclaiming` before it deletes anything, so no placement is published over bytes
    /// GC is deleting, whatever the adoption's latency (`0016:1314-1339`) — and only while every
    /// vacated position still holds the mark this adoption read there just before ([`Vacated`]).
    /// A position rebuilt in place carries no pre-mark, so it has none to consume. The vacated
    /// position's mark is the legacy shape both movers have always left there, stamped from
    /// `clock` when the position stops being named; it is written AFTER its fragment, the shape
    /// GC's sweep of marks with no fragment beneath them retires (`crate::gc`). Each read is
    /// bounded as every custodian read is, by the `MetadataStore` implementation's own network
    /// bound (#508/#636); a store fault fails the pass.
    pub(crate) async fn adopt(
        &self,
        meta: &dyn MetadataStore,
        clock: &(dyn wyrd_testkit::Clock + Sync),
        repoint: WriteBatch,
        destinations: &[Destination],
    ) -> Result<bool> {
        // Rebuilt in place: the position stays named, so nothing is vacated, and it carries no
        // pre-mark to consume.
        let moved: Vec<&Destination> = destinations.iter().filter(|d| !d.in_place()).collect();
        let mut vacated = Vec::with_capacity(moved.len());
        for dest in &moved {
            let key = orphan_key(dest.vacated, dest.frag);
            let read = Vacated::read(meta.get(&key).await?.as_deref());
            vacated.push((key, read));
        }
        // Stamped once every read has answered, as the position stops being named.
        let vacated_mark = encode_orphan_mark(&OrphanMark::legacy(clock.now_millis()));
        let mut adopt = repoint;
        for dest in &moved {
            let key = orphan_key(dest.dserver, dest.frag);
            adopt = adopt.require(key.clone(), self.value.clone()).delete(key);
        }
        for (key, read) in vacated {
            adopt = match read {
                Vacated::Absent => adopt
                    .require_absent(key.clone())
                    .put(key, vacated_mark.clone()),
                Vacated::Stamped(current) => adopt
                    .require(key.clone(), current)
                    .put(key, vacated_mark.clone()),
                Vacated::Kept(current) => adopt.require(key, current),
            };
        }
        Ok(meta.commit(adopt).await? == CommitOutcome::Committed)
    }
}

/// The `orphan:` mark at a position a move vacates, as its adoption read it just before it
/// commits — what that commit pins, and what it writes there (`0016:670`, `:1224-1226`).
enum Vacated {
    /// No mark: one is written under `require_absent`.
    Absent,
    /// A legacy or structured mark of another event: re-stamped with the adoption's own instant,
    /// under a precondition on these exact bytes.
    Stamped(Vec<u8>),
    /// A mark GC has already decided to reclaim (`reclaiming`), or one that is none of the three
    /// shapes: no writer replaces either (`wyrd_core::metadata::OrphanMark`, ADR-0045 decision 3),
    /// so it is left byte for byte, and pinned. The first already evidences the vacated bytes and
    /// GC finishes the reclaim; the second waits for a human, as GC leaves it.
    Kept(Vec<u8>),
}

impl Vacated {
    fn read(stored: Option<&[u8]>) -> Self {
        match stored {
            None => Self::Absent,
            Some(current) => match decode_orphan_mark(current) {
                Ok(mark) if !mark.is_reclaiming() => Self::Stamped(current.to_vec()),
                Ok(_) | Err(_) => Self::Kept(current.to_vec()),
            },
        }
    }
}

/// What the answers to a move's writes leave it: `None` when every write landed, `Some(reason)`
/// when the D server refused one past its deadline (`write-deadline-expired`) or could not
/// certify that one landed in time (`write-unverified`, which outranks the other) — the move
/// then adopts nothing, and whatever landed is under its pre-mark, or on a position the
/// placement still names — and the fault itself for any other failure, which fails the pass as
/// a fragment write's always has.
pub(crate) fn refusal(answers: Vec<Result<()>>) -> Result<Option<&'static str>> {
    let mut refused = None;
    for answered in answers {
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
    Ok(refused)
}

/// A fresh identity for one move's pre-marks: `repoint:<chunk>:<nonce>`, 0016's per-move nonce
/// (`0016:1179-1189`), minted as the move pre-marks. The chunk names the move for an operator;
/// the nonce alone makes it unique. It is never derived from a coordinate two moves share — the
/// chunk, the pre-mark's stamp, the root generation, the `seg:` record — so two moves of one
/// chunk that pre-mark in the same millisecond still write different bytes, and neither move's
/// adoption can be satisfied by the other's pre-mark re-creating a key GC deleted (the module doc).
///
/// The nonce is std's per-thread hash keys, through a fresh [`RandomState`]: seeded from the OS
/// once per thread, and advanced by every `RandomState` made on it, so no two moves in one process
/// draw the same keys, and a 64-bit hash under distinct keys repeats with negligible chance. No
/// new crate and no field on either move's context. Under the deterministic simulator (`madsim`)
/// those keys are drawn from the run's seed, so a seed still replays to the same run
/// (`crates/dst/tests/custodian.rs`, the nonce's replay test). Not a security boundary: the
/// nonce only has to differ between moves.
fn move_event(chunk: ChunkId) -> String {
    let nonce = RandomState::new().build_hasher().finish();
    format!("repoint:{}:{nonce:016x}", wyrd_traits::chunk_hex(chunk))
}
