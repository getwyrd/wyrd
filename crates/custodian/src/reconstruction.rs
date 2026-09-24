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
//! no backend and no on-disk-format knowledge of its own. It also names the
//! [`wyrd_testkit::Clock`] seam (ADR-0024) on [`ReconstructionContext`] — a normal
//! dependency, not a dev one, because the staged re-place below reads its time through it.
//!
//! **A staged multipart upload's bytes are never drained out from under it either**
//! (proposal 0016 decision 2, `0016:765-893`, split from #663): before concluding "no
//! committed map references this chunk, drain the obligation", the pass also checks
//! the staged classes GC and restore already read (`crate::gc::staged_fragments`) —
//! read FIRST, source before destination (`0016:782-800`) — so a chunk a committed
//! part or an in-flight owned staging entry still names is never drained, even when a
//! publication lands mid-pass. Once a committed map names the chunk, that map alone
//! decides, exactly as for any committed chunk (scrub applies the same precedence,
//! `crate::scrub`).
//!
//! **And a committed part's chunk is rebuilt while its upload is still `Open`**
//! (`0016:825`, `:888-889`; `reconstruction::staged`): it is re-placed under the upload's
//! session fence by the destination-pre-mark rule, so no outcome strands a fragment it writes,
//! and its obligation goes only in the commit that adopts the rebuilt fragment. A chunk only an
//! in-flight owned entry names, one whose upload has left `Open`, and one a staged record
//! holds with a placement it cannot use are kept queued instead: nothing this pass may do
//! resolves them, so it neither drains them nor certifies over them.

mod staged;

use std::collections::{BTreeSet, HashMap, HashSet};

use wyrd_core::metadata::{
    self, ChunkMapError, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState,
};
use wyrd_core::placement::{select_distinct_domains_excluding, FailureDomain, Topology};
use wyrd_core::write::encode_ec_fragment;
use wyrd_core::{erasure, repair};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

use crate::gc::object_name;
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
    /// The D servers that are **configured but currently unreachable** this pass — they
    /// failed the role's reachability probe and were dropped from `fleet`. A placed fragment
    /// on one of these is *transiently unavailable*, **not** confirmed lost: it returns when
    /// the server does. So a below-`k` shortfall that these fragments alone explain is a
    /// recoverable **degraded** state, NOT the high-severity data-loss the storage system
    /// raises when fragments are confirmably gone (iteration-7 MUST-FIX: distinguish
    /// "unreachable right now" from "fragments confirmed gone" before the data-loss alarm;
    /// the deployable role supplies the dropped set from `live_reconstruction_view`).
    ///
    /// **Empty for the library / M3 property contexts** (no reachability filtering): with no
    /// known-unreachable server, every missing fragment is treated as `confirmed gone` —
    /// exactly the prior semantics, unchanged.
    pub unreachable: &'a [DServerId],
    /// The [`wyrd_testkit::Clock`] seam (ADR-0024) the staged re-place
    /// (`reconstruction::staged`) reads its whole lifecycle's time through: the stamp its
    /// pre-mark carries — read as the pre-mark batch is built, never the pass's start — the
    /// `W_repoint` gate it checks once before it authorizes all of a move's destination writes
    /// together, and the stamp of the mark it leaves on each position it vacates. The write's deadline is fixed from the
    /// pre-mark's own stamp ([`Self::staged_write_window_millis`]), and the D server enforces it
    /// on the deployment clock this one reads.
    ///
    /// **One source with the pass's `now_millis`** (ADR-0009: one clock per correctness
    /// lifecycle). Whoever builds the context backs both with the same clock — the deployed
    /// loop hands the pass readings of the very clock it puts here
    /// (`crates/server/src/custodian.rs`, `LoopClock`), and a test that passes a fixed instant
    /// puts a `wyrd_testkit::ManualClock` reading that instant here, never the wall clock
    /// beside it.
    pub clock: &'a (dyn wyrd_testkit::Clock + Sync),
    /// How long after a staged re-place's pre-mark its destination write may still land —
    /// proposal 0016's `W_write` (`0016:1551-1576`), fed from this crate's own
    /// [`crate::gc::W_WRITE_MILLIS`] rather than a second definition. The re-place sends every
    /// write with the deadline `pre-mark stamp + this`, which the D server refuses at or after
    /// (`wyrd_traits::WriteDeadlineExpired`). 0016 requires
    /// `G_orphan > W_repoint + W_write + δ_clock` (`0016:1348`), and #800's late-write deadline
    /// (`crate::gc::LATE_WRITE_DEADLINE_MILLIS`) must not be sized below it.
    pub staged_write_window_millis: u64,
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
    /// The record the repair repoints: a committed object of this pass's reading, or a staged
    /// multipart upload's committed part.
    target: Target,
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

/// The record a [`RepairPlan`] repoints.
enum Target {
    /// Which committed object of this pass's ONE reading holds the chunk ([`Reading::objects`],
    /// which carries that object's identity and the generation the scan returned) — an index,
    /// never a copy: every obligation inside an N-entry object shares that one snapshot, so Q
    /// obligations cost N decoded entries **once**, not Q×N. The rest of the plan is scalars
    /// plus this chunk's own survivors.
    Committed(usize),
    /// A multipart upload's committed part, in an `Open` session, and where the rebuilt
    /// fragments go ([`staged::StagedTarget`]).
    Staged(staged::StagedTarget),
}

/// One reconstruction reconciliation pass over `ctx` at logical time `now_millis`.
/// Dispatched only from [`crate::reconcile_step`] (the fenced control point) — never a
/// parallel entry. Returns [`Reconciled::Changed`] if any chunk's placement record was
/// repointed, [`Reconciled::Satisfied`] otherwise — and [`Reconciled::Blocked`] when the pass
/// cannot certify what it did not do: a committed object it could not read (its reading has a
/// hole in it, see [`read_committed`]), a repair it refused because the chunk's committed
/// reference lives in a `seg:` record (#682 owns that write path), or one it refused because
/// the repointed record would not survive the backend value ceiling ([`RepairOutcome::Refused`]).
pub(crate) async fn reconcile(
    ctx: &ReconstructionContext<'_>,
    now_millis: u64,
) -> Result<Reconciled> {
    let stores: HashMap<DServerId, &dyn ChunkStore> = ctx.fleet.iter().copied().collect();

    // Drain the shared repair queue: the obligations scrub / the read path produced.
    let queue = repair::queued_repairs(ctx.meta).await?;
    emit_queue_depth(queue.len());

    // **ONE reading of the committed namespace for the whole pass** — every obligation is
    // answered out of it, so the namespace is SCANNED once per pass instead of once per
    // obligation — #647's open finding, and the whole of the claim. What it does NOT
    // change: the per-repair map copy + encode below is the base's own, and a queue piled
    // inside ONE object still drains over as many passes as it takes on `origin/main`.
    //
    // An EMPTY queue reads NOTHING. This pass certifies only over the reading it performed,
    // and with nothing owed there is nothing to read and nothing to claim — the base's own
    // behaviour (its per-obligation loop scanned zero times over an empty queue), kept, and
    // the same shape `rebalance.rs:115-117` uses to answer without touching `inode:` at all.
    // Extended the same way to the staged classes below: nothing is owed, so there is nothing
    // for their read to protect from a drain either.
    //
    // **Source before destination, `sidx:` → `part:` → `inode:`** (normative, `0016:782-800`;
    // GC's own order over the same two reads, `crate::gc::reconcile`). The staged classes are
    // read FIRST, via the one walk GC and restore already share
    // (`crate::gc::staged_fragments_observing`) — never re-derived here — so a publish that
    // lands mid-pass (writes the committed inode, and only LATER, in a separate retirement
    // batch, deletes the `part:` record it replaces) is caught by this read even when it beats
    // the committed scan below: the chunk is seen staged now, or committed by the later `inode:`
    // read, or both — never neither. A pass that read `inode:` first could miss a chunk in both
    // classes (see the leg this guards, `staged_protection.rs`'s (E)/(D) appendix for #663.1).
    // The same walk keeps, for each owed chunk, the committed part that names it, with the exact
    // bytes the staged re-place pins its commits to ([`staged::read`]).
    let (staged, reading) = if queue.is_empty() {
        (staged::StagedReading::default(), Reading::default())
    } else {
        let staged = staged::read(ctx.meta, &queue).await?;
        // Attributed the moment the staged reading returns — **before** the committed read
        // below, whose `?` would otherwise end the pass carrying these names down with it,
        // exactly as GC attributes its own (`gc.rs:420-425`) and as `read_committed` names each
        // unreadable object where it is met rather than batching it for a caller. A genuinely
        // corrupt staged record has no repair path and no operator tooling yet (#694), so the
        // record's name is the operator's whole situational awareness; an `inode:` store fault
        // one statement later must not be what costs it.
        for (record, fault) in &staged.set.unresolvable {
            emit_unresolvable_staged(&crate::gc::object_name(record), fault);
        }
        let reading = read_committed(ctx.meta, &queue).await?;
        (staged, reading)
    };
    // The chunk ids the staged classes name at all — a committed part's own placement, or an
    // owned entry's, whichever a staged record could still trust (`StagedSet::held` names a
    // chunk too, just not where its fragments are) — is every question this pass asks the
    // staged reading: NOT "which fragment", the reclaim/mark question GC and restore ask
    // ([`crate::gc::StagedSet::protects`]), but "does ANY staged record still name this chunk
    // at all". It is asked only of a chunk NO committed map names (`assess`): a committed chunk
    // is discharged against its committed map alone, as it always was, whatever a leftover part
    // record names; an obligation is DISCARDED — deleted with nothing resolved — only when no
    // record, committed or staged, names or holds its chunk.
    let staged_chunks: HashSet<ChunkId> = staged
        .set
        .placed
        .iter()
        .map(|(_, frag)| frag.chunk)
        .chain(staged.set.held.keys().copied())
        .collect();
    // While ANY staged record could not be read, the staged reading is INCOMPLETE — this pass
    // cannot show a chunk is named by NO staged record, so "no committed map references it" is
    // not "no record names it" and drains nothing at all, exactly as an incomplete committed
    // reading withholds every drain below (`reading.incomplete`). Leg F's rule (#663.1),
    // applied to the staged read the way `reading.incomplete` already applies it to the
    // committed one.
    let staged_incomplete = !staged.set.unresolvable.is_empty();

    // Assess each obligation (locate the chunk in that reading, gather + verify survivors)
    // so the drain can be ordered by repair priority before any rebuild commits.
    let mut plans = Vec::new();
    let mut drain_only = Vec::new();
    // The under-replicated **level**: the *repairable backlog* — every chunk this pass found
    // to have physically lost redundancy that reconstruction *can still rebuild* (`Repairable`,
    // survivors ≥ `k`). This is the durability signal (`0005:326-329`, architecture §7.4 step
    // 4) whose rise-then-return-to-ZERO shape is the binding day-one signal (brief §Success
    // criterion), so it is a level that MUST be able to reach 0 on a *populated* store once the
    // real losses are repaired.
    //
    // Deliberately it counts ONLY the auto-repairable set. The two *non-repairable* conditions
    // are worse or different, are never drained by this loop, and so would re-count every pass
    // and FLOOR this gauge at ≥1 forever — making "returns to zero" unobservable on any store
    // carrying one. Each is surfaced on its OWN distinct, higher-severity signal instead:
    //
    //   * `Unrepairable` (survivors < `k`, or a no-redundancy `EcScheme::None`): the storage
    //     system has FAILED its primary responsibility — data meant to be durable is actually
    //     LOST and cannot be reconstructed. That is *more* severe than a repairable backlog, so
    //     it is raised on a dedicated data-loss signal (`reconstruction_data_loss`, a monotonic
    //     counter + a `tracing::error!` NEEDS-HUMAN audit line; see `emit_data_loss`) — never
    //     buried in, and never poisoning the return-to-zero of, the repairable gauge.
    //   * `Malformed` (a wrong-length committed placement): not a fragment-loss event at all
    //     (the classification is made BEFORE any fragment is fetched — a chunk with every
    //     fragment physically present but a corrupt placement vector is `Malformed`), never
    //     auto-repaired (ADR-0040 decision 4). It carries its own `reconstruction_malformed_placement`
    //     counter + NEEDS-HUMAN audit line (see `emit_needs_human`).
    //
    // `Drain` — a deleted or already-full-redundancy chunk — is likewise not counted.
    let mut under_replicated = 0usize;
    // A chunk that is below `k` ONLY because a placed server is transiently unreachable this
    // pass — recoverable, NOT lost. A distinct level from the data-loss counter (a false page
    // for a rolling restart would otherwise fire), and off the repairable-backlog gauge.
    let mut unreachable_degraded = 0usize;
    // A chunk that IS repairable (survivors ≥ k) but has no free distinct domain to place the
    // rebuild this pass — off the repairable-backlog gauge so a never-completable repair does
    // not floor the day-one "returns to zero" signal.
    let mut repair_blocked = 0usize;
    // Whether this pass kept at least one obligation queued because a staged multipart record
    // — never a committed map — names its chunk and this pass may not repair it
    // (`Assessment::Staged`), or withheld a staged repair over a record it cannot read
    // (`Assessment::Withheld`). Like the `seg:` refusal, this pass may not drain it (nothing
    // resolved it, and a staged record still names or holds the chunk, so deleting it would be
    // a discard), so it withholds certification below exactly as `!reading.refused.is_empty()`
    // already does for a segmented refusal.
    let mut staged_kept = false;
    for &chunk in &queue {
        match assess(ctx, &stores, &reading, &staged, &staged_chunks, chunk).await? {
            Assessment::Repairable(plan) => {
                under_replicated += 1;
                plans.push(plan);
            }
            // The obligation refers to a chunk no longer referenced (deleted), or one
            // already at full redundancy (a duplicate / transient finding): nothing to
            // rebuild, so just drain the obligation. NOT under-replicated.
            Assessment::Drain => drain_only.push(chunk),
            // Below `k` only because a placed server is transiently unreachable this pass:
            // NOT data loss (it recovers when the server returns). Raise the distinct
            // lower-severity signal and leave the obligation queued for re-assessment — do
            // NOT alarm data-loss and do NOT count it on the repairable-backlog gauge
            // (iteration-7 MUST-FIX: a rolling restart / partition must not page as lost data).
            Assessment::Unreachable => unreachable_degraded += 1,
            // Repairable in principle but no free distinct domain to place the rebuild this
            // pass. Kept OFF the repairable-backlog gauge (a never-completable repair counted
            // there would floor the binding "returns to zero" signal at ≥1 forever —
            // iteration-7 MUST-FIX) and raised on its own level; the obligation stays queued
            // and clears when capacity returns.
            Assessment::Blocked => repair_blocked += 1,
            // Below `k` survivors (loss beyond the scheme's tolerance) or a scheme with no
            // redundancy (`EcScheme::None`): un-reconstructable — the data is LOST. This is
            // the storage system failing its primary responsibility, *more* severe than a
            // repairable backlog. Raise it on its OWN dedicated high-severity data-loss signal
            // (NEEDS-HUMAN) and leave the obligation queued — NOT on the under-replicated
            // gauge, which is a repairable-backlog level that must return to zero. Counting a
            // never-repaired loss here would floor the day-one "rise then return to zero" gauge
            // at ≥1 forever on a populated store (iteration-6 rejection).
            Assessment::Unrepairable => emit_data_loss(chunk),
            // Malformed committed placement (ADR-0040 decision 4): never rebuild over a
            // fabricated identity vector. Skip the chunk, leave the obligation queued, and
            // surface it for a human (NEEDS-HUMAN) on its OWN distinct metric — NOT the
            // under-replicated gauge. It is not a fragment-loss event (classified before any
            // fetch) and is never auto-repaired, so counting it here would floor the day-one
            // "rise then return to zero" gauge at ≥1 forever on a populated store (iteration-5
            // BLOCKING #1). `emit_needs_human` carries the `reconstruction_malformed_placement`
            // counter so the corruption is not lost.
            Assessment::Malformed => emit_needs_human(chunk),
            // A repair this pass may NOT perform: the chunk's committed reference lives in a
            // `seg:` record and the segmented write path is #682's. Already counted and named
            // ONCE PER OBJECT by the reading above — never once per chunk — and, like every
            // other never-repaired condition, kept OFF the repairable-backlog gauge so it
            // cannot floor the day-one "returns to zero" signal. The obligation stays queued
            // (it is the last record saying live data is under-replicated) and the pass
            // refuses to certify below.
            Assessment::Refused => {}
            // A staged multipart record — never a committed map — names this chunk, and this
            // pass may not repair it: only an in-flight owned entry names it, its upload has
            // left `Open`, or a staged record holds it with a placement that cannot be used
            // (`assess`'s own emit already named it and why on the audit seam). Kept queued,
            // never drained, and OFF the repairable-backlog gauge like every other
            // never-drained, never-repaired condition. A committed part's chunk in an `Open`
            // upload is assessed like a committed one instead, and lands in the arms above.
            Assessment::Staged => staged_kept = true,
            // A staged repair withheld because a record it would have to rewrite cannot be read
            // (named NEEDS-HUMAN where it was met, `staged::assess`): kept, and never
            // certified over, for the same reason.
            Assessment::Withheld => staged_kept = true,
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
    // time-to-repair sample. Every non-success is offset on its own counter so the
    // up-front count nets back to true successes: a repair that loses the CAS race is
    // recorded on `reconstruction_conflict`, one that cannot proceed (the selector
    // chose a server outside the fleet view, so nothing is committed) on
    // `reconstruction_aborted`, and one refused because the repointed record would cross the
    // backend value ceiling on `reconstruction_ceiling_refused` — so successful repairs are
    // `reconstruction_repaired − conflict − aborted − ceiling_refused`. Every offset leaves
    // the obligation queued, to be re-assessed next pass.
    //
    // The under-replicated count is the *repairable backlog* level (the `Repairable` set,
    // which equals `plans.len()` this pass): it deliberately EXCLUDES both `Unrepairable`
    // (data-loss, raised on its own `reconstruction_data_loss` signal) and `Malformed` (a
    // distinct non-loss condition on its own metric) so that a never-repaired chunk cannot
    // floor it — the gauge can then return to zero once the real repairable losses are
    // repaired, which is the binding day-one signal. See the tally comment above.
    emit_under_replicated(under_replicated);
    // Both are LEVELS, emitted every pass (even at 0) so they rise while the condition holds
    // and return to zero when it clears — the same gauge discipline as the backlog count.
    emit_unreachable(unreachable_degraded);
    emit_repair_blocked(repair_blocked);
    for plan in &plans {
        emit_repaired(plan.chunk_id, plan.missing.len(), now_millis);
    }

    // The repair loop is the base's, chunk by chunk and in its priority order: ONE
    // version-conditional commit per repaired chunk, each built from and conditioned on the
    // generation THE SCAN returned — which is now the shared snapshot the reading holds
    // rather than a namespace scan of this obligation's own, and is the only thing that
    // changed here. So a chunk that lands is durable whatever a later one does, an urgent
    // repair is never run behind a less urgent one to keep its object's writes together, and
    // a second obligation inside the same object still loses the CAS it always lost (its
    // precondition is the generation the first repoint superseded) and stays queued for the
    // next pass — exactly as on `origin/main`, where every plan is likewise assessed before
    // any repair commits.
    //
    // A staged plan takes its turn in the same order, through its own fenced sequence
    // (`staged::repair`). **One degraded chunk per part per pass**, accepted for the same reason:
    // every plan inside one part pins the part record's bytes this pass read, as every plan
    // inside one object pins that object's generation, so a second plan in a part whose first
    // one was adopted loses its pre-mark to that adoption, writes nothing, and is repaired on
    // the next pass.
    let mut changed = false;
    // A repair this pass REFUSED because the repointed record would not survive the backend
    // value ceiling: it wrote nothing at all, the obligation stays queued, and — exactly like
    // the segmented refusal the reading records — the pass may not certify over it.
    let mut ceiling_refused = false;
    for plan in &plans {
        let outcome = match &plan.target {
            Target::Committed(object) => {
                repair_chunk(ctx, &stores, &reading.objects[*object], plan, now_millis).await?
            }
            Target::Staged(target) => staged::repair(ctx, &stores, &staged, target, plan).await?,
        };
        match outcome {
            RepairOutcome::Committed => changed = true,
            RepairOutcome::Conflict => emit_conflict(plan.chunk_id),
            RepairOutcome::Aborted => emit_aborted(plan.chunk_id),
            RepairOutcome::Refused { bytes, ceiling } => {
                ceiling_refused = true;
                emit_ceiling_refused(plan.chunk_id, bytes, ceiling);
            }
        }
    }

    // Drain the no-op obligations in one commit (best-effort; not the binding repoint).
    //
    // **While this pass's reading is INCOMPLETE it drains nothing.** Both drain paths — "no
    // committed map references this chunk" (`assess`'s miss) and "already at full
    // redundancy" — are conclusions over the WHOLE committed namespace, and an object this
    // pass could not read is a hole in it: "I could not read the map" and "no committed map
    // references this chunk" are different facts, and only the second may discard an
    // obligation. Discarding one on the first is silent data loss — the obligation is the
    // last record saying live data is under-replicated. One rule over the ONE batch both
    // paths flow into, so no site can drift from it; over a complete reading both behave
    // exactly as they always have.
    //
    // **Extended to the staged reading the same way.** `drain_only` already excludes every
    // chunk a staged record still names (`assess`'s own check, before `Drain`) — but while the
    // staged reading itself is incomplete, this pass cannot show ANY of `drain_only`'s chunks
    // is named by NO staged record either, so "no committed map references it" is once again
    // not "no record names it" for every one of them, not just the ones a readable staged
    // record happened to name.
    if !reading.incomplete && !staged_incomplete && !drain_only.is_empty() {
        let mut batch = WriteBatch::new();
        for chunk in drain_only {
            batch = batch.delete(repair::repair_key(chunk));
        }
        ctx.meta.commit(batch).await?;
    }

    // A hole in what this pass may claim: an object it could not read, a staged record it
    // could not read, a repair it may not perform, an obligation it kept queued on a staged
    // record's word rather than a committed one, or one it refused because the record it
    // would leave behind could not survive.
    let hole = reading.incomplete
        || staged_incomplete
        || !reading.refused.is_empty()
        || staged_kept
        || ceiling_refused;
    Ok(if hole {
        // **This pass certifies only over the reading it performed.** It either could not
        // read every committed object, or held back a repair it may not perform — either way
        // its picture of the store's redundancy has a hole in it. What it DID repair is
        // durable regardless; what answering `Satisfied` would destroy is the only signal
        // that the picture is partial, and an operator reading `Satisfied` is being told
        // redundancy is restored and will act on it (`docs/principles.md` §5 C-1). The same
        // rule, in the same word, GC and scrub already answer over an incomplete reference
        // set (`gc.rs:234-241`).
        Reconciled::Blocked
    } else if changed {
        Reconciled::Changed
    } else {
        Reconciled::Satisfied
    })
}

/// This pass's **one** reading of the committed namespace: where each *queued* chunk's
/// committed reference lives, and what the reading could not do.
///
/// Bounded by the obligations held and by one object at a time — never the whole namespace's
/// decoded chunk lists: an object is held only if this pass actually owes a repair inside it,
/// and then exactly **once**, however many obligations fall in it.
#[derive(Default)]
struct Reading {
    /// One entry per committed **flat** object this pass owes a repair inside: the scanned
    /// generation, held once and SHARED by every obligation that falls in it.
    objects: Vec<FlatObject>,
    /// The committed reference this pass acts on for each queued chunk. **Absent** means no
    /// committed chunk map references it — the chunk was deleted out from under the
    /// obligation, which is the only fact that permits discarding one.
    sites: HashMap<ChunkId, Site>,
    /// At least one committed object could not be read at all, so this reading has a HOLE in
    /// it: every conclusion drawn over the whole namespace (both drain paths) is withheld and
    /// the pass cannot certify. Each such object is named on the audit seam the moment it is
    /// met — see [`read_committed`].
    incomplete: bool,
    /// Committed objects holding a queued chunk whose reference lives in a `seg:` record,
    /// keyed by the store's own key bytes so a refusal is counted and named exactly ONCE PER
    /// OBJECT: two obligations inside one segmented object are one refusal, not two. Ordered
    /// (the store's own byte order), so the audit trail is deterministic.
    refused: BTreeSet<Vec<u8>>,
}

impl Reading {
    /// Contain one committed object this reading could not read: name it for the operator
    /// where it was met, and record that the reading now has a hole in it.
    fn contain(&mut self, key: &[u8], fault: &str) {
        emit_unresolvable(&object_name(key), fault);
        self.incomplete = true;
    }
}

/// One committed **flat** object as the scan returned it — the generation every repair inside
/// it is built from and conditioned on, held ONCE for the whole pass.
struct FlatObject {
    /// Parsed from the scanned key, exactly as the per-obligation scan this walk replaces
    /// parsed it; the repair CASes under a key re-derived from it. A row under a
    /// non-canonical spelling would then be read at one key and written at another — real,
    /// pre-existing, unreachable while [`metadata::inode_key`] is the sole writer of the
    /// prefix, and tracked as #698. Identity is a property of the OBJECT, so it is held here
    /// once beside the record it names rather than copied into every obligation's plan.
    inode_id: InodeId,
    /// The scanned record, whole: the CAS precondition and the object metadata a repair
    /// preserves (ADR-0047) are both taken from it. Its `chunk_map` is flat — the reading
    /// admits no other shape here.
    prior: InodeRecord,
}

/// Where a queued chunk's **first** committed reference in key order was found — the same one
/// reference the base's own scan chose (a duplicate committed id is #700's).
enum Site {
    /// A **flat** committed generation: repairable.
    Flat(FlatSite),
    /// A **segmented** committed generation. The segmented write path is #682's, so this pass
    /// REFUSES the repair: it writes nothing at all, keeps the obligation, and does not
    /// certify. Never a drain — a refusal is "I may not repair this", not "nothing references
    /// this chunk".
    Refused,
}

/// One queued chunk's place in this reading: which shared snapshot holds it, where, and its
/// own committed reference. The only per-obligation material, and O(1) in the object's size.
struct FlatSite {
    /// Index into [`Reading::objects`] — the ONE snapshot of that object.
    object: usize,
    /// This chunk's index within that generation's own flat chunk list.
    index: usize,
    /// This chunk's committed reference (scheme, length, placement), copied out of that list
    /// so the assessment reads it without re-deriving the map's shape.
    chunk_ref: ChunkRef,
}

/// Read the committed namespace **once**, resolving every committed object through the ONE
/// resolver every consumer shares ([`metadata::resolve_chunk_map`], proposal 0016 decision
/// 7(e)) — the same walk `gc::referenced_fragments` (`gc.rs:360-416`) and
/// `restore::committed_chunks` (`restore.rs:621-658`) already make over the same records,
/// contained by exactly their downcast rule and no other.
///
/// **One damaged object does not end the walk.** A record that will not decode, or a
/// generation the resolver cannot read on a root that still names it ([`ChunkMapError`]),
/// marks the reading incomplete and the walk goes on, so every other obligation is still
/// answered. `Ok(None)` — no live committed generation left under this key — is skipped
/// exactly as both merged peers skip it (`gc.rs:404`, `restore.rs:646`). A fault that is
/// **not** this object's own — a store failing underneath the read — still propagates: a walk
/// that cannot reach the metadata store has no reading at all, and containing that as "one
/// object is unreadable" would be the wrong answer for every object in it.
///
/// deferred: #702 — whether `Ok(None)` for a key THIS scan saw `Committed` (the object retired
/// under the read) is a hole rather than a skip is one answer for all four loops, not this
/// one's to change alone: `gc.rs:404` and `restore.rs:646` read the same answer the same way.
///
/// Each unreadable object is named **where it is met**, not batched for the caller to emit: a
/// store fault a `?` later ends the pass with an `Err`, and a name this pass already held must
/// not go down with it (`gc.rs:155-166`). That is load-bearing rather than logging hygiene — a
/// genuinely corrupt root has no repair path and no operator tooling yet (#694), so the
/// record's name is the operator's whole situational awareness.
///
/// The network bound on the resolve await is the `MetadataStore` IMPLEMENTATION's, not this
/// caller's (#508/#636) — the same rule both merged peers follow for the same call
/// (`gc.rs:394-401`, `restore.rs:604-608`), and the same rule this loop's own
/// `meta.scan(b"inode:")` has always followed. It is fail-closed either way: an error there
/// either propagates or contains the object — it is never read as "this object owns no
/// chunks".
async fn read_committed(meta: &dyn MetadataStore, queue: &[ChunkId]) -> Result<Reading> {
    let owed: HashSet<ChunkId> = queue.iter().copied().collect();
    let mut reading = Reading::default();
    for (key, value) in meta.scan(b"inode:").await? {
        // The record's own bytes are already in hand, so a decode failure is THIS object's
        // fault and no store's — contained, and conservatively WITHOUT first asking whether
        // the record was committed: reading `state` out of bytes that will not decode needs a
        // lenient peek, and this loop holds the ADR-0010 boundary of `traits` / `core` /
        // `tracing` and owns no decoder of its own to do it with.
        let record: InodeRecord = match metadata::decode(&value) {
            Ok(record) => record,
            Err(fault) => {
                reading.contain(&key, &fault.to_string());
                continue;
            }
        };
        if record.state != InodeState::Committed {
            continue;
        }
        let resolved = match metadata::resolve_chunk_map(meta, &key, &record).await {
            Ok(Some(resolved)) => resolved,
            Ok(None) => continue,
            Err(err) => match err.downcast::<ChunkMapError>() {
                // The resolver's own typed verdict that THIS generation cannot be read —
                // recovered by downcast because the trait seam boxes every error. Contained.
                Ok(fault) => {
                    reading.contain(&key, &fault.to_string());
                    continue;
                }
                // Not a chunk-map anomaly: a store fault under the read. Not this object's
                // fault, so it is not folded into "this object is unreadable".
                Err(err) => return Err(err),
            },
        };
        // Whether this pass may write for the object is decided off **the generation the scan
        // returned** — its own `chunk_map`, already in hand — never off the shape a resolve
        // answered after restarting onto a newer root. A flat snapshot resolves to a borrow of
        // the record and reads nothing, so it can never be superseded and never restarts
        // (`crates/core/src/metadata.rs:2585`); only a segmented snapshot can, and a segmented
        // snapshot is one this pass refuses. So the restart path reaches no write at all, by
        // construction.
        let flat = match (record.chunk_map.as_flat(), parse_inode_key(&key)) {
            (Some(_), Some(inode_id)) => Some(inode_id),
            // A record whose key this pass cannot parse claims nothing and the walk goes on —
            // the `if let Some(inode_id) = parse_inode_key(&key)` the per-obligation scan made
            // here, moved with the walk and unchanged in meaning (#698 owns the fix).
            (Some(_), None) => continue,
            (None, _) => None,
        };
        // The ONE snapshot of this object, allocated on the first chunk it is owed a repair on
        // and shared by every later one: Q obligations inside an N-entry object cost N entries
        // once, never Q×N. An object owed nothing is not held at all.
        let mut object = None;
        // `index` addresses the SCANNED generation's own list too, for the only shape this
        // pass writes for: a flat snapshot resolves to a borrow of that very list, so the two
        // are one slice. A segmented one is refused, and a refusal indexes nothing.
        for (index, chunk) in resolved.chunks.iter().enumerate() {
            // Nothing is owed on this chunk, or an earlier object in key order already claimed
            // it: the FIRST committed reference wins, exactly the one the base's own scan
            // chose.
            if !owed.contains(&chunk.id) || reading.sites.contains_key(&chunk.id) {
                continue;
            }
            let site = match flat {
                Some(inode_id) => {
                    let at = *object.get_or_insert_with(|| {
                        reading.objects.push(FlatObject {
                            inode_id,
                            prior: record.clone(),
                        });
                        reading.objects.len() - 1
                    });
                    Site::Flat(FlatSite {
                        object: at,
                        index,
                        chunk_ref: chunk.clone(),
                    })
                }
                None => {
                    // Per OBJECT, before the work loop: the second obligation inside the same
                    // segmented object adds no row and no count.
                    if reading.refused.insert(key.clone()) {
                        emit_refused(&object_name(&key));
                    }
                    Site::Refused
                }
            };
            reading.sites.insert(chunk.id, site);
        }
    }
    Ok(reading)
}

/// The outcome of assessing one queued obligation.
enum Assessment {
    /// A reconstructable under-replicated chunk, with its survivors already gathered.
    Repairable(Box<RepairPlan>),
    /// Nothing to rebuild — drain the obligation (deleted chunk, or already healthy).
    Drain,
    /// Below `k` intact fragments only because one or more placed D servers are
    /// **transiently unreachable** this pass (dropped from the fleet by the reachability
    /// probe) — counting those fragments as present would reach `k`. The data is NOT
    /// confirmed lost; it recovers when the servers return. Raised on the distinct,
    /// lower-severity `reconstruction_unreachable` signal, never the data-loss counter
    /// (iteration-7 MUST-FIX). The obligation stays queued for re-assessment.
    Unreachable,
    /// Cannot be reconstructed in this slice (below `k` **confirmed-gone** fragments, or a
    /// no-redundancy scheme).
    Unrepairable,
    /// Repairable in principle (survivors ≥ `k`) but **no free failure domain** distinct
    /// from the survivors remains to place the rebuilt shard(s) this pass (a minimal cluster
    /// at exactly `n` with a domain down). Left off the repairable-backlog gauge — a
    /// never-completable repair counted there would floor the day-one "returns to zero"
    /// signal at ≥1 forever (iteration-7 MUST-FIX) — and raised on the distinct
    /// `reconstruction_repair_blocked` level. It clears when capacity returns; the
    /// obligation stays queued.
    Blocked,
    /// The committed placement is **malformed** (non-empty, wrong length): rebuilding
    /// over its fabricated identity tail is forbidden (ADR-0040 decision 4). Skip the
    /// chunk and flag it NEEDS-HUMAN; the obligation stays queued.
    Malformed,
    /// The chunk's committed reference lives in a **`seg:` record**, whose write path is
    /// #682's: this pass may not perform the repair, so it **refuses** it — the segmented
    /// record is left byte-identical, the obligation stays queued, and the pass does not
    /// certify. Named and counted once per *object* by [`read_committed`], and kept off the
    /// repairable-backlog gauge like every other never-repaired condition.
    Refused,
    /// No committed chunk map references this chunk, a **staged multipart record** still does
    /// (`staged_chunks`, `reconcile`'s own read, before `inode:` is committed), and this pass
    /// may not repair it: only an in-flight owned staging entry names it (its part has no
    /// committed scheme to rebuild from yet, `0016:776-781`), its upload has left `Open` (a
    /// Complete, an Abort or a reaper fenced it; the repair runs once the chunk is published,
    /// `0016:825`), or a staged record holds it with a placement that cannot be used. A
    /// committed part's chunk in an `Open` upload is not this: it is assessed and repaired
    /// ([`staged`]). This pass only refuses to discard the one record still saying the chunk is
    /// short a fragment. Like [`Self::Refused`] the pass does not certify, and the obligation
    /// stays queued — the Invariant to restore: "I could not read a record" never counts as
    /// "no record names it", and neither does "no COMMITTED record names it" while a staged
    /// one still does.
    Staged,
    /// A staged repair **withheld** because a record it would have to rewrite or pin cannot be
    /// read — the upload's session record, or the `orphan:` mark at a position the move would
    /// vacate — so it is never rewritten (ADR-0045). Nothing is written, the obligation stays
    /// queued, the record is named NEEDS-HUMAN where it was met, and the pass does not certify,
    /// as for [`Self::Staged`].
    Withheld,
}

/// Locate `chunk` in **this pass's own reading** of the committed namespace — never a scan of
/// its own — then gather and **verify** its surviving fragments, classifying it into an
/// [`Assessment`]. `staged` is this pass's reading of the staged classes, and `staged_chunks`
/// the chunks any staged record names or holds (`reconcile`'s own read, taken before the
/// committed one) — consulted only when the committed reading finds no site at all, so a
/// chunk WITH a committed reference is assessed exactly as before regardless of any staged
/// record's leftover.
async fn assess(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    reading: &Reading,
    staged: &staged::StagedReading,
    staged_chunks: &HashSet<ChunkId>,
    chunk: ChunkId,
) -> Result<Assessment> {
    let site = match reading.sites.get(&chunk) {
        Some(Site::Flat(site)) => site,
        // Refused, not repaired and not discarded: the reference is in a `seg:` record.
        Some(Site::Refused) => return Ok(Assessment::Refused),
        // No COMMITTED chunk map references this chunk. A staged record may still name it —
        // checked BEFORE concluding "deleted, drain it": the staged reading ran first
        // (`reconcile`), specifically so a publish racing this pass cannot make both reads
        // miss it (`0016:782-800`).
        //
        // A staged record that names the chunk with a placement no reader can trust holds it
        // whole (`crate::gc::StagedSet::held`): nothing is rebuilt over a placement that
        // cannot be trusted, whatever else names the chunk.
        None if staged.set.held.contains_key(&chunk) => {
            emit_staged(chunk, "untrusted-staged-record");
            return Ok(Assessment::Staged);
        }
        None => {
            // A committed part names it: assessed, and repaired while its upload is `Open`.
            if let Some(assessment) = staged::assess(ctx, stores, staged, chunk).await? {
                return Ok(assessment);
            }
            // Only an owned staging entry names it: its part is still streaming, with no
            // committed scheme to rebuild from (`0016:776-781`).
            if staged_chunks.contains(&chunk) {
                emit_staged(chunk, "in-flight");
                return Ok(Assessment::Staged);
            }
            // Referenced by no committed chunk map and no staged record either — it was
            // deleted out from under the obligation. Nothing to repair. Acted on only over a
            // COMPLETE reading of BOTH classes: `reconcile` gates the one batch every drain
            // path flows into.
            return Ok(Assessment::Drain);
        }
    };
    // The reading proved this generation's own map is flat and carried this chunk's own
    // reference along, so the shape is settled before the assessment starts rather than
    // re-derived here — a segmented map never reaches this point and so can never end the
    // pass. Everything below is O(1) in the object's size: the snapshot itself stays in the
    // reading, shared by every other obligation inside it.
    let chunk_ref = &site.chunk_ref;

    // Classify the committed placement BEFORE any scheme-specific handling
    // (ADR-0040 decision 4, "strict maintenance"). A MALFORMED vector (non-empty,
    // wrong length) is rejected here — for EVERY scheme, single-fragment `EcScheme::None`
    // included — so the loop flags it NEEDS-HUMAN rather than letting it pass silently.
    // This must run ahead of the scheme match: a malformed `None` placement (e.g. a
    // len>=2 vector on a `fragment_count() == 1` chunk) can only mean truncation /
    // corruption, and classifying scheme-first would return `Unrepairable` (silent) and
    // leave reconstruction the lone maintenance loop that never surfaces it. A valid
    // (empty / full-length) vector resolves through the shared strict companion
    // (`ChunkRef::checked_fragments`, `metadata.rs`) exactly as the read path and GC
    // resolve it, so a pre-M3 record resolves identically everywhere.
    let placement: Vec<DServerId> = match chunk_ref.checked_fragments() {
        Ok(frags) => frags.map(|(_, dserver)| dserver).collect(),
        Err(_) => return Ok(Assessment::Malformed),
    };

    let (k, m) = match chunk_ref.scheme {
        // A single-fragment chunk has no redundancy to reconstruct from; recovering it
        // is a replica-copy concern, not erasure reconstruction (out of scope here).
        EcScheme::None => return Ok(Assessment::Unrepairable),
        EcScheme::ReedSolomon { k, m } => (k as usize, m as usize),
    };

    let gathered = gather(ctx, stores, chunk, &placement, chunk_ref.scheme).await?;
    if let Some(settled) = gathered.settle(k) {
        return Ok(settled);
    }
    // Repairable in principle (survivors ≥ `k`). But the rebuild must PLACE each missing shard
    // in a failure domain distinct from every survivor's; if no free distinct domain remains
    // (a minimal cluster at exactly `n` with a domain down), the repair cannot proceed this
    // pass. Route it to the distinct `Blocked` signal rather than the repairable-backlog gauge
    // — a never-completable repair counted there would floor the day-one "returns to zero"
    // gauge at ≥1 forever (iteration-7 MUST-FIX). It becomes repairable again when a free
    // distinct domain (capacity) returns. This mirrors the selector `repair_chunk` runs, so a
    // chunk that WOULD abort in the repair loop is diverted before it inflates the backlog.
    if select_distinct_domains_excluding(
        ctx.topology,
        gathered.missing.len() as u16,
        &gathered.survivor_domains,
    )
    .is_err()
    {
        return Ok(Assessment::Blocked);
    }

    Ok(Assessment::Repairable(Box::new(RepairPlan {
        target: Target::Committed(site.object),
        chunk_index: site.index,
        chunk_id: chunk,
        k,
        m,
        survivors: gathered.survivors,
        survivor_domains: gathered.survivor_domains,
        missing: gathered.missing,
        placement,
        len: chunk_ref.len as usize,
    })))
}

/// One chunk's placed fragments, fetched and verified ([`gather`]).
struct Gathered {
    /// `(fragment_index, decoded shard bytes)` for each intact survivor.
    survivors: Vec<(usize, Vec<u8>)>,
    /// The failure domains the survivors occupy (to keep the rebuild disjoint).
    survivor_domains: Vec<FailureDomain>,
    /// Fragment indices that are missing or checksum-failing (to be rebuilt).
    missing: Vec<usize>,
    /// How many missing fragments are missing ONLY because their placed D server is
    /// configured-but-unreachable this pass (`ctx.unreachable`) — transiently unavailable, not
    /// confirmed lost. Used by [`Gathered::settle`] to distinguish a recoverable degraded state
    /// from real data loss (iteration-7 MUST-FIX). Zero in the M3 library contexts (empty
    /// `unreachable`), so a missing fragment is always `confirmed gone` there — prior semantics
    /// unchanged.
    transient_missing: usize,
}

impl Gathered {
    /// What this gathering decides on its own, before any destination is chosen — or `None`
    /// while the chunk is repairable in principle (survivors ≥ `k`, something missing). One
    /// rule for a committed chunk ([`assess`]) and a staged one ([`staged::assess`]).
    fn settle(&self, k: usize) -> Option<Assessment> {
        if self.missing.is_empty() {
            // Already at full redundancy: a stale / duplicate obligation. Drain it.
            return Some(Assessment::Drain);
        }
        if self.survivors.len() < k {
            // Below the scheme's tolerance. Distinguish a REACHABILITY-driven shortfall from a
            // CONFIRMED one: fragments missing only because their D server is transiently
            // unreachable this pass return with the server, so counting them as present may
            // reach `k`. Only when even then the chunk is still below `k` is data confirmably
            // LOST (iteration-7 MUST-FIX) — otherwise a rolling restart / partition would
            // falsely page as permanent data loss on physically-intact fragments.
            if self.survivors.len() + self.transient_missing >= k {
                return Some(Assessment::Unreachable);
            }
            return Some(Assessment::Unrepairable);
        }
        None
    }
}

/// Fetch and **verify** each fragment of `chunk` at `placement` under `scheme`: the gather step
/// of every repair (`0005:275`), committed ([`assess`]) or staged ([`staged::assess`]).
async fn gather(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    chunk: ChunkId,
    placement: &[DServerId],
    scheme: EcScheme,
) -> Result<Gathered> {
    let mut survivors = Vec::new();
    let mut survivor_domains = Vec::new();
    let mut missing = Vec::new();
    let mut transient_missing = 0usize;
    for (index, &dserver) in placement.iter().enumerate() {
        let frag = FragmentId {
            chunk,
            index: index as u16,
        };
        let bytes = match stores.get(&dserver) {
            // Fetch the placed fragment, classifying a fetch fault by the seam's
            // permanent-loss-vs-transient distinction (ADR-0010, the `IntegrityFault`
            // contract; the same split `scrub.rs:102` and the read path `read.rs:189`
            // honour). A PERMANENT durability fault — a corruption / integrity fault, or
            // a block-layer read fault (`EIO` / dead sector): the device cannot return
            // the bytes — is read AROUND (treated as a missing shard below and rebuilt
            // from the >=k survivors), so one faulted placed fragment never propagates
            // out of the assessment and aborts the shared per-chunk drain. A TRANSIENT
            // fault (unreachable / timed out / busy on a healthy server) carries no
            // durability signal: propagate it to the retry policy rather than silently
            // converting a reachable fragment into permanent loss / a re-placement.
            Some(store) => match store.get_fragment(frag).await {
                Ok(bytes) => bytes,
                Err(e) if is_permanent_read_fault(e.as_ref()) => None,
                Err(e) => return Err(e),
            },
            None => None,
        };
        // VERIFY: a present fragment must decode cleanly AND prove the FULL identity
        // this slot expects — chunk id, `ec_fragment_index`, and the committed EC tuple;
        // a checksum-failing, misplaced, or misencoded fragment is excluded (never
        // decoded) and treated as missing (`0005:275`). `repair::intact_shard` is the
        // shared verify, so `custodian` recovers the shard without a chunk-format
        // dependency.
        match bytes
            .as_deref()
            .and_then(|b| repair::intact_shard(b, frag, scheme))
        {
            Some(shard) => {
                survivors.push((index, shard));
                if let Some(domain) = ctx.topology.domain_of(dserver) {
                    survivor_domains.push(domain.clone());
                }
            }
            None => {
                // Missing. If the placed server is one the role dropped this pass as
                // unreachable, this absence is TRANSIENT (recoverable), not a confirmed loss.
                if ctx.unreachable.contains(&dserver) {
                    transient_missing += 1;
                }
                missing.push(index);
            }
        }
    }
    Ok(Gathered {
        survivors,
        survivor_domains,
        missing,
        transient_missing,
    })
}

/// POSIX `EIO` (errno 5) — the OS errno a block-layer read fault raises (a dead sector,
/// a `dm-error` target): the device physically could not return the bytes. Standardised
/// across the Unix platforms Wyrd targets; named here rather than pulled from `libc` to
/// keep the loop's dependency surface unchanged (ADR-0010).
const EIO: i32 = 5;

/// Classify a `get_fragment` fault on a **placed** fragment as a *permanent durability
/// fault* — one where the device cannot return the bytes, so the rebuild reads around it
/// and reconstructs from the >=`k` survivors (the [`wyrd_traits::IntegrityFault`] seam
/// contract, ADR-0010; the same permanent-vs-transient split `scrub.rs:102` and the read
/// path `read.rs:189` honour). Two permanent shapes:
///
/// * a **corruption / integrity** fault ([`wyrd_traits::IntegrityFault`]): the stored
///   bytes failed their self-describing checksum, so retrying the same fetch cannot heal
///   them; and
/// * a **block-layer read fault** (`EIO` — a dead sector / `dm-error`): the OS reported
///   the read itself failed at the device.
///
/// A **transient** fault (unreachable / timed out / busy on a healthy server) matches
/// NEITHER, so [`assess`] propagates it to the retry policy and never converts a reachable
/// fragment into permanent loss / a re-placement.
fn is_permanent_read_fault(err: &(dyn std::error::Error + 'static)) -> bool {
    wyrd_traits::is_integrity_fault(err) || is_block_read_fault(err)
}

/// Whether `err`'s source chain carries a block-layer read fault (an `EIO` `io::Error`).
/// Walks [`source`](std::error::Error::source) so the fault is found whether the backend
/// surfaces the raw `io::Error` at the top of the box — the shape `chunkstore-fs` produces,
/// `Err(e.into())` boxing the `fs::read` error directly
/// (`crates/chunkstore-fs/src/lib.rs:241`) — or **wraps** it inside its own error type and
/// exposes it via `source()`, mirroring how [`wyrd_traits::is_integrity_fault`] walks the
/// chain for a corruption fault.
fn is_block_read_fault(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(err);
    while let Some(e) = next {
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if io.raw_os_error() == Some(EIO) {
                return true;
            }
        }
        next = e.source();
    }
    false
}

/// The outcome of repairing one chunk — reported back to [`reconcile`] so the metric /
/// audit emission stays in that frame.
enum RepairOutcome {
    /// The version-conditional commit landed; the rebuilt shard(s) were re-placed.
    Committed,
    /// The commit lost the CAS race (rebuilt fragments are now collectable garbage).
    Conflict,
    /// The repair could not proceed (e.g. the selector chose a server outside the
    /// fleet view); nothing was committed. Offset on `reconstruction_aborted` so an
    /// aborted plan is not mistaken for a success (see [`emit_aborted`]). Every cause here
    /// is **transient**: it clears when the server returns to this pass's fleet view — or,
    /// for a staged re-place ([`staged::repair`]), when the next pass starts the move again
    /// from a fresh pre-mark: its pre-mark lost its precondition, its pre-mark aged past
    /// `W_repoint` before a write was authorized, or the D server refused its write past the
    /// deadline or could not certify it landed in time. Whatever such a move wrote stays
    /// under its pre-mark.
    Aborted,
    /// The repoint would have crossed the backend value ceiling
    /// ([`metadata::flat_value_ceiling_crossed`]), so it was refused before anything at all
    /// was written — no rebuilt fragment, no record. Distinct from [`Self::Conflict`] and
    /// [`Self::Aborted`] precisely because those are transient: this shape fails again every
    /// pass until the record shrinks, so it is the object's own defect, an operator signal,
    /// and — like the segmented refusal — a repair this pass may not certify over
    /// (see [`emit_ceiling_refused`]).
    Refused {
        /// The re-encoded record's own length.
        bytes: usize,
        /// The ceiling it crossed.
        ceiling: usize,
    },
}

/// Rebuild `plan`'s missing fragment(s), re-place them in distinct failure domains, and
/// repoint the chunk's placement record with **one version-conditional commit**.
///
/// `object` is the generation THIS PASS'S ONE READING returned for the chunk's committed
/// object — the same snapshot the base's own per-obligation scan handed this function, shared
/// rather than re-scanned and re-copied per obligation. Everything the commit is built from
/// and conditioned on is taken from it, so the write is decided by the generation the scan
/// returned and by nothing a resolve answered after restarting onto a newer root.
async fn repair_chunk(
    ctx: &ReconstructionContext<'_>,
    stores: &HashMap<DServerId, &dyn ChunkStore>,
    object: &FlatObject,
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

    // Resolve where every rebuilt fragment goes and encode it, WITHOUT writing any of them
    // yet.
    //
    // The failure in this loop is TRANSIENT (a target outside this pass's fleet view): the
    // repair aborts and is re-assessed next pass. It is resolved BEFORE the ceiling refusal
    // below so that a repair which could not have been placed anyway is never reported as the
    // permanent "this record must shrink" defect — a compound failure is named by the
    // recoverable cause, not by the one that pages a human. The refusal still runs before any
    // write.
    let mut new_placement = plan.placement.clone();
    let mut displaced = Vec::new();
    let mut writes = Vec::new();
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
        writes.push((*target_store, frag, frag_bytes));

        let old = plan.placement[index];
        if old != target {
            displaced.push((old, frag));
        }
        new_placement[index] = target;
    }

    // The record THE binding commit below would leave behind, built here from the rebuild's
    // own targets — no store touched yet — so the repair is judged on it before anything is
    // written. That commit is ONE version-conditional mutation that atomically repoints the
    // placement record, drains the obligation, and orphans the displaced fragments; the CAS on
    // the prior inode record is the second fence (`0005:200-203`, ADR-0015), so a racing
    // writer / superseded custodian loses there rather than corrupting the record.
    let Some(prior_chunk_map) = object.prior.chunk_map.as_flat() else {
        // Unreachable by construction — a plan exists only for a generation the reading found
        // FLAT, and the reading is the only producer of one. Fail-SAFE rather than fatal all
        // the same: nothing is committed, the obligation stays queued for the next pass, and
        // the abort is offset on `reconstruction_aborted` — never a repoint of a map this
        // pass cannot read, and never the whole-store abort this slice exists to remove.
        return Ok(RepairOutcome::Aborted);
    };
    let mut next_chunk_map = prior_chunk_map.to_vec();
    next_chunk_map[plan.chunk_index].placement = new_placement;
    let next = InodeRecord {
        size: object.prior.size,
        chunk_map: next_chunk_map.into(),
        state: InodeState::Committed,
        version: object.prior.version + 1,
        // Reconstruction rebuilds the SAME content, so it PRESERVES the object metadata
        // (ADR-0047): a repair commit must not move `Last-Modified` or drop the content
        // type.
        ..object.prior.clone()
    };

    // REFUSE, AND WRITE NOTHING AT ALL. A repoint whose re-encoded record would cross the
    // value ceiling the tightest backend enforces must never be attempted: on a store with
    // native enforcement it returns a raw `Err` indistinguishable from a transient fault,
    // and on one without it, it COMMITS a record every later repair of the object then fails
    // to overwrite (`crates/core/src/metadata.rs:333-341`). Judged here — after the transient
    // check above, and still ahead of the fragment writes below — so a refusal leaves no
    // unreferenced shard for GC to hold with no grace evidence for it. The very bytes weighed
    // are the bytes committed, so no re-encode can drift past the check.
    let next_bytes = metadata::encode(&next);
    if let Some(ceiling) = metadata::flat_value_ceiling_crossed(&next_bytes) {
        return Ok(RepairOutcome::Refused {
            bytes: next_bytes.len(),
            ceiling,
        });
    }

    // Write the rebuilt fragments to their new D servers FIRST — before the commit, so a
    // crash here leaves only collectable garbage, never a torn chunk (`0005:277`).
    for (target_store, frag, frag_bytes) in writes {
        target_store.put_fragment(frag, frag_bytes, None).await?;
    }

    let inode_key = metadata::inode_key(object.inode_id);
    let mut batch = WriteBatch::new()
        .require(inode_key.clone(), metadata::encode(&object.prior))
        .put(inode_key, next_bytes)
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

/// Emit the **under-replicated chunk count** (`0005:326-329`): the *repairable backlog* —
/// every chunk this pass found below its scheme's fragment count that reconstruction can
/// still rebuild (`Repairable`, survivors ≥ `k`) — the metric whose silent non-zero value
/// is the durability failure the request plane hides. It counts ONLY the auto-repairable
/// set: an un-reconstructable loss (`Unrepairable`) is a *more* severe data-loss event
/// raised on its own [`emit_data_loss`] signal, and a corrupt placement (`Malformed`) is a
/// distinct non-loss condition on [`emit_needs_human`] — neither is folded in here, because
/// neither is ever drained by this loop and either would floor this gauge above zero forever.
///
/// This is a **gauge**, not a monotonic counter: it is a *level* — the number of chunks
/// currently under-replicated (and repairable) as of this pass — so it RISES when a loss is
/// injected and
/// RETURNS TO ZERO once repair restores redundancy. That rise-then-zero shape is the
/// day-one durability signal (observability-floor proposal 0010; architecture §7.4 step
/// 4: kill a D server, watch the under-replicated count rise then settle to zero) and it
/// must be observable through the **real export surface** a deployment scrapes
/// ([`wyrd_telemetry::DurabilityTelemetry::gather_prometheus`]). A monotonic counter
/// cannot express it: through an accumulating Prometheus registry, `add(1)` then `add(0)`
/// stays pinned at 1 and never returns to zero — a repaired zone would read permanently
/// degraded. A gauge set to the current level reads back 1 then 0, which is the signal
/// (`0005:400-403`, ADR-0011/0012).
fn emit_under_replicated(count: usize) {
    tracing::info!(gauge.reconstruction_under_replicated = count as u64);
}

/// Emit the **unreachable-degraded chunk count**: chunks currently below their scheme's
/// tolerance ONLY because one or more placed D servers are transiently unreachable this pass
/// (dropped by the role's reachability probe), not because fragments are confirmed lost. It is
/// a **distinct, lower-severity** signal from [`emit_data_loss`]'s `reconstruction_data_loss`
/// — a rolling restart / partition that recovers when the servers return, NOT a page for lost
/// data (iteration-7 MUST-FIX). A **gauge** (a level, like the backlog count): it rises while
/// servers are down and returns to zero when they return, so a transient outage never floors
/// the data-loss counter with false positives.
fn emit_unreachable(count: usize) {
    tracing::warn!(gauge.reconstruction_unreachable = count as u64);
}

/// Emit the **repair-blocked chunk count**: chunks that ARE repairable (survivors ≥ `k`) but
/// for which no free failure domain distinct from the survivors remains to place the rebuilt
/// shard(s) this pass (a minimal cluster at exactly `n` with a domain down). A **distinct
/// level** from the repairable-backlog gauge: a never-completable repair left on the backlog
/// gauge would floor the binding day-one "returns to zero" signal at ≥1 forever (iteration-7
/// MUST-FIX). It clears when capacity (a free distinct domain) returns.
fn emit_repair_blocked(count: usize) {
    tracing::warn!(gauge.reconstruction_repair_blocked = count as u64);
}

/// Emit a **dispatched reconstruction** plus the **time-to-repair** sample (`0005:330`):
/// the metric + an append-only audit event (`0005:336-340`) for a chunk the pass is
/// reconstructing. The sample is the logical instant of the repair pass; a per-obligation
/// enqueue stamp (a precise elapsed window) is a later refinement of the shared queue's
/// value encoding. A dispatched repair that loses its CAS is recorded separately on
/// [`emit_conflict`], one that cannot proceed (no commit) on [`emit_aborted`], and one
/// refused for crossing the backend value ceiling on [`emit_ceiling_refused`], so
/// successful repairs are `reconstruction_repaired − conflict − aborted − ceiling_refused`.
fn emit_repaired(chunk: ChunkId, rebuilt: usize, now_millis: u64) {
    tracing::info!(monotonic_counter.reconstruction_repaired = 1_u64);
    tracing::info!(histogram.reconstruction_time_to_repair_millis = now_millis);
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "repair",
        chunk = %wyrd_traits::chunk_hex(chunk),
        rebuilt,
        "reconstruction is rebuilding the missing shard(s) and repointing the placement record",
    );
}

/// Emit a **NEEDS-HUMAN** signal on the durability-plane seam (ADR-0011 / ADR-0012,
/// ADR-0040 decision 4): reconstruction found a committed chunk whose `placement` vector
/// is non-empty but of the wrong length — truncation / corruption it must NOT rebuild
/// over. The chunk is skipped and its obligation left queued; a human resolves the corrupt
/// placement.
fn emit_needs_human(chunk: ChunkId) {
    tracing::warn!(monotonic_counter.reconstruction_malformed_placement = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "needs-human",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "reconstruction skipped a chunk with a malformed committed placement (wrong length); NEEDS-HUMAN, obligation left queued",
    );
}

/// Emit a **DATA-LOSS** signal on the durability-plane seam (ADR-0011 / ADR-0012): reconstruction
/// found a chunk with fewer than `k` intact fragments (or a no-redundancy `EcScheme::None` that
/// lost its single fragment) — it is **un-reconstructable**, so data that was meant to be durable
/// is actually LOST. This is the storage system failing its primary responsibility, the *most*
/// severe durability state — strictly worse than a repairable backlog or a malformed placement —
/// so it is raised at **`error` severity** on its OWN dedicated, distinct signal
/// (`reconstruction_data_loss`, a monotonic counter that keeps firing while the loss persists)
/// plus a NEEDS-HUMAN audit line, at least the parity [`emit_needs_human`] gives the malformed
/// case. It is deliberately kept OFF the `reconstruction_under_replicated` gauge: that gauge is a
/// repairable-backlog *level* which must return to zero once the auto-repairable losses are
/// repaired (the binding day-one signal), and a never-repaired chunk counted there would floor it
/// above zero forever. The obligation is left queued so the loss stays visible to a human /
/// out-of-band recovery.
fn emit_data_loss(chunk: ChunkId) {
    tracing::error!(monotonic_counter.reconstruction_data_loss = 1_u64);
    tracing::error!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "data-loss",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "reconstruction found a chunk with fewer than k intact fragments — un-reconstructable, DATA IS LOST; NEEDS-HUMAN, obligation left queued for out-of-band recovery",
    );
}

/// Emit a committed object whose chunk map this pass could **not read** on the
/// durability-plane seam (ADR-0011 / ADR-0012): its chunks are unknown, so this pass's reading
/// of the namespace has a hole in it — it drains NOTHING and certifies NOTHING until that
/// record is repaired.
///
/// The **same action string** gc, restore, scrub and the drain-status surface already publish
/// for the same condition (`gc.rs:564-567`, `restore.rs:827-830`, `scrub.rs:230-233`,
/// `desired_state.rs:260-263`), each with its own `<loop>_unresolvable_records` counter, so one
/// grep over the durability seam finds every loop blocked on one damaged record. Named through
/// [`crate::gc::object_name`], which escapes rather than replaces — two damaged records must
/// never arrive under one name, or a repair guided by it fixes one and leaves the other
/// blocking the fleet.
fn emit_unresolvable(object: &str, fault: &str) {
    tracing::warn!(monotonic_counter.reconstruction_unresolvable_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "unresolvable-chunk-map",
        inode = %object,
        fault = %fault,
        "reconstruction could not read a committed object's chunk map; this pass drains NOTHING and certifies NOTHING until that record is repaired — operator signal",
    );
}

/// Emit a staged multipart record this pass could **not read** on the durability-plane seam
/// (ADR-0011 / ADR-0012): [`emit_unresolvable`]'s signal, for the staged reading — while any
/// staged record is unreadable this pass cannot show a chunk is named by NO staged record, so
/// it drains NOTHING and certifies NOTHING until that record is repaired. The same action
/// string GC and restore already publish for the same condition (`gc.rs`'s
/// `emit_unresolvable_staged`), so one grep over the durability seam finds every loop blocked
/// on one damaged record. Named through [`crate::gc::object_name`], for the same reason
/// [`emit_unresolvable`] is.
fn emit_unresolvable_staged(record: &str, fault: &str) {
    tracing::warn!(monotonic_counter.reconstruction_unresolvable_staged_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "unresolvable-staged-record",
        record = %record,
        fault = %fault,
        "reconstruction could not read a staged multipart record; this pass drains NOTHING and certifies NOTHING until that record is repaired — operator signal",
    );
}

/// Emit a repair this pass may **not** perform on the same seam: the chunk's committed
/// reference lives in a `seg:` record, whose write path is #682's. Once per **object**, not
/// once per chunk — two obligations inside one segmented object are one refusal.
///
/// A refusal writes nothing at all: the segmented record and its root are left byte-identical,
/// and the obligation stays queued so the under-replication it records is not lost. The pass
/// answers `Blocked` for it, because an operator reading `Satisfied` would be told redundancy
/// is restored for a chunk nothing restored.
fn emit_refused(object: &str) {
    tracing::warn!(monotonic_counter.reconstruction_refused_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "refused-segmented",
        inode = %object,
        "reconstruction refused a repair for a chunk whose committed reference lives in a segmented record; nothing was written, the obligation stays queued, and the pass does not certify",
    );
}

/// Emit an obligation this pass **kept queued** on a staged record's word alone, on the same
/// seam: no committed chunk map references `chunk`, but a staged multipart record — a
/// committed part or an in-flight owned staging entry — still does, so draining it would
/// discard the only record saying it is short a fragment, and this pass may not repair it —
/// `reason` says why: `in-flight` (only an owned staging entry names it; its part has no
/// committed scheme yet), `session-not-open` (its upload has been fenced out of `Open`; the
/// repair runs once the chunk is published, `0016:825`) or `untrusted-staged-record` (a staged
/// record holds it with a placement that cannot be used). Nothing is written, and the pass
/// answers `Blocked` for it, the same reason [`emit_refused`] does: an operator reading
/// `Satisfied` would be told redundancy is restored for a chunk nothing restored.
fn emit_staged(chunk: ChunkId, reason: &'static str) {
    tracing::warn!(monotonic_counter.reconstruction_kept_staged = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "kept-staged",
        reason,
        chunk = %wyrd_traits::chunk_hex(chunk),
        "reconstruction kept an obligation queued: a staged multipart record still names this chunk, though no committed map does, and this pass may not repair it; nothing was drained, and the pass does not certify",
    );
}

/// Emit a lost-CAS conflict on the same seam: the repoint raced another writer and the
/// rebuilt fragments are now collectable garbage — a staged re-place's each under the pre-mark
/// that evidences it (a fence, a rewritten part record, a drain or GC's reclaim moved first).
fn emit_conflict(chunk: ChunkId) {
    tracing::info!(monotonic_counter.reconstruction_conflict = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "conflict",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "reconstruction lost the version-conditional commit; rebuilt fragments are collectable garbage",
    );
}

/// Emit an **aborted** repair on the same seam: the dispatched repair could not proceed
/// (the selector chose a server outside the fleet view — or a staged re-place stopped before
/// it could adopt anything, its own audit line saying why), so nothing was committed. Like
/// [`emit_conflict`], this offsets the up-front [`emit_repaired`] increment — the
/// obligation stays queued and the durability-plane success identity holds
/// (`reconstruction_repaired − conflict − aborted − ceiling_refused`), so an aborted plan
/// never inflates the successful-repair count.
fn emit_aborted(chunk: ChunkId) {
    tracing::info!(monotonic_counter.reconstruction_aborted = 1_u64);
    tracing::info!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "aborted",
        chunk = %wyrd_traits::chunk_hex(chunk),
        "reconstruction could not place the rebuilt shard(s); nothing was committed and the obligation stays queued",
    );
}

/// Emit a repair **refused** because the repointed record would cross the backend value
/// ceiling ([`metadata::flat_value_ceiling_crossed`]) on the same seam: nothing at all was
/// written — not the record and not the rebuilt shard(s) — and the obligation stays queued.
///
/// It offsets the up-front [`emit_repaired`] increment exactly as [`emit_conflict`] and
/// [`emit_aborted`] do, joining rather than inflating the documented identity
/// (`reconstruction_repaired − conflict − aborted − ceiling_refused` = successful repairs;
/// ADR-0011 §2 names this file as the source of truth for their exact emission). It is
/// **warn**, not info, because unlike those two it is not transient: this object's placement
/// can never be repaired again until its record shrinks, so the backlog it leaves behind
/// never drains on its own — the operator's signal.
fn emit_ceiling_refused(chunk: ChunkId, bytes: usize, ceiling: usize) {
    tracing::warn!(monotonic_counter.reconstruction_ceiling_refused = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.reconstruction.audit",
        action = "refused-ceiling",
        chunk = %wyrd_traits::chunk_hex(chunk),
        bytes,
        ceiling,
        "reconstruction refused a repair whose repointed record would cross the backend value ceiling; NOTHING was written, the obligation stays queued and this pass does not certify — NEEDS-HUMAN: the object's record must shrink before its placement can be repaired",
    );
}
