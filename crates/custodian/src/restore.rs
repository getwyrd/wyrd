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
//! ## 3. Bytes the restored map can no longer reach
//!
//! The subtlest of the three, and the only one where **nothing is lost and the chunk is still
//! down**. A repair or rebalance that ran after *V* rebuilt a fragment onto a **new** D server
//! and repointed `placement[index]` at it. The restore rewinds the *map* to the old server —
//! while the *bytes* stay on the new one.
//!
//! Nothing scans for them. Both the read path ([`wyrd_core::read`]) and the repair loop
//! ([`crate::reconstruction`]) fetch a fragment from the D server the **placement names**, and
//! count it missing anywhere else. So those bytes are on disk, intact, and unreachable: reads
//! fail, and reconstruction cannot even rebuild around them.
//!
//! This pass separates that from real loss, in both directions, because conflating them is
//! harmful either way. Marking such a fragment would hand the **only surviving copy** to GC and
//! turn a stale pointer into permanent data loss. Counting it as available would report a chunk
//! as **healthy while every read of it fails**. So it is kept (never marked), and its chunk is
//! reported as *misplaced* — recoverable by fixing the **placement**, never as *dangling*.
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use wyrd_core::metadata::{self, ChunkMapError, InodeRecord, InodeState};
use wyrd_traits::{ChunkId, DServerId, FragmentId, MetadataStore, Result, WriteBatch};

use crate::gc::{
    object_name, orphan_key, orphan_leases, referenced_fragments, GcContext, ReferenceSet,
};

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
    /// (repairable) into real data loss.
    ///
    /// They are **not readable**, either: the read path and the repair loop both resolve
    /// fragments strictly through the placement (see [`reconcile_after_restore`]'s pass 3), so
    /// bytes sitting anywhere else are bytes nothing will fetch. Kept, reported — and a chunk
    /// left below `k` by them lands in [`RestoreReport::misplaced`], never in
    /// [`RestoreReport::under_replicated`].
    pub displaced_kept: usize,
    /// Committed chunks with **fewer than `k` fragments anywhere in the fleet**: unreadable,
    /// and unreconstructible. A restore resurrected the map after the bytes were reclaimed.
    /// **These files are lost** — the pass reports them, it cannot recover them.
    pub dangling: Vec<ChunkId>,
    /// Committed chunks whose bytes **exist** but sit where the restored map does not look:
    /// fewer than `k` fragments at the D servers the placement names, yet at least `k` present
    /// across the fleet. Reads fail and the repair loop cannot rebuild them — both fetch by
    /// placement — so these chunks are **down**. But nothing is lost: the *placement* is stale,
    /// not the data. Recoverable, and never to be confused with [`RestoreReport::dangling`].
    pub misplaced: Vec<ChunkId>,
    /// Committed chunks missing fragments but still holding **at least `k` at their placement**:
    /// readable, and the reconstruction loop will rebuild them. Reported for visibility.
    pub under_replicated: Vec<ChunkId>,
    /// Committed objects whose chunk map this pass could **not read** — a segmented generation
    /// whose `seg:` records are incomplete, or a record that will not decode — named by
    /// `inode:` key as the store spells it ([`crate::gc::ReferenceSet::unresolvable`], escaped
    /// rather than rendered lossily so two damaged records never arrive under one name).
    /// Ordered by that key.
    ///
    /// The pass keeps going past them and marks **nothing** on their account: while any object
    /// is unresolvable the reference set is incomplete, so every fragment in the fleet is held
    /// off-limits and [`RestoreReport::stranded_marked`] stays 0.
    ///
    /// They are reported because every *other* verdict here — dangling, misplaced,
    /// under-replicated — is drawn over the objects the pass COULD read: a clean report with a
    /// non-empty list here is a clean report about **part** of the store, and an operator
    /// reading it as a clean bill would decommission on it.
    pub unresolvable: Vec<String>,
}

impl RestoreReport {
    /// Did the pass find anything an operator must act on or absorb — **and** did its reading
    /// finish?
    ///
    /// The strict superset of [`Self::needs_human`]: it also counts the work this pass DID
    /// (fragments marked collectable) and the work the repair loop will do (under-replicated
    /// chunks), neither of which is a human's. Written **in terms of** that predicate rather
    /// than beside it, so the two cannot drift as fields are added.
    ///
    /// An [unresolvable object](RestoreReport::unresolvable) counts: "clean" is a claim about a
    /// reading that FINISHED, and this one did not — so a store the pass could only partly read
    /// is never certified clean (`docs/principles.md` §5 C-1).
    pub fn is_clean(&self) -> bool {
        self.stranded_marked == 0 && self.under_replicated.is_empty() && !self.needs_human()
    }

    /// Does this run need a **human** — the question `wyrd custodian --reconcile-after-restore`
    /// turns into its exit status (`crates/server/src/cli.rs`'s `restore_verdict`)?
    ///
    /// The three findings no loop resolves on its own: chunks that can no longer be read at all,
    /// chunks whose bytes are somewhere the restored map does not look, and committed objects
    /// this pass could not read. Marks and under-replication are deliberately **not** here — the
    /// first is this pass doing its job and the second is the reconstruction loop's, so failing
    /// a restore script on either would train an operator to ignore the status. It lives on the
    /// report rather than in the command because a caller that never prints the summary still
    /// needs the same verdict, and would otherwise re-derive it slightly differently.
    pub fn needs_human(&self) -> bool {
        !self.dangling.is_empty() || !self.misplaced.is_empty() || !self.unresolvable.is_empty()
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
/// # An object it cannot read is CONTAINED, and the run is not certified
///
/// A committed object whose chunk map cannot be read (an incomplete segmented generation, a
/// record that will not decode) does not end the pass and does not blank its answer: it is
/// named in [`RestoreReport::unresolvable`], every object the pass *could* read is still
/// reported, nothing is marked anywhere in the fleet while it remains, and
/// [`RestoreReport::is_clean`] is false — the operator gets the post-restore picture *and* the
/// record to repair, instead of an `Err` carrying neither. A fault that is **not** one object's
/// map — a metadata store failing under the read — still propagates, exactly as it does for GC
/// and scrub.
///
/// Each such record is **named on the durability seam the moment a read meets it**, before the
/// next store read — `gc::reconcile`'s placement for the same set. That is what a propagating
/// store fault must not take with it: the pass can still end in `Err` (the store, not one
/// object, failed), but a record it had already identified as unreadable stays attributed, so
/// the operator has something to repair rather than an error naming nothing.
///
/// # The marks and the report rest on ONE reading
///
/// This pass reads the committed namespace twice: once to build the reference set the mark half
/// gates on ([`referenced_fragments`]), once for the per-reference expectations the report half
/// judges against ([`committed_chunks`]). They are two reads of the same records an instant
/// apart, and they can disagree — a record damaged between them, an object committed between
/// them. A mark is an authorization to delete, so a disagreement may never be resolved in the
/// direction that deletes, and an operator shown one conclusion drawn from two disagreeing
/// readings has no way to tell which one it rests on. The two are therefore treated as **one**,
/// in both directions:
///
/// - **either** read's hole withholds every mark in the fleet, and the names in the report are
///   the **union** of both; and
/// - a fragment **either** read protects is never marked — placed by a valid committed
///   placement, or bearing the id of a chunk whose placement is malformed. An object that
///   commits, or a placement that changes, between the two reads is protected by the read that
///   saw it ([`AppearedSince`]), so the mark half can never act on a reference set the report
///   half has already moved past.
///
/// Both clauses cost the same thing — a stray that survives to the next run of an idempotent
/// pass — and buy the one outcome this pass must never produce: GC handed a live object's only
/// copy. A commit that lands after *both* reads remains the runbook's business, not this pass's
/// (it is an operator one-shot, run with writers stopped); what is this pass's own is that the
/// two readings it makes of its own accord never license a mark between them.
///
/// Marking is deletion-capable at one remove, so the rule is pinned under the **deterministic
/// simulator** as well as by the per-pass tests: the seeded Tier-0 property
/// `restore_two_readings_never_license_a_mark` (`crates/dst/tests/custodian.rs`, ADR-0009) lands
/// a genuinely concurrent writer between these two reads at an instant drawn from the run seed,
/// over a store whose every read and commit spans a simulated network hop.
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
    // The SAME committed reference set GC and scrub gate on, built through the shared resolver
    // — so a **segmented** object's chunks are in it here too, and a committed record the build
    // cannot read is contained rather than raised (`gc::ReferenceSet::unresolvable`).
    //
    // The MARK half is fail-closed over an incomplete set: the gate below withholds every
    // fragment while either read found a hole, so nothing an unreadable object might own is ever
    // marked for GC.
    //
    // The REPORT half is CONTAINED rather than fatal, and that is this slice's work (#651): it
    // reports every object it could read, names the ones it could not, and refuses to call the
    // run clean. Until now it re-read the same records and `?`d out on the first it could not
    // parse — so a store holding a single unreadable or segmented object produced no report AT
    // ALL: not the stranded count, not the dangling or misplaced chunks of the objects it could
    // read, at exactly the moment an operator needs them most.
    let referenced = referenced_fragments(ctx.meta).await?;
    // ATTRIBUTED THE INSTANT IT IS KNOWN, per object, before the next store read — the placement
    // `gc::reconcile` uses for the same set (`gc.rs`, its `unresolvable` loop sits between the
    // reference build and the fleet walk). Batching these names behind the reads below would
    // mean a genuine, unrelated store fault in any of them — one `?` away — ends the pass with
    // an `Err` carrying nothing, and the record the operator must repair, ALREADY KNOWN by
    // then, never reaches them at all. Attribution that a later transient fault can swallow is
    // not attribution.
    let mut unreadable = BTreeSet::new();
    attribute_unresolvable(&referenced.unresolvable, &mut unreadable);
    let already = orphan_leases(ctx.meta).await?;
    let pending = pending_chunks(ctx.meta).await?;
    // Read UP FRONT, before a fragment is marked: the mark gate below has to know about a hole
    // THIS read found before it decides anything (see the one-reading rule in this function's
    // docs). It is the same list this pass has always materialized, taken here rather than at
    // the walk below — and what it could not read is attributed on the same terms, at once.
    let committed = committed_chunks(ctx.meta).await?;
    attribute_unresolvable(&committed.unresolvable, &mut unreadable);
    // ...and what THAT read protects which the reference build did not — normally nothing at all
    // (see [`AppearedSince`] and the one-reading rule in this function's docs). The mark gate
    // below consults both, so an object that committed between the two reads cannot have its live
    // fragments marked on the strength of the older one.
    let appeared = appeared_since(&referenced, &committed);

    let mut report = RestoreReport {
        // Either read's hole makes this report partial: the mark half is drawn from
        // `referenced`, the verdicts below from `committed`. So the names are the UNION — a
        // record only one of them could read is still a record this run cannot speak for —
        // deduplicated and in the store's own key order, whichever read met each of them.
        unresolvable: unreadable.iter().map(|key| object_name(key)).collect(),
        ..Default::default()
    };
    // ONE READING, ONE CONCLUSION. `gc::ReferenceSet::protects` already withholds every fragment
    // in the fleet while the reference BUILD found a hole; this extends the same withholding to
    // a hole the verdict read found, so the pass can never both mark a fragment and report a
    // record it could not read. Whichever read met the damage, the answer is the same one.
    let incomplete = !report.unresolvable.is_empty();
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
    // on from (below). Over BOTH readings, on the same rule as the gate: a placement only the
    // report read saw is still the map's opinion. `appeared.placed` holds only what
    // `referenced.placed` does not, so no holder is listed twice.
    let mut canonical: HashMap<FragmentId, Vec<DServerId>> = HashMap::new();
    for &(dserver, frag) in referenced.placed.iter().chain(appeared.placed.iter()) {
        canonical.entry(frag).or_default().push(dserver);
    }

    // Pass 2 — decide, with the full picture.
    for (dserver, frag) in on_disk {
        // SAFETY GATE, identical to GC's: never mark a fragment the restored map points at —
        // nor any fragment of a malformed-placement chunk, whose true placement cannot be
        // trusted (fail safe) — nor anything at all while either read of the committed
        // namespace found a record it could not read. An unreadable map hides WHICH chunks its
        // object owns, so no fragment in the fleet can be shown not to be one of them.
        //
        // Over BOTH readings of that namespace, never the older one alone: a fragment the report
        // read finds referenced is referenced, whichever read of this pass met the record that
        // says so. Otherwise an object committed in the instant between the two reads — absent
        // from `referenced` and present in `committed` — would have its live fragments marked
        // collectable, and GC would take the only copy after the grace window.
        if incomplete || referenced.protects(dserver, frag) || appeared.protects(dserver, frag) {
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

    // Pass 3 — the metadata's view. TWO questions, never conflated: can the restored map still
    // READ this chunk, and do its bytes still EXIST? A restore can break the first without
    // breaking the second, and answering only one of them is a lie in one direction or the
    // other (both spelled out below).
    for &(chunk, ref expected) in &committed.chunks {
        // READABLE is "present at the D server the committed placement NAMES" — nothing weaker.
        // Both consumers of a placement resolve it strictly, and neither scans the fleet:
        //
        //   * the read path fetches `get_fragment_at(fragment_dserver(chunk, i), ..)`
        //     (`wyrd_core::read`); and
        //   * reconstruction's `assess` walks `placement` and does `stores.get(&dserver)`
        //     (`crate::reconstruction`), counting a fragment found anywhere else as MISSING.
        //
        // So a DISPLACED fragment — on disk, but not where the rewound map looks — is unreadable
        // AND unusable by the repair loop. Counting it as available would report a chunk as
        // healthy while every read of it fails, and would let the command exit 0 over a chunk
        // that is down. A false all-clear is not a kinder error than a false alarm.
        let placed = expected
            .frags
            .iter()
            .filter(|&&(dserver, frag)| present.contains(&(dserver, frag)))
            .count();

        // ...but bytes that exist SOMEWHERE are not LOST, and "your data is gone" is the worst
        // thing this command can say. A repair after the restore point moved a fragment and
        // repointed the placement; the restore rewound the map, not the bytes. So LOSS is judged
        // across the whole fleet — and unreachability is reported as its own, recoverable state
        // rather than being rounded up into data loss or down into health.
        let anywhere = expected
            .frags
            .iter()
            .filter(|&&(_dserver, frag)| present_anywhere.contains(&frag))
            .count();

        let k = usize::from(expected.k);
        if anywhere < k {
            // Fewer than k fragments exist AT ALL: nothing to rebuild from. Lost.
            report.dangling.push(chunk);
            emit_dangling(chunk, anywhere, expected.k, expected.frags.len());
        } else if placed < k {
            // Every byte is here — just not where the map points. Reads fail, and the repair
            // loop cannot rebuild from fragments it will never fetch. The chunk is DOWN, and
            // recovering it means fixing the PLACEMENT, not the data. Reported loudly, and
            // never as loss.
            report.misplaced.push(chunk);
            emit_misplaced(chunk, placed, anywhere, expected.k, expected.frags.len());
        } else if placed < expected.frags.len() {
            // At least k readable at the placement: the repair loop rebuilds the rest from
            // exactly the fragments it can actually fetch.
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

/// What the report half could read of the committed namespace, and what it could not.
struct CommittedChunks {
    /// One entry per committed chunk **reference**, in scan order. Each is judged against ITS
    /// OWN placement: grouping by chunk id would let one object's healthy copy answer for
    /// another object's missing one — the second object is unreadable (the read path fetches
    /// strictly by ITS placement) while the merged verdict reads "under-replicated, the repair
    /// loop will handle it", and the command exits 0 over a down object.
    chunks: Vec<(ChunkId, Expected)>,
    /// Chunk ids whose committed placement is **malformed** (ADR-0040 decision 4). Not judged —
    /// a placement that cannot be trusted is not one to declare dangling, the same fail-safe skip
    /// this pass has always made — but recorded, because a chunk *this* read found and the
    /// reference build did not still protects every fragment bearing its id from the mark half
    /// ([`AppearedSince`]).
    malformed: HashSet<ChunkId>,
    /// The committed objects whose chunk map could not be read at all, keyed by `inode:` key
    /// exactly as the store spells it and valued by the fault — the shape
    /// [`crate::gc::ReferenceSet::unresolvable`] uses, for the same reason (a rendered name is
    /// not injective, so two damaged records could collapse into one entry and one would go
    /// unreported).
    unresolvable: BTreeMap<Vec<u8>, String>,
}

/// What the report read of the committed namespace protects that the reference build did not —
/// the **divergence between this pass's two readings**, and nothing else.
///
/// Empty whenever they agree, which is every run of this operator one-shot as the runbook
/// prescribes it (writers stopped). It exists for the runs where they do not: an object that
/// commits, or a placement a repair repoints, between [`referenced_fragments`] and
/// [`committed_chunks`] is absent from [`crate::gc::ReferenceSet::protects`] and present in the
/// verdicts drawn below it, and marking its fragments on the strength of the older reading would
/// hand GC bytes the newer one says are live. Only the difference is kept, never a second copy of
/// the placement set: it is exactly what the older reading cannot speak for.
#[derive(Default)]
struct AppearedSince {
    /// `(dserver, fragment)` a valid committed placement points at in the report read alone.
    placed: HashSet<(DServerId, FragmentId)>,
    /// Chunk ids the report read alone found malformed — treated as **fully referenced**, exactly
    /// as [`crate::gc::ReferenceSet`] treats one its own read found (ADR-0040 decision 4): the
    /// placement cannot be trusted, so every fragment bearing the id is off-limits.
    malformed: HashSet<ChunkId>,
}

impl AppearedSince {
    /// Whether the report read protects `frag` on `dserver` where the reference build did not —
    /// the second half of the mark gate, by the same two rules
    /// [`crate::gc::ReferenceSet::protects`] applies to the first (a valid placed reference, or
    /// any fragment of a malformed-placement chunk).
    fn protects(&self, dserver: DServerId, frag: FragmentId) -> bool {
        self.placed.contains(&(dserver, frag)) || self.malformed.contains(&frag.chunk)
    }
}

/// Difference the report read against the reference build: everything the former protects and the
/// latter never saw.
///
/// The incompleteness half of the same disagreement is handled by the caller (either read's hole
/// withholds the whole fleet), so this is only about references that EXIST in one reading — the
/// direction where acting on the older one deletes live data rather than merely over-reporting.
fn appeared_since(referenced: &ReferenceSet, committed: &CommittedChunks) -> AppearedSince {
    let mut appeared = AppearedSince::default();
    for (_chunk, expected) in &committed.chunks {
        for pair in &expected.frags {
            if !referenced.placed.contains(pair) {
                appeared.placed.insert(*pair);
            }
        }
    }
    for chunk in &committed.malformed {
        if !referenced.malformed.contains_key(chunk) {
            appeared.malformed.insert(*chunk);
        }
    }
    appeared
}

/// Every **committed** chunk this pass could read, with its `k` and its placement — plus the
/// objects it could not read at all.
///
/// Each committed record is resolved through the ONE resolver every consumer shares
/// ([`metadata::resolve_chunk_map`], proposal 0016 decision 7(e)), so a **segmented** object's
/// chunks are judged here like any other instead of ending the pass; and a record that will not
/// decode, or a generation the resolver cannot read, is CONTAINED — recorded in
/// [`CommittedChunks::unresolvable`] and skipped, with the walk going on. A fault that is not
/// this object's own (a store failing under the read) still propagates, by exactly the downcast
/// rule [`referenced_fragments`] uses: a walk that cannot reach the metadata store has no answer
/// for any object, not one unreadable object.
///
/// Malformed placements are skipped, which is the same fail-safe skip this pass always applied:
/// GC treats such a chunk as fully referenced rather than trusting a placement vector it cannot
/// (ADR-0040 decision 4), and a chunk whose placement cannot be trusted is not one to declare
/// dangling.
///
/// The network bound on the resolve await is the `MetadataStore` IMPLEMENTATION's, not this
/// caller's (#508/#636) — the same rule [`referenced_fragments`] follows for the same call, and
/// the same rule the `meta.scan(b"inode:")` here has always followed. It is fail-closed either
/// way: an error there either propagates or contains the object, never "this object owns no
/// bytes".
///
/// This is the pass's **second** reading of the committed namespace, and what it protects that
/// the first did not is reconciled by [`appeared_since`] before a single mark is written. That
/// reconciliation is exercised under the simulator by the seeded Tier-0 property
/// `restore_two_readings_never_license_a_mark` (`crates/dst/tests/custodian.rs`), not only by the
/// per-pass doubles.
///
/// deferred: #681 — this repeats [`referenced_fragments`]'s decode/resolve/contain shape over
/// the same records because the two halves need different granularity (a fleet-wide protection
/// set there, per-reference expectations here). The maintenance walk that both would share is
/// that slice's; this one is restore's own scan, upgraded in place from "fail closed on the
/// first record I cannot read" to "contain it and keep reporting".
async fn committed_chunks(meta: &dyn MetadataStore) -> Result<CommittedChunks> {
    let mut chunks = Vec::new();
    let mut malformed = HashSet::new();
    let mut unresolvable = BTreeMap::new();
    for (key, value) in meta.scan(b"inode:").await? {
        // The record's own bytes are in hand, so a decode failure is THIS object's fault and no
        // store's — contained, and conservatively without first asking whether the record was
        // committed (reading `state` out of bytes that will not decode needs a lenient peek
        // this crate owns no decoder for; blocking until the record is repaired is the
        // fail-closed direction).
        let record: InodeRecord = match metadata::decode(&value) {
            Ok(record) => record,
            Err(fault) => {
                unresolvable.insert(key.clone(), fault.to_string());
                continue;
            }
        };
        if record.state != InodeState::Committed {
            continue;
        }
        // `Ok(None)` is no live committed generation under this key (deleted or retired since
        // the scan read it): nothing left to report on, skipped exactly as an uncommitted
        // record is above.
        let resolved = match metadata::resolve_chunk_map(meta, &key, &record).await {
            Ok(Some(resolved)) => resolved,
            Ok(None) => continue,
            Err(err) => match err.downcast::<ChunkMapError>() {
                // The resolver's own typed verdict that THIS generation cannot be read —
                // recovered by downcast because the trait seam boxes every error. Contained.
                Ok(fault) => {
                    unresolvable.insert(key.clone(), fault.to_string());
                    continue;
                }
                // Not a chunk-map anomaly: a store fault under the read, which is not this
                // object's fault and is not folded into "this object is unreadable".
                Err(err) => return Err(err),
            },
        };
        for chunk in resolved.chunks.iter() {
            let Ok(frags) = chunk.checked_fragments() else {
                // Skipped as a verdict (above), KEPT as a protection: this read found the chunk,
                // and if the reference build did not, its fragments have nothing else standing
                // between them and a mark.
                malformed.insert(chunk.id);
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
            chunks.push((
                chunk.id,
                Expected {
                    k: reconstruction_threshold(chunk),
                    frags,
                },
            ));
        }
    }
    Ok(CommittedChunks {
        chunks,
        malformed,
        unresolvable,
    })
}

/// Name every object `faults` could not read on the durability seam, and record it in `named` —
/// the union this pass reports, keyed by the store's own key bytes.
///
/// Called once per read of the committed namespace, **the moment that read returns**: the mark
/// gate is driven by `referenced_fragments` and the verdicts by [`committed_chunks`], so a record
/// either could not read leaves this run unable to speak for that object — and neither read's
/// names may wait on the other's. Emitting them per object as they become known, ahead of every
/// store read that follows, is `gc::reconcile`'s placement for the same set and for the same
/// reason: a store fault a `?` later ends the pass with an `Err`, and a name this pass ALREADY
/// HELD must not go down with it. The operator's next move is repairing that record.
///
/// Attribution is once per object, not once per read: `named` is the set already emitted, so a
/// record BOTH reads met is reported and counted once, under one name.
///
/// Named through [`crate::gc::object_name`], which escapes rather than replaces — two damaged
/// records must never arrive under one name, or a repair guided by it fixes one and leaves the
/// other blocking the fleet.
fn attribute_unresolvable(faults: &BTreeMap<Vec<u8>, String>, named: &mut BTreeSet<Vec<u8>>) {
    for (key, fault) in faults {
        if named.insert(key.clone()) {
            emit_unresolvable(&object_name(key), fault);
        }
    }
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

/// A committed chunk whose bytes all still exist, but fewer than `k` of them sit where the
/// restored map looks. The read path and the repair loop both resolve fragments strictly by
/// placement, so this chunk is unreadable *and* unrebuildable — while nothing has been lost.
/// Deliberately NOT [`emit_dangling`]: telling an operator their data is gone when it is sitting
/// on a D server one hop away would send them to a backup they do not need.
fn emit_misplaced(chunk: ChunkId, placed: usize, anywhere: usize, k: u16, n: usize) {
    tracing::error!(monotonic_counter.restore_misplaced_chunks = 1_u64);
    tracing::error!(
        target: "wyrd.custodian.restore.audit",
        action = "misplaced",
        chunk = %wyrd_traits::chunk_hex(chunk),
        placed,
        anywhere,
        required = k,
        total = n,
        "post-restore: committed chunk has fewer than k fragments AT THE PLACEMENT the restored \
         map names, though at least k exist elsewhere in the fleet. Reads resolve fragments by \
         placement and will FAIL, and the repair loop fetches by placement too, so it cannot \
         rebuild this chunk either. The data is NOT lost — the PLACEMENT is stale. Restage the \
         displaced fragments onto the D servers the map names (or repoint the placement at where \
         the bytes actually are), then re-run this pass",
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

/// A committed object whose chunk map this pass could **not read**, named and attributed on the
/// durability-plane seam (ADR-0011 / ADR-0012), exactly as GC and scrub name the same record on
/// theirs: nothing of it was marked (the mark gate withholds the whole fleet while the reference
/// set is incomplete), and every verdict in the report excludes it.
///
/// Emitted as soon as a read of the committed namespace meets the record — so this name survives
/// even when a later store fault ends the whole pass with an `Err` and no report is returned at
/// all. That case is the seam's alone to carry: the operator has one thing to do about an
/// unreadable record, and it starts with knowing which one it is.
fn emit_unresolvable(object: &str, fault: &str) {
    tracing::warn!(monotonic_counter.restore_unresolvable_records = 1_u64);
    tracing::warn!(
        target: "wyrd.custodian.restore.audit",
        action = "unresolvable-chunk-map",
        inode = %object,
        fault = %fault,
        "post-restore: a committed object's chunk map could not be read; nothing was marked on its account and every count in this report is drawn over the objects that COULD be read — this run is NOT a clean bill for the store until this record is repaired",
    );
}

/// The pass's own verdict, so a restore's true cost lands in one line an operator can read.
///
/// It says **complete** only when the reading finished. Over a store with an unreadable
/// committed record in it the same line would otherwise be the certification the rest of this
/// pass refuses to give, in the one place an operator greps for it.
fn emit_summary(report: &RestoreReport) {
    tracing::info!(
        target: "wyrd.custodian.restore.audit",
        action = "summary",
        stranded_marked = report.stranded_marked,
        already_marked = report.already_marked,
        pending_skipped = report.pending_skipped,
        displaced_kept = report.displaced_kept,
        dangling = report.dangling.len(),
        misplaced = report.misplaced.len(),
        under_replicated = report.under_replicated.len(),
        // The qualifier on every count above: they are drawn over the objects this pass could
        // read, and this is how many it could not.
        unresolvable = report.unresolvable.len(),
        // The pass's own two-word verdict, so the predicate the report offers its callers is the
        // one its audit trail states rather than a third rendering of the same fields.
        clean = report.is_clean(),
        needs_human = report.needs_human(),
        "post-restore reconciliation {}",
        if report.unresolvable.is_empty() {
            "complete"
        } else {
            "INCOMPLETE — every count above covers only the objects this pass could read"
        },
    );
}
